//! Sharded book engine — single-writer per ticker.
//!
//! The engine owns N worker shards. Each ticker is routed (deterministically,
//! via `ahash`) to one shard. A worker only sees ops for the tickers in its
//! shard, processed serially — no locks, no contention.
//!
//! Architecture (`ARCHITECTURE.md §5.5.2`):
//!
//! ```text
//! parser pool ──hash(ticker)──> [shard 0 worker] ─┐
//!                ↳            > [shard 1 worker] ─┼─> emit BookDelta to fanout
//!                ↳            > [shard 2 worker] ─┤
//!                ↳            > [shard N worker] ─┘
//! ```
//!
//! For now the engine exposes a synchronous `apply` that runs in the caller's
//! thread; this is what tests + the in-process orchestrator use. A future
//! iteration will spawn dedicated worker threads and `crossbeam` channels.

use crate::book::order_book::OrderBook;
use crate::shard::shard_for;
use ahash::AHashMap;
use bytes::Bytes;
use cedro_protocol::types::BookOp;
use std::sync::{Mutex, RwLock};

/// Sharded registry of `OrderBook` instances.
///
/// Each shard is guarded by its own `Mutex<AHashMap<...>>`. The mutex never
/// contends with parser threads holding *other* shards, so a "hot" ticker
/// can be churning without blocking unrelated tickers.
#[derive(Debug)]
pub struct BookEngine {
    shards: Vec<Mutex<AHashMap<Bytes, OrderBook>>>,
    /// Tickers we accept books for (per `bqt_scope` memory: ações + opções
    /// Bovespa only). `None` means accept everything (useful in tests).
    allowed: RwLock<Option<ahash::AHashSet<Bytes>>>,
}

impl BookEngine {
    /// Build an engine with `n_shards` workers. Pick a power of two for the
    /// best modulo behaviour; defaults to 8 if you pass 0.
    pub fn new(n_shards: usize) -> Self {
        let n = if n_shards == 0 { 8 } else { n_shards };
        let mut shards = Vec::with_capacity(n);
        for _ in 0..n {
            shards.push(Mutex::new(AHashMap::new()));
        }
        Self {
            shards,
            allowed: RwLock::new(None),
        }
    }

    /// Restrict the engine to a specific set of tickers (ações + opções
    /// Bovespa in production — see ARCHITECTURE §5.5.2). Pass `None` to
    /// re-enable the unfiltered mode.
    pub fn set_allowed(&self, allowed: Option<ahash::AHashSet<Bytes>>) {
        *self.allowed.write().expect("allowed lock poisoned") = allowed;
    }

    /// Is the engine willing to track this ticker? When no filter is set,
    /// every ticker is allowed.
    pub fn is_allowed(&self, ticker: &[u8]) -> bool {
        let guard = self.allowed.read().expect("allowed lock poisoned");
        match guard.as_ref() {
            None => true,
            Some(set) => set.contains(ticker),
        }
    }

    /// Apply a [`BookOp`] to the named ticker.
    ///
    /// Returns `true` if the resulting book is observable (initial snapshot
    /// done). Returns `false` (without touching state) if the ticker is not
    /// in the allowed set.
    pub fn apply(&self, ticker: &Bytes, op: BookOp) -> bool {
        if !self.is_allowed(ticker) {
            metrics::counter!("market_engine_book_dropped_disallowed_total").increment(1);
            return false;
        }

        let idx = shard_for(ticker, self.shards.len());
        let mut guard = self.shards[idx].lock().expect("book shard mutex poisoned");
        let book = guard.entry(ticker.clone()).or_default();
        let observable = book.apply(op);
        metrics::counter!("market_engine_book_ops_total").increment(1);
        observable
    }

    /// Read-only access to a ticker's book. The closure runs while the
    /// shard mutex is held — keep it short.
    pub fn with_book<R>(&self, ticker: &[u8], f: impl FnOnce(Option<&OrderBook>) -> R) -> R {
        let idx = shard_for(ticker, self.shards.len());
        let guard = self.shards[idx].lock().expect("book shard mutex poisoned");
        f(guard.get(ticker))
    }

    /// Number of distinct tickers across every shard. `O(n_shards)` — cheap.
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().expect("book shard mutex poisoned").len())
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cedro_protocol::enums::Side;
    use cedro_protocol::types::{BookEntry, BookOp};

    fn entry(side: Side, pos: u32, price: f64) -> BookEntry {
        BookEntry {
            position: pos,
            side,
            price,
            quantity: 1,
            broker_id: 1,
            timestamp_ddmmhhmm: 0,
            order_id: None,
            offer_type: None,
        }
    }

    #[test]
    fn applies_ops_through_to_per_ticker_books() {
        let engine = BookEngine::new(4);
        let petr = Bytes::from_static(b"PETR4");
        engine.apply(&petr, BookOp::Add(entry(Side::Buy, 0, 100.0)));
        engine.apply(&petr, BookOp::Add(entry(Side::Buy, 1, 99.0)));
        engine.apply(&petr, BookOp::EndOfInitial);
        engine.with_book(b"PETR4", |b| {
            let book = b.expect("PETR4 book should exist");
            let s = book.stats();
            assert_eq!(s.bids, 2);
            assert_eq!(s.asks, 0);
            assert!(book.is_initialised());
        });
    }

    #[test]
    fn distinct_tickers_can_share_a_shard_without_corruption() {
        // Force shard count to 1 so both tickers land together.
        let engine = BookEngine::new(1);
        engine.apply(
            &Bytes::from_static(b"PETR4"),
            BookOp::Add(entry(Side::Buy, 0, 100.0)),
        );
        engine.apply(
            &Bytes::from_static(b"VALE3"),
            BookOp::Add(entry(Side::Sell, 0, 200.0)),
        );
        engine.with_book(b"PETR4", |b| {
            assert_eq!(b.unwrap().stats().bids, 1);
        });
        engine.with_book(b"VALE3", |b| {
            assert_eq!(b.unwrap().stats().asks, 1);
        });
        assert_eq!(engine.len(), 2);
    }

    #[test]
    fn allowlist_filter_blocks_disallowed_tickers() {
        let engine = BookEngine::new(2);
        let mut allowed = ahash::AHashSet::new();
        allowed.insert(Bytes::from_static(b"PETR4"));
        engine.set_allowed(Some(allowed));

        let petr = Bytes::from_static(b"PETR4");
        let wdo = Bytes::from_static(b"WDOZ25");
        // PETR4 is in the allow-list — call should succeed (return value is
        // `false` because we haven't sent EndOfInitial yet; we only assert
        // the book was created).
        let _ = engine.apply(&petr, BookOp::Add(entry(Side::Buy, 0, 1.0)));
        let result = engine.apply(&wdo, BookOp::Add(entry(Side::Buy, 0, 1.0)));
        assert!(!result, "WDO should be rejected");
        engine.with_book(b"WDOZ25", |b| assert!(b.is_none()));
        engine.with_book(b"PETR4", |b| assert_eq!(b.unwrap().stats().bids, 1));
    }

    #[test]
    fn clearing_the_filter_re_enables_every_ticker() {
        let engine = BookEngine::new(2);
        engine.set_allowed(Some(ahash::AHashSet::new())); // empty allow-list = nothing allowed
        let petr = Bytes::from_static(b"PETR4");
        assert!(!engine.apply(&petr, BookOp::Add(entry(Side::Buy, 0, 1.0))));
        engine.set_allowed(None);
        let _ = engine.apply(&petr, BookOp::Add(entry(Side::Buy, 0, 1.0)));
        engine.with_book(b"PETR4", |b| {
            assert_eq!(b.unwrap().stats().bids, 1);
        });
    }
}
