//! Quote state engine.
//!
//! Holds the per-ticker `QuoteState` and exposes a single hot-path operation:
//! [`QuoteEngine::apply_diff`]. The state is a `DashMap<Ticker, QuoteState>`;
//! `DashMap` is internally sharded with `RwLock`s per shard, so contention is
//! limited to the shard hosting a given ticker.
//!
//! Each `QuoteState` is a sparse field map keyed by Cedro's index numbers,
//! storing the strongly-typed [`QuoteFieldValue`]. Building a struct with
//! 160+ optional fields would be tempting, but the wire format already keys
//! by index — keeping the map representation cuts the per-ticker memory
//! footprint dramatically.

use ahash::AHashMap;
use bytes::Bytes;
use cedro_protocol::types::{QuoteDiff, QuoteFieldId, QuoteFieldValue};
use dashmap::DashMap;

/// One ticker's consolidated quote.
#[derive(Debug, Clone, Default)]
pub struct QuoteState {
    /// Last `HHMMSS` reported by the server (header field of `T:...`).
    /// `0` until the first message lands.
    pub time_hhmmss: u32,
    /// Number of `T:` updates we've applied to this state.
    pub version: u64,
    /// Sparse map from `QuoteFieldId.0` to its latest value.
    pub fields: AHashMap<u16, QuoteFieldValue>,
}

impl QuoteState {
    /// Apply a diff (full snapshot or delta). Returns the number of fields
    /// that were inserted or updated.
    pub fn apply_diff(&mut self, time_hhmmss: u32, diff: QuoteDiff) -> usize {
        self.time_hhmmss = time_hhmmss;
        self.version = self.version.wrapping_add(1);
        let n = diff.len();
        for (QuoteFieldId(idx), value) in diff {
            self.fields.insert(idx, value);
        }
        n
    }

    /// Look up a typed field by Cedro index.
    pub fn get(&self, idx: u16) -> Option<&QuoteFieldValue> {
        self.fields.get(&idx)
    }
}

/// Concurrent registry of `QuoteState`s, keyed by ticker.
#[derive(Debug)]
pub struct QuoteEngine {
    states: DashMap<Bytes, QuoteState, ahash::RandomState>,
}

impl QuoteEngine {
    pub fn new() -> Self {
        Self {
            states: DashMap::with_hasher(ahash::RandomState::default()),
        }
    }

    /// Apply an SQT diff to the named ticker, creating the entry if absent.
    /// Returns the number of fields touched.
    pub fn apply(&self, ticker: Bytes, time_hhmmss: u32, diff: QuoteDiff) -> usize {
        let mut entry = self.states.entry(ticker).or_default();
        let updated = entry.apply_diff(time_hhmmss, diff);
        metrics::counter!("market_engine_quote_updates_total").increment(1);
        metrics::counter!("market_engine_quote_fields_updated_total").increment(updated as u64);
        updated
    }

    /// Borrow a state for read-only access. Holds a `DashMap` shard lock for
    /// the duration of the closure — keep `f` short.
    pub fn with_state<R>(&self, ticker: &[u8], f: impl FnOnce(Option<&QuoteState>) -> R) -> R {
        match self.states.get(ticker) {
            Some(entry) => f(Some(entry.value())),
            None => f(None),
        }
    }

    /// Cheap snapshot of the (ticker, version, time) tuple for every active
    /// state. Used by observability and tests; not on the hot path.
    pub fn known_tickers(&self) -> Vec<(Bytes, u64, u32)> {
        self.states
            .iter()
            .map(|kv| (kv.key().clone(), kv.value().version, kv.value().time_hhmmss))
            .collect()
    }

    /// Number of distinct tickers currently tracked.
    pub fn len(&self) -> usize {
        self.states.len()
    }

    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }
}

impl Default for QuoteEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cedro_protocol::types::QuoteFieldValue;

    fn diff(pairs: &[(u16, QuoteFieldValue)]) -> QuoteDiff {
        pairs
            .iter()
            .cloned()
            .map(|(idx, v)| (QuoteFieldId(idx), v))
            .collect()
    }

    #[test]
    fn first_apply_creates_state_and_records_fields() {
        let engine = QuoteEngine::new();
        let updated = engine.apply(
            Bytes::from_static(b"PETR4"),
            101_758,
            diff(&[
                (2, QuoteFieldValue::Float(59.95)),
                (9, QuoteFieldValue::Int(354_600)),
            ]),
        );
        assert_eq!(updated, 2);
        engine.with_state(b"PETR4", |s| {
            let state = s.expect("PETR4 should exist");
            assert_eq!(state.time_hhmmss, 101_758);
            assert_eq!(state.version, 1);
            assert_eq!(state.get(2), Some(&QuoteFieldValue::Float(59.95)));
            assert_eq!(state.get(9), Some(&QuoteFieldValue::Int(354_600)));
        });
    }

    #[test]
    fn delta_overrides_only_changed_fields() {
        let engine = QuoteEngine::new();
        engine.apply(
            Bytes::from_static(b"PETR4"),
            101_758,
            diff(&[
                (2, QuoteFieldValue::Float(59.95)),
                (3, QuoteFieldValue::Float(59.93)),
            ]),
        );
        // Delta: only field 2 changes.
        engine.apply(
            Bytes::from_static(b"PETR4"),
            101_800,
            diff(&[(2, QuoteFieldValue::Float(60.00))]),
        );
        engine.with_state(b"PETR4", |s| {
            let state = s.expect("PETR4 should exist");
            assert_eq!(state.version, 2);
            assert_eq!(state.time_hhmmss, 101_800);
            // Updated.
            assert_eq!(state.get(2), Some(&QuoteFieldValue::Float(60.00)));
            // Preserved from the prior snapshot.
            assert_eq!(state.get(3), Some(&QuoteFieldValue::Float(59.93)));
        });
    }

    #[test]
    fn distinct_tickers_are_isolated() {
        let engine = QuoteEngine::new();
        engine.apply(
            Bytes::from_static(b"PETR4"),
            1,
            diff(&[(2, QuoteFieldValue::Float(1.0))]),
        );
        engine.apply(
            Bytes::from_static(b"VALE3"),
            2,
            diff(&[(2, QuoteFieldValue::Float(2.0))]),
        );
        assert_eq!(engine.len(), 2);
        engine.with_state(b"PETR4", |s| {
            assert_eq!(s.unwrap().get(2), Some(&QuoteFieldValue::Float(1.0)));
        });
        engine.with_state(b"VALE3", |s| {
            assert_eq!(s.unwrap().get(2), Some(&QuoteFieldValue::Float(2.0)));
        });
    }

    #[test]
    fn known_tickers_snapshot_is_independent_of_subsequent_writes() {
        let engine = QuoteEngine::new();
        engine.apply(
            Bytes::from_static(b"A"),
            1,
            diff(&[(2, QuoteFieldValue::Float(1.0))]),
        );
        let snap_before = engine.known_tickers();
        engine.apply(
            Bytes::from_static(b"B"),
            2,
            diff(&[(2, QuoteFieldValue::Float(2.0))]),
        );
        assert_eq!(snap_before.len(), 1);
        assert_eq!(engine.len(), 2);
    }

    #[test]
    fn lookup_for_unknown_ticker_returns_none() {
        let engine = QuoteEngine::new();
        engine.with_state(b"NOPE", |s| assert!(s.is_none()));
    }
}
