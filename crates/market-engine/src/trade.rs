//! Working trade buffer.
//!
//! This crate is **not** responsible for durability — the WAL + `TimescaleDB`
//! pipeline lives in the `persistence` crate (task #2). Here we simply hold
//! the last N trades per ticker in memory so the api-server can answer
//! `GET /v1/trades/{ticker}?limit=N` from the Redis snapshot without
//! waking the engine up.
//!
//! Construction: an internal `DashMap<Ticker, VecDeque<TradeRecord>>` with a
//! per-ticker capacity. Inserting past the cap rotates out the oldest.

use ahash::RandomState;
use bytes::Bytes;
use cedro_protocol::enums::{TradeAggressor, TradeCondition};
use cedro_protocol::types::Trade;
use dashmap::DashMap;
use std::collections::VecDeque;

/// Compact owned representation of a trade for in-memory caching. We
/// don't borrow from the parser frame because the buffer outlives any
/// single frame.
#[derive(Debug, Clone, PartialEq)]
pub struct TradeRecord {
    pub time_hhmmss: u32,
    pub price: f64,
    pub quantity: u64,
    pub trade_id: Bytes,
    pub broker_buy_id: u32,
    pub broker_sell_id: u32,
    pub condition: TradeCondition,
    pub aggressor: TradeAggressor,
}

impl From<&Trade> for TradeRecord {
    fn from(t: &Trade) -> Self {
        Self {
            time_hhmmss: t.time_hhmmss,
            price: t.price,
            quantity: t.quantity,
            trade_id: t.trade_id.clone(),
            broker_buy_id: t.broker_buy_id,
            broker_sell_id: t.broker_sell_id,
            condition: t.condition,
            aggressor: t.aggressor,
        }
    }
}

/// Per-ticker bounded queue of recent trades.
#[derive(Debug)]
pub struct TradeBuffer {
    per_ticker_capacity: usize,
    inner: DashMap<Bytes, VecDeque<TradeRecord>, RandomState>,
}

impl TradeBuffer {
    pub fn new(per_ticker_capacity: usize) -> Self {
        Self {
            per_ticker_capacity: per_ticker_capacity.max(1),
            inner: DashMap::with_hasher(RandomState::default()),
        }
    }

    /// Insert a trade into the ticker's queue, rotating the oldest entry out
    /// once the per-ticker cap is hit.
    pub fn push(&self, ticker: Bytes, trade: TradeRecord) {
        let cap = self.per_ticker_capacity;
        let mut entry = self.inner.entry(ticker).or_default();
        if entry.len() == cap {
            entry.pop_front();
        }
        entry.push_back(trade);
        metrics::counter!("market_engine_trades_total").increment(1);
    }

    /// Remove a specific trade id (used for Cedro bust corrections, op `D`).
    /// Returns `true` if the trade was found and removed.
    pub fn remove(&self, ticker: &[u8], trade_id: &[u8]) -> bool {
        if let Some(mut entry) = self.inner.get_mut(ticker) {
            if let Some(pos) = entry.iter().position(|t| t.trade_id.as_ref() == trade_id) {
                entry.remove(pos);
                metrics::counter!("market_engine_trade_removals_total").increment(1);
                return true;
            }
        }
        false
    }

    /// Clear every trade for a ticker (Cedro op `R`).
    pub fn clear(&self, ticker: &[u8]) {
        if let Some(mut entry) = self.inner.get_mut(ticker) {
            let n = entry.len() as u64;
            entry.clear();
            metrics::counter!("market_engine_trade_clears_total").increment(1);
            metrics::counter!("market_engine_trade_removals_total").increment(n);
        }
    }

    /// Snapshot of the last N trades for a ticker, oldest first.
    pub fn snapshot(&self, ticker: &[u8], limit: usize) -> Vec<TradeRecord> {
        let Some(entry) = self.inner.get(ticker) else {
            return Vec::new();
        };
        let take = limit.min(entry.len());
        entry.iter().rev().take(take).rev().cloned().collect()
    }

    pub fn len_for(&self, ticker: &[u8]) -> usize {
        self.inner.get(ticker).map_or(0, |e| e.len())
    }

    pub fn known_tickers(&self) -> usize {
        self.inner.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: &'static [u8], price: f64) -> TradeRecord {
        TradeRecord {
            time_hhmmss: 1,
            price,
            quantity: 1,
            trade_id: Bytes::from_static(id),
            broker_buy_id: 0,
            broker_sell_id: 0,
            condition: TradeCondition::NotDirect,
            aggressor: TradeAggressor::Undefined,
        }
    }

    #[test]
    fn push_and_snapshot_preserve_order() {
        let buf = TradeBuffer::new(10);
        buf.push(Bytes::from_static(b"PETR4"), rec(b"T1", 1.0));
        buf.push(Bytes::from_static(b"PETR4"), rec(b"T2", 2.0));
        buf.push(Bytes::from_static(b"PETR4"), rec(b"T3", 3.0));
        let snap = buf.snapshot(b"PETR4", 10);
        assert_eq!(snap.len(), 3);
        assert_eq!(snap[0].trade_id.as_ref(), b"T1");
        assert_eq!(snap[2].trade_id.as_ref(), b"T3");
    }

    #[test]
    fn cap_evicts_oldest_record() {
        let buf = TradeBuffer::new(3);
        for i in 0..5_u32 {
            let id_str = format!("T{i}");
            let id: &'static [u8] = Box::leak(id_str.into_bytes().into_boxed_slice());
            buf.push(Bytes::from_static(b"PETR4"), rec(id, f64::from(i)));
        }
        let snap = buf.snapshot(b"PETR4", 10);
        // Oldest two evicted; ordering preserved.
        assert_eq!(snap.len(), 3);
        assert_eq!(snap[0].trade_id.as_ref(), b"T2");
        assert_eq!(snap[2].trade_id.as_ref(), b"T4");
    }

    #[test]
    fn snapshot_limit_returns_most_recent_n() {
        let buf = TradeBuffer::new(10);
        for i in 0..6_u32 {
            let id_str = format!("T{i}");
            let id: &'static [u8] = Box::leak(id_str.into_bytes().into_boxed_slice());
            buf.push(Bytes::from_static(b"PETR4"), rec(id, f64::from(i)));
        }
        let snap = buf.snapshot(b"PETR4", 3);
        assert_eq!(snap.len(), 3);
        // Last three: T3, T4, T5
        assert_eq!(snap[0].trade_id.as_ref(), b"T3");
        assert_eq!(snap[2].trade_id.as_ref(), b"T5");
    }

    #[test]
    fn remove_handles_present_and_absent_ids() {
        let buf = TradeBuffer::new(10);
        buf.push(Bytes::from_static(b"PETR4"), rec(b"T1", 1.0));
        buf.push(Bytes::from_static(b"PETR4"), rec(b"T2", 2.0));
        assert!(buf.remove(b"PETR4", b"T1"));
        assert!(!buf.remove(b"PETR4", b"T999"));
        let snap = buf.snapshot(b"PETR4", 10);
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].trade_id.as_ref(), b"T2");
    }

    #[test]
    fn clear_drops_every_trade_for_ticker() {
        let buf = TradeBuffer::new(10);
        buf.push(Bytes::from_static(b"PETR4"), rec(b"T1", 1.0));
        buf.push(Bytes::from_static(b"PETR4"), rec(b"T2", 2.0));
        buf.push(Bytes::from_static(b"VALE3"), rec(b"V1", 10.0));
        buf.clear(b"PETR4");
        assert_eq!(buf.snapshot(b"PETR4", 10).len(), 0);
        assert_eq!(buf.snapshot(b"VALE3", 10).len(), 1);
    }

    #[test]
    fn from_trait_lifts_protocol_trade_into_record() {
        let t = Trade {
            operation_code: b'A',
            time_hhmmss: 12_3000,
            price: 32.5,
            broker_buy_id: 1,
            broker_sell_id: 2,
            quantity: 100,
            trade_id: Bytes::from_static(b"T1"),
            request_id: None,
            condition: TradeCondition::Direct,
            aggressor: TradeAggressor::Buyer,
            original_conditions: Bytes::from_static(b"0"),
        };
        let r: TradeRecord = (&t).into();
        assert_eq!(r.trade_id.as_ref(), b"T1");
        assert_eq!(r.condition, TradeCondition::Direct);
        assert_eq!(r.aggressor, TradeAggressor::Buyer);
    }
}
