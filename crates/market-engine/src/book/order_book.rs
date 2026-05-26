//! Per-ticker order book.
//!
//! Holds two side-specific [`indexmap::IndexMap`]s keyed by Cedro's `OrderID`
//! when present, with positional fall-back for the legacy 7-field BQT layout.
//! `IndexMap` preserves insertion order (which mirrors Cedro's positional
//! semantics) while still giving us O(1) lookup by id.
//!
//! All mutations go through methods on this struct — there is intentionally
//! no `&mut Vec<…>` exposed. The engine layer guarantees single-writer per
//! ticker, so we don't pay for any internal locking.

use ahash::AHashMap;
use bytes::Bytes;
use cedro_protocol::enums::Side;
use cedro_protocol::types::{BookDelete, BookEntry, BookOp};

/// Lightweight statistics struct, surfaced for metrics + tests without
/// exposing the internal containers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrderBookStats {
    pub bids: usize,
    pub asks: usize,
}

/// A complete book for one ticker.
#[derive(Debug, Default)]
pub struct OrderBook {
    /// Bid side — ordered by Cedro position (index 0 = best bid).
    bids: Vec<BookEntry>,
    /// Ask side — ordered by Cedro position (index 0 = best ask).
    asks: Vec<BookEntry>,
    /// Lookup from `OrderID` to `(side, current index in the vec above)`.
    /// Built only for entries that carry an order id.
    by_order_id: AHashMap<Bytes, (Side, usize)>,
    /// `true` once the `E` (end-of-initial) marker arrives.
    initialised: bool,
}

impl OrderBook {
    /// Apply a single op to the book. Returns `true` if the book is now in
    /// a clean, observable state — i.e. the initial snapshot has finished
    /// or this op is itself a post-snapshot delta. The orchestrator can use
    /// the return value to decide whether to flush snapshots to Redis.
    pub fn apply(&mut self, op: BookOp) -> bool {
        match op {
            BookOp::Add(entry) => self.add(entry),
            BookOp::Update { old_pos, entry } => self.update(old_pos, entry),
            BookOp::Delete(d) => self.delete(d),
            BookOp::EndOfInitial => {
                self.initialised = true;
            }
        }
        self.initialised
    }

    fn add(&mut self, entry: BookEntry) {
        let side = entry.side;
        let pos = entry.position as usize;
        let vec = self.vec_mut(side);
        if pos >= vec.len() {
            vec.push(entry.clone());
        } else {
            vec.insert(pos, entry.clone());
            // Every entry that previously sat at or after `pos` is now one
            // slot further down. Shifting from `pos` (inclusive) keeps the
            // by_order_id index aligned with the vector.
            self.shift_indices_after(side, pos, 1);
        }
        if let Some(id) = entry.order_id {
            let idx = pos.min(self.vec_mut(side).len().saturating_sub(1));
            self.by_order_id.insert(id, (side, idx));
        }
    }

    fn update(&mut self, old_pos: u32, entry: BookEntry) {
        let side = entry.side;
        let new_pos = entry.position as usize;
        let old_pos = old_pos as usize;

        let vec = self.vec_mut(side);
        if old_pos >= vec.len() {
            // Out-of-range update — treat as add at new position. This
            // happens when the server emits an update that arrived before
            // the corresponding add (rare; defensive fallback).
            let _ = vec; // release the borrow before the recursive add
            self.add(entry);
            return;
        }
        let removed = vec.remove(old_pos);
        self.shift_indices_after(side, old_pos, -1);
        if let Some(id) = removed.order_id {
            self.by_order_id.remove(&id);
        }

        let vec = self.vec_mut(side);
        if new_pos >= vec.len() {
            vec.push(entry.clone());
        } else {
            vec.insert(new_pos, entry.clone());
            self.shift_indices_after(side, new_pos, 1);
        }
        if let Some(id) = entry.order_id {
            let final_pos = new_pos.min(self.vec_mut(side).len().saturating_sub(1));
            self.by_order_id.insert(id, (side, final_pos));
        }
    }

    fn delete(&mut self, d: BookDelete) {
        match d {
            BookDelete::Single { side, position } => {
                let vec = self.vec_mut(side);
                let pos = position as usize;
                if pos < vec.len() {
                    let removed = vec.remove(pos);
                    self.shift_indices_after(side, pos, -1);
                    if let Some(id) = removed.order_id {
                        self.by_order_id.remove(&id);
                    }
                }
            }
            BookDelete::PrefixInclusive { side, position } => {
                let pos = position as usize;
                let vec = self.vec_mut(side);
                let end = (pos + 1).min(vec.len());
                let drained: Vec<_> = vec.drain(..end).collect();
                for e in &drained {
                    if let Some(id) = &e.order_id {
                        self.by_order_id.remove(id);
                    }
                }
                self.shift_indices_after(side, 0, -(end as isize));
            }
            BookDelete::ClearAll => {
                self.bids.clear();
                self.asks.clear();
                self.by_order_id.clear();
            }
        }
    }

    const fn vec_mut(&mut self, side: Side) -> &mut Vec<BookEntry> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn shift_indices_after(&mut self, side: Side, from: usize, by: isize) {
        for (s, i) in self.by_order_id.values_mut() {
            if *s == side && *i >= from {
                let new = (*i as isize) + by;
                if new >= 0 {
                    *i = new as usize;
                }
            }
        }
    }

    pub const fn stats(&self) -> OrderBookStats {
        OrderBookStats {
            bids: self.bids.len(),
            asks: self.asks.len(),
        }
    }

    pub fn best_bid(&self) -> Option<&BookEntry> {
        self.bids.first()
    }

    pub fn best_ask(&self) -> Option<&BookEntry> {
        self.asks.first()
    }

    pub fn bids(&self) -> &[BookEntry] {
        &self.bids
    }

    pub fn asks(&self) -> &[BookEntry] {
        &self.asks
    }

    pub const fn is_initialised(&self) -> bool {
        self.initialised
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn entry(side: Side, pos: u32, price: f64, qty: u64, broker: u32) -> BookEntry {
        BookEntry {
            position: pos,
            side,
            price,
            quantity: qty,
            broker_id: broker,
            timestamp_ddmmhhmm: 11_041_005,
            order_id: None,
            offer_type: None,
        }
    }

    fn entry_with_id(side: Side, pos: u32, id: &'static [u8]) -> BookEntry {
        BookEntry {
            position: pos,
            side,
            price: 100.0,
            quantity: 1,
            broker_id: 1,
            timestamp_ddmmhhmm: 0,
            order_id: Some(Bytes::from_static(id)),
            offer_type: Some(b'L'),
        }
    }

    #[test]
    fn add_appends_when_position_at_end() {
        let mut b = OrderBook::default();
        b.apply(BookOp::Add(entry(Side::Buy, 0, 99.99, 100, 131)));
        b.apply(BookOp::Add(entry(Side::Buy, 1, 99.98, 200, 132)));
        let stats = b.stats();
        assert_eq!(stats.bids, 2);
        assert_eq!(stats.asks, 0);
        assert!((b.best_bid().unwrap().price - 99.99).abs() < 1e-9);
    }

    #[test]
    fn add_at_middle_shifts_later_entries() {
        let mut b = OrderBook::default();
        b.apply(BookOp::Add(entry(Side::Buy, 0, 100.0, 1, 1)));
        b.apply(BookOp::Add(entry(Side::Buy, 1, 99.0, 1, 2)));
        // Insert a new entry at position 1, bumping the old one to 2.
        b.apply(BookOp::Add(entry(Side::Buy, 1, 99.5, 1, 3)));
        assert_eq!(b.stats().bids, 3);
        assert!((b.bids()[0].price - 100.0).abs() < 1e-9);
        assert!((b.bids()[1].price - 99.5).abs() < 1e-9);
        assert!((b.bids()[2].price - 99.0).abs() < 1e-9);
    }

    #[test]
    fn update_moves_entry_between_positions() {
        let mut b = OrderBook::default();
        b.apply(BookOp::Add(entry(Side::Buy, 0, 100.0, 1, 1)));
        b.apply(BookOp::Add(entry(Side::Buy, 1, 99.0, 1, 2)));

        // Move position 0 -> 1, repricing.
        let mut updated = entry(Side::Buy, 1, 98.0, 1, 1);
        updated.position = 1;
        b.apply(BookOp::Update {
            old_pos: 0,
            entry: updated,
        });

        assert_eq!(b.stats().bids, 2);
        assert!((b.bids()[1].price - 98.0).abs() < 1e-9);
    }

    #[test]
    fn delete_single_removes_one_entry() {
        let mut b = OrderBook::default();
        b.apply(BookOp::Add(entry(Side::Sell, 0, 100.0, 1, 1)));
        b.apply(BookOp::Add(entry(Side::Sell, 1, 101.0, 1, 2)));
        b.apply(BookOp::Delete(BookDelete::Single {
            side: Side::Sell,
            position: 0,
        }));
        assert_eq!(b.stats().asks, 1);
        assert!((b.best_ask().unwrap().price - 101.0).abs() < 1e-9);
    }

    #[test]
    fn delete_prefix_inclusive_clears_through_position() {
        let mut b = OrderBook::default();
        for i in 0..5 {
            b.apply(BookOp::Add(entry(Side::Buy, i, 100.0 - f64::from(i), 1, 1)));
        }
        // Delete positions 0..=2 (3 entries).
        b.apply(BookOp::Delete(BookDelete::PrefixInclusive {
            side: Side::Buy,
            position: 2,
        }));
        assert_eq!(b.stats().bids, 2);
    }

    #[test]
    fn delete_kind_three_clears_everything() {
        let mut b = OrderBook::default();
        b.apply(BookOp::Add(entry(Side::Buy, 0, 100.0, 1, 1)));
        b.apply(BookOp::Add(entry(Side::Sell, 0, 101.0, 1, 1)));
        b.apply(BookOp::Add(entry_with_id(Side::Buy, 1, b"ORD1")));
        b.apply(BookOp::Delete(BookDelete::ClearAll));
        assert_eq!(b.stats(), OrderBookStats::default());
        // Order-id index must be cleared too.
        assert!(b.by_order_id.is_empty());
    }

    #[test]
    fn end_of_initial_marks_book_as_observable() {
        let mut b = OrderBook::default();
        assert!(!b.is_initialised());
        b.apply(BookOp::EndOfInitial);
        assert!(b.is_initialised());
    }

    #[test]
    fn order_id_index_tracks_position_after_inserts_and_deletes() {
        let mut b = OrderBook::default();
        b.apply(BookOp::Add(entry_with_id(Side::Buy, 0, b"O1")));
        b.apply(BookOp::Add(entry_with_id(Side::Buy, 1, b"O2")));
        b.apply(BookOp::Add(entry_with_id(Side::Buy, 0, b"O3")));
        // O1 and O2 should have been shifted to indices 1 and 2.
        assert_eq!(b.by_order_id.get(&Bytes::from_static(b"O1")).unwrap().1, 1);
        assert_eq!(b.by_order_id.get(&Bytes::from_static(b"O2")).unwrap().1, 2);

        b.apply(BookOp::Delete(BookDelete::Single {
            side: Side::Buy,
            position: 0,
        }));
        // O3 was at 0 → removed. O1 was at 1 → now at 0. O2 was at 2 → now at 1.
        assert!(b.by_order_id.get(&Bytes::from_static(b"O3")).is_none());
        assert_eq!(b.by_order_id.get(&Bytes::from_static(b"O1")).unwrap().1, 0);
        assert_eq!(b.by_order_id.get(&Bytes::from_static(b"O2")).unwrap().1, 1);
    }
}
