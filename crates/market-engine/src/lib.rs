//! In-memory state engines for Cedro market data.
//!
//! Three engines, each tuned to its data's semantics — see `ARCHITECTURE.md §5.5`:
//!
//! - [`QuoteEngine`]: lock-free `DashMap<Ticker, QuoteState>`; every parser
//!   worker writes directly. Most recent update wins.
//! - [`BookEngine`]: **single-writer per ticker** via hash sharding; processes
//!   `BookOp`/`AggBookOp` sequentially per ticker so the book never observes
//!   out-of-order operations.
//! - [`TradeBuffer`]: append-only in-memory ring with a hook to flush into
//!   the durable trade pipeline (WAL → `TimescaleDB`) provided by another
//!   crate. This crate only holds the working buffer.

#![cfg_attr(not(test), warn(clippy::print_stdout, clippy::print_stderr))]

pub mod book;
pub mod quote;
pub mod shard;
pub mod trade;

pub use book::{BookEngine, OrderBook, OrderBookStats};
pub use quote::{QuoteEngine, QuoteState};
pub use shard::shard_for;
pub use trade::{TradeBuffer, TradeRecord};
