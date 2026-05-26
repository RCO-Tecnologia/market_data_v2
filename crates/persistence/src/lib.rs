//! Trade persistence — WAL + batch INSERT into the existing TimescaleDB
//! `trades` hypertable.
//!
//! Schema we integrate with (no DDL is owned by this crate; the table is
//! managed externally):
//!
//! ```text
//! Table "public.trades"
//!  Column            | Type          | Nullable | Default | Notes
//!  ──────────────────┼───────────────┼──────────┼─────────┼──────────────────
//!  time              | timestamptz   | NOT NULL |         | PK component
//!  asset_id          | int4          | NOT NULL |         | FK public.assets(id)
//!  price             | numeric(18,4) | NOT NULL |         |
//!  amount            | int8          | NOT NULL |         |
//!  buyer_id          | int4          | NULL     |         | broker cedro_id
//!  seller_id         | int4          | NULL     |         | broker cedro_id
//!  aggressor_side    | int2          | NULL     |         | NULL=indef, 1=buy, 2=sell
//!  trade_id          | text          | NULL     |         | PK component
//!  is_direct         | bool          | NULL     | false   |
//!  financial_volume  | numeric(18,2) | NULL     |         | = price * amount
//!
//! Indices:
//!   trades_time_idx              BTREE (time DESC)
//!   trades_time_asset_trade_unique UNIQUE (time, asset_id, trade_id)
//! ```
//!
//! Module map:
//!
//! - [`asset_cache`] resolves `ticker → asset_id` via `public.assets`, so we
//!   only pay the lookup cost once on boot.
//! - [`wal`] is an append-only file ensuring perda zero: every trade lands
//!   on disk before being released into the in-memory flush buffer.
//! - [`trade_writer`] owns the buffer + flush loop and is the only crate
//!   that talks to sqlx.

#![cfg_attr(not(test), warn(clippy::print_stdout, clippy::print_stderr))]

pub mod asset_cache;
pub mod config;
pub mod error;
pub mod trade_writer;
pub mod wal;

pub use asset_cache::AssetCache;
pub use config::PersistenceConfig;
pub use error::PersistenceError;
pub use trade_writer::{StagedTrade, TradeWriter};
pub use wal::TradeWal;
