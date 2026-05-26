//! Errors raised by the persistence layer.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("WAL I/O error: {0}")]
    Wal(#[from] std::io::Error),
    #[error("unknown ticker {0:?} — not present in public.assets")]
    UnknownTicker(String),
    #[error("invalid trade data: {0}")]
    InvalidTrade(&'static str),
    #[error("price/amount conversion failed: {0}")]
    Decimal(#[from] rust_decimal::Error),
    #[error("writer is shutting down")]
    ShuttingDown,
}
