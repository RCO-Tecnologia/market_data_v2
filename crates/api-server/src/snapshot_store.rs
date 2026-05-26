//! Redis-backed snapshot reader used by HTTP and WS surfaces.
//!
//! The ingest daemon writes `market:{quote|book|trade}:{ticker}` via the
//! `fanout` crate. This module is the *read* counterpart — both `/v1/...`
//! endpoints and the WS snapshot phase hit it for cached state.

use crate::error::ApiError;
use bytes::Bytes;
use redis::AsyncCommands;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotKind {
    Quote,
    Book,
    Trade,
}

impl SnapshotKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Quote => "quote",
            Self::Book => "book",
            Self::Trade => "trade",
        }
    }
}

/// Thin façade over a Redis connection pool.
#[derive(Debug, Clone)]
pub struct SnapshotStore {
    client: redis::Client,
}

impl SnapshotStore {
    pub fn connect(redis_url: &str) -> Result<Self, ApiError> {
        let client = redis::Client::open(redis_url)?;
        Ok(Self { client })
    }

    /// Convenience to build a key in the same format used by `fanout`.
    pub fn key(kind: SnapshotKind, ticker: &str) -> String {
        format!("market:{}:{}", kind.as_str(), ticker)
    }

    /// Fetch one snapshot. Returns `None` if the ticker isn't in Redis yet.
    pub async fn get(
        &self,
        kind: SnapshotKind,
        ticker: &str,
    ) -> Result<Option<Bytes>, ApiError> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = Self::key(kind, ticker);
        let bytes: Option<Vec<u8>> = conn.get(&key).await?;
        Ok(bytes.map(Bytes::from))
    }

    /// Fetch a batch of snapshots in one round-trip. Returned vector mirrors
    /// the order of the input slice; entries are `None` for unknown tickers.
    pub async fn get_batch(
        &self,
        kind: SnapshotKind,
        tickers: &[&str],
    ) -> Result<Vec<Option<Bytes>>, ApiError> {
        if tickers.is_empty() {
            return Ok(Vec::new());
        }
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = tickers.iter().map(|t| Self::key(kind, t)).collect();
        let values: Vec<Option<Vec<u8>>> = conn.mget(&keys).await?;
        Ok(values.into_iter().map(|v| v.map(Bytes::from)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_uses_fanout_format() {
        assert_eq!(SnapshotStore::key(SnapshotKind::Quote, "PETR4"), "market:quote:PETR4");
        assert_eq!(SnapshotStore::key(SnapshotKind::Book, "PETR4"), "market:book:PETR4");
        assert_eq!(SnapshotStore::key(SnapshotKind::Trade, "PETR4"), "market:trade:PETR4");
    }

    #[test]
    fn kind_strings_match_fanout_crate() {
        assert_eq!(SnapshotKind::Quote.as_str(), "quote");
        assert_eq!(SnapshotKind::Book.as_str(), "book");
        assert_eq!(SnapshotKind::Trade.as_str(), "trade");
    }
}
