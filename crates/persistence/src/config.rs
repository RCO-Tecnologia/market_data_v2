//! Persistence layer configuration.

use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    /// Postgres URL pointing at the TimescaleDB instance that owns
    /// `public.trades` and `public.assets`.
    pub database_url: String,
    /// Connection pool size. The writer never holds more than one
    /// connection at a time, but a tiny pool lets reads happen concurrently.
    pub pool_size: u32,
    /// Path to the append-only WAL file. Created if missing.
    pub wal_path: PathBuf,
    /// Maximum number of trades buffered in memory before forcing a flush.
    pub batch_size: usize,
    /// Maximum time a trade waits in the buffer before being flushed even
    /// if `batch_size` hasn't been reached.
    pub flush_interval: Duration,
    /// How often the asset_id cache is refreshed from the database. The
    /// universe changes slowly (corporate actions, IPOs) so a 10-minute
    /// default is generous.
    pub asset_refresh_interval: Duration,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            database_url: "postgres://postgres:postgres@127.0.0.1/market_data".into(),
            pool_size: 4,
            wal_path: PathBuf::from("/var/data/trades.wal"),
            batch_size: 5_000,
            flush_interval: Duration::from_millis(100),
            asset_refresh_interval: Duration::from_secs(600),
        }
    }
}
