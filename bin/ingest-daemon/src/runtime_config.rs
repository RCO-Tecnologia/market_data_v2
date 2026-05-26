#![allow(dead_code)]

//! Runtime configuration — pulled from env vars.
//!
//! Production deployments use a config-management layer (systemd
//! EnvironmentFile, k8s ConfigMap, etc) so we resist YAMLising secrets
//! into the repo. Every option has a sensible default that matches the
//! production target (`ARCHITECTURE.md §11.5`).

use std::env;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    // ── Observability ────────────────────────────────────────────────
    pub log_filter: String,
    pub log_json: bool,
    pub metrics_bind: String,

    // ── Cedro Crystal ────────────────────────────────────────────────
    pub cedro_host: String,
    pub cedro_port: u16,
    pub cedro_software_key: String,
    pub cedro_username: String,
    pub cedro_password: String,
    pub so_rcvbuf: usize,
    pub frame_channel_capacity: usize,
    pub reader_cpu: Option<usize>,
    pub watchdog_cpu: Option<usize>,

    // ── Engines ─────────────────────────────────────────────────────
    pub n_parser_workers: usize,
    pub n_book_shards: usize,
    pub trade_buffer_per_ticker: usize,

    // ── Fanout (NATS + Redis) ───────────────────────────────────────
    pub nats_url: String,
    pub redis_url: String,
    pub snapshot_ttl_seconds: Option<u64>,

    // ── Raw archive ─────────────────────────────────────────────────
    pub raw_archive_dir: PathBuf,
    pub raw_archive_zstd_level: i32,

    // ── Persistence (Timescale + WAL) ───────────────────────────────
    pub database_url: String,
    pub db_pool_size: u32,
    pub wal_path: PathBuf,
    pub trade_batch_size: usize,
    pub trade_flush_interval_ms: u64,
}

impl RuntimeConfig {
    pub fn from_env() -> Result<Self, &'static str> {
        Ok(Self {
            log_filter: env::var("LOG_FILTER").unwrap_or_else(|_| "info".into()),
            log_json: env::var("LOG_JSON").map_or(true, |v| v != "0"),
            metrics_bind: env::var("METRICS_BIND").unwrap_or_else(|_| "0.0.0.0:9100".into()),

            cedro_host: env::var("CEDRO_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
            cedro_port: env_u16("CEDRO_PORT", 81)?,
            cedro_software_key: env::var("CEDRO_SOFTWARE_KEY").unwrap_or_default(),
            cedro_username: env::var("CEDRO_USERNAME").map_err(|_| "CEDRO_USERNAME missing")?,
            cedro_password: env::var("CEDRO_PASSWORD").map_err(|_| "CEDRO_PASSWORD missing")?,
            so_rcvbuf: env_usize("SO_RCVBUF", 128 * 1024 * 1024)?,
            frame_channel_capacity: env_usize("FRAME_CHANNEL_CAPACITY", 2 * 1024 * 1024)?,
            reader_cpu: env_opt_usize("READER_CPU")?,
            watchdog_cpu: env_opt_usize("WATCHDOG_CPU")?,

            n_parser_workers: env_usize("PARSER_WORKERS", 8)?,
            n_book_shards: env_usize("BOOK_SHARDS", 8)?,
            trade_buffer_per_ticker: env_usize("TRADE_BUFFER_PER_TICKER", 100)?,

            nats_url: env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into()),
            redis_url: env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".into()),
            snapshot_ttl_seconds: env_opt_u64("SNAPSHOT_TTL_SECONDS")?,

            raw_archive_dir: env::var("RAW_ARCHIVE_DIR")
                .unwrap_or_else(|_| "/var/data/raw".into())
                .into(),
            raw_archive_zstd_level: env_i32("RAW_ZSTD_LEVEL", 3)?,

            database_url: env::var("DATABASE_URL").map_err(|_| "DATABASE_URL missing")?,
            db_pool_size: env_u32("DB_POOL_SIZE", 4)?,
            wal_path: env::var("WAL_PATH")
                .unwrap_or_else(|_| "/var/data/trades.wal".into())
                .into(),
            trade_batch_size: env_usize("TRADE_BATCH_SIZE", 5000)?,
            trade_flush_interval_ms: env_u64("TRADE_FLUSH_MS", 100)?,
        })
    }
}

fn env_u16(key: &str, default: u16) -> Result<u16, &'static str> {
    match env::var(key) {
        Ok(v) => v.parse().map_err(|_| static_str(key)),
        Err(_) => Ok(default),
    }
}

fn env_u32(key: &str, default: u32) -> Result<u32, &'static str> {
    match env::var(key) {
        Ok(v) => v.parse().map_err(|_| static_str(key)),
        Err(_) => Ok(default),
    }
}

fn env_u64(key: &str, default: u64) -> Result<u64, &'static str> {
    match env::var(key) {
        Ok(v) => v.parse().map_err(|_| static_str(key)),
        Err(_) => Ok(default),
    }
}

fn env_i32(key: &str, default: i32) -> Result<i32, &'static str> {
    match env::var(key) {
        Ok(v) => v.parse().map_err(|_| static_str(key)),
        Err(_) => Ok(default),
    }
}

fn env_usize(key: &str, default: usize) -> Result<usize, &'static str> {
    match env::var(key) {
        Ok(v) => v.parse().map_err(|_| static_str(key)),
        Err(_) => Ok(default),
    }
}

fn env_opt_usize(key: &str) -> Result<Option<usize>, &'static str> {
    match env::var(key) {
        Ok(v) => v.parse().map(Some).map_err(|_| static_str(key)),
        Err(_) => Ok(None),
    }
}

fn env_opt_u64(key: &str) -> Result<Option<u64>, &'static str> {
    match env::var(key) {
        Ok(v) => v.parse().map(Some).map_err(|_| static_str(key)),
        Err(_) => Ok(None),
    }
}

const fn static_str(_key: &str) -> &'static str {
    "invalid env var value"
}
