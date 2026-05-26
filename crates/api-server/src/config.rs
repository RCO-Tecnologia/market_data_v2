//! Configuration consumed by the router builder.

use ahash::AHashSet;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ApiConfig {
    /// Redis URL used for snapshot reads.
    pub redis_url: String,
    /// NATS URL used for streaming subscriptions on `/ws`.
    pub nats_url: String,
    /// Bearer tokens accepted by the auth layer. Populated from a secret
    /// store or env var in production.
    pub valid_tokens: AHashSet<String>,
    /// Tickers known to have a book in the engine (ações + opções Bovespa
    /// per `bqt_scope` memory). `None` accepts every ticker.
    pub book_universe: Option<AHashSet<String>>,
    /// Per-token request budget (HTTP only). `None` disables rate limiting.
    pub rate_limit_per_second: Option<u32>,
    /// Background ETag cache TTL — keeps recently-computed hashes around.
    pub etag_cache_ttl: Duration,
    /// CORS allowed origins. Empty = permissive (use only in dev).
    pub cors_allowed_origins: Vec<String>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            redis_url: "redis://127.0.0.1/".into(),
            nats_url: "nats://127.0.0.1:4222".into(),
            valid_tokens: AHashSet::default(),
            book_universe: None,
            rate_limit_per_second: Some(100),
            etag_cache_ttl: Duration::from_secs(5),
            cors_allowed_origins: Vec::new(),
        }
    }
}
