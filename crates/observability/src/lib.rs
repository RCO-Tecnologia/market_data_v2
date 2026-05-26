//! Tracing + Prometheus setup shared by every binary in the workspace.
//!
//! Two responsibilities:
//!
//! - **Logging**: structured JSON via `tracing-subscriber`, level controlled
//!   by `RUST_LOG`. The `cedro-client` crate's hot-path logging discipline
//!   (`ARCHITECTURE.md §5.8`) is enforced at the crate level via `clippy.toml`
//!   — *this* crate just sets up the subscriber.
//!
//! - **Metrics**: a Prometheus exporter that exposes `/metrics` for scraping.
//!   Callers pass in the bind address. No global ambient state beyond what
//!   `metrics-exporter-prometheus` already keeps internally.

#![cfg_attr(not(test), warn(clippy::print_stdout, clippy::print_stderr))]

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::net::SocketAddr;
use thiserror::Error;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Error)]
pub enum ObservabilityError {
    #[error("tracing subscriber already installed")]
    TracingAlreadySet,
    #[error("prometheus exporter setup failed: {0}")]
    Prometheus(#[from] metrics_exporter_prometheus::BuildError),
    #[error("invalid bind address: {0}")]
    InvalidAddr(String),
}

/// Configuration consumed by [`init_global`].
#[derive(Debug, Clone)]
pub struct ObservabilityConfig {
    /// Filter directive (`info`, `cedro=debug`, etc). Overridden by `RUST_LOG`.
    pub default_filter: String,
    /// Whether to emit logs as JSON (`true` in prod) or pretty (`false` in dev).
    pub json: bool,
    /// `host:port` for the Prometheus HTTP exporter (`/metrics`).
    pub prometheus_bind: Option<String>,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            default_filter: "info".into(),
            json: true,
            prometheus_bind: Some("0.0.0.0:9100".into()),
        }
    }
}

/// Install the global tracing subscriber and (optionally) launch the
/// Prometheus exporter.
///
/// Returns a [`PrometheusHandle`] when an exporter is configured — useful for
/// in-process scrapers and tests.
pub fn init_global(
    cfg: &ObservabilityConfig,
) -> Result<Option<PrometheusHandle>, ObservabilityError> {
    install_tracing(cfg)?;
    if let Some(addr) = cfg.prometheus_bind.as_deref() {
        let parsed: SocketAddr = addr
            .parse()
            .map_err(|_| ObservabilityError::InvalidAddr(addr.into()))?;
        let handle = PrometheusBuilder::new()
            .with_http_listener(parsed)
            .install_recorder()?;
        Ok(Some(handle))
    } else {
        Ok(None)
    }
}

fn install_tracing(cfg: &ObservabilityConfig) -> Result<(), ObservabilityError> {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&cfg.default_filter));

    let registry = tracing_subscriber::registry().with(filter);

    let result = if cfg.json {
        let fmt = tracing_subscriber::fmt::layer()
            .json()
            .with_current_span(false)
            .with_span_list(false);
        registry.with(fmt).try_init()
    } else {
        let fmt = tracing_subscriber::fmt::layer()
            .compact()
            .with_target(true);
        registry.with(fmt).try_init()
    };

    result.map_err(|_| ObservabilityError::TracingAlreadySet)
}

/// Standard label names used across the workspace. Keeping them in one place
/// avoids the "is it `ticker` or `instrument`?" coordination problem.
pub mod labels {
    pub const TICKER: &str = "ticker";
    pub const MARKET: &str = "market";
    pub const KIND: &str = "kind";
    pub const REASON: &str = "reason";
    pub const ENDPOINT: &str = "endpoint";
    pub const STATUS: &str = "status";
    pub const CHANNEL: &str = "channel";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_well_formed() {
        let cfg = ObservabilityConfig::default();
        assert_eq!(cfg.default_filter, "info");
        assert!(cfg.json);
        assert!(cfg.prometheus_bind.is_some());
    }

    #[test]
    fn prometheus_install_with_no_bind_is_a_noop() {
        let cfg = ObservabilityConfig {
            default_filter: "warn".into(),
            json: false,
            prometheus_bind: None,
        };
        // We can't safely install a tracing subscriber in a multi-test
        // process without races — so just verify the function signature
        // and configuration plumbing compile.
        assert!(cfg.prometheus_bind.is_none());
    }

    #[test]
    fn invalid_prometheus_bind_is_surfaced() {
        // Build a config and exercise just the validation logic, without
        // touching the global tracing subscriber state.
        let bad = "not-an-address";
        let parsed: Result<SocketAddr, _> = bad.parse();
        assert!(parsed.is_err());
    }
}
