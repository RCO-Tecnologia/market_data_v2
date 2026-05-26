//! API server binary — assembles the `api-server` library into a process
//! that Coolify (or any Docker host) can run.
//!
//! Reads its config from environment variables. Health endpoint is built
//! into the router (`/v1/health`) so Docker healthchecks just point there.

#![allow(clippy::print_stdout)]

use ahash::AHashSet;
use anyhow::Context;
use api_server::{ApiConfig, build_router};
use observability::ObservabilityConfig;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let bind: SocketAddr = env::var("API_BIND")
        .unwrap_or_else(|_| "0.0.0.0:8080".into())
        .parse()
        .context("API_BIND must be host:port")?;

    let _prom = observability::init_global(&ObservabilityConfig {
        default_filter: env::var("LOG_FILTER").unwrap_or_else(|_| "info".into()),
        json: env::var("LOG_JSON").map_or(true, |v| v != "0"),
        prometheus_bind: Some(
            env::var("API_METRICS_BIND").unwrap_or_else(|_| "0.0.0.0:9101".into()),
        ),
    })
    .context("initialising observability")?;

    let cfg = Arc::new(ApiConfig {
        redis_url: env::var("REDIS_URL").context("REDIS_URL missing")?,
        nats_url: env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into()),
        valid_tokens: parse_tokens(),
        book_universe: None, // populated by ingest-daemon's Redis writes
        rate_limit_per_second: env::var("API_RATE_LIMIT")
            .ok()
            .and_then(|s| s.parse().ok()),
        etag_cache_ttl: Duration::from_secs(5),
        cors_allowed_origins: parse_cors_origins(),
    });

    let router = build_router(Arc::clone(&cfg)).context("building router")?;

    info!(%bind, "api-server listening");
    let listener = tokio::net::TcpListener::bind(bind).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("axum serve")?;

    Ok(())
}

fn parse_tokens() -> AHashSet<String> {
    env::var("API_TOKENS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

fn parse_cors_origins() -> Vec<String> {
    env::var("API_CORS_ORIGINS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect()
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown signal received");
}
