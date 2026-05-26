//! Composition root: HTTP routes + future WS handler bolted into one Axum router.

use crate::ApiConfig;
use crate::http::build_http_routes;
use crate::snapshot_store::SnapshotStore;
use axum::Router;
use std::sync::Arc;
use tower_http::cors::CorsLayer;

/// Builds the full router but does NOT start serving. Composition happens
/// in the binary, which then wraps this in a `tokio::net::TcpListener`.
pub fn build_router(cfg: Arc<ApiConfig>) -> Result<Router, crate::ApiError> {
    let store = SnapshotStore::connect(&cfg.redis_url)?;
    let state = crate::http::HttpState {
        store,
        cfg: Arc::clone(&cfg),
    };
    let router = build_http_routes(state).layer(cors_layer(&cfg));
    Ok(router)
}

fn cors_layer(cfg: &ApiConfig) -> CorsLayer {
    if cfg.cors_allowed_origins.is_empty() {
        CorsLayer::very_permissive()
    } else {
        let origins: Vec<_> = cfg
            .cors_allowed_origins
            .iter()
            .filter_map(|o| o.parse().ok())
            .collect();
        CorsLayer::new().allow_origin(origins)
    }
}
