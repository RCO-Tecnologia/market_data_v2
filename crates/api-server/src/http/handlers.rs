//! HTTP route definitions.
//!
//! Each handler calls [`crate::auth::require_token`] manually instead of
//! relying on an axum extractor; this keeps the state plumbing flat and the
//! tests inline-testable without spinning up a router.

use super::content_neg::Accept;
use crate::auth::require_token;
use crate::config::ApiConfig;
use crate::error::ApiError;
use crate::snapshot_store::{SnapshotKind, SnapshotStore};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct HttpState {
    pub store: SnapshotStore,
    pub cfg: Arc<ApiConfig>,
}

pub fn build_http_routes(state: HttpState) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/quote/:ticker", get(quote_single))
        .route("/v1/quote", get(quote_batch))
        .route("/v1/book/:ticker", get(book_single))
        .route("/v1/book", get(book_batch))
        .route("/v1/trades/:ticker", get(trades))
        .with_state(state)
}

// ─── /v1/health ───────────────────────────────────────────────────────────

#[derive(Serialize)]
struct HealthBody {
    status: &'static str,
}

async fn health() -> Json<HealthBody> {
    Json(HealthBody { status: "ok" })
}

// ─── /v1/quote/:ticker ────────────────────────────────────────────────────

async fn quote_single(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ticker): Path<String>,
) -> Result<Response, ApiError> {
    require_token(&headers, &state.cfg)?;
    let accept = Accept::from_headers(&headers);
    let bytes = state
        .store
        .get(SnapshotKind::Quote, &ticker)
        .await?
        .ok_or_else(|| ApiError::TickerNotFound(ticker.clone()))?;
    Ok(snapshot_response(accept, &bytes))
}

// ─── /v1/quote?tickers=A,B,C ──────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct BatchQuery {
    tickers: String,
}

async fn quote_batch(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(q): Query<BatchQuery>,
) -> Result<Response, ApiError> {
    require_token(&headers, &state.cfg)?;
    batch(state, SnapshotKind::Quote, q, headers).await
}

// ─── /v1/book/:ticker ─────────────────────────────────────────────────────

async fn book_single(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ticker): Path<String>,
) -> Result<Response, ApiError> {
    require_token(&headers, &state.cfg)?;
    let accept = Accept::from_headers(&headers);
    let bytes = state
        .store
        .get(SnapshotKind::Book, &ticker)
        .await?
        .ok_or(ApiError::BookNotAvailable)?;
    Ok(snapshot_response(accept, &bytes))
}

// ─── /v1/book?tickers=A,B,C ───────────────────────────────────────────────

async fn book_batch(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(q): Query<BatchQuery>,
) -> Result<Response, ApiError> {
    require_token(&headers, &state.cfg)?;
    batch(state, SnapshotKind::Book, q, headers).await
}

// ─── /v1/trades/:ticker ───────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct TradesQuery {
    #[serde(default = "default_limit")]
    limit: u32,
    /// If present, query the TimescaleDB history. NOT implemented at this
    /// layer — we return BadRequest until task #2 lands.
    since: Option<String>,
}

const fn default_limit() -> u32 {
    50
}

async fn trades(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ticker): Path<String>,
    Query(q): Query<TradesQuery>,
) -> Result<Response, ApiError> {
    require_token(&headers, &state.cfg)?;
    if q.since.is_some() {
        return Err(ApiError::BadRequest(
            "historical range queries require the TimescaleDB backend (task #2)",
        ));
    }
    if q.limit == 0 || q.limit > 1000 {
        return Err(ApiError::BadRequest("limit must be in 1..=1000"));
    }
    let accept = Accept::from_headers(&headers);
    let bytes = state
        .store
        .get(SnapshotKind::Trade, &ticker)
        .await?
        .ok_or_else(|| ApiError::TickerNotFound(ticker.clone()))?;
    Ok(snapshot_response(accept, &bytes))
}

// ─── Shared helpers ──────────────────────────────────────────────────────

const MAX_BATCH: usize = 500;

async fn batch(
    state: HttpState,
    kind: SnapshotKind,
    q: BatchQuery,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let accept = Accept::from_headers(&headers);
    let tickers: Vec<&str> = q
        .tickers
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if tickers.is_empty() {
        return Err(ApiError::BadRequest("tickers list is empty"));
    }
    if tickers.len() > MAX_BATCH {
        return Err(ApiError::BadRequest("batch exceeds 500 tickers"));
    }

    let results = state.store.get_batch(kind, &tickers).await?;
    if matches!(accept, Accept::Json) {
        let mut out = serde_json::Map::with_capacity(tickers.len());
        for (ticker, payload) in tickers.iter().zip(results.iter()) {
            match payload {
                None => {
                    out.insert((*ticker).to_string(), serde_json::Value::Null);
                }
                Some(b) => {
                    let parsed: serde_json::Value = serde_json::from_slice(b)
                        .unwrap_or_else(|_| serde_json::json!({ "raw_hex": hex(b) }));
                    out.insert((*ticker).to_string(), parsed);
                }
            }
        }
        Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, accept.header_value())],
            serde_json::to_vec(&out).map_err(|_| ApiError::Internal("json"))?,
        )
            .into_response())
    } else {
        let mut map: std::collections::BTreeMap<&str, Option<&[u8]>> =
            std::collections::BTreeMap::new();
        for (ticker, payload) in tickers.iter().zip(results.iter()) {
            map.insert(*ticker, payload.as_deref());
        }
        let body = rmp_serde::to_vec_named(&map).map_err(|_| ApiError::Internal("msgpack"))?;
        Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, accept.header_value())],
            body,
        )
            .into_response())
    }
}

fn snapshot_response(accept: Accept, payload: &[u8]) -> Response {
    let etag = compute_etag(payload);
    let cache = HeaderValue::from_static("max-age=1");
    let etag_hv =
        HeaderValue::from_str(&etag).unwrap_or_else(|_| HeaderValue::from_static("\"0\""));
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, accept.header_value());
    headers.insert(header::CACHE_CONTROL, cache);
    headers.insert(header::ETAG, etag_hv);
    metrics::counter!("api_http_requests_total", "endpoint" => "snapshot_single").increment(1);
    (StatusCode::OK, headers, payload.to_vec()).into_response()
}

fn compute_etag(payload: &[u8]) -> String {
    // `BuildHasher::hash_one` is the idiomatic call here, but it requires
    // the trait to be in scope; we import it via the full path to avoid an
    // unused-import warning from clippy when the trait method is invoked
    // through inherent name resolution.
    let builder = ahash::RandomState::with_seeds(0x5af3, 0x01_07a4, 0xdead_beef, 0xcafe);
    let hash = <ahash::RandomState as std::hash::BuildHasher>::hash_one(&builder, payload);
    format!("\"{hash:x}\"")
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn etag_is_stable_for_identical_payload() {
        assert_eq!(compute_etag(b"hello"), compute_etag(b"hello"));
        assert_ne!(compute_etag(b"hello"), compute_etag(b"world"));
    }

    #[test]
    fn hex_round_trip_matches_expected_bytes() {
        assert_eq!(hex(&[0x00, 0xff, 0xab]), "00ffab");
    }
}
