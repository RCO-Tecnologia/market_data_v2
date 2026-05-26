//! Bearer-token validation.
//!
//! v1 keeps it deliberately simple: a function the handlers call with the
//! request `HeaderMap` + reference config. The fancier axum `FromRequestParts`
//! integration can wait until we have a stable user/principal type.

use crate::config::ApiConfig;
use crate::error::ApiError;
use axum::http::{HeaderMap, header};

/// Validate the `Authorization: Bearer …` header against `cfg.valid_tokens`.
/// Returns the raw token on success so handlers can log usage / rate-limit.
pub fn require_token(headers: &HeaderMap, cfg: &ApiConfig) -> Result<String, ApiError> {
    let token = extract_bearer(headers)?;
    if !cfg.valid_tokens.contains(token.as_str()) {
        return Err(ApiError::InvalidToken);
    }
    Ok(token)
}

fn extract_bearer(headers: &HeaderMap) -> Result<String, ApiError> {
    let header_val = headers
        .get(header::AUTHORIZATION)
        .ok_or(ApiError::MissingAuth)?;
    let raw = header_val
        .to_str()
        .map_err(|_| ApiError::InvalidToken)?
        .trim();
    let rest = raw
        .strip_prefix("Bearer ")
        .or_else(|| raw.strip_prefix("bearer "))
        .ok_or(ApiError::InvalidToken)?;
    let token = rest.trim().to_owned();
    if token.is_empty() {
        return Err(ApiError::InvalidToken);
    }
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ahash::AHashSet;
    use axum::http::HeaderValue;

    fn hdr(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(header::AUTHORIZATION, HeaderValue::from_str(value).unwrap());
        h
    }

    fn cfg_with(token: &str) -> ApiConfig {
        let mut c = ApiConfig::default();
        let mut s = AHashSet::new();
        s.insert(token.to_owned());
        c.valid_tokens = s;
        c
    }

    #[test]
    fn extracts_a_well_formed_bearer_token() {
        let token = extract_bearer(&hdr("Bearer abc123")).unwrap();
        assert_eq!(token, "abc123");
    }

    #[test]
    fn accepts_lowercase_bearer_keyword() {
        assert_eq!(extract_bearer(&hdr("bearer xyz")).unwrap(), "xyz");
    }

    #[test]
    fn rejects_missing_header() {
        let err = extract_bearer(&HeaderMap::new()).unwrap_err();
        assert!(matches!(err, ApiError::MissingAuth));
    }

    #[test]
    fn rejects_non_bearer_schemes() {
        let err = extract_bearer(&hdr("Basic dXNlcjpwYXNz")).unwrap_err();
        assert!(matches!(err, ApiError::InvalidToken));
    }

    #[test]
    fn rejects_empty_token() {
        let err = extract_bearer(&hdr("Bearer ")).unwrap_err();
        assert!(matches!(err, ApiError::InvalidToken));
    }

    #[test]
    fn require_token_accepts_known_token() {
        let cfg = cfg_with("good");
        assert_eq!(require_token(&hdr("Bearer good"), &cfg).unwrap(), "good");
    }

    #[test]
    fn require_token_rejects_unknown_token() {
        let cfg = cfg_with("good");
        let err = require_token(&hdr("Bearer bad"), &cfg).unwrap_err();
        assert!(matches!(err, ApiError::InvalidToken));
    }
}
