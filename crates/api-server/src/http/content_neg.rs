//! Content negotiation: JSON (default) vs MessagePack via `Accept` header.

use axum::http::{HeaderMap, HeaderValue, header};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Accept {
    Json,
    MessagePack,
}

impl Accept {
    pub const fn content_type(self) -> &'static str {
        match self {
            Self::Json => "application/json",
            Self::MessagePack => "application/msgpack",
        }
    }

    /// Resolve from request headers. Defaults to JSON when the client
    /// asked for `*/*`, didn't send an `Accept` header, or sent something
    /// we don't speak.
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let Some(val) = headers.get(header::ACCEPT) else {
            return Self::Json;
        };
        let s = val.to_str().unwrap_or("");
        if s.contains("application/msgpack") || s.contains("application/x-msgpack") {
            Self::MessagePack
        } else {
            Self::Json
        }
    }

    /// Render a `Content-Type` header value for this accept type.
    pub const fn header_value(self) -> HeaderValue {
        HeaderValue::from_static(self.content_type())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hdr(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(header::ACCEPT, HeaderValue::from_str(value).unwrap());
        h
    }

    #[test]
    fn defaults_to_json_when_no_header() {
        assert_eq!(Accept::from_headers(&HeaderMap::new()), Accept::Json);
    }

    #[test]
    fn defaults_to_json_for_wildcard() {
        assert_eq!(Accept::from_headers(&hdr("*/*")), Accept::Json);
    }

    #[test]
    fn detects_messagepack() {
        assert_eq!(
            Accept::from_headers(&hdr("application/msgpack")),
            Accept::MessagePack
        );
        // Also accept the x- prefixed alias used by some clients.
        assert_eq!(
            Accept::from_headers(&hdr("application/x-msgpack")),
            Accept::MessagePack
        );
    }

    #[test]
    fn unknown_accept_falls_back_to_json() {
        assert_eq!(Accept::from_headers(&hdr("text/yaml")), Accept::Json);
    }
}
