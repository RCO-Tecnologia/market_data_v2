//! Crate-level error type.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("missing Authorization header")]
    MissingAuth,
    #[error("invalid token")]
    InvalidToken,
    #[error("rate limit exceeded")]
    RateLimited,
    #[error("ticker {0:?} not known")]
    TickerNotFound(String),
    #[error("book not available for this market")]
    BookNotAvailable,
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("invalid request: {0}")]
    BadRequest(&'static str),
    #[error("internal error: {0}")]
    Internal(&'static str),
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
}

impl ApiError {
    pub const fn status(&self) -> StatusCode {
        match self {
            Self::MissingAuth | Self::InvalidToken => StatusCode::UNAUTHORIZED,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::TickerNotFound(_) | Self::BookNotAvailable => StatusCode::NOT_FOUND,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Redis(_) | Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub const fn code(&self) -> &'static str {
        match self {
            Self::MissingAuth => "missing_auth",
            Self::InvalidToken => "invalid_token",
            Self::RateLimited => "rate_limited",
            Self::TickerNotFound(_) => "ticker_not_found",
            Self::BookNotAvailable => "book_not_available",
            Self::Redis(_) | Self::Internal(_) => "internal",
            Self::BadRequest(_) => "bad_request",
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = ErrorBody {
            code: self.code(),
            message: self.to_string(),
        };
        metrics::counter!(
            "api_errors_total",
            "code" => self.code(),
        )
        .increment(1);
        (status, axum::Json(body)).into_response()
    }
}
