//! HTTP REST surface — `/v1/*` routes that serve snapshots from Redis.

mod content_neg;
pub mod handlers;

pub use content_neg::Accept;
pub use handlers::{HttpState, build_http_routes};
