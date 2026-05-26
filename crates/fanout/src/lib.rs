//! Fan-out layer: market-engine → NATS + Redis snapshot.
//!
//! Implements `ARCHITECTURE.md §5.6`. Two paths from a single update:
//!
//! 1. **NATS publish** on `market.{kind}.{ticker}` so the api-server (and any
//!    other downstream subscriber) gets the live stream.
//! 2. **Redis `HSET`** at `market:{kind}:{ticker}` so the api-server can
//!    answer `GET /v1/quote/{ticker}` and the WS subscribe-snapshot
//!    without bothering the engine.
//!
//! Both write paths run behind a coalescer keyed by `(kind, ticker)`. Within
//! a small time window (50 ms quote / 100 ms book by default) only the
//! latest update for each key is published; bursts on a hot ticker get
//! deduplicated automatically.

#![cfg_attr(not(test), warn(clippy::print_stdout, clippy::print_stderr))]

pub mod coalescer;
pub mod publisher;
pub mod types;

pub use coalescer::Coalescer;
pub use publisher::{FanoutError, FanoutPublisher, FanoutSink, NoopSink};
pub use types::{Kind, PublishKey};
