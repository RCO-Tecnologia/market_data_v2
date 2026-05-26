//! API surface for downstream consumers.
//!
//! `ARCHITECTURE.md §5.7`. Same binary exposes:
//!
//! - `/ws` — long-lived WebSocket with multi-subscribe (quote / book / trade
//!   per ticker), snapshot-then-stream semantics.
//! - `/v1/{quote,book,trades,instruments,brokers,health}` — HTTP REST.
//!
//! Both surfaces read from the same Redis snapshot store; `/ws` additionally
//! tails NATS subjects for the stream half. Auth is a bearer token; content
//! negotiation supports JSON (default) and MessagePack (`Accept: application/msgpack`).

#![cfg_attr(not(test), warn(clippy::print_stdout, clippy::print_stderr))]

pub mod auth;
pub mod config;
pub mod error;
pub mod http;
pub mod router;
pub mod snapshot_store;
pub mod ws;

pub use config::ApiConfig;
pub use error::ApiError;
pub use router::build_router;
pub use snapshot_store::{SnapshotKind, SnapshotStore};
pub use ws::{ClientMessage, ServerMessage, SubscribeChannel};
