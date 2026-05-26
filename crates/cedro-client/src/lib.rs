//! Transport layer for the Cedro Crystal feed.
//!
//! This crate owns the *socket*: opening the TCP connection, performing the
//! handshake (`api.md §2`), spawning the dedicated reader thread that drains
//! bytes into a `crossbeam` channel ([`ARCHITECTURE.md §5.1`][arch]),
//! emitting commands, and monitoring the kernel receive buffer.
//!
//! The crate intentionally does **not** parse frames — that responsibility
//! belongs to `cedro-protocol`. The reader thread only needs `FrameSplitter`
//! from `cedro-protocol` to emit `(FrameKind, Bytes)` pairs.
//!
//! [arch]: https://example.invalid/ARCHITECTURE.md

#![cfg_attr(not(test), warn(clippy::print_stdout, clippy::print_stderr))]

pub mod commands;
pub mod config;
pub mod connection;
pub mod error;
pub mod handshake;
pub mod reader;
pub mod subscription;

pub use commands::CommandSink;
pub use config::ConnectionConfig;
pub use connection::Connection;
pub use error::ClientError;
pub use reader::{Reader, ReaderHandle};
pub use subscription::{SubscriptionKind, SubscriptionRegistry};
