//! Append-only rotating zstd archive of raw Cedro frames.
//!
//! `ARCHITECTURE.md §5.3`. Each frame the splitter produces is teed off a
//! dedicated channel and written to disk by a background thread. Files
//! rotate when the hour rolls over so retention / deletion is easy
//! (cron-style cleanup of files older than N days).
//!
//! ## Hard guarantees
//!
//! - The writer thread never blocks the hot path. If its inbound channel
//!   fills up, the producer **drops** the frame and increments
//!   `raw_archive_dropped_frames_total` — we prefer losing archive coverage
//!   to losing the live Cedro connection (§5.1.2).
//!
//! - Files are flushed/closed on rotation and at shutdown. Default rotation
//!   is `YYYY-MM-DDTHH.zst` in the configured root directory.

#![cfg_attr(not(test), warn(clippy::print_stdout, clippy::print_stderr))]

mod writer;

pub use writer::{ArchiveConfig, ArchiveError, ArchiveHandle, ArchiveWriter};
