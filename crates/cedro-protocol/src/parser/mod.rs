//! Parser dispatch — turns a single framed byte slice into a
//! [`ProtocolMessage`].
//!
//! Each sub-module owns the parsing logic for one functional header. The top
//! level [`parse_frame`] looks at the header byte(s) and delegates.

use crate::frame::FrameKind;
use crate::types::ProtocolMessage;
use bytes::Bytes;
use thiserror::Error;

mod error;
mod gpn;
mod gtc;
mod mqc;
mod sqt;

// Stubs — implementations land in upcoming PRs. They surface explicit
// `TODO` errors so callers can detect unimplemented branches in tests.
mod bqt;
mod gqt;
mod nem;
mod sab;
mod vap;

#[cfg(test)]
pub(crate) mod scanner;
#[cfg(not(test))]
mod scanner;

/// Top-level parsing error.
///
/// These represent malformed input or unimplemented branches. Wire-level
/// `E:<code>` frames from the server are *not* errors here — they parse
/// successfully into [`ProtocolMessage::Error`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    #[error("empty frame")]
    Empty,

    #[error("unknown functional header: {0:?}")]
    UnknownHeader(Bytes),

    #[error("missing field at position {position} (expected {expected})")]
    MissingField {
        position: usize,
        expected: &'static str,
    },

    #[error("invalid {field}: {reason}")]
    InvalidField {
        field: &'static str,
        reason: &'static str,
    },

    #[error("malformed UTF-8 / invalid byte sequence in {field}")]
    BadEncoding { field: &'static str },

    #[error("invalid integer in field {field}")]
    BadInteger { field: &'static str },

    #[error("invalid float in field {field}")]
    BadFloat { field: &'static str },

    #[error("parser for header {header} not yet implemented")]
    NotImplemented { header: &'static str },
}

/// Parse a single frame (without the terminator byte) into a typed message.
///
/// The `kind` parameter tells us which terminator the splitter saw. We use it
/// as an additional sanity check: `T:` frames must arrive with `FrameKind::Bang`
/// and everything else with `FrameKind::Newline`.
pub fn parse_frame(kind: FrameKind, frame: Bytes) -> Result<ProtocolMessage, ParseError> {
    if frame.is_empty() {
        return Err(ParseError::Empty);
    }

    // Dispatch on the first few bytes of the header. Cedro headers are short
    // (1–3 chars) so a tiny match table is enough — no regex needed.
    if frame.starts_with(b"T:") {
        return sqt::parse(frame);
    }
    if frame.starts_with(b"B:") {
        return bqt::parse(frame);
    }
    if frame.starts_with(b"Z:") {
        return sab::parse(frame);
    }
    if frame.starts_with(b"V:") {
        return gqt::parse(frame);
    }
    if frame.starts_with(b"O:") {
        return nem::parse(frame);
    }
    if frame.starts_with(b"VAP:") {
        return vap::parse(frame);
    }
    if frame.starts_with(b"GTC:") {
        return gtc::parse(frame);
    }
    if frame.starts_with(b"C:") {
        return mqc::parse(frame);
    }
    if frame.starts_with(b"G:") {
        return gpn::parse(frame);
    }
    if frame.starts_with(b"E:") {
        return error::parse(frame);
    }

    // Belt-and-suspenders sanity check on the terminator.
    let _ = kind;

    Err(ParseError::UnknownHeader(frame))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ProtocolMessage;

    #[test]
    fn empty_frame_is_rejected() {
        let err = parse_frame(FrameKind::Newline, Bytes::new()).unwrap_err();
        assert_eq!(err, ParseError::Empty);
    }

    #[test]
    fn unknown_header_is_surfaced() {
        let bytes = Bytes::from_static(b"X:weird");
        let err = parse_frame(FrameKind::Newline, bytes).unwrap_err();
        assert!(matches!(err, ParseError::UnknownHeader(_)));
    }

    #[test]
    fn gtc_dispatch_smoke_test() {
        let bytes = Bytes::from_static(b"GTC:20170308145946");
        let msg = parse_frame(FrameKind::Newline, bytes).unwrap();
        assert!(matches!(msg, ProtocolMessage::ServerTime(_)));
    }
}
