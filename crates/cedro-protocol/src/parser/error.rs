//! Parser for wire-level `E:<code>[:<context>...]` frames.
//!
//! Per `api.md §10`, the server can emit an error at any point. Errors are not
//! a fatal parse failure on our side — they parse cleanly into
//! [`ProtocolMessage::Error`] and the upstream layer decides what to do
//! (retry, abort, ignore) using the helpers on [`CedroError`].

use crate::parser::ParseError;
use crate::parser::scanner::{Scanner, parse_u16};
use crate::types::{CedroError, ProtocolMessage};
use bytes::Bytes;

pub(super) fn parse(frame: Bytes) -> Result<ProtocolMessage, ParseError> {
    let mut scanner = Scanner::new(frame);

    // `E` header.
    let header = scanner.required("E header")?;
    debug_assert_eq!(header.as_ref(), b"E");

    let code_bytes = scanner.required("error code")?;
    let code = parse_u16(&code_bytes, "error code")?;

    // The remainder is free-form context. We collect each colon-delimited
    // token but DO NOT split the very last field on `:` — most errors have
    // structured context but some (E:12 host names, E:16 request ids) may
    // contain unexpected punctuation. Capturing them as separate tokens is
    // cheap and the engine layer can re-join if it wants.
    let mut context = Vec::new();
    while let Some(token) = scanner.next() {
        context.push(token);
    }

    Ok(ProtocolMessage::Error(CedroError { code, context }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_error(bytes: &'static [u8]) -> CedroError {
        match parse(Bytes::from_static(bytes)).unwrap() {
            ProtocolMessage::Error(e) => e,
            _ => unreachable!(),
        }
    }

    #[test]
    fn invalid_command() {
        let e = parse_error(b"E:1:SQT");
        assert_eq!(e.code, 1);
        assert_eq!(e.context.len(), 1);
        assert_eq!(e.context[0].as_ref(), b"SQT");
        assert!(e.is_client_bug());
        assert!(!e.is_transient_server());
    }

    #[test]
    fn server_migration() {
        let e = parse_error(b"E:12:new-host.example.com");
        assert_eq!(e.code, 12);
        assert!(e.is_migration());
        assert_eq!(e.context[0].as_ref(), b"new-host.example.com");
    }

    #[test]
    fn forced_disconnect_is_flagged() {
        for code in [6_u16, 7, 8, 9] {
            let frame = format!("E:{code}");
            let bytes = Bytes::from(frame.into_bytes());
            let msg = parse(bytes).unwrap();
            match msg {
                ProtocolMessage::Error(e) => assert!(e.is_forced_disconnect()),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn no_permission_for_specific_quote() {
        let e = parse_error(b"E:19:No permission in quote:PETR4");
        assert_eq!(e.code, 19);
        assert_eq!(e.context.len(), 2);
        assert_eq!(e.context[0].as_ref(), b"No permission in quote");
        assert_eq!(e.context[1].as_ref(), b"PETR4");
    }
}
