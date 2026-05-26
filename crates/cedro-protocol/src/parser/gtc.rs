//! Parser for `GTC` server-time frames. See `api.md §9.1`.
//!
//! Format: `GTC:<YYYYMMDD><HHMMSS>`
//!
//! Note the date and time are *concatenated* into a single 14-character field,
//! NOT separated by a colon. Example: `GTC:20170308145946`.

use crate::parser::ParseError;
use crate::parser::scanner::{Scanner, parse_u32};
use crate::types::{ProtocolMessage, ServerTime};
use bytes::Bytes;

pub(super) fn parse(frame: Bytes) -> Result<ProtocolMessage, ParseError> {
    let mut scanner = Scanner::new(frame);

    // First token is the literal `GTC` header.
    let header = scanner.required("GTC header")?;
    debug_assert_eq!(header.as_ref(), b"GTC");

    let payload = scanner.required("server time payload")?;
    if payload.len() != 14 {
        return Err(ParseError::InvalidField {
            field: "server time",
            reason: "expected 14-digit YYYYMMDDHHMMSS",
        });
    }
    let date = parse_u32(&payload[..8], "GTC date")?;
    let time = parse_u32(&payload[8..], "GTC time")?;

    Ok(ProtocolMessage::ServerTime(ServerTime { date, time }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_documented_example() {
        // Fixture from `api.md §9.1`.
        let msg = parse(Bytes::from_static(b"GTC:20170308145946")).unwrap();
        match msg {
            ProtocolMessage::ServerTime(ServerTime { date, time }) => {
                assert_eq!(date, 20_170_308);
                assert_eq!(time, 145_946);
            }
            other => panic!("expected ServerTime, got {other:?}"),
        }
    }

    #[test]
    fn rejects_short_payload() {
        let err = parse(Bytes::from_static(b"GTC:2017")).unwrap_err();
        assert!(matches!(err, ParseError::InvalidField { .. }));
    }
}
