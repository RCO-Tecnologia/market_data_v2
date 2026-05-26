//! Parser for `GPN` broker dictionary lines. See `mqc.md §2`.
//!
//! Format: `G:<EXCHANGE>:<CODE>:<NAME>:<CEDRO_ID>:<ACTIVE>`
//!
//! Where `<ACTIVE>` is `1` (active) or `0` (inactive). The list has no explicit
//! terminator — the client detects the end by seeing a non-`G:` line.

use crate::parser::ParseError;
use crate::parser::scanner::Scanner;
use crate::types::{GpnItem, ProtocolMessage};
use bytes::Bytes;

pub(super) fn parse(frame: Bytes) -> Result<ProtocolMessage, ParseError> {
    let mut scanner = Scanner::new(frame);

    let header = scanner.required("G header")?;
    debug_assert_eq!(header.as_ref(), b"G");

    let exchange = scanner.required("exchange")?;
    let code = scanner.required("broker code")?;
    let name = scanner.required("broker name")?;
    let cedro_id = scanner.required("cedro id")?;
    let active_field = scanner.required("active flag")?;

    let active = match active_field.as_ref() {
        b"1" => true,
        b"0" => false,
        _ => {
            return Err(ParseError::InvalidField {
                field: "active flag",
                reason: "expected '0' or '1'",
            });
        }
    };

    Ok(ProtocolMessage::GpnItem(GpnItem {
        exchange,
        code,
        name,
        cedro_id,
        active,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_gpn(b: &'static [u8]) -> GpnItem {
        match parse(Bytes::from_static(b)).unwrap() {
            ProtocolMessage::GpnItem(item) => item,
            _ => unreachable!(),
        }
    }

    #[test]
    fn xp_investimentos_fixture() {
        // Lifted from `mqc.md §2.5`.
        let it = parse_gpn(b"G:BOVESPA:3:XP INVESTIMENTOS:3:1");
        assert_eq!(it.exchange.as_ref(), b"BOVESPA");
        assert_eq!(it.code.as_ref(), b"3");
        assert_eq!(it.name.as_ref(), b"XP INVESTIMENTOS");
        assert_eq!(it.cedro_id.as_ref(), b"3");
        assert!(it.active);
    }

    #[test]
    fn bradesco_fixture() {
        let it = parse_gpn(b"G:BOVESPA:72:BRADESCO S/A CTVM:72:1");
        assert_eq!(it.code.as_ref(), b"72");
        assert_eq!(it.name.as_ref(), b"BRADESCO S/A CTVM");
        assert!(it.active);
    }

    #[test]
    fn inactive_broker() {
        let it = parse_gpn(b"G:BOVESPA:999:OLD BROKER:999:0");
        assert!(!it.active);
    }

    #[test]
    fn rejects_garbage_active_flag() {
        let err = parse(Bytes::from_static(b"G:BOVESPA:1:X:1:maybe")).unwrap_err();
        assert!(matches!(err, ParseError::InvalidField { .. }));
    }
}
