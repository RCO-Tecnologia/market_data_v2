//! Parser for `SAB` aggregated book frames. See `api.md §5.3`.
//!
//! Like BQT but with `<offer_count>` instead of `broker/order_id/offer_type`,
//! and `D` only supports kinds `1` (single) and `3` (clear all) — kind `2`
//! is *not* valid in SAB.
//!
//! Frame layouts (terminator stripped):
//!
//! | Op | Fields                                                                    |
//! |----|---------------------------------------------------------------------------|
//! | A  | `Z:<ticker>:A:<pos>:<side>:<price>:<qty>:<offer_count>:<DDMMHHMM>`         |
//! | U  | `Z:<ticker>:U:<pos>:<side>:<price>:<qty>:<offer_count>:<DDMMHHMM>`         |
//! | E  | `Z:<ticker>:E`                                                            |
//! | D1 | `Z:<ticker>:D:1:<side>:<pos>`                                              |
//! | D3 | `Z:<ticker>:D:3`                                                           |

use crate::enums::Side;
use crate::parser::ParseError;
use crate::parser::scanner::{Scanner, parse_u32};
use crate::types::{AggBookLevel, AggBookOp, BookDelete, ProtocolMessage};
use bytes::Bytes;

pub(super) fn parse(frame: Bytes) -> Result<ProtocolMessage, ParseError> {
    let mut scanner = Scanner::new(frame);

    let header = scanner.required("Z header")?;
    debug_assert_eq!(header.as_ref(), b"Z");

    let ticker = scanner.required("ticker")?;
    let op_code = scanner.required("agg book op")?;

    let op = match op_code.as_ref() {
        b"A" => AggBookOp::Add(read_level(&mut scanner)?),
        b"U" => AggBookOp::Update(read_level(&mut scanner)?),
        b"D" => AggBookOp::Delete(parse_delete(&mut scanner)?),
        b"E" => AggBookOp::EndOfInitial,
        _ => {
            return Err(ParseError::InvalidField {
                field: "agg book op",
                reason: "expected A, U, D, or E",
            });
        }
    };

    Ok(ProtocolMessage::AggBook { ticker, op })
}

fn parse_side(b: &[u8]) -> Result<Side, ParseError> {
    if b.len() != 1 {
        return Err(ParseError::InvalidField {
            field: "agg book side",
            reason: "expected one byte (A or V)",
        });
    }
    Side::from_byte(b[0]).ok_or(ParseError::InvalidField {
        field: "agg book side",
        reason: "expected A (buy) or V (sell)",
    })
}

fn read_level(scanner: &mut Scanner) -> Result<AggBookLevel, ParseError> {
    let position = scanner.required_u32("position")?;
    let side = parse_side(&scanner.required("side")?)?;
    let price = scanner.required_f64("price")?;
    let quantity = scanner.required_u64("quantity")?;
    let offer_count = scanner.required_u32("offer count")?;
    let ts_bytes = scanner.required("DDMMHHMM")?;
    let timestamp_ddmmhhmm = parse_u32(&ts_bytes, "DDMMHHMM")?;
    Ok(AggBookLevel {
        position,
        side,
        price,
        quantity,
        offer_count,
        timestamp_ddmmhhmm,
    })
}

fn parse_delete(scanner: &mut Scanner) -> Result<BookDelete, ParseError> {
    let kind_bytes = scanner.required("delete kind")?;
    match kind_bytes.as_ref() {
        b"1" => {
            let side = parse_side(&scanner.required("side")?)?;
            let position = scanner.required_u32("position")?;
            Ok(BookDelete::Single { side, position })
        }
        b"3" => Ok(BookDelete::ClearAll),
        b"2" => Err(ParseError::InvalidField {
            field: "delete kind",
            reason: "SAB does not accept delete kind 2 (BQT-only)",
        }),
        _ => Err(ParseError::InvalidField {
            field: "delete kind",
            reason: "expected 1 or 3",
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_agg(b: &'static [u8]) -> (Bytes, AggBookOp) {
        match parse(Bytes::from_static(b)).unwrap() {
            ProtocolMessage::AggBook { ticker, op } => (ticker, op),
            _ => unreachable!(),
        }
    }

    #[test]
    fn buy_level_from_spec_example() {
        // Verbatim from `api.md §5.3`: `Z:PETR4:A:3:A:32.500:1000:3:08040214`
        let (ticker, op) = parse_agg(b"Z:PETR4:A:3:A:32.500:1000:3:08040214");
        assert_eq!(ticker.as_ref(), b"PETR4");
        match op {
            AggBookOp::Add(level) => {
                assert_eq!(level.position, 3);
                assert_eq!(level.side, Side::Buy);
                assert!((level.price - 32.500).abs() < 1e-9);
                assert_eq!(level.quantity, 1000);
                assert_eq!(level.offer_count, 3);
                assert_eq!(level.timestamp_ddmmhhmm, 8_040_214);
            }
            other => panic!("expected Add, got {other:?}"),
        }
    }

    #[test]
    fn sell_level_from_spec_example() {
        // `Z:PETR4:A:0:V:32.600:3200:9:08040214`
        let (_, op) = parse_agg(b"Z:PETR4:A:0:V:32.600:3200:9:08040214");
        match op {
            AggBookOp::Add(level) => {
                assert_eq!(level.side, Side::Sell);
                assert_eq!(level.quantity, 3200);
                assert_eq!(level.offer_count, 9);
            }
            other => panic!("expected Add, got {other:?}"),
        }
    }

    #[test]
    fn update_uses_same_layout_as_add() {
        let (_, op) = parse_agg(b"Z:PETR4:U:1:A:32.550:500:2:08040214");
        match op {
            AggBookOp::Update(level) => {
                assert_eq!(level.position, 1);
                assert_eq!(level.offer_count, 2);
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn delete_one_works() {
        let (_, op) = parse_agg(b"Z:PETR4:D:1:V:3");
        match op {
            AggBookOp::Delete(BookDelete::Single { side, position }) => {
                assert_eq!(side, Side::Sell);
                assert_eq!(position, 3);
            }
            other => panic!("expected Single delete, got {other:?}"),
        }
    }

    #[test]
    fn delete_three_clears_everything() {
        let (_, op) = parse_agg(b"Z:PETR4:D:3");
        match op {
            AggBookOp::Delete(BookDelete::ClearAll) => {}
            other => panic!("expected ClearAll, got {other:?}"),
        }
    }

    #[test]
    fn delete_two_is_rejected_for_sab() {
        // BQT supports prefix-inclusive delete; SAB does not. Make sure
        // we don't silently accept it.
        let err = parse(Bytes::from_static(b"Z:PETR4:D:2:A:5")).unwrap_err();
        match err {
            ParseError::InvalidField { field: "delete kind", reason } => {
                assert!(reason.contains("BQT-only"));
            }
            other => panic!("expected InvalidField with delete-kind reason, got {other:?}"),
        }
    }

    #[test]
    fn end_of_initial_marker() {
        let (_, op) = parse_agg(b"Z:PETR4:E");
        match op {
            AggBookOp::EndOfInitial => {}
            other => panic!("expected EndOfInitial, got {other:?}"),
        }
    }
}
