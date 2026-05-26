//! Parser for `BQT` detailed book frames. See `api.md §5.1`.
//!
//! Frame layout (after the splitter strips the `\n`):
//!
//! | Op | Fields                                                                    |
//! |----|---------------------------------------------------------------------------|
//! | A  | `B:<ticker>:A:<pos>:<side>:<price>:<qty>:<broker>:<DDMMHHMM>[:<order_id>:<offer_type>]` |
//! | U  | `B:<ticker>:U:<new_pos>:<old_pos>:<side>:<price>:<qty>:<broker>:<DDMMHHMM>[:<order_id>:<offer_type>]` |
//! | E  | `B:<ticker>:E`                                                            |
//! | D1 | `B:<ticker>:D:1:<side>:<pos>`                                              |
//! | D2 | `B:<ticker>:D:2:<side>:<pos>`                                              |
//! | D3 | `B:<ticker>:D:3` (no side, no position — clears EVERYTHING)               |
//!
//! Why the optional `<order_id>:<offer_type>` tail: the documented format
//! lists 9 fields for `A`/`U`, but the example block in `api.md §5.1` shows
//! 7-field variants (`B:PETR4:A:0:A:99.99:100:131:11041005`). We support both
//! so we don't reject legitimate traffic and so legacy feeds parse cleanly.

use crate::enums::Side;
use crate::parser::ParseError;
use crate::parser::scanner::{Scanner, parse_u32};
use crate::types::{BookDelete, BookEntry, BookOp, ProtocolMessage};
use bytes::Bytes;

pub(super) fn parse(frame: Bytes) -> Result<ProtocolMessage, ParseError> {
    let mut scanner = Scanner::new(frame);

    let header = scanner.required("B header")?;
    debug_assert_eq!(header.as_ref(), b"B");

    let ticker = scanner.required("ticker")?;
    let op_code = scanner.required("book op")?;

    let op = match op_code.as_ref() {
        b"A" => parse_add(&mut scanner)?,
        b"U" => parse_update(&mut scanner)?,
        b"D" => parse_delete(&mut scanner)?,
        b"E" => BookOp::EndOfInitial,
        other => {
            // Surface unknown ops with field context so we can spot protocol
            // evolution without crashing.
            let _ = other;
            return Err(ParseError::InvalidField {
                field: "book op",
                reason: "expected A, U, D, or E",
            });
        }
    };

    Ok(ProtocolMessage::Book { ticker, op })
}

fn parse_side(b: &[u8]) -> Result<Side, ParseError> {
    if b.len() != 1 {
        return Err(ParseError::InvalidField {
            field: "book side",
            reason: "expected one byte (A or V)",
        });
    }
    Side::from_byte(b[0]).ok_or(ParseError::InvalidField {
        field: "book side",
        reason: "expected A (buy) or V (sell)",
    })
}

fn parse_add(scanner: &mut Scanner) -> Result<BookOp, ParseError> {
    let position = scanner.required_u32("position")?;
    let entry = read_entry_tail(scanner, position)?;
    Ok(BookOp::Add(entry))
}

fn parse_update(scanner: &mut Scanner) -> Result<BookOp, ParseError> {
    let new_pos = scanner.required_u32("new position")?;
    let old_pos = scanner.required_u32("old position")?;
    let entry = read_entry_tail(scanner, new_pos)?;
    Ok(BookOp::Update {
        old_pos,
        entry,
    })
}

/// Reads `<side>:<price>:<qty>:<broker>:<DDMMHHMM>[:<order_id>:<offer_type>]`
/// from `scanner` and assembles a [`BookEntry`] with the given `position`.
fn read_entry_tail(scanner: &mut Scanner, position: u32) -> Result<BookEntry, ParseError> {
    let side = parse_side(&scanner.required("side")?)?;
    let price = scanner.required_f64("price")?;
    let quantity = scanner.required_u64("quantity")?;
    let broker_id = scanner.required_u32("broker id")?;
    let ts_bytes = scanner.required("DDMMHHMM")?;
    let timestamp_ddmmhhmm = parse_u32(&ts_bytes, "DDMMHHMM")?;

    // Optional tail: order_id + offer_type. If absent, we have a legacy
    // 7-field variant. If only one of the two trailing fields is present
    // we treat it as a malformed frame (defensive — easier to spot bugs).
    let (order_id, offer_type) = match scanner.next() {
        None => (None, None),
        Some(id_bytes) => {
            let type_bytes = scanner.required("offer type")?;
            if type_bytes.len() != 1 {
                return Err(ParseError::InvalidField {
                    field: "offer type",
                    reason: "expected one byte (L or O)",
                });
            }
            (Some(id_bytes), Some(type_bytes[0]))
        }
    };

    Ok(BookEntry {
        position,
        side,
        price,
        quantity,
        broker_id,
        timestamp_ddmmhhmm,
        order_id,
        offer_type,
    })
}

fn parse_delete(scanner: &mut Scanner) -> Result<BookOp, ParseError> {
    let kind_bytes = scanner.required("delete kind")?;
    match kind_bytes.as_ref() {
        b"1" => {
            let side = parse_side(&scanner.required("side")?)?;
            let position = scanner.required_u32("position")?;
            Ok(BookOp::Delete(BookDelete::Single { side, position }))
        }
        b"2" => {
            let side = parse_side(&scanner.required("side")?)?;
            let position = scanner.required_u32("position")?;
            Ok(BookOp::Delete(BookDelete::PrefixInclusive {
                side,
                position,
            }))
        }
        b"3" => Ok(BookOp::Delete(BookDelete::ClearAll)),
        _ => Err(ParseError::InvalidField {
            field: "delete kind",
            reason: "expected 1, 2 or 3",
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_book(b: &'static [u8]) -> (Bytes, BookOp) {
        match parse(Bytes::from_static(b)).unwrap() {
            ProtocolMessage::Book { ticker, op } => (ticker, op),
            _ => unreachable!(),
        }
    }

    #[test]
    fn add_legacy_seven_field_layout_from_spec() {
        // Verbatim from `api.md §5.1` examples.
        let (ticker, op) = parse_book(b"B:PETR4:A:0:A:99.99:100:131:11041005");
        assert_eq!(ticker.as_ref(), b"PETR4");
        match op {
            BookOp::Add(entry) => {
                assert_eq!(entry.position, 0);
                assert_eq!(entry.side, Side::Buy);
                assert!((entry.price - 99.99).abs() < 1e-9);
                assert_eq!(entry.quantity, 100);
                assert_eq!(entry.broker_id, 131);
                assert_eq!(entry.timestamp_ddmmhhmm, 11_041_005);
                assert!(entry.order_id.is_none());
                assert!(entry.offer_type.is_none());
            }
            other => panic!("expected Add, got {other:?}"),
        }
    }

    #[test]
    fn add_full_nine_field_layout() {
        let (_, op) = parse_book(b"B:PETR4:A:0:A:99.99:100:131:11041005:ORD-XYZ-1:L");
        match op {
            BookOp::Add(entry) => {
                assert_eq!(entry.order_id.as_deref(), Some(b"ORD-XYZ-1".as_ref()));
                assert_eq!(entry.offer_type, Some(b'L'));
            }
            other => panic!("expected Add, got {other:?}"),
        }
    }

    #[test]
    fn update_carries_both_positions() {
        let (_, op) = parse_book(b"B:PETR4:U:1:0:A:99.98:500:37:11041130");
        match op {
            BookOp::Update { old_pos, entry } => {
                assert_eq!(old_pos, 0);
                assert_eq!(entry.position, 1);
                assert!((entry.price - 99.98).abs() < 1e-9);
                assert_eq!(entry.quantity, 500);
                assert_eq!(entry.broker_id, 37);
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn delete_kind_one_targets_a_single_entry() {
        let (_, op) = parse_book(b"B:PETR4:D:1:V:4");
        match op {
            BookOp::Delete(BookDelete::Single { side, position }) => {
                assert_eq!(side, Side::Sell);
                assert_eq!(position, 4);
            }
            other => panic!("expected Single delete, got {other:?}"),
        }
    }

    #[test]
    fn delete_kind_two_is_prefix_inclusive() {
        let (_, op) = parse_book(b"B:PETR4:D:2:A:2");
        match op {
            BookOp::Delete(BookDelete::PrefixInclusive { side, position }) => {
                assert_eq!(side, Side::Buy);
                assert_eq!(position, 2);
            }
            other => panic!("expected PrefixInclusive, got {other:?}"),
        }
    }

    #[test]
    fn delete_kind_three_is_clear_all_without_side_or_position() {
        let (_, op) = parse_book(b"B:PETR4:D:3");
        match op {
            BookOp::Delete(BookDelete::ClearAll) => {}
            other => panic!("expected ClearAll, got {other:?}"),
        }
    }

    #[test]
    fn end_of_initial_marker() {
        let (_, op) = parse_book(b"B:PETR4:E");
        match op {
            BookOp::EndOfInitial => {}
            other => panic!("expected EndOfInitial, got {other:?}"),
        }
    }

    #[test]
    fn invalid_side_byte_is_rejected() {
        let err = parse(Bytes::from_static(b"B:PETR4:A:0:X:99:1:1:11041005")).unwrap_err();
        assert!(matches!(err, ParseError::InvalidField { field: "book side", .. }));
    }

    #[test]
    fn unknown_delete_kind_is_rejected() {
        let err = parse(Bytes::from_static(b"B:PETR4:D:9:V:0")).unwrap_err();
        assert!(matches!(err, ParseError::InvalidField { field: "delete kind", .. }));
    }

    #[test]
    fn dangling_order_id_without_offer_type_is_an_error() {
        // Half a tail = malformed. Defensive: surface as MissingField rather
        // than silently dropping order_id.
        let err = parse(Bytes::from_static(b"B:PETR4:A:0:A:99.99:100:131:11041005:ORD1")).unwrap_err();
        assert!(matches!(err, ParseError::MissingField { .. }));
    }
}
