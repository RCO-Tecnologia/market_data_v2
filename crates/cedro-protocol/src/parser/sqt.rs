//! Parser for `SQT` quote frames. See `api.md §4.1`.
//!
//! Frame shape: `T:<ticker>:<HHMMSS>:<idx>:<val>:<idx>:<val>:...` (terminator
//! `!` already stripped by the splitter).
//!
//! The first message after a subscribe carries the full snapshot; subsequent
//! messages carry only the changed indices — both share the same wire format.
//!
//! Per-index typing comes from a single source of truth: [`field_type`]. New
//! indices land in that table and parsing automatically does the right thing.

use crate::parser::ParseError;
use crate::parser::scanner::{Scanner, parse_f64, parse_i64, parse_u32, parse_u64};
use crate::types::{ProtocolMessage, QuoteDiff, QuoteFieldId, QuoteFieldValue};
use bytes::Bytes;

/// What kind of value the server sends for a given quote index.
///
/// This mirrors the type column of the table in `api.md §4.1.1`. When the
/// spec ever extends the index space, the only change required here is one
/// row in [`field_type`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldType {
    Float,
    Int,
    /// HHMMSS (6 digits, possibly missing leading zero).
    Time,
    /// `HHMMSSmmm` (9 digits, milliseconds suffix).
    TimeMs,
    /// YYYYMMDD.
    Date,
    /// YYYYMMDDHHMMSS.
    DateTime,
    /// Single ASCII character (status flag, direction, etc).
    Char,
    /// 2-character ASCII (group phase).
    Phase,
    /// Free-form bytes (description, ticker codes, etc).
    Str,
}

/// Return the wire type expected for a given quote index, defaulting to
/// `Float` for unknown indices (matches the dominant case in the spec).
const fn field_type(idx: u16) -> FieldType {
    use FieldType::{Time, TimeMs, Date, DateTime, Int, Phase, Char, Str, Float};
    match idx {
        // Timestamps.
        0 | 5 | 15 | 16 | 58 | 59 => Time,
        142..=145 => TimeMs,
        // Dates.
        1 | 54 | 64 | 87 | 129 | 141 | 154 | 208 => Date,
        50 | 51 | 125 => DateTime,
        // Integers — counts, quantities, codes, status enums.
        // (Indices 67 and 84 are enum codes for instrument/asset status
        // — see §4.1.4 / §4.1.5 — but still parsed as integers here.)
        6 | 7 | 8 | 9 | 19 | 20 | 44 | 45 | 46 | 49 | 57 | 60 | 61 | 62 | 63 | 65 | 67 | 84
        | 100 | 101 | 102 | 110 | 111 | 113 | 114 | 115 | 118 | 119 | 130 | 136 | 137 | 138
        | 203 | 205 | 209 | 213 => Int,
        // Phase code (§4.1.6).
        88 => Phase,
        // Single-char flags.
        56 | 72 | 74 | 106 => Char,
        // String fields.
        47 | 48 | 52 | 53 | 66 | 81 | 96 | 105 | 109 | 116 | 117 | 122 | 126 | 139 | 204 | 206
        | 207 | 214 | 215 => Str,
        // Reserved / undocumented codes — float is the safe default per spec.
        _ => Float,
    }
}

pub(super) fn parse(frame: Bytes) -> Result<ProtocolMessage, ParseError> {
    let mut scanner = Scanner::new(frame);

    let header = scanner.required("T header")?;
    debug_assert_eq!(header.as_ref(), b"T");

    let ticker = scanner.required("ticker")?;
    let time_bytes = scanner.required("HHMMSS")?;
    let time_hhmmss = parse_u32(&time_bytes, "HHMMSS")?;

    let mut diff: QuoteDiff = Vec::with_capacity(8);

    loop {
        let Some(idx_field) = scanner.next() else {
            break;
        };
        if idx_field.is_empty() {
            // Trailing empty token from a dangling `:` — stop cleanly.
            break;
        }
        let idx_num = crate::parser::scanner::parse_u16(&idx_field, "quote index")?;
        let value_bytes = scanner.required("quote value")?;
        let value = decode_value(idx_num, value_bytes)?;
        diff.push((QuoteFieldId(idx_num), value));
    }

    Ok(ProtocolMessage::Quote {
        ticker,
        time_hhmmss,
        diff,
    })
}

fn decode_value(idx: u16, raw: Bytes) -> Result<QuoteFieldValue, ParseError> {
    Ok(match field_type(idx) {
        FieldType::Float => QuoteFieldValue::Float(parse_f64(&raw, "quote float")?),
        FieldType::Int => QuoteFieldValue::Int(parse_i64(&raw, "quote int")?),
        FieldType::Time => QuoteFieldValue::Time(parse_u32(&raw, "quote time")?),
        FieldType::TimeMs => {
            // Stored as the integer HHMMSSmmm (up to 9 digits — fits in u32).
            let n = parse_u64(&raw, "quote time-ms")?;
            QuoteFieldValue::Time(u32::try_from(n).map_err(|_| ParseError::BadInteger {
                field: "quote time-ms",
            })?)
        }
        FieldType::Date => QuoteFieldValue::Date(parse_u32(&raw, "quote date")?),
        FieldType::DateTime => QuoteFieldValue::DateTime(parse_u64(&raw, "quote datetime")?),
        FieldType::Char => {
            if raw.len() != 1 {
                return Err(ParseError::InvalidField {
                    field: "quote char",
                    reason: "expected exactly one byte",
                });
            }
            QuoteFieldValue::Char(raw[0])
        }
        FieldType::Phase => {
            let mut buf = [0u8; 2];
            let n = raw.len().min(2);
            buf[..n].copy_from_slice(&raw[..n]);
            QuoteFieldValue::Phase(buf)
        }
        FieldType::Str => QuoteFieldValue::Str(raw),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(bytes: &'static [u8]) -> (Bytes, u32, QuoteDiff) {
        match parse(Bytes::from_static(bytes)).unwrap() {
            ProtocolMessage::Quote {
                ticker,
                time_hhmmss,
                diff,
            } => (ticker, time_hhmmss, diff),
            _ => unreachable!(),
        }
    }

    #[test]
    fn first_snapshot_example_from_spec() {
        // Lifted verbatim from `api.md §4.1` (terminator `!` already stripped).
        let (ticker, time, diff) =
            parse_ok(b"T:PETR4:101758:1:20070926:2:59.95:3:59.93:4:59.96:5:101757:6:0:7:800:8:361:9:354600");
        assert_eq!(ticker.as_ref(), b"PETR4");
        assert_eq!(time, 101_758);
        assert_eq!(diff.len(), 9);

        // Spot-check a few well-known fields.
        let by_id: std::collections::HashMap<_, _> = diff.iter().cloned().collect();
        // Index 1 (last-modification date) → YYYYMMDD.
        assert_eq!(by_id[&QuoteFieldId(1)], QuoteFieldValue::Date(20_070_926));
        // Index 2 (last trade price) → float.
        assert_eq!(by_id[&QuoteFieldId(2)], QuoteFieldValue::Float(59.95));
        // Index 9 (accumulated volume) → integer.
        assert_eq!(by_id[&QuoteFieldId(9)], QuoteFieldValue::Int(354_600));
    }

    #[test]
    fn incremental_update_example_from_spec() {
        // `T:PETR4:155613:3:43.01:19:2000:60:239:17:4000`
        let (_, _, diff) = parse_ok(b"T:PETR4:155613:3:43.01:19:2000:60:239:17:4000");
        let by_id: std::collections::HashMap<_, _> = diff.iter().cloned().collect();
        assert_eq!(by_id[&QuoteFieldId(3)], QuoteFieldValue::Float(43.01));
        // Index 19 (volume of best buy offer) → integer.
        assert_eq!(by_id[&QuoteFieldId(19)], QuoteFieldValue::Int(2000));
        // Index 60 (broker code best buy) → integer.
        assert_eq!(by_id[&QuoteFieldId(60)], QuoteFieldValue::Int(239));
        // Index 17 (accumulated volume best buy) — Float per spec.
        assert_eq!(by_id[&QuoteFieldId(17)], QuoteFieldValue::Float(4000.0));
    }

    #[test]
    fn phase_field_decodes_two_byte_codes() {
        // Field 88 carries the group phase as a 1- or 2-byte code.
        let (_, _, diff) = parse_ok(b"T:PETR4:101758:88:PN");
        assert_eq!(diff[0].0, QuoteFieldId(88));
        assert_eq!(diff[0].1, QuoteFieldValue::Phase([b'P', b'N']));
    }

    #[test]
    fn description_field_keeps_string_payload() {
        // Field 47 (ticker description) → free-form string.
        let (_, _, diff) = parse_ok(b"T:WDOZ25:101758:47:DOL");
        if let QuoteFieldValue::Str(s) = &diff[0].1 {
            assert_eq!(s.as_ref(), b"DOL");
        } else {
            panic!("expected Str");
        }
    }

    #[test]
    fn malformed_value_is_surfaced_with_field_context() {
        // Index 9 expects an integer but the value is gibberish.
        let err = parse(Bytes::from_static(b"T:PETR4:101758:9:not-a-number")).unwrap_err();
        assert!(matches!(err, ParseError::BadInteger { .. }));
    }

    #[test]
    fn odd_number_of_diff_tokens_is_rejected() {
        // Missing value for index 9.
        let err = parse(Bytes::from_static(b"T:PETR4:101758:9")).unwrap_err();
        assert!(matches!(err, ParseError::MissingField { .. }));
    }
}
