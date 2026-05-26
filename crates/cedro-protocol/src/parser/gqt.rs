//! Parser for `GQT` trade frames. See `api.md §6.1`.
//!
//! Three operations and two modes:
//!
//! - `A` (add): subscribe variant has 9 fields after `A`, snapshot variant
//!   has 10 (the extra `id_requisição` token sits between `id_negócio` and
//!   `condição_trade`). The spec counts the `A` itself in its tally — we
//!   don't, since we've already consumed that token.
//! - `D:<id_negócio>` — bust correction.
//! - `R` — remove all trades for this ticker.
//! - `E` — end of subscribe.
//! - `E:<id_requisição>` — end of snapshot.
//!
//! Disambiguating subscribe vs snapshot for `A` is done by **token count**:
//! we drain the remainder and count fields. 9 = subscribe, 10 = snapshot.
//! This is more robust than heuristics on individual field shapes.

use crate::enums::{TradeAggressor, TradeCondition};
use crate::parser::ParseError;
use crate::parser::scanner::{Scanner, parse_f64, parse_u16, parse_u32, parse_u64};
use crate::types::{ProtocolMessage, Trade, TradeOperation};
use bytes::Bytes;

pub(super) fn parse(frame: Bytes) -> Result<ProtocolMessage, ParseError> {
    let mut scanner = Scanner::new(frame);

    let header = scanner.required("V header")?;
    debug_assert_eq!(header.as_ref(), b"V");

    let ticker = scanner.required("ticker")?;
    let op_field = scanner.required("trade op")?;

    let op = match op_field.as_ref() {
        b"A" => parse_add(&mut scanner, op_field[0])?,
        b"D" => {
            let trade_id = scanner.required("trade id")?;
            TradeOperation::Delete { trade_id }
        }
        b"R" => TradeOperation::RemoveAll,
        b"E" => match scanner.next() {
            None => TradeOperation::EndOfSubscribe,
            Some(req) if req.is_empty() => TradeOperation::EndOfSubscribe,
            Some(req) => TradeOperation::EndOfSnapshot { request_id: req },
        },
        _ => {
            return Err(ParseError::InvalidField {
                field: "trade op",
                reason: "expected A, D, R or E",
            });
        }
    };

    Ok(ProtocolMessage::Trade { ticker, op })
}

fn parse_add(scanner: &mut Scanner, op_byte: u8) -> Result<TradeOperation, ParseError> {
    // Drain everything so we can decide subscribe vs snapshot by count.
    let mut fields: Vec<Bytes> = Vec::with_capacity(10);
    while let Some(token) = scanner.next() {
        // Strip a single trailing empty token caused by a dangling `:` —
        // but only at the very end, never in the middle of the payload.
        if token.is_empty() && fields.len() >= 9 {
            break;
        }
        fields.push(token);
    }

    let request_id = match fields.len() {
        9 => None,
        10 => {
            // Position 6 in the snapshot layout (0-indexed after stripping
            // the `A` token) is the request_id.
            Some(fields.remove(6))
        }
        _ => {
            return Err(ParseError::InvalidField {
                field: "trade fields",
                reason: "expected 9 (subscribe) or 10 (snapshot) fields after A",
            });
        }
    };

    // Field layout (subscribe, after request_id was already extracted from
    // snapshot mode):
    //   [0] time   [1] price  [2] broker_buy  [3] broker_sell  [4] qty
    //   [5] trade_id  [6] condition  [7] aggressor  [8] original_conditions
    let time_hhmmss = parse_u32(&fields[0], "trade time")?;
    let price = parse_f64(&fields[1], "trade price")?;
    let broker_buy_id = parse_u32(&fields[2], "trade broker buy")?;
    let broker_sell_id = parse_u32(&fields[3], "trade broker sell")?;
    let quantity = parse_u64(&fields[4], "trade quantity")?;
    let trade_id = fields[5].clone();
    let condition_code = parse_u16(&fields[6], "trade condition")?;
    let aggressor_byte = single_byte(&fields[7], "trade aggressor")?;
    let original_conditions = fields[8].clone();

    Ok(TradeOperation::Add(Trade {
        operation_code: op_byte,
        time_hhmmss,
        price,
        broker_buy_id,
        broker_sell_id,
        quantity,
        trade_id,
        request_id,
        condition: TradeCondition::from_code(condition_code),
        aggressor: TradeAggressor::from_byte(aggressor_byte),
        original_conditions,
    }))
}

fn single_byte(b: &Bytes, field: &'static str) -> Result<u8, ParseError> {
    if b.len() != 1 {
        return Err(ParseError::InvalidField {
            field,
            reason: "expected exactly one byte",
        });
    }
    Ok(b[0])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_trade(b: &'static [u8]) -> (Bytes, TradeOperation) {
        match parse(Bytes::from_static(b)).unwrap() {
            ProtocolMessage::Trade { ticker, op } => (ticker, op),
            _ => unreachable!(),
        }
    }

    #[test]
    fn subscribe_add_with_ten_fields() {
        let (ticker, op) = parse_trade(b"V:PETR4:A:155613:43.01:239:354:100:T123456:0:A:0");
        assert_eq!(ticker.as_ref(), b"PETR4");
        match op {
            TradeOperation::Add(trade) => {
                assert_eq!(trade.time_hhmmss, 155_613);
                assert!((trade.price - 43.01).abs() < 1e-9);
                assert_eq!(trade.broker_buy_id, 239);
                assert_eq!(trade.broker_sell_id, 354);
                assert_eq!(trade.quantity, 100);
                assert_eq!(trade.trade_id.as_ref(), b"T123456");
                assert!(trade.request_id.is_none(), "subscribe should not carry request_id");
                assert_eq!(trade.condition, TradeCondition::NotDirect);
                assert_eq!(trade.aggressor, TradeAggressor::Buyer);
                assert_eq!(trade.original_conditions.as_ref(), b"0");
            }
            other => panic!("expected Add, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_add_carries_request_id() {
        let (_, op) = parse_trade(b"V:PETR4:A:155613:43.01:239:354:100:T123456:REQ-7:0:A:0");
        match op {
            TradeOperation::Add(trade) => {
                assert_eq!(trade.request_id.as_deref(), Some(b"REQ-7".as_ref()));
                assert_eq!(trade.condition, TradeCondition::NotDirect);
                assert_eq!(trade.aggressor, TradeAggressor::Buyer);
            }
            other => panic!("expected Add, got {other:?}"),
        }
    }

    #[test]
    fn original_conditions_are_kept_as_raw_bytes() {
        // Per spec §6.1.7, original-condition flags are SPACE-separated.
        let (_, op) = parse_trade(b"V:PETR4:A:155613:43.01:239:354:100:T1:2:V:RF RL MP");
        match op {
            TradeOperation::Add(trade) => {
                assert_eq!(trade.aggressor, TradeAggressor::Seller);
                assert_eq!(trade.condition, TradeCondition::Rlp);
                assert_eq!(trade.original_conditions.as_ref(), b"RF RL MP");
            }
            other => panic!("expected Add, got {other:?}"),
        }
    }

    #[test]
    fn delete_carries_trade_id() {
        let (_, op) = parse_trade(b"V:PETR4:D:T123456");
        match op {
            TradeOperation::Delete { trade_id } => {
                assert_eq!(trade_id.as_ref(), b"T123456");
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn remove_all_has_no_payload() {
        let (_, op) = parse_trade(b"V:PETR4:R");
        match op {
            TradeOperation::RemoveAll => {}
            other => panic!("expected RemoveAll, got {other:?}"),
        }
    }

    #[test]
    fn end_of_subscribe_has_no_request_id() {
        let (_, op) = parse_trade(b"V:PETR4:E");
        match op {
            TradeOperation::EndOfSubscribe => {}
            other => panic!("expected EndOfSubscribe, got {other:?}"),
        }
    }

    #[test]
    fn end_of_snapshot_carries_request_id() {
        let (_, op) = parse_trade(b"V:PETR4:E:REQ-7");
        match op {
            TradeOperation::EndOfSnapshot { request_id } => {
                assert_eq!(request_id.as_ref(), b"REQ-7");
            }
            other => panic!("expected EndOfSnapshot, got {other:?}"),
        }
    }

    #[test]
    fn wrong_field_count_is_rejected() {
        // 8 fields after A — neither subscribe (9) nor snapshot (10).
        let err = parse(Bytes::from_static(b"V:PETR4:A:1:2:3:4:5:6:7:8")).unwrap_err();
        assert!(matches!(err, ParseError::InvalidField { field: "trade fields", .. }));
    }

    #[test]
    fn aggressor_codes_round_trip() {
        for (byte, expected) in [
            (b'A', TradeAggressor::Buyer),
            (b'V', TradeAggressor::Seller),
            (b'I', TradeAggressor::Undefined),
        ] {
            let frame = format!("V:PETR4:A:155613:1.0:1:1:1:T1:0:{}:0", byte as char);
            let bytes = Bytes::from(frame.into_bytes());
            let msg = parse(bytes).unwrap();
            match msg {
                ProtocolMessage::Trade { op: TradeOperation::Add(trade), .. } => {
                    assert_eq!(trade.aggressor, expected);
                }
                _ => unreachable!(),
            }
        }
    }
}
