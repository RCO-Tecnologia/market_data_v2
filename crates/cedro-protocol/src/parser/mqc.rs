//! Parser for `MQC` catalog discovery frames. See `mqc.md §1`.
//!
//! Two shapes:
//! - List item: `C:<MARKET>:<TICKER>[:<extra fields>...]`
//! - Terminator: `C:<MARKET>:E`
//!
//! The third field "literally `E`" sentinel can NOT be confused with a ticker
//! called "E" — `mqc.md §1.4` is explicit that a 3-field line where token 2
//! is exactly `E` is the end of the list.

use crate::parser::ParseError;
use crate::parser::scanner::Scanner;
use crate::types::{MqcItem, ProtocolMessage};
use bytes::Bytes;

pub(super) fn parse(frame: Bytes) -> Result<ProtocolMessage, ParseError> {
    let mut scanner = Scanner::new(frame);

    let header = scanner.required("C header")?;
    debug_assert_eq!(header.as_ref(), b"C");

    let market = scanner.required("market")?;
    let third = scanner.required("ticker or end-sentinel")?;

    // Detect end-of-list. Must be exactly 3 fields total AND token 2 = "E".
    let is_end = third.as_ref() == b"E" && scanner.next().is_none();
    if is_end {
        return Ok(ProtocolMessage::MqcItem(MqcItem::End { market }));
    }

    // Any additional fields after the ticker (rare but allowed per spec) are
    // ignored — they aren't part of any documented behavior.
    Ok(ProtocolMessage::MqcItem(MqcItem::Symbol {
        market,
        ticker: third,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_symbol_lines() {
        let cases: &[(&[u8], &[u8], &[u8])] = &[
            (b"C:BOVESPA:PETR4", b"BOVESPA", b"PETR4"),
            (b"C:BOVESPA:VALE3", b"BOVESPA", b"VALE3"),
            (b"C:BMF:WDOZ25", b"BMF", b"WDOZ25"),
        ];
        for (input, market, ticker) in cases {
            let msg = parse(Bytes::from_static(input)).unwrap();
            match msg {
                ProtocolMessage::MqcItem(MqcItem::Symbol {
                    market: m,
                    ticker: t,
                }) => {
                    assert_eq!(m.as_ref(), *market);
                    assert_eq!(t.as_ref(), *ticker);
                }
                other => panic!("expected Symbol, got {other:?}"),
            }
        }
    }

    #[test]
    fn parses_end_sentinel() {
        let msg = parse(Bytes::from_static(b"C:BOVESPA:E")).unwrap();
        match msg {
            ProtocolMessage::MqcItem(MqcItem::End { market }) => {
                assert_eq!(market.as_ref(), b"BOVESPA");
            }
            other => panic!("expected End, got {other:?}"),
        }
    }

    #[test]
    fn does_not_confuse_ticker_named_e_with_terminator() {
        // Hypothetical (and unlikely) ticker named "E" — only the
        // exact-three-field rule prevents misclassification.
        // Documented in `mqc.md §1.4`. Here we just verify our heuristic:
        // a 3-field line with token 2 = `E` is always the end-sentinel.
        // A line `C:BOV:E:extra` is NOT (4 fields).
        let msg = parse(Bytes::from_static(b"C:BOV:E:extra")).unwrap();
        match msg {
            ProtocolMessage::MqcItem(MqcItem::Symbol { ticker, .. }) => {
                assert_eq!(ticker.as_ref(), b"E");
            }
            other => panic!("expected Symbol, got {other:?}"),
        }
    }
}
