//! Parser for `VAP` volume-at-price frames. See `api.md §8`.
//!
//! Frame layout (terminator stripped):
//!
//! Data line, 15 fields total:
//! `VAP:<ticker>:<price>:<qty_buyer>:<vol_buyer>:<qty_seller>:<vol_seller>:<qty_direct>:<vol_direct>:<qty_undef>:<vol_undef>:<period>:<qty_rlp>:<vol_rlp>:<qty_auction>:<vol_auction>`
//!
//! Terminators:
//! - `VAP:<ticker>:E`       — no-period variant
//! - `VAP:<ticker>:E:<period>` — period variant
//!
//! The `<period>` field on data lines is `0` when the subscribe didn't ask
//! for a specific window — we map that to `None` in [`VapEntry`].
//!
//! Note: terminator frames also produce a [`ProtocolMessage::Vap`] but with
//! a synthetic entry where every numeric field is zero. Downstream layers
//! detect the end of a batch by matching on the special token `E` in the
//! second position — that's outside this parser's concern.

use crate::parser::ParseError;
use crate::parser::scanner::{Scanner, parse_f64, parse_u32};
use crate::types::{ProtocolMessage, VapEntry};
use bytes::Bytes;

pub(super) fn parse(frame: Bytes) -> Result<ProtocolMessage, ParseError> {
    let mut scanner = Scanner::new(frame);

    let header = scanner.required("VAP header")?;
    debug_assert_eq!(header.as_ref(), b"VAP");

    let ticker = scanner.required("ticker")?;

    // Peek the next token. If it's literally `E`, this is a terminator.
    let third = scanner.required("price or terminator")?;
    if third.as_ref() == b"E" {
        // Optional `<period>` after `E` for the period variant.
        let period_minutes = match scanner.next() {
            Some(p) if !p.is_empty() => Some(scanner_parse_u32(&p, "vap period")?),
            _ => None,
        };
        return Ok(ProtocolMessage::Vap(VapEntry {
            ticker,
            price: 0.0,
            buyer_trades: 0.0,
            buyer_volume: 0.0,
            seller_trades: 0.0,
            seller_volume: 0.0,
            direct_trades: 0.0,
            direct_volume: 0.0,
            undefined_trades: 0.0,
            undefined_volume: 0.0,
            period_minutes,
            rlp_trades: 0.0,
            rlp_volume: 0.0,
            auction_trades: 0.0,
            auction_volume: 0.0,
        }));
    }

    // Data line. `third` is already the price token.
    let price = parse_f64(&third, "vap price")?;
    let buyer_trades = scanner.required_f64("vap buyer_trades")?;
    let buyer_volume = scanner.required_f64("vap buyer_volume")?;
    let seller_trades = scanner.required_f64("vap seller_trades")?;
    let seller_volume = scanner.required_f64("vap seller_volume")?;
    let direct_trades = scanner.required_f64("vap direct_trades")?;
    let direct_volume = scanner.required_f64("vap direct_volume")?;
    let undefined_trades = scanner.required_f64("vap undefined_trades")?;
    let undefined_volume = scanner.required_f64("vap undefined_volume")?;
    let period_raw = scanner.required("vap period")?;
    let period_minutes = if period_raw.as_ref() == b"0" || period_raw.is_empty() {
        None
    } else {
        Some(scanner_parse_u32(&period_raw, "vap period")?)
    };
    let rlp_trades = scanner.required_f64("vap rlp_trades")?;
    let rlp_volume = scanner.required_f64("vap rlp_volume")?;
    let auction_trades = scanner.required_f64("vap auction_trades")?;
    let auction_volume = scanner.required_f64("vap auction_volume")?;

    Ok(ProtocolMessage::Vap(VapEntry {
        ticker,
        price,
        buyer_trades,
        buyer_volume,
        seller_trades,
        seller_volume,
        direct_trades,
        direct_volume,
        undefined_trades,
        undefined_volume,
        period_minutes,
        rlp_trades,
        rlp_volume,
        auction_trades,
        auction_volume,
    }))
}

fn scanner_parse_u32(b: &Bytes, field: &'static str) -> Result<u32, ParseError> {
    parse_u32(b, field)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_vap(b: &'static [u8]) -> VapEntry {
        match parse(Bytes::from_static(b)).unwrap() {
            ProtocolMessage::Vap(e) => e,
            _ => unreachable!(),
        }
    }

    #[test]
    fn no_period_data_line() {
        // 15 fields total: ticker + price + 8 + period + 4. Period token = 0.
        let e = parse_vap(b"VAP:PETR4:32.50:5:1500:3:900:1:300:2:600:0:1:100:0:0");
        assert_eq!(e.ticker.as_ref(), b"PETR4");
        assert!((e.price - 32.50).abs() < 1e-9);
        assert!((e.buyer_trades - 5.0).abs() < 1e-9);
        assert!((e.buyer_volume - 1500.0).abs() < 1e-9);
        assert!(e.period_minutes.is_none());
        assert!((e.rlp_trades - 1.0).abs() < 1e-9);
    }

    #[test]
    fn period_data_line() {
        // Period = 5 minutes.
        let e = parse_vap(b"VAP:PETR4:32.50:5:1500:3:900:1:300:2:600:5:1:100:0:0");
        assert_eq!(e.period_minutes, Some(5));
    }

    #[test]
    fn terminator_no_period() {
        let e = parse_vap(b"VAP:PETR4:E");
        assert_eq!(e.ticker.as_ref(), b"PETR4");
        assert!(e.period_minutes.is_none());
    }

    #[test]
    fn terminator_with_period() {
        let e = parse_vap(b"VAP:PETR4:E:5");
        assert_eq!(e.ticker.as_ref(), b"PETR4");
        assert_eq!(e.period_minutes, Some(5));
    }

    #[test]
    fn auction_and_rlp_counters_are_parsed() {
        // Trailing fields ordered: rlp_trades, rlp_volume, auction_trades, auction_volume.
        let e = parse_vap(b"VAP:WDOZ25:5500:10:5000:8:4000:0:0:0:0:0:2:1000:7:7000");
        assert!((e.rlp_trades - 2.0).abs() < 1e-9);
        assert!((e.rlp_volume - 1000.0).abs() < 1e-9);
        assert!((e.auction_trades - 7.0).abs() < 1e-9);
        assert!((e.auction_volume - 7000.0).abs() < 1e-9);
    }

    #[test]
    fn rejects_short_frame() {
        let err = parse(Bytes::from_static(b"VAP:PETR4:32.50:5:1500")).unwrap_err();
        assert!(matches!(err, ParseError::MissingField { .. }));
    }
}
