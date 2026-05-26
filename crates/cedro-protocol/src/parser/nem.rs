//! Parser for `NEM` news frames. See `api.md §7.2`.
//!
//! Three sub-formats share the `O:` prefix:
//!
//! - `O:A:<agency>:<code>:<date>:<time>:<category>:<title_len>:<title>` —
//!   live headline (`NEM A <agency>` subscribe).
//! - `O:L:<req_id>:<agency>:<code>:<date>:<time>:<category>:<title_len>:<title>` —
//!   one history item from `NEM L`.
//! - `O:L:<req_id>:END` — sentinel closing a history list.
//! - `O:N:<req_id>:<agency>:<code>:<body>` — news body (`NEM N`). The body
//!   may contain `:` and almost certainly contains ASCII 0x03 (ETX) bytes
//!   in place of `\r\n`. We DO NOT translate those here — the engine layer
//!   decides whether the consumer wants the raw bytes or a `\n`-normalised
//!   version. See `api.md §7.2.3`.
//!
//! Titles may legally contain `:`. The spec gives us `<title_len>` for that
//! reason, but we sidestep it: we treat the title as "everything that's left
//! after the colon-separated prefix", which is robust against title content.

use crate::parser::ParseError;
use crate::parser::scanner::{Scanner, parse_u32};
use crate::types::{NewsHeadline, NewsMessage, ProtocolMessage};
use bytes::Bytes;

pub(super) fn parse(frame: Bytes) -> Result<ProtocolMessage, ParseError> {
    let mut scanner = Scanner::new(frame);

    let header = scanner.required("O header")?;
    debug_assert_eq!(header.as_ref(), b"O");

    let kind = scanner.required("news kind")?;

    let msg = match kind.as_ref() {
        b"A" => NewsMessage::Headline(read_headline(&mut scanner, None)?),
        b"L" => {
            let req_id = scanner.required("request id")?;
            // Peek next token to detect the END sentinel.
            let next = scanner.required("agency or END")?;
            if next.as_ref() == b"END" {
                NewsMessage::HistoryEnd { request_id: req_id }
            } else {
                let headline = read_headline(&mut scanner, Some(next))?;
                NewsMessage::HistoryItem {
                    request_id: req_id,
                    headline,
                }
            }
        }
        b"N" => {
            let request_id = scanner.required("request id")?;
            let agency = scanner.required("agency")?;
            let code = scanner.required("code")?;
            // Body keeps the rest verbatim (may contain `:` and ETX bytes).
            let body_raw = scanner.take_rest();
            NewsMessage::Body {
                request_id,
                agency,
                code,
                body_raw,
            }
        }
        _ => {
            return Err(ParseError::InvalidField {
                field: "news kind",
                reason: "expected A, L, or N",
            });
        }
    };

    Ok(ProtocolMessage::News(msg))
}

/// Reads the headline fields:
/// `<agency>:<code>:<date>:<time>:<category>:<title_len>:<title>`
///
/// When `pre_consumed_agency` is `Some`, the caller has already read the
/// agency token (used by the `L` variant to disambiguate END vs item).
fn read_headline(
    scanner: &mut Scanner,
    pre_consumed_agency: Option<Bytes>,
) -> Result<NewsHeadline, ParseError> {
    let agency = match pre_consumed_agency {
        Some(a) => a,
        None => scanner.required("agency")?,
    };
    let code = scanner.required("code")?;
    let date_bytes = scanner.required("news date")?;
    let date = parse_u32(&date_bytes, "news date")?;
    let time_bytes = scanner.required("news time")?;
    let time = parse_u32(&time_bytes, "news time")?;
    let category_bytes = scanner.required("news category")?;
    let category = parse_u32(&category_bytes, "news category")?;
    // The spec provides `<title_len>` but the title may itself contain `:`,
    // so the only safe move is to take the remainder of the buffer untouched.
    let _title_len_bytes = scanner.required("news title length")?;
    // Take the rest verbatim — this preserves any `:` inside the title.
    let title = scanner.take_rest();
    Ok(NewsHeadline {
        agency,
        code,
        date,
        time,
        category,
        title,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_news(b: &'static [u8]) -> NewsMessage {
        match parse(Bytes::from_static(b)).unwrap() {
            ProtocolMessage::News(m) => m,
            _ => unreachable!(),
        }
    }

    #[test]
    fn live_headline_from_spec_example() {
        // Fixture from `api.md §7.2.3`:
        // O:A:BOV:1565281:20090617:134432:1:46:LEILAO DE IBOVT40 (OPV IBOV AGO/40.000) ATE 13
        let msg = parse_news(
            b"O:A:BOV:1565281:20090617:134432:1:46:LEILAO DE IBOVT40 (OPV IBOV AGO/40.000) ATE 13",
        );
        match msg {
            NewsMessage::Headline(h) => {
                assert_eq!(h.agency.as_ref(), b"BOV");
                assert_eq!(h.code.as_ref(), b"1565281");
                assert_eq!(h.date, 20_090_617);
                assert_eq!(h.time, 134_432);
                assert_eq!(h.category, 1);
                assert!(h.title.starts_with(b"LEILAO"));
            }
            other => panic!("expected Headline, got {other:?}"),
        }
    }

    #[test]
    fn history_item_carries_request_id() {
        // Adapted from `api.md §7.2`:
        let msg = parse_news(
            b"O:L:123:BOV:1565287:20090617:134715:1:62:17:06-OFERTAS DISPONIVEIS NO BANCO DE TITULOS CBLC-BTC-4 13:46",
        );
        match msg {
            NewsMessage::HistoryItem { request_id, headline } => {
                assert_eq!(request_id.as_ref(), b"123");
                assert_eq!(headline.agency.as_ref(), b"BOV");
                assert_eq!(headline.code.as_ref(), b"1565287");
                // Title contains `:` and must round-trip verbatim.
                assert!(headline.title.ends_with(b"BTC-4 13:46"));
            }
            other => panic!("expected HistoryItem, got {other:?}"),
        }
    }

    #[test]
    fn history_end_marker() {
        let msg = parse_news(b"O:L:123:END");
        match msg {
            NewsMessage::HistoryEnd { request_id } => {
                assert_eq!(request_id.as_ref(), b"123");
            }
            other => panic!("expected HistoryEnd, got {other:?}"),
        }
    }

    #[test]
    fn body_preserves_colons_and_etx() {
        // `\x03` (ETX) replaces `\r\n` in news bodies — must NOT be touched here.
        let frame: &[u8] = b"O:N:123:CFN:2221941:Headline\x03Paragraph 2: more text\x03End";
        let ProtocolMessage::News(msg) = parse(Bytes::copy_from_slice(frame)).unwrap() else {
            unreachable!()
        };
        match msg {
            NewsMessage::Body { request_id, agency, code, body_raw } => {
                assert_eq!(request_id.as_ref(), b"123");
                assert_eq!(agency.as_ref(), b"CFN");
                assert_eq!(code.as_ref(), b"2221941");
                // Body keeps ETX and `:` exactly as the server sent them.
                assert_eq!(body_raw.as_ref(), b"Headline\x03Paragraph 2: more text\x03End");
            }
            other => panic!("expected Body, got {other:?}"),
        }
    }

    #[test]
    fn body_from_spec_example() {
        // `api.md §7.2.3` example:
        // O:N:123:CFN:2221941:Mineradores Mamani e Peña recebem alta do hospital no Chile
        let msg = parse_news(
            b"O:N:123:CFN:2221941:Mineradores Mamani e Pena recebem alta do hospital no Chile",
        );
        match msg {
            NewsMessage::Body { body_raw, .. } => {
                assert!(body_raw.starts_with(b"Mineradores"));
                assert!(body_raw.ends_with(b"Chile"));
            }
            other => panic!("expected Body, got {other:?}"),
        }
    }

    #[test]
    fn unknown_news_kind_is_rejected() {
        let err = parse(Bytes::from_static(b"O:Q:weird")).unwrap_err();
        assert!(matches!(err, ParseError::InvalidField { field: "news kind", .. }));
    }
}
