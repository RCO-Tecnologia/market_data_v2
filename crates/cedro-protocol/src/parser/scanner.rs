// Some helpers below are unused until BQT/SAB/GQT/NEM/VAP land — keep them
// available so each follow-up parser is a one-line `scanner.required_u64(...)`.
#![allow(dead_code)]

//! Tiny zero-copy tokenizer for colon-separated payloads.
//!
//! Cedro frames are flat lists of `:`-separated fields. We could use
//! `bytes::Bytes::split` or `slice::split` everywhere, but a single iterator
//! that returns zero-copy [`Bytes`] slices keeps the parsers ergonomic.
//!
//! Note that *some* fields can legally contain `:` (e.g., news titles,
//! free-form descriptions). Each parser handles that case explicitly by
//! taking the remainder of the buffer rather than calling [`Scanner::next`].

use crate::parser::ParseError;
use bytes::Bytes;

/// Iterator over `:`-separated fields in a frame.
///
/// The scanner holds the original `Bytes` and yields zero-copy slices into it.
#[derive(Debug, Clone)]
pub(super) struct Scanner {
    buf: Bytes,
    /// Index of the next byte to consume.
    pos: usize,
    /// 1-based index of the next token (used in error messages).
    field_index: usize,
}

impl Scanner {
    pub(super) const fn new(buf: Bytes) -> Self {
        Self {
            buf,
            pos: 0,
            field_index: 0,
        }
    }

    /// Return the next `:`-delimited token, or `None` if exhausted.
    pub(super) fn next(&mut self) -> Option<Bytes> {
        if self.pos > self.buf.len() {
            return None;
        }
        if self.pos == self.buf.len() {
            // Emit one final empty token then stop, so trailing `:` becomes
            // an empty field rather than silently disappearing.
            self.pos = self.buf.len() + 1;
            self.field_index += 1;
            return Some(Bytes::new());
        }
        self.field_index += 1;
        let rest = &self.buf[self.pos..];
        if let Some(rel) = memchr::memchr(b':', rest) {
            let token = self.buf.slice(self.pos..self.pos + rel);
            self.pos += rel + 1;
            Some(token)
        } else {
            let token = self.buf.slice(self.pos..);
            self.pos = self.buf.len() + 1;
            Some(token)
        }
    }

    /// Number of fields consumed so far.
    pub(super) const fn fields_consumed(&self) -> usize {
        self.field_index
    }

    /// Take a required string field, surfacing a structured error if missing.
    pub(super) fn required(&mut self, expected: &'static str) -> Result<Bytes, ParseError> {
        self.next().ok_or(ParseError::MissingField {
            position: self.field_index + 1,
            expected,
        })
    }

    /// Take a required field and parse it as a `u64`.
    pub(super) fn required_u64(&mut self, field: &'static str) -> Result<u64, ParseError> {
        let bytes = self.required(field)?;
        parse_u64(&bytes, field)
    }

    /// Take a required field and parse it as a `u32`.
    pub(super) fn required_u32(&mut self, field: &'static str) -> Result<u32, ParseError> {
        let bytes = self.required(field)?;
        parse_u32(&bytes, field)
    }

    /// Take a required field and parse it as an `f64`.
    pub(super) fn required_f64(&mut self, field: &'static str) -> Result<f64, ParseError> {
        let bytes = self.required(field)?;
        parse_f64(&bytes, field)
    }

    /// Return all bytes from the current position to the end, untouched.
    ///
    /// Used by parsers where the trailing field can itself contain `:`
    /// (news titles, error context).
    pub(super) fn rest(self) -> Bytes {
        if self.pos > self.buf.len() {
            Bytes::new()
        } else {
            self.buf.slice(self.pos..)
        }
    }

    /// Like [`Self::rest`] but borrows the scanner so callers can keep using
    /// it for diagnostics. Advances the scanner past every remaining byte.
    pub(super) fn take_rest(&mut self) -> Bytes {
        let out = if self.pos > self.buf.len() {
            Bytes::new()
        } else {
            self.buf.slice(self.pos..)
        };
        self.pos = self.buf.len() + 1;
        out
    }
}

pub(super) fn parse_u64(b: &[u8], field: &'static str) -> Result<u64, ParseError> {
    if b.is_empty() {
        return Err(ParseError::BadInteger { field });
    }
    let mut n: u64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            return Err(ParseError::BadInteger { field });
        }
        n = n
            .checked_mul(10)
            .and_then(|v| v.checked_add(u64::from(c - b'0')))
            .ok_or(ParseError::BadInteger { field })?;
    }
    Ok(n)
}

pub(super) fn parse_u32(b: &[u8], field: &'static str) -> Result<u32, ParseError> {
    let n = parse_u64(b, field)?;
    u32::try_from(n).map_err(|_| ParseError::BadInteger { field })
}

pub(super) fn parse_u16(b: &[u8], field: &'static str) -> Result<u16, ParseError> {
    let n = parse_u64(b, field)?;
    u16::try_from(n).map_err(|_| ParseError::BadInteger { field })
}

pub(super) fn parse_i64(b: &[u8], field: &'static str) -> Result<i64, ParseError> {
    if b.is_empty() {
        return Err(ParseError::BadInteger { field });
    }
    let (sign, rest) = if b[0] == b'-' {
        (-1_i64, &b[1..])
    } else {
        (1_i64, b)
    };
    if rest.is_empty() {
        return Err(ParseError::BadInteger { field });
    }
    let mut n: i64 = 0;
    for &c in rest {
        if !c.is_ascii_digit() {
            return Err(ParseError::BadInteger { field });
        }
        n = n
            .checked_mul(10)
            .and_then(|v| v.checked_add(i64::from(c - b'0')))
            .ok_or(ParseError::BadInteger { field })?;
    }
    Ok(sign * n)
}

pub(super) fn parse_f64(b: &[u8], field: &'static str) -> Result<f64, ParseError> {
    if b.is_empty() {
        return Err(ParseError::BadFloat { field });
    }
    // We delegate to `core::str::parse` after a quick UTF-8 check. The
    // Cedro spec mandates locale-independent floats (period as decimal
    // separator) which matches Rust's default parser.
    let s = core::str::from_utf8(b).map_err(|_| ParseError::BadEncoding { field })?;
    s.parse::<f64>().map_err(|_| ParseError::BadFloat { field })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yields_tokens_until_exhausted() {
        let mut s = Scanner::new(Bytes::from_static(b"a:bb:ccc"));
        assert_eq!(s.next().unwrap().as_ref(), b"a");
        assert_eq!(s.next().unwrap().as_ref(), b"bb");
        assert_eq!(s.next().unwrap().as_ref(), b"ccc");
        assert!(s.next().is_none());
    }

    #[test]
    fn empty_trailing_field_is_emitted() {
        let mut s = Scanner::new(Bytes::from_static(b"a:b:"));
        assert_eq!(s.next().unwrap().as_ref(), b"a");
        assert_eq!(s.next().unwrap().as_ref(), b"b");
        // Trailing colon → one final empty token. (Important for `MQC` `C:M:E:`-style)
        assert_eq!(s.next().unwrap().as_ref(), b"");
        assert!(s.next().is_none());
    }

    #[test]
    fn rest_returns_unconsumed_tail() {
        let mut s = Scanner::new(Bytes::from_static(b"a:b:title:with:colons"));
        let _ = s.next();
        let _ = s.next();
        assert_eq!(s.rest().as_ref(), b"title:with:colons");
    }

    #[test]
    fn integer_parsing_rejects_garbage() {
        assert!(parse_u64(b"123", "x").is_ok());
        assert!(parse_u64(b"-1", "x").is_err());
        assert!(parse_u64(b"abc", "x").is_err());
        assert!(parse_u64(b"", "x").is_err());
        assert!(parse_i64(b"-42", "x").is_ok());
        assert!(parse_i64(b"-", "x").is_err());
    }

    #[test]
    fn float_parsing_accepts_cedro_format() {
        assert!((parse_f64(b"59.95", "x").unwrap() - 59.95).abs() < 1e-9);
        assert!((parse_f64(b"-1.5", "x").unwrap() + 1.5).abs() < 1e-9);
        assert!(parse_f64(b"abc", "x").is_err());
    }
}
