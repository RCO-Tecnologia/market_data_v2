//! Shared types for the fanout layer.

use bytes::Bytes;
use core::fmt;

/// What kind of payload the publisher is shipping. The channel name fragment
/// (`quote`/`book`/`trade`) and the Redis hash key both derive from this.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Kind {
    Quote,
    Book,
    Trade,
}

impl Kind {
    /// Channel suffix used in `market.<kind>.<ticker>` NATS subjects and
    /// `market:<kind>:<ticker>` Redis keys.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Quote => "quote",
            Self::Book => "book",
            Self::Trade => "trade",
        }
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Key for the coalescer + publisher: `(kind, ticker)` pair.
///
/// `Bytes` is used for the ticker so we share allocations with the parser
/// frames upstream — no copying when routing updates through fanout.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PublishKey {
    pub kind: Kind,
    pub ticker: Bytes,
}

impl PublishKey {
    pub const fn new(kind: Kind, ticker: Bytes) -> Self {
        Self { kind, ticker }
    }

    /// Renders the canonical NATS subject for this key.
    /// Example: `market.quote.PETR4`.
    pub fn nats_subject(&self) -> String {
        let mut out = String::with_capacity(8 + self.ticker.len() + self.kind.as_str().len());
        out.push_str("market.");
        out.push_str(self.kind.as_str());
        out.push('.');
        // Tickers are pure ASCII per Cedro's spec; UTF-8 conversion is
        // safe but we fall back to a lossy decode in the unlikely case
        // of unexpected bytes.
        match core::str::from_utf8(&self.ticker) {
            Ok(s) => out.push_str(s),
            Err(_) => out.push_str(&String::from_utf8_lossy(&self.ticker)),
        }
        out
    }

    /// Renders the canonical Redis key for this snapshot.
    /// Example: `market:quote:PETR4`.
    pub fn redis_key(&self) -> String {
        let mut out = String::with_capacity(8 + self.ticker.len() + self.kind.as_str().len());
        out.push_str("market:");
        out.push_str(self.kind.as_str());
        out.push(':');
        match core::str::from_utf8(&self.ticker) {
            Ok(s) => out.push_str(s),
            Err(_) => out.push_str(&String::from_utf8_lossy(&self.ticker)),
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nats_subject_uses_dot_separators() {
        let k = PublishKey::new(Kind::Quote, Bytes::from_static(b"PETR4"));
        assert_eq!(k.nats_subject(), "market.quote.PETR4");
    }

    #[test]
    fn redis_key_uses_colon_separators() {
        let k = PublishKey::new(Kind::Book, Bytes::from_static(b"PETR4"));
        assert_eq!(k.redis_key(), "market:book:PETR4");
    }

    #[test]
    fn kind_strings_are_lowercase() {
        assert_eq!(Kind::Quote.as_str(), "quote");
        assert_eq!(Kind::Book.as_str(), "book");
        assert_eq!(Kind::Trade.as_str(), "trade");
    }
}
