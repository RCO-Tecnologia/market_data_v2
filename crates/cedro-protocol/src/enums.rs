//! Enumerated values used by the Cedro Crystal protocol.
//!
//! Every variant maps to a literal code from the official spec. See `api.md`
//! sections 4.1.2–4.1.6, 6.1.5, 6.1.6, and 6.1.7. Unknown codes are surfaced
//! via the `Unknown(_)` variants so the pipeline degrades gracefully rather
//! than crashing on protocol evolution.

use core::fmt;

/// Market identifier — quote index 44. See `api.md §4.1.2`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum MarketCode {
    Bovespa,
    DowJones,
    Bmf,
    Indices,
    Money,
    Forex,
    Indicators,
    Nyse,
    Nasdaq,
    Cfd,
    Bitcoin,
    Datagro,
    Inews,
    Amex,
    TesouroDireto,
    NyseFmv,
    NasdaqFmv,
    AmexFmv,
    Unknown(u16),
}

impl MarketCode {
    #[must_use]
    pub const fn from_code(code: u16) -> Self {
        match code {
            1 => Self::Bovespa,
            2 => Self::DowJones,
            3 => Self::Bmf,
            4 => Self::Indices,
            5 => Self::Money,
            7 => Self::Forex,
            8 => Self::Indicators,
            10 => Self::Nyse,
            12 => Self::Nasdaq,
            13 => Self::Cfd,
            30 => Self::Bitcoin,
            44 => Self::Datagro,
            45 => Self::Inews,
            52 => Self::Amex,
            64 => Self::TesouroDireto,
            76 => Self::NyseFmv,
            77 => Self::NasdaqFmv,
            78 => Self::AmexFmv,
            other => Self::Unknown(other),
        }
    }
}

/// Asset type — quote index 45. See `api.md §4.1.3`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AssetType {
    Spot,
    Option,
    Index,
    Commodity,
    Currency,
    Term,
    Future,
    Auction,
    Bond,
    Fractional,
    OptionExercise,
    Indicator,
    Etf,
    Volume,
    OptionOnSpot,
    OptionOnFuture,
    Test,
    Strategy,
    Corp,
    Secloan,
    TesouroDireto,
    Unknown(u16),
}

impl AssetType {
    #[must_use]
    pub const fn from_code(code: u16) -> Self {
        match code {
            1 => Self::Spot,
            2 => Self::Option,
            3 => Self::Index,
            4 => Self::Commodity,
            5 => Self::Currency,
            6 => Self::Term,
            7 => Self::Future,
            8 => Self::Auction,
            9 => Self::Bond,
            10 => Self::Fractional,
            11 => Self::OptionExercise,
            12 => Self::Indicator,
            13 => Self::Etf,
            15 => Self::Volume,
            16 => Self::OptionOnSpot,
            17 => Self::OptionOnFuture,
            18 => Self::Test,
            19 => Self::Strategy,
            20 => Self::Corp,
            21 => Self::Secloan,
            22 => Self::TesouroDireto,
            other => Self::Unknown(other),
        }
    }
}

/// Instrument trading status — quote index 67. See `api.md §4.1.4`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum InstrumentStatus {
    Normal,
    Auction,
    Suspended,
    Frozen,
    Empty,
    Unknown(i16),
}

impl InstrumentStatus {
    #[must_use]
    pub const fn from_code(code: i16) -> Self {
        match code {
            101 => Self::Normal,
            102 => Self::Auction,
            105 => Self::Suspended,
            118 => Self::Frozen,
            -1 => Self::Empty,
            other => Self::Unknown(other),
        }
    }
}

/// Asset status — quote index 84. See `api.md §4.1.5`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AssetStatus {
    Normal,
    Frozen,
    Suspended,
    Auction,
    Inhibited,
    Unknown(u16),
}

impl AssetStatus {
    #[must_use]
    pub const fn from_code(code: u16) -> Self {
        match code {
            0 => Self::Normal,
            1 => Self::Frozen,
            2 => Self::Suspended,
            3 => Self::Auction,
            4 => Self::Inhibited,
            other => Self::Unknown(other),
        }
    }
}

/// Trading session phase for an asset group — quote index 88. See `api.md §4.1.6`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum AssetGroupPhase {
    PreOpen,
    Open,
    PreClose,
    Close,
    PreAfterOpen,
    AfterOpen,
    AfterClose,
    Final,
    Closed,
    Paused,
    Unknown([u8; 2]),
}

impl AssetGroupPhase {
    /// Parse a 1- or 2-byte phase code (ASCII, uppercase) into the typed enum.
    #[must_use]
    pub fn from_code(code: &[u8]) -> Self {
        match code {
            b"P" => Self::PreOpen,
            b"A" => Self::Open,
            b"PN" => Self::PreClose,
            b"N" => Self::Close,
            b"E" => Self::PreAfterOpen,
            b"R" => Self::AfterOpen,
            b"NE" => Self::AfterClose,
            b"F" => Self::Final,
            b"NO" => Self::Closed,
            b"T" => Self::Paused,
            other => {
                let mut buf = [0u8; 2];
                let n = other.len().min(2);
                buf[..n].copy_from_slice(&other[..n]);
                Self::Unknown(buf)
            }
        }
    }
}

/// Option style (American / European) — quote index 72.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum OptionStyle {
    American,
    European,
    None,
    Unknown(u8),
}

impl OptionStyle {
    #[must_use]
    pub const fn from_byte(b: u8) -> Self {
        match b {
            b'A' => Self::American,
            b'E' => Self::European,
            b'0' => Self::None,
            other => Self::Unknown(other),
        }
    }
}

/// Option direction (Put / Call) — quote index 74.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum OptionDirection {
    Put,
    Call,
    Unknown(u8),
}

impl OptionDirection {
    #[must_use]
    pub const fn from_byte(b: u8) -> Self {
        match b {
            b'P' => Self::Put,
            b'C' => Self::Call,
            other => Self::Unknown(other),
        }
    }
}

/// Aggressor side of a trade — see `api.md §6.1.6`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TradeAggressor {
    /// `I` — undefined / unknown.
    Undefined,
    /// `A` — buyer was the aggressor.
    Buyer,
    /// `V` — seller was the aggressor.
    Seller,
    Unknown(u8),
}

impl TradeAggressor {
    #[must_use]
    pub const fn from_byte(b: u8) -> Self {
        match b {
            b'I' => Self::Undefined,
            b'A' => Self::Buyer,
            b'V' => Self::Seller,
            other => Self::Unknown(other),
        }
    }
}

/// Trade condition — `api.md §6.1.5`. Integer-coded, single-valued.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum TradeCondition {
    NotDirect,
    Direct,
    Rlp,
    Rfq,
    MidpointTrade,
    OpeningPrice,
    PointInTimeAuction,
    Unknown(u16),
}

impl TradeCondition {
    #[must_use]
    pub const fn from_code(code: u16) -> Self {
        match code {
            0 => Self::NotDirect,
            1 => Self::Direct,
            2 => Self::Rlp,
            3 => Self::Rfq,
            4 => Self::MidpointTrade,
            5 => Self::OpeningPrice,
            6 => Self::PointInTimeAuction,
            other => Self::Unknown(other),
        }
    }
}

/// Side of an order book entry — `A` (buy/bid) or `V` (sell/ask).
///
/// The Cedro spec calls the buy side `A` (Ask in their nomenclature, but
/// semantically it is the bid). We preserve the raw byte so callers can
/// disambiguate if needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Side {
    /// `A` — buy side (bid).
    Buy,
    /// `V` — sell side (ask).
    Sell,
}

impl Side {
    /// Parse a Cedro side byte (`A` or `V`).
    pub const fn from_byte(b: u8) -> Option<Self> {
        match b {
            b'A' => Some(Self::Buy),
            b'V' => Some(Self::Sell),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_byte(self) -> u8 {
        match self {
            Self::Buy => b'A',
            Self::Sell => b'V',
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_code_known_and_unknown() {
        assert_eq!(MarketCode::from_code(1), MarketCode::Bovespa);
        assert_eq!(MarketCode::from_code(64), MarketCode::TesouroDireto);
        assert_eq!(MarketCode::from_code(999), MarketCode::Unknown(999));
    }

    #[test]
    fn asset_type_full_table() {
        assert_eq!(AssetType::from_code(2), AssetType::Option);
        assert_eq!(AssetType::from_code(7), AssetType::Future);
        assert_eq!(AssetType::from_code(22), AssetType::TesouroDireto);
        // Index 14 is not listed in the spec — must degrade to Unknown.
        assert_eq!(AssetType::from_code(14), AssetType::Unknown(14));
    }

    #[test]
    fn phase_one_and_two_byte_codes() {
        assert_eq!(AssetGroupPhase::from_code(b"A"), AssetGroupPhase::Open);
        assert_eq!(AssetGroupPhase::from_code(b"PN"), AssetGroupPhase::PreClose);
        assert_eq!(AssetGroupPhase::from_code(b"NO"), AssetGroupPhase::Closed);
        assert!(matches!(
            AssetGroupPhase::from_code(b"ZZ"),
            AssetGroupPhase::Unknown(_),
        ));
    }

    #[test]
    fn side_roundtrip() {
        assert_eq!(Side::from_byte(b'A'), Some(Side::Buy));
        assert_eq!(Side::from_byte(b'V'), Some(Side::Sell));
        assert_eq!(Side::from_byte(b'X'), None);
        assert_eq!(Side::Buy.as_byte(), b'A');
    }

    #[test]
    fn aggressor_codes() {
        assert_eq!(TradeAggressor::from_byte(b'A'), TradeAggressor::Buyer);
        assert_eq!(TradeAggressor::from_byte(b'V'), TradeAggressor::Seller);
        assert_eq!(TradeAggressor::from_byte(b'I'), TradeAggressor::Undefined);
    }
}
