//! Strongly-typed protocol messages produced by the parsers.
//!
//! Every variant of [`ProtocolMessage`] corresponds to one functional header
//! defined by Cedro (see `api.md §3.2` and `mqc.md`). Downstream layers match
//! on this enum and dispatch to the appropriate engine.

use crate::enums::{Side, TradeAggressor, TradeCondition};
use bytes::Bytes;

/// All possible top-level messages parsed from a Cedro frame.
///
/// The `Bytes` fields share the original socket buffer (zero-copy slices) so
/// dispatching messages between threads is cheap.
#[derive(Debug, Clone)]
pub enum ProtocolMessage {
    /// Quote update — `T:<ticker>:<time>:<idx>:<val>:...!`
    Quote {
        ticker: Bytes,
        /// HHMMSS at the time of the update.
        time_hhmmss: u32,
        /// Sparse diff: only the indices that changed.
        diff: QuoteDiff,
    },
    /// Detailed book operation — `B:<ticker>:...`
    Book { ticker: Bytes, op: BookOp },
    /// Aggregated book operation — `Z:<ticker>:...`
    AggBook { ticker: Bytes, op: AggBookOp },
    /// Trade update — `V:<ticker>:...`
    Trade { ticker: Bytes, op: TradeOperation },
    /// News message — `O:...`
    News(NewsMessage),
    /// Volume at price — `VAP:...`
    Vap(VapEntry),
    /// Server time — `GTC:YYYYMMDDHHMMSS`
    ServerTime(ServerTime),
    /// MQC catalog item or terminator — `C:<MARKET>:<ticker>` / `C:<MARKET>:E`
    MqcItem(MqcItem),
    /// GPN broker entry — `G:<exchange>:<code>:<name>:<cedro_id>:<active>`
    GpnItem(GpnItem),
    /// Protocol-level error — `E:<code>[:<context>...]`
    Error(CedroError),
}

// ─── Quote ────────────────────────────────────────────────────────────────

/// Newtype around the Cedro quote index (0..=215).
///
/// We keep this opaque so callers can't accidentally pass an arbitrary `u16`
/// where a quote index is expected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QuoteFieldId(pub u16);

/// A heterogeneous quote field value.
///
/// Parsing keeps the cheapest representation that preserves precision:
/// integers stay `i64`/`u64`, floats stay `f64`, strings stay as zero-copy
/// `Bytes`. Single-character flags use `u8`.
#[derive(Debug, Clone, PartialEq)]
pub enum QuoteFieldValue {
    Float(f64),
    Int(i64),
    /// HHMMSS or `HHMMSSmmm` — preserved as the raw integer.
    Time(u32),
    /// YYYYMMDD as integer.
    Date(u32),
    /// YYYYMMDDHHMMSS as integer.
    DateTime(u64),
    /// Single character (flag, status code).
    Char(u8),
    /// Two-character phase code.
    Phase([u8; 2]),
    /// Free-form string (ticker description, classification name, etc).
    Str(Bytes),
}

/// Sparse list of `(field_id, value)` pairs.
///
/// The first message after a `SQT` subscribe carries the full snapshot;
/// subsequent messages only carry the changed indices.
pub type QuoteDiff = Vec<(QuoteFieldId, QuoteFieldValue)>;

// ─── Book (BQT, detailed) ─────────────────────────────────────────────────

/// Single operation on the detailed order book. See `api.md §5.1`.
#[derive(Debug, Clone, PartialEq)]
pub enum BookOp {
    /// `A:<pos>:<side>:<price>:<qty>:<broker>:<DDMMHHMM>:<order_id>:<offer_type>`
    Add(BookEntry),
    /// `U:<new_pos>:<old_pos>:<side>:<price>:<qty>:<broker>:<DDMMHHMM>:<order_id>:<offer_type>`
    Update { old_pos: u32, entry: BookEntry },
    /// `D:<delete_kind>:[<side>:<pos>]` — see [`BookDelete`].
    Delete(BookDelete),
    /// `E` — end of initial snapshot; subsequent messages are deltas.
    EndOfInitial,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BookEntry {
    pub position: u32,
    pub side: Side,
    pub price: f64,
    pub quantity: u64,
    /// Broker code (Cedro internal id — joinable with `GpnItem.cedro_id`).
    pub broker_id: u32,
    /// Raw DDMMHHMM stamp as the server emitted it. Note: NO year, NO seconds.
    pub timestamp_ddmmhhmm: u32,
    /// Unique-ish order identifier (broker + instrument + side scope).
    ///
    /// `None` when the feed only sends the legacy 7-field BQT layout (no
    /// trailing `OrderID:offer_type`); some markets / older firmware do this.
    pub order_id: Option<Bytes>,
    /// `L` = Limit order, `O` = Opening-price order. `None` paired with
    /// `order_id: None` in the legacy layout.
    pub offer_type: Option<u8>,
}

/// Variants of the `D` message in BQT. See `api.md §5.1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookDelete {
    /// `D:1:<side>:<pos>` — delete a single entry.
    Single { side: Side, position: u32 },
    /// `D:2:<side>:<pos>` — delete entries from index 0..=position.
    PrefixInclusive { side: Side, position: u32 },
    /// `D:3` — clear the entire book (both sides). No side/position payload.
    ClearAll,
}

// ─── Aggregated Book (SAB) ────────────────────────────────────────────────

/// Operation on the aggregated (price-level) book. See `api.md §5.3`.
#[derive(Debug, Clone, PartialEq)]
pub enum AggBookOp {
    Add(AggBookLevel),
    Update(AggBookLevel),
    /// Only types `1` (single position) and `3` (clear all) are valid for SAB.
    Delete(BookDelete),
    EndOfInitial,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggBookLevel {
    pub position: u32,
    pub side: Side,
    pub price: f64,
    pub quantity: u64,
    /// Number of individual offers aggregated into this level.
    pub offer_count: u32,
    pub timestamp_ddmmhhmm: u32,
}

// ─── Trades (GQT) ─────────────────────────────────────────────────────────

/// Top-level GQT operation — add, delete-one, or remove-all.
#[derive(Debug, Clone, PartialEq)]
pub enum TradeOperation {
    Add(Trade),
    /// `D:<trade_id>` — remove a specific trade (e.g., bust correction).
    Delete { trade_id: Bytes },
    /// `R` — remove every trade for this ticker.
    RemoveAll,
    /// `E` — end of subscribe snapshot.
    EndOfSubscribe,
    /// `E:<request_id>` — end of one-shot snapshot. Carries the request id.
    EndOfSnapshot { request_id: Bytes },
}

/// A single trade. Covers both subscribe and snapshot variants:
/// the optional `request_id` field is present only in snapshot mode.
#[derive(Debug, Clone, PartialEq)]
pub struct Trade {
    pub operation_code: u8, // 'A' typically — kept for downstream diagnostics.
    pub time_hhmmss: u32,
    pub price: f64,
    pub broker_buy_id: u32,
    pub broker_sell_id: u32,
    pub quantity: u64,
    pub trade_id: Bytes,
    /// Only present on snapshot (`N`) responses.
    pub request_id: Option<Bytes>,
    pub condition: TradeCondition,
    pub aggressor: TradeAggressor,
    /// Raw original-condition flags (space-separated; see `api.md §6.1.7`).
    /// Kept as bytes because the list of valid flags is open-ended.
    pub original_conditions: Bytes,
}

// ─── News (NEM) ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NewsMessage {
    /// `A:<agency>:<code>:<date>:<time>:<category>:<title_len>:<title>`
    Headline(NewsHeadline),
    /// `L:<request_id>:<agency>:<code>:<date>:<time>:<category>:<title_len>:<title>`
    HistoryItem {
        request_id: Bytes,
        headline: NewsHeadline,
    },
    /// `L:<request_id>:END` — end of history list.
    HistoryEnd { request_id: Bytes },
    /// `N:<request_id>:<agency>:<code>:<body>` — note: line breaks in the body
    /// are encoded as ASCII 0x03 (ETX) and MUST be translated back to `\n`
    /// before display. See `api.md §7.2.3`.
    Body {
        request_id: Bytes,
        agency: Bytes,
        code: Bytes,
        body_raw: Bytes,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewsHeadline {
    pub agency: Bytes,
    pub code: Bytes,
    /// YYYYMMDD.
    pub date: u32,
    /// HHMMSS.
    pub time: u32,
    pub category: u32,
    pub title: Bytes,
}

// ─── VAP (Volume at Price) ────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct VapEntry {
    pub ticker: Bytes,
    pub price: f64,
    pub buyer_trades: f64,
    pub buyer_volume: f64,
    pub seller_trades: f64,
    pub seller_volume: f64,
    pub direct_trades: f64,
    pub direct_volume: f64,
    pub undefined_trades: f64,
    pub undefined_volume: f64,
    /// Period in minutes; `None` for the no-period variant.
    pub period_minutes: Option<u32>,
    pub rlp_trades: f64,
    pub rlp_volume: f64,
    pub auction_trades: f64,
    pub auction_volume: f64,
}

// ─── Server time (GTC) ────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ServerTime {
    /// YYYYMMDD.
    pub date: u32,
    /// HHMMSS.
    pub time: u32,
}

// ─── MQC (catalog discovery) ──────────────────────────────────────────────

/// One catalog entry returned by `MQC`. See `mqc.md §1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqcItem {
    /// `C:<MARKET>:<TICKER>` (possibly with extra fields after position 2).
    Symbol { market: Bytes, ticker: Bytes },
    /// `C:<MARKET>:E` — end-of-list sentinel.
    End { market: Bytes },
}

// ─── GPN (broker dictionary) ──────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GpnItem {
    pub exchange: Bytes,
    /// Exchange-assigned broker code (e.g., "120" for Clear).
    pub code: Bytes,
    /// Short name / fantasy name.
    pub name: Bytes,
    /// Internal Cedro identifier. This is the one referenced from BQT/GQT/SQT.
    pub cedro_id: Bytes,
    pub active: bool,
}

// ─── Errors ───────────────────────────────────────────────────────────────

/// Wire-level protocol error frame — `E:<code>[:<context...>]`.
///
/// Not to be confused with [`crate::parser::ParseError`], which represents
/// our parser failing on a malformed frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CedroError {
    pub code: u16,
    /// Remaining colon-separated context tokens, in original order.
    pub context: Vec<Bytes>,
}

impl CedroError {
    /// `true` for codes that fall under "client bug" (1, 4, 5, 10, 13, 14).
    /// See `api.md §10.1`.
    #[must_use]
    pub const fn is_client_bug(&self) -> bool {
        matches!(self.code, 1 | 4 | 5 | 10 | 13 | 14)
    }

    /// `true` for codes that indicate transient server issues (11, 15).
    #[must_use]
    pub const fn is_transient_server(&self) -> bool {
        matches!(self.code, 11 | 15)
    }

    /// `true` for the server-migration code (12) — caller should reconnect on
    /// the new host carried in [`Self::context`].
    #[must_use]
    pub const fn is_migration(&self) -> bool {
        self.code == 12
    }

    /// `true` for duplicate-connection / lost-access codes (6, 7, 8, 9).
    /// **Do not** reconnect automatically in this case.
    #[must_use]
    pub const fn is_forced_disconnect(&self) -> bool {
        matches!(self.code, 6..=9)
    }
}
