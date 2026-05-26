//! Cedro Crystal socket protocol — pure parsing layer.
//!
//! This crate contains *only* zero-I/O parsing logic. It takes raw frames
//! (delimited `\n` / `!` byte slices) and produces strongly-typed
//! [`ProtocolMessage`] values that downstream layers consume.
//!
//! The crate is structured so it is testable without a network connection:
//! every parser is a pure function over `&[u8]`, with fixtures lifted directly
//! from the official documentation ([`api.md`][api] and [`mqc.md`][mqc]).
//!
//! See `ARCHITECTURE.md §5.4` for how this fits into the runtime pipeline.
//!
//! [api]: https://example.invalid/api.md
//! [mqc]: https://example.invalid/mqc.md

#![cfg_attr(not(test), warn(clippy::print_stdout, clippy::print_stderr))]

pub mod enums;
pub mod frame;
pub mod parser;
pub mod types;

pub use enums::{
    AssetGroupPhase, AssetStatus, AssetType, InstrumentStatus, MarketCode, OptionDirection,
    OptionStyle, Side, TradeAggressor, TradeCondition,
};
pub use frame::{FrameKind, FrameSplitter};
pub use parser::{ParseError, parse_frame};
pub use types::{
    BookOp, CedroError, GpnItem, MqcItem, ProtocolMessage, QuoteDiff, QuoteFieldId,
    QuoteFieldValue, ServerTime, Trade, TradeOperation,
};
