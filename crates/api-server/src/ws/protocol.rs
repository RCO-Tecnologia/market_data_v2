//! Wire protocol for the WS endpoint.
//!
//! Both directions use a tagged enum encoded as JSON or MessagePack; the
//! client picks via the `Accept` header on the upgrade request. The
//! protocol shape mirrors what we documented in `ARCHITECTURE.md §5.7.2`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SubscribeChannel {
    Quote,
    Book,
    Trade,
}

impl SubscribeChannel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Quote => "quote",
            Self::Book => "book",
            Self::Trade => "trade",
        }
    }

    pub const fn nats_prefix(self) -> &'static str {
        match self {
            Self::Quote => "market.quote.",
            Self::Book => "market.book.",
            Self::Trade => "market.trade.",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum ClientMessage {
    Sub { channel: SubscribeChannel, ticker: String },
    Unsub { channel: SubscribeChannel, ticker: String },
    Ping { ts: i64 },
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum ServerMessage {
    Snap {
        channel: SubscribeChannel,
        ticker: String,
        data: Vec<u8>,
    },
    Upd {
        channel: SubscribeChannel,
        ticker: String,
        data: Vec<u8>,
    },
    Pong {
        ts: i64,
    },
    Err {
        code: &'static str,
        message: String,
        ticker: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_subscribe_message() {
        let json = br#"{"op":"sub","channel":"quote","ticker":"PETR4"}"#;
        let msg: ClientMessage = serde_json::from_slice(json).unwrap();
        match msg {
            ClientMessage::Sub { channel, ticker } => {
                assert_eq!(channel, SubscribeChannel::Quote);
                assert_eq!(ticker, "PETR4");
            }
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn parses_ping_with_timestamp() {
        let json = br#"{"op":"ping","ts":1234567890}"#;
        let msg: ClientMessage = serde_json::from_slice(json).unwrap();
        assert!(matches!(msg, ClientMessage::Ping { ts: 1_234_567_890 }));
    }

    #[test]
    fn serializes_snap_message_with_binary_data() {
        let msg = ServerMessage::Snap {
            channel: SubscribeChannel::Quote,
            ticker: "PETR4".into(),
            data: vec![0xde, 0xad, 0xbe, 0xef],
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("\"op\":\"snap\""));
        assert!(json.contains("\"channel\":\"quote\""));
    }

    #[test]
    fn error_serializes_with_optional_ticker() {
        let msg = ServerMessage::Err {
            code: "book_not_available",
            message: "book not available for futures".into(),
            ticker: Some("WDOZ25".into()),
        };
        let json = serde_json::to_value(&msg).unwrap();
        assert_eq!(json["op"], "err");
        assert_eq!(json["ticker"], "WDOZ25");
    }
}
