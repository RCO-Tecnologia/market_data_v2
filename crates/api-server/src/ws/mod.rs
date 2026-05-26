//! WebSocket surface — `/ws` long-lived multi-subscribe endpoint.
//!
//! v1 ships the *protocol* (client/server message shapes) along with helpers
//! for `SubscribeChannel`. The handler that bridges NATS subscriptions onto
//! a `WebSocket` lives behind feature `ws_handler` and will land in the
//! next iteration once the engine integration is in place — see task #11.

pub mod protocol;

pub use protocol::{ClientMessage, ServerMessage, SubscribeChannel};
