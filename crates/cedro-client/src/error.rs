//! Errors surfaced by the `cedro-client` crate.

use std::io;
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("handshake failed at step {step}: {reason}")]
    Handshake {
        step: HandshakeStep,
        reason: &'static str,
    },

    #[error("handshake timeout at step {step:?} (waited {waited:?})")]
    HandshakeTimeout {
        step: HandshakeStep,
        waited: Duration,
    },

    #[error("connection closed unexpectedly")]
    UnexpectedEof,

    #[error("reader thread already started")]
    ReaderAlreadyStarted,

    #[error("kernel receive buffer exceeded threshold ({pending_bytes} bytes, {pct}% full)")]
    BufferPressurePanic { pending_bytes: usize, pct: u8 },

    #[error("invalid configuration: {0}")]
    InvalidConfig(&'static str),

    #[error("command write failed: {0}")]
    CommandWrite(io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeStep {
    AwaitWelcome,
    SendSoftwareKey,
    AwaitUsernamePrompt,
    SendUsername,
    AwaitPasswordPrompt,
    SendPassword,
    AwaitConnected,
}

impl core::fmt::Display for HandshakeStep {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let s = match self {
            Self::AwaitWelcome => "awaiting welcome",
            Self::SendSoftwareKey => "sending software key",
            Self::AwaitUsernamePrompt => "awaiting username prompt",
            Self::SendUsername => "sending username",
            Self::AwaitPasswordPrompt => "awaiting password prompt",
            Self::SendPassword => "sending password",
            Self::AwaitConnected => "awaiting 'You are connected'",
        };
        f.write_str(s)
    }
}
