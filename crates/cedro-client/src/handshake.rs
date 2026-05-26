//! Implementation of the Cedro Crystal handshake (`api.md §2`).
//!
//! The handshake is the only place where the client interleaves reads and
//! writes on the same connection. Once `You are connected` arrives the
//! socket is fully streaming and ownership is handed to the [`crate::reader`]
//! module.
//!
//! Sequence (server → / client →):
//!
//! ```text
//! server → "Welcome to Cedro Crystal"
//! server → "Username:"  (after Software Key is sent)
//! client → <Software Key>\r\n   (empty line if not contracted)
//! client → <Username>\r\n
//! server → "Password:"
//! client → <Password>\r\n
//! server → "You are connected"
//! ```
//!
//! In practice prompts can arrive in slightly different orders or with extra
//! whitespace; the implementation matches by substring rather than position.

use crate::config::ConnectionConfig;
use crate::error::{ClientError, HandshakeStep};
use std::io::{Read, Write};
use std::time::{Duration, Instant};

/// Performs the handshake against an already-connected stream.
///
/// On success the stream is positioned right after `You are connected` and
/// ready to consume the binary streaming protocol.
pub fn perform<S: Read + Write>(
    stream: &mut S,
    config: &ConnectionConfig,
) -> Result<(), ClientError> {
    let deadline = Instant::now() + config.handshake_timeout;
    // The buffer is **persistent across steps**: a single `read()` may
    // coalesce several Cedro prompts and we must not drop the surplus.
    let mut hs = HandshakeBuf::with_stream(stream);

    hs.consume_until(b"Welcome to Cedro", deadline, HandshakeStep::AwaitWelcome)?;
    hs.write_line(config.software_key.as_bytes(), HandshakeStep::SendSoftwareKey)?;

    hs.consume_until(b"Username:", deadline, HandshakeStep::AwaitUsernamePrompt)?;
    hs.write_line(config.username.as_bytes(), HandshakeStep::SendUsername)?;

    hs.consume_until(b"Password:", deadline, HandshakeStep::AwaitPasswordPrompt)?;
    hs.write_line(config.password.as_bytes(), HandshakeStep::SendPassword)?;

    hs.consume_until(b"You are connected", deadline, HandshakeStep::AwaitConnected)?;

    Ok(())
}

/// Buffered helper that survives across handshake steps. Without persistence
/// of the unread bytes we'd lose any prompt the kernel happens to coalesce
/// into the same `read()` as the previous one.
struct HandshakeBuf<'s, S> {
    stream: &'s mut S,
    buf: Vec<u8>,
}

impl<'s, S: Read + Write> HandshakeBuf<'s, S> {
    fn with_stream(stream: &'s mut S) -> Self {
        Self {
            stream,
            buf: Vec::with_capacity(256),
        }
    }

    /// Reads until `needle` appears in the buffer, then discards everything
    /// up to and including the needle. Surplus stays buffered for the next
    /// step.
    fn consume_until(
        &mut self,
        needle: &[u8],
        deadline: Instant,
        step: HandshakeStep,
    ) -> Result<(), ClientError> {
        let mut chunk = [0u8; 256];
        loop {
            if let Some(pos) = memchr::memmem::find(&self.buf, needle) {
                self.buf.drain(..pos + needle.len());
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(ClientError::HandshakeTimeout {
                    step,
                    waited: Duration::from_secs(0),
                });
            }
            let n = self.stream.read(&mut chunk).map_err(|e| ClientError::Handshake {
                step,
                reason: handshake_reason(&e),
            })?;
            if n == 0 {
                return Err(ClientError::UnexpectedEof);
            }
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }

    fn write_line(&mut self, payload: &[u8], step: HandshakeStep) -> Result<(), ClientError> {
        let mut out = Vec::with_capacity(payload.len() + 2);
        out.extend_from_slice(payload);
        out.extend_from_slice(b"\r\n");
        self.stream
            .write_all(&out)
            .map_err(|e| ClientError::Handshake {
                step,
                reason: handshake_reason(&e),
            })?;
        self.stream.flush().map_err(ClientError::CommandWrite)?;
        Ok(())
    }
}

fn handshake_reason(err: &std::io::Error) -> &'static str {
    match err.kind() {
        std::io::ErrorKind::TimedOut => "read/write timed out",
        std::io::ErrorKind::WouldBlock => "non-blocking I/O reported WouldBlock",
        std::io::ErrorKind::ConnectionReset => "connection reset by peer",
        std::io::ErrorKind::ConnectionAborted => "connection aborted by peer",
        std::io::ErrorKind::UnexpectedEof => "unexpected EOF",
        _ => "I/O failure (see source)",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, ErrorKind};

    /// Wraps a reader of canned server output and a writer that captures
    /// what the client wrote, so we can assert both directions of the
    /// handshake in one shot.
    struct ScriptedConn {
        to_read: Cursor<Vec<u8>>,
        written: Vec<u8>,
    }

    impl ScriptedConn {
        fn new(server_says: &[u8]) -> Self {
            Self {
                to_read: Cursor::new(server_says.to_vec()),
                written: Vec::new(),
            }
        }
    }

    impl Read for ScriptedConn {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let n = self.to_read.read(buf)?;
            if n == 0 {
                // Surface EOF as a recognisable error rather than 0 bytes
                // so the loop in `read_until_contains` exits cleanly.
                return Err(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "no more scripted data",
                ));
            }
            Ok(n)
        }
    }

    impl Write for ScriptedConn {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.written.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn make_cfg() -> ConnectionConfig {
        let mut cfg = ConnectionConfig::new("127.0.0.1", "alice", "s3cr3t");
        cfg.software_key = "KEY-42".into();
        cfg.handshake_timeout = Duration::from_secs(1);
        cfg
    }

    #[test]
    fn happy_path_writes_credentials_in_order() {
        // Server emits each prompt only after it has "received" the prior line.
        // Concatenated here because our scripted reader is non-interactive.
        let server = b"Welcome to Cedro Crystal\r\nUsername:Password:You are connected\r\n";
        let mut conn = ScriptedConn::new(server);
        let cfg = make_cfg();

        perform(&mut conn, &cfg).unwrap();

        // Verify the client wrote: key\r\n, username\r\n, password\r\n
        let written = String::from_utf8(conn.written.clone()).unwrap();
        assert!(written.starts_with("KEY-42\r\n"), "wrote: {written:?}");
        assert!(written.contains("alice\r\n"));
        assert!(written.ends_with("s3cr3t\r\n"));
    }

    #[test]
    fn empty_software_key_is_allowed() {
        let server = b"Welcome to Cedro Crystal\r\nUsername:Password:You are connected\r\n";
        let mut conn = ScriptedConn::new(server);
        let mut cfg = make_cfg();
        cfg.software_key.clear();

        perform(&mut conn, &cfg).unwrap();

        // The first thing the client sends is just CRLF.
        let written = conn.written.clone();
        assert_eq!(&written[..2], b"\r\n");
    }

    #[test]
    fn missing_welcome_surfaces_handshake_error() {
        let server = b""; // server says nothing — EOF immediately.
        let mut conn = ScriptedConn::new(server);
        let cfg = make_cfg();

        let err = perform(&mut conn, &cfg).unwrap_err();
        assert!(matches!(
            err,
            ClientError::Handshake { step: HandshakeStep::AwaitWelcome, .. }
        ));
    }
}
