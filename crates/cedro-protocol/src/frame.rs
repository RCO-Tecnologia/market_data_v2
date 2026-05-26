//! Framing layer — turns a raw byte stream into discrete protocol frames.
//!
//! The Cedro protocol uses two terminators (`api.md §3.1`):
//!
//! - `!` ends every `SQT` quote frame.
//! - `\n` ends everything else (book, trade, news, MQC, GPN, errors, etc).
//!
//! Both characters are 7-bit ASCII and cannot appear inside a frame payload,
//! so locating them is a pure byte scan. We use [`memchr::memchr2`] which
//! dispatches to AVX2 (or NEON on aarch64) at runtime — it sustains ~30 GB/s
//! on commodity CPUs, well above the workload's requirements.

use bytes::{Buf, BytesMut};

/// Which terminator ended the frame. The dispatcher uses this together with
/// the first byte (`T`, `B`, `Z`, etc) to pick the right parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameKind {
    /// Frame terminated by `!` — only `SQT` (`T:...!`) uses this.
    Bang,
    /// Frame terminated by `\n` — everything else.
    Newline,
}

/// Streaming frame splitter that owns a growing read buffer.
///
/// The intended use pattern, matching `ARCHITECTURE.md §5.1`:
///
/// ```ignore
/// let mut splitter = FrameSplitter::with_capacity(8 * 1024 * 1024);
/// loop {
///     splitter.reserve(64 * 1024);
///     let n = socket.read(splitter.bytes_mut())?;
///     splitter.advance_written(n);
///     while let Some((kind, frame)) = splitter.next_frame() {
///         channel.send((kind, frame))?;
///     }
/// }
/// ```
///
/// `next_frame()` returns owned [`bytes::Bytes`] slices that share the
/// underlying allocation — no copying happens in the hot loop.
#[derive(Debug)]
pub struct FrameSplitter {
    buf: BytesMut,
}

impl FrameSplitter {
    #[must_use]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(cap),
        }
    }

    /// Ensures the buffer has at least `additional` bytes of spare capacity
    /// for the next read.
    pub fn reserve(&mut self, additional: usize) {
        self.buf.reserve(additional);
    }

    /// Append bytes received from the socket into the splitter's buffer.
    pub fn extend_from_slice(&mut self, src: &[u8]) {
        self.buf.extend_from_slice(src);
    }

    /// Number of unread bytes currently buffered. Useful for backpressure
    /// instrumentation in tests and as a diagnostic.
    pub fn pending(&self) -> usize {
        self.buf.len()
    }

    /// Try to peel one frame off the front of the buffer.
    ///
    /// Returns `None` if the buffer does not yet contain a complete frame.
    /// The terminator byte is **not** included in the returned slice.
    pub fn next_frame(&mut self) -> Option<(FrameKind, bytes::Bytes)> {
        if self.buf.is_empty() {
            return None;
        }
        let idx = memchr::memchr2(b'\n', b'!', &self.buf)?;
        let kind = if self.buf[idx] == b'!' {
            FrameKind::Bang
        } else {
            FrameKind::Newline
        };
        // Take frame contents (without terminator), then drop the terminator.
        let mut frame = self.buf.split_to(idx + 1);
        frame.truncate(idx);
        // `split_to` already drops the terminator; we tighten the view.
        // `BytesMut::freeze` yields a zero-copy `Bytes` sharing the alloc.
        Some((kind, frame.freeze()))
    }

    /// Drains as many frames as currently available, calling `f` for each.
    ///
    /// Convenience wrapper around [`Self::next_frame`].
    pub fn drain<F: FnMut(FrameKind, bytes::Bytes)>(&mut self, mut f: F) {
        while let Some((kind, frame)) = self.next_frame() {
            f(kind, frame);
        }
    }

    /// Discards `n` leading bytes — used to skip a known prefix (e.g.,
    /// the handshake banner) before frame parsing begins.
    pub fn advance(&mut self, n: usize) {
        self.buf.advance(n);
    }
}

impl Default for FrameSplitter {
    fn default() -> Self {
        Self::with_capacity(8 * 1024 * 1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_a_bang_terminated_quote() {
        let mut s = FrameSplitter::with_capacity(1024);
        s.extend_from_slice(b"T:PETR4:101758:2:59.95!");
        let (kind, frame) = s.next_frame().expect("frame should be ready");
        assert_eq!(kind, FrameKind::Bang);
        assert_eq!(frame.as_ref(), b"T:PETR4:101758:2:59.95");
        assert!(s.next_frame().is_none());
    }

    #[test]
    fn splits_a_newline_terminated_book() {
        let mut s = FrameSplitter::with_capacity(1024);
        s.extend_from_slice(b"B:PETR4:A:0:A:99.99:100:131:11041005\n");
        let (kind, frame) = s.next_frame().expect("frame should be ready");
        assert_eq!(kind, FrameKind::Newline);
        assert_eq!(frame.as_ref(), b"B:PETR4:A:0:A:99.99:100:131:11041005");
    }

    #[test]
    fn handles_back_to_back_frames_of_mixed_kinds() {
        let mut s = FrameSplitter::with_capacity(1024);
        s.extend_from_slice(b"T:PETR4:101758:2:59.95!B:PETR4:A:0:A:99.99:100:131:11041005\nGTC:20170308145946\n");
        let (k1, f1) = s.next_frame().unwrap();
        let (k2, f2) = s.next_frame().unwrap();
        let (k3, f3) = s.next_frame().unwrap();
        assert_eq!(k1, FrameKind::Bang);
        assert_eq!(f1.as_ref(), b"T:PETR4:101758:2:59.95");
        assert_eq!(k2, FrameKind::Newline);
        assert_eq!(f2.as_ref(), b"B:PETR4:A:0:A:99.99:100:131:11041005");
        assert_eq!(k3, FrameKind::Newline);
        assert_eq!(f3.as_ref(), b"GTC:20170308145946");
        assert!(s.next_frame().is_none());
    }

    #[test]
    fn buffers_partial_frame_until_terminator_arrives() {
        let mut s = FrameSplitter::with_capacity(1024);
        s.extend_from_slice(b"T:PETR4:101758");
        assert!(s.next_frame().is_none(), "no terminator yet");
        s.extend_from_slice(b":2:59.95!");
        let (kind, frame) = s.next_frame().unwrap();
        assert_eq!(kind, FrameKind::Bang);
        assert_eq!(frame.as_ref(), b"T:PETR4:101758:2:59.95");
    }

    #[test]
    fn empty_frame_between_two_terminators_is_still_a_frame() {
        // Although Cedro never sends an empty frame in practice, the splitter
        // must not panic if it appears (e.g., due to noise on the wire).
        let mut s = FrameSplitter::with_capacity(1024);
        s.extend_from_slice(b"\n\nGTC:20170308145946\n");
        let (k1, f1) = s.next_frame().unwrap();
        assert_eq!(k1, FrameKind::Newline);
        assert!(f1.is_empty());
        let (_, f2) = s.next_frame().unwrap();
        assert!(f2.is_empty());
        let (_, f3) = s.next_frame().unwrap();
        assert_eq!(f3.as_ref(), b"GTC:20170308145946");
    }
}
