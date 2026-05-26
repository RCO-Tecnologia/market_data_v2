//! Owns the lifecycle of the Cedro TCP socket.
//!
//! Responsibilities:
//!
//! 1. Open a TCP connection with explicit socket-level tuning
//!    (`SO_RCVBUF=128MB`, `TCP_NODELAY`, blocking I/O).
//! 2. Run the handshake via [`crate::handshake::perform`].
//! 3. Split the connected socket into two halves: one consumed by the
//!    reader thread (read-only), one held by the [`CommandSink`] writer.
//!
//! The connection deliberately uses *blocking* I/O — the reader thread is
//! supposed to spend ~100% of its time in `recv()` (see `ARCHITECTURE.md
//! §5.1`). Tokio is not appropriate here.

use crate::commands::CommandSink;
use crate::config::ConnectionConfig;
use crate::error::ClientError;
use crate::handshake;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::TcpStream;
use std::time::Duration;

/// Configured, handshaked, ready-to-stream Cedro connection.
///
/// Holds the read half (consumed by the reader thread) and the write half
/// (wrapped in [`CommandSink`]). After construction, call
/// [`Connection::into_halves`] to split them up and hand the read half to
/// the [`crate::reader::Reader`] startup.
#[derive(Debug)]
pub struct Connection {
    read_stream: TcpStream,
    command_sink: CommandSink<TcpStream>,
    raw_fd: std::os::fd::RawFd,
    so_rcvbuf: usize,
}

impl Connection {
    /// Open a TCP connection to `config.addr()`, tune it, and complete the
    /// handshake. Returns a fully-prepared [`Connection`] on success.
    pub fn open(config: &ConnectionConfig) -> Result<Self, ClientError> {
        config.validate().map_err(ClientError::InvalidConfig)?;

        let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
        Self::tune_socket(&socket, config)?;

        // Use connect_timeout so a dead peer doesn't hang the daemon at boot.
        let addr = config.addr();
        let parsed = addr
            .parse::<std::net::SocketAddr>()
            .map_err(|_| ClientError::InvalidConfig("host:port could not be parsed"))?;
        socket.connect_timeout(&parsed.into(), Duration::from_secs(10))?;

        // Convert to std::net::TcpStream and clone read/write halves.
        let mut stream: TcpStream = socket.into();
        stream.set_nodelay(true)?;

        // Use a generous read timeout *only* during handshake; we remove it
        // before handing the socket to the reader thread.
        stream.set_read_timeout(Some(config.handshake_timeout))?;
        stream.set_write_timeout(Some(config.handshake_timeout))?;

        handshake::perform(&mut stream, config)?;

        // Switch back to fully blocking I/O for the streaming phase.
        stream.set_read_timeout(None)?;
        stream.set_write_timeout(None)?;

        let read_stream = stream.try_clone()?;
        let raw_fd = {
            use std::os::fd::AsRawFd;
            read_stream.as_raw_fd()
        };
        let command_sink = CommandSink::new(stream);

        Ok(Self {
            read_stream,
            command_sink,
            raw_fd,
            so_rcvbuf: config.so_rcvbuf,
        })
    }

    /// Decompose the connection into its constituent halves. The read half
    /// is owned by the caller (typically the reader thread); the writer
    /// stays as a [`CommandSink`] for the orchestrator.
    pub fn into_halves(self) -> (TcpStream, CommandSink<TcpStream>, ConnectionHandle) {
        let handle = ConnectionHandle {
            raw_fd: self.raw_fd,
            so_rcvbuf: self.so_rcvbuf,
        };
        (self.read_stream, self.command_sink, handle)
    }

    fn tune_socket(socket: &Socket, config: &ConnectionConfig) -> Result<(), ClientError> {
        // Best-effort SO_RCVBUF — the kernel caps at `net.core.rmem_max`,
        // which is part of the deployment checklist (`ARCHITECTURE.md §11.5`).
        // If the kernel refuses we log via metric but keep going; the
        // backpressure watchdog will still catch overflow.
        if let Err(e) = socket.set_recv_buffer_size(config.so_rcvbuf) {
            metrics::counter!("cedro_client_so_rcvbuf_set_failed_total").increment(1);
            // Surface as a tracing warning — handshake is the only spot we
            // are allowed to log freely (ARCHITECTURE §5.8 forbids logs in
            // the streaming hot path, not during boot).
            tracing::warn!(
                requested = config.so_rcvbuf,
                error = %e,
                "could not raise SO_RCVBUF; check net.core.rmem_max"
            );
        }
        socket.set_keepalive(true)?;
        Ok(())
    }
}

/// Side-channel handle that exposes the underlying file descriptor and the
/// configured buffer size. Consumed by [`crate::reader::Reader`] for the
/// `ioctl FIONREAD` watchdog.
#[derive(Debug, Clone)]
pub struct ConnectionHandle {
    pub raw_fd: std::os::fd::RawFd,
    pub so_rcvbuf: usize,
}

impl ConnectionHandle {
    /// Returns the number of bytes currently sitting in the kernel receive
    /// buffer for this socket. Implementation uses `ioctl FIONREAD`, the
    /// same primitive `ARCHITECTURE.md §5.1.2` calls for.
    #[allow(unsafe_code)] // single, well-bounded ioctl — see SAFETY note below
    pub fn pending_recv_bytes(&self) -> std::io::Result<usize> {
        let mut pending: libc::c_int = 0;
        // SAFETY: `raw_fd` came from a live TcpStream that the caller keeps
        // alive for the duration of this call (the reader thread owns it).
        // FIONREAD writes a single `c_int`; we pass a valid mutable pointer
        // to a local. The ioctl number is the kernel-standard FIONREAD for
        // SOCK_STREAM sockets on every supported platform.
        let rc = unsafe { libc::ioctl(self.raw_fd, libc::FIONREAD, &mut pending) };
        if rc < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(pending.max(0) as usize)
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    /// Spin up a real loopback listener that scripts the Cedro handshake
    /// (server → client) so we exercise the full socket open + handshake
    /// path against a true TCP stream.
    fn spawn_scripted_server() -> (u16, thread::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            s.write_all(b"Welcome to Cedro Crystal\r\n").unwrap();
            // Consume the software key line.
            let mut buf = [0u8; 256];
            let _ = s.read(&mut buf).unwrap();
            s.write_all(b"Username:").unwrap();
            let _ = s.read(&mut buf).unwrap();
            s.write_all(b"Password:").unwrap();
            let _ = s.read(&mut buf).unwrap();
            s.write_all(b"You are connected\r\n").unwrap();
            // Mark end of handshake. Then hold the connection open until
            // the client closes it.
            let mut sink = Vec::new();
            let _ = s.read_to_end(&mut sink);
            sink
        });
        (port, handle)
    }

    #[test]
    #[ignore = "exercises real TCP loopback; run with `cargo test -- --ignored`"]
    fn open_completes_handshake_and_keeps_socket_alive() {
        let (port, server) = spawn_scripted_server();
        let mut cfg = ConnectionConfig::new("127.0.0.1", "alice", "s3cr3t");
        cfg.port = port;
        cfg.so_rcvbuf = 8 * 1024 * 1024; // 8 MB; loopback kernels accept this.

        let conn = Connection::open(&cfg).expect("connection should open");
        let (read_stream, _sink, handle) = conn.into_halves();
        // Sanity: FIONREAD on an idle socket is 0.
        assert_eq!(handle.pending_recv_bytes().unwrap(), 0);
        drop(read_stream);
        // Server task exits when our socket closes.
        server.join().unwrap();
    }
}
