//! Blocking-I/O reader thread.
//!
//! See `ARCHITECTURE.md §5.1`. The reader is the single most important piece
//! of the daemon: its job is to spend ~100% of its time in `recv()` on the
//! Cedro socket, feed bytes into a [`FrameSplitter`], and push each frame
//! onto the `crossbeam-channel` consumed by the parser pool.
//!
//! Design choices (each one is justified in the architecture doc):
//!
//! - **`std::thread`, not a Tokio task.** Tokio can deschedule a task when
//!   another saturates the runtime; we cannot tolerate that here.
//! - **Blocking socket.** No async overhead, no syscall reordering.
//! - **CPU pinning.** When `reader_cpu_affinity` is set in the config the
//!   thread pins itself before entering the loop.
//! - **Watchdog co-thread.** A second thread samples `ioctl FIONREAD` every
//!   `watchdog_interval` and publishes Prometheus gauges. If the kernel
//!   recv buffer exceeds the panic ratio it requests a graceful self-kill
//!   (signalled via [`ReaderHandle::panic_requested`]). Per §5.1.2, we
//!   prefer self-terminating to being kicked by Cedro for queue overflow.

use crate::config::ConnectionConfig;
use crate::connection::ConnectionHandle;
use crate::error::ClientError;
use cedro_protocol::{FrameKind, FrameSplitter};
use crossbeam_channel::{Receiver, Sender, TrySendError};
use std::io::Read;
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Frame as delivered to the parser pool. Owns a zero-copy `Bytes` slice.
pub type Frame = (FrameKind, bytes::Bytes);

/// Public driver: holds the join handles + control flags for the reader
/// and watchdog threads.
#[derive(Debug)]
pub struct ReaderHandle {
    pub frames: Receiver<Frame>,
    pub stop_flag: Arc<AtomicBool>,
    pub panic_flag: Arc<AtomicBool>,
    pub reader_thread: Option<JoinHandle<()>>,
    pub watchdog_thread: Option<JoinHandle<()>>,
}

impl ReaderHandle {
    /// Signals the reader/watchdog threads to wind down and joins them.
    /// Drops the channel sender as a side effect.
    pub fn shutdown(mut self) {
        self.stop_flag.store(true, Ordering::SeqCst);
        if let Some(t) = self.reader_thread.take() {
            let _ = t.join();
        }
        if let Some(t) = self.watchdog_thread.take() {
            let _ = t.join();
        }
    }

    /// Has the watchdog flagged the kernel buffer as overfull?
    /// The orchestrator polls this and exits cleanly when it flips.
    pub fn panic_requested(&self) -> bool {
        self.panic_flag.load(Ordering::SeqCst)
    }
}

/// One-shot factory that owns the launch flow.
#[derive(Debug)]
pub struct Reader;

impl Reader {
    /// Spawn the reader + watchdog threads.
    ///
    /// Takes:
    /// - `stream`: the read half from [`Connection::into_halves`].
    /// - `handle`: side-channel handle exposing the raw fd and `SO_RCVBUF`
    ///   for the watchdog.
    /// - `config`: borrowed for affinity & threshold settings.
    pub fn spawn(
        stream: TcpStream,
        handle: ConnectionHandle,
        config: &ConnectionConfig,
    ) -> Result<ReaderHandle, ClientError> {
        let (tx, rx) = crossbeam_channel::bounded::<Frame>(config.frame_channel_capacity);
        let stop_flag = Arc::new(AtomicBool::new(false));
        let panic_flag = Arc::new(AtomicBool::new(false));

        let reader_cfg = ReaderCfg {
            cpu_affinity: config.reader_cpu_affinity,
        };
        let reader_thread = thread::Builder::new()
            .name("cedro-reader".into())
            .spawn({
                let stop_flag = Arc::clone(&stop_flag);
                let tx = tx.clone();
                move || reader_loop(stream, tx, stop_flag, reader_cfg)
            })?;

        // Drop the local sender so the channel closes once the reader exits.
        drop(tx);

        let watchdog_cfg = WatchdogCfg {
            cpu_affinity: config.watchdog_cpu_affinity,
            interval: config.watchdog_interval,
            warn_ratio: config.backpressure_warn_ratio,
            panic_ratio: config.backpressure_panic_ratio,
            buffer_size: handle.so_rcvbuf,
        };
        let watchdog_thread = thread::Builder::new()
            .name("cedro-watchdog".into())
            .spawn({
                let stop_flag = Arc::clone(&stop_flag);
                let panic_flag = Arc::clone(&panic_flag);
                move || watchdog_loop(handle, stop_flag, panic_flag, watchdog_cfg)
            })?;

        Ok(ReaderHandle {
            frames: rx,
            stop_flag,
            panic_flag,
            reader_thread: Some(reader_thread),
            watchdog_thread: Some(watchdog_thread),
        })
    }
}

#[derive(Clone, Copy)]
struct ReaderCfg {
    cpu_affinity: Option<usize>,
}

#[derive(Clone, Copy)]
struct WatchdogCfg {
    cpu_affinity: Option<usize>,
    interval: Duration,
    warn_ratio: f64,
    panic_ratio: f64,
    buffer_size: usize,
}

/// Body of the reader thread. Stays inside this function for its entire life.
fn reader_loop(
    mut stream: TcpStream,
    tx: Sender<Frame>,
    stop_flag: Arc<AtomicBool>,
    cfg: ReaderCfg,
) {
    pin_to_core(cfg.cpu_affinity);

    let mut splitter = FrameSplitter::with_capacity(8 * 1024 * 1024);
    // 64 KB read chunk — heap-allocated (a stack-allocated array this size
    // would push the thread stack past its default limit on macOS).
    let mut buf = vec![0u8; 64 * 1024];

    loop {
        if stop_flag.load(Ordering::SeqCst) {
            return;
        }
        let n = match stream.read(&mut buf) {
            Ok(0) => {
                // EOF — peer closed the socket. The orchestrator will detect
                // this via the disconnected channel and trigger a reconnect.
                metrics::counter!("cedro_reader_eof_total").increment(1);
                return;
            }
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => {
                metrics::counter!("cedro_reader_read_error_total").increment(1);
                return;
            }
        };

        metrics::counter!("cedro_bytes_read_total").increment(n as u64);

        splitter.extend_from_slice(&buf[..n]);

        while let Some((kind, frame)) = splitter.next_frame() {
            metrics::counter!("cedro_frames_total").increment(1);

            // The channel is bounded; if it fills we are losing the race
            // against the parser pool. Try-send first, then block as a
            // fallback. Either way we emit a metric so the alerting layer
            // can correlate with `cedro_kernel_recv_buf_bytes`.
            match tx.try_send((kind, frame)) {
                Ok(()) => {}
                Err(TrySendError::Full((kind, frame))) => {
                    metrics::counter!("cedro_frame_channel_blocked_total").increment(1);
                    if tx.send((kind, frame)).is_err() {
                        return;
                    }
                }
                Err(TrySendError::Disconnected(_)) => return,
            }
        }
    }
}

/// Body of the watchdog thread. Samples `FIONREAD` and updates metrics.
fn watchdog_loop(
    handle: ConnectionHandle,
    stop_flag: Arc<AtomicBool>,
    panic_flag: Arc<AtomicBool>,
    cfg: WatchdogCfg,
) {
    pin_to_core(cfg.cpu_affinity);

    let warn_bytes = ((cfg.buffer_size as f64) * cfg.warn_ratio) as usize;
    let panic_bytes = ((cfg.buffer_size as f64) * cfg.panic_ratio) as usize;

    while !stop_flag.load(Ordering::SeqCst) {
        if let Ok(pending) = handle.pending_recv_bytes() {
            metrics::gauge!("cedro_kernel_recv_buf_bytes").set(pending as f64);
            if pending >= panic_bytes {
                metrics::counter!("cedro_kernel_recv_buf_panic_total").increment(1);
                panic_flag.store(true, Ordering::SeqCst);
                // The orchestrator's main loop is responsible for the
                // graceful exit (drain WAL, flush trade buffer, etc).
                return;
            }
            if pending >= warn_bytes {
                metrics::counter!("cedro_kernel_recv_buf_warn_total").increment(1);
            }
        } else {
            // Most likely the socket is gone; metric and exit.
            metrics::counter!("cedro_watchdog_fionread_failed_total").increment(1);
            return;
        }
        thread::sleep(cfg.interval);
    }
}

fn pin_to_core(target: Option<usize>) {
    let Some(id) = target else { return };
    let Some(cores) = core_affinity::get_core_ids() else {
        metrics::counter!("cedro_pinning_unsupported_total").increment(1);
        return;
    };
    if let Some(core) = cores.into_iter().find(|c| c.id == id)
        && !core_affinity::set_for_current(core) {
            metrics::counter!("cedro_pinning_failed_total").increment(1);
        }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    fn fresh_handle(stream: &TcpStream) -> ConnectionHandle {
        use std::os::fd::AsRawFd;
        ConnectionHandle {
            raw_fd: stream.as_raw_fd(),
            so_rcvbuf: 1 << 20, // 1 MB — plenty for the test fixtures.
        }
    }

    fn reader_only_config() -> ConnectionConfig {
        let mut cfg = ConnectionConfig::new("127.0.0.1", "u", "p");
        cfg.frame_channel_capacity = 16;
        cfg.watchdog_interval = Duration::from_millis(20);
        cfg.backpressure_warn_ratio = 0.20;
        cfg.backpressure_panic_ratio = 0.40;
        cfg
    }

    #[test]
    #[ignore = "exercises real TCP loopback + reader/watchdog threads; run with `--ignored`"]
    fn reader_emits_one_frame_per_terminator() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            s.write_all(b"GTC:20170308145946\nT:PETR4:101758:2:59.95!").unwrap();
            // Hold the connection open until the test signals done.
            thread::sleep(Duration::from_millis(150));
        });

        let client = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let handle = fresh_handle(&client);
        let cfg = reader_only_config();

        let reader = Reader::spawn(client, handle, &cfg).unwrap();

        let first = reader.frames.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(first.0, FrameKind::Newline);
        assert_eq!(first.1.as_ref(), b"GTC:20170308145946");

        let second = reader.frames.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(second.0, FrameKind::Bang);
        assert_eq!(second.1.as_ref(), b"T:PETR4:101758:2:59.95");

        reader.shutdown();
        server.join().unwrap();
    }

    #[test]
    #[ignore = "depends on kernel TCP buffer behavior; run with `--ignored`"]
    fn watchdog_marks_panic_when_buffer_exceeds_threshold() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        // Server: push bytes without anyone reading them. We give the
        // socket a write timeout so the thread doesn't deadlock if the
        // kernel TCP send buffer fills up.
        let server = thread::spawn(move || {
            let (s, _) = listener.accept().unwrap();
            s.set_write_timeout(Some(Duration::from_millis(100))).unwrap();
            let mut s = s;
            let chunk = vec![b'.'; 4 * 1024];
            // Best-effort push; stop as soon as the kernel says "no more room".
            for _ in 0..256 {
                if s.write_all(&chunk).is_err() {
                    break;
                }
            }
            // Hold the connection open briefly so the client-side kernel
            // buffer stays full long enough for the watchdog to sample it.
            thread::sleep(Duration::from_millis(150));
        });

        let client = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let mut handle = fresh_handle(&client);
        // Force a tiny effective threshold so even a small backlog trips panic.
        handle.so_rcvbuf = 32 * 1024;

        // Run the watchdog standalone against a connected-but-unread socket.
        let stop = Arc::new(AtomicBool::new(false));
        let panic = Arc::new(AtomicBool::new(false));
        let wd_cfg = WatchdogCfg {
            cpu_affinity: None,
            interval: Duration::from_millis(10),
            warn_ratio: 0.05,
            panic_ratio: 0.10,
            buffer_size: handle.so_rcvbuf,
        };
        let stop_clone = Arc::clone(&stop);
        let panic_clone = Arc::clone(&panic);
        let wd = thread::spawn(move || {
            watchdog_loop(handle, stop_clone, panic_clone, wd_cfg);
        });

        // Poll up to 1 second waiting for the panic flag.
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while std::time::Instant::now() < deadline && !panic.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(20));
        }
        assert!(panic.load(Ordering::SeqCst), "watchdog should have flagged");

        stop.store(true, Ordering::SeqCst);
        let _ = wd.join();
        drop(client);
        let _ = server.join();
    }
}
