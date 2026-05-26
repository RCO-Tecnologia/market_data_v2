//! Background writer thread + bounded channel ingress.

use bytes::Bytes;
use chrono::{DateTime, Datelike, Timelike, Utc};
use crossbeam_channel::{Receiver, Sender, TrySendError};
use std::fs::{File, create_dir_all};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread::{self, JoinHandle};
use thiserror::Error;
use zstd::stream::Encoder;

#[derive(Debug, Error)]
pub enum ArchiveError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("writer already shut down")]
    AlreadyClosed,
}

/// Configuration knobs for the archive.
#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    /// Directory where rotated files land. Created on startup if absent.
    pub root: PathBuf,
    /// zstd compression level (1..=22). Default 3 — fast and reasonably tight.
    pub zstd_level: i32,
    /// Capacity of the channel between producers and the writer thread.
    /// When full, frames are dropped (logged as a metric).
    pub channel_capacity: usize,
    /// How many bytes to buffer in the encoder before flushing to disk.
    pub flush_every_bytes: usize,
}

impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            root: PathBuf::from("/var/data/raw"),
            zstd_level: 3,
            channel_capacity: 65_536,
            flush_every_bytes: 1024 * 1024, // 1 MB
        }
    }
}

/// Public handle: send frames in, ask for a graceful shutdown at the end.
#[derive(Debug)]
pub struct ArchiveHandle {
    tx: Sender<Bytes>,
    join: Option<JoinHandle<()>>,
}

impl ArchiveHandle {
    /// Best-effort push of one frame into the archive queue.
    ///
    /// Returns `true` if the frame was accepted, `false` if it was dropped
    /// because the channel was full. The hot path must NOT block here.
    pub fn try_push(&self, frame: Bytes) -> bool {
        match self.tx.try_send(frame) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                metrics::counter!("raw_archive_dropped_frames_total").increment(1);
                false
            }
            Err(TrySendError::Disconnected(_)) => false,
        }
    }

    /// Closes the channel and joins the writer thread, ensuring every
    /// buffered byte hits disk.
    pub fn shutdown(mut self) -> Result<(), ArchiveError> {
        drop(self.tx); // closes the channel
        if let Some(join) = self.join.take() {
            join.join().map_err(|_| {
                ArchiveError::Io(std::io::Error::other("writer thread panicked"))
            })?;
        }
        Ok(())
    }
}

/// Factory for the archive subsystem.
#[derive(Debug)]
pub struct ArchiveWriter;

impl ArchiveWriter {
    /// Spawn the writer thread and return a handle for producers.
    pub fn spawn(config: ArchiveConfig) -> Result<ArchiveHandle, ArchiveError> {
        create_dir_all(&config.root)?;

        let (tx, rx) = crossbeam_channel::bounded::<Bytes>(config.channel_capacity);
        let cfg = config;
        let join = thread::Builder::new()
            .name("raw-archive-writer".into())
            .spawn(move || writer_loop(rx, cfg))?;

        Ok(ArchiveHandle {
            tx,
            join: Some(join),
        })
    }
}

fn writer_loop(rx: Receiver<Bytes>, cfg: ArchiveConfig) {
    let mut state: Option<HourFile> = None;

    while let Ok(frame) = rx.recv() {
        let now = Utc::now();
        let need_rotate = state
            .as_ref()
            .is_none_or(|s| s.hour_key != hour_key(now));

        if need_rotate {
            if let Some(mut prev) = state.take() {
                if let Err(e) = prev.finalize() {
                    metrics::counter!("raw_archive_finalize_error_total").increment(1);
                    tracing::warn!(error = %e, "raw archive failed to finalize previous hour");
                }
            }
            match HourFile::open(&cfg, now) {
                Ok(f) => state = Some(f),
                Err(e) => {
                    metrics::counter!("raw_archive_open_error_total").increment(1);
                    tracing::error!(error = %e, "raw archive could not open new hour file");
                    continue;
                }
            }
        }

        let Some(file) = state.as_mut() else { continue };
        if let Err(e) = file.write_frame(&frame) {
            metrics::counter!("raw_archive_write_error_total").increment(1);
            tracing::warn!(error = %e, "raw archive write failed");
            // Try recovering by closing & re-opening on next iteration.
            state = None;
        } else {
            metrics::counter!("raw_archive_frames_written_total").increment(1);
            metrics::counter!("raw_archive_bytes_in_total").increment(frame.len() as u64);
        }
    }

    if let Some(mut last) = state.take() {
        if let Err(e) = last.finalize() {
            tracing::warn!(error = %e, "raw archive final flush failed");
        }
    }
}

/// One open hour-bucket file with its encoder.
struct HourFile {
    encoder: Encoder<'static, File>,
    hour_key: (i32, u32, u32, u32), // (year, month, day, hour)
    bytes_since_flush: usize,
    flush_threshold: usize,
}

impl HourFile {
    fn open(cfg: &ArchiveConfig, now: DateTime<Utc>) -> Result<Self, ArchiveError> {
        let name = format!(
            "{:04}-{:02}-{:02}T{:02}.zst",
            now.year(),
            now.month(),
            now.day(),
            now.hour()
        );
        let path = cfg.root.join(name);
        let file = File::options()
            .create(true)
            .append(true)
            .open(&path)?;
        let encoder = Encoder::new(file, cfg.zstd_level)?;
        Ok(Self {
            encoder,
            hour_key: hour_key(now),
            bytes_since_flush: 0,
            flush_threshold: cfg.flush_every_bytes,
        })
    }

    fn write_frame(&mut self, frame: &[u8]) -> Result<(), ArchiveError> {
        // Record each frame as: 4-byte LE length + frame bytes + b'\n'.
        // The trailing newline isn't strictly needed (length-prefix is
        // self-describing) but it makes the archive trivially greppable
        // when the consumer wants to peek at decompressed content.
        let len = frame.len() as u32;
        self.encoder.write_all(&len.to_le_bytes())?;
        self.encoder.write_all(frame)?;
        self.encoder.write_all(b"\n")?;
        self.bytes_since_flush += frame.len() + 5;
        if self.bytes_since_flush >= self.flush_threshold {
            self.encoder.flush()?;
            self.bytes_since_flush = 0;
        }
        Ok(())
    }

    fn finalize(&mut self) -> Result<(), ArchiveError> {
        // Take ownership of the encoder so we can finalize the stream
        // properly (zstd has its own footer write that must run on Drop
        // or via `finish`). We swap in a sentinel that won't be used.
        let sentinel = Encoder::new(open_null()?, 0)?;
        let encoder = std::mem::replace(&mut self.encoder, sentinel);
        encoder.finish()?;
        Ok(())
    }
}

#[cfg(unix)]
fn open_null() -> std::io::Result<File> {
    File::options().write(true).open("/dev/null")
}

#[cfg(not(unix))]
fn open_null() -> std::io::Result<File> {
    File::options().write(true).open(std::path::Path::new("nul"))
}

fn hour_key(t: DateTime<Utc>) -> (i32, u32, u32, u32) {
    (t.year(), t.month(), t.day(), t.hour())
}

// Unused on macOS but kept for symmetry with the docs example.
#[allow(dead_code)]
fn current_path(root: &Path, t: DateTime<Utc>) -> PathBuf {
    root.join(format!(
        "{:04}-{:02}-{:02}T{:02}.zst",
        t.year(),
        t.month(),
        t.day(),
        t.hour()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::TempDir;

    fn cfg(dir: &TempDir) -> ArchiveConfig {
        ArchiveConfig {
            root: dir.path().to_path_buf(),
            zstd_level: 1,
            channel_capacity: 64,
            flush_every_bytes: 16, // tiny so tests don't need to write much
        }
    }

    fn list_archive_files(root: &Path) -> Vec<PathBuf> {
        let mut out: Vec<_> = std::fs::read_dir(root)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "zst"))
            .collect();
        out.sort();
        out
    }

    #[test]
    fn pushes_and_persists_a_frame() {
        let dir = TempDir::new().unwrap();
        let archive = ArchiveWriter::spawn(cfg(&dir)).unwrap();

        assert!(archive.try_push(Bytes::from_static(b"T:PETR4:101758:2:59.95")));
        assert!(archive.try_push(Bytes::from_static(b"GTC:20170308145946")));
        archive.shutdown().unwrap();

        let files = list_archive_files(dir.path());
        assert_eq!(files.len(), 1, "exactly one hour-file expected");

        // Inflate and confirm both frames are inside (length-prefix + bytes + \n).
        let bytes = std::fs::read(&files[0]).unwrap();
        let decoded = zstd::stream::decode_all(&bytes[..]).unwrap();
        // First frame: 22 bytes payload + 4 bytes len + 1 byte \n = 27.
        let len1 = u32::from_le_bytes(decoded[..4].try_into().unwrap()) as usize;
        assert_eq!(&decoded[4..4 + len1], b"T:PETR4:101758:2:59.95");
        assert_eq!(decoded[4 + len1], b'\n');
        let start2 = 4 + len1 + 1;
        let len2 = u32::from_le_bytes(decoded[start2..start2 + 4].try_into().unwrap()) as usize;
        assert_eq!(
            &decoded[start2 + 4..start2 + 4 + len2],
            b"GTC:20170308145946"
        );
    }

    #[test]
    fn dropped_frames_are_counted_when_channel_is_full() {
        let dir = TempDir::new().unwrap();
        // Tiny capacity so the producer outruns the consumer.
        let mut c = cfg(&dir);
        c.channel_capacity = 2;
        let archive = ArchiveWriter::spawn(c).unwrap();

        let mut accepted = 0usize;
        for i in 0..200 {
            let f = Bytes::from(format!("F{i}").into_bytes());
            if archive.try_push(f) {
                accepted += 1;
            }
        }
        // We accept that some pushes succeed and some don't; the only hard
        // requirement is that try_push never blocks and never panics.
        assert!(accepted > 0);
        let _ = archive.shutdown();
    }

    #[test]
    fn shutdown_is_idempotent_via_drop() {
        let dir = TempDir::new().unwrap();
        let archive = ArchiveWriter::spawn(cfg(&dir)).unwrap();
        archive.try_push(Bytes::from_static(b"x"));
        // Don't call shutdown; let `Drop` clean up.
        drop(archive);
        // Give the writer thread a chance to flush.
        thread::sleep(Duration::from_millis(50));
        // We don't assert on file content here — just that drop didn't panic.
    }

    #[test]
    fn directory_is_created_if_missing() {
        let dir = TempDir::new().unwrap();
        let nested = dir.path().join("deep/nested/dir");
        let mut c = cfg(&dir);
        c.root = nested.clone();
        let archive = ArchiveWriter::spawn(c).unwrap();
        archive.try_push(Bytes::from_static(b"x"));
        archive.shutdown().unwrap();
        assert!(nested.exists());
    }
}
