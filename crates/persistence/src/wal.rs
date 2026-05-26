//! Append-only write-ahead log for trades.
//!
//! Goal: perda zero of trades, per `ARCHITECTURE.md §5.5.3`. Every staged
//! trade is fsync'd to this file before we acknowledge to the engine.
//! The flush loop reads from the WAL, batches into the database, and on
//! a successful commit marks records as durable so they can be evicted.
//!
//! File format — a sequence of records, each:
//!
//! ```text
//!     u32 little-endian length  (excludes header + trailing crc)
//!     <payload bytes>           (binary serialisation; see TradeRecord)
//!     u32 little-endian crc32   (over length || payload)
//! ```
//!
//! No fancy compaction, no segments. The orchestrator is expected to
//! truncate / rotate the file periodically once durable.

use std::fs::{File, OpenOptions, create_dir_all};
use std::io::{BufWriter, Read, Seek, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use crate::error::PersistenceError;

/// Wraps an append-only WAL file behind a mutex. Multiple producers can
/// call `append` concurrently — the mutex is held only for the duration
/// of the write, which is bytes-per-fsync fast.
#[derive(Debug)]
pub struct TradeWal {
    inner: Mutex<WalInner>,
    path: PathBuf,
}

#[derive(Debug)]
struct WalInner {
    writer: BufWriter<File>,
    bytes_written: u64,
}

impl TradeWal {
    /// Opens (or creates) the WAL file at `path`, creating any missing
    /// parent directories.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, PersistenceError> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        let bytes_written = file.metadata()?.len();
        let writer = BufWriter::with_capacity(64 * 1024, file);
        Ok(Self {
            inner: Mutex::new(WalInner {
                writer,
                bytes_written,
            }),
            path,
        })
    }

    /// Append a single record. `payload` is opaque to the WAL — the
    /// caller decides the encoding (bincode, json, custom).
    ///
    /// fsync is **not** called here: the WAL relies on the OS page cache
    /// for batching. The orchestrator calls [`Self::sync`] before
    /// confirming durable batch progress to upstream.
    pub fn append(&self, payload: &[u8]) -> Result<u64, PersistenceError> {
        let len = u32::try_from(payload.len()).map_err(|_| {
            PersistenceError::InvalidTrade("payload larger than 4 GiB rejected")
        })?;
        let crc = compute_crc32(len, payload);

        let mut guard = self.inner.lock().expect("WAL mutex poisoned");
        guard.writer.write_all(&len.to_le_bytes())?;
        guard.writer.write_all(payload)?;
        guard.writer.write_all(&crc.to_le_bytes())?;
        let written = 4 + payload.len() as u64 + 4;
        guard.bytes_written += written;
        let offset = guard.bytes_written;
        metrics::counter!("persistence_wal_records_total").increment(1);
        metrics::counter!("persistence_wal_bytes_total").increment(written);
        Ok(offset)
    }

    /// Force the in-memory buffer to disk via fsync.
    pub fn sync(&self) -> Result<(), PersistenceError> {
        let mut guard = self.inner.lock().expect("WAL mutex poisoned");
        guard.writer.flush()?;
        guard.writer.get_ref().sync_all()?;
        Ok(())
    }

    pub fn bytes_written(&self) -> u64 {
        self.inner.lock().expect("WAL mutex poisoned").bytes_written
    }

    pub const fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Read every record currently in the WAL, in order. Returns an empty
    /// vec when the file is fresh.
    pub fn replay(&self) -> Result<Vec<Vec<u8>>, PersistenceError> {
        let mut guard = self.inner.lock().expect("WAL mutex poisoned");
        guard.writer.flush()?;
        // Re-open read handle on the underlying file; the writer keeps
        // its append cursor independently.
        let mut file = File::open(&self.path)?;
        file.rewind()?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut out = Vec::new();
        let mut pos = 0;
        while pos + 4 <= buf.len() {
            let len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + len + 4 > buf.len() {
                // Truncated tail — likely a partial write before crash.
                metrics::counter!("persistence_wal_truncated_records_total").increment(1);
                break;
            }
            let payload = &buf[pos..pos + len];
            let stored_crc =
                u32::from_le_bytes(buf[pos + len..pos + len + 4].try_into().unwrap());
            let expected = compute_crc32(len as u32, payload);
            if stored_crc != expected {
                metrics::counter!("persistence_wal_crc_failures_total").increment(1);
                break;
            }
            out.push(payload.to_vec());
            pos += len + 4;
        }
        Ok(out)
    }

    /// Empties the WAL — called after a successful batch commit confirms
    /// every staged record is now durable downstream.
    pub fn truncate(&self) -> Result<(), PersistenceError> {
        let mut guard = self.inner.lock().expect("WAL mutex poisoned");
        guard.writer.flush()?;
        guard.writer.get_ref().set_len(0)?;
        guard.writer.get_ref().rewind()?;
        guard.bytes_written = 0;
        metrics::counter!("persistence_wal_truncations_total").increment(1);
        Ok(())
    }
}

/// Cheap CRC32 (IEEE polynomial). We don't pull a dependency for this —
/// the table is tiny and the WAL throughput is far below CPU peak.
fn compute_crc32(len: u32, payload: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for byte in len.to_le_bytes().iter().chain(payload.iter()) {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (!(crc & 1)).wrapping_add(1);
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open_in(dir: &TempDir) -> TradeWal {
        TradeWal::open(dir.path().join("trades.wal")).unwrap()
    }

    #[test]
    fn append_and_replay_round_trip() {
        let dir = TempDir::new().unwrap();
        let wal = open_in(&dir);
        wal.append(b"trade-1").unwrap();
        wal.append(b"trade-2").unwrap();
        wal.append(b"trade-3").unwrap();
        wal.sync().unwrap();

        let replayed = wal.replay().unwrap();
        assert_eq!(replayed.len(), 3);
        assert_eq!(replayed[0], b"trade-1");
        assert_eq!(replayed[2], b"trade-3");
    }

    #[test]
    fn truncate_clears_existing_records() {
        let dir = TempDir::new().unwrap();
        let wal = open_in(&dir);
        wal.append(b"trade-1").unwrap();
        wal.append(b"trade-2").unwrap();
        wal.sync().unwrap();
        wal.truncate().unwrap();
        let replayed = wal.replay().unwrap();
        assert!(replayed.is_empty());
        assert_eq!(wal.bytes_written(), 0);
    }

    #[test]
    fn corrupted_crc_stops_replay_without_panic() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("trades.wal");
        {
            let wal = TradeWal::open(&path).unwrap();
            wal.append(b"good-1").unwrap();
            wal.sync().unwrap();
        }
        // Flip a byte in the file to induce a CRC mismatch.
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();

        let wal = TradeWal::open(&path).unwrap();
        let replayed = wal.replay().unwrap();
        assert!(replayed.is_empty(), "CRC failure should abort the replay");
    }

    #[test]
    fn truncated_tail_is_tolerated() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("trades.wal");
        {
            let wal = TradeWal::open(&path).unwrap();
            wal.append(b"good-1").unwrap();
            wal.sync().unwrap();
        }
        // Append garbage that looks like the start of a record but is
        // shorter than the declared length — simulates a torn write.
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&u32::to_le_bytes(100)).unwrap();
        f.write_all(b"only-eight-bytes").unwrap();
        drop(f);

        let wal = TradeWal::open(&path).unwrap();
        let replayed = wal.replay().unwrap();
        // The first record survives; the truncated tail is dropped.
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0], b"good-1");
    }

    #[test]
    fn crc_matches_for_identical_payloads_and_lengths() {
        assert_eq!(compute_crc32(3, b"abc"), compute_crc32(3, b"abc"));
        assert_ne!(compute_crc32(3, b"abc"), compute_crc32(3, b"abd"));
        assert_ne!(compute_crc32(3, b"abc"), compute_crc32(4, b"abc\0"));
    }
}
