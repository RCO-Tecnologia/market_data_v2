//! Batch INSERT into `public.trades`.
//!
//! Flow per trade:
//!
//! 1. Engine builds a [`StagedTrade`] (already resolved `asset_id`).
//! 2. `TradeWriter::stage` serialises it, appends to the WAL, and pushes
//!    onto the in-memory buffer.
//! 3. When the buffer hits `batch_size` or `flush_interval` elapses, the
//!    flush loop runs one bulk INSERT using `INSERT ... VALUES (...), (...)`
//!    with `ON CONFLICT DO NOTHING` on the unique `(time, asset_id,
//!    trade_id)` constraint.
//! 4. On commit success the WAL is truncated. On failure the WAL is left
//!    intact so a restart replays everything.
//!
//! Why not `COPY`: the trades table is small per row (10 columns, all
//! scalars) and we batch ≤ 5 000 rows / 100 ms. A single bound INSERT with
//! `UNNEST`-style multi-row VALUES sustains > 50 k rows/s on modest
//! hardware — well above the trade throughput we need to handle.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::config::PersistenceConfig;
use crate::error::PersistenceError;
use crate::wal::TradeWal;

/// Stripped-down trade record fitting the exact column set of `public.trades`.
#[derive(Debug, Clone, PartialEq)]
pub struct StagedTrade {
    pub time: DateTime<Utc>,
    pub asset_id: i32,
    pub price: f64,
    pub amount: i64,
    pub buyer_id: Option<i32>,
    pub seller_id: Option<i32>,
    /// `aggressor_side`: `None` = undefined, `Some(1)` = buy, `Some(2)` = sell.
    pub aggressor_side: Option<i16>,
    pub trade_id: Option<String>,
    pub is_direct: bool,
}

impl StagedTrade {
    /// Computes `financial_volume = price * amount` rounded to 2 decimal places.
    pub fn financial_volume(&self) -> Option<Decimal> {
        let price = Decimal::from_f64(self.price)?;
        let qty = Decimal::from(self.amount);
        Some((price * qty).round_dp(2))
    }

    /// Serialises this trade into the WAL framing format. Internal helper
    /// kept private to discourage downstream usage — the WAL is meant to
    /// be opaque outside the persistence crate.
    pub(crate) fn encode_wal(&self) -> Vec<u8> {
        // Custom binary frame — cheap, deterministic, no extra dep.
        // Layout (little-endian unless noted):
        //   i64 unix_micros
        //   i32 asset_id
        //   f64 price
        //   i64 amount
        //   u8  flags  (bit 0: buyer present, 1: seller, 2: aggressor, 3: trade_id, 4: is_direct)
        //   [i32 buyer_id if flag 0]
        //   [i32 seller_id if flag 1]
        //   [i16 aggressor_side if flag 2]
        //   [u32 trade_id_len + trade_id bytes if flag 3]
        let mut out = Vec::with_capacity(64);
        let micros = self.time.timestamp_micros();
        out.extend_from_slice(&micros.to_le_bytes());
        out.extend_from_slice(&self.asset_id.to_le_bytes());
        out.extend_from_slice(&self.price.to_le_bytes());
        out.extend_from_slice(&self.amount.to_le_bytes());
        let mut flags: u8 = 0;
        if self.buyer_id.is_some() {
            flags |= 0b0_0001;
        }
        if self.seller_id.is_some() {
            flags |= 0b0_0010;
        }
        if self.aggressor_side.is_some() {
            flags |= 0b0_0100;
        }
        if self.trade_id.is_some() {
            flags |= 0b0_1000;
        }
        if self.is_direct {
            flags |= 0b1_0000;
        }
        out.push(flags);
        if let Some(v) = self.buyer_id {
            out.extend_from_slice(&v.to_le_bytes());
        }
        if let Some(v) = self.seller_id {
            out.extend_from_slice(&v.to_le_bytes());
        }
        if let Some(v) = self.aggressor_side {
            out.extend_from_slice(&v.to_le_bytes());
        }
        if let Some(ref id) = self.trade_id {
            let bytes = id.as_bytes();
            let len = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(bytes);
        }
        out
    }

    /// Inverse of [`Self::encode_wal`]. Used by the recovery path on boot.
    pub(crate) fn decode_wal(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 8 + 4 + 8 + 8 + 1 {
            return None;
        }
        let mut p = 0;
        let micros = i64::from_le_bytes(bytes[p..p + 8].try_into().ok()?);
        p += 8;
        let asset_id = i32::from_le_bytes(bytes[p..p + 4].try_into().ok()?);
        p += 4;
        let price = f64::from_le_bytes(bytes[p..p + 8].try_into().ok()?);
        p += 8;
        let amount = i64::from_le_bytes(bytes[p..p + 8].try_into().ok()?);
        p += 8;
        let flags = bytes[p];
        p += 1;
        let buyer_id = read_flag_i32(bytes, &mut p, flags & 0b0_0001 != 0).ok()?;
        let seller_id = read_flag_i32(bytes, &mut p, flags & 0b0_0010 != 0).ok()?;
        let aggressor_side = if flags & 0b0_0100 != 0 {
            if bytes.len() < p + 2 {
                return None;
            }
            let v = i16::from_le_bytes(bytes[p..p + 2].try_into().ok()?);
            p += 2;
            Some(v)
        } else {
            None
        };
        let trade_id = if flags & 0b0_1000 != 0 {
            if bytes.len() < p + 4 {
                return None;
            }
            let len = u32::from_le_bytes(bytes[p..p + 4].try_into().ok()?) as usize;
            p += 4;
            if bytes.len() < p + len {
                return None;
            }
            let s = String::from_utf8(bytes[p..p + len].to_vec()).ok()?;
            Some(s)
        } else {
            None
        };
        let is_direct = flags & 0b1_0000 != 0;
        let time = DateTime::<Utc>::from_timestamp_micros(micros)?;
        Some(Self {
            time,
            asset_id,
            price,
            amount,
            buyer_id,
            seller_id,
            aggressor_side,
            trade_id,
            is_direct,
        })
    }
}

/// Helper for the optional-i32 fields in the WAL frame. Returns `None`
/// on truncation; the outer wrapper turns `Some(None)` into "absent".
fn read_flag_i32(bytes: &[u8], p: &mut usize, present: bool) -> ReadResult<Option<i32>> {
    if !present {
        return ReadResult::Ok(None);
    }
    if bytes.len() < *p + 4 {
        return ReadResult::Truncated;
    }
    let Ok(arr) = bytes[*p..*p + 4].try_into() else {
        return ReadResult::Truncated;
    };
    let v = i32::from_le_bytes(arr);
    *p += 4;
    ReadResult::Ok(Some(v))
}

#[derive(Debug)]
enum ReadResult<T> {
    Ok(T),
    Truncated,
}

impl<T> ReadResult<T> {
    fn ok(self) -> Option<T> {
        match self {
            Self::Ok(v) => Some(v),
            Self::Truncated => None,
        }
    }
}

/// Trade writer: batches staged trades and INSERTs them into Timescale.
#[derive(Debug)]
pub struct TradeWriter {
    pool: PgPool,
    wal: Arc<TradeWal>,
    buffer: Arc<Mutex<Vec<StagedTrade>>>,
    batch_size: usize,
}

impl TradeWriter {
    /// Connects to the database and opens (or rewires) the WAL.
    pub async fn connect(cfg: &PersistenceConfig) -> Result<Self, PersistenceError> {
        let pool = PgPoolOptions::new()
            .max_connections(cfg.pool_size)
            .connect(&cfg.database_url)
            .await?;
        let wal = Arc::new(TradeWal::open(&cfg.wal_path)?);
        Ok(Self {
            pool,
            wal,
            buffer: Arc::new(Mutex::new(Vec::with_capacity(cfg.batch_size))),
            batch_size: cfg.batch_size,
        })
    }

    /// Test-only construction that lets callers inject a pre-built pool +
    /// WAL — used by integration tests that bring up a local Postgres.
    pub fn from_parts(pool: PgPool, wal: Arc<TradeWal>, batch_size: usize) -> Self {
        Self {
            pool,
            wal,
            buffer: Arc::new(Mutex::new(Vec::with_capacity(batch_size))),
            batch_size,
        }
    }

    /// Stage a trade: append to the WAL and push onto the in-memory buffer.
    pub async fn stage(&self, trade: StagedTrade) -> Result<(), PersistenceError> {
        let encoded = trade.encode_wal();
        self.wal.append(&encoded)?;
        let mut buf = self.buffer.lock().await;
        buf.push(trade);
        if buf.len() >= self.batch_size {
            // Drain the buffer here while we hold the lock so the next
            // staging call doesn't double-insert.
            let drained = std::mem::take(&mut *buf);
            drop(buf);
            self.flush_drained(drained).await?;
        }
        Ok(())
    }

    /// Force a flush of the buffered trades. The orchestrator calls this on
    /// the `flush_interval` tick and once on graceful shutdown.
    pub async fn flush(&self) -> Result<usize, PersistenceError> {
        let drained = {
            let mut buf = self.buffer.lock().await;
            std::mem::take(&mut *buf)
        };
        let n = drained.len();
        if n == 0 {
            return Ok(0);
        }
        self.flush_drained(drained).await?;
        Ok(n)
    }

    async fn flush_drained(&self, batch: Vec<StagedTrade>) -> Result<(), PersistenceError> {
        let n = batch.len();
        if n == 0 {
            return Ok(());
        }
        // Force the OS to durabilise pending WAL writes before we start
        // the transaction. If the daemon crashes after this point but
        // before the COMMIT, the recovery replay re-stages and we get the
        // benefit of ON CONFLICT DO NOTHING.
        self.wal.sync()?;

        let started = std::time::Instant::now();
        let mut qb = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            "INSERT INTO public.trades \
             (time, asset_id, price, amount, buyer_id, seller_id, \
              aggressor_side, trade_id, is_direct, financial_volume) ",
        );
        qb.push_values(&batch, |mut b, t| {
            let price = Decimal::from_f64(t.price).unwrap_or_default();
            let financial = t.financial_volume().unwrap_or_default();
            b.push_bind(t.time)
                .push_bind(t.asset_id)
                .push_bind(price)
                .push_bind(t.amount)
                .push_bind(t.buyer_id)
                .push_bind(t.seller_id)
                .push_bind(t.aggressor_side)
                .push_bind(t.trade_id.clone())
                .push_bind(t.is_direct)
                .push_bind(financial);
        });
        qb.push(" ON CONFLICT (time, asset_id, trade_id) DO NOTHING");
        let query = qb.build();
        let result = query.execute(&self.pool).await?;

        // Successful commit ⇒ WAL records are now durable downstream.
        // Truncating keeps the file small; if it fails (rare; disk issue)
        // we surface as an error but the data is already safe.
        self.wal.truncate()?;

        metrics::counter!("persistence_trades_flushed_total").increment(n as u64);
        metrics::counter!("persistence_trades_skipped_conflicts_total")
            .increment((n as u64).saturating_sub(result.rows_affected()));
        metrics::histogram!("persistence_trade_flush_duration_seconds")
            .record(started.elapsed().as_secs_f64());
        Ok(())
    }

    pub const fn wal(&self) -> &Arc<TradeWal> {
        &self.wal
    }

    /// Replay every record currently in the WAL into the in-memory buffer.
    /// Used on boot to recover trades that hadn't yet been flushed when
    /// the daemon stopped. The next call to [`Self::flush`] writes them
    /// out — the unique constraint on `(time, asset_id, trade_id)` makes
    /// re-processing safe even if the original flush did partially land.
    pub async fn recover_from_wal(&self) -> Result<usize, PersistenceError> {
        let records = self.wal.replay()?;
        let mut recovered = 0;
        let mut buf = self.buffer.lock().await;
        for raw in records {
            if let Some(trade) = StagedTrade::decode_wal(&raw) {
                buf.push(trade);
                recovered += 1;
            } else {
                metrics::counter!("persistence_wal_decode_failures_total").increment(1);
            }
        }
        metrics::counter!("persistence_wal_records_recovered_total")
            .increment(recovered as u64);
        Ok(recovered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_trade() -> StagedTrade {
        StagedTrade {
            time: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
            asset_id: 42,
            price: 32.55,
            amount: 100,
            buyer_id: Some(239),
            seller_id: Some(354),
            aggressor_side: Some(1),
            trade_id: Some("T123456".into()),
            is_direct: false,
        }
    }

    #[test]
    fn financial_volume_multiplies_price_by_amount() {
        let trade = sample_trade();
        let fv = trade.financial_volume().unwrap();
        assert_eq!(fv.to_string(), "3255.00");
    }

    #[test]
    fn wal_round_trip_preserves_every_field() {
        let original = sample_trade();
        let encoded = original.encode_wal();
        let decoded = StagedTrade::decode_wal(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn wal_round_trip_with_nulls() {
        let trade = StagedTrade {
            time: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
            asset_id: 1,
            price: 1.0,
            amount: 1,
            buyer_id: None,
            seller_id: None,
            aggressor_side: None,
            trade_id: None,
            is_direct: false,
        };
        let encoded = trade.encode_wal();
        let decoded = StagedTrade::decode_wal(&encoded).unwrap();
        assert_eq!(trade, decoded);
    }

    #[test]
    fn wal_round_trip_with_is_direct_flag() {
        let mut trade = sample_trade();
        trade.is_direct = true;
        let encoded = trade.encode_wal();
        let decoded = StagedTrade::decode_wal(&encoded).unwrap();
        assert!(decoded.is_direct);
    }

    #[test]
    fn decode_rejects_truncated_input() {
        let original = sample_trade();
        let mut encoded = original.encode_wal();
        encoded.truncate(encoded.len() - 2);
        assert!(StagedTrade::decode_wal(&encoded).is_none());
    }
}
