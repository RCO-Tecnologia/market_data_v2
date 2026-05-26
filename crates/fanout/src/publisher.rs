//! Concrete publisher implementations.
//!
//! The crate exposes a small [`FanoutSink`] trait so the rest of the system
//! can be tested without spinning up NATS or Redis. A real connection is
//! built via [`FanoutPublisher::connect`].

use crate::types::PublishKey;
use bytes::Bytes;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum FanoutError {
    #[error("NATS error: {0}")]
    Nats(#[from] async_nats::PublishError),
    #[error("NATS connection error: {0}")]
    NatsConnect(#[from] async_nats::ConnectError),
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("invalid configuration: {0}")]
    Config(&'static str),
}

/// Abstract sink — every publish goes through here so tests can stub the
/// network out completely.
#[async_trait::async_trait]
pub trait FanoutSink: Send + Sync + std::fmt::Debug {
    /// Publish to NATS + write the snapshot to Redis. Both writes are
    /// attempted unconditionally; partial failures are logged via metrics
    /// but do NOT abort the caller — the engine keeps running.
    async fn publish(&self, key: &PublishKey, payload: Bytes) -> Result<(), FanoutError>;
}

/// Default no-op sink — used in tests and in dev when the operator hasn't
/// wired up NATS yet. Records the calls for inspection.
#[derive(Debug, Default)]
pub struct NoopSink {
    pub calls: tokio::sync::Mutex<Vec<(PublishKey, Bytes)>>,
}

impl NoopSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn take_calls(&self) -> Vec<(PublishKey, Bytes)> {
        std::mem::take(&mut *self.calls.lock().await)
    }
}

#[async_trait::async_trait]
impl FanoutSink for NoopSink {
    async fn publish(&self, key: &PublishKey, payload: Bytes) -> Result<(), FanoutError> {
        self.calls.lock().await.push((key.clone(), payload));
        Ok(())
    }
}

/// Production publisher that fans out to NATS + Redis simultaneously.
#[derive(Debug, Clone)]
pub struct FanoutPublisher {
    nats: async_nats::Client,
    redis: Arc<redis::Client>,
    snapshot_ttl_seconds: Option<u64>,
}

impl FanoutPublisher {
    /// Connects to NATS and Redis using the provided URLs. `snapshot_ttl`
    /// is applied to every Redis write so stale snapshots eventually
    /// disappear if the daemon crashes mid-rebuild.
    pub async fn connect(
        nats_url: &str,
        redis_url: &str,
        snapshot_ttl_seconds: Option<u64>,
    ) -> Result<Self, FanoutError> {
        let nats = async_nats::connect(nats_url).await?;
        let redis = Arc::new(redis::Client::open(redis_url)?);
        Ok(Self {
            nats,
            redis,
            snapshot_ttl_seconds,
        })
    }
}

#[async_trait::async_trait]
impl FanoutSink for FanoutPublisher {
    async fn publish(&self, key: &PublishKey, payload: Bytes) -> Result<(), FanoutError> {
        let subject = key.nats_subject();
        let redis_key = key.redis_key();
        let payload_for_nats = payload.clone();

        // Fire both writes concurrently. NATS is the streaming path so we
        // measure its publish latency; Redis is the snapshot path so we
        // measure that separately.
        let nats_fut = async {
            let started = std::time::Instant::now();
            let res = self.nats.publish(subject, payload_for_nats).await;
            metrics::histogram!(
                "fanout_nats_publish_duration_seconds",
                "kind" => key.kind.as_str(),
            )
            .record(started.elapsed().as_secs_f64());
            res
        };

        let redis_fut = async {
            let started = std::time::Instant::now();
            let mut conn = self.redis.get_multiplexed_async_connection().await?;
            let mut cmd = redis::cmd("SET");
            cmd.arg(&redis_key).arg(payload.as_ref());
            if let Some(ttl) = self.snapshot_ttl_seconds {
                cmd.arg("EX").arg(ttl);
            }
            let res: Result<(), redis::RedisError> = cmd.query_async(&mut conn).await;
            metrics::histogram!(
                "fanout_redis_snapshot_duration_seconds",
                "kind" => key.kind.as_str(),
            )
            .record(started.elapsed().as_secs_f64());
            res
        };

        let (nats_res, redis_res) = tokio::join!(nats_fut, redis_fut);

        if let Err(e) = nats_res {
            metrics::counter!("fanout_nats_publish_errors_total").increment(1);
            return Err(FanoutError::Nats(e));
        }
        if let Err(e) = redis_res {
            metrics::counter!("fanout_redis_snapshot_errors_total").increment(1);
            return Err(FanoutError::Redis(e));
        }

        metrics::counter!(
            "fanout_publishes_total",
            "kind" => key.kind.as_str(),
        )
        .increment(1);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Kind;

    #[tokio::test]
    async fn noop_sink_records_every_call() {
        let sink = NoopSink::new();
        sink.publish(
            &PublishKey::new(Kind::Quote, Bytes::from_static(b"PETR4")),
            Bytes::from_static(b"payload-1"),
        )
        .await
        .unwrap();
        sink.publish(
            &PublishKey::new(Kind::Book, Bytes::from_static(b"VALE3")),
            Bytes::from_static(b"payload-2"),
        )
        .await
        .unwrap();

        let calls = sink.take_calls().await;
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0.ticker.as_ref(), b"PETR4");
        assert_eq!(calls[0].1.as_ref(), b"payload-1");
        assert_eq!(calls[1].0.kind, Kind::Book);
    }

    #[tokio::test]
    async fn noop_sink_take_calls_drains() {
        let sink = NoopSink::new();
        sink.publish(
            &PublishKey::new(Kind::Trade, Bytes::from_static(b"PETR4")),
            Bytes::from_static(b"x"),
        )
        .await
        .unwrap();
        assert_eq!(sink.take_calls().await.len(), 1);
        // Second drain is empty.
        assert_eq!(sink.take_calls().await.len(), 0);
    }
}
