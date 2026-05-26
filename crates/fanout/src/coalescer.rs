//! Per-(kind, ticker) coalescing publisher.
//!
//! On hot tickers we get hundreds of updates per second. Pushing every one
//! through the network is wasteful — and pushes the frontend past what it
//! can render anyway. The coalescer keeps the *latest* payload for each
//! [`PublishKey`] and flushes once per window (default 50 ms quote, 100 ms
//! book). Updates that arrive during the same window simply overwrite the
//! pending payload.
//!
//! The flush task runs as a Tokio task with a fixed interval. Submission
//! is non-blocking from the caller's perspective (a mutex-guarded `AHashMap`).

use crate::publisher::FanoutSink;
use crate::types::{Kind, PublishKey};
use ahash::AHashMap;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

/// Time-window coalescer wrapping an inner [`FanoutSink`].
#[derive(Debug)]
pub struct Coalescer {
    sink: Arc<dyn FanoutSink>,
    pending: Arc<Mutex<AHashMap<PublishKey, Bytes>>>,
    quote_window: Duration,
    book_window: Duration,
    trade_window: Duration,
}

impl Coalescer {
    pub fn new(sink: Arc<dyn FanoutSink>) -> Self {
        Self {
            sink,
            pending: Arc::new(Mutex::new(AHashMap::new())),
            quote_window: Duration::from_millis(50),
            book_window: Duration::from_millis(100),
            // Trades aren't coalesced by default (every trade matters), but
            // we keep a configurable window so the engine can request it.
            trade_window: Duration::from_millis(0),
        }
    }

    /// Overrides the window for a given kind. Setting `0` flushes on every
    /// submit (no batching).
    pub const fn set_window(&mut self, kind: Kind, window: Duration) {
        match kind {
            Kind::Quote => self.quote_window = window,
            Kind::Book => self.book_window = window,
            Kind::Trade => self.trade_window = window,
        }
    }

    /// Stage one update. If the window for this kind is 0 the publish runs
    /// immediately; otherwise the payload joins the pending bucket and
    /// gets sent on the next flush tick.
    pub async fn submit(&self, key: PublishKey, payload: Bytes) {
        let window = self.window_for(key.kind);
        if window.is_zero() {
            let _ = self.sink.publish(&key, payload).await;
            return;
        }
        let mut guard = self.pending.lock().await;
        guard.insert(key, payload);
    }

    /// Manually drain every pending payload. Used by tests + on shutdown
    /// to make sure no update is lost.
    pub async fn flush_once(&self) {
        let pending = {
            let mut guard = self.pending.lock().await;
            std::mem::take(&mut *guard)
        };
        for (key, payload) in pending {
            let _ = self.sink.publish(&key, payload).await;
        }
    }

    /// Launches the background flush task. The handle holds a strong Arc to
    /// the inner state and the future is cancel-safe — drop it to stop.
    pub fn spawn_flush_task(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            // Pick the smallest non-zero window as our tick rate, so every
            // kind is flushed at least once per its own window.
            let tick = me
                .min_nonzero_window()
                .unwrap_or(Duration::from_millis(50));
            let mut interval = tokio::time::interval(tick);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                me.flush_once().await;
            }
        })
    }

    const fn window_for(&self, kind: Kind) -> Duration {
        match kind {
            Kind::Quote => self.quote_window,
            Kind::Book => self.book_window,
            Kind::Trade => self.trade_window,
        }
    }

    fn min_nonzero_window(&self) -> Option<Duration> {
        [self.quote_window, self.book_window, self.trade_window]
            .into_iter()
            .filter(|d| !d.is_zero())
            .min()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publisher::NoopSink;

    fn key(kind: Kind, t: &'static [u8]) -> PublishKey {
        PublishKey::new(kind, Bytes::from_static(t))
    }

    #[tokio::test]
    async fn zero_window_kind_publishes_immediately() {
        let sink = Arc::new(NoopSink::new());
        let mut c = Coalescer::new(sink.clone());
        c.set_window(Kind::Trade, Duration::ZERO);

        c.submit(key(Kind::Trade, b"PETR4"), Bytes::from_static(b"t1"))
            .await;
        c.submit(key(Kind::Trade, b"PETR4"), Bytes::from_static(b"t2"))
            .await;

        let calls = sink.take_calls().await;
        // Both publish without coalescing.
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[1].1.as_ref(), b"t2");
    }

    #[tokio::test]
    async fn coalesces_consecutive_updates_to_same_key() {
        let sink = Arc::new(NoopSink::new());
        let c = Coalescer::new(sink.clone());

        c.submit(key(Kind::Quote, b"PETR4"), Bytes::from_static(b"v1"))
            .await;
        c.submit(key(Kind::Quote, b"PETR4"), Bytes::from_static(b"v2"))
            .await;
        c.submit(key(Kind::Quote, b"PETR4"), Bytes::from_static(b"v3"))
            .await;
        // Only the most recent payload survives the flush.
        c.flush_once().await;

        let calls = sink.take_calls().await;
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1.as_ref(), b"v3");
    }

    #[tokio::test]
    async fn distinct_keys_are_published_independently() {
        let sink = Arc::new(NoopSink::new());
        let c = Coalescer::new(sink.clone());

        c.submit(key(Kind::Quote, b"PETR4"), Bytes::from_static(b"p"))
            .await;
        c.submit(key(Kind::Quote, b"VALE3"), Bytes::from_static(b"v"))
            .await;
        c.submit(key(Kind::Book, b"PETR4"), Bytes::from_static(b"b"))
            .await;
        c.flush_once().await;

        let mut calls = sink.take_calls().await;
        calls.sort_by(|a, b| a.0.ticker.cmp(&b.0.ticker).then(a.0.kind.as_str().cmp(b.0.kind.as_str())));
        assert_eq!(calls.len(), 3);
    }

    #[tokio::test]
    async fn flush_clears_pending_bucket() {
        let sink = Arc::new(NoopSink::new());
        let c = Coalescer::new(sink.clone());

        c.submit(key(Kind::Quote, b"PETR4"), Bytes::from_static(b"v1"))
            .await;
        c.flush_once().await;
        c.flush_once().await; // second flush should be a noop

        let calls = sink.take_calls().await;
        assert_eq!(calls.len(), 1);
    }

    #[tokio::test]
    async fn spawn_flush_task_periodically_drains() {
        let sink = Arc::new(NoopSink::new());
        let mut coalescer = Coalescer::new(sink.clone());
        coalescer.set_window(Kind::Quote, Duration::from_millis(10));
        let c = Arc::new(coalescer);
        let handle = c.spawn_flush_task();

        c.submit(key(Kind::Quote, b"PETR4"), Bytes::from_static(b"v1"))
            .await;
        // Wait a few ticks then check that the sink received something.
        tokio::time::sleep(Duration::from_millis(60)).await;

        let calls = sink.take_calls().await;
        assert!(!calls.is_empty(), "background flusher should have run");
        handle.abort();
    }
}
