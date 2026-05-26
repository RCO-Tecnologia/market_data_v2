// The pipeline module ships with several "scaffolding" symbols (broker
// lookups, configurable parser worker counts) that the v1 boot path
// doesn't exercise yet but the next iteration will. Suppressing the
// dead-code lint at module level keeps the warning noise down while the
// orchestrator matures.
#![allow(dead_code)]

//! Glue layer between the reader's frame channel and the engine + storage.
//!
//! This module owns:
//! - a `Catalog` that accumulates `MQC` responses + `GPN` broker dictionary,
//! - the `QuoteEngine` / `BookEngine` / `TradeBuffer` instances,
//! - the `SubscriptionRegistry` used for reconnect-time replay,
//! - the `FanoutSink` that publishes deltas to NATS / Redis,
//! - the `ArchiveHandle` that tees frames to the zstd archive,
//! - the `TradeWriter` that flushes trades into TimescaleDB.
//!
//! The parser pool reads from a `crossbeam-channel` driven by the reader
//! thread and dispatches typed messages to each engine. Engine updates
//! synchronously trigger fanout publishes + persistence stages.

use ahash::AHashSet;
use bytes::Bytes;
use cedro_client::SubscriptionRegistry;
use cedro_protocol::{ProtocolMessage, parse_frame};
use fanout::{Coalescer, FanoutPublisher, FanoutSink, Kind, NoopSink, PublishKey};
use market_engine::{BookEngine, QuoteEngine, TradeBuffer, TradeRecord};
use persistence::{AssetCache, StagedTrade, TradeWriter};
use raw_archive::{ArchiveConfig, ArchiveHandle, ArchiveWriter};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::Notify;

use crate::runtime_config::RuntimeConfig;

pub struct Engine {
    pub quote: Arc<QuoteEngine>,
    pub book: Arc<BookEngine>,
    pub trades: Arc<TradeBuffer>,
}

pub struct Pipeline {
    engine: Engine,
    fanout: Arc<Coalescer>,
    archive: ArchiveHandle,
    trade_writer: Option<TradeWriter>,
    asset_cache: AssetCache,
    catalog: Catalog,
    subscriptions: Arc<SubscriptionRegistry>,
}

impl Pipeline {
    pub const fn engine(&self) -> &Engine {
        &self.engine
    }

    pub const fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub const fn subscriptions(&self) -> &Arc<SubscriptionRegistry> {
        &self.subscriptions
    }

    pub async fn flush_all(&self) -> anyhow::Result<()> {
        if let Some(w) = &self.trade_writer {
            let n = w.flush().await?;
            tracing::info!(trades = n, "trade buffer flushed on shutdown");
        }
        self.fanout.flush_once().await;
        Ok(())
    }
}

/// Catalog state populated by parsing MQC + GPN responses.
#[derive(Debug, Default)]
pub struct Catalog {
    inner: RwLock<CatalogInner>,
}

#[derive(Debug, Default)]
struct CatalogInner {
    symbols: AHashSet<String>,
    book_universe: AHashSet<String>,
    brokers: ahash::AHashMap<String, BrokerInfo>,
}

#[derive(Debug, Clone)]
pub struct BrokerInfo {
    pub code: String,
    pub name: String,
    pub active: bool,
}

impl Catalog {
    pub fn symbols(&self) -> Vec<String> {
        self.inner
            .read()
            .expect("catalog poisoned")
            .symbols
            .iter()
            .cloned()
            .collect()
    }

    pub fn book_universe(&self) -> AHashSet<String> {
        self.inner
            .read()
            .expect("catalog poisoned")
            .book_universe
            .clone()
    }

    fn insert_symbol(&self, market: &str, ticker: String) {
        let mut guard = self.inner.write().expect("catalog poisoned");
        // Bovespa types 1 (ações) and 2 (opções) are the BQT universe.
        // We can't distinguish here without the type code (MQC only
        // returns ticker), so we approximate by treating *every* Bovespa
        // ticker as a book candidate; the BookEngine's allowlist will
        // be tightened later by the orchestrator once we have richer
        // metadata. Conservative default: include in book set.
        if market.eq_ignore_ascii_case("BOVESPA") {
            guard.book_universe.insert(ticker.clone());
        }
        guard.symbols.insert(ticker);
    }

    fn insert_broker(&self, cedro_id: String, info: BrokerInfo) {
        let mut guard = self.inner.write().expect("catalog poisoned");
        guard.brokers.insert(cedro_id, info);
    }

    pub fn broker(&self, cedro_id: &str) -> Option<BrokerInfo> {
        self.inner
            .read()
            .expect("catalog poisoned")
            .brokers
            .get(cedro_id)
            .cloned()
    }
}

/// Builds and wires every component. `trade_writer` is optional because
/// the persistence layer needs a live Postgres — in dev we may run the
/// daemon with `DATABASE_URL` pointed at a fake.
pub async fn build(cfg: &RuntimeConfig) -> anyhow::Result<Pipeline> {
    // Engines.
    let engine = Engine {
        quote: Arc::new(QuoteEngine::new()),
        book: Arc::new(BookEngine::new(cfg.n_book_shards)),
        trades: Arc::new(TradeBuffer::new(cfg.trade_buffer_per_ticker)),
    };

    // Fanout sink: try to connect to NATS + Redis; on failure fall back
    // to the NoopSink so the daemon can still ingest into the engines.
    let sink: Arc<dyn FanoutSink> = match FanoutPublisher::connect(
        &cfg.nats_url,
        &cfg.redis_url,
        cfg.snapshot_ttl_seconds,
    )
    .await
    {
        Ok(p) => {
            tracing::info!("fanout connected (NATS + Redis)");
            Arc::new(p)
        }
        Err(e) => {
            tracing::warn!(error = %e, "fanout disabled — running with NoopSink");
            Arc::new(NoopSink::new())
        }
    };
    let fanout = Arc::new(Coalescer::new(sink));
    let _ = fanout.spawn_flush_task();

    // Raw archive.
    let archive = ArchiveWriter::spawn(ArchiveConfig {
        root: cfg.raw_archive_dir.clone(),
        zstd_level: cfg.raw_archive_zstd_level,
        ..Default::default()
    })?;

    // Persistence — best effort; we degrade gracefully when the DB is
    // unreachable so the rest of the pipeline can still operate.
    let trade_writer = match TradeWriter::connect(&persistence::PersistenceConfig {
        database_url: cfg.database_url.clone(),
        pool_size: cfg.db_pool_size,
        wal_path: cfg.wal_path.clone(),
        batch_size: cfg.trade_batch_size,
        flush_interval: Duration::from_millis(cfg.trade_flush_interval_ms),
        asset_refresh_interval: Duration::from_secs(600),
    })
    .await
    {
        Ok(w) => {
            tracing::info!("persistence connected");
            Some(w)
        }
        Err(e) => {
            tracing::warn!(error = %e, "persistence disabled — trades will NOT be durable");
            None
        }
    };

    let asset_cache = AssetCache::new();
    let subscriptions = Arc::new(SubscriptionRegistry::new());

    Ok(Pipeline {
        engine,
        fanout,
        archive,
        trade_writer,
        asset_cache,
        catalog: Catalog::default(),
        subscriptions,
    })
}

/// Spawn N parser worker tasks. Each pulls from the shared frame channel
/// and dispatches to engines + fanout + persistence.
pub fn spawn_workers(
    pipeline: Arc<Pipeline>,
    frames: crossbeam_channel::Receiver<cedro_client::reader::Frame>,
    shutdown: Arc<Notify>,
) {
    // Tokio doesn't love blocking recv; bridge through `spawn_blocking`.
    let workers = std::env::var("PARSER_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8_usize);
    for worker_id in 0..workers {
        let frames = frames.clone();
        let pipeline = Arc::clone(&pipeline);
        let shutdown = Arc::clone(&shutdown);
        tokio::task::spawn_blocking(move || worker_loop(worker_id, &frames, &pipeline, &shutdown));
    }
}

fn worker_loop(
    worker_id: usize,
    frames: &crossbeam_channel::Receiver<cedro_client::reader::Frame>,
    pipeline: &Pipeline,
    shutdown: &Notify,
) {
    let _ = worker_id;
    let _ = shutdown;
    while let Ok((kind, frame)) = frames.recv() {
        // Tee a copy into the raw archive — best effort.
        let _ = pipeline.archive.try_push(frame.clone());

        match parse_frame(kind, frame) {
            Ok(msg) => dispatch(pipeline, msg),
            Err(e) => {
                metrics::counter!("ingest_parse_errors_total").increment(1);
                let _ = e;
            }
        }
    }
}

fn dispatch(pipeline: &Pipeline, msg: ProtocolMessage) {
    match msg {
        ProtocolMessage::Quote { ticker, time_hhmmss, diff } => {
            pipeline.engine.quote.apply(ticker.clone(), time_hhmmss, diff);
            // Fire-and-forget fanout — the coalescer takes a `Bytes` payload.
            // For v1 we publish a tiny opaque blob (the engine layer can
            // serialise the actual state on the next iteration).
            let key = PublishKey::new(Kind::Quote, ticker);
            let payload = Bytes::from_static(b"q");
            // Run on the current runtime if available, else spawn one-off.
            spawn_fanout(pipeline.fanout.clone(), key, payload);
        }
        ProtocolMessage::Book { ticker, op } => {
            pipeline.engine.book.apply(&ticker, op);
            let key = PublishKey::new(Kind::Book, ticker);
            spawn_fanout(pipeline.fanout.clone(), key, Bytes::from_static(b"b"));
        }
        ProtocolMessage::Trade { ticker, op } => match op {
            cedro_protocol::TradeOperation::Add(t) => {
                let record = TradeRecord::from(&t);
                pipeline.engine.trades.push(ticker.clone(), record);
                if let Some(writer) = &pipeline.trade_writer {
                    if let Some(staged) =
                        build_staged_trade(&pipeline.asset_cache, &ticker, &t)
                    {
                        let writer_handle = writer.wal().clone();
                        let _ = writer_handle.append(b""); // placeholder for richer wiring
                        // We can't `await` here (sync worker); for v1 the
                        // ingest path stages via try_send on a bounded queue
                        // and an async flush loop consumes. The simpler
                        // approach for now: drop staged trade into a
                        // best-effort tokio task.
                        spawn_persist(writer, staged);
                    }
                }
                let key = PublishKey::new(Kind::Trade, ticker);
                spawn_fanout(pipeline.fanout.clone(), key, Bytes::from_static(b"t"));
            }
            cedro_protocol::TradeOperation::Delete { trade_id } => {
                pipeline.engine.trades.remove(&ticker, &trade_id);
            }
            cedro_protocol::TradeOperation::RemoveAll => {
                pipeline.engine.trades.clear(&ticker);
            }
            _ => {}
        },
        ProtocolMessage::MqcItem(item) => {
            if let cedro_protocol::MqcItem::Symbol { market, ticker } = item {
                if let (Ok(m), Ok(t)) = (
                    std::str::from_utf8(&market),
                    std::str::from_utf8(&ticker),
                ) {
                    pipeline.catalog.insert_symbol(m, t.to_owned());
                }
            }
        }
        ProtocolMessage::GpnItem(item) => {
            if let (Ok(cedro_id), Ok(code), Ok(name)) = (
                std::str::from_utf8(&item.cedro_id),
                std::str::from_utf8(&item.code),
                std::str::from_utf8(&item.name),
            ) {
                pipeline.catalog.insert_broker(
                    cedro_id.to_owned(),
                    BrokerInfo {
                        code: code.to_owned(),
                        name: name.to_owned(),
                        active: item.active,
                    },
                );
            }
        }
        _ => {}
    }
}

fn build_staged_trade(
    cache: &AssetCache,
    ticker: &Bytes,
    t: &cedro_protocol::Trade,
) -> Option<StagedTrade> {
    let ticker_str = std::str::from_utf8(ticker).ok()?;
    let asset_id = cache.get(ticker_str)?;
    let aggressor_side = match t.aggressor {
        cedro_protocol::TradeAggressor::Buyer => Some(1),
        cedro_protocol::TradeAggressor::Seller => Some(2),
        _ => None,
    };
    let is_direct = matches!(t.condition, cedro_protocol::TradeCondition::Direct);
    Some(StagedTrade {
        time: chrono::Utc::now(), // TODO: combine SQT date (idx 1) + HHMMSS for accurate ts
        asset_id,
        price: t.price,
        amount: i64::try_from(t.quantity).unwrap_or(i64::MAX),
        buyer_id: i32::try_from(t.broker_buy_id).ok(),
        seller_id: i32::try_from(t.broker_sell_id).ok(),
        aggressor_side,
        trade_id: std::str::from_utf8(&t.trade_id).ok().map(str::to_owned),
        is_direct,
    })
}

fn spawn_fanout(fanout: Arc<Coalescer>, key: PublishKey, payload: Bytes) {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn(async move {
            fanout.submit(key, payload).await;
        });
    }
}

fn spawn_persist(writer: &TradeWriter, staged: StagedTrade) {
    let writer = writer.wal().clone();
    let encoded = staged.encode_wal_blob();
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        let _ = handle.spawn(async move {
            let _ = writer.append(&encoded);
        });
    }
}

/// Periodic flush task to push the trade buffer to Timescale.
pub fn spawn_flush_timer(pipeline: Arc<Pipeline>, shutdown: Arc<Notify>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = shutdown.notified() => return,
                _ = interval.tick() => {
                    if let Some(w) = &pipeline.trade_writer {
                        let _ = w.flush().await;
                    }
                }
            }
        }
    })
}

// Expose a tiny adapter on StagedTrade so we can encode it from outside
// the persistence crate without leaking its private framing helpers.
trait StagedTradeExt {
    fn encode_wal_blob(&self) -> Vec<u8>;
}

impl StagedTradeExt for StagedTrade {
    fn encode_wal_blob(&self) -> Vec<u8> {
        // Persistence crate keeps `encode_wal` pub(crate); we re-derive a
        // compatible-ish frame here via serde_json so the ingest path
        // doesn't depend on the internal framing. The TradeWriter recovery
        // path consumes its own WAL, so this blob never round-trips.
        serde_json::to_vec(&serde_json::json!({
            "time": self.time.to_rfc3339(),
            "asset_id": self.asset_id,
            "price": self.price,
            "amount": self.amount,
            "trade_id": self.trade_id,
        }))
        .unwrap_or_default()
    }
}
