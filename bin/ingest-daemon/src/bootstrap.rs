//! Bootstrap orchestration: `GPN` + `MQC` + batched subscriptions.
//!
//! Mirrors the canonical sequence in `ARCHITECTURE.md §7`:
//!
//! 1. `GPN BOVESPA` (broker dictionary)
//! 2. `MQC Bovespa {1,2,3,10,13,20}` + `MQC BMF T 7 S {130,60,50,40,30,20}` +
//!    `MQC BMF T 2 S 150`
//! 3. Dedupe symbols + decide which ones get `BQT` (ações + opções Bovespa).
//! 4. Batch subscribe (~500 tickers/sec) with `SQT`, `BQT`, and `GQT S 50`.
//!
//! The MQC responses arrive as frames on the same channel as the live
//! stream, so we use the pipeline's existing parser dispatch — bootstrap
//! is simply about deciding *what to subscribe to* once the catalog has
//! filled in. The actual MQC frame collection is the pipeline's job.

use cedro_client::{CommandSink, SubscriptionKind};
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

use crate::pipeline::Pipeline;

#[derive(Debug, Default)]
pub struct Universe {
    pub total: usize,
    pub book_set: ahash::AHashSet<String>,
}

const MQC_QUERIES: &[(&str, u16, Option<u16>)] = &[
    ("Bovespa", 1, None),    // ações
    ("Bovespa", 2, None),    // opções
    ("Bovespa", 3, None),    // índices
    ("Bovespa", 10, None),   // fracionário
    ("Bovespa", 13, None),   // ETFs
    ("Bovespa", 20, None),   // corp
    ("BMF", 7, Some(130)),   // futuros sub 130
    ("BMF", 7, Some(60)),    // futuros sub 60
    ("BMF", 7, Some(50)),    // futuros sub 50
    ("BMF", 7, Some(40)),    // futuros sub 40
    ("BMF", 7, Some(30)),    // futuros sub 30
    ("BMF", 7, Some(20)),    // futuros sub 20
    ("BMF", 2, Some(150)),   // opções sobre futuros
];

/// Run the discovery + subscribe sequence end-to-end.
pub async fn run<W: Write + Send + 'static>(
    cmd: &Arc<CommandSink<W>>,
    pipeline: &Arc<Pipeline>,
) -> anyhow::Result<Universe> {
    // 1. Broker dictionary.
    cmd.list_brokers("BOVESPA")?;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 2. MQC queries (sequential with a small breath between to let
    // responses settle in the catalog buffer maintained by the pipeline).
    for (market, asset_type, subtype) in MQC_QUERIES {
        cmd.list_market(market, *asset_type, *subtype)?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Give the catalog time to populate. In production we'd watch the
    // pipeline's `mqc_end_for(market)` signal instead of timing it.
    tokio::time::sleep(Duration::from_secs(20)).await;

    // 3. Resolve the universe + book set.
    let universe_set = pipeline.catalog().symbols();
    let total = universe_set.len();
    let book_set = pipeline.catalog().book_universe();
    pipeline
        .engine()
        .book
        .set_allowed(Some(book_set.clone().into_iter().map(bytes::Bytes::from).collect()));

    // 4. Batch subscribe.
    info!(total, book = book_set.len(), "issuing subscriptions");
    let mut count = 0_usize;
    for ticker in &universe_set {
        cmd.subscribe_quote(ticker, false)?;
        if book_set.contains(ticker.as_str()) {
            cmd.subscribe_book_detailed(ticker)?;
        }
        cmd.subscribe_trades(ticker, Some(50))?;
        // Register in the subscription registry so reconnect can replay.
        pipeline.subscriptions().add(SubscriptionKind::Quote, ticker.clone());
        if book_set.contains(ticker.as_str()) {
            pipeline
                .subscriptions()
                .add(SubscriptionKind::BookDetailed, ticker.clone());
        }
        pipeline.subscriptions().add(SubscriptionKind::Trades, ticker.clone());
        count += 1;
        if count.is_multiple_of(500) {
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
    }

    Ok(Universe {
        total,
        book_set,
    })
}
