//! Ingest daemon — assembles every workspace crate into a running pipeline.
//!
//! High-level boot sequence (mirrors `ARCHITECTURE.md §7`):
//!
//! 1. Initialise tracing + Prometheus exporter via `observability::init_global`.
//! 2. Connect outbound dependencies: TimescaleDB pool, NATS, Redis.
//! 3. Open the Cedro TCP connection (handshake + reader).
//! 4. Spawn the raw-archive writer (every frame is teed off the reader).
//! 5. Bootstrap discovery: `GPN BOVESPA` + every required `MQC` query.
//! 6. Dispatch subscriptions in batches (`SQT` universal, `BQT` ações+opções
//!    Bovespa only, `GQT … S 50` universal).
//! 7. Spawn the parser pool consuming the bounded crossbeam channel.
//! 8. Wait for SIGTERM / SIGINT; gracefully flush trade buffer and exit.
//!
//! This file is *thin* — the heavy lifting lives in each crate. The job
//! of `main` is composition and shutdown choreography.

#![allow(clippy::print_stdout)] // top-level bin is allowed to talk to the operator

use anyhow::Context;
use cedro_client::{ConnectionConfig, Reader};
use observability::ObservabilityConfig;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::Notify;
use tracing::info;

mod bootstrap;
mod pipeline;
mod runtime_config;

use runtime_config::RuntimeConfig;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let cfg = RuntimeConfig::from_env().map_err(|e| anyhow::anyhow!("loading runtime config: {e}"))?;

    let _prom = observability::init_global(&ObservabilityConfig {
        default_filter: cfg.log_filter.clone(),
        json: cfg.log_json,
        prometheus_bind: Some(cfg.metrics_bind.clone()),
    })
    .context("initialising observability")?;

    info!(version = env!("CARGO_PKG_VERSION"), "ingest-daemon starting");

    // 1. Cedro connection + handshake.
    let cedro_cfg = ConnectionConfig {
        host: cfg.cedro_host.clone(),
        port: cfg.cedro_port,
        software_key: cfg.cedro_software_key.clone(),
        username: cfg.cedro_username.clone(),
        password: cfg.cedro_password.clone(),
        so_rcvbuf: cfg.so_rcvbuf,
        frame_channel_capacity: cfg.frame_channel_capacity,
        handshake_timeout: Duration::from_secs(10),
        reader_cpu_affinity: cfg.reader_cpu,
        watchdog_cpu_affinity: cfg.watchdog_cpu,
        watchdog_interval: Duration::from_millis(100),
        backpressure_warn_ratio: 0.20,
        backpressure_panic_ratio: 0.40,
    };

    let conn = cedro_client::Connection::open(&cedro_cfg)
        .context("opening Cedro connection")?;
    let (read_stream, cmd_sink, handle) = conn.into_halves();
    let cmd_sink = Arc::new(cmd_sink);

    // 2. Reader + watchdog threads.
    let reader_handle = Reader::spawn(read_stream, handle, &cedro_cfg)
        .context("spawning reader")?;
    let frame_rx = reader_handle.frames.clone();
    let panic_flag = reader_handle.panic_flag.clone();

    // 3. Pipeline (parser pool + engines + fanout sink).
    let pipeline = pipeline::build(&cfg).await.context("building pipeline")?;
    let pipeline = Arc::new(pipeline);

    // 4. Bootstrap: GPN → MQC → subscriptions.
    let universe = bootstrap::run(&cmd_sink, &pipeline)
        .await
        .context("bootstrap")?;
    info!(
        symbols = universe.total,
        book_symbols = universe.book_set.len(),
        "bootstrap complete; subscriptions issued"
    );

    // 5. Spawn parser workers (one tokio task per worker; std::thread reader
    //    is already running and feeding the channel).
    let shutdown = Arc::new(Notify::new());
    pipeline::spawn_workers(Arc::clone(&pipeline), frame_rx, Arc::clone(&shutdown));

    // 6. Periodic trade-buffer flush. The persistence layer also flushes
    //    on batch_size; this is the lower-bound timer.
    let flush_handle = pipeline::spawn_flush_timer(Arc::clone(&pipeline), Arc::clone(&shutdown));

    info!("ingest-daemon running; awaiting SIGTERM/SIGINT");
    wait_for_shutdown(panic_flag).await;
    info!("shutdown signal received; draining");

    shutdown.notify_waiters();
    let _ = flush_handle.await;

    if let Err(e) = pipeline.flush_all().await {
        tracing::error!(error = %e, "pipeline final flush failed");
    }

    reader_handle.shutdown();
    info!("ingest-daemon stopped cleanly");
    Ok(())
}

async fn wait_for_shutdown(panic_flag: Arc<std::sync::atomic::AtomicBool>) {
    let term = async {
        signal::ctrl_c().await.expect("ctrl-c handler");
    };
    let watchdog = async {
        // Poll the panic flag from the reader's watchdog at 100ms.
        loop {
            if panic_flag.load(std::sync::atomic::Ordering::SeqCst) {
                tracing::error!(
                    "reader watchdog tripped (kernel buffer >40% of SO_RCVBUF); exiting"
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::select! {
        () = term => {}
        () = watchdog => {}
    }
}
