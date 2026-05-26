//! Engine throughput benchmarks — measure the cost of applying typed
//! protocol messages to the in-memory engines after parsing.

use bytes::Bytes;
use cedro_protocol::enums::Side;
use cedro_protocol::types::{BookEntry, BookOp, QuoteFieldId, QuoteFieldValue};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main, Throughput};
use market_engine::{BookEngine, QuoteEngine};

fn bench_quote_engine(c: &mut Criterion) {
    let mut group = c.benchmark_group("quote_engine");
    let updates = 100_000;
    group.throughput(Throughput::Elements(updates as u64));
    group.bench_function("100k_diffs_2k_tickers", |b| {
        b.iter_batched(
            || {
                let engine = QuoteEngine::new();
                (engine, build_quote_workload(updates, 2_000))
            },
            |(engine, workload)| {
                for (ticker, diff) in workload {
                    engine.apply(ticker, 1, diff);
                }
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

fn bench_book_engine_single_writer(c: &mut Criterion) {
    let mut group = c.benchmark_group("book_engine");
    let ops = 100_000;
    group.throughput(Throughput::Elements(ops as u64));
    group.bench_function("100k_add_ops_8_shards", |b| {
        b.iter_batched(
            || {
                let engine = BookEngine::new(8);
                (engine, build_book_workload(ops, 5_000))
            },
            |(engine, workload)| {
                for (ticker, op) in workload {
                    engine.apply(&ticker, op);
                }
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

fn build_quote_workload(
    n: usize,
    tickers: usize,
) -> Vec<(Bytes, Vec<(QuoteFieldId, QuoteFieldValue)>)> {
    let names: Vec<Bytes> = (0..tickers)
        .map(|i| Bytes::from(format!("T{i:05}")))
        .collect();
    (0..n)
        .map(|i| {
            let t = names[i % tickers].clone();
            let diff = vec![
                (QuoteFieldId(2), QuoteFieldValue::Float(40.0 + (i % 60) as f64 / 100.0)),
                (QuoteFieldId(9), QuoteFieldValue::Int(100_000 + i as i64)),
            ];
            (t, diff)
        })
        .collect()
}

fn build_book_workload(n: usize, tickers: usize) -> Vec<(Bytes, BookOp)> {
    let names: Vec<Bytes> = (0..tickers)
        .map(|i| Bytes::from(format!("B{i:05}")))
        .collect();
    (0..n)
        .map(|i| {
            let entry = BookEntry {
                position: (i % 50) as u32,
                side: if i % 2 == 0 { Side::Buy } else { Side::Sell },
                price: 30.0 + (i % 70) as f64 / 10.0,
                quantity: 100 + (i % 1000) as u64,
                broker_id: 131,
                timestamp_ddmmhhmm: 11_041_005,
                order_id: None,
                offer_type: None,
            };
            (names[i % tickers].clone(), BookOp::Add(entry))
        })
        .collect()
}

criterion_group!(benches, bench_quote_engine, bench_book_engine_single_writer);
criterion_main!(benches);
