//! Throughput benchmark for the hot parsing path.
//!
//! Validates the §1.1 target of sustaining ≥ 200k msg/s on the parser pool.
//! Run with `cargo bench -p cedro-protocol`.

use cedro_protocol::frame::{FrameKind, FrameSplitter};
use cedro_protocol::parser::parse_frame;
use criterion::{Criterion, criterion_group, criterion_main, BatchSize, BenchmarkId, Throughput};
use std::hint::black_box;

fn quote_frames(n: usize) -> Vec<bytes::Bytes> {
    (0..n)
        .map(|i| {
            // 80-character SQT frame — close to the realistic average payload.
            let frame = format!(
                "T:PETR{:04}:101758:2:{}.95:3:{}.93:4:{}.96:9:{}",
                i % 9999,
                40 + i % 60,
                40 + i % 60,
                40 + i % 60,
                100_000 + i,
            );
            bytes::Bytes::from(frame.into_bytes())
        })
        .collect()
}

fn book_frames(n: usize) -> Vec<bytes::Bytes> {
    (0..n)
        .map(|i| {
            let frame = format!(
                "B:VALE{:04}:A:{}:A:{}.99:{}:131:{:08}",
                i % 9999,
                i % 50,
                30 + i % 70,
                100 + (i % 1000),
                10_000_000 + i % 90_000_000,
            );
            bytes::Bytes::from(frame.into_bytes())
        })
        .collect()
}

fn bench_parse_quote(c: &mut Criterion) {
    let frames = quote_frames(10_000);
    let mut group = c.benchmark_group("parse_quote");
    group.throughput(Throughput::Elements(frames.len() as u64));
    group.bench_function("10k_sqt", |b| {
        b.iter_batched(
            || frames.clone(),
            |batch| {
                for frame in batch {
                    let msg = parse_frame(FrameKind::Bang, frame).expect("parse");
                    black_box(msg);
                }
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

fn bench_parse_book(c: &mut Criterion) {
    let frames = book_frames(10_000);
    let mut group = c.benchmark_group("parse_book");
    group.throughput(Throughput::Elements(frames.len() as u64));
    group.bench_function("10k_bqt", |b| {
        b.iter_batched(
            || frames.clone(),
            |batch| {
                for frame in batch {
                    let msg = parse_frame(FrameKind::Newline, frame).expect("parse");
                    black_box(msg);
                }
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

fn bench_framing_throughput(c: &mut Criterion) {
    // Build a single large blob of N concatenated SQT frames (terminated by !)
    // and time how fast the splitter can drain it.
    let mut group = c.benchmark_group("framing");
    for n in [1_000_usize, 10_000, 100_000] {
        let blob: Vec<u8> = quote_frames(n)
            .iter()
            .flat_map(|f| {
                let mut v = f.to_vec();
                v.push(b'!');
                v
            })
            .collect();
        group.throughput(Throughput::Bytes(blob.len() as u64));
        group.bench_with_input(BenchmarkId::new("split_then_drop", n), &blob, |b, blob| {
            b.iter(|| {
                let mut s = FrameSplitter::with_capacity(blob.len() + 1024);
                s.extend_from_slice(blob);
                let mut total = 0;
                s.drain(|_, frame| {
                    black_box(frame);
                    total += 1;
                });
                assert_eq!(total, n);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_parse_quote, bench_parse_book, bench_framing_throughput);
criterion_main!(benches);
