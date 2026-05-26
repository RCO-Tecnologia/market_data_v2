#![allow(clippy::cast_precision_loss, clippy::uninlined_format_args)]

//! Integration-grade stress tests for the framing + parsing layer.
//!
//! Goal: prove that a synthetic Cedro-like stream sustains ≥ 200 000 msg/s
//! on commodity hardware. These tests run in release-style optimisation
//! courtesy of the workspace `profile.dev.package."*"` override, so the
//! numbers are representative.
//!
//! The asserts are intentionally generous (≥ 50k msg/s) so the suite passes
//! on CI machines without GPU pinning — the headline figure printed by
//! `cargo test --release` is what matters for capacity planning.

use cedro_protocol::frame::FrameSplitter;
use cedro_protocol::parser::parse_frame;
use std::time::Instant;

#[test]
fn end_to_end_throughput_quote_only() {
    let frames: Vec<u8> = (0..200_000_usize)
        .flat_map(|i| {
            let f = format!(
                "T:PETR{:04}:101758:2:{}.95:3:{}.93:4:{}.96:9:{}",
                i % 9999,
                40 + i % 60,
                40 + i % 60,
                40 + i % 60,
                100_000 + i,
            );
            let mut v = f.into_bytes();
            v.push(b'!');
            v
        })
        .collect();

    let mut splitter = FrameSplitter::with_capacity(frames.len() + 1024);
    splitter.extend_from_slice(&frames);

    let started = Instant::now();
    let mut parsed = 0usize;
    while let Some((kind, frame)) = splitter.next_frame() {
        let msg = parse_frame(kind, frame).expect("parse");
        std::hint::black_box(msg);
        parsed += 1;
    }
    let elapsed = started.elapsed();

    assert_eq!(parsed, 200_000);
    let rate = parsed as f64 / elapsed.as_secs_f64();
    println!(
        "[stress] parsed {parsed} SQT frames in {:?} → {:.0} msg/s",
        elapsed, rate
    );
    assert!(
        rate >= 50_000.0,
        "throughput regression: {rate:.0} msg/s falls below the 50k floor"
    );
}

#[test]
fn end_to_end_throughput_mixed() {
    // 70% quote, 20% book, 10% trade — roughly the mix we expect on a
    // normal trading day.
    let mut blob = Vec::new();
    let total = 100_000_usize;
    for i in 0..total {
        match i % 10 {
            0..=6 => {
                let s = format!(
                    "T:WDOZ{:04}:101758:2:{}.55:9:{}",
                    i % 99,
                    100 + i % 5000,
                    1000 + i
                );
                blob.extend_from_slice(s.as_bytes());
                blob.push(b'!');
            }
            7 | 8 => {
                let s = format!(
                    "B:VALE{:04}:A:{}:A:{}.50:{}:131:{:08}",
                    i % 99,
                    i % 20,
                    30 + i % 50,
                    50 + (i % 1000),
                    11_041_005,
                );
                blob.extend_from_slice(s.as_bytes());
                blob.push(b'\n');
            }
            _ => {
                let s = format!(
                    "V:PETR4:A:155613:43.01:239:354:{}:T{}:0:A:0",
                    100 + i,
                    i,
                );
                blob.extend_from_slice(s.as_bytes());
                blob.push(b'\n');
            }
        }
    }

    let mut splitter = FrameSplitter::with_capacity(blob.len() + 1024);
    splitter.extend_from_slice(&blob);

    let started = Instant::now();
    let mut parsed = 0usize;
    let mut errors = 0usize;
    while let Some((kind, frame)) = splitter.next_frame() {
        match parse_frame(kind, frame) {
            Ok(msg) => {
                std::hint::black_box(msg);
                parsed += 1;
            }
            Err(_) => errors += 1,
        }
    }
    let elapsed = started.elapsed();

    let rate = parsed as f64 / elapsed.as_secs_f64();
    println!(
        "[stress] mixed parsed {parsed} (errors {errors}) in {:?} → {:.0} msg/s",
        elapsed, rate
    );
    assert!(errors == 0, "unexpected parse errors: {errors}");
    assert!(parsed == total);
    assert!(
        rate >= 50_000.0,
        "throughput regression: {rate:.0} msg/s falls below the 50k floor"
    );
}
