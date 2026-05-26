//! Sharding helpers — deterministic `ticker → shard` routing.
//!
//! `ARCHITECTURE.md §5.5.2` requires that every message for a given ticker
//! reach the same engine worker so the book observes operations in order.
//! The hash function is `ahash` because it's 2-4× faster than `SipHash` and
//! we don't need `DoS` resistance here (the input space is `cedro-protocol`'s
//! own ticker stream, not adversarial user input).

use ahash::AHasher;
use std::hash::{BuildHasher, BuildHasherDefault, Hasher as _};

/// Routes a ticker to one of `n_shards` buckets.
///
/// Stable across runs: same ticker always lands in the same shard, so a
/// reconnect doesn't reshuffle in-flight state.
pub fn shard_for(ticker: &[u8], n_shards: usize) -> usize {
    debug_assert!(n_shards > 0, "n_shards must be non-zero");
    // We construct the hasher fresh per call rather than seeding with a
    // random key. AHash with the default seed is still fine for our case
    // (we want determinism, not cryptographic strength) and the per-call
    // cost is sub-nanosecond.
    let mut h = AHasher::default();
    h.write(ticker);
    (h.finish() as usize) % n_shards
}

/// Convenience: `BuildHasherDefault<AHasher>` paired with [`HashMap`] gives
/// us hash maps that are 2-4× faster than the standard ones.
pub type FastHasher = BuildHasherDefault<AHasher>;

/// Build a fresh hasher state — useful when you need to hash multiple
/// values consistently inside a single function.
#[must_use]
pub fn build_hasher() -> FastHasher {
    FastHasher::default()
}

/// Hash a single byte slice using the same algorithm as [`shard_for`].
/// Returned modulo the shard count so callers can index a `Vec`.
#[must_use]
pub fn shard_with(builder: &FastHasher, ticker: &[u8], n_shards: usize) -> usize {
    debug_assert!(n_shards > 0, "n_shards must be non-zero");
    let mut h = builder.build_hasher();
    h.write(ticker);
    (h.finish() as usize) % n_shards
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_ticker_always_maps_to_same_shard() {
        for n in [1, 2, 4, 8, 16, 64] {
            let a = shard_for(b"PETR4", n);
            let b = shard_for(b"PETR4", n);
            assert_eq!(a, b, "non-deterministic for n_shards={n}");
            assert!(a < n);
        }
    }

    #[test]
    fn different_tickers_can_share_a_shard() {
        // No assertion on distribution — just sanity that the function
        // handles many distinct tickers without panicking.
        let mut hits = vec![0usize; 8];
        for t in [
            "PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3", "ITSA4", "B3SA3", "WEGE3",
            "PRIO3", "RAIL3", "MGLU3", "VAMO3", "WDOZ25", "WINZ25",
        ] {
            hits[shard_for(t.as_bytes(), 8)] += 1;
        }
        // We trust ahash enough not to dump everything into one bucket;
        // require at least 3 of the 8 buckets to have content.
        let used = hits.iter().filter(|&&c| c > 0).count();
        assert!(used >= 3, "ahash distribution looked degenerate: {hits:?}");
    }

    #[test]
    fn shard_with_matches_shard_for() {
        let b = build_hasher();
        assert_eq!(shard_with(&b, b"PETR4", 16), shard_for(b"PETR4", 16));
        assert_eq!(shard_with(&b, b"WDOZ25", 8), shard_for(b"WDOZ25", 8));
    }
}
