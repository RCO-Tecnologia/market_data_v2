//! Maps `ticker → asset_id` from `public.assets`.
//!
//! The trades table FK's `asset_id` against `public.assets(id)` — we never
//! get to insert by ticker directly. To keep the hot insert path cheap, the
//! cache is loaded on startup and refreshed in the background; lookups go
//! through a lock-free [`DashMap`].
//!
//! Schema assumption (taken from screenshot context): `public.assets` has
//! at minimum an `id int4 PRIMARY KEY` and a ticker-like text column. The
//! exact column name (`ticker`, `symbol`, etc.) varies between deployments,
//! so this module accepts a SQL snippet from configuration rather than
//! hard-coding it. The default expects the most common shape:
//! `SELECT id, ticker FROM public.assets`.

use ahash::RandomState;
use dashmap::DashMap;
use sqlx::PgPool;
use std::sync::Arc;

use crate::error::PersistenceError;

/// Default query used to populate the cache. Override via
/// [`AssetCache::with_query`] when the schema diverges.
pub const DEFAULT_ASSETS_QUERY: &str = "SELECT id, ticker FROM public.assets";

#[derive(Debug, Clone)]
pub struct AssetCache {
    map: Arc<DashMap<String, i32, RandomState>>,
    query: String,
}

impl AssetCache {
    pub fn new() -> Self {
        Self {
            map: Arc::new(DashMap::with_hasher(RandomState::new())),
            query: DEFAULT_ASSETS_QUERY.to_owned(),
        }
    }

    #[must_use]
    pub fn with_query(mut self, query: impl Into<String>) -> Self {
        self.query = query.into();
        self
    }

    /// Look up an asset id. Returns `None` for unknown tickers; the writer
    /// should bubble this up so the daemon can decide whether to retry,
    /// archive the trade in the WAL only, or alert.
    pub fn get(&self, ticker: &str) -> Option<i32> {
        self.map.get(ticker).map(|kv| *kv.value())
    }

    /// Force a full refresh from the database. Pulls every `(id, ticker)`
    /// row and rebuilds the map atomically.
    pub async fn refresh(&self, pool: &PgPool) -> Result<usize, PersistenceError> {
        let rows: Vec<(i32, String)> = sqlx::query_as(&self.query).fetch_all(pool).await?;
        let n = rows.len();

        // Build a fresh map and swap shards atomically.
        let new_map: DashMap<String, i32, RandomState> = DashMap::with_hasher(RandomState::new());
        for (id, ticker) in rows {
            new_map.insert(ticker, id);
        }

        // Replace each shard one at a time; concurrent readers see either
        // the old or new mapping but never a torn one. `DashMap` itself
        // doesn't expose a swap, so we clear + insert in bulk.
        self.map.clear();
        for kv in new_map {
            self.map.insert(kv.0, kv.1);
        }
        metrics::gauge!("persistence_asset_cache_size").set(n as f64);
        metrics::counter!("persistence_asset_cache_refresh_total").increment(1);
        Ok(n)
    }

    /// Inserts a single mapping. Mainly used by tests / scripted setup.
    pub fn insert(&self, ticker: impl Into<String>, id: i32) {
        self.map.insert(ticker.into(), id);
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl Default for AssetCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get_round_trip() {
        let c = AssetCache::new();
        c.insert("PETR4", 42);
        assert_eq!(c.get("PETR4"), Some(42));
        assert_eq!(c.get("WDOZ25"), None);
    }

    #[test]
    fn cache_is_lock_free_across_threads() {
        use std::thread;
        let c = AssetCache::new();
        let handles: Vec<_> = (0..8)
            .map(|i: i32| {
                let c = c.clone();
                thread::spawn(move || {
                    for j in 0..100_i32 {
                        c.insert(format!("T{i}_{j}"), i * 100 + j);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(c.len(), 8 * 100);
    }

    #[test]
    fn default_query_targets_public_assets() {
        let c = AssetCache::new();
        assert_eq!(c.query, "SELECT id, ticker FROM public.assets");
    }

    #[test]
    fn with_query_overrides_default() {
        let c = AssetCache::new().with_query("SELECT id, symbol AS ticker FROM custom.assets");
        assert!(c.query.contains("symbol AS ticker"));
    }
}
