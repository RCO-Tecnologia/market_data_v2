//! Subscription state needed for resubscribing after reconnect.
//!
//! `ARCHITECTURE.md §5.1` calls this out: when the socket drops we must
//! reapply the full subscription set as part of the bootstrap. This module
//! gives us a thread-safe registry keyed by `(kind, ticker)`.

use std::collections::HashSet;
use std::sync::RwLock;

/// What kind of subscription a (kind, ticker) pair represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionKind {
    /// `SQT` quote subscription.
    Quote,
    /// `BQT` detailed book — only ações + opções Bovespa per §5.5.2.
    BookDetailed,
    /// `SAB` aggregated book.
    BookAggregated,
    /// `GQT … S` trade stream.
    Trades,
}

impl SubscriptionKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Quote => "quote",
            Self::BookDetailed => "book_detailed",
            Self::BookAggregated => "book_aggregated",
            Self::Trades => "trades",
        }
    }
}

/// Concurrent set of active subscriptions.
///
/// The registry is intentionally minimal — no refcounts. Adding a kind that
/// is already present is a no-op (subscribing again on Cedro is documented
/// as safe per `api.md §1.2`). The engine layer is the place to add
/// refcounting if dynamic subscriptions become a thing.
#[derive(Debug, Default)]
pub struct SubscriptionRegistry {
    inner: RwLock<HashSet<(SubscriptionKind, String)>>,
}

impl SubscriptionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a subscription, returning `true` if it was new.
    pub fn add(&self, kind: SubscriptionKind, ticker: impl Into<String>) -> bool {
        let key = (kind, ticker.into());
        let mut guard = self.inner.write().expect("subscription registry poisoned");
        guard.insert(key)
    }

    /// Removes a subscription, returning `true` if it was previously present.
    pub fn remove(&self, kind: SubscriptionKind, ticker: &str) -> bool {
        let mut guard = self.inner.write().expect("subscription registry poisoned");
        guard.remove(&(kind, ticker.to_owned()))
    }

    /// Number of active subscriptions across all kinds.
    pub fn len(&self) -> usize {
        self.inner.read().expect("subscription registry poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot of every active subscription. The returned vector is
    /// owned, so the caller can iterate without holding the lock.
    pub fn snapshot(&self) -> Vec<(SubscriptionKind, String)> {
        let guard = self.inner.read().expect("subscription registry poisoned");
        guard.iter().cloned().collect()
    }

    /// Filter snapshot by subscription kind. Useful for bootstrap, where we
    /// emit commands grouped by kind for predictable rate-limiting.
    pub fn snapshot_by_kind(&self, kind: SubscriptionKind) -> Vec<String> {
        let guard = self.inner.read().expect("subscription registry poisoned");
        guard
            .iter()
            .filter(|(k, _)| *k == kind)
            .map(|(_, t)| t.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_returns_true_only_on_first_insert() {
        let r = SubscriptionRegistry::new();
        assert!(r.add(SubscriptionKind::Quote, "PETR4"));
        assert!(!r.add(SubscriptionKind::Quote, "PETR4"));
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn remove_returns_true_only_on_first_removal() {
        let r = SubscriptionRegistry::new();
        r.add(SubscriptionKind::BookDetailed, "VALE3");
        assert!(r.remove(SubscriptionKind::BookDetailed, "VALE3"));
        assert!(!r.remove(SubscriptionKind::BookDetailed, "VALE3"));
        assert!(r.is_empty());
    }

    #[test]
    fn snapshot_is_independent_of_subsequent_mutations() {
        let r = SubscriptionRegistry::new();
        r.add(SubscriptionKind::Quote, "A");
        r.add(SubscriptionKind::Quote, "B");
        let snap = r.snapshot();
        r.add(SubscriptionKind::Quote, "C");
        assert_eq!(snap.len(), 2);
        assert_eq!(r.len(), 3);
    }

    #[test]
    fn filter_by_kind() {
        let r = SubscriptionRegistry::new();
        r.add(SubscriptionKind::Quote, "PETR4");
        r.add(SubscriptionKind::Quote, "VALE3");
        r.add(SubscriptionKind::Trades, "PETR4");
        let quotes = r.snapshot_by_kind(SubscriptionKind::Quote);
        assert_eq!(quotes.len(), 2);
        assert!(quotes.contains(&"PETR4".to_owned()));
        assert!(quotes.contains(&"VALE3".to_owned()));
        let books = r.snapshot_by_kind(SubscriptionKind::BookDetailed);
        assert!(books.is_empty());
    }
}
