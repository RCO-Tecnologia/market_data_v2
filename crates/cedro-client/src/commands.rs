//! Outgoing-command emission to the Cedro socket.
//!
//! Every command we support is a line terminated by `\n`. The writes are
//! issued from threads other than the reader (e.g., the orchestrator that
//! batches subscriptions during bootstrap), so this layer takes ownership of
//! a `Write` handle behind a [`Mutex`] to serialise writes.
//!
//! The list of supported commands matches `api.md §4`, `§5`, `§6`, `§9` and
//! `mqc.md §1-2`. We treat ticker / market / id values as `&str` rather than
//! validating them — the caller is responsible for honouring server quirks
//! (e.g. id ≤ 14 chars, see `api.md §11.5`).

use crate::error::ClientError;
use std::io::Write;
use std::sync::Mutex;

/// Thread-safe handle for writing commands to the Cedro socket.
#[derive(Debug)]
pub struct CommandSink<W: Write> {
    inner: Mutex<W>,
}

impl<W: Write> CommandSink<W> {
    pub const fn new(writer: W) -> Self {
        Self {
            inner: Mutex::new(writer),
        }
    }

    /// Subscribe to a quote (`SQT <ticker>`). When `snapshot_only` is true,
    /// the suffix `N` is appended and the server replies with a single
    /// snapshot instead of opening a streaming subscription.
    pub fn subscribe_quote(&self, ticker: &str, snapshot_only: bool) -> Result<(), ClientError> {
        if snapshot_only {
            self.write_line(format!("SQT {ticker} N").as_bytes())
        } else {
            self.write_line(format!("SQT {ticker}").as_bytes())
        }
    }

    pub fn unsubscribe_quote(&self, ticker: &str) -> Result<(), ClientError> {
        self.write_line(format!("USQ {ticker}").as_bytes())
    }

    /// Subscribe to the detailed order book (`BQT <ticker>`).
    pub fn subscribe_book_detailed(&self, ticker: &str) -> Result<(), ClientError> {
        self.write_line(format!("BQT {ticker}").as_bytes())
    }

    pub fn unsubscribe_book_detailed(&self, ticker: &str) -> Result<(), ClientError> {
        self.write_line(format!("UBQ {ticker}").as_bytes())
    }

    /// Subscribe to the aggregated book (`SAB <ticker>`). When `snapshot_only`
    /// is true the suffix `N` requests a one-shot snapshot.
    pub fn subscribe_book_aggregated(
        &self,
        ticker: &str,
        snapshot_only: bool,
    ) -> Result<(), ClientError> {
        if snapshot_only {
            self.write_line(format!("SAB {ticker} N").as_bytes())
        } else {
            self.write_line(format!("SAB {ticker}").as_bytes())
        }
    }

    pub fn unsubscribe_book_aggregated(&self, ticker: &str) -> Result<(), ClientError> {
        self.write_line(format!("UAB {ticker}").as_bytes())
    }

    /// Subscribe to trade stream (`GQT <ticker> S [count]`).
    pub fn subscribe_trades(
        &self,
        ticker: &str,
        history_count: Option<u32>,
    ) -> Result<(), ClientError> {
        match history_count {
            Some(n) => self.write_line(format!("GQT {ticker} S {n}").as_bytes()),
            None => self.write_line(format!("GQT {ticker} S").as_bytes()),
        }
    }

    pub fn unsubscribe_trades(&self, ticker: &str) -> Result<(), ClientError> {
        self.write_line(format!("UQT {ticker}").as_bytes())
    }

    /// Request the catalog of tickers for a market — `mqc.md §1`.
    ///
    /// `subtype` is only used in the extended form (`MQC <market> T <type> S <subtype>`),
    /// required for BMF futures and options-on-futures partitions.
    pub fn list_market(
        &self,
        market: &str,
        asset_type: u16,
        subtype: Option<u16>,
    ) -> Result<(), ClientError> {
        match subtype {
            Some(s) => self.write_line(format!("MQC {market} T {asset_type} S {s}").as_bytes()),
            None => self.write_line(format!("MQC {market} {asset_type}").as_bytes()),
        }
    }

    /// Request the broker dictionary for a market — `mqc.md §2`.
    pub fn list_brokers(&self, market: &str) -> Result<(), ClientError> {
        self.write_line(format!("GPN {market}").as_bytes())
    }

    /// Subscribe to live news headlines from an agency — `api.md §7.2.1`.
    pub fn subscribe_news(&self, agency: &str) -> Result<(), ClientError> {
        self.write_line(format!("NEM A {agency}").as_bytes())
    }

    pub fn unsubscribe_news(&self, agency: &str) -> Result<(), ClientError> {
        self.write_line(format!("UNE {agency}").as_bytes())
    }

    /// Ask the server for its current wall-clock — `api.md §9.1`.
    pub fn get_server_time(&self) -> Result<(), ClientError> {
        self.write_line(b"GTC")
    }

    /// Closes the session cleanly. Cedro responds by tearing down the socket.
    pub fn quit(&self) -> Result<(), ClientError> {
        self.write_line(b"QUIT")
    }

    fn write_line(&self, payload: &[u8]) -> Result<(), ClientError> {
        // Acquire-lock-write-flush. Acceptable because the parser pool is
        // never on this path — orchestrator and engine threads are the only
        // writers, and they batch.
        let mut guard = self.inner.lock().expect("command sink mutex poisoned");
        let result: Result<(), ClientError> = (|| {
            guard.write_all(payload).map_err(ClientError::CommandWrite)?;
            guard.write_all(b"\n").map_err(ClientError::CommandWrite)?;
            guard.flush().map_err(ClientError::CommandWrite)?;
            Ok(())
        })();
        drop(guard);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sink() -> (CommandSink<Vec<u8>>, ()) {
        (CommandSink::new(Vec::new()), ())
    }

    fn take(sink: &CommandSink<Vec<u8>>) -> String {
        let mut guard = sink.inner.lock().unwrap();
        String::from_utf8(std::mem::take(&mut *guard)).unwrap()
    }

    #[test]
    fn sqt_normal_and_snapshot() {
        let (s, ()) = sink();
        s.subscribe_quote("PETR4", false).unwrap();
        s.subscribe_quote("PETR4", true).unwrap();
        assert_eq!(take(&s), "SQT PETR4\nSQT PETR4 N\n");
    }

    #[test]
    fn bqt_subscribe_and_unsubscribe() {
        let (s, ()) = sink();
        s.subscribe_book_detailed("PETR4").unwrap();
        s.unsubscribe_book_detailed("PETR4").unwrap();
        assert_eq!(take(&s), "BQT PETR4\nUBQ PETR4\n");
    }

    #[test]
    fn sab_with_snapshot_flag() {
        let (s, ()) = sink();
        s.subscribe_book_aggregated("PETR4", true).unwrap();
        assert_eq!(take(&s), "SAB PETR4 N\n");
    }

    #[test]
    fn gqt_with_and_without_history() {
        let (s, ()) = sink();
        s.subscribe_trades("PETR4", None).unwrap();
        s.subscribe_trades("PETR4", Some(50)).unwrap();
        assert_eq!(take(&s), "GQT PETR4 S\nGQT PETR4 S 50\n");
    }

    #[test]
    fn mqc_simple_and_extended_forms() {
        let (s, ()) = sink();
        s.list_market("Bovespa", 1, None).unwrap();
        s.list_market("BMF", 7, Some(130)).unwrap();
        assert_eq!(take(&s), "MQC Bovespa 1\nMQC BMF T 7 S 130\n");
    }

    #[test]
    fn gpn_and_gtc_and_quit() {
        let (s, ()) = sink();
        s.list_brokers("BOVESPA").unwrap();
        s.get_server_time().unwrap();
        s.quit().unwrap();
        assert_eq!(take(&s), "GPN BOVESPA\nGTC\nQUIT\n");
    }

    #[test]
    fn nem_subscribe_unsubscribe() {
        let (s, ()) = sink();
        s.subscribe_news("BOV").unwrap();
        s.unsubscribe_news("BOV").unwrap();
        assert_eq!(take(&s), "NEM A BOV\nUNE BOV\n");
    }
}
