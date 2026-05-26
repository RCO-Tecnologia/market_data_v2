//! Connection configuration.
//!
//! Tuned per `ARCHITECTURE.md §5.1` and §11.5 (deployment tuning checklist).
//! Defaults match production targets; tests can override individual fields.

use std::time::Duration;

/// Configuration consumed by [`crate::connection::Connection::open`].
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// `host:port` of the Cedro server. Default port is 81.
    pub host: String,
    pub port: u16,

    /// Software key (line 1 of the handshake). Empty if not contracted.
    pub software_key: String,
    pub username: String,
    pub password: String,

    /// `SO_RCVBUF` size in bytes. Must be ≤ `sysctl net.core.rmem_max`.
    /// See ARCHITECTURE §5.1 — default 128MB.
    pub so_rcvbuf: usize,

    /// Capacity of the frame channel between reader thread and parser pool.
    /// 2M is the documented value (§5.1).
    pub frame_channel_capacity: usize,

    /// Per-step handshake timeout. Spec recommends 10s.
    pub handshake_timeout: Duration,

    /// Optional CPU core to pin the reader thread to. `None` = no pinning
    /// (useful in tests).
    pub reader_cpu_affinity: Option<usize>,

    /// Optional CPU core to pin the watchdog thread to. `None` = no pinning.
    pub watchdog_cpu_affinity: Option<usize>,

    /// How often the watchdog samples `ioctl FIONREAD` on the socket.
    pub watchdog_interval: Duration,

    /// Fraction (0.0..=1.0) of `so_rcvbuf` above which the watchdog emits
    /// a warning metric. Default 0.20 per ARCHITECTURE §5.1.2.
    pub backpressure_warn_ratio: f64,

    /// Fraction above which the daemon should self-terminate cleanly.
    /// Default 0.40 per ARCHITECTURE §5.1.2.
    pub backpressure_panic_ratio: f64,
}

impl ConnectionConfig {
    pub fn new(
        host: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            host: host.into(),
            port: 81,
            software_key: String::new(),
            username: username.into(),
            password: password.into(),
            so_rcvbuf: 128 * 1024 * 1024,
            frame_channel_capacity: 2 * 1024 * 1024,
            handshake_timeout: Duration::from_secs(10),
            reader_cpu_affinity: None,
            watchdog_cpu_affinity: None,
            watchdog_interval: Duration::from_millis(100),
            backpressure_warn_ratio: 0.20,
            backpressure_panic_ratio: 0.40,
        }
    }

    /// Returns the `(host, port)` pair as a single string.
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.host.is_empty() {
            return Err("host is empty");
        }
        if self.so_rcvbuf < 1024 {
            return Err("so_rcvbuf is unreasonably small");
        }
        if self.frame_channel_capacity == 0 {
            return Err("frame_channel_capacity must be > 0");
        }
        if !(0.0..=1.0).contains(&self.backpressure_warn_ratio) {
            return Err("backpressure_warn_ratio must be in [0,1]");
        }
        if !(0.0..=1.0).contains(&self.backpressure_panic_ratio) {
            return Err("backpressure_panic_ratio must be in [0,1]");
        }
        if self.backpressure_panic_ratio <= self.backpressure_warn_ratio {
            return Err("panic ratio must exceed warn ratio");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_validate() {
        let cfg = ConnectionConfig::new("127.0.0.1", "user", "pass");
        cfg.validate().unwrap();
    }

    #[test]
    fn warn_must_be_below_panic() {
        let mut cfg = ConnectionConfig::new("127.0.0.1", "u", "p");
        cfg.backpressure_warn_ratio = 0.5;
        cfg.backpressure_panic_ratio = 0.4;
        assert!(cfg.validate().is_err());
    }
}
