//! Process configuration parsed from environment variables.
//!
//! Every numeric/structural parameter is validated eagerly at startup:
//! an invalid configuration aborts boot with [`ConfigError`] rather than
//! failing later at runtime.

use std::env;

/// Runtime limits, all enforced before any transaction touches the store.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Maximum key length in bytes (keys must also be non-empty).
    pub max_key_bytes: usize,
    /// Maximum value length in bytes.
    pub max_value_bytes: usize,
    /// Maximum number of operations inside one transaction (>= 1).
    pub max_ops_per_txn: usize,
}

/// Fully validated process configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Directory holding WAL segments and snapshots.
    pub data_dir: String,
    /// Bind address of the HTTP server.
    pub listen_addr: String,
    /// Size threshold in bytes: when the total on-disk WAL size grows past
    /// this value, a background snapshot + compaction is scheduled.
    pub wal_compact_threshold: u64,
    /// Seed the store with the built-in demo workload on first start.
    pub seed_on_fresh: bool,
    pub limits: Limits,
}

/// Configuration error reported (and fatally rejected) at startup.
#[derive(Debug)]
pub struct ConfigError {
    pub var: String,
    pub value: String,
    pub reason: String,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid configuration: {}={:?}: {}",
            self.var, self.value, self.reason
        )
    }
}

impl Config {
    /// Build configuration from process environment, using defaults for
    /// unset variables and rejecting invalid values.
    pub fn from_env() -> Result<Config, ConfigError> {
        let data_dir = env::var("DATA_DIR").unwrap_or_else(|_| "./data".to_string());
        if data_dir.trim().is_empty() {
            return Err(ConfigError {
                var: "DATA_DIR".to_string(),
                value: data_dir,
                reason: "must not be empty".to_string(),
            });
        }

        let listen_addr = env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
        if listen_addr.trim().is_empty() {
            return Err(ConfigError {
                var: "LISTEN_ADDR".to_string(),
                value: listen_addr,
                reason: "must not be empty".to_string(),
            });
        }

        let wal_compact_threshold = parse_positive_u64("WAL_COMPACT_THRESHOLD", 4 * 1024 * 1024)?;

        let max_key_bytes = parse_positive_usize("MAX_KEY_BYTES", 64 * 1024)?;
        let max_value_bytes = parse_positive_usize("MAX_VALUE_BYTES", 16 * 1024 * 1024)?;
        let max_ops_per_txn = parse_positive_usize("MAX_OPS_PER_TXN", 1024)?;

        let seed_on_fresh = match env::var("SEED_ON_FRESH") {
            Ok(v) => parse_bool("SEED_ON_FRESH", &v)?,
            Err(_) => true,
        };

        Ok(Config {
            data_dir,
            listen_addr,
            wal_compact_threshold,
            seed_on_fresh,
            limits: Limits {
                max_key_bytes,
                max_value_bytes,
                max_ops_per_txn,
            },
        })
    }
}

fn parse_bool(var: &str, raw: &str) -> Result<bool, ConfigError> {
    match raw.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(ConfigError {
            var: var.to_string(),
            value: raw.to_string(),
            reason: "expected one of true/false, 1/0, yes/no, on/off".to_string(),
        }),
    }
}

fn parse_positive_u64(var: &str, default: u64) -> Result<u64, ConfigError> {
    let raw = match env::var(var) {
        Ok(v) => v,
        Err(_) => return Ok(default),
    };
    match raw.parse::<u64>() {
        Ok(n) if n > 0 => Ok(n),
        Ok(_) => Err(ConfigError {
            var: var.to_string(),
            value: raw,
            reason: "must be strictly greater than zero".to_string(),
        }),
        Err(_) => Err(ConfigError {
            var: var.to_string(),
            value: raw,
            reason: "must be a positive integer".to_string(),
        }),
    }
}

fn parse_positive_usize(var: &str, default: usize) -> Result<usize, ConfigError> {
    let raw = match env::var(var) {
        Ok(v) => v,
        Err(_) => return Ok(default),
    };
    match raw.parse::<usize>() {
        Ok(n) if n > 0 => Ok(n),
        Ok(_) => Err(ConfigError {
            var: var.to_string(),
            value: raw,
            reason: "must be strictly greater than zero".to_string(),
        }),
        Err(_) => Err(ConfigError {
            var: var.to_string(),
            value: raw,
            reason: "must be a positive integer".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_threshold_is_rejected() {
        env::set_var("WAL_COMPACT_THRESHOLD", "0");
        let err = Config::from_env().unwrap_err();
        assert_eq!(err.var, "WAL_COMPACT_THRESHOLD");
        env::remove_var("WAL_COMPACT_THRESHOLD");
    }

    #[test]
    fn negative_threshold_is_rejected() {
        env::set_var("WAL_COMPACT_THRESHOLD", "-5");
        assert!(Config::from_env().is_err());
        env::remove_var("WAL_COMPACT_THRESHOLD");
    }

    #[test]
    fn garbage_value_is_rejected() {
        env::set_var("MAX_OPS_PER_TXN", "lots");
        assert!(Config::from_env().is_err());
        env::remove_var("MAX_OPS_PER_TXN");
    }

    #[test]
    fn valid_values_accepted() {
        env::set_var("WAL_COMPACT_THRESHOLD", "1024");
        env::set_var("MAX_KEY_BYTES", "32");
        env::set_var("MAX_VALUE_BYTES", "1024");
        env::set_var("MAX_OPS_PER_TXN", "8");
        let cfg = Config::from_env().unwrap();
        assert_eq!(cfg.wal_compact_threshold, 1024);
        assert_eq!(cfg.limits.max_key_bytes, 32);
        assert_eq!(cfg.limits.max_ops_per_txn, 8);
        env::remove_var("WAL_COMPACT_THRESHOLD");
        env::remove_var("MAX_KEY_BYTES");
        env::remove_var("MAX_VALUE_BYTES");
        env::remove_var("MAX_OPS_PER_TXN");
    }
}
