//! Service configuration. All parameters are validated eagerly: an illegal
//! configuration is rejected at startup rather than at first use.

use crate::error::{KvError, Result};
use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub data_dir: String,
    pub listen_addr: String,
    /// Maximum allowed key size in bytes (must be > 0).
    pub max_key_bytes: usize,
    /// Maximum allowed value size in bytes (must be > 0).
    pub max_value_bytes: usize,
    /// Maximum number of operations inside one transaction (must be > 0).
    pub max_txn_ops: usize,
    /// WAL size in bytes that triggers a snapshot compaction (must be > 0).
    pub compaction_threshold_bytes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            data_dir: "./data".into(),
            listen_addr: "0.0.0.0:8080".into(),
            max_key_bytes: 64 * 1024,
            max_value_bytes: 1024 * 1024,
            max_txn_ops: 1024,
            compaction_threshold_bytes: 16 * 1024 * 1024,
        }
    }
}

fn parse_positive<T>(name: &str, raw: &str) -> Result<T>
where
    T: TryFrom<i64> + std::str::FromStr,
    T::Error: std::fmt::Display,
{
    let value: i64 = raw.parse().map_err(|_| {
        KvError::InvalidConfig(format!("{name}={raw:?} is not a valid integer"))
    })?;
    if value <= 0 {
        return Err(KvError::InvalidConfig(format!(
            "{name} must be positive, got {value}"
        )));
    }
    match T::try_from(value) {
        Ok(v) => Ok(v),
        Err(_) => Err(KvError::InvalidConfig(format!("{name}={value} out of range"))),
    }
}

impl Config {
    pub fn load_from<V, S>(vars: V) -> Result<Self>
    where
        V: IntoIterator<Item = (S, S)>,
        S: AsRef<str>,
    {
        let mut cfg = Config::default();
        for (k, v) in vars {
            match k.as_ref() {
                "DATA_DIR" => cfg.data_dir = v.as_ref().to_string(),
                "LISTEN_ADDR" => cfg.listen_addr = v.as_ref().to_string(),
                "MAX_KEY_BYTES" => cfg.max_key_bytes = parse_positive("MAX_KEY_BYTES", v.as_ref())?,
                "MAX_VALUE_BYTES" => {
                    cfg.max_value_bytes = parse_positive("MAX_VALUE_BYTES", v.as_ref())?
                }
                "MAX_TXN_OPS" => cfg.max_txn_ops = parse_positive("MAX_TXN_OPS", v.as_ref())?,
                "COMPACTION_THRESHOLD_BYTES" => {
                    cfg.compaction_threshold_bytes =
                        parse_positive("COMPACTION_THRESHOLD_BYTES", v.as_ref())?
                }
                other => {
                    return Err(KvError::InvalidConfig(format!("unknown config key: {other}")))
                }
            }
        }
        if cfg.data_dir.trim().is_empty() {
            return Err(KvError::InvalidConfig("DATA_DIR must not be empty".into()));
        }
        if cfg.listen_addr.trim().is_empty() {
            return Err(KvError::InvalidConfig("LISTEN_ADDR must not be empty".into()));
        }
        Ok(cfg)
    }

    /// Load configuration from process environment variables. Only the known
    /// configuration keys are consulted; other environment variables are
    /// ignored.
    pub fn from_env() -> Result<Self> {
        const KNOWN: &[&str] = &[
            "DATA_DIR",
            "LISTEN_ADDR",
            "MAX_KEY_BYTES",
            "MAX_VALUE_BYTES",
            "MAX_TXN_OPS",
            "COMPACTION_THRESHOLD_BYTES",
        ];
        let mut pairs: Vec<(String, String)> = Vec::new();
        for key in KNOWN {
            if let Ok(value) = env::var(key) {
                pairs.push(((*key).to_string(), value));
            }
        }
        Self::load_from(pairs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn defaults_are_valid() {
        assert!(Config::load_from(vars(&[])).is_ok());
    }

    #[test]
    fn zero_and_negative_thresholds_are_rejected() {
        for bad in ["0", "-1", "-1024"] {
            let err =
                Config::load_from(vars(&[("COMPACTION_THRESHOLD_BYTES", bad)])).unwrap_err();
            assert!(matches!(err, KvError::InvalidConfig(_)), "{bad}");
        }
    }

    #[test]
    fn zero_limits_are_rejected() {
        assert!(matches!(
            Config::load_from(vars(&[("MAX_KEY_BYTES", "0")])).unwrap_err(),
            KvError::InvalidConfig(_)
        ));
        assert!(matches!(
            Config::load_from(vars(&[("MAX_TXN_OPS", "-3")])).unwrap_err(),
            KvError::InvalidConfig(_)
        ));
        assert!(matches!(
            Config::load_from(vars(&[("MAX_VALUE_BYTES", "not-a-number")])).unwrap_err(),
            KvError::InvalidConfig(_)
        ));
    }

    #[test]
    fn unknown_config_is_rejected() {
        assert!(matches!(
            Config::load_from(vars(&[("TYPO", "1")])).unwrap_err(),
            KvError::InvalidConfig(_)
        ));
    }
}
