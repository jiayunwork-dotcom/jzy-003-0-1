//! Structured error types used across the store.
//!
//! Every error has a stable, machine-readable `kind()` so callers (including
//! the HTTP layer) can distinguish failure modes without parsing messages.

use std::fmt;

/// All errors produced by storage, WAL and configuration code.
#[derive(Debug)]
pub enum StoreError {
    /// Key is empty.
    EmptyKey,
    /// Key exceeds the configured `max_key_bytes`.
    KeyTooLarge { size: usize, max: usize },
    /// Value exceeds the configured `max_value_bytes`.
    ValueTooLarge { size: usize, max: usize },
    /// A transaction was submitted with zero operations.
    EmptyTransaction,
    /// A transaction contains more operations than `max_ops_per_txn`.
    TransactionTooLarge { count: usize, max: usize },
    /// A transaction touches the same key twice.
    DuplicateKeyInTransaction { key: Vec<u8> },
    /// A compare-and-swap check failed: the current value is not the
    /// expected value. Both values are base64-encoded for display.
    CasConflict {
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        actual: Option<Vec<u8>>,
    },
    /// A compaction/snapshot is already in progress.
    CompactionInProgress,
    /// Persistent data on disk is corrupt.
    Corruption(String),
    /// An operating-system level I/O failure.
    Io(std::io::Error),
}

impl StoreError {
    /// Stable error kind identifier used in structured error responses.
    pub fn kind(&self) -> &'static str {
        match self {
            StoreError::EmptyKey => "empty_key",
            StoreError::KeyTooLarge { .. } => "key_too_large",
            StoreError::ValueTooLarge { .. } => "value_too_large",
            StoreError::EmptyTransaction => "empty_transaction",
            StoreError::TransactionTooLarge { .. } => "transaction_too_large",
            StoreError::DuplicateKeyInTransaction { .. } => "duplicate_key_in_transaction",
            StoreError::CasConflict { .. } => "cas_conflict",
            StoreError::CompactionInProgress => "compaction_in_progress",
            StoreError::Corruption(_) => "corruption",
            StoreError::Io(_) => "io_error",
        }
    }
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::EmptyKey => write!(f, "key must not be empty"),
            StoreError::KeyTooLarge { size, max } => write!(
                f,
                "key size {size} bytes exceeds the configured maximum of {max} bytes"
            ),
            StoreError::ValueTooLarge { size, max } => write!(
                f,
                "value size {size} bytes exceeds the configured maximum of {max} bytes"
            ),
            StoreError::EmptyTransaction => {
                write!(f, "transaction must contain at least one operation")
            }
            StoreError::TransactionTooLarge { count, max } => write!(
                f,
                "transaction contains {count} operations, maximum allowed is {max}"
            ),
            StoreError::DuplicateKeyInTransaction { key } => {
                write!(
                    f,
                    "transaction contains multiple operations for the same key: {}",
                    String::from_utf8_lossy(key)
                )
            }
            StoreError::CasConflict {
                key,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "compare-and-swap conflict on key '{}': expected {}, actual {}",
                    String::from_utf8_lossy(key),
                    fmt_ov(expected),
                    fmt_ov(actual),
                )
            }
            StoreError::CompactionInProgress => {
                write!(f, "a snapshot/compaction is already in progress")
            }
            StoreError::Corruption(msg) => write!(f, "persistent data is corrupt: {msg}"),
            StoreError::Io(e) => write!(f, "I/O error: {e}"),
        }
    }
}

fn fmt_ov(v: &Option<Vec<u8>>) -> String {
    match v {
        None => "<absent>".to_string(),
        Some(v) => format!("{} bytes", v.len()),
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StoreError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        StoreError::Io(e)
    }
}

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, StoreError>;
