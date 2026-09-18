//! Test harness helpers: a fresh store in a temp directory running on a
//! multi-thread tokio runtime.

#![allow(dead_code)]

use std::sync::Arc;

use kvstore::config::Limits;
use kvstore::store::{Op, Store};
use tempfile::TempDir;

/// Create a store backed by a fresh temporary directory. The TempDir is
/// returned alongside so it stays alive for the test's duration.
pub fn temp_store(compact_threshold: u64) -> (Arc<Store>, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let store = Store::open(
        dir.path().to_path_buf(),
        compact_threshold,
        Limits {
            max_key_bytes: 64 * 1024,
            max_value_bytes: 16 * 1024 * 1024,
            max_ops_per_txn: 1024,
        },
    )
    .expect("open store");
    (Arc::new(store), dir)
}

pub fn temp_store_limited(
    compact_threshold: u64,
    max_key_bytes: usize,
    max_value_bytes: usize,
    max_ops_per_txn: usize,
) -> (Arc<Store>, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let store = Store::open(
        dir.path().to_path_buf(),
        compact_threshold,
        Limits {
            max_key_bytes,
            max_value_bytes,
            max_ops_per_txn,
        },
    )
    .expect("open store");
    (Arc::new(store), dir)
}

/// Reopen a store against the same data directory (simulates restart).
pub fn reopen(dir: &std::path::Path, compact_threshold: u64) -> Arc<Store> {
    Arc::new(
        Store::open(
            dir.to_path_buf(),
            compact_threshold,
            Limits {
                max_key_bytes: 64 * 1024,
                max_value_bytes: 16 * 1024 * 1024,
                max_ops_per_txn: 1024,
            },
        )
        .expect("reopen store"),
    )
}

pub fn put(key: &str, value: &str) -> Op {
    Op::Put {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    }
}

pub fn del(key: &str) -> Op {
    Op::Delete {
        key: key.as_bytes().to_vec(),
    }
}

pub fn cas(key: &str, expected: Option<&str>, value: &str) -> Op {
    Op::Cas {
        key: key.as_bytes().to_vec(),
        expected: expected.map(|s| s.as_bytes().to_vec()),
        value: value.as_bytes().to_vec(),
    }
}

pub async fn get_string(store: &Store, key: &str) -> Option<(String, u64)> {
    let (v, _lsn) = store.get(key.as_bytes()).await.expect("get");
    v.map(|vv| (String::from_utf8(vv.value).unwrap(), vv.version))
}
