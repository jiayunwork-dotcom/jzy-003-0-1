//! Version monotonicity and compare-and-swap semantics.

mod common;

use common::*;
use kvstore::error::StoreError;

/// Each commit that touches a key assigns that key a strictly greater
/// version; versions never repeat or move backwards, including after delete
/// and re-creation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn key_versions_are_monotonic() {
    let (s, _dir) = temp_store(4 * 1024 * 1024);

    let mut last_version = 0u64;
    for i in 0..20u64 {
        s.put(b"k".to_vec(), format!("v{i}").into_bytes())
            .await
            .unwrap();
        let (_, version) = get_string(&s, "k").await.unwrap();
        assert!(version > last_version, "version must strictly increase");
        assert_eq!(version, i + 1);
        last_version = version;
    }

    // Delete consumes an LSN; recreating the key gets an even higher version.
    s.delete(b"k".to_vec()).await.unwrap();
    assert_eq!(get_string(&s, "k").await, None);
    s.put(b"k".to_vec(), b"reborn".to_vec()).await.unwrap();
    let (_, version) = get_string(&s, "k").await.unwrap();
    assert_eq!(version, 22);
    assert!(version > last_version);
}

/// Keys in one transaction all get the same version; subsequent commits
/// keep each key's history monotonic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_versions() {
    let (s, _dir) = temp_store(4 * 1024 * 1024);

    s.commit(vec![put("a", "1"), put("b", "1"), put("c", "1")])
        .await
        .unwrap();
    let (_, va) = get_string(&s, "a").await.unwrap();
    let (_, vb) = get_string(&s, "b").await.unwrap();
    let (_, vc) = get_string(&s, "c").await.unwrap();
    assert_eq!((va, vb, vc), (1, 1, 1));

    s.put(b"b".to_vec(), b"2".to_vec()).await.unwrap();
    let (_, vb2) = get_string(&s, "b").await.unwrap();
    assert_eq!(vb2, 2);
    let (_, va2) = get_string(&s, "a").await.unwrap();
    assert_eq!(va2, 1, "untouched key keeps its version");
}

/// CAS succeeds exactly when the current value matches; mismatch returns a
/// typed conflict carrying expected/actual and performs no write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cas_basic_match_and_mismatch() {
    let (s, _dir) = temp_store(4 * 1024 * 1024);

    // CAS on absent key with expected null creates it.
    let out = s.cas(b"k".to_vec(), None, b"first".to_vec()).await.unwrap();
    assert_eq!(out.lsn, 1);
    assert_eq!(get_string(&s, "k").await, Some(("first".into(), 1)));

    // Wrong expected value -> conflict, no state change, no LSN consumed.
    let err = s
        .cas(b"k".to_vec(), Some(b"nope".to_vec()), b"x".to_vec())
        .await
        .unwrap_err();
    match err {
        StoreError::CasConflict {
            key,
            expected,
            actual,
        } => {
            assert_eq!(key, b"k");
            assert_eq!(expected, Some(b"nope".to_vec()));
            assert_eq!(actual, Some(b"first".to_vec()));
        }
        other => panic!("expected CasConflict, got {other:?}"),
    }
    assert_eq!(get_string(&s, "k").await, Some(("first".into(), 1)));
    assert_eq!(s.status().lsn, 1);

    // Correct expected value -> write.
    s.cas(b"k".to_vec(), Some(b"first".to_vec()), b"second".to_vec())
        .await
        .unwrap();
    assert_eq!(get_string(&s, "k").await, Some(("second".into(), 2)));

    // Expected null but key present -> conflict.
    let err = s
        .cas(b"k".to_vec(), None, b"third".to_vec())
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::CasConflict { .. }));
    assert_eq!(get_string(&s, "k").await, Some(("second".into(), 2)));
}

/// A CAS inside a failing multi-key transaction reports the conflict and
/// changes nothing, including the CAS key itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cas_conflict_inside_txn_is_explicit() {
    let (s, _dir) = temp_store(4 * 1024 * 1024);
    s.put(b"a".to_vec(), b"1".to_vec()).await.unwrap();

    let err = s
        .commit(vec![cas("a", Some("WRONG"), "2"), put("b", "2")])
        .await
        .expect_err("conflict");
    assert_eq!(err.kind(), "cas_conflict");

    assert_eq!(get_string(&s, "a").await, Some(("1".into(), 1)));
    assert_eq!(get_string(&s, "b").await, None);
}

/// Read returns the current committed position and reads never regress.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_positions_advance() {
    let (s, _dir) = temp_store(4 * 1024 * 1024);
    let (_v, lsn0) = s.get(b"x").await.unwrap();
    assert_eq!(lsn0, 0);
    s.put(b"x".to_vec(), b"1".to_vec()).await.unwrap();
    let (_v, lsn1) = s.get(b"x").await.unwrap();
    assert_eq!(lsn1, 1);
    s.put(b"x".to_vec(), b"2".to_vec()).await.unwrap();
    let (_v, lsn2) = s.get(b"x").await.unwrap();
    assert_eq!(lsn2, 2);
    let (entries, scan_lsn) = s.scan_prefix(b"x", 10).await.unwrap();
    assert_eq!(scan_lsn, 2);
    assert_eq!(entries.len(), 1);
}
