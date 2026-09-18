//! Multi-key transaction all-or-nothing semantics.

mod common;

use common::*;
use kvstore::error::StoreError;
use kvstore::store::Op;

/// A failing CAS inside a multi-key transaction rolls back *all* other
/// operations in the same transaction.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_cas_aborts_whole_transaction() {
    let (s, _dir) = temp_store(4 * 1024 * 1024);

    s.put(b"a".to_vec(), b"1".to_vec()).await.unwrap();
    s.put(b"b".to_vec(), b"1".to_vec()).await.unwrap();

    let err = s
        .commit(vec![
            put("a", "2"),
            // b's current value is "1", not "9" -> precondition fails
            cas("b", Some("9"), "2"),
            put("c", "2"),
        ])
        .await
        .expect_err("transaction must abort");

    assert!(matches!(
        err,
        StoreError::CasConflict { ref key, .. } if key == b"b"
    ));

    // Nothing from the aborted transaction is visible.
    assert_eq!(get_string(&s, "a").await, Some(("1".into(), 1)));
    assert_eq!(get_string(&s, "b").await, Some(("1".into(), 2)));
    assert_eq!(get_string(&s, "c").await, None);

    // LSN did not advance for the aborted transaction.
    assert_eq!(s.status().lsn, 2);
}

/// On success, all mutations commit under one LSN and become visible
/// together.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn successful_transaction_applies_atomically() {
    let (s, _dir) = temp_store(4 * 1024 * 1024);

    let out = s
        .commit(vec![put("x", "10"), put("y", "20"), put("z", "30")])
        .await
        .unwrap();
    assert_eq!(out.lsn, 1);
    assert_eq!(out.effects.len(), 3);

    for k in ["x", "y", "z"] {
        let (_, version) = get_string(&s, k).await.unwrap();
        assert_eq!(version, 1, "all keys share the commit's LSN/version");
    }
}

/// Delete + put inside one transaction apply together; a failed txn never
/// deletes anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_ops_rollback_on_conflict() {
    let (s, _dir) = temp_store(4 * 1024 * 1024);
    s.put(b"k".to_vec(), b"v".to_vec()).await.unwrap();

    let _ = s
        .commit(vec![del("k"), put("n", "1"), cas("missing", None, "v")])
        .await
        .unwrap();
    assert_eq!(get_string(&s, "k").await, None);
    assert_eq!(get_string(&s, "n").await, Some(("1".into(), 2)));
    assert_eq!(get_string(&s, "missing").await, Some(("v".into(), 2)));

    // Aborting txn that would delete missing... must leave state intact.
    let err = s
        .commit(vec![del("n"), cas("missing", Some("different"), "x")])
        .await
        .expect_err("conflict");
    assert!(matches!(err, StoreError::CasConflict { .. }));
    assert_eq!(get_string(&s, "n").await, Some(("1".into(), 2)));
}

/// Two operations on the same key inside one transaction are rejected up
/// front with a distinguishable error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn contradictory_same_key_ops_rejected() {
    let (s, _dir) = temp_store(4 * 1024 * 1024);
    let err = s
        .commit(vec![put("dup", "1"), put("dup", "2")])
        .await
        .expect_err("duplicate key rejected");
    assert!(matches!(err, StoreError::DuplicateKeyInTransaction { .. }));
    assert_eq!(err.kind(), "duplicate_key_in_transaction");
    assert_eq!(s.status().lsn, 0);

    let err = s
        .commit(vec![del("dup"), cas("dup", None, "x")])
        .await
        .expect_err("duplicate key rejected");
    assert!(matches!(err, StoreError::DuplicateKeyInTransaction { .. }));

    // Empty transactions are rejected.
    let err = s.commit(vec![]).await.expect_err("empty txn");
    assert!(matches!(err, StoreError::EmptyTransaction));

    // Too many operations is rejected.
    let (s2, _d2) = temp_store_limited(4096, 65536, 65536, 2);
    let many = vec![
        Op::Put {
            key: vec![1],
            value: vec![],
        },
        Op::Put {
            key: vec![2],
            value: vec![],
        },
        Op::Put {
            key: vec![3],
            value: vec![],
        },
    ];
    let err = s2.commit(many).await.expect_err("too many ops");
    assert!(matches!(err, StoreError::TransactionTooLarge { .. }));
}
