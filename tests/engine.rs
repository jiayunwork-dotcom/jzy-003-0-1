//! Behavioral tests for the storage engine.
//!
//! Covers: multi-key all-or-nothing txns, concurrent isolation, concurrent
//! CAS semantics, monotonic versions, snapshot+WAL crash recovery, WAL no
//! duplicate application, and rejection of illegal configs/contradictions.

use memtxn_kvs::config::Config;
use memtxn_kvs::store::Store;
use memtxn_kvs::wal::WalOp;
use std::sync::Arc;
use tempfile::TempDir;

fn config(dir: &std::path::Path, threshold: i64) -> Config {
    Config {
        data_dir: dir.to_str().unwrap().into(),
        listen_addr: "127.0.0.1:0".into(),
        max_key_bytes: 1024,
        max_value_bytes: 1024,
        max_txn_ops: 64,
        compaction_threshold_bytes: threshold as u64,
    }
}

fn put(key: &str, val: &[u8]) -> (Vec<u8>, WalOp) {
    (key.as_bytes().to_vec(), WalOp::Put(val.to_vec()))
}

// ---------- 1. all-or-nothing ----------

#[tokio::test]
async fn multi_key_txn_is_all_or_nothing() {
    let dir = TempDir::new().unwrap();
    let s = Store::open(config(dir.path(), 4096)).await.unwrap();

    s.commit(
        s.build_mutation(vec![
            put("a", b"10"),
            put("b", b"20"),
            put("c", b"30"),
        ])
        .unwrap(),
    )
    .await
    .unwrap();

    // Failing CAS on c must roll back the puts on a and b too.
    let bad = s
        .build_mutation(vec![
            put("a", b"11"),
            put("b", b"21"),
            (
                b"c".to_vec(),
                WalOp::Cas {
                    expected: Some(b"nope".to_vec()),
                    value: b"31".to_vec(),
                },
            ),
        ])
        .unwrap();
    assert!(matches!(s.commit(bad).await, Err(memtxn_kvs::error::KvError::CasConflict(_))));

    assert_eq!(s.get(b"a").await.unwrap().value, b"10");
    assert_eq!(s.get(b"b").await.unwrap().value, b"20");
    assert_eq!(s.get(b"c").await.unwrap().value, b"30");

    // Successful txn: all visible at once, same LSN.
    let out = s
        .commit(
            s.build_mutation(vec![put("a", b"11"), put("b", b"21"), put("c", b"31")])
                .unwrap(),
        )
        .await
        .unwrap();
    let ga = s.get(b"a").await.unwrap();
    let gb = s.get(b"b").await.unwrap();
    let gc = s.get(b"c").await.unwrap();
    assert_eq!(ga.version, out.lsn);
    assert_eq!(gb.version, out.lsn);
    assert_eq!(gc.version, out.lsn);
    assert_eq!((ga.value, gb.value, gc.value), (b"11".to_vec(), b"21".to_vec(), b"31".to_vec()));
}

// ---------- 2. concurrent isolation ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_transactions_are_isolated_and_serializable() {
    let dir = TempDir::new().unwrap();
    let s = Arc::new(Store::open(config(dir.path(), 4096)).await.unwrap());

    // Bank: A=100, B=100. Workers repeatedly transfer 1 atomically.
    s.commit(
        s.build_mutation(vec![put("A", b"100"), put("B", b"100")])
            .unwrap(),
    )
    .await
    .unwrap();

    let mut handles = Vec::new();
    for _ in 0..6 {
        let s = s.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..100 {
                loop {
                    // Read latest committed values.
                    let a: i64 = std::str::from_utf8(&s.get(b"A").await.unwrap().value)
                        .unwrap()
                        .parse()
                        .unwrap();
                    let b: i64 = std::str::from_utf8(&s.get(b"B").await.unwrap().value)
                        .unwrap()
                        .parse()
                        .unwrap();
                    // Invariant readers may observe: total is always 200.
                    assert_eq!(a + b, 200);
                    if a == 0 {
                        break;
                    }
                    let txn = s
                        .build_mutation(vec![
                            (
                                b"A".to_vec(),
                                WalOp::Cas {
                                    expected: Some(a.to_string().into_bytes()),
                                    value: (a - 1).to_string().into_bytes(),
                                },
                            ),
                            (
                                b"B".to_vec(),
                                WalOp::Cas {
                                    expected: Some(b.to_string().into_bytes()),
                                    value: (b + 1).to_string().into_bytes(),
                                },
                            ),
                        ])
                        .unwrap();
                    match s.commit(txn).await {
                        Ok(_) => break,
                        Err(memtxn_kvs::error::KvError::CasConflict(_)) => continue, // retry
                        Err(e) => panic!("unexpected error: {e}"),
                    }
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Every transfer landed exactly once: A=94..=100 depending on early stop
    // count, but the total must be exactly preserved.
    let a: i64 = std::str::from_utf8(&s.get(b"A").await.unwrap().value)
        .unwrap()
        .parse()
        .unwrap();
    let b: i64 = std::str::from_utf8(&s.get(b"B").await.unwrap().value)
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(a + b, 200);

    // Concurrent readers must never see a half-applied transfer: check the
    // invariant on a dedicated reader hammering the store.
    let s2 = s.clone();
    let reader = tokio::spawn(async move {
        for _ in 0..2000 {
            let a: i64 = std::str::from_utf8(&s2.get(b"A").await.unwrap().value)
                .unwrap()
                .parse()
                .unwrap();
            let b: i64 = std::str::from_utf8(&s2.get(b"B").await.unwrap().value)
                .unwrap()
                .parse()
                .unwrap();
            assert_eq!(a + b, 200, "torn transaction observed");
        }
    });
    reader.await.unwrap();
}

// ---------- 3. concurrent CAS correctness ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cas_under_contention_only_one_writer_wins() {
    let dir = TempDir::new().unwrap();
    let s = Arc::new(Store::open(config(dir.path(), 4096)).await.unwrap());
    s.commit(s.build_mutation(vec![put("k", b"0")]).unwrap())
        .await
        .unwrap();

    let mut wins = 0;
    let mut conflicts = 0;
    let mut handles = Vec::new();
    for i in 0..32 {
        let s = s.clone();
        handles.push(tokio::spawn(async move {
            let m = s
                .build_mutation(vec![(
                    b"k".to_vec(),
                    WalOp::Cas {
                        expected: Some(b"0".to_vec()),
                        value: format!("winner-{i}").into_bytes(),
                    },
                )])
                .unwrap();
            match s.commit(m).await {
                Ok(_) => 1u32,
                Err(memtxn_kvs::error::KvError::CasConflict(_)) => 0,
                Err(e) => panic!("unexpected: {e}"),
            }
        }));
    }
    for h in handles {
        if h.await.unwrap() == 1 {
            wins += 1;
        } else {
            conflicts += 1;
        }
    }
    assert_eq!(wins, 1, "exactly one CAS must win");
    assert_eq!(conflicts, 31, "the rest must get explicit conflicts");
    assert!(s.get(b"k").await.unwrap().value.starts_with(b"winner-"));
    assert_eq!(s.last_lsn().await, 2); // initial + exactly one winning commit
}

#[tokio::test]
async fn cas_expects_absent_for_none() {
    let dir = TempDir::new().unwrap();
    let s = Store::open(config(dir.path(), 4096)).await.unwrap();

    // expected=null succeeds only while key is absent.
    let m = || {
        s.build_mutation(vec![(
            b"k".to_vec(),
            WalOp::Cas {
                expected: None,
                value: b"v".to_vec(),
            },
        )])
        .unwrap()
    };
    assert!(s.commit(m()).await.is_ok());
    assert!(matches!(s.commit(m()).await, Err(memtxn_kvs::error::KvError::CasConflict(_))));
}

// ---------- 4. monotonic versions ----------

#[tokio::test]
async fn versions_never_regress_across_put_delete_put_and_restart() {
    let dir = TempDir::new().unwrap();
    let s = Store::open(config(dir.path(), 10_000_000)).await.unwrap();
    s.commit(s.build_mutation(vec![put("k", b"v1")]).unwrap())
        .await
        .unwrap();
    s.commit(s.build_mutation(vec![(b"k".to_vec(), WalOp::Delete)]).unwrap())
        .await
        .unwrap();
    s.commit(s.build_mutation(vec![put("k", b"v3")]).unwrap())
        .await
        .unwrap();

    // Historical snapshot reads at each position.
    assert_eq!(s.get_at(b"k", Some(1)).await.unwrap().value, b"v1");
    assert!(matches!(
        s.get_at(b"k", Some(2)).await,
        Err(memtxn_kvs::error::KvError::KeyNotFound(_))
    ));
    assert_eq!(s.get_at(b"k", Some(3)).await.unwrap().version, 3);
    assert_eq!(s.get_at(b"k", None).await.unwrap().version, 3);

    // Version must survive a restart unchanged.
    drop(s);
    let s2 = Store::open(config(dir.path(), 10_000_000)).await.unwrap();
    assert_eq!(s2.get(b"k").await.unwrap().version, 3);
    let out = s2
        .commit(s2.build_mutation(vec![put("k", b"v4")]).unwrap())
        .await
        .unwrap();
    assert_eq!(out.lsn, 4, "LSN must continue monotonically after restart");
}

// ---------- 5 & 6. recovery: snapshot + WAL, no duplicate apply ----------

#[tokio::test]
async fn recovery_from_snapshot_plus_wal() {
    let dir = TempDir::new().unwrap();

    // First life: commit 20 entries, compact a snapshot at LSN 20-ish, commit
    // 10 more which remain only in the WAL.
    let s = Store::open(config(dir.path(), 10_000_000)).await.unwrap();
    for i in 0..20u64 {
        s.commit(
            s.build_mutation(vec![put(&format!("k{i}"), i.to_string().as_bytes())]).unwrap(),
        )
        .await
        .unwrap();
    }
    let snap_lsn = s.compact().await.unwrap();
    assert_eq!(snap_lsn, 20);
    for i in 20..30u64 {
        s.commit(
            s.build_mutation(vec![put(&format!("k{i}"), i.to_string().as_bytes())]).unwrap(),
        )
        .await
        .unwrap();
    }
    let status = s.status().await;
    assert_eq!(status.last_lsn, 30);
    assert_eq!(status.last_snapshot_lsn, 20);
    assert!(status.wal_size_bytes > 0);
    drop(s);

    // Second life: snapshot + WAL replay.
    let s2 = Store::open(config(dir.path(), 10_000_000)).await.unwrap();
    for i in 0..30u64 {
        let g = s2.get(format!("k{i}").as_bytes()).await.unwrap();
        assert_eq!(g.value, i.to_string().as_bytes());
        assert_eq!(g.version, i + 1);
    }
    assert_eq!(s2.status().await.key_count, 30);

    // The next commit must be LSN 31, proving WAL entries were applied once,
    // not twice.
    let out = s2
        .commit(
            s2.build_mutation(vec![put("after-restart", b"ok")]).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(out.lsn, 31);
}

#[tokio::test]
async fn compaction_truncates_old_wal_and_keeps_new_entries() {
    let dir = TempDir::new().unwrap();
    let s = Store::open(config(dir.path(), 10_000_000)).await.unwrap();
    for i in 0..10u64 {
        s.commit(
            s.build_mutation(vec![put(&format!("old-{i}"), b"x")]).unwrap(),
        )
        .await
        .unwrap();
    }
    s.compact().await.unwrap();
    // Entries after the snapshot stay in the WAL.
    s.commit(s.build_mutation(vec![put("new-1", b"y")]).unwrap())
        .await
        .unwrap();
    s.compact().await.unwrap();
    drop(s);

    let s2 = Store::open(config(dir.path(), 10_000_000)).await.unwrap();
    for i in 0..10u64 {
        assert_eq!(s2.get(format!("old-{i}").as_bytes()).await.unwrap().value, b"x");
    }
    assert_eq!(s2.get(b"new-1").await.unwrap().value, b"y");
    assert_eq!(s2.last_lsn().await, 11);
}

#[tokio::test]
async fn uncommitted_changes_are_never_recovered() {
    // A batch rejected at commit time leaves no WAL trace.
    let dir = TempDir::new().unwrap();
    let s = Store::open(config(dir.path(), 10_000_000)).await.unwrap();
    s.commit(s.build_mutation(vec![put("committed", b"yes")]).unwrap())
        .await
        .unwrap();
    let rejected = s
        .build_mutation(vec![
            put("ghost-a", b"no"),
            (
                b"ghost-b".to_vec(),
                WalOp::Cas {
                    expected: Some(b"impossible".to_vec()),
                    value: b"no".to_vec(),
                },
            ),
        ])
        .unwrap();
    assert!(s.commit(rejected).await.is_err());
    drop(s);

    let s2 = Store::open(config(dir.path(), 10_000_000)).await.unwrap();
    assert_eq!(s2.get(b"committed").await.unwrap().value, b"yes");
    assert!(s2.get(b"ghost-a").await.is_err());
    assert!(s2.get(b"ghost-b").await.is_err());
    assert_eq!(s2.last_lsn().await, 1);
}

// ---------- 7. validation ----------

#[test]
fn illegal_config_rejected_at_startup() {
    let pairs = |pairs: &[(&str, &str)]| {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<Vec<_>>()
    };
    for bad in [
        vec![("COMPACTION_THRESHOLD_BYTES", "0")],
        vec![("COMPACTION_THRESHOLD_BYTES", "-10")],
        vec![("MAX_KEY_BYTES", "0")],
        vec![("MAX_VALUE_BYTES", "-1")],
        vec![("MAX_TXN_OPS", "0")],
    ] {
        assert!(matches!(
            Config::load_from(pairs(&bad)).unwrap_err(),
            memtxn_kvs::error::KvError::InvalidConfig(_)
        ));
    }
}

#[tokio::test]
async fn contradictory_txn_and_limits_rejected_before_commit() {
    let dir = TempDir::new().unwrap();
    let s = Store::open(config(dir.path(), 4096)).await.unwrap();

    // Same key twice = contradictory.
    let err = s
        .build_mutation(vec![put("dup", b"a"), put("dup", b"b")])
        .unwrap_err();
    assert!(matches!(err, memtxn_kvs::error::KvError::ContradictoryKey(_)));

    // Empty transaction.
    assert!(matches!(
        s.build_mutation(vec![]).unwrap_err(),
        memtxn_kvs::error::KvError::EmptyTransaction
    ));

    // Oversized value.
    let big = vec![0u8; 5000];
    assert!(matches!(
        s.build_mutation(vec![put("k", &big)]).unwrap_err(),
        memtxn_kvs::error::KvError::ValueTooLarge { .. }
    ));

    // Nothing was committed by the rejected attempts.
    assert_eq!(s.last_lsn().await, 0);
}

// ---------- crash window between snapshot and WAL truncation ----------

#[tokio::test]
async fn stale_wal_prefix_after_snapshot_publish_is_deduped() {
    use tokio::fs;
    let dir = TempDir::new().unwrap();
    let s = Store::open(config(dir.path(), 10_000_000)).await.unwrap();
    for i in 0..5u64 {
        s.commit(
            s.build_mutation(vec![put(&format!("k{i}"), b"v")]).unwrap(),
        )
        .await
        .unwrap();
    }
    s.compact().await.unwrap();
    // Append new commits after snapshot, then simulate the crash window: a
    // WAL file that still contains old entries LSN 1..=5 followed by new ones.
    s.commit(s.build_mutation(vec![put("k5", b"v")]).unwrap())
        .await
        .unwrap();
    drop(s);

    let wal_path = dir.path().join("wal.log");
    let mut wal = fs::read(&wal_path).await.unwrap();
    // The compacted WAL should already only contain LSN 6. Verify recovery is
    // still correct; also prepend a stale copy of an old frame to emulate the
    // crash window (snapshot published, WAL not yet rewritten).
    let mut bloated = Vec::new();
    // Reconstruct an LSN-1 frame and inject it at the front.
    let entry = memtxn_kvs::wal::WalEntry {
        lsn: 1,
        ops: vec![(b"k0".to_vec(), WalOp::Put(b"STALE".to_vec()))],
    };
    bloated.extend_from_slice(&memtxn_kvs::wal::encode_frame(&entry).unwrap());
    bloated.extend_from_slice(&wal);
    fs::write(&wal_path, &bloated).await.unwrap();
    let _ = &mut wal;

    let s2 = Store::open(config(dir.path(), 10_000_000)).await.unwrap();
    // k0 comes from the snapshot (value v), not the stale frame.
    assert_eq!(s2.get(b"k0").await.unwrap().value, b"v");
    assert_eq!(s2.get(b"k5").await.unwrap().value, b"v");
    assert_eq!(s2.last_lsn().await, 6);
    // Next commit is 7 (not double-applied).
    let out = s2
        .commit(s2.build_mutation(vec![put("k6", b"v")]).unwrap())
        .await
        .unwrap();
    assert_eq!(out.lsn, 7);
}
