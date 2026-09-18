//! Concurrent transaction isolation: readers never observe uncommitted
//! intermediate state, and concurrent commits never corrupt final values.

mod common;

use std::collections::HashSet;

use common::*;
use kvstore::store::Op;
use std::time::Duration;

/// Many writers increment a shared counter via read-then-CAS. Every failed
/// CAS is retried by the client; at the end the key's value equals the
/// total number of successful increments.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_cas_increments_are_linearizable() {
    let (s, _dir) = temp_store(4 * 1024 * 1024);
    s.put(b"counter".to_vec(), b"0".to_vec()).await.unwrap();

    let per_task = 50usize;
    let tasks = 8usize;
    let mut handles = Vec::new();
    for _ in 0..tasks {
        let store = s.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..per_task {
                loop {
                    let (cur, _) = store.get(b"counter").await.unwrap();
                    let cur_str = String::from_utf8(cur.unwrap().value).unwrap();
                    let n: u64 = cur_str.parse().unwrap();
                    let next = (n + 1).to_string();
                    match store
                        .cas(
                            b"counter".to_vec(),
                            Some(cur_str.into_bytes()),
                            next.into_bytes(),
                        )
                        .await
                    {
                        Ok(_) => break,
                        Err(kvstore::error::StoreError::CasConflict { .. }) => continue,
                        Err(e) => panic!("unexpected error: {e}"),
                    }
                }
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let (final_view, read_lsn) = s.get(b"counter").await.unwrap();
    let n: u64 = String::from_utf8(final_view.unwrap().value)
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(n as usize, tasks * per_task);
    // Initial put + every successful increment.
    assert_eq!(read_lsn, 1 + (tasks * per_task) as u64);
}

/// Readers running concurrently with big multi-key commits must never see
/// a torn commit: a prefix scan taken at one instant returns exactly one
/// commit version across every key of the batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn readers_never_see_partial_commit() {
    let (s, _dir) = temp_store(4 * 1024 * 1024);
    const N_KEYS: usize = 200;
    const ROUNDS: u64 = 100;

    // Initial state: key_i = "old:i"
    let mut init = Vec::new();
    for i in 0..N_KEYS {
        init.push(put(&format!("k{i:04}"), &format!("old:{i}")));
    }
    s.commit(init).await.unwrap();

    // Committer repeatedly flips all keys atomically between old/new values.
    let commit_store = s.clone();
    let committer = tokio::spawn(async move {
        for round in 0..ROUNDS {
            let prefix = if round % 2 == 0 { "new" } else { "old" };
            let ops: Vec<Op> = (0..N_KEYS)
                .map(|i| put(&format!("k{i:04}"), &format!("{prefix}:{round}")))
                .collect();
            commit_store.commit(ops).await.unwrap();
            tokio::time::sleep(Duration::from_micros(50)).await;
        }
    });

    // Readers scan continuously; a committed batch yields a single version
    // for all keys.
    let read_store = s.clone();
    let reader = tokio::spawn(async move {
        for _ in 0..3000 {
            let (entries, _scan_lsn) = read_store.scan_prefix(b"k", N_KEYS + 10).await.unwrap();
            assert_eq!(entries.len(), N_KEYS, "scan sees exactly the live set");
            let scan_versions: HashSet<u64> = entries.iter().map(|(_, v)| v.version).collect();
            assert_eq!(
                scan_versions.len(),
                1,
                "prefix scan observed a torn multi-key commit"
            );
            // Every observed value must be a well-formed committed value:
            // "prefix:round" whose round matches the version LSN relationship
            // indirectly; at minimum no garbage/partial strings appear.
            for (k, v) in &entries {
                let sval = String::from_utf8(v.value.clone()).unwrap();
                assert!(
                    sval.starts_with("new:") || sval.starts_with("old:"),
                    "unexpected value for {}: {sval}",
                    String::from_utf8_lossy(k)
                );
            }
        }
    });

    reader.await.unwrap();
    committer.await.unwrap();
}

/// Concurrent commits touching disjoint key sets never interfere: every
/// key's value is exactly the last value written to it and a writer's two
/// keys always share one version.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_disjoint_transactions() {
    let (s, _dir) = temp_store(4 * 1024 * 1024);
    let writers = 8usize;
    let rounds = 40usize;

    let mut handles = Vec::new();
    for w in 0..writers {
        let store = s.clone();
        handles.push(tokio::spawn(async move {
            for r in 0..rounds {
                let ops = vec![
                    put(&format!("w{w}:a"), &format!("{r}")),
                    put(&format!("w{w}:b"), &format!("{r}")),
                ];
                store.commit(ops).await.unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    for w in 0..writers {
        let (va, vera) = get_string(&s, &format!("w{w}:a")).await.unwrap();
        let (vb, verb) = get_string(&s, &format!("w{w}:b")).await.unwrap();
        assert_eq!(va, format!("{}", rounds - 1));
        assert_eq!(vb, format!("{}", rounds - 1));
        assert_eq!(vera, verb, "a/b always commit together");
    }
}
