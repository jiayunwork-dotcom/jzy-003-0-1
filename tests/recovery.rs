//! WAL durability, snapshot compaction and crash recovery.

mod common;

use std::time::Duration;

use common::*;

/// After writes + process restart (store reopened from disk) every
/// committed value and its version is recovered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_replays_committed_wal() {
    let (s, dir) = temp_store(16 * 1024 * 1024);
    s.commit(vec![put("a", "1"), put("b", "2")]).await.unwrap();
    s.put(b"c".to_vec(), b"3".to_vec()).await.unwrap();
    s.delete(b"b".to_vec()).await.unwrap();

    drop(s);
    let r = reopen(dir.path(), 16 * 1024 * 1024);

    assert_eq!(get_string(&r, "a").await, Some(("1".into(), 1)));
    assert_eq!(get_string(&r, "c").await, Some(("3".into(), 2)));
    assert_eq!(get_string(&r, "b").await, None);
    let st = r.status();
    assert_eq!(st.lsn, 3);
    assert_eq!(st.snapshot_lsn, 0);
    assert_eq!(st.key_count, 2);
}

/// Writes that cross the compaction threshold trigger a snapshot; after
/// restart the store loads snapshot + tail WAL and ends in exactly the
/// committed state, with old WAL segments deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_recovers_snapshot_plus_tail_wal() {
    let threshold = 64 * 1024u64;
    let (s, dir) = temp_store(threshold);

    // Enough writes to force at least one background compaction; each value
    // is ~100 bytes of framed record.
    for i in 0..3000u64 {
        s.put(
            format!("key-{i:05}").into_bytes(),
            format!("value-{:0100}", i % 97).into_bytes(),
        )
        .await
        .unwrap();
    }

    // Wait until the background compactor finishes and WAL shrinks.
    let mut compacted = false;
    for _ in 0..200 {
        let st = s.status();
        if st.snapshot_lsn > 0 && !st.compacting {
            compacted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(compacted, "expected an automatic compaction");

    // More commits after the snapshot: only these should live in the tail
    // WAL (plus possibly one segment straddling the snapshot boundary).
    let post_snapshot_lsn = s.status().snapshot_lsn;
    s.put(b"after-snapshot".to_vec(), b"tail".to_vec())
        .await
        .unwrap();
    s.put(b"after-snapshot-2".to_vec(), b"tail2".to_vec())
        .await
        .unwrap();
    let final_lsn = s.status().lsn;

    drop(s);

    // Old fully-covered segments must be gone. A single segment that
    // straddles the snapshot boundary may legitimately remain alongside
    // the active tail, so allow up to a couple of files.
    let mut wal_files = 0;
    for e in std::fs::read_dir(dir.path()).unwrap() {
        let name = e.unwrap().file_name();
        if name.to_string_lossy().starts_with("wal-") {
            wal_files += 1;
        }
    }
    assert!(wal_files >= 1, "a tail WAL segment must remain");
    assert!(
        wal_files <= 3,
        "snapshot-covered segments must be deleted, found {wal_files}"
    );

    let r = reopen(dir.path(), threshold);
    let st = r.status();
    assert_eq!(st.lsn, final_lsn);
    assert!(st.snapshot_lsn >= post_snapshot_lsn && st.snapshot_lsn <= final_lsn);
    assert_eq!(
        get_string(&r, "after-snapshot").await,
        Some(("tail".into(), final_lsn - 1))
    );
    assert_eq!(
        get_string(&r, "after-snapshot-2").await,
        Some(("tail2".into(), final_lsn))
    );
    // Keys from before the snapshot.
    assert_eq!(
        get_string(&r, "key-00000").await,
        Some((format!("value-{:0100}", 0usize), 1))
    );
    assert_eq!(
        get_string(&r, "key-02999").await,
        Some((format!("value-{:0100}", 2999u64 % 97), 3000))
    );
    assert_eq!(st.key_count, 3002);
}

/// Explicit synchronous compaction writes a snapshot covering all commits;
/// a second restart still recovers everything and replaying must not apply
/// snapshot-covered records twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_double_apply_after_compaction_and_restart() {
    let threshold = 1024 * 1024u64;
    let (s, dir) = temp_store(threshold);

    // Commit 1: a=1,b=2 ; commit 2: a=3.
    s.commit(vec![put("a", "1"), put("b", "2")]).await.unwrap();
    s.put(b"a".to_vec(), b"3".to_vec()).await.unwrap();
    let snap_lsn = s.compact(true).await.unwrap().unwrap();
    assert_eq!(snap_lsn, 2);

    // Tail records after the snapshot: commit 3 c=4, commit 4 delete b.
    s.put(b"c".to_vec(), b"4".to_vec()).await.unwrap();
    s.delete(b"b".to_vec()).await.unwrap();

    drop(s);
    let r = reopen(dir.path(), threshold);
    let st = r.status();
    assert_eq!(st.snapshot_lsn, 2);
    assert_eq!(st.lsn, 4);
    // Versions preserved from snapshot; tail replay did not overwrite them.
    assert_eq!(get_string(&r, "a").await, Some(("3".into(), 2)));
    assert_eq!(get_string(&r, "c").await, Some(("4".into(), 3)));
    assert_eq!(get_string(&r, "b").await, None);
    assert_eq!(st.key_count, 2);

    // Reopen a second time: snapshot + same tail, same result (idempotent
    // across restarts; records are skipped by LSN when covered).
    drop(r);
    let r2 = reopen(dir.path(), threshold);
    assert_eq!(r2.status().lsn, 4);
    assert_eq!(get_string(&r2, "a").await, Some(("3".into(), 2)));
    assert_eq!(get_string(&r2, "c").await, Some(("4".into(), 3)));
}

/// Committing then compacting, then writing more and reopening yields
/// strict version continuity: no gaps and no repeated LSN assignment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn versions_stay_contiguous_across_snapshot() {
    let threshold = 1024 * 1024u64;
    let (s, dir) = temp_store(threshold);
    for i in 0..10u64 {
        s.put(b"k".to_vec(), format!("{i}").into_bytes())
            .await
            .unwrap();
    }
    s.compact(true).await.unwrap().unwrap();
    for i in 10..20u64 {
        s.put(b"k".to_vec(), format!("{i}").into_bytes())
            .await
            .unwrap();
    }
    drop(s);
    let r = reopen(dir.path(), threshold);
    let (val, ver) = get_string(&r, "k").await.unwrap();
    assert_eq!(val, "19");
    assert_eq!(ver, 20);
    assert_eq!(r.status().lsn, 20);
}

/// A torn (partially appended) final WAL frame after a crash is truncated on
/// open: the store starts cleanly without the partial record and keeps all
/// previously complete records.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torn_tail_record_is_truncated() {
    let threshold = 1024 * 1024u64;
    let (s, dir) = temp_store(threshold);
    s.put(b"durable".to_vec(), b"yes".to_vec()).await.unwrap();
    s.put(b"durable2".to_vec(), b"yes2".to_vec()).await.unwrap();

    // Find the active WAL segment and append garbage (a torn, never-fsynced
    // tail) beyond the valid bytes.
    let mut wal_path = None;
    for e in std::fs::read_dir(dir.path()).unwrap() {
        let e = e.unwrap();
        if e.file_name().to_string_lossy().starts_with("wal-") {
            wal_path = Some(e.path());
        }
    }
    let wal_path = wal_path.unwrap();
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap();
    f.write_all(b"KVS1\x10\x00\x00\x00partial-torn-bytes")
        .unwrap();
    f.sync_all().unwrap();
    drop(f);
    drop(s);

    let r = reopen(dir.path(), threshold);
    assert_eq!(get_string(&r, "durable").await, Some(("yes".into(), 1)));
    assert_eq!(get_string(&r, "durable2").await, Some(("yes2".into(), 2)));
    assert_eq!(r.status().lsn, 2);

    // Store remains writable and LSN allocation continues from 2.
    r.put(b"after".to_vec(), b"ok".to_vec()).await.unwrap();
    assert_eq!(get_string(&r, "after").await, Some(("ok".into(), 3)));
}

/// Concurrent commits during a synchronous compaction all survive the
/// following restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_while_compacting_survive_restart() {
    let threshold = 4 * 1024 * 1024u64;
    let (s, dir) = temp_store(threshold);
    for i in 0..50u64 {
        s.put(format!("init-{i}").into_bytes(), vec![b'x'; 100])
            .await
            .unwrap();
    }

    let writer = {
        let store = s.clone();
        tokio::spawn(async move {
            for i in 0..200u64 {
                store
                    .put(
                        format!("late-{i:04}").into_bytes(),
                        format!("v{i}").into_bytes(),
                    )
                    .await
                    .unwrap();
                if i % 10 == 0 {
                    tokio::time::sleep(Duration::from_micros(100)).await;
                }
            }
        })
    };

    let snap = s.compact(true).await.unwrap().unwrap();
    assert!(snap >= 1);
    writer.await.unwrap();
    let final_lsn = s.status().lsn;

    drop(s);
    let r = reopen(dir.path(), threshold);
    assert_eq!(r.status().lsn, final_lsn);
    assert_eq!(
        get_string(&r, "late-0199").await,
        Some(("v199".into(), final_lsn))
    );
    assert_eq!(
        get_string(&r, "late-0000").await.map(|(v, _)| v),
        Some("v0".to_string())
    );
}
