//! In-memory transactional key-value engine.
//!
//! Concurrency model:
//! * Reads take a shared lock on the memtable and never block each other.
//! * Commits are serialized through a single commit gate. The gate is held
//!   while checking CAS preconditions, appending+fsyncing the WAL and applying
//!   mutations. This makes transaction validation atomic with respect to other
//!   commits (snapshot isolation / serializable commit order) while keeping
//!   readers lock-free during the fsync.
//! * Compaction snapshots the memtable under a short-lived shared lock, writes
//!   the snapshot file without blocking reads or writes, and only pauses
//!   commits for the final atomic WAL rewrite.

use crate::config::Config;
use crate::error::{KvError, Result};
use crate::snapshot::SnapshotData;
use crate::wal::{Wal, WalEntry, WalOp};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

/// One historical value of a key at LSN `lsn`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Version {
    Put { lsn: u64, value: Vec<u8> },
    Delete { lsn: u64 },
}

impl Version {
    pub fn lsn(&self) -> u64 {
        match self {
            Version::Put { lsn, .. } | Version::Delete { lsn } => *lsn,
        }
    }
}

/// A validated, ready-to-commit mutation batch.
#[derive(Debug, Clone)]
pub struct Mutation {
    /// Insertion order preserved; every key appears exactly once.
    pub ops: Vec<(Vec<u8>, WalOp)>,
}

#[derive(Debug, Clone)]
pub struct CommitOutcome {
    pub lsn: u64,
    /// Number of live (non-deleted) keys after the commit.
    pub key_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct KvEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    /// Version (LSN) that produced this value.
    pub version: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Got {
    pub value: Vec<u8>,
    pub version: u64,
    /// Log position at which this read observed the store.
    pub read_lsn: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Status {
    pub key_count: usize,
    /// Latest committed LSN / log position.
    pub last_lsn: u64,
    pub wal_size_bytes: u64,
    pub last_snapshot_lsn: u64,
    pub compacting: bool,
    pub compaction_threshold_bytes: u64,
}

#[derive(Debug)]
struct MemTable {
    /// Version history per key, ordered oldest -> newest.
    keys: BTreeMap<Vec<u8>, Vec<Version>>,
    last_lsn: u64,
}

impl MemTable {
    fn empty() -> Self {
        MemTable {
            keys: BTreeMap::new(),
            last_lsn: 0,
        }
    }

    fn apply(&mut self, entry: &WalEntry) -> Result<()> {
        debug_assert_eq!(entry.lsn, self.last_lsn + 1);
        for (key, op) in &entry.ops {
            // CAS records are revalidated during replay: a committed CAS had a
            // matching value at commit time, so it must match while rebuilding
            // state in LSN order.
            if let WalOp::Cas { expected, .. } = op {
                let current = self.live_value(key).map(|v| v.to_vec());
                if &current != expected {
                    return Err(KvError::CorruptWal {
                        offset: 0,
                        message: format!(
                            "CAS precondition failed while replaying committed LSN {}",
                            entry.lsn
                        ),
                    });
                }
            }
            let version = match op {
                WalOp::Put(value) => Version::Put {
                    lsn: entry.lsn,
                    value: value.clone(),
                },
                WalOp::Delete => Version::Delete { lsn: entry.lsn },
                WalOp::Cas { value, .. } => Version::Put {
                    lsn: entry.lsn,
                    value: value.clone(),
                },
            };
            self.keys.entry(key.clone()).or_default().push(version);
        }
        self.last_lsn = entry.lsn;
        Ok(())
    }

    fn live_value(&self, key: &[u8]) -> Option<&Vec<u8>> {
        match self.keys.get(key).and_then(|v| v.last()) {
            Some(Version::Put { value, .. }) => Some(value),
            Some(Version::Delete { .. }) | None => None,
        }
    }

    /// Value visible at snapshot position `at_lsn`: the newest version with
    /// lsn <= at_lsn.
    fn value_at(&self, key: &[u8], at_lsn: u64) -> Option<&Vec<u8>> {
        let history = self.keys.get(key)?;
        let idx = match history.binary_search_by(|v| v.lsn().cmp(&at_lsn)) {
            Ok(i) => i,
            Err(0) => return None,
            Err(i) => i - 1,
        };
        match &history[idx] {
            Version::Put { value, .. } => Some(value),
            Version::Delete { .. } => None,
        }
    }

    fn version_at(&self, key: &[u8], at_lsn: u64) -> Option<u64> {
        let history = self.keys.get(key)?;
        let idx = match history.binary_search_by(|v| v.lsn().cmp(&at_lsn)) {
            Ok(i) => i,
            Err(0) => return None,
            Err(i) => i - 1,
        };
        Some(history[idx].lsn())
    }

    fn live_key_count(&self) -> usize {
        self.keys
            .values()
            .filter(|h| matches!(h.last(), Some(Version::Put { .. })))
            .count()
    }

    fn snapshot_data(&self) -> SnapshotData {
        SnapshotData {
            lsn: self.last_lsn,
            keys: self.keys.clone(),
        }
    }
}

#[derive(Debug)]
struct Inner {
    mem: tokio::sync::RwLock<MemTable>,
    wal: AsyncMutex<Wal>,
    /// Serializes whole transactions: validation + WAL append + apply.
    commit_gate: AsyncMutex<()>,
    /// Serializes full compaction runs (snapshot capture..WAL rewrite),
    /// preventing manual and automatic compactions from interleaving.
    compaction: AsyncMutex<()>,
    last_snapshot_lsn: AtomicU64,
    compacting: AtomicBool,
}

#[derive(Clone, Debug)]
pub struct Store {
    inner: Arc<Inner>,
    pub config: Arc<Config>,
    data_dir: PathBuf,
}

impl Store {
    /// Open (or create) the store in `config.data_dir`, loading the latest
    /// snapshot and replaying the WAL on top of it.
    pub async fn open(config: Config) -> Result<Self> {
        let dir = PathBuf::from(&config.data_dir);
        tokio::fs::create_dir_all(&dir).await?;

        let mut mem = MemTable::empty();
        let mut snapshot_lsn = 0u64;
        if let Some(snap) = crate::snapshot::read(&dir).await? {
            tracing::info!(lsn = snap.lsn, "loaded snapshot");
            mem.keys = snap.keys;
            mem.last_lsn = snap.lsn;
            snapshot_lsn = snap.lsn;
        }

        let (wal, entries) = Wal::open(&dir, snapshot_lsn).await?;
        for e in &entries {
            mem.apply(e)?;
        }
        if let Some(last) = entries.last() {
            tracing::info!(from = snapshot_lsn + 1, to = last.lsn, "replayed WAL");
        }

        Ok(Store {
            inner: Arc::new(Inner {
                mem: tokio::sync::RwLock::new(mem),
                wal: AsyncMutex::new(wal),
                commit_gate: AsyncMutex::new(()),
                compaction: AsyncMutex::new(()),
                last_snapshot_lsn: AtomicU64::new(snapshot_lsn),
                compacting: AtomicBool::new(false),
            }),
            config: Arc::new(config),
            data_dir: dir,
        })
    }

    pub async fn last_lsn(&self) -> u64 {
        self.inner.mem.read().await.last_lsn
    }

    // ---------- validation ----------

    fn validate_key(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(KvError::EmptyKey);
        }
        if key.len() > self.config.max_key_bytes {
            return Err(KvError::KeyTooLarge {
                size: key.len(),
                limit: self.config.max_key_bytes,
            });
        }
        Ok(())
    }

    fn validate_value(&self, value: &[u8]) -> Result<()> {
        if value.len() > self.config.max_value_bytes {
            return Err(KvError::ValueTooLarge {
                size: value.len(),
                limit: self.config.max_value_bytes,
            });
        }
        Ok(())
    }

    /// Build a `Mutation`, rejecting malformed batches before any lock is
    /// taken. A key appearing more than once in the same transaction is a
    /// contradictory transaction and is rejected up front.
    pub fn build_mutation(&self, raw_ops: Vec<(Vec<u8>, WalOp)>) -> Result<Mutation> {
        if raw_ops.is_empty() {
            return Err(KvError::EmptyTransaction);
        }
        if raw_ops.len() > self.config.max_txn_ops {
            return Err(KvError::TooManyOperations {
                count: raw_ops.len(),
                limit: self.config.max_txn_ops,
            });
        }
        // Detect contradictions: the same key touched twice in one txn.
        let mut seen: BTreeMap<Vec<u8>, ()> = BTreeMap::new();
        for (key, op) in &raw_ops {
            self.validate_key(key)?;
            match op {
                WalOp::Put(v) | WalOp::Cas { value: v, .. } => self.validate_value(v)?,
                WalOp::Delete => {}
            }
            if seen.insert(key.clone(), ()).is_some() {
                return Err(KvError::ContradictoryKey(
                    String::from_utf8_lossy(key).into_owned(),
                ));
            }
        }
        Ok(Mutation { ops: raw_ops })
    }

    // ---------- commit ----------

    /// Validate-at-commit-time preconditions. Called under the commit gate.
    fn check_preconditions(mem: &MemTable, ops: &[(Vec<u8>, WalOp)]) -> Result<()> {
        for (key, op) in ops {
            if let WalOp::Cas { expected, .. } = op {
                let current = mem.live_value(key).map(|v| v.to_vec());
                if &current != expected {
                    return Err(KvError::CasConflict(
                        String::from_utf8_lossy(key).into_owned(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Atomically commit one mutation batch. Returns the assigned LSN.
    ///
    /// All-or-nothing semantics: if any CAS precondition fails the whole batch
    /// is rejected and nothing is written or applied.
    pub async fn commit(&self, mutation: Mutation) -> Result<CommitOutcome> {
        let _gate = self.inner.commit_gate.lock().await;

        // 1. Re-validate preconditions against the latest committed state.
        let read_guard = self.inner.mem.read().await;
        Self::check_preconditions(&read_guard, &mutation.ops)?;
        let new_lsn = read_guard.last_lsn + 1;
        drop(read_guard);

        // 2. Persist first (WAL), fsync before acknowledging. CAS entries are
        //    recorded verbatim so replay can validate them too.
        let entry = WalEntry {
            lsn: new_lsn,
            ops: mutation.ops.clone(),
        };
        let mut wal = self.inner.wal.lock().await;
        wal.append(&entry).await?;
        let wal_size = wal.size_bytes();
        drop(wal);

        // 3. Apply to the in-memory table. Readers only observe the batch once
        //    every operation is applied (single write lock per commit).
        let mut write_guard = self.inner.mem.write().await;
        write_guard.apply(&entry).expect("live apply of a freshly validated entry cannot fail");
        let key_count = write_guard.live_key_count();
        drop(write_guard);

        // 4. Trigger automatic compaction if the WAL crossed the threshold.
        if wal_size >= self.config.compaction_threshold_bytes
            && self
                .inner
                .compacting
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            let store = self.clone();
            tokio::spawn(async move {
                // Serialize against any other (e.g. manual) compaction.
                let _comp_guard = store.inner.compaction.lock().await;
                if let Err(e) = store.compact_locked().await {
                    tracing::error!(error = %e, "compaction failed");
                }
                store.inner.compacting.store(false, Ordering::SeqCst);
            });
        }

        Ok(CommitOutcome {
            lsn: new_lsn,
            key_count,
        })
    }

    // ---------- reads ----------

    /// Read the latest committed value.
    pub async fn get(&self, key: &[u8]) -> Result<Got> {
        self.validate_key(key)?;
        let mem = self.inner.mem.read().await;
        match mem.live_value(key) {
            Some(value) => Ok(Got {
                value: value.clone(),
                version: mem.keys[key].last().unwrap().lsn(),
                read_lsn: mem.last_lsn,
            }),
            None => Err(KvError::KeyNotFound(String::from_utf8_lossy(key).into_owned())),
        }
    }

    /// Read at a specific log position. `None` position means latest.
    /// Returns the value plus the read's log position.
    pub async fn get_at(&self, key: &[u8], at_lsn: Option<u64>) -> Result<Got> {
        self.validate_key(key)?;
        let mem = self.inner.mem.read().await;
        let at = at_lsn.unwrap_or(mem.last_lsn);
        if at > mem.last_lsn {
            return Err(KvError::VersionNotFound(at));
        }
        match mem.value_at(key, at) {
            Some(value) => Ok(Got {
                value: value.clone(),
                version: mem.version_at(key, at).unwrap(),
                read_lsn: at,
            }),
            None => Err(KvError::KeyNotFound(String::from_utf8_lossy(key).into_owned())),
        }
    }

    /// Range scan over committed keys. Both bounds optional; the scan reflects
    /// the committed state at the moment of the read.
    pub async fn range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: Option<usize>,
        at_lsn: Option<u64>,
    ) -> Result<(Vec<KvEntry>, u64)> {
        let mem = self.inner.mem.read().await;
        let at = at_lsn.unwrap_or(mem.last_lsn);
        if at > mem.last_lsn {
            return Err(KvError::VersionNotFound(at));
        }
        let mut out = Vec::new();
        // Collect the matching keys first; all BTreeMap range iterators share
        // the same item type.
        let keys: Vec<&Vec<u8>> = match (start, end) {
            (Some(s), Some(e)) => mem.keys.range(s.to_vec()..e.to_vec()).map(|(k, _)| k).collect(),
            (Some(s), None) => mem.keys.range(s.to_vec()..).map(|(k, _)| k).collect(),
            (None, Some(e)) => mem.keys.range(..e.to_vec()).map(|(k, _)| k).collect(),
            (None, None) => mem.keys.keys().collect(),
        };
        for key in keys {
            if let Some(value) = mem.value_at(key, at) {
                out.push(KvEntry {
                    key: key.clone(),
                    value: value.clone(),
                    version: mem.version_at(key, at).unwrap(),
                });
                if let Some(n) = limit {
                    if out.len() >= n {
                        break;
                    }
                }
            }
        }
        Ok((out, at))
    }

    /// Prefix scan: keys with the given byte prefix.
    pub async fn prefix(
        &self,
        prefix: &[u8],
        limit: Option<usize>,
        at_lsn: Option<u64>,
    ) -> Result<(Vec<KvEntry>, u64)> {
        if prefix.is_empty() {
            return self.range(None, None, limit, at_lsn).await;
        }
        let upper = prefix_upper_bound(prefix);
        let end = upper.as_deref();
        self.range(Some(prefix), end, limit, at_lsn).await
    }

    // ---------- status / maintenance ----------

    pub async fn status(&self) -> Status {
        let (key_count, last_lsn) = {
            let mem = self.inner.mem.read().await;
            (mem.live_key_count(), mem.last_lsn)
        };
        let wal_size = self.inner.wal.lock().await.size_bytes();
        Status {
            key_count,
            last_lsn,
            wal_size_bytes: wal_size,
            last_snapshot_lsn: self.inner.last_snapshot_lsn.load(Ordering::SeqCst),
            compacting: self.inner.compacting.load(Ordering::SeqCst),
            compaction_threshold_bytes: self.config.compaction_threshold_bytes,
        }
    }

    /// Manually trigger compaction. Serializes with automatic compactions and
    /// reflects itself in the `compacting` status flag.
    pub async fn compact(&self) -> Result<u64> {
        // If an automatic compaction is already running, wait for it and report
        // its result position instead of running a second one.
        if self
            .inner
            .compacting
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            let _comp_guard = self.inner.compaction.lock().await;
            return Ok(self.inner.last_snapshot_lsn.load(Ordering::SeqCst));
        }
        let _comp_guard = self.inner.compaction.lock().await;
        let result = self.compact_locked().await;
        self.inner.compacting.store(false, Ordering::SeqCst);
        result
    }

    /// Compaction body. The caller must hold `inner.compaction`.
    ///
    /// Timeline:
    /// 1. Read-lock briefly, clone the full state, release.
    /// 2. Write snapshot to a unique temp file, fsync, atomic rename (commits
    ///    may proceed concurrently).
    /// 3. Hold the commit gate; collect entries after snapshot_lsn; rewrite the
    ///    WAL atomically; update snapshot cursor.
    async fn compact_locked(&self) -> Result<u64> {
        // Phase 1: capture.
        let snap = {
            let mem = self.inner.mem.read().await;
            mem.snapshot_data()
        };
        let snapshot_lsn = snap.lsn;
        if snapshot_lsn <= self.inner.last_snapshot_lsn.load(Ordering::SeqCst) {
            // Nothing newer than the existing snapshot.
            return Ok(snapshot_lsn);
        }

        // Phase 2: durable snapshot publish (commits may proceed concurrently).
        crate::snapshot::write_atomic(&self.data_dir, &snap).await?;

        // Phase 3: serialize with commits for the WAL rewrite.
        let _gate = self.inner.commit_gate.lock().await;

        // Re-read the current state; entries appended during phase 2 must be
        // kept in the new WAL.
        let mem = self.inner.mem.read().await;
        let current_lsn = mem.last_lsn;
        let mut kept: Vec<WalEntry> = Vec::new();
        if current_lsn > snapshot_lsn {
            // Reconstruct entries after the snapshot from the version history.
            kept = self.reconstruct_entries(&mem, snapshot_lsn + 1, current_lsn);
        }
        drop(mem);

        let mut wal = self.inner.wal.lock().await;
        wal.rewrite(&kept).await?;
        drop(wal);

        self.inner
            .last_snapshot_lsn
            .store(snapshot_lsn, Ordering::SeqCst);
        tracing::info!(snapshot_lsn, kept = kept.len(), "compaction complete");
        Ok(snapshot_lsn)
    }

    /// Rebuild WAL entries for LSNs in `[from, to]` from version histories.
    /// Preconditions have already been verified for historical commits, so the
    /// rewritten ops only need to carry the resulting mutation (Put/Delete).
    fn reconstruct_entries(&self, mem: &MemTable, from: u64, to: u64) -> Vec<WalEntry> {
        // lsn -> (key -> op)
        let mut by_lsn: BTreeMap<u64, Vec<(Vec<u8>, WalOp)>> = BTreeMap::new();
        for (key, history) in &mem.keys {
            for v in history {
                let lsn = v.lsn();
                if lsn < from || lsn > to {
                    continue;
                }
                let op = match v {
                    Version::Put { value, .. } => WalOp::Put(value.clone()),
                    Version::Delete { .. } => WalOp::Delete,
                };
                by_lsn.entry(lsn).or_default().push((key.clone(), op));
            }
        }
        by_lsn
            .into_iter()
            .map(|(lsn, ops)| WalEntry { lsn, ops })
            .collect()
    }
}

/// Smallest key strictly greater than every key with the given prefix, if one
/// exists (returns None for an all-0xFF prefix).
fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    while let Some(last) = upper.last_mut() {
        if *last < 0xFF {
            *last += 1;
            return Some(upper);
        }
        upper.pop();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn test_config(dir: &std::path::Path, threshold: i64) -> Config {
        Config {
            data_dir: dir.to_str().unwrap().into(),
            listen_addr: "127.0.0.1:0".into(),
            max_key_bytes: 1024,
            max_value_bytes: 1024,
            max_txn_ops: 64,
            compaction_threshold_bytes: threshold as u64,
        }
    }

    fn put(key: &[u8], val: &[u8]) -> (Vec<u8>, WalOp) {
        (key.to_vec(), WalOp::Put(val.to_vec()))
    }

    #[tokio::test]
    async fn commit_and_get_basic() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path(), 4096)).await.unwrap();
        let m = store.build_mutation(vec![put(b"a", b"1"), put(b"b", b"2")]).unwrap();
        let out = store.commit(m).await.unwrap();
        assert_eq!(out.lsn, 1);
        assert_eq!(store.get(b"a").await.unwrap().value, b"1");
        assert_eq!(store.get(b"b").await.unwrap().value, b"2");
        assert!(matches!(store.get(b"missing").await, Err(KvError::KeyNotFound(_))));
    }

    #[tokio::test]
    async fn versions_are_monotonic_per_key() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path(), 4096)).await.unwrap();
        let mut versions = Vec::new();
        for i in 0..10 {
            let m = store
                .build_mutation(vec![(b"k".to_vec(), WalOp::Put(vec![b'0' + i]))])
                .unwrap();
            versions.push(store.commit(m).await.unwrap().lsn);
        }
        let got = store.get(b"k").await.unwrap();
        assert_eq!(got.version, 10);
        assert_eq!(got.value, b"9");
        let at5 = store.get_at(b"k", Some(5)).await.unwrap();
        assert_eq!(at5.version, 5);
        assert_eq!(at5.value, b"4");
        let mut sorted = versions.clone();
        sorted.sort();
        assert_eq!(versions, sorted);
        assert_eq!(versions.windows(2).all(|w| w[0] < w[1]), true);
    }

    #[tokio::test]
    async fn cas_all_or_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path(), 4096)).await.unwrap();
        store
            .commit(store.build_mutation(vec![put(b"a", b"1"), put(b"b", b"10")]).unwrap())
            .await
            .unwrap();

        // a expects "1" (ok), b expects "999" (fail) -> whole txn aborts.
        let bad = store
            .build_mutation(vec![
                (
                    b"a".to_vec(),
                    WalOp::Cas {
                        expected: Some(b"1".to_vec()),
                        value: b"2".to_vec(),
                    },
                ),
                (
                    b"b".to_vec(),
                    WalOp::Cas {
                        expected: Some(b"999".to_vec()),
                        value: b"11".to_vec(),
                    },
                ),
            ])
            .unwrap();
        assert!(matches!(store.commit(bad).await, Err(KvError::CasConflict(_))));
        // Nothing changed.
        assert_eq!(store.get(b"a").await.unwrap().value, b"1");
        assert_eq!(store.get(b"b").await.unwrap().value, b"10");
        assert_eq!(store.last_lsn().await, 1);

        // Both CAS conditions hold -> commits atomically.
        let good = store
            .build_mutation(vec![
                (
                    b"a".to_vec(),
                    WalOp::Cas {
                        expected: Some(b"1".to_vec()),
                        value: b"2".to_vec(),
                    },
                ),
                (
                    b"b".to_vec(),
                    WalOp::Cas {
                        expected: Some(b"10".to_vec()),
                        value: b"11".to_vec(),
                    },
                ),
            ])
            .unwrap();
        assert_eq!(store.commit(good).await.unwrap().lsn, 2);
        assert_eq!(store.get(b"a").await.unwrap().value, b"2");
        assert_eq!(store.get(b"b").await.unwrap().value, b"11");
    }

    #[tokio::test]
    async fn contradictory_batch_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path(), 4096)).await.unwrap();
        let err = store
            .build_mutation(vec![put(b"k", b"1"), put(b"k", b"2")])
            .unwrap_err();
        assert!(matches!(err, KvError::ContradictoryKey(_)));
    }

    #[tokio::test]
    async fn too_many_ops_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path(), 4096)).await.unwrap();
        let ops: Vec<_> = (0..65)
            .map(|i| (vec![b'k', i as u8], WalOp::Put(b"v".to_vec())))
            .collect();
        assert!(matches!(
            store.build_mutation(ops).unwrap_err(),
            KvError::TooManyOperations { .. }
        ));
    }

    #[tokio::test]
    async fn empty_and_oversized_inputs_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path(), 4096)).await.unwrap();
        assert!(matches!(store.build_mutation(vec![]).unwrap_err(), KvError::EmptyTransaction));
        assert!(matches!(
            store.build_mutation(vec![put(b"", b"v")]).unwrap_err(),
            KvError::EmptyKey
        ));
        let big = vec![0u8; 2000];
        assert!(matches!(
            store.build_mutation(vec![put(b"k", &big)]).unwrap_err(),
            KvError::ValueTooLarge { .. }
        ));
    }

    #[tokio::test]
    async fn prefix_and_range_scans() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(test_config(dir.path(), 4096)).await.unwrap();
        let ops = vec![
            put(b"user:1", b"a"),
            put(b"user:2", b"b"),
            put(b"order:1", b"c"),
        ];
        store.commit(store.build_mutation(ops).unwrap()).await.unwrap();
        let (p, _) = store.prefix(b"user:", None, None).await.unwrap();
        assert_eq!(p.len(), 2);
        let (r, _) = store.range(Some(b"order:"), Some(b"p"), None, None).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].key, b"order:1");
    }
}
