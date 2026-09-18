//! In-memory transactional key-value store with WAL durability.
//!
//! Concurrency model
//! -----------------
//! - Reads take an `RwLock` read guard: readers never block each other and
//!   never observe a partially applied transaction (a commit applies all
//!   its mutations inside one write guard).
//! - Commits are serialized by `commit_mutex` (tokio async mutex).
//!   Precondition evaluation (CAS comparisons) and WAL append happen under
//!   that lock before any in-memory state is changed, so concurrent
//!   transactions can neither read uncommitted data nor interleave their
//!   compare-and-write steps. WAL file I/O runs on blocking tasks.
//! - Snapshot compaction clones the table under a short read guard, then
//!   writes the snapshot file entirely off the lock; the only
//!   write-blocking section is the final WAL rotation/partition step.
//!
//! Versioning
//! ----------
//! Each successful commit advances the global LSN by one (starting at 1).
//! A key's `version` is the LSN of the transaction that last modified it,
//! so versions for a given key are strictly increasing and never repeat.
//! Reads return the LSN they observed as their read position.

use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;
use tokio::sync::Mutex as AsyncMutex;

use crate::config::Limits;
use crate::error::{Result, StoreError};
use crate::metrics::Metrics;
use crate::record::{LogRecord, Mutation};
use crate::snapshot;
use crate::wal::{self, Wal};

/// One live key entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub value: Vec<u8>,
    /// LSN of the last transaction that modified this key.
    pub version: u64,
}

/// Result of reading one key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueView {
    pub value: Vec<u8>,
    pub version: u64,
}

/// A pre-validated operation requested by a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        key: Vec<u8>,
    },
    /// Write only if the current value equals `expected` (`None` means
    /// the key must be absent).
    Cas {
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    },
}

impl Op {
    pub fn key(&self) -> &[u8] {
        match self {
            Op::Put { key, .. } | Op::Delete { key } | Op::Cas { key, .. } => key,
        }
    }
}

/// What one committed operation did, in submission order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpEffect {
    Put {
        key: Vec<u8>,
        version: u64,
        /// Whether this put replaced an existing value.
        replaced: bool,
    },
    Delete {
        key: Vec<u8>,
        /// Whether a live value was actually removed.
        existed: bool,
    },
    Cas {
        key: Vec<u8>,
        version: u64,
        /// Whether the CAS wrote over an existing value.
        replaced: bool,
    },
}

/// Result returned by [`Store::commit`].
#[derive(Debug, Clone)]
pub struct CommitOutput {
    pub lsn: u64,
    pub effects: Vec<OpEffect>,
}

/// Snapshot of service status for the status/metrics endpoints.
#[derive(Debug, Clone)]
pub struct Status {
    pub key_count: usize,
    pub lsn: u64,
    pub snapshot_lsn: u64,
    pub wal_total_bytes: u64,
    pub compacting: bool,
    pub wal_active_segment: u64,
    pub limits: Limits,
}

struct Inner {
    table: RwLock<BTreeMap<Vec<u8>, Entry>>,
    /// Current commit position (== highest assigned LSN).
    lsn: AtomicU64,
    /// LSN covered by the latest durable snapshot.
    snapshot_lsn: AtomicU64,
}

/// Handle to the store. Cheaply cloneable (`Arc` inside).
#[derive(Clone)]
pub struct Store {
    inner: Arc<Inner>,
    wal: Wal,
    /// Serializes transactions: held across precondition check + WAL append.
    commit_mutex: Arc<AsyncMutex<()>>,
    /// True while a compaction is in progress.
    compacting: Arc<AtomicBool>,
    data_dir: PathBuf,
    wal_compact_threshold: u64,
    limits: Limits,
    pub metrics: Arc<Metrics>,
}

impl Store {
    /// Open the store at `data_dir`: remove stale temp files, load the
    /// newest snapshot, replay WAL records after it (validating LSN
    /// discipline), then build the runtime handle.
    pub fn open(data_dir: PathBuf, wal_compact_threshold: u64, limits: Limits) -> Result<Store> {
        assert!(wal_compact_threshold > 0);
        std::fs::create_dir_all(&data_dir)?;
        snapshot::remove_stale_tmp(&data_dir)?;

        let snap = snapshot::load(&data_dir)?;
        let (base_lsn, table) = match snap {
            Some(s) => (s.lsn, s.entries),
            None => (0, BTreeMap::new()),
        };

        let table = RwLock::new(table);
        // First LSN not covered by the snapshot that must be present in the
        // WAL. `expected` advances with every applied record; `prev` tracks
        // strict file-order monotonicity to reject duplicated or reordered
        // records as corruption.
        let mut expected = base_lsn + 1;
        let mut prev: u64 = 0;

        let (wal, _records) = Wal::open(&data_dir, |rec| {
            if rec.lsn <= prev {
                return Err(StoreError::Corruption(format!(
                    "WAL LSN out of order or duplicated: {} after {prev}",
                    rec.lsn
                )));
            }
            prev = rec.lsn;

            if rec.lsn <= base_lsn {
                // A retained segment may legitimately straddle the snapshot
                // boundary (records before and after one snapshot share one
                // segment). Such records are already durably captured by the
                // snapshot and must never be applied twice — skip them.
                tracing::debug!(
                    lsn = rec.lsn,
                    base_lsn,
                    "skipping snapshot-covered WAL record"
                );
                return Ok(());
            }
            if rec.lsn != expected {
                return Err(StoreError::Corruption(format!(
                    "WAL LSN gap: expected {expected}, found {}",
                    rec.lsn
                )));
            }
            expected += 1;
            snapshot::apply_record(&table, rec);
            Ok(())
        })?;

        let lsn = expected - 1;
        let key_count = table.read().len();

        let metrics = Arc::new(Metrics::default());
        metrics.lsn.store(lsn, Ordering::Relaxed);
        metrics.last_snapshot_lsn.store(base_lsn, Ordering::Relaxed);
        metrics.keys.store(key_count as u64, Ordering::Relaxed);
        metrics
            .wal_total_bytes
            .store(wal.total_bytes(), Ordering::Relaxed);

        let store = Store {
            inner: Arc::new(Inner {
                table,
                lsn: AtomicU64::new(lsn),
                snapshot_lsn: AtomicU64::new(base_lsn),
            }),
            wal,
            commit_mutex: Arc::new(AsyncMutex::new(())),
            compacting: Arc::new(AtomicBool::new(false)),
            data_dir,
            wal_compact_threshold,
            limits,
            metrics,
        };
        tracing::info!(
            lsn,
            snapshot_lsn = base_lsn,
            key_count,
            wal_bytes = store.wal.total_bytes(),
            "store opened"
        );
        Ok(store)
    }

    pub fn limits(&self) -> Limits {
        self.limits
    }

    // ---- validation -------------------------------------------------

    fn validate_key(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(StoreError::EmptyKey);
        }
        if key.len() > self.limits.max_key_bytes {
            return Err(StoreError::KeyTooLarge {
                size: key.len(),
                max: self.limits.max_key_bytes,
            });
        }
        Ok(())
    }

    fn validate_value(&self, value: &[u8]) -> Result<()> {
        if value.len() > self.limits.max_value_bytes {
            return Err(StoreError::ValueTooLarge {
                size: value.len(),
                max: self.limits.max_value_bytes,
            });
        }
        Ok(())
    }

    /// Structural validation of a transaction: legal sizes, non-empty
    /// batch, and no two operations on the same key (ambiguous semantics).
    fn validate_ops(&self, ops: &[Op]) -> Result<()> {
        if ops.is_empty() {
            return Err(StoreError::EmptyTransaction);
        }
        if ops.len() > self.limits.max_ops_per_txn {
            return Err(StoreError::TransactionTooLarge {
                count: ops.len(),
                max: self.limits.max_ops_per_txn,
            });
        }
        let mut seen = std::collections::HashSet::new();
        for op in ops {
            self.validate_key(op.key())?;
            match op {
                Op::Put { value, .. } | Op::Cas { value, .. } => self.validate_value(value)?,
                Op::Delete { .. } => {}
            }
            if !seen.insert(op.key().to_vec()) {
                return Err(StoreError::DuplicateKeyInTransaction {
                    key: op.key().to_vec(),
                });
            }
        }
        Ok(())
    }

    // ---- reads ------------------------------------------------------

    /// Read the committed value of `key` and the LSN position observed.
    /// Every read sees a fully committed, consistent snapshot.
    pub async fn get(&self, key: &[u8]) -> Result<(Option<ValueView>, u64)> {
        self.validate_key(key)?;
        self.metrics.reads_total.fetch_add(1, Ordering::Relaxed);
        let g = self.inner.table.read();
        let lsn = self.inner.lsn.load(Ordering::Acquire);
        let view = g.get(key).map(|e| ValueView {
            value: e.value.clone(),
            version: e.version,
        });
        Ok((view, lsn))
    }

    /// Prefix scan. Returns ordered `(key, value, version)` triples and
    /// the read position. `limit` is capped at `max_scan_limit`.
    pub async fn scan_prefix(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, ValueView)>, u64)> {
        self.scan_range_prefix(prefix, limit).await
    }

    /// Range scan over `[start, end)` (end excluded; an unbounded end scans
    /// to the last key).
    pub async fn scan_range(
        &self,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, ValueView)>, u64)> {
        self.validate_range(&start, &end)?;
        self.metrics.scans_total.fetch_add(1, Ordering::Relaxed);
        let g = self.inner.table.read();
        let lsn = self.inner.lsn.load(Ordering::Acquire);
        let mut out = Vec::new();
        let cap = limit.max(1);
        for (k, e) in g.range((start, end)) {
            if out.len() >= cap {
                break;
            }
            out.push((
                k.clone(),
                ValueView {
                    value: e.value.clone(),
                    version: e.version,
                },
            ));
        }
        Ok((out, lsn))
    }

    async fn scan_range_prefix(
        &self,
        prefix: &[u8],
        limit: usize,
    ) -> Result<(Vec<(Vec<u8>, ValueView)>, u64)> {
        if !prefix.is_empty() {
            self.validate_key(prefix)?;
        }
        self.metrics.scans_total.fetch_add(1, Ordering::Relaxed);
        let g = self.inner.table.read();
        let lsn = self.inner.lsn.load(Ordering::Acquire);
        let cap = limit.max(1);
        let mut out = Vec::new();
        let upper = prefix_upper_bound(prefix);
        let iter: Box<dyn Iterator<Item = (&Vec<u8>, &Entry)>> = match upper {
            Some(end) => Box::new(g.range(prefix.to_vec()..end)),
            None => Box::new(g.range(prefix.to_vec()..)),
        };
        for (k, e) in iter {
            if out.len() >= cap {
                break;
            }
            if !k.starts_with(prefix) {
                break;
            }
            out.push((
                k.clone(),
                ValueView {
                    value: e.value.clone(),
                    version: e.version,
                },
            ));
        }
        Ok((out, lsn))
    }

    fn validate_range(&self, start: &Bound<Vec<u8>>, end: &Bound<Vec<u8>>) -> Result<()> {
        if let Bound::Included(k) | Bound::Excluded(k) = start {
            self.validate_key(k)?;
        }
        if let Bound::Included(k) | Bound::Excluded(k) = end {
            self.validate_key(k)?;
        }
        Ok(())
    }

    // ---- commit path ------------------------------------------------

    /// Commit one operation transactionally.
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<CommitOutput> {
        self.commit(vec![Op::Put { key, value }]).await
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<CommitOutput> {
        self.commit(vec![Op::Delete { key }]).await
    }

    pub async fn cas(
        &self,
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Result<CommitOutput> {
        self.commit(vec![Op::Cas {
            key,
            expected,
            value,
        }])
        .await
    }

    /// Atomically commit a batch of operations.
    ///
    /// All-or-nothing: any structural validation failure or unmet CAS
    /// precondition returns an error and mutates nothing. On success the
    /// WAL record is durable before the result is returned and every
    /// mutation becomes visible together under one write guard.
    pub async fn commit(&self, ops: Vec<Op>) -> Result<CommitOutput> {
        self.validate_ops(&ops)?;

        let _guard = self.commit_mutex.lock().await;

        // Phase 1: evaluate preconditions against the committed snapshot.
        // This is a synchronous, lock-scoped helper so the non-Send
        // parking_lot guard never lives inside this async state machine.
        let mutations = self.evaluate_preconditions(&ops)?;

        let lsn = self.inner.lsn.load(Ordering::Acquire) + 1;
        let record = LogRecord { lsn, mutations };

        // Phase 2: persist before publish. The blocking fsync runs off the
        // async worker pool; the commit mutex is held for its duration so
        // LSN order on disk matches commit order.
        let wal = self.wal.clone();
        let threshold = self.wal_compact_threshold;
        let frame_len = tokio::task::spawn_blocking(move || wal.append(&record, threshold))
            .await
            .map_err(|e| StoreError::Io(std::io::Error::other(format!("join error: {e}"))))??;
        self.metrics
            .wal_bytes_written_total
            .fetch_add(frame_len, Ordering::Relaxed);

        // Phase 3: publish all mutations atomically and compute effects.
        // Again confined to a synchronous helper.
        let (key_count, effects) = self.apply_committed(&ops, lsn);
        self.inner.lsn.store(lsn, Ordering::Release);
        self.metrics.keys.store(key_count as u64, Ordering::Relaxed);
        self.metrics.lsn.store(lsn, Ordering::Relaxed);
        self.metrics.commits_total.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .committed_ops_total
            .fetch_add(ops.len() as u64, Ordering::Relaxed);
        self.metrics
            .wal_total_bytes
            .store(self.wal.total_bytes(), Ordering::Relaxed);

        // Phase 4: schedule background compaction if the WAL crossed the
        // configured size threshold.
        if self.wal.total_bytes() >= self.wal_compact_threshold
            && self
                .compacting
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            self.spawn_compaction();
        }

        Ok(CommitOutput { lsn, effects })
    }

    /// Check every CAS precondition against the committed table and
    /// produce the WAL mutation vector. No state is modified.
    fn evaluate_preconditions(&self, ops: &[Op]) -> Result<Vec<Mutation>> {
        let g = self.inner.table.read();
        let mut mutations = Vec::with_capacity(ops.len());
        for op in ops {
            match op {
                Op::Put { key, value } => mutations.push(Mutation::Put {
                    key: key.clone(),
                    value: value.clone(),
                }),
                Op::Delete { key } => mutations.push(Mutation::Delete { key: key.clone() }),
                Op::Cas {
                    key,
                    expected,
                    value,
                } => {
                    let actual = g.get(key).map(|e| e.value.clone());
                    if &actual != expected {
                        self.metrics
                            .cas_conflicts_total
                            .fetch_add(1, Ordering::Relaxed);
                        self.metrics
                            .rejected_txns_total
                            .fetch_add(1, Ordering::Relaxed);
                        return Err(StoreError::CasConflict {
                            key: key.clone(),
                            expected: expected.clone(),
                            actual,
                        });
                    }
                    mutations.push(Mutation::Put {
                        key: key.clone(),
                        value: value.clone(),
                    });
                }
            }
        }
        Ok(mutations)
    }

    /// Apply an already-durable transaction to the in-memory table under a
    /// single write guard, returning `(key_count, effects)`.
    fn apply_committed(&self, ops: &[Op], lsn: u64) -> (usize, Vec<OpEffect>) {
        let mut g = self.inner.table.write();
        let mut effects = Vec::with_capacity(ops.len());
        for op in ops {
            match op {
                Op::Put { key, value } => {
                    let replaced = g.contains_key(key);
                    g.insert(
                        key.clone(),
                        Entry {
                            value: value.clone(),
                            version: lsn,
                        },
                    );
                    effects.push(OpEffect::Put {
                        key: key.clone(),
                        version: lsn,
                        replaced,
                    });
                }
                Op::Delete { key } => {
                    let existed = g.remove(key).is_some();
                    effects.push(OpEffect::Delete {
                        key: key.clone(),
                        existed,
                    });
                }
                Op::Cas { key, value, .. } => {
                    let replaced = g.contains_key(key);
                    g.insert(
                        key.clone(),
                        Entry {
                            value: value.clone(),
                            version: lsn,
                        },
                    );
                    effects.push(OpEffect::Cas {
                        key: key.clone(),
                        version: lsn,
                        replaced,
                    });
                }
            }
        }
        (g.len(), effects)
    }

    // ---- compaction -------------------------------------------------

    /// Manually trigger a compaction. `wait` awaits completion and returns
    /// the new snapshot LSN; otherwise it runs in the background.
    pub async fn compact(&self, wait: bool) -> Result<Option<u64>> {
        if self
            .compacting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(StoreError::CompactionInProgress);
        }
        if wait {
            let store = self.clone();
            let lsn = tokio::task::spawn_blocking(move || store.run_compaction_blocking())
                .await
                .map_err(|e| StoreError::Io(std::io::Error::other(format!("join error: {e}"))))??;
            Ok(Some(lsn))
        } else {
            self.spawn_compaction();
            Ok(None)
        }
    }

    fn spawn_compaction(&self) {
        let store = self.clone();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || store.run_compaction_blocking()).await;
            match result {
                Ok(Ok(lsn)) => {
                    tracing::info!(snapshot_lsn = lsn, "background compaction complete");
                }
                Ok(Err(e)) => {
                    metrics
                        .compactions_failed_total
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::error!(error = %e, "background compaction failed");
                }
                Err(e) => {
                    metrics
                        .compactions_failed_total
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::error!(error = %e, "background compaction task panicked");
                }
            }
        });
    }

    /// Compaction executed on a blocking thread.
    ///
    /// 1. Short read guard: copy the full table and capture LSN.
    /// 2. Off lock: write + fsync snapshot, atomic rename.
    /// 3. Short commit lock: rotate WAL, partition sealed segments, update
    ///    snapshot LSN (this is the only moment commits are blocked).
    /// 4. Delete covered segment files (outside any lock).
    fn run_compaction_blocking(&self) -> Result<u64> {
        let start = Instant::now();
        let (lsn, snapshot_table) = {
            let g = self.inner.table.read();
            let lsn = self.inner.lsn.load(Ordering::Acquire);
            (lsn, g.clone())
        };

        // Nothing to compact: snapshot already covers all commits.
        if lsn <= self.inner.snapshot_lsn.load(Ordering::Acquire) {
            self.compacting.store(false, Ordering::Release);
            return Ok(lsn);
        }

        // Durable snapshot first.
        let meta = snapshot::write(&self.data_dir, lsn, &snapshot_table)?;
        self.metrics
            .snapshot_bytes_written_total
            .fetch_add(snapshot_byte_size(&snapshot_table), Ordering::Relaxed);

        // Now block commits briefly to fix the WAL boundary at this same
        // LSN and partition segments.
        let _guard = self.commit_mutex.blocking_lock();

        let to_delete = self.wal.rotate_and_take_persisted(meta.lsn)?;
        self.inner.snapshot_lsn.store(meta.lsn, Ordering::Release);
        self.metrics
            .last_snapshot_lsn
            .store(meta.lsn, Ordering::Relaxed);
        self.metrics
            .wal_total_bytes
            .store(self.wal.total_bytes(), Ordering::Relaxed);

        let segments_deleted = to_delete.len();
        for seg in &to_delete {
            if let Err(e) = wal::delete_segment(seg) {
                // Non-fatal: an orphaned old segment is replayed/skipped
                // correctly on restart; it just costs space until later.
                tracing::warn!(error = %e, "failed to delete sealed WAL segment");
            }
        }

        let elapsed = start.elapsed().as_secs_f64();
        *self.metrics.last_compaction_duration_seconds.lock() = elapsed;
        self.metrics
            .compactions_total
            .fetch_add(1, Ordering::Relaxed);
        self.compacting.store(false, Ordering::Release);
        tracing::info!(
            snapshot_lsn = meta.lsn,
            segments_deleted,
            elapsed_secs = elapsed,
            "compaction finished"
        );
        Ok(meta.lsn)
    }

    // ---- status -----------------------------------------------------

    pub fn status(&self) -> Status {
        Status {
            key_count: self.inner.table.read().len(),
            lsn: self.inner.lsn.load(Ordering::Acquire),
            snapshot_lsn: self.inner.snapshot_lsn.load(Ordering::Acquire),
            wal_total_bytes: self.wal.total_bytes(),
            compacting: self.compacting.load(Ordering::Acquire),
            wal_active_segment: self.wal.active_index(),
            limits: self.limits,
        }
    }

    /// Snapshot LSN visible for tests and status.
    pub fn snapshot_lsn(&self) -> u64 {
        self.inner.snapshot_lsn.load(Ordering::Acquire)
    }
}

fn snapshot_byte_size(table: &BTreeMap<Vec<u8>, Entry>) -> u64 {
    table
        .iter()
        .map(|(k, e)| k.len() as u64 + e.value.len() as u64)
        .sum()
}

/// Compute the lexicographically smallest key strictly greater than every
/// key with the given prefix. Returns `None` when no such key exists
/// (empty prefix or an all-0xFF prefix), in which case the range end is
/// unbounded.
pub fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for i in (0..end.len()).rev() {
        if end[i] != 0xFF {
            end[i] += 1;
            end.truncate(i + 1);
            return Some(end);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_bound_basic() {
        assert_eq!(prefix_upper_bound(b"abc"), Some(b"abd".to_vec()));
        assert_eq!(prefix_upper_bound(b"a\xff"), Some(b"b".to_vec()));
        assert_eq!(prefix_upper_bound(b"\xff\xff"), None);
        assert_eq!(prefix_upper_bound(b""), None);
    }
}
