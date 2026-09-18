//! Snapshot persistence: a full image of the key table at one LSN.
//!
//! Snapshot files live in the data directory:
//! - `snapshot.tmp` is written and fsynced first,
//! - then atomically renamed over `snapshot.bin`,
//! - then the containing directory is fsynced.
//!
//! On startup the snapshot (if present) is loaded; WAL records with LSNs
//! greater than the snapshot LSN are replayed afterwards. A snapshot whose
//! magic/version/CRC checks fail is hard corruption rather than being
//! silently ignored.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use parking_lot::RwLock;

use crate::error::{Result, StoreError};
use crate::frame::{self, FrameError, FrameReader};
use crate::record::{EntrySnapshot, LogRecord, Mutation, Snapshot, SnapshotHeader, SNAPSHOT_MAGIC};
use crate::store::Entry;

const SNAPSHOT_FILE: &str = "snapshot.bin";
const TMP_FILE: &str = "snapshot.tmp";

/// Loaded snapshot metadata, also kept in memory for the status endpoint.
#[derive(Debug, Clone)]
pub struct SnapshotMeta {
    pub lsn: u64,
}

/// Load the latest snapshot from `dir`. Returns `Ok(None)` if no snapshot
/// exists.
pub fn load(dir: &Path) -> Result<Option<Snapshot>> {
    let path = dir.join(SNAPSHOT_FILE);
    let file = match OpenOptions::new().read(true).open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(StoreError::Io(e)),
    };
    let mut reader = FrameReader::new(file);

    let header_payload = match reader.next_frame() {
        Ok(p) => p,
        Err(other) => {
            return Err(StoreError::Corruption(format!(
                "cannot read snapshot header from {}: {other}",
                path.display()
            )))
        }
    };
    let header: SnapshotHeader = bincode::deserialize(&header_payload).map_err(|e| {
        StoreError::Corruption(format!(
            "invalid snapshot header in {}: {e}",
            path.display()
        ))
    })?;
    if header.magic != SNAPSHOT_MAGIC {
        return Err(StoreError::Corruption(format!(
            "bad snapshot magic in {}",
            path.display()
        )));
    }

    let mut entries = std::collections::BTreeMap::new();
    let mut seen = 0u64;
    loop {
        let payload = match reader.next_frame() {
            Ok(p) => p,
            Err(FrameError::Eof) => break,
            Err(other) => {
                return Err(StoreError::Corruption(format!(
                    "error reading snapshot entry from {}: {other}",
                    path.display()
                )))
            }
        };
        let e: EntrySnapshot = bincode::deserialize(&payload).map_err(|err| {
            StoreError::Corruption(format!(
                "invalid snapshot entry in {}: {err}",
                path.display()
            ))
        })?;
        entries.insert(
            e.key,
            Entry {
                value: e.value,
                version: e.version,
            },
        );
        seen += 1;
    }

    if seen != header.key_count {
        return Err(StoreError::Corruption(format!(
            "snapshot declares {} keys but contains {seen}",
            header.key_count
        )));
    }

    tracing::info!(lsn = header.lsn, keys = seen, "loaded snapshot");
    Ok(Some(Snapshot {
        lsn: header.lsn,
        entries,
    }))
}

/// Atomically write a snapshot containing every live entry. `lsn` is the
/// commit position the snapshot represents.
pub fn write(
    dir: &Path,
    lsn: u64,
    table: &std::collections::BTreeMap<Vec<u8>, Entry>,
) -> Result<SnapshotMeta> {
    fs::create_dir_all(dir)?;
    let tmp = dir.join(TMP_FILE);
    let final_path = dir.join(SNAPSHOT_FILE);

    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;

        let header = SnapshotHeader {
            magic: SNAPSHOT_MAGIC,
            lsn,
            key_count: table.len() as u64,
        };
        let header_payload = bincode::serialize(&header).map_err(|e| {
            StoreError::Corruption(format!("cannot serialize snapshot header: {e}"))
        })?;
        frame::write_frame(&mut file, &header_payload)?;

        for (key, entry) in table {
            let e = EntrySnapshot {
                key: key.clone(),
                value: entry.value.clone(),
                version: entry.version,
            };
            let payload = bincode::serialize(&e).map_err(|err| {
                StoreError::Corruption(format!("cannot serialize snapshot entry: {err}"))
            })?;
            frame::write_frame(&mut file, &payload)?;
        }
        file.flush()?;
        file.sync_data()?;
    }

    fs::rename(&tmp, &final_path)?;
    sync_dir(dir)?;
    tracing::info!(lsn, keys = table.len(), "snapshot written and committed");
    Ok(SnapshotMeta { lsn })
}

/// Remove a stale temporary snapshot left over from an interrupted
/// compaction. Safe to call on every startup.
pub fn remove_stale_tmp(dir: &Path) -> Result<()> {
    let tmp: PathBuf = dir.join(TMP_FILE);
    match fs::remove_file(&tmp) {
        Ok(()) => {
            sync_dir(dir)?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(StoreError::Io(e)),
    }
    Ok(())
}

fn sync_dir(dir: &Path) -> Result<()> {
    let f = File::open(dir)?;
    f.sync_data()?;
    Ok(())
}

/// Apply a decoded record to an in-memory table during replay. LSNs are
/// validated by the caller; this helper performs the state mutation only.
pub fn apply_record(table: &RwLock<std::collections::BTreeMap<Vec<u8>, Entry>>, rec: &LogRecord) {
    let mut g = table.write();
    for m in &rec.mutations {
        match m {
            Mutation::Put { key, value } => {
                g.insert(
                    key.clone(),
                    Entry {
                        value: value.clone(),
                        version: rec.lsn,
                    },
                );
            }
            Mutation::Delete { key } => {
                // Version is tracked on live entries only; removal of a
                // missing key still consumes an LSN globally.
                g.remove(key);
            }
        }
    }
}

/// Helper kept for tests/tests-only writers.
#[allow(dead_code)]
pub(crate) fn snapshot_path(dir: &Path) -> PathBuf {
    dir.join(SNAPSHOT_FILE)
}
