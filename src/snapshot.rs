//! Point-in-time snapshots of the full key history.
//!
//! File format:
//! ```text
//! magic "KVSNAP01" (8 bytes) | bincode(SnapshotData) payload | u32 LE crc32(payload)
//! ```
//! Snapshots are written to a temporary file, fsynced, then atomically renamed
//! over the old snapshot, so a crash always leaves either the previous complete
//! snapshot or the new complete snapshot — never a half-written one.

use crate::error::{KvError, Result};
use crate::store::Version;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::io::AsyncWriteExt;

pub const SNAPSHOT_FILE: &str = "snapshot.bin";
const MAGIC: &[u8; 8] = b"KVSNAP01";

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotData {
    /// LSN at which the snapshot was taken. All committed LSNs <= this value
    /// are reflected in `keys`.
    pub lsn: u64,
    /// Full version history per key, newest last.
    pub keys: BTreeMap<Vec<u8>, Vec<Version>>,
}

fn snapshot_path(dir: &Path) -> PathBuf {
    dir.join(SNAPSHOT_FILE)
}

/// Unique temp path so concurrent compactions (manual + automatic) never
/// overwrite each other's temp file.
fn tmp_path(dir: &Path) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    dir.join(format!("snapshot.bin.tmp.{nanos:x}.{seq}.{}", std::process::id()))
}

pub async fn write_atomic(dir: &Path, data: &SnapshotData) -> Result<()> {
    fs::create_dir_all(dir).await?;
    let payload = bincode::serialize(data)?;
    let crc = crc32fast::hash(&payload);

    let tmp = tmp_path(dir);
    {
        let mut f = fs::File::create(&tmp).await?;
        f.write_all(MAGIC).await?;
        f.write_all(&payload).await?;
        f.write_all(&crc.to_le_bytes()).await?;
        f.sync_all().await?;
    }
    fs::rename(&tmp, snapshot_path(dir)).await?;
    // fsync the directory that holds the snapshot so the rename is durable.
    crate::wal::sync_dir(dir).await?;
    Ok(())
}

/// Read and verify the snapshot. Returns `Ok(None)` when no snapshot exists.
pub async fn read(dir: &Path) -> Result<Option<SnapshotData>> {
    let path = snapshot_path(dir);
    let bytes = match fs::read(&path).await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if bytes.len() < MAGIC.len() + 4 || &bytes[..MAGIC.len()] != MAGIC {
        return Err(KvError::CorruptSnapshot("bad magic header".into()));
    }
    let payload_end = bytes.len() - 4;
    let payload = &bytes[MAGIC.len()..payload_end];
    let stored_crc = u32::from_le_bytes(bytes[payload_end..].try_into().unwrap());
    if crc32fast::hash(payload) != stored_crc {
        return Err(KvError::CorruptSnapshot("CRC mismatch".into()));
    }
    let data: SnapshotData = bincode::deserialize(payload)
        .map_err(|e| KvError::CorruptSnapshot(format!("payload decode failed: {e}")))?;
    Ok(Some(data))
}
