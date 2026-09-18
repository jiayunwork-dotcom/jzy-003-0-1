//! On-disk record types shared by the write-ahead log and snapshots.
//!
//! Records are serialized with `bincode` and embedded in length+CRC framed
//! records (see [`crate::frame`]). LSNs (log sequence numbers) are assigned
//! per committed transaction and are strictly increasing; the snapshot
//! stores the LSN it was taken at, and recovery applies only WAL records
//! with LSNs strictly greater than that.

use std::collections::BTreeMap;

/// A single mutation applied to the key table.
///
/// Delete is modeled explicitly rather than as a tombstone value; replay
/// still assigns the commit's LSN as the key version before removal, which
/// keeps the version-monotonicity reasoning uniform (a recreated key gets a
/// strictly higher version).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Mutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl Mutation {
    pub fn key(&self) -> &[u8] {
        match self {
            Mutation::Put { key, .. } | Mutation::Delete { key } => key,
        }
    }
}

/// One committed transaction as written to the WAL.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LogRecord {
    /// Log sequence number: index of the committed transaction, starting at 1.
    pub lsn: u64,
    /// Mutations in transaction submission order.
    pub mutations: Vec<Mutation>,
}

/// Magic prefix of a snapshot file.
pub const SNAPSHOT_MAGIC: [u8; 8] = *b"KVSSNAP1";

/// Snapshot file header (bincode-serialized inside the first frame).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SnapshotHeader {
    pub magic: [u8; 8],
    /// LSN of the last committed transaction included in the snapshot.
    pub lsn: u64,
    /// Number of live keys in the snapshot.
    pub key_count: u64,
}

/// One key/value/version entry inside a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EntrySnapshot {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    /// LSN of the transaction that last modified this key.
    pub version: u64,
}

/// Fully decoded snapshot.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub lsn: u64,
    pub entries: BTreeMap<Vec<u8>, crate::store::Entry>,
}
