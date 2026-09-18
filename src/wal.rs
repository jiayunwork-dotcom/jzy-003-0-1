//! Write-ahead log.
//!
//! On-disk framing (little-endian):
//! ```text
//! u32 payload_len | u32 crc32(payload) | payload (bincode(WalEntry))
//! ```
//! A record is considered durable only after `fsync`. On replay a torn tail
//! (short frame or CRC mismatch at the end of the file) is discarded: it can
//! only come from an unsynced partial write that crashed mid-append, so it was
//! never acknowledged as committed.

use crate::error::{KvError, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;

pub const WAL_FILE: &str = "wal.log";
const LEN_PREFIX: usize = 4;
const CRC_SUFFIX: usize = 4;
const FRAME_HEADER: usize = LEN_PREFIX + CRC_SUFFIX;
/// Frames above this size are treated as corruption rather than allocations.
const MAX_FRAME_LEN: u32 = 512 * 1024 * 1024;

/// A single mutation on one key. `Cas` only proceeds when the current
/// committed value equals `expected` (`None` means the key must be absent).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalOp {
    Put(Vec<u8>),
    Delete,
    Cas {
        expected: Option<Vec<u8>>,
        value: Vec<u8>,
    },
}

/// One committed transaction record. LSNs are gap-less, strictly increasing
/// commit sequence numbers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalEntry {
    pub lsn: u64,
    pub ops: Vec<(Vec<u8>, WalOp)>,
}

pub fn encode_frame(entry: &WalEntry) -> Result<Vec<u8>> {
    let payload = bincode::serialize(entry)?;
    let len = u32::try_from(payload.len()).map_err(|_| KvError::Serialize("frame too large".into()))?;
    let crc = crc32fast::hash(&payload);
    let mut buf = Vec::with_capacity(FRAME_HEADER + payload.len());
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(&payload);
    Ok(buf)
}

/// Parse all complete frames. Returns the entries and the byte length of the
/// valid prefix of the file.
pub fn parse_frames(bytes: &[u8]) -> Result<(Vec<WalEntry>, u64)> {
    let mut entries = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let frame_start = offset;
        if bytes.len() - offset < FRAME_HEADER {
            break; // torn header
        }
        let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let stored_crc =
            u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap());
        offset += FRAME_HEADER;
        if len > MAX_FRAME_LEN {
            return Err(KvError::CorruptWal {
                offset: frame_start as u64,
                message: format!("frame length {len} exceeds limit"),
            });
        }
        let payload_len = len as usize;
        if offset + payload_len > bytes.len() {
            break; // torn payload
        }
        let payload = &bytes[offset..offset + payload_len];
        offset += payload_len;
        if crc32fast::hash(payload) != stored_crc {
            if offset == bytes.len() {
                break; // torn tail of a crashed append: ignore
            }
            return Err(KvError::CorruptWal {
                offset: frame_start as u64,
                message: "CRC mismatch".into(),
            });
        }
        let entry: WalEntry = bincode::deserialize(payload).map_err(|e| KvError::CorruptWal {
            offset: frame_start as u64,
            message: format!("payload decode failed: {e}"),
        })?;
        entries.push(entry);
    }
    Ok((entries, offset as u64))
}

/// fsync a directory, required after rename/unlink for durable metadata.
pub async fn sync_dir(path: &Path) -> Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        use std::os::unix::io::AsRawFd;
        let file = std::fs::File::open(&path)?;
        // libc fsync on the directory fd.
        let rc = unsafe { libc_like_fsync(file.as_raw_fd()) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    })
    .await
    .map_err(|e| KvError::Io(e.to_string()))?
    .map_err(KvError::from)
}

#[cfg(unix)]
unsafe fn libc_like_fsync(fd: std::os::unix::io::RawFd) -> i32 {
    extern "C" {
        fn fsync(fd: i32) -> i32;
    }
    fsync(fd)
}

#[cfg(not(unix))]
fn libc_like_fsync(_fd: i32) -> i32 {
    0
}

#[derive(Debug)]
pub struct Wal {
    file: tokio::fs::File,
    path: PathBuf,
    /// Bytes of the valid committed prefix (may be less than file size while
    /// a torn tail from a crash has not yet been truncated).
    valid_len: u64,
}

impl Wal {
    /// Open the WAL, recovering any torn tail and stale prefix.
    /// `start_lsn` is the LSN of the newest snapshot already loaded (0 when
    /// there is no snapshot).
    ///
    /// A crash can occur between snapshot publish and WAL truncation, leaving
    /// frames with `lsn <= start_lsn` in the file; those are dropped and the
    /// file is rewritten on open.
    pub async fn open(dir: &Path, start_lsn: u64) -> Result<(Self, Vec<WalEntry>)> {
        let path = dir.join(WAL_FILE);
        let bytes = match fs::read(&path).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        let (all_entries, valid_len) = parse_frames(&bytes)?;

        // CRC-valid frames already covered by the loaded snapshot are stale.
        let mut stale = 0usize;
        let mut entries: Vec<WalEntry> = Vec::new();
        for e in all_entries {
            if e.lsn <= start_lsn {
                stale += 1;
            } else {
                entries.push(e);
            }
        }
        validate_replay(&entries, start_lsn)?;

        let torn = valid_len < bytes.len() as u64;
        if torn {
            tracing::warn!(
                valid_len,
                file_len = bytes.len(),
                "discarding torn WAL tail from crash"
            );
        }

        fs::create_dir_all(dir).await?;

        // Rewrite when the file contains a torn tail or a stale snapshot prefix.
        let mut wal = if torn || stale > 0 {
            let mut w = Wal {
                file: OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .truncate(true)
                    .open(&path)
                    .await?,
                path,
                valid_len: 0,
            };
            w.rewrite(&entries).await?;
            w
        } else {
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(&path)
                .await?;
            Wal {
                file,
                path,
                valid_len,
            }
        };
        // Ensure the open file position is at the end for later appends.
        use tokio::io::AsyncSeekExt;
        wal.file.seek(std::io::SeekFrom::End(0)).await?;

        Ok((wal, entries))
    }

    /// Append one entry, fsync, then return its starting byte offset.
    pub async fn append(&mut self, entry: &WalEntry) -> Result<u64> {
        let frame = encode_frame(entry)?;
        let offset = self.valid_len;
        self.file.write_all(&frame).await?;
        self.file.flush().await?;
        self.file.sync_all().await?;
        self.valid_len += frame.len() as u64;
        Ok(offset)
    }

    pub fn size_bytes(&self) -> u64 {
        self.valid_len
    }

    /// Atomically replace the whole WAL with `entries`. The replacement file
    /// is fully fsynced and renamed, and the directory is fsynced afterwards.
    /// Returns the new byte size.
    pub async fn rewrite(&mut self, entries: &[WalEntry]) -> Result<u64> {
        let mut bytes = Vec::new();
        for e in entries {
            bytes.extend_from_slice(&encode_frame(e)?);
        }
        let new_size = bytes.len() as u64;
        let tmp = self.path.with_extension("log.tmp");
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .await?;
            f.write_all(&bytes).await?;
            f.sync_all().await?;
        }
        fs::rename(&tmp, &self.path).await?;
        if let Some(parent) = self.path.parent() {
            sync_dir(parent).await?;
        }
        self.file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&self.path)
            .await?;
        use tokio::io::AsyncSeekExt;
        self.file.seek(std::io::SeekFrom::End(0)).await?;
        self.valid_len = new_size;
        Ok(new_size)
    }
}

/// Replayed entries must be gap-less and start exactly at `start_lsn + 1`
/// (or at 1 when no snapshot was loaded). Repeated application of the same
/// record is impossible because LSNs are unique identifiers.
fn validate_replay(entries: &[WalEntry], start_lsn: u64) -> Result<()> {
    let mut expected = start_lsn + 1;
    for e in entries {
        if e.lsn != expected {
            if e.lsn < expected {
                return Err(KvError::CorruptWal {
                    offset: 0,
                    message: format!(
                        "duplicate or out-of-order LSN {} while expecting {}",
                        e.lsn, expected
                    ),
                });
            }
            return Err(KvError::CorruptWal {
                offset: 0,
                message: format!("gap in WAL: expected LSN {expected}, found {}", e.lsn),
            });
        }
        expected += 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(lsn: u64, key: &str, val: &str) -> WalEntry {
        WalEntry {
            lsn,
            ops: vec![(key.as_bytes().to_vec(), WalOp::Put(val.as_bytes().to_vec()))],
        }
    }

    #[test]
    fn frame_roundtrip() {
        let e = entry(7, "k", "v");
        let buf = encode_frame(&e).unwrap();
        let (parsed, len) = parse_frames(&buf).unwrap();
        assert_eq!(parsed, vec![e]);
        assert_eq!(len as usize, buf.len());
    }

    #[test]
    fn multiple_frames_and_torn_tail() {
        let mut buf = Vec::new();
        for lsn in 1..=3 {
            buf.extend_from_slice(&encode_frame(&entry(lsn, "k", &lsn.to_string())).unwrap());
        }
        let (parsed, len) = parse_frames(&buf).unwrap();
        assert_eq!(parsed.len(), 3);
        assert_eq!(len as usize, buf.len());

        // A partial frame appended after the valid prefix is ignored.
        let mut torn = buf.clone();
        torn.extend_from_slice(&buf[..7]);
        let (parsed, len) = parse_frames(&torn).unwrap();
        assert_eq!(parsed.len(), 3);
        assert_eq!(len as usize, buf.len());
    }

    #[test]
    fn crc_corruption_mid_file_is_detected() {
        let mut buf = Vec::new();
        for lsn in 1..=3 {
            buf.extend_from_slice(&encode_frame(&entry(lsn, "k", &lsn.to_string())).unwrap());
        }
        // Flip a payload byte in the second frame.
        let first_len = encode_frame(&entry(1, "k", "1")).unwrap().len();
        buf[first_len + FRAME_HEADER + 1] ^= 0xFF;
        assert!(matches!(
            parse_frames(&buf).unwrap_err(),
            KvError::CorruptWal { .. }
        ));
    }

    #[test]
    fn lsn_sequence_validation() {
        assert!(validate_replay(&[entry(1, "a", "1")], 0).is_ok());
        assert!(validate_replay(&[entry(2, "a", "1")], 1).is_ok());
        assert!(validate_replay(&[entry(1, "a", "1"), entry(2, "a", "2")], 0).is_ok());
        // gap
        assert!(validate_replay(&[entry(3, "a", "1")], 0).is_err());
        // duplicate
        assert!(validate_replay(&[entry(1, "a", "1"), entry(1, "a", "1")], 0).is_err());
        // snapshot already covers first entries
        assert!(validate_replay(&[entry(1, "a", "1")], 2).is_err());
    }
}
