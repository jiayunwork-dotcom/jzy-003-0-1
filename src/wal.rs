//! Write-ahead log: strictly ordered, CRC-framed, size-rotated segments.
//!
//! Segment files live inside the data directory and are named
//! `wal-<index:020>` with monotonically increasing zero-padded indices.
//! Only the last (active) segment is appended to; previous segments are
//! sealed and eligible for deletion once a snapshot past their last LSN
//! has become durable.
//!
//! All mutations of a transaction are written as one [`LogRecord`] inside
//! one frame and fsynced before the transaction is acknowledged, so
//! recovery sees each committed transaction exactly once:
//! - fully present frame  -> record is committed and replayed;
//! - partial tail frame   -> torn by crash, truncated on open;
//! - record LSN <= snapshot LSN -> skipped (never double-applied);
//! - LSN gap              -> treated as corruption.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::error::{Result, StoreError};
use crate::frame::{sync_parent_dir, FrameError, FrameReader};
use crate::record::LogRecord;

const SEGMENT_PREFIX: &str = "wal-";
const SEGMENT_WIDTH: usize = 20;

/// A sealed WAL segment that may be removed after compaction.
pub struct SealedSegment {
    pub index: u64,
    pub path: PathBuf,
    pub size: u64,
    /// LSN of the last record contained in this segment (0 if empty).
    pub end_lsn: u64,
}

/// State protected by the WAL lock. The active file is held open for
/// appending; sealed segments are tracked so compaction can delete them
/// without rescanning the directory.
struct WalInner {
    dir: PathBuf,
    active_index: u64,
    active_file: File,
    active_size: u64,
    /// Sealed segment index -> metadata, ordered by index.
    sealed: BTreeMap<u64, SealedSegment>,
    /// Total bytes across active + sealed segments (drives compaction).
    total_bytes: u64,
}

/// Append-only WAL. Clone is cheap (shared [`Arc`]); every method holds a
/// short mutex because all operations are quick synchronous file I/O.
#[derive(Clone)]
pub struct Wal {
    inner: Arc<Mutex<WalInner>>,
}

fn segment_path(dir: &Path, index: u64) -> PathBuf {
    dir.join(format!("{SEGMENT_PREFIX}{index:0>SEGMENT_WIDTH$}"))
}

/// Parse a segment index from a file name.
fn parse_segment_name(name: &str) -> Option<u64> {
    let rest = name.strip_prefix(SEGMENT_PREFIX)?;
    if rest.len() != SEGMENT_WIDTH {
        return None;
    }
    rest.parse::<u64>().ok()
}

fn scan_segment(path: &Path, allow_torn_tail: bool) -> Result<(u64, u64, Vec<LogRecord>)> {
    // Returns (size_of_valid_bytes, last_lsn, records).
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut reader = FrameReader::new(file);
    let mut records = Vec::new();
    let mut last_lsn = 0u64;
    loop {
        let frame_start = reader.position();
        let payload = match reader.next_frame() {
            Ok(p) => p,
            Err(FrameError::Eof) => break,
            Err(FrameError::Torn) if allow_torn_tail => {
                tracing::warn!(
                    path = %path.display(),
                    valid_bytes = frame_start,
                    "truncating torn WAL tail after crash"
                );
                reader.truncate_here()?;
                break;
            }
            Err(FrameError::Torn) => {
                return Err(StoreError::Corruption(format!(
                    "torn frame inside non-tail segment {}",
                    path.display()
                )))
            }
            Err(FrameError::Corrupt(msg)) => {
                return Err(StoreError::Corruption(format!(
                    "WAL segment {}: {msg}",
                    path.display()
                )))
            }
            Err(FrameError::Io(e)) => return Err(StoreError::Io(e)),
        };
        let rec: LogRecord = bincode::deserialize(&payload).map_err(|e| {
            StoreError::Corruption(format!("invalid record in {}: {e}", path.display()))
        })?;
        last_lsn = rec.lsn.max(last_lsn);
        records.push(rec);
    }
    Ok((reader.position(), last_lsn, records))
}

impl Wal {
    /// Open (or create) the WAL in `dir`.
    ///
    /// Every valid record, in order across all segments, is fed to `apply`
    /// (a torn tail on the final segment is truncated first). Hard
    /// corruption — bad magic/CRC inside a segment, non-decodable record —
    /// produces [`StoreError::Corruption`]; LSN discipline is checked by
    /// the caller while applying.
    pub fn open<F>(dir: &Path, mut apply: F) -> Result<(Wal, Vec<LogRecord>)>
    where
        F: FnMut(&LogRecord) -> Result<()>,
    {
        fs::create_dir_all(dir)?;

        let mut indices: Vec<u64> = fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
            .filter_map(|e| e.file_name().to_str().and_then(parse_segment_name))
            .collect();
        indices.sort_unstable();
        indices.dedup();

        let mut sealed: BTreeMap<u64, SealedSegment> = BTreeMap::new();
        let mut all_records: Vec<LogRecord> = Vec::new();
        let mut total_bytes: u64 = 0;

        for (pos, &index) in indices.iter().enumerate() {
            let path = segment_path(dir, index);
            let is_last = pos + 1 == indices.len();
            let (size, end_lsn, records) = scan_segment(&path, is_last)?;
            for rec in &records {
                apply(rec)?;
                all_records.push(rec.clone());
            }
            total_bytes += size;

            if is_last {
                let active_file = OpenOptions::new().read(true).write(true).open(&path)?;
                let wal = Wal {
                    inner: Arc::new(Mutex::new(WalInner {
                        dir: dir.to_path_buf(),
                        active_index: index,
                        active_file,
                        active_size: size,
                        sealed,
                        total_bytes,
                    })),
                };
                return Ok((wal, all_records));
            }
            sealed.insert(
                index,
                SealedSegment {
                    index,
                    path,
                    size,
                    end_lsn,
                },
            );
        }

        // No segments at all: create the first one.
        let active_index = 1u64;
        let path = segment_path(dir, active_index);
        let active_file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        active_file.sync_data()?;
        sync_parent_dir(&active_file)?;
        Ok((
            Wal {
                inner: Arc::new(Mutex::new(WalInner {
                    dir: dir.to_path_buf(),
                    active_index,
                    active_file,
                    active_size: 0,
                    sealed: BTreeMap::new(),
                    total_bytes: 0,
                })),
            },
            all_records,
        ))
    }

    /// Append one record (one frame), fsyncing before return so the caller
    /// may acknowledge the commit. Rotates the active segment first when
    /// appending would cross `soft_limit_bytes`; the sealed segment then
    /// records `lsn - 1` as its end LSN (commits are serial and LSNs are
    /// contiguous).
    pub fn append(&self, rec: &LogRecord, soft_limit_bytes: u64) -> Result<u64> {
        let payload = bincode::serialize(rec)
            .map_err(|e| StoreError::Corruption(format!("failed to serialize WAL record: {e}")))?;
        let frame = crate::frame::encode_frame(&payload);
        let frame_len = frame.len() as u64;

        let mut g = self.inner.lock();
        if g.active_size > 0 && g.active_size + frame_len > soft_limit_bytes {
            self.rotate_locked(&mut g, rec.lsn.saturating_sub(1))?;
        }

        g.active_file.write_all(&frame)?;
        g.active_file.sync_data()?;
        g.active_size += frame_len;
        g.total_bytes += frame_len;
        Ok(frame_len)
    }

    /// Seal the active segment (fsync first) and open the next one.
    /// `sealed_end_lsn` is the last record LSN contained in the old segment.
    fn rotate_locked(&self, g: &mut WalInner, sealed_end_lsn: u64) -> Result<()> {
        g.active_file.sync_data()?;

        let old_index = g.active_index;
        let old_size = g.active_size;
        let old_path = segment_path(&g.dir, old_index);

        let new_index = old_index + 1;
        let new_path = segment_path(&g.dir, new_index);
        let new_file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&new_path)?;
        new_file.sync_data()?;
        sync_parent_dir(&new_file)?;

        g.sealed.insert(
            old_index,
            SealedSegment {
                index: old_index,
                path: old_path,
                size: old_size,
                end_lsn: sealed_end_lsn,
            },
        );
        g.active_file = new_file;
        g.active_index = new_index;
        g.active_size = 0;
        tracing::debug!(new_index, sealed_end_lsn, "rotated WAL segment");
        Ok(())
    }

    /// Current total bytes of all segments.
    pub fn total_bytes(&self) -> u64 {
        self.inner.lock().total_bytes
    }

    /// Index of the currently active segment.
    pub fn active_index(&self) -> u64 {
        self.inner.lock().active_index
    }

    /// Rotate without appending and atomically partition sealed segments
    /// into those fully covered by a snapshot at `snapshot_lsn` (returned
    /// for deletion) and those that must be retained. Called by the
    /// compactor while it holds the commit lock, so no append can race.
    pub fn rotate_and_take_persisted(&self, snapshot_lsn: u64) -> Result<Vec<SealedSegment>> {
        let mut g = self.inner.lock();

        if g.active_size > 0 {
            // Need exact end LSN of the live segment: scan it.
            let old_path = segment_path(&g.dir, g.active_index);
            let end_lsn = last_lsn_in_file(&old_path)?;
            self.rotate_locked(&mut g, end_lsn)?;
        }

        let mut remove = Vec::new();
        let mut keep = BTreeMap::new();
        let mut kept_bytes = 0u64;
        for (idx, seg) in g.sealed.iter() {
            if seg.end_lsn > 0 && seg.end_lsn <= snapshot_lsn {
                remove.push(SealedSegment {
                    index: seg.index,
                    path: seg.path.clone(),
                    size: seg.size,
                    end_lsn: seg.end_lsn,
                });
            } else {
                kept_bytes += seg.size;
                keep.insert(
                    *idx,
                    SealedSegment {
                        index: seg.index,
                        path: seg.path.clone(),
                        size: seg.size,
                        end_lsn: seg.end_lsn,
                    },
                );
            }
        }
        g.sealed = keep;
        // Newly rotated active segment is empty.
        g.total_bytes = kept_bytes;
        Ok(remove)
    }
}

/// Scan a segment file and return its last record's LSN (0 if empty).
fn last_lsn_in_file(path: &Path) -> Result<u64> {
    let file = OpenOptions::new().read(true).open(path)?;
    let mut reader = FrameReader::new(file);
    let mut last = 0u64;
    loop {
        match reader.next_frame() {
            Ok(payload) => {
                let rec: LogRecord = bincode::deserialize(&payload).map_err(|e| {
                    StoreError::Corruption(format!("invalid record in {}: {e}", path.display()))
                })?;
                last = rec.lsn;
            }
            Err(FrameError::Eof) => break,
            Err(other) => {
                return Err(StoreError::Corruption(format!(
                    "cannot scan sealed segment {}: {other}",
                    path.display()
                )))
            }
        }
    }
    Ok(last)
}

/// Delete a sealed segment file (missing file is fine) and best-effort
/// fsync its directory. Used by the background compactor after the
/// snapshot replacement is durable.
pub fn delete_segment(seg: &SealedSegment) -> Result<()> {
    match fs::remove_file(&seg.path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(StoreError::Io(e)),
    }
    let dir = seg.path.parent().unwrap_or_else(|| Path::new("."));
    if let Ok(dir_file) = File::open(dir) {
        let _ = dir_file.sync_data();
    }
    Ok(())
}
