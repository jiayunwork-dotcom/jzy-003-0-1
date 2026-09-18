//! Binary framing for WAL records and snapshots.
//!
//! Frame layout (all integers little-endian):
//!
//! ```text
//! +-----------+----------------+-------------+
//! | magic(4)  | payload_len(4) | payload     |
//! | 0x4B565331| u32            | len bytes   |
//! +-----------+----------------+-------------+
//! ```
//!
//! followed by a CRC32-Checksum frame:
//!
//! ```text
//! +-----------+------------+
//! | magic(4)  | crc32(4)   |
//! | 0x4B565332| u32        |
//! +-----------+------------+
//! ```
//!
//! Framing payload and checksum separately lets the reader detect a torn
//! tail (a partially appended record after a crash) without confusing it
//! with payload corruption: a short read at a frame boundary is a clean
//! EOF, a short read mid-frame or a bad CRC is a torn record.

use std::io::{self, Read, Seek, SeekFrom, Write};

use crc32fast::Hasher;

/// Magic marking the start of a payload frame ("KVS1").
pub const MAGIC_PAYLOAD: [u8; 4] = [0x4B, 0x56, 0x53, 0x31];
/// Magic marking the checksum frame that follows the payload.
pub const MAGIC_CHECKSUM: [u8; 4] = [0x4B, 0x56, 0x53, 0x32];

/// Maximum payload size of a single frame (64 MiB). A frame larger than
/// this is treated as corruption rather than allocated.
pub const MAX_PAYLOAD_LEN: u32 = 64 * 1024 * 1024;

/// Errors encountered while reading frames.
#[derive(Debug)]
pub enum FrameError {
    /// Clean end of file exactly between frames.
    Eof,
    /// A frame was only partially present (torn tail after a crash).
    Torn,
    /// Frame magic/CRC/length check failed.
    Corrupt(String),
    /// Underlying I/O failure.
    Io(io::Error),
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::Eof => write!(f, "end of frames"),
            FrameError::Torn => write!(f, "torn (incomplete) frame at tail"),
            FrameError::Corrupt(msg) => write!(f, "corrupt frame: {msg}"),
            FrameError::Io(e) => write!(f, "I/O error: {e}"),
        }
    }
}

impl std::error::Error for FrameError {}

impl From<io::Error> for FrameError {
    fn from(e: io::Error) -> Self {
        FrameError::Io(e)
    }
}

/// Encode `payload` into the two-frame wire format.
pub fn encode_frame(payload: &[u8]) -> Vec<u8> {
    let mut hasher = Hasher::new();
    hasher.update(payload);
    let crc = hasher.finalize();

    let mut buf = Vec::with_capacity(16 + payload.len());
    buf.extend_from_slice(&MAGIC_PAYLOAD);
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
    buf.extend_from_slice(&MAGIC_CHECKSUM);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

/// Streaming frame reader over a seekable file.
pub struct FrameReader<R: Read + Seek> {
    inner: R,
    /// Byte offset of the next frame to read.
    pos: u64,
}

impl<R: Read + Seek> FrameReader<R> {
    pub fn new(inner: R) -> Self {
        FrameReader { inner, pos: 0 }
    }

    /// Current byte offset of the reader (also the length of valid data so far).
    pub fn position(&self) -> u64 {
        self.pos
    }

    /// Read exactly `buf.len()` bytes. Returns:
    /// - `Ok(true)` if the buffer was fully read,
    /// - `Ok(false)` if zero bytes were available (clean EOF),
    /// - `Err(FrameError::Torn)` if some but not all bytes were read.
    fn fill(&mut self, buf: &mut [u8]) -> Result<bool, FrameError> {
        let mut filled = 0;
        while filled < buf.len() {
            match self.inner.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(FrameError::Io(e)),
            }
        }
        self.pos += filled as u64;
        if filled == buf.len() {
            Ok(true)
        } else if filled == 0 {
            Ok(false)
        } else {
            Err(FrameError::Torn)
        }
    }

    /// Read the next full frame. Returns [`FrameError::Eof`] at a clean
    /// frame boundary and [`FrameError::Torn`] for a partial tail frame.
    pub fn next_frame(&mut self) -> Result<Vec<u8>, FrameError> {
        let mut magic = [0u8; 4];
        if !self.fill(&mut magic)? {
            return Err(FrameError::Eof);
        }
        if magic != MAGIC_PAYLOAD {
            return Err(FrameError::Corrupt(format!(
                "bad payload magic {magic:02x?} at offset {}",
                self.pos - 4
            )));
        }

        let mut len_bytes = [0u8; 4];
        if !self.fill(&mut len_bytes)? {
            return Err(FrameError::Torn);
        }
        let len = u32::from_le_bytes(len_bytes);
        if len > MAX_PAYLOAD_LEN {
            return Err(FrameError::Corrupt(format!(
                "frame payload length {len} exceeds limit {MAX_PAYLOAD_LEN}"
            )));
        }

        let mut payload = vec![0u8; len as usize];
        if !self.fill(&mut payload)? {
            return Err(FrameError::Torn);
        }

        let mut crc_magic = [0u8; 4];
        if !self.fill(&mut crc_magic)? {
            return Err(FrameError::Torn);
        }
        if crc_magic != MAGIC_CHECKSUM {
            return Err(FrameError::Corrupt("bad checksum frame magic".to_string()));
        }

        let mut crc_bytes = [0u8; 4];
        if !self.fill(&mut crc_bytes)? {
            return Err(FrameError::Torn);
        }
        let stored_crc = u32::from_le_bytes(crc_bytes);

        let mut hasher = Hasher::new();
        hasher.update(&payload);
        if hasher.finalize() != stored_crc {
            return Err(FrameError::Corrupt(format!(
                "CRC mismatch at frame ending at offset {}",
                self.pos
            )));
        }

        Ok(payload)
    }
}

impl FrameReader<std::fs::File> {
    /// Truncate the underlying file at the current reader position, then
    /// sync both file and directory. Used to drop a torn tail on open.
    pub fn truncate_here(&mut self) -> io::Result<()> {
        self.inner.set_len(self.pos)?;
        self.inner.sync_data()?;
        if let Some(dir) = dir_of_open_file(&self.inner) {
            let dir = std::fs::File::open(dir)?;
            dir.sync_data()?;
        }
        Ok(())
    }
}

/// fsync the directory containing `file`, required after creating,
/// renaming or unlinking files on Linux for durable directory entries.
/// On platforms where the open file's path cannot be resolved via
/// /proc/self/fd the directory sync is skipped (best-effort refinement;
/// file contents themselves are always synced).
pub fn sync_parent_dir(file: &std::fs::File) -> io::Result<()> {
    if let Some(parent) = dir_of_open_file(file) {
        let dir = std::fs::File::open(parent)?;
        dir.sync_data()?;
    }
    Ok(())
}

/// Best-effort parent directory path of an open file.
fn dir_of_open_file(file: &std::fs::File) -> Option<std::path::PathBuf> {
    use std::os::unix::io::AsRawFd;
    let link = format!("/proc/self/fd/{}", file.as_raw_fd());
    let path = std::fs::read_link(&link).ok()?;
    path.parent().map(|p| p.to_path_buf())
}

/// Append an encoded frame to a writer (no fsync; caller decides durability).
pub fn write_frame<W: Write>(w: &mut W, payload: &[u8]) -> io::Result<()> {
    w.write_all(&encode_frame(payload))
}

impl<R: Read + Seek> Seek for FrameReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new = self.inner.seek(pos)?;
        self.pos = new;
        Ok(new)
    }
}
