//! Lightweight in-process metrics, exported in Prometheus text exposition
//! format at `/metrics`. Counters are atomic so incrementing never blocks.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use parking_lot::Mutex;

#[derive(Debug)]
pub struct Metrics {
    pub start_time: Instant,
    pub requests_total: AtomicU64,
    pub requests_by_status_2xx: AtomicU64,
    pub requests_by_status_4xx: AtomicU64,
    pub requests_by_status_5xx: AtomicU64,
    pub commits_total: AtomicU64,
    pub committed_ops_total: AtomicU64,
    pub cas_conflicts_total: AtomicU64,
    pub rejected_txns_total: AtomicU64,
    pub reads_total: AtomicU64,
    pub scans_total: AtomicU64,
    pub compactions_total: AtomicU64,
    pub compactions_failed_total: AtomicU64,
    pub wal_bytes_written_total: AtomicU64,
    pub snapshot_bytes_written_total: AtomicU64,
    pub keys: AtomicU64,
    pub lsn: AtomicU64,
    pub wal_total_bytes: AtomicU64,
    pub compacting: AtomicU64,
    pub last_snapshot_lsn: AtomicU64,
    pub last_compaction_duration_seconds: Mutex<f64>,
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics {
            start_time: Instant::now(),
            requests_total: AtomicU64::new(0),
            requests_by_status_2xx: AtomicU64::new(0),
            requests_by_status_4xx: AtomicU64::new(0),
            requests_by_status_5xx: AtomicU64::new(0),
            commits_total: AtomicU64::new(0),
            committed_ops_total: AtomicU64::new(0),
            cas_conflicts_total: AtomicU64::new(0),
            rejected_txns_total: AtomicU64::new(0),
            reads_total: AtomicU64::new(0),
            scans_total: AtomicU64::new(0),
            compactions_total: AtomicU64::new(0),
            compactions_failed_total: AtomicU64::new(0),
            wal_bytes_written_total: AtomicU64::new(0),
            snapshot_bytes_written_total: AtomicU64::new(0),
            keys: AtomicU64::new(0),
            lsn: AtomicU64::new(0),
            wal_total_bytes: AtomicU64::new(0),
            compacting: AtomicU64::new(0),
            last_snapshot_lsn: AtomicU64::new(0),
            last_compaction_duration_seconds: Mutex::new(0.0),
        }
    }
}

impl Metrics {
    pub fn record_status(&self, status: u16) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        let bucket = match status {
            200..=299 => &self.requests_by_status_2xx,
            400..=499 => &self.requests_by_status_4xx,
            _ => &self.requests_by_status_5xx,
        };
        bucket.fetch_add(1, Ordering::Relaxed);
    }

    pub fn render_prometheus(&self) -> String {
        let uptime = self.start_time.elapsed().as_secs_f64();
        let last_dur = *self.last_compaction_duration_seconds.lock();
        format!(
            "# HELP kvstore_uptime_seconds Process uptime in seconds.\n\
             # TYPE kvstore_uptime_seconds gauge\n\
             kvstore_uptime_seconds {uptime:.6}\n\
             # HELP kvstore_http_requests_total Total HTTP requests by status class.\n\
             # TYPE kvstore_http_requests_total counter\n\
             kvstore_http_requests_total{{status_class=\"2xx\"}} {a}\n\
             kvstore_http_requests_total{{status_class=\"4xx\"}} {b}\n\
             kvstore_http_requests_total{{status_class=\"5xx\"}} {c}\n\
             # HELP kvstore_commits_total Committed transactions.\n\
             # TYPE kvstore_commits_total counter\n\
             kvstore_commits_total {d}\n\
             # HELP kvstore_committed_ops_total Individual ops applied in committed transactions.\n\
             # TYPE kvstore_committed_ops_total counter\n\
             kvstore_committed_ops_total {e}\n\
             # HELP kvstore_cas_conflicts_total Failed compare-and-swap checks.\n\
             # TYPE kvstore_cas_conflicts_total counter\n\
             kvstore_cas_conflicts_total {f}\n\
             # HELP kvstore_rejected_transactions_total Transactions rejected before commit.\n\
             # TYPE kvstore_rejected_transactions_total counter\n\
             kvstore_rejected_transactions_total {g}\n\
             # HELP kvstore_reads_total Single-key reads served.\n\
             # TYPE kvstore_reads_total counter\n\
             kvstore_reads_total {h}\n\
             # HELP kvstore_scans_total Prefix/range scans served.\n\
             # TYPE kvstore_scans_total counter\n\
             kvstore_scans_total {i}\n\
             # HELP kvstore_compactions_total Completed snapshot compactions.\n\
             # TYPE kvstore_compactions_total counter\n\
             kvstore_compactions_total {j}\n\
             # HELP kvstore_compactions_failed_total Failed snapshot compactions.\n\
             # TYPE kvstore_compactions_failed_total counter\n\
             kvstore_compactions_failed_total {k}\n\
             # HELP kvstore_wal_bytes_written_total Bytes appended and fsynced to the WAL.\n\
             # TYPE kvstore_wal_bytes_written_total counter\n\
             kvstore_wal_bytes_written_total {l}\n\
             # HELP kvstore_keys_current Current number of live keys.\n\
             # TYPE kvstore_keys_current gauge\n\
             kvstore_keys_current {m}\n\
             # HELP kvstore_lsn Current commit log sequence number.\n\
             # TYPE kvstore_lsn gauge\n\
             kvstore_lsn {n}\n\
             # HELP kvstore_wal_bytes_current Current total on-disk WAL size in bytes.\n\
             # TYPE kvstore_wal_bytes_current gauge\n\
             kvstore_wal_bytes_current {o}\n\
             # HELP kvstore_compacting 1 while a snapshot compaction is running.\n\
             # TYPE kvstore_compacting gauge\n\
             kvstore_compacting {p}\n\
             # HELP kvstore_last_snapshot_lsn LSN of the most recent durable snapshot.\n\
             # TYPE kvstore_last_snapshot_lsn gauge\n\
             kvstore_last_snapshot_lsn {q}\n\
             # HELP kvstore_last_compaction_duration_seconds Wall-clock duration of the last compaction.\n\
             # TYPE kvstore_last_compaction_duration_seconds gauge\n\
             kvstore_last_compaction_duration_seconds {last_dur:.6}\n",
            a = self.requests_by_status_2xx.load(Ordering::Relaxed),
            b = self.requests_by_status_4xx.load(Ordering::Relaxed),
            c = self.requests_by_status_5xx.load(Ordering::Relaxed),
            d = self.commits_total.load(Ordering::Relaxed),
            e = self.committed_ops_total.load(Ordering::Relaxed),
            f = self.cas_conflicts_total.load(Ordering::Relaxed),
            g = self.rejected_txns_total.load(Ordering::Relaxed),
            h = self.reads_total.load(Ordering::Relaxed),
            i = self.scans_total.load(Ordering::Relaxed),
            j = self.compactions_total.load(Ordering::Relaxed),
            k = self.compactions_failed_total.load(Ordering::Relaxed),
            l = self.wal_bytes_written_total.load(Ordering::Relaxed),
            m = self.keys.load(Ordering::Relaxed),
            n = self.lsn.load(Ordering::Relaxed),
            o = self.wal_total_bytes.load(Ordering::Relaxed),
            p = self.compacting.load(Ordering::Relaxed),
            q = self.last_snapshot_lsn.load(Ordering::Relaxed),
        )
    }
}
