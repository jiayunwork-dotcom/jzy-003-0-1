//! Standalone in-memory transactional key-value store.
//!
//! Crate layout:
//! - [`config`]: startup configuration with eager validation
//! - [`error`]: structured, distinguishable error types
//! - [`frame`]: CRC-checked binary framing
//! - [`record`]: WAL/snapshot record types
//! - [`wal`]: segmented write-ahead log
//! - [`snapshot`]: full-state snapshots and recovery
//! - [`store`]: in-memory table, transactions, versions and compaction
//! - [`metrics`]: Prometheus metrics
//! - [`api`]: HTTP interface
//! - [`seed`]: built-in demo workload

pub mod api;
pub mod config;
pub mod error;
pub mod frame;
pub mod metrics;
pub mod record;
pub mod seed;
pub mod snapshot;
pub mod store;
pub mod wal;
