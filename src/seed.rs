//! Built-in demo workload, run once when the data directory is fresh.
//!
//! It performs a deterministic sequence of single-key writes followed by
//! one multi-key atomic transaction (a conditional transfer-like update
//! guarded by compare-and-swap semantics). After it completes the caller
//! can observe that all keys of the transaction became visible together.

use std::sync::Arc;

use crate::error::Result;
use crate::store::{Op, Store};

/// Run the seed workload. Should only be called on a fresh store
/// (LSN == 0, key count == 0).
pub async fn run(store: Arc<Store>) -> Result<()> {
    store
        .put(b"user:1:name".to_vec(), b"alice".to_vec())
        .await?;
    store
        .put(b"user:1:balance".to_vec(), b"100".to_vec())
        .await?;
    store.put(b"user:2:name".to_vec(), b"bob".to_vec()).await?;
    store
        .put(b"user:2:balance".to_vec(), b"50".to_vec())
        .await?;

    // --- one multi-key atomic transaction ---
    // Conditionally move 30 from alice to bob. The transaction asserts the
    // current balances via CAS-style ops; because both preconditions hold
    // here the whole batch commits atomically.
    let txn = vec![
        Op::Cas {
            key: b"user:1:balance".to_vec(),
            expected: Some(b"100".to_vec()),
            value: b"70".to_vec(),
        },
        Op::Cas {
            key: b"user:2:balance".to_vec(),
            expected: Some(b"50".to_vec()),
            value: b"80".to_vec(),
        },
        Op::Put {
            key: b"ledger:transfer:1".to_vec(),
            value: b"user:1->user:2:30".to_vec(),
        },
    ];
    let out = store.commit(txn).await?;

    tracing::info!(
        seed_lsn = out.lsn,
        ops = out.effects.len(),
        "seed workload committed"
    );
    Ok(())
}
