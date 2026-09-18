//! Service entry point: validate configuration, recover state from disk,
//! optionally seed the demo workload, then serve the HTTP API until a
//! shutdown signal.

use std::sync::Arc;

use kvstore::api;
use kvstore::config::Config;
use kvstore::seed;
use kvstore::store::Store;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,kvstore=debug")),
        )
        .init();

    // 1. Eagerly validated configuration: invalid values abort startup.
    let cfg = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("FATAL: {e}");
            std::process::exit(2);
        }
    };
    tracing::info!(?cfg.data_dir, ?cfg.listen_addr, "starting kvstore");

    // 2. Open store: load newest snapshot then replay the WAL after it.
    let store = match Store::open(
        cfg.data_dir.clone().into(),
        cfg.wal_compact_threshold,
        cfg.limits,
    ) {
        Ok(s) => Arc::new(s),
        Err(e) => {
            eprintln!("FATAL: failed to open store at {}: {e}", cfg.data_dir);
            std::process::exit(2);
        }
    };

    // 3. First-ever start: run the built-in demo workload.
    if cfg.seed_on_fresh {
        let st = store.status();
        if st.lsn == 0 && st.key_count == 0 {
            tracing::info!("fresh data directory; seeding demo workload");
            if let Err(e) = seed::run(store.clone()).await {
                eprintln!("FATAL: seed workload failed: {e}");
                std::process::exit(2);
            }
        }
    }

    // 4. Serve.
    let app = api::app(store);
    let listener = match tokio::net::TcpListener::bind(&cfg.listen_addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("FATAL: cannot bind {}: {e}", cfg.listen_addr);
            std::process::exit(2);
        }
    };
    tracing::info!("listening on http://{}", cfg.listen_addr);

    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutdown signal received");
    };

    if let Err(e) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
    {
        eprintln!("FATAL: server error: {e}");
        std::process::exit(1);
    }
}
