use memtxn_kvs::config::Config;
use memtxn_kvs::store::Store;
use std::net::SocketAddr;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Validate configuration at startup; refuse to boot on illegal values.
    let config = Config::from_env().unwrap_or_else(|e| {
        eprintln!("invalid configuration: {e}");
        std::process::exit(2);
    });

    let addr: SocketAddr = config.listen_addr.parse().unwrap_or_else(|e| {
        eprintln!("invalid LISTEN_ADDR {}: {e}", config.listen_addr);
        std::process::exit(2);
    });

    let store = Store::open(config).await?;
    let app = memtxn_kvs::server::router(store);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "memtxn-kvs listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    tracing::info!("shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
}
