# syntax=docker/dockerfile:1

# ---- build stage ----
FROM rust:1.98-slim-bookworm AS builder

WORKDIR /build

# Dependency manifest first to leverage layer caching.
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo

# Create a stub crate so third-party dependencies build independently of
# source-file changes.
RUN mkdir -p src && \
    printf 'fn main() {}\n' > src/main.rs && \
    : > src/lib.rs && \
    cargo build --release

# Real sources; rebuilding only recompiles the kvstore crate itself.
COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release

# ---- runtime stage ----
FROM debian:bookworm-slim AS runtime

# Minimal runtime needs: CA certs not strictly required (no outbound TLS),
# but kept for operational tooling; tini for proper signal handling.
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Run as a non-root user that owns the data volume.
RUN useradd --system --uid 10001 --user-group --home-dir /data kvstore && \
    mkdir -p /data && chown kvstore:kvstore /data

COPY --from=builder /build/target/release/kvstore /usr/local/bin/kvstore

USER kvstore
WORKDIR /data
VOLUME ["/data"]

ENV DATA_DIR=/data \
    LISTEN_ADDR=0.0.0.0:8080 \
    WAL_COMPACT_THRESHOLD=4194304 \
    MAX_KEY_BYTES=65536 \
    MAX_VALUE_BYTES=16777216 \
    MAX_OPS_PER_TXN=1024 \
    SEED_ON_FRESH=true \
    RUST_LOG=info

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/kvstore"]
