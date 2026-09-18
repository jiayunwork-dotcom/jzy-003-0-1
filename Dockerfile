# syntax=docker/dockerfile:1

# ---------- build stage ----------
FROM rust:1.82-bookworm AS builder
WORKDIR /build

# Cache dependencies: dummy crate first (this project has both a lib and a bin).
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && printf '' > src/lib.rs \
    && printf 'fn main() {}' > src/main.rs \
    && (cargo build --release -p memtxn-kvs || true) \
    && (cargo build --release || true)

COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release --locked -p memtxn-kvs \
    && cp target/release/kvs /usr/local/bin/kvs

# ---------- runtime stage ----------
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --user-group kvs

COPY --from=builder /usr/local/bin/kvs /usr/local/bin/kvs
COPY scripts/demo.sh /usr/local/bin/demo.sh
RUN chmod +x /usr/local/bin/demo.sh /usr/local/bin/kvs

USER kvs
WORKDIR /data
ENV DATA_DIR=/data \
    LISTEN_ADDR=0.0.0.0:8080 \
    RUST_LOG=info
VOLUME ["/data"]
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/health || exit 1
ENTRYPOINT ["kvs"]
