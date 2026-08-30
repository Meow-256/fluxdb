# ==========================================
# FluxDB Multi-stage Production Dockerfile
# ==========================================
FROM rust:latest AS builder

WORKDIR /usr/src/fluxdb

# Copy manifest and lockfile to lock exact dependency versions
COPY Cargo.toml Cargo.lock ./

# Build dependencies cache
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release || true

# Copy full source and build complete binary suite
COPY . .
RUN cargo build --release

# ==========================================
# Runtime Image
# ==========================================
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/src/fluxdb/target/release/fluxdb-server /usr/local/bin/
COPY --from=builder /usr/src/fluxdb/target/release/fluxdb-cli /usr/local/bin/
COPY --from=builder /usr/src/fluxdb/target/release/fluxdb-bench /usr/local/bin/
COPY --from=builder /usr/src/fluxdb/target/release/fluxdb-dump /usr/local/bin/
COPY --from=builder /usr/src/fluxdb/target/release/fluxdb-load /usr/local/bin/
COPY --from=builder /usr/src/fluxdb/target/release/fluxdb-check /usr/local/bin/
COPY fluxdb.toml /app/fluxdb.toml

VOLUME [ "/app/data" ]

EXPOSE 7379 7380

ENTRYPOINT ["fluxdb-server"]
CMD ["--config", "/app/fluxdb.toml"]
