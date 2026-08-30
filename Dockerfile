# ==========================================
# MeowDB Multi-stage Production Dockerfile
# ==========================================
FROM rust:1.80-bullseye AS builder

WORKDIR /usr/src/meowdb

# Cache dependencies
COPY Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release || true

# Build complete binary suite
COPY . .
RUN cargo build --release

# Runtime image
FROM debian:bullseye-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/src/meowdb/target/release/meowdb-server /usr/local/bin/
COPY --from=builder /usr/src/meowdb/target/release/meowdb-cli /usr/local/bin/
COPY --from=builder /usr/src/meowdb/target/release/meowdb-bench /usr/local/bin/
COPY --from=builder /usr/src/meowdb/target/release/meowdb-dump /usr/local/bin/
COPY --from=builder /usr/src/meowdb/target/release/meowdb-load /usr/local/bin/
COPY --from=builder /usr/src/meowdb/target/release/meowdb-check /usr/local/bin/
COPY meowdb.toml /app/meowdb.toml

VOLUME [ "/app/data" ]

EXPOSE 7379 7380

ENTRYPOINT ["meowdb-server"]
CMD ["--config", "/app/meowdb.toml"]
