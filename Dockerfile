# Build stage
FROM rust:1.80-slim-bullseye AS builder

WORKDIR /usr/src/meowdb
COPY . .

RUN cargo build --release

# Runtime stage
FROM debian:bullseye-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /usr/src/meowdb/target/release/meowdb-server /usr/local/bin/
COPY --from=builder /usr/src/meowdb/target/release/meowdb-cli /usr/local/bin/
COPY --from=builder /usr/src/meowdb/target/release/meowdb-bench /usr/local/bin/

# Persistent data volume
VOLUME ["/data"]
ENV RUST_LOG=info

EXPOSE 7379 7380

ENTRYPOINT ["meowdb-server", "--bind", "0.0.0.0:7379", "--http-bind", "0.0.0.0:7380", "--data-dir", "/data"]
