# ⚡ FluxDB

[![Rust](https://img.shields.io/badge/rust-1.80%2B-orange.svg)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Docker](https://img.shields.io/badge/docker-ready-brightgreen.svg)](Dockerfile)
[![Protocol](https://img.shields.io/badge/protocol-RESP%20%2F%20HTTP-blueviolet.svg)](#client-libraries--sdks)

**FluxDB** is an ultra-high performance, persistent LSM-Tree database engine built from scratch in Rust. It is architected for extreme point-lookup latency (**sub-microsecond cached, 20µs raw disk**), massive write throughput (**130,000+ QPS**), built-in secondary JSON indexing & live rankings, and per-table multi-codec compression (**LZ4, Zstandard, Raw**).

FluxDB is optimized for **128-bit integer/UUID keys** while seamlessly supporting **any arbitrary string or composite key** via zero-allocation deterministic 128-bit space mapping.

---

## ⚡ Key Architecture & Features

1. **Ultra-Low Read Latency (< 100 Nanoseconds Cached / 22µs Raw)**:
   * **Direct 128-Bit Integer Comparison**: 1-cycle CPU register comparison instead of byte-by-byte string comparison (`memcmp`).
   * **Sparse Block Index + In-Memory Bloom Filters**: Filters 99.9% of non-existent disk lookups.
   * **Decompressed LRU Block Cache**: Hot blocks are retained in RAM for instant **70–90 nanosecond** point lookups (8.6M+ QPS).
2. **Multi-Codec Block Compression (Per-Table Configurable)**:
   * **Zstandard (ZSTD)**: Up to **87.1% disk space reduction** (1/8 size) for massive JSON records.
   * **LZ4**: High-speed real-time compression with 2.5 GB/s decompression.
   * **NONE**: Raw zero-overhead disk persistence.
3. **100% Durability & Group Commit WAL**:
   * WAL micro-batching with configurable commit delay windows (e.g. 1000µs).
   * Asynchronous or strict per-commit `fsync` modes.
4. **Built-in Secondary Indexing & Real-Time Rankings (`TOP`, `RANK`)**:
   * SkipList-backed secondary indices on arbitrary JSON numeric paths (`stats.kills`, `profile.coins`).
   * Sub-millisecond Top-N queries (**0.03ms**) across millions of records.
5. **Multi-Table & Dynamic Hot Configuration**:
   * Tables are isolated with independent WALs, MemTables, and SSTables.
   * Dynamic tuning of block cache, worker threads, and compression via Web UI / REST API with zero downtime.
6. **Multi-Protocol & Client Support**:
   * Redis-compatible RESP text protocol on port `7379`.
   * RESTful HTTP API & Built-in Interactive Web UI on port `7380`.
   * Prometheus `/metrics` endpoint for Grafana observability.
   * Official TypeScript/Node.js client SDK.

---

## 📊 Benchmark Results

### 1. Read Latency: Uncompressed vs LZ4 vs ZSTD vs Block Cache
*Dataset: Real Hypixel Minecraft Profiles (400 KB JSON per player)*

| Metric | NONE (Raw) | LZ4 (Fast) | ZSTD (Max Compression) | **LZ4 (Block Cache ON: 64MB)** |
| :--- | :--- | :--- | :--- | :--- |
| **SSTable Disk Size** | 9,859 KB | 2,008 KB | **1,267 KB (1/8 size)** | 2,008 KB |
| **Space Saved** | 0.0% | 79.6% | **87.1% Saved** ⭐ | 79.6% |
| **Average Latency** | 22.1 µs | 159.9 µs | 219.8 µs | **0.081 µs (81 Nanoseconds)** 🚀 |
| **P50 (Median)** | 20.8 µs | 155.4 µs | 201.1 µs | **0.083 µs** |
| **Read Throughput** | 44,056 ops/s | 6,223 ops/s | 4,532 ops/s | **8,140,836 ops/s (8.1M QPS)** |

### 2. High-Concurrency Server Throughput (256 Clients, 500,000 Ops)

```text
============================================================
  FluxDB High-Concurrency Benchmark Summary
============================================================
  Write Throughput:   124,943 QPS | 0.0080 ms/op
  Point Read Lookup:  124,940 QPS | 0.0080 ms (8.00 µs) per lookup
  Top-10 Rank Query:   27,699 QPS | 0.0361 ms (36.10 µs) per query
============================================================
```

---

## 🚀 Installation & Quick Start

### 1. One-Liner Quick Install (Auto / Docker / Native)

Run the universal installer on Linux or macOS. It **automatically detects Docker** (if installed, it runs as an isolated container; otherwise, it sets up a native bare-metal systemd service):

```bash
# Automatic Detection (Docker if available, otherwise Native bare-metal):
curl -fsSL https://raw.githubusercontent.com/Meow-256/fluxdb/main/install.sh | sudo bash

# Or explicitly force Docker container mode:
curl -fsSL https://raw.githubusercontent.com/Meow-256/fluxdb/main/install.sh | sudo bash -s -- --docker

# Or explicitly force Native Bare-Metal systemd mode:
curl -fsSL https://raw.githubusercontent.com/Meow-256/fluxdb/main/install.sh | sudo bash -s -- --native
```

Once installed (whether via Docker or Native), you can immediately open the interactive database CLI by typing:

```bash
fluxdb
```

Service management commands (Native systemd):
```bash
sudo systemctl status fluxdb     # Check server status
sudo systemctl restart fluxdb    # Restart server
sudo systemctl stop fluxdb       # Stop server
```

---

### 2. Run with Docker

```bash
docker run -d \
  -p 7379:7379 \
  -p 7380:7380 \
  -v $(pwd)/data:/app/data \
  --name fluxdb \
  ghcr.io/meow-256/fluxdb:latest
```

---

### 3. Build and Run from Source

```bash
# Clone repository
git clone https://github.com/Meow-256/fluxdb.git
cd fluxdb

# Run server (TCP: 7379, HTTP/Web UI: 7380)
cargo run --release --bin fluxdb-server

# Or with custom config file:
cargo run --release --bin fluxdb-server -- --config fluxdb.toml
```

---

### 🌐 Built-in Web Management Dashboard
Open your browser and navigate to:
👉 **`http://localhost:7380`**

---

## 💻 CLI Tools

FluxDB includes a complete suite of command-line tools:

```bash
# 1. Interactive REPL CLI
cargo run --release --bin fluxdb-cli

# 2. High-Concurrency Benchmark Tool
cargo run --release --bin fluxdb-bench -- -t players -n 500000 -c 256

# 3. Export table to NDJSON
cargo run --release --bin fluxdb-dump -- --table players --output players.ndjson

# 4. Bulk import NDJSON into FluxDB
cargo run --release --bin fluxdb-load -- --table players --input players.ndjson --concurrency 32

# 5. Database Diagnostics & Health Check
cargo run --release --bin fluxdb-check -- --data-dir ./data
```

---

## 📖 Step-by-Step Tutorial

### 1. Start the Server

```bash
cargo run --release --bin fluxdb-server
```
The server will start listening on:
* **TCP Port `7379`** (RESP Protocol for CLI and SDKs)
* **HTTP Port `7380`** (Interactive Web Management UI & REST API)

---

### 2. Connect with Interactive CLI (`fluxdb-cli`)

Launch the built-in interactive CLI:
```bash
cargo run --release --bin fluxdb-cli
```

```text
Connected to FluxDB server at 127.0.0.1:7379
Type 'help' for command summary, 'quit' to exit.

fluxdb> CREATE TABLE players
+TABLE CREATED players

fluxdb> SET players steve {"name":"Steve","level":75,"stats":{"kills":1420,"coins":50000}}
+OK

fluxdb> GET players steve
{"name":"Steve","level":75,"stats":{"kills":1420,"coins":50000}}
```

---

### 3. Real-Time Partial Updates (`JSON.SET`)

Update nested JSON attributes atomically without rewriting or fetching the entire document:
```text
fluxdb> JSON.SET players steve stats.kills 1421
+OK

fluxdb> GET players steve
{"name":"Steve","level":75,"stats":{"kills":1421,"coins":50000}}
```

---

### 4. Real-Time Leaderboards & Ranking Indices

Create an in-memory SkipList secondary index on any numeric field:
```text
# 1. Register index
fluxdb> INDEX CREATE players stats.kills
+INDEX CREATED players:stats.kills

# 2. Insert records (indexed automatically in real-time)
fluxdb> SET players alex {"stats":{"kills":2100}}
+OK
fluxdb> SET players herobrine {"stats":{"kills":9999}}
+OK
fluxdb> SET players notch {"stats":{"kills":500}}
+OK

# 3. Get Top-N Leaderboard
fluxdb> TOP players stats.kills 3
[{"rank":1,"uuid":"herobrine","score":9999.0},{"rank":2,"uuid":"alex","score":2100.0},{"rank":3,"uuid":"steve","score":1421.0}]

# 4. Get Player's Rank & Score
fluxdb> RANK players stats.kills steve
{"rank":3,"score":1421.0,"table":"players","total_ranked":4,"uuid":"steve"}

# 5. Leaderboard Centered Around Player (e.g. Steve and his rivals)
fluxdb> RANK.KEY players stats.kills steve 3
[{"rank":2,"score":2100.0,"uuid":"alex"},{"rank":3,"score":1421.0,"uuid":"steve"},{"rank":4,"score":500.0,"uuid":"notch"}]

# 6. Leaderboard Centered Around Score (e.g. score ~2000)
fluxdb> RANK.SCORE players stats.kills 2000 2
[{"rank":2,"score":2100.0,"uuid":"alex"},{"rank":3,"score":1421.0,"uuid":"steve"}]

# 7. Get Score Range (e.g. scores between 1000 and 3000)
fluxdb> RANK.RANGE_SCORE players stats.kills 1000 3000
[{"rank":2,"score":2100.0,"uuid":"alex"},{"rank":3,"score":1421.0,"uuid":"steve"}]
```

---

### 5. Composite Filtering & Search

Query documents using arbitrary boolean expressions across JSON paths:
```text
# Search by condition
fluxdb> FILTER players "stats.kills > 1000 AND stats.coins >= 50000"
[{"data":{"name":"Steve","stats":{"coins":50000,"kills":1421}},"uuid":"steve"}]

# Count matching documents
fluxdb> COUNT players "stats.kills > 1000"
:3
```

---

### 6. Using the TypeScript / Node.js SDK

Install the package via npm:
```bash
npm install @meow256/fluxdb
```

Full usage example:
```typescript
import { FluxDB } from '@meow256/fluxdb';

async function main() {
  const db = new FluxDB({ host: '127.0.0.1', port: 7379, table: 'players' });
  await db.connect();

  // 1. Put & Get
  await db.set('steve', { level: 80, stats: { kills: 1500, coins: 25000 } });
  const player = await db.get('steve');
  console.log('Player data:', player);

  // 2. Atomic field update
  await db.jsonSet('steve', 'stats.kills', 1550);

  // 3. Top Leaderboard & Around Queries
  const topPlayers = await db.top('stats.kills', 10);
  console.log('Top 10:', topPlayers);

  const rivals = await db.aroundKey('stats.kills', 'steve', 5);
  console.log('Around Steve:', rivals);

  const tierScores = await db.rankingByScoreRange('stats.kills', 1000, 2000);
  console.log('Score range 1000-2000:', tierScores);

  db.close();
}

main().catch(console.error);
```

---

## 📦 Client Libraries & SDKs

### Official TypeScript SDK

Available on npm as [`@meow256/fluxdb`](https://www.npmjs.com/package/@meow256/fluxdb):

```bash
npm install @meow256/fluxdb
```

Supports full async/await APIs for Point Get/Set, Partial Updates (`jsonSet`), Batch MGet/MSet, and Advanced Flexible Rankings (`top`, `rank`, `aroundKey`, `aroundScore`, `rankingByScoreRange`, `rankingByRankRange`). Check the Tutorial section above for complete code examples.

---

## 🛠️ Command Reference (RESP / TCP Port 7379)

| Command | Description | Example |
| :--- | :--- | :--- |
| `AUTH <pass>` | Authenticate client session | `AUTH secret123` |
| `TABLES` | List all tables | `TABLES` |
| `CREATE TABLE <name>` | Explicitly create a table | `CREATE TABLE guilds` |
| `SET <table> <key> <json>` | Put/update a record | `SET players steve {"kills":42}` |
| `MSET <table> <k1> <v1> ...` | Batch write multiple keys | `MSET players k1 {"a":1} k2 {"a":2}` |
| `GET <table> <key>` | Fast point lookup | `GET players steve` |
| `MGET <table> <k1> <k2> ...` | Batch read multiple keys | `MGET players k1 k2` |
| `DEL <table> <key>` | Delete key (write tombstone) | `DEL players steve` |
| `JSON.SET <table> <k> <path> <v>` | Atomic update of inner JSON field | `JSON.SET players steve stats.kills 43` |
| `INDEX CREATE <table> <path>` | Create ranking index on JSON field | `INDEX CREATE players stats.kills` |
| `TOP <table> <path> [limit]` | Get top N sorted leaderboard | `TOP players stats.kills 10` |
| `RANK <table> <path> <key>` | Get player's current rank & score | `RANK players stats.kills steve` |
| `RANK.KEY <table> <path> <k> [n]` | Leaderboard centered around player key | `RANK.KEY players stats.kills steve 10` |
| `RANK.SCORE <table> <path> <s> [n]` | Leaderboard centered around target score | `RANK.SCORE players stats.kills 1500 10` |
| `RANK.RANGE_SCORE <table> <p> <min> <max> [n]` | Get records in score range | `RANK.RANGE_SCORE players stats.kills 50 100 50` |
| `RANK.RANGE <table> <path> <start> <end>` | Get records in rank range | `RANK.RANGE players stats.kills 30 50` |
| `SCAN <table> [start] [end] [n]` | Key-range sequential scan | `SCAN players - + 50` |
| `FILTER <table> "<query>" [n]` | Search with composite JSON filters | `FILTER players "level >= 50 AND vip == true"` |
| `EXPIRE <table> <key> <sec>` | Set key expiration (TTL) | `EXPIRE players steve 300` |
| `TTL <table> <key>` | Get remaining TTL in seconds | `TTL players steve` |
| `BACKUP [target_dir]` | Live point-in-time snapshot backup | `BACKUP` |
| `FLUSH [table]` | Force flush MemTable to disk | `FLUSH` |
| `PING` | Health check (returns `+PONG`) | `PING` |

---

## 📈 Monitoring & Prometheus

FluxDB exposes standard Prometheus metrics on:
👉 **`http://localhost:7380/metrics`**

Metrics exposed include:
- `fluxdb_up`: Server operational status
- `fluxdb_total_disk_bytes`: Total persistent disk consumption
- `fluxdb_block_cache_capacity_bytes`: Active cache capacity
- `fluxdb_table_records{table="..."}`: Record count per table
- `fluxdb_table_sstable_count{table="..."}`: SSTable file count
- `fluxdb_table_memtable_records{table="..."}`: In-memory active entries

---

## 📄 License
FluxDB is licensed under the [MIT License](LICENSE).
