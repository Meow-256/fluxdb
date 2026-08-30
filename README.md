# 🐱 MeowDB

**MeowDB** は、**UUID（128-bit / 16バイト固定長バイナリ）** をプライマリキーとし、**JSON ドキュメント** を超高速に格納・インデックス化するために設計された、Rust製の高並行・高耐久 LSM-Tree データベースシステムです。

---

## ⚡ 主な機能

1. **データ消失ゼロ（100% Durability）× 超高速同時書き込み**:
   * WAL（先行ログ）への **Group Commit（マイクロバッチング＋`fsync`）** により、クラッシュ時でも確実にデータを復元。
   * マルチクライアント（512+並行）で **毎秒 130,000+ QPS** の書き込みスループットを達成。
2. **UUID特化の超高速ポイントルックアップ**:
   * UUID（128-bit / 16 bytes）を内部で整数（`u128`）として扱い、CPUレジスタ単位で高速比較。
   * **Bloom Filter**: SSTableごとにBloom Filterをメモリ上に保持し、存在しないUUIDの無駄なディスクI/Oを99%以上排除。
   * **Sparse Block Index**: 該当ブロックを二分探索で一発特定（**1件あたり 0.006ms / 6.4µs**）。
3. **バッチ一括操作（MGET / MSET）**:
   * 1往復の通信で数百件のUUIDを一括取得・一括保存。ネットワークオーバーヘッドを極小化。
4. **存在確認（EXISTS） & 有効期限（EXPIRE / TTL）**:
   * データ本体を読まずにBloom Filterで存在確認 (`EXISTS`)。
   * セッションやキャッシュ用途に秒単位で自動失効する有効期限 (`EXPIRE` / `TTL`) をサポート。
5. **JSONフィールドの超高速ランキング（セカンダリ・インデックス）**:
   * `data.score` や `stats.kills` などの任意のJSONパスを指定してインデックス化。
   * `TOP` や `RANK` クエリに対して **ミリ秒未満（0.03ms）で即座に応答**。
6. **マルチテーブル（Multi-Table）アーキテクチャ**:
   * テーブルごとに独立した WAL・MemTable・SSTable・インデックスを完全分離。
7. **無停止バックアップ（BACKUP）**:
   * 稼働を止めずに安全にスナップショットをタイムスタンプ付きディレクトリへ退避。
8. **パスワード認証（AUTH）**:
   * 本番公開時の安全な接続保護（`--require-pass` フラグ）。
9. **内蔵 Web UI ダッシュボード**:
   * ポート `7380` でブラウザからテーブル一覧、メトリクス、UUIDデータ検索、ランキング閲覧、Flush/Backup実行が可能。

---

## 🛠️ コマンドリファレンス

| コマンド | 説明 | 例 |
| :--- | :--- | :--- |
| `AUTH <password>` | パスワード認証 | `AUTH secret123` |
| `TABLES` / `SHOW TABLES` | 存在する全テーブル一覧を取得 | `TABLES` |
| `CREATE TABLE <name>` | 新規テーブルを作成 | `CREATE TABLE guilds` |
| `SET <table> <UUID> <JSON>` | データを保存・更新 | `SET users 069a... {"name":"Alex","level":42}` |
| `MSET <table> <UUID1> <JSON1> ...` | 複数レコードを一括保存 | `MSET users <UUID1> {"score":10} <UUID2> {"score":20}` |
| `GET <table> <UUID>` | UUIDでデータを高速検索 | `GET users 069a...` |
| `MGET <table> <UUID1> <UUID2> ...` | 複数UUIDのデータを一括取得 | `MGET users <UUID1> <UUID2>` |
| `DEL <table> <UUID>` | データを削除 | `DEL users 069a...` |
| `EXISTS <table> <UUID1> ...` | キーの存在件数を高速確認 | `EXISTS users <UUID1>` |
| `EXPIRE <table> <UUID> <sec>` | 有効期限（TTL）を設定 | `EXPIRE users 069a... 300` |
| `TTL <table> <UUID>` | 残り有効期限（秒）を取得 | `TTL users 069a...` |
| `BACKUP [dir]` | 無停止スナップショットバックアップ | `BACKUP` |
| `INDEX CREATE <table> <path>` | 指定JSONパスにランキングインデックス作成 | `INDEX CREATE users level` |
| `TOP <table> <path> [limit]` | 指定フィールドのTop Nランキング取得 | `TOP users level 10` |
| `RANK <table> <path> <UUID>` | 指定UUIDの現在の順位とスコア取得 | `RANK users level 069a...` |
| `STATS [table]` | テーブルごとのレコード数・SSTable数表示 | `STATS users` |
| `FLUSH [table]` | メモリ上のデータをSSTableへ強制書き出し | `FLUSH` |
| `PING` | 死活監視ヘルスチェック | `PING` (応答: `+PONG`) |

---

## 🚀 クイックスタート

### 1. サーバー起動

```bash
# サーバー起動 (TCP: 7379 / Web UI: 7380)
cargo run --release --bin meowdb-server -- --async-fsync

# パスワード認証を有効にして起動する場合:
# cargo run --release --bin meowdb-server -- --async-fsync --require-pass my_password
```

### 2. Web UI 管理画面
ブラウザで 👉 **`http://localhost:7380`** にアクセス

### 3. CLI クライアント

```bash
cargo run --release --bin meowdb-cli

# パスワード認証付きで接続する場合:
# cargo run --release --bin meowdb-cli -- -a my_password
```

### 4. ベンチマークツールの実行

```bash
# 512並行クライアントで500万件の書き込み・読み込み・ランキング性能を測定
cargo run --release --bin meowdb-bench -- -c 512 -n 5000000
```
