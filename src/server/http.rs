use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

use crate::core::types::PlayerId;
use crate::table::TableManager;

pub struct HttpServer {
    addr: SocketAddr,
    table_manager: Arc<TableManager>,
    require_pass: Option<String>,
}

impl HttpServer {
    pub fn new(addr: SocketAddr, table_manager: Arc<TableManager>, require_pass: Option<String>) -> Self {
        Self { addr, table_manager, require_pass }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(self.addr).await?;
        info!("MeowDB Web UI Dashboard listening on http://{}", self.addr);

        loop {
            let (socket, _) = match listener.accept().await {
                Ok(res) => res,
                Err(e) => {
                    warn!("HTTP accept error: {}", e);
                    continue;
                }
            };

            let table_mgr = self.table_manager.clone();
            let require_pass = self.require_pass.clone();
            tokio::spawn(async move {
                let _ = handle_http_client(socket, table_mgr, require_pass).await;
            });
        }
    }
}

async fn handle_http_client(
    mut stream: TcpStream,
    table_manager: Arc<TableManager>,
    require_pass: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = [0u8; 8192];
    let bytes_read = stream.read(&mut buf).await?;
    if bytes_read == 0 {
        return Ok(());
    }

    let req_str = String::from_utf8_lossy(&buf[..bytes_read]);
    let mut lines = req_str.lines();
    let request_line = match lines.next() {
        Some(l) => l,
        None => return Ok(()),
    };

    let mut parts = request_line.split_whitespace();
    let _method = parts.next().unwrap_or("GET");
    let full_path = parts.next().unwrap_or("/");

    let (path, query) = if let Some(idx) = full_path.find('?') {
        (&full_path[..idx], &full_path[idx + 1..])
    } else {
        (full_path, "")
    };

    // Check optional HTTP auth token in query param (?token=password)
    if let Some(ref pass) = require_pass {
        let token = extract_query_param(query, "token").unwrap_or_default();
        if path.starts_with("/api/") && token != *pass {
            let resp = serde_json::json!({ "error": "Unauthorized. Provide ?token=password" });
            send_response(&mut stream, "401 Unauthorized", "application/json", &resp.to_string()).await?;
            return Ok(());
        }
    }

    if path == "/" || path == "/index.html" {
        send_response(&mut stream, "200 OK", "text/html; charset=utf-8", WEB_UI_HTML).await?;
    } else if path == "/api/tables" {
        let tables = table_manager.list_tables();
        let total_size = table_manager.total_disk_size_bytes();
        let resp = serde_json::json!({
            "tables": tables,
            "total_disk_size_bytes": total_size,
            "total_disk_size_human": format_bytes(total_size)
        });
        send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
    } else if path == "/api/table/create" {
        let name = extract_query_param(query, "name").unwrap_or_default().trim().to_lowercase();
        if !name.is_empty() {
            match table_manager.create_table(&name).await {
                Ok(_) => {
                    let resp = serde_json::json!({ "success": true, "table": name });
                    send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
                }
                Err(e) => {
                    let resp = serde_json::json!({ "error": e.to_string() });
                    send_response(&mut stream, "500 Internal Error", "application/json", &resp.to_string()).await?;
                }
            }
        } else {
            let resp = serde_json::json!({ "error": "Missing table name" });
            send_response(&mut stream, "400 Bad Request", "application/json", &resp.to_string()).await?;
        }
    } else if path == "/api/stats" {
        let table_name = extract_query_param(query, "table").unwrap_or_default();
        if let Some(table) = table_manager.get_table(&table_name) {
            let (mem_len, imm_len, sst_len) = table.engine.stats();
            let total_entries = table.engine.total_entries();
            let disk_bytes = table.engine.disk_size_bytes();
            let indices = table.index_manager.list_indices();
            let data = serde_json::json!({
                "status": "online",
                "table": table_name,
                "total_records": total_entries,
                "active_memtable_entries": mem_len,
                "immutable_memtables": imm_len,
                "sstable_count": sst_len,
                "disk_size_bytes": disk_bytes,
                "disk_size_human": format_bytes(disk_bytes),
                "registered_indices": indices,
            });
            send_response(&mut stream, "200 OK", "application/json", &data.to_string()).await?;
        } else {
            let resp = serde_json::json!({ "error": "Table not found" });
            send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
        }
    } else if path == "/api/get" {
        let table_name = extract_query_param(query, "table").unwrap_or_default();
        let uuid_str = extract_query_param(query, "uuid").unwrap_or_default();

        if let Some(table) = table_manager.get_table(&table_name) {
            match PlayerId::parse(&uuid_str) {
                Ok(player) => match table.engine.get(&player) {
                    Ok(Some(val)) => {
                        let val_str = String::from_utf8_lossy(&val);
                        let ttl = table.engine.get_ttl(&player);
                        let resp = serde_json::json!({
                            "found": true,
                            "table": table_name,
                            "uuid": player.to_string(),
                            "ttl_seconds": ttl,
                            "data": serde_json::from_str::<serde_json::Value>(&val_str).unwrap_or(serde_json::Value::String(val_str.into_owned()))
                        });
                        send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
                    }
                    Ok(None) => {
                        let resp = serde_json::json!({ "found": false, "error": "UUID not found or expired" });
                        send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
                    }
                    Err(e) => {
                        let resp = serde_json::json!({ "error": e.to_string() });
                        send_response(&mut stream, "500 Internal Error", "application/json", &resp.to_string()).await?;
                    }
                },
                Err(e) => {
                    let resp = serde_json::json!({ "error": format!("Invalid UUID: {}", e) });
                    send_response(&mut stream, "400 Bad Request", "application/json", &resp.to_string()).await?;
                }
            }
        } else {
            let resp = serde_json::json!({ "error": "Table not found" });
            send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
        }
    } else if path == "/api/exists" {
        let table_name = extract_query_param(query, "table").unwrap_or_default();
        let uuid_str = extract_query_param(query, "uuid").unwrap_or_default();

        if let Some(table) = table_manager.get_table(&table_name) {
            match PlayerId::parse(&uuid_str) {
                Ok(player) => {
                    let exists = table.engine.exists(&player).unwrap_or(false);
                    let resp = serde_json::json!({ "uuid": player.to_string(), "exists": exists });
                    send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
                }
                Err(e) => {
                    let resp = serde_json::json!({ "error": format!("Invalid UUID: {}", e) });
                    send_response(&mut stream, "400 Bad Request", "application/json", &resp.to_string()).await?;
                }
            }
        } else {
            let resp = serde_json::json!({ "error": "Table not found" });
            send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
        }
    } else if path == "/api/expire" {
        let table_name = extract_query_param(query, "table").unwrap_or_default();
        let uuid_str = extract_query_param(query, "uuid").unwrap_or_default();
        let seconds: u64 = extract_query_param(query, "seconds").and_then(|s| s.parse().ok()).unwrap_or(60);

        if let Some(table) = table_manager.get_table(&table_name) {
            match PlayerId::parse(&uuid_str) {
                Ok(player) => {
                    let ok = table.engine.set_expire(&player, seconds);
                    let resp = serde_json::json!({ "uuid": player.to_string(), "success": ok, "expires_in_seconds": seconds });
                    send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
                }
                Err(e) => {
                    let resp = serde_json::json!({ "error": format!("Invalid UUID: {}", e) });
                    send_response(&mut stream, "400 Bad Request", "application/json", &resp.to_string()).await?;
                }
            }
        } else {
            let resp = serde_json::json!({ "error": "Table not found" });
            send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
        }
    } else if path == "/api/flush" {
        let table_name = extract_query_param(query, "table").unwrap_or_default();
        if let Some(table) = table_manager.get_table(&table_name) {
            let _ = table.engine.force_flush().await;
            let resp = serde_json::json!({ "success": true, "table": table_name, "message": "Flush completed" });
            send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
        } else {
            let resp = serde_json::json!({ "error": "Table not found" });
            send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
        }
    } else if path == "/api/backup" {
        match table_manager.backup_all(None).await {
            Ok(dest) => {
                let resp = serde_json::json!({ "success": true, "backup_path": dest.to_string_lossy() });
                send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
            }
            Err(e) => {
                let resp = serde_json::json!({ "error": format!("Backup failed: {}", e) });
                send_response(&mut stream, "500 Internal Error", "application/json", &resp.to_string()).await?;
            }
        }
    } else if path == "/api/top" {
        let table_name = extract_query_param(query, "table").unwrap_or_default();
        let field = extract_query_param(query, "field").unwrap_or_default();
        let limit: usize = extract_query_param(query, "limit")
            .and_then(|s| s.parse().ok())
            .unwrap_or(50);

        if let Some(table) = table_manager.get_table(&table_name) {
            match table.index_manager.get_index(&field) {
                Some(idx) => {
                    let entries = idx.get_top(limit);
                    let list: Vec<serde_json::Value> = entries
                        .into_iter()
                        .map(|(p, score, rank)| {
                            serde_json::json!({
                                "rank": rank,
                                "uuid": p.to_string(),
                                "score": score
                            })
                        })
                        .collect();
                    let resp = serde_json::json!({
                        "table": table_name,
                        "field": field,
                        "count": list.len(),
                        "rankings": list
                    });
                    send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
                }
                None => {
                    let resp = serde_json::json!({ "error": format!("Index '{}' is not registered", field) });
                    send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
                }
            }
        } else {
            let resp = serde_json::json!({ "error": "Table not found" });
            send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
        }
    } else if path == "/api/index/create" {
        let table_name = extract_query_param(query, "table").unwrap_or_default();
        let field = extract_query_param(query, "field").unwrap_or_default();
        if !field.is_empty() {
            if let Some(table) = table_manager.get_table(&table_name) {
                table.index_manager.create_index(&field);
                let resp = serde_json::json!({ "success": true, "table": table_name, "created": field });
                send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
            } else {
                let resp = serde_json::json!({ "error": "Table not found" });
                send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
            }
        } else {
            let resp = serde_json::json!({ "error": "Missing field parameter" });
            send_response(&mut stream, "400 Bad Request", "application/json", &resp.to_string()).await?;
        }
    } else {
        let resp = serde_json::json!({ "error": "Endpoint not found" });
        send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
    }

    Ok(())
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

fn extract_query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        let mut kv = pair.splitn(2, '=');
        if let (Some(k), Some(v)) = (kv.next(), kv.next()) {
            if k == key {
                return Some(v.to_string());
            }
        }
    }
    None
}

async fn send_response(stream: &mut TcpStream, status: &str, content_type: &str, body: &str) -> Result<(), Box<dyn std::error::Error>> {
    let header = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nConnection: close\r\n\r\n",
        status, content_type, body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

const WEB_UI_HTML: &str = r#"<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MeowDB テーブル管理画面</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background-color: #ffffff;
      color: #1f2937;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      padding: 20px;
      font-size: 14px;
      line-height: 1.5;
    }
    .layout { display: flex; gap: 24px; max-width: 1200px; margin: 0 auto; }
    
    /* Left Sidebar: Tables List */
    .sidebar {
      width: 220px;
      border-right: 1px solid #e5e7eb;
      padding-right: 20px;
    }
    .sidebar h2 { font-size: 16px; font-weight: 700; margin-bottom: 12px; color: #111827; }
    .table-list { list-style: none; margin-bottom: 16px; min-height: 40px; }
    .table-item {
      padding: 8px 12px;
      border-radius: 6px;
      cursor: pointer;
      font-family: monospace;
      font-size: 14px;
      font-weight: 600;
      color: #4b5563;
      margin-bottom: 4px;
      border: 1px solid transparent;
    }
    .table-item:hover { background: #f3f4f6; }
    .table-item.active { background: #eff6ff; color: #2563eb; border-color: #bfdbfe; }
    
    .btn-create-table {
      width: 100%;
      background: #f3f4f6;
      border: 1px dashed #d1d5db;
      padding: 8px;
      border-radius: 6px;
      font-size: 13px;
      font-weight: 600;
      color: #4b5563;
      cursor: pointer;
      margin-bottom: 12px;
    }
    .btn-create-table:hover { background: #e5e7eb; }

    .btn-action-side {
      width: 100%;
      background: #f9fafb;
      border: 1px solid #d1d5db;
      padding: 8px;
      border-radius: 6px;
      font-size: 13px;
      font-weight: 600;
      color: #374151;
      cursor: pointer;
      margin-bottom: 8px;
    }
    .btn-action-side:hover { background: #e5e7eb; }

    /* Main Content */
    .main { flex: 1; min-width: 0; }
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding-bottom: 16px;
      margin-bottom: 20px;
      border-bottom: 1px solid #e5e7eb;
    }
    h1 { font-size: 20px; font-weight: 700; }
    .status-badge {
      background: #ecfdf5;
      color: #047857;
      border: 1px solid #a7f3d0;
      padding: 4px 10px;
      border-radius: 6px;
      font-size: 12px;
      font-weight: 600;
    }

    /* Empty state */
    .empty-banner {
      background: #f9fafb;
      border: 1px solid #e5e7eb;
      border-radius: 8px;
      padding: 32px;
      text-align: center;
      color: #6b7280;
    }

    /* Metrics Grid */
    .metrics {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 12px;
      margin-bottom: 20px;
    }
    .metric-card {
      border: 1px solid #e5e7eb;
      background: #fafafa;
      border-radius: 6px;
      padding: 12px 14px;
    }
    .metric-label { font-size: 11px; color: #6b7280; font-weight: 600; text-transform: uppercase; margin-bottom: 2px; }
    .metric-val { font-size: 20px; font-weight: 700; color: #111827; font-family: monospace; }
    .metric-sub { font-size: 11px; color: #9ca3af; margin-top: 2px; }

    /* Tabs */
    .tabs { display: flex; gap: 8px; margin-bottom: 16px; border-bottom: 1px solid #e5e7eb; }
    .tab-btn {
      background: none;
      border: none;
      border-bottom: 2px solid transparent;
      padding: 8px 14px;
      font-size: 14px;
      font-weight: 600;
      color: #6b7280;
      cursor: pointer;
      margin-bottom: -1px;
    }
    .tab-btn.active { color: #2563eb; border-bottom-color: #2563eb; }

    .panel { display: none; }
    .panel.active { display: block; }

    .form-group { display: flex; gap: 8px; margin-bottom: 14px; }
    input[type="text"], input[type="number"], select {
      flex: 1;
      border: 1px solid #d1d5db;
      border-radius: 6px;
      padding: 8px 12px;
      font-size: 14px;
      font-family: monospace;
      outline: none;
    }
    button.btn {
      background: #2563eb;
      color: #fff;
      border: none;
      border-radius: 6px;
      padding: 8px 16px;
      font-size: 14px;
      font-weight: 600;
      cursor: pointer;
    }
    button.btn:hover { background: #1d4ed8; }

    pre {
      background: #f9fafb;
      border: 1px solid #e5e7eb;
      border-radius: 6px;
      padding: 12px;
      font-family: monospace;
      font-size: 13px;
      color: #111827;
      overflow-x: auto;
      max-height: 450px;
    }

    table { width: 100%; border-collapse: collapse; margin-top: 10px; }
    th, td { padding: 8px 12px; border-bottom: 1px solid #e5e7eb; text-align: left; }
    th { background: #f3f4f6; font-size: 12px; font-weight: 600; color: #4b5563; }
    td { font-family: monospace; font-size: 13px; }
    .rank-num { font-weight: 700; color: #2563eb; }
    .score-num { font-weight: 700; color: #059669; text-align: right; }
  </style>
</head>
<body>
  <div class="layout">
    <!-- Sidebar: Tables -->
    <div class="sidebar">
      <h2>📁 テーブル一覧</h2>
      <ul class="table-list" id="sidebar-tables"></ul>
      <button class="btn-create-table" onclick="promptCreateTable()">+ 新規テーブル作成</button>

      <div style="margin-top: 16px; margin-bottom: 16px;">
        <button class="btn-action-side" onclick="triggerFlush()">⚡ ディスクへFlush</button>
        <button class="btn-action-side" onclick="triggerBackup()">💾 スナップショット退避</button>
      </div>

      <div style="font-size: 12px; color: #6b7280; padding-top: 8px; border-top: 1px solid #e5e7eb;">
        <div>総ディスク容量:</div>
        <div id="total-disk-size" style="font-weight: 700; color: #111827; font-size: 14px;">0 B</div>
      </div>
    </div>

    <!-- Main Content -->
    <div class="main">
      <header>
        <h1>テーブル: <span id="current-table-title" style="color: #2563eb; font-family: monospace;">(選択されていません)</span></h1>
        <div class="status-badge">● 稼働中 (Online)</div>
      </header>

      <div id="empty-state" class="empty-banner" style="display:none;">
        <h3 style="color:#111827; margin-bottom:8px;">テーブルが存在しません</h3>
        <p>左側の「+ 新規テーブル作成」ボタンを押すか、CLIから <code>CREATE TABLE &lt;name&gt;</code> を実行してください。</p>
      </div>

      <div id="table-content">
        <!-- Metrics -->
        <div class="metrics">
          <div class="metric-card">
            <div class="metric-label">総レコード数 (Total Records)</div>
            <div class="metric-val" id="val-total">0</div>
            <div class="metric-sub" id="val-total-sub">メモリ + ディスク合計</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">メモリ内 (MemTable)</div>
            <div class="metric-val" id="val-mem">0</div>
            <div class="metric-sub">RAM上の未フラッシュ件数</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">ディスクSSTableファイル数</div>
            <div class="metric-val" id="val-sst">0</div>
            <div class="metric-sub">永続化済みファイル</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">テーブル容量 (Disk)</div>
            <div class="metric-val" id="val-disk">0 B</div>
            <div class="metric-sub">WAL + SSTable合計</div>
          </div>
        </div>

        <!-- Tabs -->
        <div class="tabs">
          <button class="tab-btn active" onclick="showTab('lookup')">UUID 検索 (Explorer)</button>
          <button class="tab-btn" onclick="showTab('rank')">ランキング順位表 (Leaderboard)</button>
          <button class="tab-btn" onclick="showTab('index')">インデックス設定</button>
          <button class="tab-btn" onclick="showTab('ttl')">TTL / 有効期限設定</button>
        </div>

        <!-- Tab 1: Lookup -->
        <div id="tab-lookup" class="panel active">
          <div class="form-group">
            <input type="text" id="input-uuid" placeholder="検索する UUID (例: 069a79f4-44e9-4726-a5be-fca90e38aaf5)">
            <button class="btn" onclick="searchUuid()">検索</button>
            <button class="btn" style="background:#4b5563;" onclick="checkExists()">存在確認 (EXISTS)</button>
          </div>
          <pre id="lookup-result">// ここに JSON データが表示されます</pre>
        </div>

        <!-- Tab 2: Ranking -->
        <div id="tab-rank" class="panel">
          <div class="form-group">
            <select id="select-rank-field"></select>
            <button class="btn" onclick="loadRankings()">ランキング更新</button>
          </div>
          <table>
            <thead>
              <tr>
                <th style="width: 70px;">順位</th>
                <th>UUID</th>
                <th style="text-align: right; width: 120px;">スコア</th>
              </tr>
            </thead>
            <tbody id="rank-tbody">
              <tr><td colspan="3" style="text-align:center; color:#9ca3af;">読み込み中...</td></tr>
            </tbody>
          </table>
        </div>

        <!-- Tab 3: Index Manager -->
        <div id="tab-index" class="panel">
          <div class="form-group">
            <input type="text" id="new-index-input" placeholder="インデックス化するJSONパス (例: stats.wins)">
            <button class="btn" onclick="addIndex()">インデックス追加</button>
          </div>
          <div style="font-weight: 600; margin-bottom: 6px; color: #4b5563;">登録済みインデックス一覧:</div>
          <pre id="index-list">読み込み中...</pre>
        </div>

        <!-- Tab 4: TTL / Expire -->
        <div id="tab-ttl" class="panel">
          <div class="form-group">
            <input type="text" id="ttl-uuid" placeholder="対象の UUID">
            <input type="number" id="ttl-seconds" placeholder="有効期限 (秒数)" style="max-width: 160px;" value="60">
            <button class="btn" onclick="setTtl()">EXPIRE 設定</button>
          </div>
          <pre id="ttl-result">// 実行結果がここに表示されます</pre>
        </div>
      </div>
    </div>
  </div>

  <script>
    let currentTable = null;

    async function loadTables() {
      try {
        const res = await fetch('/api/tables');
        const data = await res.json();
        const listEl = document.getElementById('sidebar-tables');
        listEl.innerHTML = '';
        const tables = data.tables || [];

        if (tables.length === 0) {
          currentTable = null;
          document.getElementById('current-table-title').innerText = '(なし)';
          document.getElementById('empty-state').style.display = 'block';
          document.getElementById('table-content').style.display = 'none';
          listEl.innerHTML = '<li style="color:#9ca3af; font-size:12px; padding:4px;">テーブルなし</li>';
          return;
        }

        document.getElementById('empty-state').style.display = 'none';
        document.getElementById('table-content').style.display = 'block';

        if (!currentTable || !tables.includes(currentTable)) {
          currentTable = tables[0];
        }
        document.getElementById('current-table-title').innerText = currentTable;

        tables.forEach(t => {
          const li = document.createElement('li');
          li.className = 'table-item ' + (t === currentTable ? 'active' : '');
          li.innerText = t;
          li.onclick = () => switchTable(t);
          listEl.appendChild(li);
        });
        document.getElementById('total-disk-size').innerText = data.total_disk_size_human || '0 B';
      } catch (e) {
        console.error(e);
      }
    }

    function switchTable(name) {
      currentTable = name;
      document.getElementById('current-table-title').innerText = name;
      document.getElementById('lookup-result').innerText = '// ここに JSON データが表示されます';
      loadTables();
      updateTableStats();
    }

    async function promptCreateTable() {
      const name = prompt('作成するテーブル名を入力してください (例: players, guilds, users):');
      if (!name) return;
      const clean = name.trim().toLowerCase();
      const res = await fetch('/api/table/create?name=' + encodeURIComponent(clean));
      if (res.ok) {
        switchTable(clean);
      }
    }

    async function triggerFlush() {
      if (!currentTable) return;
      const res = await fetch('/api/flush?table=' + encodeURIComponent(currentTable));
      const data = await res.json();
      alert('Flush完了: ' + JSON.stringify(data));
      updateTableStats();
    }

    async function triggerBackup() {
      const res = await fetch('/api/backup');
      const data = await res.json();
      alert('バックアップ完了: ' + (data.backup_path || data.error));
    }

    async function updateTableStats() {
      if (!currentTable) return;
      try {
        const res = await fetch('/api/stats?table=' + encodeURIComponent(currentTable));
        const data = await res.json();
        document.getElementById('val-total').innerText = (data.total_records || 0).toLocaleString();
        document.getElementById('val-mem').innerText = (data.active_memtable_entries || 0).toLocaleString();
        document.getElementById('val-sst').innerText = data.sstable_count || 0;
        document.getElementById('val-disk').innerText = data.disk_size_human || '0 B';

        const sel = document.getElementById('select-rank-field');
        const cur = sel.value;
        sel.innerHTML = '';
        (data.registered_indices || []).forEach(f => {
          const opt = document.createElement('option');
          opt.value = f;
          opt.innerText = f;
          sel.appendChild(opt);
        });
        if (cur) sel.value = cur;
        document.getElementById('index-list').innerText = JSON.stringify(data.registered_indices || [], null, 2);
      } catch (e) {
        console.error(e);
      }
    }

    async function searchUuid() {
      if (!currentTable) return;
      const uuid = document.getElementById('input-uuid').value.trim();
      if (!uuid) return;
      const box = document.getElementById('lookup-result');
      box.innerText = '検索中...';
      try {
        const res = await fetch(`/api/get?table=${encodeURIComponent(currentTable)}&uuid=${encodeURIComponent(uuid)}`);
        const data = await res.json();
        box.innerText = JSON.stringify(data, null, 2);
      } catch (e) {
        box.innerText = 'エラー: ' + e;
      }
    }

    async function checkExists() {
      if (!currentTable) return;
      const uuid = document.getElementById('input-uuid').value.trim();
      if (!uuid) return;
      const box = document.getElementById('lookup-result');
      try {
        const res = await fetch(`/api/exists?table=${encodeURIComponent(currentTable)}&uuid=${encodeURIComponent(uuid)}`);
        const data = await res.json();
        box.innerText = '存在確認 (EXISTS): ' + (data.exists ? '存在します (TRUE)' : '存在しません (FALSE)');
      } catch (e) {
        box.innerText = 'エラー: ' + e;
      }
    }

    async function setTtl() {
      if (!currentTable) return;
      const uuid = document.getElementById('ttl-uuid').value.trim();
      const sec = document.getElementById('ttl-seconds').value.trim();
      if (!uuid) return;
      const box = document.getElementById('ttl-result');
      try {
        const res = await fetch(`/api/expire?table=${encodeURIComponent(currentTable)}&uuid=${encodeURIComponent(uuid)}&seconds=${encodeURIComponent(sec)}`);
        const data = await res.json();
        box.innerText = JSON.stringify(data, null, 2);
      } catch (e) {
        box.innerText = 'エラー: ' + e;
      }
    }

    async function loadRankings() {
      if (!currentTable) return;
      const field = document.getElementById('select-rank-field').value;
      const tbody = document.getElementById('rank-tbody');
      if (!field) {
        tbody.innerHTML = '<tr><td colspan="3" style="text-align:center; color:#9ca3af;">インデックスが登録されていません</td></tr>';
        return;
      }
      tbody.innerHTML = '<tr><td colspan="3" style="text-align:center;">読み込み中...</td></tr>';
      try {
        const res = await fetch(`/api/top?table=${encodeURIComponent(currentTable)}&field=${encodeURIComponent(field)}&limit=50`);
        const data = await res.json();
        tbody.innerHTML = '';
        if (!data.rankings || data.rankings.length === 0) {
          tbody.innerHTML = '<tr><td colspan="3" style="text-align:center; color:#9ca3af;">データがありません</td></tr>';
          return;
        }
        data.rankings.forEach(item => {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td class="rank-num">#${item.rank}</td>
            <td>${item.uuid}</td>
            <td class="score-num">${item.score.toLocaleString()}</td>
          `;
          tbody.appendChild(tr);
        });
      } catch (e) {
        tbody.innerHTML = `<tr><td colspan="3" style="text-align:center; color:#dc2626;">エラー: ${e}</td></tr>`;
      }
    }

    async function addIndex() {
      if (!currentTable) return;
      const val = document.getElementById('new-index-input').value.trim();
      if (!val) return;
      await fetch(`/api/index/create?table=${encodeURIComponent(currentTable)}&field=${encodeURIComponent(val)}`);
      document.getElementById('new-index-input').value = '';
      updateTableStats();
    }

    function showTab(name) {
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
      event.target.classList.add('active');
      document.getElementById('tab-' + name).classList.add('active');
      if (name === 'rank') loadRankings();
    }

    loadTables().then(() => updateTableStats());
    setInterval(() => {
      loadTables();
      updateTableStats();
    }, 3000);
  </script>
</body>
</html>
"#;
