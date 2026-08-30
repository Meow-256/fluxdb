use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

use crate::core::types::PlayerId;
use crate::index::QueryFilter;
use crate::storage::CompressionType;
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
    _require_pass: Option<String>,
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

    // Check optional dynamic HTTP auth token in query param (?token=password)
    let current_pass = table_manager.get_auth_password();
    if let Some(ref pass) = current_pass {
        let token = extract_query_param(query, "token").unwrap_or_default();
        if path.starts_with("/api/") && token != *pass {
            let resp = serde_json::json!({ "error": "Unauthorized. Provide ?token=password" });
            send_response(&mut stream, "401 Unauthorized", "application/json", &resp.to_string()).await?;
            return Ok(());
        }
    }

    if path == "/" || path == "/index.html" {
        send_response(&mut stream, "200 OK", "text/html; charset=utf-8", WEB_UI_HTML).await?;
    } else if path == "/metrics" {
        let conf = table_manager.get_config();
        let tables_info = table_manager.table_info_list();
        let total_disk = table_manager.total_disk_size_bytes();

        let mut out = String::new();
        out.push_str("# HELP meowdb_up MeowDB server operational status (1 = online)\n");
        out.push_str("# TYPE meowdb_up gauge\n");
        out.push_str("meowdb_up 1\n\n");

        out.push_str("# HELP meowdb_total_disk_bytes Total persistent disk space consumed by all tables\n");
        out.push_str("# TYPE meowdb_total_disk_bytes gauge\n");
        out.push_str(&format!("meowdb_total_disk_bytes {}\n\n", total_disk));

        out.push_str("# HELP meowdb_configured_worker_threads Maximum worker threads configured\n");
        out.push_str("# TYPE meowdb_configured_worker_threads gauge\n");
        out.push_str(&format!("meowdb_configured_worker_threads {}\n\n", conf.worker_threads));

        out.push_str("# HELP meowdb_block_cache_capacity_bytes LRU Block Cache capacity in bytes\n");
        out.push_str("# TYPE meowdb_block_cache_capacity_bytes gauge\n");
        out.push_str(&format!("meowdb_block_cache_capacity_bytes {}\n\n", conf.block_cache_mb * 1024 * 1024));

        out.push_str("# HELP meowdb_table_records Total records stored in table\n");
        out.push_str("# TYPE meowdb_table_records gauge\n");
        for t in &tables_info {
            let name = t["name"].as_str().unwrap_or("unknown");
            let count = t["total_records"].as_u64().unwrap_or(0);
            out.push_str(&format!("meowdb_table_records{{table=\"{}\"}} {}\n", name, count));
        }
        out.push_str("\n# HELP meowdb_table_memtable_records Active RAM records in MemTable\n");
        out.push_str("# TYPE meowdb_table_memtable_records gauge\n");
        for t in &tables_info {
            let name = t["name"].as_str().unwrap_or("unknown");
            let mem = t["memtable_records"].as_u64().unwrap_or(0);
            out.push_str(&format!("meowdb_table_memtable_records{{table=\"{}\"}} {}\n", name, mem));
        }
        out.push_str("\n# HELP meowdb_table_sstable_count Total on-disk SSTable files\n");
        out.push_str("# TYPE meowdb_table_sstable_count gauge\n");
        for t in &tables_info {
            let name = t["name"].as_str().unwrap_or("unknown");
            let sst = t["sstable_count"].as_u64().unwrap_or(0);
            out.push_str(&format!("meowdb_table_sstable_count{{table=\"{}\"}} {}\n", name, sst));
        }
        out.push_str("\n# HELP meowdb_table_disk_bytes On-disk byte size per table\n");
        out.push_str("# TYPE meowdb_table_disk_bytes gauge\n");
        for t in &tables_info {
            let name = t["name"].as_str().unwrap_or("unknown");
            let disk = t["disk_size_bytes"].as_u64().unwrap_or(0);
            out.push_str(&format!("meowdb_table_disk_bytes{{table=\"{}\"}} {}\n", name, disk));
        }

        send_response(&mut stream, "200 OK", "text/plain; version=0.0.4; charset=utf-8", &out).await?;
    } else if path == "/api/config" {
        let conf = table_manager.get_config();
        let resp = serde_json::json!({
            "worker_threads": conf.worker_threads,
            "memtable_size_bytes": conf.memtable_size_bytes,
            "memtable_size_mb": conf.memtable_size_bytes / (1024 * 1024),
            "block_cache_mb": conf.block_cache_mb,
            "compaction_trigger": conf.compaction_trigger,
            "commit_delay_us": conf.commit_delay_us,
            "async_fsync": conf.async_fsync,
            "auth_password": conf.auth_password.unwrap_or_default(),
        });
        send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
    } else if path == "/api/config/update" {
        let mut current = table_manager.get_config();
        if let Some(threads_str) = extract_query_param(query, "worker_threads") {
            if let Ok(t) = threads_str.parse::<usize>() {
                current.worker_threads = t.clamp(1, 128);
            }
        }
        if let Some(mem_str) = extract_query_param(query, "memtable_size_mb") {
            if let Ok(m) = mem_str.parse::<usize>() {
                current.memtable_size_bytes = m.max(4) * 1024 * 1024;
            }
        }
        if let Some(cache_str) = extract_query_param(query, "block_cache_mb") {
            if let Ok(c) = cache_str.parse::<usize>() {
                current.block_cache_mb = c;
            }
        }
        if let Some(comp_str) = extract_query_param(query, "compaction_trigger") {
            if let Ok(c) = comp_str.parse::<usize>() {
                current.compaction_trigger = c.max(2);
            }
        }
        if let Some(delay_str) = extract_query_param(query, "commit_delay_us") {
            if let Ok(d) = delay_str.parse::<u64>() {
                current.commit_delay_us = d;
            }
        }
        if let Some(fsync_str) = extract_query_param(query, "async_fsync") {
            current.async_fsync = fsync_str == "true" || fsync_str == "1";
        }
        if let Some(pass_str) = extract_query_param(query, "auth_password") {
            let trimmed = pass_str.trim();
            current.auth_password = if trimmed.is_empty() { None } else { Some(trimmed.to_string()) };
        }
        table_manager.update_config(current.clone());

        let resp = serde_json::json!({
            "success": true,
            "config": {
                "worker_threads": current.worker_threads,
                "memtable_size_mb": current.memtable_size_bytes / (1024 * 1024),
                "block_cache_mb": current.block_cache_mb,
                "compaction_trigger": current.compaction_trigger,
                "commit_delay_us": current.commit_delay_us,
                "async_fsync": current.async_fsync,
                "auth_password_set": current.auth_password.is_some(),
            }
        });
        send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
    } else if path == "/api/tables" {
        let tables = table_manager.list_tables();
        let total_size = table_manager.total_disk_size_bytes();
        let detailed = table_manager.table_info_list();
        let resp = serde_json::json!({
            "tables": tables,
            "detailed": detailed,
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
    } else if path == "/api/table/drop" {
        let name = extract_query_param(query, "name").unwrap_or_default().trim().to_lowercase();
        if !name.is_empty() {
            match table_manager.drop_table(&name).await {
                Ok(true) => {
                    let resp = serde_json::json!({ "success": true, "dropped": name });
                    send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
                }
                Ok(false) => {
                    let resp = serde_json::json!({ "error": "Table not found" });
                    send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
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
    } else if path == "/api/table/truncate" {
        let name = extract_query_param(query, "name").unwrap_or_default().trim().to_lowercase();
        if !name.is_empty() {
            match table_manager.truncate_table(&name).await {
                Ok(true) => {
                    let resp = serde_json::json!({ "success": true, "truncated": name });
                    send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
                }
                Ok(false) => {
                    let resp = serde_json::json!({ "error": "Table not found" });
                    send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
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
    } else if path == "/api/table/compression/update" {
        let table_name = extract_query_param(query, "table").unwrap_or_default().trim().to_lowercase();
        let c_type_str = extract_query_param(query, "type").unwrap_or_default();
        if let Some(table) = table_manager.get_table(&table_name) {
            let c_type = CompressionType::parse(&c_type_str);
            table.engine.set_compression_type(c_type);
            let resp = serde_json::json!({
                "success": true,
                "table": table_name,
                "compression": c_type.as_str()
            });
            send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
        } else {
            let resp = serde_json::json!({ "error": "Table not found" });
            send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
        }
    } else if path == "/api/set" {
        let table_name = extract_query_param(query, "table").unwrap_or_default().to_lowercase();
        let uuid_str = extract_query_param(query, "uuid").unwrap_or_default();
        let value = extract_query_param(query, "value").unwrap_or_default();

        match table_manager.create_table(&table_name).await {
            Ok(table) => match PlayerId::parse(&uuid_str) {
                Ok(player) => {
                    let val_bytes = bytes::Bytes::from(value.into_bytes());
                    table.index_manager.on_put(player, &val_bytes);
                    match table.engine.put(player, val_bytes).await {
                        Ok(_) => {
                            let resp = serde_json::json!({ "success": true, "table": table_name, "uuid": player.to_string() });
                            send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
                        }
                        Err(e) => {
                            let resp = serde_json::json!({ "error": e.to_string() });
                            send_response(&mut stream, "500 Internal Error", "application/json", &resp.to_string()).await?;
                        }
                    }
                }
                Err(e) => {
                    let resp = serde_json::json!({ "error": format!("Invalid UUID: {}", e) });
                    send_response(&mut stream, "400 Bad Request", "application/json", &resp.to_string()).await?;
                }
            },
            Err(e) => {
                let resp = serde_json::json!({ "error": e.to_string() });
                send_response(&mut stream, "500 Internal Error", "application/json", &resp.to_string()).await?;
            }
        }
    } else if path == "/api/json_set" {
        let table_name = extract_query_param(query, "table").unwrap_or_default().to_lowercase();
        let uuid_str = extract_query_param(query, "uuid").unwrap_or_default();
        let json_path = extract_query_param(query, "path").unwrap_or_default();
        let value = extract_query_param(query, "value").unwrap_or_default();

        match table_manager.create_table(&table_name).await {
            Ok(table) => match PlayerId::parse(&uuid_str) {
                Ok(player) => {
                    match table.engine.json_set(player, &json_path, &value).await {
                        Ok(updated_bytes) => {
                            table.index_manager.on_put(player, &updated_bytes);
                            let val_str = String::from_utf8_lossy(&updated_bytes);
                            let resp = serde_json::json!({
                                "success": true,
                                "table": table_name,
                                "uuid": player.to_string(),
                                "path": json_path,
                                "data": serde_json::from_str::<serde_json::Value>(&val_str).unwrap_or(serde_json::Value::String(val_str.into_owned()))
                            });
                            send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
                        }
                        Err(e) => {
                            let resp = serde_json::json!({ "error": e.to_string() });
                            send_response(&mut stream, "500 Internal Error", "application/json", &resp.to_string()).await?;
                        }
                    }
                }
                Err(e) => {
                    let resp = serde_json::json!({ "error": format!("Invalid UUID: {}", e) });
                    send_response(&mut stream, "400 Bad Request", "application/json", &resp.to_string()).await?;
                }
            },
            Err(e) => {
                let resp = serde_json::json!({ "error": e.to_string() });
                send_response(&mut stream, "500 Internal Error", "application/json", &resp.to_string()).await?;
            }
        }
    } else if path == "/api/del_where" {
        let table_name = extract_query_param(query, "table").unwrap_or_default().to_lowercase();
        let q_str = extract_query_param(query, "query").unwrap_or_default();

        if let Some(table) = table_manager.get_table(&table_name) {
            match QueryFilter::parse(&q_str) {
                Ok(filter) => match table.engine.del_where(&filter).await {
                    Ok(count) => {
                        let resp = serde_json::json!({ "success": true, "table": table_name, "deleted_count": count });
                        send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
                    }
                    Err(e) => {
                        let resp = serde_json::json!({ "error": e.to_string() });
                        send_response(&mut stream, "500 Internal Error", "application/json", &resp.to_string()).await?;
                    }
                },
                Err(e) => {
                    let resp = serde_json::json!({ "error": format!("Invalid filter query: {}", e) });
                    send_response(&mut stream, "400 Bad Request", "application/json", &resp.to_string()).await?;
                }
            }
        } else {
            let resp = serde_json::json!({ "error": "Table not found" });
            send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
        }
    } else if path == "/api/count" {
        let table_name = extract_query_param(query, "table").unwrap_or_default().to_lowercase();
        let q_str = extract_query_param(query, "query");

        if let Some(table) = table_manager.get_table(&table_name) {
            let filter_opt = match q_str {
                Some(ref q) if !q.trim().is_empty() => match QueryFilter::parse(q) {
                    Ok(f) => Some(f),
                    Err(e) => {
                        let resp = serde_json::json!({ "error": format!("Invalid filter: {}", e) });
                        send_response(&mut stream, "400 Bad Request", "application/json", &resp.to_string()).await?;
                        return Ok(());
                    }
                },
                _ => None,
            };

            match table.engine.count_records(filter_opt.as_ref()) {
                Ok(c) => {
                    let resp = serde_json::json!({ "table": table_name, "count": c });
                    send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
                }
                Err(e) => {
                    let resp = serde_json::json!({ "error": e.to_string() });
                    send_response(&mut stream, "500 Internal Error", "application/json", &resp.to_string()).await?;
                }
            }
        } else {
            let resp = serde_json::json!({ "error": "Table not found" });
            send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
        }
    } else if path == "/api/stats_calc" {
        let table_name = extract_query_param(query, "table").unwrap_or_default().to_lowercase();
        let field = extract_query_param(query, "field").unwrap_or_default();
        let q_str = extract_query_param(query, "query");

        if let Some(table) = table_manager.get_table(&table_name) {
            let filter_opt = match q_str {
                Some(ref q) if !q.trim().is_empty() => match QueryFilter::parse(q) {
                    Ok(f) => Some(f),
                    Err(e) => {
                        let resp = serde_json::json!({ "error": format!("Invalid filter: {}", e) });
                        send_response(&mut stream, "400 Bad Request", "application/json", &resp.to_string()).await?;
                        return Ok(());
                    }
                },
                _ => None,
            };

            match table.engine.calc_stats(&field, filter_opt.as_ref()) {
                Ok(metrics) => {
                    let resp = serde_json::json!({
                        "table": table_name,
                        "stats": metrics
                    });
                    send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
                }
                Err(e) => {
                    let resp = serde_json::json!({ "error": e.to_string() });
                    send_response(&mut stream, "500 Internal Error", "application/json", &resp.to_string()).await?;
                }
            }
        } else {
            let resp = serde_json::json!({ "error": "Table not found" });
            send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
        }
    } else if path == "/api/backup" {
        let target = extract_query_param(query, "target");
        match table_manager.backup_all(target.as_deref()).await {
            Ok(p) => {
                let resp = serde_json::json!({
                    "success": true,
                    "backup_path": p.display().to_string(),
                    "message": "Snapshot completed successfully without server interruption"
                });
                send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
            }
            Err(e) => {
                let resp = serde_json::json!({ "error": e.to_string() });
                send_response(&mut stream, "500 Internal Error", "application/json", &resp.to_string()).await?;
            }
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
                "compression": table.engine.compression_type().as_str(),
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
        let table_name = extract_query_param(query, "table").unwrap_or_default().trim().to_lowercase();
        let field = extract_query_param(query, "field").unwrap_or_default();
        if !field.is_empty() {
            match table_manager.create_table(&table_name).await {
                Ok(table) => {
                    table.index_manager.create_index(&field);
                    let resp = serde_json::json!({ "success": true, "table": table_name, "created": field });
                    send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
                }
                Err(e) => {
                    let resp = serde_json::json!({ "error": e.to_string() });
                    send_response(&mut stream, "500 Internal Error", "application/json", &resp.to_string()).await?;
                }
            }
        } else {
            let resp = serde_json::json!({ "error": "Missing field parameter" });
            send_response(&mut stream, "400 Bad Request", "application/json", &resp.to_string()).await?;
        }
    } else if path == "/api/scan" {
        let table_name = extract_query_param(query, "table").unwrap_or_default();
        let start_str = extract_query_param(query, "start");
        let end_str = extract_query_param(query, "end");
        let limit: usize = extract_query_param(query, "limit")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100)
            .min(1000);

        if let Some(table) = table_manager.get_table(&table_name) {
            let start_key = start_str.and_then(|s| PlayerId::parse(&s).ok());
            let end_key = end_str.and_then(|s| PlayerId::parse(&s).ok());

            match table.engine.scan(start_key, end_key, limit) {
                Ok(entries) => {
                    let list: Vec<serde_json::Value> = entries
                        .into_iter()
                        .map(|(k, v)| {
                            let val_str = String::from_utf8_lossy(&v);
                            serde_json::json!({
                                "uuid": k.to_string(),
                                "data": serde_json::from_str::<serde_json::Value>(&val_str).unwrap_or(serde_json::Value::String(val_str.into_owned()))
                            })
                        })
                        .collect();
                    let resp = serde_json::json!({
                        "table": table_name,
                        "count": list.len(),
                        "records": list
                    });
                    send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
                }
                Err(e) => {
                    let resp = serde_json::json!({ "error": e.to_string() });
                    send_response(&mut stream, "500 Internal Error", "application/json", &resp.to_string()).await?;
                }
            }
        } else {
            let resp = serde_json::json!({ "error": "Table not found" });
            send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
        }
    } else if path == "/api/filter" {
        let table_name = extract_query_param(query, "table").unwrap_or_default();
        let q_str = extract_query_param(query, "query").unwrap_or_default();
        let limit: usize = extract_query_param(query, "limit")
            .and_then(|s| s.parse().ok())
            .unwrap_or(100)
            .min(1000);

        if let Some(table) = table_manager.get_table(&table_name) {
            match QueryFilter::parse(&q_str) {
                Ok(filter) => {
                    match table.engine.scan(None, None, 10000) {
                        Ok(entries) => {
                            let mut matched = Vec::new();
                            for (k, v) in entries {
                                if filter.matches(&v) {
                                    let val_str = String::from_utf8_lossy(&v);
                                    matched.push(serde_json::json!({
                                        "uuid": k.to_string(),
                                        "data": serde_json::from_str::<serde_json::Value>(&val_str).unwrap_or(serde_json::Value::String(val_str.into_owned()))
                                    }));
                                    if matched.len() >= limit {
                                        break;
                                    }
                                }
                            }
                            let resp = serde_json::json!({
                                "table": table_name,
                                "query": q_str,
                                "count": matched.len(),
                                "records": matched
                            });
                            send_response(&mut stream, "200 OK", "application/json", &resp.to_string()).await?;
                        }
                        Err(e) => {
                            let resp = serde_json::json!({ "error": e.to_string() });
                            send_response(&mut stream, "500 Internal Error", "application/json", &resp.to_string()).await?;
                        }
                    }
                }
                Err(e) => {
                    let resp = serde_json::json!({ "error": format!("Invalid filter query: {}", e) });
                    send_response(&mut stream, "400 Bad Request", "application/json", &resp.to_string()).await?;
                }
            }
        } else {
            let resp = serde_json::json!({ "error": "Table not found" });
            send_response(&mut stream, "404 Not Found", "application/json", &resp.to_string()).await?;
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
    .layout { display: flex; gap: 24px; max-width: 1280px; margin: 0 auto; }
    
    /* Left Sidebar */
    .sidebar {
      width: 240px;
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
      display: flex;
      justify-content: space-between;
      align-items: center;
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
    .header-actions { display: flex; gap: 8px; align-items: center; }
    .btn-danger {
      background: #fee2e2;
      color: #dc2626;
      border: 1px solid #fca5a5;
      padding: 6px 12px;
      border-radius: 6px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
    }
    .btn-danger:hover { background: #fecaca; }
    .btn-warning {
      background: #fef3c7;
      color: #d97706;
      border: 1px solid #fde68a;
      padding: 6px 12px;
      border-radius: 6px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
    }
    .btn-warning:hover { background: #fde68a; }

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
    .tabs { display: flex; gap: 8px; margin-bottom: 16px; border-bottom: 1px solid #e5e7eb; overflow-x: auto; }
    .tab-btn {
      background: none;
      border: none;
      border-bottom: 2px solid transparent;
      padding: 8px 14px;
      font-size: 13px;
      font-weight: 600;
      color: #6b7280;
      cursor: pointer;
      margin-bottom: -1px;
      white-space: nowrap;
    }
    .tab-btn.active { color: #2563eb; border-bottom-color: #2563eb; }

    .panel { display: none; }
    .panel.active { display: block; }

    .form-group { display: flex; gap: 8px; margin-bottom: 14px; flex-wrap: wrap; }
    input[type="text"], input[type="number"], select {
      flex: 1;
      min-width: 140px;
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
        <button class="btn-action-side" onclick="triggerBackup()">💾 スナップショット退避 (Hot Backup)</button>
      </div>

      <div style="font-size: 12px; color: #6b7280; padding-top: 8px; border-top: 1px solid #e5e7eb;">
        <div>総ディスク容量:</div>
        <div id="total-disk-size" style="font-weight: 700; color: #111827; font-size: 14px;">0 B</div>
      </div>
    </div>

    <!-- Main Content -->
    <div class="main">
      <header>
        <div>
          <h1>テーブル: <span id="current-table-title" style="color: #2563eb; font-family: monospace;">(選択されていません)</span></h1>
        </div>
        <div class="header-actions" style="display:flex; align-items:center; gap:10px;">
          <div style="display:flex; align-items:center; gap:6px; background:#f3f4f6; padding:5px 10px; border-radius:6px; border:1px solid #e5e7eb;">
            <label style="font-size:12px; font-weight:700; color:#374151;">📦 圧縮方式:</label>
            <select id="table-compression-select" onchange="changeTableCompression(this.value)" style="padding:2px 8px; font-size:12px; border-radius:4px; border:1px solid #d1d5db; background:white; font-weight:600; cursor:pointer;">
              <option value="LZ4">LZ4 (高速・標準)</option>
              <option value="ZSTD">ZSTD (最高圧縮・大容量)</option>
              <option value="NONE">NONE (非圧縮・最速)</option>
            </select>
          </div>
          <button class="btn-warning" onclick="truncateCurrentTable()">TRUNCATE TABLE</button>
          <button class="btn-danger" onclick="dropCurrentTable()">DROP TABLE</button>
          <div class="status-badge">● 稼働中 (Online)</div>
        </div>
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
            <div class="metric-sub">永続化済み (LZ4圧縮)</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">テーブル容量 (Disk)</div>
            <div class="metric-val" id="val-disk">0 B</div>
            <div class="metric-sub">WAL + SSTable合計</div>
          </div>
        </div>

        <!-- Tabs -->
        <div class="tabs">
          <button class="tab-btn active" onclick="showTab('lookup')">UUID 検索 / 部分更新 (JSON_SET)</button>
          <button class="tab-btn" onclick="showTab('scan')">レンジスキャン (SCAN)</button>
          <button class="tab-btn" onclick="showTab('filter')">条件検索 / 一括削除 (FILTER / DEL_WHERE)</button>
          <button class="tab-btn" onclick="showTab('stats')">集計・統計 (STATS / COUNT)</button>
          <button class="tab-btn" onclick="showTab('rank')">ランキング (Leaderboard)</button>
          <button class="tab-btn" onclick="showTab('index')">インデックス設定</button>
          <button class="tab-btn" onclick="showTab('ttl')">TTL設定</button>
          <button class="tab-btn" onclick="showTab('settings')" style="color:#7c3aed;">⚙️ サーバー設定 (Settings)</button>
        </div>

        <!-- Tab 1: Lookup & JSON_SET -->
        <div id="tab-lookup" class="panel active">
          <div class="form-group">
            <input type="text" id="input-uuid" placeholder="検索する UUID (例: 909fb8ea-1b14-4ca9-b15b-277ec2559be0)">
            <button class="btn" onclick="searchUuid()">検索 (GET)</button>
            <button class="btn" style="background:#4b5563;" onclick="checkExists()">存在確認 (EXISTS)</button>
          </div>
          <div class="form-group" style="background:#f3f4f6; padding:10px; border-radius:6px;">
            <input type="text" id="jsonset-path" placeholder="JSONパス (例: stats.Bedwars.coins)" style="max-width:240px;">
            <input type="text" id="jsonset-val" placeholder="新しい値 (例: 5000000 または 'new_name')">
            <button class="btn" style="background:#059669;" onclick="performJsonSet()">部分更新 (JSON_SET)</button>
          </div>
          <pre id="lookup-result">// ここに JSON データが表示されます</pre>
        </div>

        <!-- Tab 2: Range Scan -->
        <div id="tab-scan" class="panel">
          <div class="form-group">
            <input type="text" id="scan-start" placeholder="開始 UUID (空欄で先頭から)">
            <input type="text" id="scan-end" placeholder="終了 UUID (空欄で末尾まで)">
            <input type="number" id="scan-limit" placeholder="件数" value="50" style="max-width:100px;">
            <button class="btn" onclick="runScan()">レンジスキャン実行</button>
          </div>
          <pre id="scan-result">// レンジスキャン結果が表示されます</pre>
        </div>

        <!-- Tab 3: Filter & Batch Delete -->
        <div id="tab-filter" class="panel">
          <div class="form-group">
            <input type="text" id="filter-query" placeholder="フィルタ条件 (例: player.achievements.bedwars_level >= 100 AND player.stats.SkyWars.coins > 1000000)">
            <input type="number" id="filter-limit" placeholder="件数" value="50" style="max-width:100px;">
            <button class="btn" onclick="runFilter()">検索 (FILTER)</button>
            <button class="btn" style="background:#4b5563;" onclick="runCount()">件数取得 (COUNT)</button>
            <button class="btn btn-danger" onclick="runDelWhere()">条件一括削除 (DEL_WHERE)</button>
          </div>
          <pre id="filter-result">// フィルタ検索結果が表示されます</pre>
        </div>

        <!-- Tab 4: Aggregation & Stats -->
        <div id="tab-stats" class="panel">
          <div class="form-group">
            <input type="text" id="stats-field" placeholder="集計対象の数値フィールド (例: stats.Bedwars.kills_bedwars)">
            <input type="text" id="stats-query" placeholder="フィルタ条件 (任意 例: stats.Bedwars.coins > 0)">
            <button class="btn" onclick="runStatsCalc()">集計実行 (STATS: SUM/AVG/MIN/MAX)</button>
          </div>
          <pre id="stats-result">// 集計結果 (Count, Sum, Avg, Min, Max) が表示されます</pre>
        </div>

        <!-- Tab 5: Ranking -->
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

        <!-- Tab 6: Index Manager -->
        <div id="tab-index" class="panel">
          <div class="form-group">
            <input type="text" id="new-index-input" placeholder="インデックス化するJSONパス (例: stats.Bedwars.coins)">
            <button class="btn" onclick="addIndex()">インデックス追加</button>
          </div>
          <div style="font-weight: 600; margin-bottom: 6px; color: #4b5563;">登録済みインデックス一覧:</div>
          <pre id="index-list">読み込み中...</pre>
        </div>

        <!-- Tab 7: TTL / Expire -->
        <div id="tab-ttl" class="panel">
          <div class="form-group">
            <input type="text" id="ttl-uuid" placeholder="対象の UUID">
            <input type="number" id="ttl-seconds" placeholder="有効期限 (秒数)" style="max-width: 160px;" value="60">
            <button class="btn" onclick="setTtl()">EXPIRE 設定</button>
          </div>
          <pre id="ttl-result">// 実行結果がここに表示されます</pre>
        </div>

        <!-- Tab 8: Server Settings -->
        <div id="tab-settings" class="panel">
          <div style="background:#f9fafb; border:1px solid #e5e7eb; border-radius:8px; padding:20px; max-width:650px;">
            <h3 style="margin-bottom:16px; color:#111827; font-size:16px;">⚙️ 動的サーバー設定 (Hot Config)</h3>
            
            <div style="margin-bottom:14px;">
              <label style="font-weight:600; font-size:13px; display:block; margin-bottom:4px;">MemTable 最大サイズ (RAM未フラッシュ許容量):</label>
              <select id="cfg-memtable" style="width:100%;">
                <option value="64">64 MB</option>
                <option value="128">128 MB</option>
                <option value="256">256 MB (デフォルト)</option>
                <option value="512">512 MB</option>
                <option value="1024">1,024 MB (1 GB)</option>
                <option value="2048">2,048 MB (2 GB)</option>
              </select>
            </div>

            <div style="margin-bottom:14px;">
              <label style="font-weight:600; font-size:13px; display:block; margin-bottom:4px;">解凍ブロックキャッシュ (Block Cache):</label>
              <select id="cfg-cache" style="width:100%;">
                <option value="0">0 MB (OFF / キャッシュ無効)</option>
                <option value="64">64 MB</option>
                <option value="128">128 MB</option>
                <option value="256">256 MB (推奨)</option>
                <option value="512">512 MB</option>
                <option value="1024">1,024 MB (1 GB)</option>
              </select>
              <span style="font-size:11px; color:#6b7280;">ONにすると解凍済みデータをRAM保持し、読み込みが 2〜5 µs に高速化</span>
            </div>

            <div style="margin-bottom:14px;">
              <label style="font-weight:600; font-size:13px; display:block; margin-bottom:4px;">並行処理スレッド数 (Parallel Worker Threads):</label>
              <input type="number" id="cfg-threads" min="0" max="128" style="width:100%;" placeholder="0 = 自動適応スケール, 1〜128">
              <span style="font-size:11px; color:#6b7280;">0: 負荷に応じて自動スケール / 1〜128: 最大並行ワーカースレッド数</span>
            </div>

            <div style="margin-bottom:14px;">
              <label style="font-weight:600; font-size:13px; display:block; margin-bottom:4px;">認証パスワード (Auth Password):</label>
              <input type="text" id="cfg-auth" style="width:100%;" placeholder="空欄でパスワード認証無効">
            </div>

            <div style="margin-bottom:14px;">
              <label style="font-weight:600; font-size:13px; display:block; margin-bottom:4px;">Group Commit 遅延ウィンドウ (µs):</label>
              <select id="cfg-delay" style="width:100%;">
                <option value="100">100 µs (超低遅延)</option>
                <option value="500">500 µs</option>
                <option value="1000">1,000 µs (1 ms - 推奨)</option>
                <option value="2000">2,000 µs (2 ms)</option>
                <option value="5000">5,000 µs (高スループット)</option>
              </select>
            </div>

            <div style="margin-bottom:14px;">
              <label style="font-weight:600; font-size:13px; display:block; margin-bottom:4px;">L0 コンパクション閾値 (SSTable ファイル数):</label>
              <input type="number" id="cfg-compaction" min="2" max="32" style="width:100%;" value="4">
            </div>

            <div style="margin-bottom:20px;">
              <label style="font-weight:600; font-size:13px; display:flex; align-items:center; gap:8px;">
                <input type="checkbox" id="cfg-async-fsync"> 非同期 fsync モード (高速スループット)
              </label>
            </div>

            <button class="btn" style="background:#7c3aed; width:100%;" onclick="saveServerConfig()">💾 設定を保存・即時適用 (Apply Settings)</button>
            <pre id="cfg-status" style="margin-top:12px; display:none;"></pre>
          </div>
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

    async function dropCurrentTable() {
      if (!currentTable) return;
      if (!confirm(`本当にテーブル '${currentTable}' を完全に削除 (DROP) しますか？\nディスク上の全ファイルが削除されます。`)) return;
      const res = await fetch('/api/table/drop?name=' + encodeURIComponent(currentTable));
      const data = await res.json();
      if (data.success) {
        currentTable = null;
        loadTables();
      } else {
        alert('エラー: ' + (data.error || 'Drop failed'));
      }
    }

    async function truncateCurrentTable() {
      if (!currentTable) return;
      if (!confirm(`本当にテーブル '${currentTable}' の全データを消去 (TRUNCATE) しますか？`)) return;
      const res = await fetch('/api/table/truncate?name=' + encodeURIComponent(currentTable));
      const data = await res.json();
      if (data.success) {
        updateTableStats();
        alert(`テーブル '${currentTable}' を初期化しました。`);
      } else {
        alert('エラー: ' + (data.error || 'Truncate failed'));
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

        if (data.compression && document.getElementById('table-compression-select')) {
          document.getElementById('table-compression-select').value = data.compression;
        }

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

    async function changeTableCompression(val) {
      if (!currentTable) return;
      try {
        const res = await fetch(`/api/table/compression/update?table=${encodeURIComponent(currentTable)}&type=${encodeURIComponent(val)}`);
        const data = await res.json();
        if (data.success) {
          updateTableStats();
        }
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

    async function performJsonSet() {
      if (!currentTable) return;
      const uuid = document.getElementById('input-uuid').value.trim();
      const path = document.getElementById('jsonset-path').value.trim();
      const val = document.getElementById('jsonset-val').value.trim();
      if (!uuid || !path) {
        alert('UUID と JSONパスを入力してください');
        return;
      }
      const box = document.getElementById('lookup-result');
      box.innerText = '更新中...';
      try {
        const res = await fetch(`/api/json_set?table=${encodeURIComponent(currentTable)}&uuid=${encodeURIComponent(uuid)}&path=${encodeURIComponent(path)}&value=${encodeURIComponent(val)}`);
        const data = await res.json();
        box.innerText = JSON.stringify(data, null, 2);
        updateTableStats();
      } catch (e) {
        box.innerText = 'エラー: ' + e;
      }
    }

    async function runScan() {
      if (!currentTable) return;
      const start = document.getElementById('scan-start').value.trim();
      const end = document.getElementById('scan-end').value.trim();
      const limit = document.getElementById('scan-limit').value.trim() || '50';
      const box = document.getElementById('scan-result');
      box.innerText = 'スキャン中...';
      try {
        const res = await fetch(`/api/scan?table=${encodeURIComponent(currentTable)}&start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}&limit=${encodeURIComponent(limit)}`);
        const data = await res.json();
        box.innerText = JSON.stringify(data, null, 2);
      } catch (e) {
        box.innerText = 'エラー: ' + e;
      }
    }

    async function runFilter() {
      if (!currentTable) return;
      const q = document.getElementById('filter-query').value.trim();
      const limit = document.getElementById('filter-limit').value.trim() || '50';
      const box = document.getElementById('filter-result');
      box.innerText = '検索中...';
      try {
        const res = await fetch(`/api/filter?table=${encodeURIComponent(currentTable)}&query=${encodeURIComponent(q)}&limit=${encodeURIComponent(limit)}`);
        const data = await res.json();
        box.innerText = JSON.stringify(data, null, 2);
      } catch (e) {
        box.innerText = 'エラー: ' + e;
      }
    }

    async function runCount() {
      if (!currentTable) return;
      const q = document.getElementById('filter-query').value.trim();
      const box = document.getElementById('filter-result');
      box.innerText = '件数カウント中...';
      try {
        const res = await fetch(`/api/count?table=${encodeURIComponent(currentTable)}&query=${encodeURIComponent(q)}`);
        const data = await res.json();
        box.innerText = JSON.stringify(data, null, 2);
      } catch (e) {
        box.innerText = 'エラー: ' + e;
      }
    }

    async function runDelWhere() {
      if (!currentTable) return;
      const q = document.getElementById('filter-query').value.trim();
      if (!q) {
        alert('削除条件 (Query) を指定してください');
        return;
      }
      if (!confirm(`条件 [${q}] に一致するすべてのレコードを一括削除 (DEL_WHERE) しますか？`)) return;
      const box = document.getElementById('filter-result');
      box.innerText = '一括削除中...';
      try {
        const res = await fetch(`/api/del_where?table=${encodeURIComponent(currentTable)}&query=${encodeURIComponent(q)}`);
        const data = await res.json();
        box.innerText = JSON.stringify(data, null, 2);
        updateTableStats();
      } catch (e) {
        box.innerText = 'エラー: ' + e;
      }
    }

    async function runStatsCalc() {
      if (!currentTable) return;
      const field = document.getElementById('stats-field').value.trim();
      const q = document.getElementById('stats-query').value.trim();
      if (!field) {
        alert('集計対象のフィールド名を入力してください');
        return;
      }
      const box = document.getElementById('stats-result');
      box.innerText = '集計計算中...';
      try {
        const res = await fetch(`/api/stats_calc?table=${encodeURIComponent(currentTable)}&field=${encodeURIComponent(field)}&query=${encodeURIComponent(q)}`);
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

    async function loadServerConfig() {
      try {
        const res = await fetch('/api/config');
        const cfg = await res.json();
        if (document.getElementById('cfg-memtable')) {
          document.getElementById('cfg-memtable').value = cfg.memtable_size_mb || 256;
          document.getElementById('cfg-cache').value = cfg.block_cache_mb !== undefined ? cfg.block_cache_mb : 0;
          document.getElementById('cfg-threads').value = cfg.worker_threads !== undefined ? cfg.worker_threads : 8;
          document.getElementById('cfg-auth').value = cfg.auth_password || '';
          document.getElementById('cfg-delay').value = cfg.commit_delay_us || 1000;
          document.getElementById('cfg-compaction').value = cfg.compaction_trigger || 4;
          document.getElementById('cfg-async-fsync').checked = !!cfg.async_fsync;
        }
      } catch (e) {
        console.error(e);
      }
    }

    async function saveServerConfig() {
      const mem = document.getElementById('cfg-memtable').value;
      const cache = document.getElementById('cfg-cache').value;
      const threads = document.getElementById('cfg-threads').value;
      const auth = document.getElementById('cfg-auth').value;
      const delay = document.getElementById('cfg-delay').value;
      const comp = document.getElementById('cfg-compaction').value;
      const asyncFsync = document.getElementById('cfg-async-fsync').checked;

      const q = `worker_threads=${encodeURIComponent(threads)}&memtable_size_mb=${encodeURIComponent(mem)}&block_cache_mb=${encodeURIComponent(cache)}&compaction_trigger=${encodeURIComponent(comp)}&commit_delay_us=${encodeURIComponent(delay)}&async_fsync=${asyncFsync}&auth_password=${encodeURIComponent(auth)}`;
      
      const box = document.getElementById('cfg-status');
      box.style.display = 'block';
      box.innerText = '設定適用中...';
      try {
        const res = await fetch('/api/config/update?' + q);
        const data = await res.json();
        box.innerText = '設定が正常に保存され、即時適用されました:\n' + JSON.stringify(data, null, 2);
        updateTableStats();
      } catch (e) {
        box.innerText = '設定保存エラー: ' + e;
      }
    }

    function showTab(name) {
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
      event.target.classList.add('active');
      document.getElementById('tab-' + name).classList.add('active');
      if (name === 'rank') loadRankings();
      if (name === 'settings') loadServerConfig();
    }

    loadTables().then(() => {
      updateTableStats();
      loadServerConfig();
    });
    setInterval(() => {
      loadTables();
      updateTableStats();
    }, 3000);
  </script>
</body>
</html>
"#;
