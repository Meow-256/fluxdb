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
        info!("FluxDB Web UI Dashboard listening on http://{}", self.addr);

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
        out.push_str("# HELP fluxdb_up FluxDB server operational status (1 = online)\n");
        out.push_str("# TYPE fluxdb_up gauge\n");
        out.push_str("fluxdb_up 1\n\n");

        out.push_str("# HELP fluxdb_total_disk_bytes Total persistent disk space consumed by all tables\n");
        out.push_str("# TYPE fluxdb_total_disk_bytes gauge\n");
        out.push_str(&format!("fluxdb_total_disk_bytes {}\n\n", total_disk));

        out.push_str("# HELP fluxdb_configured_worker_threads Maximum worker threads configured\n");
        out.push_str("# TYPE fluxdb_configured_worker_threads gauge\n");
        out.push_str(&format!("fluxdb_configured_worker_threads {}\n\n", conf.worker_threads));

        out.push_str("# HELP fluxdb_block_cache_capacity_bytes LRU Block Cache capacity in bytes\n");
        out.push_str("# TYPE fluxdb_block_cache_capacity_bytes gauge\n");
        out.push_str(&format!("fluxdb_block_cache_capacity_bytes {}\n\n", conf.block_cache_mb * 1024 * 1024));

        out.push_str("# HELP fluxdb_table_records Total records stored in table\n");
        out.push_str("# TYPE fluxdb_table_records gauge\n");
        for t in &tables_info {
            let name = t["name"].as_str().unwrap_or("unknown");
            let count = t["total_records"].as_u64().unwrap_or(0);
            out.push_str(&format!("fluxdb_table_records{{table=\"{}\"}} {}\n", name, count));
        }
        out.push_str("\n# HELP fluxdb_table_memtable_records Active RAM records in MemTable\n");
        out.push_str("# TYPE fluxdb_table_memtable_records gauge\n");
        for t in &tables_info {
            let name = t["name"].as_str().unwrap_or("unknown");
            let mem = t["memtable_records"].as_u64().unwrap_or(0);
            out.push_str(&format!("fluxdb_table_memtable_records{{table=\"{}\"}} {}\n", name, mem));
        }
        out.push_str("\n# HELP fluxdb_table_sstable_count Total on-disk SSTable files\n");
        out.push_str("# TYPE fluxdb_table_sstable_count gauge\n");
        for t in &tables_info {
            let name = t["name"].as_str().unwrap_or("unknown");
            let sst = t["sstable_count"].as_u64().unwrap_or(0);
            out.push_str(&format!("fluxdb_table_sstable_count{{table=\"{}\"}} {}\n", name, sst));
        }
        out.push_str("\n# HELP fluxdb_table_disk_bytes On-disk byte size per table\n");
        out.push_str("# TYPE fluxdb_table_disk_bytes gauge\n");
        for t in &tables_info {
            let name = t["name"].as_str().unwrap_or("unknown");
            let disk = t["disk_size_bytes"].as_u64().unwrap_or(0);
            out.push_str(&format!("fluxdb_table_disk_bytes{{table=\"{}\"}} {}\n", name, disk));
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
    } else if path == "/api/top" || path == "/api/rankings" {
        let table_name = extract_query_param(query, "table").unwrap_or_default();
        let field = extract_query_param(query, "field").unwrap_or_default();
        let mode = extract_query_param(query, "mode").unwrap_or_else(|| "top".to_string());
        let limit: usize = extract_query_param(query, "limit")
            .and_then(|s| s.parse().ok())
            .unwrap_or(50);

        if let Some(table) = table_manager.get_table(&table_name) {
            match table.index_manager.get_index(&field) {
                Some(idx) => {
                    let entries = match mode.as_str() {
                        "around_key" => {
                            if let Some(key_str) = extract_query_param(query, "key") {
                                if let Ok(player) = PlayerId::parse(&key_str) {
                                    idx.get_around_key(&player, limit).unwrap_or_default()
                                } else {
                                    Vec::new()
                                }
                            } else {
                                Vec::new()
                            }
                        }
                        "around_score" => {
                            let target_score = extract_query_param(query, "score")
                                .and_then(|s| s.parse::<f64>().ok())
                                .unwrap_or(0.0);
                            idx.get_around_score(target_score, limit)
                        }
                        "score_range" => {
                            let min = extract_query_param(query, "min")
                                .and_then(|s| s.parse::<f64>().ok())
                                .unwrap_or(f64::MIN);
                            let max = extract_query_param(query, "max")
                                .and_then(|s| s.parse::<f64>().ok())
                                .unwrap_or(f64::MAX);
                            idx.get_score_range(min, max, limit)
                        }
                        "rank_range" => {
                            let start = extract_query_param(query, "start")
                                .and_then(|s| s.parse::<usize>().ok())
                                .unwrap_or(1);
                            let end = extract_query_param(query, "end")
                                .and_then(|s| s.parse::<usize>().ok())
                                .unwrap_or(50);
                            idx.get_rank_range(start, end)
                        }
                        _ => idx.get_top(limit),
                    };

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
                        "mode": mode,
                        "total_ranked": idx.len(),
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
    } else if path == "/api/keys" {
        let table_name = extract_query_param(query, "table").unwrap_or_default();
        let limit: usize = extract_query_param(query, "limit")
            .and_then(|s| s.parse().ok())
            .unwrap_or(5000)
            .min(50000);

        if let Some(table) = table_manager.get_table(&table_name) {
            match table.engine.scan(None, None, limit) {
                Ok(entries) => {
                    let mut keys: Vec<String> = Vec::with_capacity(entries.len());
                    let mut records: Vec<serde_json::Value> = Vec::with_capacity(entries.len());
                    for (k, v) in entries {
                        let uuid_str = k.to_string();
                        keys.push(uuid_str.clone());
                        let val_str = String::from_utf8_lossy(&v);
                        let parsed = serde_json::from_str::<serde_json::Value>(&val_str)
                            .unwrap_or(serde_json::Value::String(val_str.into_owned()));
                        
                        let mut label = uuid_str.clone();
                        if let serde_json::Value::Object(ref map) = parsed {
                            for candidate in &["key", "name", "username", "player_name", "user", "player", "id", "title", "label", "uuid"] {
                                if let Some(serde_json::Value::String(s)) = map.get(*candidate) {
                                    if !s.trim().is_empty() {
                                        label = s.clone();
                                        break;
                                    }
                                }
                            }
                        }

                        records.push(serde_json::json!({
                            "uuid": uuid_str,
                            "label": label,
                            "data": parsed
                        }));
                    }

                    let resp = serde_json::json!({
                        "table": table_name,
                        "count": records.len(),
                        "keys": keys,
                        "records": records
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
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>FluxDB Management Console</title>
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
      border-bottom: 1px solid #e5e7eb;
      margin-bottom: 20px;
    }
    h1 { font-size: 20px; font-weight: 700; color: #111827; }
    .status-badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 10px;
      background-color: #ecfdf5;
      color: #065f46;
      border-radius: 9999px;
      font-size: 12px;
      font-weight: 600;
    }
    
    /* Metrics Top Bar */
    .metrics {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 16px;
      margin-bottom: 24px;
    }
    .metric-card {
      background: #f9fafb;
      border: 1px solid #e5e7eb;
      padding: 16px;
      border-radius: 8px;
    }
    .metric-label { font-size: 12px; color: #6b7280; font-weight: 600; margin-bottom: 4px; }
    .metric-val { font-size: 24px; font-weight: 700; color: #111827; font-family: monospace; }
    .metric-sub { font-size: 11px; color: #9ca3af; margin-top: 2px; }

    /* Navigation Tabs */
    .tabs {
      display: flex;
      gap: 8px;
      border-bottom: 1px solid #e5e7eb;
      margin-bottom: 20px;
      flex-wrap: wrap;
    }
    .tab-btn {
      padding: 8px 16px;
      font-size: 13px;
      font-weight: 600;
      color: #6b7280;
      background: none;
      border: none;
      border-bottom: 2px solid transparent;
      cursor: pointer;
    }
    .tab-btn:hover { color: #111827; }
    .tab-btn.active {
      color: #2563eb;
      border-bottom-color: #2563eb;
    }

    /* Tab Panels */
    .panel { display: none; }
    .panel.active { display: block; }

    /* Controls & Forms */
    .form-group {
      display: flex;
      gap: 8px;
      margin-bottom: 16px;
      align-items: center;
      flex-wrap: wrap;
    }
    input[type="text"], input[type="number"], select {
      padding: 8px 12px;
      border: 1px solid #d1d5db;
      border-radius: 6px;
      font-size: 13px;
      font-family: monospace;
      outline: none;
      flex: 1;
    }
    input:focus, select:focus { border-color: #2563eb; }
    .btn {
      padding: 8px 16px;
      background-color: #2563eb;
      color: white;
      border: none;
      border-radius: 6px;
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
      white-space: nowrap;
    }
    .btn:hover { background-color: #1d4ed8; }
    .btn-danger { background-color: #dc2626; }
    .btn-danger:hover { background-color: #b91c1c; }
    .btn-warning { background-color: #d97706; color: white; border: none; border-radius: 6px; padding: 6px 12px; font-size: 12px; font-weight: 600; cursor: pointer; }
    .btn-warning:hover { background-color: #b45309; }

    pre {
      background: #111827;
      color: #f3f4f6;
      padding: 16px;
      border-radius: 8px;
      font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
      font-size: 13px;
      line-height: 1.4;
      max-height: 500px;
      overflow-y: auto;
      white-space: pre-wrap;
      word-break: break-all;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 12px;
    }
    th, td {
      padding: 10px 14px;
      text-align: left;
      border-bottom: 1px solid #e5e7eb;
      font-size: 13px;
    }
    th { background: #f9fafb; font-weight: 600; color: #4b5563; }
    td { font-family: monospace; }
    .rank-num { font-weight: 700; color: #2563eb; }
    .score-num { font-weight: 700; color: #059669; text-align: right; }

    .empty-banner {
      padding: 40px;
      text-align: center;
      background: #f9fafb;
      border: 2px dashed #d1d5db;
      border-radius: 8px;
      color: #6b7280;
    }

    /* Show Tab 2-Pane Explorer */
    .show-container {
      display: flex;
      gap: 16px;
      height: 560px;
    }
    .keys-pane {
      width: 320px;
      display: flex;
      flex-direction: column;
      border: 1px solid #e5e7eb;
      border-radius: 8px;
      background: #ffffff;
      overflow: hidden;
    }
    .keys-header {
      padding: 10px 12px;
      background: #f9fafb;
      border-bottom: 1px solid #e5e7eb;
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .keys-header-top {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .keys-list {
      flex: 1;
      overflow-y: auto;
      list-style: none;
      padding: 6px;
      margin: 0;
    }
    .key-item {
      padding: 8px 10px;
      border-radius: 6px;
      cursor: pointer;
      display: flex;
      flex-direction: column;
      gap: 2px;
      margin-bottom: 3px;
      border: 1px solid transparent;
      word-break: break-all;
      transition: background 0.15s, border-color 0.15s;
    }
    .key-item:hover {
      background: #f3f4f6;
    }
    .key-item.active {
      background: #eff6ff;
      border-color: #93c5fd;
    }
    .key-item-label {
      font-family: monospace;
      font-size: 13px;
      font-weight: 700;
      color: #1f2937;
    }
    .key-item.active .key-item-label {
      color: #1d4ed8;
    }
    .key-item-uuid {
      font-family: monospace;
      font-size: 11px;
      color: #9ca3af;
    }
    .data-pane {
      flex: 1;
      display: flex;
      flex-direction: column;
      border: 1px solid #e5e7eb;
      border-radius: 8px;
      background: #ffffff;
      overflow: hidden;
      min-width: 0;
    }
    .data-header {
      padding: 10px 14px;
      background: #f9fafb;
      border-bottom: 1px solid #e5e7eb;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .data-body {
      flex: 1;
      display: flex;
      flex-direction: column;
      overflow: hidden;
      padding: 0;
    }
    .data-body pre {
      flex: 1;
      margin: 0;
      border-radius: 0;
      max-height: none;
      height: 100%;
    }
  </style>
</head>
<body>
  <div class="layout">
    <!-- Sidebar: Tables & Global Tools -->
    <div class="sidebar">
      <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:12px;">
        <h2 style="margin:0;">📁 Tables</h2>
        <a href="https://github.com/Meow-256/fluxdb" target="_blank" rel="noopener noreferrer" style="display:inline-flex; align-items:center; gap:4px; font-size:12px; color:#4b5563; text-decoration:none; padding:4px 8px; border-radius:6px; border:1px solid #e5e7eb; background:#f9fafb; font-weight:600;">
          <svg height="14" width="14" viewBox="0 0 16 16" fill="currentColor"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"></path></svg>
          GitHub
        </a>
      </div>
      <ul class="table-list" id="sidebar-tables"></ul>
      <button class="btn-create-table" onclick="promptCreateTable()">+ Create Table</button>

      <div style="margin-top: 16px; margin-bottom: 16px; display:flex; flex-direction:column; gap:8px;">
        <button class="btn-action-side" onclick="triggerFlush()">⚡ Flush to Disk</button>
        <button class="btn-action-side" onclick="triggerBackup()">💾 Hot Backup (Snapshot)</button>
        <button class="btn-action-side" id="btn-side-settings" onclick="openServerSettingsView()" style="background:#f5f3ff; color:#6d28d9; border:1px solid #ddd6fe; font-weight:700;">⚙️ Server Settings</button>
      </div>

      <div style="font-size: 12px; color: #6b7280; padding-top: 8px; border-top: 1px solid #e5e7eb;">
        <div>Total Disk Usage:</div>
        <div id="total-disk-size" style="font-weight: 700; color: #111827; font-size: 14px;">0 B</div>
      </div>
    </div>

    <!-- Main Content -->
    <div class="main">
      <div id="table-view-section">
        <header>
          <div>
            <h1>Table: <span id="current-table-title" style="color: #2563eb; font-family: monospace;">(No table selected)</span></h1>
          </div>
          <div class="header-actions" style="display:flex; align-items:center; gap:10px;">
            <div style="display:flex; align-items:center; gap:6px; background:#f3f4f6; padding:5px 10px; border-radius:6px; border:1px solid #e5e7eb;">
              <label style="font-size:12px; font-weight:700; color:#374151;">📦 Compression:</label>
              <select id="table-compression-select" onchange="changeTableCompression(this.value)" style="padding:2px 8px; font-size:12px; border-radius:4px; border:1px solid #d1d5db; background:white; font-weight:600; cursor:pointer;">
                <option value="LZ4">LZ4 (Fast / Balanced)</option>
                <option value="ZSTD">ZSTD (Max Compression)</option>
                <option value="NONE">NONE (Raw / Uncompressed)</option>
              </select>
            </div>
            <button class="btn-warning" onclick="truncateCurrentTable()">TRUNCATE TABLE</button>
            <button class="btn-danger" onclick="dropCurrentTable()">DROP TABLE</button>
            <a href="https://github.com/Meow-256/fluxdb" target="_blank" rel="noopener noreferrer" style="display:inline-flex; align-items:center; gap:5px; padding:6px 12px; background:#111827; color:#ffffff; font-size:12px; font-weight:600; border-radius:6px; text-decoration:none;">
              <svg height="14" width="14" viewBox="0 0 16 16" fill="currentColor"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"></path></svg>
              GitHub
            </a>
            <div class="status-badge">● Online</div>
          </div>
        </header>

        <div id="empty-state" class="empty-banner" style="display:none;">
          <h3 style="color:#111827; margin-bottom:8px;">No Tables Found</h3>
          <p>Click "+ Create Table" on the left or run <code>CREATE TABLE &lt;name&gt;</code> from the CLI.</p>
        </div>

        <div id="table-content">
        <!-- Metrics -->
        <div class="metrics">
          <div class="metric-card">
            <div class="metric-label">Total Records</div>
            <div class="metric-val" id="val-total">0</div>
            <div class="metric-sub" id="val-total-sub">RAM + Persistent Disk</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">MemTable (RAM)</div>
            <div class="metric-val" id="val-mem">0</div>
            <div class="metric-sub">Active Unflushed Records</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">SSTable Files (Disk)</div>
            <div class="metric-val" id="val-sst">0</div>
            <div class="metric-sub">Persisted On-Disk Files</div>
          </div>
          <div class="metric-card">
            <div class="metric-label">Table Disk Size</div>
            <div class="metric-val" id="val-disk">0 B</div>
            <div class="metric-sub">WAL + SSTables Total</div>
          </div>
        </div>

        <!-- Tabs -->
        <div class="tabs">
          <button class="tab-btn active" onclick="showTab('show')">Show</button>
          <button class="tab-btn" onclick="showTab('lookup')">Lookup & JSON_SET</button>
          <button class="tab-btn" onclick="showTab('scan')">Range Scan (SCAN)</button>
          <button class="tab-btn" onclick="showTab('filter')">Filter & DelWhere</button>
          <button class="tab-btn" onclick="showTab('stats')">Stats & Aggregation</button>
          <button class="tab-btn" onclick="showTab('rank')">Leaderboard (TOP / RANK)</button>
          <button class="tab-btn" onclick="showTab('index')">Index Manager</button>
          <button class="tab-btn" onclick="showTab('ttl')">TTL / Expiration</button>
        </div>

        <!-- Tab 0: Show (All Keys Explorer) -->
        <div id="tab-show" class="panel active">
          <div class="show-container">
            <!-- Left: Keys Pane -->
            <div class="keys-pane">
              <div class="keys-header">
                <div class="keys-header-top">
                  <span style="font-weight:700; font-size:13px; color:#111827;">🔑 Keys (<span id="show-keys-count">0</span>)</span>
                  <button class="btn" style="padding:4px 8px; font-size:11px;" onclick="loadShowKeys()">🔄 Refresh</button>
                </div>
                <input type="text" id="show-key-filter" placeholder="Filter keys..." oninput="filterShowKeysList()" style="width:100%; padding:5px 8px; font-size:12px;">
              </div>
              <ul class="keys-list" id="show-keys-list">
                <li style="padding:12px; color:#9ca3af; text-align:center; font-size:12px;">Loading keys...</li>
              </ul>
            </div>

            <!-- Right: Record Data Viewer Pane -->
            <div class="data-pane">
              <div class="data-header">
                <div style="display:flex; align-items:center; gap:8px; min-width:0;">
                  <span style="font-weight:700; font-size:13px; color:#111827; white-space:nowrap;">📄 Record Detail:</span>
                  <span id="show-selected-key" style="font-family:monospace; font-weight:600; color:#2563eb; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">(No key selected)</span>
                </div>
                <div style="display:flex; gap:6px; flex-shrink:0;">
                  <button class="btn" style="padding:4px 10px; font-size:12px; background:#4b5563;" onclick="copyShowData()">📋 Copy JSON</button>
                </div>
              </div>
              <div class="data-body">
                <pre id="show-data-view">// Select a key on the left to view its stored JSON data</pre>
              </div>
            </div>
          </div>
        </div>

        <!-- Tab 1: Lookup & JSON_SET -->
        <div id="tab-lookup" class="panel">
          <div class="form-group">
            <input type="text" id="input-uuid" placeholder="Enter UUID or string key (e.g. 909fb8ea-1b14-4ca9-b15b-277ec2559be0 or steve)">
            <button class="btn" onclick="searchUuid()">Get Record (GET)</button>
            <button class="btn" style="background:#4b5563;" onclick="checkExists()">Check Exists (EXISTS)</button>
          </div>
          <div class="form-group" style="background:#f3f4f6; padding:10px; border-radius:6px;">
            <input type="text" id="jsonset-path" placeholder="JSON field path (e.g. stats.Bedwars.coins)" style="max-width:240px;">
            <input type="text" id="jsonset-val" placeholder="New JSON value (e.g. 5000000 or 'new_name')">
            <button class="btn" style="background:#059669;" onclick="performJsonSet()">Partial Update (JSON_SET)</button>
          </div>
          <pre id="lookup-result">// Record JSON payload will be displayed here</pre>
        </div>

        <!-- Tab 2: Range Scan -->
        <div id="tab-scan" class="panel">
          <div class="form-group">
            <input type="text" id="scan-start" placeholder="Start Key / UUID (blank for beginning)">
            <input type="text" id="scan-end" placeholder="End Key / UUID (blank for end)">
            <input type="number" id="scan-limit" placeholder="Limit" value="50" style="max-width:100px;">
            <button class="btn" onclick="runScan()">Execute Range Scan</button>
          </div>
          <pre id="scan-result">// Range scan results will be displayed here</pre>
        </div>

        <!-- Tab 3: Filter & Batch Delete -->
        <div id="tab-filter" class="panel">
          <div class="form-group">
            <input type="text" id="filter-query" placeholder="Filter expression (e.g. player.achievements.bedwars_level >= 100 AND player.stats.SkyWars.coins > 1000000)">
            <input type="number" id="filter-limit" placeholder="Limit" value="50" style="max-width:100px;">
            <button class="btn" onclick="runFilter()">Search (FILTER)</button>
            <button class="btn" style="background:#4b5563;" onclick="runCount()">Count Matches (COUNT)</button>
            <button class="btn btn-danger" onclick="runDelWhere()">Batch Delete (DEL_WHERE)</button>
          </div>
          <pre id="filter-result">// Filter query results will be displayed here</pre>
        </div>

        <!-- Tab 4: Aggregation & Stats -->
        <div id="tab-stats" class="panel">
          <div class="form-group">
            <input type="text" id="stats-field" placeholder="Target numeric JSON field (e.g. stats.Bedwars.kills_bedwars)">
            <input type="text" id="stats-query" placeholder="Optional filter query (e.g. stats.Bedwars.coins > 0)">
            <button class="btn" onclick="runStatsCalc()">Calculate Stats (SUM/AVG/MIN/MAX)</button>
          </div>
          <pre id="stats-result">// Aggregation results (Count, Sum, Avg, Min, Max) will be displayed here</pre>
        </div>

        <!-- Tab 5: Ranking -->
        <div id="tab-rank" class="panel">
          <div class="form-group" style="background:#f9fafb; padding:12px; border-radius:8px; border:1px solid #e5e7eb; gap:10px;">
            <div style="display:flex; flex-direction:column; gap:4px;">
              <label style="font-size:11px; font-weight:700; color:#4b5563;">INDEXED FIELD</label>
              <select id="select-rank-field" style="min-width:180px;"></select>
            </div>
            
            <div style="display:flex; flex-direction:column; gap:4px;">
              <label style="font-size:11px; font-weight:700; color:#4b5563;">QUERY MODE</label>
              <select id="rank-mode-select" onchange="onRankModeChange()" style="min-width:180px;">
                <option value="top">Top N Leaderboard</option>
                <option value="around_key">Around Key / UUID</option>
                <option value="around_score">Around Specific Score</option>
                <option value="score_range">Score Range (Min - Max)</option>
                <option value="rank_range">Rank Range (e.g. 30 - 50)</option>
              </select>
            </div>

            <!-- Dynamic Input Container -->
            <div id="rank-inputs-top" style="display:flex; flex-direction:column; gap:4px;">
              <label style="font-size:11px; font-weight:700; color:#4b5563;">LIMIT</label>
              <input type="number" id="rank-top-limit" value="50" style="max-width:100px;">
            </div>

            <div id="rank-inputs-around-key" style="display:none; gap:8px;">
              <div style="display:flex; flex-direction:column; gap:4px;">
                <label style="font-size:11px; font-weight:700; color:#4b5563;">TARGET KEY / UUID</label>
                <input type="text" id="rank-key-input" placeholder="e.g. steve or UUID" style="min-width:200px;">
              </div>
              <div style="display:flex; flex-direction:column; gap:4px;">
                <label style="font-size:11px; font-weight:700; color:#4b5563;">COUNT (N)</label>
                <input type="number" id="rank-key-limit" value="10" style="max-width:80px;">
              </div>
            </div>

            <div id="rank-inputs-around-score" style="display:none; gap:8px;">
              <div style="display:flex; flex-direction:column; gap:4px;">
                <label style="font-size:11px; font-weight:700; color:#4b5563;">CENTER SCORE</label>
                <input type="number" step="any" id="rank-score-input" placeholder="e.g. 1500" style="min-width:120px;">
              </div>
              <div style="display:flex; flex-direction:column; gap:4px;">
                <label style="font-size:11px; font-weight:700; color:#4b5563;">COUNT (N)</label>
                <input type="number" id="rank-score-limit" value="10" style="max-width:80px;">
              </div>
            </div>

            <div id="rank-inputs-score-range" style="display:none; gap:8px;">
              <div style="display:flex; flex-direction:column; gap:4px;">
                <label style="font-size:11px; font-weight:700; color:#4b5563;">MIN SCORE</label>
                <input type="number" step="any" id="rank-min-score" placeholder="e.g. 50" style="max-width:110px;">
              </div>
              <div style="display:flex; flex-direction:column; gap:4px;">
                <label style="font-size:11px; font-weight:700; color:#4b5563;">MAX SCORE</label>
                <input type="number" step="any" id="rank-max-score" placeholder="e.g. 100" style="max-width:110px;">
              </div>
              <div style="display:flex; flex-direction:column; gap:4px;">
                <label style="font-size:11px; font-weight:700; color:#4b5563;">LIMIT</label>
                <input type="number" id="rank-range-limit" value="50" style="max-width:80px;">
              </div>
            </div>

            <div id="rank-inputs-rank-range" style="display:none; gap:8px;">
              <div style="display:flex; flex-direction:column; gap:4px;">
                <label style="font-size:11px; font-weight:700; color:#4b5563;">START RANK</label>
                <input type="number" id="rank-start-rank" value="30" style="max-width:100px;">
              </div>
              <div style="display:flex; flex-direction:column; gap:4px;">
                <label style="font-size:11px; font-weight:700; color:#4b5563;">END RANK</label>
                <input type="number" id="rank-end-rank" value="50" style="max-width:100px;">
              </div>
            </div>

            <div style="display:flex; align-items:flex-end;">
              <button class="btn" onclick="loadRankings()" style="height:36px;">Fetch Rankings</button>
            </div>
          </div>
          <table>
            <thead>
              <tr>
                <th style="width: 80px;">Rank</th>
                <th>UUID / Key</th>
                <th style="text-align: right; width: 140px;">Score</th>
              </tr>
            </thead>
            <tbody id="rank-tbody">
              <tr><td colspan="3" style="text-align:center; color:#9ca3af;">Loading rankings...</td></tr>
            </tbody>
          </table>
        </div>

        <!-- Tab 6: Index Manager -->
        <div id="tab-index" class="panel">
          <div class="form-group">
            <input type="text" id="new-index-input" placeholder="JSON field path to index (e.g. stats.Bedwars.coins)">
            <button class="btn" onclick="addIndex()">Create Index</button>
          </div>
          <div style="font-weight: 600; margin-bottom: 6px; color: #4b5563;">Registered Secondary Indices:</div>
          <pre id="index-list">Loading index list...</pre>
        </div>

        <!-- Tab 7: TTL / Expire -->
        <div id="tab-ttl" class="panel">
          <div class="form-group">
            <input type="text" id="ttl-uuid" placeholder="Target Key / UUID">
            <input type="number" id="ttl-seconds" placeholder="Time-To-Live in Seconds" style="max-width: 160px;" value="60">
            <button class="btn" onclick="setTtl()">Set EXPIRE</button>
          </div>
          <pre id="ttl-result">// TTL operation result will be displayed here</pre>
        </div>
      </div>
    </div>

      <!-- Dedicated Global Server Settings View -->
      <div id="settings-view" style="display:none;">
        <header style="margin-bottom:20px; border-bottom:1px solid #e5e7eb; padding-bottom:12px;">
          <div>
            <h1 style="color:#6d28d9; font-size:22px;">⚙️ Global Server Settings</h1>
            <div style="font-size:12px; color:#6b7280; margin-top:4px;">Configure database runtime parameters dynamically without restarts</div>
          </div>
          <div class="status-badge" style="background:#f5f3ff; color:#6d28d9; border:1px solid #ddd6fe;">● Live Engine Active</div>
        </header>

        <div style="background:#f9fafb; border:1px solid #e5e7eb; border-radius:8px; padding:24px; max-width:680px;">
          <div style="margin-bottom:16px;">
            <label style="font-weight:600; font-size:13px; display:block; margin-bottom:4px;">MemTable RAM Capacity (Flush Threshold):</label>
            <select id="cfg-memtable" style="width:100%;">
              <option value="64">64 MB</option>
              <option value="128">128 MB</option>
              <option value="256">256 MB (Default)</option>
              <option value="512">512 MB</option>
              <option value="1024">1,024 MB (1 GB)</option>
              <option value="2048">2,048 MB (2 GB)</option>
            </select>
          </div>

          <div style="margin-bottom:16px;">
            <label style="font-weight:600; font-size:13px; display:block; margin-bottom:4px;">Decompressed LRU Block Cache:</label>
            <select id="cfg-cache" style="width:100%;">
              <option value="0">0 MB (Disabled / Cache OFF)</option>
              <option value="64">64 MB</option>
              <option value="128">128 MB</option>
              <option value="256">256 MB (Recommended)</option>
              <option value="512">512 MB</option>
              <option value="1024">1,024 MB (1 GB)</option>
            </select>
            <span style="font-size:11px; color:#6b7280;">Retains decompressed data blocks in RAM for ultra-fast nanosecond point lookups</span>
          </div>

          <div style="margin-bottom:16px;">
            <label style="font-weight:600; font-size:13px; display:block; margin-bottom:4px;">Parallel Worker Threads:</label>
            <input type="number" id="cfg-threads" min="0" max="128" style="width:100%;" placeholder="0 = auto CPU core detection">
            <span style="font-size:11px; color:#6b7280;">0: Auto-scale to available CPU cores / 1-128: Explicit worker thread pool size</span>
          </div>

          <div style="margin-bottom:16px;">
            <label style="font-weight:600; font-size:13px; display:block; margin-bottom:4px;">Authentication Password (AUTH):</label>
            <input type="text" id="cfg-auth" style="width:100%;" placeholder="Leave blank to disable authentication">
          </div>

          <div style="margin-bottom:16px;">
            <label style="font-weight:600; font-size:13px; display:block; margin-bottom:4px;">Group Commit Delay Window (µs):</label>
            <select id="cfg-delay" style="width:100%;">
              <option value="100">100 µs (Ultra-low latency)</option>
              <option value="500">500 µs</option>
              <option value="1000">1,000 µs (1 ms - Recommended)</option>
              <option value="2000">2,000 µs (2 ms)</option>
              <option value="5000">5,000 µs (High throughput)</option>
            </select>
          </div>

          <div style="margin-bottom:16px;">
            <label style="font-weight:600; font-size:13px; display:block; margin-bottom:4px;">L0 SSTables Compaction Trigger:</label>
            <input type="number" id="cfg-compaction" min="2" max="32" style="width:100%;" value="4">
          </div>

          <div style="margin-bottom:20px;">
            <label style="font-weight:600; font-size:13px; display:flex; align-items:center; gap:8px;">
              <input type="checkbox" id="cfg-async-fsync"> Asynchronous periodic fsync mode (High-throughput)
            </label>
          </div>

          <button class="btn" style="background:#7c3aed; width:100%; padding:10px; font-size:14px;" onclick="saveServerConfig()">💾 Save & Apply Hot Configuration</button>
          <pre id="cfg-status" style="margin-top:14px; display:none;"></pre>
        </div>
      </div>
    </div>
  </div>

  <script>
    let currentTable = null;
    let showAllRecords = [];
    let selectedShowKey = null;

    function escapeHtml(str) {
      if (typeof str !== 'string') str = String(str);
      return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#039;');
    }

    async function loadTables() {
      try {
        const res = await fetch('/api/tables');
        const data = await res.json();
        const listEl = document.getElementById('sidebar-tables');
        listEl.innerHTML = '';
        const tables = data.tables || [];

        if (tables.length === 0) {
          currentTable = null;
          document.getElementById('current-table-title').innerText = '(None)';
          document.getElementById('empty-state').style.display = 'block';
          document.getElementById('table-content').style.display = 'none';
          listEl.innerHTML = '<li style="color:#9ca3af; font-size:12px; padding:4px;">No tables found</li>';
          return;
        }

        document.getElementById('empty-state').style.display = 'none';
        document.getElementById('table-content').style.display = 'block';

        const prevTable = currentTable;
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

        if (!prevTable && currentTable) {
          loadShowKeys();
        }
      } catch (e) {
        console.error(e);
      }
    }

    async function loadShowKeys() {
      if (!currentTable) return;
      const listEl = document.getElementById('show-keys-list');
      const countEl = document.getElementById('show-keys-count');
      try {
        const res = await fetch('/api/keys?table=' + encodeURIComponent(currentTable) + '&limit=50000');
        const data = await res.json();
        showAllRecords = data.records || [];
        countEl.innerText = showAllRecords.length.toLocaleString();
        renderShowKeysList(showAllRecords);
        if (showAllRecords.length > 0) {
          const match = showAllRecords.find(r => r.uuid === selectedShowKey);
          if (match) {
            selectShowRecord(match);
          } else {
            selectShowRecord(showAllRecords[0]);
          }
        } else {
          selectedShowKey = null;
          document.getElementById('show-selected-key').innerText = '(No keys in table)';
          document.getElementById('show-data-view').innerText = '// Table has no records yet';
        }
      } catch (e) {
        listEl.innerHTML = '<li style="padding:12px; color:#dc2626; text-align:center; font-size:12px;">Error: ' + e + '</li>';
      }
    }

    function renderShowKeysList(records) {
      const listEl = document.getElementById('show-keys-list');
      listEl.innerHTML = '';
      if (records.length === 0) {
        listEl.innerHTML = '<li style="padding:12px; color:#9ca3af; text-align:center; font-size:12px;">No keys found</li>';
        return;
      }
      records.forEach(r => {
        const li = document.createElement('li');
        li.className = 'key-item' + (r.uuid === selectedShowKey ? ' active' : '');
        const isDifferent = r.label !== r.uuid;
        li.innerHTML = `
          <div class="key-item-label">${escapeHtml(r.label)}</div>
          ${isDifferent ? `<div class="key-item-uuid">${escapeHtml(r.uuid)}</div>` : ''}
        `;
        li.onclick = () => selectShowRecord(r);
        listEl.appendChild(li);
      });
    }

    function filterShowKeysList() {
      const filter = (document.getElementById('show-key-filter').value || '').trim().toLowerCase();
      const filtered = showAllRecords.filter(r => {
        if ((r.label || '').toLowerCase().includes(filter)) return true;
        if ((r.uuid || '').toLowerCase().includes(filter)) return true;
        if (r.data && JSON.stringify(r.data).toLowerCase().includes(filter)) return true;
        return false;
      });
      renderShowKeysList(filtered);
    }

    function selectShowRecord(rec) {
      selectedShowKey = rec.uuid;
      document.querySelectorAll('.key-item').forEach(el => {
        const uuidEl = el.querySelector('.key-item-uuid');
        const labelEl = el.querySelector('.key-item-label');
        const isSelected = (uuidEl && uuidEl.innerText === rec.uuid) || (labelEl && labelEl.innerText === rec.label && (!uuidEl || labelEl.innerText === rec.uuid));
        if (isSelected) {
          el.classList.add('active');
        } else {
          el.classList.remove('active');
        }
      });
      
      const keyHeader = rec.label !== rec.uuid 
        ? `${escapeHtml(rec.label)} <span style="font-size:11px; color:#6b7280; font-weight:normal;">(${escapeHtml(rec.uuid)})</span>`
        : escapeHtml(rec.uuid);
      document.getElementById('show-selected-key').innerHTML = keyHeader;

      const box = document.getElementById('show-data-view');
      if (rec.data !== undefined) {
        box.innerText = JSON.stringify(rec.data, null, 2);
      } else {
        box.innerText = '// No data';
      }
    }

    function copyShowData() {
      const text = document.getElementById('show-data-view').innerText;
      navigator.clipboard.writeText(text).then(() => {
        alert('Copied record JSON data to clipboard!');
      }).catch(err => {
        console.error('Failed to copy', err);
      });
    }

    async function promptCreateTable() {
      const name = prompt('Enter new table name (e.g. players, guilds, users):');
      if (!name) return;
      const clean = name.trim().toLowerCase();
      const res = await fetch('/api/table/create?name=' + encodeURIComponent(clean));
      if (res.ok) {
        switchTable(clean);
      }
    }

    async function dropCurrentTable() {
      if (!currentTable) return;
      if (!confirm(`Are you sure you want to permanently DROP table '${currentTable}'?\nAll data files on disk will be deleted.`)) return;
      const res = await fetch('/api/table/drop?name=' + encodeURIComponent(currentTable));
      const data = await res.json();
      if (data.success) {
        currentTable = null;
        loadTables();
      } else {
        alert('Error: ' + (data.error || 'Drop failed'));
      }
    }

    async function truncateCurrentTable() {
      if (!currentTable) return;
      if (!confirm(`Are you sure you want to TRUNCATE all records from table '${currentTable}'?`)) return;
      const res = await fetch('/api/table/truncate?name=' + encodeURIComponent(currentTable));
      const data = await res.json();
      if (data.success) {
        updateTableStats();
        alert(`Table '${currentTable}' has been truncated.`);
      } else {
        alert('Error: ' + (data.error || 'Truncate failed'));
      }
    }

    async function triggerFlush() {
      if (!currentTable) return;
      const res = await fetch('/api/flush?table=' + encodeURIComponent(currentTable));
      const data = await res.json();
      alert('Flush completed: ' + JSON.stringify(data));
      updateTableStats();
    }

    async function triggerBackup() {
      const res = await fetch('/api/backup');
      const data = await res.json();
      alert('Backup completed: ' + (data.backup_path || data.error));
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
      box.innerText = 'Searching...';
      try {
        const res = await fetch(`/api/get?table=${encodeURIComponent(currentTable)}&uuid=${encodeURIComponent(uuid)}`);
        const data = await res.json();
        box.innerText = JSON.stringify(data, null, 2);
      } catch (e) {
        box.innerText = 'Error: ' + e;
      }
    }

    async function performJsonSet() {
      if (!currentTable) return;
      const uuid = document.getElementById('input-uuid').value.trim();
      const path = document.getElementById('jsonset-path').value.trim();
      const val = document.getElementById('jsonset-val').value.trim();
      if (!uuid || !path) {
        alert('Please enter both UUID/Key and JSON path');
        return;
      }
      const box = document.getElementById('lookup-result');
      box.innerText = 'Updating...';
      try {
        const res = await fetch(`/api/json_set?table=${encodeURIComponent(currentTable)}&uuid=${encodeURIComponent(uuid)}&path=${encodeURIComponent(path)}&value=${encodeURIComponent(val)}`);
        const data = await res.json();
        box.innerText = JSON.stringify(data, null, 2);
        updateTableStats();
      } catch (e) {
        box.innerText = 'Error: ' + e;
      }
    }

    async function runScan() {
      if (!currentTable) return;
      const start = document.getElementById('scan-start').value.trim();
      const end = document.getElementById('scan-end').value.trim();
      const limit = document.getElementById('scan-limit').value.trim() || '50';
      const box = document.getElementById('scan-result');
      box.innerText = 'Scanning...';
      try {
        const res = await fetch(`/api/scan?table=${encodeURIComponent(currentTable)}&start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}&limit=${encodeURIComponent(limit)}`);
        const data = await res.json();
        box.innerText = JSON.stringify(data, null, 2);
      } catch (e) {
        box.innerText = 'Error: ' + e;
      }
    }

    async function runFilter() {
      if (!currentTable) return;
      const q = document.getElementById('filter-query').value.trim();
      const limit = document.getElementById('filter-limit').value.trim() || '50';
      const box = document.getElementById('filter-result');
      box.innerText = 'Filtering...';
      try {
        const res = await fetch(`/api/filter?table=${encodeURIComponent(currentTable)}&query=${encodeURIComponent(q)}&limit=${encodeURIComponent(limit)}`);
        const data = await res.json();
        box.innerText = JSON.stringify(data, null, 2);
      } catch (e) {
        box.innerText = 'Error: ' + e;
      }
    }

    async function runCount() {
      if (!currentTable) return;
      const q = document.getElementById('filter-query').value.trim();
      const box = document.getElementById('filter-result');
      box.innerText = 'Counting records...';
      try {
        const res = await fetch(`/api/count?table=${encodeURIComponent(currentTable)}&query=${encodeURIComponent(q)}`);
        const data = await res.json();
        box.innerText = JSON.stringify(data, null, 2);
      } catch (e) {
        box.innerText = 'Error: ' + e;
      }
    }

    async function runDelWhere() {
      if (!currentTable) return;
      const q = document.getElementById('filter-query').value.trim();
      if (!q) {
        alert('Please specify a deletion filter query');
        return;
      }
      if (!confirm(`Are you sure you want to batch delete (DEL_WHERE) all records matching condition [${q}]?`)) return;
      const box = document.getElementById('filter-result');
      box.innerText = 'Deleting records...';
      try {
        const res = await fetch(`/api/del_where?table=${encodeURIComponent(currentTable)}&query=${encodeURIComponent(q)}`);
        const data = await res.json();
        box.innerText = JSON.stringify(data, null, 2);
        updateTableStats();
      } catch (e) {
        box.innerText = 'Error: ' + e;
      }
    }

    async function runStatsCalc() {
      if (!currentTable) return;
      const field = document.getElementById('stats-field').value.trim();
      const q = document.getElementById('stats-query').value.trim();
      if (!field) {
        alert('Please enter a target numeric field name');
        return;
      }
      const box = document.getElementById('stats-result');
      box.innerText = 'Calculating statistics...';
      try {
        const res = await fetch(`/api/stats_calc?table=${encodeURIComponent(currentTable)}&field=${encodeURIComponent(field)}&query=${encodeURIComponent(q)}`);
        const data = await res.json();
        box.innerText = JSON.stringify(data, null, 2);
      } catch (e) {
        box.innerText = 'Error: ' + e;
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
        box.innerText = 'Key Existence (EXISTS): ' + (data.exists ? 'EXISTS (TRUE)' : 'NOT FOUND (FALSE)');
      } catch (e) {
        box.innerText = 'Error: ' + e;
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
        box.innerText = 'Error: ' + e;
      }
    }

    function onRankModeChange() {
      const mode = document.getElementById('rank-mode-select').value;
      document.getElementById('rank-inputs-top').style.display = mode === 'top' ? 'flex' : 'none';
      document.getElementById('rank-inputs-around-key').style.display = mode === 'around_key' ? 'flex' : 'none';
      document.getElementById('rank-inputs-around-score').style.display = mode === 'around_score' ? 'flex' : 'none';
      document.getElementById('rank-inputs-score-range').style.display = mode === 'score_range' ? 'flex' : 'none';
      document.getElementById('rank-inputs-rank-range').style.display = mode === 'rank_range' ? 'flex' : 'none';
    }

    async function loadRankings() {
      if (!currentTable) return;
      const field = document.getElementById('select-rank-field').value;
      const tbody = document.getElementById('rank-tbody');
      if (!field) {
        tbody.innerHTML = '<tr><td colspan="3" style="text-align:center; color:#9ca3af;">No indices registered</td></tr>';
        return;
      }
      const mode = document.getElementById('rank-mode-select').value;
      let url = `/api/rankings?table=${encodeURIComponent(currentTable)}&field=${encodeURIComponent(field)}&mode=${encodeURIComponent(mode)}`;

      if (mode === 'top') {
        const lim = document.getElementById('rank-top-limit').value || '50';
        url += `&limit=${encodeURIComponent(lim)}`;
      } else if (mode === 'around_key') {
        const key = document.getElementById('rank-key-input').value.trim();
        const lim = document.getElementById('rank-key-limit').value || '10';
        if (!key) {
          alert('Please enter target UUID or key');
          return;
        }
        url += `&key=${encodeURIComponent(key)}&limit=${encodeURIComponent(lim)}`;
      } else if (mode === 'around_score') {
        const score = document.getElementById('rank-score-input').value.trim();
        const lim = document.getElementById('rank-score-limit').value || '10';
        if (!score) {
          alert('Please enter center score');
          return;
        }
        url += `&score=${encodeURIComponent(score)}&limit=${encodeURIComponent(lim)}`;
      } else if (mode === 'score_range') {
        const min = document.getElementById('rank-min-score').value.trim();
        const max = document.getElementById('rank-max-score').value.trim();
        const lim = document.getElementById('rank-range-limit').value || '50';
        if (!min || !max) {
          alert('Please enter both min and max score');
          return;
        }
        url += `&min=${encodeURIComponent(min)}&max=${encodeURIComponent(max)}&limit=${encodeURIComponent(lim)}`;
      } else if (mode === 'rank_range') {
        const start = document.getElementById('rank-start-rank').value.trim() || '1';
        const end = document.getElementById('rank-end-rank').value.trim() || '50';
        url += `&start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`;
      }

      tbody.innerHTML = '<tr><td colspan="3" style="text-align:center;">Loading rankings...</td></tr>';
      try {
        const res = await fetch(url);
        const data = await res.json();
        tbody.innerHTML = '';
        if (!data.rankings || data.rankings.length === 0) {
          tbody.innerHTML = '<tr><td colspan="3" style="text-align:center; color:#9ca3af;">No matching records found</td></tr>';
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
        tbody.innerHTML = `<tr><td colspan="3" style="text-align:center; color:#dc2626;">Error: ${e}</td></tr>`;
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
      box.innerText = 'Applying configuration...';
      try {
        const res = await fetch('/api/config/update?' + q);
        const data = await res.json();
        box.innerText = 'Configuration saved and applied live:\n' + JSON.stringify(data, null, 2);
        updateTableStats();
      } catch (e) {
        box.innerText = 'Error saving configuration: ' + e;
      }
    }

    function openServerSettingsView() {
      document.getElementById('table-view-section').style.display = 'none';
      document.getElementById('settings-view').style.display = 'block';
      document.querySelectorAll('.table-item').forEach(el => el.classList.remove('active'));
      const btn = document.getElementById('btn-side-settings');
      btn.style.background = '#7c3aed';
      btn.style.color = '#ffffff';
      loadServerConfig();
    }

    function switchTable(name) {
      document.getElementById('settings-view').style.display = 'none';
      document.getElementById('table-view-section').style.display = 'block';
      const btn = document.getElementById('btn-side-settings');
      btn.style.background = '#f5f3ff';
      btn.style.color = '#6d28d9';
      currentTable = name;
      selectedShowKey = null;
      document.getElementById('current-table-title').innerText = name;
      document.getElementById('lookup-result').innerText = '// Record JSON payload will be displayed here';
      loadTables();
      updateTableStats();
      loadShowKeys();
    }

    function showTab(name) {
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
      event.target.classList.add('active');
      document.getElementById('tab-' + name).classList.add('active');
      if (name === 'show') loadShowKeys();
      if (name === 'rank') loadRankings();
    }

    loadTables().then(() => {
      updateTableStats();
      loadServerConfig();
      loadShowKeys();
    });
    setInterval(() => {
      if (document.getElementById('table-view-section').style.display !== 'none') {
        loadTables();
        updateTableStats();
      }
    }, 3000);
  </script>
</body>
</html>
"#;
