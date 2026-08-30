use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

use crate::core::types::{OpType, PlayerId, Result};
use crate::index::QueryFilter;
use crate::proto::{Command, CommandParser};
use crate::table::TableManager;

pub struct Server {
    addr: SocketAddr,
    table_manager: Arc<TableManager>,
    require_pass: Option<String>,
}

impl Server {
    pub fn new(addr: SocketAddr, table_manager: Arc<TableManager>, require_pass: Option<String>) -> Self {
        Self { addr, table_manager, require_pass }
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(self.addr).await?;
        info!("MeowDB server listening on {}", self.addr);
        if self.require_pass.is_some() {
            info!("Password authentication (AUTH): ENABLED");
        }

        loop {
            let (socket, peer_addr) = match listener.accept().await {
                Ok(res) => res,
                Err(e) => {
                    warn!("Failed to accept incoming TCP connection: {}", e);
                    continue;
                }
            };

            let _ = socket.set_nodelay(true);

            let table_manager = self.table_manager.clone();
            let require_pass = self.require_pass.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, peer_addr, table_manager, require_pass).await {
                    if !e.to_string().contains("Broken pipe") && !e.to_string().contains("Connection reset") {
                        warn!("Connection error from {}: {}", peer_addr, e);
                    }
                }
            });
        }
    }
}

async fn execute_single_command(
    cmd: Command,
    table_manager: &Arc<TableManager>,
) -> Vec<u8> {
    match cmd {
        Command::Ping => b"+PONG\r\n".to_vec(),
        Command::Auth { .. } => b"+OK\r\n".to_vec(),
        Command::Quit => b"+OK\r\n".to_vec(),

        Command::Multi => b"-ERR MULTI calls can not be nested\r\n".to_vec(),
        Command::Exec => b"-ERR EXEC without MULTI\r\n".to_vec(),
        Command::Discard => b"-ERR DISCARD without MULTI\r\n".to_vec(),

        Command::Tables => {
            let tables = table_manager.list_tables();
            let json_res = serde_json::to_string(&tables).unwrap_or_else(|_| "[]".into());
            format!("${}\r\n{}\r\n", json_res.len(), json_res).into_bytes()
        }

        Command::CreateTable { table } => {
            match table_manager.create_table(&table).await {
                Ok(_) => format!("+TABLE CREATED {}\r\n", table).into_bytes(),
                Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
            }
        }

        Command::DropTable { table } => {
            match table_manager.drop_table(&table).await {
                Ok(true) => format!("+TABLE DROPPED {}\r\n", table).into_bytes(),
                Ok(false) => format!("-ERR Table '{}' does not exist\r\n", table).into_bytes(),
                Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
            }
        }

        Command::TruncateTable { table } => {
            match table_manager.truncate_table(&table).await {
                Ok(true) => format!("+TABLE TRUNCATED {}\r\n", table).into_bytes(),
                Ok(false) => format!("-ERR Table '{}' does not exist\r\n", table).into_bytes(),
                Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
            }
        }

        Command::Set { table, key, value } => {
            match table_manager.create_table(&table).await {
                Ok(tbl) => {
                    tbl.index_manager.on_put(key, &value);
                    match tbl.engine.put(key, value).await {
                        Ok(_) => b"+OK\r\n".to_vec(),
                        Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
                    }
                }
                Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
            }
        }

        Command::JsonSet { table, key, path, value } => {
            match table_manager.create_table(&table).await {
                Ok(tbl) => {
                    match tbl.engine.json_set(key, &path, &value).await {
                        Ok(updated_bytes) => {
                            tbl.index_manager.on_put(key, &updated_bytes);
                            b"+OK\r\n".to_vec()
                        }
                        Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
                    }
                }
                Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
            }
        }

        Command::Mset { table, entries } => {
            match table_manager.create_table(&table).await {
                Ok(tbl) => {
                    let mut success = true;
                    let mut last_err = String::new();
                    for (key, value) in entries {
                        tbl.index_manager.on_put(key, &value);
                        if let Err(e) = tbl.engine.put(key, value).await {
                            success = false;
                            last_err = e.to_string();
                            break;
                        }
                    }
                    if success {
                        b"+OK\r\n".to_vec()
                    } else {
                        format!("-ERR {}\r\n", last_err).into_bytes()
                    }
                }
                Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
            }
        }

        Command::Get { table, key } => {
            match table_manager.get_table(&table) {
                Some(tbl) => match tbl.engine.get(&key) {
                    Ok(Some(val)) => {
                        let mut resp = format!("${}\r\n", val.len()).into_bytes();
                        resp.extend_from_slice(&val);
                        resp.extend_from_slice(b"\r\n");
                        resp
                    }
                    Ok(None) => b"$-1\r\n".to_vec(),
                    Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
                },
                None => format!("-ERR Table '{}' does not exist\r\n", table).into_bytes(),
            }
        }

        Command::Mget { table, keys } => {
            match table_manager.get_table(&table) {
                Some(tbl) => {
                    let mut resp = format!("*{}\r\n", keys.len()).into_bytes();
                    for key in keys {
                        match tbl.engine.get(&key) {
                            Ok(Some(val)) => {
                                resp.extend_from_slice(format!("${}\r\n", val.len()).as_bytes());
                                resp.extend_from_slice(&val);
                                resp.extend_from_slice(b"\r\n");
                            }
                            _ => {
                                resp.extend_from_slice(b"$-1\r\n");
                            }
                        }
                    }
                    resp
                }
                None => format!("-ERR Table '{}' does not exist\r\n", table).into_bytes(),
            }
        }

        Command::Scan { table, start_key, end_key, limit } => {
            if let Some(tbl) = table_manager.get_table(&table) {
                match tbl.engine.scan(start_key, end_key, limit) {
                    Ok(entries) => {
                        let formatted: Vec<serde_json::Value> = entries
                            .into_iter()
                            .map(|(k, v)| {
                                let parsed: serde_json::Value = serde_json::from_slice(&v).unwrap_or(serde_json::Value::String(String::from_utf8_lossy(&v).into_owned()));
                                serde_json::json!({
                                    "uuid": k.to_string(),
                                    "data": parsed,
                                })
                            })
                            .collect();
                        let json_res = serde_json::to_string(&formatted).unwrap_or_else(|_| "[]".into());
                        format!("${}\r\n{}\r\n", json_res.len(), json_res).into_bytes()
                    }
                    Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
                }
            } else {
                format!("-ERR Table '{}' does not exist\r\n", table).into_bytes()
            }
        }

        Command::Filter { table, query, limit } => {
            if let Some(tbl) = table_manager.get_table(&table) {
                match QueryFilter::parse(&query) {
                    Ok(filter) => {
                        match tbl.engine.scan(None, None, 100000) {
                            Ok(entries) => {
                                let mut matched: Vec<serde_json::Value> = Vec::new();
                                for (k, v) in entries {
                                    if filter.matches(&v) {
                                        let parsed: serde_json::Value = serde_json::from_slice(&v).unwrap_or(serde_json::Value::String(String::from_utf8_lossy(&v).into_owned()));
                                        matched.push(serde_json::json!({
                                            "uuid": k.to_string(),
                                            "data": parsed,
                                        }));
                                        if matched.len() >= limit {
                                            break;
                                        }
                                    }
                                }
                                let json_res = serde_json::to_string(&matched).unwrap_or_else(|_| "[]".into());
                                format!("${}\r\n{}\r\n", json_res.len(), json_res).into_bytes()
                            }
                            Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
                        }
                    }
                    Err(e) => format!("-ERR Invalid filter query: {}\r\n", e).into_bytes(),
                }
            } else {
                format!("-ERR Table '{}' does not exist\r\n", table).into_bytes()
            }
        }

        Command::Delete { table, key } => {
            if let Some(tbl) = table_manager.get_table(&table) {
                tbl.index_manager.on_delete(key);
                let _ = tbl.engine.delete(key).await;
                b":1\r\n".to_vec()
            } else {
                format!("-ERR Table '{}' does not exist\r\n", table).into_bytes()
            }
        }

        Command::DelWhere { table, query } => {
            if let Some(tbl) = table_manager.get_table(&table) {
                match QueryFilter::parse(&query) {
                    Ok(filter) => match tbl.engine.del_where(&filter).await {
                        Ok(count) => format!(":{}\r\n", count).into_bytes(),
                        Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
                    },
                    Err(e) => format!("-ERR Invalid filter query: {}\r\n", e).into_bytes(),
                }
            } else {
                format!("-ERR Table '{}' does not exist\r\n", table).into_bytes()
            }
        }

        Command::Count { table, query } => {
            if let Some(tbl) = table_manager.get_table(&table) {
                let filter_res = match &query {
                    Some(q) => match QueryFilter::parse(q) {
                        Ok(f) => Ok(Some(f)),
                        Err(e) => Err(format!("-ERR Invalid filter query: {}\r\n", e)),
                    },
                    None => Ok(None),
                };

                match filter_res {
                    Ok(opt_f) => match tbl.engine.count_records(opt_f.as_ref()) {
                        Ok(c) => format!(":{}\r\n", c).into_bytes(),
                        Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
                    },
                    Err(err_resp) => err_resp.into_bytes(),
                }
            } else {
                format!("-ERR Table '{}' does not exist\r\n", table).into_bytes()
            }
        }

        Command::CalcStats { table, field, query } => {
            if let Some(tbl) = table_manager.get_table(&table) {
                let filter_res = match &query {
                    Some(q) => match QueryFilter::parse(q) {
                        Ok(f) => Ok(Some(f)),
                        Err(e) => Err(format!("-ERR Invalid filter query: {}\r\n", e)),
                    },
                    None => Ok(None),
                };

                match filter_res {
                    Ok(opt_f) => match tbl.engine.calc_stats(&field, opt_f.as_ref()) {
                        Ok(stats_val) => {
                            let json_res = serde_json::to_string(&stats_val).unwrap_or_else(|_| "{}".into());
                            format!("${}\r\n{}\r\n", json_res.len(), json_res).into_bytes()
                        }
                        Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
                    },
                    Err(err_resp) => err_resp.into_bytes(),
                }
            } else {
                format!("-ERR Table '{}' does not exist\r\n", table).into_bytes()
            }
        }

        Command::Exists { table, keys } => {
            if let Some(tbl) = table_manager.get_table(&table) {
                let mut count = 0;
                for key in keys {
                    if tbl.engine.exists(&key).unwrap_or(false) {
                        count += 1;
                    }
                }
                format!(":{}\r\n", count).into_bytes()
            } else {
                format!("-ERR Table '{}' does not exist\r\n", table).into_bytes()
            }
        }

        Command::Expire { table, key, seconds } => {
            if let Some(tbl) = table_manager.get_table(&table) {
                let ok = tbl.engine.set_expire(&key, seconds);
                format!(":{}\r\n", if ok { 1 } else { 0 }).into_bytes()
            } else {
                format!("-ERR Table '{}' does not exist\r\n", table).into_bytes()
            }
        }

        Command::Ttl { table, key } => {
            if let Some(tbl) = table_manager.get_table(&table) {
                let ttl = tbl.engine.get_ttl(&key);
                format!(":{}\r\n", ttl).into_bytes()
            } else {
                format!("-ERR Table '{}' does not exist\r\n", table).into_bytes()
            }
        }

        Command::IndexCreate { table, path } => {
            match table_manager.create_table(&table).await {
                Ok(tbl) => {
                    tbl.index_manager.create_index(&path);
                    format!("+INDEX CREATED {}:{}\r\n", table, path).into_bytes()
                }
                Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
            }
        }

        Command::IndexList { table } => {
            if let Some(tbl) = table_manager.get_table(&table) {
                let list = tbl.index_manager.list_indices();
                let json_res = serde_json::to_string(&list).unwrap_or_else(|_| "[]".into());
                format!("${}\r\n{}\r\n", json_res.len(), json_res).into_bytes()
            } else {
                b"$2\r\n[]\r\n".to_vec()
            }
        }

        Command::Top { table, path, limit } => {
            match table_manager.get_table(&table).and_then(|t| t.index_manager.get_index(&path)) {
                Some(index) => {
                    let top_entries = index.get_top(limit);
                    let formatted: Vec<serde_json::Value> = top_entries
                        .into_iter()
                        .map(|(player, score, rank)| {
                            serde_json::json!({
                                "rank": rank,
                                "uuid": player.to_string(),
                                "score": score,
                            })
                        })
                        .collect();

                    let json_res = serde_json::to_string(&formatted).unwrap_or_else(|_| "[]".into());
                    format!("${}\r\n{}\r\n", json_res.len(), json_res).into_bytes()
                }
                None => format!("-ERR Index '{}:{}' not found.\r\n", table, path).into_bytes(),
            }
        }

        Command::Rank { table, path, key } => {
            match table_manager.get_table(&table).and_then(|t| t.index_manager.get_index(&path)) {
                Some(index) => match index.get_rank(&key) {
                    Some((rank, score)) => {
                        let val = serde_json::json!({
                            "uuid": key.to_string(),
                            "table": table,
                            "rank": rank,
                            "score": score,
                            "total_ranked": index.len(),
                        });
                        let json_res = serde_json::to_string(&val).unwrap();
                        format!("${}\r\n{}\r\n", json_res.len(), json_res).into_bytes()
                    }
                    None => b"$-1\r\n".to_vec(),
                },
                None => format!("-ERR Index '{}:{}' not found.\r\n", table, path).into_bytes(),
            }
        }

        Command::Backup { target_dir } => {
            match table_manager.backup_all(target_dir.as_deref()).await {
                Ok(path) => format!("+BACKUP COMPLETED {}\r\n", path.display()).into_bytes(),
                Err(e) => format!("-ERR Backup failed: {}\r\n", e).into_bytes(),
            }
        }

        Command::Stats { table } => {
            if let Some(tbl_name) = table {
                if let Some(tbl) = table_manager.get_table(&tbl_name) {
                    let (mem, imm, sst) = tbl.engine.stats();
                    let total = tbl.engine.total_entries();
                    let indices = tbl.index_manager.list_indices();
                    let stats = serde_json::json!({
                        "table": tbl_name,
                        "total_records": total,
                        "active_memtable_entries": mem,
                        "immutable_memtables": imm,
                        "sstable_count": sst,
                        "disk_size_bytes": tbl.engine.disk_size_bytes(),
                        "indexed_fields": indices,
                    });
                    let json_res = serde_json::to_string_pretty(&stats).unwrap();
                    format!("${}\r\n{}\r\n", json_res.len(), json_res).into_bytes()
                } else {
                    b"-ERR Table not found\r\n".to_vec()
                }
            } else {
                let tables = table_manager.list_tables();
                let stats = serde_json::json!({
                    "tables": tables,
                    "total_disk_size_bytes": table_manager.total_disk_size_bytes(),
                });
                let json_res = serde_json::to_string_pretty(&stats).unwrap();
                format!("${}\r\n{}\r\n", json_res.len(), json_res).into_bytes()
            }
        }

        Command::Flush { table } => {
            if let Some(tbl_name) = table {
                if let Some(tbl) = table_manager.get_table(&tbl_name) {
                    let _ = tbl.engine.force_flush().await;
                }
            } else {
                for tbl_name in table_manager.list_tables() {
                    if let Some(tbl) = table_manager.get_table(&tbl_name) {
                        let _ = tbl.engine.force_flush().await;
                    }
                }
            }
            b"+OK\r\n".to_vec()
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    _peer_addr: SocketAddr,
    table_manager: Arc<TableManager>,
    _require_pass: Option<String>,
) -> Result<()> {
    let (reader, writer) = stream.into_split();
    let mut lines = BufReader::with_capacity(64 * 1024, reader).lines();
    let mut writer = BufWriter::with_capacity(64 * 1024, writer);

    let mut is_authenticated = false;
    let mut in_multi = false;
    let mut multi_queue: Vec<Command> = Vec::new();

    while let Ok(Some(line)) = lines.next_line().await {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let cmd = match CommandParser::parse(trimmed) {
            Ok(c) => c,
            Err(e) => {
                let err_msg = format!("-ERR {}\r\n", e);
                writer.write_all(err_msg.as_bytes()).await?;
                writer.flush().await?;
                continue;
            }
        };

        // Check dynamic authentication requirement
        let current_require_pass = table_manager.get_auth_password();
        let needs_auth = current_require_pass.is_some() && !is_authenticated;

        if needs_auth {
            match &cmd {
                Command::Auth { password } => {
                    if let Some(ref pass) = current_require_pass {
                        if password == pass {
                            is_authenticated = true;
                            writer.write_all(b"+OK\r\n").await?;
                        } else {
                            writer.write_all(b"-ERR invalid password\r\n").await?;
                        }
                    } else {
                        is_authenticated = true;
                        writer.write_all(b"+OK\r\n").await?;
                    }
                    writer.flush().await?;
                    continue;
                }
                Command::Ping => {
                    writer.write_all(b"+PONG\r\n").await?;
                    writer.flush().await?;
                    continue;
                }
                Command::Quit => {
                    writer.write_all(b"+OK\r\n").await?;
                    writer.flush().await?;
                    break;
                }
                _ => {
                    writer.write_all(b"-ERR NOAUTH Authentication required.\r\n").await?;
                    writer.flush().await?;
                    continue;
                }
            }
        }

        // Handle MULTI transaction mode
        if in_multi {
            match cmd {
                Command::Exec => {
                    in_multi = false;
                    let queue = std::mem::take(&mut multi_queue);
                    
                    // Group writes by table for atomic execution
                    let mut table_writes: HashMap<String, Vec<(PlayerId, Option<Bytes>, OpType)>> = HashMap::new();
                    for c in &queue {
                        match c {
                            Command::Set { table, key, value } => {
                                table_writes.entry(table.clone()).or_default().push((*key, Some(value.clone()), OpType::Put));
                            }
                            Command::Mset { table, entries } => {
                                for (k, v) in entries {
                                    table_writes.entry(table.clone()).or_default().push((*k, Some(v.clone()), OpType::Put));
                                }
                            }
                            Command::Delete { table, key } => {
                                table_writes.entry(table.clone()).or_default().push((*key, None, OpType::Delete));
                            }
                            _ => {}
                        }
                    }

                    // Apply atomic batch writes to storage engines
                    for (table_name, ops) in table_writes {
                        if let Some(tbl) = table_manager.get_table(&table_name) {
                            for (k, v, op) in &ops {
                                match op {
                                    OpType::Put => {
                                        if let Some(ref val) = v {
                                            tbl.index_manager.on_put(*k, val);
                                        }
                                    }
                                    OpType::Delete => {
                                        tbl.index_manager.on_delete(*k);
                                    }
                                }
                            }
                            let _ = tbl.engine.apply_batch(ops).await;
                        }
                    }

                    // Build array response
                    let mut resp = format!("*{}\r\n", queue.len()).into_bytes();
                    for queued_cmd in queue {
                        let item_resp = match queued_cmd {
                            Command::Set { .. } | Command::Mset { .. } => b"+OK\r\n".to_vec(),
                            Command::Delete { .. } => b":1\r\n".to_vec(),
                            other => execute_single_command(other, &table_manager).await,
                        };
                        resp.extend_from_slice(&item_resp);
                    }

                    writer.write_all(&resp).await?;
                    writer.flush().await?;
                    continue;
                }

                Command::Discard => {
                    in_multi = false;
                    multi_queue.clear();
                    writer.write_all(b"+OK\r\n").await?;
                    writer.flush().await?;
                    continue;
                }

                Command::Multi => {
                    writer.write_all(b"-ERR MULTI calls can not be nested\r\n").await?;
                    writer.flush().await?;
                    continue;
                }

                Command::Quit => {
                    writer.write_all(b"+OK\r\n").await?;
                    writer.flush().await?;
                    break;
                }

                other => {
                    multi_queue.push(other);
                    writer.write_all(b"+QUEUED\r\n").await?;
                    writer.flush().await?;
                    continue;
                }
            }
        }

        // Non-transaction command execution
        match cmd {
            Command::Multi => {
                in_multi = true;
                multi_queue.clear();
                writer.write_all(b"+OK\r\n").await?;
            }
            Command::Quit => {
                writer.write_all(b"+OK\r\n").await?;
                writer.flush().await?;
                break;
            }
            other => {
                let resp = execute_single_command(other, &table_manager).await;
                writer.write_all(&resp).await?;
            }
        }

        writer.flush().await?;
    }

    Ok(())
}
