use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

use crate::core::types::Result;
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

async fn handle_connection(
    stream: TcpStream,
    _peer_addr: SocketAddr,
    table_manager: Arc<TableManager>,
    require_pass: Option<String>,
) -> Result<()> {
    let (reader, writer) = stream.into_split();
    let mut lines = BufReader::with_capacity(64 * 1024, reader).lines();
    let mut writer = BufWriter::with_capacity(64 * 1024, writer);

    let mut is_authenticated = require_pass.is_none();

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

        // Check authentication
        if !is_authenticated {
            match &cmd {
                Command::Auth { password } => {
                    if let Some(ref pass) = require_pass {
                        if password == pass {
                            is_authenticated = true;
                            writer.write_all(b"+OK\r\n").await?;
                        } else {
                            writer.write_all(b"-ERR invalid password\r\n").await?;
                        }
                    } else {
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

        match cmd {
            Command::Ping => {
                writer.write_all(b"+PONG\r\n").await?;
            }

            Command::Auth { .. } => {
                writer.write_all(b"+OK\r\n").await?;
            }

            Command::Quit => {
                writer.write_all(b"+OK\r\n").await?;
                writer.flush().await?;
                break;
            }

            Command::Tables => {
                let tables = table_manager.list_tables();
                let json_res = serde_json::to_string(&tables).unwrap_or_else(|_| "[]".into());
                let resp = format!("${}\r\n{}\r\n", json_res.len(), json_res);
                writer.write_all(resp.as_bytes()).await?;
            }

            Command::CreateTable { table } => {
                match table_manager.create_table(&table).await {
                    Ok(_) => {
                        let resp = format!("+TABLE CREATED {}\r\n", table);
                        writer.write_all(resp.as_bytes()).await?;
                    }
                    Err(e) => {
                        let err_msg = format!("-ERR {}\r\n", e);
                        writer.write_all(err_msg.as_bytes()).await?;
                    }
                }
            }

            Command::Set { table, key, value } => {
                match table_manager.get_table(&table) {
                    Some(tbl) => {
                        tbl.index_manager.on_put(key, &value);
                        match tbl.engine.put(key, value).await {
                            Ok(_) => {
                                writer.write_all(b"+OK\r\n").await?;
                            }
                            Err(e) => {
                                let err_msg = format!("-ERR {}\r\n", e);
                                writer.write_all(err_msg.as_bytes()).await?;
                            }
                        }
                    }
                    None => {
                        let err_msg = format!("-ERR Table '{}' does not exist. Use 'CREATE TABLE {}'\r\n", table, table);
                        writer.write_all(err_msg.as_bytes()).await?;
                    }
                }
            }

            Command::Mset { table, entries } => {
                match table_manager.get_table(&table) {
                    Some(tbl) => {
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
                            writer.write_all(b"+OK\r\n").await?;
                        } else {
                            let err_msg = format!("-ERR {}\r\n", last_err);
                            writer.write_all(err_msg.as_bytes()).await?;
                        }
                    }
                    None => {
                        let err_msg = format!("-ERR Table '{}' does not exist. Use 'CREATE TABLE {}'\r\n", table, table);
                        writer.write_all(err_msg.as_bytes()).await?;
                    }
                }
            }

            Command::Get { table, key } => {
                match table_manager.get_table(&table) {
                    Some(tbl) => match tbl.engine.get(&key) {
                        Ok(Some(val)) => {
                            let resp_header = format!("${}\r\n", val.len());
                            writer.write_all(resp_header.as_bytes()).await?;
                            writer.write_all(&val).await?;
                            writer.write_all(b"\r\n").await?;
                        }
                        Ok(None) => {
                            writer.write_all(b"$-1\r\n").await?;
                        }
                        Err(e) => {
                            let err_msg = format!("-ERR {}\r\n", e);
                            writer.write_all(err_msg.as_bytes()).await?;
                        }
                    },
                    None => {
                        let err_msg = format!("-ERR Table '{}' does not exist\r\n", table);
                        writer.write_all(err_msg.as_bytes()).await?;
                    }
                }
            }

            Command::Mget { table, keys } => {
                match table_manager.get_table(&table) {
                    Some(tbl) => {
                        let header = format!("*{}\r\n", keys.len());
                        writer.write_all(header.as_bytes()).await?;
                        for key in keys {
                            match tbl.engine.get(&key) {
                                Ok(Some(val)) => {
                                    let item_header = format!("${}\r\n", val.len());
                                    writer.write_all(item_header.as_bytes()).await?;
                                    writer.write_all(&val).await?;
                                    writer.write_all(b"\r\n").await?;
                                }
                                _ => {
                                    writer.write_all(b"$-1\r\n").await?;
                                }
                            }
                        }
                    }
                    None => {
                        let err_msg = format!("-ERR Table '{}' does not exist\r\n", table);
                        writer.write_all(err_msg.as_bytes()).await?;
                    }
                }
            }

            Command::Delete { table, key } => {
                if let Some(tbl) = table_manager.get_table(&table) {
                    tbl.index_manager.on_delete(key);
                    let _ = tbl.engine.delete(key).await;
                    writer.write_all(b":1\r\n").await?;
                } else {
                    let err_msg = format!("-ERR Table '{}' does not exist\r\n", table);
                    writer.write_all(err_msg.as_bytes()).await?;
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
                    let resp = format!(":{}\r\n", count);
                    writer.write_all(resp.as_bytes()).await?;
                } else {
                    let err_msg = format!("-ERR Table '{}' does not exist\r\n", table);
                    writer.write_all(err_msg.as_bytes()).await?;
                }
            }

            Command::Expire { table, key, seconds } => {
                if let Some(tbl) = table_manager.get_table(&table) {
                    let ok = tbl.engine.set_expire(&key, seconds);
                    let resp = format!(":{}\r\n", if ok { 1 } else { 0 });
                    writer.write_all(resp.as_bytes()).await?;
                } else {
                    let err_msg = format!("-ERR Table '{}' does not exist\r\n", table);
                    writer.write_all(err_msg.as_bytes()).await?;
                }
            }

            Command::Ttl { table, key } => {
                if let Some(tbl) = table_manager.get_table(&table) {
                    let ttl = tbl.engine.get_ttl(&key);
                    let resp = format!(":{}\r\n", ttl);
                    writer.write_all(resp.as_bytes()).await?;
                } else {
                    let err_msg = format!("-ERR Table '{}' does not exist\r\n", table);
                    writer.write_all(err_msg.as_bytes()).await?;
                }
            }

            Command::IndexCreate { table, path } => {
                match table_manager.get_table(&table) {
                    Some(tbl) => {
                        tbl.index_manager.create_index(&path);
                        let resp = format!("+INDEX CREATED {}:{}\r\n", table, path);
                        writer.write_all(resp.as_bytes()).await?;
                    }
                    None => {
                        let err_msg = format!("-ERR Table '{}' does not exist\r\n", table);
                        writer.write_all(err_msg.as_bytes()).await?;
                    }
                }
            }

            Command::IndexList { table } => {
                if let Some(tbl) = table_manager.get_table(&table) {
                    let list = tbl.index_manager.list_indices();
                    let json_res = serde_json::to_string(&list).unwrap_or_else(|_| "[]".into());
                    let resp = format!("${}\r\n{}\r\n", json_res.len(), json_res);
                    writer.write_all(resp.as_bytes()).await?;
                } else {
                    writer.write_all(b"$2\r\n[]\r\n").await?;
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
                        let resp = format!("${}\r\n{}\r\n", json_res.len(), json_res);
                        writer.write_all(resp.as_bytes()).await?;
                    }
                    None => {
                        let err = format!("-ERR Index '{}:{}' not found.\r\n", table, path);
                        writer.write_all(err.as_bytes()).await?;
                    }
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
                            let resp = format!("${}\r\n{}\r\n", json_res.len(), json_res);
                            writer.write_all(resp.as_bytes()).await?;
                        }
                        None => {
                            writer.write_all(b"$-1\r\n").await?;
                        }
                    },
                    None => {
                        let err = format!("-ERR Index '{}:{}' not found.\r\n", table, path);
                        writer.write_all(err.as_bytes()).await?;
                    }
                }
            }

            Command::Backup { target_dir } => {
                match table_manager.backup_all(target_dir.as_deref()).await {
                    Ok(path) => {
                        let resp = format!("+BACKUP COMPLETED {}\r\n", path.display());
                        writer.write_all(resp.as_bytes()).await?;
                    }
                    Err(e) => {
                        let err_msg = format!("-ERR Backup failed: {}\r\n", e);
                        writer.write_all(err_msg.as_bytes()).await?;
                    }
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
                        let resp = format!("${}\r\n{}\r\n", json_res.len(), json_res);
                        writer.write_all(resp.as_bytes()).await?;
                    } else {
                        writer.write_all(b"-ERR Table not found\r\n").await?;
                    }
                } else {
                    let tables = table_manager.list_tables();
                    let stats = serde_json::json!({
                        "tables": tables,
                        "total_disk_size_bytes": table_manager.total_disk_size_bytes(),
                    });
                    let json_res = serde_json::to_string_pretty(&stats).unwrap();
                    let resp = format!("${}\r\n{}\r\n", json_res.len(), json_res);
                    writer.write_all(resp.as_bytes()).await?;
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
                writer.write_all(b"+OK\r\n").await?;
            }
        }

        writer.flush().await?;
    }

    Ok(())
}
