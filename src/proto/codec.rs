use bytes::Bytes;
use crate::core::types::{DbError, PlayerId, Result};

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping,
    Auth { password: String },
    Tables,
    CreateTable { table: String },
    Get { table: String, key: PlayerId },
    Mget { table: String, keys: Vec<PlayerId> },
    Set { table: String, key: PlayerId, value: Bytes },
    Mset { table: String, entries: Vec<(PlayerId, Bytes)> },
    Delete { table: String, key: PlayerId },
    Exists { table: String, keys: Vec<PlayerId> },
    Expire { table: String, key: PlayerId, seconds: u64 },
    Ttl { table: String, key: PlayerId },
    IndexCreate { table: String, path: String },
    IndexList { table: String },
    Top { table: String, path: String, limit: usize },
    Rank { table: String, path: String, key: PlayerId },
    Stats { table: Option<String> },
    Flush { table: Option<String> },
    Backup { target_dir: Option<String> },
    Quit,
}

pub struct CommandParser;

impl CommandParser {
    pub fn parse(input: &str) -> Result<Command> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Err(DbError::InvalidCommand("Empty command".into()));
        }

        let mut parts = trimmed.splitn(2, ' ');
        let verb = parts.next().unwrap().to_uppercase();
        let rest = parts.next().unwrap_or("").trim();

        match verb.as_str() {
            "PING" => Ok(Command::Ping),
            "TABLES" => Ok(Command::Tables),
            "QUIT" | "EXIT" => Ok(Command::Quit),

            "AUTH" => {
                if rest.is_empty() {
                    return Err(DbError::InvalidCommand("Usage: AUTH <password>".into()));
                }
                Ok(Command::Auth { password: rest.to_string() })
            }

            "SHOW" => {
                if rest.eq_ignore_ascii_case("TABLES") {
                    Ok(Command::Tables)
                } else {
                    Err(DbError::InvalidCommand("Usage: SHOW TABLES".into()))
                }
            }

            "CREATE" => {
                let mut p = rest.split_whitespace();
                let sub = p.next().unwrap_or("").to_uppercase();
                if sub == "TABLE" {
                    let table_name = p.next().ok_or_else(|| {
                        DbError::InvalidCommand("Usage: CREATE TABLE <table_name>".into())
                    })?;
                    Ok(Command::CreateTable {
                        table: table_name.to_lowercase(),
                    })
                } else {
                    Err(DbError::InvalidCommand("Usage: CREATE TABLE <name>".into()))
                }
            }

            "GET" => {
                let mut tokens = rest.split_whitespace();
                let table = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: GET <table> <UUID>".into())
                })?.to_lowercase();
                let uuid_str = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: GET <table> <UUID>".into())
                })?;

                let key = PlayerId::parse(uuid_str)?;
                Ok(Command::Get { table, key })
            }

            "MGET" => {
                let mut tokens = rest.split_whitespace();
                let table = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: MGET <table> <UUID1> <UUID2> ...".into())
                })?.to_lowercase();

                let mut keys = Vec::new();
                for u in tokens {
                    keys.push(PlayerId::parse(u)?);
                }
                if keys.is_empty() {
                    return Err(DbError::InvalidCommand("Usage: MGET <table> <UUID1> <UUID2> ...".into()));
                }
                Ok(Command::Mget { table, keys })
            }

            "SET" => {
                let mut tokens = rest.splitn(3, ' ');
                let table = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: SET <table> <UUID> <JSON>".into())
                })?.to_lowercase();
                let uuid_str = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: SET <table> <UUID> <JSON>".into())
                })?;
                let val_str = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: SET <table> <UUID> <JSON>".into())
                })?;

                let key = PlayerId::parse(uuid_str)?;
                let value = Bytes::copy_from_slice(val_str.as_bytes());
                Ok(Command::Set { table, key, value })
            }

            "MSET" => {
                // Format: MSET <table> <UUID1> <JSON1> <UUID2> <JSON2> ...
                let mut p = rest.splitn(2, ' ');
                let table = p.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: MSET <table> <UUID1> <JSON1> <UUID2> <JSON2> ...".into())
                })?.to_lowercase();
                let body = p.next().unwrap_or("").trim();

                let mut entries = Vec::new();
                let mut chars = body.chars().peekable();

                while chars.peek().is_some() {
                    // Skip whitespace
                    while let Some(&c) = chars.peek() {
                        if c.is_whitespace() { chars.next(); } else { break; }
                    }
                    if chars.peek().is_none() { break; }

                    // Parse UUID token
                    let mut uuid_str = String::new();
                    while let Some(&c) = chars.peek() {
                        if !c.is_whitespace() {
                            uuid_str.push(c);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    let key = PlayerId::parse(&uuid_str)?;

                    // Skip whitespace
                    while let Some(&c) = chars.peek() {
                        if c.is_whitespace() { chars.next(); } else { break; }
                    }

                    // Parse JSON value (handles objects {...} or raw tokens)
                    let mut json_str = String::new();
                    if let Some(&'{') = chars.peek() {
                        let mut depth = 0;
                        while let Some(c) = chars.next() {
                            json_str.push(c);
                            if c == '{' { depth += 1; }
                            else if c == '}' {
                                depth -= 1;
                                if depth == 0 { break; }
                            }
                        }
                    } else {
                        while let Some(&c) = chars.peek() {
                            if !c.is_whitespace() {
                                json_str.push(c);
                                chars.next();
                            } else {
                                break;
                            }
                        }
                    }

                    if json_str.is_empty() {
                        return Err(DbError::InvalidCommand("Malformed MSET key-value pairs".into()));
                    }

                    entries.push((key, Bytes::copy_from_slice(json_str.as_bytes())));
                }

                if entries.is_empty() {
                    return Err(DbError::InvalidCommand("Usage: MSET <table> <UUID1> <JSON1> ...".into()));
                }

                Ok(Command::Mset { table, entries })
            }

            "DEL" | "DELETE" => {
                let mut tokens = rest.split_whitespace();
                let table = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: DEL <table> <UUID>".into())
                })?.to_lowercase();
                let uuid_str = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: DEL <table> <UUID>".into())
                })?;

                let key = PlayerId::parse(uuid_str)?;
                Ok(Command::Delete { table, key })
            }

            "EXISTS" => {
                let mut tokens = rest.split_whitespace();
                let table = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: EXISTS <table> <UUID1> [UUID2 ...]".into())
                })?.to_lowercase();

                let mut keys = Vec::new();
                for u in tokens {
                    keys.push(PlayerId::parse(u)?);
                }
                if keys.is_empty() {
                    return Err(DbError::InvalidCommand("Usage: EXISTS <table> <UUID1> [UUID2 ...]".into()));
                }
                Ok(Command::Exists { table, keys })
            }

            "EXPIRE" => {
                let mut tokens = rest.split_whitespace();
                let table = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: EXPIRE <table> <UUID> <seconds>".into())
                })?.to_lowercase();
                let uuid_str = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: EXPIRE <table> <UUID> <seconds>".into())
                })?;
                let sec_str = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: EXPIRE <table> <UUID> <seconds>".into())
                })?;

                let key = PlayerId::parse(uuid_str)?;
                let seconds = sec_str.parse::<u64>().map_err(|_| {
                    DbError::InvalidCommand("Invalid expire seconds".into())
                })?;

                Ok(Command::Expire { table, key, seconds })
            }

            "TTL" => {
                let mut tokens = rest.split_whitespace();
                let table = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: TTL <table> <UUID>".into())
                })?.to_lowercase();
                let uuid_str = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: TTL <table> <UUID>".into())
                })?;

                let key = PlayerId::parse(uuid_str)?;
                Ok(Command::Ttl { table, key })
            }

            "INDEX" => {
                let mut idx_parts = rest.split_whitespace();
                let sub_cmd = idx_parts.next().unwrap_or("").to_uppercase();
                match sub_cmd.as_str() {
                    "CREATE" | "ADD" => {
                        let table = idx_parts.next().ok_or_else(|| {
                            DbError::InvalidCommand("Usage: INDEX CREATE <table> <json.path>".into())
                        })?.to_lowercase();
                        let path = idx_parts.next().ok_or_else(|| {
                            DbError::InvalidCommand("Usage: INDEX CREATE <table> <json.path>".into())
                        })?.to_string();

                        Ok(Command::IndexCreate { table, path })
                    }
                    "LIST" => {
                        let table = idx_parts.next().ok_or_else(|| {
                            DbError::InvalidCommand("Usage: INDEX LIST <table>".into())
                        })?.to_lowercase();
                        Ok(Command::IndexList { table })
                    }
                    _ => Err(DbError::InvalidCommand("Usage: INDEX <CREATE|LIST> <table> [path]".into())),
                }
            }

            "TOP" => {
                let mut top_parts = rest.split_whitespace();
                let table = top_parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: TOP <table> <json.path> [limit]".into())
                })?.to_lowercase();
                let path = top_parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: TOP <table> <json.path> [limit]".into())
                })?.to_string();
                let limit = top_parts
                    .next()
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(10)
                    .min(1000);

                Ok(Command::Top { table, path, limit })
            }

            "RANK" => {
                let mut rank_parts = rest.split_whitespace();
                let table = rank_parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: RANK <table> <json.path> <UUID>".into())
                })?.to_lowercase();
                let path = rank_parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: RANK <table> <json.path> <UUID>".into())
                })?.to_string();
                let uuid_str = rank_parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: RANK <table> <json.path> <UUID>".into())
                })?;
                let key = PlayerId::parse(uuid_str)?;

                Ok(Command::Rank { table, path, key })
            }

            "BACKUP" => {
                let target = if rest.is_empty() { None } else { Some(rest.to_string()) };
                Ok(Command::Backup { target_dir: target })
            }

            "STATS" => {
                let table = if rest.is_empty() {
                    None
                } else {
                    Some(rest.to_lowercase())
                };
                Ok(Command::Stats { table })
            }

            "FLUSH" => {
                let table = if rest.is_empty() {
                    None
                } else {
                    Some(rest.to_lowercase())
                };
                Ok(Command::Flush { table })
            }

            _ => Err(DbError::InvalidCommand(format!(
                "Unknown command: '{}'",
                verb
            ))),
        }
    }
}
