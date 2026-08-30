use bytes::Bytes;
use crate::core::types::{DbError, PlayerId, Result};

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping,
    Auth { password: String },
    Tables,
    CreateTable { table: String },
    DropTable { table: String },
    TruncateTable { table: String },
    Get { table: String, key: PlayerId },
    Mget { table: String, keys: Vec<PlayerId> },
    Set { table: String, key: PlayerId, value: Bytes },
    JsonSet { table: String, key: PlayerId, path: String, value: String },
    Mset { table: String, entries: Vec<(PlayerId, Bytes)> },
    Delete { table: String, key: PlayerId },
    DelWhere { table: String, query: String },
    Exists { table: String, keys: Vec<PlayerId> },
    Expire { table: String, key: PlayerId, seconds: u64 },
    Ttl { table: String, key: PlayerId },
    IndexCreate { table: String, path: String },
    IndexList { table: String },
    Top { table: String, path: String, limit: usize },
    Rank { table: String, path: String, key: PlayerId },
    RankAroundKey { table: String, path: String, key: PlayerId, limit: usize },
    RankAroundScore { table: String, path: String, score: f64, limit: usize },
    RankScoreRange { table: String, path: String, min_score: f64, max_score: f64, limit: usize },
    RankRange { table: String, path: String, start_rank: usize, end_rank: usize },
    Scan { table: String, start_key: Option<PlayerId>, end_key: Option<PlayerId>, limit: usize },
    Filter { table: String, query: String, limit: usize },
    Count { table: String, query: Option<String> },
    CalcStats { table: String, field: String, query: Option<String> },
    Multi,
    Exec,
    Discard,
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
            "MULTI" => Ok(Command::Multi),
            "EXEC" => Ok(Command::Exec),
            "DISCARD" => Ok(Command::Discard),

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

            "DROP" => {
                let mut p = rest.split_whitespace();
                let first = p.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: DROP TABLE <name>".into())
                })?;
                let table_name = if first.eq_ignore_ascii_case("TABLE") {
                    p.next().ok_or_else(|| {
                        DbError::InvalidCommand("Usage: DROP TABLE <name>".into())
                    })?
                } else {
                    first
                };
                Ok(Command::DropTable { table: table_name.to_lowercase() })
            }

            "TRUNCATE" => {
                let mut p = rest.split_whitespace();
                let first = p.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: TRUNCATE TABLE <name>".into())
                })?;
                let table_name = if first.eq_ignore_ascii_case("TABLE") {
                    p.next().ok_or_else(|| {
                        DbError::InvalidCommand("Usage: TRUNCATE TABLE <name>".into())
                    })?
                } else {
                    first
                };
                Ok(Command::TruncateTable { table: table_name.to_lowercase() })
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

            "RANK.KEY" | "AROUND_KEY" => {
                let mut parts = rest.split_whitespace();
                let table = parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: RANK.KEY <table> <json.path> <UUID/key> [limit]".into())
                })?.to_lowercase();
                let path = parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: RANK.KEY <table> <json.path> <UUID/key> [limit]".into())
                })?.to_string();
                let key_str = parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: RANK.KEY <table> <json.path> <UUID/key> [limit]".into())
                })?;
                let key = PlayerId::parse(key_str)?;
                let limit = parts.next().and_then(|s| s.parse::<usize>().ok()).unwrap_or(10).min(1000);

                Ok(Command::RankAroundKey { table, path, key, limit })
            }

            "RANK.SCORE" | "AROUND" | "AROUND_SCORE" => {
                let mut parts = rest.split_whitespace();
                let table = parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: RANK.SCORE <table> <json.path> <score> [limit]".into())
                })?.to_lowercase();
                let path = parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: RANK.SCORE <table> <json.path> <score> [limit]".into())
                })?.to_string();
                let score_str = parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: RANK.SCORE <table> <json.path> <score> [limit]".into())
                })?;
                let score = score_str.parse::<f64>().map_err(|_| {
                    DbError::InvalidCommand("Invalid score number".into())
                })?;
                let limit = parts.next().and_then(|s| s.parse::<usize>().ok()).unwrap_or(10).min(1000);

                Ok(Command::RankAroundScore { table, path, score, limit })
            }

            "RANK.RANGE_SCORE" | "SCORE_RANGE" => {
                let mut parts = rest.split_whitespace();
                let table = parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: RANK.RANGE_SCORE <table> <json.path> <min_score> <max_score> [limit]".into())
                })?.to_lowercase();
                let path = parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: RANK.RANGE_SCORE <table> <json.path> <min_score> <max_score> [limit]".into())
                })?.to_string();
                let min_str = parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: RANK.RANGE_SCORE <table> <json.path> <min_score> <max_score> [limit]".into())
                })?;
                let max_str = parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: RANK.RANGE_SCORE <table> <json.path> <min_score> <max_score> [limit]".into())
                })?;
                let min_score = min_str.parse::<f64>().map_err(|_| DbError::InvalidCommand("Invalid min_score number".into()))?;
                let max_score = max_str.parse::<f64>().map_err(|_| DbError::InvalidCommand("Invalid max_score number".into()))?;
                let limit = parts.next().and_then(|s| s.parse::<usize>().ok()).unwrap_or(50).min(1000);

                Ok(Command::RankScoreRange { table, path, min_score, max_score, limit })
            }

            "RANK.RANGE" | "RANK_RANGE" => {
                let mut parts = rest.split_whitespace();
                let table = parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: RANK.RANGE <table> <json.path> <start_rank> <end_rank>".into())
                })?.to_lowercase();
                let path = parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: RANK.RANGE <table> <json.path> <start_rank> <end_rank>".into())
                })?.to_string();
                let start_str = parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: RANK.RANGE <table> <json.path> <start_rank> <end_rank>".into())
                })?;
                let end_str = parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: RANK.RANGE <table> <json.path> <start_rank> <end_rank>".into())
                })?;
                let start_rank = start_str.parse::<usize>().map_err(|_| DbError::InvalidCommand("Invalid start_rank integer".into()))?;
                let end_rank = end_str.parse::<usize>().map_err(|_| DbError::InvalidCommand("Invalid end_rank integer".into()))?;

                Ok(Command::RankRange { table, path, start_rank, end_rank })
            }

            "SCAN" | "RANGE" => {
                let mut scan_parts = rest.split_whitespace();
                let table = scan_parts.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: SCAN <table> [start_uuid] [end_uuid] [limit]".into())
                })?.to_lowercase();

                let p1 = scan_parts.next();
                let p2 = scan_parts.next();
                let p3 = scan_parts.next();

                let mut start_key = None;
                let mut end_key = None;
                let mut limit = 100;

                match (p1, p2, p3) {
                    (Some(a), Some(b), Some(c)) => {
                        if a != "-" && a != "min" {
                            start_key = Some(PlayerId::parse(a)?);
                        }
                        if b != "+" && b != "max" {
                            end_key = Some(PlayerId::parse(b)?);
                        }
                        limit = c.parse::<usize>().unwrap_or(100);
                    }
                    (Some(a), Some(b), None) => {
                        if let Ok(l) = b.parse::<usize>() {
                            if a != "-" && a != "min" {
                                start_key = Some(PlayerId::parse(a)?);
                            }
                            limit = l;
                        } else {
                            if a != "-" && a != "min" {
                                start_key = Some(PlayerId::parse(a)?);
                            }
                            if b != "+" && b != "max" {
                                end_key = Some(PlayerId::parse(b)?);
                            }
                        }
                    }
                    (Some(a), None, None) => {
                        if let Ok(l) = a.parse::<usize>() {
                            limit = l;
                        } else if a != "-" && a != "min" {
                            start_key = Some(PlayerId::parse(a)?);
                        }
                    }
                    _ => {}
                }

                Ok(Command::Scan {
                    table,
                    start_key,
                    end_key,
                    limit: limit.clamp(1, 10000),
                })
            }

            "FILTER" => {
                let mut filter_tokens = rest.splitn(2, ' ');
                let table = filter_tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: FILTER <table> \"<query>\" [limit]".into())
                })?.to_lowercase();
                let query_and_limit = filter_tokens.next().unwrap_or("").trim();

                if query_and_limit.is_empty() {
                    return Err(DbError::InvalidCommand("Usage: FILTER <table> \"<query>\" [limit]".into()));
                }

                let (query, limit) = if query_and_limit.starts_with('"') {
                    if let Some(end_idx) = query_and_limit[1..].find('"') {
                        let q = &query_and_limit[1..=end_idx];
                        let rem = query_and_limit[end_idx + 2..].trim();
                        let l = rem.parse::<usize>().unwrap_or(100);
                        (q.to_string(), l)
                    } else {
                        (query_and_limit.to_string(), 100)
                    }
                } else {
                    let parts: Vec<&str> = query_and_limit.split_whitespace().collect();
                    if parts.len() > 1 && parts.last().unwrap().parse::<usize>().is_ok() {
                        let l = parts.last().unwrap().parse::<usize>().unwrap();
                        let q = parts[..parts.len() - 1].join(" ");
                        (q, l)
                    } else {
                        (query_and_limit.to_string(), 100)
                    }
                };

                Ok(Command::Filter {
                    table,
                    query,
                    limit: limit.clamp(1, 10000),
                })
            }

            "JSON_SET" | "JSON.SET" => {
                let mut tokens = rest.splitn(4, ' ');
                let table = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: JSON_SET <table> <UUID> <path> <value>".into())
                })?.to_lowercase();
                let uuid_str = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: JSON_SET <table> <UUID> <path> <value>".into())
                })?;
                let path = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: JSON_SET <table> <UUID> <path> <value>".into())
                })?.to_string();
                let value = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: JSON_SET <table> <UUID> <path> <value>".into())
                })?.trim().to_string();

                let key = PlayerId::parse(uuid_str)?;
                Ok(Command::JsonSet { table, key, path, value })
            }

            "DEL_WHERE" | "DELETE_WHERE" => {
                let mut tokens = rest.splitn(2, ' ');
                let table = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: DEL_WHERE <table> \"<query>\"".into())
                })?.to_lowercase();
                let query = tokens.next().unwrap_or("").trim().trim_matches('"').to_string();
                if query.is_empty() {
                    return Err(DbError::InvalidCommand("Usage: DEL_WHERE <table> \"<query>\"".into()));
                }
                Ok(Command::DelWhere { table, query })
            }

            "COUNT" => {
                let mut tokens = rest.splitn(2, ' ');
                let table = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: COUNT <table> [\"<query>\"]".into())
                })?.to_lowercase();
                let query = tokens.next().map(|q| q.trim().trim_matches('"').to_string()).filter(|q| !q.is_empty());
                Ok(Command::Count { table, query })
            }

            "CALC_STATS" => {
                let mut tokens = rest.splitn(3, ' ');
                let table = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: CALC_STATS <table> <field> [\"<query>\"]".into())
                })?.to_lowercase();
                let field = tokens.next().ok_or_else(|| {
                    DbError::InvalidCommand("Usage: CALC_STATS <table> <field> [\"<query>\"]".into())
                })?.to_string();
                let query = tokens.next().map(|q| q.trim().trim_matches('"').to_string()).filter(|q| !q.is_empty());
                Ok(Command::CalcStats { table, field, query })
            }

            "BACKUP" | "SNAPSHOT" => {
                let target = if rest.is_empty() { None } else { Some(rest.to_string()) };
                Ok(Command::Backup { target_dir: target })
            }

            "STATS" => {
                let parts: Vec<&str> = rest.split_whitespace().collect();
                if parts.len() >= 2 {
                    let table = parts[0].to_lowercase();
                    let field = parts[1].to_string();
                    let query = if parts.len() > 2 {
                        Some(parts[2..].join(" ").trim_matches('"').to_string())
                    } else {
                        None
                    };
                    Ok(Command::CalcStats { table, field, query })
                } else {
                    let table = if rest.is_empty() {
                        None
                    } else {
                        Some(rest.to_lowercase())
                    };
                    Ok(Command::Stats { table })
                }
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
