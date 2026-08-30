use std::io::{self, Write};
use clap::Parser;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[derive(Parser, Debug)]
#[command(name = "fluxdb-cli")]
#[command(about = "Interactive CLI client for FluxDB (Full Feature Support)")]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:7379")]
    server: String,

    #[arg(short = 'a', long)]
    auth: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!("Connecting to FluxDB at {}...", args.server);

    let stream = match TcpStream::connect(&args.server).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to {}: {}", args.server, e);
            return Ok(());
        }
    };

    let (reader, mut writer) = stream.into_split();
    let mut server_lines = BufReader::new(reader);

    // Auto-authenticate if flag passed
    if let Some(pass) = args.auth {
        writer.write_all(format!("AUTH {}\r\n", pass).as_bytes()).await?;
        writer.flush().await?;
        let mut resp = String::new();
        server_lines.read_line(&mut resp).await?;
        if !resp.starts_with("+OK") {
            eprintln!("Authentication failed: {}", resp.trim());
        } else {
            println!("Authenticated successfully.");
        }
    }

    println!("Connected to FluxDB! Type HELP or commands like:");
    println!("  TABLES                                     - List tables");
    println!("  CREATE TABLE <name>                        - Create table");
    println!("  SET <table> <uuid> <json_data>             - Insert single record");
    println!("  MSET <table> <uuid1> <json1> <uuid2> ...   - Insert multiple records");
    println!("  GET <table> <uuid>                         - Fetch record");
    println!("  MGET <table> <uuid1> <uuid2> ...           - Fetch multiple records");
    println!("  SCAN <table> [start] [end] [limit]         - Range scan sorted records");
    println!("  FILTER <table> \"<query>\" [limit]            - Filter JSON records by expression");
    println!("  MULTI / EXEC / DISCARD                     - Atomic transactions");
    println!("  EXISTS <table> <uuid1> ...                 - Check existence");
    println!("  EXPIRE <table> <uuid> <seconds>            - Set TTL");
    println!("  TTL <table> <uuid>                         - Check remaining TTL");
    println!("  BACKUP [dir]                               - Backup snapshot");
    println!("  TOP <table> <json.path> [limit]            - View leaderboard");
    println!("  QUIT\n");

    let stdin = io::stdin();
    loop {
        print!("fluxdb> ");
        io::stdout().flush()?;

        let mut input = String::new();
        if stdin.read_line(&mut input)? == 0 {
            break;
        }

        let trimmed = input.trim();
        if trimmed.is_empty() {
            continue;
        }

        if trimmed.eq_ignore_ascii_case("help") {
            println!("Available commands:");
            println!("  AUTH <password>                            - Authenticate with server");
            println!("  TABLES                                     - List all tables");
            println!("  CREATE TABLE <name>                        - Create a new table");
            println!("  DROP TABLE <name>                          - Drop table and delete all its files");
            println!("  TRUNCATE TABLE <name>                      - Truncate all records from table");
            println!("  SET <table> <uuid> <json>                  - Insert or update full record");
            println!("  JSON_SET <table> <uuid> <path> <value>     - Partial update specific JSON property");
            println!("  MSET <table> <uuid1> <json1> ...           - Batch insert records");
            println!("  GET <table> <uuid>                         - Point lookup by UUID");
            println!("  MGET <table> <uuid1> <uuid2> ...           - Batch lookup multiple UUIDs");
            println!("  SCAN <table> [start] [end] [limit]         - Range scan sorted records");
            println!("  FILTER <table> \"<query>\" [limit]            - Filter JSON records by expression");
            println!("  COUNT <table> [\"<query>\"]                  - Count matching or total records");
            println!("  STATS <table> <field> [\"<query>\"]          - Statistical metrics (sum, avg, min, max)");
            println!("  DEL_WHERE <table> \"<query>\"                - Conditionally batch delete records");
            println!("  MULTI                                      - Begin transaction block");
            println!("  EXEC                                       - Execute queued transaction block");
            println!("  DISCARD                                    - Cancel queued transaction block");
            println!("  DEL <table> <uuid>                         - Delete single record");
            println!("  EXISTS <table> <uuid1> [uuid2 ...]         - Check key existence count");
            println!("  EXPIRE <table> <uuid> <seconds>            - Set expiration on record");
            println!("  TTL <table> <uuid>                         - Get remaining time to live");
            println!("  BACKUP [target_dir]                        - Create instant snapshot hot backup");
            println!("  INDEX CREATE <table> <path>                - Create ranking index on JSON field");
            println!("  TOP <table> <path> [limit]                 - Get top N ranked entries");
            println!("  RANK <table> <path> <uuid>                 - Get rank and score of UUID");
            println!("  STATS [table]                              - View storage statistics");
            println!("  FLUSH [table]                              - Flush MemTable to SSTable");
            println!("  QUIT / EXIT                                - Exit CLI");
            continue;
        }

        writer.write_all(format!("{}\r\n", trimmed).as_bytes()).await?;
        writer.flush().await?;

        if trimmed.eq_ignore_ascii_case("quit") || trimmed.eq_ignore_ascii_case("exit") {
            break;
        }

        let mut line = String::new();
        server_lines.read_line(&mut line).await?;

        if line.starts_with('+') || line.starts_with('-') || line.starts_with(':') {
            print!("{}", line);
        } else if line.starts_with('$') {
            let len_str = line[1..].trim();
            if len_str == "-1" {
                println!("(nil)");
            } else if let Ok(len) = len_str.parse::<usize>() {
                let mut data_buf = vec![0u8; len];
                server_lines.read_exact(&mut data_buf).await?;
                let mut crlf = [0u8; 2];
                let _ = server_lines.read_exact(&mut crlf).await;

                if let Ok(json_val) = serde_json::from_slice::<serde_json::Value>(&data_buf) {
                    println!("{}", serde_json::to_string_pretty(&json_val).unwrap());
                } else {
                    println!("{}", String::from_utf8_lossy(&data_buf));
                }
            }
        } else if line.starts_with('*') {
            // RESP Array (e.g. from MGET or EXEC)
            if let Ok(count) = line[1..].trim().parse::<usize>() {
                println!("Array [{} items]:", count);
                for i in 0..count {
                    let mut item_line = String::new();
                    server_lines.read_line(&mut item_line).await?;
                    if item_line.starts_with('$') {
                        let len_str = item_line[1..].trim();
                        if len_str == "-1" {
                            println!("  {}) (nil)", i + 1);
                        } else if let Ok(len) = len_str.parse::<usize>() {
                            let mut buf = vec![0u8; len];
                            server_lines.read_exact(&mut buf).await?;
                            let mut crlf = [0u8; 2];
                            let _ = server_lines.read_exact(&mut crlf).await;
                            println!("  {}) {}", i + 1, String::from_utf8_lossy(&buf));
                        }
                    } else {
                        println!("  {}) {}", i + 1, item_line.trim());
                    }
                }
            }
        } else {
            print!("{}", line);
        }
    }

    Ok(())
}
