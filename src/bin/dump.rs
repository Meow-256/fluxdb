use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use clap::Parser;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[derive(Parser, Debug)]
#[command(name = "fluxdb-dump")]
#[command(about = "Export FluxDB tables into NDJSON format")]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:7379")]
    server: String,

    #[arg(short = 't', long, default_value = "players")]
    table: String,

    #[arg(short, long, default_value = "dump.ndjson")]
    output: PathBuf,

    #[arg(long)]
    auth: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("Connecting to FluxDB server at {}...", args.server);
    let stream = TcpStream::connect(&args.server).await?;
    stream.set_nodelay(true)?;

    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    if let Some(ref pass) = args.auth {
        writer.write_all(format!("AUTH {}\r\n", pass).as_bytes()).await?;
        writer.flush().await?;
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        if line.starts_with('-') {
            eprintln!("Authentication error: {}", line.trim());
            return Ok(());
        }
    }

    println!("Exporting table '{}' to {:?}...", args.table, args.output);
    let scan_cmd = format!("SCAN {} 0 10000000\r\n", args.table);
    writer.write_all(scan_cmd.as_bytes()).await?;
    writer.flush().await?;

    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let trimmed = line.trim();

    if !trimmed.starts_with('$') {
        eprintln!("Error from server: {}", trimmed);
        return Ok(());
    }

    let len: usize = trimmed[1..].parse()?;
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;

    let json_array: Vec<serde_json::Value> = serde_json::from_slice(&payload)?;
    println!("Fetched {} records. Writing to file...", json_array.len());

    let file = File::create(&args.output)?;
    let mut buf = BufWriter::new(file);

    for item in &json_array {
        let line = serde_json::to_string(item)?;
        buf.write_all(line.as_bytes())?;
        buf.write_all(b"\n")?;
    }
    buf.flush()?;

    println!("Dump completed successfully! Total records: {}", json_array.len());
    Ok(())
}
