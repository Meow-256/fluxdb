use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use clap::Parser;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader, BufWriter as TokioBufWriter};
use tokio::net::TcpStream;

#[derive(Parser, Debug)]
#[command(name = "fluxdb-load")]
#[command(about = "High-speed multi-threaded bulk importer from NDJSON into FluxDB")]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:7379")]
    server: String,

    #[arg(short = 't', long, default_value = "players")]
    table: String,

    #[arg(short, long)]
    input: PathBuf,

    #[arg(short = 'c', long, default_value_t = 64)]
    concurrency: usize,

    #[arg(long)]
    auth: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("Reading input file {:?}...", args.input);
    let file = File::open(&args.input)?;
    let reader = BufReader::new(file);

    let mut lines = Vec::new();
    for line in reader.lines() {
        let l = line?;
        if !l.trim().is_empty() {
            lines.push(l);
        }
    }
    let total_lines = lines.len();
    println!("Loaded {} records from file. Starting import with {} workers...", total_lines, args.concurrency);

    let lines_arc = Arc::new(lines);
    let completed = Arc::new(AtomicUsize::new(0));
    let chunk_size = (total_lines + args.concurrency - 1) / args.concurrency;

    let start_time = Instant::now();
    let mut handles = Vec::new();

    for worker_id in 0..args.concurrency {
        let server = args.server.clone();
        let table = args.table.clone();
        let auth = args.auth.clone();
        let lines = lines_arc.clone();
        let completed = completed.clone();

        let start_idx = worker_id * chunk_size;
        let end_idx = (start_idx + chunk_size).min(total_lines);

        if start_idx >= total_lines {
            break;
        }

        let handle = tokio::spawn(async move {
            let stream = match TcpStream::connect(&server).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Worker {} failed to connect: {}", worker_id, e);
                    return;
                }
            };
            let _ = stream.set_nodelay(true);
            let (reader, writer) = stream.into_split();
            let mut reader = TokioBufReader::new(reader);
            let mut writer = TokioBufWriter::with_capacity(64 * 1024, writer);

            if let Some(ref pass) = auth {
                let _ = writer.write_all(format!("AUTH {}\r\n", pass).as_bytes()).await;
                let _ = writer.flush().await;
                let mut line = String::new();
                let _ = reader.read_line(&mut line).await;
            }

            for i in start_idx..end_idx {
                let raw_line = &lines[i];
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(raw_line) {
                    let uuid = v.get("uuid").and_then(|u| u.as_str()).unwrap_or("");
                    let data = if let Some(d) = v.get("data") {
                        serde_json::to_string(d).unwrap_or_else(|_| "{}".into())
                    } else {
                        raw_line.clone()
                    };

                    let cmd = format!("SET {} {} {}\r\n", table, uuid, data);
                    let _ = writer.write_all(cmd.as_bytes()).await;
                    let _ = writer.flush().await;

                    let mut resp_line = String::new();
                    let _ = reader.read_line(&mut resp_line).await;
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.await;
    }

    let elapsed = start_time.elapsed();
    let total_done = completed.load(Ordering::Relaxed);
    let qps = total_done as f64 / elapsed.as_secs_f64();

    println!("Import completed: {} / {} records in {:.2?}", total_done, total_lines, elapsed);
    println!("Throughput: {:.2} records/sec", qps);

    Ok(())
}
