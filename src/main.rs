use std::net::SocketAddr;
use std::path::PathBuf;
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

use meow_database::server::{HttpServer, Server};
use meow_database::storage::wal::WalConfig;
use meow_database::table::TableManager;

#[derive(Parser, Debug)]
#[command(name = "meowdb-server")]
#[command(about = "High-Performance UUID-Optimized LSM-Tree Database with Multi-Table, Secondary JSON Ranking, and Full Production Tooling", long_about = None)]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:7379")]
    bind: SocketAddr,

    #[arg(long, default_value = "0.0.0.0:7380")]
    http_bind: SocketAddr,

    #[arg(short, long, default_value = "./data")]
    data_dir: PathBuf,

    /// Optional password authentication
    #[arg(long)]
    require_pass: Option<String>,

    /// MemTable size in Megabytes (MB)
    #[arg(long, default_value_t = 256)]
    memtable_size_mb: usize,

    #[arg(long, default_value_t = 4)]
    compaction_trigger: usize,

    /// Microseconds window to batch concurrent writes into a single fsync (default: 1000us = 1ms)
    #[arg(long, default_value_t = 1000)]
    commit_delay_us: u64,

    /// Enable asynchronous periodic fsync instead of fsyncing every commit batch (default: false)
    #[arg(long, default_value_t = false)]
    async_fsync: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let memtable_bytes = args.memtable_size_mb * 1024 * 1024;

    info!("Starting MeowDB server...");
    info!("Data directory: {}", args.data_dir.display());
    info!("MemTable max size: {} MB ({} bytes)", args.memtable_size_mb, memtable_bytes);
    info!("Group commit delay window: {} µs", args.commit_delay_us);
    info!("Async fsync mode: {}", if args.async_fsync { "ENABLED (High-throughput periodic fsync)" } else { "DISABLED (100% strict durability fsync)" });
    if args.require_pass.is_some() {
        info!("Authentication required: ENABLED");
    }

    let wal_config = WalConfig {
        commit_delay_us: args.commit_delay_us,
        async_fsync: args.async_fsync,
    };

    let table_manager = TableManager::init(
        args.data_dir,
        memtable_bytes,
        args.compaction_trigger,
        wal_config,
    ).await?;

    // 1. Launch Web UI HTTP Server
    let http_server = HttpServer::new(args.http_bind, table_manager.clone(), args.require_pass.clone());
    tokio::spawn(async move {
        if let Err(e) = http_server.run().await {
            tracing::error!("HTTP Web UI Server error: {}", e);
        }
    });

    // 2. Launch Main TCP Server
    let tcp_server = Server::new(args.bind, table_manager, args.require_pass);
    tcp_server.run().await?;

    Ok(())
}
