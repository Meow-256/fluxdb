use std::net::SocketAddr;
use std::path::PathBuf;
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

use fluxdb::server::{HttpServer, Server};
use fluxdb::storage::wal::WalConfig;
use fluxdb::table::TableManager;

#[derive(Parser, Debug)]
#[command(name = "fluxdb-server")]
#[command(about = "High-Performance UUID-Optimized LSM-Tree Database with Multi-Table, Secondary JSON Ranking, and Full Production Tooling", long_about = None)]
struct Args {
    /// Path to TOML configuration file (e.g. fluxdb.toml)
    #[arg(short = 'c', long)]
    config: Option<PathBuf>,

    #[arg(short, long, default_value = "0.0.0.0:7379")]
    bind: SocketAddr,

    #[arg(long, default_value = "0.0.0.0:7380")]
    http_bind: SocketAddr,

    #[arg(short, long, default_value = "./data")]
    data_dir: PathBuf,

    /// Optional password authentication
    #[arg(long)]
    require_pass: Option<String>,

    /// Maximum worker threads (0 for auto)
    #[arg(long)]
    max_threads: Option<usize>,

    /// LRU Block Cache capacity in Megabytes (0 = disabled)
    #[arg(long, default_value = "256")]
    block_cache_mb: usize,

    /// RAM limit for MemTable before SSTable flush (in MB)
    #[arg(long, default_value = "256")]
    memtable_size_mb: usize,

    /// Trigger compaction after N SSTables
    #[arg(long, default_value = "4")]
    compaction_trigger: usize,

    /// Group Commit delay window in microseconds
    #[arg(long, default_value = "1000")]
    commit_delay_us: u64,

    /// High-throughput asynchronous fsync mode
    #[arg(long)]
    async_fsync: bool,
}

#[derive(serde::Deserialize, Default)]
struct TomlConfigFile {
    server: Option<TomlServerConfig>,
    storage: Option<TomlStorageConfig>,
}

#[derive(serde::Deserialize, Default)]
struct TomlServerConfig {
    bind: Option<String>,
    http_bind: Option<String>,
    require_pass: Option<String>,
    max_threads: Option<usize>,
}

#[derive(serde::Deserialize, Default)]
struct TomlStorageConfig {
    data_dir: Option<String>,
    block_cache_mb: Option<usize>,
    memtable_size_mb: Option<usize>,
    compaction_trigger: Option<usize>,
    commit_delay_us: Option<u64>,
    async_fsync: Option<bool>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let mut args = Args::parse();

    // Check for TOML config file
    let config_path = args.config.clone().or_else(|| {
        let p = PathBuf::from("fluxdb.toml");
        if p.exists() { Some(p) } else { None }
    });

    if let Some(cfg_file) = config_path {
        if let Ok(content) = std::fs::read_to_string(&cfg_file) {
            info!("Loading configuration from {}", cfg_file.display());
            if let Ok(toml_cfg) = toml::from_str::<TomlConfigFile>(&content) {
                if let Some(s) = toml_cfg.server {
                    if let Some(b) = s.bind { if let Ok(addr) = b.parse() { args.bind = addr; } }
                    if let Some(hb) = s.http_bind { if let Ok(addr) = hb.parse() { args.http_bind = addr; } }
                    if let Some(p) = s.require_pass { args.require_pass = Some(p); }
                    if let Some(t) = s.max_threads { args.max_threads = Some(t); }
                }
                if let Some(st) = toml_cfg.storage {
                    if let Some(d) = st.data_dir { args.data_dir = PathBuf::from(d); }
                    if let Some(c) = st.block_cache_mb { args.block_cache_mb = c; }
                    if let Some(m) = st.memtable_size_mb { args.memtable_size_mb = m; }
                    if let Some(ct) = st.compaction_trigger { args.compaction_trigger = ct; }
                    if let Some(cd) = st.commit_delay_us { args.commit_delay_us = cd; }
                    if let Some(af) = st.async_fsync { args.async_fsync = af; }
                }
            }
        }
    }
    let memtable_bytes = args.memtable_size_mb * 1024 * 1024;
    let default_threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let threads = args.max_threads.unwrap_or(default_threads);

    info!("Starting FluxDB server...");
    info!("Data directory: {}", args.data_dir.display());
    info!("Max worker threads: {}", threads);
    info!("MemTable max size: {} MB ({} bytes)", args.memtable_size_mb, memtable_bytes);
    info!("Block cache size: {} MB", args.block_cache_mb);
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

    let mut current_conf = table_manager.get_config();
    current_conf.worker_threads = threads;
    current_conf.block_cache_mb = args.block_cache_mb;
    current_conf.auth_password = args.require_pass.clone();
    table_manager.update_config(current_conf);

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
