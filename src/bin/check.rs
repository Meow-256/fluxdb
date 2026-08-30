use std::fs;
use std::path::PathBuf;
use clap::Parser;
use meow_database::storage::sstable::SsTable;
use meow_database::storage::wal::WalRecovery;

#[derive(Parser, Debug)]
#[command(name = "meowdb-check")]
#[command(about = "Integrity check and diagnostics utility for MeowDB data files")]
struct Args {
    #[arg(short, long, default_value = "./data")]
    data_dir: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    println!("============================================================");
    println!("  MeowDB Database Integrity & Health Diagnostics Tool");
    println!("============================================================");
    println!("Target data directory: {}", args.data_dir.display());

    let tables_dir = args.data_dir.join("tables");
    if !tables_dir.exists() {
        println!("No tables directory found at {}", tables_dir.display());
        return Ok(());
    }

    let mut total_tables = 0;
    let mut total_sstables = 0;
    let mut total_errors = 0;

    if let Ok(entries) = fs::read_dir(&tables_dir) {
        for entry in entries.flatten() {
            if entry.file_type().map_or(false, |t| t.is_dir()) {
                let table_name = entry.file_name().to_string_lossy().to_string();
                let table_dir = entry.path();
                total_tables += 1;

                println!("\n--- Inspecting Table: '{}' ---", table_name);

                // 1. Check WAL integrity
                let wal_path = table_dir.join("wal.log");
                if wal_path.exists() {
                    match WalRecovery::recover(&wal_path) {
                        Ok((entries, last_seq)) => {
                            println!("  [WAL Check] ✓ OK (Records: {}, Last SeqNum: {})", entries.len(), last_seq);
                        }
                        Err(e) => {
                            eprintln!("  [WAL Check] ✗ CORRUPTION DETECTED: {}", e);
                            total_errors += 1;
                        }
                    }
                } else {
                    println!("  [WAL Check] - No WAL file present (cleanly flushed)");
                }

                // 2. Check SSTables
                if let Ok(sst_entries) = fs::read_dir(&table_dir) {
                    for sst_file in sst_entries.flatten() {
                        let path = sst_file.path();
                        if path.extension().map_or(false, |ext| ext == "sst") {
                            total_sstables += 1;
                            let fname = path.file_name().unwrap_or_default().to_string_lossy();
                            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                                if let Ok(id) = stem.parse::<u64>() {
                                    match SsTable::open(&path, id, 0) {
                                        Ok(sst) => {
                                            match sst.read_all_entries() {
                                                Ok(records) => {
                                                    println!("  [SSTable {}] ✓ OK (Records: {}, Level: {}, V3 Compressed)", fname, records.len(), sst.level());
                                                }
                                                Err(e) => {
                                                    eprintln!("  [SSTable {}] ✗ CORRUPTION: Failed reading blocks: {}", fname, e);
                                                    total_errors += 1;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!("  [SSTable {}] ✗ CORRUPTION: Failed to open: {}", fname, e);
                                            total_errors += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    println!("\n============================================================");
    println!("  Summary: Checked {} tables, {} SSTables", total_tables, total_sstables);
    if total_errors == 0 {
        println!("  Result: 100% HEALTHY (Zero corruption detected) ✓");
    } else {
        println!("  Result: Found {} corrupted file(s) ✗", total_errors);
    }
    println!("============================================================");

    Ok(())
}
