use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use parking_lot::RwLock;
use tokio::sync::Notify;
use tracing::{error, info};

use crate::core::types::{OpType, PlayerId, Result, ValueEntry};
use crate::storage::compaction::Compactor;
use crate::storage::memtable::MemTable;
use crate::storage::sstable::{SsTable, SsTableBuilder};
use crate::storage::wal::{WalConfig, WalRecovery, WalWriter};

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub db_path: PathBuf,
    pub memtable_max_bytes: usize,
    pub l0_compaction_trigger: usize,
    pub wal_config: WalConfig,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            db_path: PathBuf::from("./data"),
            memtable_max_bytes: 256 * 1024 * 1024, // 256 MB default
            l0_compaction_trigger: 4,
            wal_config: WalConfig::default(),
        }
    }
}

pub struct StorageEngine {
    config: EngineConfig,
    wal: Arc<WalWriter>,
    memtable: Arc<RwLock<MemTable>>,
    imm_memtables: Arc<RwLock<Vec<Arc<MemTable>>>>,
    sstables: Arc<RwLock<Vec<Arc<SsTable>>>>, // Ordered newest to oldest
    next_sst_id: AtomicU64,
    flush_notify: Arc<Notify>,
    ttls: Arc<RwLock<HashMap<PlayerId, u64>>>,
}

impl StorageEngine {
    pub async fn open(config: EngineConfig) -> Result<Arc<Self>> {
        fs::create_dir_all(&config.db_path)?;

        // 1. Recover existing SSTables
        let mut existing_sstables = Vec::new();
        let mut max_sst_id = 0;

        if let Ok(entries) = fs::read_dir(&config.db_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "sst") {
                    if let Some(file_stem) = path.file_stem().and_then(|s| s.to_str()) {
                        if let Ok(id) = file_stem.parse::<u64>() {
                            max_sst_id = max_sst_id.max(id);
                            match SsTable::open(&path, id, 0) {
                                Ok(sst) => existing_sstables.push(Arc::new(sst)),
                                Err(e) => error!("Failed to open SSTable {:?}: {}", path, e),
                            }
                        }
                    }
                }
            }
        }

        // Sort SSTables newest to oldest (higher ID = newer)
        existing_sstables.sort_by(|a, b| b.id().cmp(&a.id()));

        // 2. Recover from WAL log
        let wal_path = config.db_path.join("wal.log");
        let (recovered_entries, max_recovered_seq) = WalRecovery::recover(&wal_path)?;

        let memtable = MemTable::new();
        if !recovered_entries.is_empty() {
            info!("Recovered {} entries from WAL", recovered_entries.len());
            for (key, val) in recovered_entries {
                memtable.insert(key, val);
            }
        }

        let wal = WalWriter::open(
            &wal_path,
            max_recovered_seq + 1,
            config.wal_config.clone(),
        )?;

        let engine = Arc::new(Self {
            config,
            wal: Arc::new(wal),
            memtable: Arc::new(RwLock::new(memtable)),
            imm_memtables: Arc::new(RwLock::new(Vec::new())),
            sstables: Arc::new(RwLock::new(existing_sstables)),
            next_sst_id: AtomicU64::new(max_sst_id + 1),
            flush_notify: Arc::new(Notify::new()),
            ttls: Arc::new(RwLock::new(HashMap::new())),
        });

        // 3. Start background flush & compaction task
        let engine_clone = engine.clone();
        tokio::spawn(async move {
            engine_clone.background_worker().await;
        });

        Ok(engine)
    }

    fn is_expired(&self, key: &PlayerId) -> bool {
        if let Some(&expires_at) = self.ttls.read().get(key) {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            if now >= expires_at {
                return true;
            }
        }
        false
    }

    pub fn set_expire(&self, key: &PlayerId, seconds: u64) -> bool {
        if !self.exists(key).unwrap_or(false) {
            return false;
        }
        let expires_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + seconds;
        self.ttls.write().insert(*key, expires_at);
        true
    }

    pub fn get_ttl(&self, key: &PlayerId) -> i64 {
        if !self.exists(key).unwrap_or(false) {
            return -2; // Key does not exist
        }
        if let Some(&expires_at) = self.ttls.read().get(key) {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            if now >= expires_at {
                return -2; // Expired
            } else {
                return (expires_at - now) as i64;
            }
        }
        -1 // No expire set
    }

    pub fn exists(&self, key: &PlayerId) -> Result<bool> {
        if self.is_expired(key) {
            return Ok(false);
        }

        // 1. Check active MemTable
        if let Some(entry) = self.memtable.read().get(key) {
            return Ok(entry.value.is_some());
        }

        // 2. Check immutable MemTables
        {
            let imm_tables = self.imm_memtables.read();
            for imm in imm_tables.iter().rev() {
                if let Some(entry) = imm.get(key) {
                    return Ok(entry.value.is_some());
                }
            }
        }

        // 3. Check SSTables
        let sst_list = self.sstables.read().clone();
        for sstable in sst_list.iter() {
            if let Some(val) = sstable.get(key)? {
                return Ok(val.value.is_some());
            }
        }

        Ok(false)
    }

    /// Point lookup by UUID
    #[inline]
    pub fn get(&self, key: &PlayerId) -> Result<Option<Bytes>> {
        if self.is_expired(key) {
            return Ok(None);
        }

        // 1. Check active MemTable (fast lock-free read)
        if let Some(entry) = self.memtable.read().get(key) {
            return Ok(entry.value);
        }

        // 2. Check immutable MemTables (waiting to be flushed)
        {
            let imm_tables = self.imm_memtables.read().clone();
            for imm in imm_tables.iter().rev() {
                if let Some(entry) = imm.get(key) {
                    return Ok(entry.value);
                }
            }
        }

        // 3. Check SSTables on disk (newest to oldest)
        let sst_list = self.sstables.read().clone();
        for sstable in sst_list.iter() {
            if let Some(entry) = sstable.get(key)? {
                return Ok(entry.value);
            }
        }

        Ok(None)
    }

    /// Insert or update record with Group-Committed WAL
    #[inline]
    pub async fn put(&self, key: PlayerId, value: Bytes) -> Result<()> {
        let (seq, ts) = self
            .wal
            .append_batch(vec![(key, Some(value.clone()), OpType::Put)])
            .await?;

        let should_flush = {
            let memtable = self.memtable.read();
            memtable.insert(
                key,
                ValueEntry {
                    value: Some(value),
                    seq_num: seq,
                    timestamp: ts,
                },
            );
            memtable.size_bytes() >= self.config.memtable_max_bytes
        };

        if should_flush {
            self.rotate_memtable().await?;
        }

        Ok(())
    }

    /// Mark record as deleted (Tombstone)
    #[inline]
    pub async fn delete(&self, key: PlayerId) -> Result<()> {
        let (seq, ts) = self
            .wal
            .append_batch(vec![(key, None, OpType::Delete)])
            .await?;

        let should_flush = {
            let memtable = self.memtable.read();
            memtable.insert(
                key,
                ValueEntry {
                    value: None,
                    seq_num: seq,
                    timestamp: ts,
                },
            );
            memtable.size_bytes() >= self.config.memtable_max_bytes
        };

        if should_flush {
            self.rotate_memtable().await?;
        }

        Ok(())
    }

    /// Force immediate flush of active MemTable
    pub async fn force_flush(&self) -> Result<()> {
        self.rotate_memtable().await?;
        self.flush_notify.notify_waiters();
        Ok(())
    }

    /// Rotate active MemTable to immutable list and create fresh active MemTable
    async fn rotate_memtable(&self) -> Result<()> {
        let mut mem_guard = self.memtable.write();
        if mem_guard.len() == 0 {
            return Ok(());
        }

        let old_memtable = std::mem::replace(&mut *mem_guard, MemTable::new());
        let old_arc = Arc::new(old_memtable);

        self.imm_memtables.write().push(old_arc);
        self.flush_notify.notify_one();
        Ok(())
    }

    /// Background worker for MemTable flush and SSTable Compaction
    async fn background_worker(&self) {
        loop {
            // 1. Flush any immutable MemTables to L0 SSTable
            while let Some(imm) = {
                let mut imm_guard = self.imm_memtables.write();
                if !imm_guard.is_empty() {
                    Some(imm_guard.remove(0))
                } else {
                    None
                }
            } {
                if let Err(e) = self.flush_memtable_to_sstable(&imm).await {
                    error!("Error flushing MemTable to SSTable: {}", e);
                }
            }

            // 2. Check if Compaction is needed
            let sst_count = self.sstables.read().len();
            if sst_count >= self.config.l0_compaction_trigger {
                if let Err(e) = self.run_compaction().await {
                    error!("Error during background compaction: {}", e);
                }
            }

            tokio::select! {
                _ = self.flush_notify.notified() => {},
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(200)) => {}
            }
        }
    }

    /// Flush MemTable to a new SSTable file
    async fn flush_memtable_to_sstable(&self, memtable: &MemTable) -> Result<()> {
        if memtable.len() == 0 {
            return Ok(());
        }

        let sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let sst_path = self.config.db_path.join(format!("{:06}.sst", sst_id));

        let builder = SsTableBuilder::new(&sst_path, sst_id, 0);
        if let Some(sstable) = builder.build(memtable.iter())? {
            let mut sst_guard = self.sstables.write();
            sst_guard.insert(0, Arc::new(sstable)); // Insert as newest
        }

        Ok(())
    }

    /// Trigger multi-way merge compaction of existing SSTables
    async fn run_compaction(&self) -> Result<()> {
        let to_compact = {
            let sst_guard = self.sstables.read();
            if sst_guard.len() < self.config.l0_compaction_trigger {
                return Ok(());
            }
            sst_guard.clone()
        };

        let new_sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);

        if let Some(merged_sst) = Compactor::compact(
            &self.config.db_path,
            &to_compact,
            new_sst_id,
            1,
            false,
        )? {
            // Atomically replace compacted SSTables
            {
                let mut sst_guard = self.sstables.write();
                *sst_guard = vec![Arc::new(merged_sst)];
            }

            // Delete old SSTable files from disk
            for old_sst in to_compact {
                let _ = fs::remove_file(old_sst.path());
            }
        }

        Ok(())
    }

    pub fn stats(&self) -> (usize, usize, usize) {
        let mem_count = self.memtable.read().len();
        let imm_count: usize = self.imm_memtables.read().iter().map(|m| m.len()).sum();
        let sst_count = self.sstables.read().len();
        (mem_count, imm_count, sst_count)
    }

    /// Total number of active records across MemTable and all SSTables on disk
    pub fn total_entries(&self) -> u64 {
        let mem_len = self.memtable.read().len() as u64;
        let imm_len: u64 = self.imm_memtables.read().iter().map(|m| m.len() as u64).sum();
        let sst_len: u64 = self.sstables.read().iter().map(|s| s.entry_count()).sum();
        mem_len + imm_len + sst_len
    }

    /// Calculate total disk size in bytes used by WAL and SSTable files
    pub fn disk_size_bytes(&self) -> u64 {
        let mut total = 0u64;
        if let Ok(entries) = fs::read_dir(&self.config.db_path) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    total += meta.len();
                }
            }
        }
        total
    }

    /// Backup the table's persistent SSTable data to a target directory
    pub async fn backup(&self, target_dir: &Path) -> Result<()> {
        // 1. Force flush memory to disk first
        self.force_flush().await?;

        fs::create_dir_all(target_dir)?;

        // 2. Copy all SSTables to backup target
        if let Ok(entries) = fs::read_dir(&self.config.db_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().map_or(false, |ext| ext == "sst") {
                    if let Some(name) = path.file_name() {
                        let dest = target_dir.join(name);
                        let _ = fs::copy(&path, &dest);
                    }
                }
            }
        }

        Ok(())
    }
}
