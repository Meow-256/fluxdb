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

use crate::core::types::{DbError, OpType, PlayerId, Result, ValueEntry};
use crate::index::QueryFilter;
use crate::storage::cache::BlockCache;
use crate::storage::compaction::Compactor;
use crate::storage::iterator::MergingScanner;
use crate::storage::memtable::MemTable;
use crate::storage::sstable::{CompressionType, SsTable, SsTableBuilder};
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
    memtable_limit_bytes: Arc<AtomicU64>,
    block_cache: Arc<BlockCache>,
    compression_type: Arc<RwLock<CompressionType>>,
}

impl StorageEngine {
    pub async fn open(config: EngineConfig) -> Result<Arc<Self>> {
        fs::create_dir_all(&config.db_path)?;

        let block_cache = Arc::new(BlockCache::new(0));

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
                                Ok(mut sst) => {
                                    if sst.is_legacy_version() {
                                        info!("Upgrading legacy SSTable {:?} to V2 format...", path);
                                        match sst.upgrade_in_place() {
                                            Ok(upgraded) => sst = upgraded,
                                            Err(e) => error!("Failed to upgrade SSTable {:?}: {}", path, e),
                                        }
                                    }
                                    sst.set_block_cache(Some(block_cache.clone()));
                                    existing_sstables.push(Arc::new(sst));
                                }
                                Err(e) => {
                                    error!("Failed to open SSTable {:?}: {}", path, e);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Sort descending by ID (newest SSTable first)
        existing_sstables.sort_by(|a, b| b.id().cmp(&a.id()));

        // 2. Recover MemTable from WAL
        let wal_path = config.db_path.join("wal.log");
        let (recovered_entries, last_seq_num) = WalRecovery::recover(&wal_path)?;

        let memtable = MemTable::new();
        if !recovered_entries.is_empty() {
            info!("Recovered {} entries from WAL", recovered_entries.len());
            for (key, val) in recovered_entries {
                memtable.insert(key, val);
            }
        }

        // 3. Open WAL for writing
        let wal_writer = WalWriter::open(&wal_path, last_seq_num, config.wal_config)?;

        let memtable_limit_bytes = Arc::new(AtomicU64::new(config.memtable_max_bytes as u64));

        let engine = Arc::new(Self {
            config,
            wal: Arc::new(wal_writer),
            memtable: Arc::new(RwLock::new(memtable)),
            imm_memtables: Arc::new(RwLock::new(Vec::new())),
            sstables: Arc::new(RwLock::new(existing_sstables)),
            next_sst_id: AtomicU64::new(max_sst_id + 1),
            flush_notify: Arc::new(Notify::new()),
            ttls: Arc::new(RwLock::new(HashMap::new())),
            memtable_limit_bytes,
            block_cache,
            compression_type: Arc::new(RwLock::new(CompressionType::Lz4)),
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

    /// Range scan keys in ascending sorted order
    pub fn scan(
        &self,
        start_key: Option<PlayerId>,
        end_key: Option<PlayerId>,
        limit: usize,
    ) -> Result<Vec<(PlayerId, Bytes)>> {
        let mem = self.memtable.read();
        let imm = self.imm_memtables.read().clone();
        let sst = self.sstables.read().clone();

        let raw_entries = MergingScanner::scan(&mem, &imm, &sst, start_key, end_key, limit)?;

        let valid_entries = raw_entries
            .into_iter()
            .filter(|(key, _)| !self.is_expired(key))
            .take(limit)
            .collect();

        Ok(valid_entries)
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
            memtable.size_bytes() >= self.memtable_limit_bytes.load(Ordering::Relaxed) as usize
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
            memtable.size_bytes() >= self.memtable_limit_bytes.load(Ordering::Relaxed) as usize
        };

        if should_flush {
            self.rotate_memtable().await?;
        }

        Ok(())
    }

    /// Atomic batch write for transactions
    pub async fn apply_batch(&self, operations: Vec<(PlayerId, Option<Bytes>, OpType)>) -> Result<()> {
        if operations.is_empty() {
            return Ok(());
        }

        let (seq, ts) = self.wal.append_batch(operations.clone()).await?;

        let should_flush = {
            let memtable = self.memtable.read();
            for (key, val, op) in operations {
                let entry = match op {
                    OpType::Put => ValueEntry::put(val.unwrap_or_default(), seq, ts),
                    OpType::Delete => ValueEntry::delete(seq, ts),
                };
                memtable.insert(key, entry);
            }
            memtable.size_bytes() >= self.memtable_limit_bytes.load(Ordering::Relaxed) as usize
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

        let c_type = *self.compression_type.read();
        let builder = SsTableBuilder::new(&sst_path, sst_id, 0).with_compression_type(c_type);
        if let Some(mut sstable) = builder.build(memtable.iter())? {
            sstable.set_block_cache(Some(self.block_cache.clone()));
            let mut sst_guard = self.sstables.write();
            sst_guard.insert(0, Arc::new(sstable)); // Insert as newest
        }

        Ok(())
    }

    /// Trigger multi-way merge compaction of existing SSTables
    pub async fn run_compaction(&self) -> Result<()> {
        let plan = {
            let sst_guard = self.sstables.read();
            Compactor::pick_compaction(&sst_guard, self.config.l0_compaction_trigger)
        };

        let plan = match plan {
            Some(p) => p,
            None => return Ok(()),
        };

        let new_sst_id = self.next_sst_id.fetch_add(1, Ordering::SeqCst);
        let c_type = *self.compression_type.read();

        if let Some(mut merged_sst) = Compactor::compact_with_compression(
            &self.config.db_path,
            &plan.inputs,
            new_sst_id,
            plan.output_level,
            plan.is_bottom_level,
            c_type,
        )? {
            merged_sst.set_block_cache(Some(self.block_cache.clone()));
            let input_paths: Vec<PathBuf> = plan.inputs.iter().map(|s| s.path().to_path_buf()).collect();
            let merged_arc = Arc::new(merged_sst);

            // Atomically update SSTable list: remove compacted inputs, insert new merged SSTable
            {
                let mut sst_guard = self.sstables.write();
                let mut new_list: Vec<Arc<SsTable>> = sst_guard
                    .iter()
                    .filter(|s| !input_paths.contains(&s.path().to_path_buf()))
                    .cloned()
                    .collect();
                new_list.push(merged_arc);
                // Sort by level ASC, then ID DESC
                new_list.sort_by(|a, b| {
                    match a.level().cmp(&b.level()) {
                        std::cmp::Ordering::Equal => b.id().cmp(&a.id()),
                        ord => ord,
                    }
                });
                *sst_guard = new_list;
            }

            // Delete old SSTable files from disk
            for old_path in input_paths {
                let _ = fs::remove_file(old_path);
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

    /// Partially update a JSON document field at a specific path
    pub async fn json_set(&self, key: PlayerId, path: &str, new_val_raw: &str) -> Result<Bytes> {
        let current_val = self.get(&key)?;
        let mut root = if let Some(bytes) = current_val {
            serde_json::from_slice::<serde_json::Value>(&bytes).unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new()))
        } else {
            serde_json::Value::Object(serde_json::Map::new())
        };

        let new_val = QueryFilter::parse_json_value(new_val_raw);
        QueryFilter::set_json_path(&mut root, path, new_val);

        let updated_bytes = Bytes::from(serde_json::to_vec(&root).map_err(|e| DbError::InvalidCommand(e.to_string()))?);
        self.put(key, updated_bytes.clone()).await?;
        Ok(updated_bytes)
    }

    /// Truncate all records from MemTable and SSTables on disk
    pub async fn truncate(&self) -> Result<()> {
        // 1. Reset active and immutable MemTables
        *self.memtable.write() = MemTable::new();
        self.imm_memtables.write().clear();

        // 2. Remove SSTable files on disk
        let sst_list = self.sstables.read().clone();
        for sst in sst_list.iter() {
            let _ = fs::remove_file(sst.path());
        }
        self.sstables.write().clear();

        // 3. Clear TTLs
        self.ttls.write().clear();

        // 4. Truncate WAL file
        let wal_path = self.config.db_path.join("wal.log");
        let _ = fs::write(&wal_path, b"");

        Ok(())
    }

    /// Delete all records matching a filter query atomically in batch
    pub async fn del_where(&self, filter: &QueryFilter) -> Result<usize> {
        let all_entries = self.scan(None, None, 1_000_000)?;
        let mut del_ops = Vec::new();

        for (key, val) in all_entries {
            if filter.matches(&val) {
                del_ops.push((key, None, OpType::Delete));
            }
        }

        let count = del_ops.len();
        if count > 0 {
            self.apply_batch(del_ops).await?;
        }
        Ok(count)
    }

    /// Count matching records (or total records if no filter)
    pub fn count_records(&self, filter: Option<&QueryFilter>) -> Result<usize> {
        let all_entries = self.scan(None, None, 1_000_000)?;
        if let Some(f) = filter {
            Ok(all_entries.into_iter().filter(|(_, val)| f.matches(val)).count())
        } else {
            Ok(all_entries.len())
        }
    }

    /// Calculate statistical metrics (count, sum, avg, min, max) for a numeric field
    pub fn calc_stats(&self, field: &str, filter: Option<&QueryFilter>) -> Result<serde_json::Value> {
        let all_entries = self.scan(None, None, 1_000_000)?;
        let mut count = 0usize;
        let mut sum = 0.0f64;
        let mut min = f64::INFINITY;
        let mut max = f64::NEG_INFINITY;

        for (_, val) in all_entries {
            if let Ok(parsed) = serde_json::from_slice::<serde_json::Value>(&val) {
                if filter.map_or(true, |f| f.matches_value(&parsed)) {
                    if let Some(num) = QueryFilter::extract_number(&parsed, field) {
                        count += 1;
                        sum += num;
                        min = min.min(num);
                        max = max.max(num);
                    }
                }
            }
        }

        let avg = if count > 0 { sum / count as f64 } else { 0.0 };
        let min_val = if count > 0 { min } else { 0.0 };
        let max_val = if count > 0 { max } else { 0.0 };

        Ok(serde_json::json!({
            "field": field,
            "count": count,
            "sum": sum,
            "avg": avg,
            "min": min_val,
            "max": max_val,
        }))
    }

    /// Dynamically update the in-memory MemTable maximum size limit
    pub fn set_memtable_limit(&self, max_bytes: usize) {
        self.memtable_limit_bytes.store(max_bytes as u64, Ordering::Relaxed);
    }

    /// Access the shared BlockCache for this engine
    pub fn block_cache(&self) -> Arc<BlockCache> {
        self.block_cache.clone()
    }

    /// Set a new shared BlockCache and update all existing SSTables
    pub fn set_block_cache(&self, cache: Arc<BlockCache>) {
        let sst_list = self.sstables.read().clone();
        for sst in sst_list.iter() {
            // Unsafe or mutable helper to update SSTable cache ref
            let sst_ptr = Arc::as_ptr(sst) as *mut SsTable;
            unsafe {
                (*sst_ptr).set_block_cache(Some(cache.clone()));
            }
        }
    }

    /// Set the block compression algorithm (NONE, LZ4, ZSTD) for subsequent writes
    pub fn set_compression_type(&self, c_type: CompressionType) {
        *self.compression_type.write() = c_type;
    }

    /// Current block compression algorithm for this engine
    pub fn compression_type(&self) -> CompressionType {
        *self.compression_type.read()
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
