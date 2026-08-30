use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use parking_lot::RwLock;

use crate::core::types::Result;
use crate::index::IndexManager;
use crate::storage::{EngineConfig, StorageEngine};
use crate::storage::wal::WalConfig;

use crate::storage::cache::BlockCache;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DynamicServerConfig {
    pub worker_threads: usize,
    pub memtable_size_bytes: usize,
    pub block_cache_mb: usize,
    pub compaction_trigger: usize,
    pub commit_delay_us: u64,
    pub async_fsync: bool,
    pub auth_password: Option<String>,
}

impl Default for DynamicServerConfig {
    fn default() -> Self {
        let default_threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
        Self {
            worker_threads: default_threads,
            memtable_size_bytes: 256 * 1024 * 1024,
            block_cache_mb: 0, // 0 = OFF by default
            compaction_trigger: 4,
            commit_delay_us: 1000,
            async_fsync: false,
            auth_password: None,
        }
    }
}

pub struct Table {
    pub name: String,
    pub engine: Arc<StorageEngine>,
    pub index_manager: Arc<IndexManager>,
}

pub struct TableManager {
    base_data_dir: PathBuf,
    config: RwLock<DynamicServerConfig>,
    block_cache: Arc<BlockCache>,
    tables: RwLock<HashMap<String, Arc<Table>>>,
}

impl TableManager {
    pub async fn init<P: AsRef<Path>>(
        base_data_dir: P,
        memtable_size_bytes: usize,
        compaction_trigger: usize,
        wal_config: WalConfig,
    ) -> Result<Arc<Self>> {
        let base_path = base_data_dir.as_ref().to_path_buf();
        let tables_dir = base_path.join("tables");
        fs::create_dir_all(&tables_dir)?;

        let default_threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
        let dynamic_config = DynamicServerConfig {
            worker_threads: default_threads,
            memtable_size_bytes,
            block_cache_mb: 0,
            compaction_trigger,
            commit_delay_us: wal_config.commit_delay_us,
            async_fsync: wal_config.async_fsync,
            auth_password: None,
        };

        let block_cache = Arc::new(BlockCache::new(0));

        let manager = Arc::new(Self {
            base_data_dir: base_path,
            config: RwLock::new(dynamic_config),
            block_cache,
            tables: RwLock::new(HashMap::new()),
        });

        // Discover existing tables on disk (0 tables if brand new)
        let mut discovered = Vec::new();
        if let Ok(entries) = fs::read_dir(&tables_dir) {
            for entry in entries.flatten() {
                if entry.file_type().map_or(false, |t| t.is_dir()) {
                    if let Ok(name) = entry.file_name().into_string() {
                        discovered.push(name);
                    }
                }
            }
        }

        for table_name in discovered {
            manager.create_table_internal(&table_name).await?;
        }

        Ok(manager)
    }

    pub fn get_config(&self) -> DynamicServerConfig {
        self.config.read().clone()
    }

    pub fn update_config(&self, new_conf: DynamicServerConfig) {
        let new_mem_limit = new_conf.memtable_size_bytes;
        let new_cache_mb = new_conf.block_cache_mb;
        self.block_cache.set_capacity(new_cache_mb * 1024 * 1024);

        *self.config.write() = new_conf;

        // Apply new memtable limit to all active tables dynamically
        let tables = self.tables.read().clone();
        for table in tables.values() {
            table.engine.set_memtable_limit(new_mem_limit);
            table.engine.set_block_cache(self.block_cache.clone());
        }
    }

    pub fn get_auth_password(&self) -> Option<String> {
        self.config.read().auth_password.clone()
    }

    pub fn set_auth_password(&self, pass: Option<String>) {
        self.config.write().auth_password = pass;
    }

    pub async fn create_table(&self, name: &str) -> Result<Arc<Table>> {
        {
            let tables = self.tables.read();
            if let Some(table) = tables.get(name) {
                return Ok(table.clone());
            }
        }
        self.create_table_internal(name).await
    }

    async fn create_table_internal(&self, name: &str) -> Result<Arc<Table>> {
        let table_dir = self.base_data_dir.join("tables").join(name);
        fs::create_dir_all(&table_dir)?;

        let current_conf = self.config.read().clone();

        let config = EngineConfig {
            db_path: table_dir,
            memtable_max_bytes: current_conf.memtable_size_bytes,
            l0_compaction_trigger: current_conf.compaction_trigger,
            wal_config: WalConfig {
                commit_delay_us: current_conf.commit_delay_us,
                async_fsync: current_conf.async_fsync,
            },
        };

        let engine = StorageEngine::open(config).await?;
        engine.set_block_cache(self.block_cache.clone());
        let index_manager = Arc::new(IndexManager::new());

        let table = Arc::new(Table {
            name: name.to_string(),
            engine,
            index_manager,
        });

        let mut tables = self.tables.write();
        tables.insert(name.to_string(), table.clone());
        Ok(table)
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<Table>> {
        self.tables.read().get(name).cloned()
    }

    pub fn list_tables(&self) -> Vec<String> {
        let mut list: Vec<String> = self.tables.read().keys().cloned().collect();
        list.sort();
        list
    }

    /// Drop a table: remove from memory and completely delete its on-disk data
    pub async fn drop_table(&self, name: &str) -> Result<bool> {
        let removed = self.tables.write().remove(name);
        if removed.is_some() {
            let table_dir = self.base_data_dir.join("tables").join(name);
            let _ = fs::remove_dir_all(&table_dir);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Truncate a table: remove all data from MemTable and SSTables while preserving table structure
    pub async fn truncate_table(&self, name: &str) -> Result<bool> {
        if let Some(table) = self.get_table(name) {
            table.engine.truncate().await?;
            table.index_manager.clear();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Return detailed statistics per table (for Web UI and CLI status)
    pub fn table_info_list(&self) -> Vec<serde_json::Value> {
        let tables = self.tables.read().clone();
        let mut result = Vec::new();

        for (name, table) in tables {
            let (mem, imm, sst) = table.engine.stats();
            let total = table.engine.total_entries();
            let disk_size = table.engine.disk_size_bytes();
            let indexes = table.index_manager.list_indexes();

            result.push(serde_json::json!({
                "name": name,
                "total_records": total,
                "memtable_records": mem,
                "imm_memtable_records": imm,
                "sstable_count": sst,
                "disk_size_bytes": disk_size,
                "compression": table.engine.compression_type().as_str(),
                "indexes": indexes,
            }));
        }

        result.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        result
    }

    pub fn total_disk_size_bytes(&self) -> u64 {
        self.tables
            .read()
            .values()
            .map(|t| t.engine.disk_size_bytes())
            .sum()
    }

    /// Backup all tables to a timestamped backup folder
    pub async fn backup_all(&self, custom_target: Option<&str>) -> Result<PathBuf> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let backup_dir = if let Some(target) = custom_target {
            PathBuf::from(target)
        } else {
            self.base_data_dir.join("backups").join(format!("backup_{}", now))
        };

        fs::create_dir_all(&backup_dir)?;

        let tables = self.tables.read().clone();
        for (table_name, table) in tables {
            let table_dest = backup_dir.join("tables").join(&table_name);
            table.engine.backup(&table_dest).await?;
        }

        Ok(backup_dir)
    }
}
