use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use parking_lot::RwLock;

use crate::core::types::Result;
use crate::index::IndexManager;
use crate::storage::{EngineConfig, StorageEngine};
use crate::storage::wal::WalConfig;

pub struct Table {
    pub name: String,
    pub engine: Arc<StorageEngine>,
    pub index_manager: Arc<IndexManager>,
}

pub struct TableManager {
    base_data_dir: PathBuf,
    memtable_size_bytes: usize,
    compaction_trigger: usize,
    wal_config: WalConfig,
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

        let manager = Arc::new(Self {
            base_data_dir: base_path,
            memtable_size_bytes,
            compaction_trigger,
            wal_config,
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

        let config = EngineConfig {
            db_path: table_dir,
            memtable_max_bytes: self.memtable_size_bytes,
            l0_compaction_trigger: self.compaction_trigger,
            wal_config: self.wal_config,
        };

        let engine = StorageEngine::open(config).await?;
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
