use std::collections::BTreeMap;
use std::sync::Arc;
use bytes::Bytes;

use crate::core::types::{PlayerId, Result, ValueEntry};
use crate::storage::memtable::MemTable;
use crate::storage::sstable::SsTable;

/// Scanner that merges sorted data from active MemTable, immutable MemTables, and SSTables
pub struct MergingScanner;

impl MergingScanner {
    pub fn scan(
        active_mem: &MemTable,
        imm_mems: &[Arc<MemTable>],
        sstables: &[Arc<SsTable>],
        start_key: Option<PlayerId>,
        end_key: Option<PlayerId>,
        limit: usize,
    ) -> Result<Vec<(PlayerId, Bytes)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        // BTreeMap to resolve key deduplication with latest seq_num & priority
        let mut merged: BTreeMap<PlayerId, ValueEntry> = BTreeMap::new();

        // 1. Scan active MemTable
        for (key, entry) in active_mem.range(start_key, end_key) {
            merged.insert(key, entry);
        }

        // 2. Scan immutable MemTables (newest to oldest)
        for imm in imm_mems.iter().rev() {
            for (key, entry) in imm.range(start_key, end_key) {
                merged.entry(key).or_insert(entry);
            }
        }

        // 3. Scan SSTables (newest to oldest)
        for sst in sstables.iter() {
            let sst_entries = sst.scan_range(start_key, end_key)?;
            for (key, entry) in sst_entries {
                match merged.get_mut(&key) {
                    Some(existing) => {
                        if entry.seq_num > existing.seq_num {
                            *existing = entry;
                        }
                    }
                    None => {
                        merged.insert(key, entry);
                    }
                }
            }
        }

        // 4. Collect non-tombstone results up to limit
        let mut results = Vec::with_capacity(limit.min(merged.len()));
        for (key, entry) in merged {
            if let Some(val) = entry.value {
                results.push((key, val));
                if results.len() >= limit {
                    break;
                }
            }
        }

        Ok(results)
    }
}
