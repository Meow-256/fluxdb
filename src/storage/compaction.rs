use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use crate::core::types::{PlayerId, Result, ValueEntry};
use crate::storage::sstable::{SsTable, SsTableBuilder};

pub struct Compactor;

impl Compactor {
    /// Merge multiple SSTables into a single new SSTable, resolving duplicates by highest seq_num
    pub fn compact<P: AsRef<Path>>(
        db_dir: P,
        sstables_to_merge: &[Arc<SsTable>],
        new_id: u64,
        target_level: u32,
        is_bottom_level: bool,
    ) -> Result<Option<SsTable>> {
        if sstables_to_merge.is_empty() {
            return Ok(None);
        }

        // Collect and merge all entries in sorted order by PlayerId, picking highest seq_num
        let mut merged_map: BTreeMap<PlayerId, ValueEntry> = BTreeMap::new();

        for sst in sstables_to_merge {
            let entries = sst.read_all_entries()?;
            for (key, entry) in entries {
                match merged_map.get_mut(&key) {
                    Some(existing) => {
                        if entry.seq_num > existing.seq_num {
                            *existing = entry;
                        }
                    }
                    None => {
                        merged_map.insert(key, entry);
                    }
                }
            }
        }

        // If this is the bottom-most level, we can safely discard tombstones
        let final_entries: Vec<(PlayerId, ValueEntry)> = if is_bottom_level {
            merged_map
                .into_iter()
                .filter(|(_, entry)| !entry.is_tombstone())
                .collect()
        } else {
            merged_map.into_iter().collect()
        };

        if final_entries.is_empty() {
            return Ok(None);
        }

        let new_sst_path = db_dir.as_ref().join(format!("{:06}.sst", new_id));
        let builder = SsTableBuilder::new(&new_sst_path, new_id, target_level);
        builder.build(final_entries)
    }
}
