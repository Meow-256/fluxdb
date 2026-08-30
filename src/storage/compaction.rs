use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use crate::core::types::{PlayerId, Result, ValueEntry};
use crate::storage::sstable::{CompressionType, SsTable, SsTableBuilder};

#[derive(Debug, Clone)]
pub struct CompactionPlan {
    pub inputs: Vec<Arc<SsTable>>,
    pub output_level: u32,
    pub is_bottom_level: bool,
}

pub struct Compactor;

impl Compactor {
    /// Plan next leveled compaction step
    pub fn pick_compaction(sstables: &[Arc<SsTable>], l0_trigger: usize) -> Option<CompactionPlan> {
        let mut l0 = Vec::new();
        let mut l1 = Vec::new();
        let mut l2 = Vec::new();

        for sst in sstables {
            match sst.level() {
                0 => l0.push(sst.clone()),
                1 => l1.push(sst.clone()),
                _ => l2.push(sst.clone()),
            }
        }

        // 1. Check L0 -> L1 compaction trigger
        if l0.len() >= l0_trigger {
            let l0_min = l0.iter().map(|s| s.min_key()).min()?;
            let l0_max = l0.iter().map(|s| s.max_key()).max()?;

            // Find overlapping L1 SSTables
            let mut inputs = l0;
            for s1 in l1 {
                if s1.max_key() >= l0_min && s1.min_key() <= l0_max {
                    inputs.push(s1);
                }
            }

            let is_bottom_level = l2.is_empty();
            return Some(CompactionPlan {
                inputs,
                output_level: 1,
                is_bottom_level,
            });
        }

        // 2. Check L1 -> L2 compaction trigger
        if l1.len() >= 8 {
            if let Some(first_l1) = l1.first() {
                let mut inputs = vec![first_l1.clone()];
                for s2 in l2 {
                    if s2.max_key() >= first_l1.min_key() && s2.min_key() <= first_l1.max_key() {
                        inputs.push(s2);
                    }
                }
                return Some(CompactionPlan {
                    inputs,
                    output_level: 2,
                    is_bottom_level: true,
                });
            }
        }

        None
    }

    /// Merge multiple SSTables into a single new SSTable with specific compression algorithm
    pub fn compact_with_compression<P: AsRef<Path>>(
        db_dir: P,
        sstables_to_merge: &[Arc<SsTable>],
        new_id: u64,
        target_level: u32,
        is_bottom_level: bool,
        compression: CompressionType,
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
        let builder = SsTableBuilder::new(&new_sst_path, new_id, target_level)
            .with_compression_type(compression);
        builder.build(final_entries)
    }

    /// Merge multiple SSTables into a single new SSTable, resolving duplicates by highest seq_num
    pub fn compact<P: AsRef<Path>>(
        db_dir: P,
        sstables_to_merge: &[Arc<SsTable>],
        new_id: u64,
        target_level: u32,
        is_bottom_level: bool,
    ) -> Result<Option<SsTable>> {
        Self::compact_with_compression(
            db_dir,
            sstables_to_merge,
            new_id,
            target_level,
            is_bottom_level,
            CompressionType::Lz4,
        )
    }
}
