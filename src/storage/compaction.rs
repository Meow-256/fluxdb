use std::collections::BinaryHeap;
use std::path::Path;
use std::sync::Arc;

use crate::core::types::{PlayerId, Result, ValueEntry};
use crate::storage::sstable::{CompressionType, SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Clone)]
pub struct CompactionPlan {
    pub inputs: Vec<Arc<SsTable>>,
    pub output_level: u32,
    pub is_bottom_level: bool,
}

#[derive(Eq, PartialEq)]
struct MergeCursor {
    key: PlayerId,
    entry: ValueEntry,
    iter_idx: usize,
}

impl Ord for MergeCursor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering so BinaryHeap acts as a MinHeap for PlayerId
        other.key.cmp(&self.key)
    }
}

impl PartialOrd for MergeCursor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct CompactionStreamIter {
    iters: Vec<SsTableIterator>,
    heap: BinaryHeap<MergeCursor>,
    is_bottom_level: bool,
}

impl CompactionStreamIter {
    pub fn new(sstables: &[Arc<SsTable>], is_bottom_level: bool) -> Self {
        let mut iters = Vec::with_capacity(sstables.len());
        let mut heap = BinaryHeap::with_capacity(sstables.len());

        for (idx, sst) in sstables.iter().enumerate() {
            let mut iter = sst.iter();
            if let Some((key, entry)) = iter.next() {
                heap.push(MergeCursor {
                    key,
                    entry,
                    iter_idx: idx,
                });
            }
            iters.push(iter);
        }

        Self {
            iters,
            heap,
            is_bottom_level,
        }
    }
}

impl Iterator for CompactionStreamIter {
    type Item = (PlayerId, ValueEntry);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let current = self.heap.pop()?;
            let current_key = current.key;
            let mut best_entry = current.entry;

            if let Some((next_k, next_e)) = self.iters[current.iter_idx].next() {
                self.heap.push(MergeCursor {
                    key: next_k,
                    entry: next_e,
                    iter_idx: current.iter_idx,
                });
            }

            // Drain any duplicate entries with the same key across all SSTables
            while let Some(peek) = self.heap.peek() {
                if peek.key == current_key {
                    let dup = self.heap.pop().unwrap();
                    if dup.entry.seq_num > best_entry.seq_num {
                        best_entry = dup.entry;
                    }
                    if let Some((next_k, next_e)) = self.iters[dup.iter_idx].next() {
                        self.heap.push(MergeCursor {
                            key: next_k,
                            entry: next_e,
                            iter_idx: dup.iter_idx,
                        });
                    }
                } else {
                    break;
                }
            }

            if self.is_bottom_level && best_entry.is_tombstone() {
                continue;
            }

            return Some((current_key, best_entry));
        }
    }
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

    /// Merge multiple SSTables into a single new SSTable with specific compression algorithm using streaming K-way merge
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

        let estimated_entries: usize = sstables_to_merge.iter().map(|s| s.entry_count() as usize).sum();
        let stream_iter = CompactionStreamIter::new(sstables_to_merge, is_bottom_level);

        let new_sst_path = db_dir.as_ref().join(format!("{:06}.sst", new_id));
        let builder = SsTableBuilder::new(&new_sst_path, new_id, target_level)
            .with_compression_type(compression);
        builder.build_with_hint(stream_iter, estimated_entries)
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
