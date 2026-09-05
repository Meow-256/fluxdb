use std::collections::BinaryHeap;
use std::sync::Arc;
use bytes::Bytes;

use crate::core::types::{PlayerId, Result, ValueEntry};
use crate::storage::memtable::MemTable;
use crate::storage::sstable::SsTable;

#[derive(Eq, PartialEq)]
struct MergingCursor {
    key: PlayerId,
    entry: ValueEntry,
    iter_idx: usize,
}

impl Ord for MergingCursor {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse for MinHeap on key (smallest key popped first)
        other.key.cmp(&self.key)
    }
}

impl PartialOrd for MergingCursor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Zero-allocation streaming iterator merging active MemTable, immutable MemTables, and SSTables
pub struct MergingIter<'a> {
    iters: Vec<Box<dyn Iterator<Item = (PlayerId, ValueEntry)> + 'a>>,
    heap: BinaryHeap<MergingCursor>,
}

impl<'a> MergingIter<'a> {
    pub fn new(
        active_mem: &'a MemTable,
        imm_mems: &'a [Arc<MemTable>],
        sstables: &'a [Arc<SsTable>],
        start_key: Option<PlayerId>,
        end_key: Option<PlayerId>,
    ) -> Self {
        let total_sources = 1 + imm_mems.len() + sstables.len();
        let mut iters: Vec<Box<dyn Iterator<Item = (PlayerId, ValueEntry)> + 'a>> =
            Vec::with_capacity(total_sources);
        let mut heap = BinaryHeap::with_capacity(total_sources);

        let mut add_iter = |mut it: Box<dyn Iterator<Item = (PlayerId, ValueEntry)> + 'a>| {
            let idx = iters.len();
            if let Some((key, entry)) = it.next() {
                heap.push(MergingCursor {
                    key,
                    entry,
                    iter_idx: idx,
                });
            }
            iters.push(it);
        };

        add_iter(Box::new(active_mem.range_iter(start_key, end_key)));
        for imm in imm_mems.iter() {
            add_iter(Box::new(imm.range_iter(start_key, end_key)));
        }
        for sst in sstables.iter() {
            add_iter(Box::new(sst.scan_iter(start_key, end_key)));
        }

        Self { iters, heap }
    }
}

impl<'a> Iterator for MergingIter<'a> {
    type Item = (PlayerId, ValueEntry);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let current = self.heap.pop()?;
            let current_key = current.key;
            let mut best_entry = current.entry;

            if let Some((next_k, next_e)) = self.iters[current.iter_idx].next() {
                self.heap.push(MergingCursor {
                    key: next_k,
                    entry: next_e,
                    iter_idx: current.iter_idx,
                });
            }

            // Merge duplicate entries with the same key, picking highest seq_num
            while let Some(peek) = self.heap.peek() {
                if peek.key == current_key {
                    let dup = self.heap.pop().unwrap();
                    if dup.entry.seq_num > best_entry.seq_num {
                        best_entry = dup.entry;
                    }
                    if let Some((next_k, next_e)) = self.iters[dup.iter_idx].next() {
                        self.heap.push(MergingCursor {
                            key: next_k,
                            entry: next_e,
                            iter_idx: dup.iter_idx,
                        });
                    }
                } else {
                    break;
                }
            }

            return Some((current_key, best_entry));
        }
    }
}

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

        let iter = MergingIter::new(active_mem, imm_mems, sstables, start_key, end_key);
        let mut results = Vec::with_capacity(limit.min(1024));

        for (key, entry) in iter {
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
