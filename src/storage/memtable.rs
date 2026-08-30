use std::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_skiplist::SkipMap;

use crate::core::types::{PlayerId, ValueEntry};

/// Concurrent MemTable using lock-free SkipMap
pub struct MemTable {
    map: SkipMap<PlayerId, ValueEntry>,
    approx_size_bytes: AtomicUsize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            map: SkipMap::new(),
            approx_size_bytes: AtomicUsize::new(0),
        }
    }

    /// Insert or update an entry
    pub fn insert(&self, key: PlayerId, entry: ValueEntry) {
        let val_len = entry.value.as_ref().map(|v| v.len()).unwrap_or(0);
        // 16B key + 16B seq/time + 8B entry struct + heap val
        let added_size = 40 + val_len;

        self.map.insert(key, entry);
        self.approx_size_bytes.fetch_add(added_size, Ordering::Relaxed);
    }

    /// Get an entry by PlayerId
    pub fn get(&self, key: &PlayerId) -> Option<ValueEntry> {
        self.map.get(key).map(|entry| entry.value().clone())
    }

    /// Check if key exists
    pub fn contains_key(&self, key: &PlayerId) -> bool {
        self.map.contains_key(key)
    }

    /// Returns iterator over all items in sorted order
    pub fn iter(&self) -> impl Iterator<Item = (PlayerId, ValueEntry)> + '_ {
        self.map.iter().map(|entry| (*entry.key(), entry.value().clone()))
    }

    /// Number of items
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Estimated memory usage in bytes
    pub fn size_bytes(&self) -> usize {
        self.approx_size_bytes.load(Ordering::Relaxed)
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}
