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
        // 16B key + 40B ValueEntry + ~40-56B SkipMap node & pointer tower metadata + heap val
        let added_size = 96 + val_len;

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

    /// Returns iterator over items within key bounds in sorted order without collecting
    pub fn range_iter(&self, start: Option<PlayerId>, end: Option<PlayerId>) -> impl Iterator<Item = (PlayerId, ValueEntry)> + '_ {
        use std::ops::Bound;
        let start_bound = match start {
            Some(k) => Bound::Included(k),
            None => Bound::Unbounded,
        };
        let end_bound = match end {
            Some(k) => Bound::Included(k),
            None => Bound::Unbounded,
        };

        self.map
            .range((start_bound, end_bound))
            .map(|entry| (*entry.key(), entry.value().clone()))
    }

    /// Returns iterator over items within key bounds in sorted order
    pub fn range(&self, start: Option<PlayerId>, end: Option<PlayerId>) -> Vec<(PlayerId, ValueEntry)> {
        self.range_iter(start, end).collect()
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
