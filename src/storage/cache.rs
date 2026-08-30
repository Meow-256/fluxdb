use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use bytes::Bytes;
use parking_lot::Mutex;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub sst_id: u64,
    pub block_offset: u64,
}

struct Node {
    key: CacheKey,
    val: Bytes,
    size: usize,
    prev: Option<usize>,
    next: Option<usize>,
}

struct LruInner {
    nodes: Vec<Option<Node>>,
    free_indices: Vec<usize>,
    map: HashMap<CacheKey, usize>,
    head: Option<usize>, // Most recently used
    tail: Option<usize>, // Least recently used
    current_bytes: usize,
}

impl LruInner {
    fn new() -> Self {
        Self {
            nodes: Vec::new(),
            free_indices: Vec::new(),
            map: HashMap::new(),
            head: None,
            tail: None,
            current_bytes: 0,
        }
    }

    fn remove_node(&mut self, idx: usize) {
        let (prev, next) = {
            let n = self.nodes[idx].as_ref().unwrap();
            (n.prev, n.next)
        };

        if let Some(p) = prev {
            if let Some(ref mut pn) = self.nodes[p] {
                pn.next = next;
            }
        } else {
            self.head = next;
        }

        if let Some(nx) = next {
            if let Some(ref mut nn) = self.nodes[nx] {
                nn.prev = prev;
            }
        } else {
            self.tail = prev;
        }
    }

    fn move_to_head(&mut self, idx: usize) {
        if self.head == Some(idx) {
            return;
        }
        self.remove_node(idx);

        if let Some(ref mut n) = self.nodes[idx] {
            n.prev = None;
            n.next = self.head;
        }

        if let Some(h) = self.head {
            if let Some(ref mut hn) = self.nodes[h] {
                hn.prev = Some(idx);
            }
        } else {
            self.tail = Some(idx);
        }
        self.head = Some(idx);
    }

    fn evict_tail(&mut self) -> Option<usize> {
        let t = self.tail?;
        let (k, size) = {
            let n = self.nodes[t].as_ref().unwrap();
            (n.key, n.size)
        };
        self.remove_node(t);
        self.map.remove(&k);
        self.current_bytes = self.current_bytes.saturating_sub(size);
        self.nodes[t] = None;
        self.free_indices.push(t);
        Some(t)
    }
}

pub struct BlockCache {
    capacity_bytes: AtomicUsize,
    inner: Mutex<LruInner>,
}

impl std::fmt::Debug for BlockCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockCache")
            .field("capacity_bytes", &self.capacity_bytes.load(Ordering::Relaxed))
            .finish()
    }
}

impl BlockCache {
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes: AtomicUsize::new(capacity_bytes),
            inner: Mutex::new(LruInner::new()),
        }
    }

    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.capacity_bytes.load(Ordering::Relaxed) > 0
    }

    pub fn set_capacity(&self, capacity_bytes: usize) {
        self.capacity_bytes.store(capacity_bytes, Ordering::Relaxed);
        if capacity_bytes == 0 {
            self.clear();
        } else {
            let mut inner = self.inner.lock();
            while inner.current_bytes > capacity_bytes {
                if inner.evict_tail().is_none() {
                    break;
                }
            }
        }
    }

    pub fn get(&self, sst_id: u64, block_offset: u64) -> Option<Bytes> {
        let cap = self.capacity_bytes.load(Ordering::Relaxed);
        if cap == 0 {
            return None;
        }

        let key = CacheKey { sst_id, block_offset };
        let mut inner = self.inner.lock();
        let idx = *inner.map.get(&key)?;
        inner.move_to_head(idx);
        inner.nodes[idx].as_ref().map(|n| n.val.clone())
    }

    pub fn insert(&self, sst_id: u64, block_offset: u64, data: Bytes) {
        let cap = self.capacity_bytes.load(Ordering::Relaxed);
        if cap == 0 {
            return;
        }

        let entry_size = data.len();
        if entry_size > cap {
            return; // Data larger than entire cache
        }

        let key = CacheKey { sst_id, block_offset };
        let mut inner = self.inner.lock();

        // Evict until there is enough space
        while inner.current_bytes + entry_size > cap {
            if inner.evict_tail().is_none() {
                break;
            }
        }

        if let Some(&existing_idx) = inner.map.get(&key) {
            let old_size = inner.nodes[existing_idx].as_ref().map(|n| n.size).unwrap_or(0);
            inner.current_bytes = inner.current_bytes.saturating_sub(old_size) + entry_size;
            if let Some(ref mut n) = inner.nodes[existing_idx] {
                n.val = data;
                n.size = entry_size;
            }
            inner.move_to_head(existing_idx);
            return;
        }

        let node = Node {
            key,
            val: data,
            size: entry_size,
            prev: None,
            next: None,
        };

        let node_idx = if let Some(free_idx) = inner.free_indices.pop() {
            inner.nodes[free_idx] = Some(node);
            free_idx
        } else {
            inner.nodes.push(Some(node));
            inner.nodes.len() - 1
        };

        inner.map.insert(key, node_idx);
        inner.current_bytes += entry_size;
        inner.move_to_head(node_idx);
    }

    pub fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.nodes.clear();
        inner.free_indices.clear();
        inner.map.clear();
        inner.head = None;
        inner.tail = None;
        inner.current_bytes = 0;
    }

    pub fn current_usage(&self) -> (usize, usize) {
        let inner = self.inner.lock();
        (inner.current_bytes, self.capacity_bytes.load(Ordering::Relaxed))
    }
}
