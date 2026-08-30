use std::sync::Arc;
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;

use crate::core::types::PlayerId;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct OrderedScore(pub f64);

impl Eq for OrderedScore {}

impl Ord for OrderedScore {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

/// Zero-allocation fast JSON numeric value extractor
pub fn extract_number_from_json(path: &str, bytes: &[u8]) -> Option<f64> {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.is_empty() {
        return None;
    }

    let mut cursor = 0;
    let len = bytes.len();

    for (level, &part) in parts.iter().enumerate() {
        let is_last = level == parts.len() - 1;
        let key_bytes = part.as_bytes();

        // Search for "part": in slice
        let mut found = false;
        while cursor + key_bytes.len() + 3 <= len {
            if bytes[cursor] == b'"' && &bytes[cursor + 1..cursor + 1 + key_bytes.len()] == key_bytes && bytes[cursor + 1 + key_bytes.len()] == b'"' {
                cursor += key_bytes.len() + 2;
                // Skip whitespaces and colon
                while cursor < len && (bytes[cursor] == b' ' || bytes[cursor] == b'\t' || bytes[cursor] == b'\r' || bytes[cursor] == b'\n') {
                    cursor += 1;
                }
                if cursor < len && bytes[cursor] == b':' {
                    cursor += 1;
                    while cursor < len && (bytes[cursor] == b' ' || bytes[cursor] == b'\t' || bytes[cursor] == b'\r' || bytes[cursor] == b'\n') {
                        cursor += 1;
                    }
                    found = true;
                    break;
                }
            } else {
                cursor += 1;
            }
        }

        if !found {
            return None;
        }

        if is_last {
            // Parse numeric value at cursor
            let start = cursor;
            while cursor < len && (bytes[cursor].is_ascii_digit() || bytes[cursor] == b'.' || bytes[cursor] == b'-' || bytes[cursor] == b'+' || bytes[cursor] == b'e' || bytes[cursor] == b'E') {
                cursor += 1;
            }
            if start < cursor {
                if let Ok(s) = std::str::from_utf8(&bytes[start..cursor]) {
                    return s.parse::<f64>().ok();
                }
            }
            return None;
        }
    }

    None
}

const NUM_SHARDS: usize = 64;

struct ShardScores {
    scores: RwLock<std::collections::HashMap<PlayerId, OrderedScore>>,
}

/// Lock-free / Sharded Field Index
pub struct FieldIndex {
    path: String,
    shards: Vec<ShardScores>,
    // Lock-free concurrent sorted map for ranking
    sorted_map: SkipMap<(OrderedScore, PlayerId), ()>,
}

impl FieldIndex {
    pub fn new(path: String) -> Self {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(ShardScores {
                scores: RwLock::new(std::collections::HashMap::with_capacity(8192)),
            });
        }
        Self {
            path,
            shards,
            sorted_map: SkipMap::new(),
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    #[inline(always)]
    fn shard_idx(player: &PlayerId) -> usize {
        (player.0 as usize) % NUM_SHARDS
    }

    pub fn update(&self, player: PlayerId, new_score: f64) {
        let ordered = OrderedScore(new_score);
        let shard = &self.shards[Self::shard_idx(&player)];

        let old_score = {
            let mut s = shard.scores.write();
            s.insert(player, ordered)
        };

        if let Some(old) = old_score {
            self.sorted_map.remove(&(old, player));
        }
        self.sorted_map.insert((ordered, player), ());
    }

    pub fn remove(&self, player: &PlayerId) {
        let shard = &self.shards[Self::shard_idx(player)];
        let old_score = {
            let mut s = shard.scores.write();
            s.remove(player)
        };

        if let Some(old) = old_score {
            self.sorted_map.remove(&(old, *player));
        }
    }

    pub fn get_top(&self, limit: usize) -> Vec<(PlayerId, f64, usize)> {
        // SkipMap iter() is sorted ascending, so take from the back for highest scores
        let mut entries: Vec<(PlayerId, f64)> = Vec::with_capacity(limit);
        for item in self.sorted_map.iter().rev().take(limit) {
            let (score, player) = *item.key();
            entries.push((player, score.0));
        }

        entries
            .into_iter()
            .enumerate()
            .map(|(idx, (p, s))| (p, s, idx + 1))
            .collect()
    }

    pub fn get_rank(&self, player: &PlayerId) -> Option<(usize, f64)> {
        let shard = &self.shards[Self::shard_idx(player)];
        let score = {
            let s = shard.scores.read();
            *s.get(player)?
        };

        let higher_count = self
            .sorted_map
            .iter()
            .rev()
            .take_while(|item| {
                let (s, p) = *item.key();
                s > score || (s == score && p != *player)
            })
            .count();

        Some((higher_count + 1, score.0))
    }

    pub fn len(&self) -> usize {
        self.sorted_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sorted_map.is_empty()
    }
}

pub struct IndexManager {
    indices: RwLock<std::collections::HashMap<String, Arc<FieldIndex>>>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            indices: RwLock::new(std::collections::HashMap::new()),
        }
    }

    pub fn create_index(&self, path: &str) -> Arc<FieldIndex> {
        let mut indices = self.indices.write();
        if let Some(existing) = indices.get(path) {
            return existing.clone();
        }
        let index = Arc::new(FieldIndex::new(path.to_string()));
        indices.insert(path.to_string(), index.clone());
        index
    }

    pub fn list_indices(&self) -> Vec<String> {
        self.indices.read().keys().cloned().collect()
    }

    pub fn get_index(&self, path: &str) -> Option<Arc<FieldIndex>> {
        self.indices.read().get(path).cloned()
    }

    #[inline]
    pub fn on_put(&self, player: PlayerId, json_bytes: &[u8]) {
        let indices = self.indices.read();
        for (path, index) in indices.iter() {
            if let Some(val) = extract_number_from_json(path, json_bytes) {
                index.update(player, val);
            }
        }
    }

    #[inline]
    pub fn on_delete(&self, player: PlayerId) {
        let indices = self.indices.read();
        for index in indices.values() {
            index.remove(&player);
        }
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_number_from_json() {
        let json = br#"{"name":"Alex","stats":{"kills":350,"wins":50}}"#;
        assert_eq!(extract_number_from_json("stats.kills", json), Some(350.0));
        assert_eq!(extract_number_from_json("stats.wins", json), Some(50.0));
        assert_eq!(extract_number_from_json("stats.losses", json), None);
    }
}
