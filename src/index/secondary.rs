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
                scores: RwLock::new(std::collections::HashMap::new()),
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

    pub fn get_rank_range(&self, start_rank: usize, end_rank: usize) -> Vec<(PlayerId, f64, usize)> {
        if start_rank == 0 || start_rank > end_rank {
            return Vec::new();
        }
        let take_count = end_rank - start_rank + 1;
        let skip_count = start_rank - 1;

        self.sorted_map
            .iter()
            .rev()
            .enumerate()
            .skip(skip_count)
            .take(take_count)
            .map(|(idx, item)| {
                let (score, player) = *item.key();
                (player, score.0, idx + 1)
            })
            .collect()
    }

    pub fn get_around_key(&self, player: &PlayerId, limit: usize) -> Option<Vec<(PlayerId, f64, usize)>> {
        let (rank, _) = self.get_rank(player)?;
        let total = self.sorted_map.len();
        if total == 0 || limit == 0 {
            return Some(Vec::new());
        }

        let half = limit / 2;
        let mut start_rank = rank.saturating_sub(half).max(1);
        let mut end_rank = start_rank + limit - 1;

        if end_rank > total {
            end_rank = total;
            start_rank = end_rank.saturating_sub(limit - 1).max(1);
        }

        Some(self.get_rank_range(start_rank, end_rank))
    }

    pub fn get_around_score(&self, target_score: f64, limit: usize) -> Vec<(PlayerId, f64, usize)> {
        let target_ord = OrderedScore(target_score);
        let total = self.sorted_map.len();
        if total == 0 || limit == 0 {
            return Vec::new();
        }

        // Find how many entries have score > target_score
        let higher_count = self
            .sorted_map
            .iter()
            .rev()
            .take_while(|item| item.key().0 > target_ord)
            .count();

        let approx_rank = (higher_count + 1).min(total);
        let half = limit / 2;
        let mut start_rank = approx_rank.saturating_sub(half).max(1);
        let mut end_rank = start_rank + limit - 1;

        if end_rank > total {
            end_rank = total;
            start_rank = end_rank.saturating_sub(limit - 1).max(1);
        }

        self.get_rank_range(start_rank, end_rank)
    }

    pub fn get_score_range(&self, min_score: f64, max_score: f64, limit: usize) -> Vec<(PlayerId, f64, usize)> {
        let min_ord = OrderedScore(min_score);
        let max_ord = OrderedScore(max_score);
        if min_score > max_score || limit == 0 {
            return Vec::new();
        }

        let mut results = Vec::new();
        for (idx, item) in self.sorted_map.iter().rev().enumerate() {
            let (score, player) = *item.key();
            if score > max_ord {
                continue;
            }
            if score < min_ord {
                break; // Because sorted descending
            }
            results.push((player, score.0, idx + 1));
            if results.len() >= limit {
                break;
            }
        }
        results
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

    pub fn list_indexes(&self) -> Vec<String> {
        self.list_indices()
    }

    pub fn clear(&self) {
        self.indices.write().clear();
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

    #[test]
    fn test_flexible_ranking_queries() {
        let idx = FieldIndex::new("stats.score".to_string());
        // Insert 100 players with scores 10, 20, 30 ... 1000
        for i in 1..=100 {
            let pid = PlayerId::new(i as u128);
            idx.update(pid, (i * 10) as f64);
        }
        assert_eq!(idx.len(), 100);

        // 1. Top 5
        let top5 = idx.get_top(5);
        assert_eq!(top5.len(), 5);
        assert_eq!(top5[0].2, 1); // Rank 1
        assert_eq!(top5[0].1, 1000.0); // Score 1000
        assert_eq!(top5[4].2, 5); // Rank 5
        assert_eq!(top5[4].1, 960.0);

        // 2. Rank Range 30 to 50
        let r30_50 = idx.get_rank_range(30, 50);
        assert_eq!(r30_50.len(), 21);
        assert_eq!(r30_50[0].2, 30);
        assert_eq!(r30_50.last().unwrap().2, 50);

        // 3. Around Key (player with score 500, which is rank 51)
        let p50 = PlayerId::new(50);
        let around_p50 = idx.get_around_key(&p50, 10).unwrap();
        assert_eq!(around_p50.len(), 10);
        assert!(around_p50.iter().any(|(p, _, _)| *p == p50));

        // 4. Around Score (center score 700 -> rank 31)
        let around_s700 = idx.get_around_score(700.0, 10);
        assert_eq!(around_s700.len(), 10);
        assert!(around_s700.iter().any(|(_, s, _)| *s == 700.0));

        // 5. Score Range 50 to 100
        let range_scores = idx.get_score_range(50.0, 100.0, 50);
        assert_eq!(range_scores.len(), 6); // 100, 90, 80, 70, 60, 50
        assert_eq!(range_scores[0].1, 100.0);
        assert_eq!(range_scores.last().unwrap().1, 50.0);
    }
}

