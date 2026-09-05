use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::core::types::PlayerId;

/// High-performance Bloom Filter optimized for 128-bit UUIDs
#[derive(Clone, Debug)]
pub struct BloomFilter {
    bit_vec: Vec<u8>,
    num_hashes: u8,
    num_bits: usize,
}

impl BloomFilter {
    /// Creates a Bloom filter sized for `expected_items` with target false positive rate `fp_rate` (e.g. 0.01 = 1%)
    pub fn new(expected_items: usize, fp_rate: f64) -> Self {
        let n = expected_items.max(1) as f64;
        let p = fp_rate.clamp(0.0001, 0.5);

        // Optimal number of bits: m = - (n * ln(p)) / (ln(2)^2)
        let ln2_sq = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        let num_bits = ((-1.0 * n * p.ln()) / ln2_sq).ceil() as usize;
        let num_bits = num_bits.max(64);

        // Optimal number of hash functions: k = (m / n) * ln(2)
        let num_hashes = (((num_bits as f64 / n) * std::f64::consts::LN_2).round() as u8).clamp(1, 30);

        let num_bytes = (num_bits + 7) / 8;
        Self {
            bit_vec: vec![0u8; num_bytes],
            num_hashes,
            num_bits: num_bytes * 8,
        }
    }

    /// Hashes a 128-bit PlayerId using two 64-bit halves with Murmur-style mixers
    #[inline(always)]
    fn get_hash_pair(key: PlayerId) -> (u64, u64) {
        let val = key.0;
        let mut h1 = ((val >> 64) as u64) ^ 0x517cc1b727220a95;
        let mut h2 = (val as u64) ^ 0x9e3779b97f4a7c15;

        // 64-bit cross-mix
        h1 = h1.wrapping_add(h2);
        h1 ^= h1 >> 33;
        h1 = h1.wrapping_mul(0xff51afd7ed558ccd);
        h1 ^= h1 >> 33;
        h1 = h1.wrapping_mul(0xc4ceb9fe1a85ec53);
        h1 ^= h1 >> 33;

        h2 = h2.wrapping_add(h1);
        h2 ^= h2 >> 33;
        h2 = h2.wrapping_mul(0xff51afd7ed558ccd);
        h2 ^= h2 >> 33;
        h2 = h2.wrapping_mul(0xc4ceb9fe1a85ec53);
        h2 ^= h2 >> 33;

        (h1, h2 | 1) // Ensure h2 is odd for coprime stepping
    }

    /// Insert a PlayerId into the filter
    pub fn insert(&mut self, key: PlayerId) {
        let (h1, h2) = Self::get_hash_pair(key);
        let num_bits = self.num_bits as u64;

        for i in 0..self.num_hashes as u64 {
            let bit_idx = (h1.wrapping_add(i.wrapping_mul(h2)) % num_bits) as usize;
            let byte_idx = bit_idx / 8;
            let bit_offset = bit_idx % 8;
            self.bit_vec[byte_idx] |= 1 << bit_offset;
        }
    }

    /// Check if a PlayerId might be in the set (false positives possible, false negatives impossible)
    pub fn contains(&self, key: PlayerId) -> bool {
        if self.num_bits == 0 || self.bit_vec.is_empty() {
            return false;
        }
        let (h1, h2) = Self::get_hash_pair(key);
        let num_bits = self.num_bits as u64;

        for i in 0..self.num_hashes as u64 {
            let bit_idx = (h1.wrapping_add(i.wrapping_mul(h2)) % num_bits) as usize;
            let byte_idx = bit_idx / 8;
            let bit_offset = bit_idx % 8;
            if (self.bit_vec[byte_idx] & (1 << bit_offset)) == 0 {
                return false;
            }
        }
        true
    }

    /// Encode to bytes for SSTable file embedding
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(5 + self.bit_vec.len());
        buf.put_u8(self.num_hashes);
        buf.put_u32(self.bit_vec.len() as u32);
        buf.put_slice(&self.bit_vec);
        buf.freeze()
    }

    /// Decode from bytes from SSTable file
    pub fn decode(mut buf: &[u8]) -> Option<Self> {
        if buf.len() < 5 {
            return None;
        }
        let num_hashes = buf.get_u8();
        let vec_len = buf.get_u32() as usize;
        if buf.len() < vec_len {
            return None;
        }
        let bit_vec = buf[..vec_len].to_vec();
        let num_bits = vec_len * 8;
        Some(Self {
            bit_vec,
            num_hashes,
            num_bits,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_accuracy() {
        let mut bf = BloomFilter::new(1000, 0.01);
        let mut inserted = Vec::new();

        for i in 0..1000u128 {
            let key = PlayerId::new(i);
            bf.insert(key);
            inserted.push(key);
        }

        // All inserted keys must return true
        for key in &inserted {
            assert!(bf.contains(*key));
        }

        // Test false positive rate on non-inserted keys
        let mut fp_count = 0;
        let test_count = 10000;
        for i in 1000..1000 + test_count {
            let key = PlayerId::new(i);
            if bf.contains(key) {
                fp_count += 1;
            }
        }

        let rate = fp_count as f64 / test_count as f64;
        assert!(rate < 0.03, "FP rate too high: {}", rate);
    }
}
