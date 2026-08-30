use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::core::types::{DbError, PlayerId, Result, ValueEntry};
use crate::storage::bloom_filter::BloomFilter;
use crate::storage::cache::BlockCache;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum CompressionType {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

impl CompressionType {
    pub fn parse(s: &str) -> Self {
        match s.trim().to_uppercase().as_str() {
            "ZSTD" | "ZSTANDARD" => CompressionType::Zstd,
            "NONE" | "OFF" | "RAW" => CompressionType::None,
            _ => CompressionType::Lz4,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            CompressionType::None => "NONE",
            CompressionType::Lz4 => "LZ4",
            CompressionType::Zstd => "ZSTD",
        }
    }
}

pub const SSTABLE_MAGIC_V1: u64 = 0x4d454f5753535431; // "MEOWSST1" (80B footer, uncompressed)
pub const SSTABLE_MAGIC_V2: u64 = 0x4d454f5753535432; // "MEOWSST2" (88B footer, uncompressed)
pub const SSTABLE_MAGIC_V3: u64 = 0x4d454f5753535433; // "MEOWSST3" (88B footer, LZ4 block compression)
pub const SSTABLE_MAGIC: u64 = SSTABLE_MAGIC_V3;
pub const DEFAULT_BLOCK_SIZE: usize = 32 * 1024; // 32 KB default block size

/// Index entry pointing to a block on disk
#[derive(Debug, Clone)]
pub struct BlockMeta {
    pub first_key: PlayerId,
    pub offset: u64,
    pub length: u32,
}

/// SSTable file reader with in-memory metadata & Bloom Filter
#[derive(Debug)]
pub struct SsTable {
    file: Arc<File>,
    path: PathBuf,
    id: u64,
    level: u32,
    version: u32,
    min_key: PlayerId,
    max_key: PlayerId,
    entry_count: u64,
    block_index: Vec<BlockMeta>,
    bloom_filter: BloomFilter,
    block_cache: Option<Arc<BlockCache>>,
}

impl SsTable {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn level(&self) -> u32 {
        self.level
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn is_legacy_version(&self) -> bool {
        self.version < 3
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn min_key(&self) -> PlayerId {
        self.min_key
    }

    pub fn max_key(&self) -> PlayerId {
        self.max_key
    }

    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Rewrite legacy SSTable to latest V3 format (with LZ4 compression) in place
    pub fn upgrade_in_place(&self) -> Result<Self> {
        let entries = self.read_all_entries()?;
        let temp_path = self.path.with_extension("sst.tmp");
        let builder = SsTableBuilder::new(&temp_path, self.id, self.level);
        if let Some(_) = builder.build(entries)? {
            std::fs::rename(&temp_path, &self.path)?;
            Self::open(&self.path, self.id, self.level)
        } else {
            Self::open(&self.path, self.id, self.level)
        }
    }

    /// Open an existing SSTable and load its index + Bloom Filter into memory
    pub fn open<P: AsRef<Path>>(path: P, id: u64, fallback_level: u32) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;
        let file_len = file.metadata()?.len();

        if file_len < 80 {
            return Err(DbError::SstableCorruption(format!(
                "SSTable file {} too small",
                path.display()
            )));
        }

        // 1. Read last 8 bytes to identify magic header version
        file.seek(SeekFrom::End(-8))?;
        let mut magic_buf = [0u8; 8];
        file.read_exact(&mut magic_buf)?;
        let magic = (&magic_buf[..]).get_u64();

        let (footer_size, has_level, version) = match magic {
            SSTABLE_MAGIC_V3 => (88, true, 3),
            SSTABLE_MAGIC_V2 => (88, true, 2),
            SSTABLE_MAGIC_V1 => (80, false, 1),
            _ => {
                return Err(DbError::SstableCorruption(format!(
                    "Invalid SSTable magic header {:016x} in {}",
                    magic,
                    path.display()
                )));
            }
        };

        if file_len < footer_size as u64 {
            return Err(DbError::SstableCorruption(format!(
                "SSTable file {} too small for footer",
                path.display()
            )));
        }

        file.seek(SeekFrom::End(-(footer_size as i64)))?;
        let mut footer_buf = vec![0u8; footer_size];
        file.read_exact(&mut footer_buf)?;

        let mut buf = &footer_buf[..];
        let index_offset = buf.get_u64();
        let index_len = buf.get_u64();
        let bloom_offset = buf.get_u64();
        let bloom_len = buf.get_u64();

        let mut min_key_bytes = [0u8; 16];
        buf.copy_to_slice(&mut min_key_bytes);
        let min_key = PlayerId::from_bytes(min_key_bytes);

        let mut max_key_bytes = [0u8; 16];
        buf.copy_to_slice(&mut max_key_bytes);
        let max_key = PlayerId::from_bytes(max_key_bytes);

        let entry_count = buf.get_u64();
        let level = if has_level {
            let l = buf.get_u32();
            let _reserved = buf.get_u32();
            l
        } else {
            fallback_level
        };

        // Read Bloom Filter
        file.seek(SeekFrom::Start(bloom_offset))?;
        let mut bloom_buf = vec![0u8; bloom_len as usize];
        file.read_exact(&mut bloom_buf)?;
        let bloom_filter = BloomFilter::decode(&bloom_buf).ok_or_else(|| {
            DbError::SstableCorruption("Failed to decode SSTable bloom filter".into())
        })?;

        // Read Block Index
        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_buf = vec![0u8; index_len as usize];
        file.read_exact(&mut index_buf)?;

        let mut index_reader = &index_buf[..];
        let num_blocks = index_reader.get_u32() as usize;
        let mut block_index = Vec::with_capacity(num_blocks);

        for _ in 0..num_blocks {
            let mut k_bytes = [0u8; 16];
            index_reader.copy_to_slice(&mut k_bytes);
            let first_key = PlayerId::from_bytes(k_bytes);
            let offset = index_reader.get_u64();
            let length = index_reader.get_u32();
            block_index.push(BlockMeta {
                first_key,
                offset,
                length,
            });
        }

        Ok(Self {
            file: Arc::new(file),
            path,
            id,
            level,
            version,
            min_key,
            max_key,
            entry_count,
            block_index,
            bloom_filter,
            block_cache: None,
        })
    }

    pub fn with_block_cache(mut self, cache: Option<Arc<BlockCache>>) -> Self {
        self.block_cache = cache;
        self
    }

    pub fn set_block_cache(&mut self, cache: Option<Arc<BlockCache>>) {
        self.block_cache = cache;
    }

    /// Positional read and automatic LZ4 decompression of a data block with LRU cache
    fn read_block_bytes(&self, block_meta: &BlockMeta) -> Result<Bytes> {
        if let Some(ref cache) = self.block_cache {
            if cache.is_enabled() {
                if let Some(cached) = cache.get(self.id, block_meta.offset) {
                    return Ok(cached);
                }
            }
        }

        let mut block_buf = vec![0u8; block_meta.length as usize];
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.read_exact_at(&mut block_buf, block_meta.offset)?;
        }
        #[cfg(not(unix))]
        {
            let mut file = File::open(&self.path)?;
            file.seek(SeekFrom::Start(block_meta.offset))?;
            file.read_exact(&mut block_buf)?;
        }

        let decompressed = if self.version >= 3 {
            if block_buf.is_empty() {
                return Ok(Bytes::new());
            }
            match block_buf[0] {
                1 => {
                    // LZ4 decompressed directly into Bytes
                    let decomp = lz4_flex::decompress_size_prepended(&block_buf[1..]).map_err(|e| {
                        DbError::SstableCorruption(format!("LZ4 decompression error: {}", e))
                    })?;
                    Bytes::from(decomp)
                }
                2 => {
                    // ZSTD decompressed directly into Bytes
                    let decomp = zstd::decode_all(&block_buf[1..]).map_err(|e| {
                        DbError::SstableCorruption(format!("ZSTD decompression error: {}", e))
                    })?;
                    Bytes::from(decomp)
                }
                0 => {
                    // Zero-copy slice of raw bytes
                    let mut b = Bytes::from(block_buf);
                    b.advance(1);
                    b
                }
                _ => return Err(DbError::SstableCorruption("Unknown compression type in SSTable block".into())),
            }
        } else {
            Bytes::from(block_buf)
        };

        if let Some(ref cache) = self.block_cache {
            if cache.is_enabled() {
                cache.insert(self.id, block_meta.offset, decompressed.clone());
            }
        }

        Ok(decompressed)
    }

    /// Fast lookup by key (thread-safe positional read with zero-copy value slicing)
    pub fn get(&self, key: &PlayerId) -> Result<Option<ValueEntry>> {
        // 1. Range check
        if *key < self.min_key || *key > self.max_key {
            return Ok(None);
        }

        // 2. Bloom Filter check (instant rejection without disk I/O)
        if !self.bloom_filter.contains(*key) {
            return Ok(None);
        }

        // 3. Binary search block index
        let block_idx = match self.block_index.binary_search_by(|b| b.first_key.cmp(key)) {
            Ok(idx) => idx,
            Err(0) => 0,
            Err(idx) => idx - 1,
        };

        let block_meta = &self.block_index[block_idx];

        // 4. Positional read and decompress block
        let block_bytes = self.read_block_bytes(block_meta)?;

        // 5. Zero-copy scan/search inside block
        let mut offset = 0;
        let total_len = block_bytes.len();

        while offset + 37 <= total_len {
            let mut k_bytes = [0u8; 16];
            k_bytes.copy_from_slice(&block_bytes[offset..offset + 16]);
            let entry_key = PlayerId::from_bytes(k_bytes);
            let seq_num = u64::from_be_bytes(block_bytes[offset + 16..offset + 24].try_into().unwrap());
            let timestamp = u64::from_be_bytes(block_bytes[offset + 24..offset + 32].try_into().unwrap());
            let op_type_byte = block_bytes[offset + 32];
            let val_len = u32::from_be_bytes(block_bytes[offset + 33..offset + 37].try_into().unwrap()) as usize;

            offset += 37;
            if offset + val_len > total_len {
                break;
            }

            if entry_key == *key {
                let entry = match op_type_byte {
                    1 => ValueEntry::put(block_bytes.slice(offset..offset + val_len), seq_num, timestamp),
                    2 => ValueEntry::delete(seq_num, timestamp),
                    _ => return Err(DbError::SstableCorruption("Unknown op_type in SSTable".into())),
                };
                return Ok(Some(entry));
            } else if entry_key > *key {
                break;
            }

            offset += val_len;
        }

        Ok(None)
    }

    /// Scan entries within a range [start_key, end_key] with zero-copy slicing
    pub fn scan_range(
        &self,
        start_key: Option<PlayerId>,
        end_key: Option<PlayerId>,
    ) -> Result<Vec<(PlayerId, ValueEntry)>> {
        // 1. Quick boundary check
        if let Some(start) = start_key {
            if start > self.max_key {
                return Ok(Vec::new());
            }
        }
        if let Some(end) = end_key {
            if end < self.min_key {
                return Ok(Vec::new());
            }
        }

        // 2. Binary search start block
        let start_block_idx = match start_key {
            Some(start) => match self.block_index.binary_search_by(|b| b.first_key.cmp(&start)) {
                Ok(idx) => idx,
                Err(0) => 0,
                Err(idx) => idx - 1,
            },
            None => 0,
        };

        let mut results = Vec::new();

        // 3. Iterate through relevant blocks
        for block_meta in &self.block_index[start_block_idx..] {
            if let Some(end) = end_key {
                if block_meta.first_key > end {
                    break;
                }
            }

            let block_bytes = self.read_block_bytes(block_meta)?;
            let mut offset = 0;
            let total_len = block_bytes.len();

            while offset + 37 <= total_len {
                let mut k_bytes = [0u8; 16];
                k_bytes.copy_from_slice(&block_bytes[offset..offset + 16]);
                let entry_key = PlayerId::from_bytes(k_bytes);
                let seq_num = u64::from_be_bytes(block_bytes[offset + 16..offset + 24].try_into().unwrap());
                let timestamp = u64::from_be_bytes(block_bytes[offset + 24..offset + 32].try_into().unwrap());
                let op_type_byte = block_bytes[offset + 32];
                let val_len = u32::from_be_bytes(block_bytes[offset + 33..offset + 37].try_into().unwrap()) as usize;

                offset += 37;
                if offset + val_len > total_len {
                    break;
                }

                // Check start bound
                if let Some(start) = start_key {
                    if entry_key < start {
                        offset += val_len;
                        continue;
                    }
                }

                // Check end bound
                if let Some(end) = end_key {
                    if entry_key > end {
                        return Ok(results);
                    }
                }

                let entry = match op_type_byte {
                    1 => ValueEntry::put(block_bytes.slice(offset..offset + val_len), seq_num, timestamp),
                    2 => ValueEntry::delete(seq_num, timestamp),
                    _ => return Err(DbError::SstableCorruption("Unknown op_type in SSTable".into())),
                };
                results.push((entry_key, entry));
                offset += val_len;
            }
        }

        Ok(results)
    }

    /// Read all entries from this SSTable (used during compaction and migration)
    pub fn read_all_entries(&self) -> Result<Vec<(PlayerId, ValueEntry)>> {
        let mut entries = Vec::with_capacity(self.entry_count as usize);

        for block_meta in &self.block_index {
            let block_bytes = self.read_block_bytes(block_meta)?;
            let mut offset = 0;
            let total_len = block_bytes.len();

            while offset + 37 <= total_len {
                let mut k_bytes = [0u8; 16];
                k_bytes.copy_from_slice(&block_bytes[offset..offset + 16]);
                let entry_key = PlayerId::from_bytes(k_bytes);
                let seq_num = u64::from_be_bytes(block_bytes[offset + 16..offset + 24].try_into().unwrap());
                let timestamp = u64::from_be_bytes(block_bytes[offset + 24..offset + 32].try_into().unwrap());
                let op_type_byte = block_bytes[offset + 32];
                let val_len = u32::from_be_bytes(block_bytes[offset + 33..offset + 37].try_into().unwrap()) as usize;

                offset += 37;
                if offset + val_len > total_len {
                    break;
                }

                let entry = match op_type_byte {
                    1 => ValueEntry::put(block_bytes.slice(offset..offset + val_len), seq_num, timestamp),
                    2 => ValueEntry::delete(seq_num, timestamp),
                    _ => return Err(DbError::SstableCorruption("Unknown op_type in SSTable".into())),
                };
                entries.push((entry_key, entry));
                offset += val_len;
            }
        }

        Ok(entries)
    }
}

/// Builder for constructing SSTable files with configurable block compression (NONE, LZ4, ZSTD)
pub struct SsTableBuilder {
    path: PathBuf,
    id: u64,
    level: u32,
    target_block_size: usize,
    compression: CompressionType,
}

impl SsTableBuilder {
    pub fn new<P: AsRef<Path>>(path: P, id: u64, level: u32) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            id,
            level,
            target_block_size: DEFAULT_BLOCK_SIZE,
            compression: CompressionType::Lz4,
        }
    }

    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.target_block_size = block_size;
        self
    }

    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.compression = if enabled { CompressionType::Lz4 } else { CompressionType::None };
        self
    }

    pub fn with_compression_type(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    /// Compress and write a single block to disk using selected compression algorithm
    fn write_block<W: Write>(
        &self,
        writer: &mut W,
        block_bytes: &[u8],
        first_key: PlayerId,
        block_index: &mut Vec<BlockMeta>,
        current_offset: &mut u64,
    ) -> Result<()> {
        let mut block_data = Vec::new();

        match self.compression {
            CompressionType::Lz4 => {
                let compressed = lz4_flex::compress_prepend_size(block_bytes);
                if compressed.len() < block_bytes.len() {
                    block_data.push(1u8); // 1 = LZ4
                    block_data.extend_from_slice(&compressed);
                } else {
                    block_data.push(0u8); // 0 = raw fallback
                    block_data.extend_from_slice(block_bytes);
                }
            }
            CompressionType::Zstd => {
                match zstd::encode_all(block_bytes, 3) {
                    Ok(compressed) if compressed.len() < block_bytes.len() => {
                        block_data.push(2u8); // 2 = ZSTD
                        block_data.extend_from_slice(&compressed);
                    }
                    _ => {
                        block_data.push(0u8); // 0 = raw fallback
                        block_data.extend_from_slice(block_bytes);
                    }
                }
            }
            CompressionType::None => {
                block_data.push(0u8); // 0 = raw (uncompressed)
                block_data.extend_from_slice(block_bytes);
            }
        }

        let block_len = block_data.len() as u32;
        writer.write_all(&block_data)?;
        block_index.push(BlockMeta {
            first_key,
            offset: *current_offset,
            length: block_len,
        });
        *current_offset += block_len as u64;
        Ok(())
    }

    /// Build SSTable from sorted entries iterator
    pub fn build<I>(self, entries: I) -> Result<Option<SsTable>>
    where
        I: IntoIterator<Item = (PlayerId, ValueEntry)>,
    {
        let entries: Vec<(PlayerId, ValueEntry)> = entries.into_iter().collect();
        if entries.is_empty() {
            return Ok(None);
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        let mut writer = BufWriter::new(file);

        let mut bloom_filter = BloomFilter::new(entries.len(), 0.01);
        let mut block_index: Vec<BlockMeta> = Vec::new();

        let min_key = entries[0].0;
        let max_key = entries.last().unwrap().0;
        let entry_count = entries.len() as u64;

        let mut current_block = BytesMut::with_capacity(self.target_block_size);
        let mut block_first_key: Option<PlayerId> = None;
        let mut current_offset = 0u64;

        for (key, entry) in &entries {
            bloom_filter.insert(*key);

            if block_first_key.is_none() {
                block_first_key = Some(*key);
            }

            // Write entry to block
            current_block.put_slice(&key.to_bytes());
            current_block.put_u64(entry.seq_num);
            current_block.put_u64(entry.timestamp);
            let op_type_byte = if entry.is_tombstone() { 2 } else { 1 };
            current_block.put_u8(op_type_byte);

            let val_slice = entry.value.as_deref().unwrap_or(&[]);
            current_block.put_u32(val_slice.len() as u32);
            current_block.put_slice(val_slice);

            // Flush block if threshold exceeded
            if current_block.len() >= self.target_block_size {
                self.write_block(
                    &mut writer,
                    &current_block,
                    block_first_key.unwrap(),
                    &mut block_index,
                    &mut current_offset,
                )?;
                current_block.clear();
                block_first_key = None;
            }
        }

        // Flush remaining block
        if !current_block.is_empty() {
            self.write_block(
                &mut writer,
                &current_block,
                block_first_key.unwrap(),
                &mut block_index,
                &mut current_offset,
            )?;
            current_block.clear();
        }

        // Write Block Index
        let index_offset = current_offset;
        let mut index_bytes = BytesMut::new();
        index_bytes.put_u32(block_index.len() as u32);
        for meta in &block_index {
            index_bytes.put_slice(&meta.first_key.to_bytes());
            index_bytes.put_u64(meta.offset);
            index_bytes.put_u32(meta.length);
        }
        let index_len = index_bytes.len() as u64;
        writer.write_all(&index_bytes)?;
        current_offset += index_len;

        // Write Bloom Filter
        let bloom_offset = current_offset;
        let bloom_bytes = bloom_filter.encode();
        let bloom_len = bloom_bytes.len() as u64;
        writer.write_all(&bloom_bytes)?;

        // Write Footer (88 bytes): [Index Offset: 8B] [Index Len: 8B] [Bloom Offset: 8B] [Bloom Len: 8B] [Min Key: 16B] [Max Key: 16B] [Entry Count: 8B] [Level: 4B] [Reserved: 4B] [Magic: 8B]
        let mut footer = BytesMut::with_capacity(88);
        footer.put_u64(index_offset);
        footer.put_u64(index_len);
        footer.put_u64(bloom_offset);
        footer.put_u64(bloom_len);
        footer.put_slice(&min_key.to_bytes());
        footer.put_slice(&max_key.to_bytes());
        footer.put_u64(entry_count);
        footer.put_u32(self.level);
        footer.put_u32(0); // reserved
        footer.put_u64(SSTABLE_MAGIC_V3);
        writer.write_all(&footer)?;

        writer.flush()?;
        writer.get_ref().sync_all()?;

        SsTable::open(self.path, self.id, self.level).map(Some)
    }
}
