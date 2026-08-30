use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::core::types::{DbError, PlayerId, Result, ValueEntry};
use crate::storage::bloom_filter::BloomFilter;

pub const SSTABLE_MAGIC: u64 = 0x4d454f5753535431; // "MEOWSST1"
pub const DEFAULT_BLOCK_SIZE: usize = 16 * 1024; // 16 KB

/// Index entry pointing to a block on disk
#[derive(Debug, Clone)]
pub struct BlockMeta {
    pub first_key: PlayerId,
    pub offset: u64,
    pub length: u32,
}

/// SSTable file reader with in-memory metadata & Bloom Filter
pub struct SsTable {
    file: Arc<File>,
    path: PathBuf,
    id: u64,
    level: u32,
    min_key: PlayerId,
    max_key: PlayerId,
    entry_count: u64,
    block_index: Vec<BlockMeta>,
    bloom_filter: BloomFilter,
}

impl SsTable {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn level(&self) -> u32 {
        self.level
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

    /// Open an existing SSTable and load its index + Bloom Filter into memory
    pub fn open<P: AsRef<Path>>(path: P, id: u64, level: u32) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;
        let file_len = file.metadata()?.len();

        if file_len < 72 {
            return Err(DbError::SstableCorruption(format!(
                "SSTable file {} too small",
                path.display()
            )));
        }

        // Read footer (last 80 bytes)
        let footer_size = 80;
        file.seek(SeekFrom::End(-(footer_size as i64)))?;
        let mut footer_buf = [0u8; 80];
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
        let magic = buf.get_u64();

        if magic != SSTABLE_MAGIC {
            return Err(DbError::SstableCorruption(format!(
                "Invalid SSTable magic header {:016x} in {}",
                magic,
                path.display()
            )));
        }

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
            min_key,
            max_key,
            entry_count,
            block_index,
            bloom_filter,
        })
    }

    /// Fast lookup by key (thread-safe positional read)
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

        // 4. Positional read single data block (thread-safe, no seek contention)
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

        // 5. Scan/search inside block
        let mut slice = &block_buf[..];
        while slice.len() >= 33 {
            let mut k_bytes = [0u8; 16];
            slice.copy_to_slice(&mut k_bytes);
            let entry_key = PlayerId::from_bytes(k_bytes);
            let seq_num = slice.get_u64();
            let timestamp = slice.get_u64();
            let op_type_byte = slice.get_u8();
            let val_len = slice.get_u32() as usize;

            if slice.len() < val_len {
                break;
            }

            let val_bytes = if op_type_byte == 1 {
                let v = Bytes::copy_from_slice(&slice[..val_len]);
                slice.advance(val_len);
                Some(v)
            } else {
                slice.advance(val_len);
                None
            };

            if entry_key == *key {
                let entry = match op_type_byte {
                    1 => ValueEntry::put(val_bytes.unwrap_or_default(), seq_num, timestamp),
                    2 => ValueEntry::delete(seq_num, timestamp),
                    _ => return Err(DbError::SstableCorruption("Unknown op_type in SSTable".into())),
                };
                return Ok(Some(entry));
            } else if entry_key > *key {
                break;
            }
        }

        Ok(None)
    }

    /// Read all entries from this SSTable (used during compaction)
    pub fn read_all_entries(&self) -> Result<Vec<(PlayerId, ValueEntry)>> {
        let mut entries = Vec::with_capacity(self.entry_count as usize);

        for block_meta in &self.block_index {
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

            let mut slice = &block_buf[..];
            while slice.len() >= 33 {
                let mut k_bytes = [0u8; 16];
                slice.copy_to_slice(&mut k_bytes);
                let entry_key = PlayerId::from_bytes(k_bytes);
                let seq_num = slice.get_u64();
                let timestamp = slice.get_u64();
                let op_type_byte = slice.get_u8();
                let val_len = slice.get_u32() as usize;

                if slice.len() < val_len {
                    break;
                }

                let val_bytes = if op_type_byte == 1 {
                    let v = Bytes::copy_from_slice(&slice[..val_len]);
                    slice.advance(val_len);
                    Some(v)
                } else {
                    slice.advance(val_len);
                    None
                };

                let entry = match op_type_byte {
                    1 => ValueEntry::put(val_bytes.unwrap_or_default(), seq_num, timestamp),
                    2 => ValueEntry::delete(seq_num, timestamp),
                    _ => return Err(DbError::SstableCorruption("Unknown op_type in SSTable".into())),
                };
                entries.push((entry_key, entry));
            }
        }

        Ok(entries)
    }
}

/// Builder for constructing SSTable files
pub struct SsTableBuilder {
    path: PathBuf,
    id: u64,
    level: u32,
    target_block_size: usize,
}

impl SsTableBuilder {
    pub fn new<P: AsRef<Path>>(path: P, id: u64, level: u32) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            id,
            level,
            target_block_size: DEFAULT_BLOCK_SIZE,
        }
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
                let block_len = current_block.len() as u32;
                writer.write_all(&current_block)?;
                block_index.push(BlockMeta {
                    first_key: block_first_key.unwrap(),
                    offset: current_offset,
                    length: block_len,
                });
                current_offset += block_len as u64;
                current_block.clear();
                block_first_key = None;
            }
        }

        // Flush remaining block
        if !current_block.is_empty() {
            let block_len = current_block.len() as u32;
            writer.write_all(&current_block)?;
            block_index.push(BlockMeta {
                first_key: block_first_key.unwrap(),
                offset: current_offset,
                length: block_len,
            });
            current_offset += block_len as u64;
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

        // Write Footer: [Index Offset: 8B] [Index Len: 8B] [Bloom Offset: 8B] [Bloom Len: 8B] [Min Key: 16B] [Max Key: 16B] [Entry Count: 8B] [Magic: 8B]
        let mut footer = BytesMut::with_capacity(80);
        footer.put_u64(index_offset);
        footer.put_u64(index_len);
        footer.put_u64(bloom_offset);
        footer.put_u64(bloom_len);
        footer.put_slice(&min_key.to_bytes());
        footer.put_slice(&max_key.to_bytes());
        footer.put_u64(entry_count);
        footer.put_u64(SSTABLE_MAGIC);
        writer.write_all(&footer)?;

        writer.flush()?;
        writer.get_ref().sync_all()?;

        SsTable::open(self.path, self.id, self.level).map(Some)
    }
}
