use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc32fast::Hasher;
use tokio::sync::{mpsc, oneshot};

use crate::core::types::{DbError, OpType, PlayerId, Result, ValueEntry};

pub const HEADER_SIZE: usize = 8; // CRC (4B) + Len (4B)
pub const RECORD_FIXED_SIZE: usize = 8 + 8 + 1 + 16; // Seq (8) + Time (8) + Op (1) + Key (16) = 33B

pub struct WalRecord {
    pub seq_num: u64,
    pub timestamp: u64,
    pub op_type: OpType,
    pub key: PlayerId,
    pub value: Option<Bytes>,
}

struct WriteRequest {
    records: Vec<WalRecord>,
    response_sender: oneshot::Sender<Result<()>>,
}

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub struct WalConfig {
    pub commit_delay_us: u64, // Microseconds window to gather concurrent writes (e.g. 1000us = 1ms)
    pub async_fsync: bool,    // If true, flush to kernel and sync in background
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            commit_delay_us: 1000, // 1ms default delay window
            async_fsync: false,    // Default OFF for 100% strict durability
        }
    }
}

pub struct WalWriter {
    request_tx: mpsc::Sender<WriteRequest>,
    next_seq_num: Arc<AtomicU64>,
    closed: Arc<AtomicBool>,
}

impl WalWriter {
    pub fn open<P: AsRef<Path>>(path: P, start_seq_num: u64, config: WalConfig) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        let (request_tx, mut request_rx) = mpsc::channel::<WriteRequest>(50_000);
        let next_seq_num = Arc::new(AtomicU64::new(start_seq_num));
        let closed = Arc::new(AtomicBool::new(false));

        let closed_clone = closed.clone();
        let delay_dur = Duration::from_micros(config.commit_delay_us);
        let async_fsync = config.async_fsync;

        std::thread::Builder::new()
            .name("fluxdb-wal-writer".to_string())
            .spawn(move || {
                let mut writer = BufWriter::with_capacity(1024 * 1024, file);
                let mut batch = Vec::with_capacity(2048);
                let mut last_async_sync = Instant::now();

                while !closed_clone.load(Ordering::Relaxed) {
                    batch.clear();
                    let first = match request_rx.blocking_recv() {
                        Some(req) => req,
                        None => break,
                    };
                    batch.push(first);

                    // Group Commit Delay Window: gather concurrent requests
                    let start_wait = Instant::now();
                    while start_wait.elapsed() < delay_dur && batch.len() < 8192 {
                        if let Ok(req) = request_rx.try_recv() {
                            batch.push(req);
                        } else {
                            std::thread::yield_now();
                        }
                    }

                    // Serialize all records
                    let mut write_err = None;
                    for req in &batch {
                        for record in &req.records {
                            let val_len = record.value.as_ref().map(|v| v.len()).unwrap_or(0);
                            let payload_len = (RECORD_FIXED_SIZE + val_len) as u32;

                            let mut payload = BytesMut::with_capacity(payload_len as usize);
                            payload.put_u64(record.seq_num);
                            payload.put_u64(record.timestamp);
                            payload.put_u8(match record.op_type {
                                OpType::Put => 1,
                                OpType::Delete => 2,
                            });
                            payload.put_slice(&record.key.to_bytes());
                            if let Some(val) = &record.value {
                                payload.put_slice(val);
                            }

                            let mut hasher = Hasher::new();
                            hasher.update(&payload);
                            let crc = hasher.finalize();

                            if let Err(e) = writer.write_all(&crc.to_be_bytes()) {
                                write_err = Some(e);
                                break;
                            }
                            if let Err(e) = writer.write_all(&payload_len.to_be_bytes()) {
                                write_err = Some(e);
                                break;
                            }
                            if let Err(e) = writer.write_all(&payload) {
                                write_err = Some(e);
                                break;
                            }
                        }
                        if write_err.is_some() {
                            break;
                        }
                    }

                    let res = if let Some(e) = write_err {
                        Err(DbError::Io(e))
                    } else if async_fsync {
                        // In async_fsync mode, flush buffer to kernel instantly and sync every 500ms
                        let flush_res = writer.flush().map_err(DbError::Io);
                        if last_async_sync.elapsed() >= Duration::from_millis(500) {
                            let _ = writer.get_ref().sync_data();
                            last_async_sync = Instant::now();
                        }
                        flush_res
                    } else {
                        // Strict 100% Durability: fsync every group commit batch
                        match writer.flush().and_then(|_| writer.get_ref().sync_data()) {
                            Ok(_) => Ok(()),
                            Err(e) => Err(DbError::Io(e)),
                        }
                    };

                    for req in batch.drain(..) {
                        let _ = req.response_sender.send(match &res {
                            Ok(_) => Ok(()),
                            Err(_) => Err(DbError::WalCorruption("Failed to write to WAL".into())),
                        });
                    }
                }
            })
            .expect("Failed to spawn WAL background thread");

        Ok(Self {
            request_tx,
            next_seq_num,
            closed,
        })
    }

    pub async fn append_batch(&self, mut records: Vec<(PlayerId, Option<Bytes>, OpType)>) -> Result<(u64, u64)> {
        let count = records.len();
        if count == 0 {
            let seq = self.next_seq_num.load(Ordering::SeqCst);
            return Ok((seq, 0));
        }

        let start_seq = self.next_seq_num.fetch_add(count as u64, Ordering::SeqCst);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let wal_records = records
            .drain(..)
            .enumerate()
            .map(|(i, (key, value, op_type))| WalRecord {
                seq_num: start_seq + i as u64,
                timestamp,
                op_type,
                key,
                value,
            })
            .collect::<Vec<_>>();

        let (tx, rx) = oneshot::channel();
        self.request_tx
            .send(WriteRequest {
                records: wal_records,
                response_sender: tx,
            })
            .await
            .map_err(|_| DbError::WalCorruption("WAL background writer stopped".into()))?;

        rx.await
            .map_err(|_| DbError::WalCorruption("WAL write dropped".into()))??;

        Ok((start_seq, timestamp))
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq_num.load(Ordering::SeqCst)
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}

pub struct WalRecovery;

impl WalRecovery {
    pub fn recover<P: AsRef<Path>>(path: P) -> Result<(Vec<(PlayerId, ValueEntry)>, u64)> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok((Vec::new(), 1));
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();
        let mut max_seq = 0u64;

        let mut header_buf = [0u8; HEADER_SIZE];
        loop {
            match reader.read_exact(&mut header_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(DbError::Io(e)),
            }

            let expected_crc = u32::from_be_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
            let payload_len = u32::from_be_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]]) as usize;

            if payload_len < RECORD_FIXED_SIZE {
                return Err(DbError::WalCorruption("Corrupt WAL payload length".into()));
            }

            let mut payload = vec![0u8; payload_len];
            reader.read_exact(&mut payload)?;

            let mut hasher = Hasher::new();
            hasher.update(&payload);
            let actual_crc = hasher.finalize();

            if actual_crc != expected_crc {
                return Err(DbError::WalCorruption(format!(
                    "CRC mismatch at WAL record! Expected {:08x}, got {:08x}",
                    expected_crc, actual_crc
                )));
            }

            let mut buf = &payload[..];
            let seq_num = buf.get_u64();
            let timestamp = buf.get_u64();
            let op_type_byte = buf.get_u8();
            let mut key_bytes = [0u8; 16];
            buf.copy_to_slice(&mut key_bytes);
            let key = PlayerId::from_bytes(key_bytes);

            let val_bytes = if buf.has_remaining() {
                Some(Bytes::copy_from_slice(buf))
            } else {
                None
            };

            let entry = match op_type_byte {
                1 => ValueEntry::put(val_bytes.unwrap_or_default(), seq_num, timestamp),
                2 => ValueEntry::delete(seq_num, timestamp),
                _ => return Err(DbError::WalCorruption("Unknown op_type in WAL".into())),
            };

            if seq_num > max_seq {
                max_seq = seq_num;
            }
            entries.push((key, entry));
        }

        Ok((entries, max_seq + 1))
    }
}
