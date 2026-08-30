use std::fmt;
use std::str::FromStr;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization/Deserialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Invalid UUID format: {0}")]
    InvalidUuid(String),
    #[error("WAL corruption detected: {0}")]
    WalCorruption(String),
    #[error("SSTable corruption detected: {0}")]
    SstableCorruption(String),
    #[error("Key not found")]
    KeyNotFound,
    #[error("Invalid command: {0}")]
    InvalidCommand(String),
    #[error("Index error: {0}")]
    IndexError(String),
}

pub type Result<T> = std::result::Result<T, DbError>;

/// 16-byte Player UUID represented as a u128 for ultra-fast comparison and indexing
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Default, Serialize, Deserialize)]
pub struct PlayerId(pub u128);

impl PlayerId {
    #[inline]
    pub const fn new(val: u128) -> Self {
        Self(val)
    }

    #[inline]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(u128::from_be_bytes(bytes))
    }

    #[inline]
    pub fn to_bytes(&self) -> [u8; 16] {
        self.0.to_be_bytes()
    }

    #[inline]
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid.as_u128())
    }

    #[inline]
    pub fn to_uuid(&self) -> Uuid {
        Uuid::from_u128(self.0)
    }

    pub fn parse(s: &str) -> Result<Self> {
        let trimmed = s.trim();
        // Support both hyphenated (36 chars) and non-hyphenated (32 hex chars)
        let parsed = if trimmed.len() == 32 {
            Uuid::parse_str(&format!(
                "{}-{}-{}-{}-{}",
                &trimmed[0..8],
                &trimmed[8..12],
                &trimmed[12..16],
                &trimmed[16..20],
                &trimmed[20..32]
            ))
        } else {
            Uuid::parse_str(trimmed)
        };

        match parsed {
            Ok(u) => Ok(Self::from_uuid(u)),
            Err(e) => Err(DbError::InvalidUuid(format!("Failed to parse UUID '{}': {}", s, e))),
        }
    }
}

impl fmt::Display for PlayerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_uuid())
    }
}

impl fmt::Debug for PlayerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PlayerId({})", self.to_uuid())
    }
}

impl FromStr for PlayerId {
    type Err = DbError;
    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

impl From<Uuid> for PlayerId {
    fn from(uuid: Uuid) -> Self {
        Self::from_uuid(uuid)
    }
}

impl From<u128> for PlayerId {
    fn from(val: u128) -> Self {
        Self(val)
    }
}

/// Operation type for WAL and MemTable
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum OpType {
    Put = 1,
    Delete = 2,
}

/// In-memory and on-disk entry
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueEntry {
    pub value: Option<Bytes>, // None indicates a deletion (Tombstone)
    pub seq_num: u64,
    pub timestamp: u64,
}

impl ValueEntry {
    pub fn put(value: Bytes, seq_num: u64, timestamp: u64) -> Self {
        Self {
            value: Some(value),
            seq_num,
            timestamp,
        }
    }

    pub fn delete(seq_num: u64, timestamp: u64) -> Self {
        Self {
            value: None,
            seq_num,
            timestamp,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }
}
