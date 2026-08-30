use bytes::Bytes;
use tempfile::tempdir;
use uuid::Uuid;

use fluxdb::core::types::{OpType, PlayerId, ValueEntry};
use fluxdb::index::IndexManager;
use fluxdb::storage::sstable::SsTableBuilder;
use fluxdb::storage::wal::{WalConfig, WalRecovery, WalWriter};
use fluxdb::storage::{EngineConfig, StorageEngine};

#[tokio::test]
async fn test_wal_write_and_recovery() {
    let dir = tempdir().unwrap();
    let wal_path = dir.path().join("wal.log");

    let p1 = PlayerId::from_uuid(Uuid::new_v4());
    let p2 = PlayerId::from_uuid(Uuid::new_v4());

    // 1. Write records to WAL
    {
        let wal = WalWriter::open(&wal_path, 1, WalConfig::default()).unwrap();
        wal.append_batch(vec![
            (p1, Some(Bytes::from_static(b"{\"kills\": 10}")), OpType::Put),
            (p2, Some(Bytes::from_static(b"{\"kills\": 25}")), OpType::Put),
        ])
        .await
        .unwrap();

        wal.append_batch(vec![(p1, None, OpType::Delete)])
            .await
            .unwrap();

        wal.close();
    }

    // 2. Recover from WAL
    let (entries, next_seq) = WalRecovery::recover(&wal_path).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(next_seq, 4);

    // Latest state of p1 should be deleted, p2 has value
    let p1_entry = entries.iter().filter(|(k, _)| *k == p1).last().unwrap().1.clone();
    let p2_entry = entries.iter().filter(|(k, _)| *k == p2).last().unwrap().1.clone();

    assert!(p1_entry.is_tombstone());
    assert_eq!(p2_entry.value, Some(Bytes::from_static(b"{\"kills\": 25}")));
}

#[tokio::test]
async fn test_sstable_build_and_lookup() {
    let dir = tempdir().unwrap();
    let sst_path = dir.path().join("000001.sst");

    let mut entries = Vec::new();
    for i in 0..1000u128 {
        let key = PlayerId::new(i * 10);
        let val = Bytes::from(format!("{{\"index\": {}}}", i));
        entries.push((key, ValueEntry::put(val, (i + 1) as u64, 1000)));
    }

    let builder = SsTableBuilder::new(&sst_path, 1, 0);
    let sst = builder.build(entries).unwrap().unwrap();

    // Verify existing keys
    for i in 0..1000u128 {
        let key = PlayerId::new(i * 10);
        let found = sst.get(&key).unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().value.unwrap(), Bytes::from(format!("{{\"index\": {}}}", i)));
    }

    // Verify non-existing keys (odd numbers)
    for i in 0..500u128 {
        let non_existing = PlayerId::new(i * 10 + 1);
        let found = sst.get(&non_existing).unwrap();
        assert!(found.is_none());
    }
}

#[tokio::test]
async fn test_engine_put_get_delete_flush() {
    let dir = tempdir().unwrap();
    let config = EngineConfig {
        db_path: dir.path().to_path_buf(),
        memtable_max_bytes: 4096,
        l0_compaction_trigger: 4,
        wal_config: WalConfig::default(),
    };

    let engine = StorageEngine::open(config).await.unwrap();

    let p1 = PlayerId::from_uuid(Uuid::new_v4());
    let p2 = PlayerId::from_uuid(Uuid::new_v4());

    engine.put(p1, Bytes::from_static(b"{\"score\": 100}")).await.unwrap();
    engine.put(p2, Bytes::from_static(b"{\"score\": 200}")).await.unwrap();

    // Verify GET
    assert_eq!(engine.get(&p1).unwrap(), Some(Bytes::from_static(b"{\"score\": 100}")));
    assert_eq!(engine.get(&p2).unwrap(), Some(Bytes::from_static(b"{\"score\": 200}")));

    // Force flush to SSTable
    engine.force_flush().await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Verify GET still works after flushed to disk
    assert_eq!(engine.get(&p1).unwrap(), Some(Bytes::from_static(b"{\"score\": 100}")));

    // Delete p1
    engine.delete(p1).await.unwrap();
    assert_eq!(engine.get(&p1).unwrap(), None);
    assert_eq!(engine.get(&p2).unwrap(), Some(Bytes::from_static(b"{\"score\": 200}")));
}

#[test]
fn test_secondary_index_ranking() {
    let manager = IndexManager::new();
    let index = manager.create_index("stats.kills");

    let p1 = PlayerId::new(101);
    let p2 = PlayerId::new(102);
    let p3 = PlayerId::new(103);

    manager.on_put(p1, br#"{"stats": {"kills": 50}}"#);
    manager.on_put(p2, br#"{"stats": {"kills": 350}}"#);
    manager.on_put(p3, br#"{"stats": {"kills": 150}}"#);

    let top = index.get_top(10);
    assert_eq!(top.len(), 3);
    assert_eq!(top[0].0, p2); // 350 kills -> Rank 1
    assert_eq!(top[1].0, p3); // 150 kills -> Rank 2
    assert_eq!(top[2].0, p1); // 50 kills  -> Rank 3

    // Update p1's score to 500 (becomes Rank 1)
    manager.on_put(p1, br#"{"stats": {"kills": 500}}"#);
    let top_after = index.get_top(10);
    assert_eq!(top_after[0].0, p1);
    assert_eq!(top_after[0].1, 500.0);

    let rank_p1 = index.get_rank(&p1).unwrap();
    assert_eq!(rank_p1.0, 1);
    assert_eq!(rank_p1.1, 500.0);
}
