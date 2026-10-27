use bytes::Bytes;
use skerrykv::{config::Config, engine::Store};
use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use uuid::Uuid;

fn config(dir: &tempfile::TempDir) -> Config {
    let mut config = Config::default();
    config.storage.root = dir.path().join("store");
    config.resources.max_object_mib = 1;
    config.segments.max_size_bytes = 2 * 1024 * 1024;
    config.resources.max_open_segments = 1;
    config
}

#[test]
fn segment_pages_validate_cursors_and_reveal_no_paths() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(&dir);
    let store = Store::open(&config).unwrap();
    assert!(store.list_segments(None, 100).unwrap().segments.is_empty());
    for byte in 1..=5 {
        store.put_many(&[Bytes::from(vec![byte; 1024])]).unwrap();
        store.seal().unwrap();
    }
    let first = store.list_segments(None, 2).unwrap();
    assert_eq!(first.segments.len(), 2);
    let second = store
        .list_segments(first.next_cursor.as_deref(), 2)
        .unwrap();
    assert_eq!(second.segments.len(), 2);
    let third = store
        .list_segments(second.next_cursor.as_deref(), 2)
        .unwrap();
    assert_eq!(third.segments.len(), 1);
    assert!(third.next_cursor.is_none());
    let mut ids = first
        .segments
        .iter()
        .chain(&second.segments)
        .chain(&third.segments)
        .map(|s| s.segment_id.clone())
        .collect::<Vec<_>>();
    let all = store.list_segments(None, 100).unwrap();
    assert_eq!(
        all.segments
            .iter()
            .map(|s| &s.segment_id)
            .collect::<Vec<_>>(),
        ids.iter().collect::<Vec<_>>()
    );
    let original = ids.clone();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 5);
    assert_eq!(ids, original);
    assert!(store.list_segments(None, 0).is_err());
    assert!(store.list_segments(None, 1001).is_err());
    for bad in [
        "",
        "v2:00000000000040008000000000000000",
        "v1:abcd",
        "v1:../../secret",
        "v1:00000000000000000000000000000000",
        "v1:FFFFFFFFFFFF4FFF8FFFFFFFFFFFFFFF",
    ] {
        assert!(store.list_segments(Some(bad), 2).is_err(), "{bad}");
    }
    let unknown = format!("v1:{}", Uuid::new_v4().simple());
    assert!(store.list_segments(Some(&unknown), 2).is_err());
    let encoded = serde_json::to_string(&all).unwrap();
    assert!(!encoded.contains("path"));
    assert!(!encoded.contains(&config.storage.root.display().to_string()));
    assert!(all.segments.iter().all(|s| s.state == "sealed"
        && s.whole_segment_digest.is_some()
        && s.sealed_timestamp.is_some()));
}

#[test]
fn object_inspection_and_online_prefix_seal_verification() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(&dir);
    let store = Store::open(&config).unwrap();
    let hash = store
        .put_many(&[Bytes::from_static(b"inspect me")])
        .unwrap()[0]
        .hash;
    let info = store.object_info(hash).unwrap();
    assert!(info.exists);
    assert_eq!(info.payload_length, Some(10));
    assert_eq!(info.hash, hex::encode(hash));
    assert_eq!(info.payload_offset, Some(128));
    let missing = store.object_info([0; 32]).unwrap();
    assert!(!missing.exists);
    assert!(missing.segment_id.is_none());
    assert!(missing.payload_length.is_none());
    let id = Uuid::parse_str(info.segment_id.as_ref().unwrap()).unwrap();
    assert!(store.segment_info(Uuid::new_v4()).unwrap().is_none());
    let progress = Arc::new(AtomicU64::new(0));
    let report = store.verify_segment(id, progress.clone()).unwrap();
    assert!(!report.sealed);
    assert_eq!(report.object_count, 1);
    assert_eq!(report.verified_bytes, 138);
    assert_eq!(progress.load(Ordering::Acquire), 138);
    store.seal().unwrap();
    let report = store.verify_segment(id, progress.clone()).unwrap();
    assert!(report.sealed);
    assert_eq!(report.verified_bytes, 234);
    assert_eq!(store.status().unwrap().open_files, 1);
    let cancelled = Arc::new(AtomicBool::new(true));
    assert!(
        store
            .verify_segment_bounded(id, progress.clone(), cancelled, Duration::from_secs(10))
            .is_err()
    );
    assert!(
        store
            .verify_segment_bounded(
                id,
                progress.clone(),
                Arc::new(AtomicBool::new(false)),
                Duration::ZERO
            )
            .is_err()
    );
    assert!(
        store
            .verify_segment_bounded(
                id,
                progress.clone(),
                Arc::new(AtomicBool::new(false)),
                Duration::from_nanos(1)
            )
            .is_err()
    );
    assert!(store.verify_segment(Uuid::new_v4(), progress).is_err());
}

#[test]
fn verification_rejects_corruption_without_changing_committed_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(&dir);
    let store = Store::open(&config).unwrap();
    let hash = store.put_many(&[Bytes::from(vec![8; 4096])]).unwrap()[0].hash;
    store.seal().unwrap();
    let info = store.object_info(hash).unwrap();
    let id = Uuid::parse_str(info.segment_id.as_ref().unwrap()).unwrap();
    let path = config
        .storage
        .root
        .join("segments")
        .join(&id.simple().to_string()[..2])
        .join(format!("{id}.seg"));
    let original_len = std::fs::metadata(&path).unwrap().len();
    let mut file = OpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::Start(info.payload_offset.unwrap() + 100))
        .unwrap();
    file.write_all(&[9]).unwrap();
    file.sync_all().unwrap();
    assert!(
        store
            .verify_segment(id, Arc::new(AtomicU64::new(0)))
            .is_err()
    );
    assert_eq!(std::fs::metadata(&path).unwrap().len(), original_len);
    assert_eq!(store.segment_info(id).unwrap().unwrap().state, "sealed");
}

#[test]
fn verification_and_inspection_allow_concurrent_ingestion() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(&dir);
    let store = Arc::new(Store::open(&config).unwrap());
    let hash = store.put_many(&[Bytes::from(vec![1; 900 * 1024])]).unwrap()[0].hash;
    let id = Uuid::parse_str(
        store
            .object_info(hash)
            .unwrap()
            .segment_id
            .as_ref()
            .unwrap(),
    )
    .unwrap();
    let reader = store.clone();
    let verify = std::thread::spawn(move || {
        reader
            .verify_segment(id, Arc::new(AtomicU64::new(0)))
            .unwrap()
    });
    for n in 2..=12 {
        store.put_many(&[Bytes::from(vec![n; 64 * 1024])]).unwrap();
        let info = store.object_info(hash).unwrap();
        assert!(info.exists);
        assert_eq!(store.list_segments(None, 2).unwrap().segments.len(), 1);
    }
    let report = verify.join().unwrap();
    assert!(report.object_count >= 1);
    assert_eq!(store.get(&hash).unwrap().unwrap().len(), 900 * 1024);
    assert_eq!(store.status().unwrap().objects, 12);
}
