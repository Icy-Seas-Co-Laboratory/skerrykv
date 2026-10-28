use bytes::Bytes;
use skerrykv::{
    config::Config,
    engine::{self, Store},
};
use std::{
    fs::{self, OpenOptions},
    io::{Seek, SeekFrom, Write},
    sync::Arc,
};
use tempfile::TempDir;

fn config(dir: &TempDir) -> Config {
    let mut c = Config::default();
    c.storage.root = dir.path().join("store");
    c
}
fn files(c: &Config) -> Vec<std::path::PathBuf> {
    let mut out = vec![];
    for e in fs::read_dir(c.storage.root.join("segments")).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            for e in fs::read_dir(p).unwrap() {
                let p = e.unwrap().path();
                if p.extension().is_some_and(|e| e == "seg") {
                    out.push(p)
                }
            }
        }
    }
    out.sort();
    out
}
/// Create an empty, valid legacy activation before writing legacy records.
/// No deletion history exists at this point, so this is a v1 fixture rather
/// than a downgrade of a store that has acknowledged v2 mutations.
fn initialize_legacy(c: &Config) {
    drop(Store::open(c).unwrap());
    let mut activations: Vec<_> = fs::read_dir(c.storage.root.join("index"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("activation-")
        })
        .collect();
    activations.sort();
    let activation = activations.last().unwrap();
    let bytes = fs::read(activation).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&bytes[..bytes.len() - 4]).unwrap();
    let journal = value["journal"].as_str().unwrap().to_owned();
    value["version"] = 1.into();
    value.as_object_mut().unwrap().remove("journal");
    let mut bytes = serde_json::to_vec(&value).unwrap();
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    fs::write(activation, bytes).unwrap();
    fs::remove_file(c.storage.root.join("format-v2")).unwrap();
    fs::remove_file(c.storage.root.join(journal)).unwrap();
}

#[test]
fn roundtrip_digest_empty_large_restart_and_dedup() {
    let d = TempDir::new().unwrap();
    let c = config(&d);
    let store = Store::open(&c).unwrap();
    for p in [
        Bytes::new(),
        Bytes::from_static(b"abc"),
        Bytes::from(vec![19; 2 * 1024 * 1024]),
    ] {
        let result = store.put_many(std::slice::from_ref(&p)).unwrap().remove(0);
        assert_eq!(result.hash, *blake3::hash(&p).as_bytes());
        assert!(!result.duplicate);
        assert_eq!(store.get(&result.hash).unwrap().unwrap(), p);
        assert!(store.put_many(&[p]).unwrap()[0].duplicate);
    }
    assert_eq!(
        hex::encode(blake3::hash(b"abc").as_bytes()),
        "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
    );
    assert_eq!(store.status().unwrap().objects, 3);
    let size = store.status().unwrap().physical_bytes;
    drop(store);
    let store = Store::open(&c).unwrap();
    assert_eq!(store.status().unwrap().objects, 3);
    assert_eq!(store.status().unwrap().physical_bytes, size);
    assert!(store.exists(blake3::hash(b"abc").as_bytes()).unwrap());
    assert!(!store.exists(blake3::hash(b"missing").as_bytes()).unwrap());
}
#[test]
fn concurrent_puts_and_reads_are_deduplicated() {
    let d = TempDir::new().unwrap();
    let c = config(&d);
    let store = Arc::new(Store::open(&c).unwrap());
    let mut handles = vec![];
    for i in 0..12 {
        let store = store.clone();
        handles.push(std::thread::spawn(move || {
            for n in 0..20 {
                let p = Bytes::from(format!("different-{i}-{n}"));
                let h = store
                    .put_many(&[p.clone(), Bytes::from_static(b"same")])
                    .unwrap()[0]
                    .hash;
                assert_eq!(store.get(&h).unwrap().unwrap(), p);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(store.status().unwrap().objects, 241);
}
#[test]
fn rotation_seal_rebuild_and_fd_limit() {
    let d = TempDir::new().unwrap();
    let mut c = config(&d);
    c.resources.max_object_mib = 1;
    c.segments.max_size_bytes = 1024 * 1024 + 1024;
    c.resources.max_open_segments = 1;
    let store = Store::open(&c).unwrap();
    let mut hashes = vec![];
    for i in 0..5 {
        let p = Bytes::from(vec![i; 700 * 1024]);
        hashes.push(store.put_many(&[p]).unwrap()[0].hash);
    }
    store.seal().unwrap();
    assert_eq!(store.status().unwrap().segments, 5);
    assert!(store.status().unwrap().active_segment.is_none());
    for (i, h) in hashes.iter().enumerate() {
        assert_eq!(store.get(h).unwrap().unwrap(), vec![i as u8; 700 * 1024]);
        assert!(store.status().unwrap().open_files <= 1);
    }
    drop(store);
    let report = engine::rebuild(&c, false).unwrap();
    assert_eq!(report["objects"], 5);
    let store = Store::open(&c).unwrap();
    for h in hashes {
        assert!(store.get(&h).unwrap().is_some());
    }
}
#[test]
fn lock_excludes_service_and_maintenance() {
    let d = TempDir::new().unwrap();
    let c = config(&d);
    let _store = Store::open(&c).unwrap();
    assert!(Store::open(&c).is_err());
    assert!(engine::verify(&c).is_err());
    assert!(engine::rebuild(&c, false).is_err());
}
#[test]
fn verifies_without_truncation_then_recovers_incomplete_tail() {
    let d = TempDir::new().unwrap();
    let c = config(&d);
    let store = Store::open(&c).unwrap();
    let h = store.put_many(&[Bytes::from_static(b"committed")]).unwrap()[0].hash;
    drop(store);
    let p = files(&c).remove(0);
    let logical = fs::metadata(&p).unwrap().len();
    OpenOptions::new()
        .append(true)
        .open(&p)
        .unwrap()
        .write_all(b"incomplete-header")
        .unwrap();
    engine::verify(&c).unwrap();
    assert_eq!(fs::metadata(&p).unwrap().len(), logical + 17);
    let store = Store::open(&c).unwrap();
    assert_eq!(fs::metadata(&p).unwrap().len(), logical);
    assert_eq!(store.get(&h).unwrap().unwrap(), b"committed");
}
#[test]
fn committed_payload_and_header_corruption_detected_on_read_and_verify() {
    for offset in [64 + 60, 128] {
        let d = TempDir::new().unwrap();
        let c = config(&d);
        let store = Store::open(&c).unwrap();
        let h = store.put_many(&[Bytes::from_static(b"committed")]).unwrap()[0].hash;
        drop(store);
        let p = files(&c).remove(0);
        let mut f = OpenOptions::new().write(true).open(&p).unwrap();
        f.seek(SeekFrom::Start(offset)).unwrap();
        f.write_all(&[0x85]).unwrap();
        drop(f);
        let store = Store::open(&c).unwrap();
        assert!(store.get(&h).is_err());
        drop(store);
        assert!(engine::verify(&c).is_err());
    }
}
#[test]
fn sealed_payload_corruption_is_detected_on_read_and_verify() {
    let d = TempDir::new().unwrap();
    let c = config(&d);
    let store = Store::open(&c).unwrap();
    let h = store.put_many(&[Bytes::from_static(b"committed")]).unwrap()[0].hash;
    store.seal().unwrap();
    drop(store);
    let p = files(&c).remove(0);
    let mut f = OpenOptions::new().write(true).open(&p).unwrap();
    f.seek(SeekFrom::Start(128)).unwrap();
    f.write_all(&[0x85]).unwrap();
    drop(f);
    let store = Store::open(&c).unwrap();
    assert!(store.get(&h).is_err());
    drop(store);
    assert!(engine::verify(&c).is_err());
}
#[test]
fn truncated_committed_record_is_not_silently_repaired() {
    let d = TempDir::new().unwrap();
    let c = config(&d);
    let store = Store::open(&c).unwrap();
    store.put_many(&[Bytes::from_static(b"committed")]).unwrap();
    drop(store);
    let p = files(&c).remove(0);
    let f = OpenOptions::new().write(true).open(&p).unwrap();
    f.set_len(130).unwrap();
    drop(f);
    assert!(Store::open(&c).is_err());
}
#[test]
fn strict_rebuild_excludes_active_salvage_recovers_verified_prefix() {
    let d = TempDir::new().unwrap();
    let mut c = config(&d);
    c.segments.growth_factor = 2;
    initialize_legacy(&c);
    let store = Store::open(&c).unwrap();
    let h = store
        .put_many(&[Bytes::from_static(b"salvage me")])
        .unwrap()[0]
        .hash;
    let physical = store.status().unwrap().physical_bytes;
    assert!(physical > 64 + 64 + 10);
    drop(store);
    let r = engine::rebuild(&c, false).unwrap();
    assert_eq!(r["objects"], 0);
    assert_eq!(r["excluded_active_segments"].as_array().unwrap().len(), 1);
    let r = engine::rebuild(&c, true).unwrap();
    assert_eq!(r["objects"], 1);
    let store = Store::open(&c).unwrap();
    assert_eq!(store.get(&h).unwrap().unwrap(), b"salvage me");
}
#[test]
fn legacy_upgrade_is_explicit_and_preserves_committed_objects() {
    let directory = TempDir::new().unwrap();
    let config = config(&directory);
    initialize_legacy(&config);
    let store = Store::open(&config).unwrap();
    assert!(!store.deletion_enabled());
    let hash = store
        .put_many(&[Bytes::from_static(b"legacy data")])
        .unwrap()[0]
        .hash;
    assert!(store.delete(&hash).is_err());
    assert_eq!(store.get(&hash).unwrap().unwrap(), b"legacy data");
    // Rejecting unsupported deletion must leave ordinary legacy writes usable.
    store
        .put_many(&[Bytes::from_static(b"still writable")])
        .unwrap();
    drop(store);
    Store::upgrade(&config).unwrap();
    let store = Store::open(&config).unwrap();
    assert!(store.deletion_enabled());
    assert_eq!(store.get(&hash).unwrap().unwrap(), b"legacy data");
    assert!(store.delete(&hash).unwrap().deleted);
    drop(store);
    Store::rebuild(&config, false).unwrap();
    assert!(!Store::open(&config).unwrap().exists(&hash).unwrap());
}
#[test]
fn sealed_import_deduplicates_preserves_source_and_is_idempotent() {
    let a = TempDir::new().unwrap();
    let b = TempDir::new().unwrap();
    let ca = config(&a);
    let cb = config(&b);
    let source = Store::open(&ca).unwrap();
    let results = source
        .put_many(&[Bytes::from_static(b"same"), Bytes::from_static(b"new")])
        .unwrap();
    source.seal().unwrap();
    drop(source);
    let source_bytes = fs::read(files(&ca).remove(0)).unwrap();
    let dest = Store::open(&cb).unwrap();
    dest.put_many(&[Bytes::from_static(b"same")]).unwrap();
    drop(dest);
    let report = engine::import(&cb, &ca.storage.root).unwrap();
    assert_eq!(report["imported_segments"], 1);
    assert_eq!(report["redundant_payload_bytes"], 4);
    let report = engine::import(&cb, &ca.storage.root).unwrap();
    assert_eq!(report["imported_segments"], 0);
    assert_eq!(report["redundant_records"], 1);
    assert_eq!(report["redundant_payload_bytes"], 4);
    let dest = Store::open(&cb).unwrap();
    assert_eq!(dest.status().unwrap().objects, 2);
    for r in results {
        assert!(dest.get(&r.hash).unwrap().is_some());
    }
    assert_eq!(fs::read(files(&ca).remove(0)).unwrap(), source_bytes);
}
#[test]
fn import_independent_export_and_reject_uuid_conflict() {
    let a = TempDir::new().unwrap();
    let b = TempDir::new().unwrap();
    let ca = config(&a);
    let cb = config(&b);
    let store = Store::open(&ca).unwrap();
    store.put_many(&[Bytes::from_static(b"payload")]).unwrap();
    store.seal().unwrap();
    drop(store);
    let bundle = TempDir::new().unwrap();
    let src = files(&ca).remove(0);
    let p = bundle.path().join(src.file_name().unwrap());
    fs::copy(&src, &p).unwrap();
    engine::import(&cb, bundle.path()).unwrap();
    // Change only the seal timestamp and repair footer CRC: valid independently,
    // same UUID/content digest, but not byte-identical to existing segment.
    let mut bytes = fs::read(&p).unwrap();
    let start = bytes.len() - 96;
    bytes[start + 44] ^= 1;
    let sum = crc32fast::hash(&bytes[start..bytes.len() - 4]);
    let len = bytes.len();
    bytes[len - 4..].copy_from_slice(&sum.to_le_bytes());
    fs::write(&p, bytes).unwrap();
    assert!(engine::import(&cb, bundle.path()).is_err());
}
#[test]
fn corrupted_seal_or_activation_is_not_accepted() {
    let d = TempDir::new().unwrap();
    let c = config(&d);
    let store = Store::open(&c).unwrap();
    store.put_many(&[Bytes::from_static(b"payload")]).unwrap();
    store.seal().unwrap();
    drop(store);
    let p = files(&c).remove(0);
    let mut bytes = fs::read(&p).unwrap();
    let len = bytes.len();
    bytes[len - 1] ^= 1;
    fs::write(&p, bytes).unwrap();
    assert!(Store::open(&c).is_err());
    assert!(engine::rebuild(&c, false).is_err());
    let d = TempDir::new().unwrap();
    let c = config(&d);
    drop(Store::open(&c).unwrap());
    let p = fs::read_dir(c.storage.root.join("index"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("activation-")
        })
        .unwrap();
    fs::write(p, b"torn").unwrap();
    assert!(Store::open(&c).is_err());
}
#[test]
fn verify_does_not_initialize_missing_store() {
    let d = TempDir::new().unwrap();
    let c = config(&d);
    assert!(engine::verify(&c).is_err());
    assert!(!c.storage.root.exists());
}

#[test]
fn duplicate_source_uuids_require_complete_file_equality() {
    let a = TempDir::new().unwrap();
    let b = TempDir::new().unwrap();
    let ca = config(&a);
    let cb = config(&b);
    let store = Store::open(&ca).unwrap();
    store.put_many(&[Bytes::from_static(b"payload")]).unwrap();
    store.seal().unwrap();
    drop(store);
    let bundle = TempDir::new().unwrap();
    fs::create_dir(bundle.path().join("a")).unwrap();
    fs::create_dir(bundle.path().join("b")).unwrap();
    let src = files(&ca).remove(0);
    let p1 = bundle.path().join("a").join(src.file_name().unwrap());
    let p2 = bundle.path().join("b").join(src.file_name().unwrap());
    fs::copy(&src, &p1).unwrap();
    fs::copy(&src, &p2).unwrap();
    let mut bytes = fs::read(&p2).unwrap();
    let start = bytes.len() - 96;
    bytes[start + 44] ^= 1;
    let sum = crc32fast::hash(&bytes[start..bytes.len() - 4]);
    let len = bytes.len();
    bytes[len - 4..].copy_from_slice(&sum.to_le_bytes());
    fs::write(p2, bytes).unwrap();
    assert!(engine::import(&cb, bundle.path()).is_err());
}
