use bytes::Bytes;
use skerrykv::{config::Config, engine::Store};
use std::{collections::BTreeMap, fs, path::Path};

fn config(root: &Path) -> Config {
    let mut config = Config::default();
    config.storage.root = root.join("store");
    config.gc.delete_grace_seconds = 0;
    config.gc.cooldown_seconds = 0;
    config.gc.min_free_mib = 0;
    config.gc.max_bytes_per_second_mib = 1024;
    config
}
fn snapshot(root: &Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
    fn visit(root: &Path, path: &Path, out: &mut BTreeMap<std::path::PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, out);
            } else {
                out.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    visit(root, root, &mut out);
    out
}

#[test]
fn deletion_survives_index_directory_loss_and_explicit_rebuild() {
    let directory = tempfile::tempdir().unwrap();
    let config = config(directory.path());
    let store = Store::open(&config).unwrap();
    let results = store
        .put_many(&[Bytes::from_static(b"remove"), Bytes::from_static(b"keep")])
        .unwrap();
    store.seal().unwrap();
    store.delete(&results[0].hash).unwrap();
    drop(store);
    fs::remove_dir_all(config.storage.root.join("index")).unwrap();
    Store::rebuild(&config, true).unwrap();
    let store = Store::open(&config).unwrap();
    assert!(!store.exists(&results[0].hash).unwrap());
    assert_eq!(store.get(&results[1].hash).unwrap().unwrap(), b"keep");
}

#[test]
fn collector_reclaims_deleted_payload_and_preserves_live_empty_object() {
    let directory = tempfile::tempdir().unwrap();
    let config = config(directory.path());
    let store = Store::open(&config).unwrap();
    let results = store
        .put_many(&[Bytes::from(vec![42; 128 * 1024]), Bytes::new()])
        .unwrap();
    store.seal().unwrap();
    store.delete(&results[0].hash).unwrap();
    let before = store.status().unwrap().physical_bytes;
    let result = store.collect_garbage(true).unwrap();
    assert_eq!(result["retired_segments"], 1);
    assert!(store.status().unwrap().physical_bytes < before);
    assert_eq!(
        store.get(&results[1].hash).unwrap().unwrap(),
        Vec::<u8>::new()
    );
    assert!(!store.exists(&results[0].hash).unwrap());
    drop(store);
    let store = Store::open(&config).unwrap();
    assert_eq!(store.status().unwrap().objects, 1);
    assert!(store.exists(&results[1].hash).unwrap());
}

#[test]
fn collector_leaves_active_and_grace_protected_segments_untouched() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = config(directory.path());
    config.gc.delete_grace_seconds = 86400;
    let store = Store::open(&config).unwrap();
    let hash = store.put_many(&[Bytes::from_static(b"garbage")]).unwrap()[0].hash;
    store.delete(&hash).unwrap();
    assert_eq!(store.collect_garbage(true).unwrap()["retired_segments"], 0);
    store.seal().unwrap();
    assert_eq!(store.collect_garbage(false).unwrap()["retired_segments"], 0);
    assert_eq!(store.collect_garbage(true).unwrap()["retired_segments"], 1);
    assert_eq!(store.status().unwrap().objects, 0);
}

#[test]
fn vacuum_dry_run_preserves_every_store_file_and_full_vacuum_preserves_fences() {
    let directory = tempfile::tempdir().unwrap();
    let config = config(directory.path());
    let source = config.storage.root.parent().unwrap().join("export");
    let source_config = Config {
        storage: skerrykv::config::Storage {
            root: source.clone(),
        },
        ..config.clone()
    };
    let source_store = Store::open(&source_config).unwrap();
    source_store
        .put_many(&[Bytes::from_static(b"deleted")])
        .unwrap();
    source_store.seal().unwrap();
    drop(source_store);
    let store = Store::open(&config).unwrap();
    let results = store
        .put_many(&[
            Bytes::from_static(b"deleted"),
            Bytes::from_static(b"survivor"),
            Bytes::new(),
        ])
        .unwrap();
    store.delete(&results[0].hash).unwrap();
    drop(store);
    let before = snapshot(&config.storage.root);
    let result = Store::vacuum(&config, true).unwrap();
    assert_eq!(result["objects"], 2);
    assert_eq!(before, snapshot(&config.storage.root));
    Store::vacuum(&config, false).unwrap();
    Store::import(&config, &source).unwrap();
    let store = Store::open(&config).unwrap();
    assert!(!store.exists(&results[0].hash).unwrap());
    assert_eq!(store.get(&results[1].hash).unwrap().unwrap(), b"survivor");
    assert!(store.exists(&results[2].hash).unwrap());
    assert!(!store.put_many(&[Bytes::from_static(b"deleted")]).unwrap()[0].duplicate);
}

#[test]
fn online_collection_preserves_concurrent_delete_and_recreate() {
    let directory = tempfile::tempdir().unwrap();
    let config = config(directory.path());
    let store = std::sync::Arc::new(Store::open(&config).unwrap());
    let payloads: Vec<_> = (0..64)
        .map(|index| Bytes::from(vec![index; 8192]))
        .collect();
    let results = store.put_many(&payloads).unwrap();
    store.seal().unwrap();
    for result in results.iter().take(32) {
        store.delete(&result.hash).unwrap();
    }
    let collecting = store.clone();
    let handle = std::thread::spawn(move || collecting.collect_garbage(true).unwrap());
    for result in results.iter().skip(32).take(16) {
        store.delete(&result.hash).unwrap();
    }
    for payload in payloads.iter().take(16) {
        store.put_many(std::slice::from_ref(payload)).unwrap();
    }
    handle.join().unwrap();
    for (index, result) in results.iter().enumerate() {
        assert_eq!(
            store.exists(&result.hash).unwrap(),
            !(16..48).contains(&index)
        );
    }
    store.sync().unwrap();
    drop(store);
    assert_eq!(Store::open(&config).unwrap().status().unwrap().objects, 32);
}

#[test]
fn importing_a_v2_store_preserves_its_source_deletions() {
    let directory = tempfile::tempdir().unwrap();
    let config = config(directory.path());
    let store = Store::open(&config).unwrap();
    let results = store
        .put_many(&[
            Bytes::from_static(b"source deleted"),
            Bytes::from_static(b"source live"),
        ])
        .unwrap();
    store.seal().unwrap();
    store.delete(&results[0].hash).unwrap();
    drop(store);
    let mut destination = config.clone();
    destination.storage.root = directory.path().join("destination");
    Store::import(&destination, &config.storage.root).unwrap();
    let store = Store::open(&destination).unwrap();
    assert!(!store.exists(&results[0].hash).unwrap());
    assert_eq!(
        store.get(&results[1].hash).unwrap().unwrap(),
        b"source live"
    );
}

#[test]
fn one_descriptor_gc_readers_and_deletes_do_not_deadlock() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = config(directory.path());
    config.resources.max_open_segments = 1;
    config.gc.max_bytes_per_second_mib = 1;
    let store = std::sync::Arc::new(Store::open(&config).unwrap());
    let results = store
        .put_many(&[
            Bytes::from(vec![1; 128 * 1024]),
            Bytes::from(vec![2; 128 * 1024]),
        ])
        .unwrap();
    store.seal().unwrap();
    let other = store
        .put_many(&[Bytes::from_static(b"other segment")])
        .unwrap()[0]
        .hash;
    store.seal().unwrap();
    store.delete(&results[0].hash).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let collecting = store.clone();
    let finished = tx.clone();
    std::thread::spawn(move || {
        collecting.collect_garbage(true).unwrap();
        finished.send(()).unwrap();
    });
    let reading = store.clone();
    let finished = tx.clone();
    std::thread::spawn(move || {
        for _ in 0..20 {
            assert!(reading.get(&other).unwrap().is_some());
        }
        finished.send(()).unwrap();
    });
    let deleting = store.clone();
    std::thread::spawn(move || {
        deleting.delete(&results[1].hash).unwrap();
        tx.send(()).unwrap();
    });
    for _ in 0..3 {
        rx.recv_timeout(std::time::Duration::from_secs(15))
            .expect("descriptor visibility lock deadlock");
    }
    assert!(store.get(&other).unwrap().is_some());
}

#[test]
fn forced_collection_still_obeys_copy_budget() {
    let directory = tempfile::tempdir().unwrap();
    let mut config = config(directory.path());
    config.gc.max_copy_mib = 1;
    config.resources.max_object_mib = 2;
    let store = Store::open(&config).unwrap();
    let results = store
        .put_many(&[
            Bytes::from(vec![1; 1200 * 1024]),
            Bytes::from_static(b"dead"),
        ])
        .unwrap();
    store.seal().unwrap();
    store.delete(&results[1].hash).unwrap();
    let result = store.collect_garbage(true).unwrap();
    assert_eq!(result["retired_segments"], 0);
    assert_eq!(result["copied_bytes"], 0);
    assert_eq!(
        store.get(&results[0].hash).unwrap().unwrap().len(),
        1200 * 1024
    );
}

#[test]
fn incomplete_journal_tail_is_ignored_but_complete_frame_corruption_fails_closed() {
    use std::io::Write;
    let directory = tempfile::tempdir().unwrap();
    let config = config(directory.path());
    let store = Store::open(&config).unwrap();
    let hash = store.put_many(&[Bytes::from_static(b"durable")]).unwrap()[0].hash;
    drop(store);
    let journal = fs::read_dir(&config.storage.root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| {
            p.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("journal-")
        })
        .unwrap();
    let committed_length = fs::metadata(&journal).unwrap().len();
    fs::OpenOptions::new()
        .append(true)
        .open(&journal)
        .unwrap()
        .write_all(b"partial")
        .unwrap();
    let before = fs::read(&journal).unwrap();
    Store::vacuum(&config, true).unwrap();
    assert_eq!(fs::read(&journal).unwrap(), before);
    let store = Store::open(&config).unwrap();
    assert!(store.exists(&hash).unwrap());
    drop(store);
    assert_eq!(fs::metadata(&journal).unwrap().len(), committed_length);
    let mut bytes = fs::read(&journal).unwrap();
    bytes[24] ^= 1;
    fs::write(&journal, bytes).unwrap();
    assert!(Store::open(&config).is_err());
    assert!(Store::rebuild(&config, true).is_err());
}

#[test]
fn legacy_v1_remains_writable_and_requires_explicit_upgrade_for_deletion() {
    let directory = tempfile::tempdir().unwrap();
    let config = config(directory.path());
    let store = Store::open(&config).unwrap();
    let original = store
        .put_many(&[Bytes::from_static(b"legacy active")])
        .unwrap()[0]
        .hash;
    drop(store);
    // Version-one records/index encodings are identical. Remove the v2
    // activation field and journal to create an old-format fixture.
    let activation = fs::read_dir(config.storage.root.join("index"))
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
    let bytes = fs::read(&activation).unwrap();
    let mut selected: serde_json::Value =
        serde_json::from_slice(&bytes[..bytes.len() - 4]).unwrap();
    selected["version"] = 1.into();
    selected.as_object_mut().unwrap().remove("journal");
    let mut bytes = serde_json::to_vec(&selected).unwrap();
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    fs::write(activation, bytes).unwrap();
    fs::remove_file(config.storage.root.join("format-v2")).unwrap();
    for entry in fs::read_dir(&config.storage.root).unwrap() {
        let path = entry.unwrap().path();
        if path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("journal-")
        {
            fs::remove_file(path).unwrap();
        }
    }
    let store = Store::open(&config).unwrap();
    assert!(!store.deletion_enabled());
    assert!(store.delete(&original).is_err());
    store
        .put_many(&[Bytes::from_static(b"legacy still writable")])
        .unwrap();
    assert!(store.exists(&original).unwrap());
    drop(store);
    assert_eq!(Store::upgrade(&config).unwrap()["version"], 2);
    let store = Store::open(&config).unwrap();
    assert!(store.deletion_enabled());
    assert!(store.delete(&original).unwrap().deleted);
    drop(store);
    Store::rebuild(&config, false).unwrap();
    assert!(!Store::open(&config).unwrap().exists(&original).unwrap());
}

#[test]
fn acknowledged_delete_journal_truncation_never_resurrects_at_or_between_boundaries() {
    for at_previous_boundary in [true, false] {
        let directory = tempfile::tempdir().unwrap();
        let config = config(directory.path());
        let store = Store::open(&config).unwrap();
        let hash = store
            .put_many(&[Bytes::from_static(b"must never resurrect")])
            .unwrap()[0]
            .hash;
        store.seal().unwrap();
        let journal = fs::read_dir(&config.storage.root)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| {
                p.file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with("journal-")
            })
            .unwrap();
        let before_delete = fs::metadata(&journal).unwrap().len();
        store.delete(&hash).unwrap();
        let after_delete = fs::metadata(&journal).unwrap().len();
        drop(store);
        let truncate_to = if at_previous_boundary {
            before_delete
        } else {
            before_delete + (after_delete - before_delete) / 2
        };
        fs::OpenOptions::new()
            .write(true)
            .open(&journal)
            .unwrap()
            .set_len(truncate_to)
            .unwrap();
        assert!(
            Store::open(&config).is_err(),
            "acknowledged journal truncation was accepted"
        );
        assert!(
            Store::rebuild(&config, true).is_err(),
            "rebuild resurrected an acknowledged deletion"
        );
        assert!(Store::verify(&config).is_err());
        assert!(Store::vacuum(&config, true).is_err());
    }
}
