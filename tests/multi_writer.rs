//! Cross-component regressions for shard scheduling, maintenance, and recovery.
use bytes::Bytes;
use skerrykv::{config::Config, coordinator::Coordinator, engine::Store};
use std::{collections::BTreeMap, path::Path, time::Duration};
use tokio::task::JoinSet;

fn config(root: &Path, lanes: usize) -> Config {
    let mut config = Config::default();
    config.storage.root = root.join("store");
    config.storage.min_free_mib = 0;
    config.streaming.min_free_mib = 0;
    config.writes.writer_lanes = lanes;
    config.resources.max_object_mib = 1;
    config.resources.admission_timeout_ms = 60_000;
    config.segments.max_size_bytes = 1024 * 1024 + 1024;
    config.gc.delete_grace_seconds = 0;
    config.gc.cooldown_seconds = 0;
    config.gc.min_free_mib = 0;
    config.gc.max_copy_mib = 32;
    config.gc.max_bytes_per_second_mib = 1024;
    config.checkpoint.enabled = false;
    config
}
fn payload(key: usize, generation: usize) -> Bytes {
    let mut bytes = vec![(key % 251) as u8; 160 * 1024];
    bytes[..8].copy_from_slice(&(key as u64).to_le_bytes());
    bytes[8..16].copy_from_slice(&(generation as u64).to_le_bytes());
    Bytes::from(bytes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduled_mutations_survive_rollover_reopen_downshift_and_rebuild() {
    for lanes in [1, 2, 4, 8] {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path(), lanes);
        let coordinator = Coordinator::open(config.clone()).await.unwrap();
        let mut jobs = JoinSet::new();
        for key in 0..24 {
            let c = coordinator.clone();
            jobs.spawn(async move {
                let mut final_objects = Vec::new();
                for generation in 0..3 {
                    let bytes = payload(key, generation);
                    let first = c.put(bytes.clone()).await.unwrap();
                    assert!(!first.duplicate);
                    let duplicate = c.put(bytes.clone()).await.unwrap();
                    assert!(duplicate.duplicate);
                    assert_eq!(duplicate.hash, first.hash);
                    assert!(c.delete(first.hash).await.unwrap().deleted);
                    assert!(c.get(first.hash).await.unwrap().is_none());
                    assert!(!c.put(bytes.clone()).await.unwrap().duplicate);
                    final_objects.push((first.hash, bytes));
                }
                final_objects
            });
        }
        let mut expected = BTreeMap::new();
        tokio::time::timeout(Duration::from_secs(90), async {
            while let Some(result) = jobs.join_next().await {
                expected.extend(result.unwrap());
            }
        })
        .await
        .unwrap();
        let status = coordinator.status().await.unwrap();
        assert_eq!(status.storage.objects, expected.len() as u64);
        assert!(coordinator.is_ready());
        coordinator.shutdown().await.unwrap();
        drop(coordinator);
        // A changed lane count must retain the same namespace and all fences.
        let mut downshift = config.clone();
        downshift.writes.writer_lanes = 1;
        let store = Store::open(&downshift).unwrap();
        for (hash, bytes) in &expected {
            assert_eq!(store.get(hash).unwrap().unwrap(), bytes.as_ref());
        }
        store.seal().unwrap();
        drop(store);
        Store::rebuild(&downshift, false).unwrap();
        let store = Store::open(&downshift).unwrap();
        assert_eq!(store.status().unwrap().objects, expected.len() as u64);
        for (hash, bytes) in &expected {
            assert_eq!(store.get(hash).unwrap().unwrap(), bytes.as_ref());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_lanes_gc_and_checkpoint_preserve_deletion_fences() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path(), 4);
    let mut expected = BTreeMap::new();
    let mut deleted = Vec::new();
    {
        let store = Store::open(&config).unwrap();
        for key in 0..24 {
            let bytes = payload(key, 90);
            let hash = store.put_many(std::slice::from_ref(&bytes)).unwrap()[0].hash;
            if key % 2 == 0 {
                deleted.push(hash);
            } else {
                expected.insert(hash, bytes);
            }
        }
        store.seal().unwrap();
        for hash in &deleted {
            store.delete(hash).unwrap();
        }
    }
    let coordinator = Coordinator::open(config.clone()).await.unwrap();
    let mut jobs = JoinSet::new();
    for key in 100..116 {
        let c = coordinator.clone();
        jobs.spawn(async move {
            let mut added = Vec::new();
            for generation in 0..4 {
                let bytes = payload(key, generation);
                let result = c.put(bytes.clone()).await.unwrap();
                added.push((result.hash, bytes));
            }
            added
        });
    }
    // Both maintenance operations overlap ingestion and use the shared gate.
    tokio::time::timeout(Duration::from_secs(90), async {
        coordinator.checkpoint_journal(true).await.unwrap();
        coordinator.collect_garbage(true).await.unwrap();
        while let Some(result) = jobs.join_next().await {
            expected.extend(result.unwrap());
        }
    })
    .await
    .unwrap();
    coordinator.shutdown().await.unwrap();
    drop(coordinator);
    Store::rebuild(&config, false).unwrap();
    let store = Store::open(&config).unwrap();
    for hash in deleted {
        assert!(!store.exists(&hash).unwrap());
    }
    assert_eq!(store.status().unwrap().objects, expected.len() as u64);
    for (hash, bytes) in expected {
        assert_eq!(store.get(&hash).unwrap().unwrap(), bytes.as_ref());
    }
}

#[test]
#[ignore = "requires SKERRY_OLD_BINARY pointing to a pre-lane Skerry executable"]
fn previous_binary_rejects_multi_lane_store_without_modifying_files() {
    use std::{fs, process::Command};
    fn snapshot(root: &Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
        fn visit(root: &Path, path: &Path, result: &mut BTreeMap<std::path::PathBuf, Vec<u8>>) {
            for entry in std::fs::read_dir(path).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    visit(root, &path, result);
                } else {
                    result.insert(
                        path.strip_prefix(root).unwrap().into(),
                        std::fs::read(path).unwrap(),
                    );
                }
            }
        }
        let mut result = BTreeMap::new();
        visit(root, root, &mut result);
        result
    }
    let old = std::env::var_os("SKERRY_OLD_BINARY").expect("set SKERRY_OLD_BINARY");
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path(), 4);
    let hash = {
        let store = Store::open(&config).unwrap();
        assert_eq!(store.format_version(), 3);
        store
            .put_many(&[Bytes::from_static(b"must survive old-tool rejection")])
            .unwrap()[0]
            .hash
    };
    let mut old_config = toml::Value::try_from(&config).unwrap();
    for field in ["writer_lanes", "commit_max_delay_ms"] {
        old_config
            .get_mut("writes")
            .unwrap()
            .as_table_mut()
            .unwrap()
            .remove(field);
    }
    let config_path = config.storage.root.join("config.toml");
    fs::write(&config_path, toml::to_string(&old_config).unwrap()).unwrap();
    let before = snapshot(&config.storage.root);
    for args in [
        vec!["verify"],
        vec!["rebuild-index"],
        vec!["vacuum", "--dry-run"],
    ] {
        let output = Command::new(&old)
            .args(args)
            .arg("--path")
            .arg(&config.storage.root)
            .output()
            .unwrap();
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(
            !output.status.success(),
            "old tool unexpectedly accepted the store"
        );
        assert!(
            error.contains("activation"),
            "wrong rejection path: {error}"
        );
        assert!(
            !error.contains("unknown field"),
            "configuration must be accepted before the format rejection"
        );
        assert_eq!(snapshot(&config.storage.root), before);
    }
    let store = Store::open(&config).unwrap();
    assert_eq!(
        store.get(&hash).unwrap().unwrap(),
        b"must survive old-tool rejection"
    );
}
