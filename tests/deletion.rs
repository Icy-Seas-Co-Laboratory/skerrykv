use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use bytes::Bytes;
use skerrykv::{
    config::Config,
    coordinator::Coordinator,
    engine::{self, Store},
    server,
};
use std::{collections::BTreeMap, fs, path::Path, sync::Arc};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

fn config(dir: &TempDir) -> Config {
    let mut c = Config::default();
    c.storage.root = dir.path().join("store");
    c.resources.max_object_mib = 1;
    c.segments.max_size_bytes = 1024 * 1024 + 1024;
    c
}

fn snapshot(root: &Path) -> BTreeMap<String, Vec<u8>> {
    fn visit(root: &Path, dir: &Path, result: &mut BTreeMap<String, Vec<u8>>) {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, result);
            } else if path.file_name().unwrap() != "store.lock" {
                result.insert(
                    path.strip_prefix(root)
                        .unwrap()
                        .to_string_lossy()
                        .into_owned(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    visit(root, root, &mut result);
    result
}

#[test]
fn deletion_survives_restart_and_rebuild_and_reput_restores_object() {
    let dir = TempDir::new().unwrap();
    let c = config(&dir);
    let payload = Bytes::from_static(b"deleted then restored");
    let store = Store::open(&c).unwrap();
    let hash = store.put_many(std::slice::from_ref(&payload)).unwrap()[0].hash;
    store.seal().unwrap();
    let deleted = store.delete(&hash).unwrap();
    assert_eq!(deleted.hash, hash);
    assert!(deleted.deleted);
    assert!(!store.delete(&hash).unwrap().deleted);
    assert!(store.get(&hash).unwrap().is_none());
    drop(store);
    let store = Store::open(&c).unwrap();
    assert!(!store.exists(&hash).unwrap());
    drop(store);
    engine::rebuild(&c, false).unwrap();
    let store = Store::open(&c).unwrap();
    assert!(store.get(&hash).unwrap().is_none());
    let restored = store
        .put_many(std::slice::from_ref(&payload))
        .unwrap()
        .remove(0);
    assert_eq!(restored.hash, hash);
    assert!(!restored.duplicate);
    store.seal().unwrap();
    drop(store);
    engine::rebuild(&c, false).unwrap();
    let store = Store::open(&c).unwrap();
    assert_eq!(store.get(&hash).unwrap().unwrap(), payload);
}

#[test]
fn import_does_not_resurrect_destination_tombstone() {
    let src_dir = TempDir::new().unwrap();
    let dst_dir = TempDir::new().unwrap();
    let src = config(&src_dir);
    let dst = config(&dst_dir);
    let payload = Bytes::from_static(b"imported but deleted");
    let source = Store::open(&src).unwrap();
    let hash = source.put_many(std::slice::from_ref(&payload)).unwrap()[0].hash;
    let live = source
        .put_many(&[Bytes::from_static(b"new import")])
        .unwrap()[0]
        .hash;
    source.seal().unwrap();
    drop(source);
    let dest = Store::open(&dst).unwrap();
    dest.put_many(&[payload]).unwrap();
    assert!(dest.delete(&hash).unwrap().deleted);
    drop(dest);
    engine::import(&dst, &src.storage.root).unwrap();
    let dest = Store::open(&dst).unwrap();
    assert!(dest.get(&hash).unwrap().is_none());
    assert_eq!(dest.get(&live).unwrap().unwrap(), b"new import");
}

#[test]
fn vacuum_dry_run_preserves_files_and_vacuum_preserves_live_and_empty_objects() {
    let dir = TempDir::new().unwrap();
    let c = config(&dir);
    let store = Store::open(&c).unwrap();
    let results = store
        .put_many(&[
            Bytes::new(),
            Bytes::from_static(b"live"),
            Bytes::from_static(b"deleted"),
        ])
        .unwrap();
    store.seal().unwrap();
    assert!(store.delete(&results[2].hash).unwrap().deleted);
    drop(store);
    let before = snapshot(&c.storage.root);
    Store::vacuum(&c, true).unwrap();
    assert_eq!(snapshot(&c.storage.root), before);
    Store::vacuum(&c, false).unwrap();
    let store = Store::open(&c).unwrap();
    assert_eq!(store.get(&results[0].hash).unwrap().unwrap(), b"");
    assert_eq!(store.get(&results[1].hash).unwrap().unwrap(), b"live");
    assert!(store.get(&results[2].hash).unwrap().is_none());
    store.collect_garbage(true).unwrap();
    assert_eq!(store.get(&results[0].hash).unwrap().unwrap(), b"");
    assert_eq!(store.get(&results[1].hash).unwrap().unwrap(), b"live");
    assert!(store.get(&results[2].hash).unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cached_delete_is_visible_to_get_and_head_and_reput() {
    let dir = TempDir::new().unwrap();
    let mut c = config(&dir);
    c.cache.payload_mib = 1;
    let coordinator = Coordinator::open(c).await.unwrap();
    let app = server::router(coordinator.clone(), CancellationToken::new()).unwrap();
    let payload = Bytes::from_static(b"cache deletion");
    let hash = coordinator.put(payload.clone()).await.unwrap().hash;
    assert_eq!(coordinator.get(hash).await.unwrap().unwrap(), payload);
    assert_eq!(coordinator.get(hash).await.unwrap().unwrap(), payload);
    assert!(coordinator.delete(hash).await.unwrap().deleted);
    assert!(!coordinator.delete(hash).await.unwrap().deleted);
    let uri = format!("/v1/objects/{}", hex::encode(hash));
    for request in [Request::get(&uri), Request::head(&uri)] {
        let response = app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
    let restored = coordinator.put(payload.clone()).await.unwrap();
    assert_eq!(restored.hash, hash);
    assert_eq!(coordinator.get(hash).await.unwrap().unwrap(), payload);
    coordinator.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn garbage_collection_with_readers_and_writers_preserves_objects() {
    let dir = TempDir::new().unwrap();
    let coordinator = Coordinator::open(config(&dir)).await.unwrap();
    let empty = coordinator.put(Bytes::new()).await.unwrap().hash;
    let payload = Bytes::from_static(b"stable reader");
    let hash = coordinator.put(payload.clone()).await.unwrap().hash;
    let barrier = Arc::new(tokio::sync::Barrier::new(4));
    let reader = {
        let c = coordinator.clone();
        let b = barrier.clone();
        let payload = payload.clone();
        tokio::spawn(async move {
            b.wait().await;
            for _ in 0..20 {
                assert_eq!(c.get(hash).await.unwrap().unwrap(), payload);
            }
        })
    };
    let writer = {
        let c = coordinator.clone();
        let b = barrier.clone();
        tokio::spawn(async move {
            b.wait().await;
            let mut items = Vec::new();
            for n in 0..20 {
                let p = Bytes::from(format!("concurrent-{n}"));
                let h = c.put(p.clone()).await.unwrap().hash;
                items.push((h, p));
            }
            items
        })
    };
    let gc = {
        let c = coordinator.clone();
        let b = barrier.clone();
        tokio::spawn(async move {
            b.wait().await;
            c.collect_garbage(true).await.unwrap();
        })
    };
    barrier.wait().await;
    reader.await.unwrap();
    let items = writer.await.unwrap();
    gc.await.unwrap();
    for (h, p) in items {
        assert_eq!(coordinator.get(h).await.unwrap().unwrap(), p);
    }
    assert_eq!(coordinator.get(empty).await.unwrap().unwrap(), Bytes::new());
    coordinator.shutdown().await.unwrap();
}
