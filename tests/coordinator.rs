use bytes::Bytes;
use skerrykv::{
    config::Config,
    coordinator::{Coordinator, ResourceExhausted},
};
use std::sync::Arc;

fn configuration(root: &std::path::Path) -> Config {
    let mut config = Config::default();
    config.storage.root = root.into();
    config.resources.max_object_mib = 1;
    config.writes.max_pending_mib = 2;
    config.resources.max_inflight_read_mib = 1;
    config.cache.payload_mib = 0;
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_duplicates_batching_reads_and_shutdown() {
    let root = tempfile::tempdir().unwrap();
    let config = configuration(root.path());
    let coordinator = Coordinator::open(config.clone()).await.unwrap();
    let mut jobs = Vec::new();
    for _ in 0..32 {
        let c = coordinator.clone();
        jobs.push(tokio::spawn(async move {
            c.put(Bytes::from_static(b"identical payload"))
                .await
                .unwrap()
        }));
    }
    let mut hashes = Vec::new();
    for job in jobs {
        hashes.push(job.await.unwrap().hash);
    }
    assert!(hashes.iter().all(|h| *h == hashes[0]));
    assert_eq!(coordinator.status().await.unwrap().storage.objects, 1);
    let mut jobs = Vec::new();
    for n in 0..32 {
        let c = coordinator.clone();
        jobs.push(tokio::spawn(async move {
            let p = Bytes::from(vec![n; 32 * 1024]);
            let r = c.put(p.clone()).await.unwrap();
            assert_eq!(c.get(r.hash).await.unwrap().unwrap(), p);
        }));
    }
    for job in jobs {
        job.await.unwrap();
    }
    assert_eq!(coordinator.status().await.unwrap().storage.objects, 33);
    coordinator.shutdown().await.unwrap();
    coordinator.shutdown().await.unwrap();
    assert!(coordinator.put(Bytes::from_static(b"late")).await.is_err());
    assert!(coordinator.metrics().contains("skerry_writes_total 64"));
    drop(coordinator);
    // Detached dispatcher exits on channel close; allow it to release store ownership.
    let restarted = loop {
        match Coordinator::open(config.clone()).await {
            Ok(c) => break c,
            Err(e) => {
                if !e.to_string().contains("locked") {
                    panic!("{e:#}");
                }
                tokio::task::yield_now().await;
            }
        }
    };
    assert_eq!(
        restarted.get(hashes[0]).await.unwrap().unwrap(),
        "identical payload"
    );
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn retained_response_memory_is_bounded_and_released() {
    let root = tempfile::tempdir().unwrap();
    let c = Coordinator::open(configuration(root.path())).await.unwrap();
    let first = c.put(Bytes::from(vec![1; 700 * 1024])).await.unwrap().hash;
    let second = c.put(Bytes::from(vec![2; 700 * 1024])).await.unwrap().hash;
    let held = c.get(first).await.unwrap().unwrap();
    let error = c.get(second).await.unwrap_err();
    assert!(
        error.downcast_ref::<ResourceExhausted>().is_some(),
        "{error:#}"
    );
    drop(held);
    assert_eq!(c.get(second).await.unwrap().unwrap().len(), 700 * 1024);
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn coalesced_reads_and_cache_reuse() {
    let root = tempfile::tempdir().unwrap();
    let mut config = configuration(root.path());
    config.cache.payload_mib = 1;
    config.reads.policy = "hdd".into();
    config.reads.hdd_window_ms = 20;
    let c = Coordinator::open(config).await.unwrap();
    let hash = c.put(Bytes::from(vec![42; 8192])).await.unwrap().hash;
    let barrier = Arc::new(tokio::sync::Barrier::new(17));
    let mut jobs = Vec::new();
    for _ in 0..16 {
        let c = c.clone();
        let b = barrier.clone();
        jobs.push(tokio::spawn(async move {
            b.wait().await;
            c.get(hash).await.unwrap().unwrap()
        }));
    }
    barrier.wait().await;
    for job in jobs {
        assert_eq!(job.await.unwrap().len(), 8192);
    }
    assert!(c.status().await.unwrap().io.read_coalesced > 0);
    drop(c.get(hash).await.unwrap());
    drop(c.get(hash).await.unwrap());
    assert!(c.status().await.unwrap().cache.hits > 0);
    c.cache_clear();
    assert_eq!(c.status().await.unwrap().cache.entries, 0);
    c.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_shutdown_can_resume_and_releases_worker_ownership() {
    let root = tempfile::tempdir().unwrap();
    let mut config = configuration(root.path());
    config.reads.policy = "hdd".into();
    config.reads.hdd_window_ms = 200;
    let c = Coordinator::open(config.clone()).await.unwrap();
    let hash = c.put(Bytes::from_static(b"drain read")).await.unwrap().hash;
    let reader = {
        let c = c.clone();
        tokio::spawn(async move { c.get(hash).await.unwrap().unwrap() })
    };
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), c.shutdown())
            .await
            .is_err()
    );
    c.shutdown().await.unwrap();
    assert_eq!(reader.await.unwrap(), "drain read");
    drop(c);
    // No lock retry loop is needed after worker shutdown.
    let reopened = Coordinator::open(config).await.unwrap();
    reopened.shutdown().await.unwrap();
}
