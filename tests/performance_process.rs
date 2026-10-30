//! Process-abort tests exercise recovery after process death, not power loss.
use bytes::Bytes;
use skerrykv::{
    config::Config,
    engine::{self, Store},
};
use std::{
    process::{Command, Stdio},
    sync::Arc,
};

const LIVE: &[u8] = b"acknowledged live object";
const DELETED: &[u8] = b"acknowledged deleted object";
static UPLOAD_CHUNK: [u8; 64 * 1024] = [0x7b; 64 * 1024];
const UPLOAD_CHUNKS: usize = 145;
fn upload_hash() -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    for _ in 0..UPLOAD_CHUNKS {
        h.update(&UPLOAD_CHUNK);
    }
    *h.finalize().as_bytes()
}

fn config(path: &std::path::Path) -> Config {
    let mut c = Config::default();
    c.storage.root = path.into();
    c.streaming.min_free_mib = 0;
    c
}

#[test]
fn crash_worker() {
    let Some(root) = std::env::var_os("SKERRY_PERF_CRASH_ROOT") else {
        return;
    };
    let mode = std::env::var("SKERRY_PERF_CRASH_MODE").unwrap();
    let mut cfg = config(root.as_ref());
    if mode == "lanes" {
        cfg.writes.writer_lanes = 4;
    }
    let store = Arc::new(Store::open(&cfg).unwrap());
    if mode == "upload" {
        let mut upload = store
            .begin_upload((UPLOAD_CHUNK.len() * UPLOAD_CHUNKS) as u64, None)
            .unwrap();
        for _ in 0..UPLOAD_CHUNKS {
            upload.write_chunk(&UPLOAD_CHUNK).unwrap();
        }
        upload.finish().unwrap();
    } else if mode == "lanes" {
        let start = Arc::new(std::sync::Barrier::new(4));
        let mut threads = Vec::new();
        for lane in 0..4 {
            let store = store.clone();
            let start = start.clone();
            threads.push(std::thread::spawn(move || {
                start.wait();
                for sequence in 0..12 {
                    let bytes = lane_payload(lane, sequence);
                    store.put_many(&[bytes]).unwrap();
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
    } else {
        store.checkpoint_journal(true, None).unwrap();
    }
    panic!("failpoint did not abort child");
}

fn crash(root: &std::path::Path, mode: &str, point: &str) {
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_worker", "--nocapture"])
        .env("SKERRY_PERF_CRASH_ROOT", root)
        .env("SKERRY_PERF_CRASH_MODE", mode)
        .env("SKERRY_FAILPOINT", point)
        .env("SKERRY_FAILPOINT_ACTION", "abort")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(!status.success(), "{point} failed to terminate child");
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(6), "{point} did not abort: {status}");
    }
}

fn fixture(c: &Config) -> ([u8; 32], [u8; 32]) {
    let store = Store::open(c).unwrap();
    let result = store
        .put_many(&[Bytes::from_static(LIVE), Bytes::from_static(DELETED)])
        .unwrap();
    store.seal().unwrap();
    assert!(store.delete(&result[1].hash).unwrap().deleted);
    (result[0].hash, result[1].hash)
}
fn assert_namespace(c: &Config, live: [u8; 32], deleted: [u8; 32]) {
    let store = Store::open(c).unwrap();
    assert_eq!(store.get(&live).unwrap().unwrap(), LIVE);
    assert!(store.get(&deleted).unwrap().is_none());
    assert!(!store.exists(&upload_hash()).unwrap());
}

#[test]
fn upload_crash_preserves_namespace_and_rebuild_fences() {
    for point in ["stream_after_payload_sync", "stream_after_rename"] {
        let dir = tempfile::tempdir().unwrap();
        let c = config(&dir.path().join("store"));
        let (live, deleted) = fixture(&c);
        crash(&c.storage.root, "upload", point);
        assert_namespace(&c, live, deleted);
        assert!(
            std::fs::read_dir(c.storage.root.join("segments"))
                .unwrap()
                .all(|e| !e
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".upload-"))
        );
        engine::rebuild(&c, false).unwrap();
        assert_namespace(&c, live, deleted);
    }
}

#[test]
fn checkpoint_crashes_preserve_committed_namespace() {
    for point in [
        "checkpoint_before_activation",
        "checkpoint_after_activation",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let c = config(&dir.path().join("store"));
        let (live, deleted) = fixture(&c);
        crash(&c.storage.root, "checkpoint", point);
        assert_namespace(&c, live, deleted);
        let store = Store::open(&c).unwrap();
        let fresh = store
            .put_many(&[Bytes::from_static(b"post checkpoint write")])
            .unwrap();
        assert_eq!(
            store.get(&fresh[0].hash).unwrap().unwrap(),
            b"post checkpoint write"
        );
        drop(store);
        engine::rebuild(&c, false).unwrap();
        assert_namespace(&c, live, deleted);
        let store = Store::open(&c).unwrap();
        assert_eq!(
            store.get(&fresh[0].hash).unwrap().unwrap(),
            b"post checkpoint write"
        );
    }
}

fn lane_payload(lane: usize, sequence: usize) -> Bytes {
    for nonce in 0..u64::MAX {
        let bytes = Bytes::from(format!("multi-lane-{lane}-{sequence}-{nonce}"));
        if usize::from(blake3::hash(&bytes).as_bytes()[0]) % 4 == lane {
            return bytes;
        }
    }
    unreachable!()
}

#[test]
fn lane_crashes_keep_acknowledged_data_and_deletion_fences() {
    for point in [
        "lane_0_after_data_sync",
        "lane_group_before_publish",
        "journal_after_frame_sync",
        "after_mutation_sync",
        "after_index_commit",
        "lane_group_after_publish",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut c = config(&dir.path().join("store"));
        c.writes.writer_lanes = 4;
        let (live, deleted) = fixture(&c);
        // Leave one acknowledged active segment in every lane. Journal
        // failpoints then exercise PUT publication rather than first creation.
        let acknowledged: Vec<_> = {
            let store = Store::open(&c).unwrap();
            (0..4)
                .map(|lane| {
                    let bytes = lane_payload(lane, 1000);
                    let hash = store.put_many(std::slice::from_ref(&bytes)).unwrap()[0].hash;
                    (hash, bytes)
                })
                .collect()
        };
        crash(&c.storage.root, "lanes", point);
        // Unacknowledged new records may or may not be committed; every mapped
        // record must be valid, and earlier acknowledged mutations must persist.
        let store = Store::open(&c).unwrap();
        assert_eq!(store.get(&live).unwrap().unwrap(), LIVE);
        assert!(store.get(&deleted).unwrap().is_none());
        for (hash, bytes) in &acknowledged {
            assert_eq!(store.get(hash).unwrap().unwrap(), bytes.as_ref());
        }
        for lane in 0..4 {
            for sequence in 0..12 {
                let bytes = lane_payload(lane, sequence);
                let hash = *blake3::hash(&bytes).as_bytes();
                if let Some(found) = store.get(&hash).unwrap() {
                    assert_eq!(found, bytes.as_ref());
                }
            }
        }
        let fresh = Bytes::from_static(b"multi-lane post recovery write");
        let hash = store.put_many(std::slice::from_ref(&fresh)).unwrap()[0].hash;
        assert_eq!(store.get(&hash).unwrap().unwrap(), fresh.as_ref());
        drop(store);
        engine::rebuild(&c, false).unwrap();
        let store = Store::open(&c).unwrap();
        assert_eq!(store.get(&live).unwrap().unwrap(), LIVE);
        assert!(store.get(&deleted).unwrap().is_none());
        assert_eq!(store.get(&hash).unwrap().unwrap(), fresh.as_ref());
        for (hash, bytes) in &acknowledged {
            assert_eq!(store.get(hash).unwrap().unwrap(), bytes.as_ref());
        }
    }
}
