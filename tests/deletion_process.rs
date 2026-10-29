//! Process termination exercises recovery after OS process death, not power loss.
use bytes::Bytes;
use skerrykv::{
    config::Config,
    engine::{self, Store},
};
use std::{
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
use tempfile::TempDir;

const TOKEN: &str = "deletion-process-test-token";
const LIVE: &[u8] = b"surviving object";
const DELETED: &[u8] = b"object to delete";

struct Fixture {
    _dir: TempDir,
    config: Config,
    path: std::path::PathBuf,
    url: String,
    live: [u8; 32],
    deleted: [u8; 32],
}
impl Fixture {
    fn new(delete: bool) -> Self {
        let dir = TempDir::new().unwrap();
        let mut config = Config::default();
        config.storage.root = dir.path().join("store");
        config.cache.payload_mib = 0;
        let socket = TcpListener::bind("127.0.0.1:0").unwrap();
        config.server.listen = socket.local_addr().unwrap().to_string();
        drop(socket);
        let store = Store::open(&config).unwrap();
        let hashes = store
            .put_many(&[Bytes::from_static(LIVE), Bytes::from_static(DELETED)])
            .unwrap();
        store.seal().unwrap();
        if delete {
            assert!(store.delete(&hashes[1].hash).unwrap().deleted);
        }
        drop(store);
        let path = dir.path().join("config.toml");
        config.save(&path).unwrap();
        let url = format!("http://{}", config.server.listen);
        Self {
            _dir: dir,
            config,
            path,
            url,
            live: hashes[0].hash,
            deleted: hashes[1].hash,
        }
    }
    fn start(&self, point: Option<&str>) -> Child {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_skerry"));
        cmd.args(["serve", "--config"]).arg(&self.path);
        configure(&mut cmd, point);
        cmd.spawn().unwrap()
    }
    fn assert_namespace(&self, deleted: bool) {
        let store = Store::open(&self.config).unwrap();
        assert_eq!(store.get(&self.live).unwrap().unwrap(), LIVE);
        let value = store.get(&self.deleted).unwrap();
        if deleted {
            assert!(value.is_none());
        } else {
            assert_eq!(value.unwrap(), DELETED);
        }
    }
}
fn configure(cmd: &mut Command, point: Option<&str>) {
    cmd.env("SKERRY_TOKEN", TOKEN)
        .env_remove("SKERRY_ROOT")
        .env_remove("SKERRY_LISTEN")
        .env_remove("SKERRY_FAILPOINT")
        .env_remove("SKERRY_FAILPOINT_ACTION")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(point) = point {
        cmd.env("SKERRY_FAILPOINT", point)
            .env("SKERRY_FAILPOINT_ACTION", "abort");
    }
}
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
impl Process {
    fn kill(&mut self) {
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }
    async fn exited(&mut self, point: &str) {
        for _ in 0..200 {
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(!status.success(), "{point} did not fail");
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("{point} did not terminate child");
    }
    async fn ready(&mut self, fixture: &Fixture, client: &reqwest::Client) {
        for _ in 0..100 {
            if let Some(status) = self.0.try_wait().unwrap() {
                panic!("service exited early: {status}");
            }
            if client
                .get(format!("{}/v1/ready", fixture.url))
                .bearer_auth(TOKEN)
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("service did not become ready");
    }
}
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap()
}
fn offline_vacuum(path: &Path, point: &str) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_skerry"));
    cmd.arg("vacuum").arg("--path").arg(path);
    configure(&mut cmd, Some(point));
    let mut child = cmd.spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(!status.success(), "{point} did not fail");
            break;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("{point} did not exit");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[tokio::test]
async fn delete_crash_boundaries_preserve_prior_commits() {
    for point in [
        "journal_before_commit",
        "journal_after_frame_sync",
        "after_mutation_sync",
        "after_index_commit",
        "delete_before_ack",
    ] {
        let f = Fixture::new(false);
        let client = client();
        let mut process = Process(f.start(Some(point)));
        process.ready(&f, &client).await;
        let _ = client
            .delete(format!("{}/v1/objects/{}", f.url, hex::encode(f.deleted)))
            .bearer_auth(TOKEN)
            .send()
            .await;
        process.exited(point).await;
        let store = Store::open(&f.config).unwrap();
        assert_eq!(store.get(&f.live).unwrap().unwrap(), LIVE, "{point}");
        let deleted = store.get(&f.deleted).unwrap();
        if point == "journal_before_commit" {
            assert_eq!(deleted.unwrap(), DELETED);
        } else if let Some(value) = deleted {
            assert_eq!(value, DELETED);
        }
    }
}
#[test]
fn interrupted_marker_publication_is_reconciled_before_later_writes() {
    let fixture = Fixture::new(true);
    offline_vacuum(&fixture.config.storage.root, "activation_before_marker");
    let store = Store::open(&fixture.config).unwrap();
    let new = store
        .put_many(&[Bytes::from_static(b"after marker recovery")])
        .unwrap()[0]
        .hash;
    store.delete(&fixture.live).unwrap();
    drop(store);
    std::fs::remove_dir_all(fixture.config.storage.root.join("index")).unwrap();
    Store::rebuild(&fixture.config, false).unwrap();
    let store = Store::open(&fixture.config).unwrap();
    assert_eq!(store.get(&new).unwrap().unwrap(), b"after marker recovery");
    assert!(!store.exists(&fixture.live).unwrap());
    assert!(!store.exists(&fixture.deleted).unwrap());
}

#[tokio::test]
async fn acknowledged_delete_survives_kill_and_rebuild() {
    let f = Fixture::new(false);
    let client = client();
    let mut process = Process(f.start(None));
    process.ready(&f, &client).await;
    client
        .delete(format!("{}/v1/objects/{}", f.url, hex::encode(f.deleted)))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    process.kill();
    f.assert_namespace(true);
    engine::rebuild(&f.config, false).unwrap();
    f.assert_namespace(true);
}

#[test]
fn interrupted_vacuum_recovers_and_can_be_rerun() {
    for point in ["vacuum_before_activation", "vacuum_after_activation"] {
        let f = Fixture::new(true);
        offline_vacuum(&f.config.storage.root, point);
        f.assert_namespace(true);
        Store::vacuum(&f.config, false).unwrap();
        f.assert_namespace(true);
    }
}

#[tokio::test]
async fn interrupted_gc_preserves_namespace_and_can_be_rerun() {
    for point in ["gc_after_copy", "gc_after_retire"] {
        let f = Fixture::new(true);
        let client = client();
        let mut process = Process(f.start(Some(point)));
        process.ready(&f, &client).await;
        let _ = client
            .post(format!("{}/v1/admin/gc?force=true", f.url))
            .bearer_auth(TOKEN)
            .send()
            .await;
        process.exited(point).await;
        f.assert_namespace(true);
        let store = Store::open(&f.config).unwrap();
        store.collect_garbage(true).unwrap();
        assert_eq!(store.get(&f.live).unwrap().unwrap(), LIVE);
        assert!(store.get(&f.deleted).unwrap().is_none());
    }
}
