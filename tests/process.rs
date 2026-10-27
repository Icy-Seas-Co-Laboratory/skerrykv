//! Process death tests exercise OS/process recovery, not power-loss durability.
use skerrykv::config::Config;
use std::{
    net::TcpListener,
    process::{Child, Command, Stdio},
    time::Duration,
};
use tempfile::TempDir;

struct Service {
    child: Child,
    root: TempDir,
    config: std::path::PathBuf,
    url: String,
}
impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Service {
    fn start() -> Self {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().join("store");
        config.cache.payload_mib = 0;
        let socket = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = socket.local_addr().unwrap();
        drop(socket);
        config.server.listen = address.to_string();
        let path = root.path().join("config.toml");
        config.save(&path).unwrap();
        let child = spawn(&path, None);
        Self {
            child,
            root,
            config: path,
            url: format!("http://{address}"),
        }
    }
    async fn wait_ready(&mut self) {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();
        for _ in 0..100 {
            if let Some(exit) = self.child.try_wait().unwrap() {
                panic!("service exited early: {exit}");
            }
            if client
                .get(format!("{}/v1/health", self.url))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success())
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("service never ready");
    }
    fn kill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
    fn restart(&mut self, failpoint: Option<&str>) {
        self.child = spawn(&self.config, failpoint);
    }
}
fn spawn(config: &std::path::Path, failpoint: Option<&str>) -> Child {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_skerry"));
    cmd.args(["serve", "--config"])
        .arg(config)
        .env_remove("SKERRY_TOKEN")
        .env_remove("SKERRY_ROOT")
        .env_remove("SKERRY_LISTEN")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(point) = failpoint {
        cmd.env("SKERRY_FAILPOINT", point)
            .env("SKERRY_FAILPOINT_ACTION", "abort");
    }
    cmd.spawn().unwrap()
}

#[tokio::test]
async fn acknowledged_durable_objects_survive_forced_process_termination() {
    let mut server = Service::start();
    server.wait_ready().await;
    let client = reqwest::Client::new();
    let mut objects = Vec::new();
    for n in 0..8 {
        let payload = vec![n; 20 * 1024 + n as usize];
        let response = client
            .put(format!("{}/v1/objects", server.url))
            .header("content-type", "application/octet-stream")
            .body(payload.clone())
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        objects.push((response["hash"].as_str().unwrap().to_owned(), payload));
    }
    server.kill();
    server.restart(None);
    server.wait_ready().await;
    for (hash, payload) in objects {
        let data = client
            .get(format!("{}/v1/objects/{hash}", server.url))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(data.as_ref(), payload);
    }
    client
        .post(format!("{}/v1/admin/shutdown", server.url))
        .send()
        .await
        .unwrap();
    for _ in 0..100 {
        if server.child.try_wait().unwrap().is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("shutdown failed to exit");
}

#[tokio::test]
async fn crash_at_each_persistence_boundary_preserves_previously_acknowledged_data() {
    for point in [
        "after_record_header",
        "after_payload",
        "after_segment_sync",
        "before_index_commit",
        "after_index_commit",
        "after_index_sync",
        "before_ack",
    ] {
        let mut server = Service::start();
        server.wait_ready().await;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let previous = client
            .put(format!("{}/v1/objects", server.url))
            .header("content-type", "application/octet-stream")
            .body("previous durable")
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        let previous_hash = previous["hash"].as_str().unwrap();
        server.kill();
        server.restart(Some(point));
        server.wait_ready().await;
        let _ = client
            .put(format!("{}/v1/objects", server.url))
            .header("content-type", "application/octet-stream")
            .body("interrupted payload")
            .send()
            .await;
        for _ in 0..100 {
            if server.child.try_wait().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if server.child.try_wait().unwrap().is_none() {
            server.kill();
            panic!("failpoint {point} did not terminate process");
        }
        server.restart(None);
        server.wait_ready().await;
        let payload = client
            .get(format!("{}/v1/objects/{previous_hash}", server.url))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(payload, "previous durable", "failpoint {point}");
        let hash = blake3::hash(b"interrupted payload").to_hex();
        let response = client
            .get(format!("{}/v1/objects/{hash}", server.url))
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success() || response.status() == reqwest::StatusCode::NOT_FOUND,
            "{point}"
        );
        if response.status().is_success() {
            assert_eq!(response.bytes().await.unwrap(), "interrupted payload");
        }
        let root_path = server.root.path().to_path_buf();
        assert!(root_path.exists());
    }
}

#[cfg(unix)]
#[test]
fn terminal_restoration_after_dashboard_detach() {
    // A real pseudo-terminal verifies that raw mode and alternate screen are restored.
    let python = r#"
import os, pty, select, subprocess, sys, termios, time
master, slave=pty.openpty()
before=termios.tcgetattr(slave)
p=subprocess.Popen([sys.argv[1], 'tui', '--connect', 'http://127.0.0.1:1'],stdin=slave,stdout=slave,stderr=slave)
time.sleep(.5)
os.write(master,b'q')
end=time.monotonic()+8
output=b''
while p.poll() is None and time.monotonic()<end:
    ready,_,_=select.select([master],[],[],.1)
    if ready:
        try: output+=os.read(master,65536)
        except OSError: break
if p.poll() is None:
    p.kill();p.wait();raise RuntimeError('dashboard did not quit')
after=termios.tcgetattr(slave)
assert before==after, 'terminal mode not restored'
assert b'\x1b[?1049l' in output, 'alternate screen not restored'
os.close(master);os.close(slave)
"#;
    // Python is an optional test harness, never a runtime engine dependency.
    if Command::new("python3").arg("--version").output().is_err() {
        return;
    }
    let output = Command::new("python3")
        .args(["-c", python, env!("CARGO_BIN_EXE_skerry")])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn interrupted_sealing_reconciles_without_losing_commits() {
    for point in ["seal_before_footer", "seal_after_footer", "seal_after_sync"] {
        let mut server = Service::start();
        server.wait_ready().await;
        let client = reqwest::Client::new();
        let object = client
            .put(format!("{}/v1/objects", server.url))
            .header("content-type", "application/octet-stream")
            .body("seal me")
            .send()
            .await
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        let hash = object["hash"].as_str().unwrap();
        server.kill();
        server.restart(Some(point));
        server.wait_ready().await;
        let _ = client
            .post(format!("{}/v1/admin/shutdown", server.url))
            .send()
            .await;
        for _ in 0..100 {
            if server.child.try_wait().unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            server.child.try_wait().unwrap().is_some(),
            "seal failpoint did not abort: {point}"
        );
        server.restart(None);
        server.wait_ready().await;
        let payload = client
            .get(format!("{}/v1/objects/{hash}", server.url))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(payload, "seal me");
    }
}

#[test]
fn interrupted_index_activation_and_import_are_recoverable() {
    use bytes::Bytes;
    use skerrykv::engine::Store;
    for point in ["before_activation", "after_activation"] {
        let root = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = root.path().into();
        let store = Store::open(&config).unwrap();
        let hash = store.put_many(&[Bytes::from_static(b"preserved")]).unwrap()[0].hash;
        store.seal().unwrap();
        drop(store);
        let output = Command::new(env!("CARGO_BIN_EXE_skerry"))
            .args(["rebuild-index", "--path"])
            .arg(root.path())
            .env("SKERRY_FAILPOINT", point)
            .env("SKERRY_FAILPOINT_ACTION", "abort")
            .output()
            .unwrap();
        assert!(!output.status.success(), "{point}");
        let store = Store::open(&config).unwrap();
        assert_eq!(store.get(&hash).unwrap().unwrap(), b"preserved");
    }
    for point in ["import_after_copy", "import_after_publish_file"] {
        let source = tempfile::tempdir().unwrap();
        let destination = tempfile::tempdir().unwrap();
        let mut src = Config::default();
        src.storage.root = source.path().into();
        let mut dest = Config::default();
        dest.storage.root = destination.path().into();
        let store = Store::open(&src).unwrap();
        let hash = store.put_many(&[Bytes::from_static(b"import me")]).unwrap()[0].hash;
        store.seal().unwrap();
        drop(store);
        drop(Store::open(&dest).unwrap());
        let output = Command::new(env!("CARGO_BIN_EXE_skerry"))
            .args(["import-segments", "--source"])
            .arg(source.path())
            .arg("--path")
            .arg(destination.path())
            .env("SKERRY_FAILPOINT", point)
            .env("SKERRY_FAILPOINT_ACTION", "abort")
            .output()
            .unwrap();
        assert!(
            !output.status.success(),
            "{point}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Store::import(&dest, source.path()).unwrap();
        let store = Store::open(&dest).unwrap();
        assert_eq!(store.get(&hash).unwrap().unwrap(), b"import me");
        assert!(Store::verify(&src).is_ok());
    }
}

#[cfg(unix)]
#[test]
fn embedded_dashboard_detaches_without_stopping_service() {
    let root = tempfile::tempdir().unwrap();
    let mut config = Config::default();
    config.storage.root = root.path().join("store");
    let socket = TcpListener::bind("127.0.0.1:0").unwrap();
    config.server.listen = socket.local_addr().unwrap().to_string();
    drop(socket);
    let path = root.path().join("config.toml");
    config.save(&path).unwrap();
    let python = r#"
import fcntl, os, pty, select, struct, subprocess, sys, termios, time, urllib.request
master,slave=pty.openpty()
fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',30,100,0,0))
before=termios.tcgetattr(slave)
env=os.environ.copy()
for key in ['SKERRY_TOKEN','SKERRY_LISTEN','SKERRY_ROOT']:env.pop(key,None)
p=subprocess.Popen([sys.argv[1],'serve','--tui','--config',sys.argv[2]],stdin=slave,stdout=slave,stderr=slave,env=env)
url=sys.argv[3]
try:
    end=time.monotonic()+10
    while True:
        try:
            urllib.request.urlopen(url+'/v1/health',timeout=.2).read();break
        except Exception:
            if time.monotonic()>end:raise RuntimeError('embedded service not healthy')
            if p.poll() is not None:raise RuntimeError('embedded service exited')
            time.sleep(.05)
    # Ingest while the dashboard is active; tracing belongs in the log file.
    request=urllib.request.Request(url+'/v1/objects',data=b'tui write',method='PUT',headers={'Content-Type':'application/octet-stream'})
    urllib.request.urlopen(request,timeout=3).read()
    time.sleep(.2)
    os.write(master,b'q')
    output=b''
    end=time.monotonic()+6
    while time.monotonic()<end:
        ready,_,_=select.select([master],[],[],.1)
        if ready:output+=os.read(master,65536)
        if termios.tcgetattr(slave)==before:break
    assert termios.tcgetattr(slave)==before,'embedded detach did not restore terminal'
    assert p.poll() is None,'q stopped foreground service'
    assert b'durably completed' not in output,'tracing corrupted dashboard output'
    urllib.request.urlopen(urllib.request.Request(url+'/v1/admin/shutdown',data=b'',method='POST'),timeout=3).read()
    assert p.wait(timeout=10)==0,'embedded service shutdown failed'
finally:
    if p.poll() is None:p.kill();p.wait()
    os.close(master);os.close(slave)
"#;
    if Command::new("python3").arg("--version").output().is_err() {
        return;
    }
    let output = Command::new("python3")
        .args(["-c", python, env!("CARGO_BIN_EXE_skerry")])
        .arg(&path)
        .arg(format!("http://{}", config.server.listen))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(root.path().join("store/logs/skerry.log").is_file());
}

#[tokio::test]
async fn returned_persistence_failure_faults_writer_until_recovery() {
    let mut server = Service::start();
    server.wait_ready().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let previous = client
        .put(format!("{}/v1/objects", server.url))
        .header("content-type", "application/octet-stream")
        .body("safe previous")
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    server.kill();
    server.child = Command::new(env!("CARGO_BIN_EXE_skerry"))
        .args(["serve", "--config"])
        .arg(&server.config)
        .env_remove("SKERRY_TOKEN")
        .env_remove("SKERRY_ROOT")
        .env_remove("SKERRY_LISTEN")
        .env("SKERRY_FAILPOINT", "after_segment_sync")
        .env("SKERRY_FAILPOINT_ACTION", "error")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    server.wait_ready().await;
    let failed = client
        .put(format!("{}/v1/objects", server.url))
        .header("content-type", "application/octet-stream")
        .body("uncertain write")
        .send()
        .await
        .unwrap();
    assert!(!failed.status().is_success());
    assert_eq!(
        client
            .get(format!("{}/v1/health", server.url))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    let status = client
        .get(format!("{}/v1/status", server.url))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(status["health"], "write-faulted");
    assert!(
        !client
            .put(format!("{}/v1/objects", server.url))
            .header("content-type", "application/octet-stream")
            .body("later write")
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
    let hash = previous["hash"].as_str().unwrap();
    assert_eq!(
        client
            .get(format!("{}/v1/objects/{hash}", server.url))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        "safe previous"
    );
    server.kill();
    server.restart(None);
    server.wait_ready().await;
    assert!(
        client
            .put(format!("{}/v1/objects", server.url))
            .header("content-type", "application/octet-stream")
            .body("recovered write")
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );
}
