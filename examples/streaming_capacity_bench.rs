//! Bounded-memory streaming HTTP capacity benchmark for large PUT and GET bodies.
use anyhow::{Context, Result, ensure};
use clap::{Parser, ValueEnum};
use futures_util::StreamExt;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use skerrykv::config::Config;
use std::{
    collections::BTreeMap,
    ffi::OsString,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    process::{Child, Command},
    task::JoinSet,
};

const CHUNK_BYTES: usize = 1024 * 1024;
const SAMPLE_LIMIT: usize = 100_000;

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
enum Mode {
    Put,
    Get,
}

#[derive(Clone, Debug, Parser)]
struct Args {
    #[arg(long)]
    binary: PathBuf,
    #[arg(long)]
    store_parent: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, value_enum)]
    mode: Mode,
    #[arg(long, default_value_t = 4)]
    workers: usize,
    #[arg(long, default_value_t = 1)]
    writer_lanes: usize,
    /// Override writes.commit_max_delay_ms for controlled tuning.
    #[arg(long)]
    commit_delay_ms: Option<u64>,
    /// Strip lane and gather-delay fields for a pre-multi-writer 0.3 server.
    #[arg(long)]
    baseline_config_0_3: bool,
    #[arg(long, default_value_t = 16)]
    file_mib: usize,
    #[arg(long, default_value_t = 3)]
    seconds: u64,
    #[arg(long, default_value_t = 2048)]
    max_store_mib: u64,
}

fn effective_server_config(
    config: &Config,
    lanes: usize,
    commit_delay_ms: Option<u64>,
    baseline_0_3: bool,
) -> Result<(toml::Value, Vec<String>)> {
    let mut value = toml::Value::try_from(config)?;
    let mut removed = Vec::new();
    value["writes"]["writer_lanes"] = toml::Value::Integer(lanes as i64);
    value["writes"]["commit_max_delay_ms"] =
        toml::Value::Integer(commit_delay_ms.unwrap_or(config.writes.commit_max_delay_ms) as i64);
    if baseline_0_3 {
        for key in ["writer_lanes", "commit_max_delay_ms"] {
            value["writes"]
                .as_table_mut()
                .and_then(|table| table.remove(key));
            removed.push(format!("writes.{key}"));
        }
    }
    Ok((value, removed))
}

fn clear_inherited_skerry_env(command: &mut Command, keys: impl IntoIterator<Item = OsString>) {
    for key in keys {
        if key.to_string_lossy().starts_with("SKERRY_") {
            command.env_remove(key);
        }
    }
}

struct ChildGuard {
    child: Option<Child>,
    root: PathBuf,
}
impl ChildGuard {
    async fn shutdown(&mut self, client: &Client, url: &str, token: &str) {
        let _ = client
            .post(format!("{url}/v1/admin/shutdown"))
            .bearer_auth(token)
            .send()
            .await;
        if let Some(child) = self.child.as_mut() {
            let _ = tokio::time::timeout(Duration::from_secs(15), child.wait()).await;
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.start_kill();
                let _ = child.wait().await;
            }
        }
        self.child.take();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut()
            && child.try_wait().ok().flatten().is_none()
        {
            let _ = child.start_kill();
            for _ in 0..500 {
                if child.try_wait().ok().flatten().is_some() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        // Do not race a still-running server while removing its files. Successful
        // runs use shutdown() and remove the directory after the child exits.
        if self
            .child
            .as_mut()
            .is_none_or(|c| c.try_wait().ok().flatten().is_some())
        {
            let _ = std::fs::remove_dir_all(&self.root);
        } else {
            eprintln!(
                "server did not stop; retained benchmark store {}",
                self.root.display()
            );
        }
    }
}

#[derive(Default)]
struct Stats {
    successful_puts: u64,
    rejected_puts: u64,
    unknown_puts: u64,
    successful_gets: u64,
    failed_gets: u64,
    bytes: u64,
    latencies_ms: Vec<f64>,
    get_latencies_ms: Vec<f64>,
    rejected_statuses: BTreeMap<u16, u64>,
}
#[derive(Default, Serialize)]
struct Counts {
    successful_puts: u64,
    rejected_puts: u64,
    unknown_puts: u64,
    successful_gets: u64,
    failed_gets: u64,
    successful_bytes: u64,
    rejected_statuses: BTreeMap<u16, u64>,
}
#[derive(Deserialize)]
struct PutResponse {
    hash: String,
    duplicate: bool,
}

fn add_sample(samples: &mut Vec<f64>, v: f64) {
    if samples.len() < SAMPLE_LIMIT {
        samples.push(v);
    }
}
fn distribution(input: &[f64]) -> serde_json::Value {
    let mut sorted = input.to_vec();
    sorted.sort_by(f64::total_cmp);
    let at = |p: f64| {
        if sorted.is_empty() {
            None
        } else {
            Some(sorted[((sorted.len() - 1) as f64 * p).ceil() as usize])
        }
    };
    serde_json::json!({"samples":sorted.len(),"p50_ms":at(0.50),"p95_ms":at(0.95),"p99_ms":at(0.99),"max_ms":at(1.0),"sampling":"first 100000 observations"})
}
fn content_chunk(seed: [u8; 32], worker: u64, operation: u64, index: u64, len: usize) -> Vec<u8> {
    let mut h = blake3::Hasher::new();
    h.update(&seed);
    h.update(&worker.to_le_bytes());
    h.update(&operation.to_le_bytes());
    h.update(&index.to_le_bytes());
    let mut out = vec![0; len];
    h.finalize_xof().fill(&mut out);
    out
}
fn unique_seed() -> [u8; 32] {
    *blake3::hash(uuid::Uuid::new_v4().as_bytes()).as_bytes()
}
fn reserve(budget: &AtomicU64, cap: u64, n: u64) -> bool {
    let mut current = budget.load(Ordering::Relaxed);
    loop {
        let Some(next) = current.checked_add(n) else {
            return false;
        };
        if next > cap {
            return false;
        }
        match budget.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Relaxed) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
}
fn release(budget: &AtomicU64, n: u64) {
    budget.fetch_sub(n, Ordering::AcqRel);
}

fn upload_body(
    seed: [u8; 32],
    worker: u64,
    operation: u64,
    size: usize,
    digest: Arc<Mutex<Option<String>>>,
) -> reqwest::Body {
    let stream = async_stream::stream! {
        let mut whole = blake3::Hasher::new();
        let count = size.div_ceil(CHUNK_BYTES);
        for index in 0..count {
            let len = (size - index * CHUNK_BYTES).min(CHUNK_BYTES);
            let bytes = content_chunk(seed, worker, operation, index as u64, len);
            whole.update(&bytes);
            yield Ok::<_, std::io::Error>(bytes::Bytes::from(bytes));
        }
        *digest.lock().unwrap() = Some(whole.finalize().to_hex().to_string());
    };
    reqwest::Body::wrap_stream(stream)
}

async fn put_one(
    client: &Client,
    url: &str,
    token: &str,
    seed: [u8; 32],
    worker: u64,
    operation: u64,
    size: usize,
) -> Result<(StatusCode, Option<String>, f64)> {
    let digest = Arc::new(Mutex::new(None));
    let body = upload_body(seed, worker, operation, size, digest.clone());
    let started = Instant::now();
    let response = client
        .put(format!("{url}/v1/objects"))
        .bearer_auth(token)
        .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
        .body(body)
        .send()
        .await?;
    let elapsed = started.elapsed().as_secs_f64() * 1000.;
    let status = response.status();
    if !status.is_success() {
        let _ = response.bytes().await;
        return Ok((status, None, elapsed));
    }
    let result: PutResponse = response
        .json()
        .await
        .context("successful PUT response was invalid")?;
    ensure!(!result.duplicate, "fresh PUT unexpectedly deduplicated");
    let expected = digest
        .lock()
        .unwrap()
        .clone()
        .context("upload body was not fully consumed")?;
    ensure!(
        result.hash == expected,
        "server returned a different payload hash"
    );
    Ok((status, Some(result.hash), elapsed))
}

async fn read_one(
    client: &Client,
    url: &str,
    token: &str,
    hash: &str,
    expected_size: usize,
) -> Result<(u64, f64)> {
    let started = Instant::now();
    let response = client
        .get(format!("{url}/v1/objects/{hash}"))
        .bearer_auth(token)
        .send()
        .await?;
    ensure!(
        response.status().is_success(),
        "GET returned {}",
        response.status()
    );
    let mut stream = response.bytes_stream();
    let mut digest = blake3::Hasher::new();
    let mut count = 0u64;
    while let Some(chunk) = stream.next().await {
        let bytes = chunk?;
        digest.update(&bytes);
        count += bytes.len() as u64;
    }
    ensure!(
        count == expected_size as u64,
        "GET length mismatch: got {count}, expected {expected_size}"
    );
    ensure!(
        digest.finalize().to_hex().as_str() == hash,
        "GET content hash mismatch"
    );
    Ok((count, started.elapsed().as_secs_f64() * 1000.))
}

struct WorkerContext {
    mode: Mode,
    client: Client,
    url: String,
    token: String,
    hash: String,
    size: usize,
    deadline: Instant,
    budget: Arc<AtomicU64>,
    cap: u64,
    stop: Arc<AtomicBool>,
    stats: Arc<Mutex<Stats>>,
}
async fn worker_loop(id: usize, context: WorkerContext) {
    let WorkerContext {
        mode,
        client,
        url,
        token,
        hash,
        size,
        deadline,
        budget,
        cap,
        stop,
        stats,
    } = context;
    let mut op = 0u64;
    while Instant::now() < deadline && !stop.load(Ordering::Acquire) {
        op += 1;
        match mode {
            Mode::Put => {
                if !reserve(&budget, cap, size as u64) {
                    stop.store(true, Ordering::Release);
                    break;
                }
                let seed = unique_seed();
                match put_one(&client, &url, &token, seed, id as u64, op, size).await {
                    Ok((status, Some(_), ms)) if status.is_success() => {
                        let mut s = stats.lock().unwrap();
                        s.successful_puts += 1;
                        s.bytes += size as u64;
                        add_sample(&mut s.latencies_ms, ms);
                    }
                    Ok((status, _, _))
                        if status.is_server_error()
                            && status != StatusCode::SERVICE_UNAVAILABLE
                            && status != StatusCode::INSUFFICIENT_STORAGE =>
                    {
                        // A 5xx may arrive after durable work began, so retain
                        // the logical store reservation as an unknown outcome.
                        stats.lock().unwrap().unknown_puts += 1;
                    }
                    Ok((status, _, _)) => {
                        release(&budget, size as u64);
                        let mut s = stats.lock().unwrap();
                        s.rejected_puts += 1;
                        *s.rejected_statuses.entry(status.as_u16()).or_default() += 1;
                    }
                    Err(e) => {
                        eprintln!("PUT outcome unknown (worker {id}, op {op}): {e:#}");
                        let mut s = stats.lock().unwrap();
                        s.unknown_puts += 1;
                    }
                }
            }
            Mode::Get => match read_one(&client, &url, &token, &hash, size).await {
                Ok((n, ms)) => {
                    let mut s = stats.lock().unwrap();
                    s.successful_gets += 1;
                    s.bytes += n;
                    add_sample(&mut s.get_latencies_ms, ms);
                }
                Err(e) => {
                    eprintln!("GET failed (worker {id}, op {op}): {e:#}");
                    stats.lock().unwrap().failed_gets += 1;
                }
            },
        }
    }
}

async fn probe(client: &Client, url: &str, token: &str, path: &str) -> Result<serde_json::Value> {
    let response = client
        .get(format!("{url}{path}"))
        .bearer_auth(token)
        .send()
        .await?;
    ensure!(
        response.status().is_success(),
        "probe {path} returned {}",
        response.status()
    );
    if path.ends_with("metrics") {
        Ok(
            serde_json::json!({"content_type":response.headers().get(reqwest::header::CONTENT_TYPE).and_then(|h|h.to_str().ok()),"body":response.text().await?}),
        )
    } else {
        Ok(response.json().await?)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        (1..=128).contains(&args.workers)
            && [1, 2, 4, 8].contains(&args.writer_lanes)
            && (!args.baseline_config_0_3 || args.writer_lanes == 1)
            && (!args.baseline_config_0_3 || args.commit_delay_ms.is_none())
            && args.commit_delay_ms.is_none_or(|delay| delay <= 1000)
            && args.seconds > 0
            && (2..=4096).contains(&args.file_mib),
        "workers must be 1..128, seconds positive, file_mib 2..4096, compatibility mode requires one lane, and commit delay must be 0..=1000 ms"
    );
    let size = args
        .file_mib
        .checked_mul(1024 * 1024)
        .context("file size overflow")?;
    let cap = args
        .max_store_mib
        .checked_mul(1024 * 1024)
        .context("store budget overflow")?;
    ensure!(
        cap >= size as u64,
        "max-store-mib must fit the fixture or one upload"
    );
    let binary = std::fs::canonicalize(&args.binary).context("resolve --binary")?;
    let parent = std::fs::canonicalize(&args.store_parent).context("resolve --store-parent")?;
    let binary_blake3 = blake3::hash(&std::fs::read(&binary)?).to_hex().to_string();
    let root = parent.join(format!("skerry-stream-bench-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root)?;
    let port = std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port();
    let url = format!("http://127.0.0.1:{port}");
    let token = uuid::Uuid::new_v4().to_string();
    let mut config = Config::default();
    config.storage.root = root.join("data");
    config.storage.min_free_mib = 0;
    config.server.listen = format!("127.0.0.1:{port}");
    config.resources.admission_timeout_ms = 60_000;
    config.resources.max_requests = args.workers + 16;
    config.streaming.enabled = true;
    config.streaming.threshold_mib = 1;
    config.streaming.max_object_mib = args.file_mib.saturating_add(1).clamp(64, 4096);
    config.streaming.chunk_kib = 1024;
    config.streaming.max_uploads = 2;
    config.streaming.max_reads = 4;
    config.streaming.min_free_mib = 0;
    config.mcp.enabled = false;
    config.cache.payload_mib = 0;
    config.validate()?;
    let (effective_config, removed_config_fields) = effective_server_config(
        &config,
        args.writer_lanes,
        args.commit_delay_ms,
        args.baseline_config_0_3,
    )?;
    let config_fingerprint = blake3::hash(toml::to_string(&effective_config)?.as_bytes())
        .to_hex()
        .to_string();
    let config_path = root.join("config.toml");
    std::fs::write(&config_path, toml::to_string_pretty(&effective_config)?)?;
    let log = std::fs::File::create(root.join("server.log"))?;
    let mut child = Command::new(&binary);
    child.arg("serve").arg("--config").arg(&config_path);
    clear_inherited_skerry_env(&mut child, std::env::vars_os().map(|(key, _)| key));
    let child = child
        .env("SKERRY_TOKEN", &token)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(log)
        .kill_on_drop(true)
        .spawn()?;
    let mut guard = ChildGuard {
        child: Some(child),
        root: root.clone(),
    };
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(args.workers + 4)
        .build()?;
    let ready_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = guard.child.as_mut().unwrap().try_wait()? {
            anyhow::bail!(
                "server exited before ready: {status}; log at {}",
                root.join("server.log").display()
            );
        }
        if client
            .get(format!("{url}/v1/ready"))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            break;
        }
        ensure!(Instant::now() < ready_deadline, "server readiness timeout");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // GET mode uses one fixture, fully scanned before timed requests so the
    // first verification pass is separately visible from checksum-cache hits.
    let mut fixture_hash = String::new();
    let mut first_get_ms = None;
    if matches!(args.mode, Mode::Get) {
        let digest = Arc::new(Mutex::new(None));
        let body = upload_body(unique_seed(), 0, 0, size, digest.clone());
        let response = client
            .put(format!("{url}/v1/objects"))
            .bearer_auth(&token)
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(body)
            .send()
            .await?;
        ensure!(
            response.status().is_success(),
            "fixture upload returned {}",
            response.status()
        );
        let result: PutResponse = response.json().await?;
        fixture_hash = result.hash;
        ensure!(
            digest.lock().unwrap().as_deref() == Some(fixture_hash.as_str()),
            "fixture upload hash mismatch"
        );
        let (_, ms) = read_one(&client, &url, &token, &fixture_hash, size).await?;
        first_get_ms = Some(ms);
    }
    let budget = Arc::new(AtomicU64::new(if matches!(args.mode, Mode::Get) {
        size as u64
    } else {
        0
    }));
    let stop = Arc::new(AtomicBool::new(false));
    let stats = Arc::new(Mutex::new(Stats::default()));
    let start = Instant::now();
    let deadline = start + Duration::from_secs(args.seconds);
    let mut jobs = JoinSet::new();
    for worker in 0..args.workers {
        let (client, url, token, mode, hash, size, budget, stop, stats) = (
            client.clone(),
            url.clone(),
            token.clone(),
            args.mode,
            args.mode_hash(&fixture_hash),
            size,
            budget.clone(),
            stop.clone(),
            stats.clone(),
        );
        jobs.spawn(worker_loop(
            worker,
            WorkerContext {
                mode,
                client,
                url,
                token,
                hash,
                size,
                deadline,
                budget,
                cap,
                stop,
                stats,
            },
        ));
    }
    while let Some(result) = jobs.join_next().await {
        result.context("benchmark worker panicked")?;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let snapshot_deadline = Instant::now() + Duration::from_secs(3);
    let status_probe = loop {
        let snapshot = probe(&client, &url, &token, "/v1/status").await?;
        if snapshot["stats_age_seconds"].as_f64().unwrap_or(99.0) <= 0.25
            || Instant::now() >= snapshot_deadline
        {
            break snapshot;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let metrics_probe = probe(&client, &url, &token, "/v1/metrics").await?;
    let result = {
        let s = stats.lock().unwrap();
        let counts = Counts {
            successful_puts: s.successful_puts,
            rejected_puts: s.rejected_puts,
            unknown_puts: s.unknown_puts,
            successful_gets: s.successful_gets,
            failed_gets: s.failed_gets,
            successful_bytes: s.bytes,
            rejected_statuses: s.rejected_statuses.clone(),
        };
        let rate = if elapsed > 0.0 {
            s.bytes as f64 / (1024. * 1024.) / elapsed
        } else {
            0.
        };
        let result = serde_json::json!({
            "schema":"skerry.streaming-capacity.v1","mode":args.mode,"binary":binary,"binary_blake3":binary_blake3,"config_fingerprint":config_fingerprint,
            "requested_window_seconds":args.seconds,"elapsed_seconds":elapsed,"workers":args.workers,"file_mib":args.file_mib,
            "configuration":{"streaming_threshold_mib":1,"chunk_mib":1,"max_uploads":2,"max_reads":4,"checksum_cache_mib":16,"max_store_mib":args.max_store_mib,"writer_lanes":args.writer_lanes,"commit_max_delay_ms":if args.baseline_config_0_3{None}else{Some(args.commit_delay_ms.unwrap_or(config.writes.commit_max_delay_ms))}},
            "effective_server_config":effective_config,
            "server_config_compatibility":{"baseline_config_0_3":args.baseline_config_0_3,"removed_fields":removed_config_fields},
            "throughput_mib_s":rate,"counts":counts,"put_latency":distribution(&s.latencies_ms),"get_latency":distribution(&s.get_latencies_ms),
            "first_get_full_scan_ms":first_get_ms,"fixture_hash":if matches!(args.mode,Mode::Get){Some(fixture_hash)}else{None},
            "client_memory_note":"each concurrent upload generates one 1 MiB chunk at a time; GET bodies are hashed incrementally; no whole-file client buffer is retained",
            "measurement_note":"isolated temporary store; warm repeated GETs follow one separately timed full checksum scan; throughput includes drained work after request offering stops",
            "server_status":status_probe,"server_metrics":metrics_probe
        });
        result
    };
    if let Some(parent) = args.output.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&args.output, serde_json::to_vec_pretty(&result)?)?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    guard.shutdown(&client, &url, &token).await;
    Ok(())
}

impl Args {
    fn mode_hash(&self, fixture: &str) -> String {
        if matches!(self.mode, Mode::Get) {
            fixture.to_string()
        } else {
            String::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_child_removes_inherited_skerry_variables_before_setting_token() {
        let mut command = Command::new("skerry");
        clear_inherited_skerry_env(
            &mut command,
            [
                OsString::from("SKERRY_ROOT"),
                OsString::from("OTHER_SETTING"),
            ],
        );
        command.env("SKERRY_TOKEN", "benchmark-token");
        let envs: Vec<_> = command.as_std().get_envs().collect();
        assert!(
            envs.iter()
                .any(|(key, value)| *key == std::ffi::OsStr::new("SKERRY_ROOT") && value.is_none())
        );
        assert!(
            !envs
                .iter()
                .any(|(key, _)| *key == std::ffi::OsStr::new("OTHER_SETTING"))
        );
        assert!(envs.iter().any(|(key, value)| *key == std::ffi::OsStr::new("SKERRY_TOKEN") && value.is_some()));
    }
}
