//! Closed-loop HTTP capacity benchmark. Requests started during the window are drained;
//! this does not correct for coordinated omission. Latencies are bounded samples.
use anyhow::{Context, Result, ensure};
use clap::{Parser, ValueEnum};
use futures_util::StreamExt;
use serde::Serialize;
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
use tokio::process::{Child, Command};

#[derive(Clone, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Workload {
    UniquePut,
    DuplicatePut,
    SkewPut,
    BatchPut,
    RandomGet,
    HotGet,
    Mixed,
}
#[derive(Clone, Debug, Parser, Serialize)]
struct Args {
    #[arg(long)]
    binary: PathBuf,
    #[arg(long)]
    store_parent: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, default_value_t = 8)]
    workers: usize,
    #[arg(long, default_value_t = 1)]
    writer_lanes: usize,
    #[arg(long, default_value_t = 5)]
    seconds: u64,
    #[arg(long, default_value_t = 3)]
    warmup_seconds: u64,
    #[arg(long, default_value_t = 4096)]
    payload_bytes: usize,
    #[arg(long, default_value_t = 32)]
    batch_items: usize,
    #[arg(long, default_value_t = 32)]
    fixture_items: usize,
    #[arg(long, value_enum, default_value = "unique-put")]
    workload: Workload,
    #[arg(long, default_value_t = 80)]
    read_percent: u64,
    #[arg(long, default_value_t = 128)]
    max_requests: usize,
    #[arg(long)]
    inflight_per_batch: Option<usize>,
    #[arg(long, default_value_t = 256)]
    upload_mib: usize,
    #[arg(long, default_value_t = 64)]
    max_object_mib: usize,
    #[arg(long, default_value_t = 0)]
    cache_mib: usize,
    #[arg(long, default_value_t = 1)]
    batch_delay_ms: u64,
    /// Override writes.commit_max_delay_ms for controlled tuning.
    #[arg(long)]
    commit_delay_ms: Option<u64>,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long)]
    max_operations: Option<u64>,
    #[arg(long, default_value_t = 256)]
    max_store_mib: u64,
    #[arg(long, default_value_t = 10)]
    timeout_seconds: u64,
    #[arg(long, default_value_t = 512)]
    client_memory_mib: usize,
    #[arg(long)]
    keep_store: bool,
    /// Emit the pre-0.3.0 TOML schema accepted by a 0.2.2 baseline binary.
    #[arg(long)]
    legacy_config: bool,
    /// Remove writes.writer_lanes when comparing with the pre-multi-writer 0.3 schema.
    #[arg(long)]
    baseline_config_0_3: bool,
}
struct ChildGuard {
    child: Option<Child>,
    root: PathBuf,
    keep: bool,
}
fn clear_inherited_skerry_env(command: &mut Command, keys: impl IntoIterator<Item = OsString>) {
    for key in keys {
        if key.to_string_lossy().starts_with("SKERRY_") {
            command.env_remove(key);
        }
    }
}
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let mut stopped = true;
        if let Some(child) = self.child.as_mut()
            && child.try_wait().ok().flatten().is_none()
        {
            let _ = child.start_kill();
            stopped = false;
            for _ in 0..500 {
                if matches!(child.try_wait(), Ok(Some(_))) {
                    stopped = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        if !self.keep && stopped {
            if let Err(error) = std::fs::remove_dir_all(&self.root) {
                eprintln!(
                    "could not remove benchmark store {}: {error}",
                    self.root.display()
                );
            }
        } else if !self.keep && !stopped {
            eprintln!(
                "server did not exit; retained benchmark store {}",
                self.root.display()
            );
        }
    }
}
#[derive(Clone, Default)]
struct Stats {
    requests: u64,
    failed_requests: u64,
    success: u64,
    read_success: u64,
    write_success: u64,
    failed_read_items: u64,
    unknown_write_items: u64,
    rejected: u64,
    unknown: u64,
    bytes: u64,
    duplicates: u64,
    latencies: Vec<f64>,
    item_latencies: Vec<f64>,
    latency_max: f64,
    item_latency_max: f64,
    success_request_latencies: Vec<f64>,
    error_request_latencies: Vec<f64>,
    success_request_max: f64,
    error_request_max: f64,
    errors: BTreeMap<String, u64>,
}
fn fixture_count(workload: &Workload, requested: usize) -> usize {
    if matches!(workload, Workload::RandomGet | Workload::Mixed) {
        requested
    } else {
        1
    }
}
fn effective_server_config(
    config: &Config,
    legacy: bool,
    baseline_0_3: bool,
    lanes: usize,
    commit_delay_ms: Option<u64>,
) -> Result<(toml::Value, Vec<String>)> {
    let mut value = toml::Value::try_from(config)?;
    let mut removed = Vec::new();
    value["writes"]["writer_lanes"] = toml::Value::Integer(lanes as i64);
    value["writes"]["commit_max_delay_ms"] =
        toml::Value::Integer(commit_delay_ms.unwrap_or(config.writes.commit_max_delay_ms) as i64);
    if baseline_0_3 {
        value["writes"]
            .as_table_mut()
            .and_then(|t| t.remove("writer_lanes"));
        removed.push("writes.writer_lanes".to_string());
        value["writes"]
            .as_table_mut()
            .and_then(|t| t.remove("commit_max_delay_ms"));
        removed.push("writes.commit_max_delay_ms".to_string());
    }
    if legacy {
        value["writes"]
            .as_table_mut()
            .and_then(|t| t.remove("writer_lanes"));
        value["writes"]
            .as_table_mut()
            .and_then(|t| t.remove("commit_max_delay_ms"));
        removed.push("writes.writer_lanes".to_string());
        let fields = [
            ("", "streaming"),
            ("", "checkpoint"),
            ("server", "admin_listen"),
            ("storage", "min_free_mib"),
            ("writes", "batch_max_bytes_mib"),
            ("resources", "admission_timeout_ms"),
            ("resources", "max_inflight_per_batch"),
            ("resources", "admin_max_requests"),
            ("cache", "max_entries"),
            ("cache", "max_object_mib"),
            ("cache", "metadata_mib"),
            ("cache", "shards"),
        ];
        for (section, key) in fields {
            let field = if section.is_empty() {
                value.as_table_mut().and_then(|t| t.remove(key))
            } else {
                value
                    .as_table_mut()
                    .and_then(|t| t.get_mut(section))
                    .and_then(toml::Value::as_table_mut)
                    .and_then(|t| t.remove(key))
            };
            let _ = field;
            removed.push(if section.is_empty() {
                key.to_string()
            } else {
                format!("{section}.{key}")
            });
        }
        removed.push("writes.commit_max_delay_ms".to_string());
    }
    Ok((value, removed))
}
fn sample(v: &mut Vec<f64>, x: f64) {
    if v.len() < 100_000 {
        v.push(x);
    }
}
fn reservoir(v: &mut Vec<f64>, x: f64, n: u64) {
    const LIMIT: usize = 100_000;
    if v.len() < LIMIT {
        v.push(x);
        return;
    }
    let hash = blake3::hash(&n.to_le_bytes());
    let slot = u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap()) % n;
    if slot < LIMIT as u64 {
        v[slot as usize] = x;
    }
}
fn percentiles(v: &[f64]) -> serde_json::Value {
    let mut v = v.to_vec();
    v.sort_by(f64::total_cmp);
    let p = |q: f64| {
        if v.is_empty() {
            None
        } else {
            Some(v[((v.len() - 1) as f64 * q).ceil() as usize])
        }
    };
    serde_json::json!({"samples":v.len(),"p50_ms":p(0.5),"p95_ms":p(0.95),"p99_ms":p(0.99),"max_ms":p(1.0),"sampling":"deterministic reservoir of up to 100000 observations"})
}
fn with_total(mut value: serde_json::Value, total: u64, max: f64) -> serde_json::Value {
    value["total_count"] = total.into();
    if total > 0 {
        value["max_ms"] = max.into();
    }
    value
}
fn payload(seed: u64, worker: u64, op: u64, len: usize) -> Vec<u8> {
    let mut b = vec![0; len];
    let mut h = blake3::Hasher::new();
    h.update(&seed.to_le_bytes());
    h.update(&worker.to_le_bytes());
    h.update(&op.to_le_bytes());
    h.finalize_xof().fill(&mut b);
    b
}
fn skew_payload(seed: u64, worker: u64, op: u64, len: usize) -> Vec<u8> {
    // Hash prefixes select virtual shards. Rejection sampling ensures every
    // distinct payload routes to lane zero for lane counts 1, 2, 4, and 8.
    let mut nonce = 0u64;
    loop {
        let candidate = payload(seed.wrapping_add(nonce), worker, op, len);
        if blake3::hash(&candidate).as_bytes()[0] & 7 == 0 {
            return candidate;
        }
        nonce = nonce.wrapping_add(1);
    }
}
fn duplicate_outcome_valid(workload: &Workload, duplicate: bool) -> bool {
    !matches!(workload, Workload::DuplicatePut) || duplicate
}
fn header(kind: u8, ordinal: u64, data: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(60 + data.len());
    b.extend_from_slice(&((56 + data.len()) as u32).to_le_bytes());
    b.extend_from_slice(b"SKB1");
    b.extend_from_slice(&[kind, 0, 0, 0]);
    b.extend_from_slice(&ordinal.to_le_bytes());
    b.extend_from_slice(&[0; 32]);
    b.extend_from_slice(&(data.len() as u64).to_le_bytes());
    b.extend_from_slice(data);
    b
}
fn decode_frame(buf: &[u8], hashes: &[[u8; 32]], seen: &mut [bool]) -> Result<(usize, u8)> {
    ensure!(
        buf.len() == 60
            && u32::from_le_bytes(buf[0..4].try_into()?) == 56
            && &buf[4..8] == b"SKB1"
            && buf[10..12] == [0, 0]
            && buf[52..60] == [0; 8],
        "invalid batch result frame"
    );
    let kind = buf[8];
    let status = buf[9];
    let ordinal = u64::from_le_bytes(buf[12..20].try_into()?);
    if kind == 255 {
        ensure!(ordinal == u64::MAX && status != 0, "invalid terminal frame");
        anyhow::bail!("terminal_status_{status}");
    }
    ensure!(
        kind == 129 && (ordinal as usize) < hashes.len(),
        "invalid result kind or ordinal"
    );
    let i = ordinal as usize;
    ensure!(!seen[i], "duplicate result ordinal");
    if status == 0 {
        ensure!(buf[20..52] == hashes[i], "incorrect result hash");
    } else {
        ensure!(buf[20..52] == [0; 32], "invalid error hash");
    }
    seen[i] = true;
    Ok((i, status))
}
fn account_frame(
    frame: &[u8],
    hashes: &[[u8; 32]],
    seen: &mut [bool],
    ok: &mut u64,
    rejected: &mut u64,
) -> Result<u8> {
    let (_, status) = decode_frame(frame, hashes, seen)?;
    if status == 0 {
        *ok += 1;
    } else {
        *rejected += 1;
    }
    Ok(status)
}
async fn put(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    data: Vec<u8>,
) -> Result<(bool, usize)> {
    let hash = blake3::hash(&data).to_hex().to_string();
    let size = data.len();
    let r = client
        .put(format!("{url}/v1/objects"))
        .bearer_auth(token)
        .header("content-type", "application/octet-stream")
        .body(data)
        .send()
        .await?;
    let status = r.status();
    let body = r.text().await?;
    ensure!(status.is_success(), "http_{status}: {body}");
    let v: serde_json::Value = serde_json::from_str(&body)?;
    ensure!(v["hash"] == hash, "write hash mismatch");
    Ok((v["duplicate"].as_bool().unwrap_or(false), size))
}
async fn get(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    data: &[u8],
    hash: blake3::Hash,
) -> Result<usize> {
    let hash_text = hash.to_hex();
    let r = client
        .get(format!("{url}/v1/objects/{hash_text}"))
        .bearer_auth(token)
        .send()
        .await?;
    let status = r.status();
    ensure!(status.is_success(), "http_{status}");
    let b = r.bytes().await?;
    ensure!(
        blake3::hash(&b) == hash && b.as_ref() == data,
        "read mismatch"
    );
    Ok(b.len())
}
async fn batch(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    items: Vec<Vec<u8>>,
    start: Instant,
) -> (u64, u64, u64, Vec<f64>, Option<String>) {
    let hashes: Vec<_> = items.iter().map(|x| *blake3::hash(x).as_bytes()).collect();
    let n = items.len();
    let frames: Vec<_> = items
        .iter()
        .enumerate()
        .map(|(i, x)| Ok::<_, std::io::Error>(bytes::Bytes::from(header(1, i as u64, x))))
        .collect();
    let body = reqwest::Body::wrap_stream(futures_util::stream::iter(frames));
    let response = client
        .post(format!("{url}/v1/objects/batch-put"))
        .bearer_auth(token)
        .header("content-type", "application/vnd.skerry.batch-v1")
        .body(body)
        .send()
        .await;
    let mut ok = 0;
    let mut rejected = 0;
    let mut times = Vec::new();
    let mut seen = vec![false; n];
    let mut buf = Vec::new();
    let err = match response {
        Err(e) => Some(format!("transport: {e:?}")),
        Ok(r) if !r.status().is_success() => {
            let status = r.status();
            let text = r.text().await.unwrap_or_default();
            if status.as_u16() == 503 {
                rejected = n as u64;
            }
            Some(format!(
                "http_{status}: {}",
                text.chars().take(200).collect::<String>()
            ))
        }
        Ok(r) => {
            let mut stream = r.bytes_stream();
            let mut error = None;
            let mut fatal = false;
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(chunk) => {
                        buf.extend_from_slice(&chunk);
                        while buf.len() >= 60 {
                            let frame: Vec<_> = buf.drain(..60).collect();
                            match account_frame(&frame, &hashes, &mut seen, &mut ok, &mut rejected)
                            {
                                Ok(0) => {
                                    sample(&mut times, start.elapsed().as_secs_f64() * 1000.0);
                                }
                                Ok(s) => {
                                    error = Some(format!("item_status_{s}"));
                                }
                                Err(e) => {
                                    error = Some(e.to_string());
                                    fatal = true;
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error = Some(format!("stream: {e}"));
                        break;
                    }
                }
                if fatal {
                    break;
                }
            }
            if !buf.is_empty() {
                error = Some("truncated_frame".into());
            }
            if seen.iter().any(|x| !*x) && error.is_none() {
                error = Some("missing_results".into());
            }
            error
        }
    };
    (ok, rejected, n as u64 - ok - rejected, times, err)
}
#[tokio::main]
async fn main() -> Result<()> {
    let a = Args::parse();
    ensure!(
        a.workers > 0
            && [1, 2, 4, 8].contains(&a.writer_lanes)
            && !(a.legacy_config && a.baseline_config_0_3)
            && (!(a.legacy_config || a.baseline_config_0_3) || a.writer_lanes == 1)
            && (!(a.legacy_config || a.baseline_config_0_3) || a.commit_delay_ms.is_none())
            && a.seconds > 0
            && a.payload_bytes >= 24
            && a.batch_items > 0
            && a.batch_items <= 4096
            && a.read_percent <= 100
            && a.max_store_mib > 0,
        "invalid benchmark arguments"
    );
    ensure!(
        a.commit_delay_ms.is_none_or(|delay| delay <= 1000),
        "commit-delay-ms must be in 0..=1000"
    );
    ensure!(
        a.max_object_mib
            .checked_mul(1024 * 1024)
            .is_some_and(|max| a.payload_bytes <= max)
            && a.upload_mib >= a.max_object_mib,
        "payload or upload configuration exceeds limits"
    );
    ensure!(
        a.payload_bytes
            .checked_mul(a.batch_items)
            .is_some_and(|n| n <= 64 * 1024 * 1024),
        "batch payload exceeds 64 MiB per worker"
    );
    ensure!(a.fixture_items > 0, "fixture-items must be positive");
    let fixture_count = fixture_count(&a.workload, a.fixture_items);
    let batch_bytes = a
        .payload_bytes
        .checked_mul(a.batch_items)
        .context("batch size overflow")?;
    let fixture_bytes = a
        .payload_bytes
        .checked_mul(fixture_count)
        .context("fixture byte count overflow")?;
    let max_store_bytes = a
        .max_store_mib
        .checked_mul(1024 * 1024)
        .context("maximum store size overflow")?;
    let memory_estimate = batch_bytes
        .checked_mul(2)
        .and_then(|n| n.checked_mul(a.workers))
        .and_then(|n| n.checked_add(fixture_bytes))
        .context("client memory estimate overflow")?;
    ensure!(
        memory_estimate
            <= a.client_memory_mib
                .checked_mul(1024 * 1024)
                .context("client memory limit overflow")?,
        "estimated client payload memory exceeds --client-memory-mib"
    );
    let binary = std::fs::canonicalize(&a.binary)?;
    let binary_hash = blake3::hash(&std::fs::read(&binary)?).to_hex().to_string();
    let version_output = Command::new(&binary).arg("--version").output().await?;
    ensure!(
        version_output.status.success(),
        "could not read benchmark server version"
    );
    let binary_version = String::from_utf8_lossy(&version_output.stdout)
        .trim()
        .to_string();
    let parent = std::fs::canonicalize(&a.store_parent)?;
    let root = parent.join(format!("skerry-http-bench-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root)?;
    let mut child = ChildGuard {
        child: None,
        root: root.clone(),
        keep: a.keep_store,
    };
    let port = std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port();
    let url = format!("http://127.0.0.1:{port}");
    let token = uuid::Uuid::new_v4().to_string();
    let mut config = Config::default();
    config.storage.root = root.join("data");
    config.server.listen = format!("127.0.0.1:{port}");
    config.resources.max_requests = a.max_requests;
    if let Some(window) = a.inflight_per_batch {
        config.resources.max_inflight_per_batch = window;
    }
    config.resources.max_object_mib = a.max_object_mib;
    config.writes.max_pending_mib = a.upload_mib;
    config.writes.batch_max_bytes_mib = a.upload_mib.min(256);
    config.writes.batch_max_delay_ms = a.batch_delay_ms;
    config.cache.payload_mib = a.cache_mib;
    config.mcp.enabled = false;
    config.validate()?;
    let (effective_config, removed_legacy_fields) = effective_server_config(
        &config,
        a.legacy_config,
        a.baseline_config_0_3,
        a.writer_lanes,
        a.commit_delay_ms,
    )?;
    let config_path = root.join("config.toml");
    std::fs::write(&config_path, toml::to_string_pretty(&effective_config)?)?;
    // Keep the report aligned with the exact TOML written to disk, especially
    // when targeting a legacy server schema.
    let config_fingerprint = blake3::hash(toml::to_string(&effective_config)?.as_bytes())
        .to_hex()
        .to_string();
    let config = effective_config.clone();
    let log = std::fs::File::create(root.join("server.log"))?;
    let mut server = Command::new(&binary);
    server.arg("serve").arg("--config").arg(&config_path);
    clear_inherited_skerry_env(&mut server, std::env::vars_os().map(|(key, _)| key));
    let server = server
        .env("SKERRY_TOKEN", &token)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(log)
        .kill_on_drop(true)
        .spawn()?;
    child.child = Some(server);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(a.timeout_seconds))
        .pool_max_idle_per_host(a.workers + 4)
        .build()?;
    let ready_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.child.as_mut().unwrap().try_wait()? {
            anyhow::bail!(
                "server exited before ready: {status}; log: {}",
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
    let fixtures = Arc::new(
        (0..fixture_count)
            .map(|i| payload(a.seed, u64::MAX, i as u64, a.payload_bytes))
            .collect::<Vec<_>>(),
    );
    let fixture_hashes = Arc::new(fixtures.iter().map(|x| blake3::hash(x)).collect::<Vec<_>>());
    ensure!(
        fixture_bytes as u64 <= max_store_bytes,
        "fixture exceeds store budget"
    );
    let seed_batch_items = if fixture_count > 256 {
        256.min((16 * 1024 * 1024 / a.payload_bytes).max(1))
    } else {
        a.batch_items
            .min((256 * 1024 * 1024 / a.payload_bytes).max(1))
    };
    for group_start in (0..fixtures.len()).step_by(seed_batch_items) {
        let group_end = (group_start + seed_batch_items).min(fixtures.len());
        let items = fixtures[group_start..group_end].to_vec();
        let (ok, rejected, unknown, _, error) =
            batch(&client, &url, &token, items, Instant::now()).await;
        ensure!(
            ok == (group_end - group_start) as u64 && rejected == 0 && unknown == 0,
            "fixture batch write failed (ok={ok}, rejected={rejected}, unknown={unknown}, error={error:?})"
        );
    }
    let warm_end = Instant::now() + Duration::from_secs(a.warmup_seconds);
    let mut warm_index = 0usize;
    let mut warm_jobs = tokio::task::JoinSet::new();
    for _ in 0..a.workers {
        let index = warm_index % fixtures.len();
        warm_index = warm_index.wrapping_add(1);
        let (client, url, token, fixtures, hashes) = (
            client.clone(),
            url.clone(),
            token.clone(),
            fixtures.clone(),
            fixture_hashes.clone(),
        );
        warm_jobs.spawn(async move {
            get(&client, &url, &token, &fixtures[index], hashes[index]).await
        });
    }
    while Instant::now() < warm_end {
        let Some(result) = warm_jobs.join_next().await else {
            break;
        };
        result
            .context("warmup worker panicked")?
            .context("warmup GET")?;
        if Instant::now() < warm_end {
            let index = warm_index % fixtures.len();
            warm_index = warm_index.wrapping_add(1);
            let (client, url, token, fixtures, hashes) = (
                client.clone(),
                url.clone(),
                token.clone(),
                fixtures.clone(),
                fixture_hashes.clone(),
            );
            warm_jobs.spawn(async move {
                get(&client, &url, &token, &fixtures[index], hashes[index]).await
            });
        }
    }
    while let Some(result) = warm_jobs.join_next().await {
        result
            .context("warmup worker panicked")?
            .context("warmup GET")?;
    }
    let pre_load_status: serde_json::Value = client
        .get(format!("{url}/v1/status"))
        .bearer_auth(&token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let pre_load_metrics = client
        .get(format!("{url}/v1/metrics"))
        .bearer_auth(&token)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let cache_stats_before: serde_json::Value = client
        .get(format!("{url}/v1/cache/stats"))
        .bearer_auth(&token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let stats = Arc::new(Mutex::new(Stats::default()));
    let reserved = Arc::new(AtomicU64::new(fixture_bytes as u64));
    let operations = Arc::new(AtomicU64::new(0));
    let limited = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    let end = start + Duration::from_secs(a.seconds);
    let mut tasks = Vec::new();
    for worker in 0..a.workers {
        let a = a.clone();
        let client = client.clone();
        let url = url.clone();
        let token = token.clone();
        let fixtures = fixtures.clone();
        let fixture_hashes = fixture_hashes.clone();
        let stats = stats.clone();
        let reserved = reserved.clone();
        let operations = operations.clone();
        let limited = limited.clone();
        tasks.push(tokio::spawn(async move {
            let mut seq = 0u64;
            while Instant::now() < end {
                let ordinal = operations.fetch_add(1, Ordering::Relaxed);
                if a.max_operations.is_some_and(|m| ordinal >= m) {
                    break;
                }
                let read = matches!(a.workload, Workload::RandomGet | Workload::HotGet)
                    || matches!(a.workload, Workload::Mixed)
                        && (u64::from_le_bytes(
                            blake3::hash(&ordinal.to_le_bytes()).as_bytes()[..8]
                                .try_into()
                                .unwrap(),
                        ) % 100)
                            < a.read_percent;
                let count = if !read && matches!(a.workload, Workload::BatchPut) {
                    a.batch_items
                } else {
                    1
                };
                let duplicate_workload = matches!(a.workload, Workload::DuplicatePut);
                if !read && !duplicate_workload {
                    let amount = (a.payload_bytes as u64).saturating_mul(count as u64);
                    if reserved
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                            v.checked_add(amount).filter(|x| *x <= max_store_bytes)
                        })
                        .is_err()
                    {
                        limited.store(true, Ordering::Relaxed);
                        break;
                    }
                }
                let started = Instant::now();
                let mut s = Stats {
                    requests: 1,
                    ..Stats::default()
                };
                if read {
                    let index = if matches!(a.workload, Workload::HotGet) {
                        0
                    } else {
                        (ordinal as usize).wrapping_mul(2654435761) % fixtures.len()
                    };
                    match get(
                        &client,
                        &url,
                        &token,
                        &fixtures[index],
                        fixture_hashes[index],
                    )
                    .await
                    {
                        Ok(n) => {
                            s.success = 1;
                            s.bytes = n as u64;
                            sample(
                                &mut s.item_latencies,
                                started.elapsed().as_secs_f64() * 1000.0,
                            );
                        }
                        Err(e) => {
                            if e.to_string().starts_with("http_503") {
                                s.rejected = 1;
                            } else {
                                s.unknown = 1;
                            }
                            *s.errors.entry(e.to_string()).or_default() += 1;
                        }
                    }
                } else if count == 1 {
                    let data = if duplicate_workload {
                        fixtures[0].clone()
                    } else if matches!(a.workload, Workload::SkewPut) {
                        skew_payload(a.seed, worker as u64, seq, a.payload_bytes)
                    } else {
                        payload(a.seed, worker as u64, seq, a.payload_bytes)
                    };
                    match put(&client, &url, &token, data).await {
                        Ok((d, n)) => {
                            if !duplicate_outcome_valid(&a.workload, d) {
                                *s.errors
                                    .entry("expected_duplicate_false".into())
                                    .or_default() += 1;
                            }
                            s.success = 1;
                            s.bytes = n as u64;
                            s.duplicates = u64::from(d);
                            sample(
                                &mut s.item_latencies,
                                started.elapsed().as_secs_f64() * 1000.0,
                            );
                        }
                        Err(e) => {
                            if e.to_string().starts_with("http_503") {
                                s.rejected = 1;
                            } else {
                                s.unknown = 1;
                            }
                            *s.errors.entry(e.to_string()).or_default() += 1;
                        }
                    }
                } else {
                    let items = (0..count)
                        .map(|i| payload(a.seed, worker as u64, seq + i as u64, a.payload_bytes))
                        .collect();
                    let (ok, rejected, unknown, times, error) =
                        batch(&client, &url, &token, items, started).await;
                    s.success = ok;
                    s.rejected = rejected;
                    s.unknown = unknown;
                    s.bytes = ok * a.payload_bytes as u64;
                    s.item_latencies = times;
                    if let Some(e) = error {
                        *s.errors.entry(e).or_default() += 1;
                    }
                }
                if read && s.success == 0 {
                    s.failed_read_items = 1;
                }
                if !read {
                    s.unknown_write_items = s.unknown;
                }
                if !read && s.rejected > 0 {
                    reserved.fetch_sub(s.rejected * a.payload_bytes as u64, Ordering::SeqCst);
                }
                if read {
                    s.read_success = s.success;
                } else {
                    s.write_success = s.success;
                }
                seq += count as u64;
                sample(&mut s.latencies, started.elapsed().as_secs_f64() * 1000.0);
                let mut out = stats.lock().unwrap();
                out.failed_requests += u64::from(!s.errors.is_empty());
                out.requests += s.requests;
                let previous_success = out.success;
                out.success += s.success;
                out.read_success += s.read_success;
                out.write_success += s.write_success;
                out.failed_read_items += s.failed_read_items;
                out.unknown_write_items += s.unknown_write_items;
                out.rejected += s.rejected;
                out.unknown += s.unknown;
                out.bytes += s.bytes;
                out.duplicates += s.duplicates;
                for x in s.latencies {
                    out.latency_max = out.latency_max.max(x);
                    let n = out.requests;
                    reservoir(&mut out.latencies, x, n);
                    if s.errors.is_empty() {
                        out.success_request_max = out.success_request_max.max(x);
                        let n = out.requests - out.failed_requests;
                        reservoir(&mut out.success_request_latencies, x, n);
                    } else {
                        out.error_request_max = out.error_request_max.max(x);
                        let n = out.failed_requests;
                        reservoir(&mut out.error_request_latencies, x, n);
                    }
                }
                for (i, x) in s.item_latencies.into_iter().enumerate() {
                    out.item_latency_max = out.item_latency_max.max(x);
                    reservoir(&mut out.item_latencies, x, previous_success + i as u64 + 1);
                }
                for (k, v) in s.errors {
                    let key = if out.errors.contains_key(&k) || out.errors.len() < 32 {
                        k
                    } else {
                        "other_errors".to_string()
                    };
                    *out.errors.entry(key).or_default() += v;
                }
            }
        }));
    }
    let mut probes = BTreeMap::<String, serde_json::Value>::new();
    let mut probe_tasks = Vec::new();
    for path in ["ready", "status", "metrics"] {
        let client = client.clone();
        let url = url.clone();
        let token = token.clone();
        let name = path.to_string();
        probe_tasks.push(tokio::spawn(async move {
            let mut lat = Vec::new();
            let mut failures = 0;
            let mut missing = 0;
            let mut count = 0;
            let mut queue_samples = Vec::new();
            while Instant::now() < end {
                let t = Instant::now();
                let r = client
                    .get(format!("{url}/v1/{name}"))
                    .bearer_auth(&token)
                    .send()
                    .await;
                match r {
                    Ok(r) if r.status().is_success() => {
                        if name == "status" {
                            match r.json::<serde_json::Value>().await {
                                Ok(v) => {
                                    if v.get("storage").is_none_or(|storage| storage.is_null() || storage == false) { missing += 1; }
                                    if queue_samples.len() < 1000 { queue_samples.push(serde_json::json!({"write_queue_bytes":v.get("write_queue_bytes"),"write_queue_capacity_bytes":v.get("write_queue_capacity_bytes")})); }
                                }
                                Err(_) => missing += 1,
                            }
                        } else if name == "ready" {
                            if r.json::<serde_json::Value>().await.ok().is_none_or(|v| v["ready"] != true) { failures += 1; }
                        } else if r.bytes().await.is_err() { failures += 1; }
                    }
                    _ => failures += 1,
                };
                count += 1;
                sample(&mut lat, t.elapsed().as_secs_f64() * 1000.0);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            (name, count, failures, missing, lat, queue_samples)
        }));
    }
    for task in tasks {
        task.await?;
    }
    let drained = Instant::now();
    let post_load_status: serde_json::Value = client
        .get(format!("{url}/v1/status"))
        .bearer_auth(&token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let post_load_metrics = client
        .get(format!("{url}/v1/metrics"))
        .bearer_auth(&token)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    for task in probe_tasks {
        let (name, count, failures, missing, lat, queue_samples) = task.await?;
        probes.insert(name,serde_json::json!({"attempts":count,"successes":count-failures,"failures":failures,"missing_storage":missing,"latency":percentiles(&lat),"queue_samples":queue_samples}));
    }
    let elapsed = drained.duration_since(start).as_secs_f64();
    let measurement = elapsed.min(a.seconds as f64);
    let cache_response = client
        .get(format!("{url}/v1/cache/stats"))
        .bearer_auth(&token)
        .send()
        .await?;
    ensure!(
        cache_response.status().is_success(),
        "cache stats probe returned {}",
        cache_response.status()
    );
    let cache_stats: serde_json::Value = cache_response.json().await?;
    let s = stats.lock().unwrap().clone();
    let mut report = serde_json::json!({"benchmark":"http_capacity", "config_fingerprint":config_fingerprint, "logical_counter_snapshots":{"before":{"status":pre_load_status,"metrics":pre_load_metrics},"after":{"status":post_load_status,"metrics":post_load_metrics}}, "phase_seconds":{"load_window":measurement,"load_and_drain":elapsed,"drain":(elapsed-measurement).max(0.0)}, "method":"closed-loop; GET warmup cycles through the fixture set; includes operations started in the window and their complete drain; no coordinated-omission correction", "measurement_limitations":["closed-loop workload; no coordinated omission correction", "logical payload budget is not physical disk usage", "latency percentiles use a bounded deterministic reservoir", "single PUT HTTP 503 is rejected; other errors are unknown", "batch duplicates cannot be identified from protocol", "unknown_items includes unresolved reads; failed_read_items and unknown_write_items are reported separately"], "config":a, "server_config":config, "server_config_compatibility":{"legacy_config":a.legacy_config,"target_schema":if a.legacy_config{"0.2.2-compatible"}else if a.baseline_config_0_3{"0.3-compatible"}else{"current"},"removed_fields":removed_legacy_fields}, "environment":{"os":std::env::consts::OS,"arch":std::env::consts::ARCH,"binary_version":binary_version,"binary_blake3":binary_hash,"store_path":root,"store_retained":a.keep_store,"estimated_client_payload_bytes":memory_estimate}, "summary":{"attempted_requests":s.requests,"failed_requests":s.failed_requests,"successful_items":s.success,"successful_read_items":s.read_success,"successful_write_items":s.write_success,"read_ops_per_second":s.read_success as f64/elapsed,"write_ops_per_second":s.write_success as f64/elapsed,"rejected_items":s.rejected,"unknown_items":s.unknown,"failed_read_items":s.failed_read_items,"unknown_write_items":s.unknown_write_items,"successful_bytes":s.bytes,"duplicates":s.duplicates,"duration_seconds":elapsed,"load_seconds":measurement,"drain_seconds":(elapsed-measurement).max(0.0),"requests_per_second":s.requests as f64/measurement,"item_ops_per_second":s.success as f64/elapsed,"bytes_per_second":s.bytes as f64/elapsed,"store_limited":limited.load(Ordering::Relaxed),"logical_bytes_reserved":reserved.load(Ordering::Relaxed)},"latency":{"request_ms":with_total(percentiles(&s.latencies),s.requests,s.latency_max),"item_ack_ms":with_total(percentiles(&s.item_latencies),s.success,s.item_latency_max),"success_request_ms":with_total(percentiles(&s.success_request_latencies),s.requests-s.failed_requests,s.success_request_max),"error_request_ms":with_total(percentiles(&s.error_request_latencies),s.failed_requests,s.error_request_max)},"errors":s.errors,"cache_stats_before":cache_stats_before,"cache_stats_final":{"hits":cache_stats.get("hits"),"misses":cache_stats.get("misses"),"bytes":cache_stats.get("bytes"),"retained_bytes":cache_stats.get("retained_bytes"),"metadata_bytes":cache_stats.get("metadata_bytes"),"metadata_budget_bytes":cache_stats.get("metadata_budget_bytes"),"raw":cache_stats},"probes":probes});
    report["workload_notes"] = serde_json::Value::String(
        if matches!(a.workload, Workload::SkewPut) {
            "Skew-put deterministically generates distinct 4 KiB payloads with hash[0]&7==0; client hashing/rejection sampling CPU is included. Same-host localhost results include client CPU and scheduling contention; separate client/server hosts are preferable for server-capacity attribution.".into()
        } else if matches!(a.workload, Workload::DuplicatePut) {
            "Duplicate-put repeatedly sends the preloaded fixture and validates duplicate=true; physical storage growth is limited to the one preloaded fixture. Same-host localhost results include client CPU and scheduling contention.".into()
        } else {
            "Same-host localhost results include client CPU and scheduling contention.".into()
        },
    );
    let integrity_error = report["errors"].as_object().is_some_and(|errors| {
        errors.keys().any(|e| {
            [
                "mismatch",
                "incorrect result hash",
                "invalid batch result",
                "invalid result kind",
                "duplicate result ordinal",
                "truncated_frame",
                "expected_duplicate_false",
            ]
            .iter()
            .any(|needle| e.contains(needle))
        })
    });
    std::fs::write(&a.output, serde_json::to_vec_pretty(&report)?)?;
    child.child.as_mut().unwrap().kill().await?;
    let _ = child.child.as_mut().unwrap().wait().await;
    ensure!(
        !integrity_error,
        "integrity or protocol validation failed; see output report"
    );
    Ok(())
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
    #[test]
    fn frame_validation() {
        let hash = *blake3::hash(b"x").as_bytes();
        let mut frame = header(129, 0, &[]);
        frame[20..52].copy_from_slice(&hash);
        let mut seen = [false];
        assert_eq!(decode_frame(&frame, &[hash], &mut seen).unwrap(), (0, 0));
        assert!(decode_frame(&frame, &[hash], &mut seen).is_err());
        assert!(decode_frame(&frame[..59], &[hash], &mut [false]).is_err());
    }
    #[test]
    fn legacy_config_contains_only_old_schema_fields() {
        let config = Config::default();
        let (current, _) = effective_server_config(&config, false, false, 1, None).unwrap();
        assert!(current.get("streaming").is_some());
        assert_eq!(current["writes"]["writer_lanes"].as_integer(), Some(1));
        assert_eq!(
            current["writes"]["commit_max_delay_ms"].as_integer(),
            Some(10)
        );
        let (legacy, removed) = effective_server_config(&config, true, false, 1, None).unwrap();
        assert!(legacy.get("streaming").is_none());
        assert!(legacy["writes"].get("writer_lanes").is_none());
        assert!(legacy.get("checkpoint").is_none());
        assert!(legacy["cache"].get("shards").is_none());
        assert!(legacy["storage"].get("min_free_mib").is_none());
        assert!(removed.contains(&"resources.admission_timeout_ms".to_string()));
        let (baseline_0_3, removed) =
            effective_server_config(&config, false, true, 1, None).unwrap();
        assert!(baseline_0_3["writes"].get("writer_lanes").is_none());
        assert!(removed.contains(&"writes.writer_lanes".to_string()));
        assert!(baseline_0_3["writes"].get("commit_max_delay_ms").is_none());
        assert!(removed.contains(&"writes.commit_max_delay_ms".to_string()));
    }
    #[test]
    fn only_random_and_mixed_use_configured_fixture_count() {
        assert_eq!(fixture_count(&Workload::RandomGet, 128), 128);
        assert_eq!(fixture_count(&Workload::Mixed, 128), 128);
        assert_eq!(fixture_count(&Workload::HotGet, 128), 1);
        assert_eq!(fixture_count(&Workload::UniquePut, 128), 1);
        assert_eq!(fixture_count(&Workload::DuplicatePut, 128), 1);
        assert_eq!(fixture_count(&Workload::SkewPut, 128), 1);
    }
    #[test]
    fn writer_stress_patterns_validate_duplicate_and_skew_routing() {
        assert!(duplicate_outcome_valid(&Workload::DuplicatePut, true));
        assert!(!duplicate_outcome_valid(&Workload::DuplicatePut, false));
        assert!(duplicate_outcome_valid(&Workload::SkewPut, false));
        let first = skew_payload(7, 3, 11, 4096);
        let second = skew_payload(7, 3, 12, 4096);
        assert_ne!(first, second);
        for data in [&first, &second] {
            let shard = blake3::hash(data).as_bytes()[0];
            assert_eq!(shard & 7, 0);
            for lanes in [1, 2, 4, 8] {
                assert_eq!(usize::from(shard) % lanes, 0);
            }
        }
    }
    #[test]
    fn partial_item_failure_does_not_hide_later_ack() {
        let hash = *blake3::hash(b"x").as_bytes();
        let mut failed = header(129, 0, &[]);
        failed[9] = 5;
        let mut success = header(129, 1, &[]);
        success[20..52].copy_from_slice(&hash);
        let mut seen = [false; 2];
        let (mut ok, mut rejected) = (0, 0);
        assert_eq!(
            account_frame(&failed, &[hash, hash], &mut seen, &mut ok, &mut rejected).unwrap(),
            5
        );
        assert_eq!(
            account_frame(&success, &[hash, hash], &mut seen, &mut ok, &mut rejected).unwrap(),
            0
        );
        assert_eq!((ok, rejected, seen), (1, 1, [true, true]));
    }
    #[test]
    fn percentile_empty() {
        assert!(percentiles(&[])["p95_ms"].is_null());
        assert_eq!(percentiles(&[1., 2., 3.])["p95_ms"], 3.);
    }
}
