//! Reproducible engine-level foreground GET latency probe during segment verification.
//!
//! Run with `cargo run --release --example verification_foreground`. The benchmark
//! seeds a disposable store, disables the application payload cache, measures 500
//! verified `Store::get` calls, then repeats while a rate-limited segment scan runs.
//! It does not measure Coordinator queueing, the HTTP stack, or MCP overhead.

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use skerrykv::{config::Config, engine::Store, progress::Progress};
use std::{
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;

const OBJECTS: usize = 128;
const OBJECT_BYTES: usize = 128 * 1024;
const FOREGROUND_READS: usize = 500;
const VERIFY_RATE_MIB: u64 = 8;

struct DisposableStore(PathBuf);
impl Drop for DisposableStore {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Measurement {
    p50_ms: f64,
    p95_ms: f64,
    total_ms: f64,
    requests_per_second: f64,
    overlapped_requests: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let root = std::env::temp_dir().join(format!(
        "skerrykv-verification-foreground-{}-{nonce}",
        std::process::id()
    ));
    let _cleanup = DisposableStore(root.clone());

    let mut config = Config::default();
    config.storage.root = root;
    config.cache.payload_mib = 0;
    config.verification.max_concurrency = 1;
    config.verification.max_bytes_per_second_mib = VERIFY_RATE_MIB;
    config.validate()?;

    let store = Arc::new(Store::open(&config)?);
    let payloads: Vec<Bytes> = (0..OBJECTS)
        .map(|object| Bytes::from(payload(object)))
        .collect();
    let results = store.put_many(&payloads)?;
    ensure!(results.len() == OBJECTS, "seed write count mismatch");
    ensure!(
        results.iter().all(|result| !result.duplicate),
        "unexpected duplicate seed object"
    );
    let hashes: Vec<[u8; 32]> = payloads
        .iter()
        .map(|bytes| *blake3::hash(bytes).as_bytes())
        .collect();
    store.seal()?;
    drop(payloads);

    let page = store.list_segments(None, 1000)?;
    ensure!(
        page.segments.len() == 1,
        "benchmark seed should occupy exactly one segment"
    );
    let segment_id = Uuid::parse_str(&page.segments[0].segment_id)
        .context("segment listing returned an invalid UUID")?;

    let baseline = measure(store.clone(), &hashes, None).await?;

    let progress = Arc::new(Progress::new());
    let scan_store = store.clone();
    let scan_progress = progress.clone();
    let scan = tokio::task::spawn_blocking(move || {
        scan_store.verify_segment_with_progress_bounded(
            segment_id,
            scan_progress,
            Arc::new(AtomicBool::new(false)),
            Duration::from_secs(120),
        )
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while progress.snapshot().state != "running" {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .context("verification did not start in time")?;

    let during_verification = measure(store.clone(), &hashes, Some(progress.clone())).await?;
    let report = scan.await.context("verification worker panicked")??;
    let snapshot = progress.snapshot();
    ensure!(
        snapshot.state == "completed",
        "scan did not complete: {}",
        snapshot.state
    );
    ensure!(
        during_verification.overlapped_requests > 0,
        "GET samples did not overlap verification"
    );

    let output = serde_json::json!({
        "benchmark": "verification_foreground_get_latency",
        "method": "engine-level Store::get (BLAKE3-verified payload reads)",
        "scope": "This does not include Coordinator admission/queueing, application cache, HTTP, or MCP overhead.",
        "environment": {"os": std::env::consts::OS, "architecture": std::env::consts::ARCH},
        "seed": {
            "objects": OBJECTS,
            "object_bytes": OBJECT_BYTES,
            "total_payload_bytes": OBJECTS * OBJECT_BYTES,
            "segments": page.segments.len(),
            "payload_cache_mib": config.cache.payload_mib,
            "note": "Application payload cache is disabled. The baseline runs first and can warm the operating-system page cache, so this is not a cold-disk comparison."
        },
        "verification": {
            "segment_id": segment_id,
            "max_concurrency": config.verification.max_concurrency,
            "max_bytes_per_second_mib": VERIFY_RATE_MIB,
            "state": snapshot.state,
            "duration_seconds": snapshot.elapsed_seconds,
            "bytes_scanned": snapshot.bytes_scanned,
            "bytes_verified": snapshot.bytes_verified,
            "segments_verified": snapshot.segments_verified,
            "report_verified_bytes": report.verified_bytes,
            "report_verified_records": report.object_count
        },
        "foreground_reads": FOREGROUND_READS,
        "baseline_idle": measurement_json(&baseline),
        "while_verification_runs": measurement_json(&during_verification),
        "comparison": {
            "p95_ratio_during_to_idle": ratio(during_verification.p95_ms, baseline.p95_ms),
            "requests_overlapping_verification": during_verification.overlapped_requests
        }
    });
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

async fn measure(
    store: Arc<Store>,
    hashes: &[[u8; 32]],
    progress: Option<Arc<Progress>>,
) -> Result<Measurement> {
    let start = Instant::now();
    let mut latencies = Vec::with_capacity(FOREGROUND_READS);
    let mut overlapped_requests = 0;
    for n in 0..FOREGROUND_READS {
        if progress
            .as_ref()
            .is_some_and(|p| p.snapshot().state == "running")
        {
            overlapped_requests += 1;
        }
        let hash = hashes[(n * 73 + n / hashes.len()) % hashes.len()];
        let db = store.clone();
        let before = Instant::now();
        let payload = tokio::task::spawn_blocking(move || db.get(&hash))
            .await
            .context("foreground GET worker panicked")??;
        ensure!(
            payload.is_some(),
            "seed object disappeared during benchmark"
        );
        latencies.push(before.elapsed().as_secs_f64() * 1000.0);
    }
    let total = start.elapsed();
    latencies.sort_by(f64::total_cmp);
    Ok(Measurement {
        p50_ms: percentile(&latencies, 0.50),
        p95_ms: percentile(&latencies, 0.95),
        total_ms: total.as_secs_f64() * 1000.0,
        requests_per_second: FOREGROUND_READS as f64 / total.as_secs_f64(),
        overlapped_requests,
    })
}

fn payload(object: usize) -> Vec<u8> {
    (0..OBJECT_BYTES)
        .map(|offset| {
            (object
                .wrapping_mul(37)
                .wrapping_add(offset.wrapping_mul(17))
                .wrapping_add(offset / 251)) as u8
        })
        .collect()
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    sorted[((sorted.len() as f64 * quantile).ceil() as usize)
        .saturating_sub(1)
        .min(sorted.len() - 1)]
}

fn ratio(value: f64, baseline: f64) -> Option<f64> {
    (baseline > 0.0).then_some(value / baseline)
}

fn measurement_json(value: &Measurement) -> serde_json::Value {
    serde_json::json!({
        "p50_ms": value.p50_ms,
        "p95_ms": value.p95_ms,
        "total_ms": value.total_ms,
        "requests_per_second": value.requests_per_second,
        "overlapped_requests": value.overlapped_requests
    })
}
