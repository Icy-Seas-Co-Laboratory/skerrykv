use crate::{config::Config, coordinator::Coordinator};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use std::{path::PathBuf, sync::Arc, time::Instant};
use uuid::Uuid;

const SIZES: [usize; 4] = [5 * 1024, 20 * 1024, 32 * 1024, 128 * 1024];

#[derive(Default)]
struct Metrics {
    latency_ms: Vec<f64>,
    logical_bytes: u64,
}
impl Metrics {
    fn push(&mut self, started: Instant, bytes: usize) {
        self.latency_ms
            .push(started.elapsed().as_secs_f64() * 1000.0);
        self.logical_bytes += bytes as u64;
    }
    fn append(&mut self, mut other: Self) {
        self.logical_bytes += other.logical_bytes;
        self.latency_ms.append(&mut other.latency_ms);
    }
}

/// Run the concise default workload through the coordinator.
pub async fn run(
    config: &Config,
    objects: usize,
    payload_bytes: usize,
    workers: usize,
    mixed: bool,
) -> Result<()> {
    ensure!(objects > 0, "--objects must be greater than zero");
    ensure!(payload_bytes > 0, "--bytes must be greater than zero");
    ensure!(workers > 0, "--workers must be greater than zero");
    ensure!(
        payload_bytes <= config.max_object_bytes(),
        "--bytes exceeds configured max_object_mib"
    );
    let label = if mixed {
        "concurrent-ingest-random-read"
    } else {
        "concurrent-ingest"
    };
    let run_id = Uuid::new_v4().simple().to_string();
    let scenario = scenario_config(config, &run_id, &config.reads.policy, label)?;
    let coordinator = Coordinator::open(scenario.clone()).await?;
    let started = Instant::now();
    let writes = ingest(&coordinator, label, objects, payload_bytes, workers, true).await?;
    let mut reads = Metrics::default();
    if mixed {
        let hashes = (0..objects)
            .map(|i| object_hash(label, i, payload_bytes))
            .collect::<Vec<_>>();
        reads = random_single_reads(&coordinator, &hashes, label).await?;
    }
    finish(&coordinator, &scenario, label, started, writes, reads).await
}

/// Run repeatable workload comparisons in isolated stores for each I/O policy and scenario.
pub async fn suite(config: &Config, objects: usize, workers: usize) -> Result<()> {
    ensure!(objects > 0, "--objects must be greater than zero");
    ensure!(workers > 0, "--workers must be greater than zero");
    ensure!(
        SIZES.iter().all(|size| *size <= config.max_object_bytes()),
        "the benchmark suite includes 128 KiB objects; configured max_object_mib is too small"
    );
    let run_id = Uuid::new_v4().simple().to_string();
    println!(
        "benchmark suite: run {run_id}, {objects} objects per workload, {workers} requested workers"
    );
    println!(
        "isolated stores: {}",
        config
            .storage
            .root
            .join("bench-suite")
            .join(&run_id)
            .display()
    );
    for policy in ["ssd", "hdd"] {
        println!("\n=== read policy: {policy} ===");
        for size in SIZES {
            for (name, concurrent) in [("sequential-ingest", false), ("concurrent-ingest", true)] {
                let cfg = scenario_config(config, &run_id, policy, &format!("{name}-{size}"))?;
                let coordinator = Coordinator::open(cfg.clone())
                    .await
                    .context("opening suite store")?;
                let started = Instant::now();
                let writes = ingest(
                    &coordinator,
                    &format!("{run_id}/{policy}/{name}-{size}"),
                    objects,
                    size,
                    workers,
                    concurrent,
                )
                .await?;
                finish(
                    &coordinator,
                    &cfg,
                    &format!("{name} ({size} bytes/object)"),
                    started,
                    writes,
                    Metrics::default(),
                )
                .await?;
            }

            let cfg = scenario_config(config, &run_id, policy, &format!("random-singles-{size}"))?;
            let coordinator = Coordinator::open(cfg.clone()).await?;
            let label = format!("{run_id}/{policy}/random-singles-{size}");
            let hashes = seed_objects(&coordinator, &label, objects, size).await?;
            let started = Instant::now();
            let reads = random_single_reads(&coordinator, &hashes, &label).await?;
            finish(
                &coordinator,
                &cfg,
                &format!("random single reads ({size} bytes/object, warm)"),
                started,
                Metrics::default(),
                reads,
            )
            .await?;

            for ordered in [true, false] {
                let mode = if ordered {
                    "batch-ordered"
                } else {
                    "batch-completion"
                };
                let cfg = scenario_config(config, &run_id, policy, &format!("{mode}-{size}"))?;
                let coordinator = Coordinator::open(cfg.clone()).await?;
                let label = format!("{run_id}/{policy}/{mode}-{size}");
                let mut hashes = seed_objects(&coordinator, &label, objects, size).await?;
                shuffle_hashes(
                    &mut hashes,
                    seed(&format!("{run_id}/{policy}/batch-order-{size}")),
                );
                let window = batch_window(&cfg, workers, size, ordered);
                let started = Instant::now();
                let reads = buffered_batch_reads(
                    &coordinator,
                    hashes,
                    window,
                    cfg.resources.max_batch_items,
                    ordered,
                )
                .await?;
                let mode_label = if ordered {
                    "batch ordered (sequential window=1)"
                } else {
                    "batch completion (bounded unordered)"
                };
                finish(
                    &coordinator,
                    &cfg,
                    &format!("{mode_label}, window {window}, {size} bytes/object"),
                    started,
                    Metrics::default(),
                    reads,
                )
                .await?;
            }
        }

        let cfg = scenario_config(config, &run_id, policy, "mixed-size-ingest")?;
        let coordinator = Coordinator::open(cfg.clone()).await?;
        let label = format!("{run_id}/{policy}/mixed-size-ingest");
        let started = Instant::now();
        let writes = mixed_size_ingest(&coordinator, &label, objects, workers).await?;
        finish(
            &coordinator,
            &cfg,
            "mixed-size concurrent ingest",
            started,
            writes,
            Metrics::default(),
        )
        .await?;

        let cfg = scenario_config(config, &run_id, policy, "mixed-read-write")?;
        let coordinator = Coordinator::open(cfg.clone()).await?;
        let label = format!("{run_id}/{policy}/mixed-read-write");
        let hashes = seed_objects(
            &coordinator,
            &format!("{label}/read-seed"),
            objects,
            SIZES[0],
        )
        .await?;
        let started = Instant::now();
        let (writes, reads) =
            concurrent_mixed_read_write(&coordinator, &hashes, &label, objects, workers).await?;
        finish(
            &coordinator,
            &cfg,
            "concurrent mixed read/write (warm reads)",
            started,
            writes,
            reads,
        )
        .await?;
    }
    println!(
        "\nSuite notes: reads run after prefill or ingest; cache is not reset and results make no cold-cache claim. CPU/RSS unavailable."
    );
    Ok(())
}

fn scenario_config(config: &Config, run_id: &str, policy: &str, label: &str) -> Result<Config> {
    let mut scenario = config.clone();
    scenario.storage.root = absolute_path(
        scenario
            .storage
            .root
            .join("bench-suite")
            .join(run_id)
            .join(policy)
            .join(label),
    )?;
    scenario.reads.policy = policy.to_owned();
    scenario.validate()?;
    Ok(scenario)
}
fn absolute_path(path: PathBuf) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

async fn ingest(
    coordinator: &Arc<Coordinator>,
    label: &str,
    objects: usize,
    size: usize,
    workers: usize,
    concurrent: bool,
) -> Result<Metrics> {
    if !concurrent {
        let mut metrics = Metrics::default();
        for index in 0..objects {
            let payload = payload(label, index, size);
            let started = Instant::now();
            coordinator.put(Bytes::from(payload)).await?;
            metrics.push(started, size);
        }
        return Ok(metrics);
    }
    let worker_count = workers.min(objects.max(1));
    let mut jobs = tokio::task::JoinSet::new();
    for worker in 0..worker_count {
        let count = objects / worker_count + usize::from(worker < objects % worker_count);
        let first = worker * (objects / worker_count) + worker.min(objects % worker_count);
        let coordinator = Arc::clone(coordinator);
        let label = label.to_owned();
        jobs.spawn(async move {
            let mut result = Metrics::default();
            for offset in 0..count {
                let size = size;
                let data = payload(&label, first + offset, size);
                let started = Instant::now();
                coordinator.put(Bytes::from(data)).await?;
                result.push(started, size);
            }
            Ok::<_, anyhow::Error>(result)
        });
    }
    let mut metrics = Metrics::default();
    while let Some(job) = jobs.join_next().await {
        metrics.append(job.context("ingest worker failed")??);
    }
    Ok(metrics)
}

async fn mixed_size_ingest(
    coordinator: &Arc<Coordinator>,
    label: &str,
    objects: usize,
    workers: usize,
) -> Result<Metrics> {
    let worker_count = workers.min(objects.max(1));
    let mut jobs = tokio::task::JoinSet::new();
    for worker in 0..worker_count {
        let count = objects / worker_count + usize::from(worker < objects % worker_count);
        let first = worker * (objects / worker_count) + worker.min(objects % worker_count);
        let coordinator = Arc::clone(coordinator);
        let label = label.to_owned();
        jobs.spawn(async move {
            let mut result = Metrics::default();
            for offset in 0..count {
                let index = first + offset;
                let size = SIZES[index % SIZES.len()];
                let data = payload(&label, index, size);
                let started = Instant::now();
                coordinator.put(Bytes::from(data)).await?;
                result.push(started, size);
            }
            Ok::<_, anyhow::Error>(result)
        });
    }
    let mut metrics = Metrics::default();
    while let Some(job) = jobs.join_next().await {
        metrics.append(job.context("mixed-size worker failed")??);
    }
    Ok(metrics)
}

async fn seed_objects(
    coordinator: &Arc<Coordinator>,
    label: &str,
    objects: usize,
    size: usize,
) -> Result<Vec<[u8; 32]>> {
    let mut hashes = Vec::with_capacity(objects);
    for index in 0..objects {
        let result = coordinator
            .put(Bytes::from(payload(label, index, size)))
            .await?;
        hashes.push(result.hash);
    }
    Ok(hashes)
}

async fn random_single_reads(
    coordinator: &Arc<Coordinator>,
    hashes: &[[u8; 32]],
    label: &str,
) -> Result<Metrics> {
    ensure!(!hashes.is_empty(), "no benchmark objects to read");
    let mut metrics = Metrics::default();
    let mut rng = seed(label);
    for _ in 0..hashes.len() {
        rng = xorshift(rng);
        let hash = hashes[rng as usize % hashes.len()];
        let started = Instant::now();
        let data = coordinator
            .get(hash)
            .await?
            .context("benchmark object disappeared")?;
        metrics.push(started, data.len());
        drop(data);
    }
    Ok(metrics)
}

fn batch_window(config: &Config, workers: usize, object_bytes: usize, ordered: bool) -> usize {
    if ordered {
        return 1;
    }
    let budget_slots =
        (config.resources.max_inflight_read_mib * 1024 * 1024 / object_bytes.max(1)).max(1);
    workers
        .max(1)
        .min(config.reads.max_concurrency.clamp(1, 32))
        .min(budget_slots)
}

fn shuffle_hashes(hashes: &mut [[u8; 32]], mut rng: u64) {
    for index in (1..hashes.len()).rev() {
        rng = xorshift(rng);
        hashes.swap(index, rng as usize % (index + 1));
    }
}

async fn buffered_batch_reads(
    coordinator: &Arc<Coordinator>,
    hashes: Vec<[u8; 32]>,
    concurrency: usize,
    max_batch_items: usize,
    ordered: bool,
) -> Result<Metrics> {
    let mut metrics = Metrics::default();
    for batch in hashes.chunks(max_batch_items.max(1)) {
        let operations = stream::iter(batch.iter().copied().map(|hash| {
            let coordinator = Arc::clone(coordinator);
            async move {
                let started = Instant::now();
                let data = coordinator
                    .get(hash)
                    .await?
                    .context("benchmark object disappeared")?;
                let bytes = data.len();
                drop(data); // Release the payload before yielding timing metadata.
                Ok::<_, anyhow::Error>((started.elapsed().as_secs_f64() * 1000.0, bytes as u64))
            }
        }));
        let completed = if ordered {
            operations.buffered(concurrency).collect::<Vec<_>>().await
        } else {
            operations
                .buffer_unordered(concurrency)
                .collect::<Vec<_>>()
                .await
        };
        for operation in completed {
            let (latency_ms, bytes) = operation?;
            metrics.latency_ms.push(latency_ms);
            metrics.logical_bytes += bytes;
        }
    }
    Ok(metrics)
}

async fn concurrent_mixed_read_write(
    coordinator: &Arc<Coordinator>,
    read_hashes: &[[u8; 32]],
    label: &str,
    objects: usize,
    workers: usize,
) -> Result<(Metrics, Metrics)> {
    ensure!(!read_hashes.is_empty(), "no benchmark read seed objects");
    let writer_count = workers.div_ceil(2).max(1).min(objects.max(1));
    let reader_count = (workers / 2).max(1).min(objects.max(1));
    let mut jobs = tokio::task::JoinSet::new();
    for worker in 0..writer_count {
        let count = objects / writer_count + usize::from(worker < objects % writer_count);
        let first = worker * (objects / writer_count) + worker.min(objects % writer_count);
        let coordinator = Arc::clone(coordinator);
        let label = label.to_owned();
        jobs.spawn(async move {
            let mut metrics = Metrics::default();
            for offset in 0..count {
                let size = SIZES[(first + offset) % SIZES.len()];
                let started = Instant::now();
                coordinator
                    .put(Bytes::from(payload(&label, first + offset, size)))
                    .await?;
                metrics.push(started, size);
            }
            Ok::<_, anyhow::Error>((true, metrics))
        });
    }
    for worker in 0..reader_count {
        let count = objects / reader_count + usize::from(worker < objects % reader_count);
        let coordinator = Arc::clone(coordinator);
        let hashes = read_hashes.to_vec();
        let label = label.to_owned();
        jobs.spawn(async move {
            let mut metrics = Metrics::default();
            let mut rng = seed(&label).wrapping_add(worker as u64);
            for _ in 0..count {
                rng = xorshift(rng);
                let hash = hashes[rng as usize % hashes.len()];
                let started = Instant::now();
                let data = coordinator
                    .get(hash)
                    .await?
                    .context("benchmark object disappeared")?;
                metrics.push(started, data.len());
                drop(data);
            }
            Ok::<_, anyhow::Error>((false, metrics))
        });
    }
    let mut writes = Metrics::default();
    let mut reads = Metrics::default();
    while let Some(job) = jobs.join_next().await {
        let (is_write, metrics) = job.context("mixed worker failed")??;
        if is_write {
            writes.append(metrics);
        } else {
            reads.append(metrics);
        }
    }
    Ok((writes, reads))
}

async fn finish(
    coordinator: &Arc<Coordinator>,
    config: &Config,
    label: &str,
    started: Instant,
    writes: Metrics,
    reads: Metrics,
) -> Result<()> {
    let elapsed = started.elapsed().as_secs_f64();
    let status = coordinator.status().await?;
    coordinator.shutdown().await?;
    println!("\n--- {label} ({}) ---", config.reads.policy);
    report_metrics("write", &writes, elapsed);
    if !reads.latency_ms.is_empty() {
        report_metrics("read", &reads, elapsed);
    }
    let physical = status.storage.physical_bytes;
    let payload_ratio = if physical == 0 {
        0.0
    } else {
        status.storage.payload_bytes as f64 / physical as f64 * 100.0
    };
    let capacity = status.segment_capacity_bytes as u128 * status.storage.segments as u128;
    let capacity_ratio = if capacity == 0 {
        0.0
    } else {
        physical as f64 / capacity as f64 * 100.0
    };
    println!(
        "storage: objects {}, segments {}, logical payload {}, index {}, physical {}; payload/physical {:.1}%, segment capacity {}/{} ({:.2}%)",
        status.storage.objects,
        status.storage.segments,
        status.storage.payload_bytes,
        status.storage.index_bytes,
        physical,
        payload_ratio,
        physical,
        capacity,
        capacity_ratio,
    );
    println!(
        "actual I/O: reads {}, read bytes {}, writes {}, write bytes {}, deduplicated {}, deduplicated bytes {}; cache served {} bytes, hits {}, misses {}",
        status.io.reads,
        status.io.read_bytes,
        status.io.writes,
        status.io.write_bytes,
        status.io.deduplicated,
        status.io.deduplicated_bytes,
        status.cache.served_bytes,
        status.cache.hits,
        status.cache.misses,
    );
    println!(
        "coordinator totals (whole scenario, including prefill): queue wait {:.3} ms, index lookup {:.3} ms, disk read {:.3} ms, storage write {:.3} ms; current write queue {}/{} bytes",
        status.io.queue_wait_ms,
        status.io.index_lookup_ms,
        status.io.disk_read_ms,
        status.io.storage_write_ms,
        status.write_queue_bytes,
        status.write_queue_capacity_bytes,
    );
    println!(
        "index lookup is measured via coordinator timing; CPU/RSS unavailable. Reads run after prefill or ingest, with no cold-cache claim."
    );
    Ok(())
}

fn report_metrics(name: &str, metrics: &Metrics, duration: f64) {
    if metrics.latency_ms.is_empty() {
        return;
    }
    let mut sorted = metrics.latency_ms.clone();
    sorted.sort_by(f64::total_cmp);
    let percentile = |p: f64| sorted[((sorted.len() - 1) as f64 * p).round() as usize];
    let secs = duration.max(f64::EPSILON);
    println!(
        "{name}: {} ops, {:.2} ops/s, {:.2} logical MiB/s, p50 {:.3} ms, p95 {:.3} ms, p99 {:.3} ms",
        sorted.len(),
        sorted.len() as f64 / secs,
        metrics.logical_bytes as f64 / secs / (1024.0 * 1024.0),
        percentile(0.50),
        percentile(0.95),
        percentile(0.99)
    );
}

fn payload(label: &str, object: usize, size: usize) -> Vec<u8> {
    let mut result = vec![0u8; size];
    for (block, output) in result.chunks_mut(32).enumerate() {
        let mut hasher = blake3::Hasher::new();
        hasher.update(label.as_bytes());
        hasher.update(&(object as u64).to_le_bytes());
        hasher.update(&(block as u64).to_le_bytes());
        output.copy_from_slice(&hasher.finalize().as_bytes()[..output.len()]);
    }
    result
}
fn object_hash(label: &str, object: usize, size: usize) -> [u8; 32] {
    *blake3::hash(&payload(label, object, size)).as_bytes()
}
fn seed(label: &str) -> u64 {
    u64::from_le_bytes(
        blake3::hash(label.as_bytes()).as_bytes()[..8]
            .try_into()
            .unwrap(),
    )
}
fn xorshift(mut value: u64) -> u64 {
    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
    value
}
