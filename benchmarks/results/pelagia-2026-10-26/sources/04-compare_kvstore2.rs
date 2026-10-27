use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use skerrykv::{config::Config, coordinator::Coordinator, engine::Store};
use std::{path::PathBuf, sync::Arc, time::Instant};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    Engine,
    Coordinator,
}
#[derive(Clone, Copy, Debug, ValueEnum)]
enum WritePattern {
    Sequential,
    Concurrent,
    Batch64,
}
#[derive(Parser)]
struct Args {
    #[arg(long)]
    store: PathBuf,
    #[arg(long)]
    fixture: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long, value_enum)]
    mode: Mode,
    #[arg(long, default_value_t = 8)]
    workers: usize,
    #[arg(long)]
    operations: Option<usize>,
    #[arg(long, default_value_t = 20261026)]
    seed: u64,
    #[arg(long, value_enum, default_value_t = WritePattern::Sequential)]
    write_pattern: WritePattern,
    #[arg(long, default_value_t = 0)]
    payload_cache_mib: usize,
    #[arg(long, default_value_t = 10)]
    batch_delay_ms: u64,
}
#[derive(Deserialize)]
struct Manifest {
    lengths: Vec<usize>,
    read_order: Vec<usize>,
    hot_order: Vec<usize>,
    mixed_order: Vec<usize>,
    mixed_write_positions: Vec<usize>,
}
struct Fixture {
    payloads: Vec<Bytes>,
    hashes: Vec<[u8; 32]>,
    random: Vec<usize>,
    hot: Vec<usize>,
    mixed_order: Vec<usize>,
    mixed_lookup: Vec<Option<usize>>,
    mixed_payloads: Vec<Bytes>,
    mixed_hashes: Vec<[u8; 32]>,
}
#[derive(Clone)]
enum Backend {
    Engine(Arc<Store>),
    Coordinator(Arc<Coordinator>),
}
impl Backend {
    async fn put(&self, p: Bytes, expected: [u8; 32]) -> Result<()> {
        let result = match self {
            Self::Engine(s) => s.put_many(&[p])?.remove(0),
            Self::Coordinator(c) => c.put(p).await?,
        };
        ensure!(result.hash == expected, "put hash mismatch");
        Ok(())
    }
    async fn get(&self, hash: [u8; 32]) -> Result<Option<Bytes>> {
        match self {
            Self::Engine(s) => Ok(s.get(&hash)?.map(Bytes::from)),
            Self::Coordinator(c) => c.get(hash).await,
        }
    }
}
#[derive(Serialize)]
struct Phase {
    name: String,
    operations: usize,
    logical_bytes: u64,
    elapsed_seconds: f64,
    ops_per_second: f64,
    mib_per_second: f64,
    latency_p50_us: f64,
    latency_p95_us: f64,
    latency_p99_us: f64,
    max_us: f64,
    error_count: usize,
    errors: Vec<String>,
    latency_unit: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    read_operations: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    write_operations: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    read_logical_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    write_logical_bytes: Option<u64>,
}
struct Sample {
    us: f64,
    bytes: usize,
    error: Option<String>,
}
async fn one(b: &Backend, f: &Fixture, i: usize, write: bool) -> Sample {
    let start = Instant::now();
    if write {
        let result = b.put(f.payloads[i].clone(), f.hashes[i]).await;
        let us = start.elapsed().as_secs_f64() * 1e6;
        Sample {
            us,
            bytes: f.payloads[i].len(),
            error: result.err().map(|e| format!("{i}: {e:#}")),
        }
    } else {
        let result = b.get(f.hashes[i]).await;
        let us = start.elapsed().as_secs_f64() * 1e6;
        let error = match result {
            Ok(Some(v)) if v == f.payloads[i] => None,
            Ok(Some(_)) => Some(format!("{i}: payload mismatch")),
            Ok(None) => Some(format!("{i}: missing")),
            Err(e) => Some(format!("{i}: {e:#}")),
        };
        Sample {
            us,
            bytes: f.payloads[i].len(),
            error,
        }
    }
}
async fn mixed_one(b: &Backend, f: &Fixture, position: usize) -> Sample {
    let Some(i) = f.mixed_lookup[position] else {
        return one(b, f, f.mixed_order[position], false).await;
    };
    let start = Instant::now();
    let result = b.put(f.mixed_payloads[i].clone(), f.mixed_hashes[i]).await;
    let us = start.elapsed().as_secs_f64() * 1e6;
    Sample {
        us,
        bytes: f.mixed_payloads[i].len(),
        error: result.err().map(|e| format!("mixed {position}: {e:#}")),
    }
}
async fn phase(
    name: &str,
    b: &Backend,
    f: &Arc<Fixture>,
    order: &[usize],
    write: bool,
    concurrency: usize,
    batch_put: bool,
) -> Phase {
    let mixed = name == "mixed_read_write";
    let start = Instant::now();
    let mut samples = Vec::with_capacity(order.len());
    if batch_put && matches!(b, Backend::Engine(_)) {
        let Backend::Engine(s) = b else {
            unreachable!()
        };
        for chunk in order.chunks(64) {
            let payloads: Vec<_> = chunk.iter().map(|&i| f.payloads[i].clone()).collect();
            let t = Instant::now();
            let result = s.put_many(&payloads);
            let us = t.elapsed().as_secs_f64() * 1e6;
            for (j, &i) in chunk.iter().enumerate() {
                let error = match &result {
                    Ok(v) if v.get(j).is_some_and(|r| r.hash == f.hashes[i]) => None,
                    Ok(_) => Some(format!("{i}: put hash mismatch")),
                    Err(e) => Some(format!("{i}: {e:#}")),
                };
                samples.push(Sample {
                    us,
                    bytes: f.payloads[i].len(),
                    error,
                });
            }
        }
    } else if concurrency == 1 {
        for &i in order {
            samples.push(if mixed {
                mixed_one(b, f, i).await
            } else {
                one(b, f, i, write).await
            });
        }
    } else {
        // Chunked batches bound outstanding payloads and retain only timings, not returned data.
        let batch_size = if name.contains("batch_") || batch_put {
            64
        } else {
            order.len()
        };
        for batch in order.chunks(batch_size) {
            let mut tasks = Vec::new();
            for worker in 0..concurrency.min(batch.len()) {
                let indices: Vec<_> = batch
                    .iter()
                    .skip(worker)
                    .step_by(concurrency)
                    .copied()
                    .collect();
                let b = b.clone();
                let f = f.clone();
                tasks.push(tokio::spawn(async move {
                    let mut out = Vec::new();
                    for i in indices {
                        out.push(if mixed {
                            mixed_one(&b, &f, i).await
                        } else {
                            one(&b, &f, i, write).await
                        });
                    }
                    out
                }));
            }
            for task in tasks {
                match task.await {
                    Ok(v) => samples.extend(v),
                    Err(e) => samples.push(Sample {
                        us: 0.0,
                        bytes: 0,
                        error: Some(format!("worker: {e}")),
                    }),
                }
            }
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    let bytes = samples.iter().map(|s| s.bytes as u64).sum();
    let errors: Vec<_> = samples.iter().filter_map(|s| s.error.clone()).collect();
    let mut times: Vec<_> = samples.iter().map(|s| s.us).collect();
    times.sort_by(|a, b| a.total_cmp(b));
    let p = |v: f64| {
        if times.is_empty() {
            0.0
        } else {
            times[((times.len() as f64 * v).ceil() as usize)
                .saturating_sub(1)
                .min(times.len() - 1)]
        }
    };
    let write_operations = f.mixed_hashes.len();
    let write_bytes: u64 = f.mixed_payloads.iter().map(|p| p.len() as u64).sum();
    let read_bytes: u64 = f
        .mixed_order
        .iter()
        .enumerate()
        .filter(|(i, _)| f.mixed_lookup[*i].is_none())
        .map(|(_, &i)| f.payloads[i].len() as u64)
        .sum();
    Phase {
        read_operations: mixed.then_some(order.len() - write_operations),
        write_operations: mixed.then_some(write_operations),
        read_logical_bytes: mixed.then_some(read_bytes),
        write_logical_bytes: mixed.then_some(write_bytes),
        name: name.into(),
        operations: order.len(),
        logical_bytes: bytes,
        elapsed_seconds: elapsed,
        ops_per_second: order.len() as f64 / elapsed,
        mib_per_second: bytes as f64 / 1048576.0 / elapsed,
        latency_p50_us: p(0.50),
        latency_p95_us: p(0.95),
        latency_p99_us: p(0.99),
        max_us: p(1.0),
        error_count: errors.len(),
        errors,
        latency_unit: if batch_put && matches!(b, Backend::Engine(_)) {
            "per_item_batch_completion"
        } else {
            "per_operation"
        },
    }
}
async fn warm(b: &Backend, f: &Fixture, passes: usize) -> Result<()> {
    for _ in 0..passes {
        for i in 0..f.payloads.len() {
            let s = one(b, f, i, false).await;
            if let Some(e) = s.error {
                bail!("warmup failed: {e}");
            }
        }
    }
    Ok(())
}
fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(args.workers > 0, "workers must be nonzero");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.workers.max(2))
        .enable_all()
        .build()?;
    runtime.block_on(run(args))
}
async fn run(args: Args) -> Result<()> {
    ensure!(args.workers > 0, "workers must be nonzero");
    ensure!(
        args.payload_cache_mib == 0 || args.payload_cache_mib == 256,
        "payload cache must be 0 or 256 MiB"
    );
    ensure!(
        !args.store.exists() || std::fs::read_dir(&args.store)?.next().is_none(),
        "store must be absent or empty"
    );
    let start = Instant::now();
    let manifest: Manifest =
        serde_json::from_slice(&std::fs::read(args.fixture.join("manifest.json"))?)?;
    let n = args.operations.unwrap_or(manifest.lengths.len());
    ensure!(
        n > 0 && n == manifest.lengths.len(),
        "operations must equal nonzero fixture count"
    );
    ensure!(
        manifest.read_order.len() == n && manifest.hot_order.len() == n,
        "order lengths must match fixture count"
    );
    ensure!(
        manifest.lengths.iter().all(|&x| x > 0),
        "payload lengths must be nonzero"
    );
    ensure!(
        manifest
            .read_order
            .iter()
            .chain(&manifest.hot_order)
            .all(|&x| x < n),
        "order index out of range"
    );
    let mut sorted = manifest.read_order.clone();
    sorted.sort_unstable();
    ensure!(
        sorted == (0..n).collect::<Vec<_>>(),
        "read order must be a permutation"
    );
    let mut mixed_sorted = manifest.mixed_order.clone();
    mixed_sorted.sort_unstable();
    ensure!(
        mixed_sorted == (0..n).collect::<Vec<_>>(),
        "mixed order must be a permutation"
    );
    ensure!(
        manifest.mixed_write_positions.len() == n.div_ceil(4),
        "mixed write count must be ceil(N/4)"
    );
    let mut positions = manifest.mixed_write_positions.clone();
    positions.sort_unstable();
    positions.dedup();
    ensure!(
        positions.len() == n.div_ceil(4) && positions.iter().all(|&i| i < n),
        "mixed write positions must be unique and in range"
    );
    let raw = Bytes::from(std::fs::read(args.fixture.join("payloads.bin"))?);
    let mut pos = 0usize;
    let mut payloads = Vec::with_capacity(n);
    for len in manifest.lengths {
        let end = pos.checked_add(len).context("fixture length overflow")?;
        ensure!(end <= raw.len(), "truncated payloads");
        payloads.push(raw.slice(pos..end));
        pos = end;
    }
    ensure!(pos == raw.len(), "trailing payload data");
    let hashes: Vec<[u8; 32]> = payloads
        .iter()
        .map(|p| *blake3::hash(p).as_bytes())
        .collect();
    let mut seen: std::collections::HashSet<[u8; 32]> = hashes.iter().copied().collect();
    ensure!(seen.len() == n, "fixture original payloads must be unique");
    let mut mixed_payloads = Vec::with_capacity(n.div_ceil(4));
    let mut mixed_hashes = Vec::with_capacity(n.div_ceil(4));
    let mut mixed_lookup = vec![None; n];
    for position in positions {
        let i = manifest.mixed_order[position];
        mixed_lookup[position] = Some(mixed_payloads.len());
        let mut p = payloads[i].to_vec();
        p[0] ^= 0xff;
        let hash = *blake3::hash(&p).as_bytes();
        ensure!(
            seen.insert(hash),
            "mixed payload {i} collides with another fixture payload"
        );
        mixed_payloads.push(Bytes::from(p));
        mixed_hashes.push(hash);
    }
    let f = Arc::new(Fixture {
        payloads,
        hashes,
        random: manifest.read_order,
        hot: manifest.hot_order,
        mixed_order: manifest.mixed_order,
        mixed_lookup,
        mixed_payloads,
        mixed_hashes,
    });
    let mut config = Config::default();
    config.storage.root = args.store.clone();
    config.cache.payload_mib = args.payload_cache_mib;
    config.writes.batch_max_delay_ms = args.batch_delay_ms;
    config.validate()?;
    let b = match args.mode {
        Mode::Engine => Backend::Engine(Arc::new(Store::open(&config)?)),
        Mode::Coordinator => Backend::Coordinator(Coordinator::open(config.clone()).await?),
    };
    let setup_seconds = start.elapsed().as_secs_f64();
    let seq: Vec<_> = (0..n).collect();
    let mut phases = Vec::new();
    let (name, workers, batch) = match args.write_pattern {
        WritePattern::Sequential => ("write_sequential", 1, false),
        WritePattern::Concurrent => ("write_concurrent", args.workers, false),
        WritePattern::Batch64 => ("write_batch64", 64, true),
    };
    phases.push(phase(name, &b, &f, &seq, true, workers, batch).await);
    phases.push(phase("write_duplicate", &b, &f, &f.hot, true, 1, false).await);
    for (name, order, workers) in [
        ("read_sequential", &seq, 1),
        ("read_random", &f.random, 1),
        ("read_hot", &f.hot, 1),
        ("read_concurrent_random", &f.random, args.workers),
        ("read_batch_ordered64", &f.random, 1),
        ("read_batch_unordered64", &f.random, args.workers.min(64)),
    ] {
        warm(
            &b,
            &f,
            if matches!(args.mode, Mode::Coordinator) && args.payload_cache_mib > 0 {
                2
            } else {
                1
            },
        )
        .await?;
        phases.push(phase(name, &b, &f, order, false, workers, false).await);
    }
    warm(
        &b,
        &f,
        if matches!(args.mode, Mode::Coordinator) && args.payload_cache_mib > 0 {
            2
        } else {
            1
        },
    )
    .await?;
    phases.push(phase("mixed_read_write", &b, &f, &seq, false, args.workers, false).await);
    let (storage, io, cache) = match &b {
        Backend::Engine(s) => {
            s.seal()?;
            s.sync()?;
            (serde_json::to_value(s.status()?)?, None, None)
        }
        Backend::Coordinator(c) => {
            c.shutdown().await?;
            let st = c.status().await?;
            (
                serde_json::to_value(st.storage)?,
                Some(st.io),
                Some(st.cache),
            )
        }
    };
    let failed = phases.iter().any(|p| p.error_count > 0);
    let output = serde_json::json!({"engine": match args.mode { Mode::Engine => "skerry_engine", Mode::Coordinator => "skerry_coordinator" }, "config": {"durability":"SyncAll", "batch_delay_ms":args.batch_delay_ms, "payload_cache_mib":args.payload_cache_mib, "workers":args.workers, "operations":n, "seed":args.seed, "write_pattern":format!("{:?}",args.write_pattern), "read_warmup":"full pass before each phase; two passes for enabled coordinator cache; OS cache warm; no cold-cache claim", "mixed_workload":"mixed_order is a random permutation; mixed_write_positions independently select ceil(N/4) new writes; remaining positions read originals", "runtime_worker_threads":args.workers.max(2), "storage_config": config}, "setup_seconds":setup_seconds, "phases":phases, "storage":storage, "io":io, "cache":cache });
    std::fs::write(&args.output, serde_json::to_vec_pretty(&output)?)?;
    ensure!(
        !failed,
        "one or more benchmark operations failed; see output"
    );
    Ok(())
}
