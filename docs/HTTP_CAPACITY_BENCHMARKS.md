# HTTP capacity and worker-scaling benchmarks

This page includes historical 0.2.2 incident interpretation and current multi-writer
benchmark commands. See [Performance and memory controls in 0.3.0](PERFORMANCE_UPGRADE.md)
for the capacity changes and streaming I/O costs.

This suite measures real authenticated HTTP traffic against an isolated SkerryKV
process. The core worker counts are **4, 8, 16, 32, and 64 concurrent client workers**.
Each client issues one HTTP request at a time; a batch request contains many object
operations and Skerry may pipeline those operations internally. These counts are
not Skerry read threads or a simulation of segmentation CPU work.

## What is measured

For every worker count, the `quick` profile measures:

| Access pattern | Payload | Request shape |
| --- | --- | --- |
| Unique writes | 4 KiB | One PUT/object, unique deterministic bytes |
| Batch writes | 4 KiB/object | 250 objects/request, matching Pelagia's default item cap |
| Mixed traffic | 32 KiB | Approximately 80% reads / 20% unique single writes |
| Random working-set reads | 32 KiB | Deterministic access across 32 preseeded objects |
| Hot reads | 4 KiB | One repeatedly requested object; 64 MiB payload cache |

Every object acknowledgment and GET payload is checked. Batch results are consumed
while the request streams, checked by ordinal and hash, and accounted per item.
Partial failure does not discard later acknowledgments. Transport failures leave
unacknowledged writes unknown; they are never silently counted as rejected or
successful. There are no automatic retries, so overload remains visible.

Reports distinguish:

- Successful object operations/s and bytes/s, including the time to drain requests started within the load window.
- Offered HTTP request rate during the load window, failed requests, rejected items, failed reads, and unknown write outcomes.
- HTTP-request latency, successful-request latency, error-request latency, and per-item acknowledgment latency. A batch's latency is not divided by its item count.
- p50/p95/p99, actual observed maxima and sample counts. High-volume percentiles use a deterministic bounded reservoir of up to 100,000 samples.
- Periodic `/v1/ready`, authenticated `/v1/status`, and `/v1/metrics` probe latency and failure counts. A status HTTP 200 without storage counters is counted separately as unavailable detailed status, rather than successful observability.
- Write queue byte/capacity samples when status remains available, configured admission limits, logical payload reservation, and budget-limited runs.

For current multi-writer runs, the global commit gather delay defaults to 10 ms and applies only when multiple writer lanes are enabled; the single-lane path remains immediate at the sequencer. The separate per-lane append gather setting remains 1 ms. Historical 0.3 baselines predate the global gather field, so matched version rows intentionally compare the new default multi-lane policy as part of the version change. Use `--commit-delay-ms N` to record a controlled override in each case's settings and full effective-config fingerprint; compatibility baseline modes strip both `writes.writer_lanes` and `writes.commit_max_delay_ms`.

Probe failure fractions are fractions of probe attempts, not time-based availability.
An approximately 200 ms polling interval is extended by each probe's own duration.
HTTP status-code buckets are not currently collected for probes; workload rejection
messages retain their HTTP status/capacity reason.

## Running the suite

Build both optimized executables:

```sh
cargo build --release --locked --bin skerry --example http_capacity_bench
```

Run the requested worker matrix on a **disposable directory on the filesystem to
test** (replace the example parent path):

```sh
python3 scripts/benchmark-http-capacity.py \
  --binary target/release/skerry \
  --store-parent /path/to/benchmark-scratch \
  --output results/http-workers.json \
  --profile quick --workers 4 8 16 32 64 \
  --seconds 10 --warmup-seconds 2 --repeats 3 \
  --max-store-mib 2048
```

The orchestrator runs on Linux/macOS and uses owned process groups for timeout and
interrupt cleanup. Individual Rust harness cases can also be run directly on other
platforms. The service is always a local child; there is no option that points this
load generator at a production URL.

Each case uses a fresh UUID directory and a random loopback port. Tokens are
passed only to the child environment and are excluded from reports. Stores are
removed after stopping/reaping the child unless `--keep-stores` is requested.
The suite checks initial free space with headroom before each run. The logical
payload cap is conservative for acknowledged and unknown writes; known rejected
writes release their reservation. It is **not a physical disk quota**, and journal,
index, and segment overhead still consume space. The harness also validates an
estimated client-payload memory cap; this is not an RSS measurement.

`--output` writes a combined report, and a sibling `*-runs` directory contains raw
per-run reports. Existing output paths are overwritten; choose a new output path
when retaining a baseline. Case order is randomized within each repetition using
the recorded seed. Results include all samples and median/range/MAD summaries.

Profiles:

- `smoke`: short batch/mixed correctness check.
- `quick`: all five access patterns at 4/8/16/32/64 workers (25 cases).
- `writer-stress`: duplicate-put and skew-put across 4/8/16/32/64 workers and 1/2/4/8 lanes (40 cases per repetition). Duplicate PUTs send one preloaded object and validate `duplicate=true`; skew PUTs use distinct 4 KiB hashes constrained to 32 virtual shards that all route to lane zero.
- `buffered`: batch unique 32 KiB × 64 and 128 KiB × 16 writes (2 MiB per request) across 4/8/16/32/64 clients and 1/2/4/8 lanes. It sets the client payload-memory budget to 1024 MiB, with 3-second windows and 2 repetitions by default; these larger batches help separate payload-buffered writes from the 4 KiB fsync-limited case.
- `pelagia`: core cases plus smaller batches, smaller maximum-object reservation, and larger coordinator admission budgets, varied independently for all four worker counts. These are isolated experiments, not production-default recommendations.
- `sweep`: four payload sizes (1/4/32/128 KiB), all five patterns, and all four worker counts. Larger-object batch sizes are bounded to stay within default client payload memory limits.

The example accepts `--fixture-items` for the random/mixed working set (hot reads
always use one object). It checks fixture bytes plus twice the per-worker batch
payload against `--client-memory-mib`; fixture seeding uses bounded batch requests.
The default warmup is three seconds and cycles through that fixture set.

The `cache` profile compares `cache_mib` values 256, 1024, 4096, and 8192 across
the selected worker counts using a 16,384-object, 32 KiB random working set.
These are explicit payload budgets, not RSS caps. To run one case directly, the
Rust example also accepts `--cache-mib`. To run the same harness against a 0.2.2
baseline, pass `--legacy-config`; the harness removes 0.3.0-only TOML keys and
records the effective server TOML, binary version, and removed fields in the
report:

```sh
python3 scripts/benchmark-http-capacity.py \
  --binary target/release/skerry \
  --store-parent /path/to/benchmark-scratch \
  --output results/cache-capacity.json \
  --profile cache --workers 4 8 16 32 64 --seconds 10 \
  --warmup-seconds 3 --repeats 2 --max-store-mib 2048
```

```sh
target/release/examples/http_capacity_bench \
  --binary target/performance-baseline/skerry-0.2.2 \
  --store-parent /path/to/benchmark-scratch \
  --output results/baseline-0.2.2.json \
  --legacy-config --workload random-get --fixture-items 32 \
  --cache-mib 256 --workers 4 --seconds 10
```

For matched 0.3-version comparisons, the baseline must use lane 1 and
`--baseline-config-0-3`; this removes `writes.writer_lanes` and
`writes.commit_max_delay_ms`. The current candidate should use identical workload,
duration, warmup, and store-budget settings without the compatibility flag. The
new global commit gather defaults to 10 ms for multi-lane publication; the prior
per-lane append gather remains 1 ms. `--commit-delay-ms N` overrides only the new
global setting and appears in case settings and the full TOML fingerprint.

Example matched core and stress study commands (50 core baseline rows and 20
stress baseline rows at two repetitions):

```sh
python3 scripts/benchmark-http-capacity.py --binary target/performance-baseline/skerry-0.3.0 \
  --store-parent /path/to/benchmark-scratch --output results/http-03-matched-core.json \
  --profile quick --workers 4 8 16 32 64 --writer-lanes 1 \
  --baseline-config-0-3 --seconds 10 --warmup-seconds 2 --repeats 2 --max-store-mib 2048
python3 scripts/benchmark-http-capacity.py --binary target/performance-baseline/skerry-0.3.0 \
  --store-parent /path/to/benchmark-scratch --output results/http-03-matched-stress.json \
  --profile writer-stress --workers 4 8 16 32 64 --writer-lanes 1 \
  --baseline-config-0-3 --seconds 10 --warmup-seconds 2 --repeats 2 --max-store-mib 2048
python3 scripts/benchmark-http-capacity.py --binary target/release/skerry \
  --store-parent /path/to/benchmark-scratch --output results/http-04-multilane-core.json \
  --profile quick --workers 4 8 16 32 64 --writer-lanes 1 2 4 8 \
  --seconds 10 --warmup-seconds 2 --repeats 2 --max-store-mib 2048
python3 scripts/benchmark-http-capacity.py --binary target/release/skerry \
  --store-parent /path/to/benchmark-scratch --output results/http-04-multilane-stress.json \
  --profile writer-stress --workers 4 8 16 32 64 --writer-lanes 1 2 4 8 \
  --seconds 10 --warmup-seconds 2 --repeats 2 --max-store-mib 2048
```

For large streamed objects, build and use the separate helper. GET mode seeds one
fixture, times its first complete verified GET (checksum preparation, response
transfer, and client verification), then repeatedly reads that warmed fixture. PUT mode generates a fresh unique object per request. Both
modes use bounded chunks, integrity checks, an isolated child server, and a
logical store cap:

```sh
cargo build --release --bin skerry --example streaming_capacity_bench
target/release/examples/streaming_capacity_bench \
  --binary target/release/skerry --store-parent /path/to/benchmark-scratch \
  --output results/stream-get-4w.json --mode get --workers 4 \
  --file-mib 16 --seconds 3 --max-store-mib 2048
```

The streaming helper accepts `--writer-lanes 1|2|4|8`, optional
`--commit-delay-ms N`, and `--baseline-config-0-3` (lane 1 only). Compatibility
mode removes both new `writes` keys and includes the complete effective TOML plus
removed-field list in its raw result. The 40-run streaming matrix should be
collected as separate PUT/GET invocations over each selected client/lane pair;
the report helper accepts repeatable `--streaming FILE_OR_DIRECTORY` inputs and
keeps throughput in MiB/s.

For a five-client × four-lane streaming matrix, use generated names of the form
`streaming-04-{put|get}-c{4|8|16|32|64}-w{1|2|4|8}.json`. The matched old-binary
lane-1 subset uses `streaming-03-{put|get}-c{4|8|16|32|64}-w1.json` and adds
`--baseline-config-0-3`:

```sh
mkdir -p results/streaming-04
for mode in put get; do
  for workers in 4 8 16 32 64; do
    for lanes in 1 2 4 8; do
      target/release/examples/streaming_capacity_bench \
        --binary target/release/skerry --store-parent /path/to/benchmark-scratch \
        --output "results/streaming-04/streaming-04-${mode}-c${workers}-w${lanes}.json" \
        --mode "$mode" --workers "$workers" --writer-lanes "$lanes" \
        --file-mib 16 --seconds 10 --max-store-mib 4096
    done
  done
done
python3 scripts/summarize-multi-writer-results.py \
  --historical docs/HTTP_64_WORKER_RESULTS.json \
  --historical docs/PERFORMANCE_UPGRADE_RESULTS.json \
  --streaming results/streaming-04 \
  --output results/cumulative-benchmark-results.json
```

Run `--mode put` for streamed uploads. Each invocation measures one worker count;
run the desired counts separately. The helper reports admission/rejection/unknown
outcomes, p50/p95/p99, throughput including drained operations, server status and
metrics, and the end-to-end time for the first verified GET. Its GET fixture is intentionally a warmed
single-object test, not archive-scale random I/O.

Use repeated `--case` arguments to select named cases, for example
`--profile pelagia --case batch-c4 --case batch-c32 --case batch-c4-maxobj4`.
Use `--seconds 60` or longer and an explicitly sized store budget for sustained
runs that exercise index growth and background work; a short local profile does
not establish long-term compaction throughput.

## Regression and acceptance budgets

Comparison and acceptance criteria are explicit:

```sh
python3 scripts/benchmark-http-capacity.py \
  --binary target/release/skerry \
  --store-parent /path/to/benchmark-scratch \
  --output results/candidate.json \
  --profile quick --workers 4 8 16 32 64 \
  --seconds 10 --warmup-seconds 2 --repeats 3 --max-store-mib 2048 \
  --baseline results/baseline.json \
  --max-throughput-regression-percent 15 \
  --max-p99-regression-percent 25 \
  --max-probe-failure-fraction 0.01 \
  --require-no-rejections
```

Baseline comparisons reject different case settings, duration, warmup, or store
caps. Use the same machine, filesystem, cache settings, and quiet-host conditions;
those are not automatically certified. The p99 comparison uses successful-request
latency, so fast admission failures cannot mask a latency regression.

Without explicit rejection/probe budgets, `passed` means the study completed and
its configured measurement/regression budgets passed. It does not mean the service
met an availability SLO. A store-budget-limited run fails the study rather than
being silently reported as a sustained-duration capacity result. Set meaningful
budgets for the worker counts the deployment intends to support.

The manually dispatched `HTTP capacity benchmark` GitHub workflow runs the matrix
and uploads raw and aggregate artifacts. Shared-runner results are useful regression
signals, not capacity estimates for a production server. Ordinary CI runs the
harness parser/accounting tests and Python aggregation/budget tests.

## Measurement constraints and 0.2.2 incident baseline

This is a **closed-loop concurrency study**. It does not offer fixed-rate arrivals
or correct for coordinated omission. GET-only warmup is excluded from measurements;
write cases still include the behavior of a freshly created index. Client and server
share one machine and the client generates/validates bytes, so client CPU and loopback
overhead can limit results.

The read working set is deliberately small and cache-warm. Random reads here are
random working-set accesses, not archive-scale cold disk reads. No OS cache flushing,
physical-device telemetry, CPU/RSS accounting, production network latency, or
segmentation/Postgres processing is claimed. Use representative captured object-size
distributions and separate client/server hosts before assigning a production SLO.

The following explanations apply to the reported incident and the 0.2.2 baseline;
they should not be read as a description of current 0.3.0 admission behavior. The
legacy harness path above reproduces the old configuration schema.

1. In the 0.2.2 batch path, a decoder initially reserves the configured maximum object size. With the defaults (64 MiB maximum object, 256 MiB upload budget), only four full-size decoder reservations fit, and decoded payloads also occupy that budget. Active streams can vary as reservations shrink; this is not a fixed four-client hard limit.
2. The baseline batch path could pipeline up to 64 writes. Multiple clients could consume the 128 coordinator request permits, while status and metrics competed for the same request capacity.
3. Successful PUT counters include deduplicated writes and grouped durable writes; they are not physical disk commits. The reported 35,600 operations over 30 seconds is approximately 1,187 PUT operations/s, without enough information to identify a storage-device throughput limit.
4. Pelagia normally persists an ROI and a mask for each detection. Frame processing can finish before tens of thousands of blob uploads drain. Pause checkpoints do not interrupt a database replacement/upload already underway.

The 0.3.0 upgrade adds bounded payload-based upload admission, independent admin
capacity and a dedicated monitoring listener, cache metadata limits, and explicit
streaming/checkpoint controls. These changes do not establish a production SLO;
compare current and baseline runs on the same host and filesystem, then use
representative object sizes and separate client/server hosts before deployment.

## 64-client extension

The default matrix now includes 64 concurrent clients. Run only that count with
`--workers 64`; these are closed-loop client workers, not 64 independent storage
writers or a forced operating-system thread count. Historical 4–32-worker reports
retain their original measurements. See [the multi-writer design](MULTI_WRITER_DESIGN.md)
for the current serialization boundaries and proposed append-lane scheduler.

The [64-client result report](HTTP_64_WORKER_RESULTS.md) preserves matched 0.2.2/0.3.0
core runs (ten seconds, three repetitions), plus the cache and streaming extension.
To regenerate its tables from retained source reports:

```sh
python3 scripts/summarize-performance-upgrade.py \
  --before target/performance-64-before.json \
  --after target/performance-64-after.json \
  --cache target/performance-64-cache.json \
  --streaming-dir target/performance-64-streaming \
  --output-dir docs --basename HTTP_64_WORKER_RESULTS \
  --title "64-client benchmark results"
```

The old baseline's completion flag does not certify its availability; the candidate
was additionally evaluated against no rejected/unknown items, at most 1% unavailable
probes, at most 15% throughput regression, and at most 25% p99 regression.
