# Pelagia KVStore2 and Skerry comparison

`compare_pelagia.py` runs a local comparison of Pelagia's `KVStore2` and Skerry's engine, coordinator, and HTTP service. It creates deterministic fixtures and disposable stores, checks returned content, and writes raw measurements plus summaries. It does not read or modify a live Pelagia store.

See [the measured comparison](COMPARISON.md) for results and source-review findings.

## Reproduce

Build the native Skerry programs from the repository root:

```sh
cargo build --release --example compare_kvstore2
cargo build --release --bin skerry
```

Run the script with Pelagia's Python 3.12 environment, which needs `blake3` and `psutil`. For example, if that environment is Pelagia's `.venv` and Pelagia is a sibling checkout:

```sh
RESULTS="$(mktemp -d "$PWD/benchmark-results-XXXXXX")"
rmdir "$RESULTS"
../Pelagia/.venv/bin/python benchmarks/compare_pelagia.py run \
  --results "$RESULTS" \
  --pelagia-root ../Pelagia \
  --records 1024 --trials 3 --workers 8
```

The results directory must not already exist; the script creates it. Use a new unique path for each run. The harness creates a separate `kv2-compare-*` work root under the repository by default, then removes that root on completion. It only deletes the temporary work root it created. `--keep-work` retains it for inspection. The Pelagia module is loaded from `Pelagia/storage/kvstore2.py` by file path; no installed Pelagia package is required. The default Pelagia root is `../Pelagia`.

The default suite runs 47 planned cases per trial (141 case executions over three trials): seven fixture sets, with profiles selected per fixture. Datasets are 5, 20, 32, and 128 KiB, a mixed-size fixture, 1 MiB, and a hot-prefix fixture. The 1 MiB set is capped at 128 records and hot-prefix at 512; other sets use 1,024 records. Each trial shuffles case order. The standard configuration uses eight workers, a 1 ms write batch delay (`--batch-delay-ms`), and no payload cache except the explicit 256 MiB cached profile. See `--help` for dataset/profile filters and other run options.

For a smaller exploratory run, pass filters, for example:

```sh
../Pelagia/.venv/bin/python benchmarks/compare_pelagia.py run \
  --results /path/to/new-results-dir --pelagia-root ../Pelagia \
  --datasets 20k --profiles kv2_current skerry_engine --records 128 --trials 1
```

Do not point `--results` at an existing directory. Do not use a live storage root as `--work-parent`; the default is the repository. Every benchmark store is created inside the harness-owned temporary work root.

## What is measured

The harness compares sequential and concurrent initial writes, duplicate writes, sequential/random/hot/concurrent reads, ordered and unordered batches, and a mixed workload. The mixed workload uses one shared deterministic manifest for each dataset: a shuffled operation order, with 25% of positions selected independently for new writes and the rest reading original objects. New mixed payloads are made by changing the first byte of an original payload and hashing the result. Their positions are distributed independently of payload size.

The hot read order repeatedly selects from a small hot pool (about 1% of original objects). The special hot-prefix fixture filters original BLAKE3 hashes to prefix `00`; this applies to initial objects only. Newly written mixed hashes are uniform and are not constrained to that prefix.

Pelagia is initialized with BLAKE3, a two-character prefix, and a 64 GiB maximum blob size. This is the harness's packaged configuration, not a readout or proof of the actual deployment configuration. `kv2_current` uses Pelagia's current SQLite `WAL`/`NORMAL` connection and does not hash payloads again on reads. `kv2_full_verify` is a benchmark-only sensitivity profile that changes SQLite to `FULL` and verifies each returned payload hash; it does not represent hardened production behavior. `kv2_stream` sends fixture bytes through `put_stream(BytesIO(...))`; it measures that API path but does not establish a memory-use advantage.

The coordinator bulk-64 case allows 64 outstanding requests even though the ordinary concurrent pattern uses eight workers. This tests its batching capacity, with different concurrency from that pattern.

Skerry's engine profile calls the storage engine directly; coordinator profiles exercise the coordinator; HTTP profiles use the local Skerry service and its client. Skerry writes use `SyncAll`. The updated default coordinator and HTTP batch delay is 1 ms; the preserved original suite used 10 ms; the delay-zero coordinator case is a sensitivity comparison, not the default. The HTTP profile uses eight clients. The original service allowed four maximum-size upload reservations. Updated known-length uploads reserve their declared size; batch frames shrink their reservation to actual payload size after decoding. Its 503 responses and transport retries are retried and included in elapsed time; retry counts are recorded. The explicitly cached coordinator profile uses a 256 MiB payload cache and a second full read warmup pass. Other profiles have the payload cache disabled.

Every read phase is preceded by a full read pass (two passes for enabled coordinator cache). The fixtures fit in RAM, and the OS cache is left warm: these are warm-cache results, not cold-cache or HDD results. The harness makes no claims about larger indexes. Write/read phase throughput excludes initialization, while process wall time includes setup and warmups. HTTP results measure the local frontend/network path; Pelagia is called directly through its Python API, so this is not a comparison against Pelagia's HTTP/API frontend.

Per-operation latency runs from call to return; validation and byte comparison follow that clock but remain part of phase wall time. Batch latencies have a different unit: Skerry engine batch writes report each item's batch-completion time, and HTTP batch responses report each item's arrival time in the response. Do not compare these batch latency percentiles to each other or to individual-call latency percentiles. Throughput is also reported for batch phases.

The summary reports the median and min/max across trials. Three trials provide estimates, not confidence intervals; the 1 MiB, 128-record p99 is particularly sensitive to individual samples. RSS is sampled every 20 ms for the whole child process/family, including fixture preload, initialization, and warmups; it is not backend-only memory. Filesystem usage is measured after the process closes and reports logical and allocated bytes for files in the store. Initialization time is reported separately from phase throughput.

## Output

The results directory contains:

- `metadata.json`: runtime/platform details, source-file SHA-256 hashes, run configuration, available disk space at start, and completion status.
- `fixtures.json`: deterministic fixture manifests, including payload hash, read/hot/mixed orders, and mixed-write positions.
- `raw/`: one JSON result per completed case, with phase measurements, store/I/O/cache status, filesystem usage, and sampled process RSS. Nonempty case logs are copied alongside results.
- `measurements.csv`: per-trial phase rows regenerated as cases finish.
- `summary.json`: per-dataset/profile/pattern/phase median and min/max measurements across available trials.

If a case fails, the harness records the failed case and copies available diagnostics into the results directory. Incomplete summaries contain only completed cases. Compare source hashes, configuration, and completion status before interpreting results.

The harness records available disk space at startup. Check that the volume has enough free space for temporary stores and results before a run; low free space can affect storage measurements and may cause failures.

## Performance patch rerun

The original results are preserved in `results/pelagia-2026-10-26`, including archived source files and their SHA-256 hashes. The recorded commands below use identical fixture manifests and 1 ms service batching. These result paths already exist; choose fresh paths for a new experiment. The archived main suite used the initial pool implementation, and the final KV2-only suite used the narrower lock scope:

```sh
../Pelagia/.venv/bin/python benchmarks/compare_pelagia.py run \
  --results benchmarks/results/pelagia-after-2026-10-26 \
  --records 1024 --trials 3 --workers 8 --batch-delay-ms 1
../Pelagia/.venv/bin/python benchmarks/compare_pelagia.py run \
  --results benchmarks/results/kv2-after-lock-scope-2026-10-26 \
  --records 1024 --trials 3 --workers 8 --profiles kv2_current kv2_stream
../Pelagia/.venv/bin/python benchmarks/analyze_improvements.py
```

See [the before/after report](IMPROVEMENTS.md). Its audit verifies historical source snapshots instead of requiring old hashes to match the modified checkout. The 141-case updated suite captures the initial connection pool; the final 48-case KV2-only suite measures the narrower lock scope. Reported final KV2 rates come from that last suite. Sources for the historical suites are archived under their `sources/` directories. The original `analyze_comparison.py` command below expects the historical checkout; use the improvements analyzer for the current checkout. The worker records its effective batching delay and the runner checks it against the planned value.

KVStore2 now uses production connection reuse. The `kv2_full_verify` sensitivity still overrides `_connect`, bypassing that cache, so it is not a measure of the connection-reuse improvement. No timed phase received extra harness-level pooling. `analyze_improvements.py` regenerates and audits the shipped historical report; it verifies the archived source bytes for each measured version. New experiments should use fresh result directories and retain source snapshots matching their metadata hashes before comparing results.

## macOS flush-strength control

Run this separately after the primary suite, with no overlapping benchmark or build:

```sh
../Pelagia/.venv/bin/python benchmarks/compare_fullsync.py run \
  --results benchmarks/results/fullsync-new --records 256 --trials 3 --workers 8
```

This runs 18 additional cases: two write patterns for three profiles over three trials. The Python FULL-plus-read-verification baseline and a child-local variant using `fcntl(F_FULLFSYNC)` for blob flushes and SQLite `fullfsync=ON` share fixtures with the native Skerry engine. The variant checks SQLite flags and records calls to the Python flush override. It does not modify Pelagia, trace SQLite syscalls, or prove equivalent crash recovery. Its smaller dataset is a separate control, not a directly matched primary-suite sample.

Audit both completed runs and regenerate the report:

```sh
../Pelagia/.venv/bin/python benchmarks/analyze_comparison.py \
  --primary benchmarks/results/pelagia-2026-10-26 \
  --control benchmarks/results/fullsync-2026-10-26
```

The report audit validates all expected cases, phases, operation counts, payload totals, zero reported errors, summary medians, and recorded source hashes. Keep the measured harness and product source unchanged when reproducing an audit; source hashes intentionally detect changes.
