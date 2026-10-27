# SkerryKV and Pelagia performance improvements

At 20 KiB, HTTP bulk ingestion increased from 47 to 4,534 objects/s (97.1×), and HTTP mixed throughput from 665 to 1,992 ops/s (3.0×). Final KV2 results below include the narrower-lock refinement; rows report regressions as well as gains.

Measured on Apple M3 Max (macOS-26.6.2-arm64-arm-64bit) with 137,438,953,472 bytes RAM and the same APFS SSD. The runtime versions match: Python `3.12.2 | packaged by conda-forge | (main, Feb 16 2024, 20:54:21) [Clang 16.0.6 ]` and `rustc 1.94.1 (e408947bf 2026-03-25)`. The original and main updated suites use identical fixture manifests and seeds, 141 cases each, three randomized serial trials, and report trial medians. A final 48-case KV2-only suite measures the narrower cache lock scope with the same fixture manifests, three trials, and runtime. All KV2 current rows below use that final suite; all Skerry rows use the main updated suite. The suites ran sequentially, so environmental drift remains possible.

Reads use an explicit warmup pass. These results describe warm-cache workloads and a small in-memory index. They do not establish Linux HDD cold-cache performance or power-loss behavior. The service batch delay changed from 10 ms before to 1 ms after; this is a deliberate configuration and code change measured together, so its effects cannot be attributed separately.

The updated current KV2 profile adds a 16-connection LRU per thread across 256 hash prefixes, with explicit shutdown cleanup and pruning of finished-thread caches. SQLite open, PRAGMAs, and close/checkpoint work execute outside the cache-wide condition; active leases keep shutdown waiting until this work finishes. It keeps WAL/NORMAL and the existing ordinary-read integrity behavior. The cache has low potential hit rate for uniform access across all prefixes. The full-verify benchmark override bypasses the cache and is not evidence of a production KV2 improvement.

## KV2 current profile by payload size

Random read, hot-set read, and mixed workload rates (operations/s):

| Payload | Random before | Random after | Hot before | Hot after | Mixed before | Mixed after |
|---|---:|---:|---:|---:|---:|---:|
| 5 KiB | 1,758 | 2,219 | 3,989 | 15,108 | 2,559 | 3,009 |
| 20 KiB | 1,767 | 2,226 | 3,862 | 13,585 | 2,404 | 2,629 |
| 32 KiB | 1,756 | 2,224 | 3,510 | 13,748 | 2,446 | 2,572 |
| 128 KiB | 1,678 | 2,160 | 3,528 | 12,623 | 2,360 | 2,897 |
| 1 MiB | 1,587 | 1,911 | 3,057 | 8,950 | 1,290 | 1,354 |
| Mixed sizes | 1,755 | 2,227 | 3,863 | 14,153 | 2,638 | 2,926 |

## 20 KiB workloads

### KV2 current

| Case | Before ops/s | After ops/s | After / before | Before p95 ms | After p95 ms |
|---|---:|---:|---:|---:|---:|
| Single write | 741 | 900 | 1.21× | 5.638 | 1.477 |
| Concurrent writes | 1,555 | 2,005 | 1.29× | 6.417 | 5.555 |
| Random reads | 1,767 | 2,226 | 1.26× | 0.590 | 0.599 |
| Hot-set reads | 3,862 | 13,585 | 3.52× | 0.431 | 0.380 |
| Concurrent random reads | 3,269 | 3,932 | 1.20× | 3.070 | 2.954 |
| Mixed 25% writes / 75% reads | 2,404 | 2,629 | 1.09× | 5.992 | 5.286 |

### Skerry coordinator and HTTP

| Case | Before ops/s | After ops/s | After / before | Before p95 ms | After p95 ms |
|---|---:|---:|---:|---:|---:|
| Coordinator single write | 47 | 86 | 1.84× | 22.502 | 12.396 |
| Coordinator concurrent write | 363 | 688 | 1.90× | 23.308 | 12.411 |
| Coordinator bulk-64 | 2,621 | 4,432 | 1.69× | 36.164 | 28.620 |
| Coordinator duplicate write | 84 | 62,658 | 749.16× | 12.193 | 0.018 |
| Coordinator random read | 18,471 | 22,904 | 1.24× | 0.061 | 0.049 |
| Coordinator mixed | 1,130 | 2,201 | 1.95× | 22.402 | 11.842 |
| HTTP single write | 46 | 88 | 1.90× | 22.898 | 12.407 |
| HTTP concurrent write | 160 | 669 | 4.19× | 23.474 | 13.197 |
| HTTP bulk-64 | 47 | 4,534 | 97.09× | 1329.735 | 28.650 |
| HTTP duplicate write | 82 | 9,557 | 116.65× | 12.483 | 0.147 |
| HTTP random read | 7,129 | 8,702 | 1.22× | 0.152 | 0.127 |
| HTTP mixed | 665 | 1,992 | 3.00× | 22.501 | 16.604 |

### Coordinator delay=0 sensitivity

| Case | Before ops/s | After ops/s | After / before | Before p95 ms | After p95 ms |
|---|---:|---:|---:|---:|---:|
| Coordinator delay=0 single write | 93 | 96 | 1.03× | 11.405 | 11.177 |
| Coordinator delay=0 random read | 18,451 | 23,295 | 1.26× | 0.061 | 0.047 |
| Coordinator delay=0 mixed | 2,246 | 2,288 | 1.02× | 11.567 | 10.832 |

### Direct-engine control (unchanged operation paths)

| Case | Before ops/s | After ops/s | After / before | Before p95 ms | After p95 ms |
|---|---:|---:|---:|---:|---:|
| Direct engine single writes | 104 | 116 | 1.12× | 10.573 | 9.914 |
| Direct engine random reads | 48,388 | 72,249 | 1.49× | 0.020 | 0.015 |
| Direct engine mixed | 422 | 465 | 1.10× | 77.748 | 68.917 |

These direct-engine operations do not use the coordinator fast path or HTTP changes. Changes in these control rates show that before/after ratios also contain environmental or build variation; they are not isolated causal estimates.

### KV2 cache lock-scope follow-up

| 20 KiB operation | Initial pooled version ops/s | Final narrower-lock version ops/s |
|---|---:|---:|
| Eight-client random reads | 2,872 | 3,932 |
| 25% new writes / 75% reads | 2,173 | 2,629 |

## Changes and interpretation

The after implementation reduces the default service batch delay to 1 ms, pipelines up to 64 HTTP batch writes, reserves actual known upload sizes, and lets healthy, committed duplicates bypass the writer queue. Existing config files keep their values; set `writes.batch_max_delay_ms = 1` explicitly to adopt the tested setting. The suite reports their combined outcome, including single and concurrent writes, duplicate writes, bulk-64, random reads, and mixed traffic. Per-row ratios are computed from the audited suite medians. Individual and bulk write latency use different units: the HTTP bulk percentile measures each item’s arrival from batch start, while individual calls measure call-to-return latency.

The HTTP pipeline now handles bounded pending requests with unknown outcomes explicitly. After a storage failure, decoding stops and ordered outcomes are returned for already-queued items when the response remains usable. A missing result leaves its outcome unknown; content-addressed PUT can be safely retried. Benchmark cases require successful replies and exact content validation, and have zero reported operation errors. Review the implementation’s documented retry and error semantics alongside throughput before relying on pipeline results.

The KV2 comparisons use the production current profile with the new connection lifecycle. Because each thread retains only 16 shard connections across 256 possible prefixes, uniform random reads have low connection-cache hit potential. Full-verify bypasses that cache and changes integrity work, so it is intentionally excluded from production improvement claims.

## Method and limits

The harness uses the same deterministic fixture seeds and dataset manifests in both suites. Three randomized serial trials were run on the same hardware/runtime; summary values are medians. Reads are warm after a full read pass, and the dataset fits in RAM. The 1 ms service delay and code path changed together, preventing isolated attribution to delay tuning. This benchmark does not measure Linux HDD cold-cache workloads, power-loss outcomes, or durability parity. Throughput must be interpreted with each implementation’s acknowledgment and synchronization behavior.

## Evidence

Baseline audit: `{'completed_cases': 141, 'validated_phases': 1269, 'validated_operations': 1085184, 'errors': 0}`. Main updated audit: `{'completed_cases': 141, 'validated_phases': 1269, 'validated_operations': 1085184, 'errors': 0}`. Final KV2 audit: `{'completed_cases': 48, 'validated_phases': 432, 'validated_operations': 342144, 'errors': 0}`.

- [Baseline summary](results/pelagia-2026-10-26/summary.json) · [baseline CSV](results/pelagia-2026-10-26/measurements.csv)
- [After summary](results/pelagia-after-2026-10-26/summary.json) · [after CSV](results/pelagia-after-2026-10-26/measurements.csv)
- [Final KV2 summary](results/kv2-after-lock-scope-2026-10-26/summary.json) · [final KV2 CSV](results/kv2-after-lock-scope-2026-10-26/measurements.csv)
- [Benchmark harness instructions](README.md) · [comparison harness](compare_pelagia.py)


Timed HTTP phases recorded 5,226 → 0 HTTP 503 retries and 2,622 → 0 client transport retries. Retry costs are included in the measured operations.
