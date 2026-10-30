# 64-client benchmark results

Generated from the supplied JSON reports. Missing or incomplete cases are shown as `n/a`; no values are inferred.

## Study context

| Study | Profile / mode | Duration × repeats | Warmup | Store cap | Platform | Passed / budget failures | Binary BLAKE3 fingerprint(s) |
| --- | --- | ---: | ---: | ---: | --- | --- | --- |
| Before | quick | 10 s × 3 | 2 s | 2048 MiB | macOS-26.6.2-arm64-arm-64bit-Mach-O | True / [] | `78c434775c38073ef6d5b5e428f9ab93f38a32878bb38eca400ffe1aa66afa4a` |
| After | quick | 10 s × 3 | 2 s | 2048 MiB | macOS-26.6.2-arm64-arm-64bit-Mach-O | True / [] | `a475892a65d2ada906f9593603dd49f580cef9696168eba3cde3809017135a4f` |
| Cache | cache | 3 s × 1 | 5 s | 2048 MiB | macOS-26.6.2-arm64-arm-64bit-Mach-O | True / [] | `a475892a65d2ada906f9593603dd49f580cef9696168eba3cde3809017135a4f` |

The baseline should identify the 0.2.2 binary with `legacy_config=true`; the candidate is the 0.3.0 binary. See the raw JSON for exact per-case settings and full effective server TOML.

## Item throughput, before → after

Median successful item operations/s across repetitions. Each workload compares the same worker count.

| Pattern | Workers | Before ops/s | After ops/s | Change |
| --- | ---: | ---: | ---: | ---: |
| unique PUT | 64 | 2,361.6 | 2,278.4 | -3.5% |
| batch PUT | 64 | 4,809.4 | 4,964.0 | 3.2% |
| mixed | 64 | 8,274.8 | 7,944.5 | -4.0% |
| random GET | 64 | 54,335.8 | 51,587.5 | -5.1% |
| hot GET | 64 | 57,402.9 | 54,905.7 | -4.4% |

## Batch latency and observability

Rejection and unknown counts are totals across included runs. Probe availability is 1 minus the unavailable fraction; missing status payloads count as unavailable.

| Workers | Before p99 ms | After p99 ms | Before status available | After status available | Before metrics available | After metrics available | Before rejected / unknown | After rejected / unknown |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 3,721.7 | 3,340.9 | 0.7% | 100.0% | 0.7% | 100.0% | 132250 / 23000 | 0 / 0 |

## Cache profile

The measured-window hit ratio is calculated from per-run counter deltas: (final hits − initial hits) / (hit delta + miss delta). Initial and final cumulative ratios are shown for context only. Counter decreases or empty intervals are reported as unavailable. The working set and run duration are supplied in the raw report.

| Payload budget MiB | Workers | Measured-window hit ratio | Initial cumulative ratio | Final cumulative ratio | Item ops/s |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 64 | 40.0% | 35.2% | 37.1% | 50,991.7 |
| 1024 | 64 | 100.0% | 86.0% | 91.4% | 48,370.4 |
| 4096 | 64 | 100.0% | 86.2% | 91.5% | 49,388.2 |
| 8192 | 64 | 100.0% | 86.6% | 91.7% | 50,580.5 |

## Streaming transfer

MiB/s is reported by the helper and includes requests drained after load offering stops. The initial GET timing is the whole first verified GET, including checksum preparation, response transfer, and client-side verification; it is not an isolated checksum-scan duration.

| Mode | Workers | File MiB | MiB/s | First GET with full scan, end-to-end ms | Successful / failed-or-rejected / unknown |
| --- | ---: | ---: | ---: | ---: | ---: |
| get | 64 | 16 | 6,009.9 | 29.2 | 1189 / 0 / n/a |
| put | 64 | 16 | 290.9 | n/a | 112 / 0 / 0 |

## Settings and interpretation

Exact settings, source report bodies, case outcomes, effective TOML, error details, and fingerprints are retained in [the raw JSON reports](HTTP_64_WORKER_RESULTS.json). Failed or budget-limited runs are retained and identified by their source reports; they are not dropped from the combined artifact.

These are local closed-loop studies; durations, repetition counts, and warmup periods are listed above. They do not correct for coordinated omission. Cache comparisons use the specified 16,384 × 32 KiB working set; streaming GET repeats one warmed fixture after a separately measured first verified GET. They are not cold/random archive-read results. No RSS or physical-device throughput claim is made. Client and server share a host, and throughput includes drain time.

Cache hit ratios and throughput should be read alongside the explicit cache budget and metadata limits. A failed candidate budget remains a failure even when throughput numbers are present; see the raw report's `passed`, `budget_failures`, errors, and per-case `store_limited` fields.
