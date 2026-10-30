# 0.3.0 performance upgrade results

Generated from the supplied JSON reports. Missing or incomplete cases are shown as `n/a`; no values are inferred.

## Study context

| Study | Profile / mode | Duration × repeats | Warmup | Store cap | Platform | Passed / budget failures | Binary BLAKE3 fingerprint(s) |
| --- | --- | ---: | ---: | ---: | --- | --- | --- |
| Before | quick | 3 s × 2 | 1 s | 2048 MiB | macOS-26.6.2-arm64-arm-64bit-Mach-O | True / [] | `78c434775c38073ef6d5b5e428f9ab93f38a32878bb38eca400ffe1aa66afa4a` |
| After | quick | 3 s × 2 | 1 s | 2048 MiB | macOS-26.6.2-arm64-arm-64bit-Mach-O | False / ['single-c8: p99 latency regressed beyond budget (25.69 -> 164.80 ms)'] | `a475892a65d2ada906f9593603dd49f580cef9696168eba3cde3809017135a4f` |
| Cache | cache | 3 s × 1 | 5 s | 2048 MiB | macOS-26.6.2-arm64-arm-64bit-Mach-O | True / [] | `a475892a65d2ada906f9593603dd49f580cef9696168eba3cde3809017135a4f` |

The baseline should identify the 0.2.2 binary with `legacy_config=true`; the candidate is the 0.3.0 binary. See the raw JSON for exact per-case settings and full effective server TOML.

## Item throughput, before → after

Median successful item operations/s across repetitions. Each workload compares the same worker count.

| Pattern | Workers | Before ops/s | After ops/s | Change |
| --- | ---: | ---: | ---: | ---: |
| unique PUT | 4 | 171.3 | 187.4 | 9.4% |
| unique PUT | 8 | 362.7 | 328.7 | -9.4% |
| unique PUT | 16 | 727.7 | 722.4 | -0.7% |
| unique PUT | 32 | 1,413.6 | 1,380.3 | -2.4% |
| batch PUT | 4 | 4,610.2 | 4,698.1 | 1.9% |
| batch PUT | 8 | 5,298.6 | 5,040.2 | -4.9% |
| batch PUT | 16 | 5,164.1 | 5,153.4 | -0.2% |
| batch PUT | 32 | 5,083.3 | 4,939.5 | -2.8% |
| mixed | 4 | 694.6 | 655.4 | -5.6% |
| mixed | 8 | 1,389.7 | 1,315.5 | -5.3% |
| mixed | 16 | 2,560.8 | 2,262.1 | -11.7% |
| mixed | 32 | 4,525.7 | 4,905.6 | 8.4% |
| random GET | 4 | 26,301.3 | 26,206.6 | -0.4% |
| random GET | 8 | 37,239.8 | 36,522.6 | -1.9% |
| random GET | 16 | 47,547.7 | 47,222.9 | -0.7% |
| random GET | 32 | 50,194.5 | 49,871.0 | -0.6% |
| hot GET | 4 | 59,954.2 | 58,468.4 | -2.5% |
| hot GET | 8 | 59,858.2 | 58,099.3 | -2.9% |
| hot GET | 16 | 57,553.1 | 54,400.2 | -5.5% |
| hot GET | 32 | 56,876.6 | 55,714.9 | -2.0% |

## Batch latency and observability

Rejection and unknown counts are totals across included runs. Probe availability is 1 minus the unavailable fraction; missing status payloads count as unavailable.

| Workers | Before p99 ms | After p99 ms | Before status available | After status available | Before metrics available | After metrics available | Before rejected / unknown | After rejected / unknown |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 542.6 | 306.5 | 6.7% | 100.0% | 6.7% | 100.0% | 250 / 0 | 0 / 0 |
| 8 | 420.6 | 435.7 | 6.7% | 100.0% | 6.7% | 100.0% | 1750 / 250 | 0 / 0 |
| 16 | 835.1 | 862.8 | 6.7% | 100.0% | 6.7% | 100.0% | 5500 / 1250 | 0 / 0 |
| 32 | 1,631.5 | 1,882.2 | 6.7% | 100.0% | 6.7% | 100.0% | 11000 / 5250 | 0 / 0 |

## Focused longer single-write follow-up

This separately supplied longer run targets the isolated short-window single-write p99 stall. It does not replace or erase the short-run comparison or its budget failure. Acceptance status is the source report’s overall status; it remains unavailable until a report is supplied.

| Study | Duration × repeats | Passed | Budget failures | Single-write ops/s | Request p99 ms | Rejected / unknown items |
| --- | ---: | --- | --- | ---: | ---: | ---: |
| Follow-up before | 10 s × 3 | True | none | 351.9 | 29.3 | 0 / 0 |
| Follow-up after | 10 s × 3 | True | none | 354.4 | 29.9 | 0 / 0 |

## Additional validation reports

These reports are preserved separately and do not alter the primary before/after status or focused follow-up table.

| Source report | Profile | Duration × repeats | Passed | Budget failures |
| --- | --- | ---: | --- | --- |
| `performance-tail-before.json` | quick | 10 s × 3 | True | none |
| `performance-tail-after.json` | quick | 10 s × 3 | False | ['single-c8: p99 latency regressed beyond budget (28.20 -> 41.09 ms)'] |

## Cache profile

The measured-window hit ratio is calculated from per-run counter deltas: (final hits − initial hits) / (hit delta + miss delta). Initial and final cumulative ratios are shown for context only. Counter decreases or empty intervals are reported as unavailable. The working set and run duration are supplied in the raw report.

| Payload budget MiB | Workers | Measured-window hit ratio | Initial cumulative ratio | Final cumulative ratio | Item ops/s |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 4 | 40.0% | 28.7% | 33.2% | 27,297.9 |
| 256 | 8 | 40.1% | 33.4% | 36.1% | 39,761.3 |
| 256 | 16 | 40.1% | 34.1% | 36.4% | 46,732.9 |
| 256 | 32 | 40.3% | 33.6% | 36.2% | 47,458.5 |
| 1024 | 4 | 100.0% | 80.2% | 89.0% | 44,066.5 |
| 1024 | 8 | 100.0% | 85.2% | 91.2% | 50,574.3 |
| 1024 | 16 | 100.0% | 86.5% | 91.7% | 50,941.7 |
| 1024 | 32 | 100.0% | 86.2% | 91.5% | 49,726.5 |
| 4096 | 4 | 100.0% | 80.3% | 89.1% | 44,608.1 |
| 4096 | 8 | 100.0% | 85.4% | 91.3% | 51,200.3 |
| 4096 | 16 | 100.0% | 85.8% | 91.3% | 48,658.6 |
| 4096 | 32 | 100.0% | 86.1% | 91.4% | 49,012.3 |
| 8192 | 4 | 100.0% | 80.2% | 89.0% | 43,801.9 |
| 8192 | 8 | 100.0% | 85.3% | 91.3% | 51,230.4 |
| 8192 | 16 | 100.0% | 86.1% | 91.5% | 49,766.7 |
| 8192 | 32 | 100.0% | 86.3% | 91.6% | 50,017.0 |

## Streaming transfer

MiB/s is reported by the helper and includes requests drained after load offering stops. The initial GET timing is the whole first verified GET, including checksum preparation, response transfer, and client-side verification; it is not an isolated checksum-scan duration.

| Mode | Workers | File MiB | MiB/s | First GET with full scan, end-to-end ms | Successful / failed-or-rejected / unknown |
| --- | ---: | ---: | ---: | ---: | ---: |
| get | 4 | 16 | 5,649.5 | 29.2 | 1060 / 0 / n/a |
| get | 8 | 16 | 6,076.4 | 28.9 | 1146 / 0 / n/a |
| get | 16 | 16 | 6,104.0 | 29.1 | 1160 / 0 / n/a |
| get | 32 | 16 | 6,096.6 | 29.4 | 1175 / 0 / n/a |
| put | 4 | 16 | 287.0 | n/a | 56 / 0 / 0 |
| put | 8 | 16 | 355.7 | n/a | 75 / 0 / 0 |
| put | 16 | 16 | 299.4 | n/a | 72 / 0 / 0 |
| put | 32 | 16 | 337.6 | n/a | 98 / 0 / 0 |

## Settings and interpretation

Exact settings, source report bodies, case outcomes, effective TOML, error details, and fingerprints are retained in [the raw JSON reports](PERFORMANCE_UPGRADE_RESULTS.json). Failed or budget-limited runs are retained and identified by their source reports; they are not dropped from the combined artifact.

These are short, local closed-loop studies (the HTTP profile uses the supplied two-repetition runs; cache studies should use three-second load and five-second warmup). They do not correct for coordinated omission. Cache comparisons use the specified 16,384 × 32 KiB working set; streaming GET repeats one warmed fixture after a separately measured first verified GET. They are not cold/random archive-read results. No RSS or physical-device throughput claim is made. Client and server share a host, and throughput includes drain time.

Cache hit ratios and throughput should be read alongside the explicit cache budget and metadata limits. A failed candidate budget remains a failure even when throughput numbers are present; see the raw report's `passed`, `budget_failures`, errors, and per-case `store_limited` fields.
