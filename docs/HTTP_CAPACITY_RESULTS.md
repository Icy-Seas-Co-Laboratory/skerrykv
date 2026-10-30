# Local HTTP worker-scaling results

Measured on local macOS/aarch64 using SkerryKV 0.2.2. Each case ran twice with a three-second load window and a one-second GET-only warmup. Rates are medians of successful object operations/s including request drain; mixed traffic uses 32 KiB objects and approximately 80% reads. Unique/batch/hot use 4 KiB objects. Batch size is 250. These short, cache-warm loopback results are a diagnostic baseline, not a production SLO.

| Client workers | Single PUT/s | Batch PUT objects/s | Mixed ops/s | Random working-set GET/s | Hot GET/s |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 193 | 4,730 | 685 | 26,705 | 60,189 |
| 8 | 325 | 5,141 | 1,427 | 37,695 | 60,836 |
| 16 | 732 | 5,367 | 2,443 | 46,892 | 56,597 |
| 32 | 1,252 | 4,666 | 4,807 | 49,953 | 57,638 |

Batch uploads reproduced the production symptoms:

| Client workers | Successful batch-request p99 (ms) | Rejected objects | Unknown write outcomes | Detailed status unavailable | Metrics unavailable |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 510 | 1,750 | 0 | 93.3% | 93.3% |
| 8 | 434 | 1,500 | 500 | 93.3% | 93.3% |
| 16 | 794 | 3,250 | 1,500 | 93.3% | 93.3% |
| 32 | 1,849 | 14,750 | 5,250 | 93.3% | 93.3% |

Rejections and unknown outcomes are totals across both repetitions, not per-second rates. Probe percentages are fractions of sampled attempts. Status failures above were HTTP 200 responses lacking storage counters; metrics requests failed. Unknown batch outcomes were connection resets/broken pipes during request-body transmission, not hash mismatches. They are conservatively unknown writes, not proof of data loss.

The batch-throughput spread does not establish a statistically significant winner: only two short repetitions were run, and ranges overlap. The useful result is the plateau, increasing tail latency/rejections, and loss of observability as concurrency rises. Single writes and mixed traffic benefit from concurrency; read-heavy workloads flatten for different reasons and remain client/cache/loopback-sensitive.

The study completed all 40 runs without a harness/integrity error or store-budget truncation. An example acceptance contract of no rejected/unknown outcomes and at most 1% unavailable monitoring probes **fails** on the batch cases at all four worker counts. A green study-completion flag must not be read as production readiness.

Next work should isolate administrator status/metrics from data-plane admission, right-size batch decoder memory reservations, expose rejection/headroom counters, and tune bounded client concurrency/retries. Increasing worker count alone did not improve batch throughput meaningfully in this profile.

See [raw reports and explicit acceptance failures](HTTP_CAPACITY_RESULTS.json) and [reproduction commands, limits, and profiles](HTTP_CAPACITY_BENCHMARKS.md).
