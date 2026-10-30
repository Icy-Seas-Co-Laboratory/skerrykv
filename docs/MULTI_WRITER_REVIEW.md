# Multi-writer implementation and cumulative performance review

Measured locally on 2026-10-29. **Keep `writes.writer_lanes = 1` for this host, and consider a 2 ms gather window.** Parallel append lanes are implemented and opt-in, but the completed matrix does not show a consistent small-write throughput gain on this single APFS volume. A higher lane count is a capability, not a demonstrated performance improvement here.

The [filterable report](MULTI_WRITER_RESULTS.html), [complete tables](MULTI_WRITER_RESULTS.md), and [raw results](MULTI_WRITER_RESULTS.json) retain earlier studies, fresh 0.3.0 controls, each lane count, effective configurations, executable fingerprints, rejected requests, and unknown outcomes. Historical studies with different payloads, concurrency, durability contracts, or benchmark methods remain descriptive comparisons; their rates are not combined into one speedup.

## What changed

- 256 FIFO virtual hash shards feed 1/2/4/8 append lanes with round-robin shard scheduling. Independent append/data synchronization overlaps; namespace publication remains durable and coordinated.
- Buffered PUT, DELETE, and prepared streamed publication share shard ordering. Pending requests and bytes stay globally bounded, including after caller cancellation.
- Bounded shared group commit, deletion-aware duplicate shortcuts, fresh per-lane status/metrics, multi-active-file recovery, shutdown, GC, checkpoints, rebuild, and vacuum use the same authority rules.
- The final HTTP reconciliation separates active batch writes from completed results waiting for ordered output. It refills available slots, bounds retained results, and stops dispatch on failure.

See [configuration and compatibility](MULTI_WRITER.md). Enabling multiple lanes activates sticky store capability version 3; old tools reject it. Returning the lane setting to one does not downgrade the store.

## How the earlier improvements compare

| Study | Within-study result | Interpretation |
| --- | --- | --- |
| Original Pelagia comparison | At 20 KiB, Skerry direct warm random reads 48,388/s versus KVStore2 1,767/s; Skerry HTTP bulk was 47 objects/s | Direct calls and HTTP have different costs; synchronization contracts also differ |
| First ingestion improvements | 20 KiB Skerry HTTP bulk 47 → 4,534 objects/s; HTTP mixed 665 → 1,992 ops/s | Code and service gather setting changed together; this is a matched historical workload, not the new 4 KiB matrix |
| Stronger v2 durability protocol | Separate five-phase before/after comparison retained | Extra journal/witness barriers change the write contract; earlier engine rates cannot predict current durable service rates |
| 0.2.2 → 0.3.0 capacity work | Batch status/metrics availability 6.7% → 100%; write rejections/unknowns eliminated in the candidate core study | The main gain was admission and observability reliability; throughput gains were not uniform |
| 0.3.0 cache study | A 512 MiB working set achieved about 40% measured-window hits with 256 MiB payload cache and 100% with 1 GiB or more | Extra cache helped when it covered the working set; 4–8 GiB offered little additional benefit in that study |
| Current append-lane study | Initial 4 KiB batch throughput decreased as lanes increased; warmed read rates stayed close | Independent files did not overcome shared synchronization and smaller groups on this device |

The detailed historical evidence is in [the original comparison](../benchmarks/COMPARISON.md), [first improvements](../benchmarks/IMPROVEMENTS.md), [v2 protocol study](V2_PROTOCOL_BENCHMARK.json), [0.3 results](PERFORMANCE_UPGRADE_RESULTS.md), and [64-client extension](HTTP_64_WORKER_RESULTS.md). The cumulative report preserves 1,044 historical benchmark-summary rows, five protocol phases, 94 historical HTTP cases, ten historical streaming rows, three verification-duration scenarios, and two foreground-read verification rows. Earlier verification optimization reduced full-check wall time by about 15–18% in its three scenarios; the separate throttled engine-level foreground study measured 0.091 ms idle versus 0.088 ms during verification at p95. Those checks excluded service/admission overhead and are not current HTTP SLOs.

## Initial complete matrix

The initial campaign ran **480 trials**: 70 matched 0.3 core/stress controls; 200 new core trials; 80 duplicate/skew trials; 60 buffered-size trials; 50 streaming trials; and 20 admission-tuning trials. HTTP runs used three-second offering windows, one-second warmup and two repetitions. Every request drains before measured throughput is calculated.

4 KiB binary batch PUT, 250 objects/request, successful objects/s (median of two runs):

| Client workers | 0.3 / one lane | 0.4 / one lane | Two lanes | Four lanes | Eight lanes |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 4,759 | 4,361 | 3,448 | 1,608 | 1,161 |
| 8 | 4,947 | 4,859 | 3,433 | 2,007 | 1,514 |
| 16 | 4,910 | 4,756 | 4,008 | 2,307 | 1,517 |
| 32 | 5,186 | 4,812 | 3,937 | 2,096 | 1,472 |
| 64 | 4,711 | 4,125 | 3,766 | 2,263 | 1,459 |

These are the **pre-refill-fix** results and remain in the report. The 64-client/eight-lane case reported **2,777 unknown write outcomes** at the client's ten-second deadline. No writes were explicitly rejected. Unknown outcomes are not proof of acknowledged data loss; they are a failed client-outcome budget and must not be counted as successful operations. Duplicate and skew studies, buffered-size studies, and admission studies passed their outcome/probe budgets.

Across all 344 new HTTP trials in this initial campaign, **5,160 status probes and 5,160 metrics probes succeeded**. Even the timed-out batch case remained observable. The probes sample during load offering, not the entire subsequent drain.

At 64 clients and one lane, the same initial comparison measured unique PUT 2,713 → 2,015/s, mixed 8,474 → 7,315 ops/s, random GET 52,516 → 48,934/s, and hot GET 56,051 → 55,838/s. These regressions are retained. Engine counters show similar per-group durable commit time but smaller default write groups. The new scheduler checks its nominal gather deadline on arrivals; the older receive-first timeout could gather beyond that boundary until Tokio's rounded timer fired. This is a concrete policy difference, not an isolated causal measurement.

## Separate tuning and large-file results

Keeping the shared byte budgets unchanged and increasing request capacity to 512 improved the initial one-lane, 64-client 4 KiB batch case from **4,125 to 10,838 objects/s** (2.63×). Fresh 0.3 with that same setting reached **11,047/s**. This is a request-capacity experiment, not a multi-lane speedup. Two/four/eight lanes reached 9,125 / 5,937 / 4,591/s with that setting.

16 MiB streaming, one trial per setting, MiB/s:

| Clients | 0.3 PUT | 0.4 one-lane PUT | 0.4 eight-lane PUT | 0.3 warm GET | 0.4 one-lane warm GET |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 333.6 | 359.9 | 324.6 | 5,648.5 | 5,642.5 |
| 8 | 352.2 | 347.8 | 330.9 | 6,114.5 | 6,095.2 |
| 16 | 349.1 | 333.0 | 333.2 | 6,079.3 | 6,067.4 |
| 32 | 350.1 | 339.6 | 327.9 | 6,065.8 | 6,052.3 |
| 64 | 336.8 | 346.1 | 332.4 | 6,030.4 | 6,056.4 |

All 50 fresh streaming trials had zero failed GETs, rejected PUTs, and unknown PUTs. GET repeats one warmed fixture after a separately timed first complete verified GET. These are application/cache transfer rates, not cold disk bandwidth. Streaming upload/read slots stayed at two/four, so this is not an unrestricted lane-scaling experiment.

## Review and validation

The full final-source regression pass after the HTTP refill correction passed **222 Rust tests**, with the old-binary compatibility test separately executed and passed, **35 Python tests**, strict all-target/all-feature Clippy, and release builds. The release [maintenance smoke](MULTI_WRITER_SMOKE.json) passed deletion, collection, vacuum, verification, rebuild, restart, and recreation. Controlled tests cover same-shard ordering, cross-lane progress while another lane stalls, publication uncertainty, deletion/duplicate races, cancellation ownership, active-file downshift, concurrent GC/checkpointing, and subprocess recovery boundaries. Process-crash tests do not establish power-loss safety.

## Final batch reconciliation and longer controls

Another **106 trials** followed the final HTTP refill correction: 40 small-batch, 32 buffered-size, 16 admission-capacity, 12 alternating old/new longer controls, and six 2 ms tuning trials. The total new campaign is **586 trials**. Sources containing `refill`, `long-alternating`, and `delay2` distinguish these measurements from the initial matrix; the source labels and executable fingerprints remain in every comparison.

Final 4 KiB binary batch PUT, default 1 ms gathering, objects/s:

| Clients | Fresh 0.3 control | Final 0.4 / one lane | Two lanes | Four lanes | Eight lanes |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | 4,759 | 4,528 | 3,646 | 2,241 | 1,415 |
| 8 | 4,947 | 4,163 | 4,172 | 2,169 | 1,355 |
| 16 | 4,910 | 4,809 | 4,255 | 2,364 | 1,386 |
| 32 | 5,186 | 4,863 | 4,095 | 2,095 | 1,378 |
| 64 | 4,711 | 4,508 | 3,745 | 2,148 | 1,557 |

The refill fix improved some cases (four lanes/four clients: 1,608 → 2,241/s), but did not provide a consistent gain across cases. The final eight-lane/64-client case still reported **1,028 unknown outcomes**, so its outcome budget failed. Other final short-study cases passed outcome/probe budgets. Final 512-request one-lane throughput at 64 clients was **11,189/s**, versus 11,047/s in the matched fresh old control, with shared byte budgets unchanged. This is a separate admission experiment.

Longer controls used ten-second offering windows and two repetitions, alternating old/new then new/old version order at 64 clients. The 2 ms candidate runs followed afterward, so configuration comparisons can still include environmental drift.

| Workload | Old 0.3, nominal 1 ms | Final 0.4, 1 ms | Final 0.4, 2 ms | 2 ms candidate vs old |
| --- | ---: | ---: | ---: | ---: |
| Unique PUT objects/s | 2,219 | 1,639 | 2,353 | +6.0% |
| Batch PUT objects/s | 4,837 | 4,570 | 4,680 | −3.2% |
| Mixed operations/s | 7,781 | 7,314 | 8,459 | +8.7% |
| Unique PUT request p99 ms | 51.66 | 166.04 | 50.06 | −3.1% |

The default-setting unique-write regression remains real evidence; the 2 ms configuration result does not erase it. The tuning recovered throughput and tail latency in this focused test, consistent with gathering larger groups. It does not establish an optimal delay at other client counts, devices, or arrival rates. All longer/tuning writes had zero rejected or unknown outcomes.

Across **all 444 new-version HTTP trials**, **7,080 status probes and 7,080 metrics probes succeeded** with no missing storage payloads. Their largest sampled response times were 16.42 ms and 15.22 ms respectively. These are local observed maxima, not hard latency guarantees. The 92 fresh old-version HTTP trials and 50 fresh streaming trials complete the 586-trial inventory.

## Operating recommendation and next work

Keep one lane by default. For this host, the focused test supports explicitly setting:

```toml
[writes]
writer_lanes = 1
batch_max_delay_ms = 2
```

The package default remains 1 ms; existing configuration is not silently rewritten. Evaluate a larger global request allowance with the current byte budgets and production-like pressure; do not multiply byte limits by lane count. The 2 ms gather and 512-request experiments were measured separately, so their benefits must not be multiplied or attributed to one combined configuration. Larger write groups appear more promising on this device than additional independently synchronized files. Sparse/skewed multi-lane traffic can also pay the extra shared gather delay.

Before enabling multiple lanes in demos, reproduce a benefit on the target Linux filesystem/device and workload. Profile group size, queue age, data-sync and commit-wait time; tune gather windows and admission independently. Add sustained separate-host tests that overlap GC/checkpoints, include churn and disk pressure, and measure RSS and physical I/O. The current client/server shared an Apple M3 Max with 16 cores and 128 GiB RAM; the APFS volume had about 36 GiB free and was nearly full. No cache flush, physical-device telemetry, container memory-pressure study, Linux/Windows execution, or production latency SLO is claimed.

## Reproducing comparisons

Build release examples, then use `scripts/benchmark-http-capacity.py` with explicit `--workers 4 8 16 32 64 --writer-lanes 1 2 4 8`, matched offering/warmup/repetition/store limits, and isolated stores. Use `--baseline-config-0-3 --writer-lanes 1` for the preserved 0.3 binary. The `quick`, `writer-stress`, `buffered`, and `pelagia` profiles describe distinct studies. `examples/streaming_capacity_bench.rs` measures streaming separately.

`scripts/summarize-multi-writer-results.py` accepts repeated `--candidate`, `--baseline`, `--historical`, `--streaming`, and `--historical-context` inputs. Use `--baseline-section ''` for direct baseline aggregates and `--output docs/MULTI_WRITER_RESULTS.json`; it also generates Markdown and HTML. Every included source body is embedded in the JSON, including failed budgets and compatibility metadata. Same-lane scaling requires the same executable fingerprint; cross-version comparisons require matching settings and methods represented by the report metadata.
