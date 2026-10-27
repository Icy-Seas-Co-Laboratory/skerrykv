# Pelagia KVStore2 versus SkerryKV — local comparison

At 20 KiB, Skerry direct warm random reads are 27.4× faster, and native bulk-64 writes reach 5,085 objects/s. Current KV2 single writes are 7.1× faster than Skerry single engine writes under their different synchronization contracts. Default Skerry HTTP writes remain limited by coordinator and request-path behavior; see the separate service rows and macOS control below.

Measured on 2026-10-26 using an Apple M3 Max, 16 logical CPUs, 128 GiB RAM and the same APFS SSD. Three trials, randomized serial case order; medians below. Fixtures are synthetic, deterministic, incompressible-like binary data. They fit in RAM. Timed reads follow explicit warmup passes: these are warm-cache, small-index results, not HDD/cold-cache/large-archive predictions. Disk free space was about 80 GiB on a 2 TB volume (roughly 96% full).

Pelagia uses the current source file and **packaged** BLAKE3 / prefix-length 2 / 64 GiB shard configuration, not an inspected live deployment configuration. Skerry direct-engine, coordinator, and loopback HTTP results are distinct. No production code or live store was modified. Initialization and fixture generation are excluded from phase throughput.

## Main result by payload size

| Payload | Objects/trial | KV2 single PUT/s | Skerry engine single PUT/s | Skerry engine bulk-64 PUT/s | KV2 random GET/s | Skerry engine random GET/s | Read ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| 5 KiB | 1024 | 737 | 114 | 5,444 | 1,758 | 139,215 | 79.2× |
| 20 KiB | 1024 | 741 | 104 | 5,085 | 1,767 | 48,388 | 27.4× |
| 32 KiB | 1024 | 771 | 110 | 5,129 | 1,756 | 31,938 | 18.2× |
| 128 KiB | 1024 | 658 | 111 | 3,384 | 1,678 | 8,688 | 5.2× |
| Mixed 5/20/32/128 KiB | 1024 | 722 | 105 | 4,725 | 1,755 | 22,845 | 13.0× |
| 1 MiB | 128 | 298 | 103 | 816 | 1,587 | 1,694 | 1.1× |

Single PUTs and native bulk-64 have different acknowledgment granularity; batching can amortize synchronization. KV2 has no native cross-object batch operation. MiB/s, sampled latency percentiles, and latency units are in the raw CSV/JSON.

## Access patterns at 20 KiB

| Pattern | KV2 ops/s | Skerry engine ops/s | Skerry/KV2 | KV2 p95 ms | Skerry p95 ms |
|---|---:|---:|---:|---:|---:|
| Single new writes | 741 | 104 | 0.14× | 5.638 | 10.573 |
| 8-client new writes | 1,555 | 102 | 0.07× | 6.417 | 81.009 |
| Duplicate hot writes | 1,622 | 69,044 | 42.58× | 1.701 | 0.016 |
| Sequential reads | 1,749 | 50,125 | 28.66× | 0.590 | 0.019 |
| Uniform random reads | 1,767 | 48,388 | 27.39× | 0.590 | 0.020 |
| 95% hot-set reads | 3,862 | 54,871 | 14.21× | 0.431 | 0.020 |
| 8-client random reads | 3,269 | 187,828 | 57.45× | 3.070 | 0.055 |
| Logical ordered batches | 1,809 | 48,439 | 26.77× | 0.592 | 0.022 |
| Logical unordered batches | 2,447 | 176,858 | 72.26× | 3.386 | 0.037 |
| 25% new writes / 75% reads | 2,404 | 422 | 0.18× | 5.992 | 77.748 |

Mixed actions are randomized independently of object size and distributed across workers; counts are fixed, closed-loop operations, not a constant-rate arrival process. The hot set is about 1% of objects. In-process read batches are bounded groups of individual API calls, not the HTTP binary batch protocol.

## Coordinator and HTTP overhead at 20 KiB

| Access layer | New single writes/s | New 8-client writes/s | Duplicate writes/s | Random reads/s | Random GET p95 ms |
|---|---:|---:|---:|---:|---:|
| KV2 direct Python | 741 | 1,555 | 1,622 | 1,767 | 0.590 |
| Skerry direct engine | 104 | 102 | 69,044 | 48,388 | 0.020 |
| Skerry coordinator, default 10 ms | 47 | 363 | 84 | 18,471 | 0.061 |
| Skerry coordinator, warm 256 MiB payload cache | — | 370 | 84 | 517,019 | 0.001 |
| Skerry loopback HTTP, default 10 ms | 46 | 160 | 82 | 7,129 | 0.152 |
| Skerry coordinator, delay=0 sensitivity | 93 | — | 851 | 18,451 | 0.061 |

The HTTP row includes network/client/parser/JSON costs and production default INFO logging; Pelagia was called in process, so it is not a matched comparison of two HTTP servers. The dataset payload size fits within the configured cache capacity; the profile includes explicit cache warmup. In-process cache throughput is not a network throughput claim.

## Bulk ingestion at 20 KiB

| Path | Logical objects/s | Actual operation |
|---|---:|---|
| KV2 | 741 | One put_store call per object |
| Skerry engine | 5,085 | Native 64-object atomic publication groups |
| Skerry coordinator | 2,621 | Up to 64 outstanding requests; bounded writer groups |
| Skerry HTTP batch-put | 47 | Binary batches of 64; current handler awaits each item |

Batch latency units differ: engine bulk latency is batch completion per member, HTTP streaming batch latency is item arrival from batch start, and individual calls have individual completion latency. Do not compare these percentile columns as one common latency metric. Units are preserved in raw output.

## Same-prefix contention

| Initial new writes, 8 workers | KV2 objects/s | Skerry engine objects/s |
|---|---:|---:|
| Uniform BLAKE3 prefixes, 1,024 objects | 1,555 | 102 |
| All original hashes prefix 00, 512 objects | 1,220 | 107 |

The forced-prefix fixture concerns original objects; mixed new objects are not prefix-filtered. These two cases have different counts, so initialization/growth effects also differ; rates are normalized but this is not a pure lock-only microbenchmark.

## Storage footprint after the mixed phase

| Dataset / implementation | Unique payload MiB | Final allocated file MiB | Allocated/payload | File count |
|---|---:|---:|---:|---:|
| 20 KiB / KV2 | 25.00 | 41.35 | 1.65× | 514 |
| 20 KiB / Skerry | 25.00 | 26.43 | 1.06× | 17 |
| 128 KiB / KV2 | 160.00 | 261.01 | 1.63× | 514 |
| 128 KiB / Skerry | 160.00 | 161.15 | 1.01× | 17 |
| 1 MiB / KV2 | 160.00 | 317.01 | 1.98× | 514 |
| 1 MiB / Skerry | 160.00 | 170.11 | 1.06× | 17 |

Final allocation is measured with filesystem stat blocks after the child closes. It includes store files/indexes and growth padding; it is not physical disk I/O or whole-filesystem metadata overhead.

## Durability and integrity controls

The unmodified KV2 profile uses SQLite WAL synchronous=NORMAL, Python os.fsync for blobs, and no payload hash recomputation on ordinary GET. Skerry synchronizes segment bytes and Fjall before success and verifies payload BLAKE3 on disk reads. These contracts are not equivalent. On this installed Rust/macOS target, sync_all uses F_FULLFSYNC; SQLite fullfsync defaults OFF and Python os.fsync is not that stronger call. A SQLite FULL-only sensitivity still does not align those macOS flush calls.

A separate, randomized control uses 256 objects × 20 KiB per trial, three trials, and the same fixtures across its three implementations. It runs after the primary suite, so cross-suite drift remains possible. The KV2 macOS-fullsync variant is an isolated benchmark adaptation: Python blob fsync calls use fcntl(F_FULLFSYNC), SQLite uses WAL/FULL with fullfsync=ON, and reads verify BLAKE3. This is a flush-strength control, not a claim of crash-recovery/directory-sync parity.

| Control / 256 objects | Single PUT/s | 8-client PUT/s | Random GET/s |
|---|---:|---:|---:|
| KV2 FULL + read hash, ordinary fsync | 986 | 1,642 | 2,268 |
| KV2 FULL + read hash + macOS fullsync | 36 | 54 | 2,198 |
| Skerry SyncAll + read hash | 109 | 110 | 77,054 |

Strengthening the flush calls reverses the single-write ranking within this control. This demonstrates that synchronization policy materially affects these measurements; it does not attribute every difference to one syscall or prove durability parity.

## Review findings

1. **Ordinary KV2 reads do not verify payload contents.** `kvstore2.py:432-442,1147-1161` checks metadata/header and length. A same-length payload corruption was returned by GET in a disposable probe; deep verification detected it. Skerry verifies BLAKE3 on disk reads. This is a reliability gap, not a performance optimization to copy.

2. **KV2 does not provide the same durable-ack contract.** Its index connection uses WAL/NORMAL at `kvstore2.py:1311-1316`; a system crash/power loss can lose recent committed mappings. Blob fsync before commit does not strengthen that index setting. No power-loss tests were run on either implementation.

3. **Per-call operations and periodic growth work deserve profiling.** Every GET opens a connection, sets WAL/synchronous/foreign-key pragmas, opens a blob, and parses a JSON record header. The connection context manager does not itself close it. Growth zero-fills and synchronizes on this host (no posix_fallocate), then payload writes synchronize again. See `kvstore2.py:907-923,1121-1145,1311-1317`.

4. **Stale-lock recovery relies on age, not owner liveness.** The probe obtained a second file lock after a short configured TTL while its owner was alive. Default TTL is one hour. This breaks file-lock exclusivity, but SQLite BEGIN IMMEDIATE independently serializes the examined append paths; the probe does not establish overlapping appends or corruption. See `kvstore2.py:94-146,302-305,982-985`.

5. **Skerry default service writes need tuning.** A sparse coordinator batch waits up to its configured window unless filled or interrupted. Native HTTP batch-put currently awaits each item, missing bulk grouping. HTTP admission reserves a maximum-object allowance: 256 MiB / 64 MiB gives four concurrent uploads despite small objects, so the 8-client benchmark includes retry costs. These are source-backed current behaviors, not limitations of Rust or Fjall.

## Scope and next steps

Read gains here concern warm, small indexes; bulk gains include batching and synchronization amortization. Do not infer disk-cold, HDD, very large index, sustained compaction, crash-recovery speed, Windows, or Linux performance from this Mac. The large-object corner has 128 operations/trial, so p99 tails are especially noisy. Synthetic size distributions are assumptions, not a measured Pelagia traffic distribution.

Prioritize Skerry bounded HTTP batch pipelining and upload admission based on known content length, plus a durable-safe duplicate fast path. Preserve the acknowledgment contract. Independently add ordinary-read integrity verification and explicit durability/connection-lifetime policies to KV2 before treating it as an archive reliability baseline. Re-run on the intended Linux/Windows storage and a representative working set before migration. KV2 blobs are not directly importable Skerry segments; migration must validate existing hashes and stream/reinsert objects, with reference mapping if source hashes are SHA-256.

## Evidence and reproduction

Primary audit: {'completed_cases': 141, 'validated_phases': 1269, 'validated_operations': 1085184, 'payload_or_operation_errors': 0}.
Control audit: {'completed_cases': 18, 'validated_phases': 162, 'validated_operations': 41472, 'payload_or_operation_errors': 0}.

- [Harness instructions](README.md)
- [Primary raw results](results/pelagia-2026-10-26/summary.json)
- [Primary measurements CSV](results/pelagia-2026-10-26/measurements.csv)
- [Supplemental control](results/fullsync-2026-10-26/summary.json)
- [Review probe evidence](review-evidence.json)
- [Flush source evidence](sync-semantics.json)

References: [SQLite synchronous/fullfsync](https://www.sqlite.org/pragma.html#pragma_synchronous), [Python connection context manager](https://docs.python.org/3.12/library/sqlite3.html#how-to-use-the-connection-context-manager), [Rust Apple sync implementation](https://doc.rust-lang.org/src/std/sys/fs/unix.rs.html).
