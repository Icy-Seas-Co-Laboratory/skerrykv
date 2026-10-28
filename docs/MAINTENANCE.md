# Deletion and maintenance

Delete an object by its 64-character BLAKE3 digest through the live service:

```sh
SKERRY_TOKEN=... skerry delete <hash> --connect http://127.0.0.1:7788
```

The service must have `SKERRY_TOKEN` configured. DELETE requires that service
administrator credential even on loopback; scoped MCP credentials do not grant
it. Deleting a missing object succeeds as an idempotent no-op. A successful
response makes the deletion durable subject to the storage synchronization
assumptions in [FORMAT.md](FORMAT.md). An interrupted request may have committed,
so retry it. A later PUT of the same bytes may create the object again.

Run an online collection pass with `skerry gc`, or `skerry gc --force` to
request a forced pass. Both require the service administrator credential. Force
bypasses age, cooldown and garbage-ratio thresholds, but retains the copy budget,
rate limit, and free-space reserve. The service also supports
`POST /v1/admin/gc?force=true`. A pass can outlast the service request deadline;
if a request times out or disconnects, the outcome is unknown and the operation
continues. Check `skerry status --json` for `gc.running`, `gc.last_result` and
`gc.last_error`; a subsequent pass is safe.

Stop the service before running offline operations against its store. Each
operation acquires exclusive access and refuses a concurrently open store:

```sh
skerry upgrade --path ./data
skerry vacuum --path ./data --dry-run
skerry vacuum --path ./data
```

New stores use version 2. Existing version 1 stores require explicit `upgrade`
before deletion, GC, or vacuum. Older SkerryKV binaries reject version 2 stores;
upgrade all tools before activating the new format. `upgrade` prepares an older
store for deletion-aware maintenance. Vacuum
rewrites live data to reclaim obsolete data. Review the dry-run output and
ensure sufficient free space for the rewrite before running it. Logical
deletion and physical reclamation are not secure erasure: old data may remain
in filesystem snapshots, backups, device remapping, or other copies.

The `[gc]` configuration controls automatic collection; it is disabled by
default. Defaults are initial tuning values, not performance guarantees:

| Setting | Default |
|---|---:|
| enabled | false |
| interval_seconds | 1800 |
| min_garbage_ratio | 0.5 |
| delete_grace_seconds | 86400 |
| cooldown_seconds | 86400 |
| max_copy_mib | 1024 |
| max_bytes_per_second_mib | 25 |
| low_space_percent | 20 |
| target_free_percent | 30 |
| min_free_mib | 256 |

Online GC copies a whole source segment within one pass. A source whose live
copy size exceeds `max_copy_mib` stays in place until more of its contents become
obsolete, the budget is raised, or offline vacuum is run. Low-space pressure
does not bypass the grace or cooldown periods. Set thresholds and budgets for
the workload and available space. Percentages
must satisfy `low_space_percent < target_free_percent <= 100`, and the garbage
ratio must be finite and between 0 and 1 inclusive.

Importing a v2 store root filters its source through the source journal's live
namespace. Standalone segment directories and independent segment bundles without a store journal contain no
deletion history and are not logical backups: importing them can reintroduce
records deleted from the source store.

## Local validation and performance

The full Rust suite passed 112 tests. A real v0.1-to-v0.2 executable migration and maintenance smoke passed; see [acceptance evidence](ACCEPTANCE.md) and [smoke results](MAINTENANCE_SMOKE.json). Reproduce with:

```sh
python3 scripts/smoke-maintenance.py --binary target/release/skerry
# Add --legacy /path/to/skerry-v0.1.0 to exercise the real upgrade.
```

A small three-trial comparison on the same Mac used identical 128-object fixtures, 20 KiB payloads, loopback HTTP, 1 ms batching and GC disabled. Values are medians; this is a small warm-cache comparison, not the original larger benchmark matrix or a cold-disk test. The mixed workload uses eight clients and 25% new writes.

| Operation | v0.1 ops/s | v0.2 ops/s | v0.2 / v0.1 |
|---|---:|---:|---:|
| single_write | 79 | 46 | 0.59× |
| duplicate_write | 5,274 | 3,952 | 0.75× |
| random_read | 8,138 | 9,015 | 1.11× |
| batch64_write | 5,068 | 3,077 | 0.61× |
| mixed_25_percent_write | 1,885 | 1,019 | 0.54× |

[Raw comparison](V2_PROTOCOL_BENCHMARK.json) includes timings and executable hashes; `scripts/benchmark-protocol-v2.py` reproduces the workload. V2 adds mutation-frame and witness synchronization ahead of the existing index synchronization. Its additional durable write barriers cost throughput here; batching remains important. Duplicate/read differences are not isolated causal estimates. Earlier v0.1 benchmark gains must not be advertised as v0.2 performance.

The next improvements should be incremental compaction for sources larger than a pass budget, candidate-scan/time limits with resumable progress, and periodic safe journal checkpoints to bound mutation history and first-open migration recovery (normal startup uses the persisted replay cursor). Vacuum already compacts the journal and index. Tombstones remain retained to fence old imports; they must not be dropped merely by age. New durability-barrier optimizations require crash tests and remeasurement.
