# Performance and memory controls in 0.3.0

This note describes the 0.3.0 capacity controls and the I/O tradeoffs they introduce. The defaults retain a 256 MiB payload-cache budget; larger cache profiles are explicit operator choices. See the [measured worker, cache, and streaming results](PERFORMANCE_UPGRADE_RESULTS.md).

## Capacity defaults

| Area | Default | Effect |
| --- | ---: | --- |
| Buffered object limit | `resources.max_object_mib = 64` | Maximum object held by the ordinary buffered PUT/GET path. |
| Stream threshold / object limit | `streaming.threshold_mib = 8`, `streaming.max_object_mib = 4096` | Objects above the threshold use file-backed streaming, up to 4 GiB. |
| Stream chunks | `streaming.chunk_kib = 1024` | One MiB per transfer chunk; it must fit both write and in-flight read budgets. |
| Stream slots | `streaming.max_uploads = 2`, `streaming.max_reads = 4` | Concurrent large upload and read slots. |
| Stream checksum tables | `streaming.checksum_cache_mib = 16` | Retained per-chunk BLAKE3 tables; one maximum-object table must fit. Zero disables retention and bounds tables by the configured read concurrency. |
| Stream deadlines | `transfer_timeout_seconds = 3600`, `idle_timeout_seconds = 30` | Handler deadline for stream uploads/GET preparation and stalled streamed-GET consumer timeout. Upload body chunks also have a fixed 30-second idle wait. |
| Write byte budgets | `writes.max_pending_mib = 256`, `writes.batch_max_bytes_mib = 16` | The former bounds upload and queued-write payload bytes; the latter is a write aggregation target. |
| Batch admission | `resources.max_inflight_per_batch = 32`, `resources.admission_timeout_ms = 5000` | Caps concurrent batch items and time spent waiting for request capacity. |
| Cache budgets | `payload_mib = 256`, `index_mib = 128`, `metadata_mib = 64` | Separate payload, Fjall block-cache, and estimated cache-index/history budgets. |
| Checkpoint | enabled, every 300 seconds | A checkpoint is attempted periodically after at least 256 MiB of journal growth. |

The full configuration, including limits and comments, is in [`config.example.toml`](../config.example.toml). Optional `server.admin_listen` and cache settings are described below.

## Scale the payload cache deliberately

`cache.payload_mib` is a hard payload-allocation budget, independent of `cache.index_mib`. Keep the default at 256 MiB unless the host memory budget supports more. For an explicit cache-capacity study, increase it in controlled steps such as 256, 1024, 4096, and 8192 MiB while holding the other settings constant and watching `/v1/cache/stats`, process RSS, and system memory pressure.

The payload cache also has three separate constraints:

- `cache.max_object_mib` defaults to 8 MiB. Larger individual values bypass payload admission.
- `cache.max_entries` defaults to 1,000,000 and can be raised to 16,000,000.
- `cache.metadata_mib` defaults to 64 MiB and bounds resident indexes plus second-hit admission history. Its conservative per-entry charge means the default metadata budget permits roughly one hundred thousand resident entries even though `max_entries` is higher. Raise metadata and entry limits together only when measured memory allows it.

`cache.admission_entries` (65,536 by default) is the independent history limit for second-hit admission. A cache entry is admitted on a repeat observation within `admission_window_seconds`; history rotates when full. `cache.shards` must be a power of two from 1 through 64. Each shard has a fixed share of payload, entry, and metadata capacity, so skewed hash distribution can leave space unused in other shards. More shards reduce lock contention but can increase this fragmentation.

The `/v1/cache/stats` response reports resident and retained payload bytes, hits and misses, metadata usage and budget, and skip counts. Payload references held by active responses remain charged after eviction or cache clear until the last reference drops.

## Configured application budget is not process RSS

`Status.configured_budget_mib` is a sum of configured control budgets: MCP response/request memory, two write payload budgets (HTTP ingress and the queued writer), in-flight read buffers, payload/index/cache metadata budgets, and streaming checksum metadata. It is useful for comparing configurations, but it is not a process RSS ceiling or a complete container-memory estimate.

Actual RSS also includes Fjall memtables and other database structures, HTTP/TLS and runtime buffers, request and task state, allocator fragmentation, thread stacks, checkpoint scratch state, and executable/runtime mappings. The operating system page cache is outside the process RSS but still consumes host memory. Measure RSS and host memory pressure under representative workloads; do not infer either from `configured_budget_mib` or the cache payload budget alone.

`writes.max_pending_mib` bounds admitted payload bytes, while `writes.batch_max_bytes_mib` controls how many accepted writes the coordinator tries to gather into one durable group. The grouping target is not another request-admission limit; the byte permits remain the controlling bound. Batch item concurrency is separately controlled by `resources.max_inflight_per_batch`.

## Dedicated monitoring listener

Set `server.admin_listen`, for example `127.0.0.1:7789`, to expose a separate listener for health/readiness, status, metrics, cache statistics, MCP status, and shutdown. It has an independent `resources.admin_max_requests` capacity (8 by default). The listener does not expose the object API, cache mutation, GC, or checkpoint routes. The primary listener remains available for the normal API and its existing authorization rules.

Use the same `SKERRY_TOKEN` authentication on the admin listener. A non-loopback admin address requires that token. The listener is an additional endpoint, not a network firewall; restrict it with host or network policy as appropriate.

## Write admission and disk reserve

`storage.min_free_mib` and `streaming.min_free_mib` default to 256 MiB. Store writes use the larger reserve. Small `put_many` batches hash and deduplicate the complete request, estimate all new record payloads, framing, and metadata allowance, then check space before appending the first record. A failed check returns a typed storage-full error (HTTP 507) without faulting the writer. Large uploads check headroom before creating the temporary upload and before accepting each chunk. Checkpoints use the same effective reserve and retain at least 16 MiB of headroom.

The free-space check includes the configured reserve, a one MiB metadata margin, and estimated write bytes. It is a preflight check, not a filesystem quota or a reservation for future writes: unrelated processes and concurrent uploads can consume space after a check. Keep operational headroom above the configured minimum.

Request and upload admission wait at most `resources.admission_timeout_ms` by default. HTTP 503 is retryable capacity rejection; HTTP 507 indicates the disk-space reserve rejected a write. Clients should distinguish both from unknown transport outcomes and retry only when their operation semantics permit it.

## Journal checkpoints and correctness

V2 checkpointing is enabled by default and starts on the configured interval. It does nothing until journal growth since the prior checkpoint reaches `checkpoint.min_journal_mib` (256 MiB by default). The operation builds a snapshot journal and validates it by replaying the journal authority into a scratch namespace and comparing object, segment, tombstone, and retired-index contents before activation.

Snapshot copying and journal catch-up occur outside the writer lock. `max_catchup_mib` bounds each copy pass; once the remaining suffix is within `max_tail_kib` (256 KiB by default), the checkpoint copies that suffix and durably activates the new journal while holding the writer lock. The old journal is removed only after activation succeeds. This keeps the write pause to a bounded tail while preserving the journal as recovery authority. Checkpoints use temporary disk space, so allow room for both the source journal and the new checkpoint journal in addition to the configured free-space reserve.

## Streaming I/O tradeoffs

Repeated small-object GETs reuse a fixed, sharded set of at most 4,096 routing hints to avoid a separate metadata lookup on every read. A hint only selects the buffered path; the canonical store read still checks existence and content. This small fixed index is additional process overhead, separate from the payload cache.

Streaming avoids retaining an entire large object in the HTTP request buffer. A streamed PUT writes to a temporary file while hashing chunks; finalization then reads the written prefix to calculate the segment digest before sealing the record. This adds a sequential disk read after upload.

The first streaming GET without a cached checksum table scans the full object to calculate its per-chunk BLAKE3 table, then reads and streams the object while verifying each chunk. A later GET with that table retained can verify while streaming in one object read. If `checksum_cache_mib = 0`, each streamed GET rebuilds its checksum table. For large objects, benchmark cold first reads separately from warmed repeats and account for the extra read when interpreting disk throughput.

`transfer_timeout_seconds` caps handler time for streamed routes, including upload reception and GET checksum preparation. Once a streamed GET response body is being delivered, its producer is governed by `idle_timeout_seconds` when a client stops consuming; there is no separate absolute deadline for an actively consumed response body. Upload body chunks have a fixed 30-second inter-chunk idle timeout.

The streaming capacity helper times the complete initial verified GET, including checksum preparation, transfer, and client verification, then measures repeated GETs against that warmed 16 MiB fixture. It is not a cold random-read or multi-object working-set benchmark. It also supports streamed PUT tests. See [`HTTP_CAPACITY_BENCHMARKS.md`](HTTP_CAPACITY_BENCHMARKS.md) for cache profiles, legacy-config comparison, and helper commands.
