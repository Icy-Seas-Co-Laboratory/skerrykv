# Append lanes in 0.4.0

SkerryKV supports 1, 2, 4, or 8 append lanes per store. The default remains one. Several lanes can append and synchronize separate segment files concurrently while sharing the same hash namespace, index, and durable publication gate.

```toml
[writes]
writer_lanes = 4
batch_max_records = 512
batch_max_bytes_mib = 16
batch_max_delay_ms = 1
commit_max_delay_ms = 10
max_pending_mib = 256
```

Configure the lane count before starting the service. It is not a live-resizing setting. Multiple processes still cannot open the same store for writing; the exclusive ownership lock remains in force. The UUID directory layout and cache shards are separate from write scheduling.

## Ordering and scheduling

The coordinator hashes each buffered payload once, assigns its first hash byte to one of 256 virtual shards, and maps the shard deterministically to a lane. Ready shards receive round-robin service. PUT, DELETE, and prepared streamed-upload publication use the same shard queue, preserving FIFO order within a shard. A hot hash remains serialized. Independent shards can make progress on different lanes.

Buffered batches respect record, byte, and delay targets. Each append lane owns its active file and retains ownership through publication of the batch. Once its payload is synchronized, the batch joins a shared group-commit queue. In multi-lane mode, the leader waits at most `commit_max_delay_ms` (10 ms by default), waking early when every configured lane has a ready group or a record/byte target is full. This separate gather window holds no publication lock; sparse or skewed traffic may pay the full extra wait. One configured lane ignores this extra window. A leader combines ready batches under the configured targets; journal/witness synchronization and durable index publication precede acknowledgments. Stream, deletion, GC, and checkpoint publication use the same global publication gate. They are not independent journals.

A committed-object duplicate can use a non-mutating shortcut with any lane count. It requires a healthy publication probe, no pending DELETE in the virtual shard, an unchanged deletion epoch, and a final healthy-publication check under admission. It may bypass queued PUT/stream work that cannot remove an existing hash, but it cannot bypass a deletion fence. Actual namespace mutations retain FIFO shard order. Fast duplicates are counted in the global I/O counters; per-lane completion counts describe queued operations.

Request and pending-byte admission remain global. Adding lanes does not multiply those allowances. Accepted queued work retains its permits until blocking I/O completes even if its caller disconnects. The coordinator drains lane work before sealing/synchronizing all active files on shutdown. Disk-reserve rejection before append does not fault the writer; uncertain publication errors conservatively stop new durable acknowledgments.

## Store capability and recovery

Selecting more than one lane activates store version **3**. The physical segment-record and mutation-journal encodings are retained, but recovery now permits several active files. Older 0.3.0 tools reject this activation before opening the index. The root marker retains its legacy filename, `format-v2`; its checksummed activation content identifies the required capability.

Changing `writer_lanes` back to one does **not** downgrade the store. Use 0.4.0 tools for subsequent operation, verification, rebuild, import, and vacuum. A legacy v1 store still requires the explicit offline upgrade command before multi-lane operation.

Lane ownership is recoverable from the segment UUID modulo eight. Recovery understands all eight slots regardless of the currently configured count, so an interrupted store can reopen with fewer lanes without rehashing or copying its objects. Previously active, now-unused slots remain protected from GC until a seal/shutdown or offline vacuum; startup does not hash entire files merely to retire unused lanes. Clean shutdown seals all active slots. Activation version 3 remains sticky through checkpoint, rebuild, and vacuum.

The segment size target is per lane. With the default 8 GiB target, eight lanes can approach 64 GiB of unsealed segment data before rotation; increasing lanes can therefore delay garbage reclamation. Choose a smaller segment target when churn and reclaim latency matter. Already written or dormant files retain their committed contents when settings change.

Each lane's committed end is journal-authoritative. Recovery truncates unacknowledged tails and preserves earlier acknowledged objects and tombstones. A prepared stream upload dropped before publication starts removes its unreferenced file. Files from an uncertain publication or a process crash remain subject to the existing conservative orphan rules.

## Monitoring and limits

`GET /v1/status` includes fresh `writers` counters independent of the cached filesystem metadata snapshot:

- Configured lane count and virtual-shard count.
- Per-lane queued/in-flight items and logical bytes, active shards, and oldest queued wait.
- Batch/completion counts and cumulative queue, engine-service, append, data-sync, and commit-wait time.

Prometheus exposes corresponding `skerry_writer_lane_*` families labeled by lane. `storage.active_segments` lists every active UUID; the older `active_segment` field remains for compatibility, and `active_bytes` is the total across active files.

These timings are logical wall-clock measurements, not physical device telemetry. A group leader's commit-wait time includes its publication work; summed concurrent lane times are not elapsed service time. Global application memory limits are not an RSS ceiling. Writers can hold up to eight active file descriptors in addition to the bounded read descriptor cache, database files, and staged uploads.

Parallel appends do not remove journal/index synchronization or guarantee a gain on one device. Start with the default and select a higher count based on the [matched benchmark results](MULTI_WRITER_REVIEW.md), including successful-request p99, rejection/unknown outcomes, and monitoring availability. The [original design](MULTI_WRITER_DESIGN.md) records the intended invariants and staged implementation.

Native batch PUTs refill their active window as writes complete while emitting results in request order. Completed later results can wait behind an earlier write without occupying active write slots. Retained active/completed results are bounded to four windows, capped by `resources.max_batch_items`; global payload-byte admission still applies. A completed write failure stops further dispatch, and already dispatched items drain in order.
