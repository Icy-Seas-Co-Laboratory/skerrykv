# Architecture

SkerryKV is one Rust package with an independent synchronous storage engine and bounded asynchronous service coordination. `engine` owns Fjall, segment files, synchronization, and exclusive store ownership. `coordinator` owns write batching, read scheduling/coalescing, memory permits, and payload caching. `server` implements Axum endpoints and binary transport. `main`, `tui`, and `bench` provide native tools.

## Authority and data flow

Payload bytes in segments are authoritative for object contents. During normal operation the selected Fjall generation is authoritative for committed locations and logical ends. A structurally valid record beyond the indexed committed end is uncommitted. A durable commit can exist without its acknowledgment reaching a client.

PUT admission bounds the object and outstanding bytes. A single coordinator groups up to 512 calls over at most 10 ms. The engine hashes, checks persistent and same-batch duplicates, appends unique records, synchronizes segment bytes, atomically publishes object locations and segment metadata across two Fjall keyspaces, then calls `Database::persist(SyncAll)`. Successful waiters receive the canonical digest. Rotation commits the current group before sealing its segment and continuing in a new one. Batch PUT is partial-success transport, not a transaction.

The writer mutex serializes storage mutation. A publication visibility lock protects index commit and persistence, so ordinary readers do not see the intermediate publication phase. This lock is not held during payload reads or append I/O. On ambiguous failure the writer faults and further writes fail; valid but unacknowledged records whose index commit already occurred may remain readable. Segment synchronization precedes index publication, so those reads still validate actual bytes.

GET checks the payload cache, coalesces pending identical hashes, resolves metadata on a blocking worker, and enqueues bounded positional reads. Every disk-read payload is BLAKE3 verified. Cache hits reuse previously verified immutable buffers. No hash mismatch is returned as successful data. HEAD checks existence in the index and does not read/hash payload contents.

## Store layout and index schema

```
store/
  config.toml                 # created by init; service may use an external config
  store.lock                  # exclusive ownership, never removed to steal a lock
  index/
    generation-<uuid>/        # Fjall files
    activation-<sequence>.json # checksummed immutable activation records
  segments/
    ab/ab......-....seg       # complete UUIDv4, first two compact UUID digits
  logs/
```

Object keys are raw 32-byte BLAKE3 digests. Values are version byte 1, 16 raw UUID bytes, payload-start offset u64 LE, and payload length u64 LE (33 bytes total). Record headers are recoverable at `payload_offset - 64`.

Segment keys are raw 16-byte UUIDs. Values encode version, active/sealed/quarantined state, relative slash-separated path, allocated file length, committed logical record end, physical record count, indexed live payload bytes, creation/seal timestamps, and optional segment digest. See the exact [format specification](docs/FORMAT.md). Fjall value separation is explicitly disabled. An optional geometric growth factor changes logical file length with `set_len`; it does not claim to reserve physical blocks.

## Segment lifecycle and durability boundaries

A new UUID segment header is synchronized, its containing directory is synchronized on Unix, then its metadata is published. Records have independent fixed headers with CRC32 and full payload BLAKE3. The indexed committed end is updated in the same batch as the objects it includes. Segment capacity includes the eventual seal footer.

Sealing requires all records to be committed. The writer truncates unused logical allocation, computes BLAKE3 over `[0, data_end)`, writes a fixed checksummed footer, synchronizes it, closes the writer handle, and persists sealed metadata. The footer digest excludes the footer itself. Records never change after sealing.

| Interruption | Startup action |
|---|---|
| Partial append or fully written unindexed record | Validate committed prefix, truncate active tail |
| Segment synchronized before index commit | Same; never promote the tail in normal recovery |
| Index commit before journal synchronization | Recover whichever atomic batch Fjall retained; payload was synchronized first |
| Durable commit before response | Object may exist; retry deduplicates |
| Partial seal footer | Truncate to indexed committed end and resume |
| Valid synchronized footer, index still active | Validate identity/count/end/digest against metadata, finish sealing |
| Committed payload corruption/missing segment | Fail startup; no automatic truncation of committed data |

Fault injection covers record header/payload writes, segment synchronization, index commit/persistence, acknowledgment, footer boundaries, activation, and import publication. These process-level tests validate protocol recovery; hardware power-loss behavior remains an external validation requirement.

## Index rebuilding and import

Offline commands acquire the same exclusive store lock. Rebuild scans without depending on old object mappings, constructs a fresh generation, persists it, closes its handles, and activates it via a new checksummed immutable control record. Unix directory synchronization orders publication. Older generations/control records remain retained for operator recovery. The highest activation sequence is authoritative; malformed newest activation is an explicit error, avoiding silent rollback to an index that predates later writes. This is not a directory replacement scheme and does not require renaming an open Fjall directory.

Default rebuilding includes sealed files only. Active records require explicit salvage and can include unacknowledged writes. Deterministic deduplication follows sorted relative segment paths and increasing record offsets. UUID collisions and validated-record corruption are errors. There is no way to recover exact client acknowledgment history from segment contents.

Import locks the source and destination, verifies sealed input, copies into staging, verifies destination bytes independently, synchronizes and publishes the destination file, then commits object/segment metadata. Existing mappings win. A crash can leave unreferenced bytes; retry verifies them before reuse. Same UUID with different complete bytes is rejected. Redundant payload bytes remain until an explicit future garbage-collection feature.

## Bounds and scheduling

HTTP admissions last through response-body lifetime. Uploads reserve a maximum-object ingress allowance before reading. Coordinator writes separately retain byte permits until completion. Read allocations retain permits through every reference, including network responses. Cache allocations have an independent semaphore; eviction does not release a pinned allocation prematurely. Admission metadata, queues, worker count, descriptor cache, and batch item count are separately bounded.

The cache has probation and protected LRU segments with an 80% protected target. A bounded, expiring second-hit tracker reduces one-time scan pollution. The cache has no content expiration, warming, or automatic pressure detector. Thirty-second maintenance expires admission history. Memory accounting covers payload allocations, not a promised process RSS ceiling.

SSD scheduling adds no accumulation sleep. HDD scheduling accumulates a short configurable window, groups by segment and offset within priority classes, and promotes requests older than 100 ms. The scheduling window does not impose a hard device-I/O deadline. A single dispatch window is at most 512 operations. Positional APIs are Unix `read_at` and Windows `seek_read`; no direct I/O, io_uring, or unsafe Rust is required. Reader handles use a bounded cache and active leases prevent eviction of in-use files. Reader/writer handles are not renamed or deleted during ordinary operation.

Ordered batch GET dispatches one item at a time. Completion mode has a bounded window and polls input and completed results concurrently. A full retained-read budget returns a retryable resource error, so clients cannot deadlock the server by collecting more result buffers than capacity. Large clients should consume and release results incrementally.

## Shutdown and observability

Cancellation stops HTTP admission, drains accepted HTTP work for a bounded interval, then drains the accepted write queue, seals, and persists. Final persistence failure produces a nonzero service result. Forced termination cannot turn a failed operation into a successful acknowledgment. Signals are native Ctrl+C and Unix SIGTERM; the TUI is optional and detachment leaves the foreground service running.

Counters represent logical application requests/bytes. Cache hits do not establish physical disk reads avoided, since the OS page cache may already have served a miss. Latency percentiles use the most recent 4,096 completed coordinator GET/PUT calls; cumulative storage/queue timings are wall-clock logical-operation measurements. Errors are retained in a bounded list; tokens are never part of status. General events use tracing on stderr/stdout. The process has no physical disk, CPU, or RSS measurement claim.


## MCP adapter

`mcp` implements official rmcp tools/resources over the same `Arc<Coordinator>` as native HTTP. `mcp_http` supplies stateless Streamable HTTP, per-request principals, exact Origin checks, gateway attestation, rate/concurrency limits, and response allocations retained through network lifetime. Its routes sit inside the existing global HTTP admission middleware. The adapter never opens an index, creates a writer, or takes another store lock. `mcp_stdio` is a pure network proxy to that HTTP endpoint using the SDK on both transports.

Inspection APIs return bounded UUID/index pages and path-free metadata. Their blocking closures own global request permits until work actually exits, even if an asynchronous caller times out. Online verification takes a committed metadata snapshot and scans immutable prefix bytes with fixed buffers, the shared descriptor cache and read-memory permits. Job admission, history and identity rate state are bounded; shutdown prevents new jobs and cooperatively cancels/joins active jobs. Verification does not repair storage. Root paths and payload-bearing diagnostics are not exposed through MCP status/errors.

Local static credentials have read/write/maintenance scopes. The administrator credential grants read through MCP; writes and verification require separate configuration and scopes. Non-loopback MCP requires an explicit HTTPS OAuth-capable gateway contract. Gateway-provided scope/subject headers are trusted only with its private backend credential; untrusted scope headers cannot elevate ordinary callers. SDK tool annotations describe behavior but never authorize it. See [MCP.md](docs/MCP.md) for protocol revisions, security deployment and limits.
