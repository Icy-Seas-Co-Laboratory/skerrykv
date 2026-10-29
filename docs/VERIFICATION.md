# Verification, startup status, and index discovery

The HTTP listener starts after configuration and credential validation, before
storage recovery. Full integrity verification remains explicit; ordinary startup
checks metadata and replays only unapplied journal entries.

## Lifecycle and readiness

- `GET /v1/health` is process/API liveness and can return 200 while storage is starting or failed.
- `GET /v1/ready` is unauthenticated readiness: 200 when storage can accept work, otherwise 503. A faulted writer makes readiness fail.
- `GET /v1/status` uses the existing administrator authentication policy. It includes `lifecycle` (`starting`, `ready`, or `failed`) and a `startup` progress snapshot. During startup it has no storage statistics yet. If storage statistics are busy, lifecycle and startup progress remain available.
- Object and maintenance routes return 503 until storage is ready. Administrators receive lifecycle/progress details; scoped and unauthenticated clients do not receive administrator progress details.
- Startup failures keep liveness/status available until shutdown. Public failure messages exclude filesystem paths; detailed diagnostics remain in service logs.
- `POST /v1/admin/shutdown` works during startup under the same administrator policy. Startup recovery and verification check cancellation between bounded I/O chunks/transactions; filesystem/database calls themselves are not interruptible.

Use `skerry ready --connect http://127.0.0.1:7788` for readiness checks. Docker uses
readiness for its health check. Deployments previously using `/v1/health` to route
object traffic should switch to `/v1/ready`.

## Current indexes

`GET /v1/indexes?limit=100&after=generation-<uuid>` lists persisted index generations
with administrator authorization. `limit` is 1–1000. Responses contain `indexes`
and `next_cursor`; pass a non-null cursor as `after` for the next page.

Each entry includes `name`, `selected`, `state` (`selected`, `retained`, or
`incomplete`), `format_version`, and `keyspaces`. Keyspace names are enumerated for
the selected, already-open generation only. Retained generations have an empty
keyspace list and null format version because listing does not open them.
Filesystem paths and object contents are excluded. Pagination is by generation
name and is not a snapshot across maintenance operations.

The equivalent command is `skerry indexes --connect http://127.0.0.1:7788`.
Skerry still uses one configured store per service; this endpoint lists its index
generations and active keyspaces, not a registry of independent stores.

## Verification coverage

`skerry verify --path ./archive` takes the exclusive store lock. It checks journal
frames and committed records, payload BLAKE3 hashes, segment metadata and sealed
segment digests. Payload and whole-segment digests are computed during one buffered
read pass. V2 journal frames are validated while reconstructing the namespace in
one replay pass; disposable reconstruction does not synchronize a durable recovery
cursor. Temporary index copies avoid opening the source database for verification.
The source index and segment files are not repaired or rewritten.

For v2, verification compares objects, segments, tombstones, and retirements in the
selected index with journal state at its persisted replay cursor. A valid journal
suffix not yet applied to the index is reported as `recovery_required: true`.
An older index without a usable cursor also reports recovery required; journal and
payload validation can succeed, but serving-index parity is not certified until
normal recovery establishes a cursor. A cursor claiming an invalid frame boundary
or a differing indexed namespace fails verification.

Online MCP segment verification uses a committed metadata snapshot and checks its
records and seal. It does not certify whole-store journal/index consistency. Jobs
remain bounded, cancellable, and scoped to their authenticated owner.

## Progress counters

Startup status, CLI verification, MCP verification jobs, and the TUI share progress
fields. CLI emits periodic progress JSON prefixed with `verify progress:` on stderr;
stdout remains the final JSON result. Ctrl-C/termination requests cancellation and
allows temporary verification state to be cleaned up.

- `state`, `phase`, `elapsed_seconds`, `bytes_per_second`, and bounded `failures` describe execution.
- `bytes_scanned` counts segment bytes processed; `bytes_verified` counts payload bytes whose individual hashes passed. They differ when corruption is encountered, and neither means physical device I/O.
- `records_checked` advances after successful payload hash checks. `segments_processed` and `segments_verified` distinguish scanned/recovered metadata from complete segment verification.
- `journal_bytes` and `transactions_processed` track journal recovery/reconstruction. `index_bytes_copied` tracks private index copying; `index_entries_compared` tracks namespace comparison.
- `bytes_total` and `segments_total` are optional. Unknown totals stay null; there is no synthetic overall percentage across phases.

MCP's legacy `verified_bytes` field retains its previous scanned-byte meaning;
new consumers should use the detailed `progress` fields. Active job details on the
scoped MCP status endpoint are filtered to the authenticated principal. Administrator
service status and the TUI may show all active jobs.

## Resource limits

```toml
[verification]
max_concurrency = 1
max_bytes_per_second_mib = 0
```

Concurrency is bounded to 1–16 workers; one is the conservative default for HDDs.
The bandwidth limit is shared across scans within a store and zero means unlimited.
It governs segment scanning, not journal replay or private index copying. The
existing MCP job limit also applies. Online scans retain shared request, memory,
and descriptor reservations; interrupted jobs cannot release capacity while their
blocking workers continue. Offline verification uses bounded worker batches and
stops sibling scans when one fails.

## Benchmarking

`scripts/benchmark-verification.py --baseline /path/to/old/skerry --candidate
/path/to/new/skerry --output results.json` compares optimized executables on isolated
small-object, large-object, and deletion-churn stores. It alternates executable runs,
excludes one warmup, and reports individual times and medians. The OS page cache is
not evicted. CLI/database startup and index reconstruction are included, so fewer
segment reads do not imply a proportional end-to-end speedup. This benchmark does
not measure physical device bytes, CPU use, or foreground latency under load.

Saved local measurements are in [offline results](VERIFICATION_BENCHMARK.json)
and [foreground results](VERIFICATION_FOREGROUND_BENCHMARK.json). The latter is an
engine-level probe with 500 verified reads overlapping a rate-limited scan; it
excludes HTTP, Coordinator queueing, and MCP overhead. It is a small cache-warm
sample, not evidence about cold HDD performance or sustained production latency.
Reproduce it with `cargo run --release --example verification_foreground`.

Recovery mode, clearing counters, journal-specific throughput and server upgrade
instructions are described in [the 0.2.2 recovery fix](RECOVERY_UPDATE.md).
