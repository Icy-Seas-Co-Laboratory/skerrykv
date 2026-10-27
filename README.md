# SkerryKV

SkerryKV is a native Rust service for immutable binary objects. A BLAKE3 hash identifies each object; Fjall stores compact locations, while payloads live in append-only UUIDv4 segment files. It suits write-once/read-many archives and small binary datasets. No external database or Docker installation is required for native use.

The MVP uses one storage volume, one coordinated batching writer, concurrent verified positional reads, SSD/HDD scheduling, and a bounded second-hit SLRU payload cache. Every successful PUT is synchronized. See [architecture](ARCHITECTURE.md), [disk format](docs/FORMAT.md), [batch protocol](docs/PROTOCOL.md), and [implementation evidence](docs/ACCEPTANCE.md).

See the [performance improvements and rerun](benchmarks/IMPROVEMENTS.md) and original [Pelagia KVStore2 comparison](benchmarks/COMPARISON.md) for measured read/write tradeoffs, review findings, and a reproducible benchmark harness.

## Development and native startup

Install stable Rust (minimum 1.90 for the locked dependencies), native C/C++ build tools, and CMake for the MCP TLS backend. Packaged executables need none of these build tools.

```sh
./scripts/dev.sh             # foreground headless service
./scripts/dev.sh --tui       # service with dashboard
cargo run -- serve --tui     # creates config.toml and ./data if absent
```

PowerShell:

```powershell
./scripts/dev.ps1
./scripts/dev.ps1 -Tui
cargo run -- serve --tui
```

Build a standalone executable:

```sh
cargo build --release --locked
./target/release/skerry init --path ./archive
./target/release/skerry serve --config ./archive/config.toml
```

On Windows use `.\target\release\skerry.exe`. Native deployments run in the foreground; use your operating system's service manager if needed. CI builds archives for Windows x86-64, Linux x86-64, and Linux ARM64, including the executable and example configuration.

```sh
skerry status
skerry tui --connect http://127.0.0.1:7788
skerry stop
skerry --help
```

`status`, `stop`, and cache commands accept `--url`. Set `SKERRY_TOKEN` in the client environment when authentication is enabled. Use `Ctrl+C` or SIGTERM to drain and persist the service. A second writable or maintenance process fails on the exclusive store lock.

## Docker

The container listener is non-loopback, so authentication is required even with host loopback publication. Set a token before starting:

```sh
export SKERRY_TOKEN="$(openssl rand -hex 32)"
docker compose up -d --build
docker compose exec skerry skerry tui
# A named container also supports: docker exec -it <container> skerry tui
docker compose down
```

PowerShell can set `$env:SKERRY_TOKEN` to a randomly generated secret. Compose inherits this token for both the server and commands executed inside the container. Do not commit it. The default host mapping is `127.0.0.1:7788:7788`.

The image runs as an unprivileged user and stores configuration and data under `/var/lib/skerry`, backed by the `skerry-data` named volume. Named volumes receive image ownership on initial creation. For bind mounts, grant the container user read/write access to the entire store, including its lock, index, and segment directories. Do not share a live store with another service. The health check queries the actual service; SIGTERM initiates graceful shutdown. Use TLS through a trusted reverse proxy for remote access.

## HTTP API

```sh
printf 'example binary payload' > example.bin
curl -sS -H 'Content-Type: application/octet-stream' \
  --data-binary @example.bin -X PUT http://127.0.0.1:7788/v1/objects
# {"hash":"<64 hex characters>","duplicate":false}
curl -f http://127.0.0.1:7788/v1/objects/<hash> -o recovered.bin
curl -I http://127.0.0.1:7788/v1/objects/<hash>
curl http://127.0.0.1:7788/v1/status
curl http://127.0.0.1:7788/v1/metrics
```

When configured, add `-H "Authorization: Bearer $SKERRY_TOKEN"`. All endpoints except health require the configured token, including loopback clients. Non-loopback listeners refuse startup without one. Tokens are read from the environment and excluded from configuration/status. MCP scoped tokens additionally allow only their native object read/write routes; administrator status and control endpoints remain separate. Proxy forwarding headers do not bypass authentication.

| Method | Route | Response |
|---|---|---|
| GET | `/v1/health` | JSON, 503 if unhealthy |
| GET | `/v1/status` | Actual storage, I/O, and cache counters |
| GET | `/v1/metrics` | Prometheus text exposition |
| PUT | `/v1/objects` | Raw upload; JSON canonical hash and duplicate flag |
| GET / HEAD | `/v1/objects/{hash}` | Raw bytes / existence |
| POST | `/v1/objects/batch-put` | Streaming binary per-item results |
| POST | `/v1/objects/batch-get` | Streaming binary payloads and statuses |
| GET | `/v1/cache/stats` | Cache counters |
| POST | `/v1/admin/cache-clear` | Evict resident payload cache |
| POST | `/v1/admin/shutdown` | Begin graceful shutdown |

GET supports `?priority=interactive`, `normal`, or `bulk`. Batch GET defaults to input order; `?ordered=false` permits completion order. The [binary protocol specification](docs/PROTOCOL.md) includes layouts and failure semantics. Batch PUT allows partial success; previously committed items survive a malformed suffix. Clients must consume responses while streaming large requests to avoid transport backpressure deadlocks.

## Dashboard and cache

`serve --tui` embeds the dashboard; `tui --connect <url>` attaches independently. `q` detaches, `r` refreshes, `Tab` changes panels, `?` shows help, `c` clears cache, and `s` requests shutdown with explicit Enter confirmation. Detaching an embedded dashboard leaves the foreground service running. With `tui --config <file>`, an offline local endpoint offers a launch key; detaching leaves that child service running and `stop` shuts it down.

The dashboard displays measured counters and rates. Latency percentiles summarize the latest 4,096 completed GET/PUT calls, including queue waits. They are not physical disk latency percentiles. Storage errors and coordinator events appear in the logs panel. General tracing output goes to the process log stream; embedded TUI mode writes to `<store>/logs/skerry.log`.

```sh
skerry cache stats
skerry cache clear
```

The payload cache uses a bounded second-hit admission tracker and probation/protected SLRU segments. Immutable payloads need no TTL. Admission history expires after five minutes by default, with maintenance every 30 seconds. Large objects exceeding one-quarter of the cache budget bypass admission. Evicted buffers still held by responses remain charged until their last reference drops. Cache clear evicts resident entries; it cannot reclaim active response references immediately. There is no speculative warming or automatic memory-pressure resizing in this version.

## Configuration

`init --path <directory>` creates a store and its `config.toml`. Relative `storage.root` paths resolve against the configuration file directory. Environment overrides are `SKERRY_LISTEN`, `SKERRY_ROOT`, and `SKERRY_TOKEN`. Explicit command destination/configuration options select the store or endpoint; unknown TOML keys fail validation. `config.example.toml` supplies a starting configuration.

All fields have defaults:

| Setting | Default | Meaning |
|---|---:|---|
| `server.listen` | `127.0.0.1:7788` | Numeric IP and port |
| `server.request_timeout_seconds` | 120 | Handler deadline |
| `server.shutdown_timeout_seconds` | 30 | HTTP drain interval before final writer persistence |
| `storage.root` | `./data` | Entire relocatable store |
| `segments.max_size_bytes` | 8589934592 | Segment limit including header/footer |
| `segments.growth_factor` | 1 | 1 disables growth; 2 enables logical geometric file growth |
| `writes.batch_max_records` | 512 | Maximum coordinated write group |
| `writes.batch_max_delay_ms` | 1 | Maximum accumulation window; existing configs retain their value |
| `writes.max_pending_mib` | 256 | Writer payload budget; HTTP ingress has a separate budget of the same size |
| `reads.policy` | `ssd` | `ssd` or `hdd` |
| `reads.max_concurrency` | 32 | Concurrent read operations |
| `reads.hdd_window_ms` | 4 | HDD accumulation window |
| `reads.queue_items` | 1024 | Bounded read channel |
| `resources.max_object_mib` | 64 | Largest admitted/read payload |
| `resources.max_inflight_read_mib` | 256 | Allocated read buffers, including retained response references |
| `resources.max_open_segments` | 256 | Cached segment reader handles; writer/index handles are additional |
| `resources.max_requests` | 128 | HTTP admissions and coordinator request permits |
| `resources.max_batch_items` | 4096 | Frames per batch request |
| `cache.payload_mib` | 256 | Payload allocation budget; 0 disables |
| `cache.index_mib` | 128 | Fjall block-cache budget |
| `cache.admission_entries` | 65536 | Bounded second-hit tracking entries |
| `cache.admission_window_seconds` | 300 | Second-hit eligibility window |
| `logging.level` | `info` | Tracing filter fallback; `RUST_LOG` overrides |

Objects must fit a segment with framing overhead. Resource limits must fit at least one maximum object. Limits represent independently configured budgets, not a whole-process memory bound: Fjall memtables, network/TLS buffers, cache metadata, and allocator overhead are additional. A full read buffer budget produces a retryable resource error rather than waiting indefinitely behind slow responses. Accepted TCP connections are capped at twice `resources.max_requests`, including idle sockets. HTTP input has a 30-second idle timeout. There is no public buffered acknowledgment mode.

## Verification, recovery, and imports

Stop the service before maintenance:

```sh
skerry stop
skerry verify --path ./archive
skerry rebuild-index --path ./archive
# Only after assessing the possible recovery of unacknowledged active records:
skerry rebuild-index --path ./archive --salvage
skerry import-segments --source ./other-archive --path ./archive
```

Normal startup verifies committed data and index references, discards uncommitted active tails, and reconciles interrupted seals. Committed corruption or a missing referenced file fails closed. Synchronization failures fault the writer; restart/recovery is required before further writes. Existing validated reads may continue.

A default disaster rebuild includes validated sealed segments and excludes active files. This can exclude acknowledged objects from the last active segment if the index has been lost. `--salvage` explicitly permits recovering valid active records that might never have been acknowledged. Neither mode reconstructs client acknowledgment history. The old index generation remains available; checksummed activation records select the latest generation. Invalid newest activation records do not silently fall back to a stale generation.

Import copies sealed segments while both stores are offline. It verifies staged bytes, synchronizes the destination, then publishes index mappings. Existing object mappings win; redundant retained physical payloads are reported. A conflicting UUID is an error. The source remains unchanged. No deletion or garbage collection is implemented; unreferenced archival files are reported for explicit maintenance.

## Model Context Protocol (MCP)

MCP is enabled at `/mcp` for the loopback service and defaults to read-only. It uses the existing store, coordinators, durability rules, verified reads, and cache. Start the service first, then attach an MCP client through the native stdio bridge:

```sh
skerry mcp stdio --connect http://127.0.0.1:7788
skerry mcp status
```

Example for clients supporting an `mcpServers` JSON configuration:

```json
{
  "mcpServers": {
    "skerrykv": {
      "command": "/absolute/path/to/skerry",
      "args": ["mcp", "stdio", "--connect", "http://127.0.0.1:7788"]
    }
  }
}
```

The stdio process never opens a store or acquires its lock; stdout is protocol-only. On Windows use the actual path to `skerry.exe`. For authenticated local access supply `SKERRY_MCP_TOKEN` through the client's secret environment.

Tools inspect service/segment/object state, retrieve complete small objects, put deduplicated objects when explicitly authorized, and run bounded online verification jobs. The `skerry://objects/{hash}` resource template provides binary resources without enumerating every object. Inline retrieval and MCP PUT default to 128 KiB; larger retrievals return explicit native HTTP instructions. List pages default to 100. The dashboard includes an MCP panel with measured counters and latency.

Use separate `SKERRY_MCP_READ_TOKEN`, `SKERRY_MCP_WRITE_TOKEN`, and `SKERRY_MCP_MAINTENANCE_TOKEN` secrets. Writes additionally require `[mcp] read_only = false`; verification requires `allow_verification = true`. The native administrator token grants only read through MCP. Scoped read credentials can retrieve large objects through native HTTP without gaining administrator access.

Remote Streamable HTTP requires HTTPS and an explicitly configured compatible MCP OAuth authorization gateway. A simple public static token is not advertised as OAuth compatibility. Non-loopback MCP refuses startup without gateway configuration; disable MCP for a non-MCP native deployment. Origin/Host validation, per-client rate limits, shared global admission, response-memory limits, and scope checks apply. Docker disables MCP by default until the gateway deployment is configured explicitly.

See [MCP setup, tools, permissions, limits, client examples, and gateway contract](docs/MCP.md).

## Benchmarking and validation

```sh
skerry bench --path ./bench-store --objects 1000 --bytes 20480 --workers 4
skerry bench --path ./bench-suite --objects 200 --suite
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --locked
```

Benchmarks create isolated stores under `<path>/bench-suite/<run UUID>/`: use a disposable destination. Results are measured logical throughput and wall-clock latency. The suite runs 44 scenarios across SSD/HDD policies: 5/20/32/128 KiB and mixed-size ingestion, sequential/concurrent writes, random single and ordered/completion batch reads, and concurrent mixed reads/writes. It reports index lookup, queue and storage timings, utilization, and cache/dedup counters. CPU/RSS measurements are unavailable. It does not evict the operating-system page cache or claim cache-cold/physical-device measurements.

## Durability assumptions and current limits

A successful PUT follows segment synchronization, atomic index publication, and Fjall journal synchronization. Retrying an interrupted request is safe: an unacknowledged object may already exist. These guarantees depend on the filesystem and device honoring synchronization. Native process-termination tests do not establish power-loss safety. Unix directory synchronization is used for new files/control records; Windows directory-entry durability requires additional filesystem/hardware validation.

The initial version has one volume, no deletion, no replication, no S3/RocksDB adapters, no automatic cache resizing, and no physical-device telemetry. HDD scheduling sorts a bounded window by priority, segment, and offset with aging; it does not merge physical reads through gaps. Ordered batch reads are sequential to guarantee progress without an unbounded reorder buffer. See [acceptance evidence and remaining limits](docs/ACCEPTANCE.md) for platform verification and test scope.
