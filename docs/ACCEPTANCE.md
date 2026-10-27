# Implementation and verification evidence

This matrix distinguishes implemented behavior from execution evidence. Local validation is on macOS ARM64; CI configuration is not evidence that remote jobs have run.

| Area | Implementation | Evidence |
|---|---|---|
| Native executable | Single Rust package, `skerry` binary; locked dependency graph | Local debug/release builds, CLI integration tests |
| Segments and Fjall | Versioned compact metadata, two keyspaces, no value separation; UUIDv4 files with checksummed headers and sealed footers | Golden format vectors, roundtrip, corruption, rotation, restart tests |
| Durable acknowledgments | Segment sync → atomic cross-keyspace batch → index sync → success | Native subprocess termination and seven write-boundary abort tests |
| Recovery | Indexed committed end, tail discard, interrupted seal reconciliation, fail-closed committed corruption | Truncation/header/payload/footer corruption, incomplete append, sealing abort tests |
| Exclusive ownership | Same lock for service and offline maintenance | Concurrent open/maintenance refusal tests |
| Reads and writes | Bounded writer batching, concurrent positional reads, duplicate suppression | Different/identical concurrent inserts, reads during ingestion, queue/cache tests |
| Read scheduling | SSD immediate; HDD bounded accumulation, physical ordering and priority aging | HDD coordinator/coalescing tests and benchmark scenarios; physical read merging is not implemented |
| Resource management | Ingress/write/read/cache byte budgets, item/admission caps, bounded descriptors and waiters | Retained-response exhaustion/recovery, ingress admission, file-descriptor cap, cancellation tests |
| Payload caching | Byte/item bounded second-hit SLRU, no TTL for immutable payloads, periodic admission-history maintenance | Cache admission, retained-reference accounting, hits/clear tests |
| HTTP | Versioned raw/binary endpoints, strict framing, per-item status, partial batch success | Router and actual TCP tests including duplex progression and stalled-upload drain |
| Authentication | Token for non-loopback bind; configured token required except health; constant-time comparison | Non-loopback startup rejection, protected control routes, query-token rejection tests |
| Graceful shutdown | Stop admission, bounded HTTP drain, force-close stalled sockets, drain/sync/seal writer, join readers | TCP drain, process exit, cancellation-resume/idempotence tests |
| Rebuild | Fresh generations and checksummed activation; sealed-only default, explicit active salvage | Strict/salvage rebuild, corrupt activation, before/after activation abort tests |
| Import | Copy/verify/sync/publish, bounded chunks, deterministic existing mapping wins, full UUID collision checks | Duplicate/import/idempotency/export/footer collision and interrupted-import tests |
| CLI and dashboard | All operational commands, five monitoring panels, rates, errors/events, confirmed shutdown, attach/embedded modes | CLI tests; Unix pseudo-terminal attach/embedded detach/restoration tests; local development startup smoke |
| Development scripts | POSIX shell and PowerShell; tool checks, default config, headless/TUI modes | Local POSIX startup/status/shutdown smoke, shell syntax check; PowerShell unexecuted locally |
| Docker | Multistage non-root image, persistent volume, loopback host publication, token requirement, actual health probe | Configuration supplied; daemon unavailable locally, build/runtime unverified |
| Windows/Linux | Platform positional I/O, native locking, CI jobs | Native CI configured; neither Windows nor Linux execution verified locally |
| Linux ARM64 | Native ARM runner job and release archive | CI configured; unexecuted locally |
| Benchmarks | Realistic sizes, sequential/concurrent ingestion, random and batch reads, policies and mixed load | 44-scenario local suite smoke (8 objects per workload, 2 workers); no hardware performance conclusions |
| Packaging | Native archives include executable/config; Docker smoke CI | Workflow supplied; remote artifacts not generated locally |

## Limits and deferred optimizations

- One storage volume and at most one active segment during salvage. Default rebuild may exclude acknowledged data from a lost active index; salvage is explicit because it may recover unacknowledged records.
- Ordered batch reads deliberately dispatch sequentially; completion-order reads have a bounded window. No separate reorder buffer is necessary.
- HDD scheduling does not coalesce adjacent or nearby physical operations. Priorities are best effort with aging; they are not hard device-I/O deadlines. A coalesced request shares the priority of the original operation.
- There is no automatic cache memory-pressure adaptation, speculative prefetch, or location cache. Fixed absolute budgets work in containers. General process memory includes additional index memtables, protocol buffers, metadata, and allocator overhead.
- Metrics are logical operations/bytes and wall-clock timings, not physical storage-device I/O. Cache hits cannot establish physical disk reads avoided. Latency samples are bounded and session-scoped. CPU/RSS and reliably cold-cache measurements are reported unavailable.
- Imports can become partially visible across bounded durable batches; interruption is safe to retry. Source stores must be offline; independent export bundles must remain immutable throughout copying.
- Older index generations and unreferenced final segment files are retained. Startup cleans abandoned import staging files. Automatic garbage collection and deletion are absent.
- The TUI logs panel contains bounded coordinator events/errors. General tracing remains available in the service log output (embedded TUI directs it to a file).
- Windows directory-entry power-loss behavior has not been validated. No process-kill test establishes safety against power loss, OS failure, lying drive caches, or unsupported network filesystem semantics. Use local filesystems with reliable locking/synchronization, backups, and appropriate hardware validation.

## Initial MVP validation (before MCP upgrade)

On macOS ARM64 with Rust/Cargo 1.94.1:

- `cargo fmt --all -- --check`: passed.
- `cargo clippy --all-targets --all-features -- -D warnings`: passed.
- `cargo test --all-targets --locked`: 45 tests passed, zero failures (9 library, 2 CLI, 4 coordinator, 10 HTTP, 7 subprocess/terminal, 13 storage).
- `cargo build --release --locked`: passed.
- POSIX development script: service startup, real status query, graceful stop and clean process exit passed. `sh -n scripts/dev.sh` passed.
- Benchmark suite: 44 isolated scenarios with 8 objects per workload and 2 requested workers passed. This is functionality validation, not a hardware benchmark recommendation.
- Native release smoke: initialization, foreground service, binary PUT/GET, deduplication, metrics, graceful shutdown, verification, rebuilding, sealed import, and a small benchmark passed.

Docker CLI was present but its daemon socket was unavailable; Docker build/runtime was not executed. PowerShell, Windows/NTFS, Linux, and remote CI execution remain unverified here.


## MCP upgrade validation

MCP is implemented using the official Rust SDK `rmcp` 3.8.0, with native HTTP and network-proxy stdio transports. New modules are `mcp`, `mcp_http`, `mcp_stdio`, and `inspection`. Existing engine formats/durability ordering remain the storage authority; no separate database or writer was introduced.

Validation on macOS ARM64:

- Formatting and strict all-target/all-feature Clippy passed.
- The complete test suite passed: 75 tests (12 library, 2 CLI, 4 coordinator, 10 native HTTP, 4 inspection, 18 MCP HTTP/security/process, 5 stdio, 7 existing subprocess/terminal, 13 storage).
- Both legacy initialize/tool-discovery and modern official-SDK client exchanges passed for HTTP and stdio.
- Authorized stdio PUT/retry deduplicates to one stored object; read scope cannot write. The service owns its store lock throughout bridge operation.
- An acknowledged MCP PUT survived forced native subprocess termination and was readable after restart. This remains a process-crash test, not power-loss evidence.
- Tests cover gateway scope attestation, default read-only policy, native least-privilege large downloads, integrity errors, strict pagination, asynchronous verification jobs, encoded response lifetime, rate/concurrency/global budgets, and log redaction even with an explicit SDK trace directive.
- A release executable smoke passed HTTP/stdio initialization/discovery, scoped durable PUT/deduplication, tool/binary-resource/native roundtrips, verification progress and shared result, actual MCP status, and graceful service shutdown.

Windows/Linux process invocation uses native argument arrays and the existing CI jobs run these tests. They were not executed locally. Docker configuration now disables MCP by default and includes CMake for SDK TLS source builds; Docker execution still requires an available daemon. A production OAuth gateway is a documented integration contract, not a bundled or externally validated deployment. See [MCP deployment and limits](MCP.md).
