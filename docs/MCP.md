# Model Context Protocol

SkerryKV exposes the official Rust MCP SDK (`rmcp` 3.8.0) at `/mcp` in its existing Axum service. It shares the live Fjall index, durable writer, verified reads, deduplication, cache, descriptor pool, and resource budgets. The stdio adapter is a network client of that service: it never opens a second store or obtains its lock.

The SDK supports protocol revisions `2024-11-05`, `2025-03-26`, `2025-06-18`, `2025-11-25`, and `2026-07-28`. SkerryKV uses stateless Streamable HTTP with unary JSON responses, supporting the older initialize handshake as well as the 2026 per-request discovery lifecycle. It does not keep resumable server sessions or a legacy HTTP+SSE endpoint. Stdio supports both modern and older initialization clients.

## Local stdio setup

Start the service first:

```sh
cargo run -- serve --config config.toml
# Another terminal:
cargo run -- mcp status --connect http://127.0.0.1:7788
cargo run -- mcp stdio --connect http://127.0.0.1:7788
```

The last command is intended to be launched by an MCP client, which owns stdin/stdout. Diagnostics go to stderr; stdout contains only newline-delimited MCP JSON-RPC. Use the built native executable for client configurations, avoiding Cargo startup output:

```sh
cargo build --release --locked
```

Example for a client supporting an `mcpServers` JSON configuration (replace the executable with its actual absolute path):

```json
{
  "mcpServers": {
    "skerrykv": {
      "command": "/absolute/path/to/skerry",
      "args": ["mcp", "stdio", "--connect", "http://127.0.0.1:7788"],
      "env": { "SKERRY_MCP_TOKEN": "<read credential supplied securely>" }
    }
  }
}
```

Omit `env` for a loopback service with no configured credentials. On Windows use a JSON-escaped path such as `C:\\Tools\\skerry.exe`; no shell, WSL, or Docker is needed. The service must already be running. A custom configured endpoint can be selected with `--http-path /custom_mcp`. The bridge accepts plain HTTP only for literal loopback addresses or localhost; remote URLs require HTTPS and must contain no credentials, query, or fragment. Credentials come from `SKERRY_MCP_TOKEN`, falling back to `SKERRY_TOKEN`.

The stdio bridge's fixed safety limits are 1 MiB per input frame, 16 outstanding requests retained through response delivery, and 512 KiB encoded output. A stricter HTTP service limit still applies. Raw binary content travels as base64 per MCP, not through the native binary batch protocol.

## Tools and resources

| Tool | Behavior | Permission |
|---|---|---|
| `store_status()` | Health, uptime, capacity, objects, segments, I/O, queues, cache and MCP telemetry; logs/path-bearing errors excluded | read |
| `list_segments(cursor?, limit?)` | At most 100 entries by default, opaque validated cursor; no filesystem paths | read |
| `segment_info(segment_id)` | State, logical file size, committed bytes, record count, live payloads, utilization, seal digest, known verification result | read |
| `object_info(hash)` | Validated full BLAKE3 digest, existence, length and non-sensitive UUID/offset metadata | read |
| `get_object(hash)` | Complete verified payload in `data_base64` when small; explicit metadata/HTTP instructions otherwise | read |
| `put_object(data_base64)` | Strict base64 decoding, normal durable PUT, canonical hash, size and duplicate status | write and `read_only=false` |
| `verify_segment(segment_id)` | Start bounded integrity verification and return `job_id` | maintenance and `allow_verification=true` |
| `verification_status(job_id)` | Read your job's progress, completion/failure/cancellation and report | read, same principal that started job |

Read tools use `readOnlyHint=true`; all operations use `openWorldHint=false`. PUT is additive (`destructiveHint=false`) and content-idempotent. Verification is non-destructive but starts ephemeral job state, so it advertises `readOnlyHint=false`, `destructiveHint=false`, `idempotentHint=false`. These annotations never substitute for permission checks.

The `skerry://objects/{hash}` resource template returns small verified objects using MCP binary `blob` contents and MIME type `application/octet-stream`. `resources/list` stays empty: nothing enumerates or preloads all stored objects. Missing resources and oversized resources return explicit errors; oversized `get_object` returns metadata and a relative `/v1/objects/<hash>` path. No operation silently truncates bytes. Scoped read credentials also work on native GET/HEAD and batch-get, so large retrieval does not require an administrator credential.

Segment pages follow arbitrary UUID/index order, not creation time. Pagination is not a point-in-time snapshot: inserts before an existing cursor require a new traversal. Segment `object_count` counts physical records, including retained redundant imported records; `live_payload_bytes` counts indexed live contents. Utilization is committed bytes divided by configured segment capacity and can exceed 100% for an imported segment larger than the current capacity.

Verification scans a snapshot of the committed prefix, checking header CRC, record boundaries, payload BLAKE3, counts, and, when sealed at snapshot time, the complete seal digest/footer. Active appends after that snapshot are outside its report. It uses a fixed 64 KiB buffer, shared read-memory/request permits, and the existing descriptor cache. Progress is actual bytes checked. The latest bounded verification result is visible in `segment_info`; job IDs/results are process-local, expire through retention, and are not persisted as a repair journal. A verification error never repairs, truncates, quarantines, or replaces storage. Jobs cooperate with deadlines/shutdown between reads; a blocked OS syscall cannot be forcibly interrupted, and its permits remain charged until it exits.

## Local authentication and least privilege

MCP is read-only by default. With no credentials, anonymous loopback access has read permission only. Configuring any scoped credential requires authentication for MCP and native object routes. Set secrets in the server environment or your deployment secret manager, never committed TOML:

| Environment variable | Scope |
|---|---|
| `SKERRY_TOKEN` | Existing native administrator; grants only read through MCP |
| `SKERRY_MCP_READ_TOKEN` | MCP read and native object retrieval |
| `SKERRY_MCP_WRITE_TOKEN` | Read + write; MCP PUT additionally requires `read_only=false` |
| `SKERRY_MCP_MAINTENANCE_TOKEN` | Read + verification; verification additionally requires `allow_verification=true` |
| `SKERRY_MCP_GATEWAY_TOKEN` | Trusted gateway-to-backend credential; not a public end-user credential |

Different scopes must use distinct nonempty credentials. Scope headers from ordinary clients are ignored. Scoped credentials cannot delete objects, stop the service, clear caches, rebuild indexes, or retrieve the native administrator status/log output. No MCP delete tool is provided. Native write scope permits raw uploads; `mcp.read_only` controls MCP tool writes specifically. Retain a separate `SKERRY_TOKEN` if using native administrative commands. Credentials are compared without content-dependent comparisons, never accepted in URLs, and never returned by tools.

For an explicitly writable local service:

```toml
[mcp]
enabled = true
read_only = false
allow_verification = true
```

Configure independent read, write, maintenance, and administrator secrets in the environment. Put the appropriate scoped token in your MCP client's secret configuration. Retrying a timed-out or disconnected PUT is safe: the durable write might already have completed, and its content hash deduplicates the retry. Changing adapter permissions never weakens the existing durable acknowledgment contract.

## Remote Streamable HTTP and authorization gateway

Static local tokens are **not** claimed to implement the public MCP OAuth authorization specification. Non-loopback MCP listeners refuse startup unless an authentication gateway and HTTPS external URL are explicitly configured. SkerryKV does not bundle an OAuth authorization server or JWT issuer.

A compatible external gateway is required to:

1. Terminate HTTPS and expose the configured canonical MCP resource URL.
2. Implement MCP-compatible OAuth 2.1 authorization, protected-resource metadata discovery, authorization-server metadata, appropriate client registration, authorization-code/PKCE flows, resource/audience binding, expiry, and scope validation. Supply public `WWW-Authenticate` challenges/discovery responses; do not forward raw failed backend authentication as an OAuth flow.
3. Validate public access tokens for this resource. Never pass arbitrary end-user tokens through as backend credentials.
4. Strip client-supplied `X-Skerry-Mcp-*` headers. Replace backend `Authorization` with its private gateway token, and inject only validated `X-Skerry-Mcp-Subject` and `X-Skerry-Mcp-Scopes` values. The subject must be stable per principal, nonempty graphical ASCII up to 128 bytes. Scopes are whitespace-delimited `read`, `write`, and/or `maintenance`; `read` is required.
5. Restrict backend access to the gateway using a private network/firewall. The static gateway token is a backend trust credential, not a public MCP authentication mechanism. Do not expose native administrative routes through the public gateway.
6. Preserve the MCP protocol/content/accept headers and Host authority appropriately. Validate public Host, origin and request size/rate policies too. If browser clients are supported, serve approved-origin CORS preflights and expose the required MCP headers at the gateway; the backend does not add permissive CORS headers.

Example backend settings:

```toml
[server]
listen = "127.0.0.1:7788"

[mcp]
enabled = true
auth_gateway = true
external_url = "https://mcp.example.com/mcp"
read_only = true
allowed_origins = ["https://approved-client.example"]
```

Set `SKERRY_MCP_GATEWAY_TOKEN` privately on the server and gateway. For a non-loopback backend listener also set `SKERRY_TOKEN` to protect the native API. The gateway forwards `/mcp` and its public OAuth metadata endpoints according to its own configuration. Clients connect to `https://mcp.example.com/mcp` and obtain tokens through that gateway's standards-compatible flow. Gateway-mode requests require attested read scope even on loopback; the normal local stdio static-token bridge is not an OAuth login client for gateway mode. Use a native remote MCP-capable OAuth client instead, or a compatible externally managed credential bridge.

If external agents also need large native downloads, explicitly publish only authenticated GET/HEAD object routes through the gateway. Validate public read scope there, then translate to a dedicated internal `SKERRY_MCP_READ_TOKEN`; do not expose the administrator token or unrestricted native routes. This additional gateway routing is deployment-specific.

Every present HTTP `Origin` must match `allowed_origins` by exact scheme/host/port. Empty means reject all present Origin values. Non-browser MCP clients may omit Origin. `null`, wildcards, and malformed/duplicate origins are rejected. SDK Host validation additionally restricts local hosts and the configured external hostname. Forwarded headers never automatically enable gateway trust or HTTPS claims.

Docker disables MCP by default (`SKERRY_MCP_ENABLED=false`), preserving the authenticated native service with loopback host publication. To enable remote MCP in Docker, mount a gateway-mode config, set its required secrets, explicitly set `SKERRY_MCP_ENABLED=true`, and place the backend on a private gateway network. Merely toggling the environment flag on the default wildcard listener fails closed; it does not expose unauthenticated MCP.

## Bounds and telemetry

All MCP settings are optional and validated:

| Setting under `[mcp]` | Default |
|---|---:|
| `enabled`, `http_path`, `read_only` | `true`, `/mcp`, `true` |
| `allow_verification` | `false` |
| `max_inline_object_kib`, `max_put_object_kib` | 128 each |
| `max_list_page_size` | 100 |
| `max_concurrent_requests` | 16 |
| `max_request_kib` | 256 |
| `max_encoded_response_kib` | 512 |
| `max_response_memory_mib` | 16 |
| `max_verification_jobs`, `max_retained_jobs` | 1, 64 |
| `tool_timeout_seconds`, `verification_timeout_seconds` | 30, 300 |
| `requests_per_minute` | 120 per authenticated identity/token |
| `auth_gateway`, `external_url` | false, unset |
| `allowed_origins` | empty (deny present origins) |

`SKERRY_MCP_ENABLED` overrides the enabled flag. Inline/PUT limits are at most 1 MiB and cannot exceed the store object limit; list limits are at most 1000. Encoded/request sizes must accommodate the base64 and JSON envelopes. HTTP initialization/discovery/status requests also count toward the MCP rate limit. Identity rate history is capped at 1024 active identities and expires after a minute; clients above that identity limit are rejected rather than creating unbounded bookkeeping.

HTTP MCP calls consume the same global admission pool as native requests. Read/write calls and inspection workers use the same coordinator pools; cancelled inspection waiters do not release capacity while their blocking workers remain active. The adapter adds bounded concurrency, request size, encoded-response reservations retained through actual response lifetime, and job limits. Full capacity returns retryable 503 or an MCP resource error; rate exhaustion returns 429. The response budget covers encoded buffers/reservations, not total process RSS or every temporary SDK/base64 allocation; those are additionally bounded by request size/concurrency. No unrestricted filesystem operations, shell commands, URL fetches, shutdown, or index replacement are tools.

`skerry mcp status` reports actual MCP counters, also available at authenticated `/v1/mcp/status`, nested in `/v1/status`, in Prometheus metrics, and in the dashboard's MCP tab. `active_connections` is measured active MCP HTTP exchanges (including retained response bodies), **not** a count of persistent TCP sockets or resumable sessions. Tool calls/sec is derived from counter deltas; average tool latency and failures count dispatched tool handler calls, while protocol/schema failures can occur before dispatch. Dependency diagnostics that could contain payloads, tokens, peer notifications, URLs or index paths are suppressed even under verbose `RUST_LOG` overrides.

## Verification and known limits

Tests exercise official SDK HTTP and child-process stdio clients, legacy initialization, annotation discovery, binary round trips, deduplication, default/read/write/maintenance policies, gateway attestation, origin/body/rate/response limits, shared native budgets, online verification, cursor errors, integrity failures, mixed ingestion, and diagnostic redaction. Windows-compatible process invocation uses exact executable paths and argument arrays; native Windows/Linux execution is delegated to the existing CI runners and is not claimed from local macOS tests.

There is no semantic search, eager object enumeration, resumable MCP session store, OAuth issuer, automatic gateway installation, or public remote static-token compatibility claim. Completed verification records are ephemeral and bounded. The stdio bridge forwards the backend's tool/resource surfaces only and enforces its fixed local transport limits; it does not implement a second storage engine.

Sources: [official Rust SDK](https://github.com/modelcontextprotocol/rust-sdk), [MCP transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports), [MCP authorization](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization).
