# HTTP and batch protocol v1

All routes begin with `/v1`. The default listener is `127.0.0.1:7788`.
`SKERRY_TOKEN`, when configured, protects every route except health, including
local routes. Send `Authorization: Bearer <token>`. Non-loopback listeners refuse
to start without a token. Tokens are never accepted in query parameters. Use TLS
at a trusted reverse proxy for remote traffic; forwarded headers do not bypass
authentication. Docker's internal wildcard listener also requires a token,
regardless of host port publication.

## Single objects and control

- `PUT /objects`: raw `application/octet-stream` bytes, including empty objects.
  Returns JSON `{ "hash": "<64 lowercase hex digits>", "duplicate": false }`.
  Successful responses mean durable completion, including duplicate requests.
- `GET /objects/{hash}`: raw `application/octet-stream` bytes or HTTP 404.
- `HEAD /objects/{hash}`: HTTP 200 if indexed, otherwise 404; no payload read.
- `DELETE /objects/{hash}`: idempotent HTTP 204, including for an absent hash. Requires an explicitly configured `SKERRY_TOKEN` service administrator credential, even on loopback; MCP scoped credentials cannot authorize it. A v1 store returns HTTP 409 with offline upgrade instructions.
- Successful object responses include `X-Skerry-Hash` with the canonical digest.
- `GET /health`: JSON health/version; unhealthy states return HTTP 503.
- `GET /status`: measured storage, I/O, cache, service status.
- `GET /metrics`: Prometheus text exposition.
- `GET /cache/stats`: measured payload cache counters.
- `POST /admin/cache-clear`: evict resident cache entries; active responses may
  still hold shared buffers. Returns `{ "cleared": true }`.
- `POST /admin/gc?force=true`: run a collection pass; `force` defaults to false. Requires an explicitly configured service administrator credential. A v1 store returns HTTP 409 with offline upgrade instructions.
- `POST /admin/shutdown`: HTTP 202; stops new admission and drains accepted work.

Malformed hashes return 400, wrong upload media types 415, oversized single
objects 413, admission exhaustion 503, and storage failures 500. Error responses
are JSON and never contain tokens. Uploads have a 30-second idle timeout between
body chunks. Global request capacity remains charged until the response is
consumed or cancelled. Accepted TCP connections, including slow-header and idle
keep-alive connections, are independently capped at twice `resources.max_requests`;
capacity is reserved before accepting each socket and held through its lifetime.
Additional connection attempts remain subject to the operating system's bounded
listen backlog. Upload memory is reserved before body buffering.

## Batch framing

`POST /objects/batch-put` and `POST /objects/batch-get` use
`Content-Type: application/vnd.skerry.batch-v1` for request and response.
HTTP transfer chunks have no protocol significance. The following fields use
little-endian integer encoding and no implicit padding:

| Offset from frame beginning | Width | Field |
| --- | --- | --- |
| 0 | 4 | Frame length, excluding this prefix |
| 4 | 4 | ASCII `SKB1` (magic and protocol version) |
| 8 | 1 | Kind |
| 9 | 1 | Status |
| 10 | 2 | Reserved flags, zero |
| 12 | 8 | Request ordinal |
| 20 | 32 | Raw BLAKE3 digest |
| 52 | 8 | Payload length |
| 60 | variable | Payload |

The fixed header after the prefix is 56 bytes. Frame length must equal
`56 + payload_length`. Its maximum is `56 + max_object_bytes`; the server rejects
lengths before allocating payload buffers. GET request frames always have frame
length 56 and payload length zero. PUT requests must have an all-zero digest;
the service computes and returns the canonical identity. Empty payloads are valid.
Request status and flags must be zero. Ordinals are client-selected correlation
identifiers, preferably unique within a batch; `2^64-1` is reserved for terminal
stream errors. Response ordering means original input order, not numeric sorting
of ordinals.

| Kind | Value | Fields |
| --- | --- | --- |
| PUT request | 1 | Ordinal, zero digest, payload |
| GET request | 2 | Ordinal, requested digest, no payload |
| PUT result | 129 | Ordinal, canonical digest on success, no payload |
| GET result | 130 | Ordinal, requested digest, payload on success |
| Terminal stream error | 255 | Reserved ordinal, zero digest, no payload |

| Status | Value |
| --- | --- |
| Success | 0 |
| Not found | 1 |
| Integrity/read failure | 2 |
| Invalid protocol input | 3 |
| Object too large (reserved) | 4 |
| Resource/item limit exceeded | 5 |
| Storage/write failure | 6 |
| Shutting down (reserved) | 7 |

The PUT-result failure digest is zero. GET errors retain the requested digest.
Status 2 reports retrieval failure conservatively; detailed storage diagnostics
are in server logs. No internal error messages are embedded in binary payloads.

Request EOF at a frame boundary ends input. Response EOF ends output; a transport
failure or missing response leaves that item's outcome unknown. Retrying PUT is
safe because identity is content-addressed. A complete frame may commit before a
connection fails, even if no response reaches the client.

Batch PUT has per-item durability and partial success; it is not all-or-nothing.
Each valid item commits before its successful result is emitted. A malformed
frame stops processing and normally emits a terminal status 3 frame when the
connection remains usable. Previously committed items remain intact. Batch PUT
pipelines a bounded number of items, so later valid items may already be accepted
when a storage failure is observed. The server stops accepting new
items and, while the connection remains usable, emits ordered results for all
items already queued. A storage failure reports item status 6 (or status 5 for
resource exhaustion). A missing result leaves the item's outcome unknown; PUT
may safely be retried. A batch item limit violation normally emits terminal
status 5. A write failure takes precedence over a pending terminal stream error.
The configured item count limit applies to every stream; streaming avoids
accumulating a whole batch in memory.

Batch GET defaults to ordered responses and dispatches one item at a time,
which bounds reordering memory and guarantees progress. `?ordered=false` permits
completion-order responses, with at most `min(read_worker_count,32)` outstanding
reads. Completed payloads are emitted as separate body chunks without copying
into a combined frame buffer. Reader memory remains charged while a response
holds the payload. On malformed input, already accepted reads finish before the
terminal error frame. Single and batch GET accept
`priority=interactive|normal|bulk` (normal by default).

Clients streaming request bodies should concurrently consume the response body;
otherwise transport flow control may stall both peers. A client may also submit
a finite bounded request before reading its response. Backpressure applies to
both directions.

### Golden vector

A PUT request with ordinal 7 and payload ASCII `abc` begins with these 20 bytes:

```
3b000000 534b4231 01000000 0700000000000000
```

They are followed by 32 zero digest bytes, payload length
`0300000000000000`, then `616263`. Total size is 63 bytes. Unit tests decode the
same vector fragmented into one-byte transport chunks and reject truncated or
inconsistent lengths.

## Shutdown

Shutdown cancels new admission, stops accepting connections, and allows accepted
HTTP streams up to the configured shutdown timeout (30 seconds by default) to
drain. Expiry force-cancels accepted socket I/O and allows up to two further
seconds for connection teardown. The durable writer then drains and synchronizes.
Timeout or final persistence errors produce a nonzero service exit.
A client that disconnects or times out must treat a missing write acknowledgment
as an unknown outcome, never as proof that no write occurred.
