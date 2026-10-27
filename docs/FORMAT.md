# SkerryKV binary storage format, version 1

All integers are unsigned and little-endian. UUIDs are their 16 RFC 4122 bytes,
not host-endian integers; digests are the 32 raw BLAKE3 bytes. Timestamps are Unix
seconds. CRC is the standard IEEE CRC-32 implemented by `crc32fast`, stored LE.
No Rust struct layout is serialized. There is no record padding or alignment.
All reserved bytes must be zero. Unknown versions/flags fail closed.

Canonical segment path: `segments/<first-two-compact-UUID-hex>/<hyphenated-UUID>.seg`.
The identifier must be UUIDv4. Paths in index metadata use slash separators and
are relative to `segments/`; absolute paths, `..`, and backslashes are rejected.
Objects do not determine file placement.

## Segment header: 64 bytes

| Offset | Size | Meaning |
|---|---|---|
| 0 | 8 | ASCII `SKRYSEG1` |
| 8 | 2 | Format version, 1 |
| 10 | 2 | Header length, 64 |
| 12 | 16 | Segment UUIDv4 |
| 28 | 8 | Creation timestamp |
| 36 | 24 | Reserved, zero |
| 60 | 4 | CRC-32 of bytes `[0,60)` |

## Record: 64-byte header followed immediately by payload

| Offset relative to record | Size | Meaning |
|---|---|---|
| 0 | 8 | ASCII `SKRYREC1` |
| 8 | 2 | Record version, 1 |
| 10 | 2 | Header length, 64 |
| 12 | 8 | Payload byte length |
| 20 | 32 | BLAKE3 of payload |
| 52 | 8 | Reserved, zero |
| 60 | 4 | CRC-32 of header bytes `[0,60)` |
| 64 | payload length | Payload bytes, possibly empty |

Index offsets always point to **payload beginnings**. Subtract 64 to recover the
record header. The next record begins at `payload_offset + payload_length`.
Parsing uses checked arithmetic; a syntactically valid header does not prove
that its payload is in bounds or correct. Verify the payload digest too.

Records have no independent transaction-commit marker. A complete, verified
record establishes physical integrity, not visibility, durability acknowledgment,
or membership in the normal committed prefix. The intact index's committed end
is authoritative during normal recovery. Do not scan past it and promote tails.

## Seal footer: 96 bytes

A sealed file consists of the header, records through `data_end`, then exactly
one footer. Its exact file length is `data_end + 96`; no allocation tail remains.

| Offset relative to footer | Size | Meaning |
|---|---|---|
| 0 | 8 | ASCII `SKRYEND1` |
| 8 | 2 | Footer version, 1 |
| 10 | 2 | Footer length, 96 |
| 12 | 16 | Segment UUID, matching header |
| 28 | 8 | Data end (first footer byte, absolute file offset) |
| 36 | 8 | Physical record count |
| 44 | 8 | Seal timestamp |
| 52 | 32 | BLAKE3 of file bytes `[0,data_end)` |
| 84 | 8 | Reserved, zero |
| 92 | 4 | CRC-32 of footer bytes `[0,92)` |

The segment digest excludes the footer and unused allocation. The footer CRC
protects its metadata, including the digest. Standalone validation reads the
last 96 bytes, checks footer identity/end/CRC, verifies the prefix digest, then
parses the prefix and checks every record digest/count. Sealed payload records
are never modified. A footer with an active index entry is accepted only when
its end/count agree with that index entry and its digest verifies.

## Fjall keyspaces

One Fjall database, with key-value separation explicitly disabled.

`objects`: key = raw 32-byte BLAKE3. Value is exactly 33 bytes:

| Offset | Size | Meaning |
|---|---|---|
| 0 | 1 | Encoding version, 1 |
| 1 | 16 | Segment UUID |
| 17 | 8 | Payload offset |
| 25 | 8 | Payload length |

`segments`: key = raw 16-byte UUID. Value is 85 fixed bytes plus UTF-8 path:

| Offset | Size | Meaning |
|---|---|---|
| 0 | 1 | Metadata encoding version, 1 |
| 1 | 1 | State: 0 active, 1 sealed, 2 quarantined |
| 2 | 2 | Relative path byte length |
| 4 | 8 | File length / logical allocated capacity at publication |
| 12 | 8 | Committed data end, excluding footer |
| 20 | 8 | Physical committed record count |
| 28 | 8 | Payload bytes currently referenced by object index |
| 36 | 8 | Creation timestamp |
| 44 | 8 | Seal timestamp, 0 when absent |
| 52 | 1 | Digest present: 0 or 1 |
| 53 | 32 | Whole-segment digest, zero if absent |
| 85 | variable | Relative UTF-8 segment path |

Live bytes can be less than physical record payload bytes after deduplicating
an import. Empty objects still count as objects. Physical length is distinct
from committed end. Growth factor 2 uses `set_len` geometrically, with no claim
of physical disk reservation. Recovery discards allocation and uncommitted tails
of active files; verification alone does not truncate them.

## Index generations and activation

Generation directories are `index/generation-<UUID>/`. Immutable activation
records are `index/activation-<20-digit-sequence>.json`. Contents are a JSON
object `{version:1,generation:<directory-name>,sequence:<integer>}` encoded as
UTF-8, followed by four LE bytes containing CRC-32 of exactly the JSON bytes.
No trailing newline is required. `.activation-*.tmp` files are unpublished.

Activation synchronizes the candidate database, closes handles, writes and
synchronizes a new temporary activation file, then renames it to a new,
previously nonexistent activation filename and synchronizes the containing
directory where supported. No existing generation is renamed or replaced.
The highest activation filename selects the generation. An invalid newest
record or missing selected generation fails closed. Never automatically select
an older generation: it may omit acknowledged writes. Old generations are
retained for operator recovery, not automatic rollback.

## Recovery, rebuilding, and import

Normal startup obtains the store lock and validates all indexed committed data
and every object mapping. Truncation, corrupt payloads, missing referenced files,
and inconsistent metadata are errors. Active trailing bytes are excluded and
truncated. A fully synchronized, valid interrupted seal is reconciled. Unknown
unreferenced `.seg` files remain untouched with a warning; `verify` reports them.
Unpublished `.import-*.tmp` files are removed during normal recovery.

Strict disaster rebuilding includes validated sealed files only and reports
excluded active files. `--salvage` additionally includes verified active prefixes,
possibly containing unacknowledged writes. Incomplete trailing headers/payloads
and all-zero growth capacity are excluded; malformed headers or digest failures
are errors rather than silently skipped. At most one active file can be salvaged
in this single-volume implementation. Rebuilding creates a new generation and
retains the previous one. Deterministic duplicate winner: lexicographic canonical
segment path, then increasing record offset. Duplicate payloads remain physically
present, with one hash mapping.

Import accepts an offline store root, its `segments` directory, or an export
directory of independent sealed `.seg` files. Export nesting is limited to 16
levels; symlinks are rejected. File names must contain the exact UUID. Active
files are excluded and reported. Payloads are copied, synchronized, independently
verified, and installed before index publication. A UUID conflict requires exact
file equality, checked with bounded buffers; differing footer metadata also
constitutes a conflict. Existing object-hash mappings win.

Index publication for large imports uses bounded batches, each atomically
publishing object mappings with the segment's updated live-byte metadata. A
crash can leave a partially registered import. Retry verifies file identity and
registers the remaining objects, so it is safe and idempotent. Imported file data
is durable before any index publication. Orphan installed files are retained
and can be registered on retry. Source payload files are never modified.

## Synchronization assumptions

Successful PUTs synchronize payload data and file metadata before atomically
publishing object locations and segment committed end, then synchronizing Fjall
with `PersistMode::SyncAll` before acknowledgment. Previously acknowledged writes
must survive process termination. Unacknowledged writes may survive too. An I/O
or persistence failure faults the writer until restart; validated reads can
continue, including objects from an ambiguous commit, without granting a durable
PUT acknowledgment.

Unix directory entries are explicitly synchronized. Windows uses standard Rust
file `sync_all` (FlushFileBuffers); this implementation does not issue a native
directory-handle flush. New-file/activation metadata behavior under actual power
loss on NTFS remains an explicit filesystem-dependent limitation requiring native
hardware validation. Neither process-kill tests nor successful flush calls prove
protection against storage firmware that lies about durable completion.

## Golden header vectors

Segment header for UUID `00112233-4455-4677-8899-aabbccddeeff`, creation timestamp
`0x0102030405060708`, as hexadecimal:

```
534b5259534547310100400000112233445546778899aabbccddeeff0807060504030201
000000000000000000000000000000000000000000000000a2f8fd80
```

Record header for the three payload bytes `abc`:

```
534b5259524543310100400003000000000000006437b3ac38465133ffb63b75273a8d
b548c558465d79db03fd359c6cd5bd9d850000000000000000dde8f237
```

Line breaks above are presentation only. Both vectors decode to 64 bytes.
Unit tests compare the exact encoded bytes against these independent vectors.
