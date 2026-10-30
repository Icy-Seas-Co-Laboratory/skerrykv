# Multiple writers: design recorded before 0.4.0

This document preserves the design and its 0.3.0 baseline. The implementation and current configuration are described in [Append lanes in 0.4.0](MULTI_WRITER.md).

SkerryKV 0.3.0 accepts many simultaneous clients but uses one buffered-write coordinator and one active append writer per store. Large uploads already stage payloads concurrently; publication is serialized. Starting more processes against the same root is rejected by the exclusive store lock.

The 256 UUID-prefix segment directories and the payload-cache shards are independent layout/cache mechanisms. They are not independent write engines. Adding coordinator tasks alone would still contend on `Store.writer`, the journal, and index persistence.

At the time of this design, the change was not implemented. The 64-client benchmark below measures the prior single-writer architecture.

## 64-client evidence

The [64-client results](HTTP_64_WORKER_RESULTS.md) compare 0.2.2 and 0.3.0 with three ten-second repetitions per access pattern. On 0.3.0, single PUTs reached 2,278/s, batch writes 4,964 objects/s, and mixed traffic 7,944 ops/s. Random and hot reads reached about 51,588/s and 54,906/s. All candidate core acceptance budgets passed, with zero rejected/unknown items and all 750 status plus 750 metrics probes available.

Batch throughput remains near the prior 32-client result, while successful batch-request p99 is about 3.34 seconds at 64 clients. The historical 32-client study used shorter windows, so it is context rather than a matched scaling comparison. More client concurrency is not evidence of more durable writer throughput. The matched 0.2.2 baseline lost 99.3% of detailed monitoring samples during batch load and recorded 132,250 rejected plus 23,000 unknown batch items across its three repetitions; its study-completion flag was not an availability acceptance contract.

The 64-client cache sweep still reached 100% measured hits when its 512 MiB working set fit, but did not improve throughput at that count. Its one-repetition results do not establish a cache-size performance winner. Native 16 MiB streaming achieved 291 MiB/s PUT and 6,010 MiB/s repeated warmed GET; the latter is not cold-disk bandwidth. Streaming used two upload slots and four read slots, with a 4 GiB logical write cap to accommodate the larger offered queue. These studies do not test a multi-writer implementation.

## Recommended first implementation

Start with an opt-in **2 or 4 append lanes feeding one durable group-commit sequencer**. Keep a single logical hash namespace, one journal authority, and one index. Retain one lane as the default until crash and performance tests justify a change.

1. Compute the canonical hash once and route buffered PUTs and DELETEs to one of 256 stable virtual shards using the hash prefix. Preserve FIFO mutation order within each shard. A hot hash remains serialized; unrelated hashes can advance concurrently.
2. Assign virtual shards to append lanes deterministically. Within a lane, round-robin ready shard queues, with bounded records, bytes, and batching delay per turn. Check for administrative shutdown and maintenance barriers between batches. Do not pick the globally shortest queue for individual operations: that could send one hash to competing writers.
3. Each lane owns its active segment, append position, and append/sync work. No two lanes append to the same file. Data writes and data synchronization can overlap across lanes.
4. After payload synchronization, submit immutable mutation groups to one commit sequencer. It combines ready lane groups under bounded byte/record/delay limits, synchronizes the journal and witness, applies/persists the index, and only then acknowledges callers. It must also serialize stream publication, deletion, and maintenance publication.
5. Keep upload bytes, pending-write bytes, and request permits global. More lanes must not multiply memory allowances or queue capacity. Expose per-lane queued bytes/items, oldest wait, active shard count, append/sync time, and global commit-wait counters. Administrative admission stays independent.

This overlaps payload I/O with other lanes and with publication. It does **not** make journal/index commits independent. If publication dominates, grouping ready groups from several lanes matters more than adding more lanes. Existing commit-path timing counters should guide that choice. Do not infer device saturation or a guaranteed speedup from a throughput plateau alone.

## Correctness work required

- **Recovery:** startup currently assumes at most one active segment. It must recover every lane's committed end, truncate each unacknowledged tail, and preserve acknowledged data. Persist lane ownership/configuration generation, or define an equally explicit recovery rule. Changing lane count must drain/seal/reassign active state safely.
- **Compatibility:** the record encoding need not change merely to allow several active files, but current engines/tools do not support that recovery invariant. Define durable feature/version negotiation and rejection by older binaries before enabling multiple active segments. Do not silently create a store an older tool could mishandle.
- **Duplicates and deletion:** all same-hash mutations need one ordering path. Duplicate probes cannot bypass an in-flight deletion fence or certify an index entry after a failed publication. Streamed uploads join that path once their hash is known; concurrent upload/delete ordering must have a documented linearization point.
- **Publication:** do not publish one lane's in-memory append end if it includes a later unsynchronized batch. The first version should allow at most one unpublished batch per lane. Preserve the existing data → journal/witness → index → acknowledgment ordering.
- **GC:** every lane's active and prepared segments must be excluded from source selection. Continue conditional relocation against the current location, with reader leases and deletion fences intact.
- **Checkpoints:** capture a consistent published namespace/journal prefix under the commit barrier. Unpublished lane tails are not snapshot contents. Activation must fence publication, validate authority, and retain a bounded final catch-up.
- **Shutdown/cancellation:** close admission, drain lane append work into the still-running sequencer, finish durable acknowledgments, seal/sync every lane, then join the sequencer. A cancelled caller cannot release ownership of unfinished blocking I/O.
- **Failures:** a clean disk-reserve rejection remains retryable without poisoning the writer. Uncertain journal/index durability must conservatively fault the global publication path; other lanes cannot certify new acknowledgments past that failure.

## Delivery and acceptance

Implement in three reviewable steps: (1) separate append preparation from publication while retaining one lane; (2) add lane recovery/compatibility and the bounded shard scheduler behind an opt-in setting; (3) benchmark lane counts 1/2/4/8 with client counts 4/8/16/32/64 on the same hardware.

Required regressions include same-key PUT/DELETE/PUT interleavings, duplicate races, shard skew/starvation, simultaneous lane rollover, low-space fanout, shutdown during append and publication, GC/checkpoint races, and subprocess termination around each lane's data sync and the shared commit boundaries. Reopen, verify, and rebuild must preserve every acknowledged mutation and deletion fence.

Benchmark small unique writes, Pelagia batches, read/write mixes, and large transfers. Record per-phase time, throughput, successful-request p99, rejection/unknown counts, monitoring availability, memory pressure, and disk queue behavior. Include hot-shard and sustained maintenance workloads. Multiple journals/independent store shards are a later alternative if the shared sequencer remains the bottleneck; they introduce routing, migration, aggregation, and maintenance semantics beyond this first step.
