#!/usr/bin/env python3
"""Audit completed measurements and render a source-backed engineering comparison."""
import argparse, hashlib, json, pathlib, statistics

def audit(directory, expected_cases=None):
    metadata=json.loads((directory/'metadata.json').read_text());assert metadata.get('completed') is True
    fixtures=json.loads((directory/'fixtures.json').read_text());files=sorted((directory/'raw').glob('*.json'));groups={}
    if expected_cases is not None:assert len(files)==expected_cases
    assert len(files)==metadata['completed_cases'];phases=operations=0
    for file in files:
        result=json.loads(file.read_text());manifest=fixtures[result['dataset']];lengths=manifest['lengths'];count=manifest['records']
        assert len(result['phases'])==9
        for phase in result['phases']:
            assert phase['error_count']==0 and phase['operations']==count,(file,phase)
            expected=sum(lengths[i]for i in manifest['hot_order'])if phase['name']in('write_duplicate','read_hot')else sum(lengths)
            assert phase['logical_bytes']==expected,(file,phase['name'])
            expected_rate=count/phase['elapsed_seconds'];assert abs(expected_rate-phase['ops_per_second'])<expected_rate*1e-9
            phases+=1;operations+=count
            key=(result['dataset'],result['profile'],result['write_pattern'],phase['name'])
            groups.setdefault(key,[]).append(phase)
        mixed=result['phases'][-1];assert mixed['name']=='mixed_read_write'
        assert mixed['write_operations']==len(manifest['mixed_write_positions'])
        assert mixed['read_operations']+mixed['write_operations']==count
        assert mixed['read_logical_bytes']+mixed['write_logical_bytes']==sum(lengths)
        storage=result['storage'];objects=storage.get('objects',storage.get('total_stored_blobs'))
        payload=storage.get('payload_bytes',storage.get('total_stored_payload_bytes'))
        assert objects==count+len(manifest['mixed_write_positions'])
        expected_payload=sum(lengths)+sum(lengths[manifest['mixed_order'][p]]for p in manifest['mixed_write_positions'])
        assert payload==expected_payload
    for source,expected in metadata.get('sources',{}).items():
        assert hashlib.sha256(pathlib.Path(source).read_bytes()).hexdigest()==expected,source
    if 'supplement_sha256' in metadata:
        here=pathlib.Path(__file__).resolve().parent
        assert hashlib.sha256((here/'compare_fullsync.py').read_bytes()).hexdigest()==metadata['supplement_sha256']
        assert hashlib.sha256((here/'compare_pelagia.py').read_bytes()).hexdigest()==metadata['primary_harness_sha256']
    for row in json.loads((directory/'summary.json').read_text()):
        key=tuple(row[k]for k in ('dataset','profile','write_pattern','phase'));samples=groups.pop(key)
        assert row['trials']==len(samples)==metadata['trials']
        for metric in ('ops_per_second','elapsed_seconds','latency_p50_us','latency_p95_us','latency_p99_us'):
            assert row[metric]==statistics.median(p[metric]for p in samples),(key,metric)
        assert all(p['latency_unit']==row['latency_unit']for p in samples)
    assert not groups
    return {'completed_cases':len(files),'validated_phases':phases,'validated_operations':operations,'payload_or_operation_errors':0}

def main(args):
    primary=args.primary.resolve();control=args.control.resolve() if args.control else None
    primary_audit=audit(primary,141);control_audit=audit(control,18)if control else None
    summary=json.loads((primary/'summary.json').read_text())
    def item(dataset,profile,pattern,phase):
        row=next(r for r in summary if(r['dataset'],r['profile'],r['write_pattern'],r['phase'])==(dataset,profile,pattern,phase));assert row['trials']==3;return row
    def rate(dataset,profile,pattern,phase):return item(dataset,profile,pattern,phase)['ops_per_second']
    def number(value):return f'{value:,.0f}'
    def milliseconds(us):return f'{us/1000:.3f}'
    lines=['# Pelagia KVStore2 versus SkerryKV — local comparison','',
           'Measured on 2026-10-26 using an Apple M3 Max, 16 logical CPUs, 128 GiB RAM and the same APFS SSD. Three trials, randomized serial case order; medians below. Fixtures are synthetic, deterministic, incompressible-like binary data. They fit in RAM. Timed reads follow explicit warmup passes: these are warm-cache, small-index results, not HDD/cold-cache/large-archive predictions. Disk free space was about 80 GiB on a 2 TB volume (roughly 96% full).', '',
           'Pelagia uses the current source file and **packaged** BLAKE3 / prefix-length 2 / 64 GiB shard configuration, not an inspected live deployment configuration. Skerry direct-engine, coordinator, and loopback HTTP results are distinct. No production code or live store was modified. Initialization and fixture generation are excluded from phase throughput.', '',
           '## Main result by payload size','',
           '| Payload | Objects/trial | KV2 single PUT/s | Skerry engine single PUT/s | Skerry engine bulk-64 PUT/s | KV2 random GET/s | Skerry engine random GET/s | Read ratio |',
           '|---|---:|---:|---:|---:|---:|---:|---:|']
    lines.insert(2, 'At 20 KiB, Skerry direct warm random reads are %.1f× faster, and native bulk-64 writes reach %s objects/s. Current KV2 single writes are %.1f× faster than Skerry single engine writes under their different synchronization contracts. Default Skerry HTTP writes remain limited by coordinator and request-path behavior; see the separate service rows and macOS control below.' % (rate('20k','skerry_engine','sequential','read_random')/rate('20k','kv2_current','sequential','read_random'), number(rate('20k','skerry_engine','batch64','write_batch64')), rate('20k','kv2_current','sequential','write_sequential')/rate('20k','skerry_engine','sequential','write_sequential')))
    lines.insert(3, '')
    names={'5k':'5 KiB','20k':'20 KiB','32k':'32 KiB','128k':'128 KiB','mixed':'Mixed 5/20/32/128 KiB','1m':'1 MiB'}
    fixtures=json.loads((primary/'fixtures.json').read_text())
    for name,label in names.items():
        a=rate(name,'kv2_current','sequential','write_sequential');b=rate(name,'skerry_engine','sequential','write_sequential');bulk=rate(name,'skerry_engine','batch64','write_batch64');read_a=rate(name,'kv2_current','sequential','read_random');read_b=rate(name,'skerry_engine','sequential','read_random')
        lines.append(f'| {label} | {fixtures[name]["records"]} | {number(a)} | {number(b)} | {number(bulk)} | {number(read_a)} | {number(read_b)} | {read_b/read_a:.1f}× |')
    lines+=['','Single PUTs and native bulk-64 have different acknowledgment granularity; batching can amortize synchronization. KV2 has no native cross-object batch operation. MiB/s, sampled latency percentiles, and latency units are in the raw CSV/JSON.','', '## Access patterns at 20 KiB','',
            '| Pattern | KV2 ops/s | Skerry engine ops/s | Skerry/KV2 | KV2 p95 ms | Skerry p95 ms |','|---|---:|---:|---:|---:|---:|']
    patterns=[('Single new writes','sequential','write_sequential'),('8-client new writes','concurrent','write_concurrent'),('Duplicate hot writes','sequential','write_duplicate'),('Sequential reads','sequential','read_sequential'),('Uniform random reads','sequential','read_random'),('95% hot-set reads','sequential','read_hot'),('8-client random reads','sequential','read_concurrent_random'),('Logical ordered batches','sequential','read_batch_ordered64'),('Logical unordered batches','sequential','read_batch_unordered64'),('25% new writes / 75% reads','sequential','mixed_read_write')]
    for label,pattern,phase in patterns:
        a=item('20k','kv2_current',pattern,phase);b=item('20k','skerry_engine',pattern,phase)
        lines.append(f'| {label} | {number(a["ops_per_second"])} | {number(b["ops_per_second"])} | {b["ops_per_second"]/a["ops_per_second"]:.2f}× | {milliseconds(a["latency_p95_us"])} | {milliseconds(b["latency_p95_us"])} |')
    lines+=['','Mixed actions are randomized independently of object size and distributed across workers; counts are fixed, closed-loop operations, not a constant-rate arrival process. The hot set is about 1% of objects. In-process read batches are bounded groups of individual API calls, not the HTTP binary batch protocol.','', '## Coordinator and HTTP overhead at 20 KiB','',
            '| Access layer | New single writes/s | New 8-client writes/s | Duplicate writes/s | Random reads/s | Random GET p95 ms |','|---|---:|---:|---:|---:|---:|']
    for label,profile in [('KV2 direct Python','kv2_current'),('Skerry direct engine','skerry_engine'),('Skerry coordinator, default 10 ms','skerry_coordinator'),('Skerry coordinator, warm 256 MiB payload cache','skerry_cached'),('Skerry loopback HTTP, default 10 ms','skerry_http'),('Skerry coordinator, delay=0 sensitivity','skerry_coordinator_delay0')]:
        s=[r for r in summary if(r['dataset'],r['profile'],r['phase'])==('20k',profile,'write_sequential')]
        c=[r for r in summary if(r['dataset'],r['profile'],r['phase'])==('20k',profile,'write_concurrent')]
        pattern='concurrent'if profile=='skerry_cached'else'sequential';du=item('20k',profile,pattern,'write_duplicate');rd=item('20k',profile,pattern,'read_random')
        lines.append(f'| {label} | {number(s[0]["ops_per_second"])if s else "—"} | {number(c[0]["ops_per_second"])if c else "—"} | {number(du["ops_per_second"])} | {number(rd["ops_per_second"])} | {milliseconds(rd["latency_p95_us"])} |')
    lines+=['','The HTTP row includes network/client/parser/JSON costs and production default INFO logging; Pelagia was called in process, so it is not a matched comparison of two HTTP servers. The dataset payload size fits within the configured cache capacity; the profile includes explicit cache warmup. In-process cache throughput is not a network throughput claim.','', '## Bulk ingestion at 20 KiB','', '| Path | Logical objects/s | Actual operation |','|---|---:|---|']
    for label,profile,pattern,phase,description in [('KV2','kv2_current','sequential','write_sequential','One put_store call per object'),('Skerry engine','skerry_engine','batch64','write_batch64','Native 64-object atomic publication groups'),('Skerry coordinator','skerry_coordinator','batch64','write_batch64','Up to 64 outstanding requests; bounded writer groups'),('Skerry HTTP batch-put','skerry_http','batch64','write_batch64','Binary batches of 64; current handler awaits each item')]:lines.append(f'| {label} | {number(rate("20k",profile,pattern,phase))} | {description} |')
    lines+=['','Batch latency units differ: engine bulk latency is batch completion per member, HTTP streaming batch latency is item arrival from batch start, and individual calls have individual completion latency. Do not compare these percentile columns as one common latency metric. Units are preserved in raw output.','', '## Same-prefix contention','', '| Initial new writes, 8 workers | KV2 objects/s | Skerry engine objects/s |','|---|---:|---:|']
    for name,label in [('20k','Uniform BLAKE3 prefixes, 1,024 objects'),('hotprefix','All original hashes prefix 00, 512 objects')]:lines.append(f'| {label} | {number(rate(name,"kv2_current","concurrent","write_concurrent"))} | {number(rate(name,"skerry_engine","concurrent","write_concurrent"))} |')
    lines+=['','The forced-prefix fixture concerns original objects; mixed new objects are not prefix-filtered. These two cases have different counts, so initialization/growth effects also differ; rates are normalized but this is not a pure lock-only microbenchmark.','', '## Storage footprint after the mixed phase','', '| Dataset / implementation | Unique payload MiB | Final allocated file MiB | Allocated/payload | File count |','|---|---:|---:|---:|---:|']
    for name in ['20k','128k','1m']:
        for profile,label in [('kv2_current','KV2'),('skerry_engine','Skerry')]:
            rows=[json.loads(p.read_text())for p in(primary/'raw').glob(f'{name}__{profile}__sequential__trial*.json')]
            payload=statistics.median(r['storage'].get('payload_bytes',r['storage'].get('total_stored_payload_bytes'))for r in rows);allocated=statistics.median(r['filesystem']['all_file_allocated_bytes']for r in rows);files=statistics.median(r['filesystem']['file_count']for r in rows)
            lines.append(f'| {names[name]} / {label} | {payload/1048576:.2f} | {allocated/1048576:.2f} | {allocated/payload:.2f}× | {files:.0f} |')
    lines+=['','Final allocation is measured with filesystem stat blocks after the child closes. It includes store files/indexes and growth padding; it is not physical disk I/O or whole-filesystem metadata overhead.','', '## Durability and integrity controls','',
            'The unmodified KV2 profile uses SQLite WAL synchronous=NORMAL, Python os.fsync for blobs, and no payload hash recomputation on ordinary GET. Skerry synchronizes segment bytes and Fjall before success and verifies payload BLAKE3 on disk reads. These contracts are not equivalent. On this installed Rust/macOS target, sync_all uses F_FULLFSYNC; SQLite fullfsync defaults OFF and Python os.fsync is not that stronger call. A SQLite FULL-only sensitivity still does not align those macOS flush calls.','']
    if control:
        control_rows=json.loads((control/'summary.json').read_text());lines+=['A separate, randomized control uses 256 objects × 20 KiB per trial, three trials, and the same fixtures across its three implementations. It runs after the primary suite, so cross-suite drift remains possible. The KV2 macOS-fullsync variant is an isolated benchmark adaptation: Python blob fsync calls use fcntl(F_FULLFSYNC), SQLite uses WAL/FULL with fullfsync=ON, and reads verify BLAKE3. This is a flush-strength control, not a claim of crash-recovery/directory-sync parity.','', '| Control / 256 objects | Single PUT/s | 8-client PUT/s | Random GET/s |','|---|---:|---:|---:|']
        for profile,label in [('kv2_full_verify','KV2 FULL + read hash, ordinary fsync'),('kv2_macos_fullsync_verify','KV2 FULL + read hash + macOS fullsync'),('skerry_engine','Skerry SyncAll + read hash')]:
            def cr(pattern,phase):return next(r['ops_per_second']for r in control_rows if(r['profile'],r['write_pattern'],r['phase'])==(profile,pattern,phase))
            lines.append(f'| {label} | {number(cr("sequential","write_sequential"))} | {number(cr("concurrent","write_concurrent"))} | {number(cr("sequential","read_random"))} |')
    if control:
        lines+=['', 'Strengthening the flush calls reverses the single-write ranking within this control. This demonstrates that synchronization policy materially affects these measurements; it does not attribute every difference to one syscall or prove durability parity.']
    lines+=['','## Review findings','',
            '1. **Ordinary KV2 reads do not verify payload contents.** `kvstore2.py:432-442,1147-1161` checks metadata/header and length. A same-length payload corruption was returned by GET in a disposable probe; deep verification detected it. Skerry verifies BLAKE3 on disk reads. This is a reliability gap, not a performance optimization to copy.','',
            '2. **KV2 does not provide the same durable-ack contract.** Its index connection uses WAL/NORMAL at `kvstore2.py:1311-1316`; a system crash/power loss can lose recent committed mappings. Blob fsync before commit does not strengthen that index setting. No power-loss tests were run on either implementation.','',
            '3. **Per-call operations and periodic growth work deserve profiling.** Every GET opens a connection, sets WAL/synchronous/foreign-key pragmas, opens a blob, and parses a JSON record header. The connection context manager does not itself close it. Growth zero-fills and synchronizes on this host (no posix_fallocate), then payload writes synchronize again. See `kvstore2.py:907-923,1121-1145,1311-1317`.','',
            '4. **Stale-lock recovery relies on age, not owner liveness.** The probe obtained a second file lock after a short configured TTL while its owner was alive. Default TTL is one hour. This breaks file-lock exclusivity, but SQLite BEGIN IMMEDIATE independently serializes the examined append paths; the probe does not establish overlapping appends or corruption. See `kvstore2.py:94-146,302-305,982-985`.','',
            '5. **Skerry default service writes need tuning.** A sparse coordinator batch waits up to its configured window unless filled or interrupted. Native HTTP batch-put currently awaits each item, missing bulk grouping. HTTP admission reserves a maximum-object allowance: 256 MiB / 64 MiB gives four concurrent uploads despite small objects, so the 8-client benchmark includes retry costs. These are source-backed current behaviors, not limitations of Rust or Fjall.','',
            '## Scope and next steps','',
            'Read gains here concern warm, small indexes; bulk gains include batching and synchronization amortization. Do not infer disk-cold, HDD, very large index, sustained compaction, crash-recovery speed, Windows, or Linux performance from this Mac. The large-object corner has 128 operations/trial, so p99 tails are especially noisy. Synthetic size distributions are assumptions, not a measured Pelagia traffic distribution.','',
            'Prioritize Skerry bounded HTTP batch pipelining and upload admission based on known content length, plus a durable-safe duplicate fast path. Preserve the acknowledgment contract. Independently add ordinary-read integrity verification and explicit durability/connection-lifetime policies to KV2 before treating it as an archive reliability baseline. Re-run on the intended Linux/Windows storage and a representative working set before migration. KV2 blobs are not directly importable Skerry segments; migration must validate existing hashes and stream/reinsert objects, with reference mapping if source hashes are SHA-256.','',
            '## Evidence and reproduction','',
            f'Primary audit: {primary_audit}.',f'Control audit: {control_audit}.','',
            '- [Harness instructions](README.md)',f'- [Primary raw results]({primary.relative_to(args.output.parent).as_posix()}/summary.json)'if primary.is_relative_to(args.output.parent)else f'- Primary results: `{primary}`',
            f'- [Primary measurements CSV]({primary.relative_to(args.output.parent).as_posix()}/measurements.csv)', f'- [Supplemental control]({control.relative_to(args.output.parent).as_posix()}/summary.json)' if control else '', '- [Review probe evidence](review-evidence.json)','- [Flush source evidence](sync-semantics.json)','',
            'References: [SQLite synchronous/fullfsync](https://www.sqlite.org/pragma.html#pragma_synchronous), [Python connection context manager](https://docs.python.org/3.12/library/sqlite3.html#how-to-use-the-connection-context-manager), [Rust Apple sync implementation](https://doc.rust-lang.org/src/std/sys/fs/unix.rs.html).']
    args.output.write_text('\n'.join(lines)+'\n')
    (primary/'audit.json').write_text(json.dumps(primary_audit,indent=2)+'\n')
    if control:(control/'audit.json').write_text(json.dumps(control_audit,indent=2)+'\n')
    print(json.dumps({'report':str(args.output),'primary':primary_audit,'control':control_audit}))

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--primary',type=pathlib.Path,required=True);p.add_argument('--control',type=pathlib.Path);p.add_argument('--output',type=pathlib.Path,default=pathlib.Path(__file__).parent/'COMPARISON.md');main(p.parse_args())
