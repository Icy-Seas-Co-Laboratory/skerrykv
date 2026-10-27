#!/usr/bin/env python3
"""Reproducible, disposable KVStore2/Skerry comparison; never opens a live store.
Run with Pelagia's Python 3.12 environment. Native Skerry examples are release builds.
"""
from __future__ import annotations
import argparse, base64, concurrent.futures, gc, hashlib, http.client, importlib.util, io, json
import math, os, platform, random, resource, shutil, socket, sqlite3, struct, subprocess
import sys, tempfile, threading, time, traceback
from datetime import datetime, timezone
import io as bytes_io
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SEED = 20261026

def write_json(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)

def source_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def fixture(path, name, records, seed):
    import blake3
    path.mkdir(parents=True)
    rng = random.Random(seed)
    sizes = {'5k': 5*1024, '20k': 20*1024, '32k': 32*1024,
             '128k': 128*1024, '1m': 1024*1024, 'hotprefix': 20*1024}
    lengths = []; prefixes = {}; attempts = 0
    digest = hashlib.sha256()
    with (path/'payloads.bin').open('wb') as output:
        while len(lengths) < records:
            size = sizes.get(name, [5*1024,20*1024,32*1024,128*1024][len(lengths)%4])
            payload = rng.randbytes(size)
            # Unique deterministic content, including for small/filtered fixtures.
            payload = attempts.to_bytes(8, 'little') + payload[8:]; attempts += 1
            key = blake3.blake3(payload).hexdigest()
            if name == 'hotprefix' and not key.startswith('00'):
                continue
            prefixes[key[:2]] = prefixes.get(key[:2], 0) + 1
            output.write(payload); digest.update(payload); lengths.append(size)
    order = list(range(records)); rng.shuffle(order)
    hot = order[:max(1,records//100)]
    hot_order = [rng.choice(hot) if rng.random()<.95 else rng.randrange(records) for _ in range(records)]
    mixed_order=list(range(records));rng.shuffle(mixed_order)
    mixed_write_positions=sorted(rng.sample(range(records),(records+3)//4))
    manifest = {'mixed_order':mixed_order,'mixed_write_positions':mixed_write_positions,'name':name,'seed':seed,'lengths':lengths,'read_order':order,'hot_order':hot_order,
                'sha256':digest.hexdigest(),'prefix_counts':prefixes,'generation_attempts':attempts,
                'logical_bytes':sum(lengths),'records':records,'hot_pool_size':len(hot)}
    write_json(path/'manifest.json',manifest)
    return manifest

def load_fixture(path):
    import blake3
    manifest = json.loads((path/'manifest.json').read_text())
    all_bytes = (path/'payloads.bin').read_bytes()
    assert hashlib.sha256(all_bytes).hexdigest() == manifest['sha256']
    payloads=[]; offset=0
    for length in manifest['lengths']:
        payloads.append(all_bytes[offset:offset+length]); offset += length
    assert offset == len(all_bytes)
    keys=[blake3.blake3(p).hexdigest() for p in payloads]
    return manifest,payloads,keys

def percentile(values,p):
    return values[min(len(values)-1, math.ceil(p*len(values))-1)] if values else 0.

def measured_phase(name, order, payloads, operation, executor, workers=1, batch=None):
    """Latency covers call->return; validation follows that clock but is in wall time."""
    start = time.perf_counter(); samples=[]; errors=[]
    def lane(indices):
        local=[]; failures=[]
        for index in indices:
            begin=time.perf_counter_ns()
            try:
                returned=operation(index)
                elapsed=(time.perf_counter_ns()-begin)/1000.
                # Each operation returns validation status; storage read verification
                # (when enabled) occurs inside operation, byte comparison follows it.
                expected_index=index
                if isinstance(returned,tuple):returned,expected_index=returned
                if isinstance(returned,bytes) and returned != payloads[expected_index]:
                    raise AssertionError('retrieved bytes differ from fixture')
                local.append((elapsed,len(payloads[expected_index])))
            except Exception as error:
                failures.append(f'{index}: {type(error).__name__}: {error}')
        return local,failures
    if batch is not None:
        for pos in range(0,len(order),64):
            chunk=order[pos:pos+64]
            try:
                samples.extend(batch(chunk))
            except Exception as error:
                errors.append(f'batch {pos}: {type(error).__name__}: {error}')
    elif workers==1:
        samples,errors=lane(order)
    else:
        pending=[executor.submit(lane,order[w::workers]) for w in range(workers)]
        for job in pending:
            local,failures=job.result(); samples.extend(local);errors.extend(failures)
    elapsed=time.perf_counter()-start
    latencies=sorted(s[0] for s in samples); logical=sum(s[1] for s in samples)
    return {'name':name,'operations':len(samples),'logical_bytes':logical,'elapsed_seconds':elapsed,
            'ops_per_second':len(samples)/elapsed,'mib_per_second':logical/elapsed/1048576.,
            'latency_p50_us':percentile(latencies,.5),'latency_p95_us':percentile(latencies,.95),
            'latency_p99_us':percentile(latencies,.99),'max_us':max(latencies,default=0.),
            'error_count':len(errors),'errors':errors[:20],
            'latency_unit':'per_item_batch_completion' if batch else 'per_operation'}

def filesystem_usage(root):
    logical=allocated=files=0
    for p in root.rglob('*'):
        if p.is_file():
            stat=p.stat(); logical+=stat.st_size;allocated+=getattr(stat,'st_blocks',0)*512;files+=1
    return {'all_file_logical_bytes':logical,'all_file_allocated_bytes':allocated,'file_count':files}

class HttpBackend:
    def __init__(self,store,payloads,keys,args):
        self.store=store; self.payloads=payloads;self.keys=keys;self.local=threading.local();self.retry_count=0;self.transport_retries=0;self.retry_lock=threading.Lock()
        sock=socket.socket();sock.bind(('127.0.0.1',0));self.port=sock.getsockname()[1];sock.close()
        self.config=store.parent/'http-config.toml'
        self.config.write_text(f'''[server]\nlisten = "127.0.0.1:{self.port}"\n[storage]\nroot = {json.dumps(str(store))}\n[writes]\nbatch_max_delay_ms = {args.batch_delay_ms}\n[cache]\npayload_mib = {args.payload_cache_mib}\n[mcp]\nenabled = false\n''')
        env=os.environ.copy()
        for key in list(env):
            if key.startswith('SKERRY_'):env.pop(key,None)
        self.log=(store.parent/'service.log').open('w')
        self.process=subprocess.Popen([str(ROOT/'target/release/skerry'),'serve','--config',str(self.config)],env=env,stdout=self.log,stderr=self.log)
        for _ in range(200):
            if self.process.poll() is not None:raise RuntimeError('HTTP service failed to start')
            try:
                c=http.client.HTTPConnection('127.0.0.1',self.port,timeout=5);c.request('GET','/v1/health');r=c.getresponse();r.read();c.close()
                if r.status==200:break
            except OSError:pass
            time.sleep(.025)
        else:raise TimeoutError('HTTP startup timeout')
    def connection(self):
        if not hasattr(self.local,'connection'):
            self.local.connection=http.client.HTTPConnection('127.0.0.1',self.port,timeout=120)
        return self.local.connection
    def request(self,method,path,body=None,content_type='application/octet-stream'):
        deadline=time.monotonic()+120
        delay=.001
        while True:
            try:
                c=self.connection();c.request(method,path,body=body,headers={'Content-Type':content_type})
                response=c.getresponse();data=response.read()
            except (OSError,http.client.HTTPException):
                if time.monotonic()>=deadline:raise
                if hasattr(self.local,'connection'):
                    self.local.connection.close();del self.local.connection
                with self.retry_lock:self.transport_retries+=1
                time.sleep(delay);delay=min(.01,delay*2);continue
            if response.status==503 and time.monotonic()<deadline:
                with self.retry_lock:self.retry_count+=1
                time.sleep(delay);delay=min(.01,delay*2);continue
            if response.status!=200:raise RuntimeError(f'{method} {path} -> {response.status}: {data[:200]!r}')
            return data
    def put_data(self,data,key):
        result=json.loads(self.request('PUT','/v1/objects',data))
        assert result['hash']==key
    def put(self,index):
        self.put_data(self.payloads[index],self.keys[index])
    def get(self,index):
        return self.request('GET','/v1/objects/'+self.keys[index])
    def batch(self,indices,put=False,ordered=True):
        frames=[]
        for ordinal,index in enumerate(indices):
            payload=self.payloads[index] if put else b''
            digest=b'\0'*32 if put else bytes.fromhex(self.keys[index])
            header=struct.pack('<4sBBHQ32sQ',b'SKB1',1 if put else 2,0,0,ordinal,digest,len(payload))
            frames.append(struct.pack('<I',56+len(payload))+header+payload)
        start=time.perf_counter_ns();c=self.connection()
        c.request('POST','/v1/objects/batch-put' if put else f'/v1/objects/batch-get?ordered={str(ordered).lower()}',body=b''.join(frames),headers={'Content-Type':'application/vnd.skerry.batch-v1'})
        response=c.getresponse()
        if response.status!=200:raise RuntimeError(f'batch status {response.status}')
        samples=[];seen=set()
        def exact(n):
            data=response.read(n)
            if len(data)!=n:raise RuntimeError('truncated batch response')
            return data
        for _ in indices:
            length=struct.unpack('<I',exact(4))[0]
            magic,kind,status,flags,ordinal,digest,size=struct.unpack('<4sBBHQ32sQ',exact(56))
            assert magic==b'SKB1' and status==0 and flags==0 and kind==(129 if put else 130)
            assert length==56+size and ordinal<len(indices) and ordinal not in seen
            seen.add(ordinal);index=indices[ordinal]
            data=exact(size)
            elapsed=(time.perf_counter_ns()-start)/1000.
            assert digest.hex()==self.keys[index]
            if not put:assert data==self.payloads[index]
            samples.append((elapsed,len(self.payloads[index])))
        assert response.read()==b''
        return samples
    def finish(self):
        status=json.loads(self.request('GET','/v1/status'))
        c=self.connection();c.request('POST','/v1/admin/shutdown',body=b'');r=c.getresponse();r.read()
        assert r.status==202
        assert self.process.wait(timeout=30)==0;self.log.close()
        return status
    def abort(self):
        if self.process.poll() is None:self.process.kill();self.process.wait()
        self.log.close()

def python_worker(args):
    import blake3
    setup=time.perf_counter()
    manifest,payloads,keys=load_fixture(args.fixture)
    backend=None
    if args.profile.startswith('kv2'):
        path=args.pelagia_root/'Pelagia/storage/kvstore2.py'
        spec=importlib.util.spec_from_file_location('bench_pelagia_kvstore2',path)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        cls=module.KVStore2
        if args.profile=='kv2_full_verify':
            class FullIndex(cls):
                def _connect(self,db_path):
                    db_path.parent.mkdir(parents=True,exist_ok=True)
                    connection=sqlite3.connect(db_path)
                    connection.execute('PRAGMA journal_mode=WAL');connection.execute('PRAGMA synchronous=FULL');connection.execute('PRAGMA foreign_keys=ON')
                    return connection
            cls=FullIndex
        backend=cls(args.store);backend.initialize(hash_algorithm='blake3',prefix_length=2,max_blob_bytes=64*1024**3)
        def put_data(data,key):
            result=backend.put_stream(bytes_io.BytesIO(data)) if args.profile=='kv2_stream' else backend.put_store(data)
            assert result==key
        def put(i):put_data(payloads[i],keys[i])
        def get(i):
            value=backend.get_store(keys[i])
            if args.profile=='kv2_full_verify':assert blake3.blake3(value).hexdigest()==keys[i]
            return value
    else:
        backend=HttpBackend(args.store,payloads,keys,args);put=backend.put;get=backend.get;put_data=backend.put_data
    mixed={}
    seen=set(keys)
    for position in manifest['mixed_write_positions']:
        i=manifest['mixed_order'][position]
        data=bytes([payloads[i][0]^255])+payloads[i][1:];key=blake3.blake3(data).hexdigest();assert key not in seen;seen.add(key);mixed[i]=(data,key)
    executor=concurrent.futures.ThreadPoolExecutor(max_workers=args.workers)
    barrier=threading.Barrier(args.workers)
    list(executor.map(lambda _:barrier.wait(),range(args.workers)))
    setup_seconds=time.perf_counter()-setup;phases=[];seq=list(range(len(payloads)))
    def phase_measure(*positional,**kwargs):
        before=backend.retry_count if isinstance(backend,HttpBackend) else 0
        transport_before=backend.transport_retries if isinstance(backend,HttpBackend) else 0
        value=measured_phase(*positional,**kwargs)
        value['http_503_retries']=(backend.retry_count-before) if isinstance(backend,HttpBackend) else 0
        value['http_transport_retries']=(backend.transport_retries-transport_before) if isinstance(backend,HttpBackend) else 0
        return value
    try:
        pattern=args.write_pattern
        if pattern=='batch64' and args.profile=='skerry_http':
            phases.append(phase_measure('write_batch64',seq,payloads,put,executor,batch=lambda indices:backend.batch(indices,put=True)))
        else:
            phases.append(phase_measure('write_'+pattern,seq,payloads,put,executor,args.workers if pattern=='concurrent' else 1))
        if phases[-1]['error_count']:raise RuntimeError(f'initial ingestion failed: {phases[-1]["errors"]}')
        phases.append(phase_measure('write_duplicate',manifest['hot_order'],payloads,put,executor))
        for name,order,workers in [('read_sequential',seq,1),('read_random',manifest['read_order'],1),('read_hot',manifest['hot_order'],1),('read_concurrent_random',manifest['read_order'],args.workers),('read_batch_ordered64',manifest['read_order'],1),('read_batch_unordered64',manifest['read_order'],args.workers)]:
            for _ in range(2 if args.profile=='skerry_http' and args.payload_cache_mib else 1):
                for i in seq:assert get(i)==payloads[i]
            if args.profile=='skerry_http' and name.startswith('read_batch_'):
                batch=lambda indices:backend.batch(indices,ordered=name=='read_batch_ordered64')
                phases.append(phase_measure(name,order,payloads,get,executor,batch=batch))
            elif name.startswith('read_batch_') and workers>1:
                def batch(indices):
                    begin=time.perf_counter_ns()
                    def lane(items):
                        values=[]
                        for i in items:
                            start=time.perf_counter_ns();value=get(i);elapsed=(time.perf_counter_ns()-start)/1000.;assert value==payloads[i];values.append((elapsed,len(value)))
                        return values
                    jobs=[executor.submit(lane,indices[w::workers]) for w in range(min(workers,len(indices)))]
                    return [sample for job in jobs for sample in job.result()]
                phases.append(phase_measure(name,order,payloads,get,executor,batch=batch))
            else:phases.append(phase_measure(name,order,payloads,get,executor,workers))
        for i in seq:assert get(i)==payloads[i]
        write_positions=set(manifest['mixed_write_positions'])
        def mixed_operation(position):
            i=manifest['mixed_order'][position]
            if position in write_positions:
                data,key=mixed[i];put_data(data,key);return None,i
            return get(i),i
        phases.append(phase_measure('mixed_read_write',seq,payloads,mixed_operation,executor,args.workers))
        phase=phases[-1]
        phase['read_operations']=len(seq)-len(write_positions);phase['write_operations']=len(write_positions)
        phase['write_logical_bytes']=sum(len(payloads[manifest['mixed_order'][p]])for p in write_positions)
        phase['read_logical_bytes']=sum(len(payloads[manifest['mixed_order'][p]])for p in seq if p not in write_positions)
        if args.profile.startswith('kv2'):
            for phase in phases:phase['latency_unit']='per_operation'
            storage=backend.status(deep=True);io=cache=None
        else:
            status=backend.finish();storage=status['storage'];io=status['io'];io['client_http_503_retries']=backend.retry_count;io['client_http_transport_retries']=backend.transport_retries;cache=status['cache']
        gc.collect()
        usage=filesystem_usage(args.store)
        result={'engine':args.profile,'config':{'hash':'blake3','prefix_length':2,'max_blob_bytes':64*1024**3,'sqlite_synchronous':'FULL' if args.profile=='kv2_full_verify' else 'NORMAL' if args.profile.startswith('kv2') else None,'verify_payload_on_read':args.profile not in ('kv2_current','kv2_stream'),'write_pattern':args.write_pattern,'workers':args.workers,'payload_cache_mib':args.payload_cache_mib,'batch_delay_ms':args.batch_delay_ms},'setup_seconds':setup_seconds,'phases':phases,'storage':storage,'io':io,'cache':cache,'filesystem':usage}
        write_json(args.output,result)
        if any(p['error_count'] for p in phases):raise RuntimeError('benchmark operation errors; see output')
    finally:
        executor.shutdown(wait=True)
        if isinstance(backend,HttpBackend):backend.abort()

def command_result(command,output,timeout=600):
    import psutil
    start=time.monotonic();log=output.with_suffix('.log')
    with log.open('w') as diagnostics:
        process=subprocess.Popen(command,stdout=diagnostics,stderr=diagnostics)
        peak_rss=0;peak_family_rss=0
        while process.poll() is None:
            try:
                p=psutil.Process(process.pid);rss=p.memory_info().rss
                family=rss+sum(c.memory_info().rss for c in p.children(recursive=True) if c.is_running())
                peak_rss=max(peak_rss,rss);peak_family_rss=max(peak_family_rss,family)
            except (psutil.NoSuchProcess,psutil.AccessDenied):pass
            if time.monotonic()-start>timeout:
                for child in psutil.Process(process.pid).children(recursive=True):child.kill()
                process.kill();process.wait();raise TimeoutError(f'benchmark case timeout; {log}')
            time.sleep(.02)
    if process.returncode!=0:raise RuntimeError(f'case failed ({process.returncode}); inspect {log}:\n'+log.read_text()[-3000:])
    result=json.loads(output.read_text());result['process']={'wall_seconds_including_setup_warmups':time.monotonic()-start,'sampled_peak_rss_bytes':peak_rss,'sampled_peak_family_rss_bytes':peak_family_rss,'rss_sampling_interval_ms':20}
    return result

def summarize(results_dir):
    import csv,statistics
    rows=[]
    for path in sorted((results_dir/'raw').glob('*.json')):
        result=json.loads(path.read_text())
        for phase in result['phases']:
            rows.append({'dataset':result['dataset'],'profile':result['profile'],'trial':result['trial'],'write_pattern':result['write_pattern'],**{k:v for k,v in phase.items() if k not in ('errors',)}})
    if not rows:return
    with (results_dir/'measurements.csv').open('w',newline='') as out:
        fieldnames=list(dict.fromkeys(key for row in rows for key in row));writer=csv.DictWriter(out,fieldnames=fieldnames);writer.writeheader();writer.writerows(rows)
    groups={}
    for row in rows:groups.setdefault((row['dataset'],row['profile'],row['write_pattern'],row['name']),[]).append(row)
    aggregates=[]
    for (dataset,profile,pattern,phase),values in sorted(groups.items()):
        result={'dataset':dataset,'profile':profile,'write_pattern':pattern,'phase':phase,'trials':len(values),'latency_unit':values[0]['latency_unit']}
        assert len({v['latency_unit'] for v in values})==1
        for metric in ['ops_per_second','mib_per_second','latency_p50_us','latency_p95_us','latency_p99_us','max_us','elapsed_seconds']:
            data=[v[metric] for v in values];result[metric]=statistics.median(data);result[metric+'_min']=min(data);result[metric+'_max']=max(data)
        result['operations_per_trial']=values[0]['operations'];result['error_count']=sum(v['error_count'] for v in values);aggregates.append(result)
    write_json(results_dir/'summary.json',aggregates)

def orchestrate(args):
    import blake3,importlib.metadata
    args.results.mkdir(parents=True,exist_ok=False);(args.results/'raw').mkdir()
    work=Path(tempfile.mkdtemp(prefix='kv2-compare-',dir=args.work_parent))
    metadata={'started_utc':datetime.now(timezone.utc).isoformat(),'service_batch_delay_ms':args.batch_delay_ms,'seed':SEED,'records':args.records,'trials':args.trials,'workers':args.workers,'python':sys.version,'sqlite':sqlite3.sqlite_version,'blake3':importlib.metadata.version('blake3'),'platform':platform.platform(),'machine':platform.machine(),'sources':{},'cache_condition':'Explicit full read pass before each timed read phase; all data fits RAM; no OS cache eviction; no cold-cache or HDD claims.','durability_caveat':'KV2 current WAL NORMAL vs Skerry SyncAll; FULL+verify is benchmark-only sensitivity, not production KV2 behavior.','fixture_work_root':str(work),'rss_scope':'whole child/family, including fixture preload, initialization and warmups; not backend-only memory','sqlite_context_manager':'kv2_current uses production cache/context semantics; harness does not add pooling; kv2_full_verify overrides connections and bypasses production cache','os_posix_fallocate_available':hasattr(os,'posix_fallocate'),'load_average_start':os.getloadavg(),'disk_free_bytes_start':shutil.disk_usage(work).free}
    for file in [args.pelagia_root/'Pelagia/storage/kvstore2.py',args.pelagia_root/'Pelagia/default.config.toml',args.pelagia_root/'Pelagia/services/context.py',ROOT/'src/config.rs',ROOT/'src/engine.rs',ROOT/'src/coordinator.rs',ROOT/'src/server.rs',ROOT/'Cargo.lock',ROOT/'examples/compare_kvstore2.rs',Path(__file__)]:metadata['sources'][str(file)]=source_hash(file)
    for key,cmd in [('cpu',['sysctl','-n','machdep.cpu.brand_string']),('memory_bytes',['sysctl','-n','hw.memsize']),('rustc',['rustc','--version'])]:
        try:metadata[key]=subprocess.check_output(cmd,text=True).strip()
        except (FileNotFoundError,subprocess.CalledProcessError):metadata[key]=None
    write_json(args.results/'metadata.json',metadata)
    try:
        manifests={};fixtures={}
        for j,name in enumerate(args.datasets):
            count=min(args.records,128) if name=='1m' else min(args.records,512) if name=='hotprefix' else args.records
            fixtures[name]=work/'fixtures'/name;manifests[name]=fixture(fixtures[name],name,count,SEED+j)
        write_json(args.results/'fixtures.json',manifests)
        plan=[]
        for name in args.datasets:
            for profile,pattern,cache,delay in [('kv2_current','sequential',0,10),('kv2_current','concurrent',0,10),('skerry_engine','sequential',0,10),('skerry_engine','concurrent',0,10),('skerry_engine','batch64',0,10)]:plan.append((name,profile,pattern,cache,delay))
            if name in ('20k','1m'):
                plan.append((name,'kv2_stream','sequential',0,10))
            if name in ('20k','128k'):
                plan.append((name,'kv2_full_verify','sequential',0,10))
            if name=='20k':
                plan += [(name,'skerry_coordinator','sequential',0,10),(name,'skerry_coordinator','concurrent',0,10),(name,'skerry_coordinator','batch64',0,10),(name,'skerry_cached','concurrent',256,10),(name,'skerry_http','sequential',0,10),(name,'skerry_http','concurrent',0,10),(name,'skerry_http','batch64',0,10),(name,'skerry_coordinator_delay0','sequential',0,0)]
        plan=[(name,profile,pattern,cache,args.batch_delay_ms if profile in ('skerry_coordinator','skerry_cached','skerry_http') else delay) for name,profile,pattern,cache,delay in plan]
        if args.profiles:plan=[p for p in plan if p[1] in args.profiles]
        total=len(plan)*args.trials;done=0
        for trial in range(args.trials):
            tasks=plan[:];random.Random(SEED+trial).shuffle(tasks)
            for name,profile,pattern,cache,delay in tasks:
                done+=1;case=f'{name}__{profile}__{pattern}__trial{trial+1}'
                case_root=work/'cases'/case;case_root.mkdir(parents=True)
                output=case_root/'result.json';store=case_root/'store'
                print(f'[{done}/{total}] {case}',flush=True)
                if profile.startswith('kv2') or profile=='skerry_http':
                    command=[sys.executable,str(Path(__file__)),'worker','--profile',profile,'--store',str(store),'--fixture',str(fixtures[name]),'--output',str(output),'--pelagia-root',str(args.pelagia_root),'--workers',str(args.workers),'--write-pattern',pattern,'--payload-cache-mib',str(cache),'--batch-delay-ms',str(delay)]
                else:
                    command=[str(ROOT/'target/release/examples/compare_kvstore2'),'--mode','engine' if profile=='skerry_engine' else 'coordinator','--store',str(store),'--fixture',str(fixtures[name]),'--output',str(output),'--workers',str(args.workers),'--write-pattern',pattern,'--payload-cache-mib',str(cache),'--batch-delay-ms',str(delay)]
                try:result=command_result(command,output,args.case_timeout)
                except Exception:
                    log=output.with_suffix('.log')
                    if log.exists():shutil.copy2(log,args.results/f'FAILED_{case}.log')
                    if output.exists():shutil.copy2(output,args.results/f'FAILED_{case}.json')
                    metadata.update(completed=False,failed_case=case);write_json(args.results/'metadata.json',metadata)
                    raise
                if profile.startswith('skerry_'):
                    assert result['config']['batch_delay_ms']==delay,(case,'effective delay differs from plan')
                result.update(dataset=name,profile=profile,trial=trial+1,write_pattern=pattern)
                result['filesystem']=filesystem_usage(store)
                expected=manifests[name]['records']+(manifests[name]['records']+3)//4
                actual=result['storage'].get('objects',result['storage'].get('total_stored_blobs'))
                assert actual==expected,(case,actual,expected)
                assert all(phase['operations']==manifests[name]['records'] and phase['error_count']==0 for phase in result['phases']),case
                write_json(args.results/'raw'/f'{case}.json',result)
                log=output.with_suffix('.log')
                if log.stat().st_size:shutil.copy2(log,args.results/'raw'/f'{case}.log')
                summarize(args.results)
                shutil.rmtree(case_root)
        metadata['completed_utc']=datetime.now(timezone.utc).isoformat();metadata['completed_cases']=done;metadata['completed']=True;write_json(args.results/'metadata.json',metadata)
    finally:
        if not args.keep_work:shutil.rmtree(work)

def parser():
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='command',required=True)
    run=sub.add_parser('run');run.add_argument('--results',type=Path,required=True);run.add_argument('--pelagia-root',type=Path,default=ROOT.parent/'Pelagia');run.add_argument('--records',type=int,default=1024);run.add_argument('--batch-delay-ms',type=int,default=1);run.add_argument('--trials',type=int,default=3);run.add_argument('--workers',type=int,default=8);run.add_argument('--datasets',nargs='+',choices=['5k','20k','32k','128k','mixed','1m','hotprefix'],default=['5k','20k','32k','128k','mixed','1m','hotprefix']);run.add_argument('--profiles',nargs='+',choices=['kv2_current','kv2_full_verify','kv2_stream','skerry_engine','skerry_coordinator','skerry_cached','skerry_http','skerry_coordinator_delay0']);run.add_argument('--work-parent',type=Path,default=ROOT);run.add_argument('--keep-work',action='store_true');run.add_argument('--case-timeout',type=int,default=600)
    worker=sub.add_parser('worker');worker.add_argument('--profile',required=True);worker.add_argument('--store',type=Path,required=True);worker.add_argument('--fixture',type=Path,required=True);worker.add_argument('--output',type=Path,required=True);worker.add_argument('--pelagia-root',type=Path,required=True);worker.add_argument('--workers',type=int,default=8);worker.add_argument('--write-pattern',choices=['sequential','concurrent','batch64'],default='sequential');worker.add_argument('--payload-cache-mib',type=int,default=0);worker.add_argument('--batch-delay-ms',type=int,default=10)
    summary=sub.add_parser('summarize');summary.add_argument('--results',type=Path,required=True)
    return p
if __name__=='__main__':
    args=parser().parse_args()
    if args.command=='worker':python_worker(args)
    elif args.command=='summarize':summarize(args.results)
    else:
        if args.records<=0 or args.trials<=0 or not 1<=args.workers<=64:raise ValueError('records/trials must be positive; workers 1..64')
        orchestrate(args)
