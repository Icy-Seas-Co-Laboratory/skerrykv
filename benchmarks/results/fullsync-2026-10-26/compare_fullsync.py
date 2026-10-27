#!/usr/bin/env python3
"""macOS-only supplemental flush-strength experiment, after the primary run.
No production file is edited. Changes to fsync/SQLite are confined to worker processes.
"""
import argparse, importlib.util, json, os, pathlib, random, shutil, sqlite3, subprocess, sys, tempfile
from datetime import datetime, timezone

HERE=pathlib.Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('comparison',HERE/'compare_pelagia.py')
comparison=importlib.util.module_from_spec(spec);spec.loader.exec_module(comparison)

def worker(args):
    import fcntl
    if sys.platform!='darwin' or not hasattr(fcntl,'F_FULLFSYNC'):raise RuntimeError('requires macOS F_FULLFSYNC')
    variant=args.variant;observed={'python_fullfsync_calls':0,'sqlite_fullfsync_connections':0}
    if variant=='kv2_macos_fullsync_verify':
        # These are worker-local benchmark adaptations, never product changes.
        original_connect=sqlite3.connect
        def connect(*positional,**keywords):
            connection=original_connect(*positional,**keywords)
            connection.execute('PRAGMA fullfsync=ON')
            if connection.execute('PRAGMA fullfsync').fetchone()[0]!=1:raise RuntimeError('SQLite refused fullfsync')
            observed['sqlite_fullfsync_connections']+=1
            return connection
        sqlite3.connect=connect
        def fullsync(descriptor):
            result=fcntl.fcntl(descriptor,fcntl.F_FULLFSYNC)
            observed['python_fullfsync_calls']+=1
            return result
        os.fsync=fullsync
        check=args.store.parent/'pragma-validation.sqlite'
        connection=connect(check)
        connection.execute('PRAGMA journal_mode=WAL');connection.execute('PRAGMA synchronous=FULL')
        flags={key:connection.execute('PRAGMA '+key).fetchone()[0]for key in ['journal_mode','synchronous','fullfsync']}
        connection.close()
        if flags!={'journal_mode':'wal','synchronous':2,'fullfsync':1}:raise RuntimeError(flags)
        observed['validation_pragmas']=flags
    args.profile='kv2_full_verify';args.payload_cache_mib=0;args.batch_delay_ms=10
    comparison.python_worker(args)
    result=json.loads(args.output.read_text());result['engine']=variant
    result['config'].update(blob_sync='fcntl(F_FULLFSYNC)' if variant=='kv2_macos_fullsync_verify' else 'os.fsync',sqlite_fullfsync=variant=='kv2_macos_fullsync_verify',benchmark_only_override=True)
    result['control_observations']=observed
    comparison.write_json(args.output,result)

def main(args):
    if sys.platform!='darwin':raise RuntimeError('supplemental experiment is macOS-specific')
    args.results.mkdir(parents=True,exist_ok=False);(args.results/'raw').mkdir()
    work=pathlib.Path(tempfile.mkdtemp(prefix='kv2-fullsync-',dir=comparison.ROOT))
    try:
        manifest=comparison.fixture(work/'fixture','20k',args.records,comparison.SEED+1)
        comparison.write_json(args.results/'fixtures.json',{'20k':manifest})
        metadata={'started_utc':datetime.now(timezone.utc).isoformat(),'records':args.records,'trials':args.trials,'workers':args.workers,'benchmark_only_adaptations':['SQLite synchronous=FULL','SQLite fullfsync=ON','replace Python os.fsync with fcntl(F_FULLFSYNC) inside isolated child','BLAKE3 verification after each ordinary read'],'primary_harness_sha256':comparison.source_hash(HERE/'compare_pelagia.py'),'supplement_sha256':comparison.source_hash(__file__),'pelagia_source_sha256':comparison.source_hash(args.pelagia_root/'Pelagia/storage/kvstore2.py')}
        comparison.write_json(args.results/'metadata.json',metadata)
        for trial in range(1,args.trials+1):
            cases=[(profile,pattern)for profile in ['kv2_full_verify','kv2_macos_fullsync_verify','skerry_engine']for pattern in ['sequential','concurrent']]
            random.Random(comparison.SEED+trial).shuffle(cases)
            for profile,pattern in cases:
                name=f'20k__{profile}__{pattern}__trial{trial}'
                case=work/name;case.mkdir();output=case/'output.json'
                print(name,flush=True)
                if profile=='skerry_engine':
                    command=[str(comparison.ROOT/'target/release/examples/compare_kvstore2'),'--mode','engine','--store',str(case/'store'),'--fixture',str(work/'fixture'),'--output',str(output),'--workers',str(args.workers),'--write-pattern',pattern]
                else:
                    command=[sys.executable,str(pathlib.Path(__file__).resolve()),'worker','--variant',profile,'--store',str(case/'store'),'--fixture',str(work/'fixture'),'--output',str(output),'--pelagia-root',str(args.pelagia_root),'--workers',str(args.workers),'--write-pattern',pattern]
                try:result=comparison.command_result(command,output,1200)
                except Exception:
                    log=output.with_suffix('.log')
                    if log.exists():shutil.copy2(log,args.results/f'FAILED_{name}.log')
                    raise
                result.update(dataset='20k',profile=profile,trial=trial,write_pattern=pattern)
                result['filesystem']=comparison.filesystem_usage(case/'store')
                assert all(p['operations']==args.records and p['error_count']==0 for p in result['phases'])
                objects=result['storage'].get('objects',result['storage'].get('total_stored_blobs'))
                assert objects==args.records+(args.records+3)//4
                comparison.write_json(args.results/'raw'/f'{name}.json',result)
                comparison.summarize(args.results)
                shutil.rmtree(case)
        metadata.update(completed=True,completed_cases=args.trials*6,completed_utc=datetime.now(timezone.utc).isoformat());comparison.write_json(args.results/'metadata.json',metadata)
    finally:shutil.rmtree(work)

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='command',required=True)
    run=sub.add_parser('run');run.add_argument('--results',type=pathlib.Path,required=True);run.add_argument('--records',type=int,default=256);run.add_argument('--trials',type=int,default=3);run.add_argument('--workers',type=int,default=8);run.add_argument('--pelagia-root',type=pathlib.Path,default=comparison.ROOT.parent/'Pelagia')
    child=sub.add_parser('worker');child.add_argument('--variant',choices=['kv2_full_verify','kv2_macos_fullsync_verify'],required=True);child.add_argument('--store',type=pathlib.Path,required=True);child.add_argument('--fixture',type=pathlib.Path,required=True);child.add_argument('--output',type=pathlib.Path,required=True);child.add_argument('--pelagia-root',type=pathlib.Path,required=True);child.add_argument('--workers',type=int,default=8);child.add_argument('--write-pattern',choices=['sequential','concurrent'],required=True)
    args=parser.parse_args();worker(args) if args.command=='worker' else main(args)
