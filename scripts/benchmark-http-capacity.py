#!/usr/bin/env python3
"""Repeat isolated HTTP load scenarios and evaluate explicit regression budgets."""
import argparse
import json
import math
import os
import signal
import shutil
import tempfile
from pathlib import Path
import platform
import random
import statistics
import subprocess
import time


def scenario(name, workload="batch-put", workers=6, payload=4096, batch=250, **overrides):
    return dict(name=name, workload=workload, workers=workers, payload_bytes=payload, batch_items=batch, **overrides)


def cases(profile, workers=(4, 8, 16, 32, 64), writer_lanes=(1, 2, 4, 8)):
    if profile == "writer-stress":
        return [scenario(f"{mode}-c{count}-w{lane}", mode, count, 4096, 1,
                         fixture_items=1, writer_lanes=lane)
                for mode in ("duplicate-put", "skew-put")
                for count in workers for lane in writer_lanes]
    if profile == "buffered":
        return [scenario(f"buffered{payload}-c{count}-w{lane}", "batch-put", count,
                         payload, batch, client_memory_mib=1024, writer_lanes=lane)
                for payload, batch in ((32768, 64), (131072, 16))
                for count in workers for lane in writer_lanes]
    if profile == "smoke":
        return [scenario(f"batch-c4-w{lane}", workers=4, batch=8, writer_lanes=lane) for lane in writer_lanes] + [scenario(f"mixed-c4-w{lane}", "mixed", 4, batch=8, writer_lanes=lane) for lane in writer_lanes]
    if profile == "cache":
        return [scenario(f"cache{cache}-c{count}-w{lane}", "random-get", count, 32768, 1, cache_mib=cache, fixture_items=16384, client_memory_mib=1024, writer_lanes=lane) for cache in (256,1024,4096,8192) for count in workers for lane in writer_lanes]
    modes = [("unique-put", "single", 4096, 1), ("batch-put", "batch", 4096, 250),
             ("mixed", "mixed", 32768, 1), ("random-get", "random-get", 32768, 1),
             ("hot-get", "hot-get", 4096, 1)]
    selected = [scenario(f"{label}-c{count}-w{lane}", mode, count, payload, batch,
                         writer_lanes=lane, **({"cache_mib": 64} if mode == "hot-get" else {}))
                for mode, label, payload, batch in modes for count in workers for lane in writer_lanes]
    if profile == "pelagia":
        for count in workers:
            selected.extend([scenario(f"batch-c{count}-w{lane}-b32", workers=count, batch=32, writer_lanes=lane) for lane in writer_lanes] +
                             [scenario(f"batch-c{count}-w{lane}-maxobj4", workers=count, max_object_mib=4, writer_lanes=lane) for lane in writer_lanes] +
                             [scenario(f"batch-c{count}-w{lane}-requests512", workers=count, max_requests=512, writer_lanes=lane) for lane in writer_lanes])
    if profile == "sweep":
        selected = [scenario(f"{mode}-c{count}-b{payload}-w{lane}", mode, count, payload,
                             batch=(250 if payload <= 4096 else 64 if payload <= 32768 else 32) if mode == "batch-put" else 1, writer_lanes=lane)
                    for mode in ("unique-put", "batch-put", "mixed", "random-get", "hot-get")
                    for count in workers for payload in (1024, 4096, 32768, 131072) for lane in writer_lanes]
    return selected


def percentile_from(raw):
    latency = raw.get("latency", {}).get("success_request_ms", raw.get("latency", {}).get("request_ms", {}))
    return latency.get("p99", latency.get("p99_ms"))


def summarize(rows):
    throughput = [r["summary"]["item_ops_per_second"] for r in rows]
    p99 = [percentile_from(r) for r in rows]
    p99 = [x for x in p99 if x is not None]
    observations = {}
    for name in ("status", "metrics", "ready"):
        probes = [row.get("probes", {}).get(name, {}) for row in rows]
        attempts = sum(p.get("attempts", p.get("count", 0)) for p in probes)
        failures = sum(p.get("failures", p.get("http_failures", 0)) for p in probes)
        missing = sum(p.get("missing_storage", p.get("status_200_missing_storage", 0)) for p in probes)
        observations[name] = {"attempts": attempts, "failures": failures,
                              "missing_storage": missing,
                              "unavailable_fraction": (failures + missing) / attempts if attempts else None}
    return {"runs": len(rows), "item_ops_per_second": {"median": statistics.median(throughput), "min": min(throughput), "max": max(throughput), "mad": statistics.median(abs(x - statistics.median(throughput)) for x in throughput)},
            "request_p99_ms_median": statistics.median(p99) if p99 else None,
            "unknown_items": sum(row["summary"].get("unknown_items", 0) for row in rows),
            "rejected_items": sum(row["summary"].get("rejected_items", 0) for row in rows),
            "store_limited_runs": sum(bool(row["summary"].get("store_limited", False)) for row in rows),
            "probes": observations}


def check_budgets(report, baseline, args):
    failures = []
    for name, case in report["cases"].items():
        summary = case["summary"]
        if args.require_no_rejections and (summary["rejected_items"] or summary["unknown_items"]):
            failures.append(f"{name}: rejected or unknown item outcomes")
        if args.max_probe_failure_fraction is not None:
            for route in ("status", "metrics", "ready"):
                value = summary["probes"][route]["unavailable_fraction"]
                if value is None:
                    failures.append(f"{name}: no {route} samples")
                elif value > args.max_probe_failure_fraction:
                    failures.append(f"{name}: {route} unavailable fraction {value:.3f}")
        if summary["store_limited_runs"]:
            failures.append(f"{name}: store budget ended a run before its full duration; increase cap or shorten run")
        if baseline is None:
            continue
        previous = baseline.get("cases", {}).get(name)
        if previous is None:
            failures.append(f"{name}: missing baseline case")
            continue
        # Compare only identical workloads/settings and duration. Hardware/filesystem
        # equivalence is still the operator's responsibility, recorded in reports.
        if previous["settings"] != case["settings"] or baseline["seconds"] != report["seconds"] or baseline.get("warmup_seconds") != report["warmup_seconds"] or baseline.get("max_store_mib") != report["max_store_mib"]:
            failures.append(f"{name}: baseline configuration/duration mismatch")
            continue
        old = previous["summary"]
        new_rate, old_rate = summary["item_ops_per_second"]["median"], old["item_ops_per_second"]["median"]
        if new_rate < old_rate * (1 - args.max_throughput_regression_percent / 100):
            failures.append(f"{name}: throughput regressed beyond budget ({old_rate:.1f} -> {new_rate:.1f})")
        new_p99, old_p99 = summary["request_p99_ms_median"], old["request_p99_ms_median"]
        if new_p99 is not None and old_p99 is not None and new_p99 > old_p99 * (1 + args.max_p99_regression_percent / 100):
            failures.append(f"{name}: p99 latency regressed beyond budget ({old_p99:.2f} -> {new_p99:.2f} ms)")
    return failures


def terminate_group(process, grace_seconds=5):
    """Terminate the owned harness AND its server, even if the harness exits first."""
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        process.wait()
        return
    except PermissionError:
        if process.poll() is not None:
            return
        raise
    deadline = time.monotonic() + grace_seconds
    while time.monotonic() < deadline:
        # Keep the direct child unreaped until group termination is finished;
        # its PID cannot then be reused for an unrelated process group.
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            break
        except PermissionError:
            # macOS can report EPERM for a group whose last process has
            # exited. Only accept it after our direct child is confirmed dead.
            if process.poll() is not None:
                break
            raise
        time.sleep(0.05)
    else:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--harness", type=Path, default=Path("target/release/examples/http_capacity_bench"))
    parser.add_argument("--store-parent", type=Path, required=True, help="Disposable benchmark parent on the filesystem to test")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", choices=("smoke", "quick", "pelagia", "sweep", "cache", "writer-stress", "buffered"), default="quick")
    parser.add_argument("--seconds", type=int)
    parser.add_argument("--workers", type=int, nargs="+", default=[4, 8, 16, 32, 64], help="Concurrent client workers per scenario")
    parser.add_argument("--writer-lanes", type=int, nargs="+", default=[1, 2, 4, 8], help="Append lane counts to compare")
    parser.add_argument("--commit-delay-ms", type=int, help="Override writes.commit_max_delay_ms; omitted by default so matrix settings stay matched across binary versions")
    parser.add_argument("--legacy-config", action="store_true", help="Emit a 0.2.2-compatible server config for baseline executables")
    parser.add_argument("--baseline-config-0-3", action="store_true", help="Strip writes.writer_lanes for the pre-lane 0.3 binary schema")
    parser.add_argument("--keep-stores", action="store_true", help="Retain owned test stores for diagnosis")
    parser.add_argument("--warmup-seconds", type=int, default=1)
    parser.add_argument("--repeats", type=int)
    parser.add_argument("--seed", type=int, default=20261029)
    parser.add_argument("--max-store-mib", type=int, default=512)
    parser.add_argument("--case", action="append", help="Run only these named cases")
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--max-throughput-regression-percent", type=float, default=15)
    parser.add_argument("--max-p99-regression-percent", type=float, default=25)
    parser.add_argument("--max-probe-failure-fraction", type=float)
    parser.add_argument("--require-no-rejections", action="store_true")
    args = parser.parse_args()
    if os.name != "posix":
        parser.error("this orchestrator requires POSIX process groups; run the Rust harness directly on other platforms")
    if not args.workers or any(worker < 1 or worker > 256 for worker in args.workers) or len(set(args.workers)) != len(args.workers):
        parser.error("workers must be distinct values in 1..256")
    if not args.writer_lanes or any(lane not in (1, 2, 4, 8) for lane in args.writer_lanes) or len(set(args.writer_lanes)) != len(args.writer_lanes):
        parser.error("writer lanes must be distinct values from 1, 2, 4, 8")
    if args.legacy_config and args.baseline_config_0_3:
        parser.error("choose one compatibility schema")
    if (args.legacy_config or args.baseline_config_0_3) and args.commit_delay_ms is not None:
        parser.error("compatibility baselines do not support a commit-delay override")
    if (args.legacy_config or args.baseline_config_0_3) and args.writer_lanes != [1]:
        parser.error("compatibility baselines only support --writer-lanes 1")
    if args.commit_delay_ms is not None and not 0 <= args.commit_delay_ms <= 1000:
        parser.error("commit delay must be in 0..1000 ms")
    seconds = args.seconds if args.seconds is not None else {"smoke": 1, "quick": 3, "pelagia": 10, "sweep": 10, "cache": 5, "writer-stress": 10, "buffered": 3}[args.profile]
    repeats = args.repeats if args.repeats is not None else {"smoke": 1, "quick": 2, "pelagia": 3, "sweep": 3, "cache": 1, "writer-stress": 3, "buffered": 2}[args.profile]
    if not 1 <= seconds <= 3600 or not 1 <= repeats <= 20 or not 0 <= args.warmup_seconds <= 300 or not 1 <= args.max_store_mib <= 1048576:
        parser.error("duration/repeats/warmup/store budget outside supported bounds")
    if args.max_probe_failure_fraction is not None and not 0 <= args.max_probe_failure_fraction <= 1:
        parser.error("probe failure fraction must be in [0,1]")
    if any(not math.isfinite(value) or value < 0 for value in (args.max_throughput_regression_percent, args.max_p99_regression_percent)):
        parser.error("regression budgets must be finite and nonnegative")
    selected = cases(args.profile, args.workers, args.writer_lanes)
    if args.commit_delay_ms is not None:
        for case in selected:
            case["commit_delay_ms"] = args.commit_delay_ms
    if args.case:
        unknown = set(args.case) - {case["name"] for case in selected}
        if unknown:
            parser.error(f"unknown cases: {sorted(unknown)}")
        selected = [case for case in selected if case["name"] in args.case]
    args.output = args.output.resolve()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    raw_dir = args.output.parent / (args.output.stem + "-runs")
    raw_dir.mkdir(exist_ok=True)
    args.store_parent.mkdir(parents=True, exist_ok=True)
    report = {"schema_version": 1, "profile": args.profile, "seconds": seconds, "repeats": repeats,
              "warmup_seconds": args.warmup_seconds, "workers": args.workers, "writer_lanes": args.writer_lanes, "commit_delay_ms_override": args.commit_delay_ms, "max_store_mib": args.max_store_mib, "legacy_config": args.legacy_config, "baseline_config_0_3": args.baseline_config_0_3, "seed": args.seed, "runner_platform": platform.platform(),
              "method": "randomized case order within each repetition; isolated store per run; successful item throughput includes drain; closed-loop clients, no coordinated-omission correction; no automatic retries",
              "cases": {}, "errors": []}
    rng = random.Random(args.seed)
    for repetition in range(repeats):
        order = selected.copy()
        rng.shuffle(order)
        for case in order:
            name = case["name"]
            output = raw_dir / f"{name}-{repetition + 1}.json"
            # Never accept a previous report as proof that a new run completed.
            if output.exists():
                output.unlink()
            required_free = (2 * args.max_store_mib + 1024) * 1024 * 1024
            available = shutil.disk_usage(args.store_parent).free
            if available < required_free:
                report["errors"].append({"case": name, "repeat": repetition + 1, "error": f"insufficient free space: {available} bytes available, {required_free} required for payload budget and headroom"})
                args.output.write_text(json.dumps(report, indent=2) + "\n")
                continue
            run_parent = Path(tempfile.mkdtemp(prefix="skerry-capacity-run-", dir=args.store_parent.resolve()))
            command = [str(args.harness.resolve()), "--binary", str(args.binary.resolve()), "--store-parent", str(run_parent), "--output", str(output), "--seconds", str(seconds), "--warmup-seconds", str(args.warmup_seconds), "--max-store-mib", str(args.max_store_mib), "--seed", str(args.seed + repetition)]
            if args.keep_stores:
                command.append("--keep-store")
            if args.legacy_config:
                command.append("--legacy-config")
            if args.baseline_config_0_3:
                command.append("--baseline-config-0-3")
            for key, value in case.items():
                if key != "name":
                    command.extend(["--" + key.replace("_", "-"), str(value)])
            started = time.monotonic()
            process = None
            try:
                process = subprocess.Popen(command, stdout=subprocess.DEVNULL, start_new_session=True)
                try:
                    returncode = process.wait(timeout=seconds + args.warmup_seconds + 180)
                except subprocess.TimeoutExpired:
                    terminate_group(process)
                    raise
                if returncode:
                    raise subprocess.CalledProcessError(returncode, command)
                row = json.loads(output.read_text())
                entry = report["cases"].setdefault(name, {"settings": {key: value for key, value in case.items() if key != "name"}, "runs": []})
                entry["runs"].append(row)
                entry["summary"] = summarize(entry["runs"])
                print(f"{name} repeat {repetition + 1}/{repeats}: {row['summary']['item_ops_per_second']:.1f} item ops/s ({time.monotonic() - started:.1f}s)", flush=True)
            except (subprocess.SubprocessError, OSError, ValueError, KeyError) as error:
                report["errors"].append({"case": name, "repeat": repetition + 1, "error": str(error)})
            finally:
                if process is not None and process.poll() is None:
                    terminate_group(process)
                if not args.keep_stores:
                    shutil.rmtree(run_parent)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
    baseline = json.loads(args.baseline.read_text()) if args.baseline else None
    report["budget_failures"] = check_budgets(report, baseline, args)
    report["completed"] = not report["errors"] and len(report["cases"]) == len(selected) and all(len(case["runs"]) == repeats for case in report["cases"].values())
    report["passed"] = report["completed"] and not report["budget_failures"]
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    if not report["passed"]:
        raise SystemExit("Benchmark reports written; one or more run errors or configured budget failures occurred.")


if __name__ == "__main__":
    main()
