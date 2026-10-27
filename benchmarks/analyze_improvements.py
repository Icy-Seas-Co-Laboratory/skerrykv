#!/usr/bin/env python3
"""Validate and compare the preserved Pelagia benchmark before and after changes."""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import statistics

ROOT = pathlib.Path(__file__).resolve().parents[1]
BASELINE = ROOT / "benchmarks/results/pelagia-2026-10-26"
AFTER = ROOT / "benchmarks/results/pelagia-after-2026-10-26"
KV_FINAL = ROOT / "benchmarks/results/kv2-after-lock-scope-2026-10-26"


def read(path: pathlib.Path):
    return json.loads(path.read_text())


def validate_snapshot_sources(directory: pathlib.Path) -> None:
    """Check old metadata hashes against archived bytes, never mutable live files."""
    metadata = read(directory / "metadata.json")
    index = read(directory / "sources/index.json")
    for original_path, snapshot_name in index.items():
        assert original_path in metadata["sources"], original_path
        digest = hashlib.sha256((directory / "sources" / snapshot_name).read_bytes()).hexdigest()
        assert digest == metadata["sources"][original_path], (original_path, snapshot_name)
    assert set(index) == set(metadata["sources"])


def validate_baseline(directory: pathlib.Path, expected_cases=141) -> dict:
    """Validate raw phases and the summary medians without checking old hashes live."""
    metadata = read(directory / "metadata.json")
    fixtures = read(directory / "fixtures.json")
    files = sorted((directory / "raw").glob("*.json"))
    assert metadata.get("completed") is True and len(files) == metadata["completed_cases"] == expected_cases
    groups: dict[tuple, list] = {}
    phase_count = operation_count = 0
    for path in files:
        trial = read(path)
        manifest = fixtures[trial["dataset"]]
        lengths, count = manifest["lengths"], manifest["records"]
        assert len(trial["phases"]) == 9, path
        for phase in trial["phases"]:
            assert phase["error_count"] == 0 and phase["operations"] == count, (path, phase["name"])
            expected_bytes = sum(lengths[i] for i in manifest["hot_order"]) if phase["name"] in ("write_duplicate", "read_hot") else sum(lengths)
            assert phase["logical_bytes"] == expected_bytes, (path, phase["name"])
            assert abs(count / phase["elapsed_seconds"] - phase["ops_per_second"]) < count / phase["elapsed_seconds"] * 1e-9
            key = (trial["dataset"], trial["profile"], trial["write_pattern"], phase["name"])
            groups.setdefault(key, []).append(phase)
            phase_count += 1
            operation_count += count
        mixed = trial["phases"][-1]
        assert mixed["name"] == "mixed_read_write"
        assert mixed["write_operations"] == len(manifest["mixed_write_positions"])
        assert mixed["read_operations"] + mixed["write_operations"] == count
        assert mixed["read_logical_bytes"] + mixed["write_logical_bytes"] == sum(lengths)
        storage = trial["storage"]
        objects = storage.get("objects", storage.get("total_stored_blobs"))
        payload = storage.get("payload_bytes", storage.get("total_stored_payload_bytes"))
        assert objects == count + len(manifest["mixed_write_positions"])
        assert payload == sum(lengths) + sum(lengths[manifest["mixed_order"][p]] for p in manifest["mixed_write_positions"])
    rows = read(directory / "summary.json")
    assert len(rows) == len(groups)
    for row in rows:
        key = tuple(row[k] for k in ("dataset", "profile", "write_pattern", "phase"))
        samples = groups.pop(key)
        assert len(samples) == row["trials"] == metadata["trials"] == 3
        for metric in ("ops_per_second", "elapsed_seconds", "latency_p50_us", "latency_p95_us", "latency_p99_us"):
            assert row[metric] == statistics.median(p[metric] for p in samples), (key, metric)
        assert all(p["latency_unit"] == row["latency_unit"] for p in samples)
    assert not groups
    validate_snapshot_sources(directory)
    return {"completed_cases": len(files), "validated_phases": phase_count, "validated_operations": operation_count, "errors": 0}


def render(output: pathlib.Path) -> None:
    before_meta, after_meta = read(BASELINE / "metadata.json"), read(AFTER / "metadata.json")
    assert before_meta["completed"] is True and after_meta.get("completed") is True
    assert before_meta["completed_cases"] == after_meta["completed_cases"] == 141
    assert before_meta["trials"] == after_meta["trials"] == 3
    assert before_meta["python"] == after_meta["python"] and before_meta["rustc"] == after_meta["rustc"]
    assert before_meta["cpu"] == after_meta["cpu"] and before_meta["memory_bytes"] == after_meta["memory_bytes"]
    before_fixtures, after_fixtures = read(BASELINE / "fixtures.json"), read(AFTER / "fixtures.json")
    assert before_fixtures == after_fixtures, "fixture manifests differ"
    baseline_audit = validate_baseline(BASELINE)
    after_audit = validate_baseline(AFTER)
    kv_audit = validate_baseline(KV_FINAL, expected_cases=48)
    assert read(KV_FINAL / "fixtures.json") == before_fixtures
    kv_meta = read(KV_FINAL / "metadata.json")
    for field in ("python", "sqlite", "blake3", "rustc", "cpu", "memory_bytes"):
        assert kv_meta[field] == after_meta[field] == before_meta[field]
    before_rows, after_rows = read(BASELINE / "summary.json"), read(AFTER / "summary.json")
    before = {(r["dataset"], r["profile"], r["write_pattern"], r["phase"]): r for r in before_rows}
    after = {(r["dataset"], r["profile"], r["write_pattern"], r["phase"]): r for r in after_rows}

    preliminary = after.copy()
    after.update({(r["dataset"], r["profile"], r["write_pattern"], r["phase"]): r
                  for r in read(KV_FINAL / "summary.json")})

    def row(data, dataset, profile, pattern, phase):
        return data[(dataset, profile, pattern, phase)]

    def n(value):
        return f"{value:,.0f}"

    def ms(us):
        return f"{us / 1000:.3f}"

    def compare_table(title, cases, p95=True):
        lines = [f"### {title}", "", "| Case | Before ops/s | After ops/s | After / before | Before p95 ms | After p95 ms |", "|---|---:|---:|---:|---:|---:|"]
        for label, dataset, profile, pattern, phase in cases:
            a, b = row(before, dataset, profile, pattern, phase), row(after, dataset, profile, pattern, phase)
            lines.append(f"| {label} | {n(a['ops_per_second'])} | {n(b['ops_per_second'])} | {b['ops_per_second']/a['ops_per_second']:.2f}× | {ms(a['latency_p95_us'])} | {ms(b['latency_p95_us'])} |")
        return lines

    lines = [
        "# SkerryKV and Pelagia performance improvements", "",
        f"Measured on {before_meta['cpu']} ({before_meta['platform']}) with {int(before_meta['memory_bytes']):,} bytes RAM and the same APFS SSD. The runtime versions match: Python `{before_meta['python']}` and `{before_meta['rustc']}`. The original and main updated suites use identical fixture manifests and seeds, 141 cases each, three randomized serial trials, and report trial medians. A final 48-case KV2-only suite measures the narrower cache lock scope with the same fixture manifests, three trials, and runtime. All KV2 current rows below use that final suite; all Skerry rows use the main updated suite. The suites ran sequentially, so environmental drift remains possible.", "",
        "Reads use an explicit warmup pass. These results describe warm-cache workloads and a small in-memory index. They do not establish Linux HDD cold-cache performance or power-loss behavior. The service batch delay changed from 10 ms before to 1 ms after; this is a deliberate configuration and code change measured together, so its effects cannot be attributed separately.", "",
        "The updated current KV2 profile adds a 16-connection LRU per thread across 256 hash prefixes, with explicit shutdown cleanup and pruning of finished-thread caches. SQLite open, PRAGMAs, and close/checkpoint work execute outside the cache-wide condition; active leases keep shutdown waiting until this work finishes. It keeps WAL/NORMAL and the existing ordinary-read integrity behavior. The cache has low potential hit rate for uniform access across all prefixes. The full-verify benchmark override bypasses the cache and is not evidence of a production KV2 improvement.", "",
        "## KV2 current profile by payload size", "",
        "Random read, hot-set read, and mixed workload rates (operations/s):", "",
        "| Payload | Random before | Random after | Hot before | Hot after | Mixed before | Mixed after |", "|---|---:|---:|---:|---:|---:|---:|"
    ]
    bulk_before=before[("20k","skerry_http","batch64","write_batch64")]["ops_per_second"]
    bulk_after=after[("20k","skerry_http","batch64","write_batch64")]["ops_per_second"]
    mixed_before=before[("20k","skerry_http","sequential","mixed_read_write")]["ops_per_second"]
    mixed_after=after[("20k","skerry_http","sequential","mixed_read_write")]["ops_per_second"]
    lines.insert(2, f"At 20 KiB, HTTP bulk ingestion increased from {n(bulk_before)} to {n(bulk_after)} objects/s ({bulk_after/bulk_before:.1f}×), and HTTP mixed throughput from {n(mixed_before)} to {n(mixed_after)} ops/s ({mixed_after/mixed_before:.1f}×). Final KV2 results below include the narrower-lock refinement; rows report regressions as well as gains.")
    lines.insert(3, "")
    for dataset, label in (("5k", "5 KiB"), ("20k", "20 KiB"), ("32k", "32 KiB"), ("128k", "128 KiB"), ("1m", "1 MiB"), ("mixed", "Mixed sizes")):
        vals = [row(d, dataset, "kv2_current", "sequential", phase)["ops_per_second"] for d in (before, after) for phase in ("read_random", "read_hot", "mixed_read_write")]
        lines.append(f"| {label} | {n(vals[0])} | {n(vals[3])} | {n(vals[1])} | {n(vals[4])} | {n(vals[2])} | {n(vals[5])} |")
    lines += ["", "## 20 KiB workloads", ""]
    kv_cases = [("Single write", "20k", "kv2_current", "sequential", "write_sequential"), ("Concurrent writes", "20k", "kv2_current", "concurrent", "write_concurrent"), ("Random reads", "20k", "kv2_current", "sequential", "read_random"), ("Hot-set reads", "20k", "kv2_current", "sequential", "read_hot"), ("Concurrent random reads", "20k", "kv2_current", "sequential", "read_concurrent_random"), ("Mixed 25% writes / 75% reads", "20k", "kv2_current", "sequential", "mixed_read_write")]
    lines += compare_table("KV2 current", kv_cases) + [""]
    skerry_cases = []
    for profile, name in (("skerry_coordinator", "Coordinator"), ("skerry_http", "HTTP")):
        for pattern, phase, label in (("sequential", "write_sequential", "single write"), ("concurrent", "write_concurrent", "concurrent write"), ("batch64", "write_batch64", "bulk-64"), ("sequential", "write_duplicate", "duplicate write"), ("sequential", "read_random", "random read"), ("sequential", "mixed_read_write", "mixed")):
            skerry_cases.append((f"{name} {label}", "20k", profile, pattern, phase))
    lines += compare_table("Skerry coordinator and HTTP", skerry_cases) + [""]
    sensitivity = [("Coordinator delay=0 single write", "20k", "skerry_coordinator_delay0", "sequential", "write_sequential"), ("Coordinator delay=0 random read", "20k", "skerry_coordinator_delay0", "sequential", "read_random"), ("Coordinator delay=0 mixed", "20k", "skerry_coordinator_delay0", "sequential", "mixed_read_write")]
    lines += compare_table("Coordinator delay=0 sensitivity", sensitivity) + [""]
    controls = [("Direct engine single writes", "20k", "skerry_engine", "sequential", "write_sequential"), ("Direct engine random reads", "20k", "skerry_engine", "sequential", "read_random"), ("Direct engine mixed", "20k", "skerry_engine", "sequential", "mixed_read_write")]
    lines += compare_table("Direct-engine control (unchanged operation paths)", controls) + [
        "", "These direct-engine operations do not use the coordinator fast path or HTTP changes. Changes in these control rates show that before/after ratios also contain environmental or build variation; they are not isolated causal estimates.",
        "", "### KV2 cache lock-scope follow-up", "",
        "| 20 KiB operation | Initial pooled version ops/s | Final narrower-lock version ops/s |",
        "|---|---:|---:|",
    ]
    for phase, label in [("read_concurrent_random", "Eight-client random reads"), ("mixed_read_write", "25% new writes / 75% reads")]:
        a=preliminary[("20k","kv2_current","sequential",phase)]
        b=after[("20k","kv2_current","sequential",phase)]
        lines.append(f"| {label} | {n(a['ops_per_second'])} | {n(b['ops_per_second'])} |")
    lines += [ 
        "", "## Changes and interpretation", "",
        "The after implementation reduces the default service batch delay to 1 ms, pipelines up to 64 HTTP batch writes, reserves actual known upload sizes, and lets healthy, committed duplicates bypass the writer queue. Existing config files keep their values; set `writes.batch_max_delay_ms = 1` explicitly to adopt the tested setting. The suite reports their combined outcome, including single and concurrent writes, duplicate writes, bulk-64, random reads, and mixed traffic. Per-row ratios are computed from the audited suite medians. Individual and bulk write latency use different units: the HTTP bulk percentile measures each item’s arrival from batch start, while individual calls measure call-to-return latency.", "",
        "The HTTP pipeline now handles bounded pending requests with unknown outcomes explicitly. After a storage failure, decoding stops and ordered outcomes are returned for already-queued items when the response remains usable. A missing result leaves its outcome unknown; content-addressed PUT can be safely retried. Benchmark cases require successful replies and exact content validation, and have zero reported operation errors. Review the implementation’s documented retry and error semantics alongside throughput before relying on pipeline results.", "",
        "The KV2 comparisons use the production current profile with the new connection lifecycle. Because each thread retains only 16 shard connections across 256 possible prefixes, uniform random reads have low connection-cache hit potential. Full-verify bypasses that cache and changes integrity work, so it is intentionally excluded from production improvement claims.", "",
        "## Method and limits", "",
        "The harness uses the same deterministic fixture seeds and dataset manifests in both suites. Three randomized serial trials were run on the same hardware/runtime; summary values are medians. Reads are warm after a full read pass, and the dataset fits in RAM. The 1 ms service delay and code path changed together, preventing isolated attribution to delay tuning. This benchmark does not measure Linux HDD cold-cache workloads, power-loss outcomes, or durability parity. Throughput must be interpreted with each implementation’s acknowledgment and synchronization behavior.", "",
        "## Evidence", "",
        f"Baseline audit: `{baseline_audit}`. Main updated audit: `{after_audit}`. Final KV2 audit: `{kv_audit}`.", "",
        "- [Baseline summary](results/pelagia-2026-10-26/summary.json) · [baseline CSV](results/pelagia-2026-10-26/measurements.csv)",
        "- [After summary](results/pelagia-after-2026-10-26/summary.json) · [after CSV](results/pelagia-after-2026-10-26/measurements.csv)",
        "- [Final KV2 summary](results/kv2-after-lock-scope-2026-10-26/summary.json) · [final KV2 CSV](results/kv2-after-lock-scope-2026-10-26/measurements.csv)",
        "- [Benchmark harness instructions](README.md) · [comparison harness](compare_pelagia.py)", ""
    ]
    (KV_FINAL / "audit.json").write_text(json.dumps(kv_audit, indent=2)+"\n")
    def retries(directory, metric):
        return sum(p.get(metric,0) for file in (directory/"raw").glob("*__skerry_http__*.json") for p in read(file)["phases"])
    lines += ["", f"Timed HTTP phases recorded {retries(BASELINE,'http_503_retries'):,} → {retries(AFTER,'http_503_retries'):,} HTTP 503 retries and {retries(BASELINE,'http_transport_retries'):,} → {retries(AFTER,'http_transport_retries'):,} client transport retries. Retry costs are included in the measured operations.", ""]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines))
    print(json.dumps({"report": str(output), "baseline": baseline_audit, "after": after_audit, "final_kv2": kv_audit}, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path, default=ROOT / "benchmarks/IMPROVEMENTS.md")
    args = parser.parse_args()
    render(args.output.resolve())


if __name__ == "__main__":
    main()
