#!/usr/bin/env python3
"""Combine HTTP, cache, and streaming capacity study JSON into upgrade artifacts."""

import argparse
import json
import math
import statistics
from pathlib import Path


PATTERNS = [
    ("unique-put", "unique PUT"),
    ("batch-put", "batch PUT"),
    ("mixed", "mixed"),
    ("random-get", "random GET"),
    ("hot-get", "hot GET"),
]
WORKERS = (4, 8, 16, 32)
CACHE_MIB = (256, 1024, 4096, 8192)


def load(path):
    return json.loads(path.read_text())


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def fmt(value, digits=1):
    return "n/a" if not number(value) else f"{value:,.{digits}f}"


def median(values):
    values = [value for value in values if number(value)]
    return statistics.median(values) if values else None


def http_cases(report):
    return report.get("cases", {}) if isinstance(report, dict) else {}


def worker_counts(*reports):
    counts = {case.get("settings", {}).get("workers")
              for report in reports for case in http_cases(report).values()}
    return tuple(sorted(count for count in counts if isinstance(count, int))) or WORKERS


def select_case(report, pattern, workers, cache=None):
    for name, case in http_cases(report).items():
        settings = case.get("settings", {})
        mode = settings.get("workload")
        if pattern == "unique-put" and mode == "single":
            pass
        elif mode != pattern:
            continue
        if settings.get("workers") != workers:
            continue
        if cache is not None and settings.get("cache_mib") != cache:
            continue
        return name, case
    return None, None


def case_rate(case):
    if not case:
        return None
    summary = case.get("summary", {})
    rate = summary.get("item_ops_per_second")
    if isinstance(rate, dict):
        return rate.get("median")
    return rate if number(rate) else median(
        run.get("summary", {}).get("item_ops_per_second") for run in case.get("runs", [])
    )


def batch_details(case):
    if not case:
        return None
    summary = case.get("summary", {})
    probes = summary.get("probes", {})
    run_rows = case.get("runs", [])
    def probe_unavailable(name):
        p = probes.get(name, {})
        if number(p.get("unavailable_fraction")):
            return p["unavailable_fraction"]
        attempts = sum(r.get("probes", {}).get(name, {}).get("attempts", 0) for r in run_rows)
        failures = sum(r.get("probes", {}).get(name, {}).get("failures", 0) for r in run_rows)
        missing = sum(r.get("probes", {}).get(name, {}).get("missing_storage", 0) for r in run_rows)
        return (failures + missing) / attempts if attempts else None

    if number(summary.get("request_p99_ms_median")):
        p99 = summary["request_p99_ms_median"]
    else:
        p99 = median(r.get("latency", {}).get("request_ms", {}).get("p99_ms") for r in run_rows)
    rejects = summary.get("rejected_items")
    unknown = summary.get("unknown_items")
    if not number(rejects):
        rejects = sum(r.get("summary", {}).get("rejected_items", 0) for r in run_rows)
    if not number(unknown):
        unknown = sum(r.get("summary", {}).get("unknown_items", 0) for r in run_rows)
    return {"p99_ms": p99, "status_unavailable": probe_unavailable("status"),
            "metrics_unavailable": probe_unavailable("metrics"), "rejected_items": rejects,
            "unknown_items": unknown}


def cache_stats(run, field):
    value = run.get(field)
    if not isinstance(value, dict):
        return None
    if isinstance(value.get("raw"), dict):
        value = value["raw"]
    return value


def hit_ratio(stats):
    if not stats:
        return None
    hits, misses = stats.get("hits"), stats.get("misses")
    if not number(hits) or not number(misses) or hits + misses == 0:
        return None
    return hits / (hits + misses)


def measured_hit_ratio(before, final):
    """Return the interval hit ratio from monotonic cumulative counters."""
    if not before or not final:
        return None
    before_hits, before_misses = before.get("hits"), before.get("misses")
    final_hits, final_misses = final.get("hits"), final.get("misses")
    if not all(number(value) for value in (before_hits, before_misses, final_hits, final_misses)):
        return None
    delta_hits, delta_misses = final_hits - before_hits, final_misses - before_misses
    if delta_hits < 0 or delta_misses < 0 or delta_hits + delta_misses == 0:
        return None
    return delta_hits / (delta_hits + delta_misses)


def cache_case_rows(report):
    rows = []
    for size in CACHE_MIB:
        for workers in worker_counts(report):
            name, case = select_case(report, "random-get", workers, size)
            ratios_before, ratios_final, ratios_measured = [], [], []
            for run in case.get("runs", []) if case else []:
                before = cache_stats(run, "cache_stats_before")
                final = cache_stats(run, "cache_stats_final")
                if before is None:
                    before = run.get("cache_stats_before")
                ratios_before.append(hit_ratio(before))
                ratios_final.append(hit_ratio(final))
                ratios_measured.append(measured_hit_ratio(before, final))
            before_ratio, final_ratio = median(ratios_before), median(ratios_final)
            measured_ratio = median(ratios_measured)
            rows.append({"cache_mib": size, "workers": workers, "case": name,
                         "hit_ratio_before": before_ratio, "hit_ratio_final": final_ratio,
                         "measured_hit_ratio": measured_ratio,
                         "summary": case.get("summary") if case else None})
    return rows


def collect_streaming(directory):
    rows = []
    if not directory:
        return rows
    paths = sorted(directory.glob("*.json")) if directory.is_dir() else [directory]
    for path in paths:
        report = load(path)
        if not isinstance(report, dict) or report.get("schema") != "skerry.streaming-capacity.v1":
            continue
        rows.append({"source": str(path), "mode": report.get("mode"),
                     "workers": report.get("workers"), "file_mib": report.get("file_mib"),
                     "throughput_mib_s": report.get("throughput_mib_s"),
                     "first_get_end_to_end_ms": report.get("first_get_full_scan_ms"),
                     "counts": report.get("counts"), "configuration": report.get("configuration"),
                     "binary": report.get("binary"), "binary_blake3": report.get("binary_blake3"),
                     "config_fingerprint": report.get("config_fingerprint"),
                     "measurement_note": report.get("measurement_note")})
    return rows


def settings_metadata(report):
    return {key: report.get(key) for key in (
        "profile", "seconds", "repeats", "warmup_seconds", "workers", "writer_lanes", "max_store_mib",
        "legacy_config", "baseline_config_0_3", "runner_platform", "seed", "passed", "completed", "budget_failures",
        "acceptance_contract", "notes") if key in report}


def run_fingerprints(report):
    fingerprints = set()
    for case in http_cases(report).values():
        for run in case.get("runs", []):
            env = run.get("environment", {})
            if env.get("binary_blake3"):
                fingerprints.add(env["binary_blake3"])
    return sorted(fingerprints)


def config_fingerprints(report):
    return sorted({run["config_fingerprint"] for case in http_cases(report).values()
                   for run in case.get("runs", []) if run.get("config_fingerprint")})


def followup_case(report):
    _, case = select_case(report, "unique-put", 8)
    if not case:
        return None
    summary = case.get("summary", {})
    p99 = summary.get("request_p99_ms_median")
    if not number(p99):
        p99 = median(run.get("latency", {}).get("request_ms", {}).get("p99_ms") for run in case.get("runs", []))
    return {"item_ops_per_second": case_rate(case), "request_p99_ms": p99,
            "rejected_items": summary.get("rejected_items"),
            "unknown_items": summary.get("unknown_items"), "case": case}


def markdown(before, after, cache, streams, followup_before=None, followup_after=None,
             additional_reports=(), title="0.3.0 performance upgrade results"):
    lines = [f"# {title}", "",
             "Generated from the supplied JSON reports. Missing or incomplete cases are shown as `n/a`; no values are inferred.", "",
             "## Study context", "",
             "| Study | Profile / mode | Duration × repeats | Warmup | Store cap | Platform | Passed / budget failures | Binary BLAKE3 fingerprint(s) |",
             "| --- | --- | ---: | ---: | ---: | --- | --- | --- |"]
    for label, report in (("Before", before), ("After", after), ("Cache", cache)):
        if not report:
            continue
        m = settings_metadata(report)
        lines.append(f"| {label} | {m.get('profile', 'n/a')} | {m.get('seconds', 'n/a')} s × {m.get('repeats', 'n/a')} | {m.get('warmup_seconds', 'n/a')} s | {m.get('max_store_mib', 'n/a')} MiB | {m.get('runner_platform', 'n/a')} | {m.get('passed', 'n/a')} / {m.get('budget_failures', 'n/a')} | `{', '.join(run_fingerprints(report)) or 'n/a'}` |")
    lines += ["", "The baseline should identify the 0.2.2 binary with `legacy_config=true`; the candidate is the 0.3.0 binary. See the raw JSON for exact per-case settings and full effective server TOML.", "",
              "## Item throughput, before → after", "",
              "Median successful item operations/s across repetitions. Each workload compares the same worker count.", "",
              "| Pattern | Workers | Before ops/s | After ops/s | Change |", "| --- | ---: | ---: | ---: | ---: |"]
    for pattern, label in PATTERNS:
        for workers in worker_counts(before, after):
            _, bcase = select_case(before, pattern, workers)
            _, acase = select_case(after, pattern, workers)
            b, a = case_rate(bcase), case_rate(acase)
            change = (100 * (a / b - 1)) if number(a) and number(b) and b else None
            lines.append(f"| {label} | {workers} | {fmt(b)} | {fmt(a)} | {fmt(change, 1)}% |")
    lines += ["", "## Batch latency and observability", "", "Rejection and unknown counts are totals across included runs. Probe availability is 1 minus the unavailable fraction; missing status payloads count as unavailable.", "",
              "| Workers | Before p99 ms | After p99 ms | Before status available | After status available | Before metrics available | After metrics available | Before rejected / unknown | After rejected / unknown |", "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for workers in worker_counts(before, after):
        _, bcase = select_case(before, "batch-put", workers)
        _, acase = select_case(after, "batch-put", workers)
        b, a = batch_details(bcase), batch_details(acase)
        def avail(row, key):
            x = row.get(key) if row else None
            return fmt(100 * (1 - x), 1) + "%" if number(x) else "n/a"
        def counts(row):
            return "n/a" if not row else f"{row['rejected_items']} / {row['unknown_items']}"
        lines.append(f"| {workers} | {fmt(b.get('p99_ms') if b else None)} | {fmt(a.get('p99_ms') if a else None)} | {avail(b, 'status_unavailable')} | {avail(a, 'status_unavailable')} | {avail(b, 'metrics_unavailable')} | {avail(a, 'metrics_unavailable')} | {counts(b)} | {counts(a)} |")
    if followup_before is not None or followup_after is not None:
        fb, fa = followup_case(followup_before or {}), followup_case(followup_after or {})
        lines += ["", "## Focused longer single-write follow-up", "",
                  "This separately supplied longer run targets the isolated short-window single-write p99 stall. It does not replace or erase the short-run comparison or its budget failure. Acceptance status is the source report’s overall status; it remains unavailable until a report is supplied.", "",
                  "| Study | Duration × repeats | Passed | Budget failures | Single-write ops/s | Request p99 ms | Rejected / unknown items |", "| --- | ---: | --- | --- | ---: | ---: | ---: |"]
        for label, report, row in (("Follow-up before", followup_before, fb), ("Follow-up after", followup_after, fa)):
            metadata = settings_metadata(report or {})
            failures = metadata.get("budget_failures")
            failure_text = "n/a" if failures is None else (str(failures) if failures else "none")
            rejected = row.get("rejected_items") if row else None
            unknown = row.get("unknown_items") if row else None
            counts = f"{rejected} / {unknown}" if number(rejected) and number(unknown) else "n/a"
            lines.append(f"| {label} | {metadata.get('seconds', 'n/a')} s × {metadata.get('repeats', 'n/a')} | {metadata.get('passed', 'n/a')} | {failure_text} | {fmt(row.get('item_ops_per_second') if row else None)} | {fmt(row.get('request_p99_ms') if row else None)} | {counts} |")
    if additional_reports:
        lines += ["", "## Additional validation reports", "",
                  "These reports are preserved separately and do not alter the primary before/after status or focused follow-up table.", "",
                  "| Source report | Profile | Duration × repeats | Passed | Budget failures |", "| --- | --- | ---: | --- | --- |"]
        for path, report in additional_reports:
            metadata = settings_metadata(report)
            failures = metadata.get("budget_failures")
            failure_text = "n/a" if failures is None else (str(failures) if failures else "none")
            lines.append(f"| `{path.name}` | {metadata.get('profile', 'n/a')} | {metadata.get('seconds', 'n/a')} s × {metadata.get('repeats', 'n/a')} | {metadata.get('passed', 'n/a')} | {failure_text} |")
    lines += ["", "## Cache profile", "", "The measured-window hit ratio is calculated from per-run counter deltas: (final hits − initial hits) / (hit delta + miss delta). Initial and final cumulative ratios are shown for context only. Counter decreases or empty intervals are reported as unavailable. The working set and run duration are supplied in the raw report.", "",
              "| Payload budget MiB | Workers | Measured-window hit ratio | Initial cumulative ratio | Final cumulative ratio | Item ops/s |", "| ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in cache_case_rows(cache or {}):
        final = row["hit_ratio_final"]
        before_ratio = row["hit_ratio_before"]
        rate = case_rate((cache or {}).get("cases", {}).get(row["case"], {}) if row["case"] else None)
        measured = row["measured_hit_ratio"]
        lines.append(f"| {row['cache_mib']} | {row['workers']} | {fmt(measured * 100 if number(measured) else None, 1)}% | {fmt(before_ratio * 100 if number(before_ratio) else None, 1)}% | {fmt(final * 100 if number(final) else None, 1)}% | {fmt(rate)} |")
    lines += ["", "## Streaming transfer", "", "MiB/s is reported by the helper and includes requests drained after load offering stops. The initial GET timing is the whole first verified GET, including checksum preparation, response transfer, and client-side verification; it is not an isolated checksum-scan duration.", "",
              "| Mode | Workers | File MiB | MiB/s | First GET with full scan, end-to-end ms | Successful / failed-or-rejected / unknown |", "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for row in sorted(streams, key=lambda r: (str(r.get("mode")), r.get("workers") or 0)):
        c = row.get("counts") or {}
        if row.get("mode") == "get":
            successes, failures, unknown = c.get("successful_gets", "n/a"), c.get("failed_gets", "n/a"), "n/a"
        else:
            successes, failures, unknown = c.get("successful_puts", "n/a"), c.get("rejected_puts", "n/a"), c.get("unknown_puts", "n/a")
        lines.append(f"| {row.get('mode', 'n/a')} | {row.get('workers', 'n/a')} | {row.get('file_mib', 'n/a')} | {fmt(row.get('throughput_mib_s'))} | {fmt(row.get('first_get_end_to_end_ms'))} | {successes} / {failures} / {unknown} |")
    lines += ["", "## Settings and interpretation", "",
              "Exact settings, source report bodies, case outcomes, effective TOML, error details, and fingerprints are retained in [the raw JSON reports](PERFORMANCE_UPGRADE_RESULTS.json). Failed or budget-limited runs are retained and identified by their source reports; they are not dropped from the combined artifact.", "",
              "These are local closed-loop studies; durations, repetition counts, and warmup periods are listed above. They do not correct for coordinated omission. Cache comparisons use the specified 16,384 × 32 KiB working set; streaming GET repeats one warmed fixture after a separately measured first verified GET. They are not cold/random archive-read results. No RSS or physical-device throughput claim is made. Client and server share a host, and throughput includes drain time.", "",
              "Cache hit ratios and throughput should be read alongside the explicit cache budget and metadata limits. A failed candidate budget remains a failure even when throughput numbers are present; see the raw report's `passed`, `budget_failures`, errors, and per-case `store_limited` fields.", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True, help="0.2.2 legacy-config HTTP aggregate JSON")
    parser.add_argument("--after", type=Path, required=True, help="0.3.0 HTTP aggregate JSON")
    parser.add_argument("--cache", type=Path, required=True, help="0.3.0 cache-profile aggregate JSON")
    parser.add_argument("--streaming-dir", type=Path, required=True, help="Directory of streaming helper JSON reports")
    parser.add_argument("--followup-before", type=Path, help="Optional longer before report focused on the single-write p99 case")
    parser.add_argument("--followup-after", type=Path, help="Optional longer after report focused on the single-write p99 case")
    parser.add_argument("--additional-report", type=Path, action="append", default=[],
                        help="Additional complete JSON report to retain and list (repeatable)")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--basename", default="PERFORMANCE_UPGRADE_RESULTS", help="Output filename stem")
    parser.add_argument("--title", default="0.3.0 performance upgrade results")
    args = parser.parse_args()
    if not args.basename or Path(args.basename).name != args.basename:
        parser.error("basename must be one filename component")

    before, after, cache = (load(args.before), load(args.after), load(args.cache))
    followup_before = load(args.followup_before) if args.followup_before else None
    followup_after = load(args.followup_after) if args.followup_after else None
    additional_reports = [(path, load(path)) for path in args.additional_report]
    streams = collect_streaming(args.streaming_dir)
    combined = {
        "schema_version": 1,
        "title": args.title,
        "sources": {"before": str(args.before), "after": str(args.after), "cache": str(args.cache),
                    "followup_before": str(args.followup_before) if args.followup_before else None,
                    "followup_after": str(args.followup_after) if args.followup_after else None,
                    "additional_reports": [str(path) for path, _ in additional_reports],
                    "streaming_dir": str(args.streaming_dir)},
        "study_metadata": {"before": {**settings_metadata(before), "config_fingerprints": config_fingerprints(before)},
                           "after": {**settings_metadata(after), "config_fingerprints": config_fingerprints(after)},
                           "cache": {**settings_metadata(cache), "config_fingerprints": config_fingerprints(cache)}},
        "followup_metadata": {"before": settings_metadata(followup_before) if followup_before else None,
                              "after": settings_metadata(followup_after) if followup_after else None},
        "additional_reports_metadata": [{"source": str(path), **settings_metadata(report)}
                                        for path, report in additional_reports],
        "derived": {"cache_profile": cache_case_rows(cache), "streaming_runs": streams},
        "source_reports": {"before": before, "after": after, "cache": cache,
                           "followup_before": followup_before, "followup_after": followup_after,
                           "additional_reports": [{"source": str(path), "report": report}
                                                  for path, report in additional_reports],
                           "streaming": [load(Path(row["source"])) for row in streams]},
        "interpretation_limits": [
            "Short closed-loop local runs; no coordinated-omission correction.",
            "Cache profile uses the configured working set and cache budgets; it is not RSS.",
            "Streaming GET repeatedly reads one warmed fixture after a separately measured first verified GET.",
            "No physical-device throughput or RSS claim is made.",
            "Failed and budget-limited candidate runs are preserved in source_reports.",
        ],
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / f"{args.basename}.json").write_text(json.dumps(combined, indent=2, sort_keys=True) + "\n")
    (args.output_dir / f"{args.basename}.md").write_text(
        markdown(before, after, cache, streams, followup_before, followup_after, additional_reports, title=args.title).replace("PERFORMANCE_UPGRADE_RESULTS.json", f"{args.basename}.json"))


if __name__ == "__main__":
    main()
