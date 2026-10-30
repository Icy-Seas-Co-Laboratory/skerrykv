#!/usr/bin/env python3
"""Build a cumulative multi-writer comparison with explicit historical controls."""
import argparse
import hashlib
import json
import math
import statistics
from pathlib import Path


def read_json(path):
    raw = path.read_bytes()
    return json.loads(raw), hashlib.blake2b(raw, digest_size=32).hexdigest()


def median(values):
    values = [v for v in values if isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)]
    return statistics.median(values) if values else None


def fmt(value, digits=1):
    return "n/a" if value is None else f"{value:,.{digits}f}"


def fmt_percent(value, digits=1):
    return "n/a" if value is None else f"{value:+.{digits}f}%"


def fingerprints(case):
    return sorted({run.get("environment", {}).get("binary_blake3") for run in case.get("runs", [])
                   if run.get("environment", {}).get("binary_blake3")})


def probe_unavailable(summary, route):
    value = summary.get("probes", {}).get(route, {}).get("unavailable_fraction")
    return value


def flatten_studies(report, label):
    """Return named HTTP studies from either an aggregate or its embedded source reports."""
    if not isinstance(report, dict):
        return []
    source_reports = report.get("source_reports", {})
    studies = []
    if isinstance(source_reports, dict):
        for key, value in source_reports.items():
            if isinstance(value, dict) and isinstance(value.get("cases"), dict):
                studies.append((f"{label}:{key}", value))
            elif isinstance(value, list):
                for index, item in enumerate(value, 1):
                    nested = item.get("report") if isinstance(item, dict) else None
                    if isinstance(nested, dict) and isinstance(nested.get("cases"), dict):
                        studies.append((f"{label}:{key}-{index}", nested))
    if not studies and isinstance(report.get("cases"), dict):
        studies.append((label, report))
    return studies


def case_metrics(case):
    summary = case.get("summary", {})
    runs = case.get("runs", [])
    rate = summary.get("item_ops_per_second")
    if isinstance(rate, dict):
        rate = rate.get("median")
    if rate is None:
        rate = median(run.get("summary", {}).get("item_ops_per_second") for run in runs)
    p99 = summary.get("request_p99_ms_median")
    if p99 is None:
        p99 = median(run.get("latency", {}).get("success_request_ms", {}).get("p99_ms") for run in runs)
    rejected = summary.get("rejected_items")
    unknown = summary.get("unknown_items")
    if rejected is None:
        rejected = sum(run.get("summary", {}).get("rejected_items", 0) for run in runs)
    if unknown is None:
        unknown = sum(run.get("summary", {}).get("unknown_items", 0) for run in runs)
    probes = summary.get("probes", {})
    if runs and not probes:
        probes = {}
        for route in ("status", "metrics", "ready"):
            attempts = sum(run.get("probes", {}).get(route, {}).get("attempts", 0) for run in runs)
            unavailable = sum(run.get("probes", {}).get(route, {}).get("failures", 0) + run.get("probes", {}).get(route, {}).get("missing_storage", 0) for run in runs)
            probes[route] = {"unavailable_fraction": unavailable / attempts if attempts else None}
    return {"item_ops_per_second": rate, "request_p99_ms": p99, "rejected_items": rejected,
            "unknown_items": unknown, "status_unavailable_fraction": probe_unavailable({"probes": probes}, "status"),
            "metrics_unavailable_fraction": probe_unavailable({"probes": probes}, "metrics")}


def study_metadata(report):
    return {key: report.get(key) for key in ("profile", "seconds", "repeats", "warmup_seconds", "max_store_mib", "runner_platform", "method", "legacy_config", "baseline_config_0_3")}


def normalized_settings(case):
    settings = dict(case.get("settings", {}))
    settings.pop("writer_lanes", None)
    return settings


def historical_rows(studies):
    rows = []
    for label, report in studies:
        for name, case in report.get("cases", {}).items():
            settings = case.get("settings", {})
            metric = case_metrics(case)
            rows.append({"study": label, "profile": report.get("profile"), "seconds": report.get("seconds"),
                         "kind": "historical_http",
                         "warmup_seconds": report.get("warmup_seconds"), "runner_platform": report.get("runner_platform"),
                         "binary_fingerprints": fingerprints(case), "case": name, "settings": settings, **metric})
    return rows


def historical_result_rows(inputs):
    """Preserve benchmark summary lists and V2 protocol phase summaries row by row."""
    rows = []
    for path, report, digest in inputs:
        label = str(path)
        if isinstance(report, list):
            for index, raw in enumerate(report):
                if not isinstance(raw, dict):
                    continue
                rows.append({"study": label, "kind": "benchmark_summary", "source_row": index,
                             "profile": raw.get("profile"), "dataset": raw.get("dataset"),
                             "phase": raw.get("phase"), "write_pattern": raw.get("write_pattern"),
                             "clients": raw.get("clients", raw.get("workers")), "writer_lanes": raw.get("writer_lanes"),
                             "ops_per_second": raw.get("ops_per_second"), "throughput_unit": "operations/second",
                             "p99": raw.get("latency_p99_us"), "p99_unit": "microseconds" if raw.get("latency_p99_us") is not None else None,
                             "latency_definition": raw.get("latency_unit"),
                             "trials": raw.get("trials"), "method_boundary": "historical benchmark method; descriptive only",
                             "raw": raw})
        elif isinstance(report, dict) and isinstance(report.get("summary"), list) and all(isinstance(row, dict) and "before_ops_s" in row for row in report["summary"]):
            for index, raw in enumerate(report["summary"]):
                rows.append({"study": label, "kind": "protocol_phase", "source_row": index,
                             "profile": "V2 durability protocol comparison", "phase": raw.get("phase"),
                             "ops_per_second": raw.get("after_ops_s"), "throughput_unit": "operations/second",
                             "before_ops_per_second": raw.get("before_ops_s"), "after_ops_per_second": raw.get("after_ops_s"),
                             "after_before_ratio": raw.get("after_before"), "method_boundary": "within-study V2 protocol before/after; not comparable to multi-writer HTTP studies",
                             "raw": raw})
        elif isinstance(report, dict) and isinstance(report.get("scenarios"), list) and isinstance(report.get("baseline"), dict) and isinstance(report.get("candidate"), dict):
            for index, raw in enumerate(report["scenarios"]):
                seconds = raw.get("median_seconds", {})
                rows.append({"study": label, "kind": "verification_duration", "source_row": index,
                             "profile": "index-verification", "phase": raw.get("name"),
                             "scenario": raw.get("name"), "objects_written": raw.get("objects_written"),
                             "payload_bytes": raw.get("payload_bytes"), "deleted": raw.get("deleted"),
                             "baseline_seconds": seconds.get("baseline"), "candidate_seconds": seconds.get("candidate"),
                             "baseline_samples_seconds": raw.get("seconds", {}).get("baseline", []),
                             "candidate_samples_seconds": raw.get("seconds", {}).get("candidate", []),
                             "speedup": raw.get("speedup"), "throughput_unit": None, "p99": None,
                             "method_boundary": report.get("method", "within-study verification duration comparison; no throughput conversion"),
                             "raw": raw})
        elif isinstance(report, dict) and report.get("benchmark") == "verification_foreground_get_latency":
            for condition in ("baseline_idle", "while_verification_runs"):
                raw = report.get(condition, {})
                rows.append({"study": label, "kind": "verification_foreground", "phase": condition,
                             "profile": "foreground GET latency during verification", "dataset": (report.get("seed") or {}).get("total_payload_bytes"),
                             "requests": report.get("foreground_reads"), "overlapped_requests": raw.get("overlapped_requests"),
                             "requests_per_second": raw.get("requests_per_second"), "throughput_unit": "requests/second",
                             "p50_ms": raw.get("p50_ms"), "p95_ms": raw.get("p95_ms"), "total_ms": raw.get("total_ms"),
                             "p99": None, "method_boundary": f"{report.get('method', '')}; {report.get('scope', '')}".strip("; "),
                             "raw": raw})
    return rows


def streaming_rows_from_report(report, label, origin="historical"):
    """Extract streaming-capacity records without converting MiB/s to ops/s."""
    if not isinstance(report, dict):
        return []
    found = []
    sources = report.get("source_reports", {})
    nested = sources.get("streaming", []) if isinstance(sources, dict) else []
    direct = []
    if isinstance(nested, list):
        for item in nested:
            if not isinstance(item, dict):
                continue
            record = item.get("report") if isinstance(item.get("report"), dict) else item
            if record.get("schema") == "skerry.streaming-capacity.v1":
                if item.get("source") and not record.get("source"):
                    record = {**record, "source": item["source"]}
                direct.append(record)
    if not direct:
        direct = [item for item in report.get("derived", {}).get("streaming_runs", [])
                  if isinstance(item, dict) and ("throughput_mib_s" in item or item.get("schema") == "skerry.streaming-capacity.v1")]
    for index, raw in enumerate(direct, 1):
        configuration = raw.get("configuration", {})
        latency_name = "put_latency" if raw.get("mode") == "put" else "get_latency"
        latency = raw.get(latency_name, {})
        p99 = latency.get("p99_ms") if isinstance(latency, dict) else None
        if p99 is None:
            p99 = raw.get("first_get_full_scan_ms", raw.get("first_get_end_to_end_ms"))
        found.append({"study": f"{label}:streaming", "kind": "streaming", "origin": origin,
                      "source": raw.get("source", raw.get("binary", f"row-{index}")),
                      "profile": "streaming-capacity", "phase": raw.get("mode"),
                      "dataset": raw.get("file_mib"), "clients": raw.get("workers"),
                      "writer_lanes": configuration.get("writer_lanes"),
                      "throughput_mib_s": raw.get("throughput_mib_s"), "throughput_unit": "MiB/s",
                      "p99": p99, "p99_unit": "milliseconds",
                      "rejected": (raw.get("counts") or {}).get("rejected_puts"),
                      "unknown": (raw.get("counts") or {}).get("unknown_puts"),
                      "method_boundary": raw.get("measurement_note", "Streaming capacity; throughput is MiB/s, not object operations/s"),
                      "raw": raw})
    return found


def streaming_input_paths(paths):
    """Expand repeatable --streaming file or directory inputs in stable order."""
    expanded = []
    for path in paths:
        if path.is_dir():
            expanded.extend(sorted(path.rglob("*.json")))
        else:
            expanded.append(path)
    return expanded


def streaming_input_rows(inputs):
    rows = []
    for path, report, _digest in inputs:
        if isinstance(report, dict) and report.get("schema") == "skerry.streaming-capacity.v1":
            wrapper = {"source_reports": {"streaming": [report]}}
        else:
            wrapper = report
        rows.extend(streaming_rows_from_report(wrapper, str(path), "candidate"))
    return rows


def compact_benchmark_rows(rows):
    groups = {}
    for row in rows:
        if row.get("kind") != "benchmark_summary":
            continue
        key = (row["study"], row.get("profile"), row.get("phase"), row.get("dataset"), row.get("write_pattern"))
        groups.setdefault(key, []).append(row)
    compact = []
    for (study, profile, phase, dataset, pattern), values in sorted(groups.items(), key=lambda item: tuple(str(v) for v in item[0])):
        compact.append({"study": study, "profile": profile, "phase": phase, "dataset": dataset,
                        "write_pattern": pattern, "summary_rows": len(values),
                        "median_ops_per_second": median([v.get("ops_per_second") for v in values]),
                        "median_p99": median([v.get("p99") for v in values]),
                        "p99_unit": next((v.get("p99_unit") for v in values if v.get("p99_unit")), None),
                        "latency_definitions": sorted({str(v.get("latency_definition")) for v in values if v.get("latency_definition")}),
                        "trials": sorted({v.get("trials") for v in values if v.get("trials") is not None})})
    return compact


def historical_row_counts(inputs, rows, streaming=()):
    counts = []
    for path, report, digest in inputs:
        label = str(path)
        source_rows = [row for row in rows if row.get("study") == label]
        stream_rows = [row for row in streaming if row.get("origin") == "historical" and row.get("study") == f"{label}:streaming"]
        http_studies = flatten_studies(report, label)
        http_case_rows = [{"profile": study.get("profile"), "phase": case.get("settings", {}).get("workload"),
                           "dataset": case.get("settings", {}).get("payload_bytes")}
                          for _, study in http_studies for case in study.get("cases", {}).values()]
        profiles = sorted({str(row.get("profile")) for row in source_rows + http_case_rows if row.get("profile") is not None})
        phases = sorted({str(row.get("phase")) for row in source_rows + http_case_rows if row.get("phase") is not None})
        datasets = sorted({str(row.get("dataset")) for row in source_rows + http_case_rows if row.get("dataset") is not None})
        http_cases = len(http_case_rows)
        benchmark_rows = sum(row.get("kind") == "benchmark_summary" for row in source_rows)
        protocol_phases = sum(row.get("kind") == "protocol_phase" for row in source_rows)
        verification_scenarios = sum(row.get("kind") == "verification_duration" for row in source_rows)
        foreground_rows = sum(row.get("kind") == "verification_foreground" for row in source_rows)
        counts.append({"study": label, "row_count": len(source_rows) + http_cases + len(stream_rows),
                       "benchmark_summary_rows": benchmark_rows, "protocol_phases": protocol_phases,
                       "verification_scenarios": verification_scenarios, "foreground_rows": foreground_rows,
                       "http_cases": http_cases, "streaming_rows": len(stream_rows), "profiles": profiles,
                       "phases": phases, "datasets": datasets, "sha256_blake2b_256": digest})
    return counts


def compact_phase_rows(rows):
    groups = {}
    for row in rows:
        if row.get("kind") != "benchmark_summary":
            continue
        key = (row.get("study"), row.get("profile"), row.get("phase"))
        groups.setdefault(key, []).append(row)
    output = []
    for (study, profile, phase), values in sorted(groups.items(), key=lambda pair: tuple(str(v) for v in pair[0])):
        output.append({"study": study, "profile": profile, "phase": phase,
                       "summary_rows": len(values), "datasets": sorted({v.get("dataset") for v in values if v.get("dataset")}),
                       "write_patterns": sorted({v.get("write_pattern") for v in values if v.get("write_pattern")}),
                       "median_ops_per_second": median([v.get("ops_per_second") for v in values]),
                       "median_p99_us": median([v.get("p99") for v in values if v.get("p99_unit") == "microseconds"]),
                       "latency_definitions": sorted({str(v.get("latency_definition")) for v in values if v.get("latency_definition")})})
    return output


def dashboard_rows(http_rows, historical_rows_raw, lane_rows, version_rows, streams=()):
    rows = list(historical_rows_raw) + list(http_rows) + list(streams)
    for row in lane_rows:
        metric = row["metrics"]
        rows.append({"study": row["source"], "kind": "candidate_lane", "profile": row["study"].get("profile"),
                     "phase": row["settings"].get("workload"), "dataset": row["settings"].get("payload_bytes"),
                     "clients": row["settings"].get("workers"), "writer_lanes": row["writer_lanes"],
                     "ops_per_second": metric.get("item_ops_per_second"), "throughput_unit": "item operations/second",
                     "p99": metric.get("request_p99_ms"), "p99_unit": "milliseconds",
                     "method_boundary": row["match_status"], "comparison_change_percent": row["change_vs_lane_1_percent"]})
    for row in version_rows:
        rows.append({"study": f"{row['baseline_study']} → {row['candidate_study']}", "kind": "matched_version_lane_1",
                     "phase": row["settings"].get("workload"), "dataset": row["settings"].get("payload_bytes"),
                     "clients": row["settings"].get("workers"), "writer_lanes": 1,
                     "ops_per_second": row["candidate_metrics"].get("item_ops_per_second"),
                     "throughput_unit": "item operations/second", "p99": row["candidate_metrics"].get("request_p99_ms"),
                     "p99_unit": "milliseconds", "method_boundary": row["comparison_policy"],
                     "comparison_change_percent": row["change_vs_baseline_percent"]})
    return rows


def html_document(rows, context=None):
    """Standalone interactive table. Embedded data is script-safe and rendered only with textContent."""
    payload = json.dumps({"rows": rows, "historical_context": context or []}, ensure_ascii=False, separators=(",", ":"))
    payload = payload.replace("&", "\\u0026").replace("<", "\\u003c").replace(">", "\\u003e").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")
    return """<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>SkerryKV cumulative benchmark results</title>
<style>
body{font:14px system-ui,sans-serif;margin:2rem;color:#17212b}h1{font-size:1.6rem}p{max-width:1000px;line-height:1.5}.filters{display:flex;gap:.6rem;flex-wrap:wrap;position:sticky;top:0;background:#fff;padding:.7rem 0}input,select{padding:.45rem;border:1px solid #9ba8b4;border-radius:4px}table{border-collapse:collapse;width:100%;font-size:12px}th,td{padding:.45rem;border-bottom:1px solid #d8dee5;text-align:left;vertical-align:top}th{position:sticky;top:54px;background:#eff3f6}td{max-width:28rem;overflow-wrap:anywhere}.notice{background:#f1f5f9;padding:.7rem;border-left:4px solid #4b6b82}
</style>
<h1>SkerryKV cumulative benchmark results</h1>
<p class="notice">Historical studies preserve their own methods and units. Comparisons across methods are not calculated. Candidate lane rows are shown first; streaming throughput remains MiB/s, separate from operation rates.</p>
<div class="filters"><input id="search" type="search" placeholder="Search all fields"><select id="kind"><option value="">All kinds</option><option value="candidate_lane" selected>Candidate lane results</option></select><select id="study"><option value="">All studies</option></select><select id="profile"><option value="">All profiles</option></select><select id="phase"><option value="">All phases</option></select><select id="dataset"><option value="">All datasets</option></select><span id="count"></span></div>
<table><thead><tr><th>Study</th><th>Kind</th><th>Profile</th><th>Dataset</th><th>Phase</th><th>Clients</th><th>Lanes</th><th>Rate / duration</th><th>Latency / p95</th><th>Details / method boundary</th></tr></thead><tbody id="rows"></tbody></table>
<section id="streaming-section"><h2>Streaming capacity runs</h2><p>Streaming throughput is reported in MiB/s, not object operations/s.</p><table><thead><tr><th>Study</th><th>Kind</th><th>Profile</th><th>File MiB</th><th>Mode</th><th>Clients</th><th>Lanes</th><th>Rate / duration</th><th>Latency / p95</th><th>Method / details</th></tr></thead><tbody id="streaming-rows"></tbody></table></section>
<section id="historical-context"><h2>Historical comparison context</h2></section>
<script id="benchmark-data" type="application/json">""" + payload + """</script>
<script>
const embedded=JSON.parse(document.getElementById('benchmark-data').textContent);const data=embedded.rows;
for(const source of embedded.historical_context){const details=document.createElement('details');const summary=document.createElement('summary');summary.textContent=source.path;const pre=document.createElement('pre');pre.textContent=source.text;details.append(summary,pre);document.getElementById('historical-context').appendChild(details)}
const fields={kind:'kind',study:'study',profile:'profile',phase:'phase',dataset:'dataset'};
const val=(x)=>x===undefined||x===null?'':String(x);
const enc=(x)=>JSON.stringify(x===undefined?null:x);
for(const [id,key] of Object.entries(fields)){const sel=document.getElementById(id);const vals=[...new Set(data.map(r=>val(r[key])).filter(Boolean))].sort();for(const v of vals){if([...sel.children].some(o=>o.value===v))continue;const o=document.createElement('option');o.value=v;o.textContent=v;sel.appendChild(o)}}
function throughput(r){if(r.kind==='streaming')return `${val(r.throughput_mib_s)} MiB/s`;if(r.kind==='verification_duration')return `baseline ${val(r.baseline_seconds)} s → candidate ${val(r.candidate_seconds)} s`;if(r.kind==='verification_foreground')return `${val(r.requests_per_second)} requests/s`;if(r.kind==='protocol_phase')return `${val(r.before_ops_per_second)} → ${val(r.after_ops_per_second)} operations/s`;const rate=r.ops_per_second??r.item_ops_per_second??r.metrics?.item_ops_per_second;return `${val(rate)} ${val(r.throughput_unit||'operations/s')}`}
function p99(r){if(r.kind==='verification_duration')return '';if(r.kind==='verification_foreground')return `p50 ${val(r.p50_ms)} / p95 ${val(r.p95_ms)} ms`;const n=r.p99??r.request_p99_ms??r.metrics?.request_p99_ms;return n===undefined||n===null?'':`${val(n)} ${val(r.p99_unit||'ms')}`}
function fill(id,items){const body=document.getElementById(id);body.replaceChildren();for(const r of items){const tr=document.createElement('tr');for(const x of [r.study,r.kind,r.profile,r.dataset,r.phase,r.clients,r.writer_lanes,throughput(r),p99(r)]){const td=document.createElement('td');td.textContent=val(x);tr.appendChild(td)}const td=document.createElement('td');td.textContent=val(r.method_boundary);if(r.raw!==undefined){const d=document.createElement('details');const s=document.createElement('summary');s.textContent='View raw details';const pre=document.createElement('pre');pre.textContent=enc(r.raw);d.append(s,pre);td.appendChild(d)}tr.appendChild(td);body.appendChild(tr)}}
function render(){const term=document.getElementById('search').value.toLowerCase();const selected=Object.fromEntries(Object.keys(fields).map(k=>[k,document.getElementById(k).value]));const shown=data.filter(r=>Object.entries(fields).every(([id,key])=>!selected[id]||val(r[key])===selected[id])&&(!term||JSON.stringify(r).toLowerCase().includes(term)));fill('rows',shown.filter(r=>r.kind!=='streaming'));fill('streaming-rows',shown.filter(r=>r.kind==='streaming'));document.getElementById('count').textContent=`${shown.length} / ${data.length} rows; ${shown.filter(r=>r.kind==='streaming').length} streaming`}
for(const id of ['search','kind','study','profile','phase','dataset'])document.getElementById(id).addEventListener(id==='search'?'input':'change',render);render();
</script></html>"""


def candidate_lane_rows(candidate_studies):
    groups = {}
    for label, report in candidate_studies:
        meta = study_metadata(report)
        for name, case in report.get("cases", {}).items():
            settings = dict(case.get("settings", {}))
            lane = settings.pop("writer_lanes", None)
            if lane not in (1, 2, 4, 8):
                continue
            fps = fingerprints(case)
            key = json.dumps([meta, settings, fps], sort_keys=True)
            groups.setdefault(key, {"study": meta, "settings": settings, "lanes": {}})["lanes"][str(lane)] = {
                "case": name, "label": label, "case_data": case, "fingerprints": fps,
                "metrics": case_metrics(case),
            }
    rows = []
    for group in groups.values():
        lanes = group["lanes"]
        for lane, value in sorted(lanes.items(), key=lambda item: int(item[0])):
            baseline = lanes.get("1")
            rate, base_rate = value["metrics"]["item_ops_per_second"], baseline["metrics"]["item_ops_per_second"] if baseline else None
            rows.append({"study": group["study"], "settings": group["settings"], "writer_lanes": int(lane),
                         "case": value["case"], "source": value["label"], "metrics": value["metrics"],
                         "binary_fingerprints": value["fingerprints"],
                         "match_status": "matched same binary and settings" if baseline and value["fingerprints"] else "unmatched: lane-1 or binary fingerprint missing",
                         "change_vs_lane_1_percent": 100 * (rate / base_rate - 1) if baseline and value["fingerprints"] and rate is not None and base_rate else None})
    return rows


def cross_version_rows(candidate_studies, baseline_studies):
    rows = []
    for candidate_label, candidate in candidate_studies:
        cmeta = study_metadata(candidate)
        for cname, ccase in candidate.get("cases", {}).items():
            csettings = normalized_settings(ccase)
            lane = ccase.get("settings", {}).get("writer_lanes")
            if lane != 1:
                continue
            for baseline_label, baseline in baseline_studies:
                bmeta = study_metadata(baseline)
                for bname, bcase in baseline.get("cases", {}).items():
                    if normalized_settings(bcase) != csettings:
                        continue
                    dimensions = ("seconds", "warmup_seconds", "max_store_mib", "runner_platform")
                    mismatches = [key for key in dimensions if cmeta.get(key) != bmeta.get(key)]
                    if mismatches:
                        continue
                    cm, bm = case_metrics(ccase), case_metrics(bcase)
                    old_rate, new_rate = bm["item_ops_per_second"], cm["item_ops_per_second"]
                    rows.append({"candidate_study": candidate_label, "baseline_study": baseline_label,
                                 "settings": csettings, "seconds": cmeta.get("seconds"),
                                 "baseline_case": bname, "candidate_case": cname,
                                 "baseline_fingerprints": fingerprints(bcase), "candidate_fingerprints": fingerprints(ccase),
                                 "baseline_metrics": bm, "candidate_metrics": cm,
                                 "change_vs_baseline_percent": 100 * (new_rate / old_rate - 1) if old_rate and new_rate is not None else None,
                                 "comparison_policy": "matched workload settings, duration, warmup, store cap, and platform; binary version difference is deliberate"})
    return rows


def describe_historical(studies):
    return [{"study": label, "metadata": study_metadata(report),
             "binary_fingerprints": sorted({fp for case in report.get("cases", {}).values() for fp in fingerprints(case)}),
             "cases": len(report.get("cases", {}))} for label, report in studies]


def lane_comparison_markdown(rows):
    lines = ["| Study | Workload settings | Seconds | Lane | Item ops/s | Request p99 ms | Change vs lane 1 | Rejected / unknown |",
             "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in rows:
        metrics = row["metrics"]
        settings = ", ".join(f"{key}={value}" for key, value in sorted(row["settings"].items()))
        lines.append(f"| {row['source']} | {settings} | {row['study'].get('seconds', 'n/a')} | {row['writer_lanes']} | {fmt(metrics['item_ops_per_second'])} | {fmt(metrics['request_p99_ms'], 2)} | {fmt_percent(row['change_vs_lane_1_percent'])} | {metrics['rejected_items']} / {metrics['unknown_items']} |")
    return lines


def version_comparison_markdown(rows):
    lines = ["| Baseline source | Candidate source | Workload settings | Seconds | Prior item ops/s | Candidate item ops/s | Change | Prior / candidate p99 ms | Prior / candidate rejected / unknown |",
             "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in rows:
        baseline, candidate = row["baseline_metrics"], row["candidate_metrics"]
        settings = ", ".join(f"{key}={value}" for key, value in sorted(row["settings"].items()))
        lines.append(f"| {row['baseline_study']} | {row['candidate_study']} | {settings} | {row['seconds']} | {fmt(baseline['item_ops_per_second'])} | {fmt(candidate['item_ops_per_second'])} | {fmt_percent(row['change_vs_baseline_percent'])} | {fmt(baseline['request_p99_ms'], 2)} / {fmt(candidate['request_p99_ms'], 2)} | {baseline['rejected_items']} / {baseline['unknown_items']} ; {candidate['rejected_items']} / {candidate['unknown_items']} |")
    return lines


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, action="append", default=[], help="New lane-matrix aggregate JSON; repeatable")
    parser.add_argument("--baseline", type=Path, action="append", default=[], help="Prior-version aggregate JSON used only for matched lane-1 comparisons; repeatable")
    parser.add_argument("--baseline-section", default="source_reports.after", help="Optional dotted JSON path for the prior HTTP aggregate")
    parser.add_argument("--historical", type=Path, action="append", default=[], help="Prior HTTP/streaming/protocol JSON, context only; repeatable")
    parser.add_argument("--streaming", type=Path, action="append", default=[], help="Streaming-capacity result JSON or directory; repeatable, reports MiB/s separately")
    parser.add_argument("--historical-context", type=Path, action="append", default=[], help="Text/Markdown historical comparison, context only; repeatable")
    parser.add_argument("--output", type=Path, required=True, help="New cumulative JSON report path")
    args = parser.parse_args()
    candidate_inputs = [(p, *read_json(p)) for p in args.candidate]
    baseline_inputs = [(p, *read_json(p)) for p in args.baseline]
    historical_inputs = [(p, *read_json(p)) for p in args.historical]
    streaming_paths = streaming_input_paths(args.streaming)
    streaming_inputs = [(p, *read_json(p)) for p in streaming_paths]
    candidate_studies = [study for p, report, digest in candidate_inputs for study in flatten_studies(report, str(p))]
    baseline_studies = []
    baseline_selected = []
    for path, report, digest in baseline_inputs:
        selected = report
        if args.baseline_section:
            for key in args.baseline_section.split("."):
                selected = selected.get(key) if isinstance(selected, dict) else None
        if not isinstance(selected, dict) or not isinstance(selected.get("cases"), dict):
            raise SystemExit(f"{path}: baseline section {args.baseline_section!r} has no HTTP cases")
        baseline_studies.append((f"{path}:{args.baseline_section}", selected))
        baseline_selected.append(selected)
    historical_studies = [study for p, report, digest in historical_inputs for study in flatten_studies(report, str(p))]
    hist_rows = historical_rows(historical_studies)
    historical_raw_rows = historical_result_rows(historical_inputs)
    historical_streams = [row for path, report, _ in historical_inputs
                          for row in streaming_rows_from_report(report, str(path), "historical")]
    candidate_streams = streaming_input_rows(streaming_inputs)
    all_streams = historical_streams + candidate_streams
    lane_rows = candidate_lane_rows(candidate_studies)
    version_rows = cross_version_rows(candidate_studies, baseline_studies)
    counts = historical_row_counts(historical_inputs, historical_raw_rows, historical_streams)
    compact_phases = compact_phase_rows(historical_raw_rows)
    protocol_rows = [row for row in historical_raw_rows if row.get("kind") == "protocol_phase"]
    html_rows = dashboard_rows(hist_rows, historical_raw_rows, lane_rows, version_rows, all_streams)
    text_context = []
    for path in args.historical_context:
        raw = path.read_text(errors="replace")
        text_context.append({"path": str(path), "blake2b_256": hashlib.blake2b(raw.encode(), digest_size=32).hexdigest(), "text": raw})
    body = {"schema_version": 2, "title": "Cumulative multi-writer benchmark comparison",
            "comparison_policy": "Lane-count deltas use the same executable fingerprint. Prior-version comparisons are lane 1 only and require matching workload settings, duration, warmup, store cap, and platform; the version change is deliberate. Other historical studies are context only. No SLO is inferred.",
            "candidate_sources": [{"path": str(p), "blake2b_256": digest, "metadata": study_metadata(report), "binary_fingerprints": sorted({fp for case in report.get("cases", {}).values() for fp in fingerprints(case)})} for p, report, digest in candidate_inputs],
            "baseline_sources": [{"path": str(p), "section": args.baseline_section, "blake2b_256": digest, "metadata": study_metadata(selected), "binary_fingerprints": sorted({fp for case in selected.get("cases", {}).values() for fp in fingerprints(case)})} for (p, report, digest), selected in zip(baseline_inputs, baseline_selected)],
            "historical_sources": [{"path": str(p), "blake2b_256": digest, "study_summaries": describe_historical(flatten_studies(report, str(p)))} for p, report, digest in historical_inputs],
            "historical_source_row_counts": counts,
            "candidate_source_reports": [{"path": str(p), "report": report} for p, report, _ in candidate_inputs],
            "baseline_source_reports": [{"path": str(p), "section": args.baseline_section, "report": report} for p, report, _ in baseline_inputs],
            "historical_source_reports": [{"path": str(p), "report": report} for p, report, _ in historical_inputs],
            "streaming_sources": [{"path": str(p), "blake2b_256": digest, "report": report} for p, report, digest in streaming_inputs],
            "streaming_rows": all_streams,
            "historical_lane_and_http_case_rows": hist_rows,
            "historical_benchmark_summary_rows": historical_raw_rows,
            "verification_duration_rows": [row for row in historical_raw_rows if row.get("kind") == "verification_duration"],
            "verification_foreground_rows": [row for row in historical_raw_rows if row.get("kind") == "verification_foreground"],
            "historical_benchmark_phase_summaries": compact_phases,
            "v2_protocol_phase_summary_rows": protocol_rows,
            "same_binary_lane_comparisons": lane_rows,
            "matched_prior_version_lane_1_comparisons": version_rows,
            "historical_text_context": text_context,
            "dashboard_rows": html_rows}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n")
    md = ["# Cumulative multi-writer benchmark comparison", "", body["comparison_policy"], "", "## Lane scaling within the candidate binary", ""]
    md += lane_comparison_markdown(lane_rows)
    md += ["", "## Matched prior-version lane-1 comparison", "", "Only matched candidate lane-1 rows are compared with the selected prior-version aggregate. Binary fingerprints differ by design and are listed in JSON.", ""]
    md += version_comparison_markdown(version_rows)
    md += ["", "## Historical HTTP studies", "", "These rows remain separate by source study and binary. They are descriptive and do not contribute to candidate lane deltas.", "",
           "| Study | Case | Settings | Seconds | Item ops/s | Request p99 ms | Status / metrics unavailable | Rejected / unknown |", "| --- | --- | --- | ---: | ---: | ---: | --- | ---: |"]
    for row in hist_rows:
        status_available = None if row['status_unavailable_fraction'] is None else (1 - row['status_unavailable_fraction']) * 100
        metrics_available = None if row['metrics_unavailable_fraction'] is None else (1 - row['metrics_unavailable_fraction']) * 100
        md.append(f"| {row['study']} | {row['case']} | {', '.join(f'{k}={v}' for k, v in sorted(row['settings'].items()))} | {row['seconds']} | {fmt(row['item_ops_per_second'])} | {fmt(row['request_p99_ms'], 2)} | {fmt(status_available,1)}% / {fmt(metrics_available,1)}% available | {row['rejected_items']} / {row['unknown_items']} |")
    md += ["", "## Additional historical benchmark result sets", "", "The source JSON files and every summary row are retained in the JSON artifact. The compact table below groups the original rows by study, profile, and phase; it reports medians only within each source study, in that study's original units.", "",
           "| Study | Profile | Phase | Rows | Datasets | Write patterns | Median ops/s | Median p99 µs | Latency definition |", "| --- | --- | --- | ---: | --- | --- | ---: | ---: | --- |"]
    for row in compact_phases:
        md.append(f"| {row['study']} | {row['profile']} | {row['phase']} | {row['summary_rows']} | {', '.join(row['datasets'])} | {', '.join(row['write_patterns'])} | {fmt(row['median_ops_per_second'])} | {fmt(row['median_p99_us'], 1)} | {', '.join(row['latency_definitions'])} |")
    md += ["", "### Historical source row counts", "", "| Source | Rows retained | Benchmark rows | Protocol phases | Verification scenarios | Foreground rows | HTTP cases | Streaming rows | Profiles | Phases | Datasets |", "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |"]
    for row in counts:
        md.append(f"| {row['study']} | {row['row_count']} | {row['benchmark_summary_rows']} | {row['protocol_phases']} | {row['verification_scenarios']} | {row['foreground_rows']} | {row['http_cases']} | {row['streaming_rows']} | {', '.join(row['profiles'])} | {', '.join(row['phases'])} | {', '.join(row['datasets'])} |")
    md += ["", "### V2 protocol phases", "", "These five before/after phases are a within-study durability protocol comparison. They do not produce cross-method deltas against HTTP or Pelagia results.", "",
           "| Phase | Before ops/s | After ops/s | After / before |", "| --- | ---: | ---: | ---: |"]
    for row in protocol_rows:
        md.append(f"| {row['phase']} | {fmt(row['before_ops_per_second'])} | {fmt(row['after_ops_per_second'])} | {fmt(row['after_before_ratio'], 3)} |")
    verification_duration = [row for row in historical_raw_rows if row.get("kind") == "verification_duration"]
    md += ["", "### Historical verification duration scenarios", "", "These rows preserve within-study baseline/candidate verification wall durations; the source reports no operation-throughput rate for the scenario itself.", "",
           "| Study | Scenario | Objects | Payload bytes | Deleted | Baseline median seconds | Candidate median seconds | Speedup | Baseline / candidate samples (s) |", "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |"]
    for row in verification_duration:
        md.append(f"| {row['study']} | {row['scenario']} | {row['objects_written']} | {row['payload_bytes']} | {row['deleted']} | {fmt(row['baseline_seconds'], 3)} | {fmt(row['candidate_seconds'], 3)} | {fmt(row['speedup'], 3)}× | {', '.join(fmt(value, 3) for value in row['baseline_samples_seconds'])} / {', '.join(fmt(value, 3) for value in row['candidate_samples_seconds'])} |")
    verification_foreground = [row for row in historical_raw_rows if row.get("kind") == "verification_foreground"]
    md += ["", "### Historical foreground GET latency during verification", "", "These engine-level measurements report p95 (not p99) in milliseconds and requests/s for idle and overlapping verification conditions.", "",
           "| Study | Condition | Requests | Overlapped requests | Requests/s | p50 ms | p95 ms | Total ms |", "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in verification_foreground:
        md.append(f"| {row['study']} | {row['phase']} | {row['requests']} | {row['overlapped_requests']} | {fmt(row['requests_per_second'])} | {fmt(row['p50_ms'], 3)} | {fmt(row['p95_ms'], 3)} | {fmt(row['total_ms'], 3)} |")
    md += ["", "## Historical comparison documents", "", "The older KVStore2/Pelagia studies use different tools, payloads, durability contracts, and methods. Their source text is preserved below as context; no cross-method percent change is computed.", ""]
    for source in text_context:
        md += [f"### {source['path']}", "", "```text", source["text"].rstrip(), "```", ""]
    md += ["## Streaming capacity rows", "", "These large-object HTTP runs retain their original MiB/s units and are presented separately from object operations/s. Historical aggregate inputs and repeatable `--streaming` files/directories are preserved in JSON.", "",
           "| Study | Origin | Mode | File MiB | Clients | Lanes | Throughput MiB/s | p99 ms | Rejected / unknown |", "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in all_streams:
        md.append(f"| {row['study']} | {row['origin']} | {row['phase']} | {row['dataset']} | {row['clients']} | {row['writer_lanes']} | {fmt(row['throughput_mib_s'])} | {fmt(row['p99'], 2)} | {row['rejected']} / {row['unknown']} |")
    md += ["Full source metadata, fingerprints, snapshots, and rows are in the adjacent JSON report.", ""]
    args.output.with_suffix(".md").write_text("\n".join(md))
    args.output.with_suffix(".html").write_text(html_document(html_rows, text_context))


if __name__ == "__main__":
    main()
