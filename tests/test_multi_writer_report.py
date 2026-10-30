import importlib.util
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("multi_writer_report", Path(__file__).resolve().parents[1] / "scripts" / "summarize-multi-writer-results.py")
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


def source(lanes, binary="same", seconds=10, warmup=1):
    cases = {}
    for lane, rate in lanes:
        name = f"single-c8-w{lane}"
        cases[name] = {"settings": {"workload": "single", "workers": 8, "writer_lanes": lane, "payload_bytes": 4096},
                       "runs": [{"summary": {"item_ops_per_second": rate, "rejected_items": 0, "unknown_items": 0},
                                 "latency": {"success_request_ms": {"p99_ms": 2.0}},
                                 "environment": {"binary_blake3": binary}, "config_fingerprint": f"cfg-{lane}"}]}
    return {"seconds": seconds, "warmup_seconds": warmup, "max_store_mib": 512, "runner_platform": "host", "profile": "quick", "cases": cases}


class MultiWriterReportTests(unittest.TestCase):
    def test_lane_deltas_require_same_binary_and_settings(self):
        rows = report.candidate_lane_rows([("matrix", source([(1, 100), (2, 150)]))])
        lane2 = next(row for row in rows if row["writer_lanes"] == 2)
        self.assertEqual(lane2["change_vs_lane_1_percent"], 50)
        self.assertTrue(lane2["match_status"].startswith("matched"))
        mismatch = source([(1, 100), (2, 150)])
        mismatch["cases"]["single-c8-w2"]["runs"][0]["environment"]["binary_blake3"] = "different"
        rows = report.candidate_lane_rows([("matrix", mismatch)])
        lane2 = next(row for row in rows if row["writer_lanes"] == 2)
        self.assertIsNone(lane2["change_vs_lane_1_percent"])

    def test_comparison_markdown_keeps_study_labels_and_html_rows_expose_sources(self):
        metric = {"item_ops_per_second": 100.0, "request_p99_ms": 2.0,
                  "rejected_items": 0, "unknown_items": 0}
        lane = {"source": "release-final-refill.json", "study": {"seconds": 3},
                "settings": {"workload": "batch-put", "workers": 8}, "writer_lanes": 4,
                "metrics": metric, "change_vs_lane_1_percent": 25.0,
                "match_status": "matched same binary and settings"}
        lane_md = report.lane_comparison_markdown([lane])
        self.assertIn("| Study | Workload settings |", lane_md[0])
        self.assertIn("| release-final-refill.json |", lane_md[2])

        version = {"baseline_study": "baseline-03.json:section", "candidate_study": "refill-candidate.json",
                   "settings": {"workload": "batch-put", "workers": 8}, "seconds": 3,
                   "baseline_metrics": metric, "candidate_metrics": metric,
                   "change_vs_baseline_percent": 0.0,
                   "comparison_policy": "matched workload and duration"}
        version_md = report.version_comparison_markdown([version])
        self.assertIn("Baseline source | Candidate source", version_md[0])
        self.assertIn("| baseline-03.json:section | refill-candidate.json |", version_md[2])

        dashboard = report.dashboard_rows([], [], [lane], [version])
        self.assertEqual(dashboard[0]["study"], "release-final-refill.json")
        self.assertEqual(dashboard[1]["study"], "baseline-03.json:section → refill-candidate.json")
        html = report.html_document(dashboard)
        self.assertIn("release-final-refill.json", html)
        self.assertIn("baseline-03.json:section", html)

    def test_cross_version_lane_one_comparison_allows_binary_change_only_when_study_matches(self):
        old = source([(1, 100)], binary="old")
        new = source([(1, 125)], binary="new")
        rows = report.cross_version_rows([("candidate", new)], [("baseline", old)])
        self.assertEqual(rows[0]["change_vs_baseline_percent"], 25)
        self.assertNotEqual(rows[0]["baseline_fingerprints"], rows[0]["candidate_fingerprints"])
        mismatched = source([(1, 125)], binary="new", seconds=5)
        self.assertEqual(report.cross_version_rows([("candidate", mismatched)], [("baseline", old)]), [])

    def test_historical_summaries_keep_studies_separate(self):
        report_blob = {"source_reports": {"before": source([(1, 100)], binary="v0.2"),
                                           "after": source([(1, 200)], binary="v0.3")}}
        studies = report.flatten_studies(report_blob, "history")
        rows = report.historical_rows(studies)
        self.assertEqual({row["study"] for row in rows}, {"history:before", "history:after"})
        self.assertEqual({row["binary_fingerprints"][0] for row in rows}, {"v0.2", "v0.3"})

    def test_historical_list_rows_and_protocol_phases_are_all_retained(self):
        summary = [{"profile": "p", "dataset": "20k", "phase": f"phase-{i % 10}", "ops_per_second": i,
                    "latency_p99_us": i * 10, "latency_unit": "per_operation", "trials": 3}
                   for i in range(1, 424)]
        protocol = {"summary": [{"phase": f"v2-{i}", "before_ops_s": i, "after_ops_s": i + 1, "after_before": (i + 1) / i}
                                 for i in range(1, 6)]}
        rows = report.historical_result_rows([(Path("bench.json"), summary, "digest"),
                                              (Path("protocol.json"), protocol, "digest")])
        self.assertEqual(len([row for row in rows if row["kind"] == "benchmark_summary"]), 423)
        self.assertEqual(len([row for row in rows if row["kind"] == "protocol_phase"]), 5)
        self.assertEqual(len(report.compact_phase_rows(rows)), 10)

    def test_verification_duration_and_foreground_sources_keep_native_units_and_rows(self):
        verification = {
            "baseline": {"version": "old"}, "candidate": {"version": "new"},
            "method": "alternating isolated stores",
            "scenarios": [{"name": f"scenario-{i}", "objects_written": 100 + i,
                           "payload_bytes": 4096 * i, "deleted": i,
                           "seconds": {"baseline": [2.0, 1.0], "candidate": [1.5, 1.2]},
                           "median_seconds": {"baseline": 1.5, "candidate": 1.35}, "speedup": 1.11,
                           "candidate_progress": {"state": "completed"}}
                          for i in range(1, 4)],
        }
        foreground = {
            "benchmark": "verification_foreground_get_latency", "method": "engine Store::get",
            "scope": "no HTTP overhead", "foreground_reads": 500,
            "seed": {"total_payload_bytes": 8192},
            "baseline_idle": {"overlapped_requests": 0, "requests_per_second": 1000,
                              "p50_ms": .1, "p95_ms": .2, "total_ms": 500},
            "while_verification_runs": {"overlapped_requests": 500, "requests_per_second": 900,
                                         "p50_ms": .11, "p95_ms": .25, "total_ms": 555},
        }
        inputs = [(Path("verification.json"), verification, "v-digest"),
                  (Path("foreground.json"), foreground, "f-digest")]
        rows = report.historical_result_rows(inputs)
        durations = [row for row in rows if row["kind"] == "verification_duration"]
        foreground_rows = [row for row in rows if row["kind"] == "verification_foreground"]
        self.assertEqual(len(durations), 3)
        self.assertIsNone(durations[0]["throughput_unit"])
        self.assertEqual(durations[0]["baseline_seconds"], 1.5)
        self.assertEqual(durations[0]["raw"]["candidate_progress"]["state"], "completed")
        self.assertEqual(len(foreground_rows), 2)
        self.assertEqual(foreground_rows[1]["p95_ms"], .25)
        self.assertIsNone(foreground_rows[1]["p99"])
        counts = report.historical_row_counts(inputs, rows)
        self.assertEqual([row["verification_scenarios"] for row in counts], [3, 0])
        self.assertEqual([row["foreground_rows"] for row in counts], [0, 2])
        self.assertEqual([row["row_count"] for row in counts], [3, 2])

    def test_direct_and_aggregate_streaming_rows_keep_mib_units_and_raw_records(self):
        raw = {"schema": "skerry.streaming-capacity.v1", "mode": "put", "workers": 8,
               "file_mib": 16, "throughput_mib_s": 321.5, "binary_blake3": "fp",
               "configuration": {"writer_lanes": 4}, "counts": {"rejected_puts": 0, "unknown_puts": 1},
               "put_latency": {"p99_ms": 23.0}}
        direct = report.streaming_input_rows([(Path("direct.json"), raw, "digest")])
        self.assertEqual(len(direct), 1)
        self.assertEqual(direct[0]["throughput_mib_s"], 321.5)
        self.assertEqual(direct[0]["throughput_unit"], "MiB/s")
        self.assertEqual(direct[0]["p99"], 23.0)
        self.assertEqual(direct[0]["raw"], raw)
        aggregate = {"source_reports": {"streaming": [raw]}}
        historical = report.streaming_rows_from_report(aggregate, "historical.json")
        self.assertEqual(len(historical), 1)
        self.assertEqual(historical[0]["writer_lanes"], 4)
        self.assertEqual(historical[0]["origin"], "historical")

    def test_html_embeds_escaped_json_and_renders_with_text_content(self):
        html = report.html_document([{"study": "</script><script>alert(1)</script>", "phase": "fixture"}])
        self.assertIn("\\u003c/script\\u003e", html)
        self.assertIn("textContent", html)
        self.assertNotIn("innerHTML", html)
        self.assertNotIn("</script><script>alert(1)", html)

    @unittest.skipUnless(shutil.which("node"), "Node.js DOM-stub runtime")
    def test_html_dom_renders_numeric_rows_and_kind_filters(self):
        rows = [
            {"study": "new", "kind": "candidate_lane", "profile": "quick", "phase": "unique-put",
             "dataset": 4096, "clients": 8, "writer_lanes": 2, "ops_per_second": 123.5,
             "throughput_unit": "item operations/second", "p99": 7.2, "p99_unit": "milliseconds"},
            {"study": "old → new", "kind": "matched_version_lane_1", "phase": "unique-put", "dataset": 4096,
             "clients": 8, "writer_lanes": 1, "ops_per_second": 75, "throughput_unit": "item operations/second",
             "p99": 9.1, "p99_unit": "milliseconds"},
            {"study": "old", "kind": "historical_http", "profile": "old", "phase": "put", "dataset": 4,
             "item_ops_per_second": 88, "request_p99_ms": 3.5, "raw": {"raw": "source"}},
            {"study": "protocol", "kind": "protocol_phase", "phase": "commit", "before_ops_per_second": 10,
             "after_ops_per_second": 12},
            {"study": "large", "kind": "streaming", "profile": "streaming-capacity", "phase": "put",
             "dataset": 16, "clients": 4, "writer_lanes": 2, "throughput_mib_s": 42.5,
             "throughput_unit": "MiB/s", "p99": 20, "p99_unit": "milliseconds"},
        ]
        with tempfile.TemporaryDirectory() as directory:
            html_path = Path(directory) / "report.html"
            html_path.write_text(report.html_document(rows))
            script = Path(__file__).with_name("test_multi_writer_report_dom.js")
            result = subprocess.run(["node", str(script), str(html_path)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("DOM report checks passed", result.stdout)


if __name__ == "__main__":
    unittest.main()
