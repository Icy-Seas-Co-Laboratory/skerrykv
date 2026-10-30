import argparse
import importlib.util
from pathlib import Path
import subprocess
import sys
import unittest
import os

spec = importlib.util.spec_from_file_location("http_capacity", Path(__file__).resolve().parents[1] / "scripts" / "benchmark-http-capacity.py")
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def run(ops=100, rejected=0, unknown=0, missing=0, limited=False):
    return {"summary": {"item_ops_per_second": ops, "unknown_items": unknown, "rejected_items": rejected, "store_limited": limited},
            "latency": {"request_ms": {"p99_ms": 1}, "success_request_ms": {"p99_ms": 20}},
            "probes": {name: {"attempts": 10, "failures": 1, "missing_storage": missing if name == "status" else 0} for name in ("status", "metrics", "ready")}}


def args(**values):
    return argparse.Namespace(require_no_rejections=values.get("require_no_rejections", False), max_probe_failure_fraction=values.get("max_probe_failure_fraction"), max_throughput_regression_percent=15, max_p99_regression_percent=25)


def report(rows):
    return {"seconds": 3, "warmup_seconds": 1, "max_store_mib": 512,
            "cases": {"batch-c4": {"settings": {"workers": 4}, "summary": bench.summarize(rows)}}}


class CapacityBenchmarkTests(unittest.TestCase):
    def test_every_access_pattern_has_requested_worker_counts_and_lane_matrix(self):
        cases = bench.cases("quick")
        for workload in ("unique-put", "batch-put", "mixed", "random-get", "hot-get"):
            self.assertEqual({case["workers"] for case in cases if case["workload"] == workload}, {4, 8, 16, 32, 64})
        self.assertEqual({case["writer_lanes"] for case in cases if case["workload"] == "unique-put"}, {1, 2, 4, 8})
        self.assertEqual(len({case["name"] for case in cases}), len(cases))

    def test_writer_stress_profile_is_duplicate_and_lane_zero_skew_only(self):
        cases = bench.cases("writer-stress")
        self.assertEqual(len(cases), 2 * 5 * 4)
        self.assertEqual({case["workload"] for case in cases}, {"duplicate-put", "skew-put"})
        for workload in ("duplicate-put", "skew-put"):
            selected = [case for case in cases if case["workload"] == workload]
            self.assertEqual({case["workers"] for case in selected}, {4, 8, 16, 32, 64})
            self.assertEqual({case["writer_lanes"] for case in selected}, {1, 2, 4, 8})
            self.assertTrue(all(case["payload_bytes"] == 4096 and case["batch_items"] == 1 for case in selected))

    def test_buffered_profile_uses_two_mebibyte_batches_and_full_lane_matrix(self):
        cases = bench.cases("buffered")
        self.assertEqual(len(cases), 2 * 5 * 4)
        self.assertEqual({case["workers"] for case in cases}, {4, 8, 16, 32, 64})
        self.assertEqual({case["writer_lanes"] for case in cases}, {1, 2, 4, 8})
        self.assertEqual({case["payload_bytes"] for case in cases}, {32768, 131072})
        self.assertEqual({(case["payload_bytes"] * case["batch_items"]) for case in cases}, {2 * 1024 * 1024})
        self.assertTrue(all(case["workload"] == "batch-put" and case["client_memory_mib"] == 1024 for case in cases))
        self.assertEqual(len({case["name"] for case in cases}), len(cases))

    def test_success_latency_and_missing_storage_are_not_masked_by_fast_errors(self):
        summary = bench.summarize([run(100, missing=3), run(200, missing=1)])
        self.assertEqual(summary["item_ops_per_second"]["median"], 150)
        self.assertEqual(summary["request_p99_ms_median"], 20)
        self.assertEqual(summary["probes"]["status"]["unavailable_fraction"], 0.3)

    def test_limits_and_unknown_outcomes_fail_explicit_acceptance_contract(self):
        current = report([run(rejected=2, unknown=1, limited=True)])
        failures = bench.check_budgets(current, None, args(require_no_rejections=True, max_probe_failure_fraction=0))
        self.assertTrue(any("rejected or unknown" in failure for failure in failures))
        self.assertTrue(any("store budget" in failure for failure in failures))
        self.assertTrue(any("status unavailable" in failure for failure in failures))

    def test_regression_comparison_rejects_mismatched_baseline_and_slow_run(self):
        baseline = report([run(200)])
        current = report([run(100)])
        self.assertTrue(any("throughput regressed" in failure for failure in bench.check_budgets(current, baseline, args())))
        current["seconds"] = 10
        self.assertTrue(any("configuration/duration mismatch" in failure for failure in bench.check_budgets(current, baseline, args())))

    @unittest.skipUnless(os.name == "posix", "POSIX process groups")
    def test_harness_process_group_is_terminated(self):
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], start_new_session=True)
        try:
            bench.terminate_group(process, grace_seconds=0.2)
            self.assertIsNotNone(process.poll())
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()


if __name__ == "__main__":
    unittest.main()
