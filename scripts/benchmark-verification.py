#!/usr/bin/env python3
"""Compare two optimized skerry executables using isolated, cache-warm stores."""
import argparse
import hashlib
import http.client
import importlib.util
import json
from pathlib import Path
import statistics
import platform
import struct
import tempfile
import time

spec = importlib.util.spec_from_file_location("smoke", Path(__file__).with_name("smoke-maintenance.py"))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


def batch(service, payloads):
    frames = []
    for ordinal, data in enumerate(payloads):
        frames.append(struct.pack("<I4sBBHQ32sQ", 56 + len(data), b"SKB1", 1, 0, 0, ordinal, bytes(32), len(data)) + data)
    connection = http.client.HTTPConnection("127.0.0.1", service.port, timeout=120)
    try:
        connection.request("POST", "/v1/objects/batch-put", b"".join(frames), {"Authorization": "Bearer " + service.secret, "Content-Type": "application/vnd.skerry.batch-v1"})
        response = connection.getresponse()
        result = response.read()
        assert response.status == 200
        hashes = []
        for offset in range(0, len(result), 60):
            length, magic, kind, status, flags, ordinal, digest, size = struct.unpack("<I4sBBHQ32sQ", result[offset:offset + 60])
            assert (length, magic, kind, status, flags, size) == (56, b"SKB1", 129, 0, 0, 0)
            hashes.append(digest.hex())
        assert len(hashes) == len(payloads)
        return hashes
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.repeats <= 10:
        parser.error("--repeats must be 1..10")
    baseline, candidate = args.baseline.resolve(), args.candidate.resolve()
    report = {"baseline": {"name": baseline.name, "version": smoke.cli(baseline, "--version").stdout.strip(), "sha256": hashlib.sha256(baseline.read_bytes()).hexdigest()}, "candidate": {"name": candidate.name, "version": smoke.cli(candidate, "--version").stdout.strip(), "sha256": hashlib.sha256(candidate.read_bytes()).hexdigest()}, "platform": platform.platform(), "method": "alternating executables; warmup excluded; OS cache not evicted; wall time includes CLI and private index reconstruction; isolated temporary stores", "scenarios": []}
    with tempfile.TemporaryDirectory(prefix="skerry-verify-bench-") as temporary:
        for name, count, size, churn in [("small_objects", 2048, 4096, False), ("large_objects", 256, 512 * 1024, False), ("deletion_churn", 256, 4096, True)]:
            archive = Path(temporary) / name
            smoke.cli(baseline, "init", "--path", archive)
            with smoke.Service(candidate, archive / "config.toml") as service:
                hashes = []
                for start in range(0, count, 32):
                    payloads = [(hashlib.sha256(str(i).encode()).digest() * ((size + 31) // 32))[:size] for i in range(start, min(start + 32, count))]
                    hashes.extend(batch(service, payloads))
                if churn:
                    for digest in hashes[::2]:
                        assert service.call("DELETE", "/v1/objects/" + digest)[0] == 204
            # Seal to exercise whole-segment validation; graceful service close seals.
            timings = {"baseline": [], "candidate": []}
            final_progress = None
            for repetition in range(args.repeats + 1):
                for label, binary in [("baseline", baseline), ("candidate", candidate)]:
                    start = time.perf_counter()
                    result = smoke.cli(binary, "verify", "--path", archive)
                    elapsed = time.perf_counter() - start
                    assert json.loads(result.stdout)["status"] == "verified"
                    if label == "candidate":
                        snapshots = [line.removeprefix("verify progress: ") for line in result.stderr.splitlines() if line.startswith("verify progress: ")]
                        final_progress = json.loads(snapshots[-1]) if snapshots else None
                    if repetition:
                        timings[label].append(elapsed)
            medians = {key: statistics.median(values) for key, values in timings.items()}
            report["scenarios"].append({"name": name, "objects_written": count, "payload_bytes": count * size, "deleted": count // 2 if churn else 0, "seconds": timings, "median_seconds": medians, "speedup": medians["baseline"] / medians["candidate"], "candidate_progress": final_progress})
            print(name, json.dumps(medians), flush=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
