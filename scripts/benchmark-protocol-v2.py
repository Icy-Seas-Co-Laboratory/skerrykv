#!/usr/bin/env python3
"""Small warm-cache protocol comparison; all stores are disposable and local."""
import argparse
import concurrent.futures
import hashlib
import http.client
import importlib.util
import json
from pathlib import Path
import random
import statistics
import struct
import tempfile
import time

spec = importlib.util.spec_from_file_location("smoke", Path(__file__).with_name("smoke-maintenance.py"))
smoke = importlib.util.module_from_spec(spec)
spec.loader.exec_module(smoke)


def run_case(binary, payloads, trial):
    with tempfile.TemporaryDirectory(prefix="skerry-protocol-benchmark-") as temporary:
        archive = Path(temporary) / "archive"
        smoke.cli(binary, "init", "--path", archive)
        with smoke.Service(binary, archive / "config.toml") as service:
            def connection():
                return http.client.HTTPConnection("127.0.0.1", service.port, timeout=60)
            def call(conn, method, path, body=None, media="application/octet-stream"):
                conn.request(method, path, body=body, headers={"Authorization": f"Bearer {service.secret}", "Content-Type": media})
                response = conn.getresponse()
                data = response.read()
                assert response.status == 200, (response.status, data)
                return data
            conn = connection()
            hashes = []
            phases = []
            def measure(name, operations, function):
                start = time.perf_counter()
                function()
                elapsed = time.perf_counter() - start
                phases.append({"phase": name, "operations": operations, "seconds": elapsed, "ops_per_second": operations / elapsed})
            def writes():
                for payload in payloads:
                    value = json.loads(call(conn, "PUT", "/v1/objects", payload))
                    assert value["duplicate"] is False
                    hashes.append(value["hash"])
            measure("single_write", len(payloads), writes)
            def duplicates():
                for payload in payloads:
                    assert json.loads(call(conn, "PUT", "/v1/objects", payload))["duplicate"] is True
            measure("duplicate_write", len(payloads), duplicates)
            order = list(range(len(payloads)))
            random.Random(20261027).shuffle(order)
            for i in order:
                assert call(conn, "GET", f"/v1/objects/{hashes[i]}") == payloads[i]
            def reads():
                for i in order:
                    assert call(conn, "GET", f"/v1/objects/{hashes[i]}") == payloads[i]
            measure("random_read", len(payloads), reads)
            def bulk():
                for start in range(0, len(payloads), 64):
                    batch = payloads[start:start + 64]
                    request = bytearray()
                    for ordinal, payload in enumerate(batch):
                        changed = bytes([payload[0] ^ 1]) + payload[1:]
                        request.extend(struct.pack("<I4sBBHQ32sQ", 56 + len(changed), b"SKB1", 1, 0, 0, ordinal, bytes(32), len(changed)))
                        request.extend(changed)
                    data = call(conn, "POST", "/v1/objects/batch-put", request, "application/vnd.skerry.batch-v1")
                    assert len(data) == len(batch) * 60
                    for ordinal in range(len(batch)):
                        frame = data[ordinal * 60:(ordinal + 1) * 60]
                        assert frame[8] == 129 and frame[9] == 0
                        assert int.from_bytes(frame[12:20], "little") == ordinal
            measure("batch64_write", len(payloads), bulk)
            conn.close()
            def mixed():
                def lane(worker):
                    client = connection()
                    try:
                        for position in range(worker, len(order), 8):
                            i = order[position]
                            if position in write_positions:
                                changed = bytes([payloads[i][0] ^ 2]) + payloads[i][1:]
                                assert json.loads(call(client, "PUT", "/v1/objects", changed))["duplicate"] is False
                            else:
                                assert call(client, "GET", f"/v1/objects/{hashes[i]}") == payloads[i]
                    finally:
                        client.close()
                list(executor.map(lane, range(8)))
            write_positions = set(random.Random(7).sample(range(len(order)), (len(order) + 3) // 4))
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                list(executor.map(lambda _: None, range(8)))
                measure("mixed_25_percent_write", len(payloads), mixed)
            return {"binary": str(binary), "trial": trial, "phases": phases}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--records", type=int, default=128)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert args.records > 0 and args.trials > 0
    rng = random.Random(20261027)
    payloads = [rng.randbytes(20 * 1024) for _ in range(args.records)]
    binaries = {name: path.resolve() for name, path in [("before", args.before), ("after", args.after)]}
    results = []
    for trial in range(1, args.trials + 1):
        cases = list(binaries.items())
        random.Random(20261027 + trial).shuffle(cases)
        for name, binary in cases:
            print(f"trial {trial}: {name}", flush=True)
            value = run_case(binary, payloads, trial)
            value["profile"] = name
            results.append(value)
    summary = []
    for phase in results[0]["phases"]:
        name = phase["phase"]
        rates = {profile: [p["ops_per_second"] for r in results if r["profile"] == profile for p in r["phases"] if p["phase"] == name] for profile in binaries}
        before, after = (statistics.median(rates[p]) for p in ("before", "after"))
        summary.append({"phase": name, "before_ops_s": before, "after_ops_s": after, "after_before": after / before})
    output = {"records": args.records, "payload_bytes": 20 * 1024, "trials": args.trials,
              "binary_sha256": {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in binaries.items()},
              "scope": "Small warm-cache loopback HTTP comparison; sequential randomized case order; identical fixtures; GC disabled; default 1ms batching for both; not the full original benchmark matrix.",
              "results": results, "summary": summary}
    args.output.write_text(json.dumps(output, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
