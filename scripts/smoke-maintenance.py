#!/usr/bin/env python3
"""Exercise upgrade/deletion/GC/vacuum in disposable local stores (Python 3.11+)."""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
import tomllib


def clean_environment():
    return {key: value for key, value in os.environ.items() if not key.startswith("SKERRY_")}


def cli(binary, *arguments, success=True):
    result = subprocess.run([str(binary), *map(str, arguments)], env=clean_environment(),
                            capture_output=True, text=True, timeout=120)
    if success and result.returncode:
        raise RuntimeError(f"{arguments[0]} failed: {result.stderr}")
    return result


class Service:
    def __init__(self, binary, config):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            self.port = listener.getsockname()[1]
        self.secret = secrets.token_urlsafe(32)
        environment = clean_environment()
        environment.update(SKERRY_TOKEN=self.secret, SKERRY_MCP_ENABLED="false",
                           SKERRY_LISTEN=f"127.0.0.1:{self.port}")
        self.child = subprocess.Popen([str(binary), "serve", "--config", str(config)],
                                      env=environment, stdin=subprocess.DEVNULL,
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            for _ in range(200):
                if self.child.poll() is not None:
                    raise RuntimeError("smoke service exited before readiness")
                try:
                    if self.call("GET", "/v1/health")[0] == 200:
                        return
                except OSError:
                    pass
                time.sleep(0.025)
            raise TimeoutError("smoke service readiness timeout")
        except BaseException:
            self.close()
            raise

    def call(self, method, path, data=None, authorized=True):
        headers = {"Authorization": f"Bearer {self.secret}"} if authorized else {}
        if method == "PUT":
            headers["Content-Type"] = "application/octet-stream"
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=30)
        try:
            connection.request(method, path, body=data, headers=headers)
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()

    def close(self):
        if self.child.poll() is None:
            try:
                self.call("POST", "/v1/admin/shutdown")
                self.child.wait(timeout=30)
            except (OSError, subprocess.TimeoutExpired):
                self.child.kill()
                self.child.wait()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def snapshot(root):
    return {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in root.rglob("*") if path.is_file() and path.name != "store.lock"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("target/release/skerry"))
    parser.add_argument("--legacy", type=Path, help="Optional v0.1 executable for real v1 migration")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve()
    initializer = args.legacy.resolve() if args.legacy else binary
    results = {"binary_version": cli(binary, "--version").stdout.strip(), "checks": []}
    with tempfile.TemporaryDirectory(prefix="skerry-maintenance-smoke-") as temporary:
        archive = Path(temporary) / "archive"
        cli(initializer, "init", "--path", archive)
        config = archive / "config.toml"
        root = Path(tomllib.loads(config.read_text())["storage"]["root"])
        if not root.is_absolute():
            root = (Path.cwd() / root).resolve()
        payloads = [b"survivor", b"delete this object", b"", bytes(range(256)) * 512]
        hashes = []
        with Service(initializer, config) as service:
            for payload in payloads:
                status, body = service.call("PUT", "/v1/objects", payload)
                assert status == 200, (status, body)
                hashes.append(json.loads(body)["hash"])
        if args.legacy:
            cli(binary, "upgrade", "--path", archive)
            assert cli(initializer, "verify", "--path", archive, success=False).returncode != 0
            results["checks"].append("real_v1_upgrade_and_old_binary_rejection")
        with Service(binary, config) as service:
            status, _ = service.call("DELETE", f"/v1/objects/{hashes[1]}", authorized=False)
            assert status in (401, 403)
            for _ in range(2):
                assert service.call("DELETE", f"/v1/objects/{hashes[1]}")[0] == 204
            assert service.call("GET", f"/v1/objects/{hashes[1]}")[0] == 404
            assert service.call("HEAD", f"/v1/objects/{hashes[1]}")[0] == 404
            status, body = service.call("POST", "/v1/admin/gc?force=true")
            assert status == 200, (status, body)
            results["collection"] = json.loads(body)
        before = snapshot(root)
        cli(binary, "vacuum", "--path", archive, "--dry-run")
        assert snapshot(root) == before, "dry-run modified the store"
        results["vacuum"] = json.loads(cli(binary, "vacuum", "--path", archive).stdout)
        cli(binary, "verify", "--path", archive)
        cli(binary, "rebuild-index", "--path", archive)
        with Service(binary, config) as service:
            for i, payload in enumerate(payloads):
                status, body = service.call("GET", f"/v1/objects/{hashes[i]}")
                assert (status == 404) if i == 1 else (status == 200 and body == payload)
            status, body = service.call("PUT", "/v1/objects", payloads[1])
            assert status == 200 and json.loads(body)["hash"] == hashes[1]
        results["checks"] += ["authorized_idempotent_delete", "empty_object_preserved",
                              "forced_collection", "nonmutating_dry_run", "full_vacuum",
                              "verify_and_rebuild_keep_deletion", "reput_recreates"]
    results["passed"] = True
    text = json.dumps(results, indent=2) + "\n"
    if args.output:
        args.output.write_text(text)
    print(text, end="")


if __name__ == "__main__":
    main()
