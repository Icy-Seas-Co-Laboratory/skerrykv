#!/usr/bin/env python3
"""Check health, authorization and v2 persistence in an isolated Docker volume."""
import http.client
import json
import os
import secrets
import subprocess
import time
import uuid


def main():
    name = f"skerry-smoke-{uuid.uuid4().hex[:12]}"
    volume = f"{name}-data"
    secret = secrets.token_urlsafe(32)
    environment = dict(os.environ, SKERRY_TOKEN=secret)
    def docker(*arguments, check=True):
        return subprocess.run(["docker", *arguments], env=environment, check=check,
                              capture_output=True, text=True, timeout=120).stdout.strip()
    def start():
        docker("run", "-d", "--name", name, "-p", "127.0.0.1:0:7788", "-e", "SKERRY_TOKEN",
               "-e", "SKERRY_MCP_ENABLED=false", "--mount", f"type=volume,source={volume},target=/var/lib/skerry", "skerrykv:ci")
    def port():
        return int(docker("port", name, "7788/tcp").splitlines()[0].rsplit(":", 1)[1])
    def call(method, path, payload=None, token=secret):
        connection = http.client.HTTPConnection("127.0.0.1", current_port, timeout=30)
        try:
            connection.request(method, path, body=payload,
                               headers={"Authorization": f"Bearer {token}", "Content-Type": "application/octet-stream"})
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()
    def ready():
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                if call("GET", "/v1/health")[0] == 200:
                    docker("exec", name, "skerry", "health", "--url", "http://127.0.0.1:7788")
                    return
            except (OSError, http.client.HTTPException):
                pass
            time.sleep(0.25)
        raise TimeoutError("container health timeout")
    docker("volume", "create", volume)
    try:
        start()
        current_port = port()
        ready()
        hashes = []
        payloads = [b"persistent Docker object", b"", b"delete in container"]
        for payload in payloads:
            status, body = call("PUT", "/v1/objects", payload)
            assert status == 200, (status, body)
            hashes.append(json.loads(body)["hash"])
        assert call("GET", f"/v1/objects/{hashes[0]}", token="incorrect")[0] == 401
        assert call("DELETE", f"/v1/objects/{hashes[2]}")[0] == 204
        def verify():
            for i, payload in enumerate(payloads):
                status, body = call("GET", f"/v1/objects/{hashes[i]}")
                assert (status == 404) if i == 2 else (status == 200 and body == payload)
        docker("restart", name)
        current_port = port()
        ready()
        verify()
        docker("rm", "-f", name)
        start()
        current_port = port()
        ready()
        verify()
        assert call("POST", "/v1/admin/gc?force=true")[0] == 200
        verify()
        print(json.dumps({"passed": True, "checks": ["health_exit_code", "authorization", "restart_persistence", "recreate_volume_persistence", "deletion_fence", "empty_object", "forced_gc"]}))
    except BaseException:
        print(docker("logs", name, check=False))
        raise
    finally:
        docker("rm", "-f", name, check=False)
        docker("volume", "rm", volume, check=False)


if __name__ == "__main__":
    main()
