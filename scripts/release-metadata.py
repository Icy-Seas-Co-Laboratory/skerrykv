#!/usr/bin/env python3
"""Validate a release tag against its checked-out commit and package version."""
import json
import os
from pathlib import Path
import re
import subprocess
import tomllib


def metadata(root: Path, tag: str = "") -> dict:
    version = tomllib.loads((root / "Cargo.toml").read_text())["package"]["version"]
    if not re.fullmatch(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)", version):
        raise ValueError("release automation requires a stable X.Y.Z package version")
    def git(*arguments):
        return subprocess.check_output(["git", *arguments], cwd=root, text=True).strip()
    commit = git("rev-parse", "HEAD")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("invalid checked-out commit")
    if tag:
        if tag != f"v{version}":
            raise ValueError("release tag must exactly match v<Cargo.toml version>")
        if git("rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}") != commit:
            raise ValueError("release tag does not identify the checked-out commit")
        subprocess.run(["git", "merge-base", "--is-ancestor", commit, "origin/main"],
                       cwd=root, check=True)
    return {"version": version, "commit": commit}


if __name__ == "__main__":
    result = metadata(Path(__file__).resolve().parents[1], os.environ.get("RELEASE_TAG", ""))
    if output := os.environ.get("GITHUB_OUTPUT"):
        with open(output, "a", encoding="utf-8") as stream:
            for name, value in result.items():
                stream.write(f"{name}={value}\n")
    print(json.dumps(result))
