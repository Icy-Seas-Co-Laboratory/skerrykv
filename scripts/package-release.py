#!/usr/bin/env python3
"""Create a deterministic, self-contained skerry release archive."""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tarfile
import tempfile
import tomllib
from typing import NoReturn
import zipfile


TARGETS = {
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "x86_64-pc-windows-msvc",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
}
VERSION_RE = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")


def fail(message: str) -> NoReturn:
    raise ValueError(message)


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def native_target() -> str | None:
    machine = platform.machine().lower()
    system = platform.system()
    arch = {"amd64": "x86_64", "arm64": "aarch64"}.get(machine, machine)
    if system == "Linux":
        return f"{arch}-unknown-linux-gnu"
    if system == "Darwin":
        return f"{arch}-apple-darwin"
    if system == "Windows":
        return f"{arch}-pc-windows-msvc"
    return None


def source_file(root: Path, relative: str, required: bool = True) -> Path | None:
    path = root / relative
    if not path.exists() and not path.is_symlink():
        if required:
            fail(f"required release input is missing: {relative}")
        return None
    if path.is_symlink() or not path.is_file():
        fail(f"release input must be a regular non-symlink file: {relative}")
    return path


def git_metadata(root: Path, supplied_commit: str | None) -> tuple[str, int]:
    if supplied_commit is None:
        try:
            commit = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=root, text=True, stderr=subprocess.DEVNULL
            ).strip()
        except (OSError, subprocess.CalledProcessError):
            fail("cannot determine git HEAD; pass --commit with a 40-character git SHA")
    else:
        commit = supplied_commit
    if not COMMIT_RE.fullmatch(commit):
        fail("commit must be exactly 40 lowercase hexadecimal characters")
    epoch_text = os.environ.get("SOURCE_DATE_EPOCH")
    if epoch_text is not None:
        try:
            epoch = int(epoch_text)
            if epoch < 0:
                raise ValueError
        except ValueError:
            fail("SOURCE_DATE_EPOCH must be a non-negative integer")
    else:
        try:
            epoch = int(subprocess.check_output(
                ["git", "show", "-s", "--format=%ct", commit], cwd=root,
                text=True, stderr=subprocess.DEVNULL
            ).strip())
        except (OSError, subprocess.CalledProcessError, ValueError):
            fail("cannot determine commit timestamp; set SOURCE_DATE_EPOCH")
    return commit, epoch


def package(args: argparse.Namespace) -> tuple[Path, Path]:
    if not VERSION_RE.fullmatch(args.version):
        fail("version must be a semantic version (X.Y.Z with optional prerelease/build metadata)")
    if args.target not in TARGETS:
        fail(f"unsupported target: {args.target}")

    root = project_root()
    cargo_path = source_file(root, "Cargo.toml")
    with cargo_path.open("rb") as stream:
        cargo = tomllib.load(stream)
    expected_version = cargo.get("package", {}).get("version")
    if args.version != expected_version:
        fail(f"version {args.version} does not match Cargo.toml version {expected_version}")

    binary_arg = Path(args.binary).expanduser()
    binary = binary_arg if binary_arg.is_absolute() else (Path.cwd() / binary_arg)
    if binary.is_symlink() or not binary.is_file():
        fail("binary must be an existing regular non-symlink file")
    expected_name = "skerry.exe" if args.target.endswith("windows-msvc") else "skerry"
    if binary.name != expected_name:
        fail(f"binary must be named {expected_name} for target {args.target}")

    if args.target == native_target():
        try:
            result = subprocess.run([str(binary), "--version"], check=True, capture_output=True, text=True)
        except (OSError, subprocess.CalledProcessError) as exc:
            fail(f"could not run binary --version: {exc}")
        if result.stdout.strip() != f"skerry {args.version}":
            fail(f"binary --version must print exactly 'skerry {args.version}'")

    commit, epoch = git_metadata(root, args.commit)
    # Tar's USTAR timestamps are limited to 8-digit octal; ZIP supports 1980 onward.
    archive_epoch = min(epoch, 8_640_000_000)
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    if output_dir.is_symlink() or not output_dir.is_dir():
        fail("output-dir must be a directory")

    stem = f"skerry-v{args.version}-{args.target}"
    is_windows = args.target.endswith("windows-msvc")
    archive = output_dir / (stem + (".zip" if is_windows else ".tar.gz"))
    sidecar = Path(str(archive) + ".sha256")
    files: list[tuple[str, Path | None, bytes | None]] = []
    files.append((expected_name, binary, None))
    names = ["config.example.toml", "README.md", "docs/MAINTENANCE.md", "docs/FORMAT.md", "docs/MCP.md", "docs/PROTOCOL.md", "ARCHITECTURE.md", "docs/RELEASES.md", "LICENSE"]
    for name in names:
        path = source_file(root, name, required=name not in {"docs/RELEASES.md", "LICENSE"})
        if path is not None:
            files.append((name, path, None))
    build_info = json.dumps(
        {"version": args.version, "target": args.target, "git_commit": commit},
        sort_keys=True, indent=2
    ).encode("utf-8") + b"\n"
    files.append(("build-info.json", None, build_info))

    # Build atomically so failed writes never leave a seemingly complete release.
    with tempfile.NamedTemporaryFile(dir=output_dir, delete=False) as temp:
        temporary = Path(temp.name)
    try:
        if is_windows:
            dos_dt = dt.datetime.fromtimestamp(max(315532800, archive_epoch), dt.timezone.utc)
            date_time = (dos_dt.year, dos_dt.month, dos_dt.day, dos_dt.hour, dos_dt.minute, dos_dt.second // 2 * 2)
            with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
                for name, path, data in files:
                    info = zipfile.ZipInfo(f"{stem}/{name}", date_time=date_time)
                    info.compress_type = zipfile.ZIP_DEFLATED
                    info.external_attr = ((0o100755 if name == expected_name else 0o100644) & 0xFFFF) << 16
                    payload = data if data is not None else path.read_bytes()
                    zf.writestr(info, payload)
        else:
            with temporary.open("wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=archive_epoch) as gz:
                with tarfile.open(fileobj=gz, mode="w", format=tarfile.USTAR_FORMAT) as tf:
                    for name, path, data in files:
                        payload = data if data is not None else path.read_bytes()
                        info = tarfile.TarInfo(f"{stem}/{name}")
                        info.size = len(payload)
                        info.mtime = archive_epoch
                        info.uid = info.gid = 0
                        info.uname = info.gname = ""
                        info.mode = 0o755 if name == expected_name else 0o644
                        import io
                        tf.addfile(info, io.BytesIO(payload))
        os.replace(temporary, archive)
    finally:
        temporary.unlink(missing_ok=True)

    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    sidecar.write_bytes(f"{digest}  {archive.name}\n".encode("ascii"))
    return archive, sidecar


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, choices=sorted(TARGETS))
    parser.add_argument("--version", required=True)
    parser.add_argument("--binary", required=True, help="path to the built skerry executable")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--commit", help="40-character GitHub SHA; defaults to git HEAD")
    args = parser.parse_args()
    try:
        archive, sidecar = package(args)
    except (OSError, ValueError, tarfile.TarError, zipfile.BadZipFile) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(archive)
    print(sidecar)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
