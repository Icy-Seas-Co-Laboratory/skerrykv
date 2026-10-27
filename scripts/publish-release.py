#!/usr/bin/env python3
"""Validate packaged release artifacts and publish a verified GitHub draft."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
import sys
import time
import tarfile
import tomllib
import zipfile

ROOT = Path(__file__).resolve().parent.parent
TARGETS = ("x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu", "x86_64-pc-windows-msvc", "x86_64-apple-darwin", "aarch64-apple-darwin")
TAG = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\Z")
COMMIT = re.compile(r"[0-9a-f]{40}\Z")


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def validate_archive(path, stem, target, version, commit):
    required = {"build-info.json", "config.example.toml", "LICENSE", "skerry.exe" if target.endswith('windows-msvc') else "skerry"}
    seen = set()
    info = None
    def check(name, regular, read):
        nonlocal info
        parts = PurePosixPath(name).parts
        if not regular or not name or '\\' in name or name.startswith('/') or len(parts) < 2 or parts[0] != stem or any(p in ('', '.', '..') for p in name.split('/')):
            raise ValueError(f"unsafe archive entry: {name}")
        relative = '/'.join(parts[1:])
        if relative in seen:
            raise ValueError(f"duplicate archive entry: {name}")
        seen.add(relative)
        if relative == 'build-info.json':
            info = json.loads(read())
    if path.name.endswith('.zip'):
        with zipfile.ZipFile(path) as archive:
            for entry in archive.infolist():
                mode = entry.external_attr >> 16
                regular = not entry.is_dir() and (stat.S_IFMT(mode) in (0, stat.S_IFREG))
                check(entry.filename, regular, lambda: archive.read(entry))
    else:
        with tarfile.open(path, 'r:gz') as archive:
            for entry in archive:
                check(entry.name, entry.isfile(), lambda: archive.extractfile(entry).read())
    if not required <= seen:
        raise ValueError(f"{path.name}: missing required files: {sorted(required - seen)}")
    if not isinstance(info, dict) or any(info.get(k) != v for k, v in {'version': version, 'target': target, 'git_commit': commit}.items()):
        raise ValueError(f"{path.name}: build-info does not match release")


def validate(dist, tag, commit):
    match = TAG.fullmatch(tag)
    if not match:
        raise ValueError('release tag must be a stable vX.Y.Z version')
    version = tag[1:]
    with (ROOT / 'Cargo.toml').open('rb') as stream:
        if tomllib.load(stream)['package']['version'] != version:
            raise ValueError('release tag does not match Cargo.toml version')
    if not COMMIT.fullmatch(commit):
        raise ValueError('release commit must be 40 lowercase hexadecimal characters')
    if dist.is_symlink() or not dist.is_dir():
        raise ValueError('dist must be a non-symlink directory')
    archives = {f"skerry-{tag}-{target}" + ('.zip' if target.endswith('windows-msvc') else '.tar.gz'): target for target in TARGETS}
    expected = set(archives) | {name + '.sha256' for name in archives}
    entries = list(dist.iterdir())
    names = {p.name for p in entries}
    if names not in (expected, expected | {'SHA256SUMS'}) or any(p.is_symlink() or not p.is_file() for p in entries):
        raise ValueError('dist must contain exactly the five archives and their five checksum sidecars, all regular files')
    sums = []
    for name, target in sorted(archives.items()):
        path = dist / name
        value = digest(path)
        if (dist / (name + '.sha256')).read_bytes() != f'{value}  {name}\n'.encode('ascii'):
            raise ValueError(f'{name}: invalid checksum sidecar')
        stem = name[:-4] if name.endswith('.zip') else name[:-7]
        validate_archive(path, stem, target, version, commit)
        sums.append(f'{value}  {name}\n')
    sums_path = dist / 'SHA256SUMS'
    sums_bytes = ''.join(sums).encode('ascii')
    if sums_path.exists() and sums_path.read_bytes() != sums_bytes:
        raise ValueError('existing SHA256SUMS does not match archives')
    sums_path.write_bytes(sums_bytes)
    return sorted([dist / name for name in expected] + [sums_path], key=lambda p: p.name)


def gh(*args, missing_ok=False):
    result = subprocess.run(['gh', *args], text=True, capture_output=True)
    if result.returncode:
        if missing_ok and ('HTTP 404' in result.stderr or 'HTTP 404' in result.stdout):
            return None
        raise RuntimeError(f"gh {' '.join(args[:3])} failed: {result.stderr.strip()}")
    return result.stdout


def api(path, missing_ok=False):
    output = gh('api', path, missing_ok=missing_ok)
    return json.loads(output) if output is not None else None


def find_release(repo, tag):
    # GitHub's tag lookup endpoint does not return drafts, even when authenticated.
    pages = json.loads(gh('api', '--paginate', '--slurp', f'repos/{repo}/releases?per_page=100'))
    matches = [release for page in pages for release in page if release.get('tag_name') == tag]
    if len(matches) > 1:
        raise ValueError('multiple releases have the requested tag')
    return matches[0] if matches else None


def wait_for_release(repo, tag):
    deadline = time.monotonic() + 30
    while True:
        release = find_release(repo, tag)
        if release is not None:
            return release
        if time.monotonic() >= deadline:
            raise ValueError('created draft did not appear in the releases list')
        time.sleep(2)


def verify_tag(repo, tag, commit):
    obj = api(f'repos/{repo}/git/ref/tags/{tag}').get('object', {})
    for depth in range(17):
        if obj.get('type') == 'commit':
            if obj.get('sha') != commit:
                raise ValueError('remote tag does not point to the expected commit')
            return
        if depth == 16:
            break
        if obj.get('type') != 'tag' or not COMMIT.fullmatch(obj.get('sha', '')):
            raise ValueError('remote tag has an invalid target')
        obj = api(f"repos/{repo}/git/tags/{obj['sha']}").get('object', {})
    raise ValueError('remote tag exceeds annotated tag depth limit')


def inspect_draft(endpoint, commit, expected, require_all=False):
    # GitHub can briefly return a null digest after an asset upload.
    deadline = time.monotonic() + 30
    while True:
        release = api(endpoint)
        if not release.get('draft'):
            raise ValueError('release is published; refusing to modify it')
        if release.get('target_commitish') != commit:
            raise ValueError('existing draft targets a different commit')
        assets = release.get('assets', [])
        seen = set()
        pending = False
        for asset in assets:
            name = asset.get('name')
            if name in seen or name not in expected:
                raise ValueError('draft contains unexpected or duplicate assets')
            seen.add(name)
            value, size = expected[name]
            if asset.get('size') != size or (asset.get('digest') is not None and asset['digest'] != value):
                raise ValueError(f'draft asset does not match local file: {name}')
            if asset.get('digest') is None:
                pending = True
        if require_all and seen != set(expected):
            raise ValueError('uploaded assets do not match expected names')
        if not pending:
            return seen
        if time.monotonic() >= deadline:
            raise ValueError('GitHub asset digests are unavailable')
        time.sleep(2)


def publish(dist, tag, commit, repo):
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo):
        raise ValueError('GH_REPO must be OWNER/REPO')
    assets = validate(dist, tag, commit)
    expected = {p.name: ('sha256:' + digest(p), p.stat().st_size) for p in assets}
    verify_tag(repo, tag, commit)
    release = find_release(repo, tag)
    if release is not None:
        if not release.get('draft'):
            raise ValueError('release already exists and is published; refusing to overwrite')
        if release.get('target_commitish') != commit:
            raise ValueError('existing draft targets a different commit')
    else:
        notes = ROOT / 'docs' / 'releases' / f'{tag}.md'
        args = ['release', 'create', tag, '--verify-tag', '--target', commit, '--draft', '--title', f'SkerryKV {tag}']
        args += ['--notes-file', str(notes)] if notes.is_file() else ['--generate-notes']
        gh(*args)
        release = wait_for_release(repo, tag)
    release_id = release.get('id')
    if not isinstance(release_id, int) or isinstance(release_id, bool) or release_id <= 0:
        raise ValueError('release has an invalid ID')
    endpoint = f'repos/{repo}/releases/{release_id}'
    existing = inspect_draft(endpoint, commit, expected)
    missing = [p for p in assets if p.name not in existing]
    if missing:
        gh('release', 'upload', tag, *(str(p) for p in missing))
    inspect_draft(endpoint, commit, expected, require_all=True)
    latest = api(f'repos/{repo}/releases/latest', missing_ok=True)
    latest_tag = TAG.fullmatch(latest.get('tag_name', '')) if latest else None
    # Preserve latest when its version cannot be compared safely.
    make_latest = latest is None or (latest_tag is not None and tuple(map(int, TAG.fullmatch(tag).groups())) >= tuple(map(int, latest_tag.groups())))
    verify_tag(repo, tag, commit)
    inspect_draft(endpoint, commit, expected, require_all=True)
    gh('release', 'edit', tag, '--draft=false', '--latest=' + str(make_latest).lower())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dist', type=Path, required=True)
    args = parser.parse_args()
    try:
        for key in ('GH_TOKEN', 'GH_REPO', 'RELEASE_TAG', 'RELEASE_COMMIT'):
            if not os.environ.get(key):
                raise ValueError(f'{key} must be set')
        publish(args.dist, os.environ['RELEASE_TAG'], os.environ['RELEASE_COMMIT'], os.environ['GH_REPO'])
    except (ValueError, OSError, RuntimeError, KeyError, json.JSONDecodeError, tarfile.TarError, zipfile.BadZipFile) as exc:
        print(f'release publication failed: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
