import hashlib
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import unittest
import zipfile


ROOT = Path(__file__).resolve().parents[1]
with (ROOT / "Cargo.toml").open("rb") as cargo_file:
    VERSION = tomllib.load(cargo_file)["package"]["version"]
SCRIPT = ROOT / "scripts" / "package-release.py"
SPEC = importlib.util.spec_from_file_location("package_release", SCRIPT)
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)


class ReleasePackagingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.binary = self.base / "skerry"
        self.binary.write_text(f"#!/bin/sh\nprintf 'skerry {VERSION}\\n'\n", encoding="utf-8")
        self.binary.chmod(0o755)
        self.out = self.base / "dist"
        self.old_epoch = os.environ.get("SOURCE_DATE_EPOCH")
        os.environ["SOURCE_DATE_EPOCH"] = "1700000000"
        self.addCleanup(self._restore_epoch)

    def _restore_epoch(self):
        if self.old_epoch is None:
            os.environ.pop("SOURCE_DATE_EPOCH", None)
        else:
            os.environ["SOURCE_DATE_EPOCH"] = self.old_epoch

    def args(self, **overrides):
        values = {"target": "x86_64-unknown-linux-gnu", "version": VERSION,
                  "binary": str(self.binary), "output_dir": str(self.out),
                  "commit": "a" * 40}
        values.update(overrides)
        return type("Args", (), values)()

    def test_tar_roundtrip_digest_and_executable_mode(self):
        archive, sidecar = release.package(self.args())
        self.assertEqual(archive.name, f"skerry-v{VERSION}-x86_64-unknown-linux-gnu.tar.gz")
        expected = hashlib.sha256(archive.read_bytes()).hexdigest()
        self.assertEqual(sidecar.read_text(), f"{expected}  {archive.name}\n")
        with tarfile.open(archive, "r:gz") as tf:
            members = {member.name: member for member in tf.getmembers()}
            stem = archive.name[:-7]
            self.assertIn(f"{stem}/skerry", members)
            self.assertTrue(members[f"{stem}/skerry"].mode & 0o111)
            self.assertIn(f"{stem}/build-info.json", members)
            self.assertEqual(tf.extractfile(f"{stem}/build-info.json").read(),
                             b'{\n  "git_commit": "' + b"a" * 40 + b'",\n  "target": "x86_64-unknown-linux-gnu",\n  "version": "' + VERSION.encode() + b'"\n}\n')

    def test_binary_version_checked_for_native_target(self):
        original = release.native_target
        release.native_target = lambda: "x86_64-unknown-linux-gnu"
        self.addCleanup(setattr, release, "native_target", original)
        self.binary.write_text("#!/bin/sh\necho skerry 9.9.9\n", encoding="utf-8")
        self.binary.chmod(0o755)
        with self.assertRaisesRegex(ValueError, "binary --version"):
            release.package(self.args())

    def test_unsafe_version_rejected(self):
        for version in (f"../{VERSION}", f"{VERSION}/evil", f"v{VERSION}", f"{VERSION}\\evil"):
            with self.subTest(version=version), self.assertRaises(ValueError):
                release.package(self.args(version=version))

    def test_windows_zip_roundtrip_and_binary_mode(self):
        win_binary = self.base / "skerry.exe"
        win_binary.write_bytes(b"fake windows executable")
        win_args = self.args(target="x86_64-pc-windows-msvc", binary=str(win_binary))
        archive, _ = release.package(win_args)
        with zipfile.ZipFile(archive) as zf:
            stem = archive.name[:-4]
            info = zf.getinfo(f"{stem}/skerry.exe")
            self.assertTrue((info.external_attr >> 16) & 0o111)
            self.assertEqual(zf.read(info), b"fake windows executable")


if __name__ == "__main__":
    unittest.main()
