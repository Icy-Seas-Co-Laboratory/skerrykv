import importlib.util
from pathlib import Path
import subprocess
import tempfile
import tomllib
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("metadata", ROOT / "scripts/release-metadata.py")
metadata = importlib.util.module_from_spec(spec)
spec.loader.exec_module(metadata)
COMMIT = "a" * 40
with (ROOT / "Cargo.toml").open("rb") as cargo_file:
    VERSION = tomllib.load(cargo_file)["package"]["version"]
TAG = f"v{VERSION}"
INVALID_TAG = f"v{int(VERSION.split('.')[0]) + 1}.0.0"


class MetadataTests(unittest.TestCase):
    def test_ci_uses_checked_out_commit(self):
        with patch.object(metadata.subprocess, "check_output", return_value=COMMIT + "\n"):
            result = metadata.metadata(ROOT)
        self.assertEqual(result, {"version": VERSION, "commit": COMMIT})

    def test_release_validates_tag_identity_and_main_ancestry(self):
        with patch.object(metadata.subprocess, "check_output", return_value=COMMIT + "\n"), \
             patch.object(metadata.subprocess, "run") as run:
            self.assertEqual(metadata.metadata(ROOT, TAG)["commit"], COMMIT)
            self.assertEqual(run.call_args.args[0], ["git", "merge-base", "--is-ancestor", COMMIT, "origin/main"])

    def test_mismatched_or_unsafe_tags_fail_before_tag_lookup(self):
        with patch.object(metadata.subprocess, "check_output", return_value=COMMIT + "\n") as git:
            for tag in [INVALID_TAG, f"{TAG}\ncommit=evil", f"{TAG};echo bad", f"{TAG}-rc.1"]:
                with self.assertRaises(ValueError):
                    metadata.metadata(ROOT, tag)
            self.assertTrue(all(call.args[0] == ["git", "rev-parse", "HEAD"] for call in git.call_args_list))

    def test_wrong_tag_commit_or_non_main_commit_fail(self):
        with patch.object(metadata.subprocess, "check_output", side_effect=[COMMIT, "b" * 40]):
            with self.assertRaises(ValueError):
                metadata.metadata(ROOT, TAG)
        with patch.object(metadata.subprocess, "check_output", return_value=COMMIT), \
             patch.object(metadata.subprocess, "run", side_effect=subprocess.CalledProcessError(1, ["git"])):
            with self.assertRaises(subprocess.CalledProcessError):
                metadata.metadata(ROOT, TAG)


if __name__ == "__main__":
    unittest.main()
