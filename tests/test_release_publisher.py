import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import tomllib
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location('publisher', ROOT / 'scripts/publish-release.py')
publisher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publisher)
COMMIT = 'a' * 40
with (ROOT / 'Cargo.toml').open('rb') as cargo_file:
    VERSION = tomllib.load(cargo_file)['package']['version']
TAG = f'v{VERSION}'
INVALID_TAG = f'v{int(VERSION.split(".")[0]) + 1}.0.0'
NEWER_TAG = INVALID_TAG


class PublisherTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.dist = Path(self.temp.name)
        for target in publisher.TARGETS:
            stem = f'skerry-{TAG}-{target}'
            windows = target.endswith('windows-msvc')
            path = self.dist / (stem + ('.zip' if windows else '.tar.gz'))
            files = {'skerry.exe' if windows else 'skerry': b'binary', 'LICENSE': b'license', 'config.example.toml': b'config', 'build-info.json': json.dumps({'version': TAG[1:], 'target': target, 'git_commit': COMMIT}).encode()}
            if windows:
                with zipfile.ZipFile(path, 'w') as archive:
                    for name, data in files.items():
                        archive.writestr(stem + '/' + name, data)
            else:
                with tarfile.open(path, 'w:gz') as archive:
                    for name, data in files.items():
                        entry = tarfile.TarInfo(stem + '/' + name)
                        entry.size = len(data)
                        archive.addfile(entry, io.BytesIO(data))
            self.sidecar(path)

    def sidecar(self, path):
        value = hashlib.sha256(path.read_bytes()).hexdigest()
        Path(str(path) + '.sha256').write_text(f'{value}  {path.name}\n', encoding='ascii')

    def test_valid_and_repeat(self):
        assets = publisher.validate(self.dist, TAG, COMMIT)
        self.assertEqual(len(assets), 11)
        self.assertEqual(assets, publisher.validate(self.dist, TAG, COMMIT))
        self.assertEqual(len((self.dist / 'SHA256SUMS').read_text().splitlines()), 5)

    def test_invalid_inputs_and_sidecar(self):
        for tag, commit in [(INVALID_TAG, COMMIT), (f'{TAG}-rc.1', COMMIT), (TAG, 'abc')]:
            with self.assertRaises(ValueError):
                publisher.validate(self.dist, tag, commit)
        next(self.dist.glob('*.sha256')).write_text('bad\n')
        with self.assertRaises(ValueError):
            publisher.validate(self.dist, TAG, COMMIT)

    def test_extra_and_symlink(self):
        (self.dist / 'extra').write_text('x')
        with self.assertRaises(ValueError):
            publisher.validate(self.dist, TAG, COMMIT)
        (self.dist / 'extra').unlink()
        sidecar = next(self.dist.glob('*.sha256'))
        sidecar.unlink()
        sidecar.symlink_to('/dev/null')
        with self.assertRaises(ValueError):
            publisher.validate(self.dist, TAG, COMMIT)

    def test_archive_rejects_links_and_wrong_metadata(self):
        path = next(self.dist.glob('*.tar.gz'))
        stem = path.name[:-7]
        with tarfile.open(path, 'w:gz') as archive:
            entry = tarfile.TarInfo(stem + '/skerry')
            entry.type = tarfile.SYMTYPE
            entry.linkname = '/etc/passwd'
            archive.addfile(entry)
        self.sidecar(path)
        with self.assertRaisesRegex(ValueError, 'unsafe'):
            publisher.validate(self.dist, TAG, COMMIT)

    def asset_metadata(self, paths):
        return [{'name': p.name, 'size': p.stat().st_size, 'digest': 'sha256:' + publisher.digest(p)} for p in paths]

    def test_publishes_only_after_uploaded_digests_match(self):
        calls = []
        def api(endpoint, missing_ok=False):
            calls.append(('api', endpoint))
            if '/git/ref/' in endpoint:
                return {'object': {'type': 'commit', 'sha': COMMIT}}
            if endpoint.endswith('/latest'):
                return {'tag_name': 'v0.3.0'}
            count = sum(1 for c in calls if c == ('api', endpoint))
            if count == 1:
                return None
            return {'draft': True, 'target_commitish': COMMIT, 'assets': [] if count == 2 else self.asset_metadata(self.dist.iterdir())}
        def gh(*args, **kwargs):
            calls.append(args)
            return ''
        with patch.object(publisher, 'api', side_effect=api), patch.object(publisher, 'gh', side_effect=gh):
            publisher.publish(self.dist, TAG, COMMIT, 'owner/repo')
        self.assertEqual(calls[-1], ('release', 'edit', TAG, '--draft=false', '--latest=false'))
        self.assertTrue(any(c[:3] == ('release', 'create', TAG) for c in calls))
        upload = next(c for c in calls if c[:2] == ('release', 'upload'))
        self.assertNotIn('--clobber', upload)
        self.assertEqual(sum('/git/ref/' in c[1] for c in calls if c[0] == 'api'), 2)

    def test_existing_published_and_unverified_assets_fail(self):
        with patch.object(publisher, 'verify_tag'), patch.object(publisher, 'api', return_value={'draft': False}), patch.object(publisher, 'gh') as gh:
            with self.assertRaisesRegex(ValueError, 'published'):
                publisher.publish(self.dist, TAG, COMMIT, 'owner/repo')
            gh.assert_not_called()
        draft = {'draft': True, 'target_commitish': COMMIT, 'assets': []}
        with patch.object(publisher, 'verify_tag'), patch.object(publisher, 'api', return_value=draft), patch.object(publisher, 'gh') as gh:
            with self.assertRaisesRegex(ValueError, 'expected names'):
                publisher.publish(self.dist, TAG, COMMIT, 'owner/repo')
            self.assertEqual(gh.call_count, 1)
            self.assertEqual(gh.call_args.args[:2], ('release', 'upload'))

    def test_resume_reuses_matching_assets_and_rejects_conflicts(self):
        paths = publisher.validate(self.dist, TAG, COMMIT)
        first = self.asset_metadata(paths[:1])
        full = self.asset_metadata(paths)
        calls = []
        def api(endpoint, missing_ok=False):
            if endpoint.endswith('/latest'):
                return None
            count = sum(c == endpoint for c in calls)
            calls.append(endpoint)
            return {'draft': True, 'target_commitish': COMMIT, 'assets': first if count < 2 else full}
        with patch.object(publisher, 'verify_tag'), patch.object(publisher, 'api', side_effect=api), patch.object(publisher, 'gh') as gh:
            publisher.publish(self.dist, TAG, COMMIT, 'owner/repo')
            upload = gh.call_args_list[0].args
            self.assertEqual(len(upload), 3 + 10)
            self.assertNotIn(str(paths[0]), upload)
        first[0]['digest'] = 'sha256:' + '0' * 64
        with patch.object(publisher, 'verify_tag'), patch.object(publisher, 'api', return_value={'draft': True, 'target_commitish': COMMIT, 'assets': first}), patch.object(publisher, 'gh') as gh:
            with self.assertRaisesRegex(ValueError, 'does not match'):
                publisher.publish(self.dist, TAG, COMMIT, 'owner/repo')
            gh.assert_not_called()

    def test_tag_dereferences_and_rejects_wrong_commit(self):
        annotated = 'b' * 40
        with patch.object(publisher, 'api', side_effect=[{'object': {'type': 'tag', 'sha': annotated}}, {'object': {'type': 'commit', 'sha': COMMIT}}]) as api:
            publisher.verify_tag('owner/repo', TAG, COMMIT)
            self.assertEqual(api.call_count, 2)
        with patch.object(publisher, 'api', return_value={'object': {'type': 'commit', 'sha': 'c' * 40}}):
            with self.assertRaisesRegex(ValueError, 'remote tag'):
                publisher.verify_tag('owner/repo', TAG, COMMIT)

    def test_missing_digest_polls_until_available(self):
        path = publisher.validate(self.dist, TAG, COMMIT)[0]
        asset = self.asset_metadata([path])[0]
        expected = {path.name: (asset['digest'], asset['size'])}
        pending = dict(asset, digest=None)
        with patch.object(publisher, 'api', side_effect=[{'draft': True, 'target_commitish': COMMIT, 'assets': [pending]}, {'draft': True, 'target_commitish': COMMIT, 'assets': [asset]}]), patch.object(publisher.time, 'sleep') as sleep:
            self.assertEqual(publisher.inspect_draft('endpoint', COMMIT, expected, True), {path.name})
            sleep.assert_called_once()


if __name__ == '__main__':
    unittest.main()
