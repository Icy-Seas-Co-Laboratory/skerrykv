# Installing releases and publishing new versions

Download a native archive from the repository's GitHub Releases page. Choose
your operating system and architecture; Apple Silicon uses `aarch64-apple-darwin`,
Intel Macs use `x86_64-apple-darwin`. Windows archives are ZIP; Unix archives are
gzip-compressed tar files. Check `SHA256SUMS` before extracting.

On Linux/macOS:

```sh
sha256sum --check skerry-vVERSION-TARGET.tar.gz.sha256 # macOS: shasum -a 256 -c ...
tar -xzf skerry-vVERSION-TARGET.tar.gz
cd skerry-vVERSION-TARGET
./skerry --version
sudo install -m 755 skerry /usr/local/bin/skerry
```

On Windows, use `Get-FileHash -Algorithm SHA256` to compare the archive with its
sidecar, expand the ZIP, and put `skerry.exe` in a directory on PATH. Preserve
your own configuration and data when upgrading. No service is started by unpacking.
macOS archives are not notarized; only approve a downloaded executable after
verifying its source and checksum through your usual macOS security controls.

## Release protocol

1. Update the package version in `Cargo.toml` and the corresponding package
   entry in `Cargo.lock`. Add release notes at `docs/releases/vX.Y.Z.md`, including
   any storage compatibility changes. The initial release is **v0.2.1**.
2. Commit and push main. CI runs the pinned Rust toolchain, strict linting, full
   native tests on five platform/architecture pairs, release maintenance smoke,
   and Docker restart/recreation persistence checks.
3. After CI succeeds, create an annotated matching tag and push it:

   ```sh
   git tag -a vX.Y.Z -m "SkerryKV vX.Y.Z"
   git push origin vX.Y.Z
   ```

4. The Release workflow checks that tag/version/commit agree and the commit is
   on main, repeats the validation pipeline, packages all five native builds,
   and verifies embedded build information and SHA-256 sidecars. It creates a
   draft, uploads every asset, verifies GitHub's uploaded digests, then publishes
   the complete release. Build jobs have read-only repository permissions;
   only the publisher receives `contents: write`.

Workflow dispatch accepts an existing `vX.Y.Z` tag to retry a failed build or
draft publication. Repository release immutability is enabled: published assets and their tags are
protected. Published releases are not overwritten; fix the source and publish a new version. Do not move a published tag. Older version releases do
not replace a newer latest release.

Native archives contain `build-info.json` identifying the version, Rust target
and exact Git commit. Archive file ordering, permissions and timestamps are
normalized for the same input executable; this is not a claim of independently
reproducible compiler output. macOS signing/notarization and Windows Authenticode
signing require separately configured developer identities.
