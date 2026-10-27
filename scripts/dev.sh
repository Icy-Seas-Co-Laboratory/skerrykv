#!/usr/bin/env sh
set -eu
repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_dir"
command -v cargo >/dev/null 2>&1 || { echo "cargo is required (install Rust 1.90 or later)" >&2; exit 1; }
command -v rustc >/dev/null 2>&1 || { echo "rustc is required (install Rust 1.90 or later)" >&2; exit 1; }
if [ ! -f config.toml ]; then
  cp config.example.toml config.toml
  mkdir -p data
fi
if [ "${1-}" = "--tui" ]; then
  exec cargo run -- serve --config config.toml --tui
fi
if [ "$#" -ne 0 ]; then
  echo "usage: scripts/dev.sh [--tui]" >&2
  exit 2
fi
exec cargo run -- serve --config config.toml
