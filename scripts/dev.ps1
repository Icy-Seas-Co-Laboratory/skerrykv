param([switch]$Tui)
$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepoRoot
try {
    if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) { throw 'cargo is required (install Rust 1.90 or later)' }
    if (-not (Get-Command rustc -ErrorAction SilentlyContinue)) { throw 'rustc is required (install Rust 1.90 or later)' }
    if (-not (Test-Path config.toml)) {
        Copy-Item config.example.toml config.toml
        New-Item -ItemType Directory -Force data | Out-Null
    }
    if ($Tui) {
        cargo run -- serve --config config.toml --tui
    } else {
        cargo run -- serve --config config.toml
    }
    $CargoExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}
if ($CargoExitCode -ne 0) { exit $CargoExitCode }
