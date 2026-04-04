# Run the CI "build" job locally using Docker (Linux build).
# Requires: Docker Desktop with WSL2 backend.
# Usage: from repo root, run:
#   .\.github\scripts\ci-build-local.ps1
# Or from any dir:
#   pwsh -File .github/scripts/ci-build-local.ps1

$ErrorActionPreference = "Stop"
$repoRoot = (Get-Item $PSScriptRoot).Parent.Parent.FullName

Write-Host "Repo root: $repoRoot" -ForegroundColor Cyan
Write-Host "Running CI build (Linux) in Docker..." -ForegroundColor Cyan

$cmd = @(
  'set -e',
  'apt-get update -qq',
  'apt-get install -y -qq libayatana-appindicator3-dev',
  'go build -o bin/pgrollback ./cmd/pgrollback',
  "echo 'Build OK: bin/pgrollback'"
) -join ' && '
docker run --rm `
  -v "${repoRoot}:/app" `
  -w /app `
  golang:1.23-bookworm `
  bash -c $cmd

if ($LASTEXITCODE -eq 0) {
  Write-Host "Done. Binary: $repoRoot\bin\pgrollback" -ForegroundColor Green
} else {
  exit $LASTEXITCODE
}
