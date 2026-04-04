#!/usr/bin/env bash
# Run the CI "build" job locally using Docker (Linux build).
# Use from WSL or Linux. From repo root:
#   .github/scripts/ci-build-local.sh

set -e
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
echo "Repo root: $REPO_ROOT"
echo "Running CI build (Linux) in Docker..."

docker run --rm \
  -v "${REPO_ROOT}:/app" \
  -w /app \
  golang:1.23-bookworm \
  bash -c '
    set -e
    apt-get update -qq
    apt-get install -y -qq libayatana-appindicator3-dev
    go build -o bin/pgrollback ./cmd/pgrollback
    echo "Build OK: bin/pgrollback"
  '

echo "Done. Binary: $REPO_ROOT/bin/pgrollback"
