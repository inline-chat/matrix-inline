#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INLINE_PUBLIC="${INLINE_PUBLIC:-$(cd "${ROOT}/.." && pwd)/inline-public}"

cd "${ROOT}"

if [[ ! -d "${INLINE_PUBLIC}" ]]; then
	echo "Missing inline-public checkout at ${INLINE_PUBLIC}" >&2
	echo "Set INLINE_PUBLIC=/path/to/inline-public or place it next to matrix-inline." >&2
	exit 1
fi

echo "==> Go format"
gofmt_files="$(gofmt -l cmd pkg scripts/e2econfig scripts/e2efixture)"
if [[ -n "${gofmt_files}" ]]; then
	echo "Go files need gofmt:" >&2
	echo "${gofmt_files}" >&2
	exit 1
fi

echo "==> Go module tidy"
go mod tidy -diff

echo "==> Shell syntax"
bash -n scripts/check.sh scripts/smoke-local.sh scripts/e2e-local.sh docker-run.sh

echo "==> Compose config"
docker compose config >/dev/null
docker compose config | grep -q 'INLINE_API_BASE_URL:'
docker compose config | grep -q 'INLINE_REALTIME_URL:'

echo "==> Go tests"
go test -tags goolm ./...

echo "==> Go vet"
go vet -tags goolm ./...

echo "==> Rust format"
cargo fmt -p matrix-inline-adapter --check

echo "==> Rust adapter tests"
cargo test -p matrix-inline-adapter

echo "==> Rust client dependency tests"
cargo test --manifest-path "${INLINE_PUBLIC}/Cargo.toml" -p inline-client

if [[ "${RUN_DOCKER_BUILD:-0}" == "1" ]]; then
	echo "==> Docker image build"
	docker build -f "${ROOT}/Dockerfile" "$(cd "${ROOT}/.." && pwd)"
fi

echo "All checks passed."
