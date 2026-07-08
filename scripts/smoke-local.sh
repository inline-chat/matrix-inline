#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIND="${INLINE_SIDECAR_BIND:-127.0.0.1:29342}"
URL="${INLINE_SIDECAR_URL:-http://${BIND}}"
STORE="${INLINE_CLIENT_STORE:-${ROOT}/data/inline-client/inline-client.sqlite3}"
START_ADAPTER=0
ADAPTER_PID=""

if [[ "${1:-}" == "--start-adapter" ]]; then
	START_ADAPTER=1
fi

function cleanup {
	if [[ -n "${ADAPTER_PID}" ]]; then
		kill -TERM "${ADAPTER_PID}" 2>/dev/null || true
		wait "${ADAPTER_PID}" 2>/dev/null || true
	fi
}
trap cleanup EXIT INT TERM

if [[ "${START_ADAPTER}" == "1" ]]; then
	mkdir -p "$(dirname "${STORE}")"
	cd "${ROOT}"
	cargo run -p matrix-inline-adapter -- \
		--bind "${BIND}" \
		--store "${STORE}" \
		--api-base-url "${INLINE_API_BASE_URL:-https://api.inline.chat}" \
		--realtime-url "${INLINE_REALTIME_URL:-wss://api.inline.chat/realtime}" &
	ADAPTER_PID=$!
fi

echo "==> Waiting for adapter health at ${URL}/health"
for _ in $(seq 1 60); do
	if curl -fsS "${URL}/health" >/dev/null 2>&1; then
		break
	fi
	sleep 1
done

curl -fsS "${URL}/health" | jq .

echo "==> Adapter status"
curl -fsS "${URL}/status" | jq .

echo "==> Adapter resume"
curl -fsS -X POST "${URL}/rpc/resume" | jq .

echo "Smoke check passed."
