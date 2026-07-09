#!/usr/bin/env bash
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
E2E_ROOT="${MATRIX_INLINE_E2E_ROOT:-${ROOT}/data/e2e}"
PROJECT="${MATRIX_INLINE_E2E_PROJECT:-matrix-inline-e2e}"

SYNAPSE_IMAGE="${MATRIX_INLINE_E2E_SYNAPSE_IMAGE:-matrixdotorg/synapse:latest}"
SERVER_NAME="${MATRIX_INLINE_E2E_SERVER_NAME:-localhost}"
SYNAPSE_PORT="${MATRIX_INLINE_E2E_SYNAPSE_PORT:-18008}"
BRIDGE_PORT="${MATRIX_INLINE_E2E_BRIDGE_PORT:-29343}"
APPSERVICE_HOSTNAME="${MATRIX_INLINE_E2E_APPSERVICE_HOSTNAME:-0.0.0.0}"
APPSERVICE_ADDRESS="${MATRIX_INLINE_E2E_APPSERVICE_ADDRESS:-http://host.docker.internal:${BRIDGE_PORT}}"
HOMESERVER_ADDRESS="${MATRIX_INLINE_E2E_HOMESERVER_ADDRESS:-http://127.0.0.1:${SYNAPSE_PORT}}"

SIDECAR_BIND="${INLINE_SIDECAR_BIND:-127.0.0.1:29342}"
SIDECAR_URL="${INLINE_SIDECAR_URL:-http://${SIDECAR_BIND}}"
INLINE_API_BASE_URL="${INLINE_API_BASE_URL:-https://api.inline.chat/v1}"
INLINE_REALTIME_URL="${INLINE_REALTIME_URL:-wss://api.inline.chat/realtime}"

TEST_USER="${MATRIX_INLINE_E2E_USER:-alice}"
TEST_PASSWORD="${MATRIX_INLINE_E2E_PASSWORD:-matrix-inline-e2e-password}"
TEST_DEVICE_ID="${MATRIX_INLINE_E2E_DEVICE_ID:-matrix-inline-e2e}"

BIN_DIR="${E2E_ROOT}/bin"
RUN_DIR="${E2E_ROOT}/run"
LOG_DIR="${E2E_ROOT}/logs"
BRIDGE_DATA="${E2E_ROOT}/bridge"
SYNAPSE_DATA="${E2E_ROOT}/synapse"

BRIDGE_BIN="${BIN_DIR}/matrix-inline"
ADAPTER_BIN="${ROOT}/target/debug/matrix-inline-adapter"
BRIDGE_CONFIG="${BRIDGE_DATA}/config.yaml"
BRIDGE_REGISTRATION="${BRIDGE_DATA}/registration.yaml"
SYNAPSE_CONFIG="${SYNAPSE_DATA}/homeserver.yaml"
SYNAPSE_REGISTRATION="${SYNAPSE_DATA}/matrix-inline-registration.yaml"
COMPOSE_FILE="${E2E_ROOT}/docker-compose.yml"
REGISTRATION_SECRET_FILE="${SYNAPSE_DATA}/registration_shared_secret"

function usage {
	cat <<EOF
Usage: scripts/e2e-local.sh <prepare|start|smoke|status|logs|stop|restart>

Commands:
  prepare  Build host binaries and generate local Synapse/bridge configs.
  start    Start Synapse in Docker plus host bridge and adapter binaries.
  smoke    Start if needed, then verify adapter health and Matrix appservice bot round-trip.
  status   Print local service status.
  logs     Tail bridge, adapter, and Synapse logs.
  stop     Stop host bridge/adapter and stop the Synapse container.
  restart  Stop then start the local E2E environment.

Data is stored under:
  ${E2E_ROOT}
EOF
}

function require_cmd {
	if ! command -v "$1" >/dev/null 2>&1; then
		echo "Missing required command: $1" >&2
		exit 1
	fi
}

function require_base_commands {
	require_cmd go
	require_cmd cargo
	require_cmd curl
	require_cmd jq
}

function require_docker_commands {
	require_cmd docker
}

function compose {
	docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" "$@"
}

function write_compose {
	mkdir -p "${E2E_ROOT}"
	cat >"${COMPOSE_FILE}" <<EOF
services:
  synapse:
    image: ${SYNAPSE_IMAGE}
    environment:
      SYNAPSE_CONFIG_PATH: /data/homeserver.yaml
    ports:
      - "127.0.0.1:${SYNAPSE_PORT}:8008"
    volumes:
      - "${SYNAPSE_DATA}:/data"
    extra_hosts:
      - "host.docker.internal:host-gateway"
EOF
}

function random_hex {
	if command -v openssl >/dev/null 2>&1; then
		openssl rand -hex 32
	else
		uuidgen | tr '[:upper:]' '[:lower:]' | tr -d '-'
	fi
}

function registration_secret {
	mkdir -p "$(dirname "${REGISTRATION_SECRET_FILE}")"
	if [[ ! -f "${REGISTRATION_SECRET_FILE}" ]]; then
		random_hex >"${REGISTRATION_SECRET_FILE}"
		chmod 600 "${REGISTRATION_SECRET_FILE}"
	fi
	cat "${REGISTRATION_SECRET_FILE}"
}

function build_binaries {
	mkdir -p "${BIN_DIR}"
	echo "==> Building Go bridge"
	(cd "${ROOT}" && go build -tags goolm -o "${BRIDGE_BIN}" ./cmd/matrix-inline)
	echo "==> Building Rust adapter"
	(cd "${ROOT}" && cargo build -p matrix-inline-adapter)
}

function generate_bridge_config {
	mkdir -p "${BRIDGE_DATA}"
	if [[ ! -f "${BRIDGE_CONFIG}" ]]; then
		"${BRIDGE_BIN}" -c "${BRIDGE_CONFIG}" -e
	fi

	(cd "${ROOT}" && go run ./scripts/e2econfig patch-bridge \
		--config "${BRIDGE_CONFIG}" \
		--homeserver-address "${HOMESERVER_ADDRESS}" \
		--homeserver-domain "${SERVER_NAME}" \
		--appservice-address "${APPSERVICE_ADDRESS}" \
		--appservice-hostname "${APPSERVICE_HOSTNAME}" \
		--appservice-port "${BRIDGE_PORT}" \
		--sidecar-url "${SIDECAR_URL}" \
		--database-uri "file:${BRIDGE_DATA}/matrix-inline.db?_txlock=immediate" \
		--admin-localpart "${TEST_USER}")

	if [[ ! -f "${BRIDGE_REGISTRATION}" ]]; then
		"${BRIDGE_BIN}" -g -c "${BRIDGE_CONFIG}" -r "${BRIDGE_REGISTRATION}"
	fi
	cp "${BRIDGE_REGISTRATION}" "${SYNAPSE_REGISTRATION}"
}

function generate_synapse_config {
	mkdir -p "${SYNAPSE_DATA}"
	if [[ ! -f "${SYNAPSE_CONFIG}" ]]; then
		echo "==> Generating Synapse config with ${SYNAPSE_IMAGE}"
		docker run --rm \
			-v "${SYNAPSE_DATA}:/data" \
			-e "SYNAPSE_SERVER_NAME=${SERVER_NAME}" \
			-e "SYNAPSE_REPORT_STATS=no" \
			"${SYNAPSE_IMAGE}" generate
	fi

	(cd "${ROOT}" && go run ./scripts/e2econfig patch-synapse \
		--config "${SYNAPSE_CONFIG}" \
		--registration-path "/data/$(basename "${SYNAPSE_REGISTRATION}")" \
		--public-base-url "${HOMESERVER_ADDRESS}/" \
		--registration-shared-secret "$(registration_secret)")
}

function prepare {
	require_base_commands
	require_docker_commands
	mkdir -p "${RUN_DIR}" "${LOG_DIR}" "${BRIDGE_DATA}" "${SYNAPSE_DATA}"
	write_compose
	build_binaries
	generate_bridge_config
	generate_synapse_config
	echo "Prepared local E2E environment under ${E2E_ROOT}"
}

function wait_for_http {
	local name=$1 url=$2
	echo "==> Waiting for ${name}: ${url}"
	for _ in $(seq 1 90); do
		if curl -fsS "${url}" >/dev/null 2>&1; then
			return 0
		fi
		sleep 1
	done
	echo "Timed out waiting for ${name}: ${url}" >&2
	return 1
}

function wait_for_tcp {
	local name=$1 host=$2 port=$3
	echo "==> Waiting for ${name}: ${host}:${port}"
	for _ in $(seq 1 60); do
		if bash -c ":</dev/tcp/${host}/${port}" >/dev/null 2>&1; then
			return 0
		fi
		sleep 1
	done
	echo "Timed out waiting for ${name}: ${host}:${port}" >&2
	return 1
}

function ensure_test_user {
	local output
	echo "==> Ensuring Matrix test user @${TEST_USER}:${SERVER_NAME}"
	set +e
	output=$(compose exec -T synapse register_new_matrix_user \
		-u "${TEST_USER}" \
		-p "${TEST_PASSWORD}" \
		-a \
		-c /data/homeserver.yaml \
		http://localhost:8008 2>&1)
	local code=$?
	set -e
	if [[ "${code}" == "0" ]]; then
		return 0
	fi
	if grep -qi "already" <<<"${output}"; then
		return 0
	fi
	echo "${output}" >&2
	return "${code}"
}

function read_pid {
	local file=$1
	if [[ -f "${file}" ]]; then
		cat "${file}"
	fi
}

function process_running {
	local pid=$1
	[[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

function start_background {
	local log_file=$1
	shift

	if command -v setsid >/dev/null 2>&1; then
		nohup setsid "$@" >"${log_file}" 2>&1 </dev/null &
	elif command -v perl >/dev/null 2>&1; then
		nohup perl -MPOSIX=setsid -e 'setsid() or die "setsid: $!"; exec @ARGV; die "exec: $!"' "$@" >"${log_file}" 2>&1 </dev/null &
	else
		nohup "$@" >"${log_file}" 2>&1 </dev/null &
	fi
	echo $!
}

function start_adapter {
	local pid_file="${RUN_DIR}/adapter.pid"
	local pid
	pid=$(read_pid "${pid_file}")
	if process_running "${pid}"; then
		echo "==> Adapter already running (${pid})"
		return 0
	fi

	echo "==> Starting Rust adapter"
	mkdir -p "${BRIDGE_DATA}/inline-client" "${LOG_DIR}" "${RUN_DIR}"
	start_background "${LOG_DIR}/adapter.log" \
		env RUST_LOG="${RUST_LOG:-info}" \
		"${ADAPTER_BIN}" \
		--bind "${SIDECAR_BIND}" \
		--store "${BRIDGE_DATA}/inline-client/inline-client.sqlite3" \
		--api-base-url "${INLINE_API_BASE_URL}" \
		--realtime-url "${INLINE_REALTIME_URL}" >"${pid_file}"
	wait_for_http "adapter health" "${SIDECAR_URL}/health"
}

function start_bridge {
	local pid_file="${RUN_DIR}/bridge.pid"
	local pid
	pid=$(read_pid "${pid_file}")
	if process_running "${pid}"; then
		echo "==> Bridge already running (${pid})"
		return 0
	fi

	echo "==> Starting Go bridge"
	start_background "${LOG_DIR}/bridge.log" \
		env BRIDGEV2=1 \
		"${BRIDGE_BIN}" -c "${BRIDGE_CONFIG}" -r "${BRIDGE_REGISTRATION}" >"${pid_file}"
	wait_for_tcp "bridge appservice" "127.0.0.1" "${BRIDGE_PORT}"
}

function start {
	prepare
	echo "==> Starting Synapse"
	compose up -d synapse
	wait_for_http "Synapse" "${HOMESERVER_ADDRESS}/_matrix/client/versions"
	ensure_test_user
	start_adapter
	start_bridge
	echo "Local E2E environment is running."
}

function stop_pid {
	local name=$1 pid_file=$2
	local pid
	pid=$(read_pid "${pid_file}")
	if process_running "${pid}"; then
		echo "==> Stopping ${name} (${pid})"
		kill -TERM "${pid}" 2>/dev/null || true
		for _ in $(seq 1 20); do
			if ! process_running "${pid}"; then
				break
			fi
			sleep 0.5
		done
	fi
}

function stop {
	stop_pid "bridge" "${RUN_DIR}/bridge.pid"
	stop_pid "adapter" "${RUN_DIR}/adapter.pid"
	if [[ -f "${COMPOSE_FILE}" ]]; then
		echo "==> Stopping Synapse"
		compose stop synapse >/dev/null || true
	fi
}

function restart {
	stop
	start
}

function matrix_login_token {
	local body response
	body=$(jq -nc \
		--arg user "${TEST_USER}" \
		--arg password "${TEST_PASSWORD}" \
		--arg device "${TEST_DEVICE_ID}" \
		'{type:"m.login.password", identifier:{type:"m.id.user", user:$user}, password:$password, device_id:$device}')
	response=$(curl -fsS \
		-H "Content-Type: application/json" \
		-d "${body}" \
		"${HOMESERVER_ADDRESS}/_matrix/client/v3/login")
	jq -r '.access_token' <<<"${response}"
}

function matrix_auth_json {
	local token=$1 method=$2 path=$3 body=${4:-}
	if [[ -n "${body}" ]]; then
		curl -fsS \
			-X "${method}" \
			-H "Authorization: Bearer ${token}" \
			-H "Content-Type: application/json" \
			-d "${body}" \
			"${HOMESERVER_ADDRESS}${path}"
	else
		curl -fsS \
			-X "${method}" \
			-H "Authorization: Bearer ${token}" \
			"${HOMESERVER_ADDRESS}${path}"
	fi
}

function uri_encode {
	jq -rn --arg value "$1" '$value|@uri'
}

function registration_value {
	(cd "${ROOT}" && go run ./scripts/e2econfig get --config "${BRIDGE_REGISTRATION}" --path "$1")
}

function bridge_config_value {
	(cd "${ROOT}" && go run ./scripts/e2econfig get --config "${BRIDGE_CONFIG}" --path "$1")
}

function wait_for_bot_message {
	local token=$1 room_id=$2 bot_mxid=$3 pattern=$4
	local encoded_room response
	encoded_room=$(uri_encode "${room_id}")
	for _ in $(seq 1 45); do
		response=$(matrix_auth_json "${token}" GET "/_matrix/client/v3/rooms/${encoded_room}/messages?dir=b&limit=30")
		if jq -e --arg sender "${bot_mxid}" --arg pattern "${pattern}" '
			.chunk[]?
			| select(.sender == $sender)
			| (.content.body? // "")
			| select(test($pattern; "i"))
		' >/dev/null <<<"${response}"; then
			return 0
		fi
		sleep 1
	done
	echo "Timed out waiting for bot message matching ${pattern}" >&2
	return 1
}

function smoke {
	start

	echo "==> Adapter status"
	curl -fsS "${SIDECAR_URL}/status" | jq .
	echo "==> Adapter resume"
	curl -fsS -X POST "${SIDECAR_URL}/rpc/resume" | jq .

	local token bot_localpart bot_mxid room_body room_id txn send_body encoded_room
	token=$(matrix_login_token)
	bot_localpart=$(bridge_config_value appservice.bot.username)
	bot_mxid="@${bot_localpart}:${SERVER_NAME}"

	echo "==> Creating management room with ${bot_mxid}"
	room_body=$(jq -nc \
		--arg bot "${bot_mxid}" \
		'{preset:"private_chat", is_direct:true, invite:[$bot], name:"matrix-inline e2e management"}')
	room_id=$(matrix_auth_json "${token}" POST "/_matrix/client/v3/createRoom" "${room_body}" | jq -r '.room_id')
	wait_for_bot_message "${token}" "${room_id}" "${bot_mxid}" "Inline bridge bot|management room|Use .*help"

	echo "==> Sending command through Matrix appservice transaction"
	txn="e2e-$(date +%s)"
	send_body=$(jq -nc '{msgtype:"m.text", body:"!inline list-logins"}')
	encoded_room=$(uri_encode "${room_id}")
	matrix_auth_json "${token}" PUT "/_matrix/client/v3/rooms/${encoded_room}/send/m.room.message/${txn}" "${send_body}" >/dev/null
	wait_for_bot_message "${token}" "${room_id}" "${bot_mxid}" "not logged in"

	echo "Local Matrix/appservice smoke check passed."
}

function status {
	echo "E2E root: ${E2E_ROOT}"
	if [[ -f "${COMPOSE_FILE}" ]]; then
		compose ps
	fi
	if curl -fsS "${HOMESERVER_ADDRESS}/_matrix/client/versions" >/dev/null 2>&1; then
		echo "Synapse: reachable at ${HOMESERVER_ADDRESS}"
	else
		echo "Synapse: not reachable at ${HOMESERVER_ADDRESS}"
	fi
	if curl -fsS "${SIDECAR_URL}/health" >/dev/null 2>&1; then
		echo "Adapter: reachable at ${SIDECAR_URL}"
	else
		echo "Adapter: not reachable at ${SIDECAR_URL}"
	fi
	local bridge_pid adapter_pid
	bridge_pid=$(read_pid "${RUN_DIR}/bridge.pid")
	adapter_pid=$(read_pid "${RUN_DIR}/adapter.pid")
	if process_running "${bridge_pid}"; then
		echo "Bridge PID: ${bridge_pid}"
	else
		echo "Bridge PID: ${bridge_pid:-none} (not running)"
	fi
	if process_running "${adapter_pid}"; then
		echo "Adapter PID: ${adapter_pid}"
	else
		echo "Adapter PID: ${adapter_pid:-none} (not running)"
	fi
}

function logs {
	if [[ -f "${LOG_DIR}/bridge.log" ]]; then
		echo "==> Bridge log"
		tail -80 "${LOG_DIR}/bridge.log"
	fi
	if [[ -f "${LOG_DIR}/adapter.log" ]]; then
		echo "==> Adapter log"
		tail -80 "${LOG_DIR}/adapter.log"
	fi
	if [[ -f "${COMPOSE_FILE}" ]]; then
		echo "==> Synapse log"
		compose logs --tail=80 synapse
	fi
}

cmd="${1:-}"
case "${cmd}" in
prepare) prepare ;;
start) start ;;
smoke) smoke ;;
status) status ;;
logs) logs ;;
stop) stop ;;
restart) restart ;;
"" | -h | --help | help) usage ;;
*)
	echo "Unknown command: ${cmd}" >&2
	usage >&2
	exit 1
	;;
esac
