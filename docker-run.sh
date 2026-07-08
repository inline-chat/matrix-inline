#!/bin/bash
set -Eeuo pipefail

BINARY_NAME=/usr/bin/matrix-inline
SIDECAR_NAME=/usr/bin/matrix-inline-adapter
DATA_DIR=${DATA_DIR:-/data}
CONFIG_PATH=${CONFIG_PATH:-${DATA_DIR}/config.yaml}
REGISTRATION_PATH=${REGISTRATION_PATH:-${DATA_DIR}/registration.yaml}
INLINE_SIDECAR_BIND=${INLINE_SIDECAR_BIND:-127.0.0.1:29342}
INLINE_SIDECAR_URL=${INLINE_SIDECAR_URL:-http://${INLINE_SIDECAR_BIND}}
INLINE_CLIENT_STORE=${INLINE_CLIENT_STORE:-${DATA_DIR}/inline-client/inline-client.sqlite3}
MATRIX_INLINE_DB_URI=${MATRIX_INLINE_DB_URI:-file:${DATA_DIR}/matrix-inline.db?_txlock=immediate}
MATRIX_INLINE_APPSERVICE_HOSTNAME=${MATRIX_INLINE_APPSERVICE_HOSTNAME:-0.0.0.0}
MATRIX_INLINE_APPSERVICE_ADDRESS=${MATRIX_INLINE_APPSERVICE_ADDRESS:-}
MATRIX_INLINE_HOMESERVER_ADDRESS=${MATRIX_INLINE_HOMESERVER_ADDRESS:-}
MATRIX_INLINE_HOMESERVER_DOMAIN=${MATRIX_INLINE_HOMESERVER_DOMAIN:-}
PUID=${PUID:-1337}
PGID=${PGID:-1337}

export DATA_DIR CONFIG_PATH REGISTRATION_PATH INLINE_SIDECAR_BIND INLINE_SIDECAR_URL INLINE_CLIENT_STORE MATRIX_INLINE_DB_URI
export MATRIX_INLINE_APPSERVICE_HOSTNAME MATRIX_INLINE_APPSERVICE_ADDRESS MATRIX_INLINE_HOMESERVER_ADDRESS MATRIX_INLINE_HOMESERVER_DOMAIN

mkdir -p "${DATA_DIR}" "$(dirname "${INLINE_CLIENT_STORE}")"

function fixperms {
	chown -R "${PUID}:${PGID}" "${DATA_DIR}"
}

function patch_generated_config {
	yq -i '
		.appservice.hostname = strenv(MATRIX_INLINE_APPSERVICE_HOSTNAME) |
		.database.type = "sqlite3-fk-wal" |
		.database.uri = strenv(MATRIX_INLINE_DB_URI) |
		.database.max_open_conns = 1 |
		.network.sidecar_url = strenv(INLINE_SIDECAR_URL)
	' "${CONFIG_PATH}"
	patch_config_value MATRIX_INLINE_APPSERVICE_ADDRESS .appservice.address
	patch_config_value MATRIX_INLINE_HOMESERVER_ADDRESS .homeserver.address
	patch_config_value MATRIX_INLINE_HOMESERVER_DOMAIN .homeserver.domain
}

function patch_config_value {
	local env_name path value
	env_name=$1
	path=$2
	value=${!env_name:-}
	if [[ -n "${value}" ]]; then
		CONFIG_VALUE="${value}" yq -i "${path} = strenv(CONFIG_VALUE)" "${CONFIG_PATH}"
	fi
}

function generate_registration_from_config {
	local as_token hs_token app_id app_url bot_user ephemeral hs_domain username_tpl user_regex
	as_token=$(yq -r '.appservice.as_token // ""' "${CONFIG_PATH}")
	hs_token=$(yq -r '.appservice.hs_token // ""' "${CONFIG_PATH}")
	app_id=$(yq -r '.appservice.id // "inline"' "${CONFIG_PATH}")
	app_url=$(yq -r '.appservice.address // ""' "${CONFIG_PATH}")
	bot_user=$(yq -r '.appservice.bot.username // "inlinebot"' "${CONFIG_PATH}")
	ephemeral=$(yq -r '.appservice.ephemeral_events // "true"' "${CONFIG_PATH}")
	hs_domain=$(yq -r '.homeserver.domain // ""' "${CONFIG_PATH}")
	username_tpl=$(yq -r '.appservice.username_template // "inline_{{.}}"' "${CONFIG_PATH}")

	if [[ -z "${as_token}" || -z "${hs_token}" || -z "${app_url}" || -z "${hs_domain}" ]]; then
		echo "config.yaml is missing appservice tokens, address, or homeserver domain; cannot generate registration.yaml" >&2
		exit 1
	fi

	user_regex=$(echo "${username_tpl}" | sed 's/{{\.}}/.+/g')
	user_regex="@${user_regex}:${hs_domain}"
	user_regex=$(echo "${user_regex}" | sed 's/\./\\./g')

	APP_ID="${app_id}" \
	APP_URL="${app_url}" \
	AS_TOKEN="${as_token}" \
	HS_TOKEN="${hs_token}" \
	BOT_USER="${bot_user}" \
	EPHEMERAL="${ephemeral}" \
	USER_REGEX="${user_regex}" \
	yq -n '
		.id = strenv(APP_ID) |
		.url = strenv(APP_URL) |
		.as_token = strenv(AS_TOKEN) |
		.hs_token = strenv(HS_TOKEN) |
		.sender_localpart = strenv(BOT_USER) |
		.rate_limited = false |
		.namespaces.users[0].regex = strenv(USER_REGEX) |
		.namespaces.users[0].exclusive = true |
		.receive_ephemeral = (strenv(EPHEMERAL) == "true")
	' > "${REGISTRATION_PATH}"
	chmod 600 "${REGISTRATION_PATH}"
}

if [[ ! -f "${CONFIG_PATH}" ]]; then
	"${BINARY_NAME}" -c "${CONFIG_PATH}" -e
	patch_generated_config
	fixperms
	echo "Created ${CONFIG_PATH}."
	echo "Edit it, then start the container again to generate ${REGISTRATION_PATH}."
	exit 0
fi

if [[ ! -f "${REGISTRATION_PATH}" ]]; then
	if grep -q 'as_token.*This value is generated' "${CONFIG_PATH}" 2>/dev/null; then
		"${BINARY_NAME}" -g -c "${CONFIG_PATH}" -r "${REGISTRATION_PATH}"
		echo "Created ${REGISTRATION_PATH} and updated appservice tokens in ${CONFIG_PATH}."
	else
		echo "Config already has appservice tokens. Generating registration.yaml from existing config values..."
		generate_registration_from_config
		echo "Created ${REGISTRATION_PATH} from existing config tokens."
	fi
	fixperms
	exit 0
fi

fixperms

su-exec "${PUID}:${PGID}" "${SIDECAR_NAME}" \
	--bind "${INLINE_SIDECAR_BIND}" \
	--store "${INLINE_CLIENT_STORE}" \
	--api-base-url "${INLINE_API_BASE_URL:-https://api.inline.chat}" \
	--realtime-url "${INLINE_REALTIME_URL:-wss://api.inline.chat/realtime}" \
	${INLINE_SIDECAR_EXTRA_ARGS:-} &
sidecar_pid=$!

function stop_children {
	kill -TERM "${bridge_pid:-}" "${sidecar_pid:-}" 2>/dev/null || true
	wait "${bridge_pid:-}" "${sidecar_pid:-}" 2>/dev/null || true
}

trap stop_children INT TERM

for _ in $(seq 1 60); do
	if curl -fsS "http://${INLINE_SIDECAR_BIND}/health" >/dev/null 2>&1; then
		break
	fi
	if ! kill -0 "${sidecar_pid}" 2>/dev/null; then
		wait "${sidecar_pid}"
	fi
	sleep 1
done

if ! curl -fsS "http://${INLINE_SIDECAR_BIND}/health" >/dev/null 2>&1; then
	echo "matrix-inline-adapter did not become healthy" >&2
	stop_children
	exit 1
fi

su-exec "${PUID}:${PGID}" "${BINARY_NAME}" -c "${CONFIG_PATH}" -r "${REGISTRATION_PATH}" &
bridge_pid=$!

set +e
wait -n "${bridge_pid}" "${sidecar_pid}"
exit_code=$?
set -e
stop_children
exit "${exit_code}"
