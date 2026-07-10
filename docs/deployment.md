# Deployment

This guide covers Docker Compose and native systemd deployments for
`matrix-inline`.

## Architecture

`matrix-inline` runs two local processes:

- `matrix-inline`: the mautrix-go bridge process that talks to Matrix/Beeper
- `matrix-inline-adapter`: the local Inline adapter used by the bridge

The adapter should listen on loopback only. The Matrix homeserver only needs to
reach the bridge appservice listener.

Default adapter URL:

```text
http://127.0.0.1:29342
```

## Docker Compose

The default Compose setup uses the published image:

```sh
docker compose pull
```

Published images are built for `linux/amd64` and `linux/arm64`.

Source builds are available with the build override. Build commands must run
from the `matrix-inline` repo root with the sibling `inline` repo available:

```text
inline-chat/
  inline/
  matrix-inline/
```

Create the data directory:

```sh
mkdir -p data
```

### Beeper

Generate a Beeper bridgev2 config:

```sh
bbctl c --type bridgev2 <inline-bridge-type> > data/config.yaml
```

Start the bridge:

```sh
docker compose up -d
```

On first start, the container writes `data/registration.yaml` from the
appservice tokens already present in the Beeper config.

### Self-hosted Matrix

Start once to generate an example config:

```sh
docker compose up
```

Edit `data/config.yaml` and set:

- `homeserver.address`
- `homeserver.domain`
- `appservice.address`
- `appservice.hostname`
- `bridge.permissions`

The appservice address must be reachable by the homeserver. If the bridge and
homeserver share a Docker network, use the bridge service name:

```yaml
appservice:
  address: http://matrix-inline:29343
```

For first-time config generation, these environment variables can prefill the
common homeserver and appservice values:

```text
MATRIX_INLINE_HOMESERVER_ADDRESS=http://synapse:8008
MATRIX_INLINE_HOMESERVER_DOMAIN=example.com
MATRIX_INLINE_APPSERVICE_ADDRESS=http://matrix-inline:29343
```

Start again to generate `data/registration.yaml`:

```sh
docker compose up
```

Add `data/registration.yaml` to your homeserver appservice registrations and
restart the homeserver. For Synapse, add the file path to
`app_service_config_files` in `homeserver.yaml`.

Run detached:

```sh
docker compose up -d
```

Build from source:

```sh
docker compose -f docker-compose.yml -f docker-compose.build.yml up --build
```

## Docker Storage

The default Compose file mounts `./data` at `/data`.

Important files:

```text
/data/config.yaml
/data/registration.yaml
/data/matrix-inline.db
/data/matrix-inline.db-shm
/data/matrix-inline.db-wal
/data/inline-client/inline-client.sqlite3
/data/inline-client/inline-client.sqlite3-shm
/data/inline-client/inline-client.sqlite3-wal
/data/inline-client/accounts/<session-namespace>.sqlite3
/data/inline-client/accounts/<session-namespace>.sqlite3-shm
/data/inline-client/accounts/<session-namespace>.sqlite3-wal
```

The base Inline client SQLite file contains the durable adapter event log and
may retain a legacy session during migration. Per-account files under
`accounts/` contain active session credentials. The adapter creates account
storage with private permissions; keep the full data directory private.

## Docker Environment

Common environment variables:

```text
MATRIX_INLINE_IMAGE=ghcr.io/inline-chat/matrix-inline:latest
DATA_DIR=/data
CONFIG_PATH=/data/config.yaml
REGISTRATION_PATH=/data/registration.yaml
INLINE_SIDECAR_BIND=127.0.0.1:29342
INLINE_SIDECAR_URL=http://127.0.0.1:29342
INLINE_CLIENT_STORE=/data/inline-client/inline-client.sqlite3
INLINE_CLIENT_ACCOUNT_STORE_DIR=/data/inline-client/accounts
MATRIX_INLINE_DB_URI=file:/data/matrix-inline.db?_txlock=immediate
MATRIX_INLINE_HOMESERVER_ADDRESS=http://synapse:8008
MATRIX_INLINE_HOMESERVER_DOMAIN=example.com
MATRIX_INLINE_APPSERVICE_ADDRESS=http://matrix-inline:29343
MATRIX_INLINE_APPSERVICE_HOSTNAME=0.0.0.0
MATRIX_INLINE_NETWORK_DISPLAYNAME=Inline
MATRIX_INLINE_NETWORK_URL=https://inline.chat
MATRIX_INLINE_NETWORK_ICON=mxc://matrix.org/ITxccqHQkLCnPQDouWfsPhqs
MATRIX_INLINE_BOT_DISPLAYNAME=Inline bridge bot
MATRIX_INLINE_BOT_AVATAR=
INLINE_API_BASE_URL=https://api.inline.chat/v1
INLINE_REALTIME_URL=wss://api.inline.chat/realtime
RUST_LOG=info
```

## Bridge Profile

matrix-inline ships with the official Inline display name, URL, and bridge icon.
Docker deployments also apply the same icon to the appservice bot profile by
default.

```yaml
appservice:
  bot:
    displayname: Inline bridge bot
    avatar: mxc://matrix.org/ITxccqHQkLCnPQDouWfsPhqs
network:
  displayname: Inline
  network_url: https://inline.chat
  network_icon: mxc://matrix.org/ITxccqHQkLCnPQDouWfsPhqs
```

For custom branding, set `MATRIX_INLINE_NETWORK_ICON`. Use
`MATRIX_INLINE_BOT_AVATAR` only when the bot avatar should be different from the
bridge icon.

## Native Build

Install system dependencies:

- Go 1.25
- Rust 1.96
- C compiler and build tools
- protobuf compiler
- SQLite development headers

Build the adapter:

```sh
cargo build --release -p matrix-inline-adapter
```

Build the bridge:

```sh
go build -tags goolm -o ./matrix-inline ./cmd/matrix-inline
```

Install binaries:

```sh
sudo install -d -m 0755 /opt/inline/bin
sudo install -m 0755 target/release/matrix-inline-adapter /opt/inline/bin/
sudo install -m 0755 matrix-inline /opt/inline/bin/
```

## Native Configuration

Create a service user and private storage:

```sh
sudo useradd --system --home /var/lib/matrix-inline --shell /usr/sbin/nologin inline-bridge
sudo install -d -o inline-bridge -g inline-bridge -m 0700 /var/lib/inline-client /var/lib/matrix-inline
sudo install -d -o inline-bridge -g inline-bridge -m 0750 /etc/matrix-inline
```

Generate a config:

```sh
sudo -u inline-bridge /opt/inline/bin/matrix-inline \
  --config /etc/matrix-inline/config.yaml \
  --generate-example-config
```

Edit `/etc/matrix-inline/config.yaml` and set Matrix homeserver, appservice,
database, permissions, and the adapter URL:

```yaml
network:
  displayname: Inline
  network_url: https://inline.chat
  network_icon: mxc://matrix.org/ITxccqHQkLCnPQDouWfsPhqs
  sidecar_url: http://127.0.0.1:29342
appservice:
  bot:
    displayname: Inline bridge bot
    avatar: mxc://matrix.org/ITxccqHQkLCnPQDouWfsPhqs
```

Generate registration:

```sh
sudo -u inline-bridge /opt/inline/bin/matrix-inline \
  --config /etc/matrix-inline/config.yaml \
  --registration /etc/matrix-inline/registration.yaml \
  --generate-registration
```

Register the generated file with your homeserver and restart the homeserver.

## systemd

Install the units:

```sh
sudo install -m 0644 deploy/systemd/matrix-inline-adapter.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/matrix-inline.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now matrix-inline-adapter.service
sudo systemctl enable --now matrix-inline.service
```

Check status:

```sh
systemctl status matrix-inline-adapter.service
systemctl status matrix-inline.service
journalctl -u matrix-inline-adapter.service -u matrix-inline.service -f
curl -fsS http://127.0.0.1:29342/health
```

## After Deployment

1. Start a Matrix chat with the bridge bot.
2. Run `login`.
3. Enter your Inline email or phone number.
4. Enter the verification code sent by Inline.
5. Run `inline-status`.
6. Run the checklist in [smoke-test.md](smoke-test.md).
