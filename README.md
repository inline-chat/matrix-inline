# matrix-inline

Matrix bridge for Inline, built with mautrix-go bridgev2 for Beeper and
self-hosted Matrix deployments.

The bridge lets Matrix users log in with an existing Inline account, sync Inline
chats into Matrix rooms, and send messages between Matrix/Beeper and Inline.

## Features

- [x] Login with Inline email or SMS verification code
- [x] Session resume after bridge restart
- [x] Startup chat sync
- [x] Recent history sync and bridgev2 backfill
- [x] Text messages
- [x] Normal message replies
- [x] Message edits
- [x] Message deletes/redactions
- [x] Reactions
- [x] Read receipts from Matrix to Inline
- [x] Typing notifications
- [x] Images, videos, files, audio, and voice/audio files
- [x] Inline member list sync for bridged rooms
- [x] Matrix ghosts with Inline display names and avatars when available
- [x] Configurable bridge bot and network profile metadata
- [x] DM creation with a numeric Inline user ID
- [x] Basic group/thread creation from Matrix
- [x] Management commands for status and reconnect
- [ ] New Inline account signup or invite-code onboarding
- [ ] Matrix-native thread UI for Inline reply-thread chats
- [ ] Contact search for DM creation
- [ ] Room avatars
- [ ] Calls

## Requirements

- An existing Inline account
- A Matrix homeserver or Beeper bridge deployment
- Docker and Docker Compose, or Go 1.25 plus Rust 1.96 for native builds
- A published container image from `ghcr.io/inline-chat/matrix-inline`

Source builds require a local checkout of `inline-chat/inline` next to this repo
because the bridge builds against the Rust Inline client crates:

```text
inline-chat/
  inline/
  matrix-inline/
```

## Docker Setup

Create a data directory:

```sh
mkdir -p data
```

Pull the published image:

```sh
docker compose pull
```

Published images are built for `linux/amd64` and `linux/arm64`.

### Beeper

Generate a bridgev2 config with the bridge type assigned by your Beeper
deployment:

```sh
bbctl c --type bridgev2 <inline-bridge-type> > data/config.yaml
```

Start the bridge:

```sh
docker compose up -d
```

The container uses the Beeper-issued appservice tokens from `data/config.yaml`
and writes `data/registration.yaml` on first start.

### Self-hosted Matrix

Start once to generate `data/config.yaml`:

```sh
docker compose up
```

Edit `data/config.yaml` and set at least:

- `homeserver.address`
- `homeserver.domain`
- `appservice.address`
- `bridge.permissions`

Start again to generate `data/registration.yaml`:

```sh
docker compose up
```

Register `data/registration.yaml` with your homeserver, restart the homeserver,
then run the bridge detached:

```sh
docker compose up -d
```

When the homeserver and bridge are on the same Docker network,
`appservice.address` usually looks like:

```text
http://matrix-inline:29343
```

The URL must be reachable by the homeserver container or host.

### Build from source

Use the build override when developing or testing unreleased code:

```sh
docker compose -f docker-compose.yml -f docker-compose.build.yml up --build
```

## Native Build

Build the Rust adapter:

```sh
cargo build --release -p matrix-inline-adapter
```

Build the Go bridge:

```sh
go build -tags goolm -o ./matrix-inline ./cmd/matrix-inline
```

For systemd installation, see [deploy/systemd](deploy/systemd).

## Login

Use Beeper Desktop settings or the Matrix bridge bot to add an Inline account.
The login flow asks for your Inline email address or phone number, then asks for
the verification code sent by Inline.

Bridge bot commands:

```text
login
inline-status
inline-reconnect
logout <login ID>
```

Aliases:

```text
istatus
ireconnect
```

## Configuration

The Docker entrypoint stores bridge state in `/data`:

```text
/data/config.yaml
/data/registration.yaml
/data/matrix-inline.db
/data/inline-client/inline-client.sqlite3
```

Useful environment variables:

```text
MATRIX_INLINE_IMAGE=ghcr.io/inline-chat/matrix-inline:latest
INLINE_SIDECAR_BIND=127.0.0.1:29342
INLINE_SIDECAR_URL=http://127.0.0.1:29342
INLINE_CLIENT_STORE=/data/inline-client/inline-client.sqlite3
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

The Inline client store contains session credentials. Keep it private and back
it up with the bridge database.

### Bridge Profile

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

## Development

Run local checks:

```sh
scripts/check.sh
```

Run a local adapter smoke test:

```sh
scripts/smoke-local.sh --start-adapter
```

Local Go commands use `-tags goolm` so development builds do not require system
libolm headers.

## Documentation

- [Deployment](docs/deployment.md)
- [Operations](docs/operations.md)
- [Smoke test](docs/smoke-test.md)
- [systemd units](deploy/systemd)

## License

Apache-2.0. See [LICENSE](LICENSE).
