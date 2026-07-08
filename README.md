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
- A local checkout of `inline-chat/inline` next to this repo when building from
  source, because the bridge builds against the Rust Inline client crates

Expected checkout layout:

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

### Beeper

Generate a bridgev2 config with the bridge type assigned by your Beeper
deployment:

```sh
bbctl c --type bridgev2 <inline-bridge-type> > data/config.yaml
```

Start the bridge:

```sh
docker compose up --build -d
```

The container uses the Beeper-issued appservice tokens from `data/config.yaml`
and writes `data/registration.yaml` on first start.

### Self-hosted Matrix

Start once to generate `data/config.yaml`:

```sh
docker compose up --build
```

Edit `data/config.yaml` and set at least:

- `homeserver.address`
- `homeserver.domain`
- `appservice.address`
- `bridge.permissions`

Start again to generate `data/registration.yaml`:

```sh
docker compose up --build
```

Register `data/registration.yaml` with your homeserver, restart the homeserver,
then run the bridge detached:

```sh
docker compose up -d
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
INLINE_SIDECAR_BIND=127.0.0.1:29342
INLINE_SIDECAR_URL=http://127.0.0.1:29342
INLINE_CLIENT_STORE=/data/inline-client/inline-client.sqlite3
MATRIX_INLINE_DB_URI=file:/data/matrix-inline.db?_txlock=immediate
INLINE_API_BASE_URL=https://api.inline.chat
INLINE_REALTIME_URL=wss://api.inline.chat/realtime
RUST_LOG=info
```

The Inline client store contains session credentials. Keep it private and back
it up with the bridge database.

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
