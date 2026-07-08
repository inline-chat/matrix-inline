# Beta Deployment

This guide describes the current one-team beta deployment shape. It keeps the
Go bridge thin and runs the Rust `matrix-inline-adapter` on the same host,
either in one Docker container or as two systemd services.

## Process Layout

```text
matrix-inline service
  -> http://127.0.0.1:29342
  -> matrix-inline-adapter service
  -> /var/lib/inline-client/inline-client.sqlite3
```

Only the Go bridge should be reachable by the homeserver/appservice listener.
The adapter sidecar must stay bound to loopback.

The adapter resumes any stored Inline session during startup. If the adapter
restarts while the Go bridge is running, the bridge reconnects to the event
stream and checks `/status` before marking the login connected again.

## Build

### Docker

The Docker image builds both binaries:

- Go `matrix-inline`
- Rust `matrix-inline-adapter` from this repo, linked against sibling `../inline-public`

Run Docker commands from the `matrix-inline` repo root. The compose file uses
the parent `inline-chat` directory as build context so both repos are visible;
`Dockerfile.dockerignore` narrows the effective context to `matrix-inline` and
`inline-public` build inputs.

For Beeper/bbctl, generate the bridgev2 config into `data/config.yaml` using
the Beeper-assigned bridge type for Inline, then start compose:

```sh
mkdir -p data
bbctl c --type bridgev2 <inline-bridge-type> > data/config.yaml
docker compose up --build
```

When `config.yaml` already contains Beeper-issued appservice tokens, the
container creates `data/registration.yaml` from those existing values instead
of generating replacement tokens. This matches the Beeper LINE bridge pattern
and avoids breaking the Beeper-issued registration.

For self-hosted Matrix, start once to generate an example config:

```sh
docker compose up --build
```

The container writes `data/config.yaml`, patches Docker-friendly defaults, and
exits. Edit the config, then start again to generate `data/registration.yaml`:

```sh
docker compose up --build
```

Install the registration with your homeserver, then run detached:

```sh
docker compose up -d
```

The Docker entrypoint stores bridge DB state in `/data/matrix-inline.db` and
Inline session/cache state in `/data/inline-client/inline-client.sqlite3`.

Useful Docker environment variables:

```text
DATA_DIR=/data
CONFIG_PATH=/data/config.yaml
REGISTRATION_PATH=/data/registration.yaml
INLINE_SIDECAR_BIND=127.0.0.1:29342
INLINE_SIDECAR_URL=http://127.0.0.1:29342
INLINE_CLIENT_STORE=/data/inline-client/inline-client.sqlite3
MATRIX_INLINE_DB_URI=file:/data/matrix-inline.db?_txlock=immediate
INLINE_API_BASE_URL=https://api.inline.chat
INLINE_REALTIME_URL=wss://api.inline.chat/realtime
RUST_LOG=info
```

### Native/systemd

Build both binaries from local checkouts:

```sh
cd /path/to/matrix-inline
cargo build --release -p matrix-inline-adapter

cd /path/to/matrix-inline
go build -tags goolm -o ./matrix-inline ./cmd/matrix-inline
```

Install them on the target host:

```sh
sudo install -d -m 0755 /opt/inline/bin
sudo install -m 0755 /path/to/matrix-inline/target/release/matrix-inline-adapter /opt/inline/bin/
sudo install -m 0755 /path/to/matrix-inline/matrix-inline /opt/inline/bin/
```

## Service User and Storage

Use a dedicated service user. The sidecar store contains Inline session
credentials, so keep it private.

```sh
sudo useradd --system --home /var/lib/matrix-inline --shell /usr/sbin/nologin inline-bridge
sudo install -d -o inline-bridge -g inline-bridge -m 0700 /var/lib/inline-client /var/lib/matrix-inline
sudo install -d -o inline-bridge -g inline-bridge -m 0750 /etc/matrix-inline
```

## Bridge Config and Registration

Generate the mautrix bridge config:

```sh
sudo -u inline-bridge /opt/inline/bin/matrix-inline \
  --config /etc/matrix-inline/config.yaml \
  --generate-example-config
```

Edit `/etc/matrix-inline/config.yaml`:

- Set homeserver domain and URL.
- Set appservice address/hostname for this host.
- Set the bridge database URI.
- Keep network sidecar URL on loopback:

```yaml
network:
    sidecar_url: http://127.0.0.1:29342
```

Generate the Matrix appservice registration:

```sh
sudo -u inline-bridge /opt/inline/bin/matrix-inline \
  --config /etc/matrix-inline/config.yaml \
  --registration /etc/matrix-inline/registration.yaml \
  --generate-registration
```

Install `/etc/matrix-inline/registration.yaml` in the homeserver and restart the
homeserver according to its appservice-registration process.

## systemd

Install the unit templates in `deploy/systemd`:

```sh
sudo install -m 0644 deploy/systemd/matrix-inline-adapter.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/matrix-inline.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now matrix-inline-adapter.service
sudo systemctl enable --now matrix-inline.service
```

Health checks:

```sh
curl -fsS http://127.0.0.1:29342/health
curl -fsS -X POST http://127.0.0.1:29342/rpc/resume
systemctl status matrix-inline-adapter.service
systemctl status matrix-inline.service
journalctl -u matrix-inline-adapter.service -u matrix-inline.service -f
```

Run the deploy smoke checklist after the first successful login and after every
upgrade:

```sh
scripts/smoke-local.sh
```

Then follow [smoke-test.md](smoke-test.md) for Matrix/Beeper user-visible
checks.

## Login Flow

For the current beta scaffold, login happens through the bridge management flow
using either Inline email or SMS verification. The bridge asks the Rust adapter
to send and verify the code; the adapter persists the resulting Inline session
in its SQLite store. Bridge metadata stores only account/session glue such as
the Inline account ID, sidecar URL, display name, and sidecar store namespace.

Invite-code signup is not part of the one-team beta bridge flow. Users should
create or join Inline first, then log in through the Matrix/Beeper bridge.

## History and Backfill

The bridge supports mautrix bridgev2 historical backfill through
`matrix-inline-adapter` history pagination. The Go bridge stores Matrix message
mappings in the mautrix database, while adapter history cursors and cached Inline
messages come from the Rust client store. Backfilled batches are sent silently
and marked read on Matrix to avoid notification storms when a portal is created
or older history is paged in.

## Management Commands

In the management room, beta users can run:

- `inline-status` or `istatus` to check sidecar, bridge login, account, and
  protocol status.
- `inline-reconnect` or `ireconnect` to reload the bridge login, resume the
  adapter session, and restart sidecar event handling.
- `logout <login ID>` to use mautrix's built-in logout flow, which calls the
  adapter logout command and removes the Matrix login.

## Current Deployment Gaps

- Docker image exists locally, but published image/release automation is still
  pending.
- Native/systemd installs still require installing both binaries explicitly.
- Invite-code signup/onboarding through Beeper is deferred.
- The exact Beeper `bbctl c --type ...` value depends on registering the bridge
  type with Beeper.
- DM lookup/search is not implemented yet; beta DM creation uses numeric Inline
  user IDs.
- Inline reply-thread chats are standalone Matrix rooms for beta.
