# Local E2E Harness

This harness runs a minimal local Matrix environment for bridge development:

- Synapse runs in Docker.
- `matrix-inline` runs as a host Go binary.
- `matrix-inline-adapter` runs as a host Rust binary.
- The bridge is registered as a real Matrix appservice.

It is intended for validating Matrix/appservice setup, bridge startup, adapter
health, bot management-room behavior, and the base path used by later Inline
account tests.

## Requirements

- Docker and Docker Compose
- Go 1.25
- Rust 1.96
- `curl`
- `jq`

## Run

From the repo root:

```sh
scripts/e2e-local.sh smoke
```

The smoke command prepares the environment, starts Synapse, starts the Rust
adapter and Go bridge, creates a local Matrix test user, invites the bridge bot
to a management room, and verifies that the bot replies to a command.

Data and logs are written to `data/e2e/`, which is ignored by git.

Useful commands:

```sh
scripts/e2e-local.sh prepare
scripts/e2e-local.sh start
scripts/e2e-local.sh status
scripts/e2e-local.sh logs
scripts/e2e-local.sh stop
```

## Fixture Flow

For a deterministic bridge-level check that does not require an Inline account:

```sh
scripts/e2e-local.sh fixture-check
scripts/e2e-local.sh fixture-restart-check
scripts/e2e-local.sh fixture-stop
```

This uses a local fixture sidecar instead of the Rust adapter. Synapse and the
Go bridge still run normally. The check logs in through the bridge provisioning
API, waits for DM and group portals to become visible to the Matrix test user,
verifies backfilled messages, sends a Matrix message through a portal, and pushes
a fixture realtime inbound message into the bridge. The restart check also stops
and restarts bridge-side processes to verify missed-message catch-up and realtime
delivery after an existing login reconnects.

Fixture data is stored under `data/e2e-fixture/` by default.

## Configuration

The defaults are local-only except for the appservice bind address, which must
be reachable from the Synapse container:

```text
MATRIX_INLINE_E2E_ROOT=data/e2e
MATRIX_INLINE_E2E_SERVER_NAME=localhost
MATRIX_INLINE_E2E_SYNAPSE_PORT=18008
MATRIX_INLINE_E2E_BRIDGE_PORT=29343
MATRIX_INLINE_E2E_APPSERVICE_HOSTNAME=0.0.0.0
MATRIX_INLINE_E2E_APPSERVICE_ADDRESS=http://host.docker.internal:29343
INLINE_SIDECAR_BIND=127.0.0.1:29342
INLINE_API_BASE_URL=https://api.inline.chat/v1
INLINE_REALTIME_URL=wss://api.inline.chat/realtime
```

Use `MATRIX_INLINE_E2E_SYNAPSE_IMAGE` to pin or override the Synapse image used
for local testing.

## What This Proves

The default smoke test verifies:

- Synapse can load the generated appservice registration.
- Synapse can reach the host bridge appservice.
- The bridge can reach Synapse.
- The host bridge can reach the loopback Rust adapter.
- A Matrix user can create a management room with the appservice bot.
- Matrix events are delivered to the bridge and bot replies are delivered back
  through Synapse.

## Inline Account Tests

After logging into Inline through the management room, use the checklist in
[Smoke Test](smoke-test.md) to verify:

- adapter resume, live Inline RPCs, bridge-visible login status, per-account
  name/avatar metadata, portal visibility for the Matrix user, and bridged
  message rows when sampled Inline history has messages:

```sh
scripts/e2e-local.sh live-check
scripts/e2e-local.sh live-restart-check
```

- DM and group portal creation
- startup sync and catch-up
- inbound Inline to Matrix delivery
- outbound Matrix to Inline delivery
- restart recovery
- media, edits, deletes, reactions, typing, and read receipts

The local harness intentionally does not store Inline credentials outside the
normal adapter store in `data/e2e/bridge/inline-client/`.

`live-check` does not take Inline credentials. It verifies the already-logged-in
adapter session by fetching dialogs, recent history, one group member list when
a group chat is available, and a Matrix management-room `inline-status` command.
It also checks that the stored bridge login is named `Inline` and has an uploaded
account avatar, then waits for at least one bridge-created portal room to be
visible to the local Matrix user. If sampled Inline history contains messages,
it also requires at least one bridged message row.

`live-restart-check` runs the same verification, restarts the Rust adapter and
Go bridge, then repeats the verification against the existing Inline login.

Optional tuning:

```text
MATRIX_INLINE_E2E_MIN_VISIBLE_PORTALS=1
MATRIX_INLINE_E2E_MIN_BRIDGED_MESSAGES=0
MATRIX_INLINE_E2E_BRIDGE_WAIT_SECONDS=120
```

`fixture-check` is not a replacement for `live-check`: it does not exercise the
real Inline API, Rust adapter, auth provider, or production realtime server.
