# Operations

This runbook covers routine operation for `matrix-inline`.

## Start and Stop

Docker:

```sh
docker compose pull
docker compose up -d
docker compose logs -f matrix-inline
docker compose down
```

systemd:

```sh
sudo systemctl start matrix-inline-adapter.service matrix-inline.service
sudo systemctl stop matrix-inline.service matrix-inline-adapter.service
sudo systemctl restart matrix-inline-adapter.service matrix-inline.service
```

Start the adapter before the bridge. Stop the bridge before the adapter.

## Health

Adapter health:

```sh
curl -fsS http://127.0.0.1:29342/health | jq .
```

Account-specific adapter endpoints require the bridge's private session
namespace header. Use `inline-status` for routine account checks rather than
copying that namespace into shell history.

Bridge status from Matrix:

```text
inline-status
```

Status values:

- `AuthRequired`: no Inline account is logged in yet.
- `Connected`: Inline session is active.
- `AuthExpired`: log in again.
- `Reconnecting`: temporary network or realtime recovery.
- `Disconnected`: adapter is stopped or logged out.

## Logs

Docker:

```sh
docker compose logs -f matrix-inline
```

systemd:

```sh
journalctl -u matrix-inline-adapter.service -u matrix-inline.service -f
```

Logging defaults:

```text
RUST_LOG=info
```

Useful troubleshooting setting:

```text
RUST_LOG=matrix_inline_adapter=debug,inline_client=debug,info
```

Do not enable request body or token logging in production.

At debug level, the adapter logs lossless event sequence assignment,
replay, acknowledgement, and replay gaps; the bridge logs persisted cursors,
dialog-sync summaries, and rate-limit retry delays. The adapter retains up to
10,000 lossless events per account namespace and runs durable state
reconciliation if a bridge cursor falls behind that window.

The Rust client also keeps committed lossless events in its SQLite outbox until
the adapter has stored them. The adapter uses the client delivery ID to make
that handoff idempotent, then acknowledges the client event. A crash before or
after either write therefore replays the same logical event instead of silently
advancing the Inline sync cursor or assigning duplicate adapter sequences.

Adapter event cursors are `(generation, sequence)` pairs. If the adapter SQLite
database is restored behind the Matrix database, the adapter rotates to a new
generation and returns a structured reset. The Go bridge then performs
authoritative reconciliation and atomically replaces its saved cursor before
resuming replay. Protocol v4 requires the Go bridge and Rust sidecar to be
upgraded together.

Dialog reconciliation uses stable chat-ID keyset pagination and persists its
next cursor in login metadata. Activity reordering cannot skip rows, and large
accounts resume from the last completed page after restart or RPC-budget yield.
Delivered message IDs are also persisted monotonically in portal metadata.

Permanent media projection failures (invalid schemes, reviewed terminal 4xx,
or the configured size ceiling) produce a stable Matrix unavailable notice and
allow the lossless cursor to continue. Timeouts, 429, and 5xx remain retryable.

### Hidden Inline dialogs

The network config defaults `hidden_dialogs` to `exclude`. Dialogs whose Inline
state has `showInChatList: false` remain in the durable Rust cache but do not
create Matrix portals, fill history, or project inbound events. This reduces
reply-thread and other hidden-dialog noise without putting bridge policy in the
generic Inline client.

Each login can override the operator default through the bridge bot:

```text
inline-settings
inline-hidden-chats exclude
inline-hidden-chats include
inline-hidden-chats default
```

The override is stored in user-login metadata. Changing it resets the stable
dialog scan and reconnects the Go event loops so newly included dialogs are
reconciled. Excluding a dialog is non-destructive: an existing Matrix room is
not left or deleted automatically, but new Inline events for the hidden dialog
are no longer projected. If the Inline dialog later becomes visible, normal
reconciliation resumes and fills its missing history from durable state.

## Backups

Stop the bridge before making filesystem-level backups.

Docker paths:

```text
data/config.yaml
data/registration.yaml
data/matrix-inline.db*
data/inline-client/inline-client.sqlite3*
data/inline-client/accounts/
```

systemd paths:

```text
/etc/matrix-inline/config.yaml
/etc/matrix-inline/registration.yaml
/var/lib/matrix-inline/*
/var/lib/inline-client/inline-client.sqlite3*
/var/lib/inline-client/accounts/
```

Per-account Inline client stores contain session credentials, and the base
store may retain a legacy credential after a non-destructive migration. Store
backups encrypted and restrict access to the bridge operator.

## Upgrade

1. Read the release notes for config or migration notes.
2. Stop the bridge.
3. Back up config, registration, bridge database, and Inline client store.
4. Pull the new image or install the new binaries.
5. Start the adapter.
6. Start the bridge.
7. Run `inline-status`.
8. Run the checklist in [smoke-test.md](smoke-test.md).

When upgrading from the initial PoC release, do not remove
`inline-client.sqlite3` or `matrix-inline.db`. The adapter non-destructively
imports the legacy credential into the matching per-account store. Existing
login metadata with an older recovery version automatically triggers one live
dialog refresh followed by durable chat, message, deletion, reaction, read,
membership, and previously unavailable media reconciliation. Recovery is split
into bounded RPC passes, throttles group-member requests, and persists the last
completed chat ID. Chat cold repair owns the current snapshot and latest 50
messages; older history is streamed separately from the Matrix delivery
checkpoint instead of being downloaded in one unbounded account walk. The
recovery version is persisted only after mautrix accepts the complete
reconciliation, so an interrupted upgrade resumes safely.
Look for `Completed one-time Inline bridge state recovery` before declaring the
upgrade healthy.

For the server sync-metadata transition, upgrade to `v0.2.2` or newer before
deploying a server that omits `core_sync_schema_revision`. The sidecar continues
to validate explicit sequence accounting and page structure; the health field
with that name is informational and retained only for protocol-v4 compatibility.

Docker image updates:

```sh
docker compose pull
docker compose up -d
```

## User Commands

In the bridge management room:

```text
login
inline-status
inline-reconnect
logout <login ID>
```

Short aliases:

```text
istatus
ireconnect
```

## Troubleshooting

### Adapter Unreachable

- Confirm the adapter process is running.
- Confirm it is bound to `127.0.0.1:29342`.
- Confirm `network.sidecar_url` or `INLINE_SIDECAR_URL` points to the same URL.
- Check adapter logs before restarting the bridge.

### Login Fails

- Confirm the Inline account already exists.
- Confirm the email or phone number is entered in the same format used by
  Inline.
- Request a new verification code and try again.
- If the account requires an invite code, complete signup in Inline first.

### Auth Expired

- Run `inline-status`.
- Use `login` again from the bridge bot.
- If the old login should be removed, run `logout <login ID>`.

### Messages Do Not Send

- Confirm adapter status is `Connected`.
- Check adapter logs for send or upload errors.
- Check whether the message contains unsupported media.
- Run `inline-reconnect` and try again.

### Rooms Have Missing Members

- Open the Matrix room again to trigger a membership refresh.
- Run `inline-reconnect`.
- Check adapter logs for participant fetch errors.

### Restart Does Not Resume Login

- Confirm the adapter store path is stable.
- Confirm the service user owns the store directory.
- Run:

```sh
curl -fsS -X POST http://127.0.0.1:29342/rpc/resume | jq .
```
