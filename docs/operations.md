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
curl -fsS http://127.0.0.1:29342/status | jq .
curl -fsS -X POST http://127.0.0.1:29342/rpc/resume | jq .
```

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

## Backups

Stop the bridge before making filesystem-level backups.

Docker paths:

```text
data/config.yaml
data/registration.yaml
data/matrix-inline.db*
data/inline-client/inline-client.sqlite3*
```

systemd paths:

```text
/etc/matrix-inline/config.yaml
/etc/matrix-inline/registration.yaml
/var/lib/matrix-inline/*
/var/lib/inline-client/inline-client.sqlite3*
```

The Inline client store contains session credentials. Store backups encrypted
and restrict access to the bridge operator.

## Upgrade

1. Read the release notes for config or migration notes.
2. Stop the bridge.
3. Back up config, registration, bridge database, and Inline client store.
4. Pull the new image or install the new binaries.
5. Start the adapter.
6. Start the bridge.
7. Run `inline-status`.
8. Run the checklist in [smoke-test.md](smoke-test.md).

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
