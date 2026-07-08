# Operations Runbook

This runbook is for the one-team beta operator.

## Start And Stop

Docker:

```sh
docker compose up -d
docker compose logs -f
docker compose down
```

systemd:

```sh
sudo systemctl start matrix-inline-adapter.service matrix-inline.service
sudo systemctl stop matrix-inline.service matrix-inline-adapter.service
sudo systemctl restart matrix-inline-adapter.service matrix-inline.service
```

## Health

```sh
curl -fsS http://127.0.0.1:29342/health | jq .
curl -fsS http://127.0.0.1:29342/status | jq .
curl -fsS -X POST http://127.0.0.1:29342/rpc/resume | jq .
```

Expected statuses:

- `AuthRequired`: adapter is healthy, but no Inline login is stored yet.
- `Connected`: adapter has a valid Inline session.
- `AuthExpired`: user needs to log in again.
- `Reconnecting`: adapter hit a transient network/realtime issue.

## Logs

Docker:

```sh
docker compose logs -f matrix-inline
```

systemd:

```sh
journalctl -u matrix-inline-adapter.service -u matrix-inline.service -f
```

Useful log knobs:

```text
RUST_LOG=info
RUST_LOG=matrix_inline_adapter=debug,inline_client=debug,info
```

Do not enable verbose HTTP/body logging in production. Inline auth material is
owned by the Rust client store and should not be printed.

## Backup

Back up these files together while the services are stopped:

```text
/data/config.yaml
/data/registration.yaml
/data/matrix-inline.db*
/data/inline-client/inline-client.sqlite3*
```

For systemd installs, the equivalent paths are:

```text
/etc/matrix-inline/config.yaml
/etc/matrix-inline/registration.yaml
/var/lib/matrix-inline/*
/var/lib/inline-client/inline-client.sqlite3*
```

The adapter SQLite store contains Inline session credentials. Keep backups
private and encrypted.

## Upgrade

1. Run `scripts/check.sh` before building an image or copying binaries.
2. Stop the bridge first, then the adapter.
3. Back up config, registration, Matrix DB, and Inline client store.
4. Install the new Go bridge and Rust adapter binaries or deploy the new image.
5. Start the adapter first, then the bridge.
6. Run `scripts/smoke-local.sh`.
7. Run the Matrix/Beeper smoke checklist in [smoke-test.md](smoke-test.md).

## Common Failures

Adapter unreachable:

- Confirm it is bound to `127.0.0.1:29342`.
- Check `INLINE_SIDECAR_URL` and `network.sidecar_url`.
- Check adapter logs before restarting the bridge.

Inline auth expired:

- `inline-status` should show `AuthExpired`.
- Use the bridge login flow again.
- If logout was intentional, use mautrix `logout <login ID>` and then log in.

Messages send from Matrix but do not appear in Inline:

- Check adapter logs for send/upload errors.
- Confirm `/status` is `Connected`.
- Confirm media size is under the beta upload limit.

Rooms appear without full members:

- Check adapter logs for `GetChatParticipants` failures.
- Run `inline-reconnect`, then open the portal again.

Restart does not resume:

- Confirm the adapter store path is stable and writable.
- Confirm the service user owns the store directory.
- Run `curl -fsS -X POST http://127.0.0.1:29342/rpc/resume | jq .`.
