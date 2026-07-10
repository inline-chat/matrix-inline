# Agent update: matrix-inline v0.2.0

Update the existing Matrix Inline deployment to `v0.2.0`. This release embeds
the Rust Inline client from public commit
[`51ee50d4c9969f4031587ca4d2973fe6b04d4fd0`](https://github.com/inline-chat/inline/commit/51ee50d4c9969f4031587ca4d2973fe6b04d4fd0)
and coordinates the Go bridge and Rust adapter on protocol version 3.

## What changed

- Durable per-bucket sync cursors, pending journals, lossless adapter replay,
  acknowledgement, and replay-gap reconciliation.
- Authoritative cold snapshots for user, space, chat, membership, history,
  reaction, read, deletion, and pin state before advancing recovery cursors.
- Explicit live-versus-cache failures so stale state cannot masquerade as a
  successful recovery.
- Exhaustive lossless event decoding, retryable media delivery, and idempotent
  outbound send reconciliation after uncertain timeouts.
- Non-destructive migration from the initial shared client store plus a
  retryable one-time bridge-state recovery for existing logins.

## Safety rules

- Preserve the existing data directory. Do not delete or recreate
  `matrix-inline.db`, `inline-client.sqlite3`, or `inline-client/accounts/`.
- Back up the whole data directory before updating.
- Stop the bridge before replacing it. The published container includes the
  matching bridge and adapter, so update them together.
- Use the versioned `v0.2.0` image tag or the immutable digest recorded on the
  GitHub release, not `latest`.
- If the source checkout has local changes, stop and preserve them instead of
  stashing, discarding, or overwriting them during the update.

## Update

From the existing `matrix-inline` checkout:

```sh
git status --short
git fetch --prune origin
git fetch --tags origin
git switch main
git pull --ff-only origin main
git rev-parse 'v0.2.0^{commit}'
```

Back up the deployment's full `data/` directory using the operator's normal
encrypted backup process. Then pin the Compose deployment and update it:

```sh
export MATRIX_INLINE_IMAGE=ghcr.io/inline-chat/matrix-inline:v0.2.0
docker compose pull
docker compose up -d
```

The container starts the adapter before the Go bridge. Keep the existing
volume/configuration mounted exactly as before.

## Verify

```sh
docker compose ps
docker compose logs --tail=300 matrix-inline
```

For an account upgraded from the initial bridge, wait for:

```text
Completed one-time Inline bridge state recovery
```

Then run `inline-status` in the bridge management room and complete the
[smoke checklist](smoke-test.md). Confirm that existing DMs/groups and members
are present, inbound and outbound messages work, and a restart catches up a
message sent while the bridge was down without duplicates.

If recovery fails, retain the logs and data directory and stop. Do not wipe the
database or log the account out; recovery remains retryable until its version is
persisted successfully.
