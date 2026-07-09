# Smoke Test

Run this checklist after a fresh install, config change, or upgrade.

## Local Checks

From the repo root:

```sh
scripts/check.sh
scripts/smoke-local.sh --start-adapter
scripts/e2e-local.sh fixture-check
scripts/e2e-local.sh fixture-restart-check
```

`fixture-check` uses a deterministic local sidecar to verify Matrix portal
creation, backfill, outbound messages, and inbound realtime delivery without an
Inline account. `fixture-restart-check` extends that coverage to bridge restart
catch-up and realtime delivery after reconnecting an existing login.

For an installed bridge:

```sh
curl -fsS http://127.0.0.1:29342/health | jq .
curl -fsS http://127.0.0.1:29342/status | jq .
curl -fsS -X POST http://127.0.0.1:29342/rpc/resume | jq .
```

Expected adapter states:

- `AuthRequired`: adapter is healthy and no Inline account is logged in yet.
- `Connected`: adapter has a valid Inline session.
- `AuthExpired`: the Inline account needs to log in again.
- `Reconnecting`: adapter is recovering from a network or realtime issue.

## Login

1. Open a chat with the bridge bot.
2. Send `login`.
3. Choose email or phone login when prompted.
4. Enter the verification code sent by Inline.
5. Run `inline-status`.
6. Confirm the account ID and sidecar status are shown.
7. For the local E2E harness, run `scripts/e2e-local.sh live-check`.
8. For local restart validation, run `scripts/e2e-local.sh live-restart-check`.
9. Confirm `live-check` reports at least one Matrix-visible portal for a
   non-empty Inline account.

## Messaging

Verify both directions:

1. Existing Inline chats appear as Matrix rooms.
2. Recent messages appear when a room is opened.
3. Sending text from Matrix appears in Inline.
4. Sending text from Inline appears in Matrix.
5. Normal replies work in both directions.
6. Matrix edits update the Inline message.
7. Matrix redactions delete or unsend the Inline message.
8. Reactions bridge in both directions.
9. Typing notifications appear where supported.
10. Opening a Matrix room marks the Inline chat read.

## Media

1. Send an image from Matrix and confirm it appears in Inline.
2. Send a file from Matrix and confirm it appears in Inline.
3. Send an image or video from Inline and confirm it appears in Matrix.
4. Send a voice or audio file and confirm it appears as playable media or a
   clear unavailable notice.

## Rooms and Members

1. Confirm the Matrix member list includes current Inline members.
2. Confirm ghost display names match Inline users where available.
3. Create a DM using a numeric Inline user ID.
4. Create a basic Matrix group/thread and confirm an Inline thread is created.

## Restart

1. Restart the bridge and adapter.
2. For the local E2E harness, run `scripts/e2e-local.sh live-restart-check`.
3. Confirm `inline-status` returns `Connected`.
4. Confirm existing rooms keep their Matrix mappings.
5. Send a message after restart in both directions.
6. Confirm messages sent during downtime are synced after reconnect.

The local `live-check` and `live-restart-check` commands catch the common
failure where the bridge has created portal rows internally but the Matrix user
has not been invited to or joined to those rooms.

## Known Limitations

- Login requires an existing Inline account.
- Invite-code signup is not handled inside the bridge.
- DM creation currently requires a numeric Inline user ID.
- Inline reply-thread chats are represented as Matrix rooms, not Matrix-native
  thread UI.
- Room avatars and some rich chat metadata may be missing.
- Calls are not bridged.
