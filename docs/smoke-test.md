# Team Beta Smoke Test

Run this after a fresh deploy, after changing config, and after upgrading either
the Go bridge or Rust adapter.

## Local Process Health

```sh
scripts/check.sh
scripts/smoke-local.sh --start-adapter
```

For a deployed host:

```sh
curl -fsS http://127.0.0.1:29342/health | jq .
curl -fsS http://127.0.0.1:29342/status | jq .
curl -fsS -X POST http://127.0.0.1:29342/rpc/resume | jq .
```

Expected result:

- `/health` returns protocol version `1`.
- `/status` returns `Connected` after login, or `AuthRequired` before login.
- `/rpc/resume` does not return a transport/protocol error.

## Matrix/Beeper Flow

1. Start the adapter and bridge.
2. Log in with an existing Inline account using email or SMS code.
3. Confirm `inline-status` reports the sidecar status and account ID.
4. Confirm existing Inline dialogs appear as Matrix rooms.
5. Open a room and confirm recent history appears without duplicate outgoing echoes.
6. Confirm the member list includes all Inline members currently exposed by Inline.
7. Send text from Matrix and confirm it appears in Inline.
8. Send text from Inline and confirm it appears in Matrix.
9. Reply to a normal message from Matrix and confirm Inline receives a normal reply.
10. Reply to a normal message from Inline and confirm Matrix shows a normal reply.
11. Send an image or file from Matrix and confirm Inline receives it.
12. Send an image or file from Inline and confirm Matrix receives it or gets a clear unavailable notice.
13. Edit, delete, and react to a Matrix-originated text message.
14. Create a DM through Beeper/bridgev2 provisioning using a numeric Inline user ID.
15. Create a basic Matrix group/thread using bridgev2 group creation and confirm it opens as an Inline thread.
16. Restart the adapter and bridge.
17. Confirm login resumes, dialogs remain mapped, and new messages sent during downtime catch up.

## Beta Limitations To Verify Are Acceptable

- New account signup and invite-code onboarding are out of scope.
- DM lookup is numeric Inline user ID only until search/contact lookup is added.
- Inline reply-thread chats are bridged as standalone Matrix rooms, not Matrix thread UI.
- Room avatars and rich chat metadata are partial.
- Very old history/backfill policy is still conservative.
