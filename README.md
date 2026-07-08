# matrix-inline

Official Matrix/Beeper bridge for Inline.

This bridge is intentionally thin:

```text
Beeper / Matrix
  -> mautrix-go bridgev2 connector
  -> local matrix-inline-adapter sidecar
  -> Rust inline-client
  -> inline-sdk / inline-protocol
  -> Inline API + realtime
```

The beta target is one real team with reliable daily coverage for the common
90% of usage. Inline behavior such as sessions, realtime reconnect, update
ordering, caching, cursors, media, and transaction reconciliation belongs in
Rust `inline-client`; this repo owns Matrix/Beeper glue.

Current scaffold:

- bridgev2 process entrypoint
- connector metadata and capabilities
- Beeper login flows for existing Inline users using email or SMS verification codes through the Rust adapter
- typed Go client for sidecar auth, status, dialogs, history, sends, uploads, edits, deletes, reactions, reads, typing, and event stream
- Docker image/compose packaging that runs the Go bridge and Rust adapter together with persistent `/data` storage
- Matrix text and media send paths through the sidecar for numeric Inline chat portal IDs
- Matrix-originated replies, text edits, redactions, reactions, read receipts, and typing forwarded to Inline
- Matrix/Beeper chat-viewing signals mark the Inline chat read when the client opens a portal
- startup dialog sync into Matrix portals
- checkpoint-aware recent history sync: unchanged dialogs skip startup history, and changed dialogs request only messages after the sidecar's durable checkpoint
- bridgev2 historical backfill through sidecar history pagination, including stored pagination cursors and silent/read-marked Matrix batch sends
- sidecar session resume: the Rust adapter resumes the stored Inline session on restart, and the Go bridge rechecks sidecar status after event-stream reconnects
- live sidecar event loop for committed `MessageStored`, `MessageDeleted`, `ReactionChanged`, and `Typing` events
- chat-scoped Matrix message IDs, transaction-aware echo reconciliation, and delete forwarding
- text/reply conversion, inbound message upserts for edits, inbound reactions/typing, plus notice fallback for unsupported Inline content
- inbound Inline media conversion for sidecar descriptors with CDN URLs: image, video, file, and voice/audio media are downloaded, uploaded to Matrix, and degraded to unavailable notices if download data is missing/expired
- outbound Matrix-originated image, video, file, and audio media sends: Matrix media is downloaded/decrypted by mautrix-go, posted to the Rust adapter as multipart bytes, uploaded by `inline-client`, and sent through Inline realtime
- startup dialog/history sync consumes sidecar user records so Matrix ghosts can use Inline display names, bot flags, and avatars without Go parsing Inline protocol payloads
- full member snapshots from Inline participants for portal membership sync
- Beeper/bridgev2 DM creation through numeric Inline user IDs
- basic Matrix group creation mapped to Inline thread creation
- Rust adapter routes for creating DMs, normal threads, and reply-thread chats
- management commands: `inline-status` / `istatus`, `inline-reconnect` / `ireconnect`, plus mautrix's built-in `logout <login ID>` flow

Expected local sidecar default:

```text
http://127.0.0.1:29342
```

Current local adapter command:

```sh
cargo run -p matrix-inline-adapter -- \
  --bind 127.0.0.1:29342 \
  --store ./data/inline-client.sqlite3
```

The bridge currently connects to a configured loopback sidecar URL. The beta
deployment shape supports either a single Docker container with both binaries or
two supervised systemd services. See
[docs/beta-deployment.md](docs/beta-deployment.md) and
[deploy/systemd](deploy/systemd).

Not yet implemented in the bridge scaffold:

- invite-code signup/onboarding inside Beeper; create or join Inline first, then log in through the bridge
- richer outbound media edge cases: thumbnails, waveform/voice-specific upload if Inline exposes it, and preserving videos as videos when Matrix omits required dimensions/duration
- inbound read receipts beyond current sidecar read-state detail
- richer profile refresh beyond the current sidecar-backed ghost name/bot/avatar cache
- room/group avatars when Inline exposes suitable chat avatar descriptors
- search/contact lookup for DM creation; beta DM provisioning currently expects numeric Inline user IDs
- Matrix-native thread rendering for Inline reply threads; Inline reply-thread chats are standalone Matrix rooms for beta
- archive-grade historical backfill policy, search-driven history discovery, and admin/user controls beyond current bridgev2 pagination
- published image/release automation for Docker artifacts

Development checks:

```sh
scripts/check.sh
scripts/smoke-local.sh --start-adapter
```

Without `-tags goolm`, mautrix-go uses system libolm headers/libraries. That is
fine for Docker or production images, but local development should prefer the
pure-Go Olm tag unless we explicitly need to test libolm packaging.

Deployment and operations:

- [docs/beta-deployment.md](docs/beta-deployment.md)
- [docs/smoke-test.md](docs/smoke-test.md)
- [docs/operations.md](docs/operations.md)
