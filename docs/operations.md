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
curl -fsS -H "X-Inline-Protocol-Version: 5" http://127.0.0.1:29342/health | jq .
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
resuming replay. Protocol v5 requires the Go bridge and Rust sidecar to be
upgraded together.

Receipt reconciliation conservatively reinspects timeline order after delivery,
including messages beyond the source read cursor. Inspection is limited to 20
pages of 500 events and a five-second reconciliation timeout. Long timelines can
delay the serialized room lane or leave read projection pending. The persisted
applied cursor alone cannot justify a shortcut after failed completion saves,
late delivery, deletion or mapping changes; receipt recovery remains enabled.

Dialog reconciliation uses stable chat-ID keyset pagination and persists its
next cursor in login metadata. Activity reordering cannot skip rows, and large
accounts resume from the last completed page after restart or RPC-budget yield.
Live message delivery and historical coverage use separate monotonic portal
checkpoints. A live message does not skip an older history page. History advances
only for an explicit source-ID catchup page requested from its current historical
frontier, after every selected message has a real mapping in the same portal and
receiver. Generic forward and backward pages verify mappings without advancing
that frontier; a new empty frontier requests source IDs after zero. Existing message and room IDs are retained. Enable
`backfill.enabled` to import or recover history; disabled backfill leaves history
pending. The pinned framework's timestamp cutoff is supplemented by bounded
source-ID recovery in the existing portal event lane.

Permanent media projection failures (invalid schemes, reviewed terminal 4xx,
empty files, or the configured size ceiling) produce a stable Matrix unavailable notice and
allow the lossless cursor to continue. Timeouts, 429, and 5xx remain retryable.

Matrix kicks and invite revocations are unsupported because removing a direct
participant may leave access through a group or public workspace. The bridge
does not claim the user lost access when it cannot verify that result.

### Rich messages and media

Bold, italic, code, preformatted text and safe links survive both directions.
Entity ranges use checked UTF-16 offsets. Only typed mentions whose identities
are verified for the current login become user mentions; unknown identities
stay readable and group mentions never become room-wide broadcasts.

Native message revisions take precedence over edit dates. Older or equal
revisions do not replace accepted content. When the source supplies neither,
content fingerprints suppress identical replays but cannot establish an order
between different revisions. Formatting changes are included in fingerprints.

Caption edits reuse the saved Matrix media object, including after restart.
Caption-only edits require the original Matrix media type, URI, filename and
attachment metadata. Clients that rewrite or omit that metadata can receive an
explicit unsupported error. Replacing a file through an edit is unsupported. Old messages without enough
saved media identity reject caption edits rather than silently ignoring a file
replacement. Outgoing media uses a bounded authenticated stream, including
encrypted-file hash verification, without an unbounded temporary-file spool.
The 100 MiB ceiling applies to declared and actual bytes. Empty and oversized
files are rejected. A Matrix backend without this bounded transport reports an
explicit unsupported error.

Incoming native voice records retain voice and duration metadata. A waveform
is omitted when the source does not provide one. Outgoing audio uses a file
fallback; native voice upload is not advertised. Video upload requires the
native dimension and duration metadata, otherwise it uses a file fallback.
Conflicting source kind/MIME information also uses a file presentation.
Photo-backed sticker content remains an image; dedicated sticker and animated
GIF semantics are not advertised.

### Accessible people and channel creation

User lookup accepts exact positive IDs and canonical usernames from this
login's accessible directory. Search returns a bounded subset, not a global
directory. Cached users alone do not prove access. Bounded or ambiguous username
results return an error rather than selecting an arbitrary person. Email and
phone lookup are unsupported. A missed exact ID in a truncated refresh also
returns an incomplete-result error; a known exact ID remains usable.

Peers known only through hidden or deleted conversations are excluded from
directory discovery. This is an eligibility limit, not a claim that a hidden
peer is unreachable or that Inline denied permission. The hidden-dialog
`include` setting changes room projection, not this directory policy. An
independent accessible-workspace member or successful live participant read
can still make the same peer discoverable. Restore source discovery before
starting a DM from Matrix when no eligible entry exists.

`thread` and `private_thread` create ordinary private Inline channels, with
explicit accessible participants. An optional parent must identify an accessible
workspace for the same login. `public_thread` requires that workspace parent
and no explicit participants. A true group DM is unsupported. Current Inline
permission errors remain authoritative; cached membership or role information
does not grant permission to create or manage a channel.

### Separate reply-child context

Eligible reply-child conversations keep their own room, message IDs, history
and access boundaries. Their topics retain readable source parent context and,
when the parent message already has a mapping for this login, a parent backlink.
Missing or deleted roots keep the source chat/message reference. Context is
written only inside the child; it does not publish private child details in the
parent. The default hidden-dialog policy still excludes hidden children.

Source channel descriptions can populate empty or connector-owned Matrix topics;
later user topics are preserved. This read-side projection does not enable topic
editing or generate DM topics. Temporary parent-mapping database errors retain
existing child context; conclusive missing mappings retain readable native IDs.

Generated topics use a durable pending-intent journal. An authoritative detach
retracts only connector-owned context after Matrix state comparison/readback;
unknown descriptors, later user edits and legacy unowned topics are preserved.
Pending topic writes can retry on a later portal event, at most once per room
every 30 seconds. An idle room has no automatic retry, and failures before an
intent is saved require a later dialog reconciliation.
Topic replacement requires a controlled window: Matrix state comparison is not
an atomic compare-and-swap with concurrent user edits.

Matrix thread routing and native child creation are unsupported. Thread history
requests fail explicitly, and a parent chat's message ID cannot be sent as a
child-local reply ID. No audience/history equivalence or thread flattening is
assumed.

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

### Channel identity and workspace navigation

Authoritative Inline structure distinguishes DMs, channels, and separate reply
children. Channels use ordinary Matrix rooms; this changes their presentation in
clients that previously treated all non-DMs as group DMs. A partial or conflicting
descriptor does not invent a room type. Descriptions and workspace names come
from the account's cached source records.

A workspace becomes a Matrix Space only with source-backed access evidence for
that login. Explicitly removed access or absence from a complete access list detaches
navigation; unknown access holds existing links and pending hierarchy work. A
failed snapshot refresh retains the last known snapshot and does not trigger a
detach. Navigation records never grant mutation permission or delete
independently accessible chat rooms or their history. Access proof is refreshed
in bounded passes, so a newly discovered workspace can appear later. Status
reports unverified access without exposing workspace names or member lists.

### Notification and read-state limits

`network.mute_channels_by_default: true` records the operator's desired default
for new channels. Each login can override that desired default:

```text
inline-channel-default muted
inline-channel-default normal
inline-channel-default default
inline-settings
```

Automatic mute, Low Priority, and manual-unread writes are disabled because the
current target cannot reliably distinguish their bridge echoes from later user
choices. `notification_policy_degraded` is reported immediately and message
delivery continues. These settings do not silently quiet channels. For a quiet
inbox, mute channels manually in Beeper/Matrix; putting them in Low Priority and
choosing a DMs view are separate client settings. Inline's native notification
mode is preserved independently. Explicit user mute/unmute remains supported.

Incoming source read state uses only the verified prefix of the actual Matrix
timeline. An unmapped event or an unread source event is a barrier, including
stickers and polls. Timeline inspection is bounded to 10,000 events; missing
mappings, exhausted bounds, or a missing double puppet leave the intent pending.
With double puppeting, the bridge writes private read receipts and fully-read
markers, verifies their readback, and checks that manual-unread was untouched.
Exact receipt observations and sync positions survive later readback failures in
the journal, bound to the account, room, puppet, target, and current source intent.
A changed intent or conflicting private receipt invalidates that evidence.
It does not emit public source-driven receipts. The pinned mautrix version
forwards genuine public user receipts to Inline but ignores genuine private user
receipts, which is a known outgoing fidelity limit. Unverified manual-unread
projection remains pending rather than being claimed as applied.

### Existing-room presentation migration

Migration preserves room IDs, message mappings, history checkpoints, and local
notification/tag state. Select 1–50 explicit positive numeric chat IDs in the
management room; omission of a mode means preview:

```text
inline-migrate preview 202 203
inline-migrate apply 202 203
inline-migrate rollback 202 203
```

Preview performs no presentation writes. Apply journals the before and desired
room kind, workspace links, and bridge metadata before making effects. A repeated
apply or rollback resumes an interrupted operation. Every link write uses an
exact journaled value, so restart can distinguish a partial migration from an
unrelated later edit. Rollback restores saved relationship metadata as well as
both link directions. A differing user value causes a conflict. Rolled-back or
conflicted presentation stays held until explicitly selected again. The hold runs
before the framework receives mutable room metadata, including on resync after a
restart. History checks and post-delivery reconciliation continue while
presentation is held.

Run apply/rollback in a controlled window with no concurrent administrators or
clients editing room presentation. Matrix has no atomic conditional state write:
per-write read guards detect existing edits, but an edit racing between the read
and write can still be overwritten. This is not an atomic remote compare-and-set
guarantee. Inspect preview and status before and after the operation; resolve
conflicts deliberately rather than repeatedly overriding a user's choice.

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
curl -fsS -H "X-Inline-Protocol-Version: 5" -X POST http://127.0.0.1:29342/rpc/resume | jq .
```
