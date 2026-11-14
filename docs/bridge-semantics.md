# Bridge semantics and upgrade notes

## Upgrading

Upgrade the Go bridge and Rust adapter together: this source uses adapter
protocol **5**. Bare `/rpc/*` requests require the
`X-Inline-Protocol-Version: 5` header; `/rpc` validates its versioned envelope.
`/health` and `/status` remain usable for diagnostics. Build the native client revision pinned in the CI workflows.
Back up both databases before upgrading. Native client structure and directory
state use additive SQLite changes; preserve the backup if rolling back binaries.
The bridge retains existing Matrix room IDs and message mappings.

For local validation without Docker, use `SKIP_DOCKER_CHECKS=1 scripts/check.sh`.
This explicitly skips Compose validation; container validation remains a separate
check. Do not interpret the skipped check as a tested container build.

## Conversations and workspace navigation

New channels are ordinary Matrix rooms; direct messages remain DMs. Unknown
legacy source structure is refreshed before assigning a type. A child
conversation keeps a separate room and its own history and audience. Its topic
can show parent context and a backlink when the parent is accessible and already
mapped for that login. Private child details are never written into the parent.
Hidden dialogs remain excluded by default.

User-authored Matrix topics are preserved. Pending child context and workspace
links retry in bounded passes even when the room is idle. Workspace Spaces are
scoped to the source account. Confirmed workspace removal removes navigation;
a temporary refresh failure retains known navigation. Neither cached membership
nor a Matrix Space grants source permissions or causes independently accessible
chat history to be deleted.

Workspace detail refreshes rotate through at most eight workspaces per pass
within a ten-second budget. Partial successes are retained, and an incomplete
pass does not erase previously proven membership. Larger accounts may need
multiple passes before every workspace has current detail.

Existing rooms previously classified as group DMs require an explicit repair:

```text
inline-repair-room 123
inline-repair-room 123 apply
```

The first command previews. Apply updates bridge room classification in place and
saves the previous classification in bridge metadata. It preserves room/message
IDs, user topics and notification preferences. Run repairs without concurrent
administrators editing room presentation; Matrix state writes do not provide an
atomic compare-and-swap against those edits.

## People and chat creation

Lookup accepts positive source user IDs or canonical usernames and uses the
current login's source directory. Search is bounded; an incomplete result is not
a complete global contact list. Ambiguous usernames produce an error. Phone and
email directory searches are not supported.

Creation types are `thread` (ordinary private chat), `private_channel`, and
`public_channel`. Workspace channel types require this login's Inline workspace
as the parent. Source permissions decide creation and participant access.
Native child creation, Matrix thread routing, and true group-DM creation are not
advertised; unsupported thread-history requests return an error.

## Rich messages, edits and media

Supported formatting includes bold, italic, underline, strikethrough, code,
preformatted text and safe links. Supported user mentions retain verified
identity; unknown identities and group mentions remain readable without
broadcast notifications. Unicode ranges use checked UTF-16 coordinates.

Source revisions prevent stale edits from overwriting accepted content, including
formatting-only changes. Attachment caption edits reuse the saved Matrix media
object and encryption envelope across restart. Replacing an attachment through
an edit is unsupported. Legacy attachments require retrieval of their original
Matrix event before a caption change can be validated.

Matrix uploads use the configured authenticated media transport with a 100 MiB
limit on declared and actual bytes while reading. Encrypted attachment hashes
are verified before plaintext is uploaded. Invalid/oversized attachments fail
explicitly; transient network errors remain retryable. An unsupported Matrix
backend without bounded transport fails explicitly rather than downloading
without a limit. Ordinary audio uses a file with its audio MIME type; explicit
voice-note metadata is retained where supported. Invalid video metadata uses a
file fallback. Dedicated sticker/GIF semantics are not advertised.

## Delivery and read state

The bridge waits for completed framework handling and verifies durable message
mappings. A queued/backgrounded response cannot acknowledge lossless source
work. History coverage is separate from live arrivals; a newer live message does
not skip an older history page. Failed backfill conversion returns an error so
its cursor does not silently advance. Backfill itself does not mark the room read.

Source read intents are stored durably per login and account namespace. With the
supported double-puppet transport, the bridge sends private read/fully-read
markers only for a verified prefix of the Matrix timeline. An unread or unmapped
event is a barrier, including unknown message types. Inspection is limited to
20 pages of 500 events and a five-second deadline; unproven work remains pending
and retries during dialog scans. Large or partially mapped timelines can delay
read projection.

Verified plaintext edits by the original sender can be traversed. Encrypted or
unverifiable edits, and redacted events whose durable mapping has been removed,
remain barriers.

In the other direction, an explicit public Matrix receipt needs a real mapped
event, completed source history coverage through that message, and verified
Matrix timeline order. A lower source message delivered later must not be read
by an earlier Matrix receipt. Opening/viewing a room alone performs no source
read mutation. Unverifiable timelines withhold receipts rather than guessing.

Automatic source-driven marked-unread and mute/tag writes are not performed:
they can overwrite later Matrix user choices. Private marker writes do not
clear the Matrix marked-unread flag. Explicit Matrix user unread/mute changes
still reach Inline. The pinned framework forwards public Matrix receipts but
not private user receipts; that direction remains a framework limitation.
