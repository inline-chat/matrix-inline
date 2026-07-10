# Agent update: matrix-inline v0.2.1

Upgrade the Inline server first, then upgrade the Go bridge and Rust sidecar
together to `v0.2.1`. This release uses Inline Rust crates `0.6.2` and sidecar
protocol v4; mixed v3/v4 bridge-sidecar processes intentionally fail their
version check instead of risking cursor corruption.

This patch release repairs accounts created by the original beta as well as
normal upgraded accounts:

- the current server lossless schema is negotiated before catch-up;
- every cursor advance is backed by delivered or explicitly skipped sequences;
- poisoned journals are refetched without advancing;
- long gaps commit per page and dialogs resume with stable keyset cursors;
- adapter database rollback triggers generation reset plus authoritative
  reconciliation instead of permanent ack-ahead;
- cached/live history and backfill advance by message ID, not presentation
  timestamps;
- terminal media failures become handled notices while transient failures retry;
- delivered history checkpoints are stored in Matrix portal metadata.
- dialogs hidden from Inline's normal chat list are excluded from Matrix by
  default, with operator and per-account overrides.

Users can inspect or change the account policy through `inline-settings` and
`inline-hidden-chats <exclude|include|default>`. The default is non-destructive:
it prevents new hidden-dialog portal/history/event projection but does not
delete Matrix rooms created by an older bridge version.

Before rollout, back up both the Matrix database and sidecar SQLite database.
Do not reuse an older sidecar binary with the new Go bridge. After upgrade,
verify `/health` reports protocol `4`, core sync schema `1`, and a non-empty
event generation. Watch logs until one full dialog reconciliation completes and
event acknowledgements advance without generation resets.

Use the versioned `v0.2.1` image tag or the immutable digest recorded on the
GitHub release. Keep the existing database and Inline client store during the
upgrade.
