# Agent update: matrix-inline v0.2.2

Upgrade the Inline server and bridge together to restore sync after the server
removes the short-lived core sync revision metadata. This release embeds
`inline-client` `0.6.3`, which validates every returned sequence directly and
accepts both the old revision-bearing response and the new additive response.

This is an in-place compatibility update for existing `v0.2.0` and `v0.2.1`
deployments. Keep the existing Matrix database and Inline client stores. The
release preserves credentials, recovery checkpoints, per-bucket cursors,
pending event delivery, hidden-dialog policy, and previously bridged rooms.

Before rollout:

- back up `matrix-inline.db`, `inline-client.sqlite3`, and
  `inline-client/accounts/`;
- stop the old bridge before replacing the bundled Go bridge and Rust adapter;
- do not wipe or recreate either database to recover sync;
- use the versioned image tag or immutable digest from the GitHub release.

After upgrade, confirm `/health` reports adapter protocol `4` and a non-empty
event generation. The `core_sync_schema_revision` health value is retained only
for sidecar protocol compatibility and is not a server compatibility gate.
Watch logs until dialog reconciliation completes and event acknowledgements
advance without repeated generation resets or cursor mismatch errors.

The server change remains safe for existing first-party clients because the
protobuf fields were additive and are now reserved. Older `matrix-inline`
`v0.2.1` deployments are the exception: they reject the new server's default
revision value, so deploy this bridge update before or together with the server.
