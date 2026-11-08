# Adapter compatibility

The bridge and adapter use protocol 5. Upgrade them together with the matching
`inline-client` revision recorded by the build. Keep each account's namespace
and durable stores when upgrading.

`GET /compatibility` returns protocol metadata without selecting an account,
opening a native client or creating an event-log namespace. Clients must verify
`protocol_version` before calling account routes. The Go bridge probes before
every request and websocket connection, including direct authentication calls.

Every other route requires `X-Inline-Protocol-Version: 5`, including `/health`,
`/status`, multipart uploads, event acknowledgements and websocket setup. The
adapter rejects missing, unsupported, malformed or duplicate version headers
with HTTP 409 before resolving an account or parsing a request body. The generic
`/rpc` envelope must also have `protocol_version: 5`; it is validated before the
account client is resolved. Alias routes use the required header as their
request version.

For a manual compatibility check:

```sh
curl -fsS http://127.0.0.1:29342/compatibility | jq .
curl -fsS -H 'X-Inline-Protocol-Version: 5' http://127.0.0.1:29342/health | jq .
```

Protocol 4 adapters do not implement this ingress guarantee. The preflight
rejects an old adapter before an operation when it is already installed. It
cannot prevent an old adapter from ignoring the version header if it replaces
the compatible process between the probe and that operation. Stop bridge
clients before a downgrade; an in-flight downgrade is unsupported.

Protocol 5 carries a nullable complete structural descriptor for a dialog.
An absent descriptor means unknown metadata; explicit null parent or workspace
IDs in a complete descriptor clear the old relation. Structural completeness
does not imply complete read state, notifications or membership. Cached space,
member and user arrays likewise do not establish current access: consumers
must check access evidence and its completeness before creating navigation or
enabling operations.
