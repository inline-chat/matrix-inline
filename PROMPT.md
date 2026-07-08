# Agent Setup Prompt

Use this prompt with an agent that is setting up matrix-inline on a server,
NAS, VPS, self-hosting platform, or Beeper/Matrix bridge host.

```text
Set up the official Inline Matrix bridge.

Repository:
https://github.com/inline-chat/matrix-inline

Primary docs:
- README: https://github.com/inline-chat/matrix-inline/blob/main/README.md
- Deployment guide: https://github.com/inline-chat/matrix-inline/blob/main/docs/deployment.md
- Operations guide: https://github.com/inline-chat/matrix-inline/blob/main/docs/operations.md

Use the current published GHCR image or the deployment method requested by the
operator. Follow the docs for the selected path: Beeper bridgev2, self-hosted
Docker/Compose, native/systemd, or another compatible Matrix appservice host.

Requirements:
- Preserve a persistent data directory for config, registration, database, and
  Inline session state.
- Generate or provide the bridge config and appservice registration using the
  documented flow.
- Register the appservice with the Matrix homeserver, then restart the
  homeserver before keeping the bridge running.
- Keep all tokens, appservice registrations, bridge databases, and Inline
  session files private. Do not print secrets in logs or chat.
- Do not configure bridge icon/avatar manually unless custom branding is
  requested; the bridge ships with Inline defaults.

After setup, verify:
- The bridge process stays running.
- The adapter health check passes.
- The homeserver can reach the bridge appservice URL.
- The Matrix/Beeper management room can start Inline login.
- A test Inline login, chat sync, and message send work.

If something fails, collect the relevant bridge, adapter, and homeserver logs
with secrets redacted, then identify whether the issue is bridge config,
homeserver appservice registration, network reachability, or Inline API auth.
```
