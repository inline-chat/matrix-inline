# Matrix Inline Agent Notes

- This repo is the official Matrix/Beeper bridge for Inline.
- Keep Inline protocol, sync, cache, retry, and transaction behavior in Rust `inline-client`.
- Keep this Go bridge focused on mautrix-go bridgev2 glue, Matrix/Beeper provisioning, portals, ghosts, Matrix content conversion, packaging, and docs.
- Never store long-lived Inline auth tokens in Go bridge metadata unless the Rust client store cannot own the session yet. Prefer passing credentials to the local sidecar during login and storing only account/session glue in bridge metadata.
- Use Beeper LINE bridge patterns for bridgev2 capabilities, login flows, bridge state, startup sync, media placeholders, message status errors, and deployment docs.
