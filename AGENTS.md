# Agent Notes

This repository contains the Matrix bridge for Inline.

## Boundaries

- Keep Matrix/Beeper appservice behavior in the Go bridge.
- Keep Inline API/session/cache behavior behind the local Rust adapter and
  `inline-client`.
- Keep the adapter bound to loopback by default.
- Do not commit secrets, generated local config, registrations, databases, or
  `.env` files.

## Checks

Run before committing:

```sh
scripts/check.sh
docker build --check -f Dockerfile ..
```

Use `-tags goolm` for local Go commands unless explicitly testing system
libolm packaging.

## Docs

Public docs should describe installation, operation, commands, supported
features, known limitations, and licensing. Keep private project notes out of
committed docs.
