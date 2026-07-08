# systemd units

These units run the Rust `matrix-inline-adapter` and the Go `matrix-inline`
bridge as two supervised processes for the one-team beta.

Expected paths:

```text
/opt/inline/bin/matrix-inline-adapter
/opt/inline/bin/matrix-inline
/etc/matrix-inline/config.yaml
/var/lib/inline-client/inline-client.sqlite3
/var/lib/matrix-inline/
```

Create the service user and directories before installing:

```sh
sudo useradd --system --home /var/lib/matrix-inline --shell /usr/sbin/nologin inline-bridge
sudo install -d -o inline-bridge -g inline-bridge -m 0700 /var/lib/inline-client /var/lib/matrix-inline
sudo install -d -o inline-bridge -g inline-bridge -m 0750 /etc/matrix-inline
```

Install and start:

```sh
sudo install -m 0644 deploy/systemd/matrix-inline-adapter.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/matrix-inline.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now matrix-inline-adapter.service
sudo systemctl enable --now matrix-inline.service
```

Both processes intentionally run under the same user. The sidecar SQLite store
contains Inline session material and must stay readable only by that service
user.
