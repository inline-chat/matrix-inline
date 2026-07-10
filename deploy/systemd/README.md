# systemd

These unit files run `matrix-inline-adapter` and `matrix-inline` as two
supervised services.

## Paths

The included units use these paths:

```text
/opt/inline/bin/matrix-inline-adapter
/opt/inline/bin/matrix-inline
/etc/matrix-inline/config.yaml
/etc/matrix-inline/registration.yaml
/var/lib/inline-client/inline-client.sqlite3
/var/lib/inline-client/accounts/<session-namespace>.sqlite3
/var/lib/matrix-inline/
```

## Service User

Create a dedicated service user and private storage:

```sh
sudo useradd --system --home /var/lib/matrix-inline --shell /usr/sbin/nologin inline-bridge
sudo install -d -o inline-bridge -g inline-bridge -m 0700 /var/lib/inline-client /var/lib/matrix-inline
sudo install -d -o inline-bridge -g inline-bridge -m 0750 /etc/matrix-inline
```

Per-account adapter stores contain Inline session credentials, and the base
store may retain a legacy credential after migration. Both paths should only
be readable by the service user.

## Install Binaries

Build from the repo root:

```sh
cargo build --release -p matrix-inline-adapter
go build -tags goolm -o ./matrix-inline ./cmd/matrix-inline
```

Install:

```sh
sudo install -d -m 0755 /opt/inline/bin
sudo install -m 0755 target/release/matrix-inline-adapter /opt/inline/bin/
sudo install -m 0755 matrix-inline /opt/inline/bin/
```

## Configure

Generate and edit the bridge config:

```sh
sudo -u inline-bridge /opt/inline/bin/matrix-inline \
  --config /etc/matrix-inline/config.yaml \
  --generate-example-config
```

Generate registration:

```sh
sudo -u inline-bridge /opt/inline/bin/matrix-inline \
  --config /etc/matrix-inline/config.yaml \
  --registration /etc/matrix-inline/registration.yaml \
  --generate-registration
```

Register `/etc/matrix-inline/registration.yaml` with your homeserver before
starting the bridge service.

## Install Units

```sh
sudo install -m 0644 deploy/systemd/matrix-inline-adapter.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/matrix-inline.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now matrix-inline-adapter.service
sudo systemctl enable --now matrix-inline.service
```

## Logs and Health

```sh
systemctl status matrix-inline-adapter.service
systemctl status matrix-inline.service
journalctl -u matrix-inline-adapter.service -u matrix-inline.service -f
curl -fsS http://127.0.0.1:29342/health
```
