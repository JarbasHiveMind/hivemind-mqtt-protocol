# Operations

## Broker requirements

Any MQTT 3.1.1 or 5.0 broker works. Common choices:

- **Mosquitto** — lightweight, easy to configure, widely deployed on Raspberry Pi and
  Home Assistant OS.
- **EMQX** — production-grade, built-in dashboard, native RediSearch and Postgres
  integrations.
- **HiveMQ** — enterprise-grade, strong ACL support.

The hub and satellites only need TCP access to the broker — no inbound ports on
either side.

## Mosquitto quick setup

Install and enable:

```bash
sudo apt install mosquitto mosquitto-clients
sudo systemctl enable --now mosquitto
```

Basic config at `/etc/mosquitto/conf.d/hivemind.conf`:

```
listener 1883
allow_anonymous false
password_file /etc/mosquitto/passwd
```

Create a user for the hub:

```bash
sudo mosquitto_passwd -c /etc/mosquitto/passwd hivemind-node
```

Set `broker_username` and `broker_password` in `server.json` to match.

## ACL (per-satellite topic restriction)

For production, restrict each satellite's MQTT credentials to its own topics.
In Mosquitto, create `/etc/mosquitto/acl`:

```
# Hub can read and write anything under hivemind/
user hivemind-node
topic hivemind/#

# Satellite with key "abc123" can only use its own topics
user abc123
topic read hivemind/+/s2c/abc123
topic write hivemind/+/c2s/abc123
topic write hivemind/+/status/abc123
```

Set `per_listener_settings true` and `acl_file /etc/mosquitto/acl` in
the Mosquitto config.

## Mosquitto with TLS

```
listener 8883
cafile   /etc/mosquitto/ca.crt
certfile /etc/mosquitto/server.crt
keyfile  /etc/mosquitto/server.key
require_certificate false
```

Set `tls: true` and `broker_port: 8883` in the plugin config. Point
`tls_ca_certs` at the CA certificate that signed the broker's cert.

## Home Assistant integration

If your Home Assistant instance already runs an MQTT broker (Mosquitto add-on),
you can run `hivemind-core` alongside it and share the broker. Use
`hash_topics: true` if the HA MQTT namespace is shared and you don't want
the satellite's HiveMind access key visible in topic names.

## Authoring a transport plugin

See [hivemind-websocket-protocol: authoring a transport plugin](https://github.com/JarbasHiveMind/hivemind-websocket-protocol/blob/dev/docs/architecture.md#authoring-a-transport-plugin)
for the `NetworkProtocol` ABC and `pyproject.toml` entry-point registration pattern.
