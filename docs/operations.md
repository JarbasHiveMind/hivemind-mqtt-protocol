# Operations

## Broker requirements

Any MQTT 3.1.1 or 5.0 broker works. Common choices:

- **Mosquitto**: lightweight, easy to configure, widely deployed on Raspberry
  Pi and Home Assistant OS.
- **EMQX**: built-in dashboard, native RediSearch and Postgres integrations.
- **HiveMQ**: strong ACL support.

The hub and satellites only need TCP access to the broker. Neither side needs
an inbound port.

## Mosquitto quick setup

Install and enable Mosquitto:

```bash
sudo apt install mosquitto mosquitto-clients
sudo systemctl enable --now mosquitto
```

Add a basic config at `/etc/mosquitto/conf.d/hivemind.conf`:

```
listener 1883
allow_anonymous false
password_file /etc/mosquitto/passwd
```

Create a broker user for the master:

```bash
sudo mosquitto_passwd -c /etc/mosquitto/passwd hivemind-master
```

Set `broker_username` (`hivemind-master`) and `broker_password` in
`server.json` to match.

## ACL: per-satellite topic restriction

For production, restrict each satellite's MQTT credentials to its own topics.
The `api_key` in the topic is acceptable in the clear. It works like a
username, and without the matching crypto key the payload ciphertext stays
useless. Still, a per-`api_key` ACL stops one satellite from reading or
impersonating another at the broker level. In Mosquitto, create
`/etc/mosquitto/acl`:

```
# Master can read and write anything under hivemind/ (including its own
# presence topic hivemind/<master_name>/status).
user hivemind-master
topic hivemind/#

# Satellite with api_key "abc123" can only use its own topics:
#   - read its own out topic (master → satellite)
#   - write its own in topic (satellite → master) and status (LWT/presence)
user abc123
topic read hivemind/abc123/out
topic write hivemind/abc123/in
topic write hivemind/abc123/status
```

Repeat the three-line `user <api_key>` block for each satellite. Set
`per_listener_settings true` and `acl_file /etc/mosquitto/acl` in the
Mosquitto config.

## Mosquitto with TLS

```
listener 8883
cafile   /etc/mosquitto/ca.crt
certfile /etc/mosquitto/server.crt
keyfile  /etc/mosquitto/server.key
require_certificate false
```

Set `tls: true` and `broker_port: 8883` in the plugin config. Point
`tls_ca_certs` at the CA certificate that signed the broker's certificate.

## Home Assistant integration

If your Home Assistant instance already runs an MQTT broker (the Mosquitto
add-on), you can run `hivemind-core` alongside it and share the broker. Use a
distinct `topic_prefix` and per-satellite ACLs so HiveMind traffic stays
isolated from the rest of the HA MQTT namespace.

## Authoring a transport plugin

See [hivemind-websocket-protocol: authoring a transport plugin](https://github.com/JarbasHiveMind/hivemind-websocket-protocol/blob/dev/docs/architecture.md#authoring-a-transport-plugin)
for the `NetworkProtocol` ABC and the `pyproject.toml` entry-point
registration pattern.

---
[← Configuration](configuration.md) · [Home](../README.md)
