# Architecture

## Class hierarchy

```
hivemind_plugin_manager.protocols.NetworkProtocol  (abstract)
        │
        └─ hivemind_mqtt_protocol.HiveMindMqttProtocol
                │
                └─ paho.mqtt.client.Client (ONE broker connection for the hub)
```

`HiveMindMqttProtocol.run()` is the blocking server entry point called by
`hivemind-core`. It connects a single paho-mqtt client to the external broker
and subscribes to the wildcard topic for incoming satellite messages.

## Broker-mediated topology

```
satellite ──pub──▶  broker  ◀──sub── hub
hub       ──pub──▶  broker  ◀──sub── satellite
```

The hub does not bind any TCP port. Both hub and satellites are broker
**clients**. This means:

- No inbound firewall rule is needed on the hub.
- Any satellite that can reach the broker can reach the hub.
- The broker handles delivery, buffering (QoS 1), and presence (LWT).

## Topic scheme

```
<prefix>/<hub_id>/c2s/<satellite_id>     # satellite → hub  (hub subscribes …/c2s/+)
<prefix>/<hub_id>/s2c/<satellite_id>     # hub → satellite
<prefix>/<hub_id>/status/<satellite_id>  # retained LWT presence (online / offline)
```

Defaults: `prefix = hivemind`, `hub_id` = node identity name.

The `satellite_id` is the satellite's HiveMind access key (or its SHA-256
hash when `hash_topics: true` is set).

## Crypto

The MQTT payload carries the **same encrypted HiveMessage frame** that the
WebSocket transport sends. The broker sees only ciphertext. HiveMind's full
AES-GCM / RSA / PAKE handshake runs unchanged inside the payload bytes.

No additional encryption layer is added by this transport.

## Authentication layers

1. **Broker-level**: MQTT `username` / `password` (config keys `broker_username` /
   `broker_password`), or TLS client-cert (config keys `tls_certfile` /
   `tls_keyfile`). Configure the broker's ACL so each satellite may only publish
   to its own `c2s/<key>` topic and subscribe to its own `s2c/<key>` topic.

2. **HiveMind-level**: the HELLO / HANDSHAKE exchange embedded in the encrypted
   payload, identical to the WebSocket path. The satellite's MQTT username must
   equal its HiveMind access key so the hub can look up the DB record on first
   contact.

## QoS

| Traffic type | QoS | Rationale |
|---|---|---|
| Control frames (default) | 1 (at-least-once) | Delivery guarantee for messages. |
| Binary / audio frames | 0 (fire-and-forget) | Low latency; re-transmission of audio is worse than a gap. |

## Idle eviction

Satellites that send no messages for `idle_timeout` seconds are evicted
(treated as disconnected). Their LWT `status/<satellite_id>` topic is
checked on eviction. Set `idle_timeout: 0` to disable eviction.

Default: 300 seconds.

## Privacy: hashed topics

Set `hash_topics: true` to replace the `satellite_id` segment with a
16-character hex SHA-256 hash. The broker then sees only an opaque token —
useful when the broker is shared or untrusted.

## Authoring a transport plugin

See [hivemind-websocket-protocol: authoring a transport plugin](https://github.com/JarbasHiveMind/hivemind-websocket-protocol/blob/dev/docs/architecture.md#authoring-a-transport-plugin)
for the `NetworkProtocol` ABC and entry-point registration pattern.
