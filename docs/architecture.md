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
<prefix>/<api_key>/in      # satellite → master  (master subscribes <prefix>/+/in)
<prefix>/<api_key>/out     # master → satellite
<prefix>/<api_key>/status  # retained LWT presence (online / offline)
```

Defaults: `prefix = hivemind`.

The `api_key` segment is the satellite's HiveMind access key. It is unique per
client, so the master can look up the matching DB record from the topic as soon
as the first frame arrives; without the matching crypto key the payload
ciphertext remains useless.

## Crypto

The MQTT payload carries the **same encrypted HiveMessage frame** that the
WebSocket transport sends. The broker sees only ciphertext. HiveMind's full
AES-GCM / RSA / PAKE handshake runs unchanged inside the payload bytes.

No additional encryption layer is added by this transport.

## Authentication layers

1. **Broker-level**: MQTT `username` / `password` (config keys `broker_username` /
   `broker_password`), or TLS client-cert (config keys `tls_certfile` /
   `tls_keyfile`). Configure the broker's ACL so each satellite may only publish
   to its own `<api_key>/in` topic and subscribe to its own `<api_key>/out` topic.

2. **HiveMind-level**: the HELLO / HANDSHAKE exchange embedded in the encrypted
   payload, identical to the WebSocket path. The `api_key` IS the topic segment,
   so the master can look up the DB record on first contact without a separate
   MQTT-layer credential handshake.

## QoS

| Traffic type | QoS | Rationale |
|---|---|---|
| Control frames (default) | 1 (at-least-once) | Delivery guarantee for messages. |
| Binary / audio frames | 0 (fire-and-forget) | Low latency; re-transmission of audio is worse than a gap. |

## Idle eviction

Satellites that send no messages for `idle_timeout` seconds are evicted
(treated as disconnected). Their LWT `<api_key>/status` topic is
checked on eviction. Set `idle_timeout: 0` to disable eviction.

Default: 300 seconds.

## Authoring a transport plugin

See [hivemind-websocket-protocol: authoring a transport plugin](https://github.com/JarbasHiveMind/hivemind-websocket-protocol/blob/dev/docs/architecture.md#authoring-a-transport-plugin)
for the `NetworkProtocol` ABC and entry-point registration pattern.
