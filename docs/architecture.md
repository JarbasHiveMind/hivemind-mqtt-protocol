# Architecture

## Class hierarchy

```
hivemind_plugin_manager.protocols.NetworkProtocol  (abstract)
        │
        └─ hivemind_mqtt_protocol.HiveMindMqttProtocol
                │
                └─ paho.mqtt.client.Client (ONE broker connection for the hub)
```

`HiveMindMqttProtocol.run()` is the blocking server entry point that
`hivemind-core` calls. It connects a single paho-mqtt client to the external
broker and subscribes to the wildcard topic for incoming satellite messages.

## Broker-mediated topology

```
satellite ──pub──▶  broker  ◀──sub── hub
hub       ──pub──▶  broker  ◀──sub── satellite
```

The hub does not bind any TCP port. Both the hub and the satellites are broker
clients. This has three effects:

- The hub needs no inbound firewall rule.
- Any satellite that can reach the broker can reach the hub.
- The broker handles delivery, buffering (QoS 1), and presence (LWT).

## Topic scheme

```
<prefix>/<api_key>/in      # satellite → master  (master subscribes <prefix>/+/in)
<prefix>/<api_key>/out     # master → satellite
<prefix>/<api_key>/status  # retained LWT presence (online / offline)
```

The default prefix is `hivemind`.

The `api_key` segment is the satellite's HiveMind access key. It is unique per
client, so the master can look up the matching database record from the topic
as soon as the first frame arrives. Without the matching crypto key, the
payload ciphertext stays useless.

## Crypto

The MQTT payload carries the same encrypted `HiveMessage` frame that the
WebSocket transport sends. The broker sees only ciphertext. HiveMind's full
AES-GCM / RSA / PAKE handshake runs unchanged inside the payload bytes.

This transport adds no extra encryption layer.

## Wire format: text vs. binary frames

A `HiveMessage` frame is one of the following:

- A **text** frame: a UTF-8 JSON object. This is the default, non-binarized
  path: the plaintext handshake bootstrap, and AES-GCM ciphertext-JSON for
  everything after.
- A **binary** frame: a packed bitstring, used for the binarized / audio path.

WebSocket keeps this text/binary distinction natively. MQTT does not: every
MQTT payload is opaque `bytes`. On receive, the master inspects the payload: a
value that starts with `{` or `[` and is valid UTF-8 decodes back to `str` (so
`HiveMindClientConnection.decode` takes its JSON path). Anything else passes
through as `bytes` and decodes as a binary bitstring frame.

## Connection lifecycle

MQTT has no connection event the master can hook, so there is no `accept()`
loop. The master creates a logical per-satellite connection lazily, on the
first inbound frame on `<prefix>/<api_key>/in`:

1. The satellite announces presence (retained LWT plus `online`), subscribes
   to its `out` topic, and publishes its first frame to its `in` topic.
2. The master's single client receives the frame on `<prefix>/+/in`, looks up
   the `api_key` in the database, builds the `HiveMindClientConnection`, and
   (through `handle_new_client`) replies on the `out` topic with `HELLO` and a
   handshake request.
3. The satellite completes one HiveMind handshake. Both sides derive the same
   session key, and encrypted frames flow in both directions.

Because the master starts the handshake in step 2, the satellite must not
start its own handshake before that first round-trip. Doing so would race the
master's request and derive a mismatched key.

### Master self-presence

The master publishes its own presence to `<prefix>/<master_name>/status`
(`<master_name>` is the master's `NodeIdentity.name`). This topic also matches
the master's own `<prefix>/+/status` subscription, so the master receives its
own status echo. The master recognizes and ignores this self-echo, so it never
treats itself as a satellite peer.

## Authentication layers

1. **Broker-level**: MQTT `username` / `password` (config keys
   `broker_username` / `broker_password`), or a TLS client certificate (config
   keys `tls_certfile` / `tls_keyfile`). Configure the broker's ACL so each
   satellite can only publish to its own `<api_key>/in` topic and subscribe to
   its own `<api_key>/out` topic.
2. **HiveMind-level**: the HELLO / HANDSHAKE exchange embedded in the
   encrypted payload, the same as the WebSocket path. The `api_key` is the
   topic segment, so the master can look up the database record on first
   contact without a separate MQTT-layer credential handshake.

## QoS

| Traffic type | QoS | Rationale |
|---|---|---|
| Control frames (default) | 1 (at-least-once) | Delivery guarantee for messages. |
| Binary / audio frames | 0 (fire-and-forget) | Low latency; re-transmitting audio is worse than a gap. |

## Idle eviction

The master evicts satellites that send no message for `idle_timeout` seconds
(treats them as disconnected), and checks their LWT `<api_key>/status` topic
on eviction. Set `idle_timeout: 0` to disable eviction.

The default is 300 seconds.

## Authoring a transport plugin

See [hivemind-websocket-protocol: authoring a transport plugin](https://github.com/JarbasHiveMind/hivemind-websocket-protocol/blob/dev/docs/architecture.md#authoring-a-transport-plugin)
for the `NetworkProtocol` ABC and the entry-point registration pattern.

---
[Home](../README.md) · [Configuration →](configuration.md)
