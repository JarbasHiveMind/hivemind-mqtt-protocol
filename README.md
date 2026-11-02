# hivemind-mqtt-protocol

An MQTT broker-mediated network protocol plugin for
[hivemind-core](https://github.com/JarbasHiveMind/hivemind-core).

Satellites connect to the same MQTT broker they already use for sensors (Home
Assistant, ESPHome, Tasmota, ESP32) and exchange encrypted `HiveMessage` frames
over that broker. The hub needs no inbound port or WebSocket stack. Both the
hub and the satellites are broker clients.

## The broker-mediated model

```
satellite ──pub──▶  broker  ◀──sub── hub
hub       ──pub──▶  broker  ◀──sub── satellite
```

The hub runs one paho-mqtt client. Logical per-satellite connections come from
the topic hierarchy.

### Topic scheme

```
<prefix>/<api_key>/in      # satellite → master, legacy standalone layout
<prefix>/<api_key>/out     # master → satellite, legacy standalone layout
<prefix>/<api_key>/status  # retained LWT presence, legacy standalone layout

<prefix>/<topic_namespace>/c2s/<api_key>     # satellite → master, namespaced layout
<prefix>/<topic_namespace>/s2c/<api_key>     # master → satellite, namespaced layout
<prefix>/<topic_namespace>/status/<api_key>  # retained LWT presence, namespaced layout
```

The default prefix is `hivemind`. Each satellite's HiveMind access key
(`api_key`) is its own topic segment. The key is unique per client, so the
master can look up the matching database record as soon as the first frame
arrives. When `topic_namespace` is set, the topics are under that namespace. A broker
ACL can then grant one listener its own topic tree.

The master also publishes its own retained presence, on the status topic of
the same layout, with `<master_name>` (the master's `NodeIdentity.name`) in
place of the api key:

```
<prefix>/<master_name>/status           # legacy standalone layout
<prefix>/<topic_namespace>/status/<master_name>  # namespaced layout
```

The master subscribes to `<prefix>/+/status`, or to `<prefix>/<topic_namespace>/status/+`
when `topic_namespace` is set. Either subscription matches its own presence topic, so
the master receives its own status echo. The master ignores this self-echo, so
it never treats itself as a satellite peer. A broker ACL for a namespaced listener
must let the master publish to `<prefix>/<topic_namespace>/status/<master_name>`.

## Crypto

The MQTT payload carries the same encrypted `HiveMessage` frame that the
WebSocket transport sends. The broker sees only ciphertext. HiveMind's full
AES-GCM / RSA / PAKE handshake runs unchanged inside the payload.

## Authentication

The plugin uses two independent layers:

1. **Broker-level**: MQTT `username` / `password`, or a TLS client
   certificate. Configure the broker's ACL so each satellite can only publish
   to its own inbound topic and subscribe to its own outbound topic:
   `<prefix>/<api_key>/in` and `<prefix>/<api_key>/out`, or with `topic_namespace` set,
   `<prefix>/<topic_namespace>/c2s/<api_key>` and `<prefix>/<topic_namespace>/s2c/<api_key>`.
   It also publishes its own presence topic, `<prefix>/<api_key>/status` or
   `<prefix>/<topic_namespace>/status/<api_key>`.
2. **HiveMind-level**: the HELLO / HANDSHAKE exchange embedded in the
   encrypted payload, the same as the WebSocket path. The `api_key` is the
   topic segment, so the master knows which database record to check as soon
   as the first frame arrives.

## QoS

| Traffic type | QoS |
|---|---|
| Control frames (default) | 1 (at-least-once) |
| Binary / audio frames | 0 (fire-and-forget, low latency) |

## Configuration keys

| Key | Default | Description |
|---|---|---|
| `broker_host` | `localhost` | MQTT broker hostname or IP |
| `broker_port` | `1883` | Broker port (8883 for TLS) |
| `broker_username` | none | MQTT broker username for the master |
| `broker_password` | none | MQTT broker password for the master |
| `tls` | `false` | Enable TLS |
| `tls_ca_certs` | none | Path to CA bundle |
| `tls_certfile` | none | Path to client cert (mTLS) |
| `tls_keyfile` | none | Path to client key (mTLS) |
| `topic_prefix` | `hivemind` | Topic namespace prefix |
| `topic_namespace` | — | Optional topic namespace, so a broker ACL can grant one listener its own topic tree |
| `qos` | `1` | Default MQTT QoS for control frames |
| `idle_timeout` | `300` | Seconds of silence before evicting a peer (0 = off) |
| `client_id` | — | Explicit broker client id for special deployments |
| `client_id_suffix` | `$HOSTNAME`, else the machine hostname | Replica-specific suffix hashed into the default broker client id |

### When to use `topic_namespace`

Leave it unset for a broker that serves one listener: topics are `<prefix>/<api_key>/in|out` and every satellite shares one flat tree.

Set it when several listeners share one broker and each must only see its own satellites, or when the broker's ACL should grant a listener one bounded topic tree. Satellites publish to `<prefix>/<namespace>/c2s/<api_key>`, the listener publishes to `<prefix>/<namespace>/s2c/<api_key>`, and presence is retained on `<prefix>/<namespace>/status/<key>`. The value must be a single literal topic level: `+`, `#`, `/` inside it and control characters are rejected when the listener is built, so a namespace can never widen its own subscription.

A minimal Mosquitto ACL for a listener named `ns-1` with the default prefix:

```
user listener-1
topic readwrite hivemind/ns-1/#
```

Two replicas of one listener need distinct broker client ids or the broker disconnects one of them; set `client_id_suffix` per replica (it defaults to the pod or machine hostname) and the suffix is hashed into the default client id.

## Usage

```python
from hivemind_plugin_manager import NetworkProtocolFactory

server = NetworkProtocolFactory.create(
    "hivemind-mqtt-plugin",
    config={
        "broker_host": "192.168.1.100",
        "broker_port": 1883,
    },
)
server.run()   # blocks
```

## Satellite side

A satellite is any MQTT client that does the following. Topics are shown for
the legacy layout, with the namespaced layout (`topic_namespace` set) in brackets.

1. Sets a retained LWT `offline` on `<prefix>/<api_key>/status`
   (`<prefix>/<topic_namespace>/status/<api_key>`), connects, and publishes a retained
   `online` there.
2. Subscribes to `<prefix>/<api_key>/out` (`<prefix>/<topic_namespace>/s2c/<api_key>`).
3. Publishes its first HiveMind frame to `<prefix>/<api_key>/in`
   (`<prefix>/<topic_namespace>/c2s/<api_key>`). This makes
   the master create the logical connection and reply, over the `out` topic,
   with its `HELLO` and handshake request. The satellite then runs the normal
   HiveMind handshake and exchanges encrypted frames.

A reference satellite that drives a real `HiveMindSlaveProtocol` over MQTT
lives in the end-to-end test harness (`tests/e2e/mqtt_satellite.py`). See
[Testing](#testing). Planned follow-ups include a dedicated transport option
in [hivemind-bus-client](https://github.com/JarbasHiveMind/hivemind-bus-client)
(or a dedicated `hivemind-mqtt-client`) and an ESPHome / Tasmota
external-component example for ESP32 satellites.

## Testing

```bash
pip install -e .[e2e]   # installs hivescope + the in-process harness
pytest tests/           # unit + end-to-end
```

- `tests/test_mqtt_protocol.py`: unit tests for topic, routing, and lifecycle
  logic, with a mocked paho client and `hm_protocol`.
- `tests/e2e/`: end-to-end tests that run the real `HiveMindMqttProtocol`
  master and a real `HiveMindSlaveProtocol` satellite. They exercise the full
  loop: handshake, encrypted BUS round-trip, and LWT presence. The MQTT broker
  is an in-process double (`tests/e2e/broker.py`), so the tests need no
  external mosquitto instance, sockets, or network.

## Where it fits

```
hivemind-core
  └── hivemind-plugin-manager  (NetworkProtocolFactory loads plugins by entry-point)
        └── hivemind-mqtt-protocol  ← this repo
              └── paho-mqtt client connected to an external MQTT broker
```

The plugin registers under the `hivemind.network.protocol` entry-point group
as `hivemind-mqtt-plugin`.

## Related projects

- [JarbasHiveMind/hivemind-core](https://github.com/JarbasHiveMind/hivemind-core): the hub this plugin serves.
- [JarbasHiveMind/hivemind-plugin-manager](https://github.com/JarbasHiveMind/hivemind-plugin-manager): loads this plugin by entry-point.
- [JarbasHiveMind/hivemind-websocket-protocol](https://github.com/JarbasHiveMind/hivemind-websocket-protocol): the default WebSocket transport, and the source for the transport-plugin authoring pattern.
- [JarbasHiveMind/hivemind-bus-client](https://github.com/JarbasHiveMind/hivemind-bus-client): the satellite-side client library.

## Docs

- [docs/architecture.md](docs/architecture.md): topic scheme, crypto, QoS, idle eviction
- [docs/configuration.md](docs/configuration.md): full configuration reference
- [docs/operations.md](docs/operations.md): broker setup, TLS/mTLS, authoring a transport plugin

## Install

```bash
pip install hivemind-mqtt-protocol
```
