# hivemind-mqtt-protocol

An MQTT broker-mediated network protocol plugin for [hivemind-core](https://github.com/JarbasHiveMind/hivemind-core).

Satellites connect to the same MQTT broker they already use for sensors (Home
Assistant, ESPHome, Tasmota, ESP32) and exchange encrypted `HiveMessage` frames
over that broker.  No bespoke inbound port or WebSocket stack is required on the
hub; both hub and satellites are broker clients.

## The broker-mediated model

```
satellite ──pub──▶  broker  ◀──sub── hub
hub       ──pub──▶  broker  ◀──sub── satellite
```

The hub runs ONE paho-mqtt client.  Logical per-satellite connections are
derived from the topic hierarchy.

### Topic scheme

```
<prefix>/<api_key>/in      # satellite → master  (master subscribes <prefix>/+/in)
<prefix>/<api_key>/out     # master → satellite
<prefix>/<api_key>/status  # retained LWT presence (online / offline)
```

Defaults: `prefix = hivemind`. Each satellite's HiveMind access key (`api_key`)
is its own topic segment — it is unique per client and identifies which DB
record to look up as soon as the first frame arrives.

## Crypto

The MQTT payload carries the **same encrypted HiveMessage frame** the WebSocket
transport sends.  The broker only ever sees ciphertext.  HiveMind's full
AES-GCM / RSA / PAKE handshake runs unchanged inside the payload.

## Authentication

Two independent layers:

1. **Broker-level** — MQTT `username` / `password`, or TLS client-cert.
   Configure the broker's ACL so each satellite may only publish to its own
   `<api_key>/in` topic and subscribe to its own `<api_key>/out` topic.

2. **HiveMind-level** — the HELLO / HANDSHAKE exchange embedded in the
   encrypted payload, identical to the WebSocket path.  The `api_key` IS the
   topic segment, so the master knows which DB record to look up as soon as the
   first frame arrives.

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
| `broker_username` | — | MQTT broker username for the master |
| `broker_password` | — | MQTT broker password for the master |
| `tls` | `false` | Enable TLS |
| `tls_ca_certs` | — | Path to CA bundle |
| `tls_certfile` | — | Path to client cert (mTLS) |
| `tls_keyfile` | — | Path to client key (mTLS) |
| `tls_insecure` | `false` | Skip broker certificate verification for trusted internal brokers |
| `topic_prefix` | `hivemind` | Topic namespace prefix |
| `qos` | `1` | Default MQTT QoS for control frames |
| `idle_timeout` | `300` | Seconds of silence before evicting a peer (0 = off) |
| `client_id` | — | Explicit broker client id for special deployments |
| `client_id_suffix` | `$HOSTNAME` | Replica-specific suffix hashed into the default broker client id |

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

The matching satellite client (publish to `<api_key>/in`, subscribe to
`<api_key>/out`, set the LWT on `<api_key>/status`) is a planned follow-up as a
transport option in `hivemind-bus-client` or a dedicated `hivemind-mqtt-client`.  An
ESPHome / Tasmota external-component example for ESP32 satellites is also
planned.

## Where it fits

```
hivemind-core
  └── hivemind-plugin-manager  (NetworkProtocolFactory loads plugins by entry-point)
        └── hivemind-mqtt-protocol  ← this repo
              └── paho-mqtt client connected to an external MQTT broker
```

The plugin registers under the `hivemind.network.protocol` entry-point group as
`hivemind-mqtt-plugin`.

## Docs

- [docs/architecture.md](docs/architecture.md) — topic scheme, crypto, QoS, idle eviction
- [docs/configuration.md](docs/configuration.md) — full configuration reference
- [docs/operations.md](docs/operations.md) — broker setup, TLS/mTLS, authoring a transport plugin

## Install

```bash
pip install hivemind-mqtt-protocol
```
