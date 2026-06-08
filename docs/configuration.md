# Configuration Reference

All settings are passed in the `hivemind-mqtt-plugin` block of
`~/.config/hivemind-core/server.json`.

| Key | Default | Description |
|---|---|---|
| `broker_host` | `localhost` | MQTT broker hostname or IP. |
| `broker_port` | `1883` | Broker port (use `8883` for TLS). |
| `broker_username` | — | MQTT username for the hub. |
| `broker_password` | — | MQTT password for the hub. |
| `tls` | `false` | Enable TLS. |
| `tls_ca_certs` | — | Path to CA bundle for broker TLS verification. |
| `tls_certfile` | — | Path to client certificate (mTLS). |
| `tls_keyfile` | — | Path to client key (mTLS). |
| `node_id` | node identity name | Hub identifier used in topic paths. |
| `topic_prefix` | `hivemind` | Topic namespace prefix. |
| `qos` | `1` | Default MQTT QoS for control frames. |
| `hash_topics` | `false` | SHA-256-hash the `satellite_id` segment in topics. |
| `idle_timeout` | `300` | Seconds of silence before evicting a peer (0 = off). |

## Basic (no TLS)

```json
{
  "network_protocol": {
    "module": "hivemind-mqtt-plugin",
    "hivemind-mqtt-plugin": {
      "broker_host": "192.168.1.100",
      "broker_port": 1883,
      "node_id": "living-room-hub"
    }
  }
}
```

## With broker authentication

```json
{
  "network_protocol": {
    "module": "hivemind-mqtt-plugin",
    "hivemind-mqtt-plugin": {
      "broker_host": "192.168.1.100",
      "broker_port": 1883,
      "broker_username": "hivemind-node",
      "broker_password": "secret"
    }
  }
}
```

## With TLS

```json
{
  "network_protocol": {
    "module": "hivemind-mqtt-plugin",
    "hivemind-mqtt-plugin": {
      "broker_host": "mqtt.example.com",
      "broker_port": 8883,
      "tls": true,
      "tls_ca_certs": "/etc/ssl/certs/ca-certificates.crt"
    }
  }
}
```

## With mTLS (client certificate)

```json
{
  "network_protocol": {
    "module": "hivemind-mqtt-plugin",
    "hivemind-mqtt-plugin": {
      "broker_host": "mqtt.example.com",
      "broker_port": 8883,
      "tls": true,
      "tls_ca_certs": "/path/to/ca.crt",
      "tls_certfile": "/path/to/hub.crt",
      "tls_keyfile": "/path/to/hub.key"
    }
  }
}
```

## Hashed topics (privacy)

```json
{
  "network_protocol": {
    "module": "hivemind-mqtt-plugin",
    "hivemind-mqtt-plugin": {
      "broker_host": "shared-broker.example.com",
      "hash_topics": true
    }
  }
}
```
