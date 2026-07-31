# Configuration Reference

All settings go in the `hivemind-mqtt-plugin` block of
`~/.config/hivemind-core/server.json`.

| Key | Default | Description |
|---|---|---|
| `broker_host` | `localhost` | MQTT broker hostname or IP. |
| `broker_port` | `1883` | Broker port (use `8883` for TLS). |
| `broker_username` | none | MQTT broker username for the master. |
| `broker_password` | none | MQTT broker password for the master. |
| `tls` | `false` | Enable TLS. |
| `tls_ca_certs` | none | Path to CA bundle for broker TLS verification. |
| `tls_certfile` | none | Path to client certificate (mTLS). |
| `tls_keyfile` | none | Path to client key (mTLS). |
| `topic_prefix` | `hivemind` | Topic namespace prefix. |
| `qos` | `1` | Default MQTT QoS for control frames. |
| `idle_timeout` | `300` | Seconds of silence before evicting a peer (0 = off). |

## Basic (no TLS)

```json
{
  "network_protocol": {
    "module": "hivemind-mqtt-plugin",
    "hivemind-mqtt-plugin": {
      "broker_host": "192.168.1.100",
      "broker_port": 1883
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
      "broker_username": "hivemind-hub",
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

---
[← Architecture](architecture.md) · [Home](../README.md) · [Operations →](operations.md)
