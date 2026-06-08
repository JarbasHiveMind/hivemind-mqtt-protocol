"""
HiveMind MQTT Network Protocol Plugin

Transports encrypted HiveMessage frames over an MQTT broker so that any
satellite — including embedded ESP32 devices — can ride an existing IoT/HA
MQTT bus rather than opening a dedicated WebSocket connection.

Topology
--------
The master node runs ONE paho-mqtt client that connects to an external broker.  There
is no accept() loop; logical per-satellite "connections" are derived from the
topic hierarchy:

    <prefix>/<node_id>/c2s/<satellite_id>     satellite → master (master subscribes …/c2s/+)
    <prefix>/<node_id>/s2c/<satellite_id>     master → satellite
    <prefix>/<node_id>/status/<satellite_id>  retained LWT presence (online/offline)

Crypto
------
The MQTT payload IS the same encrypted HiveMessage frame that the WebSocket
transport sends.  The broker only ever sees ciphertext.  No extra encryption
layer is added here; hivemind-core's AES-GCM / RSA / PAKE handshake runs
unchanged inside the payload bytes.

Auth
----
Two layers, consistent with the design doc:
  1. Broker-level: MQTT username/password or TLS client-cert (config keys
     ``broker_username`` / ``broker_password`` / ``tls`` / ``cert``).
  2. HiveMind-level: the HELLO/HANDSHAKE in-payload exchange, identical to the
     WebSocket path.  The access key is passed as the MQTT username (the
     satellite MUST set username=<access_key>).
"""

import dataclasses
import hashlib
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt
from ovos_bus_client.session import Session
from ovos_utils.log import LOG
from poorman_handshake import PasswordHandShake

from hivemind_core.protocol import (
    HiveMindClientConnection,
    HiveMindListenerProtocol,
    HiveMindNodeType,
)
from hivemind_plugin_manager.protocols import ClientCallbacks, NetworkProtocol

_ONLINE = "online"
_OFFLINE = "offline"

# Seconds of silence from a peer before it is considered idle-disconnected.
# Set to 0 to disable the sweep.
_DEFAULT_IDLE_TIMEOUT = 300


@dataclass
class HiveMindMqttProtocol(NetworkProtocol):
    """MQTT broker-mediated network protocol for hivemind-core.

    Config keys (all optional, with defaults shown):
        broker_host        (str)  "localhost"
        broker_port        (int)  1883
        broker_username    (str)  None  — MQTT username (not the HiveMind key)
        broker_password    (str)  None
        tls                (bool) False — enable TLS
        tls_ca_certs       (str)  None  — path to CA bundle
        tls_certfile       (str)  None  — path to client cert (mTLS)
        tls_keyfile        (str)  None  — path to client key  (mTLS)
        node_id             (str)  NodeIdentity.name or "hivemind-node"
        topic_prefix       (str)  "hivemind"
        qos                (int)  1
        hash_topics        (bool) False — hash satellite_id in topics for privacy
        idle_timeout       (int)  300   — seconds; 0 disables
    """

    config: Dict[str, Any] = field(default_factory=dict)
    hm_protocol: Optional[HiveMindListenerProtocol] = None
    callbacks: ClientCallbacks = field(default_factory=ClientCallbacks)

    # Internal state — not part of the dataclass constructor signature.
    _peers: Dict[str, HiveMindClientConnection] = field(default_factory=dict, init=False, repr=False)
    _last_seen: Dict[str, float] = field(default_factory=dict, init=False, repr=False)
    _mqtt: Optional[mqtt.Client] = field(default=None, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _cfg(self, key: str, default: Any = None) -> Any:
        return self.config.get(key, default)

    def _node_id(self) -> str:
        return str(self._cfg("node_id") or self.identity.name or "hivemind-node")

    def _prefix(self) -> str:
        return str(self._cfg("topic_prefix") or "hivemind")

    def _qos(self, is_bin: bool = False) -> int:
        """QoS 0 for binary/audio (latency-sensitive); QoS 1 for control."""
        if is_bin:
            return 0
        v = self._cfg("qos")
        return int(v) if v is not None else 1

    def _sat_topic_seg(self, satellite_id: str) -> str:
        """Return the topic segment for satellite_id, optionally hashed."""
        if self._cfg("hash_topics", False):
            return hashlib.sha256(satellite_id.encode()).hexdigest()[:16]
        return satellite_id

    # topic builders ---------------------------------------------------

    def c2s_topic(self, satellite_id: str) -> str:
        """Inbound topic: satellite → hub."""
        return f"{self._prefix()}/{self._node_id()}/c2s/{self._sat_topic_seg(satellite_id)}"

    def s2c_topic(self, satellite_id: str) -> str:
        """Outbound topic: master → satellite."""
        return f"{self._prefix()}/{self._node_id()}/s2c/{self._sat_topic_seg(satellite_id)}"

    def status_topic(self, satellite_id: str) -> str:
        """Retained LWT presence topic."""
        return f"{self._prefix()}/{self._node_id()}/status/{self._sat_topic_seg(satellite_id)}"

    def c2s_wildcard(self) -> str:
        """Wildcard subscription for all satellites' c2s traffic."""
        return f"{self._prefix()}/{self._node_id()}/c2s/+"

    def status_wildcard(self) -> str:
        """Wildcard subscription for all satellites' status topics."""
        return f"{self._prefix()}/{self._node_id()}/status/+"

    # satellite_id extraction ------------------------------------------

    @staticmethod
    def _satellite_id_from_topic(topic: str) -> Optional[str]:
        """Parse the satellite_id segment from a c2s or status topic."""
        parts = topic.split("/")
        # expected: <prefix>/<node_id>/<segment>/<satellite_id>
        if len(parts) >= 4:
            return parts[-1]
        return None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def _build_client_connection(
        self, satellite_id: str, key: str, useragent: str
    ) -> Optional[HiveMindClientConnection]:
        """Create and register a HiveMindClientConnection for a new peer.

        Mirrors HiveMindTornadoWebSocket.open() exactly: look up DB client by
        key, populate ACL fields, then call handle_new_client.

        Returns the new connection or None if auth fails.
        """
        prefix = self._prefix()
        node_id = self._node_id()
        mqttclient = self._mqtt
        qos_fn = self._qos
        s2c = self.s2c_topic(satellite_id)
        status = self.status_topic(satellite_id)

        def do_send(payload: Any, is_bin: bool = False) -> None:
            if isinstance(payload, str):
                payload = payload.encode()
            mqttclient.publish(s2c, payload, qos=qos_fn(is_bin))

        def do_disconnect() -> None:
            # Publish an offline tombstone then clean up.
            mqttclient.publish(status, _OFFLINE, qos=1, retain=True)
            with self._lock:
                self._peers.pop(satellite_id, None)
                self._last_seen.pop(satellite_id, None)

        conn = HiveMindClientConnection(
            key=key,
            disconnect=do_disconnect,
            send_msg=do_send,
            sess=Session(session_id="default"),
            name=useragent,
            hm_protocol=self.hm_protocol,
        )

        self.hm_protocol.db.sync()
        user = self.hm_protocol.db.get_client_by_api_key(key)

        if not user:
            LOG.error(f"[MQTT] Client {satellite_id!r} provided invalid API key")
            self.hm_protocol.handle_invalid_key_connected(conn)
            return None

        conn.name = f"{useragent}::{user.client_id}::{user.name}"
        conn.crypto_key = user.crypto_key
        conn.skill_blacklist = user.skill_blacklist or []
        conn.intent_blacklist = user.intent_blacklist or []
        conn.allowed_types = user.allowed_types
        conn.can_broadcast = user.can_broadcast
        conn.can_propagate = user.can_propagate
        conn.can_escalate = user.can_escalate
        conn.is_admin = user.is_admin
        if user.password:
            conn.pswd_handshake = PasswordHandShake(user.password)

        conn.node_type = HiveMindNodeType.NODE

        if (
            not conn.crypto_key
            and not self.hm_protocol.handshake_enabled
            and self.hm_protocol.require_crypto
        ):
            LOG.error(
                "[MQTT] No pre-shared crypto key and handshake disabled, "
                "but require_crypto=True"
            )
            self.hm_protocol.handle_invalid_protocol_version(conn)
            return None

        with self._lock:
            self._peers[satellite_id] = conn
            self._last_seen[satellite_id] = time.monotonic()

        self.hm_protocol.handle_new_client(conn)
        LOG.info(f"[MQTT] New logical connection: {satellite_id!r} → {conn.name!r}")
        return conn

    def _disconnect_peer(self, satellite_id: str) -> None:
        """Tear down the logical connection for satellite_id (LWT or timeout)."""
        with self._lock:
            conn = self._peers.pop(satellite_id, None)
            self._last_seen.pop(satellite_id, None)
        if conn is not None:
            LOG.info(f"[MQTT] Disconnecting peer {satellite_id!r}")
            self.hm_protocol.handle_client_disconnected(conn)

    # ------------------------------------------------------------------
    # paho callbacks
    # ------------------------------------------------------------------

    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: Any, rc: int) -> None:
        if rc != 0:
            LOG.error(f"[MQTT] Broker connection failed, rc={rc}")
            return
        LOG.info("[MQTT] Connected to broker")
        client.subscribe(self.c2s_wildcard(), qos=self._qos())
        client.subscribe(self.status_wildcard(), qos=1)
        LOG.debug(f"[MQTT] Subscribed to {self.c2s_wildcard()} and {self.status_wildcard()}")

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        topic: str = msg.topic
        payload: bytes = msg.payload

        # Determine segment type (c2s or status).
        parts = topic.split("/")
        if len(parts) < 4:
            LOG.warning(f"[MQTT] Unexpected topic shape: {topic!r}")
            return

        segment = parts[-2]  # "c2s" or "status"
        satellite_id = parts[-1]

        if segment == "status":
            status_val = payload.decode(errors="replace").strip()
            if status_val == _OFFLINE:
                LOG.info(f"[MQTT] LWT offline for {satellite_id!r}")
                self._disconnect_peer(satellite_id)
            return

        if segment != "c2s":
            return

        # Inbound data frame.
        with self._lock:
            conn = self._peers.get(satellite_id)
            if conn is not None:
                self._last_seen[satellite_id] = time.monotonic()

        if conn is None:
            # Unknown satellite — must authenticate via the MQTT username.
            # The hub's MQTT client receives the *message*; MQTT does not expose
            # the sender's credentials directly here.  We use the satellite_id
            # itself as a key placeholder so the payload HELLO frame can carry
            # the real access key through the normal HiveMind handshake.
            # For brokers that enforce ACLs, the MQTT username IS the HiveMind
            # access key; we record it from the first-frame satellite_id.
            # Treat satellite_id as the key for the initial lookup so that the
            # broker-ACL username == HiveMind access key pattern works.
            useragent = satellite_id
            key = satellite_id  # satellite_id == MQTT username == HiveMind key
            conn = self._build_client_connection(satellite_id, key, useragent)
            if conn is None:
                # Auth failed; drop frame.
                return

        try:
            message = conn.decode(payload)
        except Exception as e:
            LOG.warning(f"[MQTT] Failed to decode frame from {satellite_id!r}: {e}")
            return

        self.hm_protocol.handle_message(message, conn)

    def _on_disconnect(self, client: mqtt.Client, userdata: Any, rc: int) -> None:
        if rc != 0:
            LOG.warning(f"[MQTT] Unexpected broker disconnect, rc={rc}")

    # ------------------------------------------------------------------
    # Idle-timeout sweep
    # ------------------------------------------------------------------

    def _idle_sweep(self, idle_timeout: float) -> None:
        """Background thread: evict peers that have been silent too long."""
        while True:
            time.sleep(max(idle_timeout / 4, 30))
            now = time.monotonic()
            with self._lock:
                stale = [
                    sid
                    for sid, ts in list(self._last_seen.items())
                    if (now - ts) > idle_timeout
                ]
            for sid in stale:
                LOG.info(f"[MQTT] Idle timeout for peer {sid!r}")
                self._disconnect_peer(sid)

    # ------------------------------------------------------------------
    # run() — the blocking entry point called by hivemind-core
    # ------------------------------------------------------------------

    def run(self) -> None:
        LOG.debug(f"[MQTT] protocol config: {self.config}")

        broker_host: str = str(self._cfg("broker_host") or "localhost")
        broker_port: int = int(self._cfg("broker_port") or 1883)
        node_id = self._node_id()

        client_id = f"hivemind-node-{node_id}"
        self._mqtt = mqtt.Client(client_id=client_id)

        # Broker-level auth.
        username: Optional[str] = self._cfg("broker_username")
        password: Optional[str] = self._cfg("broker_password")
        if username:
            self._mqtt.username_pw_set(username, password)

        # TLS.
        if self._cfg("tls", False):
            self._mqtt.tls_set(
                ca_certs=self._cfg("tls_ca_certs"),
                certfile=self._cfg("tls_certfile"),
                keyfile=self._cfg("tls_keyfile"),
            )

        # Hub's own LWT — signals the hub going offline to any listener.
        hub_status_topic = f"{self._prefix()}/{node_id}/status/hub"
        self._mqtt.will_set(hub_status_topic, _OFFLINE, qos=1, retain=True)

        self._mqtt.on_connect = self._on_connect
        self._mqtt.on_message = self._on_message
        self._mqtt.on_disconnect = self._on_disconnect

        self._mqtt.connect(broker_host, broker_port, keepalive=60)

        # Publish hub online status once connected (done in on_connect would
        # need the client reference; simpler to publish after connect()).
        self._mqtt.publish(hub_status_topic, _ONLINE, qos=1, retain=True)

        # Start idle-timeout sweep if configured.
        idle_timeout = float(self._cfg("idle_timeout") or _DEFAULT_IDLE_TIMEOUT)
        if idle_timeout > 0:
            t = threading.Thread(
                target=self._idle_sweep,
                args=(idle_timeout,),
                daemon=True,
                name="mqtt-idle-sweep",
            )
            t.start()

        LOG.info(f"[MQTT] listener started — broker={broker_host}:{broker_port}, node_id={node_id!r}")
        self._mqtt.loop_forever()  # blocking — mirrors tornado ioloop.start()
