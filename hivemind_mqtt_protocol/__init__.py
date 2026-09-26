"""
Implements the MQTT binding of HIVEMIND-TRANSPORT-1 §5. A binding MUST define
a topic scheme comprising a client-to-server topic, a server-to-client topic,
and a retained status topic per satellite. Disconnect detection MUST use the
broker's last-will mechanism.

HiveMind MQTT Network Protocol Plugin
====================================

Transports encrypted HiveMessage frames over an MQTT broker so that any
satellite — including embedded ESP32 devices — can ride an existing IoT/HA
MQTT bus rather than opening a dedicated WebSocket connection.

Topic layout
------------
Each satellite's api_key IS its topic identifier — it is unique per client,
and without the matching password/crypto key the payload ciphertext is useless.

    <prefix>/<api_key>/in      satellite → master  (master subscribes <prefix>/+/in)
    <prefix>/<api_key>/out     master → satellite
    <prefix>/<api_key>/status  retained LWT presence (online/offline)

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
  2. HiveMind-level: the HELLO/HANDSHAKE in-payload exchange, identical to
     the WebSocket path.  The api_key IS the MQTT topic segment, so the
     master knows which DB record to look up as soon as the first frame
     arrives.  No separate credential handshake is needed at the MQTT layer.
"""

import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import paho.mqtt.client as mqtt
from ovos_bus_client.session import Session
from ovos_utils.log import LOG
from poorman_handshake import PasswordHandShake

try:
    from hivemind_core.config import runtime_password_min_bits
except ImportError:  # released hivemind-core without the helper
    import os

    def runtime_password_min_bits():
        return 0.0 if os.environ.get("HIVEMIND_DISABLE_PASSWORD_STRENGTH_CHECK", "").strip().lower() in ("1", "true", "yes", "on") else 40.0

from hivemind_core.protocol import (
    HiveMindClientConnection,
    HiveMindListenerProtocol,
    HiveMindNodeType,
)
from hivemind_plugin_manager.protocols import ClientCallbacks, NetworkProtocol

_ONLINE = "online"
_OFFLINE = "offline"

_DEFAULT_IDLE_TIMEOUT = 300


#: Config keys whose values are credentials. They are replaced before the
#: config is logged, and the config is kept out of the dataclass repr, so a
#: usable broker password never reaches a log line.
_SECRET_CONFIG_KEYS = frozenset({"broker_password"})


def _redacted_config(config: dict[str, Any]) -> dict[str, Any]:
    """A copy of ``config`` that is safe to log."""
    return {
        key: ("***" if key in _SECRET_CONFIG_KEYS and value else value)
        for key, value in config.items()
    }


def _key_id(api_key: str) -> str:
    """A stable, non-reversible label for one access key, for logs.

    The access key IS the satellite's credential: it is what
    ``get_client_by_api_key`` is called with, so a log line that carries it
    hands an operator's log reader a working identity. The reference
    WebSocket binding refuses to log it in any recoverable form for that
    reason, keeping only a length.

    A length is not enough here. These logs exist to follow one peer across
    connect, decode failure, idle timeout and disconnect, and every key has
    the same length. So the label is the first 8 hex characters of the
    SHA-256 of the key: the same key always gives the same label, and the key
    cannot be read back out of it. Truncating the key itself would have done
    neither.

    Two limits, stated rather than implied.

    The label is 32 bits. Two labels collide with even chance at about 77000
    keys, so "two keys rarely collide" is true for a house and not for a large
    fleet; at that size two satellites can share a label, which is the one
    thing the helper exists to prevent. Take more characters if that day comes.

    The hash is UNSALTED, so against a GUESSABLE key the label is an offline
    oracle: hash the candidates and compare 8 characters. That is acceptable
    here and the reason is the key, not the label. ``hivemind-core`` mints an
    access key as ``os.urandom(16).hex()``, 128 bits, against which the label
    tells an attacker nothing. A salt would not close the gap either: the
    node's public key is public, so salting with it defeats cross-node linkage
    and not a dictionary attack, and a secret salt would break the label's
    stability across a rotation. The real exposure is that an operator-supplied
    access key gets NO strength check while a password does, which is core's to
    answer and is filed there.
    """
    return "key#" + hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:8]


@dataclass
class HiveMindMqttProtocol(NetworkProtocol):
    """MQTT broker-mediated network protocol for hivemind-core.

    Config keys (all optional, with defaults shown):
        broker_host        (str)  "localhost"
        broker_port        (int)  1883
        broker_username    (str)  None  — MQTT broker username (not the HiveMind key)
        broker_password    (str)  None  — MQTT broker password
        tls                (bool) False — enable TLS
        tls_ca_certs       (str)  None  — path to CA bundle
        tls_certfile       (str)  None  — path to client cert (mTLS)
        tls_keyfile        (str)  None  — path to client key  (mTLS)
        topic_prefix       (str)  "hivemind"
        qos                (int)  1
        idle_timeout       (int)  300   — seconds of silence before eviction; 0 disables
    """

    config: Dict[str, Any] = field(default_factory=dict, repr=False)
    hm_protocol: Optional[HiveMindListenerProtocol] = None
    callbacks: ClientCallbacks = field(default_factory=ClientCallbacks)

    _peers: Dict[str, HiveMindClientConnection] = field(default_factory=dict, init=False, repr=False)
    _last_seen: Dict[str, float] = field(default_factory=dict, init=False, repr=False)
    _mqtt: Optional[mqtt.Client] = field(default=None, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _cfg(self, key: str, default: Any = None) -> Any:
        return self.config.get(key, default)

    def _prefix(self) -> str:
        return str(self._cfg("topic_prefix") or "hivemind")

    def _qos(self, is_bin: bool = False) -> int:
        if is_bin:
            return 0
        v = self._cfg("qos")
        return int(v) if v is not None else 1

    # topic builders ---------------------------------------------------

    def in_topic(self, api_key: str) -> str:
        """Inbound topic: satellite → master."""
        return f"{self._prefix()}/{api_key}/in"

    def out_topic(self, api_key: str) -> str:
        """Outbound topic: master → satellite."""
        return f"{self._prefix()}/{api_key}/out"

    def status_topic(self, api_key: str) -> str:
        """Retained LWT presence topic."""
        return f"{self._prefix()}/{api_key}/status"

    def in_wildcard(self) -> str:
        return f"{self._prefix()}/+/in"

    def status_wildcard(self) -> str:
        return f"{self._prefix()}/+/status"

    def master_status_topic(self) -> str:
        return f"{self._prefix()}/{self.identity.name or 'master'}/status"

    # api_key extraction -----------------------------------------------

    @staticmethod
    def _api_key_from_topic(topic: str) -> Optional[str]:
        """Extract the api_key segment from <prefix>/<api_key>/<direction>."""
        parts = topic.split("/")
        if len(parts) >= 3:
            return parts[-2]
        return None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def _build_client_connection(self, api_key: str) -> Optional[HiveMindClientConnection]:
        mqttclient = self._mqtt
        qos_fn = self._qos
        out = self.out_topic(api_key)
        status = self.status_topic(api_key)

        def do_send(payload: Any, is_bin: bool = False) -> None:
            if isinstance(payload, str):
                payload = payload.encode()
            mqttclient.publish(out, payload, qos=qos_fn(is_bin))

        def do_disconnect(code: int = 1000, reason: str = "") -> None:
            LOG.debug(f"[MQTT] disconnecting {_key_id(api_key)} (code={code}, reason={reason})")
            mqttclient.publish(status, _OFFLINE, qos=1, retain=True)
            # the same teardown the broker's last-will path runs, so a
            # session core closes leaves core's client table too
            self._disconnect_peer(api_key)

        conn = HiveMindClientConnection(
            key=api_key,
            disconnect=do_disconnect,
            send_msg=do_send,
            sess=Session(session_id="default"),
            # NOT the access key. ``name`` feeds ``HiveMindClientConnection.peer``,
            # which core stamps into ``context["source"]`` of every injected
            # message, so a credential here reaches the bus and not just a log.
            # This value is used before the DB row resolves (the invalid-key
            # path logs and hands this connection to core), so it has to be a
            # non-secret label rather than a name taken from the row.
            name=_key_id(api_key),
            hm_protocol=self.hm_protocol,
        )

        self.hm_protocol.db.sync()
        user = self.hm_protocol.db.get_client_by_api_key(api_key)

        if not user:
            LOG.error(f"[MQTT] Invalid api_key in topic: {_key_id(api_key)}")
            self.hm_protocol.handle_invalid_key_connected(conn)
            return None

        # The reference bindings compose this as
        # f"{useragent}::{client_id}::{name}" (hivemind-websocket-protocol,
        # hivemind-http-protocol) and never put the credential in it. MQTT has
        # no useragent, so the transport takes that slot. The access key MUST
        # NOT appear: ``peer`` is built from this, and core stamps ``peer`` into
        # ``context["source"]``, which every bus observer reads.
        conn.name = f"mqtt::{user.client_id}::{user.name}"
        conn.allowed_types = user.allowed_types
        conn.can_broadcast = user.can_broadcast
        conn.can_propagate = user.can_propagate
        conn.can_escalate = user.can_escalate
        conn.is_admin = user.is_admin
        if user.password:
            conn.pswd_handshake = PasswordHandShake(user.password, min_bits=runtime_password_min_bits())

        conn.node_type = HiveMindNodeType.NODE

        with self._lock:
            self._peers[api_key] = conn
            self._last_seen[api_key] = time.monotonic()

        self.hm_protocol.handle_new_client(conn)
        # The client id and the client name say which satellite this is
        # without saying how to be it, and the label follows it across
        # connect, decode failure, idle timeout and disconnect.
        LOG.info(f"[MQTT] New connection: {user.client_id}/{user.name!r} "
                 f"({_key_id(api_key)})")
        return conn

    def _disconnect_peer(self, api_key: str) -> None:
        with self._lock:
            conn = self._peers.pop(api_key, None)
            self._last_seen.pop(api_key, None)
        if conn is not None:
            LOG.info(f"[MQTT] Disconnecting peer {_key_id(api_key)}")
            self.hm_protocol.handle_client_disconnected(conn)

    # ------------------------------------------------------------------
    # paho callbacks
    # ------------------------------------------------------------------

    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: Any, rc: int) -> None:
        if rc != 0:
            LOG.error(f"[MQTT] Broker connection failed, rc={rc}")
            return
        LOG.info("[MQTT] Connected to broker")
        client.subscribe(self.in_wildcard(), qos=self._qos())
        client.subscribe(self.status_wildcard(), qos=1)

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        topic: str = msg.topic
        payload: bytes = msg.payload

        parts = topic.split("/")
        if len(parts) < 3:
            LOG.warning(f"[MQTT] Unexpected topic shape: {topic!r}")
            return

        direction = parts[-1]   # "in", "out", or "status"
        api_key = parts[-2]

        if direction == "status":
            # The master publishes its own presence to
            # <prefix>/<master_name>/status, which also matches its
            # <prefix>/+/status subscription. Ignore that self-echo so the
            # master never tries to treat itself as a satellite peer.
            if api_key == (self.identity.name or "master"):
                return
            status_val = payload.decode(errors="replace").strip()
            if status_val == _OFFLINE:
                LOG.info(f"[MQTT] LWT offline for {_key_id(api_key)}")
                self._disconnect_peer(api_key)
            return

        if direction != "in":
            return

        with self._lock:
            conn = self._peers.get(api_key)
            if conn is not None:
                self._last_seen[api_key] = time.monotonic()

        if conn is None:
            conn = self._build_client_connection(api_key)
            if conn is None:
                return

        try:
            if conn.noise_transport is None:
                payload = self._coerce_payload(payload)
            message = conn.decode(payload)
        except Exception as e:
            LOG.warning(f"[MQTT] Failed to decode frame from {_key_id(api_key)}: {e}")
            return

        if message is None:
            # one chunk of a multi-frame Noise message; the transport is
            # still buffering and hands back the message with the last chunk
            return

        self.hm_protocol.handle_message(message, conn)

    @staticmethod
    def _coerce_payload(payload: bytes):
        """Return the payload typed the way ``HiveMindClientConnection.decode``
        expects.

        MQTT delivers every payload as ``bytes``, but ``decode`` treats any
        ``bytes`` value as a binary *bitstring* frame and any ``str`` value as
        a JSON frame (plaintext handshake or AES-GCM ciphertext-JSON). Text
        HiveMessage frames — the default, non-binarized path used by the
        handshake and ordinary BUS messages — are valid UTF-8 JSON objects, so
        decode them back to ``str``; anything that is not valid UTF-8 JSON is a
        genuine binary frame and is passed through as ``bytes``.
        """
        if not isinstance(payload, (bytes, bytearray)):
            return payload
        stripped = payload.lstrip()
        if stripped[:1] in (b"{", b"["):
            try:
                return payload.decode("utf-8")
            except UnicodeDecodeError:
                return bytes(payload)
        return bytes(payload)

    def _on_disconnect(self, client: mqtt.Client, userdata: Any, rc: int) -> None:
        if rc != 0:
            LOG.warning(f"[MQTT] Unexpected broker disconnect, rc={rc}")

    # ------------------------------------------------------------------
    # Idle-timeout sweep
    # ------------------------------------------------------------------

    def _idle_sweep(self, idle_timeout: float) -> None:
        while True:
            time.sleep(max(idle_timeout / 4, 30))
            now = time.monotonic()
            with self._lock:
                stale = [k for k, ts in list(self._last_seen.items()) if (now - ts) > idle_timeout]
            for key in stale:
                LOG.info(f"[MQTT] Idle timeout for peer {_key_id(key)}")
                self._disconnect_peer(key)

    # ------------------------------------------------------------------
    # run()
    # ------------------------------------------------------------------

    def run(self) -> None:
        LOG.debug("[MQTT] protocol config: %s", _redacted_config(self.config))

        broker_host: str = str(self._cfg("broker_host") or "localhost")
        broker_port: int = int(self._cfg("broker_port") or 1883)

        # one broker session per replica: a shared client id makes the broker
        # treat every replica as the same client reconnecting, and they kick
        # each other off
        self._mqtt = mqtt.Client(
            client_id=f"hivemind-{self.identity.name or 'master'}-{uuid.uuid4().hex[:12]}")

        username: Optional[str] = self._cfg("broker_username")
        password: Optional[str] = self._cfg("broker_password")
        if username:
            self._mqtt.username_pw_set(username, password)

        if self._cfg("tls", False):
            self._mqtt.tls_set(
                ca_certs=self._cfg("tls_ca_certs"),
                certfile=self._cfg("tls_certfile"),
                keyfile=self._cfg("tls_keyfile"),
            )

        master_status = self.master_status_topic()
        self._mqtt.will_set(master_status, _OFFLINE, qos=1, retain=True)

        self._mqtt.on_connect = self._on_connect
        self._mqtt.on_message = self._on_message
        self._mqtt.on_disconnect = self._on_disconnect

        self._mqtt.connect(broker_host, broker_port, keepalive=60)
        self._mqtt.publish(master_status, _ONLINE, qos=1, retain=True)

        # Missing/None → default; any value <= 0 disables the sweep entirely.
        raw_idle = self._cfg("idle_timeout")
        idle_timeout = float(raw_idle if raw_idle is not None else _DEFAULT_IDLE_TIMEOUT)
        if idle_timeout > 0:
            threading.Thread(
                target=self._idle_sweep, args=(idle_timeout,),
                daemon=True, name="mqtt-idle-sweep",
            ).start()

        LOG.info(f"[MQTT] listener started — broker={broker_host}:{broker_port}")
        self._mqtt.loop_forever()
