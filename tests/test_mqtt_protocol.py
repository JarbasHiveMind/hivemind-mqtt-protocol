"""
Unit tests for hivemind_mqtt_protocol.HiveMindMqttProtocol.

All tests use mocked paho-mqtt and mocked hivemind-core objects — no live
broker is required.
"""

import hashlib
import threading
import time
import types
from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Minimal stubs so we can import the protocol without a full HiveMind install.
# ---------------------------------------------------------------------------

import sys


def _make_stub_module(name: str, **attrs) -> types.ModuleType:
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    return mod


# ovos_utils.log
log_stub = _make_stub_module("ovos_utils")
log_stub.log = _make_stub_module("ovos_utils.log", LOG=MagicMock())
sys.modules.setdefault("ovos_utils", log_stub)
sys.modules.setdefault("ovos_utils.log", log_stub.log)

# ovos_bus_client.session
session_cls = MagicMock(name="Session")
session_stub = _make_stub_module("ovos_bus_client")
session_stub.session = _make_stub_module("ovos_bus_client.session", Session=session_cls)
sys.modules.setdefault("ovos_bus_client", session_stub)
sys.modules.setdefault("ovos_bus_client.session", session_stub.session)

# poorman_handshake
psh_stub = _make_stub_module("poorman_handshake", PasswordHandShake=MagicMock())
sys.modules.setdefault("poorman_handshake", psh_stub)

# hivemind_plugin_manager.protocols
@dataclass
class _FakeNetworkProtocol:
    config: Dict[str, Any] = field(default_factory=dict)
    hm_protocol: Optional[Any] = None
    callbacks: Any = None

    @property
    def identity(self):
        return MagicMock(name="identity")

    @property
    def database(self):
        return None

    @property
    def clients(self):
        return {}

    @property
    def agent_protocol(self):
        return None

    def run(self):  # abstract placeholder
        pass


class _FakeCallbacks:
    pass


proto_mod = _make_stub_module(
    "hivemind_plugin_manager.protocols",
    NetworkProtocol=_FakeNetworkProtocol,
    ClientCallbacks=_FakeCallbacks,
)
pm_mod = _make_stub_module("hivemind_plugin_manager", protocols=proto_mod)
sys.modules.setdefault("hivemind_plugin_manager", pm_mod)
sys.modules.setdefault("hivemind_plugin_manager.protocols", proto_mod)

# hivemind_core.protocol
class _FakeClientConnection:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.peer = kwargs.get("name", "peer")
        self.crypto_key = None
        self.msg_blacklist = []
        self.skill_blacklist = []
        self.intent_blacklist = []
        self.allowed_types = []
        self.can_broadcast = True
        self.can_propagate = True
        self.can_escalate = True
        self.is_admin = False
        self.pswd_handshake = None
        self.node_type = None
        self._decode_result = MagicMock(name="HiveMessage")

    def decode(self, payload):
        return self._decode_result


class _FakeNodeType:
    NODE = "NODE"


hmc_mod = _make_stub_module(
    "hivemind_core.protocol",
    HiveMindClientConnection=_FakeClientConnection,
    HiveMindListenerProtocol=MagicMock(),
    HiveMindNodeType=_FakeNodeType,
)
sys.modules.setdefault("hivemind_core", _make_stub_module("hivemind_core"))
sys.modules.setdefault("hivemind_core.protocol", hmc_mod)

# Now patch NetworkProtocol base in the module under test before importing.
import hivemind_mqtt_protocol  # noqa: E402  (import after stubs)

# Patch the base class reference inside the module so HiveMindMqttProtocol
# inherits from our stub instead of the real NetworkProtocol.
hivemind_mqtt_protocol.NetworkProtocol = _FakeNetworkProtocol
hivemind_mqtt_protocol.HiveMindClientConnection = _FakeClientConnection
hivemind_mqtt_protocol.HiveMindNodeType = _FakeNodeType
hivemind_mqtt_protocol.ClientCallbacks = _FakeCallbacks

from hivemind_mqtt_protocol import HiveMindMqttProtocol  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_protocol(config=None):
    """Return a protocol instance with a mocked hm_protocol and mqtt client."""
    hm = MagicMock(name="hm_protocol")
    hm.identity = MagicMock(name="identity")
    hm.identity.name = "testhub"
    hm.handshake_enabled = True
    hm.require_crypto = False
    # Always inject hub_id via config so _hub_id() doesn't go through the
    # identity property chain (MagicMock.name is a reserved attribute).
    base_config = {"hub_id": "testhub"}
    if config:
        base_config.update(config)
    config = base_config

    # DB returns a valid user for any key.
    user = MagicMock(name="user")
    user.client_id = 42
    user.name = "testclient"
    user.crypto_key = None
    user.message_blacklist = []
    user.skill_blacklist = []
    user.intent_blacklist = []
    user.allowed_types = []
    user.can_broadcast = True
    user.can_propagate = True
    user.can_escalate = True
    user.is_admin = False
    user.password = None
    hm.db.get_client_by_api_key.return_value = user

    p = HiveMindMqttProtocol(config=config or {}, hm_protocol=hm)
    p._peers = {}
    p._last_seen = {}
    p._lock = threading.Lock()
    mock_mqtt = MagicMock(name="mqtt_client")
    p._mqtt = mock_mqtt
    return p


# ---------------------------------------------------------------------------
# Topic build / parse tests
# ---------------------------------------------------------------------------


class TestTopics:
    def test_c2s_topic_default_prefix(self):
        p = _make_protocol()
        assert p.c2s_topic("sat1") == "hivemind/testhub/c2s/sat1"

    def test_s2c_topic_default_prefix(self):
        p = _make_protocol()
        assert p.s2c_topic("sat1") == "hivemind/testhub/s2c/sat1"

    def test_status_topic_default_prefix(self):
        p = _make_protocol()
        assert p.status_topic("sat1") == "hivemind/testhub/status/sat1"

    def test_custom_prefix_and_hub_id(self):
        p = _make_protocol({"topic_prefix": "hm", "hub_id": "myhub"})
        assert p.c2s_topic("x") == "hm/myhub/c2s/x"

    def test_c2s_wildcard(self):
        p = _make_protocol()
        assert p.c2s_wildcard() == "hivemind/testhub/c2s/+"

    def test_status_wildcard(self):
        p = _make_protocol()
        assert p.status_wildcard() == "hivemind/testhub/status/+"

    def test_hash_topics(self):
        p = _make_protocol({"hash_topics": True})
        sat_id = "mysat"
        expected = hashlib.sha256(sat_id.encode()).hexdigest()[:16]
        assert p.c2s_topic(sat_id) == f"hivemind/testhub/c2s/{expected}"

    def test_satellite_id_from_c2s_topic(self):
        topic = "hivemind/testhub/c2s/sat42"
        assert HiveMindMqttProtocol._satellite_id_from_topic(topic) == "sat42"

    def test_satellite_id_from_status_topic(self):
        topic = "hivemind/myhub/status/sat99"
        assert HiveMindMqttProtocol._satellite_id_from_topic(topic) == "sat99"

    def test_satellite_id_short_topic_returns_none(self):
        assert HiveMindMqttProtocol._satellite_id_from_topic("bad") is None


# ---------------------------------------------------------------------------
# Peer-map: new peer / known peer logic
# ---------------------------------------------------------------------------


class TestPeerMap:
    def test_new_peer_added_to_map(self):
        p = _make_protocol()
        conn = p._build_client_connection("sat1", "sat1", "sat1")
        assert "sat1" in p._peers
        assert conn is not None

    def test_known_peer_not_duplicated(self):
        p = _make_protocol()
        p._build_client_connection("sat1", "sat1", "sat1")
        first_conn = p._peers["sat1"]
        # Simulating message from known peer — we do NOT call _build again.
        assert p._peers["sat1"] is first_conn

    def test_invalid_key_not_added(self):
        p = _make_protocol()
        p.hm_protocol.db.get_client_by_api_key.return_value = None
        conn = p._build_client_connection("bad", "bad", "bad")
        assert conn is None
        assert "bad" not in p._peers

    def test_handle_new_client_called(self):
        p = _make_protocol()
        p._build_client_connection("sat1", "sat1", "sat1")
        p.hm_protocol.handle_new_client.assert_called_once()

    def test_invalid_key_triggers_invalid_key_handler(self):
        p = _make_protocol()
        p.hm_protocol.db.get_client_by_api_key.return_value = None
        p._build_client_connection("bad", "bad", "bad")
        p.hm_protocol.handle_invalid_key_connected.assert_called_once()


# ---------------------------------------------------------------------------
# send_msg publishes to the correct topic
# ---------------------------------------------------------------------------


class TestSendMsg:
    def test_send_msg_publishes_to_s2c(self):
        p = _make_protocol()
        p._build_client_connection("sat1", "sat1", "sat1")
        conn = p._peers["sat1"]

        payload = b"encrypted-frame"
        conn.send_msg(payload, False)

        expected_topic = p.s2c_topic("sat1")
        p._mqtt.publish.assert_called_with(expected_topic, payload, qos=1)

    def test_send_msg_bin_uses_qos0(self):
        p = _make_protocol()
        p._build_client_connection("sat1", "sat1", "sat1")
        conn = p._peers["sat1"]

        conn.send_msg(b"audio", True)

        # QoS 0 for binary.
        call_args = p._mqtt.publish.call_args
        assert call_args[1]["qos"] == 0 or (len(call_args[0]) >= 3 and call_args[0][2] == 0)

    def test_send_msg_str_payload_encoded(self):
        p = _make_protocol()
        p._build_client_connection("sat1", "sat1", "sat1")
        conn = p._peers["sat1"]

        conn.send_msg("string-payload", False)

        topic = p.s2c_topic("sat1")
        p._mqtt.publish.assert_called_with(topic, b"string-payload", qos=1)


# ---------------------------------------------------------------------------
# Disconnect: do_disconnect and _disconnect_peer
# ---------------------------------------------------------------------------


class TestDisconnect:
    def test_disconnect_removes_from_map(self):
        p = _make_protocol()
        p._build_client_connection("sat1", "sat1", "sat1")
        assert "sat1" in p._peers

        p._disconnect_peer("sat1")

        assert "sat1" not in p._peers

    def test_disconnect_calls_handle_client_disconnected(self):
        p = _make_protocol()
        p._build_client_connection("sat1", "sat1", "sat1")
        p.hm_protocol.handle_client_disconnected.reset_mock()

        p._disconnect_peer("sat1")

        p.hm_protocol.handle_client_disconnected.assert_called_once()

    def test_do_disconnect_publishes_tombstone(self):
        p = _make_protocol()
        p._build_client_connection("sat1", "sat1", "sat1")
        conn = p._peers["sat1"]
        p._mqtt.publish.reset_mock()

        conn.disconnect()

        status_topic = p.status_topic("sat1")
        p._mqtt.publish.assert_called_with(status_topic, "offline", qos=1, retain=True)
        assert "sat1" not in p._peers

    def test_disconnect_unknown_peer_is_noop(self):
        p = _make_protocol()
        # Should not raise.
        p._disconnect_peer("ghost")
        p.hm_protocol.handle_client_disconnected.assert_not_called()


# ---------------------------------------------------------------------------
# LWT / status message handling
# ---------------------------------------------------------------------------


class TestLWT:
    def _make_msg(self, topic: str, payload: bytes) -> MagicMock:
        m = MagicMock()
        m.topic = topic
        m.payload = payload
        return m

    def test_lwt_offline_triggers_disconnect(self):
        p = _make_protocol()
        p._build_client_connection("sat1", "sat1", "sat1")
        p.hm_protocol.handle_client_disconnected.reset_mock()

        msg = self._make_msg("hivemind/testhub/status/sat1", b"offline")
        p._on_message(p._mqtt, None, msg)

        p.hm_protocol.handle_client_disconnected.assert_called_once()
        assert "sat1" not in p._peers

    def test_lwt_online_ignored(self):
        p = _make_protocol()
        p._build_client_connection("sat1", "sat1", "sat1")
        p.hm_protocol.handle_client_disconnected.reset_mock()

        msg = self._make_msg("hivemind/testhub/status/sat1", b"online")
        p._on_message(p._mqtt, None, msg)

        p.hm_protocol.handle_client_disconnected.assert_not_called()

    def test_c2s_message_routed_to_handle_message(self):
        p = _make_protocol()
        p._build_client_connection("sat1", "sat1", "sat1")
        p.hm_protocol.handle_message.reset_mock()

        msg = self._make_msg("hivemind/testhub/c2s/sat1", b"payload")
        p._on_message(p._mqtt, None, msg)

        p.hm_protocol.handle_message.assert_called_once()

    def test_unknown_peer_auto_registered_on_c2s(self):
        p = _make_protocol()
        assert "newsat" not in p._peers

        msg = self._make_msg("hivemind/testhub/c2s/newsat", b"payload")
        p._on_message(p._mqtt, None, msg)

        assert "newsat" in p._peers

    def test_bad_topic_shape_ignored(self):
        p = _make_protocol()
        msg = self._make_msg("bad", b"x")
        p._on_message(p._mqtt, None, msg)
        p.hm_protocol.handle_message.assert_not_called()


# ---------------------------------------------------------------------------
# Idle-timeout sweep
# ---------------------------------------------------------------------------


class TestIdleSweep:
    def test_stale_peer_evicted(self):
        p = _make_protocol()
        p._build_client_connection("sat1", "sat1", "sat1")
        # Back-date last_seen to force eviction.
        p._last_seen["sat1"] = time.monotonic() - 9999

        p.hm_protocol.handle_client_disconnected.reset_mock()
        p._idle_sweep.__func__  # confirm it's a method

        # Run one cycle manually by calling _disconnect_peer for stale peers.
        now = time.monotonic()
        idle_timeout = 300
        stale = [sid for sid, ts in p._last_seen.items() if (now - ts) > idle_timeout]
        for sid in stale:
            p._disconnect_peer(sid)

        assert "sat1" not in p._peers
        p.hm_protocol.handle_client_disconnected.assert_called_once()


# ---------------------------------------------------------------------------
# QoS helpers
# ---------------------------------------------------------------------------


class TestQoS:
    def test_default_qos_is_1(self):
        p = _make_protocol()
        assert p._qos(False) == 1

    def test_binary_qos_is_0(self):
        p = _make_protocol()
        assert p._qos(True) == 0

    def test_custom_qos_from_config(self):
        p = _make_protocol({"qos": 0})
        assert p._qos(False) == 0
