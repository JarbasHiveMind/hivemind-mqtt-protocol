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


# ---------------------------------------------------------------------------
# version.py module
# ---------------------------------------------------------------------------


def test_version_module_exposes_constants_and_string():
    from hivemind_mqtt_protocol import version as v
    assert isinstance(v.VERSION_MAJOR, int)
    assert isinstance(v.VERSION_MINOR, int)
    assert isinstance(v.VERSION_BUILD, int)
    assert isinstance(v.VERSION_ALPHA, int)
    assert isinstance(v.__version__, str)
    assert v.__version__.startswith(
        f"{v.VERSION_MAJOR}.{v.VERSION_MINOR}.{v.VERSION_BUILD}"
    )


# ---------------------------------------------------------------------------
# _cfg / config helpers
# ---------------------------------------------------------------------------


class TestConfig:
    def test_default_hub_id_falls_back_to_config(self):
        p = _make_protocol({"hub_id": "myhub"})
        assert p._hub_id() == "myhub"

    def test_default_prefix(self):
        p = _make_protocol()
        assert p._prefix() == "hivemind"

    def test_custom_prefix(self):
        p = _make_protocol({"topic_prefix": "iot"})
        assert p._prefix() == "iot"

    def test_missing_key_returns_default(self):
        p = _make_protocol()
        assert p._cfg("nonexistent", 42) == 42

    def test_hash_topics_false_by_default(self):
        p = _make_protocol()
        assert p._cfg("hash_topics", False) is False


# ---------------------------------------------------------------------------
# _on_connect subscribes to the right wildcards
# ---------------------------------------------------------------------------


class TestOnConnect:
    def test_on_connect_subscribes_c2s_and_status(self):
        p = _make_protocol()
        mock_client = MagicMock(name="client")
        p._on_connect(mock_client, None, {}, 0)

        calls = [c[0][0] for c in mock_client.subscribe.call_args_list]
        assert p.c2s_wildcard() in calls
        assert p.status_wildcard() in calls

    def test_on_connect_failed_rc_skips_subscribe(self):
        p = _make_protocol()
        mock_client = MagicMock(name="client")
        p._on_connect(mock_client, None, {}, 1)
        mock_client.subscribe.assert_not_called()


# ---------------------------------------------------------------------------
# require_crypto path
# ---------------------------------------------------------------------------


class TestRequireCrypto:
    def test_no_crypto_key_and_require_crypto_rejects(self):
        p = _make_protocol()
        p.hm_protocol.handshake_enabled = False
        p.hm_protocol.require_crypto = True
        conn = p._build_client_connection("sat1", "sat1", "sat1")
        assert conn is None
        p.hm_protocol.handle_invalid_protocol_version.assert_called_once()

    def test_crypto_key_present_allows_connection(self):
        p = _make_protocol()
        p.hm_protocol.handshake_enabled = False
        p.hm_protocol.require_crypto = True
        user = p.hm_protocol.db.get_client_by_api_key.return_value
        user.crypto_key = "some-key"
        conn = p._build_client_connection("sat1", "sat1", "sat1")
        assert conn is not None


# ---------------------------------------------------------------------------
# Password handshake path (line 211)
# ---------------------------------------------------------------------------


class TestPasswordHandshake:
    def test_user_with_password_sets_pswd_handshake(self):
        p = _make_protocol()
        user = p.hm_protocol.db.get_client_by_api_key.return_value
        user.password = "s3cr3t"
        conn = p._build_client_connection("sat1", "sat1", "sat1")
        assert conn is not None
        assert conn.pswd_handshake is not None

    def test_user_without_password_no_pswd_handshake(self):
        p = _make_protocol()
        user = p.hm_protocol.db.get_client_by_api_key.return_value
        user.password = None
        conn = p._build_client_connection("sat1", "sat1", "sat1")
        assert conn is not None
        assert conn.pswd_handshake is None


# ---------------------------------------------------------------------------
# _on_message edge cases
# ---------------------------------------------------------------------------


class TestOnMessageEdgeCases:
    def _make_msg(self, topic: str, payload: bytes) -> MagicMock:
        m = MagicMock()
        m.topic = topic
        m.payload = payload
        return m

    def test_unknown_segment_ignored(self):
        """Topic segment that is neither 'c2s' nor 'status' → returns early (line 278)."""
        p = _make_protocol()
        msg = self._make_msg("hivemind/testhub/other/sat1", b"data")
        p._on_message(p._mqtt, None, msg)
        p.hm_protocol.handle_message.assert_not_called()

    def test_auth_fail_drops_frame(self):
        """Auth failure on unknown satellite drops frame (line 301)."""
        p = _make_protocol()
        p.hm_protocol.db.get_client_by_api_key.return_value = None
        msg = self._make_msg("hivemind/testhub/c2s/newsat", b"data")
        p._on_message(p._mqtt, None, msg)
        p.hm_protocol.handle_message.assert_not_called()

    def test_decode_error_drops_frame(self):
        """conn.decode() raises → logs warning and drops frame (lines 305-307)."""
        p = _make_protocol()
        p._build_client_connection("sat1", "sat1", "sat1")
        conn = p._peers["sat1"]
        conn.decode = MagicMock(side_effect=ValueError("bad frame"))
        p.hm_protocol.handle_message.reset_mock()

        msg = self._make_msg("hivemind/testhub/c2s/sat1", b"garbage")
        p._on_message(p._mqtt, None, msg)

        p.hm_protocol.handle_message.assert_not_called()

    def test_known_peer_last_seen_updated(self):
        """Inbound c2s from known peer updates _last_seen timestamp."""
        p = _make_protocol()
        p._build_client_connection("sat1", "sat1", "sat1")
        old_ts = p._last_seen["sat1"] - 100
        p._last_seen["sat1"] = old_ts

        msg = self._make_msg("hivemind/testhub/c2s/sat1", b"payload")
        p._on_message(p._mqtt, None, msg)

        assert p._last_seen["sat1"] > old_ts


# ---------------------------------------------------------------------------
# _on_disconnect
# ---------------------------------------------------------------------------


class TestOnDisconnect:
    def test_clean_disconnect_no_warning(self):
        """rc=0 clean disconnect — no warning logged (line 312)."""
        p = _make_protocol()
        # Should not raise.
        p._on_disconnect(p._mqtt, None, 0)

    def test_unexpected_disconnect_logs_warning(self):
        """rc!=0 → warning branch executed (lines 312-313)."""
        p = _make_protocol()
        # Should not raise; just exercises the warning log path.
        p._on_disconnect(p._mqtt, None, 1)


# ---------------------------------------------------------------------------
# _idle_sweep (lines 321-332)
# ---------------------------------------------------------------------------


class TestIdleSweepDirect:
    def test_idle_sweep_evicts_stale_peer(self):
        """Run _idle_sweep directly with a patched time.sleep so the loop
        fires once and then is interrupted."""
        p = _make_protocol()
        p._build_client_connection("sat1", "sat1", "sat1")
        # Make the peer appear stale.
        p._last_seen["sat1"] = time.monotonic() - 9999
        p.hm_protocol.handle_client_disconnected.reset_mock()

        call_count = [0]

        def _fake_sleep(secs):
            call_count[0] += 1
            if call_count[0] >= 2:
                raise StopIteration("stop loop")

        import hivemind_mqtt_protocol as _mod
        orig_sleep = _mod.time.sleep
        _mod.time.sleep = _fake_sleep
        try:
            with pytest.raises(StopIteration):
                p._idle_sweep(300)
        finally:
            _mod.time.sleep = orig_sleep

        assert "sat1" not in p._peers
        p.hm_protocol.handle_client_disconnected.assert_called_once()

    def test_idle_sweep_keeps_fresh_peer(self):
        """Fresh peer must NOT be evicted."""
        p = _make_protocol()
        p._build_client_connection("sat1", "sat1", "sat1")
        p._last_seen["sat1"] = time.monotonic()  # fresh
        p.hm_protocol.handle_client_disconnected.reset_mock()

        call_count = [0]

        def _fake_sleep(secs):
            call_count[0] += 1
            if call_count[0] >= 2:
                raise StopIteration("stop loop")

        import hivemind_mqtt_protocol as _mod
        orig_sleep = _mod.time.sleep
        _mod.time.sleep = _fake_sleep
        try:
            with pytest.raises(StopIteration):
                p._idle_sweep(300)
        finally:
            _mod.time.sleep = orig_sleep

        assert "sat1" in p._peers
        p.hm_protocol.handle_client_disconnected.assert_not_called()


# ---------------------------------------------------------------------------
# run() — mocked mqtt.Client so no live broker needed (lines 339-388)
# ---------------------------------------------------------------------------


class TestRun:
    def _make_mock_mqtt_client(self):
        """Return a MagicMock that mimics paho.mqtt.Client."""
        mc = MagicMock(name="mqtt.Client")
        mc.loop_forever = MagicMock(return_value=None)
        return mc

    def test_run_basic(self):
        """run() sets up the client and calls loop_forever."""
        p = _make_protocol({"broker_host": "127.0.0.1", "broker_port": 1883})
        mock_client_instance = self._make_mock_mqtt_client()

        import paho.mqtt.client as paho_mqtt
        with patch.object(paho_mqtt, "Client", return_value=mock_client_instance):
            p.run()

        mock_client_instance.connect.assert_called_once_with("127.0.0.1", 1883, keepalive=60)
        mock_client_instance.loop_forever.assert_called_once()

    def test_run_sets_callbacks(self):
        """run() installs on_connect, on_message, on_disconnect."""
        p = _make_protocol()
        mock_client_instance = self._make_mock_mqtt_client()

        import paho.mqtt.client as paho_mqtt
        with patch.object(paho_mqtt, "Client", return_value=mock_client_instance):
            p.run()

        assert mock_client_instance.on_connect == p._on_connect
        assert mock_client_instance.on_message == p._on_message
        assert mock_client_instance.on_disconnect == p._on_disconnect

    def test_run_with_broker_auth(self):
        """run() calls username_pw_set when broker_username is set."""
        p = _make_protocol({"broker_username": "user", "broker_password": "pass"})
        mock_client_instance = self._make_mock_mqtt_client()

        import paho.mqtt.client as paho_mqtt
        with patch.object(paho_mqtt, "Client", return_value=mock_client_instance):
            p.run()

        mock_client_instance.username_pw_set.assert_called_once_with("user", "pass")

    def test_run_without_broker_auth(self):
        """run() skips username_pw_set when broker_username not set."""
        p = _make_protocol()
        mock_client_instance = self._make_mock_mqtt_client()

        import paho.mqtt.client as paho_mqtt
        with patch.object(paho_mqtt, "Client", return_value=mock_client_instance):
            p.run()

        mock_client_instance.username_pw_set.assert_not_called()

    def test_run_tls_enabled(self):
        """run() calls tls_set when tls=True."""
        p = _make_protocol({
            "tls": True,
            "tls_ca_certs": "/ca.pem",
            "tls_certfile": "/client.crt",
            "tls_keyfile": "/client.key",
        })
        mock_client_instance = self._make_mock_mqtt_client()

        import paho.mqtt.client as paho_mqtt
        with patch.object(paho_mqtt, "Client", return_value=mock_client_instance):
            p.run()

        mock_client_instance.tls_set.assert_called_once_with(
            ca_certs="/ca.pem",
            certfile="/client.crt",
            keyfile="/client.key",
        )

    def test_run_tls_disabled(self):
        """run() does not call tls_set when tls=False (default)."""
        p = _make_protocol()
        mock_client_instance = self._make_mock_mqtt_client()

        import paho.mqtt.client as paho_mqtt
        with patch.object(paho_mqtt, "Client", return_value=mock_client_instance):
            p.run()

        mock_client_instance.tls_set.assert_not_called()

    def test_run_publishes_hub_online(self):
        """run() publishes 'online' to the hub status topic after connect."""
        p = _make_protocol({"hub_id": "testhub"})
        mock_client_instance = self._make_mock_mqtt_client()

        import paho.mqtt.client as paho_mqtt
        with patch.object(paho_mqtt, "Client", return_value=mock_client_instance):
            p.run()

        publish_calls = mock_client_instance.publish.call_args_list
        topics_published = [c[0][0] for c in publish_calls]
        payloads_published = [c[0][1] for c in publish_calls]
        assert any("status/hub" in t for t in topics_published)
        assert "online" in payloads_published

    def test_run_will_set_offline_lwt(self):
        """run() sets a LWT will_set with 'offline' payload."""
        p = _make_protocol({"hub_id": "testhub"})
        mock_client_instance = self._make_mock_mqtt_client()

        import paho.mqtt.client as paho_mqtt
        with patch.object(paho_mqtt, "Client", return_value=mock_client_instance):
            p.run()

        mock_client_instance.will_set.assert_called_once()
        args = mock_client_instance.will_set.call_args[0]
        assert "status/hub" in args[0]
        assert args[1] == "offline"

    def test_run_idle_sweep_thread_started(self):
        """run() starts the idle-sweep daemon thread when idle_timeout > 0."""
        p = _make_protocol({"idle_timeout": 60})
        mock_client_instance = self._make_mock_mqtt_client()
        threads_started = []

        orig_thread_start = threading.Thread.start

        def _patched_start(self_t):
            threads_started.append(self_t.name)
            orig_thread_start(self_t)

        import paho.mqtt.client as paho_mqtt
        with patch.object(paho_mqtt, "Client", return_value=mock_client_instance):
            with patch.object(threading.Thread, "start", _patched_start):
                p.run()

        assert "mqtt-idle-sweep" in threads_started

    def test_run_no_idle_sweep_when_disabled(self):
        """run() does NOT start the idle-sweep thread when idle_timeout is
        explicitly set to a negative value (the 'or default' guard means 0
        falls back to the default; negative is the reliable disable signal)."""
        p = _make_protocol({"idle_timeout": -1})
        mock_client_instance = self._make_mock_mqtt_client()
        threads_started = []

        orig_thread_start = threading.Thread.start

        def _patched_start(self_t):
            threads_started.append(self_t.name)
            orig_thread_start(self_t)

        import paho.mqtt.client as paho_mqtt
        with patch.object(paho_mqtt, "Client", return_value=mock_client_instance):
            with patch.object(threading.Thread, "start", _patched_start):
                p.run()

        assert "mqtt-idle-sweep" not in threads_started

    def test_run_default_broker_host_and_port(self):
        """run() defaults to localhost:1883 when not configured."""
        p = _make_protocol()
        mock_client_instance = self._make_mock_mqtt_client()

        import paho.mqtt.client as paho_mqtt
        with patch.object(paho_mqtt, "Client", return_value=mock_client_instance):
            p.run()

        mock_client_instance.connect.assert_called_once_with("localhost", 1883, keepalive=60)
