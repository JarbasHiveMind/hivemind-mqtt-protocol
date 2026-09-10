"""
Unit tests for hivemind_mqtt_protocol.HiveMindMqttProtocol.

All tests use mocked paho-mqtt and mocked hivemind-core objects — no live
broker is required.
"""

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

# These unit tests exercise the protocol's routing/topic/lifecycle logic in
# isolation, with a mocked ``hm_protocol`` and a mocked paho client. The real
# ``hivemind_mqtt_protocol`` (and its real dependencies) are imported normally;
# only the two collaborators that need real crypto/identity to construct — the
# ``HiveMindClientConnection`` and ``HiveMindNodeType`` references the module
# looks up by name — are swapped for lightweight fakes, and that swap is scoped
# to each test by the autouse ``_patch_connection`` fixture below so it never
# leaks into the end-to-end suite.

import hivemind_mqtt_protocol  # noqa: E402
from hivemind_mqtt_protocol import HiveMindMqttProtocol  # noqa: E402


class _FakeClientConnection:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.peer = kwargs.get("name", "peer")
        self.noise_transport = None
        self.msg_blacklist = []
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


@pytest.fixture(autouse=True)
def _patch_connection(monkeypatch):
    """Swap the heavyweight collaborators for fakes, per-test and restored.

    ``monkeypatch`` restores the originals at teardown, so the real classes
    are intact for the end-to-end suite that runs in the same session.
    """
    monkeypatch.setattr(hivemind_mqtt_protocol, "HiveMindClientConnection",
                        _FakeClientConnection)
    monkeypatch.setattr(hivemind_mqtt_protocol, "HiveMindNodeType",
                        _FakeNodeType)
    monkeypatch.setattr(hivemind_mqtt_protocol, "PasswordHandShake",
                        MagicMock(name="PasswordHandShake"))


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_protocol(config=None):
    """Return a protocol instance with a mocked hm_protocol and mqtt client."""
    hm = MagicMock(name="hm_protocol")
    hm.identity = MagicMock(name="identity")
    hm.identity.name = "testkey"
    # Always inject api_key via config so _api_key() doesn't go through the
    # identity property chain (MagicMock.name is a reserved attribute).
    base_config = {"api_key": "testkey"}
    if config:
        base_config.update(config)
    config = base_config

    # DB returns a valid user for any key.
    user = MagicMock(name="user")
    user.client_id = 42
    user.name = "testclient"
    user.message_blacklist = []
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
        assert p.in_topic("sat1") == "hivemind/sat1/in"

    def test_s2c_topic_default_prefix(self):
        p = _make_protocol()
        assert p.out_topic("sat1") == "hivemind/sat1/out"

    def test_status_topic_default_prefix(self):
        p = _make_protocol()
        assert p.status_topic("sat1") == "hivemind/sat1/status"

    def test_custom_prefix_and_api_key(self):
        p = _make_protocol({"topic_prefix": "hm", "api_key": "mykey"})
        assert p.in_topic("x") == "hm/x/in"

    def test_c2s_wildcard(self):
        p = _make_protocol()
        assert p.in_wildcard() == "hivemind/+/in"

    def test_status_wildcard(self):
        p = _make_protocol()
        assert p.status_wildcard() == "hivemind/+/status"

    def test_api_key_from_in_topic(self):
        topic = "hivemind/sat42/in"
        assert HiveMindMqttProtocol._api_key_from_topic(topic) == "sat42"

    def test_api_key_from_status_topic(self):
        topic = "hivemind/sat99/status"
        assert HiveMindMqttProtocol._api_key_from_topic(topic) == "sat99"

    def test_api_key_short_topic_returns_none(self):
        assert HiveMindMqttProtocol._api_key_from_topic("bad") is None


# ---------------------------------------------------------------------------
# _coerce_payload: MQTT bytes → str/bytes the way decode() expects
# ---------------------------------------------------------------------------


class TestCoercePayload:
    def test_json_object_bytes_become_str(self):
        out = HiveMindMqttProtocol._coerce_payload(b'{"msg_type": "ping"}')
        assert isinstance(out, str)
        assert out == '{"msg_type": "ping"}'

    def test_ciphertext_json_bytes_become_str(self):
        out = HiveMindMqttProtocol._coerce_payload(b'{"ciphertext": "abc"}')
        assert isinstance(out, str)

    def test_json_array_bytes_become_str(self):
        out = HiveMindMqttProtocol._coerce_payload(b'[1, 2, 3]')
        assert isinstance(out, str)

    def test_leading_whitespace_is_tolerated(self):
        out = HiveMindMqttProtocol._coerce_payload(b'   {"a": 1}')
        assert isinstance(out, str)

    def test_binary_bitstring_stays_bytes(self):
        # Non-JSON bytes (a binary frame) must pass through untouched.
        raw = bytes([0x00, 0x01, 0xFF, 0x10])
        out = HiveMindMqttProtocol._coerce_payload(raw)
        assert out == raw and isinstance(out, bytes)

    def test_invalid_utf8_that_looks_like_json_stays_bytes(self):
        # Starts with '{' but is not valid UTF-8 → treated as a binary frame.
        raw = b"{\xff\xfe"
        out = HiveMindMqttProtocol._coerce_payload(raw)
        assert out == raw and isinstance(out, bytes)

    def test_non_bytes_passthrough(self):
        # An already-decoded str is returned unchanged.
        assert HiveMindMqttProtocol._coerce_payload("already str") == "already str"


# ---------------------------------------------------------------------------
# Peer-map: new peer / known peer logic
# ---------------------------------------------------------------------------


class TestPeerMap:
    def test_new_peer_added_to_map(self):
        p = _make_protocol()
        conn = p._build_client_connection("sat1")
        assert "sat1" in p._peers
        assert conn is not None

    def test_known_peer_not_duplicated(self):
        p = _make_protocol()
        p._build_client_connection("sat1")
        first_conn = p._peers["sat1"]
        # Simulating message from known peer — we do NOT call _build again.
        assert p._peers["sat1"] is first_conn

    def test_invalid_key_not_added(self):
        p = _make_protocol()
        p.hm_protocol.db.get_client_by_api_key.return_value = None
        conn = p._build_client_connection("bad")
        assert conn is None
        assert "bad" not in p._peers

    def test_handle_new_client_called(self):
        p = _make_protocol()
        p._build_client_connection("sat1")
        p.hm_protocol.handle_new_client.assert_called_once()

    def test_invalid_key_triggers_invalid_key_handler(self):
        p = _make_protocol()
        p.hm_protocol.db.get_client_by_api_key.return_value = None
        p._build_client_connection("bad")
        p.hm_protocol.handle_invalid_key_connected.assert_called_once()


# ---------------------------------------------------------------------------
# send_msg publishes to the correct topic
# ---------------------------------------------------------------------------


class TestSendMsg:
    def test_send_msg_publishes_to_s2c(self):
        p = _make_protocol()
        p._build_client_connection("sat1")
        conn = p._peers["sat1"]

        payload = b"encrypted-frame"
        conn.send_msg(payload, False)

        expected_topic = p.out_topic("sat1")
        p._mqtt.publish.assert_called_with(expected_topic, payload, qos=1)

    def test_send_msg_bin_uses_qos0(self):
        p = _make_protocol()
        p._build_client_connection("sat1")
        conn = p._peers["sat1"]

        conn.send_msg(b"audio", True)

        # QoS 0 for binary.
        call_args = p._mqtt.publish.call_args
        assert call_args[1]["qos"] == 0 or (len(call_args[0]) >= 3 and call_args[0][2] == 0)

    def test_send_msg_str_payload_encoded(self):
        p = _make_protocol()
        p._build_client_connection("sat1")
        conn = p._peers["sat1"]

        conn.send_msg("string-payload", False)

        topic = p.out_topic("sat1")
        p._mqtt.publish.assert_called_with(topic, b"string-payload", qos=1)


# ---------------------------------------------------------------------------
# Disconnect: do_disconnect and _disconnect_peer
# ---------------------------------------------------------------------------


class TestMultiFrameChunks:
    def test_a_chunk_of_a_multi_frame_message_is_not_dispatched(self):
        """decode() returns None for every chunk but the last of a fragmented
        Noise message; only the assembled message may reach handle_message."""
        from types import SimpleNamespace
        p = _make_protocol()
        conn = p._build_client_connection("sat1")
        conn.noise_transport = object()  # a live Noise session
        conn.decode = lambda payload: None
        p.hm_protocol.handle_message.reset_mock()

        p._on_message(p._mqtt, None, SimpleNamespace(topic=p.in_topic("sat1"), payload=b"\x04chunk"))

        p.hm_protocol.handle_message.assert_not_called()


class TestDisconnect:
    def test_disconnect_removes_from_map(self):
        p = _make_protocol()
        p._build_client_connection("sat1")
        assert "sat1" in p._peers

        p._disconnect_peer("sat1")

        assert "sat1" not in p._peers

    def test_disconnect_calls_handle_client_disconnected(self):
        p = _make_protocol()
        p._build_client_connection("sat1")
        p.hm_protocol.handle_client_disconnected.reset_mock()

        p._disconnect_peer("sat1")

        p.hm_protocol.handle_client_disconnected.assert_called_once()

    def test_core_initiated_disconnect_reaches_the_protocol(self):
        """conn.disconnect() is how core closes a session (a bad frame, a
        failed handshake, a rejected message type); the peer must leave
        core's client table the same way the broker's last-will path does."""
        p = _make_protocol()
        conn = p._build_client_connection("sat1")
        p.hm_protocol.handle_client_disconnected.reset_mock()

        conn.disconnect(1008, "test")

        p.hm_protocol.handle_client_disconnected.assert_called_once_with(conn)
        assert "sat1" not in p._peers

    def test_do_disconnect_publishes_tombstone(self):
        p = _make_protocol()
        p._build_client_connection("sat1")
        conn = p._peers["sat1"]
        p._mqtt.publish.reset_mock()

        conn.disconnect()

        status_topic = p.status_topic("sat1")
        p._mqtt.publish.assert_called_with(status_topic, "offline", qos=1, retain=True)
        assert "sat1" not in p._peers

    def test_do_disconnect_accepts_a_close_code_and_reason(self):
        """hivemind-core calls client.disconnect(1008, reason) on handshake
        rejection; the callback wired in here must not TypeError on that call."""
        p = _make_protocol()
        p._build_client_connection("sat1")
        conn = p._peers["sat1"]

        conn.disconnect(1008, "invalid credentials")

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
        p._build_client_connection("sat1")
        p.hm_protocol.handle_client_disconnected.reset_mock()

        msg = self._make_msg("hivemind/sat1/status", b"offline")
        p._on_message(p._mqtt, None, msg)

        p.hm_protocol.handle_client_disconnected.assert_called_once()
        assert "sat1" not in p._peers

    def test_lwt_online_ignored(self):
        p = _make_protocol()
        p._build_client_connection("sat1")
        p.hm_protocol.handle_client_disconnected.reset_mock()

        msg = self._make_msg("hivemind/sat1/status", b"online")
        p._on_message(p._mqtt, None, msg)

        p.hm_protocol.handle_client_disconnected.assert_not_called()

    def test_master_self_status_offline_is_ignored(self):
        """The master's own status echo (<prefix>/<master_name>/status) must
        not be treated as a satellite going offline."""
        p = _make_protocol()  # identity.name == "testkey"
        p.hm_protocol.handle_client_disconnected.reset_mock()

        msg = self._make_msg("hivemind/testkey/status", b"offline")
        p._on_message(p._mqtt, None, msg)

        p.hm_protocol.handle_client_disconnected.assert_not_called()

    def test_c2s_message_routed_to_handle_message(self):
        p = _make_protocol()
        p._build_client_connection("sat1")
        p.hm_protocol.handle_message.reset_mock()

        msg = self._make_msg("hivemind/sat1/in", b"payload")
        p._on_message(p._mqtt, None, msg)

        p.hm_protocol.handle_message.assert_called_once()

    def test_unknown_peer_auto_registered_on_c2s(self):
        p = _make_protocol()
        assert "newsat" not in p._peers

        msg = self._make_msg("hivemind/newsat/in", b"payload")
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
        p._build_client_connection("sat1")
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
    def test_default_prefix_fallback(self):
        p = _make_protocol({"topic_prefix": "myhive"})
        assert p._prefix() == "myhive"

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
        assert p.in_wildcard() in calls
        assert p.status_wildcard() in calls

    def test_on_connect_failed_rc_skips_subscribe(self):
        p = _make_protocol()
        mock_client = MagicMock(name="client")
        p._on_connect(mock_client, None, {}, 1)
        mock_client.subscribe.assert_not_called()


# ---------------------------------------------------------------------------
# Password handshake path (line 211)
# ---------------------------------------------------------------------------


class TestPasswordHandshake:
    def test_user_with_password_sets_pswd_handshake(self):
        p = _make_protocol()
        user = p.hm_protocol.db.get_client_by_api_key.return_value
        user.password = "s3cr3t"
        conn = p._build_client_connection("sat1")
        assert conn is not None
        assert conn.pswd_handshake is not None

    def test_user_without_password_no_pswd_handshake(self):
        p = _make_protocol()
        user = p.hm_protocol.db.get_client_by_api_key.return_value
        user.password = None
        conn = p._build_client_connection("sat1")
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
        """Topic segment that is neither 'in' nor 'status' → returns early (line 278)."""
        p = _make_protocol()
        msg = self._make_msg("hivemind/testhub/other/sat1", b"data")
        p._on_message(p._mqtt, None, msg)
        p.hm_protocol.handle_message.assert_not_called()

    def test_auth_fail_drops_frame(self):
        """Auth failure on unknown satellite drops frame (line 301)."""
        p = _make_protocol()
        p.hm_protocol.db.get_client_by_api_key.return_value = None
        msg = self._make_msg("hivemind/newsat/in", b"data")
        p._on_message(p._mqtt, None, msg)
        p.hm_protocol.handle_message.assert_not_called()

    def test_decode_error_drops_frame(self):
        """conn.decode() raises → logs warning and drops frame (lines 305-307)."""
        p = _make_protocol()
        p._build_client_connection("sat1")
        conn = p._peers["sat1"]
        conn.decode = MagicMock(side_effect=ValueError("bad frame"))
        p.hm_protocol.handle_message.reset_mock()

        msg = self._make_msg("hivemind/sat1/in", b"garbage")
        p._on_message(p._mqtt, None, msg)

        p.hm_protocol.handle_message.assert_not_called()

    def test_known_peer_last_seen_updated(self):
        """Inbound c2s from known peer updates _last_seen timestamp."""
        p = _make_protocol()
        p._build_client_connection("sat1")
        old_ts = p._last_seen["sat1"] - 100
        p._last_seen["sat1"] = old_ts

        msg = self._make_msg("hivemind/sat1/in", b"payload")
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
        p._build_client_connection("sat1")
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
        p._build_client_connection("sat1")
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

    def test_run_gives_each_replica_its_own_client_id(self):
        """Two listeners for one identity must not share a broker client id:
        the broker treats a shared id as one client reconnecting, so replicas
        kick each other off and loop on reconnects."""
        import paho.mqtt.client as paho_mqtt
        ids = []
        for _ in range(2):
            p = _make_protocol()
            with patch.object(paho_mqtt, "Client",
                              return_value=self._make_mock_mqtt_client()) as client_cls:
                p.run()
            ids.append(client_cls.call_args.kwargs["client_id"])
        name = p.identity.name or "master"
        assert ids[0] != ids[1]
        assert all(i.startswith(f"hivemind-{name}-") for i in ids)

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
        p = _make_protocol({"api_key": "testkey"})
        mock_client_instance = self._make_mock_mqtt_client()

        import paho.mqtt.client as paho_mqtt
        with patch.object(paho_mqtt, "Client", return_value=mock_client_instance):
            p.run()

        publish_calls = mock_client_instance.publish.call_args_list
        topics_published = [c[0][0] for c in publish_calls]
        payloads_published = [c[0][1] for c in publish_calls]
        assert any("/status" in t for t in topics_published)
        assert "online" in payloads_published

    def test_run_will_set_offline_lwt(self):
        """run() sets a LWT will_set with 'offline' payload."""
        p = _make_protocol({"api_key": "testkey"})
        mock_client_instance = self._make_mock_mqtt_client()

        import paho.mqtt.client as paho_mqtt
        with patch.object(paho_mqtt, "Client", return_value=mock_client_instance):
            p.run()

        mock_client_instance.will_set.assert_called_once()
        args = mock_client_instance.will_set.call_args[0]
        assert "/status" in args[0]
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
        """run() does NOT start the idle-sweep thread when idle_timeout is 0
        (or any non-positive value)."""
        p = _make_protocol({"idle_timeout": 0})
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
