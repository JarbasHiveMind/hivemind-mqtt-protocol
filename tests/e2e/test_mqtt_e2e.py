"""End-to-end tests for hivemind-mqtt-protocol over a real in-process broker.

These drive the *actual* ``HiveMindMqttProtocol`` master and a *real*
``HiveMindSlaveProtocol`` satellite, with every frame published / subscribed
through the broker double (``tests/e2e/broker.py``). No external mosquitto, no
sockets, no network — yet the full HiveMind handshake, AES-GCM crypto, BUS
routing and LWT presence run unchanged.

Mirrors the pattern the websocket protocol's e2e suite uses (real protocol,
faithful-but-in-process transport).
"""
import time

from ovos_bus_client.message import Message
from hivemind_bus_client.message import HiveMessage, HiveMessageType


def _wait(predicate, timeout=5.0, interval=0.02):
    deadline = time.monotonic() + timeout
    value = predicate()
    while not value and time.monotonic() < deadline:
        time.sleep(interval)
        value = predicate()
    return value


# --- master bring-up / presence ------------------------------------------

def test_master_announces_online_presence(mqtt_master):
    """run() publishes a retained 'online' to the master's status topic."""
    topic = f"{mqtt_master.prefix}/M0/status"
    assert _wait(lambda: mqtt_master.broker.retained(topic) == b"online")


def test_master_subscribes_to_inbound_and_status_wildcards(mqtt_master):
    """The master subscribes the +/in and +/status wildcards on connect."""
    filters = {s.filter for s in mqtt_master.broker._subs
               if s.client is mqtt_master.protocol._mqtt}
    assert f"{mqtt_master.prefix}/+/in" in filters
    assert f"{mqtt_master.prefix}/+/status" in filters


# --- handshake ------------------------------------------------------------

def test_satellite_completes_handshake_over_mqtt(connected_satellite):
    """HELLO/HANDSHAKE complete and the Noise session is established."""
    _master, sat = connected_satellite
    assert sat.node.shim.handshake_event.is_set()
    assert sat.node.shim.noise_transport is not None, "no Noise transport after handshake"


def test_satellite_appears_as_connected_peer(connected_satellite):
    """After the handshake the master tracks exactly one logical peer."""
    master, sat = connected_satellite
    peers = _wait(lambda: list(master.listener.clients) or None)
    assert peers and len(peers) == 1
    assert sat.api_key in peers[0]


def test_satellite_publishes_retained_online(connected_satellite):
    """The satellite's own status topic carries a retained 'online'."""
    master, sat = connected_satellite
    assert master.broker.retained(sat.status_topic) == b"online"


def test_topic_uses_api_key_directly(connected_satellite):
    """The in/out/status topics are namespaced by the satellite's api_key."""
    _master, sat = connected_satellite
    assert sat.in_topic == f"hivemind/{sat.api_key}/in"
    assert sat.out_topic == f"hivemind/{sat.api_key}/out"
    assert sat.status_topic == f"hivemind/{sat.api_key}/status"


# --- BUS message round-trip ----------------------------------------------

def test_upstream_bus_message_reaches_listener(connected_satellite):
    """Satellite → master: an allowlisted utterance is forwarded to the agent.

    Observed at ``handle_inject_agent_msg`` — the listener hook that forwards
    an admitted BUS frame onto the master's internal agent bus.
    """
    master, sat = connected_satellite
    seen = []
    original = master.listener.handle_inject_agent_msg

    def _capture(message, conn):
        seen.append(message)
        return original(message, conn)

    master.listener.handle_inject_agent_msg = _capture
    try:
        sat.emit_bus(Message("recognizer_loop:utterance",
                             {"utterances": ["hello mqtt"]}))
        assert _wait(lambda: bool(seen))
        assert seen[0].msg_type == "recognizer_loop:utterance"
        assert seen[0].data["utterances"] == ["hello mqtt"]
    finally:
        master.listener.handle_inject_agent_msg = original


def test_upstream_bus_message_is_encrypted_on_the_wire(connected_satellite):
    """The bytes published to the inbound topic are a Noise transport frame,
    never plaintext — the broker only ever sees ciphertext (TRANSPORT-1 §5)."""
    master, sat = connected_satellite
    before = len(master.broker.published)
    sat.emit_bus(Message("recognizer_loop:utterance",
                         {"utterances": ["a very secret phrase"]}))
    assert _wait(lambda: len(master.broker.published) > before)
    inbound = [p for (t, p, _r) in master.broker.published[before:]
               if t == sat.in_topic]
    assert inbound, "nothing published to the inbound topic"
    frame = inbound[-1]
    assert b"a very secret phrase" not in frame
    assert b"recognizer_loop:utterance" not in frame
    assert not frame.lstrip().startswith((b"{", b"[")), "plaintext JSON on the wire"


def test_downstream_bus_message_reaches_satellite_bus(connected_satellite):
    """Master → satellite: a speak is decrypted and hits the internal bus."""
    master, sat = connected_satellite
    peer = _wait(lambda: list(master.listener.clients) or None)[0]

    got = []
    sat.node.internal_bus.on("speak", lambda m: got.append(m))

    master.master.send_to_satellite(
        peer, HiveMessage(HiveMessageType.BUS,
                          payload=Message("speak", {"utterance": "hi there"})))
    assert _wait(lambda: bool(got))
    assert got[0].data["utterance"] == "hi there"


def test_multiple_sequential_messages(connected_satellite):
    """Five utterances in a row all reach the listener, in order."""
    master, sat = connected_satellite
    seen = []
    original = master.listener.handle_inject_agent_msg

    def _capture(message, conn):
        seen.append(message)
        return original(message, conn)

    master.listener.handle_inject_agent_msg = _capture
    try:
        for i in range(5):
            sat.emit_bus(Message("recognizer_loop:utterance",
                                 {"utterances": [f"msg {i}"]}))
        assert _wait(lambda: len(seen) >= 5)
        assert [m.data["utterances"][0] for m in seen[:5]] == \
            [f"msg {i}" for i in range(5)]
    finally:
        master.listener.handle_inject_agent_msg = original


# --- presence / LWT -------------------------------------------------------

def test_graceful_disconnect_evicts_peer(connected_satellite):
    """A clean OFFLINE publish makes the master drop the logical peer."""
    master, sat = connected_satellite
    assert _wait(lambda: bool(master.listener.clients))
    sat.disconnect()
    assert _wait(lambda: not master.listener.clients)
    assert master.broker.retained(sat.status_topic) == b"offline"


def test_ungraceful_drop_fires_lwt_and_evicts_peer(connected_satellite):
    """An ungraceful drop triggers the broker-delivered LWT, evicting the peer."""
    master, sat = connected_satellite
    assert _wait(lambda: bool(master.listener.clients))
    sat.disconnect(ungraceful=True)
    assert _wait(lambda: not master.listener.clients)
    assert master.broker.retained(sat.status_topic) == b"offline"


# --- security: invalid key ------------------------------------------------

def test_unknown_api_key_is_rejected(mqtt_master):
    """A frame on an unregistered api_key topic must not create a peer."""
    client = mqtt_master.broker.make_client()
    client.connect("localhost", 1883)
    client.publish(f"{mqtt_master.prefix}/not-a-real-key/in", b'{"msg_type": "ping"}',
                   qos=1)
    # Give the master a moment to (not) register it.
    time.sleep(0.3)
    assert not mqtt_master.listener.clients
    client.disconnect()
