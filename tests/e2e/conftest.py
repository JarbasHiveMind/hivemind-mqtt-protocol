"""End-to-end fixtures for hivemind-mqtt-protocol.

These exercise the *real* :class:`HiveMindMqttProtocol` against an in-process
MQTT broker double (:class:`tests.e2e.broker.FakeMqttBroker`) — no external
mosquitto, no sockets, no network. The master's ``paho.mqtt.client.Client`` is
patched to the broker's client factory; nothing in ``hivemind_mqtt_protocol``
is modified.

The HiveMind listener protocol + client database come from hivescope's
``MasterNode`` (the same harness the websocket/http protocols use for e2e),
so the master runs the genuine handshake / crypto / policy stack.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass

import paho.mqtt.client as paho
import pytest

from hivescope.node import MasterNode

from hivemind_mqtt_protocol import HiveMindMqttProtocol

from .broker import FakeMqttBroker
from .mqtt_satellite import MqttSatellite


@dataclass
class MqttMaster:
    broker: FakeMqttBroker
    protocol: HiveMindMqttProtocol
    master: MasterNode
    prefix: str

    @property
    def listener(self):
        return self.master.hm_protocol

    @property
    def db(self):
        return self.master.db

    def register_satellite(self, *args, **kwargs):
        return self.master.register_satellite(*args, **kwargs)


@pytest.fixture
def mqtt_master(monkeypatch):
    """A running ``HiveMindMqttProtocol`` master on an in-process broker.

    ``run()`` is executed on a daemon thread (its ``loop_forever`` blocks on
    the broker double, exactly as it would against a real broker). Yields an
    :class:`MqttMaster` handle and tears the master down afterwards.
    """
    broker = FakeMqttBroker()
    # Patch the paho client constructor so the protocol gets a broker-backed
    # client without any change to the protocol source.
    monkeypatch.setattr(paho, "Client", broker.make_client)

    prefix = "hivemind"
    master = MasterNode.create("M0", require_crypto=False, handshake_enabled=True)

    protocol = HiveMindMqttProtocol(
        config={"topic_prefix": prefix, "idle_timeout": -1},
        hm_protocol=master.hm_protocol,
    )
    # NetworkProtocol.identity is read from hm_protocol; ensure the name is set.
    master.hm_protocol.identity.name = "M0"

    thread = threading.Thread(target=protocol.run, daemon=True, name="mqtt-master")
    thread.start()

    # Wait for the broker connection + subscriptions to be in place.
    deadline = 5.0
    waited = 0.0
    while protocol._mqtt is None and waited < deadline:
        threading.Event().wait(0.01)
        waited += 0.01
    assert protocol._mqtt is not None, "master never connected to broker"

    handle = MqttMaster(broker=broker, protocol=protocol, master=master, prefix=prefix)
    try:
        yield handle
    finally:
        try:
            protocol._mqtt.disconnect()
        except Exception:
            pass
        thread.join(timeout=2.0)


@pytest.fixture
def connected_satellite(mqtt_master):
    """A satellite that has completed the MQTT handshake with the master.

    Yields ``(mqtt_master, satellite)``; the satellite is fully connected,
    crypto is active, and it is granted the utterance/speak allowlist so BUS
    round-trip tests work out of the box.
    """
    sat = MqttSatellite(mqtt_master.broker, mqtt_master.master,
                        name="S0", prefix=mqtt_master.prefix)
    sat.connect(allowed_types=["recognizer_loop:utterance", "speak"])
    assert sat.node.shim.handshake_event.wait(timeout=5.0), \
        "satellite handshake did not complete over MQTT"
    # handshake_event fires on the satellite as soon as it derives the key,
    # but the master only finishes registering the logical peer once the
    # satellite's post-handshake session HELLO has round-tripped. Wait for the
    # master to actually track the peer so tests see a fully-established link.
    listener = mqtt_master.listener
    deadline = time.monotonic() + 5.0
    while not listener.clients and time.monotonic() < deadline:
        time.sleep(0.02)
    assert listener.clients, "master never registered the satellite peer"
    try:
        yield mqtt_master, sat
    finally:
        try:
            sat.disconnect()
        except Exception:
            pass
