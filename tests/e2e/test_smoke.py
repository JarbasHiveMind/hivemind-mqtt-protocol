"""Hivescope smoke test for hivemind-mqtt-protocol.

Uses the in-process hivescope shim — no live MQTT broker required.
The fixture is defined in tests/conftest.py.

Any test that requires a real broker must guard with:
    pytest.importorskip("paho.mqtt.client")
    ... and a skip if the broker is unreachable.
"""
import pytest

paho = pytest.importorskip("paho.mqtt.client", reason="paho-mqtt not installed")

from hivescope.assertions import assert_handshake_complete


def test_hivescope_wiring_handshake(hive):
    """A single-satellite in-process topology completes a handshake."""
    master, satellite = hive
    assert_handshake_complete(master, satellite)
