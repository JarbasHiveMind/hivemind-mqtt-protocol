"""Shared fixtures for hivemind-mqtt-protocol tests.

The hivescope fixture provides an in-process shim (no live broker needed)
for protocol-routing assertions.  Broker-requiring tests must be guarded
with pytest.importorskip or a skip mark.
"""
import pytest

from hivescope.node import MasterNode
from hivescope.scenarios import single_satellite


@pytest.fixture
def hive():
    """Single-satellite in-process topology.

    Yields ``(master, satellite)`` for the standard M0/S0 pair.
    Teardown is automatic.
    """
    builder = single_satellite()
    try:
        builder.start_all()
        yield builder.get_master("M0"), builder.get_satellite("S0")
    finally:
        builder.stop_all()
