"""A real satellite that talks to the master over the in-process MQTT broker.

This wraps hivescope's :class:`~hivescope.node.SatelliteNode` (which itself
drives a *real* ``HiveMindSlaveProtocol``) but replaces the in-process direct
wiring with genuine MQTT publish / subscribe against a
:class:`tests.e2e.broker.FakeMqttBroker`.

Outbound frames are encoded exactly as a production satellite encodes them
(mirroring ``HiveMessageBusClient.emit`` — plaintext JSON until the Noise
handshake completes, Noise transport frames from then on) and
published to ``<prefix>/<api_key>/in``.  Inbound frames arrive on
``<prefix>/<api_key>/out``, are decoded, and dispatched through the slave
protocol's handlers.

The api_key on the topic is the satellite's own access key — exactly the
production scheme.
"""
from __future__ import annotations

import json
from typing import Union

from ovos_bus_client.message import Message
from ovos_bus_client.session import Session
from hivemind_bus_client.message import HiveMessage, HiveMessageType
from hivemind_bus_client.serialization import decode_bitstring
from hivescope.node import SatelliteNode

from .broker import FakeMqttBroker


class MqttSatellite:
    """Drive a real ``HiveMindSlaveProtocol`` over MQTT topics.

    Parameters mirror what a deployed satellite needs: the broker to attach
    to, the topic prefix, and the (already DB-registered) master node so the
    satellite's api_key is admitted.
    """

    def __init__(self, broker: FakeMqttBroker, master,
                 name: str = "mqtt-sat", prefix: str = "hivemind") -> None:
        self.broker = broker
        self.master = master
        self.prefix = prefix
        # The real slave protocol lives inside the hivescope SatelliteNode.
        self.node: SatelliteNode = SatelliteNode.create(name)
        self.api_key: str = self.node.identity.access_key

        # MQTT client for this satellite.
        self.client = broker.make_client()
        self.client.on_message = self._on_message

    # -- topics --------------------------------------------------------

    @property
    def in_topic(self) -> str:
        return f"{self.prefix}/{self.api_key}/in"

    @property
    def out_topic(self) -> str:
        return f"{self.prefix}/{self.api_key}/out"

    @property
    def status_topic(self) -> str:
        return f"{self.prefix}/{self.api_key}/status"

    # -- lifecycle -----------------------------------------------------

    def connect(self, allowed_types=None) -> None:
        """Register with the master, subscribe, announce presence, handshake.

        Mirrors a production satellite startup:
          1. master admits this api_key (DB row).
          2. MQTT connect + LWT on the status topic + retained ``online``.
          3. subscribe to the outbound topic.
          4. send a bootstrap HELLO upstream. Because MQTT has no connection
             event, this first inbound frame is what makes the master create
             the logical connection and reply with its own HELLO + a HANDSHAKE
             *request*; the satellite then performs exactly one password
             handshake in response. (Initiating the handshake from the
             satellite side instead would race the master's request and derive
             a mismatched key.)
        """
        # 1. DB admission for this satellite's own key.
        self.master.register_satellite(
            key=self.api_key,
            password=self.node.identity.password,
            allowed_types=allowed_types,
        )

        # 2. presence: retained LWT + online announcement.
        self.client.will_set(self.status_topic, "offline", qos=1, retain=True)
        self.client.connect("localhost", 1883)
        self.client.publish(self.status_topic, "online", qos=1, retain=True)

        # 3. listen for master → satellite frames.
        self.client.subscribe(self.out_topic, qos=1)

        # 4. wire the slave protocol's transport to MQTT, then bootstrap.
        shim = self.node.shim
        shim.emit = self._emit_upstream  # type: ignore[method-assign]
        self.node._master = self.master  # satisfy SatelliteNode.send guards

        sess = Session(session_id=shim.session_id,
                       site_id=self.node.identity.site_id or "unknown")
        bootstrap = HiveMessage(
            HiveMessageType.HELLO,
            {"pubkey": self.node.identity.public_key,
             "session": sess.serialize(),
             "site_id": self.node.identity.site_id},
        )
        shim.emit(bootstrap)

    def emit_bus(self, message: Message) -> None:
        """Send an OVOS bus message upstream, injecting session context.

        Mirrors what a production satellite's ``HiveMessageBusClient`` does
        before emitting, so the master does not reject it as a 'default'
        session message.
        """
        if "session" not in message.context:
            sess = Session(session_id=self.node.shim.session_id,
                           site_id=self.node.identity.site_id or "unknown")
            message.context["session"] = sess.serialize()
        self.node.shim.emit(HiveMessage(HiveMessageType.BUS, payload=message))

    def disconnect(self, *, ungraceful: bool = False) -> None:
        if ungraceful:
            # Drop without a clean OFFLINE publish; the broker fires the LWT.
            self.client.force_will()
        else:
            self.client.publish(self.status_topic, "offline", qos=1, retain=True)
            self.client.disconnect()

    # -- transport -----------------------------------------------------
    #
    # Mirrors HiveMessageBusClient: before the Noise handshake completes every
    # frame is plaintext JSON; once the slave protocol installs the transport
    # on the shim, every frame in both directions is a Noise transport
    # message (the post-handshake HELLO included).

    def _emit_upstream(self, message: Union[HiveMessage, Message]) -> None:
        """Publish a HiveMessage to the inbound topic."""
        if isinstance(message, Message):
            message = HiveMessage(HiveMessageType.BUS, payload=message)
        self.node.recorder.record("out", message.msg_type, message._payload, "master")
        transport = getattr(self.node.shim, "noise_transport", None)
        if transport is None:
            self.client.publish(self.in_topic, message.serialize(), qos=1)
            return
        transport.send_message(
            message.serialize(),
            lambda frame: self.client.publish(self.in_topic, frame, qos=1))

    def _decode(self, payload) -> Union[HiveMessage, None]:
        transport = getattr(self.node.shim, "noise_transport", None)
        if transport is not None:
            payload = transport.decrypt_frame(bytes(payload))
            if payload is None:  # one chunk of a multi-frame message
                return None
        if isinstance(payload, (bytes, bytearray)):
            try:
                payload = bytes(payload).decode("utf-8")
            except UnicodeDecodeError:
                return decode_bitstring(bytes(payload))
        return HiveMessage(**json.loads(payload))

    def _on_message(self, client, userdata, msg) -> None:
        """Master → satellite: decode and dispatch through the slave handlers."""
        if not msg.topic.endswith("/out"):
            return
        try:
            message = self._decode(msg.payload)
        except Exception:
            return
        if message is None:
            return
        self.node.recorder.record("in", message.msg_type, message._payload, "master")
        # The slave protocol registered its handlers on shim.emitter via on().
        self.node.shim.emitter.emit(message.msg_type, message)
