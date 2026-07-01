"""Live ROS2 topic relay via rclpy (optional dependency).

Subscribes to live ROS2 topics and relays parsed messages to the bridge
server via a callback. Uses raw CDR parsing from resurrector.ingest.parser
to avoid needing compiled message types on the client.

Requires rclpy: pip install 'rosbag-resurrector[bridge-live]'
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable

logger = logging.getLogger("resurrector.bridge.live")


def is_rclpy_available() -> bool:
    """Check if rclpy is importable."""
    try:
        import rclpy
        return True
    except ImportError:
        return False


class LiveSubscriber:
    """Subscribe to live ROS2 topics and relay parsed messages.

    Uses rclpy in a background thread. Message callbacks are invoked
    on the rclpy thread — the server should handle thread-safety.
    """

    def __init__(
        self,
        topics: list[str] | None = None,
        message_callback: Callable | None = None,
        on_topic_discovered: Callable | None = None,
    ):
        """Construct a live subscriber.

        Args:
            topics: Topics to subscribe to on start().
            message_callback: Called for each incoming message
                (Message dict — raw_data is populated as of v0.6).
            on_topic_discovered: Called once per unique topic with a
                TopicInfo describing the topic's schema. Used by the
                bridge to feed BridgeRecorder.register_topic_info so
                live-mode recording works.
        """
        if not is_rclpy_available():
            raise ImportError(
                "Live mode requires rclpy (ROS2 Python client). "
                "Install ROS2 or use playback mode instead."
            )

        import rclpy
        from rclpy.node import Node

        self._callback = message_callback
        self._on_topic_discovered = on_topic_discovered
        self._requested_topics = topics or []
        self._subscriptions: dict[str, Any] = {}
        self._discovered_topics: set[str] = set()
        self._spin_thread: threading.Thread | None = None
        self._running = False

        # Initialize rclpy if not already done
        if not rclpy.ok():
            rclpy.init()

        self._node = rclpy.create_node("resurrector_bridge")
        logger.info("Created ROS2 node: resurrector_bridge")

    def get_available_topics(self) -> list[dict[str, str]]:
        """Query the ROS2 graph for available topics and types."""
        topic_list = self._node.get_topic_names_and_types()
        return [
            {"name": name, "type": types[0] if types else "unknown"}
            for name, types in topic_list
            if not name.startswith("/rosout") and not name.startswith("/parameter_events")
        ]

    def subscribe(self, topic: str, msg_type: str | None = None) -> None:
        """Subscribe to a topic."""
        if topic in self._subscriptions:
            return

        from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy

        qos = QoSProfile(
            reliability=ReliabilityPolicy.BEST_EFFORT,
            history=HistoryPolicy.KEEP_LAST,
            depth=10,
        )

        # Use raw bytes subscription to avoid needing compiled message types
        # This requires knowing the message type for CDR parsing
        from rclpy.serialization import deserialize_message

        # For generic subscription, we use the AnyMsg approach
        # rclpy doesn't directly support AnyMsg, so we subscribe with
        # a bytes-like interface using the serialized message
        try:
            # Try to get message type from graph if not provided
            if msg_type is None:
                for name, types in self._node.get_topic_names_and_types():
                    if name == topic and types:
                        msg_type = types[0]
                        break

            # Import the message class dynamically
            msg_class = self._import_msg_class(msg_type) if msg_type else None

            if msg_class:
                sub = self._node.create_subscription(
                    msg_class, topic,
                    lambda msg, t=topic, mt=msg_type: self._on_typed_message(t, mt, msg),
                    qos,
                )
            else:
                logger.warning("Cannot determine message type for %s, skipping", topic)
                return

            self._subscriptions[topic] = sub
            logger.info("Subscribed to %s (%s)", topic, msg_type)

        except Exception as e:
            logger.error("Failed to subscribe to %s: %s", topic, e)

    def unsubscribe(self, topic: str) -> None:
        """Unsubscribe from a topic."""
        if topic in self._subscriptions:
            self._node.destroy_subscription(self._subscriptions.pop(topic))
            logger.info("Unsubscribed from %s", topic)

    def start(self) -> None:
        """Start the rclpy spin thread."""
        if self._running:
            return

        # Subscribe to requested topics
        for topic in self._requested_topics:
            self.subscribe(topic)

        self._running = True
        self._spin_thread = threading.Thread(target=self._spin_loop, daemon=True)
        self._spin_thread.start()
        logger.info("Live subscriber started")

    def stop(self) -> None:
        """Shutdown rclpy node and spin thread."""
        self._running = False
        if self._spin_thread:
            self._spin_thread.join(timeout=5.0)
        self._node.destroy_node()
        logger.info("Live subscriber stopped")

    def _spin_loop(self) -> None:
        """Background thread running rclpy spin."""
        import rclpy
        while self._running and rclpy.ok():
            rclpy.spin_once(self._node, timeout_sec=0.1)

    def _on_typed_message(self, topic: str, msg_type: str, msg: Any) -> None:
        """Handle a typed ROS2 message — convert to Resurrector Message format.

        v0.6.0: re-serializes the message to CDR via
        ``rclpy.serialization.serialize_message`` so ``raw_data`` is
        populated, allowing the bridge's recorder to write live-relayed
        messages to MCAP. Also lazily discovers the topic's schema and
        notifies any registered topic-info-discovery callback (used by
        the bridge to feed BridgeRecorder.register_topic_info).
        """
        from resurrector.ingest.parser import Message

        # Re-serialize to CDR — required so the recorder has bytes to write
        raw_data = None
        try:
            from rclpy.serialization import serialize_message
            raw_data = serialize_message(msg)
        except Exception as e:
            logger.debug("serialize_message failed for %s: %s", topic, e)

        # Lazy schema discovery — first message on each topic triggers the
        # topic_info callback so downstream consumers (the recorder) can
        # register the channel.
        if topic not in self._discovered_topics:
            self._discovered_topics.add(topic)
            try:
                topic_info = self._build_topic_info(topic, msg_type, msg)
                if topic_info is not None and self._on_topic_discovered:
                    self._on_topic_discovered(topic_info)
            except Exception as e:
                logger.debug("topic-info introspection failed for %s: %s", topic, e)

        data = self._msg_to_dict(msg)
        timestamp_ns = self._get_timestamp_ns(msg)

        resurrector_msg = Message(
            topic=topic,
            timestamp_ns=timestamp_ns,
            data=data,
            raw_data=raw_data,
            sequence=0,
        )

        if self._callback:
            self._callback(resurrector_msg)

    def _build_topic_info(self, topic: str, msg_type: str, sample_msg: Any) -> Any:
        """Construct a TopicInfo for newly-discovered topic.

        For schema_data we try to grab the message class's docstring
        (rclpy auto-generates one in approximately .msg syntax). If that's
        empty we fall back to a minimal placeholder — the recorded MCAP
        is still readable; downstream type introspection is just thinner.
        """
        from resurrector.ingest.parser import TopicInfo

        msg_class = type(sample_msg)
        # Prefer the IDL string from get_fields_and_field_types for a
        # machine-parseable schema; fall back to the class docstring.
        schema_data = ""
        try:
            fields = sample_msg.get_fields_and_field_types()
            schema_data = "\n".join(f"{t} {n}" for n, t in fields.items())
        except AttributeError:
            schema_data = (msg_class.__doc__ or "").strip()

        if not schema_data:
            schema_data = f"# Placeholder schema for {msg_type}"

        return TopicInfo(
            name=topic,
            message_type=msg_type,
            message_count=0,  # unknown for live; recorder doesn't use this field
            frequency_hz=None,
            schema_encoding="ros2msg",
            schema_data=schema_data,
        )

    def _msg_to_dict(self, msg: Any) -> dict[str, Any]:
        """Convert a ROS2 message to a nested dict."""
        result = {}
        for field_name in msg.get_fields_and_field_types():
            value = getattr(msg, field_name, None)
            if hasattr(value, 'get_fields_and_field_types'):
                result[field_name] = self._msg_to_dict(value)
            elif isinstance(value, (list, tuple)):
                result[field_name] = [
                    self._msg_to_dict(v) if hasattr(v, 'get_fields_and_field_types') else v
                    for v in value
                ]
            else:
                result[field_name] = value
        return result

    def _get_timestamp_ns(self, msg: Any) -> int:
        """Extract timestamp from a ROS2 message header."""
        import time
        if hasattr(msg, 'header') and hasattr(msg.header, 'stamp'):
            stamp = msg.header.stamp
            return stamp.sec * 1_000_000_000 + stamp.nanosec
        return int(time.time() * 1e9)

    @staticmethod
    def _import_msg_class(msg_type: str) -> Any:
        """Dynamically import a ROS2 message class from its type string.

        E.g., "sensor_msgs/msg/Imu" -> sensor_msgs.msg.Imu
        """
        parts = msg_type.replace("/", ".")
        module_path = ".".join(parts.split(".")[:-1])
        class_name = parts.split(".")[-1]
        try:
            import importlib
            module = importlib.import_module(module_path)
            return getattr(module, class_name)
        except (ImportError, AttributeError) as e:
            logger.warning("Could not import %s: %s", msg_type, e)
            return None
