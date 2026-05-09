"""Tests for BridgeRecorder.register_topic_info — Sub-feature A.5.

Live-mode recording requires the recorder to accept TopicInfo entries
*after* construction (because the live subscriber discovers topics one
message at a time). This test file covers the late-registration API
and verifies it composes correctly with the existing record() path.

The full LiveSubscriber → bridge → recorder integration would need
rclpy installed; that's covered by the documented manual verification
in docs/v0.6.0_build_checkpoints.md. Here we test the seam.
"""

from __future__ import annotations

import struct
import tempfile
from pathlib import Path

import pytest

from resurrector.bridge.recorder import BridgeRecorder
from resurrector.ingest.parser import TopicInfo


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


def _string_msg_cdr(payload: str) -> bytes:
    """A std_msgs/String CDR encoding for tests that just need bytes."""
    raw = payload.encode("utf-8") + b"\x00"
    cdr = b"\x00\x01\x00\x00"
    return cdr + struct.pack("<I", len(raw)) + raw


# ---------------------------------------------------------------------------
# register_topic_info
# ---------------------------------------------------------------------------


class TestRegisterTopicInfo:
    def test_registers_a_topic_after_construction(self, tmp_dir):
        # Recorder constructed with empty topic_info — simulates live mode
        rec = BridgeRecorder(output_path=tmp_dir / "out.mcap", topic_info=[])
        rec.register_topic_info(TopicInfo(
            name="/foo", message_type="std_msgs/msg/String",
            message_count=0, schema_encoding="ros2msg", schema_data="string data",
        ))
        # Recording on /foo should now succeed (no warning about unknown topic)
        rec.record("/foo", 1_000_000_000, _string_msg_cdr("hello"))
        assert rec.messages_written == 1
        rec.close()

    def test_drops_message_for_unregistered_topic(self, tmp_dir):
        # Default behavior — record() on an unregistered topic logs once + drops
        rec = BridgeRecorder(output_path=tmp_dir / "out.mcap", topic_info=[])
        rec.record("/unknown", 1_000_000_000, _string_msg_cdr("x"))
        assert rec.messages_written == 0
        rec.close()

    def test_register_then_record_writes_to_mcap(self, tmp_dir):
        out = tmp_dir / "out.mcap"
        rec = BridgeRecorder(output_path=out, topic_info=[])
        rec.register_topic_info(TopicInfo(
            name="/a", message_type="std_msgs/msg/String",
            message_count=0, schema_encoding="ros2msg", schema_data="string data",
        ))
        for i in range(5):
            rec.record("/a", 1_000_000_000 + i * 100_000, _string_msg_cdr(f"msg-{i}"))
        rec.close()

        # Read the recording back to verify
        from mcap.reader import make_reader
        with open(out, "rb") as f:
            reader = make_reader(f)
            records = list(reader.iter_messages())
        assert len(records) == 5
        assert all(r[1].topic == "/a" for r in records)

    def test_re_register_updates_topic_info(self, tmp_dir):
        rec = BridgeRecorder(output_path=tmp_dir / "out.mcap", topic_info=[])
        # First registration with placeholder schema
        rec.register_topic_info(TopicInfo(
            name="/foo", message_type="std_msgs/msg/String",
            message_count=0, schema_encoding="ros2msg", schema_data="placeholder",
        ))
        rec.record("/foo", 1_000_000_000, _string_msg_cdr("first"))
        # Re-register with a more accurate schema (e.g., updated by introspection)
        rec.register_topic_info(TopicInfo(
            name="/foo", message_type="std_msgs/msg/String",
            message_count=0, schema_encoding="ros2msg", schema_data="updated schema",
        ))
        # Recorder may now re-create the channel; the test verifies no crash
        rec.record("/foo", 1_000_100_000, _string_msg_cdr("second"))
        assert rec.messages_written == 2
        rec.close()

    def test_re_register_after_drop_clears_drop_mark(self, tmp_dir):
        # If a topic was first seen without schema (recorder marked it -1,-1
        # to skip subsequent messages), a later register_topic_info() should
        # clear that mark so the topic gets written.
        rec = BridgeRecorder(output_path=tmp_dir / "out.mcap", topic_info=[])
        # Drop attempt — no topic_info → marked -1, -1
        rec.record("/foo", 1_000_000_000, _string_msg_cdr("dropped"))
        assert rec.messages_written == 0
        # Now register and try again
        rec.register_topic_info(TopicInfo(
            name="/foo", message_type="std_msgs/msg/String",
            message_count=0, schema_encoding="ros2msg", schema_data="real schema",
        ))
        rec.record("/foo", 1_000_100_000, _string_msg_cdr("recorded"))
        assert rec.messages_written == 1
        rec.close()

    def test_register_idempotent(self, tmp_dir):
        rec = BridgeRecorder(output_path=tmp_dir / "out.mcap", topic_info=[])
        info = TopicInfo(
            name="/foo", message_type="std_msgs/msg/String",
            message_count=0, schema_encoding="ros2msg", schema_data="x",
        )
        rec.register_topic_info(info)
        rec.register_topic_info(info)  # same info, no error
        rec.record("/foo", 1_000_000_000, _string_msg_cdr("ok"))
        assert rec.messages_written == 1
        rec.close()


# ---------------------------------------------------------------------------
# Live-mode recorder construction (bridge wiring)
# ---------------------------------------------------------------------------


class TestBridgeLiveModeWiring:
    def test_bridge_constructs_recorder_for_live_mode(self, tmp_dir):
        """When mode='live' + record_path, the bridge constructs an
        empty-topic-info recorder ready for late registration."""
        from resurrector.bridge.server import BridgeServer

        bridge = BridgeServer(
            mode="live", record_path=str(tmp_dir / "live.mcap"),
        )
        assert bridge._recorder is not None
        # Empty topic_info_by_name initially — populated by register_topic_info
        # as the live subscriber discovers topics
        assert bridge._recorder._topic_info_by_name == {}

    def test_playback_mode_still_pre_populates_topics(self, tmp_dir):
        """Playback recording behavior unchanged — topic_info comes from the bag."""
        from resurrector.bridge.server import BridgeServer
        from resurrector.demo.sample_bag import generate_bag, BagConfig

        synth_bag = tmp_dir / "synth.mcap"
        generate_bag(synth_bag, BagConfig(duration_sec=0.5))

        bridge = BridgeServer(
            mode="playback",
            bag_path=synth_bag,
            record_path=str(tmp_dir / "playback.mcap"),
        )
        assert bridge._recorder is not None
        # topic_info_by_name is pre-populated from the bag's metadata
        assert len(bridge._recorder._topic_info_by_name) > 0


# ---------------------------------------------------------------------------
# LiveSubscriber._build_topic_info — schema introspection helper
# ---------------------------------------------------------------------------


class _MockROSMessage:
    """A minimal mock of a rclpy ROS2 message class for testing
    schema-introspection without needing rclpy installed."""

    __doc__ = "A test ROS2 message"

    def __init__(self, fields_and_types: dict[str, str]):
        self._fields_and_types = fields_and_types

    def get_fields_and_field_types(self) -> dict[str, str]:
        return self._fields_and_types


class TestLiveSubscriberIntrospection:
    """Test the schema-introspection helper without instantiating LiveSubscriber.

    Full LiveSubscriber requires rclpy (which our test env doesn't have).
    We exercise the helper via direct class-method access on a mock."""

    def _call_helper(self, topic, msg_type, mock_msg):
        # _build_topic_info doesn't actually use self in its body, so we
        # can call it as an unbound function with a stub self. Bypasses
        # the rclpy requirement of LiveSubscriber.__init__.
        from resurrector.bridge.live import LiveSubscriber
        stub_self = type("S", (), {})()
        return LiveSubscriber._build_topic_info(
            stub_self, topic, msg_type, mock_msg,
        )

    def test_builds_topic_info_from_get_fields_and_field_types(self):
        msg = _MockROSMessage({"x": "float64", "y": "float64", "z": "float64"})
        info = self._call_helper("/foo", "geometry_msgs/msg/Point", msg)
        assert info.name == "/foo"
        assert info.message_type == "geometry_msgs/msg/Point"
        assert info.schema_encoding == "ros2msg"
        # Schema is the field list
        assert "float64 x" in info.schema_data
        assert "float64 y" in info.schema_data
        assert "float64 z" in info.schema_data

    def test_falls_back_to_class_docstring_when_no_fields(self):
        class NoFieldsMsg:
            __doc__ = "Schema as a docstring"
        info = self._call_helper("/foo", "my_pkg/msg/Custom", NoFieldsMsg())
        assert info.schema_data == "Schema as a docstring"

    def test_uses_placeholder_when_no_schema_available(self):
        class EmptyMsg:
            __doc__ = ""
        info = self._call_helper("/foo", "my_pkg/msg/Custom", EmptyMsg())
        assert "Placeholder schema" in info.schema_data
        assert "my_pkg/msg/Custom" in info.schema_data

    def test_message_count_is_zero_for_live(self):
        msg = _MockROSMessage({"x": "int32"})
        info = self._call_helper("/foo", "std_msgs/msg/Int32", msg)
        # Live-mode message_count is unknown; recorder doesn't use it
        assert info.message_count == 0
        assert info.frequency_hz is None
