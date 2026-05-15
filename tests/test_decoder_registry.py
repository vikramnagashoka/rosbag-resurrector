"""Tests for the v0.6.0 pluggable CDR decoder registry — Sub-feature B.1.

The registry replaces v0.5.x's static if/elif dispatch in
``_parse_cdr_message``. Built-in decoders (Imu, JointState, etc.)
register themselves at module import; users add their own via
``register_decoder()``.

Covers: register / unregister / list_decoders, custom-type happy
path, custom-type exception → fail-closed, re-registration replaces,
built-ins still present after import, no-decoder → _unparsed
placeholder, removing a built-in restores the placeholder.
"""

from __future__ import annotations

import struct

import pytest

from resurrector.ingest.parser import (
    _DECODER_REGISTRY,
    _parse_cdr_message,
    list_decoders,
    register_decoder,
    unregister_decoder,
)


# Save / restore the registry around each test so users can register
# decoders for one test without polluting others.
@pytest.fixture(autouse=True)
def _restore_registry():
    snapshot = dict(_DECODER_REGISTRY)
    yield
    _DECODER_REGISTRY.clear()
    _DECODER_REGISTRY.update(snapshot)


# ---------------------------------------------------------------------------
# register_decoder / unregister_decoder / list_decoders
# ---------------------------------------------------------------------------


class TestRegisterDecoder:
    def test_register_a_custom_type(self):
        called = []

        def decoder(raw):
            called.append(raw)
            return {"value": 42}

        register_decoder("my_pkg/msg/Custom", decoder)
        assert "my_pkg/msg/Custom" in list_decoders()

        result = _parse_cdr_message("my_pkg/msg/Custom", b"\x00\x01\x00\x00abc")
        assert result == {"value": 42}
        assert called == [b"\x00\x01\x00\x00abc"]

    def test_register_replaces_existing(self):
        register_decoder("my_pkg/msg/X", lambda r: {"v": 1})
        register_decoder("my_pkg/msg/X", lambda r: {"v": 2})
        assert _parse_cdr_message("my_pkg/msg/X", b"\x00\x01\x00\x00")["v"] == 2

    def test_register_built_in_message_type_overrides(self):
        # User can override a built-in (e.g. for testing or special handling)
        register_decoder("sensor_msgs/msg/Imu", lambda r: {"override": True})
        result = _parse_cdr_message("sensor_msgs/msg/Imu", b"\x00\x01\x00\x00")
        assert result == {"override": True}

    def test_register_rejects_non_callable(self):
        with pytest.raises(TypeError, match="callable"):
            register_decoder("foo/msg/X", "not a function")  # type: ignore

    def test_register_rejects_empty_msg_type(self):
        with pytest.raises(ValueError, match="empty"):
            register_decoder("", lambda r: {})


class TestUnregisterDecoder:
    def test_unregister_returns_true_when_present(self):
        register_decoder("my_pkg/msg/X", lambda r: {})
        assert unregister_decoder("my_pkg/msg/X") is True
        assert "my_pkg/msg/X" not in list_decoders()

    def test_unregister_returns_false_when_missing(self):
        assert unregister_decoder("never/registered") is False

    def test_unregister_built_in_restores_unparsed_placeholder(self):
        # Removing a built-in is allowed — _unparsed appears again
        unregister_decoder("sensor_msgs/msg/Imu")
        result = _parse_cdr_message("sensor_msgs/msg/Imu", b"\x00\x01\x00\x00")
        assert result.get("_unparsed") is True
        assert result["_msg_type"] == "sensor_msgs/msg/Imu"


# ---------------------------------------------------------------------------
# list_decoders / built-ins are seeded
# ---------------------------------------------------------------------------


class TestBuiltInDecoders:
    def test_all_v0_5_types_present(self):
        names = set(list_decoders())
        assert "sensor_msgs/msg/Imu" in names
        assert "sensor_msgs/msg/JointState" in names
        assert "sensor_msgs/msg/Image" in names
        assert "sensor_msgs/msg/LaserScan" in names
        assert "sensor_msgs/msg/CompressedImage" in names
        assert "tf2_msgs/msg/TFMessage" in names
        assert "sensor_msgs/msg/PointCloud2" in names

    def test_v0_6_marker_types_present(self):
        names = set(list_decoders())
        assert "visualization_msgs/msg/Marker" in names
        assert "visualization_msgs/msg/MarkerArray" in names

    def test_list_decoders_returns_sorted(self):
        names = list_decoders()
        assert names == sorted(names)

    def test_built_in_imu_decode_via_real_synth_bag(self, tmp_path):
        # Use the real synth-bag generator (which produces correctly-
        # CDR-aligned IMU messages) so this test is a true regression
        # check on the registry path rather than on our hand-rolled bytes
        from resurrector.demo.sample_bag import generate_bag, BagConfig
        from resurrector.ingest.parser import parse_bag

        bag = tmp_path / "synth.mcap"
        generate_bag(bag, BagConfig(duration_sec=0.05))
        msgs = list(parse_bag(bag).read_messages(topics=["/imu/data"]))
        assert len(msgs) > 0
        # Decoder routed through registry; must have all the IMU fields
        first = msgs[0]
        assert "linear_acceleration" in first.data
        assert "angular_velocity" in first.data
        assert "orientation" in first.data
        # z-acc is 9.81 + small noise in the synth fixture
        assert 9.0 < first.data["linear_acceleration"]["z"] < 11.0


# ---------------------------------------------------------------------------
# Decoder errors are fail-closed
# ---------------------------------------------------------------------------


class TestDecoderErrors:
    def test_decoder_raising_returns_parse_error(self):
        def bad_decoder(_raw):
            raise ValueError("intentional")
        register_decoder("foo/msg/Bad", bad_decoder)
        result = _parse_cdr_message("foo/msg/Bad", b"\x00\x01\x00\x00abc")
        assert result.get("_parse_error") is True
        assert result["_msg_type"] == "foo/msg/Bad"

    def test_decoder_raising_struct_error_doesnt_crash(self):
        def bad_decoder(_raw):
            struct.unpack_from("<10d", b"x")  # raises struct.error
        register_decoder("foo/msg/StructErr", bad_decoder)
        result = _parse_cdr_message("foo/msg/StructErr", b"\x00\x01\x00\x00")
        assert result.get("_parse_error") is True

    def test_decoder_returning_non_dict_passes_through(self):
        # We don't enforce decoder return type — caller's problem
        register_decoder("foo/msg/Weird", lambda r: ["not", "a", "dict"])
        result = _parse_cdr_message("foo/msg/Weird", b"\x00\x01\x00\x00")
        assert result == ["not", "a", "dict"]

    def test_no_decoder_returns_unparsed_placeholder(self):
        result = _parse_cdr_message("never/registered/Type", b"\x00\x01\x00\x00xyz")
        assert result["_unparsed"] is True
        assert result["_msg_type"] == "never/registered/Type"
        assert result["_raw_size"] == 7

    def test_too_short_payload_returns_empty(self):
        # < 4 bytes can't have a CDR header
        result = _parse_cdr_message("anything", b"")
        assert result == {}
        result = _parse_cdr_message("anything", b"\x00")
        assert result == {}


# ---------------------------------------------------------------------------
# End-to-end: BagFrame uses registered decoders for custom types
# ---------------------------------------------------------------------------


def _generate_bag_with_custom_msg(output_path, topic, msg_type, payload):
    from mcap.writer import Writer
    base_ns = 1_700_000_000_000_000_000
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        writer = Writer(f)
        writer.start(profile="ros2", library="custom-decoder-test")
        sid = writer.register_schema(
            name=msg_type, encoding="ros2msg",
            data=b"# Placeholder schema",
        )
        cid = writer.register_channel(
            topic=topic, message_encoding="cdr", schema_id=sid,
        )
        writer.add_message(
            channel_id=cid, log_time=base_ns, publish_time=base_ns,
            data=payload,
        )
        writer.finish()
    return output_path


class TestEndToEnd:
    def test_custom_decoder_makes_iter_messages_typed(self, tmp_path):
        # Build a tiny "MyVec3" message: header + 3 float64 = 32 bytes after CDR
        bag_path = tmp_path / "custom.mcap"
        cdr = b"\x00\x01\x00\x00"
        body = struct.pack("<3d", 1.5, 2.5, 3.5)
        _generate_bag_with_custom_msg(
            bag_path, "/my_vec", "my_pkg/msg/MyVec3", cdr + body,
        )

        # Without a decoder: data is _unparsed
        from resurrector.ingest.parser import parse_bag
        msgs = list(parse_bag(bag_path).read_messages(topics=["/my_vec"]))
        assert len(msgs) == 1
        assert msgs[0].data["_unparsed"] is True

        # Register a decoder; re-iterate
        def decode_vec3(raw):
            x, y, z = struct.unpack_from("<3d", raw, 4)
            return {"x": x, "y": y, "z": z}
        register_decoder("my_pkg/msg/MyVec3", decode_vec3)

        msgs = list(parse_bag(bag_path).read_messages(topics=["/my_vec"]))
        assert msgs[0].data == {"x": 1.5, "y": 2.5, "z": 3.5}

    def test_custom_decoder_flows_through_to_polars(self, tmp_path):
        # Custom message with two fields; verify BagFrame.to_polars()
        # surfaces them as columns
        bag_path = tmp_path / "custom2.mcap"
        cdr = b"\x00\x01\x00\x00"
        # 3 messages so to_polars has rows
        from mcap.writer import Writer
        base_ns = 1_700_000_000_000_000_000
        with open(bag_path, "wb") as f:
            writer = Writer(f)
            writer.start(profile="ros2", library="custom-decoder-test")
            sid = writer.register_schema(
                name="my_pkg/msg/Pair", encoding="ros2msg",
                data=b"int32 a\nint32 b",
            )
            cid = writer.register_channel(
                topic="/pairs", message_encoding="cdr", schema_id=sid,
            )
            for i in range(3):
                payload = cdr + struct.pack("<ii", i, i * 10)
                writer.add_message(
                    channel_id=cid, log_time=base_ns + i * 1_000_000,
                    publish_time=base_ns + i * 1_000_000, data=payload,
                )
            writer.finish()

        def decode_pair(raw):
            a, b = struct.unpack_from("<ii", raw, 4)
            return {"a": a, "b": b}
        register_decoder("my_pkg/msg/Pair", decode_pair)

        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(bag_path)
        df = bf["/pairs"].to_polars()
        assert "a" in df.columns
        assert "b" in df.columns
        assert df["a"].to_list() == [0, 1, 2]
        assert df["b"].to_list() == [0, 10, 20]
