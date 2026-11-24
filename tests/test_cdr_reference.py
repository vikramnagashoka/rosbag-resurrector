"""Decoding and reading, checked against CDR written by mcap_ros2.

Resurrector's synthetic bag generator and its hand-written decoders once
shared the same padding mistakes (padding after every string, padding
empty float64 arrays), so tests built from the generator passed while
real bags decoded wrong: Image step/pixels, most TF messages, Imu with
some frame_id lengths, MarkerArray, JointState with an empty array.

Every message here is encoded by mcap_ros2, which writes CDR the way
real recorders do, from definitions copied out of real bags
(``tests/fixtures/ros2_definitions.py``). Nothing in this file goes
through Resurrector's own encoders.
"""

from __future__ import annotations

import io
import math
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from mcap.reader import make_reader
from mcap.writer import CompressionType
from mcap_ros2.writer import Writer as Ros2Writer

from resurrector.core.bag_frame import BagFrame, _rows_to_frame
from resurrector.ingest import fast_mcap
from resurrector.ingest.parser import (
    _DECODER_REGISTRY, flatten_into, get_image_array, parse_bag,
)
from resurrector.ingest.ros2msg import (
    CDRDecodeError, UnsupportedSchema, compile_decoder, compile_row_decoder,
)
from tests.fixtures.ros2_definitions import DEFINITIONS

T0 = 1_700_000_000_000_000_000


def _obj(value):
    """Dicts as attribute objects for mcap_ros2's writer, which reads fields
    with getattr first: on a plain dict, a field named ``items`` or
    ``values`` (DiagnosticStatus has one) would find the dict's method."""
    if isinstance(value, dict):
        return SimpleNamespace(**{k: _obj(v) for k, v in value.items()})
    if isinstance(value, list):
        return [_obj(v) for v in value]
    return value


def _cdr(type_name: str, message: dict, definition: str | None = None) -> bytes:
    """One message's CDR bytes, as mcap_ros2 serializes it."""
    buf = io.BytesIO()
    w = Ros2Writer(buf, compression=CompressionType.NONE)
    schema = w.register_msgdef(type_name, definition or DEFINITIONS[type_name])
    w.write_message("/t", schema, _obj(message), log_time=T0, publish_time=T0)
    w.finish()
    buf.seek(0)
    (_, _, msg), = make_reader(buf).iter_messages()
    return bytes(msg.data)


def _header(frame_id: str, sec: int = 12, nanosec: int = 345) -> dict:
    return {"stamp": {"sec": sec, "nanosec": nanosec}, "frame_id": frame_id}


# Frame ids of every length 0..12 put the field after the header string at
# every possible alignment, which is where the old decoders went wrong.
FRAME_IDS = ["", "a", "ab", "abc", "base", "livox", "imu_lk", "imu_lnk",
             "imu_link", "imu_link9", "camera_rgb", "camera_rgb1", "camera_rgb12"]


class TestBuiltinDecoders:
    @pytest.mark.parametrize("frame_id", FRAME_IDS)
    def test_imu(self, frame_id):
        msg = {
            "header": _header(frame_id),
            "orientation": {"x": 0.1, "y": 0.2, "z": 0.3, "w": 0.9},
            "angular_velocity": {"x": 1.5, "y": -2.5, "z": 3.5},
            "linear_acceleration": {"x": -9.8, "y": 0.01, "z": 7.0},
        }
        d = _DECODER_REGISTRY["sensor_msgs/msg/Imu"](_cdr("sensor_msgs/msg/Imu", msg))
        assert d["header"] == {"stamp_sec": 12, "stamp_nsec": 345, "frame_id": frame_id}
        assert d["orientation"] == msg["orientation"]
        assert d["angular_velocity"] == msg["angular_velocity"]
        assert d["linear_acceleration"] == msg["linear_acceleration"]

    @pytest.mark.parametrize("encoding,dtype,channels", [
        ("mono8", np.uint8, 1), ("rgb8", np.uint8, 3), ("bgra8", np.uint8, 4),
        ("mono16", np.uint16, 1), ("16UC1", np.uint16, 1), ("32FC1", np.float32, 1),
    ])
    @pytest.mark.parametrize("frame_id", ["", "cam", "camera_rgb_optical_frame"])
    def test_image_metadata_and_pixels(self, encoding, dtype, channels, frame_id):
        h, w = 3, 5
        pixels = np.arange(h * w * channels, dtype=dtype).reshape(h, w, channels)
        data = pixels.tobytes()
        msg = {"header": _header(frame_id), "height": h, "width": w, "encoding": encoding,
               "is_bigendian": 0, "step": w * channels * pixels.itemsize, "data": data}
        raw = _cdr("sensor_msgs/msg/Image", msg)
        d = _DECODER_REGISTRY["sensor_msgs/msg/Image"](raw)
        assert (d["height"], d["width"], d["encoding"], d["step"], d["data_length"]) == (
            h, w, encoding, msg["step"], len(data))
        start = 4 + d["_pixel_data_offset"]
        assert raw[start:start + d["data_length"]] == data

    def test_image_array_from_real_layout(self, tmp_path):
        h, w = 4, 6
        rgb = np.arange(h * w * 3, dtype=np.uint8).reshape(h, w, 3)
        path = tmp_path / "img.mcap"
        _write_bag(path, [("/cam", "sensor_msgs/msg/Image", {
            "header": _header("cam"), "height": h, "width": w, "encoding": "rgb8",
            "is_bigendian": 0, "step": w * 3, "data": rgb.tobytes()})])
        (msg,) = parse_bag(path).read_messages()
        assert np.array_equal(get_image_array(msg), rgb)

    @pytest.mark.parametrize("position,velocity,effort", [
        ([1.0, 2.0], [], []), ([], [], []), ([1.0], [2.0], []), ([], [0.5], [0.25]),
        ([1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]),
    ])
    @pytest.mark.parametrize("names", [[], ["j1"], ["joint_a", "b"]])
    def test_joint_state_empty_arrays(self, position, velocity, effort, names):
        msg = {"header": _header("base"), "name": names,
               "position": position, "velocity": velocity, "effort": effort}
        d = _DECODER_REGISTRY["sensor_msgs/msg/JointState"](_cdr("sensor_msgs/msg/JointState", msg))
        assert (d["name"], d["position"], d["velocity"], d["effort"]) == (
            names, position, velocity, effort)

    def test_tf_message(self):
        transforms = []
        for i, (parent, child) in enumerate([("map", "odom"), ("odom", "base_link"),
                                             ("base_link", "imu"), ("world", "camera_rgb12")]):
            transforms.append({
                "header": _header(parent, sec=i), "child_frame_id": child,
                "transform": {"translation": {"x": i + 0.5, "y": -1.0, "z": 2.0},
                              "rotation": {"x": 0.0, "y": 0.0, "z": 0.6, "w": 0.8}},
            })
        d = _DECODER_REGISTRY["tf2_msgs/msg/TFMessage"](
            _cdr("tf2_msgs/msg/TFMessage", {"transforms": transforms}))
        got = [(t["parent_frame"], t["child_frame"], tuple(t["translation"]), tuple(t["rotation"]))
               for t in d["transforms"]]
        assert got == [
            (t["header"]["frame_id"], t["child_frame_id"],
             (t["transform"]["translation"]["x"], -1.0, 2.0), (0.0, 0.0, 0.6, 0.8))
            for t in transforms
        ]

    def test_marker_array(self, tmp_path):
        markers = [
            {"header": _header("base"), "ns": "contacts", "id": 1, "type": 2,
             "pose": {"position": {"x": 1.0, "y": 2.0, "z": 3.0}}, "text": "a"},
            {"header": _header("world_frame"), "ns": "b", "id": 7, "type": 1,
             "pose": {"position": {"x": -1.0, "y": 0.5, "z": 0.0}}},
            {"header": _header(""), "ns": "", "id": 9, "type": 3},
        ]
        # The definition here is the Iron+/Jazzy layout (texture and mesh_file
        # fields), so the parser must decode from it rather than from the
        # hand-written Humble-order parser.
        path = _write_bag(Path(tmp_path) / "m.mcap",
                          [("/m", "visualization_msgs/msg/MarkerArray", {"markers": markers})])
        (msg,) = parse_bag(path).read_messages()
        got = msg.data["markers"]
        assert [(m["ns"], m["id"], m["type"]) for m in got] == [("contacts", 1, 2), ("b", 7, 1), ("", 9, 3)]
        assert got[0]["position"] == [1.0, 2.0, 3.0] and got[0]["text"] == "a"
        assert got[1]["frame_id"] == "world_frame" and got[1]["position"] == [-1.0, 0.5, 0.0]


# A definition that exercises every construct the schema compiler handles.
CUSTOM = """\
std_msgs/Header header
bool flag
int8 i8
uint8 u8
int16 i16
uint16 u16
int32 i32
uint32 u32
int64 i64
uint64 u64
float32 f32
float64 f64
string name
string<=8 short_name
float32[3] fixed_f32
float64[] seq_f64
int16[<=4] bounded_i16
uint8[] blob
byte[4] fixed_bytes
string[] tags
my_pkg/Item[] items
my_pkg/Item[2] pair
float64[100] long_fixed
int32[] long_seq
int32 CONSTANT=7
string NAME="x"
================================================================================
MSG: my_pkg/Item
string label
float64 value
uint8 kind
================================================================================
MSG: std_msgs/Header
builtin_interfaces/Time stamp
string frame_id
================================================================================
MSG: builtin_interfaces/Time
int32 sec
uint32 nanosec
"""


def _custom_message(**over) -> dict:
    msg = {
        "header": _header("frame"), "flag": True, "i8": -5, "u8": 250, "i16": -300,
        "u16": 60000, "i32": -70000, "u32": 4_000_000_000, "i64": -(2 ** 40), "u64": 2 ** 50,
        "f32": 1.5, "f64": math.pi, "name": "hello", "short_name": "hi",
        "fixed_f32": [1.0, 2.0, 3.0], "seq_f64": [0.5, 0.25], "bounded_i16": [1, -2, 3],
        "blob": b"\x01\x02\x03", "fixed_bytes": b"abcd", "tags": ["x", "yz", ""],
        "items": [{"label": "a", "value": 1.0, "kind": 1}, {"label": "bcd", "value": -2.0, "kind": 2}],
        "pair": [{"label": "p", "value": 3.0, "kind": 3}, {"label": "", "value": 4.0, "kind": 4}],
        "long_fixed": [float(i) for i in range(100)], "long_seq": list(range(70)),
    }
    msg.update(over)
    return msg


class TestSchemaCompiler:
    def test_every_construct(self):
        msg = _custom_message()
        d = compile_decoder("my_pkg/msg/Custom", CUSTOM)(_cdr("my_pkg/msg/Custom", msg, CUSTOM))
        assert d["header"] == {"stamp_sec": 12, "stamp_nsec": 345, "frame_id": "frame"}
        for key in ("flag", "i8", "u8", "i16", "u16", "i32", "u32", "i64", "u64", "f64",
                    "name", "short_name", "seq_f64", "bounded_i16", "tags", "items", "pair"):
            assert d[key] == msg[key], key
        assert d["f32"] == pytest.approx(1.5)
        assert d["fixed_f32"] == [1.0, 2.0, 3.0]
        assert d["blob"] == b"\x01\x02\x03" and d["fixed_bytes"] == b"abcd"
        # Past LIST_MAX elements, arrays come back as NumPy views.
        assert isinstance(d["long_fixed"], np.ndarray) and d["long_fixed"].tolist() == msg["long_fixed"]
        assert isinstance(d["long_seq"], np.ndarray) and d["long_seq"].tolist() == msg["long_seq"]
        assert "CONSTANT" not in d and "NAME" not in d

    @pytest.mark.parametrize("over", [
        {"seq_f64": [], "bounded_i16": [], "blob": b"", "tags": [], "items": [], "long_seq": []},
        {"name": "", "short_name": "abcdefgh", "seq_f64": [1.0]},
        {"tags": ["a" * 9], "items": [{"label": "x" * 5, "value": 9.0, "kind": 9}]},
    ])
    def test_empty_and_odd_lengths(self, over):
        msg = _custom_message(**over)
        d = compile_decoder("my_pkg/msg/Custom", CUSTOM)(_cdr("my_pkg/msg/Custom", msg, CUSTOM))
        for key, value in over.items():
            got = d[key].tolist() if isinstance(d[key], np.ndarray) else d[key]
            assert got == (list(value) if isinstance(value, list) else value), key

    @pytest.mark.parametrize("type_name,message", [
        ("nav_msgs/msg/Odometry", {
            "header": _header("odom"), "child_frame_id": "base_link",
            "pose": {"pose": {"position": {"x": 1.0, "y": 2.0, "z": 3.0},
                              "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}},
                     "covariance": [float(i) for i in range(36)]},
            "twist": {"twist": {"linear": {"x": 0.1, "y": 0.0, "z": 0.0},
                                "angular": {"x": 0.0, "y": 0.0, "z": 0.2}}}}),
        ("geometry_msgs/msg/PoseStamped", {
            "header": _header("map"), "pose": {"position": {"x": 1.0, "y": 0.0, "z": 0.0}}}),
        ("diagnostic_msgs/msg/DiagnosticArray", {
            "header": _header("diag"), "status": [
                {"level": 1, "name": "motor", "message": "hot", "hardware_id": "m1",
                 "values": [{"key": "temp", "value": "80"}, {"key": "rpm", "value": "1200"}]}]}),
        ("std_msgs/msg/UInt8MultiArray", {
            "layout": {"dim": [{"label": "x", "size": 3, "stride": 3}], "data_offset": 0},
            "data": b"\x07\x08\x09"}),
        ("sensor_msgs/msg/Imu", {"header": _header("livox"), "orientation": {"w": 1.0}}),
    ])
    def test_rows_match_flattened_dicts(self, type_name, message):
        definition = DEFINITIONS[type_name]
        raw = _cdr(type_name, message)
        nested = compile_decoder(type_name, definition)(raw)
        expected = {"timestamp_ns": 42}
        flatten_into(nested, expected)
        row = compile_row_decoder(type_name, definition)(raw, 42)
        assert row == expected and list(row) == list(expected)

    def test_custom_rows_match_flattened_dicts(self):
        raw = _cdr("my_pkg/msg/Custom", _custom_message(), CUSTOM)
        expected = {"timestamp_ns": 1}
        flatten_into(compile_decoder("my_pkg/msg/Custom", CUSTOM)(raw), expected)
        assert compile_row_decoder("my_pkg/msg/Custom", CUSTOM)(raw, 1) == expected

    def test_truncated_message_raises(self):
        raw = _cdr("my_pkg/msg/Custom", _custom_message(), CUSTOM)
        decode = compile_decoder("my_pkg/msg/Custom", CUSTOM)
        with pytest.raises((CDRDecodeError, Exception)):
            decode(raw[: len(raw) // 2])

    def test_wrong_definition_is_caught(self):
        # Bytes of a long message read with a short definition leave a lot over.
        raw = _cdr("my_pkg/msg/Custom", _custom_message(), CUSTOM)
        with pytest.raises(CDRDecodeError, match="does not match"):
            compile_decoder("my_pkg/msg/Small", "int32 a\n")(raw)

    def test_short_type_names_resolve_in_the_same_package(self):
        raw = _cdr("my_pkg/msg/Custom", _custom_message(), CUSTOM)
        short = CUSTOM.replace("my_pkg/Item[] items", "Item[] items").replace(
            "my_pkg/Item[2] pair", "Item[2] pair")
        a = compile_decoder("my_pkg/msg/Custom", short)(raw)
        b = compile_decoder("my_pkg/msg/Custom", CUSTOM)(raw)
        assert a["items"] == b["items"] and a["pair"] == b["pair"]

    def test_fieldless_definition_is_unsupported(self):
        with pytest.raises(UnsupportedSchema):
            compile_decoder("my_pkg/msg/Placeholder", "# nothing here\n")

    def test_big_endian_is_rejected(self):
        raw = bytearray(_cdr("geometry_msgs/msg/PoseStamped", {"header": _header("m")}))
        raw[1] = 0  # CDR_BE
        with pytest.raises(CDRDecodeError):
            compile_decoder("geometry_msgs/msg/PoseStamped",
                            DEFINITIONS["geometry_msgs/msg/PoseStamped"])(bytes(raw))


# ---------------------------------------------------------------------------
# The fast reader against the mcap reader
# ---------------------------------------------------------------------------

def _write_bag(path: Path, messages, *, compression=CompressionType.NONE, chunk_size=1 << 20,
               log_times=None):
    """Write ``(topic, type, message)`` tuples with mcap_ros2. ``log_times``
    overrides the default 1 ms spacing (for out-of-order recordings)."""
    with open(path, "wb") as f:
        w = Ros2Writer(f, compression=compression, chunk_size=chunk_size)
        schemas = {}
        for i, (topic, type_name, msg) in enumerate(messages):
            if type_name not in schemas:
                schemas[type_name] = w.register_msgdef(type_name, DEFINITIONS[type_name])
            t = T0 + (log_times[i] if log_times else i * 1_000_000)
            w.write_message(topic, schemas[type_name], _obj(msg), log_time=t, publish_time=t,
                            sequence=i)
        w.finish()
    return path


def _mixed_messages(n: int):
    out = []
    for i in range(n):
        k = i % 4
        if k == 0:
            out.append(("/imu", "sensor_msgs/msg/Imu", {"header": _header("imu" * (i % 3)),
                        "angular_velocity": {"x": float(i)}}))
        elif k == 1:
            out.append(("/joints", "sensor_msgs/msg/JointState", {
                "header": _header("base"), "name": ["a"], "position": [float(i)],
                "velocity": [] if i % 3 else [1.0]}))
        elif k == 2:
            out.append(("/pose", "geometry_msgs/msg/PoseStamped", {
                "header": _header("map"), "pose": {"position": {"x": float(i)}}}))
        else:
            out.append(("/odom", "nav_msgs/msg/Odometry", {
                "header": _header("odom"), "child_frame_id": "b" * (i % 5)}))
    return out


def _reference(path, topics=None, start=None, end=None):
    with open(path, "rb") as f:
        return [(ch.topic, m.log_time, m.sequence, bytes(m.data))
                for _, ch, m in make_reader(f).iter_messages(topics=topics, start_time=start,
                                                             end_time=end)]


def _fast(path, topics=None, start=None, end=None):
    with open(path, "rb") as f:
        summary = make_reader(f).get_summary()
        assert fast_mcap.can_read(summary)
        names = {cid: ch.topic for cid, ch in summary.channels.items()}
        cids = None if topics is None else [c for c, t in names.items() if t in topics]
        return [(names[m.channel_id], m.log_time, m.sequence, m.data)
                for m in fast_mcap.iter_raw(f, summary, cids, start, end)]


class TestFastReader:
    @pytest.mark.parametrize("compression", [CompressionType.NONE, CompressionType.ZSTD,
                                             CompressionType.LZ4])
    @pytest.mark.parametrize("log_times", ["in_order", "shuffled"])
    def test_same_messages_same_order(self, tmp_path, compression, log_times):
        n = 400
        rng = np.random.default_rng(7)
        times = None
        if log_times == "shuffled":
            # Out-of-order log times make chunks overlap in time, the case the
            # cursor merge exists for. Some duplicates test the tie-break.
            times = (rng.integers(0, n // 2, size=n) * 1_000_000).tolist()
        path = _write_bag(tmp_path / "b.mcap", _mixed_messages(n), compression=compression,
                          chunk_size=2048, log_times=times)
        with open(path, "rb") as f:
            summary = make_reader(f).get_summary()
        assert len(summary.chunk_indexes) > 10
        mid = T0 + (n // 4) * 1_000_000
        cases = [
            dict(), dict(topics=["/imu"]), dict(topics=["/joints", "/odom"]),
            dict(start=mid), dict(end=mid), dict(topics=["/pose"], start=mid - 5_000_000, end=mid),
            dict(topics=[]),
        ]
        for case in cases:
            assert _fast(path, **case) == _reference(path, **case), case

    def test_unindexed_file_falls_back_to_the_mcap_reader(self, tmp_path):
        path = tmp_path / "flat.mcap"
        with open(path, "wb") as f:
            from mcap.writer import Writer
            w = Writer(f, use_chunking=False)
            w.start(profile="ros2", library="test")
            sid = w.register_schema("sensor_msgs/msg/Imu", "ros2msg",
                                    DEFINITIONS["sensor_msgs/msg/Imu"].encode())
            cid = w.register_channel("/imu", "cdr", sid)
            data = _cdr("sensor_msgs/msg/Imu", {"header": _header("imu"),
                                                "linear_acceleration": {"z": 9.8}})
            for i in range(3):
                w.add_message(cid, log_time=T0 + i, publish_time=T0 + i, data=data)
            w.finish()
        with open(path, "rb") as f:
            assert not fast_mcap.can_read(make_reader(f).get_summary())
        msgs = list(parse_bag(path).read_messages())
        assert [m.data["linear_acceleration"]["z"] for m in msgs] == [9.8] * 3


class TestFramesAndHealth:
    def test_iter_chunks_equals_flattened_messages(self, tmp_path):
        path = _write_bag(tmp_path / "b.mcap", _mixed_messages(200), chunk_size=4096)
        bf = BagFrame(path)
        parser = parse_bag(path)
        for topic in bf.topic_names:
            rows = []
            for m in parser.read_messages(topics=[topic]):
                row = {"timestamp_ns": m.timestamp_ns}
                flatten_into(m.data, row)
                rows.append(row)
            (chunk,) = list(bf[topic].iter_chunks(chunk_size=10_000))
            assert chunk.equals(_rows_to_frame(rows)), topic
        # Schema-decoded types are now columns, not timestamp-only rows.
        pose = next(bf["/pose"].iter_chunks())
        assert "pose.position.x" in pose.columns

    def test_health_counts_every_message(self, tmp_path):
        path = _write_bag(tmp_path / "b.mcap", _mixed_messages(200), chunk_size=4096)
        report = BagFrame(path).health_report()
        assert set(report.topic_scores) == {"/imu", "/joints", "/pose", "/odom"}
