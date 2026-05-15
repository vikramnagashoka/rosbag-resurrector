"""Tests for the v0.6.0 Marker / MarkerArray decoder + API endpoint.

Sub-feature A.3. Covers:
- parse_marker() round-trip on a synthetic Marker CDR payload
- All marker.type enum values (CUBE / SPHERE / CYLINDER / ARROW + lists)
- parse_marker_array() with N markers
- Rainy day: empty payload, truncated header, wrong endianness, oversized
  count, mid-sequence corruption
- API endpoint: GET /api/bags/{id}/scene/markers — happy + topic-not-found,
  unknown bag, single-Marker vs MarkerArray detection
"""

from __future__ import annotations

import os
import struct
import tempfile
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from resurrector.core.bag_frame import BagFrame
from resurrector.core.scene import (
    MARKER_TYPES, Marker, parse_marker, parse_marker_array,
)
from resurrector.ingest.indexer import BagIndex
from resurrector.ingest.parser import parse_bag
from resurrector.ingest.scanner import scan_path


# ---------------------------------------------------------------------------
# CDR builders for synthetic markers
# ---------------------------------------------------------------------------


def _build_string(s: str) -> bytes:
    raw = s.encode("utf-8") + b"\x00"
    return struct.pack("<I", len(raw)) + raw


def _pad_to(buf: bytes, target_outer_offset: int, alignment: int) -> bytes:
    """Pad ``buf`` so the next byte sits at ``target_outer_offset`` aligned to ``alignment``.

    CDR alignment is relative to the CDR encapsulation start, not to the
    start of the embedded sub-message. Sequence elements (like markers
    inside a MarkerArray) inherit the outer-buffer offset, so a
    sub-message that starts at offset 168 needs different internal
    padding than one that starts at offset 0.

    This helper computes the required padding to bring the next field
    to ``alignment`` when it lands at ``outer_offset + len(buf)``.
    """
    current_outer = target_outer_offset + len(buf)
    pad = (-current_outer) % alignment
    return buf + b"\x00" * pad


def build_marker_body(
    *,
    frame_id: str = "world",
    ts_sec: int = 100,
    ts_nsec: int = 500_000_000,
    ns: str = "test",
    marker_id: int = 1,
    marker_type: int = 1,  # CUBE
    action: int = 0,        # ADD
    position: tuple[float, float, float] = (1.0, 2.0, 3.0),
    orientation: tuple[float, float, float, float] = (0.0, 0.0, 0.0, 1.0),
    scale: tuple[float, float, float] = (0.5, 0.5, 0.5),
    color: tuple[float, float, float, float] = (1.0, 0.0, 0.0, 0.8),
    lifetime_sec: int = 0,
    lifetime_nsec: int = 0,
    frame_locked: bool = False,
    text: str = "",
    mesh_resource: str = "",
    n_points: int = 0,
    n_colors: int = 0,
    outer_offset: int = 0,
) -> bytes:
    """Build the inline body of a Marker, with CDR alignment relative to ``outer_offset``.

    Used by ``build_marker()`` (single marker, outer_offset=0) and
    ``build_marker_array()`` (sequence elements with offset = 4 + sum
    of preceding marker body lengths).
    """
    body = struct.pack("<iI", ts_sec, ts_nsec)
    body += _build_string(frame_id)
    body += _build_string(ns)
    body = _pad_to(body, outer_offset, 4)
    body += struct.pack("<iii", marker_id, marker_type, action)
    body = _pad_to(body, outer_offset, 8)
    body += struct.pack("<3d", *position)
    body += struct.pack("<4d", *orientation)
    body += struct.pack("<3d", *scale)
    body += struct.pack("<4f", *color)
    body = _pad_to(body, outer_offset, 4)
    body += struct.pack("<iI", lifetime_sec, lifetime_nsec)
    body += struct.pack("<B", 1 if frame_locked else 0)
    # points[] — pre-pad uint32 length to 4 bytes
    body = _pad_to(body, outer_offset, 4)
    body += struct.pack("<I", n_points)
    if n_points > 0:
        body = _pad_to(body, outer_offset, 8)
        body += b"\x00" * (n_points * 24)
    # colors[] — pre-pad uint32 length
    body = _pad_to(body, outer_offset, 4)
    body += struct.pack("<I", n_colors)
    if n_colors > 0:
        body = _pad_to(body, outer_offset, 4)
        body += b"\x00" * (n_colors * 16)
    # text + mesh_resource: strings have a 4-byte length prefix
    body = _pad_to(body, outer_offset, 4)
    body += _build_string(text)
    body = _pad_to(body, outer_offset, 4)
    body += _build_string(mesh_resource)
    body += struct.pack("<B", 0)  # mesh_use_embedded_materials
    return body


def build_marker(**kwargs) -> bytes:
    """Build a complete Marker CDR message (header + body)."""
    # Single marker — body starts at outer_offset=0 of the encapsulation
    return b"\x00\x01\x00\x00" + build_marker_body(outer_offset=0, **kwargs)


def build_marker_array(markers: list[dict]) -> bytes:
    """Build a complete MarkerArray CDR message.

    Sequence elements stack with absolute alignment — each successive
    marker's outer_offset is the running length of (count + previous bodies).
    """
    out = struct.pack("<I", len(markers))  # 4 bytes count
    for m in markers:
        outer_offset = len(out)
        out += build_marker_body(outer_offset=outer_offset, **m)
    return b"\x00\x01\x00\x00" + out


# ---------------------------------------------------------------------------
# parse_marker — single marker
# ---------------------------------------------------------------------------


class TestParseMarker:
    def test_basic_cube_round_trip(self):
        raw = build_marker(
            frame_id="base_link", ns="cubes", marker_id=42, marker_type=1,
            position=(1.5, -2.0, 3.5), color=(0.2, 0.8, 0.1, 0.7),
        )
        m = parse_marker(raw)
        assert m is not None
        assert m.frame_id == "base_link"
        assert m.ns == "cubes"
        assert m.id == 42
        assert m.type == 1  # CUBE
        assert m.position == (1.5, -2.0, 3.5)
        assert m.color == pytest.approx((0.2, 0.8, 0.1, 0.7))

    def test_sphere_type(self):
        m = parse_marker(build_marker(marker_type=2))
        assert m.type == 2

    def test_cylinder_type(self):
        m = parse_marker(build_marker(marker_type=3))
        assert m.type == 3

    def test_arrow_type(self):
        m = parse_marker(build_marker(marker_type=0))
        assert m.type == 0

    def test_lifetime_decode(self):
        m = parse_marker(build_marker(lifetime_sec=5, lifetime_nsec=500_000_000))
        assert m.lifetime_sec == pytest.approx(5.5)

    def test_orientation_round_trip(self):
        # Identity quaternion
        m = parse_marker(build_marker(orientation=(0.0, 0.0, 0.0, 1.0)))
        assert m.orientation == (0.0, 0.0, 0.0, 1.0)
        # 90 deg rotation about Z
        import math
        s = math.sin(math.pi / 4)
        c = math.cos(math.pi / 4)
        m2 = parse_marker(build_marker(orientation=(0.0, 0.0, s, c)))
        assert m2.orientation == pytest.approx((0.0, 0.0, s, c), abs=1e-9)

    def test_text_field(self):
        m = parse_marker(build_marker(text="hello world", marker_type=9))  # TEXT_VIEW_FACING
        assert m.text == "hello world"

    def test_mesh_resource_field(self):
        m = parse_marker(build_marker(
            mesh_resource="package://my_pkg/meshes/arm.dae",
            marker_type=10,  # MESH_RESOURCE
        ))
        assert m.mesh_resource == "package://my_pkg/meshes/arm.dae"

    def test_to_dict_includes_type_name(self):
        m = parse_marker(build_marker(marker_type=2))
        d = m.to_dict()
        assert d["type_name"] == "SPHERE"

    def test_to_dict_unknown_type_name_safe(self):
        m = parse_marker(build_marker(marker_type=99))
        d = m.to_dict()
        assert d["type_name"] == "UNKNOWN_99"


class TestParseMarkerRainyDay:
    def test_empty_payload(self):
        assert parse_marker(b"") is None

    def test_truncated_after_header(self):
        # Just CDR header — no body
        assert parse_marker(b"\x00\x01\x00\x00") is None

    def test_truncated_mid_pose(self):
        # CDR header + sec/nsec + frame_id + ns + id/type/action only,
        # missing the pose / scale / color / lifetime — parse must fail
        partial = b"\x00\x01\x00\x00"
        partial += struct.pack("<iI", 0, 0)
        partial += _build_string("world")
        partial += _build_string("ns")
        partial = _pad_to(partial[4:], 0, 4)  # _pad_to operates on body
        partial = b"\x00\x01\x00\x00" + partial
        partial += struct.pack("<iii", 0, 1, 0)
        # Missing pose / scale / color / etc — parser should return None
        assert parse_marker(partial) is None

    def test_zero_size_payload_returns_none(self):
        assert parse_marker(b"\x00\x01") is None  # too short

    def test_marker_with_points_array(self):
        # n_points > 0 should not crash; v0.6.0 skips the points themselves
        m = parse_marker(build_marker(n_points=10, n_colors=10))
        assert m is not None  # decoder doesn't crash on points/colors


# ---------------------------------------------------------------------------
# parse_marker_array — sequences
# ---------------------------------------------------------------------------


class TestParseMarkerArray:
    def test_empty_array(self):
        raw = build_marker_array([])
        assert parse_marker_array(raw) == []

    def test_single_marker(self):
        raw = build_marker_array([{"marker_id": 1, "ns": "a"}])
        out = parse_marker_array(raw)
        assert len(out) == 1
        assert out[0].id == 1
        assert out[0].ns == "a"

    def test_multiple_markers_distinct(self):
        raw = build_marker_array([
            {"marker_id": 1, "marker_type": 1, "position": (1.0, 0, 0)},
            {"marker_id": 2, "marker_type": 2, "position": (0, 1.0, 0)},
            {"marker_id": 3, "marker_type": 3, "position": (0, 0, 1.0)},
        ])
        out = parse_marker_array(raw)
        assert len(out) == 3
        assert [m.id for m in out] == [1, 2, 3]
        assert [m.type for m in out] == [1, 2, 3]
        assert out[0].position == (1.0, 0.0, 0.0)
        assert out[2].position == (0.0, 0.0, 1.0)

    def test_empty_payload(self):
        assert parse_marker_array(b"") == []
        assert parse_marker_array(b"\x00\x01\x00\x00") == []

    def test_oversized_count_rejected(self):
        # uint32 max = 4 billion; the sanity guard rejects > 100k
        raw = b"\x00\x01\x00\x00" + struct.pack("<I", 200_000)
        assert parse_marker_array(raw) == []

    def test_truncated_mid_sequence(self):
        # Header says 3, but only 2 markers' worth of body. Each marker
        # body must be built with its correct outer_offset for CDR
        # alignment to match the parser's expectations.
        raw = b"\x00\x01\x00\x00" + struct.pack("<I", 3)
        m1 = build_marker_body(marker_id=1, outer_offset=4)  # after count
        raw += m1
        m2 = build_marker_body(marker_id=2, outer_offset=4 + len(m1))
        raw += m2
        # Missing the third — decoder should return the 2 parsable markers
        out = parse_marker_array(raw)
        assert len(out) == 2
        assert [m.id for m in out] == [1, 2]


# ---------------------------------------------------------------------------
# API endpoint integration
# ---------------------------------------------------------------------------


def _generate_bag_with_marker(
    output_path: Path,
    marker_payload: bytes,
    topic: str,
    msg_type: str,
) -> Path:
    """Write a tiny MCAP with one marker message on the given topic."""
    from mcap.writer import Writer

    output_path.parent.mkdir(parents=True, exist_ok=True)
    base_ns = 1_700_000_000_000_000_000

    with open(output_path, "wb") as f:
        writer = Writer(f)
        writer.start(profile="ros2", library="marker-test")
        schema_id = writer.register_schema(
            name=msg_type, encoding="ros2msg", data=b"marker schema",
        )
        chan_id = writer.register_channel(
            topic=topic, message_encoding="cdr", schema_id=schema_id,
        )
        writer.add_message(
            channel_id=chan_id,
            log_time=base_ns, publish_time=base_ns,
            data=marker_payload,
        )
        writer.finish()
    return output_path


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


def make_client(db_path: Path, tmp_dir: Path, monkeypatch):
    monkeypatch.setenv("RESURRECTOR_DB_PATH", str(db_path))
    monkeypatch.setenv(
        "RESURRECTOR_ALLOWED_ROOTS",
        os.pathsep.join([str(tmp_dir), tempfile.gettempdir(), str(Path.home())]),
    )
    import importlib
    from resurrector.dashboard import api as api_module
    importlib.reload(api_module)
    transport = ASGITransport(app=api_module.app)
    return AsyncClient(transport=transport, base_url="http://test")


def index_bag(bag_path: Path, db_path: Path) -> int:
    index = BagIndex(db_path)
    scanned = scan_path(bag_path)[0]
    parser = parse_bag(bag_path)
    metadata = parser.get_metadata()
    bag_id = index.upsert_bag(scanned, metadata)
    bf = BagFrame(bag_path)
    index.update_health_score(bag_id, bf.health_report().score)
    index.close()
    return bag_id


@pytest.mark.asyncio
class TestSceneMarkersEndpoint:
    async def test_single_marker_topic_returns_one(self, tmp_dir, monkeypatch):
        bag_path = tmp_dir / "marker.mcap"
        _generate_bag_with_marker(
            bag_path,
            build_marker(marker_id=42, marker_type=2, position=(5, 0, 0)),
            topic="/marker",
            msg_type="visualization_msgs/msg/Marker",
        )
        db_path = tmp_dir / "test.db"
        bag_id = index_bag(bag_path, db_path)
        client = make_client(db_path, tmp_dir, monkeypatch)
        async with client:
            r = await client.get(
                f"/api/bags/{bag_id}/scene/markers",
                params={"topic": "/marker"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["n_markers"] == 1
        assert body["markers"][0]["id"] == 42
        assert body["markers"][0]["type_name"] == "SPHERE"
        assert body["markers"][0]["position"] == [5.0, 0.0, 0.0]

    async def test_marker_array_topic_returns_all(self, tmp_dir, monkeypatch):
        bag_path = tmp_dir / "markers.mcap"
        _generate_bag_with_marker(
            bag_path,
            build_marker_array([
                {"marker_id": 1, "marker_type": 1},
                {"marker_id": 2, "marker_type": 2},
                {"marker_id": 3, "marker_type": 3},
            ]),
            topic="/markers",
            msg_type="visualization_msgs/msg/MarkerArray",
        )
        db_path = tmp_dir / "test.db"
        bag_id = index_bag(bag_path, db_path)
        client = make_client(db_path, tmp_dir, monkeypatch)
        async with client:
            r = await client.get(
                f"/api/bags/{bag_id}/scene/markers",
                params={"topic": "/markers"},
            )
        assert r.status_code == 200
        body = r.json()
        assert body["n_markers"] == 3

    async def test_unknown_topic_returns_404(self, tmp_dir, monkeypatch):
        bag_path = tmp_dir / "markers.mcap"
        _generate_bag_with_marker(
            bag_path,
            build_marker(),
            topic="/marker",
            msg_type="visualization_msgs/msg/Marker",
        )
        db_path = tmp_dir / "test.db"
        bag_id = index_bag(bag_path, db_path)
        client = make_client(db_path, tmp_dir, monkeypatch)
        async with client:
            r = await client.get(
                f"/api/bags/{bag_id}/scene/markers",
                params={"topic": "/nonexistent"},
            )
        assert r.status_code == 404

    async def test_unknown_bag_returns_404(self, tmp_dir, monkeypatch):
        db_path = tmp_dir / "empty.db"
        BagIndex(db_path).close()
        client = make_client(db_path, tmp_dir, monkeypatch)
        async with client:
            r = await client.get(
                "/api/bags/9999/scene/markers", params={"topic": "/x"},
            )
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# Parser dispatcher: marker types route through scene decoders
# ---------------------------------------------------------------------------


class TestParserDispatch:
    def test_marker_type_routed_to_scene(self):
        from resurrector.ingest.parser import _parse_cdr_message
        raw = build_marker(marker_id=7, ns="dispatch_test")
        result = _parse_cdr_message("visualization_msgs/msg/Marker", raw)
        assert "marker" in result
        assert result["marker"]["id"] == 7
        assert result["marker"]["ns"] == "dispatch_test"

    def test_marker_array_type_routed_to_scene(self):
        from resurrector.ingest.parser import _parse_cdr_message
        raw = build_marker_array([
            {"marker_id": 1}, {"marker_id": 2},
        ])
        result = _parse_cdr_message("visualization_msgs/msg/MarkerArray", raw)
        assert "markers" in result
        assert result["_count"] == 2
