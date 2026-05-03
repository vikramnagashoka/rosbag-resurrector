"""Tests for the 3D scene primitives in resurrector/core/scene.py.

Covers:
- TFTree insert / lookup / chain math (identity, parent-of, sibling, broken)
- Static-vs-dynamic transform precedence
- TF history bound enforcement
- TFMessage CDR parser round-trip
- PointCloud2 CDR parser round-trip + xyz decode + decimation + NaN filtering
"""

from __future__ import annotations

import struct

import numpy as np
import pytest

from resurrector.core.scene import (
    PointCloud2Meta,
    TFTree,
    Transform,
    decode_pointcloud2_xyz,
    parse_pointcloud2_meta,
    parse_tf_message,
)


# ---------------------------------------------------------------------------
# CDR builders (synthetic test fixtures)
# ---------------------------------------------------------------------------


def _build_string(s: str) -> bytes:
    raw = s.encode("utf-8") + b"\x00"
    return struct.pack("<I", len(raw)) + raw


def _align(buf: bytes, alignment: int) -> bytes:
    pad = (-len(buf)) % alignment
    return buf + b"\x00" * pad


def _build_tf_message(transforms: list[tuple[str, str, int, tuple, tuple]]) -> bytes:
    """Build a synthetic tf2_msgs/TFMessage CDR payload.

    Each input tuple is (parent, child, timestamp_ns, translation, quat).
    """
    cdr = b"\x00\x01\x00\x00"
    body = struct.pack("<I", len(transforms))
    for parent, child, ts_ns, trans, quat in transforms:
        sec = ts_ns // 1_000_000_000
        nsec = ts_ns % 1_000_000_000
        body += struct.pack("<iI", sec, nsec)
        body += _build_string(parent)
        body += _build_string(child)
        body = _align(body, 8)
        body += struct.pack("<3d", *trans)
        body += struct.pack("<4d", *quat)
    return cdr + body


def _build_pointcloud2(
    points: np.ndarray, frame_id: str = "lidar_link",
) -> bytes:
    """Build a PointCloud2 CDR payload from an Nx3 float32 array."""
    n = points.shape[0]
    cdr = b"\x00\x01\x00\x00"
    body = struct.pack("<iI", 0, 0)  # header sec, nsec
    body += _build_string(frame_id)
    body = _align(body, 4)
    body += struct.pack("<II", 1, n)  # height=1, width=n
    body += struct.pack("<I", 3)  # 3 fields
    for name, off in (("x", 0), ("y", 4), ("z", 8)):
        body += _build_string(name)
        body = _align(body, 4)
        body += struct.pack("<I", off)
        body += struct.pack("<B", 7)  # FLOAT32
        body += b"\x00\x00\x00"
        body += struct.pack("<I", 1)  # count
    body += struct.pack("<B", 0)  # is_bigendian
    body = _align(body, 4)
    body += struct.pack("<II", 12, 12 * n)  # point_step, row_step
    body += struct.pack("<I", points.nbytes)
    body += points.astype(np.float32).tobytes()
    body += struct.pack("<B", 1)  # is_dense
    return cdr + body


# ---------------------------------------------------------------------------
# TFTree
# ---------------------------------------------------------------------------


class TestTFTreeBasics:
    def test_empty_tree(self):
        tree = TFTree()
        assert tree.frames() == set()
        assert tree.root_frames() == []
        assert tree.lookup_at("anything", 0) is None
        assert tree.chain("a", "b", 0) is None

    def test_add_static_transform(self):
        tree = TFTree()
        tf = Transform("world", "base", 0, (1.0, 0, 0), (0, 0, 0, 1), is_static=True)
        tree.add(tf)
        assert "world" in tree.frames()
        assert "base" in tree.frames()
        # Lookup at any time returns the static
        assert tree.lookup_at("base", 0) == tf
        assert tree.lookup_at("base", 999_999_999_999) == tf

    def test_add_dynamic_transform(self):
        tree = TFTree()
        tf1 = Transform("world", "base", 1000, (1.0, 0, 0), (0, 0, 0, 1))
        tf2 = Transform("world", "base", 2000, (2.0, 0, 0), (0, 0, 0, 1))
        tree.add(tf1)
        tree.add(tf2)
        assert tree.lookup_at("base", 1000).translation == (1.0, 0, 0)
        assert tree.lookup_at("base", 2000).translation == (2.0, 0, 0)
        # Nearest-time
        assert tree.lookup_at("base", 1100).translation == (1.0, 0, 0)
        assert tree.lookup_at("base", 1900).translation == (2.0, 0, 0)
        # Outside range — returns boundary
        assert tree.lookup_at("base", 0).translation == (1.0, 0, 0)
        assert tree.lookup_at("base", 9999).translation == (2.0, 0, 0)

    def test_static_overrides_dynamic_lookup(self):
        """If both static and dynamic exist for a child, static wins (matches tf2 behavior)."""
        tree = TFTree()
        tree.add(Transform("world", "base", 0, (10, 0, 0), (0, 0, 0, 1)))
        tree.add(Transform("world", "base", 0, (1, 0, 0), (0, 0, 0, 1), is_static=True))
        assert tree.lookup_at("base", 999).translation == (1, 0, 0)

    def test_root_frames(self):
        tree = TFTree()
        tree.add(Transform("world", "base", 0, (0, 0, 0), (0, 0, 0, 1), is_static=True))
        tree.add(Transform("base", "imu", 0, (0, 0, 0), (0, 0, 0, 1), is_static=True))
        tree.add(Transform("base", "lidar", 0, (0, 0, 0), (0, 0, 0, 1), is_static=True))
        assert tree.root_frames() == ["world"]

    def test_history_eviction(self):
        tree = TFTree(max_history=5)
        for i in range(10):
            tree.add(Transform("a", "b", i * 1000, (float(i), 0, 0), (0, 0, 0, 1)))
        # Only the last 5 should remain
        # Earliest reachable now is i=5 (translation x=5.0)
        assert tree.lookup_at("b", 0).translation == (5.0, 0, 0)
        assert tree.lookup_at("b", 9000).translation == (9.0, 0, 0)

    def test_out_of_order_insert_keeps_sorted(self):
        tree = TFTree()
        tree.add(Transform("a", "b", 3000, (3.0, 0, 0), (0, 0, 0, 1)))
        tree.add(Transform("a", "b", 1000, (1.0, 0, 0), (0, 0, 0, 1)))
        tree.add(Transform("a", "b", 2000, (2.0, 0, 0), (0, 0, 0, 1)))
        assert tree.lookup_at("b", 1500).translation == (1.0, 0, 0)
        assert tree.lookup_at("b", 2500).translation == (2.0, 0, 0)


class TestTFTreeChain:
    def setup_method(self):
        self.tree = TFTree()
        # world ─┬─ base ──┬── imu
        #        │         └── camera
        self.tree.add(Transform("world", "base", 0, (1.0, 0, 0), (0, 0, 0, 1), is_static=True))
        self.tree.add(Transform("base", "imu", 0, (0, 0.5, 0.1), (0, 0, 0, 1), is_static=True))
        self.tree.add(Transform("base", "camera", 0, (0.2, 0, 0.3), (0, 0, 0, 1), is_static=True))

    def test_identity_chain(self):
        m = self.tree.chain("imu", "imu", 0)
        np.testing.assert_array_equal(m, np.eye(4))

    def test_parent_chain(self):
        # world ← imu → translation = (1.0, 0.5, 0.1)
        m = self.tree.chain("world", "imu", 0)
        np.testing.assert_allclose(m[0:3, 3], (1.0, 0.5, 0.1), atol=1e-9)

    def test_inverse_chain(self):
        # imu ← world → translation = (-1.0, -0.5, -0.1)
        m = self.tree.chain("imu", "world", 0)
        np.testing.assert_allclose(m[0:3, 3], (-1.0, -0.5, -0.1), atol=1e-9)

    def test_sibling_chain(self):
        # camera ← imu = (camera ← base) @ (base ← imu)
        # = inv(0.2, 0, 0.3) @ (0, 0.5, 0.1) = (-0.2, 0.5, -0.2)
        m = self.tree.chain("camera", "imu", 0)
        np.testing.assert_allclose(m[0:3, 3], (-0.2, 0.5, -0.2), atol=1e-9)

    def test_unknown_frame_returns_none(self):
        assert self.tree.chain("world", "ghost", 0) is None
        assert self.tree.chain("ghost", "world", 0) is None

    def test_disconnected_frames_returns_none(self):
        tree = TFTree()
        tree.add(Transform("a", "b", 0, (0, 0, 0), (0, 0, 0, 1), is_static=True))
        tree.add(Transform("c", "d", 0, (0, 0, 0), (0, 0, 0, 1), is_static=True))
        assert tree.chain("b", "d", 0) is None


class TestTFTreeRotation:
    def test_90_deg_rotation(self):
        # base → child rotated +90° about z (q = (0, 0, sin(45°), cos(45°)))
        s = np.sin(np.pi / 4)
        c = np.cos(np.pi / 4)
        tree = TFTree()
        tree.add(Transform("base", "child", 0, (0, 0, 0), (0, 0, s, c), is_static=True))
        m = tree.chain("base", "child", 0)
        # Point (1, 0, 0) in child should map to (0, 1, 0) in base
        p_child = np.array([1.0, 0, 0, 1.0])
        p_base = m @ p_child
        np.testing.assert_allclose(p_base[:3], (0, 1, 0), atol=1e-9)

    def test_quaternion_normalization_robust_to_drift(self):
        tree = TFTree()
        # Slightly de-normalized quaternion (norm ≈ 1.001)
        tree.add(Transform("a", "b", 0, (0, 0, 0), (0.001, 0, 0, 1.0), is_static=True))
        m = tree.chain("a", "b", 0)
        # Should still be ~identity rotation, no NaN
        assert m is not None
        assert np.isfinite(m).all()


class TestTFTreeSerialization:
    def test_to_dict_returns_resolved_edges(self):
        tree = TFTree()
        tree.add(Transform("world", "base", 1000, (1.0, 0, 0), (0, 0, 0, 1)))
        tree.add(Transform("base", "imu", 0, (0, 0.5, 0), (0, 0, 0, 1), is_static=True))
        out = tree.to_dict(1000)
        assert out["time_ns"] == 1000
        assert sorted(out["frames"]) == ["base", "imu", "world"]
        assert "world" in out["roots"]
        assert out["static_count"] == 1
        assert out["dynamic_count"] == 1
        assert len(out["edges"]) == 2
        # Each edge has the standard shape
        for edge in out["edges"]:
            assert "parent_frame" in edge
            assert "child_frame" in edge
            assert "translation" in edge
            assert "rotation" in edge


# ---------------------------------------------------------------------------
# parse_tf_message — CDR round-trip
# ---------------------------------------------------------------------------


class TestParseTFMessage:
    def test_single_transform(self):
        raw = _build_tf_message([
            ("world", "base", 100_500_000_000, (1.0, 2.0, 3.0), (0, 0, 0, 1)),
        ])
        out = parse_tf_message(raw, is_static=False)
        assert len(out) == 1
        t = out[0]
        assert t.parent_frame == "world"
        assert t.child_frame == "base"
        assert t.timestamp_ns == 100_500_000_000
        assert t.translation == (1.0, 2.0, 3.0)
        assert t.rotation == (0, 0, 0, 1)
        assert t.is_static is False

    def test_static_flag_propagates(self):
        raw = _build_tf_message([("a", "b", 0, (0, 0, 0), (0, 0, 0, 1))])
        out = parse_tf_message(raw, is_static=True)
        assert out[0].is_static is True

    def test_multiple_transforms(self):
        raw = _build_tf_message([
            ("world", "base", 0, (1, 0, 0), (0, 0, 0, 1)),
            ("base", "imu", 0, (0, 1, 0), (0, 0, 0, 1)),
            ("base", "lidar", 0, (0, 0, 1), (0, 0, 0, 1)),
        ])
        out = parse_tf_message(raw, is_static=False)
        assert len(out) == 3
        assert {t.child_frame for t in out} == {"base", "imu", "lidar"}

    def test_empty_payload_returns_empty(self):
        assert parse_tf_message(b"", is_static=False) == []
        assert parse_tf_message(b"\x00\x01\x00\x00", is_static=False) == []

    def test_malformed_payload_returns_empty(self):
        # Truncated body
        assert parse_tf_message(b"\x00\x01\x00\x00\x05\x00\x00\x00", is_static=False) == []


# ---------------------------------------------------------------------------
# PointCloud2 — parse meta + decode xyz
# ---------------------------------------------------------------------------


class TestPointCloud2:
    def test_parse_meta_basic(self):
        pts = np.array([[1, 2, 3], [4, 5, 6]], dtype=np.float32)
        raw = _build_pointcloud2(pts, frame_id="lidar")
        meta = parse_pointcloud2_meta(raw)
        assert meta is not None
        assert meta.frame_id == "lidar"
        assert meta.height == 1
        assert meta.width == 2
        assert meta.point_step == 12
        assert len(meta.fields) == 3
        assert {f["name"] for f in meta.fields} == {"x", "y", "z"}

    def test_decode_xyz_round_trip(self):
        pts = np.array([
            [1.0, 2.0, 3.0],
            [-4.0, 5.5, 0.0],
            [10.0, -10.0, 100.0],
        ], dtype=np.float32)
        raw = _build_pointcloud2(pts)
        meta = parse_pointcloud2_meta(raw)
        out = decode_pointcloud2_xyz(raw, meta)
        np.testing.assert_array_equal(out, pts)

    def test_decode_decimation(self):
        pts = np.arange(300, dtype=np.float32).reshape(100, 3)
        raw = _build_pointcloud2(pts)
        meta = parse_pointcloud2_meta(raw)
        out = decode_pointcloud2_xyz(raw, meta, max_points=10)
        # Decimation produces ~10 points (could be 10, depending on stride)
        assert 5 <= out.shape[0] <= 15

    def test_decode_filters_nan(self):
        pts = np.array([
            [1.0, 2.0, 3.0],
            [float("nan"), 5.0, 6.0],
            [7.0, 8.0, 9.0],
        ], dtype=np.float32)
        raw = _build_pointcloud2(pts)
        meta = parse_pointcloud2_meta(raw)
        out = decode_pointcloud2_xyz(raw, meta)
        assert out.shape[0] == 2
        np.testing.assert_array_equal(out[0], [1, 2, 3])
        np.testing.assert_array_equal(out[1], [7, 8, 9])

    def test_decode_empty_returns_zero_array(self):
        pts = np.zeros((0, 3), dtype=np.float32)
        raw = _build_pointcloud2(pts)
        meta = parse_pointcloud2_meta(raw)
        out = decode_pointcloud2_xyz(raw, meta)
        assert out.shape == (0, 3)

    def test_malformed_returns_none_meta(self):
        assert parse_pointcloud2_meta(b"") is None
        assert parse_pointcloud2_meta(b"\x00\x01\x00\x00\x00") is None

    def test_decode_without_xyz_returns_none(self):
        # Build a malformed PointCloud2 with non-xyz field names
        cdr = b"\x00\x01\x00\x00"
        body = struct.pack("<iI", 0, 0)
        body += _build_string("x")  # frame_id
        body = _align(body, 4)
        body += struct.pack("<II", 1, 1)
        body += struct.pack("<I", 1)  # 1 field, but it's "intensity" not xyz
        body += _build_string("intensity")
        body = _align(body, 4)
        body += struct.pack("<I", 0)
        body += struct.pack("<B", 7)
        body += b"\x00\x00\x00"
        body += struct.pack("<I", 1)
        body += struct.pack("<B", 0)
        body = _align(body, 4)
        body += struct.pack("<II", 4, 4)
        body += struct.pack("<I", 4)
        body += struct.pack("<f", 1.5)
        body += struct.pack("<B", 1)
        meta = parse_pointcloud2_meta(cdr + body)
        assert meta is not None
        out = decode_pointcloud2_xyz(cdr + body, meta)
        assert out is None
