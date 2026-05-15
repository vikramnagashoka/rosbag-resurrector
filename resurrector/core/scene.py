"""3D scene primitives: TF tree + PointCloud2 decoding.

Two responsibilities:

1. **TF tree** — accumulate ``geometry_msgs/TransformStamped`` records
   from ``/tf`` and ``/tf_static`` and resolve a transform between any
   two frames at a given timestamp by walking parent chains and
   composing the per-edge transforms.

2. **PointCloud2 decoding** — parse ``sensor_msgs/PointCloud2`` CDR
   metadata (height, width, point_step, fields[]) and decode an Nx3
   float32 array of (x, y, z) coordinates from the payload.

Both surfaces are kept small and pure: the bridge / dashboard wires
them up, but the core math is unit-testable without a bag file.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from typing import Any

import numpy as np


# ---------------------------------------------------------------------------
# TF tree
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Transform:
    """A single parent→child transform at a timestamp.

    Translation is meters, rotation is a unit quaternion (x, y, z, w).
    """
    parent_frame: str
    child_frame: str
    timestamp_ns: int
    translation: tuple[float, float, float]
    rotation: tuple[float, float, float, float]  # (x, y, z, w)
    is_static: bool = False

    def to_matrix(self) -> np.ndarray:
        """Return a 4x4 homogeneous transform matrix."""
        return _tf_to_matrix(self.translation, self.rotation)

    def to_dict(self) -> dict[str, Any]:
        return {
            "parent_frame": self.parent_frame,
            "child_frame": self.child_frame,
            "timestamp_ns": self.timestamp_ns,
            "translation": list(self.translation),
            "rotation": list(self.rotation),
            "is_static": self.is_static,
        }


class TFTree:
    """Accumulates transforms and resolves chains at a given timestamp.

    Static transforms (from ``/tf_static``) are valid for all time.
    Dynamic transforms (from ``/tf``) are stored as a time-sorted list
    per child-frame edge; lookups use the sample nearest in time.

    Memory bound: each child frame stores at most ``max_history``
    dynamic samples (default 10_000), oldest evicted FIFO. For typical
    1-hour bags at 100 Hz, that's ~1.6 minutes of history per edge —
    enough for time-anchored scrubbing within the recent window.
    """

    DEFAULT_MAX_HISTORY = 10_000

    def __init__(self, max_history: int = DEFAULT_MAX_HISTORY):
        # child_frame → list of (timestamp_ns, Transform), sorted ascending
        self._dynamic: dict[str, list[tuple[int, Transform]]] = {}
        # child_frame → static Transform (only one allowed per child)
        self._static: dict[str, Transform] = {}
        self._max_history = max_history

    def add(self, tf: Transform) -> None:
        """Insert a transform. Static and dynamic stored separately.

        For dynamic edges we keep at most ``max_history`` samples per
        child frame to bound memory on long bags.
        """
        if tf.is_static:
            self._static[tf.child_frame] = tf
            return
        history = self._dynamic.setdefault(tf.child_frame, [])
        # Common case: incoming is newer than the tail → just append.
        # Out-of-order: keep the list sorted (rare; cheap to bisect-insert).
        if not history or tf.timestamp_ns >= history[-1][0]:
            history.append((tf.timestamp_ns, tf))
        else:
            import bisect
            idx = bisect.bisect_left([h[0] for h in history], tf.timestamp_ns)
            history.insert(idx, (tf.timestamp_ns, tf))
        # Evict oldest if over capacity
        if len(history) > self._max_history:
            del history[0:len(history) - self._max_history]

    def add_many(self, transforms: list[Transform]) -> None:
        for tf in transforms:
            self.add(tf)

    def frames(self) -> set[str]:
        """Return the set of all known frame names (parents + children)."""
        out: set[str] = set()
        for tf in self._static.values():
            out.add(tf.parent_frame)
            out.add(tf.child_frame)
        for hist in self._dynamic.values():
            for _, tf in hist:
                out.add(tf.parent_frame)
                out.add(tf.child_frame)
        return out

    def root_frames(self) -> list[str]:
        """Return frames that appear as a parent but never as a child."""
        children: set[str] = set(self._static.keys()) | set(self._dynamic.keys())
        parents: set[str] = set()
        for tf in self._static.values():
            parents.add(tf.parent_frame)
        for hist in self._dynamic.values():
            if hist:
                parents.add(hist[-1][1].parent_frame)
        return sorted(parents - children)

    def lookup_at(self, child_frame: str, time_ns: int) -> Transform | None:
        """Return the transform parent→child at the given time, or None."""
        if child_frame in self._static:
            return self._static[child_frame]
        history = self._dynamic.get(child_frame)
        if not history:
            return None
        # Find nearest sample by absolute time difference (no extrapolation
        # beyond the recorded window — return the boundary sample instead)
        idx = _bisect_nearest([h[0] for h in history], time_ns)
        return history[idx][1]

    def chain(
        self, target_frame: str, source_frame: str, time_ns: int,
    ) -> np.ndarray | None:
        """Resolve target←source as a 4x4 matrix at ``time_ns``.

        Walks parent chains from both target and source to find their
        lowest common ancestor, then composes the per-edge transforms.
        The result M satisfies ``p_target = M @ p_source``.

        Returns ``None`` if either frame is unknown or the chains don't
        meet at a common ancestor (broken / disconnected TF tree).
        """
        if target_frame == source_frame:
            return np.eye(4)
        src_ancestors = self._ancestors(source_frame, time_ns)
        tgt_ancestors = self._ancestors(target_frame, time_ns)
        if src_ancestors is None or tgt_ancestors is None:
            return None
        # Either frame must be known (appears as a parent or child somewhere).
        known = self.frames()
        if source_frame not in known or target_frame not in known:
            return None
        tgt_set = set(tgt_ancestors)
        common: str | None = None
        for a in src_ancestors:
            if a in tgt_set:
                common = a
                break
        if common is None:
            return None
        src_to_common = self._transform_from(source_frame, common, time_ns)
        tgt_to_common = self._transform_from(target_frame, common, time_ns)
        if src_to_common is None or tgt_to_common is None:
            return None
        common_to_tgt = np.linalg.inv(tgt_to_common)
        return common_to_tgt @ src_to_common

    def _transform_from(
        self, frame: str, common: str, time_ns: int,
    ) -> np.ndarray | None:
        """Matrix that takes a point in ``frame`` and gives it in ``common``."""
        if frame == common:
            return np.eye(4)
        M = np.eye(4)
        current = frame
        for _ in range(64):
            if current == common:
                return M
            tf = self.lookup_at(current, time_ns)
            if tf is None:
                return None
            M = tf.to_matrix() @ M
            current = tf.parent_frame
        return None  # cycle / depth limit

    def _ancestors(self, frame: str, time_ns: int) -> list[str] | None:
        """Return [frame, parent, grandparent, ..., root] at the given time.

        Returns ``None`` only if a cycle / depth-limit is hit. An unknown
        starting frame yields ``[frame]`` (the caller's responsibility
        to validate frame existence).
        """
        out = [frame]
        current = frame
        for _ in range(64):
            tf = self.lookup_at(current, time_ns)
            if tf is None:
                return out
            out.append(tf.parent_frame)
            current = tf.parent_frame
        return None

    def to_dict(self, time_ns: int) -> dict[str, Any]:
        """Serialize the tree state at ``time_ns`` for the API.

        Returns each edge's resolved transform plus a list of frames
        and inferred root frames so the frontend can render the tree
        without re-walking.
        """
        edges: list[dict[str, Any]] = []
        for child in sorted(self._static.keys() | self._dynamic.keys()):
            tf = self.lookup_at(child, time_ns)
            if tf is not None:
                edges.append(tf.to_dict())
        return {
            "time_ns": time_ns,
            "edges": edges,
            "frames": sorted(self.frames()),
            "roots": self.root_frames(),
            "static_count": len(self._static),
            "dynamic_count": sum(len(h) for h in self._dynamic.values()),
        }


def _tf_to_matrix(
    translation: tuple[float, float, float],
    rotation: tuple[float, float, float, float],
) -> np.ndarray:
    """Build a 4x4 homogeneous transform from translation + quaternion (x,y,z,w)."""
    qx, qy, qz, qw = rotation
    # Normalize defensively (CDR data sometimes has tiny drift)
    norm = (qx * qx + qy * qy + qz * qz + qw * qw) ** 0.5
    if norm > 0:
        qx, qy, qz, qw = qx / norm, qy / norm, qz / norm, qw / norm
    # Standard quaternion → rotation matrix
    xx = qx * qx
    yy = qy * qy
    zz = qz * qz
    xy = qx * qy
    xz = qx * qz
    yz = qy * qz
    wx = qw * qx
    wy = qw * qy
    wz = qw * qz
    m = np.eye(4, dtype=np.float64)
    m[0, 0] = 1 - 2 * (yy + zz)
    m[0, 1] = 2 * (xy - wz)
    m[0, 2] = 2 * (xz + wy)
    m[1, 0] = 2 * (xy + wz)
    m[1, 1] = 1 - 2 * (xx + zz)
    m[1, 2] = 2 * (yz - wx)
    m[2, 0] = 2 * (xz - wy)
    m[2, 1] = 2 * (yz + wx)
    m[2, 2] = 1 - 2 * (xx + yy)
    m[0, 3] = translation[0]
    m[1, 3] = translation[1]
    m[2, 3] = translation[2]
    return m


def _bisect_nearest(sorted_keys: list[int], target: int) -> int:
    """Return the index of the element nearest to ``target``."""
    import bisect
    idx = bisect.bisect_left(sorted_keys, target)
    if idx == 0:
        return 0
    if idx == len(sorted_keys):
        return idx - 1
    before = sorted_keys[idx - 1]
    after = sorted_keys[idx]
    return idx if abs(after - target) < abs(target - before) else idx - 1


# ---------------------------------------------------------------------------
# CDR parsing for TF + PointCloud2
# ---------------------------------------------------------------------------


def parse_tf_message(raw_data: bytes, is_static: bool = False) -> list[Transform]:
    """Decode a ``tf2_msgs/TFMessage`` CDR payload.

    A TFMessage is a length-prefixed sequence of TransformStamped:
    each is ``Header (sec, nsec, frame_id) + child_frame_id + Vector3
    translation + Quaternion rotation``.

    Returns a list of ``Transform`` records. Empty list on malformed
    payloads (logged but not raised — one bad TF message shouldn't
    abort a whole bag scan).
    """
    if len(raw_data) < 4:
        return []
    buf = raw_data[4:]  # skip CDR encapsulation header
    out: list[Transform] = []
    try:
        # Sequence count (uint32, 4-byte aligned)
        (n,) = struct.unpack_from("<I", buf, 0)
        off = 4
        if n > 100_000:  # sanity
            return []
        for _ in range(n):
            tf, off = _parse_transform_stamped(buf, off, is_static)
            if tf is not None:
                out.append(tf)
    except Exception:
        return out
    return out


def _parse_transform_stamped(
    buf: bytes, off: int, is_static: bool,
) -> tuple[Transform | None, int]:
    """Parse one TransformStamped starting at ``off``. Returns (tf, new_off)."""
    # Header: sec (int32), nsec (uint32), frame_id (string)
    sec, nsec = struct.unpack_from("<iI", buf, off)
    off += 8
    parent_frame, off = _read_cdr_string(buf, off)
    child_frame, off = _read_cdr_string(buf, off)
    # Align to 8 for double-precision Vector3 + Quaternion
    off = (off + 7) & ~7
    tx, ty, tz = struct.unpack_from("<3d", buf, off)
    off += 24
    qx, qy, qz, qw = struct.unpack_from("<4d", buf, off)
    off += 32
    timestamp_ns = sec * 1_000_000_000 + nsec
    return Transform(
        parent_frame=parent_frame,
        child_frame=child_frame,
        timestamp_ns=timestamp_ns,
        translation=(tx, ty, tz),
        rotation=(qx, qy, qz, qw),
        is_static=is_static,
    ), off


def _read_cdr_string(buf: bytes, off: int) -> tuple[str, int]:
    """Read a CDR-encoded length-prefixed string."""
    (n,) = struct.unpack_from("<I", buf, off)
    off += 4
    if n == 0:
        return "", off
    raw = buf[off:off + n]
    off += n
    # Strip trailing null terminator if present
    if raw.endswith(b"\x00"):
        raw = raw[:-1]
    return raw.decode("utf-8", errors="replace"), off


# PointCloud2 field datatype enum (from sensor_msgs/PointField)
_PC_DATATYPES = {
    1: ("b", 1),    # INT8
    2: ("B", 1),    # UINT8
    3: ("h", 2),    # INT16
    4: ("H", 2),    # UINT16
    5: ("i", 4),    # INT32
    6: ("I", 4),    # UINT32
    7: ("f", 4),    # FLOAT32
    8: ("d", 8),    # FLOAT64
}


@dataclass
class PointCloud2Meta:
    """Parsed metadata for a sensor_msgs/PointCloud2 message."""
    frame_id: str
    height: int
    width: int
    point_step: int
    row_step: int
    is_bigendian: bool
    is_dense: bool
    fields: list[dict[str, Any]] = field(default_factory=list)
    data_offset: int = 0  # offset (within raw_data, AFTER CDR header) where points start
    data_length: int = 0


def parse_pointcloud2_meta(raw_data: bytes) -> PointCloud2Meta | None:
    """Parse a ``sensor_msgs/PointCloud2`` CDR message header.

    Returns metadata + offset to the raw point bytes. The actual point
    decoding is handled by ``decode_pointcloud2_xyz()`` so callers can
    avoid copying the bytes if they only need metadata.

    Returns ``None`` on parse failure.
    """
    if len(raw_data) < 4:
        return None
    buf = raw_data[4:]
    try:
        # Header
        sec, nsec = struct.unpack_from("<iI", buf, 0)
        off = 8
        frame_id, off = _read_cdr_string(buf, off)
        # Align to 4
        off = (off + 3) & ~3
        height, width = struct.unpack_from("<II", buf, off)
        off += 8
        # Fields[]
        (n_fields,) = struct.unpack_from("<I", buf, off)
        off += 4
        if n_fields > 100:  # sanity
            return None
        fields = []
        for _ in range(n_fields):
            name, off = _read_cdr_string(buf, off)
            off = (off + 3) & ~3
            f_offset, datatype, count = struct.unpack_from("<IBI", buf, off)
            # PointField CDR layout has padding between datatype (uint8) and count (uint32)
            # datatype starts at off+4 (1 byte), then 3 bytes padding, then count at off+8
            # The above unpack reads it as off..off+9 — fix the offsets:
            f_offset = struct.unpack_from("<I", buf, off)[0]
            datatype = struct.unpack_from("<B", buf, off + 4)[0]
            # Align to 4 for count
            count = struct.unpack_from("<I", buf, off + 8)[0]
            off += 12
            fields.append({
                "name": name,
                "offset": f_offset,
                "datatype": datatype,
                "count": count,
            })
        (is_bigendian,) = struct.unpack_from("<B", buf, off)
        off += 1
        # Align to 4 for point_step / row_step
        off = (off + 3) & ~3
        point_step, row_step = struct.unpack_from("<II", buf, off)
        off += 8
        (data_len,) = struct.unpack_from("<I", buf, off)
        off += 4
        data_offset = off + 4  # +4 for the CDR encapsulation header we skipped
        # is_dense follows the data array; we don't read it (not on the hot path)
        return PointCloud2Meta(
            frame_id=frame_id,
            height=height,
            width=width,
            point_step=point_step,
            row_step=row_step,
            is_bigendian=bool(is_bigendian),
            is_dense=True,
            fields=fields,
            data_offset=data_offset,
            data_length=data_len,
        )
    except Exception:
        return None


def decode_pointcloud2_xyz(
    raw_data: bytes, meta: PointCloud2Meta, max_points: int | None = None,
) -> np.ndarray | None:
    """Decode the (x, y, z) points of a PointCloud2 into an Nx3 float32 array.

    Skips fields other than x/y/z. Stride-based reading so we don't
    need to materialize the full struct array. Returns ``None`` if x/y/z
    aren't present as float32 fields (the common case; uncommon
    encodings are deferred to v0.6+ when we have real data to test on).

    ``max_points`` caps the returned array length — useful for the API
    layer to enforce wire-size budgets.
    """
    # Locate x/y/z field offsets, require float32
    field_lookup = {f["name"]: f for f in meta.fields}
    needed = ("x", "y", "z")
    if not all(n in field_lookup for n in needed):
        return None
    if not all(field_lookup[n]["datatype"] == 7 for n in needed):
        return None

    n_points = meta.height * meta.width
    if n_points == 0:
        return np.zeros((0, 3), dtype=np.float32)

    if max_points and n_points > max_points:
        # Decimate: every Kth point
        stride = max(1, n_points // max_points)
    else:
        stride = 1

    payload = raw_data[meta.data_offset:meta.data_offset + meta.data_length]
    if len(payload) < n_points * meta.point_step:
        n_points = len(payload) // meta.point_step
        if n_points <= 0:
            return None

    x_off = field_lookup["x"]["offset"]
    y_off = field_lookup["y"]["offset"]
    z_off = field_lookup["z"]["offset"]
    point_step = meta.point_step

    indices = np.arange(0, n_points, stride, dtype=np.int64)
    base = indices * point_step
    out = np.empty((len(indices), 3), dtype=np.float32)
    raw_arr = np.frombuffer(payload, dtype=np.uint8)
    for i, b in enumerate(base):
        out[i, 0] = raw_arr[b + x_off:b + x_off + 4].view(np.float32)[0]
        out[i, 1] = raw_arr[b + y_off:b + y_off + 4].view(np.float32)[0]
        out[i, 2] = raw_arr[b + z_off:b + z_off + 4].view(np.float32)[0]
    # Filter out NaN/inf (common in unstructured sweeps)
    mask = np.isfinite(out).all(axis=1)
    return out[mask]


# ---------------------------------------------------------------------------
# visualization_msgs/Marker + MarkerArray
# ---------------------------------------------------------------------------


# Marker.type enum values from ROS 2 visualization_msgs/Marker
MARKER_TYPES = {
    0: "ARROW",
    1: "CUBE",
    2: "SPHERE",
    3: "CYLINDER",
    4: "LINE_STRIP",
    5: "LINE_LIST",
    6: "CUBE_LIST",
    7: "SPHERE_LIST",
    8: "POINTS",
    9: "TEXT_VIEW_FACING",
    10: "MESH_RESOURCE",
    11: "TRIANGLE_LIST",
}

# Marker.action enum
MARKER_ACTIONS = {
    0: "ADD",
    1: "MODIFY",
    2: "DELETE",
    3: "DELETEALL",
}


@dataclass
class Marker:
    """Decoded visualization_msgs/Marker.

    Subset of the full Marker message — covers the fields the SceneViewer
    actually needs to render. Less-used fields (mesh_resource, mesh_use_embedded_materials,
    text content for TEXT_VIEW_FACING) are present but not rendered in v0.6.0.
    """
    frame_id: str
    timestamp_ns: int
    ns: str
    id: int
    type: int  # see MARKER_TYPES
    action: int  # see MARKER_ACTIONS
    position: tuple[float, float, float]
    orientation: tuple[float, float, float, float]  # x, y, z, w
    scale: tuple[float, float, float]
    color: tuple[float, float, float, float]  # r, g, b, a
    lifetime_sec: float
    frame_locked: bool
    text: str = ""
    mesh_resource: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "frame_id": self.frame_id,
            "timestamp_ns": self.timestamp_ns,
            "ns": self.ns,
            "id": self.id,
            "type": self.type,
            "type_name": MARKER_TYPES.get(self.type, f"UNKNOWN_{self.type}"),
            "action": self.action,
            "action_name": MARKER_ACTIONS.get(self.action, f"UNKNOWN_{self.action}"),
            "position": list(self.position),
            "orientation": list(self.orientation),
            "scale": list(self.scale),
            "color": list(self.color),
            "lifetime_sec": self.lifetime_sec,
            "frame_locked": self.frame_locked,
            "text": self.text,
            "mesh_resource": self.mesh_resource,
        }


def _parse_marker_inline(buf: bytes, off: int) -> tuple[Marker | None, int]:
    """Parse a Marker body starting at ``off`` (no CDR header skip).

    Returns ``(marker, new_offset)`` or ``(None, off)`` on parse failure.
    Used by both :func:`parse_marker` (after stripping the CDR header) and
    :func:`parse_marker_array` (which iterates inline marker bodies after
    its sequence-count header).
    """
    try:
        # Header: sec (int32), nsec (uint32), frame_id (string)
        sec, nsec = struct.unpack_from("<iI", buf, off)
        off += 8
        frame_id, off = _read_cdr_string(buf, off)
        # ns (string), id (int32), type (int32), action (int32)
        ns, off = _read_cdr_string(buf, off)
        off = (off + 3) & ~3
        marker_id, marker_type, marker_action = struct.unpack_from(
            "<iii", buf, off,
        )
        off += 12
        # Pose: position + orientation (3+4 float64), align to 8
        off = (off + 7) & ~7
        px, py, pz = struct.unpack_from("<3d", buf, off)
        off += 24
        qx, qy, qz, qw = struct.unpack_from("<4d", buf, off)
        off += 32
        # Scale: Vector3
        sx, sy, sz = struct.unpack_from("<3d", buf, off)
        off += 24
        # Color: ColorRGBA (4 float32)
        cr, cg, cb, ca = struct.unpack_from("<4f", buf, off)
        off += 16
        # Lifetime: Duration (int32 + uint32), align to 4
        off = (off + 3) & ~3
        lt_sec, lt_nsec = struct.unpack_from("<iI", buf, off)
        off += 8
        lifetime = lt_sec + lt_nsec / 1e9
        # frame_locked (bool)
        (frame_locked,) = struct.unpack_from("<B", buf, off)
        off += 1
        # Tail: points[], colors[], text, mesh_resource, mesh_use_embedded_materials.
        # CDR alignment is relative to the buf start (= encapsulation body
        # start), so each uint32 / string-length field needs 4-byte alignment.
        text = ""
        mesh_resource = ""
        try:
            # Align to 4 for the points[] length prefix
            off = (off + 3) & ~3
            (n_points,) = struct.unpack_from("<I", buf, off)
            off += 4
            if n_points > 1_000_000:  # sanity
                return None, off
            if n_points > 0:
                off = (off + 7) & ~7
                off += n_points * 24
            # colors[] length — align to 4
            off = (off + 3) & ~3
            (n_colors,) = struct.unpack_from("<I", buf, off)
            off += 4
            if n_colors > 1_000_000:
                return None, off
            if n_colors > 0:
                off = (off + 3) & ~3
                off += n_colors * 16
            # text (string) — length prefix needs 4-byte alignment
            off = (off + 3) & ~3
            text, off = _read_cdr_string(buf, off)
            # mesh_resource (string)
            off = (off + 3) & ~3
            mesh_resource, off = _read_cdr_string(buf, off)
            # mesh_use_embedded_materials (bool) — skip
            off += 1
        except (struct.error, IndexError):
            # Tail fields unreadable — caller-relevant fields already captured
            pass

        marker = Marker(
            frame_id=frame_id,
            timestamp_ns=sec * 1_000_000_000 + nsec,
            ns=ns,
            id=marker_id,
            type=marker_type,
            action=marker_action,
            position=(px, py, pz),
            orientation=(qx, qy, qz, qw),
            scale=(sx, sy, sz),
            color=(cr, cg, cb, ca),
            lifetime_sec=lifetime,
            frame_locked=bool(frame_locked),
            text=text,
            mesh_resource=mesh_resource,
        )
        return marker, off
    except (struct.error, IndexError, UnicodeDecodeError):
        return None, off


def parse_marker(raw_data: bytes) -> Marker | None:
    """Parse a single visualization_msgs/Marker CDR message.

    Returns None on malformed input — never raises. Decodes the fields
    needed for rendering primitives (CUBE / SPHERE / CYLINDER / ARROW)
    plus text and mesh_resource for completeness.
    """
    if len(raw_data) < 4:
        return None
    marker, _ = _parse_marker_inline(raw_data[4:], 0)
    return marker


def parse_marker_array(raw_data: bytes) -> list[Marker]:
    """Parse a visualization_msgs/MarkerArray CDR payload.

    Wire format: CDR encapsulation header (4 bytes) + uint32 sequence
    count + N inline Marker bodies (no per-marker CDR header). Returns
    the decoded list; if a marker mid-sequence fails to parse, we stop
    rather than continue with potentially mis-aligned offsets.
    """
    if len(raw_data) < 8:
        return []
    buf = raw_data[4:]  # strip CDR encapsulation header
    try:
        (n,) = struct.unpack_from("<I", buf, 0)
    except struct.error:
        return []
    if n > 100_000:  # sanity guard
        return []
    if n == 0:
        return []
    out: list[Marker] = []
    off = 4
    for _ in range(n):
        marker, new_off = _parse_marker_inline(buf, off)
        if marker is None:
            break  # mis-aligned or truncated; stop rather than emit garbage
        out.append(marker)
        off = new_off
    return out
