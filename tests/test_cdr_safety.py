"""Tests for CDR parser bounds checking.

The parser must handle malformed / truncated CDR buffers without crashing.
On failure, it should either raise CDRParseError (when called directly) or
surface a _parse_error marker in the returned dict (when called via
_parse_cdr_message, which catches and reports).
"""

from __future__ import annotations

import struct

import pytest

from resurrector.ingest.parser import (
    CDRParseError,
    _parse_cdr_message,
    _parse_imu,
    _parse_joint_state,
    _parse_laser_scan,
    _safe_read_string,
    _safe_unpack,
)


class TestSafeUnpack:
    def test_succeeds_on_valid_buffer(self):
        buf = struct.pack("<II", 42, 99)
        a, b = _safe_unpack("<II", buf, 0, "test")
        assert a == 42 and b == 99

    def test_raises_on_short_buffer(self):
        buf = b"\x00\x01"  # 2 bytes; need 8 for <II
        with pytest.raises(CDRParseError) as exc:
            _safe_unpack("<II", buf, 0, "test")
        assert exc.value.needed == 8
        assert "test" in str(exc.value)

    def test_raises_on_offset_past_end(self):
        buf = struct.pack("<II", 1, 2)  # 8 bytes
        with pytest.raises(CDRParseError):
            _safe_unpack("<I", buf, 100, "test")


class TestSafeReadString:
    def test_reads_valid_string(self):
        payload = b"hello\x00\x00\x00"  # padded to 8
        buf = struct.pack("<I", 5) + payload
        s, off = _safe_read_string(buf, 0, "test")
        assert s == "hello"

    def test_rejects_string_longer_than_buffer(self):
        # Say string is 1000 bytes long, but buffer only has 4
        buf = struct.pack("<I", 1000)
        with pytest.raises(CDRParseError) as exc:
            _safe_read_string(buf, 0, "test")
        assert "exceeds buffer" in str(exc.value)


class TestImuParser:
    def test_truncated_imu_raises(self):
        # Only 10 bytes — way too short for a full Imu message
        buf = b"\x00" * 10
        with pytest.raises(CDRParseError):
            _parse_imu(buf)


class TestJointStateParser:
    def test_inflated_n_names_rejected(self):
        """A corrupt joint_state with n_names=10 million should be rejected."""
        # sec, nsec, frame_id (empty), n_names (huge)
        buf = (
            struct.pack("<II", 0, 0)          # sec, nsec
            + struct.pack("<I", 0)            # empty frame_id
            + struct.pack("<I", 50_000_000)   # n_names way too big
        )
        with pytest.raises(CDRParseError) as exc:
            _parse_joint_state(buf)
        assert "exceeds max" in str(exc.value)

    def test_truncated_name_string_rejected(self):
        """n_names says 1, but first string's length exceeds buffer."""
        buf = (
            struct.pack("<II", 0, 0)        # sec, nsec
            + struct.pack("<I", 0)          # empty frame_id
            + struct.pack("<I", 1)          # n_names = 1
            + struct.pack("<I", 99999)      # str_len too big, buffer ends
        )
        with pytest.raises(CDRParseError):
            _parse_joint_state(buf)


class TestLaserScanParser:
    def test_inflated_n_ranges_rejected(self):
        # Minimal header + all floats valid, but n_ranges is huge
        buf = (
            struct.pack("<II", 0, 0)          # sec, nsec
            + struct.pack("<I", 0)            # empty frame_id
            + struct.pack("<5f", 0.0, 0.0, 0.0, 0.0, 0.0)  # angle, time
            + struct.pack("<2f", 0.0, 100.0)  # range_min, range_max
            + struct.pack("<I", 20_000_000)   # n_ranges enormous
        )
        with pytest.raises(CDRParseError) as exc:
            _parse_laser_scan(buf)
        assert "exceeds max" in str(exc.value)


class TestParseCdrMessage:
    """_parse_cdr_message is the outer boundary and should NEVER raise —
    it catches errors and returns a _parse_error marker instead, so one
    corrupt message doesn't abort an entire bag scan."""

    def test_wraps_cdr_parse_error(self):
        """Garbage buffer with known msg_type surfaces as _parse_error."""
        # 4 bytes of CDR encapsulation + 8 bytes that look like a header
        # but n_names will read past the end
        cdr_header = b"\x00\x01\x00\x00"
        payload = (
            struct.pack("<II", 0, 0)         # sec, nsec
            + struct.pack("<I", 0)           # frame_id len 0
            + struct.pack("<I", 999999)      # n_names bogus
        )
        result = _parse_cdr_message("sensor_msgs/msg/JointState", cdr_header + payload)
        assert result.get("_parse_error") is True
        assert result.get("_msg_type") == "sensor_msgs/msg/JointState"
        assert "_error" in result

    def test_short_buffer_returns_empty(self):
        # Less than the 4-byte CDR header
        result = _parse_cdr_message("sensor_msgs/msg/Imu", b"ab")
        assert result == {}

    def test_unknown_type_marked_unparsed(self):
        cdr_header = b"\x00\x01\x00\x00"
        result = _parse_cdr_message("custom/msg/Unknown", cdr_header + b"x" * 20)
        assert result.get("_unparsed") is True
