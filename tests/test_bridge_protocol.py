"""Tests for the PlotJuggler-compatible message protocol."""

import pytest

from resurrector.bridge.protocol import flatten_to_plotjuggler, encode_status_message


class TestFlattenToPlotjuggler:
    def test_imu_data(self):
        data = {
            "header": {"stamp_sec": 1700000001, "stamp_nsec": 5000000, "frame_id": "imu_link"},
            "orientation": {"x": 0.001, "y": 0.002, "z": 0.003, "w": 0.999},
            "angular_velocity": {"x": 0.01, "y": 0.02, "z": 0.03},
            "linear_acceleration": {"x": 0.1, "y": 0.2, "z": 9.81},
        }
        result = flatten_to_plotjuggler("/imu/data", data, 1700000001.005)

        assert result["timestamp"] == 1700000001.005
        assert result["/imu/data/orientation/x"] == 0.001
        assert result["/imu/data/orientation/w"] == 0.999
        assert result["/imu/data/linear_acceleration/z"] == 9.81
        # Header should be excluded
        assert "/imu/data/header/stamp_sec" not in result

    def test_list_expansion(self):
        data = {
            "header": {"stamp_sec": 0, "stamp_nsec": 0, "frame_id": ""},
            "position": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "velocity": [0.1, 0.2, 0.3, 0.4, 0.5, 0.6],
        }
        result = flatten_to_plotjuggler("/joint_states", data, 100.0)

        assert result["/joint_states/position/0"] == 1.0
        assert result["/joint_states/position/5"] == 6.0
        assert result["/joint_states/velocity/0"] == 0.1

    def test_internal_keys_excluded(self):
        data = {
            "_unparsed": True,
            "_raw_size": 1024,
            "_msg_type": "sensor_msgs/msg/Unknown",
            "value": 42.0,
        }
        result = flatten_to_plotjuggler("/custom", data, 1.0)

        assert "_unparsed" not in str(result)
        assert "_raw_size" not in str(result)
        assert result["/custom/value"] == 42.0

    def test_large_arrays_omitted(self):
        data = {
            "ranges": list(range(360)),  # 360 floats — too large
            "angle_min": -3.14,
        }
        result = flatten_to_plotjuggler("/lidar/scan", data, 1.0)

        # Large array should be omitted
        assert "/lidar/scan/ranges/0" not in result
        # Scalar should be included
        assert result["/lidar/scan/angle_min"] == -3.14

    def test_string_fields_excluded(self):
        data = {
            "name": ["joint_0", "joint_1"],
            "position": [1.0, 2.0],
        }
        result = flatten_to_plotjuggler("/joints", data, 1.0)

        # String list should not expand
        assert "/joints/name/0" not in result
        # Numeric list should expand
        assert result["/joints/position/0"] == 1.0


class TestEncodeStatusMessage:
    def test_status_format(self):
        msg = encode_status_message("playback", "playing", speed=2.0, progress=0.5)
        assert msg["type"] == "status"
        assert msg["mode"] == "playback"
        assert msg["state"] == "playing"
        assert msg["speed"] == 2.0
        assert msg["progress"] == 0.5
