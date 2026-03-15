"""Common robotics data transforms.

Provides utilities for:
- Quaternion to Euler angle conversion
- Coordinate frame transforms
- Image decompression
- Point cloud conversions
- Downsampling
- Unit conversions
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import polars as pl


def quaternion_to_euler(
    qx: float | np.ndarray,
    qy: float | np.ndarray,
    qz: float | np.ndarray,
    qw: float | np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Convert quaternion (x, y, z, w) to Euler angles (roll, pitch, yaw) in radians.

    Uses the ZYX (yaw-pitch-roll) convention common in robotics.
    """
    qx, qy, qz, qw = (np.asarray(v, dtype=float) for v in (qx, qy, qz, qw))

    # Roll (x-axis rotation)
    sinr_cosp = 2.0 * (qw * qx + qy * qz)
    cosr_cosp = 1.0 - 2.0 * (qx * qx + qy * qy)
    roll = np.arctan2(sinr_cosp, cosr_cosp)

    # Pitch (y-axis rotation)
    sinp = 2.0 * (qw * qy - qz * qx)
    pitch = np.where(
        np.abs(sinp) >= 1.0,
        np.copysign(np.pi / 2, sinp),
        np.arcsin(sinp),
    )

    # Yaw (z-axis rotation)
    siny_cosp = 2.0 * (qw * qz + qx * qy)
    cosy_cosp = 1.0 - 2.0 * (qy * qy + qz * qz)
    yaw = np.arctan2(siny_cosp, cosy_cosp)

    return roll, pitch, yaw


def euler_to_quaternion(
    roll: float | np.ndarray,
    pitch: float | np.ndarray,
    yaw: float | np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Convert Euler angles (roll, pitch, yaw) to quaternion (x, y, z, w)."""
    roll, pitch, yaw = (np.asarray(v, dtype=float) for v in (roll, pitch, yaw))

    cr = np.cos(roll / 2)
    sr = np.sin(roll / 2)
    cp = np.cos(pitch / 2)
    sp = np.sin(pitch / 2)
    cy = np.cos(yaw / 2)
    sy = np.sin(yaw / 2)

    qw = cr * cp * cy + sr * sp * sy
    qx = sr * cp * cy - cr * sp * sy
    qy = cr * sp * cy + sr * cp * sy
    qz = cr * cp * sy - sr * sp * cy

    return qx, qy, qz, qw


def radians_to_degrees(rad: float | np.ndarray) -> np.ndarray:
    """Convert radians to degrees."""
    return np.degrees(np.asarray(rad, dtype=float))


def degrees_to_radians(deg: float | np.ndarray) -> np.ndarray:
    """Convert degrees to radians."""
    return np.radians(np.asarray(deg, dtype=float))


def add_euler_columns(df: pl.DataFrame, prefix: str = "orientation") -> pl.DataFrame:
    """Add roll/pitch/yaw columns from quaternion columns in a DataFrame.

    Expects columns: {prefix}.x, {prefix}.y, {prefix}.z, {prefix}.w
    Adds columns: {prefix}.roll, {prefix}.pitch, {prefix}.yaw
    """
    qx = df[f"{prefix}.x"].to_numpy()
    qy = df[f"{prefix}.y"].to_numpy()
    qz = df[f"{prefix}.z"].to_numpy()
    qw = df[f"{prefix}.w"].to_numpy()

    roll, pitch, yaw = quaternion_to_euler(qx, qy, qz, qw)

    return df.with_columns([
        pl.Series(f"{prefix}.roll", roll),
        pl.Series(f"{prefix}.pitch", pitch),
        pl.Series(f"{prefix}.yaw", yaw),
    ])


def downsample_temporal(
    df: pl.DataFrame,
    target_hz: float,
    timestamp_col: str = "timestamp_ns",
) -> pl.DataFrame:
    """Downsample a DataFrame to a target frequency.

    Selects the nearest message to each target timestamp.
    """
    if df.height == 0:
        return df

    timestamps = df[timestamp_col].to_numpy()
    start = timestamps[0]
    end = timestamps[-1]
    interval_ns = int(1e9 / target_hz)

    target_times = np.arange(start, end, interval_ns)
    indices = np.searchsorted(timestamps, target_times)
    indices = np.clip(indices, 0, len(timestamps) - 1)

    # Check if previous index is closer
    for i in range(len(indices)):
        idx = indices[i]
        if idx > 0:
            d1 = abs(timestamps[idx] - target_times[i])
            d2 = abs(timestamps[idx - 1] - target_times[i])
            if d2 < d1:
                indices[i] = idx - 1

    # Remove duplicates while preserving order
    unique_indices = list(dict.fromkeys(indices.tolist()))
    return df[unique_indices]


def laser_scan_to_cartesian(
    ranges: np.ndarray,
    angle_min: float,
    angle_max: float,
    angle_increment: float | None = None,
) -> np.ndarray:
    """Convert laser scan polar coordinates to Cartesian (x, y) points.

    Returns:
        Array of shape (N, 2) with x, y coordinates.
    """
    n = len(ranges)
    if angle_increment is None:
        angle_increment = (angle_max - angle_min) / n
    angles = np.arange(angle_min, angle_min + n * angle_increment, angle_increment)[:n]

    x = ranges * np.cos(angles)
    y = ranges * np.sin(angles)

    return np.column_stack([x, y])
