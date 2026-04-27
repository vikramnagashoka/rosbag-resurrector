"""
RosBag Resurrector — Stop letting your rosbag data rot.

A pandas-like data analysis tool for robotics bag files with automatic
quality validation, multi-stream synchronization, ML-ready export,
and an interactive web dashboard.
"""

__version__ = "0.4.0"

from resurrector.core.bag_frame import BagFrame
from resurrector.ingest.scanner import scan
from resurrector.core.query import search
from resurrector.core.dataset import DatasetManager, BagRef, SyncConfig, DatasetMetadata

__all__ = [
    "BagFrame",
    "scan",
    "search",
    "DatasetManager",
    "BagRef",
    "SyncConfig",
    "DatasetMetadata",
]
