"""
RosBag Resurrector — Stop letting your rosbag data rot.

A pandas-like data analysis tool for robotics bag files with automatic
quality validation, multi-stream synchronization, ML-ready export,
and an interactive web dashboard.
"""

__version__ = "0.8.0"

from resurrector.core.bag_frame import BagFrame
from resurrector.ingest.scanner import scan
from resurrector.core.query import search
from resurrector.core.dataset import DatasetManager, BagRef, SyncConfig, DatasetMetadata
from resurrector.ingest.parser import (
    register_decoder, unregister_decoder, list_decoders,
)
from resurrector.core.concatenated import (
    ConcatenatedBagFrame, ConcatenatedTopicView, concatenate_bags,
)
from resurrector.core.bag_diff import diff_bags, BagDiff
from resurrector.core.benchmark import run_benchmark, capture_baseline, MetricSpec
from resurrector.core.publish import publish_dataset, build_dataset_card
from resurrector.core.copilot import explain_time_range, gather_evidence
from resurrector.core.report import generate_incident_report, build_incident_report
from resurrector.core.contract import (
    infer_contract, check_contract, load_contract, save_contract, Contract,
)

__all__ = [
    "BagFrame",
    "scan",
    "search",
    "DatasetManager",
    "BagRef",
    "SyncConfig",
    "DatasetMetadata",
    "ConcatenatedBagFrame",
    "ConcatenatedTopicView",
    "concatenate_bags",
    "register_decoder",
    "unregister_decoder",
    "list_decoders",
    # v0.7 features
    "diff_bags",
    "BagDiff",
    "run_benchmark",
    "capture_baseline",
    "MetricSpec",
    "publish_dataset",
    "build_dataset_card",
    "explain_time_range",
    "gather_evidence",
    # v0.7.1 features
    "generate_incident_report",
    "build_incident_report",
    "infer_contract",
    "check_contract",
    "load_contract",
    "save_contract",
    "Contract",
]
