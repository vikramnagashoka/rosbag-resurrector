"""Runtime detection of optional capabilities.

Each capability corresponds to a feature in the dashboard that requires
something the base ``pip install rosbag-resurrector`` does not pull in —
either a pip extra (``[vision]``, ``[all-exports]``) or a system binary
(``mcap`` CLI, a ROS 2 install on PATH).

The dashboard's Search / Bridge / Library / Export surfaces import
``get_capabilities()`` to render the same install banner everywhere
instead of each page rolling its own ImportError handler.

Keep this list narrow: a capability earns inclusion only when a
real UI surface gates on it. Adding speculative entries here just
clutters the response shape.
"""
from __future__ import annotations

import shutil
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Capability:
    name: str
    available: bool
    install_command: str
    description: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "available": self.available,
            "install_command": self.install_command,
            "description": self.description,
        }


def _vision_available() -> bool:
    """True if either the local CLIP backend or the OpenAI backend is importable."""
    try:
        import sentence_transformers  # noqa: F401
        return True
    except ImportError:
        pass
    try:
        import openai  # noqa: F401
        return True
    except ImportError:
        return False


def _bridge_live_available() -> bool:
    try:
        import rclpy  # noqa: F401
        return True
    except ImportError:
        return False


def _all_exports_available() -> bool:
    """Available means both zarr AND tensorflow-datasets are importable."""
    try:
        import zarr  # noqa: F401
        import tensorflow_datasets  # noqa: F401
        return True
    except ImportError:
        return False


def _ros1_convert_available() -> bool:
    """Converting ROS 1 ``.bag`` to MCAP needs the ``mcap`` CLI binary."""
    return shutil.which("mcap") is not None


def _publish_available() -> bool:
    """Publishing to the HuggingFace Hub needs huggingface_hub."""
    try:
        import huggingface_hub  # noqa: F401
        return True
    except ImportError:
        return False


def _copilot_available() -> bool:
    """The 'Ask your bag' copilot needs the anthropic SDK."""
    try:
        import anthropic  # noqa: F401
        return True
    except ImportError:
        return False


def get_capabilities() -> dict[str, Capability]:
    """Return the runtime-detected capability map keyed by name."""
    caps = [
        Capability(
            name="vision",
            available=_vision_available(),
            install_command="pip install rosbag-resurrector[vision]",
            description="Semantic frame search via CLIP embeddings",
        ),
        Capability(
            name="bridge_live",
            available=_bridge_live_available(),
            install_command=(
                "Install ROS 2 (which provides rclpy). See "
                "https://docs.ros.org/en/jazzy/Installation.html"
            ),
            description="Record / relay topics from a running ROS 2 system in real time",
        ),
        Capability(
            name="ros1_convert",
            available=_ros1_convert_available(),
            install_command=(
                "brew install mcap   # macOS\n"
                "# or download a release for your platform from\n"
                "# https://github.com/foxglove/mcap/releases"
            ),
            description="Auto-convert ROS 1 .bag files to MCAP during scan",
        ),
        Capability(
            name="all_exports",
            available=_all_exports_available(),
            install_command="pip install rosbag-resurrector[all-exports]",
            description="Zarr and TensorFlow Datasets (RLDS) export formats",
        ),
        Capability(
            name="publish",
            available=_publish_available(),
            install_command="pip install rosbag-resurrector[publish]",
            description="Publish datasets to the HuggingFace Hub with an auto card",
        ),
        Capability(
            name="copilot",
            available=_copilot_available(),
            install_command="pip install rosbag-resurrector[copilot]",
            description="'Ask your bag' — grounded natural-language analysis",
        ),
    ]
    return {c.name: c for c in caps}
