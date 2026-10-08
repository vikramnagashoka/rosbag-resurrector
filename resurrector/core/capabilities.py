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
    """Both formats the extra unlocks can run: zarr, and tensorflow for RLDS.

    Presence-only (``find_spec``): importing tensorflow costs seconds and
    this runs on every capabilities request.
    """
    from resurrector.core.export import export_dependency_problem
    return all(export_dependency_problem(f) is None for f in ("zarr", "rlds"))


def _all_exports_description() -> str:
    """What the extra unlocks, plus why RLDS stays off where pip can't
    deliver tensorflow (Python 3.14, Intel macOS on 3.13, ...).

    The explanation lives here because the dashboard pastes
    ``install_command`` into a copy block: it must stay a runnable command,
    and the extra's pip command still installs Zarr on these platforms.
    """
    from resurrector.core import export
    base = "Zarr and RLDS (TFRecord) export formats"
    if export.tensorflow_wheels_available() or export.export_dependency_problem("rlds") is None:
        return base
    return (
        f"{base}. On this interpreter the extra installs Zarr only: "
        f"{export.tensorflow_missing_detail()}. For RLDS, use {export.TENSORFLOW_WHERE}."
    )


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


def _lerobot_available() -> bool:
    """LeRobot export drives LeRobot's own dataset writer (Python 3.12+).

    Presence-only, like ``_all_exports_available``: importing the writer
    pulls in torch, and this runs on every capabilities request.
    """
    from resurrector.core.export import export_dependency_problem
    return export_dependency_problem("lerobot") is None


def get_capabilities() -> dict[str, Capability]:
    """Return the runtime-detected capability map keyed by name."""
    from resurrector.core.export import ALL_EXPORTS_INSTALL
    caps = [
        Capability(
            name="vision",
            available=_vision_available(),
            install_command="pip install 'rosbag-resurrector[vision]'",
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
            install_command=ALL_EXPORTS_INSTALL,
            description=_all_exports_description(),
        ),
        Capability(
            name="lerobot",
            available=_lerobot_available(),
            # No trailing "# ..." note: zsh doesn't treat # as a comment
            # interactively, so a pasted command would hand it to pip.
            install_command="pip install 'rosbag-resurrector[lerobot]'",
            description="LeRobot v3 dataset export (state, actions, camera video). Needs Python 3.12+",
        ),
        Capability(
            name="publish",
            available=_publish_available(),
            install_command="pip install 'rosbag-resurrector[publish]'",
            description="Publish datasets to the HuggingFace Hub with an auto card",
        ),
        Capability(
            name="copilot",
            available=_copilot_available(),
            install_command="pip install 'rosbag-resurrector[copilot]'",
            description="'Ask your bag' — grounded natural-language analysis",
        ),
    ]
    return {c.name: c for c in caps}
