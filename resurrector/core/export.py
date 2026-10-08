"""Export bag data to ML-friendly formats.

Supports: Parquet, HDF5, CSV, NumPy, Zarr, LeRobot, RLDS.

Per the v0.4.0 performance contract, exports to the streaming-friendly
formats (Parquet, HDF5, CSV, Zarr, RLDS) write chunk-by-chunk so peak
memory is bounded by ``CHUNK_SIZE``. With ``sync=True`` the synced
table streams too (:func:`resurrector.core.sync.iter_synchronize`); its
downsample grid carries across chunks, so the rows written match
downsampling the whole table. LeRobot goes through LeRobot's own writer
(:mod:`resurrector.core.lerobot_export`): input is streamed, but one
episode's frame grid is held in memory until LeRobot saves the episode. NumPy ``.npz`` is the
exception: the format can't be incrementally appended, so writing
requires materializing every column. We hard-cap NumPy export at
``NUMPY_HARD_CAP`` rows and raise :class:`LargeTopicError` past that
— users on bigger topics should use Parquet (which streams).

HDF5, Zarr and NumPy have no missing value for integers or Booleans,
and their arrays can't change dtype after the first chunk, so those
columns are written as float64 with NaN for a missing value (see
:class:`_NumpyColumns`); ``timestamp_ns`` stays int64. String columns
are written as text (variable-length UTF-8 in HDF5 and Zarr,
fixed-width unicode in ``.npz``) and a missing string as ``""``;
Parquet keeps nulls.

A topic's columns can change between chunks (a driver that starts
publishing JointState velocity late). Every writer keeps each row
aligned with its ``timestamp_ns``. HDF5, Zarr and NumPy write a column's
rows outside the chunks that carry it as its missing value, and fail a
column whose dtype has none (``timestamp_ns``, datetimes). CSV and
Parquet fix their columns from the first chunk: a column a later chunk
lacks is written empty / null there, a column that first appears later
is reported and left out, and a Parquet column whose later values can't
be stored losslessly as the first chunk's type is reported and written
as null from that chunk on. Reported columns raise :class:`ExportError`
once the file is written; HDF5, Zarr and NumPy leave them out of the
file.
"""

from __future__ import annotations

import importlib.util
import logging
import platform
import sys
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Callable, Iterable, Iterator, Mapping, Sequence

import numpy as np
import polars as pl

from resurrector.core.exceptions import LargeTopicError

if TYPE_CHECKING:
    from resurrector.core.bag_frame import BagFrame

logger = logging.getLogger("resurrector.core.export")

CHUNK_SIZE = 50_000

# NumPy .npz can't append; we have to hold every column in memory until
# savez_compressed flushes. Past ~1 M rows the materialized arrays plus
# compression buffers easily exceed 1 GB. Refuse early with a clear
# error pointing to Parquet, which streams.
NUMPY_HARD_CAP = 1_000_000

# .npz text is fixed width, so one long value widens every row of its
# column (1 M rows x 300 characters is 1.2 GB). Past this size the
# writer warns and points to Parquet / Zarr; it still writes the column.
_NPZ_TEXT_WARN_BYTES = 256 * 2**20

ALL_EXPORTS_INSTALL = "pip install 'rosbag-resurrector[all-exports]'"

# Where tensorflow (the RLDS writer's dependency) publishes stable wheels,
# as (system, machine) -> newest Python with one. pyproject.toml's
# [all-exports] extra carries the same table as environment markers on its
# tensorflow lines, so pip never tries to build tensorflow where no wheel
# exists; tests/test_rlds_capability.py fails if the two drift. Python 3.14
# stays out until a stable cp314 wheel ships, because pip would otherwise
# fall back to a release candidate.
TENSORFLOW_PLATFORMS: Mapping[tuple[str, str], tuple[int, int]] = MappingProxyType({
    # tensorflow 2.21 ships cp310-cp313 here.
    ("Linux", "x86_64"): (3, 13),
    ("Linux", "aarch64"): (3, 13),
    ("Darwin", "arm64"): (3, 13),
    # CPython on Windows reports AMD64; uv's cross-platform resolver uses x86_64.
    ("Windows", "AMD64"): (3, 13),
    ("Windows", "x86_64"): (3, 13),
    # Intel macOS stopped at 2.16 (cp310-cp312), which pins numpy<2; the
    # extra caps tensorflow at <2.17 there.
    ("Darwin", "x86_64"): (3, 12),
})
# Low end of every range above: the package's own floor (requires-python),
# which every tensorflow release in the table also covers.
TENSORFLOW_MIN_PYTHON = (3, 10)

_MACOS_NAMES = {"x86_64": "Intel macOS", "arm64": "Apple-silicon macOS"}


def _platform_names(keys: Sequence[tuple[str, str]]) -> list[str]:
    """Readable names for platform-table keys, in order, without repeats.

    Linux machines share one name ("x86_64/aarch64 Linux"), and Windows'
    two spellings of x64 collapse into "x64 Windows".
    """
    linux = "/".join(machine for system, machine in keys if system == "Linux")
    names: list[str] = []
    for system, machine in keys:
        if system == "Linux":
            name = f"{linux} Linux"
        elif system == "Darwin":
            name = _MACOS_NAMES.get(machine, f"macOS {machine}")
        elif system == "Windows" and machine in ("AMD64", "x86_64"):
            name = "x64 Windows"
        else:
            name = f"{system} {machine}"
        if name not in names:
            names.append(name)
    return names


def describe_tensorflow_platforms(
    table: Mapping[tuple[str, str], tuple[int, int]],
) -> str:
    """Prose for where ``[all-exports]`` installs tensorflow, built from a
    platform table such as :data:`TENSORFLOW_PLATFORMS`. Platforms that
    share a newest Python share a clause, newest Python first."""
    by_max: dict[tuple[int, int], list[tuple[str, str]]] = {}
    for key, max_python in table.items():
        by_max.setdefault(max_python, []).append(key)
    low = "%d.%d" % TENSORFLOW_MIN_PYTHON
    clauses = []
    for (major, minor), keys in sorted(by_max.items(), reverse=True):
        *rest, last = _platform_names(keys)
        where = f"{', '.join(rest)} or {last}" if rest else last
        clauses.append(f"Python {low}-{major}.{minor} on {where}")
    return ", or ".join(clauses)


TENSORFLOW_WHERE = describe_tensorflow_platforms(TENSORFLOW_PLATFORMS)


def _running_platform() -> tuple[tuple[int, int], str, str]:
    """``(python, system, machine)`` for this interpreter, as pip's
    environment markers see it. Tests pin it to check other platforms."""
    return (sys.version_info[0], sys.version_info[1]), platform.system(), platform.machine()


def tensorflow_wheels_available(
    python: tuple[int, int] | None = None,
    system: str | None = None,
    machine: str | None = None,
) -> bool:
    """True when ``[all-exports]`` installs tensorflow on this interpreter.

    Each argument defaults to the running interpreter (``platform.system()``
    / ``platform.machine()``, the same values pip's markers read).
    """
    here_python, here_system, here_machine = _running_platform()
    python = tuple(python or here_python)
    max_python = TENSORFLOW_PLATFORMS.get((system or here_system, machine or here_machine))
    return max_python is not None and python[:2] <= max_python


def tensorflow_install_hint() -> str:
    """How to get tensorflow: the extra's pip command, or, where the extra
    can't install it, which interpreter to switch to first. That second
    form is prose, so it belongs in messages and `doctor`'s fix column,
    never in a capability's ``install_command``."""
    if tensorflow_wheels_available():
        return ALL_EXPORTS_INSTALL
    return f"Use {TENSORFLOW_WHERE}, then: {ALL_EXPORTS_INSTALL}"


def _tensorflow_gap() -> str:
    # Phrased to follow "tensorflow, which ..." / "tensorflow ...".
    if tensorflow_wheels_available():
        return "isn't installed"
    (major, minor), system, machine = _running_platform()
    if system == "Darwin":
        where = _MACOS_NAMES.get(machine, f"macOS {machine}")
    else:
        where = f"{system} {machine}"
    return f"publishes no stable wheel for Python {major}.{minor} on {where}"


def tensorflow_missing_detail() -> str:
    """Why tensorflow is absent: not installed, or no wheel for this platform."""
    return f"tensorflow {_tensorflow_gap()}"


def _module_installed(name: str) -> bool:
    # find_spec locates the package without executing it; importing
    # tensorflow just to answer "is it there?" costs seconds.
    try:
        return importlib.util.find_spec(name) is not None
    except (ImportError, ValueError):
        return False


def export_dependency_problem(format: str) -> str | None:
    """Why ``format`` can't be exported with this install, or ``None``.

    Covers every format with an optional dependency: ``zarr`` and ``rlds``
    (``[all-exports]``) and ``lerobot`` (``[lerobot]``, which also needs
    Python 3.12+). Presence-only (``find_spec``), so it is cheap enough for
    every dashboard request and never imports tensorflow or LeRobot (and
    with it torch). A package that is present but fails to import passes
    here; :func:`require_export_dependencies` catches that for LeRobot.
    """
    if format == "zarr" and not _module_installed("zarr"):
        return f"Zarr export requires the zarr package. Install with: {ALL_EXPORTS_INSTALL}"
    if format == "rlds" and not _module_installed("tensorflow"):
        hint = tensorflow_install_hint()
        if hint == ALL_EXPORTS_INSTALL:
            hint = f"Install with: {hint}"
        return f"RLDS export needs tensorflow, which {_tensorflow_gap()}. {hint}"
    if format == "lerobot":
        from resurrector.core.lerobot_export import INSTALL_HINT, LEROBOT_MIN_PYTHON
        python, _, _ = _running_platform()
        if python < LEROBOT_MIN_PYTHON or not _module_installed("lerobot"):
            return INSTALL_HINT
    return None


def require_export_dependencies(format: str) -> None:
    """Raise ``ImportError`` before any output is written if ``format``'s
    optional dependency is missing, so a failed export leaves no empty
    directory behind.

    For ``lerobot`` this also imports LeRobot's writer: the export is about
    to anyway, and a LeRobot that is installed but won't import (e.g.
    without its ``[dataset]`` extra) would otherwise fail only after a
    split or dataset export had created its directories.
    """
    problem = export_dependency_problem(format)
    if problem:
        raise ImportError(problem)
    if format == "lerobot":
        from resurrector.core.lerobot_export import import_lerobot_dataset
        import_lerobot_dataset()


@dataclass(frozen=True)
class ExportPreset:
    """A named bundle of export settings for common workflows.

    Presets are convention-over-configuration shortcuts. User-supplied
    flags always override preset values, so a preset is a baseline,
    not a constraint.

    Attributes:
        name: Short identifier (used as the ``--preset`` flag value).
        format: Default export format (``parquet``, ``hdf5``, etc.).
        sync: Whether to time-align selected topics before export.
        sync_method: ``nearest`` / ``interpolate`` / ``sample_and_hold``.
        downsample_hz: Resample rate applied per chunk. ``None`` = native.
        topic_filter: Selector applied to ``bf.topic_names`` when the
            user doesn't pass ``--topics``. ``"images"`` / ``"non-images"``
            / ``None`` (no filter).
        description: One-line human-readable description (used in
            ``--help`` output and the dashboard preset dropdown).
        extras_required: Optional list of pip extras the format needs
            at runtime (e.g. ``["all-exports"]`` for zarr/rlds).
    """
    name: str
    format: str
    sync: bool
    sync_method: str
    downsample_hz: float | None
    topic_filter: str | None
    description: str
    extras_required: tuple[str, ...] = ()


# The ROS image message types we use to identify "image topics" for
# preset filtering. Kept in sync with resurrector.core.bag_frame.
_IMAGE_MESSAGE_TYPES = {
    "sensor_msgs/msg/Image",
    "sensor_msgs/msg/CompressedImage",
}


PRESETS: dict[str, ExportPreset] = {
    "lerobot": ExportPreset(
        name="lerobot",
        format="lerobot",
        sync=True,
        sync_method="nearest",
        downsample_hz=30.0,
        topic_filter=None,  # LeRobot wants images + state, all selected topics
        description=(
            "LeRobot-format dataset for robot-learning training. "
            "Resampled onto a uniform 30 fps grid, camera topics as video."
        ),
        extras_required=("lerobot",),
    ),
    "rlds": ExportPreset(
        name="rlds",
        format="rlds",
        sync=True,
        sync_method="nearest",
        downsample_hz=10.0,
        topic_filter=None,
        description=(
            "RLDS / TFRecord for RT-2 / OpenX-style training pipelines. "
            "Time-synced, 10 Hz."
        ),
        extras_required=("all-exports",),
    ),
    "training-tabular": ExportPreset(
        name="training-tabular",
        format="parquet",
        sync=True,
        sync_method="nearest",
        downsample_hz=50.0,
        topic_filter="non-images",
        description=(
            "Numerical sensor data for classical ML. Parquet, time-synced "
            "at 50 Hz, image topics excluded."
        ),
    ),
    "camera-only": ExportPreset(
        name="camera-only",
        format="hdf5",
        sync=False,
        sync_method="nearest",
        downsample_hz=None,
        topic_filter="images",
        description=(
            "Image topics only, native rates. HDF5 for CV training data prep."
        ),
    ),
    "multimodal": ExportPreset(
        name="multimodal",
        format="zarr",
        sync=True,
        sync_method="nearest",
        downsample_hz=None,
        topic_filter=None,
        description=(
            "All topics, time-synced, Zarr for chunked multimodal datasets."
        ),
        extras_required=("all-exports",),
    ),
}


def list_presets() -> list[ExportPreset]:
    """Return every named preset as a list (useful for the dashboard API)."""
    return list(PRESETS.values())


def resolve_preset(
    preset_name: str | None,
    *,
    format: str | None = None,
    sync: bool | None = None,
    sync_method: str | None = None,
    downsample_hz: float | None = None,
    topics: list[str] | None = None,
) -> dict:
    """Merge a named preset with user-supplied overrides.

    User-supplied values always win; the preset only fills holes. This
    is the "preset is a baseline, not a constraint" rule. Returns a
    dict with the resolved settings ready to pass to ``Exporter.export``.

    Args:
        preset_name: Name of a preset in ``PRESETS``, or ``None`` for
            no preset (returns the user's values verbatim, with sensible
            defaults for any missing).
        format / sync / sync_method / downsample_hz / topics: User-supplied
            values. ``None`` for any field means "use the preset's value
            if there is one, else the default".

    Returns:
        ``dict`` with keys: ``format``, ``sync``, ``sync_method``,
        ``downsample_hz``, ``topics`` (may be None — caller resolves to
        all-topics later), and ``topic_filter`` (the preset's filter, or
        None). Caller applies the topic_filter to bag_frame.topics if
        the user didn't pass explicit topics.

    Raises:
        ValueError: If ``preset_name`` is not in ``PRESETS``.
    """
    if preset_name is None:
        return {
            "format": format if format is not None else "parquet",
            "sync": sync if sync is not None else False,
            "sync_method": sync_method if sync_method is not None else "nearest",
            "downsample_hz": downsample_hz,
            "topics": topics,
            "topic_filter": None,
        }

    if preset_name not in PRESETS:
        available = ", ".join(sorted(PRESETS.keys()))
        raise ValueError(
            f"Unknown preset {preset_name!r}. Available: {available}"
        )

    preset = PRESETS[preset_name]
    return {
        "format": format if format is not None else preset.format,
        "sync": sync if sync is not None else preset.sync,
        "sync_method": sync_method if sync_method is not None else preset.sync_method,
        "downsample_hz": downsample_hz if downsample_hz is not None else preset.downsample_hz,
        "topics": topics,  # User's explicit topic list always wins
        "topic_filter": preset.topic_filter if topics is None else None,
    }


def apply_topic_filter(
    bag_frame: "BagFrame",
    topic_filter: str | None,
) -> list[str]:
    """Apply a preset's ``topic_filter`` to a BagFrame's topic list.

    Args:
        bag_frame: The :class:`BagFrame` to filter.
        topic_filter: ``"images"`` (only image topics) / ``"non-images"``
            (only non-image topics) / ``None`` (all topics).

    Returns:
        Filtered list of topic names.
    """
    if topic_filter is None:
        return bag_frame.topic_names
    image_topic_names = {
        t.name for t in bag_frame.topics
        if t.message_type in _IMAGE_MESSAGE_TYPES
    }
    if topic_filter == "images":
        return [n for n in bag_frame.topic_names if n in image_topic_names]
    if topic_filter == "non-images":
        return [n for n in bag_frame.topic_names if n not in image_topic_names]
    raise ValueError(
        f"Unknown topic_filter {topic_filter!r}. "
        f"Expected 'images', 'non-images', or None."
    )


@dataclass
class ExportColumnFailure:
    """One column that failed to serialize during export."""
    column: str
    error_type: str
    message: str


class ExportError(Exception):
    """Raised when the chosen format can't store some columns.

    The failed columns are not in the output file; every other column is
    complete. Inspect ``failures`` to see which columns failed and why.

    The message names each column with its reason, says those columns
    are not in the file and, for HDF5, Zarr and ``.npz``, points at
    Parquet, so the CLI and the dashboard show ``str(error)`` as it is.
    """

    def __init__(self, failures: list[ExportColumnFailure], output: Path):
        self.failures = failures
        self.output = output
        reasons = "\n".join(
            f"  - {f.column}: {f.error_type}: {f.message}" for f in failures
        )
        # Every caller (Exporter.export, splits, dataset versions, trim)
        # lets this propagate, so nothing after this file gets written.
        message = (
            f"{len(failures)} column(s) could not be written to {output}:\n"
            f"{reasons}\n"
            "These columns are not in that file; every other column is "
            "complete. The export stopped there, so any later topics, splits "
            "or bags were not exported."
        )
        if self.suggests_parquet:
            message += (
                " To keep a column this format can't store, export to "
                "Parquet, which stores every column type."
            )
        super().__init__(message)

    @property
    def suggests_parquet(self) -> bool:
        """True when the message suggests Parquet: the failed file is
        HDF5, Zarr or ``.npz``, which can't store every column type.

        CSV and Parquet fix their columns from the first chunk, so their
        failures are columns that appear or change type later, which
        exporting to Parquet doesn't fix.
        """
        return Path(self.output).suffix in (".h5", ".zarr", ".npz")


@dataclass
class ExportResult:
    path: Path
    rows_written: int
    failures: list[ExportColumnFailure] = field(default_factory=list)


class Exporter:
    """Lower-level export engine. Most users should call :meth:`BagFrame.export` instead.

    Use this directly if you want fine-grained control: per-topic
    streaming via :meth:`export_frames` / :meth:`export_video`, or
    custom orchestration where ``BagFrame.export`` doesn't quite fit.

    Chunk-streaming export paths keep peak memory near one chunk
    (``CHUNK_SIZE`` rows), regardless of total topic size, synced or not
    (below ``LARGE_TOPIC_THRESHOLD`` the eager sync engine loads the
    topics it aligns). Exceptions:
    ``numpy`` materializes per-topic and refuses topics over
    ``NUMPY_HARD_CAP`` (1 M rows); ``lerobot`` streams its input but holds
    one episode's frame grid in memory (LeRobot's writer buffers episodes).

    Example::

        from resurrector import BagFrame
        from resurrector.core.export import Exporter

        bf = BagFrame("experiment.mcap")
        Exporter().export(
            bag_frame=bf,
            topics=["/imu/data", "/joint_states"],
            format="parquet",
            output_dir="./out",
        )
    """

    def export(
        self,
        bag_frame: "BagFrame",
        topics: list[str],
        format: str = "parquet",
        output_dir: str = "./export",
        sync: bool = False,
        sync_method: str = "nearest",
        downsample_hz: float | None = None,
        task: str | None = None,
        action_topics: Sequence[str] = (),
    ) -> Path:
        """Stream-export selected topics to the given format.

        Args:
            bag_frame: The :class:`BagFrame` to read from.
            topics: List of topic names. Missing topics are logged and
                skipped — the export does NOT fail on a single missing
                topic.
            format: ``parquet`` (default), ``hdf5``, ``csv``, ``numpy``,
                ``zarr`` (needs ``[all-exports]``), ``lerobot`` (needs
                ``[lerobot]``, Python 3.12+), or ``rlds`` (needs
                ``[all-exports]``, which installs tensorflow where it
                ships wheels; see :data:`TENSORFLOW_PLATFORMS`).
                ``hdf5``, ``zarr`` and ``numpy``
                write integer and Boolean columns as float64, NaN for a
                missing value (``timestamp_ns`` stays int64); Parquet
                keeps every dtype and nulls as they are.
            output_dir: Directory to write into. Created if missing.
            sync: When True (and 2+ topics), time-align with
                :func:`~resurrector.core.sync.iter_synchronize` (same rows
                as :meth:`BagFrame.sync`) and stream the result, chunk by
                chunk, into a single ``synced.<ext>`` file.
            sync_method: ``nearest`` / ``interpolate`` / ``sample_and_hold``.
                Only used when ``sync`` is True.
            downsample_hz: Resampling rate before writing. ``None``
                preserves the native rate. Synced output is downsampled as
                one stream (the same rows as downsampling the whole table);
                unsynced topics are downsampled per chunk, so their grid
                restarts every ``CHUNK_SIZE`` rows. For ``lerobot`` this is
                the integer fps of the uniform frame grid (default 30).
            task: ``lerobot`` only: task label attached to every frame.
            action_topics: ``lerobot`` only: topics whose numeric fields
                form the ``action`` vector instead of ``observation.state``.

        Returns:
            ``Path`` to ``output_dir``.

        Raises:
            LargeTopicError: If ``format == "numpy"`` and a topic has
                more than ``NUMPY_HARD_CAP`` rows.
            ImportError: The format's optional dependency is missing.
                Raised before ``output_dir`` is created.
            ValueError: For unknown format strings.
            KeyError: ``sync=True`` and a topic isn't in the bag.
            SyncOutOfOrderError, SyncBufferExceededError,
                SyncSchemaDriftError: ``sync=True`` on the streaming
                engine (topics over ``LARGE_TOPIC_THRESHOLD``). Raised
                mid-stream, so ``synced.<ext>`` may already be partly
                written.
            ExportError: Some columns couldn't be written (e.g. list,
                struct or binary columns, or a column that first
                appears after a CSV or Parquet file's columns were set
                by its first chunk); the other columns are complete.
        """
        require_export_dependencies(format)
        output_path = Path(output_dir)

        if format == "lerobot":
            # LeRobot needs every topic in one uniform-grid episode and
            # writes through its own API, so it can't use the per-topic
            # chunk dispatch below. ``sync`` is implied by the grid.
            from resurrector.core.lerobot_export import DEFAULT_FPS, export_lerobot
            export_lerobot(
                [bag_frame], topics, output_path,
                fps=int(round(downsample_hz or DEFAULT_FPS)),
                task=task or bag_frame.path.stem,
                action_topics=action_topics,
            )
            return output_path

        output_path.mkdir(parents=True, exist_ok=True)

        if sync and len(topics) > 1:
            from resurrector.core.sync import iter_synchronize
            from resurrector.core.transforms import iter_downsample_temporal

            views = {name: bag_frame[name] for name in topics}
            chunks = iter_synchronize(views, method=sync_method, chunk_size=CHUNK_SIZE)
            if downsample_hz:
                chunks = iter_downsample_temporal(chunks, downsample_hz)
            self._stream_dataframe_chunks(
                _at_least_one_chunk(chunks), format, output_path, "synced",
            )
            return output_path

        for topic in topics:
            try:
                view = bag_frame[topic]
            except KeyError:
                logger.warning("Topic '%s' not found, skipping", topic)
                continue

            # Pre-flight: NumPy export is hard-capped because .npz
            # can't append. Refuse early with a clear pointer to
            # Parquet rather than letting the user wait through a
            # multi-GB materialization.
            if format == "numpy" and view.message_count > NUMPY_HARD_CAP:
                raise LargeTopicError(
                    topic_name=view.name,
                    message_count=view.message_count,
                    threshold=NUMPY_HARD_CAP,
                )

            safe_name = topic.lstrip("/").replace("/", "_")
            chunks = _transform_chunks(
                view.iter_chunks(CHUNK_SIZE), downsample_hz
            )
            self._stream_dataframe_chunks(chunks, format, output_path, safe_name)

        return output_path

    def _stream_dataframe_chunks(
        self,
        chunks: Iterable,
        format: str,
        output_path: Path,
        name: str,
    ) -> ExportResult:
        """Dispatch streaming chunks to the right format writer."""
        if format == "parquet":
            return _stream_parquet(chunks, output_path, name)
        elif format == "csv":
            return _stream_csv(chunks, output_path, name)
        elif format == "hdf5":
            return _stream_hdf5(chunks, output_path, name)
        elif format == "numpy":
            return _stream_numpy(chunks, output_path, name)
        elif format == "zarr":
            return _stream_zarr(chunks, output_path, name)
        elif format == "lerobot":
            raise ValueError(
                "LeRobot export needs the whole bag, not a chunk stream; "
                "use Exporter.export(format='lerobot') or export_lerobot()"
            )
        elif format == "rlds":
            return _stream_rlds(chunks, output_path, name)
        else:
            raise ValueError(
                f"Unknown export format: {format}. "
                f"Supported: parquet, hdf5, csv, numpy, zarr, lerobot, rlds"
            )

    def export_frames(
        self,
        topic_view,
        output_dir: str | Path,
        format: str = "png",
        max_frames: int | None = None,
        every_n: int = 1,
    ) -> Path:
        """Write every frame of an image topic as a numbered image file.

        Args:
            topic_view: A :class:`~resurrector.core.bag_frame.TopicView`
                for an image topic.
            output_dir: Parent directory; a sub-directory named after
                the topic is created beneath it.
            format: ``png`` (lossless) or ``jpeg`` (smaller, lossy).
            max_frames: Stop after this many frames. ``None`` for no limit.
            every_n: Sample every Nth frame (1 = every frame, 5 = thin a
                30 Hz stream to 6 Hz).

        Returns:
            ``Path`` to the topic's frames directory.

        Raises:
            ImportError: If Pillow, a base dependency, is missing (a
                partial install).

        Example::

            Exporter().export_frames(bf["/camera/rgb"], "./frames", every_n=5)
        """
        try:
            from PIL import Image as PILImage
        except ImportError:
            raise ImportError(
                "Frame export requires Pillow, which this install is "
                "missing. Install with: pip install Pillow"
            )

        output_path = Path(output_dir)
        safe_name = topic_view.name.lstrip("/").replace("/", "_")
        frames_dir = output_path / safe_name
        frames_dir.mkdir(parents=True, exist_ok=True)

        count = 0
        for i, (ts, arr) in enumerate(topic_view.iter_images()):
            if i % every_n != 0:
                continue
            img = PILImage.fromarray(arr)
            ext = "jpg" if format == "jpeg" else format
            img.save(frames_dir / f"frame_{count:06d}.{ext}")
            count += 1
            if max_frames and count >= max_frames:
                break

        logger.info("Exported %d frames to %s", count, frames_dir)
        return frames_dir

    def export_video(
        self,
        topic_view,
        output_path: str | Path,
        fps: float | None = None,
        codec: str = "mp4v",
    ) -> Path:
        """Encode an image topic as a single MP4 video file (via OpenCV).

        Useful for quick visual review of a long camera recording without
        materializing thousands of PNGs.

        Args:
            topic_view: A :class:`TopicView` for an image topic.
            output_path: MP4 file to write. Parent directory is created.
            fps: Output frame rate. Defaults to ``topic_view.frequency_hz``
                (the recorded rate) if available, else 30.0.
            codec: FourCC string for the encoder. Default ``"mp4v"``;
                use ``"avc1"`` for broader compatibility on some players.

        Returns:
            ``Path`` to the written MP4.

        Raises:
            ImportError: If OpenCV isn't installed (in ``[vision-lite]``).

        Example::

            Exporter().export_video(bf["/camera/rgb"], "preview.mp4", fps=10)
        """
        try:
            import cv2
        except ImportError:
            raise ImportError(
                "Video export requires OpenCV. "
                "Install with: pip install 'rosbag-resurrector[vision-lite]'"
            )

        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        if fps is None:
            fps = topic_view.frequency_hz or 30.0

        writer = None
        count = 0
        try:
            for ts, arr in topic_view.iter_images():
                if writer is None:
                    h, w = arr.shape[:2]
                    fourcc = cv2.VideoWriter_fourcc(*codec)
                    writer = cv2.VideoWriter(str(output_file), fourcc, fps, (w, h))
                if len(arr.shape) == 3 and arr.shape[2] == 3:
                    arr = arr[:, :, ::-1]
                writer.write(arr)
                count += 1
        finally:
            if writer is not None:
                writer.release()

        logger.info("Exported %d frames as video to %s", count, output_file)
        return output_file


def _transform_chunks(chunks: Iterable, downsample_hz: float | None) -> Iterator:
    """Apply optional downsampling to each chunk as it streams through."""
    if downsample_hz is None:
        yield from chunks
        return
    from resurrector.core.transforms import downsample_temporal
    for chunk in chunks:
        yield downsample_temporal(chunk, downsample_hz)


def _at_least_one_chunk(chunks: Iterable) -> Iterator:
    """Pass chunks through; if there were none, yield one empty frame so
    the writer still creates its (empty) output file."""
    empty = True
    for chunk in chunks:
        empty = False
        yield chunk
    if empty:
        yield pl.DataFrame()


def _with_final_flag(chunks: Iterable) -> Iterator[tuple]:
    """Yield ``(chunk, is_final)`` for each non-empty chunk.

    Reads one chunk ahead, so at most two chunks are alive at once.
    """
    pending = None
    for chunk in chunks:
        if chunk.height == 0:
            continue
        if pending is not None:
            yield pending, False
        pending = chunk
    if pending is not None:
        yield pending, True


# ---------------------------------------------------------------------------
# Streaming writers — one per format. Each consumes an iterable of
# pl.DataFrame chunks, writes them, and returns an ExportResult with any
# per-column failures collected.
# ---------------------------------------------------------------------------


# float64 represents every integer up to this magnitude exactly.
_FLOAT64_EXACT_INT = 2**53


class _NumpyColumns:
    """Column -> NumPy conversion for the HDF5, Zarr and ``.npz`` writers.

    HDF5 and Zarr append each chunk to an array whose dtype is fixed by
    the first chunk written (``.npz`` follows the same rule so the three
    agree), so a column's NumPy dtype is chosen once, from its polars
    dtype, assuming any chunk may hold nulls. (A chunk's own conversion
    won't do: Int64 converts to int64 or float64, Boolean to bool or
    object, depending on whether that chunk has a null.)

    - Float32 / Float64 keep their width; null -> NaN.
    - Integer and Boolean columns -> float64 (Booleans as 1.0 / 0.0);
      null -> NaN. Integers beyond +/-2**53 are rounded, with a warning.
    - ``timestamp_ns`` stays int64: every row has one, and float64 would
      round nanosecond timestamps.
    - String, Categorical and Enum columns -> text (an object array of
      ``str``; each writer stores it as its own string type); null ->
      ``""``. None of the three formats has a missing string, so a null
      and an empty string read back the same; Parquet keeps them apart.
    - A chunk where the column is all null (polars dtype Null) says
      nothing about its dtype, so those rows are held back until a chunk
      does, then written first as that dtype's missing value. Rows where
      the column is absent (a chunk without it, or the rows before the
      chunk it first appears in) are treated the same way. A column that
      is null or absent in every chunk is written as float64 NaN.
    - Anything else converts as polars does, and the writer decides
      whether it can store it. A column that converts to an object array
      (lists, structs, binary, ...) fails rather than being written as
      Python reprs or pickled objects.

    A later chunk whose column can't be cast to the chosen dtype fails
    that column instead of being stored wrong, and so does a column with
    rows to fill whose dtype has no missing value (``timestamp_ns``,
    datetimes).
    """

    def __init__(self) -> None:
        self._targets: dict[str, "pl.DataType | None"] = {}
        self._first_dtypes: dict[str, pl.DataType] = {}
        self._held: dict[str, int] = {}
        self._warned: set[str] = set()

    def is_text(self, col: str) -> bool:
        """True if ``col`` is written as strings."""
        return self._targets.get(col) == pl.String

    def knows(self, col: str) -> bool:
        return col in self._targets or col in self._held

    def known(self) -> list[str]:
        """Every column seen so far, typed or still held."""
        return list(self._targets) + [c for c in self._held if c not in self._targets]

    def hold(self, col: str, rows: int) -> None:
        """Hold ``rows`` missing rows for ``col`` ahead of its next values."""
        self._held[col] = self._held.get(col, 0) + rows

    def pad(
        self, col: str, rows: int, start: int,
    ) -> tuple[Iterator[np.ndarray], ExportColumnFailure | None]:
        """Missing values for the ``rows`` rows from row ``start`` of a
        chunk that lacks known column ``col``, or a failure record."""
        if col not in self._targets:
            self.hold(col, rows)
            return iter(()), None
        missing = _missing_value(self._targets[col])
        if missing is None:
            return iter(()), ExportColumnFailure(
                column=col, error_type="ValueError",
                message=(
                    f"absent from rows {start} to {start + rows - 1}, and "
                    f"{self._first_dtypes[col]} has no missing value in this "
                    f"format to fill them with"
                ),
            )
        return _repeat_rows(*missing, rows), None

    def convert(
        self, chunk, col: str,
    ) -> tuple[Iterator[np.ndarray], ExportColumnFailure | None]:
        """Arrays to append for one column of ``chunk``, rows held back
        for it first, or a failure record."""
        series = chunk[col]
        if col not in self._targets:
            if series.dtype == pl.Null:
                self.hold(col, len(series))
                return iter(()), None
            self._targets[col] = _numpy_target(col, series.dtype)
            self._first_dtypes[col] = series.dtype
        target = self._targets[col]
        held = self._held.pop(col, 0)
        try:
            arr = self._to_numpy(series, col, target)
            if not held:
                return iter((arr,)), None
            missing = _missing_value(target)
            if missing is None:
                raise ValueError(
                    f"absent or null in its first {held} rows, and "
                    f"{self._first_dtypes[col]} has no missing value in this "
                    f"format to fill them with"
                )
            return _held_then(_repeat_rows(*missing, held), arr), None
        except Exception as e:
            return iter(()), ExportColumnFailure(
                column=col, error_type=type(e).__name__, message=str(e),
            )

    def never_typed(self) -> Iterator[tuple[str, Iterator[np.ndarray]]]:
        """Columns that were null or absent in every chunk, as float64 NaN
        (an empty array for a column seen only in 0-row chunks)."""
        for col, held in self._held.items():
            if held:
                yield col, _repeat_rows(np.nan, np.dtype(np.float64), held)
            else:
                yield col, iter((np.empty(0, dtype=np.float64),))

    def _to_numpy(self, series: "pl.Series", col: str, target) -> np.ndarray:
        dtype = series.dtype
        if target is None:
            first = self._first_dtypes[col]
            if dtype != first:
                raise TypeError(
                    f"column is {dtype} in this chunk but was {first} in an "
                    f"earlier one"
                )
            arr = series.to_numpy()
            if arr.dtype == object:
                raise TypeError(
                    f"{dtype} columns can't be written as a numeric or string "
                    f"array; export to Parquet to keep them"
                )
            return arr
        if target == pl.String:
            if not (_is_text(dtype) or dtype == pl.Null):
                raise TypeError(
                    f"column is {dtype} in this chunk but was written as "
                    f"{target} from an earlier one"
                )
            return series.cast(pl.String).fill_null("").to_numpy()
        if not (dtype.is_numeric() or dtype == pl.Boolean or dtype == pl.Null):
            raise TypeError(
                f"column is {dtype} in this chunk but was written as "
                f"{target} from an earlier one"
            )
        if target == pl.Int64 and series.null_count():
            raise ValueError(f"{col} has missing values")
        if (
            dtype.is_integer() and target == pl.Float64
            and col not in self._warned
            and _beyond_float64_exact(series)
        ):
            logger.warning(
                "Column %r has integers beyond +/-2**53; they are written "
                "as float64 and rounded. Export to Parquet to keep them "
                "exact.", col,
            )
            self._warned.add(col)
        return series.cast(target, strict=True).to_numpy()


def _write_numpy_columns(
    chunks: Iterable,
    append: Callable[[str, np.ndarray, bool], None],
) -> tuple[int, list[ExportColumnFailure]]:
    """Convert every column of every chunk with :class:`_NumpyColumns` and
    pass it to ``append(col, array, is_text)``, which creates the
    column's array on its first call and appends to it after that.

    Rows stay aligned when the column set changes between chunks: a
    known column that a chunk lacks gets that chunk's rows as missing
    values, and a column first seen after rows were written gets those
    rows first (held, so they're written in ``CHUNK_SIZE`` pieces).
    ``append`` is never called with a 0-row array, except once at the end
    for a column seen only in 0-row chunks, so its (empty) array exists.

    A column that fails to convert or append is recorded once and skipped
    from then on; so is any column that doesn't end with exactly
    ``rows_written`` rows. The caller removes failed columns from its
    output. Returns ``(rows_written, failures)``.
    """
    columns = _NumpyColumns()
    failures: list[ExportColumnFailure] = []
    failed: set[str] = set()
    lengths: dict[str, int] = {}
    empty: dict[str, np.ndarray] = {}
    rows_written = 0

    def fail(failure: ExportColumnFailure) -> None:
        failures.append(failure)
        failed.add(failure.column)

    def write(col: str, arrays: Iterator[np.ndarray]) -> None:
        try:
            for arr in arrays:
                if len(arr) == 0:
                    empty.setdefault(col, arr)
                    continue
                append(col, arr, columns.is_text(col))
                lengths[col] = lengths.get(col, 0) + len(arr)
        except Exception as e:
            fail(ExportColumnFailure(
                column=col, error_type=type(e).__name__, message=str(e),
            ))

    for chunk in chunks:
        present = set(chunk.columns)
        for col in columns.known():
            if col in present or col in failed or chunk.height == 0:
                continue
            arrays, failure = columns.pad(col, chunk.height, rows_written)
            if failure is not None:
                fail(failure)
                continue
            write(col, arrays)
        for col in chunk.columns:
            if col in failed:
                continue
            if rows_written and not columns.knows(col):
                columns.hold(col, rows_written)
            arrays, failure = columns.convert(chunk, col)
            if failure is not None:
                fail(failure)
                continue
            write(col, arrays)
        rows_written += chunk.height
    for col, arrays in columns.never_typed():
        write(col, arrays)
    for col, arr in empty.items():
        if col not in lengths and col not in failed:
            try:
                append(col, arr, columns.is_text(col))
                lengths[col] = 0
            except Exception as e:
                fail(ExportColumnFailure(
                    column=col, error_type=type(e).__name__, message=str(e),
                ))
    for col, rows in lengths.items():
        if col not in failed and rows != rows_written:
            fail(ExportColumnFailure(
                column=col, error_type="ValueError",
                message=f"{rows} rows written where the export has {rows_written}",
            ))
    return rows_written, failures


def _is_text(dtype) -> bool:
    return dtype == pl.String or isinstance(dtype, (pl.Categorical, pl.Enum))


def _missing_value(target) -> tuple[object, np.dtype] | None:
    """What a missing value is written as for ``target``, and as which
    NumPy dtype, or None if it has none."""
    if target == pl.String:
        return "", np.dtype(object)
    if target == pl.Float32:
        return np.nan, np.dtype(np.float32)
    if target == pl.Float64:
        return np.nan, np.dtype(np.float64)
    return None


def _repeat_rows(value, dtype: np.dtype, rows: int) -> Iterator[np.ndarray]:
    """``rows`` copies of ``value``, at most ``CHUNK_SIZE`` at a time, so
    a long all-null run doesn't become one big array."""
    while rows > 0:
        size = min(rows, CHUNK_SIZE)
        yield np.full(size, value, dtype=dtype)
        rows -= size


def _held_then(held: Iterator[np.ndarray], arr: np.ndarray) -> Iterator[np.ndarray]:
    yield from held
    yield arr


def _beyond_float64_exact(series: "pl.Series") -> bool:
    """True if an integer series holds a value beyond +/-2**53, where
    float64 stops representing every integer. Checked on the integers
    themselves: after the cast, 2**53 + 1 has already become 2**53."""
    lo, hi = series.min(), series.max()
    return (hi is not None and hi > _FLOAT64_EXACT_INT) or (
        lo is not None and lo < -_FLOAT64_EXACT_INT
    )


def _numpy_target(col: str, dtype) -> "pl.DataType | None":
    """The polars dtype a column is cast to before NumPy conversion, or
    None to convert it as is. See :class:`_NumpyColumns`."""
    if col == "timestamp_ns" and dtype.is_integer():
        return pl.Int64
    if dtype == pl.Float32:
        return pl.Float32
    if dtype.is_integer() or dtype.is_float() or dtype == pl.Boolean:
        return pl.Float64
    if _is_text(dtype):
        return pl.String
    return None


class _FixedColumns:
    """The columns a CSV or Parquet file took from its first chunk.

    Both formats fix their columns when the first chunk is written (the
    CSV header, the Parquet schema). A later chunk's own columns are
    never written as they are: a chunk without one of the columns, or
    with them in another order, would otherwise shift values under the
    wrong header or fail the Parquet writer. Each later chunk is laid out
    as the file's columns instead: a column it lacks is null (empty in
    CSV), and a column that first appears in it is reported once and
    left out, since the file can't add it.
    """

    def __init__(self, columns: Sequence[str], file_kind: str) -> None:
        self.columns = list(columns)
        self._names = set(self.columns)
        self._file_kind = file_kind
        self.failures: list[ExportColumnFailure] = []
        self.failed: set[str] = set()

    def fail(self, col: str, error_type: str, message: str) -> None:
        self.failures.append(ExportColumnFailure(
            column=col, error_type=error_type, message=message,
        ))
        self.failed.add(col)

    def report_new(self, chunk_columns: Sequence[str], start: int, rows: int) -> None:
        """Report columns first seen in a chunk of ``rows`` rows that
        starts at row ``start``. A 0-row chunk loses no values."""
        if rows == 0:
            return
        for col in chunk_columns:
            if col not in self._names and col not in self.failed:
                self.fail(
                    col, "ValueError",
                    f"first appears at row {start}, after the {self._file_kind} "
                    f"took its columns from the first chunk, so it is not in "
                    f"the file",
                )


def _stream_parquet(chunks: Iterable, output_path: Path, name: str) -> ExportResult:
    """Stream chunks to one Parquet file, a row group per chunk.

    The schema comes from the first chunk (see :class:`_FixedColumns`).
    A later chunk whose column has another type is cast to the file's
    type only when every value survives the cast; otherwise the column
    is reported and written as null from that chunk on.
    """
    import pyarrow.parquet as pq

    filepath = output_path / f"{name}.parquet"
    writer = None
    schema = None
    fixed: _FixedColumns | None = None
    rows_written = 0
    try:
        for chunk in chunks:
            table = chunk.to_arrow()
            if writer is None:
                schema = table.schema
                writer = pq.ParquetWriter(str(filepath), schema)
                fixed = _FixedColumns(schema.names, "Parquet schema")
            else:
                table = _fit_arrow_table(table, schema, fixed, rows_written)
            writer.write_table(table)
            rows_written += chunk.height
    finally:
        if writer is not None:
            writer.close()

    logger.info("Streamed %d rows to %s", rows_written, filepath)
    if fixed is not None and fixed.failures:
        raise ExportError(fixed.failures, filepath)
    return ExportResult(path=filepath, rows_written=rows_written)


def _fit_arrow_table(table, schema, fixed: _FixedColumns, start: int):
    """``table`` laid out as ``schema``: missing or failed columns null,
    other types cast losslessly or the column reported."""
    import pyarrow as pa

    fixed.report_new(table.column_names, start, table.num_rows)
    present = set(table.column_names)
    arrays = []
    for spec in schema:
        column = None
        if spec.name in present and spec.name not in fixed.failed:
            column = table.column(spec.name)
            if not column.type.equals(spec.type):
                cast = _lossless_cast(column, spec.type)
                if cast is None and pa.types.is_null(spec.type):
                    fixed.fail(spec.name, "TypeError", (
                        f"has no values in the first chunk, which set the "
                        f"file's columns, so the file stores it as all null "
                        f"and its {column.type} values from row {start} on "
                        f"are not written"
                    ))
                elif cast is None:
                    fixed.fail(spec.name, "TypeError", (
                        f"{column.type} values from row {start} on can't be "
                        f"stored losslessly as the file's {spec.type} column "
                        f"(its type comes from the first chunk), so it is "
                        f"null from row {start} on"
                    ))
                column = cast
        arrays.append(column if column is not None else pa.nulls(table.num_rows, spec.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def _lossless_cast(column, target):
    """``column`` cast to the Arrow type ``target``, or None if any value
    wouldn't survive it. Arrow's safe cast still lets some lossy casts
    through (float64 to float32, int to bool, "01" to 1), so the result
    must also cast back to the original values."""
    import pyarrow as pa
    import pyarrow.compute as pc

    try:
        cast = pc.cast(column, target, safe=True)
        if pa.types.is_null(column.type):
            return cast
        back = pc.cast(cast, column.type, safe=True)
        # polars' equals counts null == null and NaN == NaN as equal.
        if pl.from_arrow(back).equals(pl.from_arrow(column)):
            return cast
    except Exception:
        pass
    return None


def _stream_csv(chunks: Iterable, output_path: Path, name: str) -> ExportResult:
    """Stream chunks to one CSV file. The header comes from the first
    chunk (see :class:`_FixedColumns`); a null is an empty field."""
    filepath = output_path / f"{name}.csv"
    rows_written = 0
    fixed: _FixedColumns | None = None
    with open(filepath, "wb") as f:
        for chunk in chunks:
            first = fixed is None
            if first:
                fixed = _FixedColumns(chunk.columns, "CSV header")
            else:
                fixed.report_new(chunk.columns, rows_written, chunk.height)
                chunk = chunk.with_columns([
                    pl.lit(None).alias(c) for c in fixed.columns if c not in chunk.columns
                ]).select(fixed.columns)
            csv_bytes = chunk.write_csv(file=None, include_header=first).encode("utf-8")
            f.write(csv_bytes)
            rows_written += chunk.height
    logger.info("Streamed %d rows to %s", rows_written, filepath)
    if fixed is not None and fixed.failures:
        raise ExportError(fixed.failures, filepath)
    return ExportResult(path=filepath, rows_written=rows_written)


def _stream_hdf5(chunks: Iterable, output_path: Path, name: str) -> ExportResult:
    """Stream chunks to HDF5 using resizable datasets (append mode).

    Each column becomes a resizable dataset; each chunk extends it.
    Dataset dtypes come from the polars schema (see
    :class:`_NumpyColumns`: integers and Booleans as float64 with NaN for
    a missing value). String columns are variable-length UTF-8, with a
    missing string written as ``""`` (h5py reads them back as ``bytes``;
    ``dataset.asstr()[:]`` gives ``str``). Every dataset has one row per
    ``timestamp_ns`` (see :func:`_write_numpy_columns`). Columns that fail
    to serialize are removed from the file and reported.
    """
    import h5py

    filepath = output_path / f"{name}.h5"

    with h5py.File(filepath, "w") as f:
        group = f.create_group(name)
        datasets: dict[str, h5py.Dataset] = {}

        def append(col: str, arr: np.ndarray, text: bool) -> None:
            if col not in datasets:
                if text:
                    datasets[col] = group.create_dataset(
                        col, shape=(0,), maxshape=(None,),
                        dtype=h5py.string_dtype(),
                    )
                else:
                    datasets[col] = group.create_dataset(
                        col, shape=(0,), maxshape=(None,),
                        dtype=arr.dtype, compression="gzip",
                    )
            if len(arr) == 0:
                return
            ds = datasets[col]
            start = ds.shape[0]
            ds.resize((start + len(arr),))
            ds[start:] = arr

        rows_written, failures = _write_numpy_columns(chunks, append)
        for failure in failures:
            if failure.column in datasets:
                del group[failure.column]

    logger.info("Streamed %d rows to %s", rows_written, filepath)
    if failures:
        raise ExportError(failures, filepath)
    return ExportResult(path=filepath, rows_written=rows_written, failures=failures)


def _stream_numpy(chunks: Iterable, output_path: Path, name: str) -> ExportResult:
    """Write chunks into an .npz archive.

    NumPy's .npz format can't be incrementally appended, so this writer
    accumulates column arrays in memory and ``savez_compressed`` flushes
    at the end. Memory scales with total topic size, NOT chunk size —
    the v0.4.0 ``Exporter.export`` pre-flight refuses topics larger
    than ``NUMPY_HARD_CAP`` (1 M rows) before reaching here, raising
    :class:`LargeTopicError` with a pointer to Parquet (which streams).
    The cap exists because past ~1 M rows the materialized arrays
    plus ``savez_compressed`` buffers easily exceed 1 GB. Column dtypes
    follow :class:`_NumpyColumns`, as for HDF5 and Zarr.

    String columns are fixed-width unicode (``<U``, every row as wide as
    the longest value, 4 bytes per character), a missing string written
    as ``""``. Unlike an object array, that loads with ``np.load``'s
    default ``allow_pickle=False``. One long value widens every row, so
    a column whose fixed-width array would pass
    ``_NPZ_TEXT_WARN_BYTES`` logs a warning pointing to Parquet and Zarr,
    which store each string at its own length.
    """
    filepath = output_path / f"{name}.npz"
    col_chunks: dict[str, list[np.ndarray]] = {}
    text_cols: set[str] = set()

    def append(col: str, arr: np.ndarray, text: bool) -> None:
        if text:
            text_cols.add(col)
        col_chunks.setdefault(col, []).append(arr)

    rows_written, failures = _write_numpy_columns(chunks, append)
    failed_cols = {failure.column for failure in failures}

    # Joined one column at a time, its parts released as it goes, so the
    # parts and the joined arrays are never all held at once.
    arrays = {}
    for col in list(col_chunks):
        parts = col_chunks.pop(col)
        if col in failed_cols:
            continue
        if col in text_cols:
            arrays[col] = _fixed_width_text(col, parts)
        else:
            arrays[col] = np.concatenate(parts) if len(parts) > 1 else parts[0]
        del parts
    np.savez_compressed(filepath, **arrays)

    logger.info("Streamed %d rows to %s", rows_written, filepath)
    if failures:
        raise ExportError(failures, filepath)
    return ExportResult(path=filepath, rows_written=rows_written, failures=failures)


def _fixed_width_text(col: str, parts: list[np.ndarray]) -> np.ndarray:
    """One ``<U`` array from a text column's object-array parts.

    Filled part by part, each part dropped once copied, so only one
    fixed-width copy of the column is ever built (``np.concatenate`` of
    per-part ``<U`` arrays holds two).
    """
    total = sum(len(part) for part in parts)
    width = max(1, max((max(map(len, part), default=0) for part in parts), default=0))
    nbytes = total * width * 4
    if nbytes > _NPZ_TEXT_WARN_BYTES:
        logger.warning(
            "Column %r is written to .npz as fixed-width text: %d rows, each "
            "as wide as its longest value (%d characters), %.0f MB before "
            "compression and in memory while writing. Export to Parquet or "
            "Zarr to store each string at its own length.",
            col, total, width, nbytes / 2**20,
        )
    out = np.empty(total, dtype=f"<U{width}")
    start = 0
    for i, part in enumerate(parts):
        out[start:start + len(part)] = part
        start += len(part)
        parts[i] = None
    return out


def _stream_rlds(
    chunks: Iterable,
    output_path: Path,
    name: str,
) -> ExportResult:
    """Export to RLDS (TFRecord) format — streaming.

    Each chunk becomes a contiguous run of steps inside a single episode.
    Per-step features:
        observation: dict of all numeric columns (excluding timestamp_ns)
        action: empty dict (rosbag has no explicit action signal — users
                can post-process to extract actions from /cmd_vel etc.)
        reward: 0.0
        discount: 1.0
        is_first: True for first step
        is_last: True for last step
        is_terminal: True for last step

    Output: <output_path>/<name>.tfrecord

    Memory: two chunks. ``is_last`` comes from reading one chunk ahead
    (the last row of the last non-empty chunk), so it lands on the row
    actually written last even when downsampling or a time slice makes
    that differ from the topic's message count.
    """
    try:
        import tensorflow as tf
    except ImportError as e:
        # find_spec can see a tensorflow that still fails to import (a
        # broken install), in which case there's no packaging reason to give.
        raise ImportError(
            export_dependency_problem("rlds")
            or f"RLDS export needs tensorflow, which failed to import: {e}"
        ) from e

    output_path.mkdir(parents=True, exist_ok=True)
    filepath = output_path / f"{name}.tfrecord"
    rows_written = 0
    columns: list[str] = []

    def _to_feature(value) -> tf.train.Feature:
        if isinstance(value, (int, bool)):
            return tf.train.Feature(int64_list=tf.train.Int64List(value=[int(value)]))
        if isinstance(value, float):
            return tf.train.Feature(float_list=tf.train.FloatList(value=[float(value)]))
        if isinstance(value, str):
            return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value.encode("utf-8")]))
        # Fallback: stringify
        return tf.train.Feature(bytes_list=tf.train.BytesList(value=[str(value).encode("utf-8")]))

    with tf.io.TFRecordWriter(str(filepath)) as writer:
        for chunk, is_final_chunk in _with_final_flag(chunks):
            if not columns:
                columns = list(chunk.columns)
            chunk_dicts = chunk.to_dicts()
            last_row_idx = len(chunk_dicts) - 1
            for row_idx, row in enumerate(chunk_dicts):
                is_first = rows_written + row_idx == 0
                is_last = is_final_chunk and row_idx == last_row_idx

                feature_map: dict[str, tf.train.Feature] = {}
                for col, val in row.items():
                    if col == "timestamp_ns":
                        feature_map["step/timestamp_ns"] = _to_feature(val)
                    else:
                        feature_map[f"step/observation/{col}"] = _to_feature(val)

                feature_map["step/reward"] = _to_feature(0.0)
                feature_map["step/discount"] = _to_feature(1.0)
                feature_map["step/is_first"] = _to_feature(is_first)
                feature_map["step/is_last"] = _to_feature(is_last)
                feature_map["step/is_terminal"] = _to_feature(is_last)

                example = tf.train.Example(features=tf.train.Features(feature=feature_map))
                writer.write(example.SerializeToString())
            rows_written += chunk.height

    logger.info("Wrote RLDS TFRecord (%d steps) to %s", rows_written, filepath)
    return ExportResult(path=filepath, rows_written=rows_written)


def _stream_zarr(chunks: Iterable, output_path: Path, name: str) -> ExportResult:
    """Stream chunks to Zarr using appendable chunked arrays.

    Compatible with both zarr 2.x (DirectoryStore + create_dataset) and
    zarr 3.x (LocalStore + create_array). Detected at import time.
    Either way: peak memory bounded by chunk size, not topic size.
    Array dtypes follow :class:`_NumpyColumns`, as for HDF5. String
    columns are variable-length UTF-8 arrays (zarr 3's string dtype, or
    zarr 2's ``VLenUTF8`` object codec), with a missing string written as
    ``""``; ``zarr.open(path)[col][:]`` reads them back as text. Every
    array has one row per ``timestamp_ns`` (see
    :func:`_write_numpy_columns`) and ``CHUNK_SIZE``-row Zarr chunks,
    whatever the size of the chunk that created it. Columns that fail to
    serialize are removed from the store and reported.
    """
    try:
        import zarr
    except ImportError:
        raise ImportError(
            f"Zarr export requires the zarr package. Install with: {ALL_EXPORTS_INSTALL}"
        )

    filepath = output_path / f"{name}.zarr"
    filepath.parent.mkdir(parents=True, exist_ok=True)

    # Zarr 2.x → 3.x renamed DirectoryStore → LocalStore and create_dataset →
    # create_array. Detect via attribute presence to support both.
    zarr_v3 = not hasattr(zarr, "DirectoryStore")
    if zarr_v3:
        store = zarr.storage.LocalStore(str(filepath))
        root = zarr.create_group(store=store, overwrite=True)
    else:
        store = zarr.DirectoryStore(str(filepath))  # type: ignore[attr-defined]
        root = zarr.group(store, overwrite=True)

    arrays: dict = {}

    def append(col: str, arr: np.ndarray, text: bool) -> None:
        if col not in arrays:
            # Not the size of the chunk that types the column: a 3-row tail
            # typing a column held through 100k null rows would otherwise
            # store those rows as 33k 3-row chunk files.
            chunk_shape = (CHUNK_SIZE,)
            if zarr_v3:
                # For dtype=str, zarr 3.0 to 3.1.0 warn that the string
                # dtype isn't in the v3 spec yet. Left visible: other v3
                # readers of that era may not open these arrays.
                arrays[col] = root.create_array(
                    name=col, shape=(0,), chunks=chunk_shape,
                    dtype=str if text else arr.dtype,
                )
            elif text:
                import numcodecs
                arrays[col] = root.create_dataset(  # type: ignore[attr-defined]
                    col, shape=(0,), chunks=chunk_shape,
                    dtype=object, object_codec=numcodecs.VLenUTF8(),
                )
            else:
                arrays[col] = root.create_dataset(  # type: ignore[attr-defined]
                    col, shape=(0,), chunks=chunk_shape, dtype=arr.dtype,
                )
        if len(arr):
            arrays[col].append(arr)

    rows_written, failures = _write_numpy_columns(chunks, append)
    for failure in failures:
        if failure.column in arrays:
            del root[failure.column]

    logger.info("Streamed %d rows to %s", rows_written, filepath)
    if failures:
        raise ExportError(failures, filepath)
    return ExportResult(path=filepath, rows_written=rows_written, failures=failures)
