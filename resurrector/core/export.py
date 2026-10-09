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
as null from that chunk on. No writer turns text into numbers or numbers
into text. RLDS keeps one feature type per column (see
:func:`_stream_rlds`). Reported columns raise :class:`ExportError` once
the file is written; HDF5, Zarr and NumPy leave them out of the file.
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

from resurrector.core.exceptions import LargeTopicError, ResurrectorError

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


# ExportColumnFailure.kind: what went wrong, which decides what
# ExportError says about the column (see ExportError).
FAILURE_UNSTORABLE = "unstorable"
FAILURE_LATE_COLUMN = "late_column"
FAILURE_UNTYPED = "untyped_first_chunk"
FAILURE_TYPE_CHANGE = "type_change"
FAILURE_NO_MISSING_VALUE = "no_missing_value"
FAILURE_OTHER = "other"

_FORMAT_BY_SUFFIX = {
    ".parquet": "parquet", ".csv": "csv", ".h5": "hdf5", ".zarr": "zarr",
    ".npz": "numpy", ".tfrecord": "rlds",
}


@dataclass
class ExportColumnFailure:
    """One column that failed to serialize during export.

    ``kind`` is one of the ``FAILURE_*`` constants (see
    :class:`ExportError` for what each means per format); ``"other"``
    when nothing more specific applies. ``array_storable`` is False for a
    ``late_column`` or ``untyped_first_chunk`` failure whose type HDF5 and
    Zarr can't hold either (lists, structs, binary, datetimes), so the
    message doesn't send the user there.
    """
    column: str
    error_type: str
    message: str
    kind: str = FAILURE_OTHER
    array_storable: bool = True


class _ColumnError(Exception):
    """A column conversion failure that knows its reported error type
    and :class:`ExportColumnFailure` kind."""

    def __init__(self, error_type: str, kind: str, message: str):
        super().__init__(message)
        self.error_type = error_type
        self.kind = kind


def _column_failure(col: str, e: Exception, kind: str = FAILURE_OTHER) -> ExportColumnFailure:
    """The failure record for ``e``; ``kind`` unless ``e`` carries its own."""
    if isinstance(e, _ColumnError):
        return ExportColumnFailure(col, e.error_type, str(e), e.kind)
    return ExportColumnFailure(col, type(e).__name__, str(e), kind)


class ExportError(ResurrectorError):
    """Raised once a file is written when the chosen format couldn't
    write some columns. Every column not in ``failures`` is complete.

    What happened to a failed column depends on the format and on the
    failure's ``kind``:

    - HDF5, Zarr, ``.npz``: the column is removed from the file.
      ``unstorable``: no numeric or string array holds it (lists,
      structs, binary; datetimes in HDF5), and Parquet would store it.
      ``type_change``: a later chunk's values don't fit the dtype an
      earlier chunk set. ``no_missing_value``: rows where it is absent or
      null need a filler its dtype doesn't have (``timestamp_ns``,
      datetimes).
    - CSV: ``late_column``, first seen after the header was written from
      the first chunk; it is not in the file. HDF5 and Zarr add such
      columns.
    - Parquet: ``late_column`` as for CSV. ``untyped_first_chunk``: null
      in all of the first chunk, so it stays in the file as all null
      (HDF5 and Zarr keep its later values). ``type_change``: it stays in
      the file, null from the failing chunk on.
    - RLDS: ``type_change``; the feature is left out of the steps from
      the failing chunk on.

    The message lists each column with its reason, says which columns
    are not in the file and which are in it without some values, that
    the export stopped at this file, and which format would keep the
    columns where one would: Parquet for ``unstorable``, HDF5 or Zarr for
    ``late_column`` and ``untyped_first_chunk``. It never suggests the
    format the file already is. The CLI and the dashboard show
    ``str(error)`` as it is.
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
            f"{self._file_sentence()} The export stopped there, so any later "
            "topics, splits or bags were not exported."
        )
        for advice in self._advice():
            message += " " + advice
        super().__init__(message)

    @property
    def format(self) -> str | None:
        """The failed file's export format (``"parquet"``, ``"hdf5"``,
        ...), from its suffix; None if the suffix isn't an export's."""
        return _FORMAT_BY_SUFFIX.get(Path(self.output).suffix)

    def _kept(self, failure: ExportColumnFailure) -> bool:
        """True if the failed column is still in the file, without some
        of its values."""
        if self.format == "parquet":
            return failure.kind in (FAILURE_UNTYPED, FAILURE_TYPE_CHANGE)
        return self.format == "rlds" and failure.kind == FAILURE_TYPE_CHANGE

    def _file_sentence(self) -> str:
        kept = [f.column for f in self.failures if self._kept(f)]
        absent = [f.column for f in self.failures if not self._kept(f)]
        if not kept:
            return "These columns are not in that file; every other column is complete."
        if not absent:
            return (
                "These columns are in that file but have no values where the "
                "reasons above say; every other column is complete."
            )
        return (
            f"{_names(absent)} not in that file, and {_names(kept)} in it but "
            "without values where the reasons above say; every other column "
            "is complete."
        )

    def _late_kinds(self) -> set[str]:
        """The late/untyped kinds among failures HDF5 or Zarr would keep."""
        return {
            f.kind for f in self.failures
            if f.kind in (FAILURE_LATE_COLUMN, FAILURE_UNTYPED) and f.array_storable
        }

    def _advice(self) -> list[str]:
        advice = []
        if self.suggests_parquet:
            advice.append(
                "To keep a column this format can't store, export to "
                "Parquet, which stores every column type."
            )
        if "hdf5" in self.suggested_formats:
            when = (
                "first appears, or first has values," if FAILURE_UNTYPED in self._late_kinds()
                else "first appears"
            )
            advice.append(
                f"To keep a column that {when} after the first chunk, export "
                "to HDF5 or Zarr, which fill the rows before it with missing "
                "values."
            )
        return advice

    @property
    def suggested_formats(self) -> list[str]:
        """The formats the message suggests, in its order: ``"parquet"``
        when some column's type is one this format can't store
        (``unstorable``) and the file isn't Parquet; ``"hdf5"`` and
        ``"zarr"`` when some CSV or Parquet column first appears (or first
        has values) after the first chunk and its type is one they hold.
        Empty when no other format would keep the columns: Parquet can't
        add a late column either and fails the same type changes."""
        formats = []
        if self.format != "parquet" and any(
            f.kind == FAILURE_UNSTORABLE for f in self.failures
        ):
            formats.append("parquet")
        if self._late_kinds() and self.format not in ("hdf5", "zarr"):
            formats.extend(["hdf5", "zarr"])
        return formats

    @property
    def suggests_parquet(self) -> bool:
        """True when the message suggests Parquet (see
        :attr:`suggested_formats`)."""
        return "parquet" in self.suggested_formats


def _names(columns: list[str]) -> str:
    """``"a is"`` / ``"a and b are"`` / ``"a, b and c are"``."""
    if len(columns) == 1:
        return f"{columns[0]} is"
    return f"{', '.join(columns[:-1])} and {columns[-1]} are"


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
            ExportError: The chosen format couldn't write some columns
                (list, struct or binary columns in HDF5, Zarr or NumPy; a
                column that first appears after a CSV or Parquet file's
                columns were set by its first chunk; a type change
                between chunks). Raised once that file is written; its
                other columns are complete, and later topics are not
                written. See :class:`ExportError` for what each format
                does with the failed columns.
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
                kind=FAILURE_NO_MISSING_VALUE,
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
                raise _ColumnError("ValueError", FAILURE_NO_MISSING_VALUE, (
                    f"absent or null in its first {held} rows, and "
                    f"{self._first_dtypes[col]} has no missing value in this "
                    f"format to fill them with"
                ))
            return _held_then(_repeat_rows(*missing, held), arr), None
        except Exception as e:
            return iter(()), _column_failure(col, e)

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
                raise _ColumnError("TypeError", FAILURE_TYPE_CHANGE, (
                    f"column is {dtype} in this chunk but was {first} in an "
                    f"earlier one"
                ))
            arr = series.to_numpy()
            if arr.dtype == object:
                raise _ColumnError("TypeError", FAILURE_UNSTORABLE, (
                    f"{dtype} columns can't be written as a numeric or string "
                    f"array"
                ))
            return arr
        if target == pl.String:
            if not (_is_text(dtype) or dtype == pl.Null):
                raise _ColumnError("TypeError", FAILURE_TYPE_CHANGE, (
                    f"column is {dtype} in this chunk but was written as "
                    f"{target} from an earlier one"
                ))
            return series.cast(pl.String).fill_null("").to_numpy()
        if not (dtype.is_numeric() or dtype == pl.Boolean or dtype == pl.Null):
            raise _ColumnError("TypeError", FAILURE_TYPE_CHANGE, (
                f"column is {dtype} in this chunk but was written as "
                f"{target} from an earlier one"
            ))
        if target == pl.Int64 and series.null_count():
            raise _ColumnError(
                "ValueError", FAILURE_NO_MISSING_VALUE, f"{col} has missing values",
            )
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
            fail(_append_failure(col, e))

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
                fail(_append_failure(col, e))
    for col, rows in lengths.items():
        if col not in failed and rows != rows_written:
            fail(ExportColumnFailure(
                column=col, error_type="ValueError",
                message=f"{rows} rows written where the export has {rows_written}",
            ))
    return rows_written, failures


def _append_failure(col: str, e: Exception) -> ExportColumnFailure:
    # The array library refusing an array's dtype (h5py has no datetime
    # type) raises TypeError; anything else (a full disk) isn't the
    # column type's fault.
    return _column_failure(
        col, e, FAILURE_UNSTORABLE if isinstance(e, TypeError) else FAILURE_OTHER,
    )


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

    def fail(
        self, col: str, error_type: str, kind: str, message: str,
        array_storable: bool = True,
    ) -> None:
        self.failures.append(ExportColumnFailure(
            column=col, error_type=error_type, message=message, kind=kind,
            array_storable=array_storable,
        ))
        self.failed.add(col)

    def report_new(
        self, chunk_columns: Sequence[str], start: int, rows: int,
        array_storable: Callable[[str], bool] = lambda col: True,
    ) -> None:
        """Report columns first seen in a chunk of ``rows`` rows that
        starts at row ``start``. A 0-row chunk loses no values.
        ``array_storable(col)`` says whether HDF5 and Zarr could hold the
        column's type in this chunk."""
        if rows == 0:
            return
        for col in chunk_columns:
            if col not in self._names and col not in self.failed:
                self.fail(
                    col, "ValueError", FAILURE_LATE_COLUMN,
                    f"first appears at row {start}, after the {self._file_kind} "
                    f"took its columns from the first chunk, so it is not in "
                    f"the file",
                    array_storable=array_storable(col),
                )


def _stream_parquet(chunks: Iterable, output_path: Path, name: str) -> ExportResult:
    """Stream chunks to one Parquet file, a row group per chunk.

    The schema comes from the first chunk (see :class:`_FixedColumns`).
    A later chunk whose column has another type is cast to the file's
    type only when every value survives the cast; otherwise the column
    is reported and written as null from that chunk on. Text and
    non-text never convert into each other (Int64 values in a String
    column, or "1.5" in a Float64 one, are reported), the same rule the
    HDF5, Zarr and ``.npz`` writers apply.
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

    fixed.report_new(
        table.column_names, start, table.num_rows,
        lambda col: _arrow_array_storable(table.schema.field(col).type),
    )
    present = set(table.column_names)
    arrays = []
    for spec in schema:
        column = None
        if spec.name in present and spec.name not in fixed.failed:
            column = table.column(spec.name)
            if not column.type.equals(spec.type):
                text_mismatch = (
                    not pa.types.is_null(spec.type)
                    and not pa.types.is_null(column.type)
                    and _arrow_is_text(spec.type) != _arrow_is_text(column.type)
                )
                cast = None if text_mismatch else _lossless_cast(column, spec.type)
                if cast is None and pa.types.is_null(spec.type):
                    fixed.fail(spec.name, "TypeError", FAILURE_UNTYPED, (
                        f"has no values in the first chunk, which set the "
                        f"file's columns, so the file stores it as all null "
                        f"and its {column.type} values from row {start} on "
                        f"are not written"
                    ), array_storable=_arrow_array_storable(column.type))
                elif text_mismatch:
                    fixed.fail(spec.name, "TypeError", FAILURE_TYPE_CHANGE, (
                        f"{column.type} values from row {start} on can't go "
                        f"in the file's {spec.type} column (its type comes "
                        f"from the first chunk, and text and non-text values "
                        f"are never converted into each other), so it is "
                        f"null from row {start} on"
                    ))
                elif cast is None:
                    fixed.fail(spec.name, "TypeError", FAILURE_TYPE_CHANGE, (
                        f"{column.type} values from row {start} on can't be "
                        f"stored losslessly as the file's {spec.type} column "
                        f"(its type comes from the first chunk), so it is "
                        f"null from row {start} on"
                    ))
                column = cast
        arrays.append(column if column is not None else pa.nulls(table.num_rows, spec.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def _arrow_array_storable(arrow_type) -> bool:
    """True if HDF5 and Zarr can hold a column of this Arrow type when it
    first appears in a later chunk: numbers and booleans (as float64 with
    NaN for the rows before), text (empty string) and all-null columns."""
    import pyarrow as pa

    return (
        pa.types.is_integer(arrow_type) or pa.types.is_floating(arrow_type)
        or pa.types.is_boolean(arrow_type) or pa.types.is_null(arrow_type)
        or _arrow_is_text(arrow_type)
    )


def _polars_array_storable(dtype) -> bool:
    """:func:`_arrow_array_storable` for a polars dtype."""
    return (
        dtype.is_numeric() and not dtype.is_decimal()
        or dtype in (pl.Boolean, pl.Null) or _is_text(dtype)
    )


def _arrow_is_text(arrow_type) -> bool:
    """True for Arrow string types, and dictionaries of them (polars
    Categorical / Enum)."""
    import pyarrow as pa

    if pa.types.is_dictionary(arrow_type):
        arrow_type = arrow_type.value_type
    is_view = getattr(pa.types, "is_string_view", lambda t: False)
    return (
        pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type)
        or is_view(arrow_type)
    )


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
                fixed.report_new(
                    chunk.columns, rows_written, chunk.height,
                    lambda col: _polars_array_storable(chunk.schema[col]),
                )
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


# HDF5 caches chunks per dataset: 8 MiB, indexed by an 8191-slot (64 KiB)
# table, by default since HDF5 2.0, which h5py 3.16 bundles. This writer
# only appends, so a written chunk is never read again and the cache just
# holds it: peak RSS grew by up to 8 MiB per column with the row count.
# With a fixed chunk length (h5py's own guess for these datasets) a cache
# of a few chunks still keeps each column's trailing partial chunk between
# appends. The widest element is a 16-byte variable-length string reference.
_HDF5_CHUNK_ROWS = 1024
_HDF5_CHUNK_CACHE = {"rdcc_nbytes": 4 * 16 * _HDF5_CHUNK_ROWS, "rdcc_nslots": 521}


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

    with h5py.File(filepath, "w", **_HDF5_CHUNK_CACHE) as f:
        group = f.create_group(name)
        datasets: dict[str, h5py.Dataset] = {}

        def append(col: str, arr: np.ndarray, text: bool) -> None:
            if col not in datasets:
                if text:
                    datasets[col] = group.create_dataset(
                        col, shape=(0,), maxshape=(None,),
                        chunks=(_HDF5_CHUNK_ROWS,),
                        dtype=h5py.string_dtype(),
                    )
                else:
                    datasets[col] = group.create_dataset(
                        col, shape=(0,), maxshape=(None,),
                        chunks=(_HDF5_CHUNK_ROWS,),
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
        step/timestamp_ns: the row's timestamp (int64)
        step/observation/<column>: every other column
        reward: 0.0
        discount: 1.0
        is_first: True for first step
        is_last: True for last step
        is_terminal: True for last step
    There is no action feature: a bag has no explicit action signal
    (users can post-process one from /cmd_vel etc.).

    Each column keeps one feature type in every step, fixed by its polars
    dtype in the first chunk where that isn't Null (all null), never by
    a row's Python value:

    - Float32 / Float64 -> ``float_list``; a null is NaN.
    - Integers and Booleans -> ``int64_list`` (Booleans as 0 / 1). int64
      has no missing value, so a null leaves the feature out of that step.
    - String, Categorical, Enum -> ``bytes_list`` (UTF-8); a null is ``b""``.
    - Binary -> ``bytes_list`` as is; a null is ``b""``.
    - Anything else (lists, structs, datetimes) -> ``bytes_list`` of the
      value's text; a null is ``b""``.

    A column a chunk lacks is null in that chunk's steps. Steps written
    before a column's type is known (it first appears in a later chunk,
    or every value so far was null) don't carry it. A later chunk whose
    values don't fit the column's feature (Int64 in a String column, 2.5
    in an int64 feature) leaves the feature out of every step from that
    chunk on, and is reported with :class:`ExportError` once the file is
    written.

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
    features = _RldsFeatures(tf)

    def flag(value: bool):
        return tf.train.Feature(int64_list=tf.train.Int64List(value=[int(value)]))

    reward = tf.train.Feature(float_list=tf.train.FloatList(value=[0.0]))
    discount = tf.train.Feature(float_list=tf.train.FloatList(value=[1.0]))

    with tf.io.TFRecordWriter(str(filepath)) as writer:
        for chunk, is_final_chunk in _with_final_flag(chunks):
            columns = features.chunk_columns(chunk, rows_written)
            last_row_idx = chunk.height - 1
            for row_idx in range(chunk.height):
                is_last = is_final_chunk and row_idx == last_row_idx
                feature_map = {}
                for key, make, values in columns:
                    value = values[row_idx]
                    if value is not None:
                        feature_map[key] = make(value)
                feature_map["step/reward"] = reward
                feature_map["step/discount"] = discount
                feature_map["step/is_first"] = flag(rows_written + row_idx == 0)
                feature_map["step/is_last"] = flag(is_last)
                feature_map["step/is_terminal"] = flag(is_last)

                example = tf.train.Example(features=tf.train.Features(feature=feature_map))
                writer.write(example.SerializeToString())
            rows_written += chunk.height

    logger.info("Wrote RLDS TFRecord (%d steps) to %s", rows_written, filepath)
    if features.failures:
        raise ExportError(features.failures, filepath)
    return ExportResult(path=filepath, rows_written=rows_written)


class _RldsFeatures:
    """Per-column feature values for :func:`_stream_rlds`, each column
    held to the one feature type its first typed chunk set (see that
    function's docstring for the rules)."""

    _LIST_NAMES = {
        "float": "float_list", "int": "int64_list", "text": "bytes_list",
        "binary": "bytes_list", "repr": "bytes_list",
    }

    def __init__(self, tf) -> None:
        train = tf.train
        self._makers = {
            "float": lambda v: train.Feature(float_list=train.FloatList(value=[v])),
            "int": lambda v: train.Feature(int64_list=train.Int64List(value=[v])),
        }
        for kind in ("text", "binary", "repr"):
            self._makers[kind] = lambda v: train.Feature(bytes_list=train.BytesList(value=[v]))
        self._kinds: dict[str, str] = {}
        self._first_dtypes: dict[str, pl.DataType] = {}
        self._failed: set[str] = set()
        self.failures: list[ExportColumnFailure] = []

    @staticmethod
    def _kind_of(dtype) -> str | None:
        if dtype == pl.Null:
            return None
        if dtype.is_float():
            return "float"
        if dtype.is_integer() or dtype == pl.Boolean:
            return "int"
        if _is_text(dtype):
            return "text"
        if dtype == pl.Binary:
            return "binary"
        return "repr"

    def chunk_columns(self, chunk, start: int) -> list[tuple[str, Callable, list]]:
        """``(feature key, value -> Feature, one value per row)`` for every
        column to write in ``chunk``, whose first row is step ``start``.
        A None value means the step leaves the feature out."""
        out = []
        for col in chunk.columns:
            if col in self._failed:
                continue
            series = chunk[col]
            typed_now = col not in self._kinds
            if typed_now:
                kind = self._kind_of(series.dtype)
                if kind is None:
                    continue
                self._kinds[col] = kind
                self._first_dtypes[col] = series.dtype
            try:
                values = self._values(self._kinds[col], series)
            except _ColumnError as e:
                kind = self._kinds[col]
                self._failed.add(col)
                if typed_now:
                    # No step carries it yet, so it is not in the file at
                    # all. Only UInt64 values past int64's range get here,
                    # and Parquet stores those.
                    failure = ExportColumnFailure(
                        column=col, error_type=e.error_type, kind=FAILURE_UNSTORABLE,
                        message=(
                            f"{series.dtype} values past int64's range can't be "
                            f"written as an {self._LIST_NAMES[kind]} feature, so "
                            f"no step has it"
                        ),
                    )
                else:
                    failure = ExportColumnFailure(
                        column=col, error_type=e.error_type, kind=e.kind,
                        message=(
                            f"{series.dtype} values from row {start} on don't fit "
                            f"its {self._LIST_NAMES[kind]} feature (set by "
                            f"{self._first_dtypes[col]} values in an earlier "
                            f"chunk), so the steps from row {start} on leave it out"
                        ),
                    )
                self.failures.append(failure)
                continue
            out.append((_rlds_key(col), self._makers[self._kinds[col]], values))
        absent = [
            (col, kind) for col, kind in self._kinds.items()
            if col not in self._failed and col not in chunk.columns
        ]
        if absent:
            nulls = pl.Series("absent", [None] * chunk.height, dtype=pl.Null)
            for col, kind in absent:
                out.append((_rlds_key(col), self._makers[kind], self._values(kind, nulls)))
        return out

    @staticmethod
    def _values(kind: str, series: "pl.Series") -> list:
        """``series`` as one feature value per row (None: leave it out),
        or :class:`_ColumnError` if it doesn't fit ``kind``."""
        dtype = series.dtype
        if kind == "repr":
            return [b"" if v is None else str(v).encode("utf-8") for v in series.to_list()]
        text = _is_text(dtype)
        if kind in ("float", "int"):
            if text or not (dtype.is_numeric() or dtype in (pl.Boolean, pl.Null)):
                raise _ColumnError("TypeError", FAILURE_TYPE_CHANGE, "")
            if kind == "float":
                return series.cast(pl.Float64).fill_null(float("nan")).to_list()
            ints = _lossless_int64(series)
            if ints is None:
                raise _ColumnError("ValueError", FAILURE_TYPE_CHANGE, "")
            return ints.to_list()
        if kind == "binary" and dtype in (pl.Binary, pl.Null):
            return series.cast(pl.Binary).fill_null(b"").to_list()
        if text or dtype == pl.Null:
            return [v.encode("utf-8") for v in series.cast(pl.String).fill_null("").to_list()]
        raise _ColumnError("TypeError", FAILURE_TYPE_CHANGE, "")


def _rlds_key(col: str) -> str:
    return "step/timestamp_ns" if col == "timestamp_ns" else f"step/observation/{col}"


def _lossless_int64(series: "pl.Series") -> "pl.Series | None":
    """``series`` as Int64, or None if a value would change (a fraction,
    or an unsigned value past int64)."""
    try:
        ints = series.cast(pl.Int64, strict=True)
    except Exception:
        return None
    if series.dtype.is_float() and not ints.cast(series.dtype).equals(series):
        return None
    return ints


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
