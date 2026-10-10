"""Train/val/test split helpers for bag-data export.

Produces conventional ML directory layouts (``train/``, ``val/``, ``test/``)
from a single bag, applying one of:

- **time** (default) — chronological. First N% of the bag's time → train,
  next M% → val, last K% → test. Best for time-series; preserves temporal
  locality so val/test contain "future" data the model didn't train on.
- **random** — each row goes to a split at random (fixed seed, so the
  same bag gives the same splits). Disregards temporal locality; only
  valid when row-level independence is OK (rare in robotics).
- **stratified** — not implemented in v0.5.0; see the ``NotImplementedError``
  message for the v0.6+ design sketch.

A split spec is a dict ``{"train": 0.8, "val": 0.1, "test": 0.1}``. Names
are arbitrary — caller could use ``{"a": 0.5, "b": 0.5}`` for a 2-way split.
Ratios must sum to ~1.0 (1e-6 tolerance).
"""

from __future__ import annotations

import logging
import zlib
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from resurrector.core.bag_frame import BagFrame

logger = logging.getLogger("resurrector.core.splits")

_RATIO_SUM_TOLERANCE = 1e-6


def validate_split_ratios(split: dict[str, float]) -> None:
    """Raise ValueError if the split spec is malformed.

    Validations:
    - Empty dict not allowed
    - Each ratio in (0.0, 1.0]
    - Ratios sum to 1.0 within ``_RATIO_SUM_TOLERANCE``
    - Names are non-empty strings (avoids accidental ``{None: 1.0}`` etc.)

    Args:
        split: The split-ratios dict to validate.

    Raises:
        ValueError: With a clear message on the first failed check.
    """
    if not split:
        raise ValueError("split must be a non-empty dict like {'train': 0.8, 'val': 0.1, 'test': 0.1}")
    for name, ratio in split.items():
        if not isinstance(name, str) or not name:
            raise ValueError(f"split keys must be non-empty strings; got {name!r}")
        if not isinstance(ratio, (int, float)):
            raise ValueError(f"split[{name!r}] must be numeric; got {type(ratio).__name__}")
        if ratio <= 0.0 or ratio > 1.0:
            raise ValueError(
                f"split[{name!r}]={ratio} out of range (0.0, 1.0]"
            )
    total = sum(split.values())
    if abs(total - 1.0) > _RATIO_SUM_TOLERANCE:
        raise ValueError(
            f"split ratios must sum to 1.0 (within {_RATIO_SUM_TOLERANCE}); "
            f"got {total} for {dict(split)!r}"
        )


def split_export(
    bag_frame: "BagFrame",
    topics: list[str],
    output: Path,
    split: dict[str, float],
    strategy: str = "time",
    *,
    format: str = "parquet",
    sync: bool = False,
    sync_method: str = "nearest",
    downsample_hz: float | None = None,
) -> Path:
    """Materialize a multi-way split of a bag, one subdirectory per split.

    Output structure::

        <output>/
            train/
                <per-format files, same as a single export>
            val/
                ...
            test/
                ...
            split_manifest.json   (records the split ratios + strategy + boundaries)

    Both strategies stream: memory is bounded by the chunk size, not the
    topic size. A split with no rows gets no subdirectory.

    Args:
        bag_frame: The :class:`BagFrame` to split + export.
        topics: List of topic names to include in each split.
        output: Parent output directory; created if missing.
        split: Ratios dict (``{"train": 0.8, ...}``). Must already pass
            :func:`validate_split_ratios`.
        strategy: ``"time"`` (default), ``"random"``, or ``"stratified"``.
        format / sync / sync_method / downsample_hz: Applied to each split
            the way :meth:`Exporter.export` applies them. ``"random"``
            assigns rows after syncing and downsampling, and can't write
            ``lerobot`` (an episode is a contiguous run of frames).

    Returns:
        ``Path`` to ``output`` (same as the single-export case, except the
        directory now contains per-split subdirectories).

    Raises:
        ValueError: For unknown strategies, or ``"random"`` with
            ``format="lerobot"``.
        NotImplementedError: For ``"stratified"``.
        ImportError: The format's optional dependency is missing (raised
            before ``output`` is created).
        TopicNotFoundError: A topic isn't in the bag (raised before
            ``output`` is created).
        EmptyExportError: No split has any rows.
    """
    from resurrector.core.export import (
        check_topics,
        first_missing_dir,
        remove_empty_dirs,
        require_export_dependencies,
    )

    require_export_dependencies(format)
    check_topics(bag_frame, topics)
    if not topics:
        raise ValueError("No topics selected, so nothing was exported.")
    if strategy == "stratified":
        raise NotImplementedError(
            "stratified split is not yet supported (v0.6+ candidate). "
            "It needs a column-to-stratify-by argument and per-stratum "
            "shuffling. Use 'time' or 'random' for v0.5.0."
        )
    if strategy not in ("time", "random"):
        raise ValueError(
            f"Unknown split strategy {strategy!r}. "
            f"Supported: 'time', 'random'. ('stratified' planned for v0.6.)"
        )
    if strategy == "random" and format == "lerobot":
        raise ValueError(
            "A random split can't write LeRobot datasets: each episode is a "
            "contiguous run of frames, and a random split scatters them. "
            "Use --split-strategy time."
        )
    output = Path(output)
    created = first_missing_dir(output)
    output.mkdir(parents=True, exist_ok=True)
    run = _split_export_time if strategy == "time" else _split_export_random
    try:
        return run(
            bag_frame, topics, output, split,
            format=format, sync=sync, sync_method=sync_method,
            downsample_hz=downsample_hz,
        )
    except BaseException:
        remove_empty_dirs(output, created)
        raise


def _split_export_time(
    bag_frame: "BagFrame",
    topics: list[str],
    output: Path,
    split: dict[str, float],
    *,
    format: str,
    sync: bool,
    sync_method: str,
    downsample_hz: float | None,
) -> Path:
    """Chronological split: each named split gets a contiguous time window."""
    from resurrector.core.exceptions import EmptyExportError
    from resurrector.core.export import Exporter, bag_label

    duration = bag_frame.duration_sec
    boundaries = _cumulative_time_boundaries(split, duration)

    manifest = {
        "strategy": "time",
        "duration_sec": duration,
        "splits": {},
    }

    cursor_sec = 0.0
    written = 0
    for split_name, end_sec in boundaries:
        sliced = bag_frame.time_slice(cursor_sec, end_sec)
        try:
            Exporter().export(
                bag_frame=sliced,
                topics=topics,
                format=format,
                output_dir=str(output / split_name),
                sync=sync,
                sync_method=sync_method,
                downsample_hz=downsample_hz,
            )
            written += 1
        except EmptyExportError:
            # A short window can hold no messages; that split is just empty.
            logger.warning(
                "Split '%s' (%.3fs to %.3fs) has no messages; nothing written for it",
                split_name, cursor_sec, end_sec,
            )
        manifest["splits"][split_name] = {
            "start_sec": cursor_sec,
            "end_sec": end_sec,
            "ratio": split[split_name],
        }
        cursor_sec = end_sec

    if not written:
        raise EmptyExportError(topics, bag_label(bag_frame))
    _write_manifest(output, manifest)
    logger.info(
        "Time-split export to %s: %d splits over %.2fs",
        output, len(split), duration,
    )
    return output


# Every random split draws from this seed, so the same bag and split spec
# always give the same rows in each split.
RANDOM_SPLIT_SEED = 42


def _split_export_random(
    bag_frame: "BagFrame",
    topics: list[str],
    output: Path,
    split: dict[str, float],
    *,
    format: str,
    sync: bool,
    sync_method: str,
    downsample_hz: float | None,
) -> Path:
    """Random split: each row goes to a split drawn with the split's ratio.

    Streams in one pass per split instead of materializing a topic: each
    pass re-reads the topic (or the synced table) chunk by chunk, draws a
    label per row from an RNG seeded by :data:`RANDOM_SPLIT_SEED` and the
    stream's name, and keeps the rows labelled for that split. Re-seeding
    per pass gives every row the same label in every pass, so the splits
    partition the rows. Memory is bounded by the chunk size; reading cost
    is one read per split. Rows are assigned after syncing and
    downsampling, so a split holds whole synced rows.
    """
    import numpy as np
    import polars as pl
    from resurrector.core.exceptions import EmptyExportError, LargeTopicError
    from resurrector.core.export import (
        NUMPY_HARD_CAP,
        Exporter,
        bag_label,
        nonempty_chunks,
        synced_chunks,
        topic_chunks,
    )

    split_names = list(split.keys())
    thresholds = np.cumsum([split[n] for n in split_names])

    if sync and len(topics) > 1:
        streams = [("synced", lambda: synced_chunks(bag_frame, topics, sync_method, downsample_hz))]
    else:
        streams = []
        for topic in topics:
            view = bag_frame[topic]
            if format == "numpy" and view.message_count > NUMPY_HARD_CAP:
                raise LargeTopicError(
                    topic_name=view.name,
                    message_count=view.message_count,
                    threshold=NUMPY_HARD_CAP,
                )
            streams.append((
                topic.lstrip("/").replace("/", "_"),
                lambda view=view: topic_chunks(view, downsample_hz),
            ))

    manifest = {
        "strategy": "random",
        "seed": RANDOM_SPLIT_SEED,
        "splits": {n: {"ratio": split[n], "rows": 0} for n in split_names},
    }
    exporter = Exporter()
    for name, make_chunks in streams:
        stream_rows = 0
        for idx, split_name in enumerate(split_names):
            counter = [0]

            def rows_for_split(idx=idx, counter=counter, name=name, make_chunks=make_chunks):
                # zlib.crc32 is stable across runs (hash() of a str isn't).
                rng = np.random.default_rng([RANDOM_SPLIT_SEED, zlib.crc32(name.encode())])
                for chunk in make_chunks():
                    labels = np.searchsorted(thresholds, rng.random(chunk.height), side="right")
                    picked = chunk.filter(pl.Series(np.minimum(labels, len(split_names) - 1) == idx))
                    counter[0] += picked.height
                    yield picked

            chunks = nonempty_chunks(rows_for_split())
            if chunks is None:
                continue
            sub_output = output / split_name
            sub_output.mkdir(parents=True, exist_ok=True)
            exporter._stream_dataframe_chunks(chunks, format, sub_output, name)
            manifest["splits"][split_name]["rows"] += counter[0]
            stream_rows += counter[0]
        if not stream_rows:
            logger.warning("'%s' has no messages in the exported range; no file written for it", name)

    if not any(s["rows"] for s in manifest["splits"].values()):
        raise EmptyExportError(topics, bag_label(bag_frame))
    _write_manifest(output, manifest)
    logger.info(
        "Random-split export to %s: %d splits", output, len(split),
    )
    return output


def _cumulative_time_boundaries(
    split: dict[str, float],
    duration: float,
) -> list[tuple[str, float]]:
    """Convert ratio dict to list of ``(split_name, end_sec)`` cumulative boundaries.

    Insertion order of ``split`` decides assignment order — Python 3.7+
    dicts preserve insertion order, so passing
    ``{"train": 0.8, "val": 0.1, "test": 0.1}`` gives chronological
    train → val → test (matching the typical convention).
    """
    boundaries: list[tuple[str, float]] = []
    cumulative = 0.0
    for split_name, ratio in split.items():
        cumulative += ratio
        end_sec = cumulative * duration
        boundaries.append((split_name, end_sec))
    # Pin the last boundary to the exact duration to avoid floating-point drift
    if boundaries:
        last_name, _ = boundaries[-1]
        boundaries[-1] = (last_name, duration)
    return boundaries


def _write_manifest(output: Path, manifest: dict) -> None:
    """Write split_manifest.json next to the per-split subdirectories."""
    import json
    (output / "split_manifest.json").write_text(json.dumps(manifest, indent=2))
