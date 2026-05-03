"""Train/val/test split helpers for bag-data export.

Produces conventional ML directory layouts (``train/``, ``val/``, ``test/``)
from a single bag, applying one of:

- **time** (default) — chronological. First N% of the bag's time → train,
  next M% → val, last K% → test. Best for time-series; preserves temporal
  locality so val/test contain "future" data the model didn't train on.
- **random** — uniform random per-row split. Disregards temporal
  locality; only valid when row-level independence is OK (rare in robotics).
- **stratified** — not implemented in v0.5.0; see the ``NotImplementedError``
  message for the v0.6+ design sketch.

A split spec is a dict ``{"train": 0.8, "val": 0.1, "test": 0.1}``. Names
are arbitrary — caller could use ``{"a": 0.5, "b": 0.5}`` for a 2-way split.
Ratios must sum to ~1.0 (1e-6 tolerance).
"""

from __future__ import annotations

import logging
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

    Args:
        bag_frame: The :class:`BagFrame` to split + export.
        topics: List of topic names to include in each split.
        output: Parent output directory; created if missing.
        split: Ratios dict (``{"train": 0.8, ...}``). Must already pass
            :func:`validate_split_ratios`.
        strategy: ``"time"`` (default), ``"random"``, or ``"stratified"``.
        format / sync / sync_method / downsample_hz: Forwarded to each
            sub-export's :meth:`Exporter.export` call. Same shape as
            :meth:`BagFrame.export`.

    Returns:
        ``Path`` to ``output`` (same as the single-export case, except the
        directory now contains per-split subdirectories).

    Raises:
        ValueError: For unknown strategies.
        NotImplementedError: For ``"stratified"``.
    """
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)

    if strategy == "time":
        return _split_export_time(
            bag_frame, topics, output, split,
            format=format, sync=sync, sync_method=sync_method,
            downsample_hz=downsample_hz,
        )
    if strategy == "random":
        return _split_export_random(
            bag_frame, topics, output, split,
            format=format, sync=sync, sync_method=sync_method,
            downsample_hz=downsample_hz,
        )
    if strategy == "stratified":
        raise NotImplementedError(
            "stratified split is not yet supported (v0.6+ candidate). "
            "It needs a column-to-stratify-by argument and per-stratum "
            "shuffling. Use 'time' or 'random' for v0.5.0."
        )
    raise ValueError(
        f"Unknown split strategy {strategy!r}. "
        f"Supported: 'time', 'random'. ('stratified' planned for v0.6.)"
    )


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
    duration = bag_frame.duration_sec
    boundaries = _cumulative_time_boundaries(split, duration)

    manifest = {
        "strategy": "time",
        "duration_sec": duration,
        "splits": {},
    }

    cursor_sec = 0.0
    for split_name, end_sec in boundaries:
        sub_output = output / split_name
        sub_output.mkdir(parents=True, exist_ok=True)
        sliced = bag_frame.time_slice(cursor_sec, end_sec)
        # Note: TimeslicedBagFrame.export isn't defined; we go through the
        # exporter directly with the time-bounded TopicViews.
        from resurrector.core.export import Exporter
        Exporter().export(
            bag_frame=sliced,
            topics=topics,
            format=format,
            output_dir=str(sub_output),
            sync=sync,
            sync_method=sync_method,
            downsample_hz=downsample_hz,
        )
        manifest["splits"][split_name] = {
            "start_sec": cursor_sec,
            "end_sec": end_sec,
            "ratio": split[split_name],
        }
        cursor_sec = end_sec

    _write_manifest(output, manifest)
    logger.info(
        "Time-split export to %s: %d splits over %.2fs",
        output, len(split), duration,
    )
    return output


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
    """Random split: assign each row to a split label by sampled probability.

    For v0.5.0 this is implemented at row granularity per-topic by
    materializing each topic, shuffling the index, and writing per-split
    parquet files. Memory is bounded by topic size, NOT chunk size — the
    random strategy is fundamentally non-streaming because we have to
    decide each row's destination up front.

    For very large topics, prefer ``strategy="time"`` (which IS streaming).
    """
    import numpy as np
    import polars as pl
    from resurrector.core.export import Exporter

    rng = np.random.default_rng(seed=42)  # deterministic per call
    split_names = list(split.keys())
    split_ratios = np.array([split[n] for n in split_names], dtype=np.float64)
    # Cumulative thresholds for np.searchsorted
    thresholds = np.cumsum(split_ratios)

    manifest = {
        "strategy": "random",
        "seed": 42,
        "splits": {n: {"ratio": split[n], "rows": 0} for n in split_names},
    }

    # Materialize per-topic, shuffle, write per-split.
    # We bypass Exporter.export and call the per-format writers directly
    # because Exporter assumes one BagFrame in → one set of files out;
    # here we want N output sets per topic.
    for topic in topics:
        try:
            view = bag_frame[topic]
        except KeyError:
            logger.warning("Topic %s not found; skipping", topic)
            continue

        chunks_full: list[pl.DataFrame] = []
        for chunk in view.iter_chunks():
            if chunk.height > 0:
                chunks_full.append(chunk)
        if not chunks_full:
            continue
        full_df = pl.concat(chunks_full)
        n_rows = full_df.height
        # Assign each row to a split label
        random_vals = rng.random(n_rows)
        labels = np.searchsorted(thresholds, random_vals, side="right")
        labels = np.clip(labels, 0, len(split_names) - 1)

        for idx, split_name in enumerate(split_names):
            mask = labels == idx
            split_df = full_df.filter(pl.Series(mask))
            if split_df.height == 0:
                continue
            sub_output = output / split_name
            sub_output.mkdir(parents=True, exist_ok=True)
            # Write via the parquet streamer (one chunk = whole split)
            from resurrector.core.export import _stream_parquet
            safe_name = topic.lstrip("/").replace("/", "_")
            _stream_parquet(iter([split_df]), sub_output, safe_name)
            manifest["splits"][split_name]["rows"] += split_df.height

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
