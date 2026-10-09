"""LeRobot export when a topic's numeric fields change between chunks.

``iter_chunks`` gives each chunk the fields its own messages carry, so a
JointState driver that publishes ``velocity`` only for a while yields
chunks with and without ``velocity.0``. The LeRobot resampler took its
columns from the first chunk and selected them from every chunk, so a
later chunk without one raised a raw ``ColumnNotFoundError`` (exit 1 on
the verifier's 120k-message bag), and a field first seen in a later
chunk was silently dropped.

Now ``asof_on_grid`` matches the as-of join on the whole topic (a field
a chunk lacks is null there) for any chunking, ``_resample_topics``
resamples the fields of every chunk, and frames before a field's first
value hold that value, with a warning: NaN would make LeRobot's
statistics for the field NaN.

The unit tests need no LeRobot; the round trip skips without it (it runs
in CI's lerobot extras job).
"""

from __future__ import annotations

import logging

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from resurrector.core import bag_frame as bag_frame_module
from resurrector.core.bag_frame import BagFrame
from resurrector.core.lerobot_export import _resample_topics, asof_on_grid, build_grid
from tests.fixtures.changing_columns import BASE_NS, write_joint_state_bag


def _topic() -> pl.DataFrame:
    """1000 rows, 1 ms apart: ``x`` in rows 0-599, ``y`` in rows 300-999,
    ``flag`` (Boolean) in rows 0-99 and 900-999, ``n`` (Int64) everywhere."""
    i = np.arange(1000)
    return pl.DataFrame({
        "timestamp_ns": (i * 1_000_000).astype(np.int64),
        "x": [float(k) if k < 600 else None for k in i],
        "y": [-float(k) if k >= 300 else None for k in i],
        "flag": [bool(k % 2) if k < 100 or k >= 900 else None for k in i],
        "n": i.astype(np.int64),
    })


def _as_iter_chunks(df: pl.DataFrame, size: int) -> list[pl.DataFrame]:
    """``df`` cut into ``size``-row chunks the way iter_chunks builds them:
    a field no row of the chunk carries isn't a column of that chunk."""
    chunks = []
    for start in range(0, df.height, size):
        chunk = df.slice(start, size)
        chunks.append(chunk.select(
            [c for c in chunk.columns if c == "timestamp_ns" or chunk[c].null_count() < chunk.height]
        ))
    return chunks


GRID = build_grid(-5_000_000, 1_050_000_000, 240)  # past both ends of the topic


@pytest.mark.parametrize("size", [1, 7, 128, 300, 450, 5000])
@pytest.mark.parametrize("value_cols", [["x", "y", "flag", "n"], None], ids=["named", "discovered"])
def test_asof_matches_full_join_when_columns_change(size, value_cols):
    """Would catch: ``ColumnNotFoundError`` for a chunk without ``x``
    (named), or ``y`` / ``flag`` dropped because the first chunk lacked
    them (discovered). Equal to the join on the whole topic, so where the
    chunks start and end doesn't matter."""
    full = _topic()
    chunks = _as_iter_chunks(full, size)
    assert len({tuple(c.columns) for c in chunks}) > 1 or size >= 1000
    expected = pl.DataFrame({"timestamp_ns": GRID}).join_asof(
        full, on="timestamp_ns", strategy="backward",
    )
    got = asof_on_grid(iter(chunks), GRID, value_cols)
    assert got.height == len(GRID)
    assert sorted(got.columns) == sorted(expected.columns)
    assert_frame_equal(got.select(expected.columns), expected)


def test_named_column_missing_from_a_later_chunk_is_null_there():
    """The verifier's shape: the column is in the first chunks and gone
    from the last one."""
    chunks = [
        pl.DataFrame({"timestamp_ns": [0, 10], "v": [1.0, 2.0]}),
        pl.DataFrame({"timestamp_ns": [20, 30]}),
    ]
    got = asof_on_grid(iter(chunks), np.array([5, 15, 25, 35], dtype=np.int64), ["v"])
    assert got["v"].to_list() == [1.0, 2.0, None, None]


def test_discovered_columns_keep_first_seen_order():
    chunks = [
        pl.DataFrame({"timestamp_ns": [0], "b": [1.0], "label": ["s"]}),
        pl.DataFrame({"timestamp_ns": [10], "a": [2.0], "b": [3.0]}),
    ]
    got = asof_on_grid(iter(chunks), np.array([0, 10], dtype=np.int64), None)
    assert got.columns == ["timestamp_ns", "b", "a"]
    assert got["a"].to_list() == [None, 2.0]


# ---------------------------------------------------------------------------
# _resample_topics on a real JointState bag, small chunks.
# ---------------------------------------------------------------------------

JS_ROWS = 400
JS_CHUNK = 150

# Chunks are rows [0, 150), [150, 300), [300, 400).
JS_BAGS = {
    # The verifier's intermittent.mcap, scaled down: velocity starts past
    # row 100 of the first chunk and is gone from the last chunk, and so
    # is effort.
    "intermittent": (range(120, 250), 200),
    # velocity first appears in the second chunk.
    "late": (range(160, JS_ROWS), JS_ROWS),
}


@pytest.fixture(params=list(JS_BAGS))
def joint_bag(request, tmp_path):
    velocity, effort_until = JS_BAGS[request.param]
    path = tmp_path / f"{request.param}.mcap"
    return write_joint_state_bag(path, JS_ROWS, velocity, effort_until), velocity


def _small_chunks(monkeypatch, size: int) -> None:
    real = bag_frame_module.TopicView.iter_chunks
    monkeypatch.setattr(
        bag_frame_module.TopicView, "iter_chunks",
        lambda self, chunk_size=size: real(self, chunk_size),
    )


def _expected_state(bf: BagFrame, grid: np.ndarray) -> pl.DataFrame:
    """The eager answer: join on the whole topic, then hold values."""
    full = bf["/joint_states"].to_polars()
    df = pl.DataFrame({"timestamp_ns": grid}).join_asof(
        full, on="timestamp_ns", strategy="backward",
    )
    cols = [c for c in df.columns if c.startswith(("position.", "velocity.", "effort."))]
    return df.select(cols).fill_null(strategy="forward").fill_null(strategy="backward")


def test_resample_topics_keeps_every_field_and_ignores_chunking(joint_bag, monkeypatch, caplog):
    """Would catch: ``ColumnNotFoundError: unable to find column
    "velocity.0"`` (intermittent), velocity dropped because the first
    chunk lacked it (late), or a result that depends on the chunk size."""
    bag, velocity = joint_bag
    bf = BagFrame(bag)
    grid = build_grid(BASE_NS, BASE_NS + (JS_ROWS - 1) * 1_000_000, 100)
    expected = _expected_state(bf, grid)

    results = {}
    for size in (JS_CHUNK, 10_000):
        _small_chunks(monkeypatch, size)
        with caplog.at_level(logging.WARNING, logger="resurrector.core.lerobot_export"):
            caplog.clear()
            state, names = _resample_topics(bf, ["/joint_states"], grid)
        results[size] = (state, names, [r.getMessage() for r in caplog.records])

    state, names, warnings = results[JS_CHUNK]
    assert np.array_equal(state, results[10_000][0])
    assert names == results[10_000][1]
    assert sorted(names) == sorted(f"joint_states/{c}" for c in expected.columns)
    assert state.dtype == np.float32 and not np.isnan(state).any()
    for i, name in enumerate(names):
        col = name.split("/", 1)[1]
        assert np.allclose(state[:, i], expected[col].to_numpy().astype(np.float32)), col
    [warning] = warnings
    lead = int(np.searchsorted(grid, BASE_NS + velocity.start * 1_000_000))
    assert f"velocity.0 has no value in the first {lead} of {len(grid)} frames" in warning
    assert "position" not in warning


def test_round_trip_through_lerobot(joint_bag, tmp_path, monkeypatch):
    """The exported dataset loads in LeRobot and its state holds every
    field, velocity included, with the eager values."""
    lerobot_dataset = pytest.importorskip("lerobot.datasets.lerobot_dataset")
    from resurrector.core.lerobot_export import export_lerobot

    bag, _ = joint_bag
    _small_chunks(monkeypatch, JS_CHUNK)
    bf = BagFrame(bag)
    out = tmp_path / "lr"
    result = export_lerobot([bf], ["/joint_states"], out, fps=100)
    ds = lerobot_dataset.LeRobotDataset(repo_id=f"local/{out.name}", root=out)

    grid = build_grid(BASE_NS, BASE_NS + (JS_ROWS - 1) * 1_000_000, 100)
    expected = _expected_state(bf, grid)
    names = ds.meta.features["observation.state"]["names"]
    assert names == result.state_names
    assert {"joint_states/velocity.0", "joint_states/effort.0"} <= set(names)
    assert ds.num_frames == len(grid)
    for k in (0, 5, 15, 20, 30, len(grid) - 1):
        state = ds[k]["observation.state"].numpy()
        for i, name in enumerate(names):
            want = expected[name.split("/", 1)[1]][k]
            assert state[i] == pytest.approx(want, rel=1e-6), (k, name)
