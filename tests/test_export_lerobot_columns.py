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
    [warning] = [w for w in warnings if "first value" in w]
    lead = int(np.searchsorted(grid, BASE_NS + velocity.start * 1_000_000))
    assert f"velocity.0 has no value in the first {lead} of {len(grid)} frames" in warning
    assert "position" not in warning

    # A field that stops while its topic keeps publishing is held, and
    # says so (intermittent: velocity stops at row 250, effort at 200).
    stops = [w for w in warnings if "last value" in w]
    _, effort_until = JS_BAGS["intermittent" if velocity.stop < JS_ROWS else "late"]
    if velocity.stop < JS_ROWS:
        [stop] = stops
        for field, last_row in (("velocity.0", velocity.stop - 1), ("effort.0", effort_until - 1)):
            last = int(np.searchsorted(grid, BASE_NS + last_row * 1_000_000, side="right")) - 1
            assert f"{field} has no value after frame {last} of {len(grid)}" in stop
        assert "position" not in stop
    else:
        assert stops == []


def test_resample_topics_streams_its_chunks(tmp_path, monkeypatch):
    """Would catch: collecting a topic's chunks before resampling them
    (``list(iter_chunks())`` in _resample_topics, or ``list(chunks)`` in
    asof_on_grid to find every column up front), which holds the whole
    topic in memory. Counts live chunks rather than measuring RSS: the
    memory-regression bag is too small for a whole-topic copy to show."""
    import gc
    import weakref

    bag = write_joint_state_bag(tmp_path / "js.mcap", 2_000, range(500, 2_000), 2_000)
    real = bag_frame_module.TopicView.iter_chunks
    refs: list = []
    live: list[int] = []

    def tracking(self, chunk_size=50_000):
        for chunk in real(self, 100):
            gc.collect()
            live.append(sum(r() is not None for r in refs))
            refs.append(weakref.ref(chunk))
            yield chunk

    monkeypatch.setattr(bag_frame_module.TopicView, "iter_chunks", tracking)
    bf = BagFrame(bag)
    grid = build_grid(BASE_NS, BASE_NS + 1_999 * 1_000_000, 100)
    state, names = _resample_topics(bf, ["/joint_states"], grid)
    assert "joint_states/velocity.0" in names
    assert len(refs) == 20
    # The first chunk stays referenced (it is peeked at); nothing piles up.
    assert max(live) <= 2, live


def test_later_bag_with_fields_in_another_order_is_aligned():
    """Would catch: a second bag whose driver starts publishing velocity
    later (so it is first seen after effort) being refused as "a
    different feature set", or its columns kept in their own order under
    the first bag's names."""
    from resurrector.core.lerobot_export import _Episode, _match_field_order

    first = (["js/position.0", "js/velocity.0", "js/effort.0"], [])
    state = np.array([[1.0, 3.0, 2.0], [4.0, 6.0, 5.0]], dtype=np.float32)
    ep = _Episode(np.array([0, 1]), state, None,
                  ["js/position.0", "js/effort.0", "js/velocity.0"], [], [])
    got = _match_field_order(ep, first)
    assert got.state_names == first[0]
    assert got.state.tolist() == [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]

    other = _Episode(np.array([0, 1]), state, None,
                     ["js/position.0", "js/effort.0", "js/extra.0"], [], [])
    assert _match_field_order(other, first).state_names == other.state_names


def test_two_bags_with_fields_first_seen_in_another_order(tmp_path, monkeypatch):
    """Both bags export into one dataset, and the second episode's state
    is in the first episode's field order."""
    lerobot_dataset = pytest.importorskip("lerobot.datasets.lerobot_dataset")
    from resurrector.core.lerobot_export import export_lerobot

    a = write_joint_state_bag(tmp_path / "a.mcap", JS_ROWS, range(0, JS_ROWS), JS_ROWS)
    b = write_joint_state_bag(tmp_path / "b.mcap", JS_ROWS, range(160, JS_ROWS), JS_ROWS)
    _small_chunks(monkeypatch, JS_CHUNK)
    out = tmp_path / "lr"
    result = export_lerobot([BagFrame(a), BagFrame(b)], ["/joint_states"], out, fps=100)
    assert result.episodes == 2
    ds = lerobot_dataset.LeRobotDataset(repo_id=f"local/{out.name}", root=out)
    names = ds.meta.features["observation.state"]["names"]
    grid = build_grid(BASE_NS, BASE_NS + (JS_ROWS - 1) * 1_000_000, 100)
    expected = _expected_state(BagFrame(b), grid)
    k = len(grid) - 1
    state = ds[len(grid) + k]["observation.state"].numpy()  # episode 2, last frame
    for i, name in enumerate(names):
        assert state[i] == pytest.approx(expected[name.split("/", 1)[1]][k], rel=1e-6), name


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
