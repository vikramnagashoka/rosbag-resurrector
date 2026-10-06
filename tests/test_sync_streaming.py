"""Streaming sync engine — equivalence and contract tests.

Spec source: tests/fixtures/sync_fixtures.py builds 9 timing-pathology
fixtures plus a memory-regression scenario. For each fixture we assert
streaming engine output matches eager engine output (with the most-
permissive streaming config: out_of_order='reorder', boundary='null').

Documented divergence cases are tested separately — sync with
out_of_order='error' must raise on the out-of-order fixture, etc.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import polars as pl
import pytest

from resurrector.core.bag_frame import BagFrame
from resurrector.core.exceptions import (
    SyncBoundaryError,
    SyncBufferExceededError,
    SyncOutOfOrderError,
)
from resurrector.core.sync import synchronize
from tests.fixtures.sync_fixtures import (
    ALL_FIXTURE_BUILDERS,
    bursty_fast,
    fast_vs_slow,
    missing_after_last,
    missing_before_first,
    out_of_order_within_topic,
    sparse_no_match,
    tie_at_anchor,
    topic_stops_halfway,
)


@pytest.fixture(scope="session")
def sync_fixtures_dir():
    """Build all sync fixtures once per session."""
    with tempfile.TemporaryDirectory() as d:
        out = Path(d)
        for builder in ALL_FIXTURE_BUILDERS:
            builder(out)
        yield out


def _topic_views(bag_path: Path):
    bf = BagFrame(bag_path)
    return {
        "/joint_states": bf["/joint_states"],
        "/imu/data": bf["/imu/data"],
    }


def _frames_equivalent(a: pl.DataFrame, b: pl.DataFrame) -> bool:
    """Compare two sync frames for equivalence.

    NaN == NaN treated as True. Tolerates float drift up to 1e-9.
    """
    if a.height != b.height:
        return False
    if set(a.columns) != set(b.columns):
        return False
    for col in a.columns:
        ac = a[col]
        bc = b[col]
        # Cast both to consistent dtypes for comparison.
        if ac.dtype.is_numeric() and bc.dtype.is_numeric():
            an = ac.to_numpy().astype(float)
            bn = bc.to_numpy().astype(float)
            import numpy as np
            both_nan = np.isnan(an) & np.isnan(bn)
            close = np.isclose(an, bn, equal_nan=False, rtol=1e-9, atol=1e-9)
            if not (both_nan | close).all():
                return False
        else:
            if ac.to_list() != bc.to_list():
                return False
    return True


# ---------------------------------------------------------------------------
# Equivalence: streaming with permissive config matches eager on every
# pathology fixture.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("builder", ALL_FIXTURE_BUILDERS, ids=lambda b: b.__name__)
def test_nearest_streaming_matches_eager(builder, sync_fixtures_dir):
    """Streaming nearest with permissive config == eager nearest."""
    fixture = builder(sync_fixtures_dir)
    eager = synchronize(
        _topic_views(fixture.path),
        method="nearest",
        tolerance_ms=50.0,
        anchor="/joint_states",
        engine="eager",
    )
    streaming = synchronize(
        _topic_views(fixture.path),
        method="nearest",
        tolerance_ms=50.0,
        anchor="/joint_states",
        engine="streaming",
        out_of_order="reorder",
        max_lateness_ms=100.0,
    )
    assert _frames_equivalent(eager, streaming), (
        f"{builder.__name__}: streaming != eager\n"
        f"eager:\n{eager}\n\nstreaming:\n{streaming}"
    )


def test_sample_and_hold_streaming_matches_eager(sync_fixtures_dir):
    """Streaming sample_and_hold matches eager on a representative bag."""
    fixture = topic_stops_halfway(sync_fixtures_dir)
    eager = synchronize(
        _topic_views(fixture.path),
        method="sample_and_hold",
        anchor="/joint_states",
        engine="eager",
    )
    streaming = synchronize(
        _topic_views(fixture.path),
        method="sample_and_hold",
        anchor="/joint_states",
        engine="streaming",
        out_of_order="reorder",
        max_lateness_ms=100.0,
    )
    assert _frames_equivalent(eager, streaming), (
        f"streaming != eager\neager:\n{eager}\n\nstreaming:\n{streaming}"
    )


# ---------------------------------------------------------------------------
# Documented divergence: stricter streaming configs deliberately raise.
# ---------------------------------------------------------------------------


def test_out_of_order_error_raises_at_row_iter():
    """The MCAP reader returns time-sorted messages, so the out-of-order
    fixture loses its regressions before the sync engine sees them.
    Test the policy directly at the _row_iter helper that owns it.
    """
    from resurrector.core.sync import _row_iter

    class FakeView:
        """Minimal stand-in that yields chunks with non-monotonic timestamps."""
        def iter_chunks(self):
            # Two chunks: 1st with ts=0,10,20; 2nd with ts=15 (regression!).
            yield pl.DataFrame({"timestamp_ns": [0, 10, 20], "x": [1.0, 2.0, 3.0]})
            yield pl.DataFrame({"timestamp_ns": [15], "x": [4.0]})

    with pytest.raises(SyncOutOfOrderError) as exc:
        list(_row_iter(
            FakeView(), "/fake",
            out_of_order="error", max_lateness_ns=0,
        ))
    assert exc.value.topic_name == "/fake"
    assert exc.value.prev_ts == 20
    assert exc.value.regressing_ts == 15


def test_out_of_order_warn_drop_at_row_iter():
    """warn_drop silently skips regressing samples."""
    from resurrector.core.sync import _row_iter

    class FakeView:
        def iter_chunks(self):
            yield pl.DataFrame({"timestamp_ns": [0, 10, 20], "x": [1.0, 2.0, 3.0]})
            yield pl.DataFrame({"timestamp_ns": [15], "x": [4.0]})

    out = list(_row_iter(
        FakeView(), "/fake",
        out_of_order="warn_drop", max_lateness_ns=0,
    ))
    # Regression at ts=15 dropped; 3 rows remain.
    assert len(out) == 3
    assert [t for t, _ in out] == [0, 10, 20]


def test_buffer_exceeded_raises(sync_fixtures_dir):
    """A burst of 10K samples between anchors with max_buffer_messages=100
    must raise SyncBufferExceededError."""
    fixture = bursty_fast(sync_fixtures_dir)
    with pytest.raises(SyncBufferExceededError) as exc:
        synchronize(
            _topic_views(fixture.path),
            method="nearest",
            tolerance_ms=200.0,  # window large enough to swallow the burst
            anchor="/joint_states",
            engine="streaming",
            out_of_order="warn_drop",  # avoid the OOO check firing first
            max_buffer_messages=100,
        )
    assert exc.value.topic_name == "/imu/data"


def test_interpolate_boundary_null_default(sync_fixtures_dir):
    """boundary='null' (default) emits None for unmatched edges.

    Setup: anchors at 0,100,200,300,400 ms; IMU at 200,300,400 ms.
    - Anchors 0, 100: no IMU prev → emit None.
    - Anchors 200, 300: bracketed by IMU pairs → interpolated.
    - Anchor 400: prev=400 exists but next is None → emit None.
    """
    fixture = missing_before_first(sync_fixtures_dir)
    result = synchronize(
        _topic_views(fixture.path),
        method="interpolate",
        anchor="/joint_states",
        engine="streaming",
        out_of_order="reorder",
        max_lateness_ms=100.0,
        boundary="null",
    )
    assert result.height == 5
    imu_col = [c for c in result.columns if "imu_data__linear_acceleration.x" == c][0]
    values = result[imu_col].to_list()
    # First two: no prev.
    assert values[0] is None
    assert values[1] is None
    # Middle two: interpolated, finite.
    assert values[2] is not None
    assert values[3] is not None
    # Last: no next (IMU exhausted at 400ms exactly = anchor).
    assert values[4] is None


def test_interpolate_boundary_drop(sync_fixtures_dir):
    """boundary='drop' skips anchor rows lacking bracketing samples.

    Same setup as the null test — only the middle two anchor rows
    survive, since the first two lack prev and the last lacks next.
    """
    fixture = missing_before_first(sync_fixtures_dir)
    result = synchronize(
        _topic_views(fixture.path),
        method="interpolate",
        anchor="/joint_states",
        engine="streaming",
        out_of_order="reorder",
        max_lateness_ms=100.0,
        boundary="drop",
    )
    assert result.height == 2


def test_interpolate_boundary_error(sync_fixtures_dir):
    """boundary='error' raises SyncBoundaryError on missing brackets."""
    fixture = missing_before_first(sync_fixtures_dir)
    with pytest.raises(SyncBoundaryError) as exc:
        synchronize(
            _topic_views(fixture.path),
            method="interpolate",
            anchor="/joint_states",
            engine="streaming",
            out_of_order="reorder",
            max_lateness_ms=100.0,
            boundary="error",
        )
    assert exc.value.topic_name == "/imu/data"
    assert exc.value.position == "before_first"


# ---------------------------------------------------------------------------
# Engine="auto" routing.
# ---------------------------------------------------------------------------


def test_auto_picks_eager_for_small_topics(sync_fixtures_dir, monkeypatch):
    """With default threshold (1M), the small fixture topics route to eager."""
    fixture = fast_vs_slow(sync_fixtures_dir)
    # We can't easily spy on private functions — instead, verify that the
    # auto-selected engine produces equivalent output to explicit eager.
    auto = synchronize(
        _topic_views(fixture.path),
        method="nearest",
        anchor="/joint_states",
        engine="auto",
    )
    eager = synchronize(
        _topic_views(fixture.path),
        method="nearest",
        anchor="/joint_states",
        engine="eager",
    )
    assert _frames_equivalent(auto, eager)


def test_auto_picks_streaming_when_threshold_lowered(sync_fixtures_dir, monkeypatch):
    """Lower the threshold so even a small topic routes to streaming."""
    from resurrector.core import bag_frame as bag_frame_module
    monkeypatch.setattr(bag_frame_module, "LARGE_TOPIC_THRESHOLD", 5)
    # Re-import sync to pick up the patched value at module-level.
    from resurrector.core import sync as sync_module
    monkeypatch.setattr(sync_module, "LARGE_TOPIC_THRESHOLD", 5)
    fixture = fast_vs_slow(sync_fixtures_dir)
    # Must succeed (streaming with default permissive config).
    result = synchronize(
        _topic_views(fixture.path),
        method="nearest",
        anchor="/joint_states",
        engine="auto",
        out_of_order="reorder",
        max_lateness_ms=100.0,
    )
    assert result.height == 10


# ---------------------------------------------------------------------------
# iter_synchronize: the chunked form behind synced exports. Concatenating
# its chunks must reproduce synchronize() exactly (values, dtypes, column
# order), for every fixture, both engines, and chunk sizes that put
# boundaries everywhere.
# ---------------------------------------------------------------------------

_PERMISSIVE = {"out_of_order": "reorder", "max_lateness_ms": 100.0}

_ITER_CASES = [
    ("eager", "nearest", {}),
    ("eager", "sample_and_hold", {}),
    ("eager", "interpolate", {}),
    ("streaming", "nearest", _PERMISSIVE),
    ("streaming", "sample_and_hold", _PERMISSIVE),
    ("streaming", "interpolate", {**_PERMISSIVE, "boundary": "null"}),
    ("streaming", "interpolate", {**_PERMISSIVE, "boundary": "hold"}),
    ("streaming", "interpolate", {**_PERMISSIVE, "boundary": "drop"}),
]


def _chunk_sizes(n_rows: int) -> tuple[int, ...]:
    """Small enough to split every fixture, with boundaries that drift."""
    if n_rows <= 60:
        return (1, 3)
    return (n_rows // 7 + 1,)


_DECODED: dict[tuple[str, str], pl.DataFrame] = {}


class _DecodedView:
    """A fixture topic decoded once per session.

    Stands in for TopicView (the engines only use ``message_count``,
    ``iter_chunks`` and ``to_polars``) so the equivalence matrix below
    doesn't re-parse the 10K-message burst fixture for every case.
    """

    def __init__(self, bag_path: Path, name: str):
        key = (str(bag_path), name)
        if key not in _DECODED:
            _DECODED[key] = BagFrame(bag_path)[name].to_polars()
        self._df = _DECODED[key]
        self.name = name
        self.message_count = self._df.height

    def to_polars(self, force: bool = False) -> pl.DataFrame:
        return self._df

    def iter_chunks(self, chunk_size: int = 50_000):
        for start in range(0, self._df.height, chunk_size):
            yield self._df.slice(start, chunk_size)


def _decoded_views(bag_path: Path):
    return {
        "/joint_states": _DecodedView(bag_path, "/joint_states"),
        "/imu/data": _DecodedView(bag_path, "/imu/data"),
    }


@pytest.mark.parametrize("anchor", ["/joint_states", "/imu/data"])
@pytest.mark.parametrize(
    "engine,method,extra", _ITER_CASES,
    ids=[f"{e}-{m}-{x.get('boundary', '')}" for e, m, x in _ITER_CASES],
)
@pytest.mark.parametrize("builder", ALL_FIXTURE_BUILDERS, ids=lambda b: b.__name__)
def test_iter_synchronize_chunks_concat_to_synchronize(
    builder, engine, method, extra, anchor, sync_fixtures_dir,
):
    """Would catch: a chunk boundary changing which sample is matched,
    or chunks with drifting schemas (a streaming writer can't append
    those)."""
    from polars.testing import assert_frame_equal

    from resurrector.core.sync import iter_synchronize

    fixture = builder(sync_fixtures_dir)
    kwargs = dict(
        method=method, tolerance_ms=50.0, anchor=anchor, engine=engine, **extra,
    )
    whole = synchronize(_decoded_views(fixture.path), **kwargs)

    for chunk_size in _chunk_sizes(whole.height):
        chunks = list(iter_synchronize(
            _decoded_views(fixture.path), chunk_size=chunk_size, **kwargs,
        ))
        assert all(0 < c.height <= chunk_size for c in chunks)
        assert all(c.schema == chunks[0].schema for c in chunks)
        joined = pl.concat(chunks) if chunks else pl.DataFrame()
        assert_frame_equal(joined, whole)


class _ProbeView:
    """TopicView stand-in that counts how many input chunks were pulled."""

    def __init__(self, name: str, n_chunks: int, rows: int, period_ns: int):
        self.name = name
        self.n_chunks = n_chunks
        self.rows = rows
        self.period_ns = period_ns
        self.message_count = n_chunks * rows
        self.pulled = 0

    def iter_chunks(self, chunk_size: int = 50_000):
        import numpy as np
        for c in range(self.n_chunks):
            self.pulled += 1
            ts = (np.arange(self.rows) + c * self.rows) * self.period_ns
            yield pl.DataFrame({"timestamp_ns": ts, "x": ts.astype(float)})


@pytest.mark.parametrize("method", ["nearest", "sample_and_hold", "interpolate"])
def test_streaming_iter_synchronize_yields_before_reading_everything(method):
    """The streaming engine must hand out output as it goes, not after
    reading the whole bag. Would catch: output rows accumulated into one
    list and converted at the end (the behaviour through 0.8.4)."""
    from resurrector.core.sync import iter_synchronize

    anchor = _ProbeView("/anchor", n_chunks=50, rows=10, period_ns=10_000_000)
    other = _ProbeView("/other", n_chunks=50, rows=10, period_ns=10_000_000)
    it = iter_synchronize(
        {"/anchor": anchor, "/other": other},
        method=method, anchor="/anchor", engine="streaming", chunk_size=10,
    )
    first = next(it)
    assert first.height == 10
    assert anchor.pulled <= 2 and other.pulled <= 2

    rest = list(it)
    assert sum(c.height for c in rest) == 490
    assert anchor.pulled == 50 and other.pulled == 50


def test_streaming_warns_when_a_topic_gains_columns(caplog):
    """The output schema comes from each topic's first chunk. A column
    that only appears later can't be added, so say so instead of
    dropping it silently."""
    from resurrector.core.sync import iter_synchronize

    class GrowingView:
        name = "/grow"
        message_count = 4

        def iter_chunks(self, chunk_size=50_000):
            yield pl.DataFrame({"timestamp_ns": [0, 10], "x": [1.0, 2.0]})
            yield pl.DataFrame({"timestamp_ns": [20, 30], "x": [3.0, 4.0], "y": [5.0, 6.0]})

    anchor = _ProbeView("/anchor", n_chunks=1, rows=4, period_ns=10)
    with caplog.at_level("WARNING", logger="resurrector.core.sync"):
        out = pl.concat(list(iter_synchronize(
            {"/anchor": anchor, "/grow": GrowingView()},
            anchor="/anchor", engine="streaming", tolerance_ms=1.0,
        )))

    assert "grow__y" not in out.columns
    assert out["grow__x"].to_list() == [1.0, 2.0, 3.0, 4.0]
    assert "/grow gained columns ['y']" in caplog.text


def test_iter_synchronize_rejects_bad_chunk_size(sync_fixtures_dir):
    from resurrector.core.sync import iter_synchronize

    fixture = fast_vs_slow(sync_fixtures_dir)
    with pytest.raises(ValueError, match="chunk_size"):
        iter_synchronize(_topic_views(fixture.path), chunk_size=0)


def test_streaming_schema_fixed_when_a_topic_matches_late(sync_fixtures_dir):
    """A topic that has no match in the first chunk still gets its
    columns (typed, null) in that chunk, so every chunk can go to the
    same Parquet writer. Would catch: schema inferred per chunk, which
    breaks a synced export when e.g. a camera starts recording late."""
    from resurrector.core.sync import iter_synchronize

    fixture = missing_before_first(sync_fixtures_dir)
    chunks = list(iter_synchronize(
        _topic_views(fixture.path),
        method="nearest", tolerance_ms=5.0, anchor="/joint_states",
        engine="streaming", chunk_size=1,
    ))
    imu_col = "imu_data__linear_acceleration.x"
    assert chunks[0][imu_col].to_list() == [None]
    assert chunks[0].schema[imu_col] == pl.Float64
    assert chunks[2][imu_col].to_list() != [None]
    assert all(c.schema == chunks[0].schema for c in chunks)
