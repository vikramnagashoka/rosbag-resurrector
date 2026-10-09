"""Synced exports stream: the synced table is never built in memory.

``Exporter.export(sync=True)`` (CLI ``--sync``; the ``rlds``,
``training-tabular`` and ``multimodal`` presets) consumes
:func:`resurrector.core.sync.iter_synchronize` chunk by chunk and
downsamples with :func:`iter_downsample_temporal`, which carries its
grid across chunk boundaries. The output must be identical to the
whole-table path it replaced: ``downsample_temporal(bf.sync(...))``.

The memory bound itself is checked in ``test_streaming_oom.py``.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from resurrector.core import export as export_module
from resurrector.core import sync as sync_module
from resurrector.core.bag_frame import BagFrame
from resurrector.core.export import ExportError, Exporter
from resurrector.core.transforms import downsample_temporal, iter_downsample_temporal
from resurrector.demo.sample_bag import SCHEMAS
from resurrector.ingest.parser import register_decoder, unregister_decoder
from tests.fixtures.generate_test_bags import BagConfig, generate_bag
from tests.fixtures.sync_fixtures import BASE_TIME_NS, _encode_imu_at, _encode_joint_at

TOPICS = ["/imu/data", "/joint_states"]


@pytest.fixture(scope="module")
def bag(tmp_path_factory) -> Path:
    path = tmp_path_factory.mktemp("sync_export") / "bag.mcap"
    return generate_bag(path, BagConfig(
        duration_sec=3.0, camera_hz=5.0, lidar_hz=2.0, include_compressed=False,
    ))


@pytest.fixture
def small_chunks(monkeypatch):
    """Shrink the export chunk so a 3 s bag spans many chunks."""
    monkeypatch.setattr(export_module, "CHUNK_SIZE", 64)
    return 64


@pytest.fixture(params=["eager", "streaming"])
def sync_engine(request, monkeypatch):
    """Exports use engine='auto'; lowering the threshold routes them to
    the streaming engine without needing a 1M-message bag."""
    if request.param == "streaming":
        monkeypatch.setattr(sync_module, "LARGE_TOPIC_THRESHOLD", 5)
    return request.param


def _spy_parquet_chunks(monkeypatch) -> list[int]:
    heights: list[int] = []
    real = export_module._stream_parquet

    def spy(chunks, output_path, name):
        def tap():
            for chunk in chunks:
                heights.append(chunk.height)
                yield chunk
        return real(tap(), output_path, name)

    monkeypatch.setattr(export_module, "_stream_parquet", spy)
    return heights


# ---------------------------------------------------------------------------
# Exporter: the synced path writes bounded chunks, same bytes-for-rows
# output as the old whole-table path.
# ---------------------------------------------------------------------------


def test_synced_export_writes_bounded_chunks(
    bag, tmp_path, monkeypatch, small_chunks, sync_engine,
):
    """Would catch: the synced table built in one piece and handed to
    the writer as a single chunk (the behaviour through 0.8.4)."""
    heights = _spy_parquet_chunks(monkeypatch)
    bf = BagFrame(bag)

    Exporter().export(
        bag_frame=bf, topics=TOPICS, format="parquet",
        output_dir=str(tmp_path), sync=True,
    )

    expected = bf.sync(TOPICS)
    assert expected.height > 3 * small_chunks
    assert len(heights) > 1
    assert max(heights) <= small_chunks
    assert_frame_equal(pl.read_parquet(tmp_path / "synced.parquet"), expected)


@pytest.mark.parametrize("hz", [7.0, 50.0, 333.0])
@pytest.mark.parametrize("method", ["nearest", "interpolate"])
def test_synced_downsampled_export_matches_whole_table(
    bag, tmp_path, small_chunks, sync_engine, hz, method,
):
    """Chunk boundaries must not shift the downsample grid: the streamed
    export has to equal downsampling the whole synced table at once."""
    bf = BagFrame(bag)

    Exporter().export(
        bag_frame=bf, topics=TOPICS, format="parquet",
        output_dir=str(tmp_path), sync=True, sync_method=method,
        downsample_hz=hz,
    )

    expected = downsample_temporal(bf.sync(TOPICS, method=method), hz)
    assert_frame_equal(pl.read_parquet(tmp_path / "synced.parquet"), expected)


def test_synced_csv_export_matches_whole_table(bag, tmp_path, small_chunks):
    """One header, rows in order, across many chunks."""
    bf = BagFrame(bag)

    Exporter().export(
        bag_frame=bf, topics=TOPICS, format="csv",
        output_dir=str(tmp_path), sync=True, downsample_hz=50.0,
    )

    expected = downsample_temporal(bf.sync(TOPICS), 50.0)
    assert (tmp_path / "synced.csv").read_text() == expected.write_csv()


def test_synced_export_with_no_rows_still_writes_file(bag, tmp_path):
    """A time slice past the end of the bag syncs to nothing; the export
    still produces the (empty) file, as it did before streaming."""
    sliced = BagFrame(bag).time_slice(100.0, 101.0)

    Exporter().export(
        bag_frame=sliced, topics=TOPICS, format="parquet",
        output_dir=str(tmp_path), sync=True, downsample_hz=10.0,
    )

    assert pl.read_parquet(tmp_path / "synced.parquet").height == 0


def test_synced_export_missing_topic_still_raises(bag, tmp_path):
    with pytest.raises(KeyError, match="/nope"):
        Exporter().export(
            bag_frame=BagFrame(bag), topics=["/imu/data", "/nope"],
            format="parquet", output_dir=str(tmp_path), sync=True,
        )


# ---------------------------------------------------------------------------
# HDF5 / Zarr: a column's dataset dtype is fixed when the first chunk is
# written, so it must not depend on whether that chunk happens to hold an
# unmatched row. The bag below syncs to fully matched leading chunks and
# unmatched trailing ones.
# ---------------------------------------------------------------------------

FLAG_TYPE = "resurrector_test/msg/Flag"
GAP_TOPICS = ["/imu/data", "/joint_states", "/flag"]
GAP_ROWS = 24      # /imu/data, the anchor: 0 .. 2.3 s every 100 ms
GAP_MATCHED = 8    # /joint_states and /flag stop after 0.7 s


def _decode_flag(data: bytes) -> dict:
    return {"data": bool(data[4])}


@pytest.fixture
def flag_decoder():
    """A one-field Boolean message type, decoded through the public
    decoder registry (none of the built-in types carry a bool)."""
    register_decoder(FLAG_TYPE, _decode_flag)
    yield
    unregister_decoder(FLAG_TYPE)


@pytest.fixture(scope="module")
def gap_bag(tmp_path_factory) -> Path:
    from mcap.writer import Writer

    path = tmp_path_factory.mktemp("gap") / "gap.mcap"
    with open(path, "wb") as f:
        writer = Writer(f)
        writer.start(profile="ros2", library="resurrector-test")
        channels = {}
        for topic, msg_type, schema_text in [
            ("/imu/data", "sensor_msgs/msg/Imu", SCHEMAS["sensor_msgs/msg/Imu"]["data"]),
            ("/joint_states", "sensor_msgs/msg/JointState",
             SCHEMAS["sensor_msgs/msg/JointState"]["data"]),
            ("/flag", FLAG_TYPE, "bool data\n"),
        ]:
            sid = writer.register_schema(
                name=msg_type, encoding="ros2msg", data=schema_text.encode(),
            )
            channels[topic] = writer.register_channel(
                topic=topic, message_encoding="cdr", schema_id=sid,
            )
        for i in range(GAP_ROWS):
            t = BASE_TIME_NS + i * 100_000_000
            writer.add_message(channels["/imu/data"], log_time=t, publish_time=t,
                               data=_encode_imu_at(t))
            if i < GAP_MATCHED:
                writer.add_message(channels["/joint_states"], log_time=t,
                                   publish_time=t, data=_encode_joint_at(t))
                writer.add_message(channels["/flag"], log_time=t, publish_time=t,
                                   data=b"\x00\x01\x00\x00" + bytes([i % 2 == 0]))
        writer.finish()
    return path


def _read_columns(out_dir: Path, fmt: str) -> dict[str, np.ndarray]:
    if fmt == "hdf5":
        import h5py
        with h5py.File(out_dir / "synced.h5", "r") as f:
            return {k: f["synced"][k][:] for k in f["synced"]}
    import zarr
    group = zarr.open_group(str(out_dir / "synced.zarr"), mode="r")
    return {k: group[k][:] for k in group.array_keys()}


def _export_gap(bf: BagFrame, topics: list[str], fmt: str, out: Path) -> set[str]:
    """Synced export of ``topics``; returns the columns that failed."""
    try:
        Exporter().export(
            bag_frame=bf, topics=topics, format=fmt, output_dir=str(out), sync=True,
        )
    except ExportError as e:
        return {f.column for f in e.failures}
    return set()


def _assert_written_like(got: dict[str, np.ndarray], expected: pl.DataFrame,
                         strings: set[str]) -> None:
    """Every non-string column written as float64 with the reference's
    values, NaN exactly where the reference has NaN or null."""
    assert got["timestamp_ns"].dtype == np.int64
    np.testing.assert_array_equal(got["timestamp_ns"], expected["timestamp_ns"].to_numpy())
    for col in set(expected.columns) - strings - {"timestamp_ns"}:
        assert got[col].dtype == np.float64, col
        np.testing.assert_array_equal(
            got[col], expected[col].cast(pl.Float64).to_numpy(), err_msg=col,
        )


@pytest.mark.parametrize("fmt", ["hdf5", "zarr"])
def test_synced_hdf5_zarr_int_columns_unmatched_after_first_chunk(
    gap_bag, tmp_path, monkeypatch, sync_engine, fmt,
):
    """The multimodal preset (Zarr + sync) on the streaming engine.

    Would catch: the streaming engine keeping the non-anchor Int64
    columns (the header stamps every ROS topic carries) as Int64 with
    nulls where unmatched, while the writer sized the dataset as int64
    from a fully matched first chunk: every unmatched stamp was then
    stored as 0, silently, in both formats.
    """
    if fmt == "zarr":
        pytest.importorskip("zarr")
    topics = ["/imu/data", "/joint_states"]
    monkeypatch.setattr(export_module, "CHUNK_SIZE", 4)
    bf = BagFrame(gap_bag)
    expected = bf.sync(topics, engine="eager")
    stamp = "joint_states__header.stamp_sec"
    assert expected.height == GAP_ROWS
    assert expected.schema[stamp] == pl.Float64
    assert expected[stamp][:4].is_not_nan().all()  # first chunk fully matched
    assert expected[stamp][GAP_MATCHED:].is_nan().all()

    failed = _export_gap(bf, topics, fmt, tmp_path)
    strings = {c for c, dt in expected.schema.items() if dt == pl.String}
    assert failed == set()

    got = _read_columns(tmp_path, fmt)
    assert got[stamp][0] == expected[stamp][0] > 1.6e9
    assert np.isnan(got[stamp][GAP_MATCHED:]).all(), got[stamp]
    _assert_written_like(got, expected, strings)


@pytest.mark.parametrize("fmt", ["hdf5", "zarr"])
def test_synced_hdf5_zarr_bool_column_unmatched_after_first_chunk(
    gap_bag, tmp_path, monkeypatch, flag_decoder, sync_engine, fmt,
):
    """A non-anchor Boolean column stays Boolean in the synced frame,
    null where unmatched (both engines). HDF5 / Zarr write Booleans as
    float64: 1.0 / 0.0, NaN for a missing value, whatever chunk the
    first null lands in.

    Would catch: the dataset sized as bool from a fully matched first
    chunk, after which the chunk with nulls (an object array) failed the
    column, on both engines.
    """
    if fmt == "zarr":
        pytest.importorskip("zarr")
    monkeypatch.setattr(export_module, "CHUNK_SIZE", 4)
    bf = BagFrame(gap_bag)
    expected = bf.sync(GAP_TOPICS, engine="eager")
    assert expected.schema["flag__data"] == pl.Boolean
    assert expected["flag__data"][:4].null_count() == 0
    assert expected["flag__data"][GAP_MATCHED:].is_null().all()

    failed = _export_gap(bf, GAP_TOPICS, fmt, tmp_path)
    strings = {c for c, dt in expected.schema.items() if dt == pl.String}
    assert failed == set()

    got = _read_columns(tmp_path, fmt)
    np.testing.assert_array_equal(got["flag__data"][:GAP_MATCHED], [1.0, 0.0] * 4)
    assert np.isnan(got["flag__data"][GAP_MATCHED:]).all()
    _assert_written_like(got, expected, strings)


# ---------------------------------------------------------------------------
# iter_downsample_temporal == downsample_temporal on the concatenation,
# for any chunking. Duplicate timestamps on chunk boundaries are the
# tricky case: the nearest-row pick must still land on the first of a
# run, as np.searchsorted does on the whole column.
# ---------------------------------------------------------------------------


def _split(df: pl.DataFrame, rng: np.random.Generator) -> list[pl.DataFrame]:
    cuts = np.sort(rng.integers(0, df.height + 1, size=rng.integers(0, 8)))
    bounds = [0, *cuts.tolist(), df.height]
    return [df.slice(a, b - a) for a, b in zip(bounds[:-1], bounds[1:])]


@pytest.mark.parametrize("seed", range(40))
def test_iter_downsample_matches_whole_frame(seed):
    rng = np.random.default_rng(seed)
    n = int(rng.integers(1, 300))
    steps = rng.choice([0, 0, 1, 3, 10, 37], size=n)
    ts = 1_000_000 + np.cumsum(steps)
    df = pl.DataFrame({"timestamp_ns": ts, "row": np.arange(n)})
    hz = 1e9 / float(rng.choice([1, 2, 5, 13, 50, 400]))

    chunks = _split(df, rng)
    got = list(iter_downsample_temporal(iter(chunks), hz))

    expected = downsample_temporal(df, hz)
    assert_frame_equal(pl.concat(got), expected)


@pytest.mark.parametrize("ts,cuts", [
    ([0, 5, 5, 5, 9], [2]),        # run of duplicates split across chunks
    ([0, 5, 5, 5, 9], [1, 2, 3]),  # every row its own chunk inside the run
    ([0, 5, 5, 5], [2]),           # stream ends on the run
    ([5, 5, 5, 5], [1, 3]),        # all one timestamp: nothing selected
    ([7], []),                     # single row
    ([0, 10, 20, 30], [1, 2, 3]),
])
def test_iter_downsample_edge_chunkings(ts, cuts):
    df = pl.DataFrame({"timestamp_ns": ts, "row": list(range(len(ts)))})
    bounds = [0, *cuts, df.height]
    chunks = [df.slice(a, b - a) for a, b in zip(bounds[:-1], bounds[1:])]

    for hz in (1e9 / 5, 1e9 / 4, 1e9 / 1):
        got = list(iter_downsample_temporal(iter(chunks), hz))
        assert_frame_equal(pl.concat(got), downsample_temporal(df, hz))


def test_iter_downsample_empty_stream_yields_nothing():
    assert list(iter_downsample_temporal(iter([]), 10.0)) == []


def test_iter_downsample_is_lazy():
    """Each input chunk is resolved before the next one is pulled."""
    pulled = []

    def chunks():
        for i in range(4):
            pulled.append(i)
            yield pl.DataFrame({"timestamp_ns": np.arange(i * 100, (i + 1) * 100)})

    it = iter_downsample_temporal(chunks(), 1e9 / 10)
    first = next(it)
    assert first.height > 0
    assert pulled == [0]
