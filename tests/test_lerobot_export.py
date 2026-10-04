"""LeRobot export: grid/resampling unit tests + real-LeRobot round-trips.

The pre-v0.8.4 exporter only had tests asserting its own hand-rolled file
layout, so it shipped datasets that LeRobot refused to load (wrong codebase
version, missing index columns, string columns typed float32, no camera
pixels). The round-trip tests here load the export with the *installed*
``LeRobotDataset`` and check frames, so a spec change in LeRobot fails CI
instead of shipping. They run in CI's ``extras-test (lerobot)`` job and skip
elsewhere; the unit tests below always run.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from resurrector.core.bag_frame import BagFrame
from resurrector.core.lerobot_export import (
    INSTALL_HINT,
    _prepare_root,
    asof_on_grid,
    build_grid,
    numeric_columns,
    to_rgb,
)
from resurrector.demo.sample_bag import BagConfig, generate_bag


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def sample_bag(tmp_dir):
    return generate_bag(tmp_dir / "sample.mcap", BagConfig(duration_sec=3.0))


# ------------------------------------------------------------------ grid

class TestBuildGrid:
    def test_uniform_spacing_and_bounds(self):
        g = build_grid(1_000_000_000, 2_000_000_000, 30)
        assert g[0] == 1_000_000_000
        assert g[-1] <= 2_000_000_000
        assert len(g) == 31  # 0..30 frames inclusive over exactly 1 s
        steps = np.diff(g)
        assert steps.min() >= 33_333_333 and steps.max() <= 33_333_334

    def test_empty_when_no_overlap(self):
        assert len(build_grid(10, 5, 30)) == 0

    def test_no_cumulative_drift(self):
        # 1 hour at 30 fps: frame k must be within 1 ns of k/fps.
        g = build_grid(0, 3600 * 10**9, 30)
        k = len(g) - 1
        assert abs(int(g[k]) - round(k * 1e9 / 30)) <= 1


class TestAsofOnGrid:
    def _topic(self, n=1000, seed=0):
        rng = np.random.default_rng(seed)
        ts = np.sort(rng.integers(0, 10**9, n)).astype(np.int64)
        return pl.DataFrame({"timestamp_ns": ts, "x": rng.normal(size=n), "y": np.arange(n)})

    @pytest.mark.parametrize("chunk", [1, 7, 128, 5000])
    def test_matches_full_join_asof(self, chunk):
        """Streaming result == polars join_asof on the whole topic, any chunking."""
        df = self._topic()
        grid = build_grid(-50_000_000, 1_100_000_000, 60)  # extends past both ends
        expected = pl.DataFrame({"timestamp_ns": grid}).join_asof(
            df, on="timestamp_ns", strategy="backward",
        )
        chunks = (df.slice(i, chunk) for i in range(0, df.height, chunk))
        got = asof_on_grid(chunks, grid, ["x", "y"])
        assert got.height == len(grid)
        assert got.select("x", "y").equals(expected.select("x", "y"))

    def test_never_uses_future_samples(self):
        df = pl.DataFrame({"timestamp_ns": [0, 100, 200], "x": [1.0, 2.0, 3.0]})
        got = asof_on_grid([df], np.array([99, 100, 150, 250], dtype=np.int64), ["x"])
        assert got["x"].to_list() == [1.0, 2.0, 2.0, 3.0]


class TestColumnSelection:
    def test_numeric_columns_skip_bookkeeping(self):
        schema = pl.Schema({
            "timestamp_ns": pl.Int64, "header.stamp_sec": pl.Int64,
            "header.stamp_nsec": pl.Int64, "header.frame_id": pl.String,
            "data_length": pl.UInt32, "position.0": pl.Float64, "ok": pl.Boolean,
        })
        assert numeric_columns(schema) == ["position.0", "ok"]


class TestToRgb:
    def test_gray_to_three_channels(self):
        out = to_rgb(np.full((2, 3), 7, np.uint8), "mono8")
        assert out.shape == (2, 3, 3) and (out == 7).all()

    def test_bgr_is_reversed(self):
        px = np.array([[[10, 20, 30]]], np.uint8)
        assert to_rgb(px, "bgr8")[0, 0].tolist() == [30, 20, 10]
        assert to_rgb(px, "rgb8")[0, 0].tolist() == [10, 20, 30]

    def test_alpha_dropped(self):
        px = np.array([[[10, 20, 30, 255]]], np.uint8)
        assert to_rgb(px, "bgra8")[0, 0].tolist() == [30, 20, 10]
        assert to_rgb(px, "rgba8").shape == (1, 1, 3)


class TestStartMethodProbe:
    def test_probe_does_not_fix_start_method(self, tmp_dir):
        """Deciding whether to encode cameras in parallel must not fix the start method.

        Would catch: multiprocessing.get_start_method() (allow_none=False),
        which sets the default context as a side effect, so a caller's later
        set_start_method() raised "context has already been set". Fresh
        interpreter so nothing else has set it. (LeRobot's save_episode()
        currently fixes it on its own; this pins our code's behavior.)
        """
        import subprocess

        script = tmp_dir / "probe.py"
        script.write_text(
            "import multiprocessing\n"
            "from resurrector.core.lerobot_export import _start_method_is_fork\n"
            "_start_method_is_fork()\n"
            "multiprocessing.set_start_method('spawn')\n"
            "print('still settable')\n"
        )
        proc = subprocess.run([sys.executable, str(script)], capture_output=True, text=True, timeout=120)
        assert proc.returncode == 0, proc.stderr[-1500:]
        assert "still settable" in proc.stdout


class TestCliOutput:
    """What the user actually sees in the terminal, not just exception text.

    Would catch: Rich treating "[lerobot]" as a markup tag and swallowing
    it, so the CLI told users to `pip install 'rosbag-resurrector'` (the
    package they already had). The exception-level test above passed the
    whole time; only the rendered output was wrong.
    """

    def test_missing_lerobot_hint_keeps_extra_name(self, tmp_dir, sample_bag, monkeypatch):
        from typer.testing import CliRunner
        from resurrector.cli.main import app

        monkeypatch.setitem(sys.modules, "lerobot.datasets.lerobot_dataset", None)
        result = CliRunner().invoke(
            app, ["export", str(sample_bag), "--preset", "lerobot", "-o", str(tmp_dir / "out")],
            env={"COLUMNS": "200"},
        )
        assert result.exit_code == 1
        assert "rosbag-resurrector[lerobot]" in result.output, result.output

    def test_list_presets_needs_no_path_and_shows_extras(self):
        from typer.testing import CliRunner
        from resurrector.cli.main import app

        result = CliRunner().invoke(app, ["export", "--list-presets"], env={"COLUMNS": "200"})
        assert result.exit_code == 0, result.output
        assert "rosbag-resurrector[lerobot]" in result.output
        assert "rosbag-resurrector[all-exports]" in result.output


class TestGuards:
    def test_nonempty_target_refused(self, tmp_dir):
        (tmp_dir / "x.txt").write_text("keep me")
        with pytest.raises(FileExistsError):
            _prepare_root(tmp_dir)
        assert (tmp_dir / "x.txt").exists()

    def test_missing_lerobot_gives_install_hint(self, tmp_dir, sample_bag, monkeypatch):
        monkeypatch.setitem(sys.modules, "lerobot.datasets.lerobot_dataset", None)
        with pytest.raises(ImportError, match=r"\[lerobot\]"):
            BagFrame(sample_bag).export(format="lerobot", output=str(tmp_dir / "out"))
        assert "Python 3.12" in INSTALL_HINT


# -------------------------------------------- round-trip through real LeRobot

def _lerobot():
    return pytest.importorskip("lerobot.datasets.lerobot_dataset")


def _load(root: Path):
    return _lerobot().LeRobotDataset(repo_id=f"local/{root.name}", root=root)


class TestLeRobotRoundTrip:
    def test_preset_export_loads_in_lerobot(self, tmp_dir, sample_bag):
        _lerobot()
        out = tmp_dir / "lr"
        BagFrame(sample_bag).export(preset="lerobot", output=str(out), task="pick cube")
        ds = _load(out)

        assert ds.num_episodes == 1
        assert isinstance(ds.fps, int) and ds.fps == 30
        assert ds.num_frames > 60  # ~3 s at 30 fps

        f = ds[5]
        assert f["task"] == "pick cube"
        assert f["observation.state"].dtype.is_floating_point
        names = ds.meta.features["observation.state"]["names"]
        assert len(names) == f["observation.state"].shape[0]
        assert any(n.startswith("imu/data/") for n in names)
        assert not any("frame_id" in n or "stamp" in n for n in names)

        cams = [k for k in ds.meta.features if k.startswith("observation.images.")]
        assert "observation.images.camera_rgb" in cams
        assert f["observation.images.camera_rgb"].shape[0] == 3  # CHW

        ts = [float(ds[i]["timestamp"]) for i in range(10)]
        assert np.allclose(np.diff(ts), 1 / 30, atol=1e-6)

    def test_state_values_match_source_bag(self, tmp_dir, sample_bag):
        """Frame k holds the latest source sample at or before grid time k.

        30 fps against 200 Hz IMU puts most grid points *between* samples,
        so a forward/nearest resampler would produce different values; the
        test asserts it is checking such frames, otherwise it couldn't tell
        causal from non-causal resampling apart.
        """
        _lerobot()
        out = tmp_dir / "lr"
        bf = BagFrame(sample_bag)
        bf.export(format="lerobot", topics=["/imu/data", "/joint_states"],
                  output=str(out), downsample_hz=30)
        ds = _load(out)
        names = ds.meta.features["observation.state"]["names"]
        col = names.index("imu/data/linear_acceleration.x")

        imu = bf["/imu/data"].to_polars().sort("timestamp_ns")
        joints = bf["/joint_states"].to_polars()
        t0 = max(int(imu["timestamp_ns"].min()), int(joints["timestamp_ns"].min()))
        grid = build_grid(t0, min(int(imu["timestamp_ns"].max()),
                                  int(joints["timestamp_ns"].max())), 30)
        assert ds.num_frames == len(grid)
        g = pl.DataFrame({"timestamp_ns": grid})
        src = imu.select("timestamp_ns", "linear_acceleration.x")
        back = g.join_asof(src, on="timestamp_ns", strategy="backward")["linear_acceleration.x"].to_numpy()
        fwd = g.join_asof(src, on="timestamp_ns", strategy="forward")["linear_acceleration.x"].to_numpy()

        frames = [1, 2, 7, len(grid) // 2, len(grid) - 2]
        assert sum(back[k] != fwd[k] for k in frames) >= 3, "test data can't distinguish strategies"
        for k in frames:
            assert float(ds[k]["observation.state"][col]) == pytest.approx(back[k], rel=1e-5, abs=1e-6)

    def test_action_topics_split_out(self, tmp_dir, sample_bag):
        _lerobot()
        out = tmp_dir / "lr"
        BagFrame(sample_bag).export(
            format="lerobot", topics=["/imu/data", "/joint_states"],
            output=str(out), action_topics=["/joint_states"],
        )
        ds = _load(out)
        state = ds.meta.features["observation.state"]["names"]
        action = ds.meta.features["action"]["names"]
        assert all(n.startswith("joint_states/") for n in action)
        assert not any(n.startswith("joint_states/") for n in state)
        assert ds[0]["action"].shape[0] == len(action)

    def test_camera_pixels_survive(self, tmp_dir, sample_bag):
        """Decoded video frame ≈ source image (lossy codec, so tolerance)."""
        _lerobot()
        out = tmp_dir / "lr"
        bf = BagFrame(sample_bag)
        bf.export(format="lerobot", topics=["/camera/rgb"], output=str(out))
        ds = _load(out)
        _, src = next(iter(bf["/camera/rgb"].iter_images()))
        got = (ds[0]["observation.images.camera_rgb"].permute(1, 2, 0).numpy() * 255)
        assert got.shape == src.shape
        assert np.abs(got - src.astype(np.float32)).mean() < 20

    def test_multi_bag_dataset_has_one_episode_per_bag(self, tmp_dir):
        _lerobot()
        from resurrector.core.dataset import BagRef, DatasetManager

        a = generate_bag(tmp_dir / "a.mcap", BagConfig(duration_sec=2.0))
        b = generate_bag(tmp_dir / "b.mcap", BagConfig(duration_sec=2.0))
        mgr = DatasetManager(tmp_dir / "idx.db")
        mgr.create("pick")
        mgr.create_version(
            "pick", "1.0", [BagRef(path=str(a)), BagRef(path=str(b))],
            topics=["/imu/data", "/joint_states"], export_format="lerobot",
        )
        root = mgr.export_version("pick", "1.0", str(tmp_dir / "datasets"))
        ds = _load(Path(root))
        assert ds.num_episodes == 2
        assert (Path(root) / "manifest.json").exists()

    def test_unguarded_script_under_spawn_with_two_cameras(self, tmp_dir, sample_bag):
        """The documented one-liner must work from a plain script on macOS.

        Would catch: LeRobot's parallel video encoding (a process pool when a
        bag has 2+ cameras) under the spawn start method — macOS/Windows
        default, Linux forkserver on 3.14+ — re-importing the user's
        unguarded __main__, which re-ran the export and died with
        BrokenProcessPool. Forced to spawn here so the Linux CI job covers it.
        """
        import subprocess
        import textwrap

        _lerobot()
        out = tmp_dir / "lr_spawn"
        script = tmp_dir / "user_script.py"
        script.write_text(textwrap.dedent(f"""
            import multiprocessing
            multiprocessing.set_start_method("spawn", force=True)
            from resurrector.core.bag_frame import BagFrame
            BagFrame({str(sample_bag)!r}).export(preset="lerobot", output={str(out)!r})
        """))
        proc = subprocess.run([sys.executable, str(script)], capture_output=True, text=True, timeout=600)
        assert proc.returncode == 0, proc.stderr[-2000:]
        ds = _load(out)
        cams = [k for k in ds.meta.features if k.startswith("observation.images.")]
        assert len(cams) >= 2, f"test needs a multi-camera bag to exercise the pool, got {cams}"
        assert ds.num_frames > 0

    def test_existing_output_dir_is_refused(self, tmp_dir, sample_bag):
        _lerobot()
        out = tmp_dir / "lr"
        out.mkdir()
        (out / "old.parquet").write_text("previous export")
        with pytest.raises(FileExistsError):
            BagFrame(sample_bag).export(preset="lerobot", output=str(out))
        assert (out / "old.parquet").exists()
