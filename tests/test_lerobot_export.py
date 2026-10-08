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
from resurrector.core.exceptions import ResurrectorError
from resurrector.core.lerobot_export import (
    INSTALL_HINT,
    MIN_VIDEO_WIDTH,
    LeRobotFrameShapeError,
    _prepare_root,
    asof_on_grid,
    build_grid,
    check_frame_shape,
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


class TestFrameShapeGuard:
    """Which decoded (H, W, 3) frames are refused before LeRobot sees them.

    Each refused shape is one LeRobot 0.6.1 was seen to mishandle: height 1
    crashes (FileNotFoundError), height 3 is transposed as channels-first,
    and the AV1 encoder rejects sides under 4 px and hangs on narrow
    frames. The accepted shapes keep the guard from refusing frames LeRobot
    stores correctly.
    """

    @pytest.mark.parametrize("shape,use_videos", [
        ((1, 1, 3), True), ((1, 1, 3), False), ((1, 64, 3), False),
        ((3, 64, 3), False), ((3, 3, 3), True),
        ((48, 2, 3), True), ((2, 64, 3), True),
        ((48, 4, 3), True), ((48, 24, 3), True), ((4, 24, 3), True),
    ])
    def test_refused(self, shape, use_videos):
        with pytest.raises(LeRobotFrameShapeError) as exc:
            check_frame_shape("/cam", shape, use_videos)
        msg = str(exc.value)
        assert "'/cam'" in msg and f"{shape[0]}x{shape[1]} (height x width)" in msg
        assert isinstance(exc.value, ResurrectorError) and isinstance(exc.value, ValueError)

    @pytest.mark.parametrize("shape,use_videos", [
        ((48, 64, 3), True), ((48, 25, 3), True), ((4, 25, 3), True),
        ((48, 2, 3), False), ((2, 2, 3), False), ((8, 1, 3), False),
    ])
    def test_accepted(self, shape, use_videos):
        check_frame_shape("/cam", shape, use_videos)

    def test_messages_name_what_lerobot_needs(self):
        def msg(shape, use_videos=True):
            with pytest.raises(LeRobotFrameShapeError) as exc:
                check_frame_shape("/cam", shape, use_videos)
            return str(exc.value)

        assert "at least 2 pixels high" in msg((1, 64, 3))
        assert "resurrector demo --force" in msg((1, 1, 3))
        assert "resurrector demo --force" not in msg((1, 64, 3))
        assert "other than 1 or 3" in msg((3, 64, 3), use_videos=False)
        narrow = msg((48, 8, 3))
        assert f"at least {MIN_VIDEO_WIDTH} pixels wide" in narrow
        assert "use_videos=False" in narrow


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


class TestImageSpillCleanup:
    """LeRobot spills camera frames to ``images/<camera>/episode-N/`` and
    deletes each episode directory once its PNGs are encoded to video (or,
    in image mode, embedded in the data parquet), but leaves the per-camera
    directories."""

    def test_removes_only_empty_image_dirs(self, tmp_dir):
        from resurrector.core.lerobot_export import _remove_empty_image_dirs

        images = tmp_dir / "images"
        (images / "observation.images.cam_a" / "episode-000000").mkdir(parents=True)
        (images / "observation.images.cam_b").mkdir()
        kept = images / "observation.images.cam_c" / "episode-000000"
        kept.mkdir(parents=True)
        (kept / "frame-000000.png").write_bytes(b"png")
        (tmp_dir / "videos").mkdir()  # outside images/: never touched

        _remove_empty_image_dirs(tmp_dir)

        assert not (images / "observation.images.cam_a").exists()
        assert not (images / "observation.images.cam_b").exists()
        assert (kept / "frame-000000.png").read_bytes() == b"png"
        assert (tmp_dir / "videos").is_dir()

    def test_drops_images_dir_once_empty(self, tmp_dir):
        from resurrector.core.lerobot_export import _remove_empty_image_dirs

        (tmp_dir / "images" / "observation.images.cam_a").mkdir(parents=True)
        _remove_empty_image_dirs(tmp_dir)
        assert not (tmp_dir / "images").exists()

    def test_no_images_dir_is_a_no_op(self, tmp_dir):
        from resurrector.core.lerobot_export import _remove_empty_image_dirs

        _remove_empty_image_dirs(tmp_dir)
        assert list(tmp_dir.iterdir()) == []


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

    def test_no_empty_image_dirs_left_behind(self, tmp_dir, sample_bag):
        """Would catch: empty ``images/observation.images.*`` directories
        left in every video export after LeRobot encoded and deleted the
        spilled PNGs."""
        _lerobot()
        out = tmp_dir / "lr"
        BagFrame(sample_bag).export(preset="lerobot", output=str(out))
        empty = [str(p.relative_to(out)) for p in out.rglob("*")
                 if p.is_dir() and not any(p.iterdir())]
        assert empty == []
        assert not (out / "images").exists()
        ds = _load(out)
        assert any(k.startswith("observation.images.") for k in ds.meta.features)
        assert ds[0]["observation.images.camera_rgb"].shape[0] == 3

    def test_image_mode_dataset_still_loads(self, tmp_dir, sample_bag):
        """``use_videos=False``: LeRobot embeds each frame's PNG bytes in
        ``data/*.parquet`` and deletes the spilled PNGs, so ``images/`` is
        left empty in image mode too. The frames load from the parquet."""
        _lerobot()
        import pyarrow.parquet as pq

        from resurrector.core.lerobot_export import export_lerobot

        out = tmp_dir / "lr_img"
        export_lerobot([BagFrame(sample_bag)], ["/imu/data", "/camera/rgb"], out,
                       use_videos=False)
        empty = [str(p.relative_to(out)) for p in out.rglob("*")
                 if p.is_dir() and not any(p.iterdir())]
        assert empty == []
        cell = pq.read_table(next((out / "data").rglob("*.parquet")),
                             columns=["observation.images.camera_rgb"]).column(0)[0].as_py()
        assert cell["bytes"][:8] == b"\x89PNG\r\n\x1a\n"
        ds = _load(out)
        assert ds.meta.features["observation.images.camera_rgb"]["dtype"] == "image"
        assert ds[0]["observation.images.camera_rgb"].shape[0] == 3

    def test_one_pixel_high_camera_refused_before_writing(self, tmp_dir):
        """Would catch: 1x1 placeholder demo frames (and any 1-pixel-high
        camera) ending the export in a raw FileNotFoundError from LeRobot's
        image writer instead of a message naming the topic."""
        _lerobot()
        bag = generate_bag(tmp_dir / "flat.mcap",
                           BagConfig(duration_sec=1.0, image_height=1, image_width=8))
        out = tmp_dir / "lr"
        with pytest.raises(LeRobotFrameShapeError, match=r"1x8 \(height x width\)"):
            BagFrame(bag).export(preset="lerobot", output=str(out))
        assert not out.exists()

    def test_cli_reports_bad_frame_shape_in_one_line(self, tmp_dir):
        from typer.testing import CliRunner
        from resurrector.cli.main import app

        _lerobot()
        bag = generate_bag(tmp_dir / "flat.mcap",
                           BagConfig(duration_sec=1.0, image_height=1, image_width=1))
        result = CliRunner().invoke(
            app, ["export", str(bag), "--preset", "lerobot", "-o", str(tmp_dir / "out")],
            env={"COLUMNS": "400"},
        )
        assert result.exit_code == 1
        assert isinstance(result.exception, SystemExit), repr(result.exception)
        assert "Export failed" in result.output and "resurrector demo --force" in result.output
        assert not (tmp_dir / "out").exists()

    def test_three_pixel_high_image_mode_refused(self, tmp_dir):
        """Would catch: image mode quietly storing 3-pixel-high frames
        transposed (LeRobot treats them as channels-first)."""
        _lerobot()
        from resurrector.core.lerobot_export import export_lerobot

        bag = generate_bag(tmp_dir / "strip.mcap",
                           BagConfig(duration_sec=1.0, image_height=3, image_width=16))
        out = tmp_dir / "lr_img"
        with pytest.raises(LeRobotFrameShapeError, match="channels-first"):
            export_lerobot([BagFrame(bag)], ["/camera/rgb"], out, use_videos=False)
        assert not out.exists()

    def test_narrow_camera_refused_for_video_but_kept_as_images(self, tmp_dir):
        """Video mode refuses a frame its encoder can't take (without the
        guard: an encoder error for widths under 4, a hang for narrow
        frames); image mode stores the same frames exactly, so the guard
        doesn't refuse what LeRobot handles."""
        _lerobot()
        from resurrector.core.lerobot_export import export_lerobot

        bag = generate_bag(tmp_dir / "narrow.mcap",
                           BagConfig(duration_sec=1.0, image_height=48, image_width=2))
        bf = BagFrame(bag)
        with pytest.raises(LeRobotFrameShapeError, match="use_videos=False"):
            export_lerobot([bf], ["/camera/rgb"], tmp_dir / "lr_vid")
        assert not (tmp_dir / "lr_vid").exists()

        out = tmp_dir / "lr_img"
        export_lerobot([bf], ["/camera/rgb"], out, use_videos=False)
        ds = _load(out)
        _, src = next(iter(bf["/camera/rgb"].iter_images()))
        got = (ds[0]["observation.images.camera_rgb"].permute(1, 2, 0).numpy() * 255).round()
        assert got.shape == src.shape == (48, 2, 3)
        assert np.array_equal(got.astype(np.uint8), src)

    def test_dataset_readme_quick_start_runs(self, tmp_dir, monkeypatch):
        """The README's quick start must actually load the dataset, from any
        working directory.

        Would catch: a LeRobot dataset README whose quick start was a
        "load your lerobot files" comment (and whose config section listed
        a sync method the export never applied); and one whose ``root=`` was
        the relative ``datasets/...`` path export_version was given, so run
        from anywhere else LeRobot fell through to a Hub lookup."""
        import os
        import re
        import subprocess

        _lerobot()
        from resurrector.core.dataset import BagRef, DatasetManager, SyncConfig

        a = generate_bag(tmp_dir / "a.mcap", BagConfig(duration_sec=2.0))
        b = generate_bag(tmp_dir / "b.mcap", BagConfig(duration_sec=2.0))
        monkeypatch.chdir(tmp_dir)
        mgr = DatasetManager(tmp_dir / "idx.db")
        mgr.create("pick")
        mgr.create_version(
            "pick", "1.0", [BagRef(path=str(a)), BagRef(path=str(b))],
            topics=["/imu/data", "/joint_states"], export_format="lerobot",
            sync_config=SyncConfig(method="nearest", tolerance_ms=50),
        )
        root = mgr.export_version("pick", "1.0", "datasets")  # relative, like the default
        mgr.close()
        assert not root.is_absolute()

        readme = (root / "README.md").read_text(encoding="utf-8")
        assert "Sync method" not in readme
        assert "**Frame rate**: `30 fps`" in readme
        code = re.search(r"## Quick Start\n\n```python\n(.*?)```", readme, re.S).group(1)
        assert "LeRobotDataset" in code
        elsewhere = tmp_dir / "elsewhere"
        elsewhere.mkdir()
        # Offline: a root LeRobot can't find must fail here, not hit the Hub.
        proc = subprocess.run([sys.executable, "-c", code], capture_output=True,
                              text=True, timeout=600, cwd=str(elsewhere),
                              env={**os.environ, "HF_HUB_OFFLINE": "1"})
        assert proc.returncode == 0, proc.stderr[-2000:]
        assert "episodes: 2" in proc.stdout, proc.stdout

    def test_publish_card_for_bare_export(self, tmp_dir, sample_bag):
        """Would catch: the HF card for a bare `export --preset lerobot` dir
        (no dataset_config.json, no manifest.json) saying 'Source bags 0 |
        Topics 0 | Data files 0'. Checked against the meta/info.json the
        installed LeRobot actually writes."""
        _lerobot()
        from resurrector.core.publish import build_dataset_card

        out = tmp_dir / "lr"
        BagFrame(sample_bag).export(preset="lerobot", output=str(out))
        ds = _load(out)
        card = build_dataset_card(out, "me/lr")
        assert "| Format | `lerobot` |" in card
        assert "| Episodes | 1 |" in card
        assert f"| Frames | {ds.num_frames} |" in card
        assert "| Frame rate | 30 fps |" in card
        for zero in ("| Source bags | 0 |", "| Topics | 0 |", "| Data files | 0 |"):
            assert zero not in card
        n_state = len(ds.meta.features["observation.state"]["names"])
        assert f"| `observation.state` | float32 | {n_state} |" in card
        assert "| `observation.images.camera_rgb` | video |" in card
