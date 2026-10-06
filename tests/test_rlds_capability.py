"""RLDS export needs tensorflow: the [all-exports] extra, the capability
report, the dashboard preset list, `doctor`, and the export pre-flight must
all agree on that.

Before this was fixed, [all-exports] installed zarr + tensorflow-datasets
but not tensorflow, so the capability and the dashboard's RLDS preset read
"available" and the export then failed, leaving an empty output directory.

Every test fakes the import state through ``sys.modules`` / a meta-path
finder, so the suite gives the same answers whether or not tensorflow is
really installed (CI's all-exports job has it; the default job doesn't).
"""

from __future__ import annotations

import importlib.abc
import importlib.machinery
import itertools
import sys
import tempfile
from pathlib import Path

import pytest

from resurrector.core.bag_frame import BagFrame
from tests.fixtures.generate_test_bags import BagConfig, generate_bag

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"
EXTRA_CMD = "pip install 'rosbag-resurrector[all-exports]'"


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def sample_bag(tmp_dir):
    return generate_bag(tmp_dir / "sample.mcap", BagConfig(duration_sec=1.0))


class _FakeLoader(importlib.abc.Loader):
    def __init__(self, explode: bool):
        self.explode = explode

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        if self.explode:
            raise AssertionError(
                f"{module.__name__} was imported; availability checks must "
                "use importlib.util.find_spec instead"
            )


class _FakeFinder(importlib.abc.MetaPathFinder):
    """Reports chosen top-level modules as installed.

    ``importable`` modules import as empty stubs. ``exploding`` modules are
    findable but raise if anything actually imports them, which is how a
    test proves a check never paid for ``import tensorflow``.
    """

    def __init__(self, importable=(), exploding=()):
        self.loaders = {n: _FakeLoader(False) for n in importable}
        self.loaders.update({n: _FakeLoader(True) for n in exploding})

    def find_spec(self, fullname, path=None, target=None):
        loader = self.loaders.get(fullname)
        if loader is None:
            return None
        return importlib.machinery.ModuleSpec(fullname, loader)


@pytest.fixture
def deps(monkeypatch):
    """``deps(installed=..., missing=..., no_import=...)`` fakes module state."""

    def _set(installed=(), missing=(), no_import=()):
        for name in (*installed, *no_import):
            # setitem records the original entry (or its absence) so a stub
            # imported during the test never leaks into later tests.
            monkeypatch.setitem(sys.modules, name, None)
            del sys.modules[name]
        for name in missing:
            # A None entry makes find_spec return None and import raise.
            monkeypatch.setitem(sys.modules, name, None)
        finder = _FakeFinder(importable=installed, exploding=no_import)
        monkeypatch.setattr(sys, "meta_path", [finder, *sys.meta_path])

    return _set


@pytest.fixture
def tf_platform(monkeypatch):
    """``tf_platform(True/False)`` pins whether tensorflow ships wheels here."""
    from resurrector.core import export

    def _set(supported: bool):
        monkeypatch.setattr(export, "tensorflow_wheels_available", lambda *a, **k: supported)

    return _set


# ---------------------------------------------------------------- packaging

def _all_exports_requirements():
    tomllib = pytest.importorskip("tomllib")
    from packaging.requirements import Requirement
    data = tomllib.loads(PYPROJECT.read_text())
    return [Requirement(r) for r in data["project"]["optional-dependencies"]["all-exports"]]


class TestAllExportsExtra:
    def test_extra_installs_tensorflow(self):
        """Would catch: [all-exports] dropping tensorflow again, which made
        every RLDS export fail on a 'fully installed' extra."""
        names = {r.name for r in _all_exports_requirements()}
        assert "tensorflow" in names, names

    def test_tensorflow_marker_matches_runtime_platform_table(self):
        """The pyproject marker decides where pip installs tensorflow; the
        runtime table decides what `doctor` / the dashboard say. Would catch
        the two drifting apart (e.g. a Python bump in one but not the other,
        which would tell users to install an extra that can't deliver)."""
        from packaging.markers import default_environment
        from resurrector.core.export import tensorflow_wheels_available

        tf_req = next(r for r in _all_exports_requirements() if r.name == "tensorflow")
        assert tf_req.marker is not None, "tensorflow must be marker-gated so installs never break"

        platforms = [
            ("Linux", "x86_64"), ("Linux", "aarch64"), ("Linux", "ppc64le"),
            ("Linux", "armv7l"), ("Darwin", "arm64"), ("Darwin", "x86_64"),
            ("Windows", "AMD64"), ("Windows", "x86_64"), ("Windows", "ARM64"),
            ("FreeBSD", "amd64"),
        ]
        for minor, (system, machine) in itertools.product(range(10, 16), platforms):
            env = default_environment()
            env.update(
                python_version=f"3.{minor}", python_full_version=f"3.{minor}.0",
                platform_system=system, platform_machine=machine,
            )
            assert tf_req.marker.evaluate(env) == tensorflow_wheels_available(
                (3, minor), system, machine,
            ), (minor, system, machine)

    def test_no_prerelease_only_python_is_admitted(self):
        """tensorflow 2.21 (latest stable) ships cp310-cp313. On 3.14 pip
        would silently fall back to a release candidate, so 3.14 stays out
        until a stable cp314 wheel exists."""
        from resurrector.core.export import tensorflow_wheels_available
        assert tensorflow_wheels_available((3, 13), "Linux", "x86_64")
        assert not tensorflow_wheels_available((3, 14), "Linux", "x86_64")
        assert not tensorflow_wheels_available((3, 12), "Darwin", "x86_64")


# ---------------------------------------------------------------- capability

class TestAllExportsCapability:
    def test_unavailable_when_tensorflow_missing(self, deps, tf_platform):
        """Would catch: the capability passing on zarr + tensorflow-datasets
        alone, so the dashboard offers RLDS that then fails to export."""
        from resurrector.core.capabilities import get_capabilities
        deps(installed=("zarr", "tensorflow_datasets"), missing=("tensorflow",))
        tf_platform(True)
        cap = get_capabilities()["all_exports"]
        assert cap.available is False
        assert cap.install_command == EXTRA_CMD

    def test_available_without_importing_tensorflow(self, deps):
        """Would catch: an availability check that imports tensorflow (a
        multi-second import) on every /api/system/capabilities call."""
        from resurrector.core.capabilities import get_capabilities
        deps(installed=("zarr",), no_import=("tensorflow",))
        assert get_capabilities()["all_exports"].available is True
        assert "tensorflow" not in sys.modules

    def test_unsupported_platform_says_why(self, deps, tf_platform):
        """Would catch: telling a Python 3.14 user to install an extra that
        can't install tensorflow on their interpreter."""
        from resurrector.core.capabilities import get_capabilities
        deps(installed=("zarr",), missing=("tensorflow",))
        tf_platform(False)
        cmd = get_capabilities()["all_exports"].install_command
        assert "tensorflow" in cmd and "3.10-3.13" in cmd
        assert "'rosbag-resurrector[all-exports]'" in cmd

    def test_unsupported_platform_still_offers_zarr(self, deps, tf_platform):
        """Would catch: a Zarr-only user on Python 3.14 being told to switch
        Python when the extra would install zarr fine."""
        from resurrector.core.capabilities import get_capabilities
        deps(missing=("zarr", "tensorflow"))
        tf_platform(False)
        cmd = get_capabilities()["all_exports"].install_command
        assert cmd.startswith(EXTRA_CMD) and "Zarr only" in cmd
        assert "3.10-3.13" in cmd


class TestExportPresetsEndpoint:
    def _presets(self):
        from fastapi.testclient import TestClient
        from resurrector.dashboard.api import app
        r = TestClient(app).get("/api/export-presets")
        assert r.status_code == 200
        return {p["name"]: p for p in r.json()}

    def test_rlds_preset_needs_tensorflow_not_just_zarr(self, deps, tf_platform):
        """Would catch: the dashboard enabling the rlds preset because zarr
        is installed (the pre-fix `_extra_available('all-exports')`)."""
        deps(installed=("zarr",), missing=("tensorflow",))
        tf_platform(True)
        presets = self._presets()
        assert presets["rlds"]["available"] is False
        assert "tensorflow" in presets["rlds"]["unavailable_reason"]
        # Zarr itself is fine, so the zarr preset stays usable.
        assert presets["multimodal"]["available"] is True
        assert presets["multimodal"]["unavailable_reason"] is None

    def test_rlds_preset_available_without_importing_tensorflow(self, deps):
        deps(installed=("zarr",), no_import=("tensorflow",))
        assert self._presets()["rlds"]["available"] is True
        assert "tensorflow" not in sys.modules


# ---------------------------------------------------------------- pre-flight

class TestExportPreflight:
    @pytest.mark.parametrize("sync", [True, False])
    def test_rlds_without_tensorflow_creates_nothing(
        self, deps, tf_platform, tmp_dir, sample_bag, sync,
    ):
        """Would catch: Exporter.export mkdir-ing the output before the RLDS
        writer discovers tensorflow is missing (left an empty directory)."""
        deps(missing=("tensorflow",))
        tf_platform(True)
        out = tmp_dir / "rlds_out"
        with pytest.raises(ImportError) as ei:
            BagFrame(sample_bag).export(
                topics=["/imu/data", "/joint_states"], format="rlds",
                output=str(out), sync=sync,
            )
        assert EXTRA_CMD in str(ei.value)
        assert not out.exists()

    def test_rlds_reason_on_unsupported_python(self, deps, tf_platform, tmp_dir, sample_bag):
        deps(missing=("tensorflow",))
        tf_platform(False)
        v = sys.version_info
        with pytest.raises(ImportError) as ei:
            BagFrame(sample_bag).export(
                topics=["/imu/data"], format="rlds", output=str(tmp_dir / "o"),
            )
        msg = str(ei.value)
        assert f"Python {v[0]}.{v[1]}" in msg and "3.10-3.13" in msg

    def test_zarr_without_zarr_creates_nothing(self, deps, tmp_dir, sample_bag):
        deps(missing=("zarr",))
        out = tmp_dir / "zarr_out"
        with pytest.raises(ImportError, match=r"\[all-exports\]"):
            BagFrame(sample_bag).export(topics=["/imu/data"], format="zarr", output=str(out))
        assert not out.exists()

    def test_split_export_creates_nothing(self, deps, tf_platform, tmp_dir, sample_bag):
        """split_export mkdirs the parent before each per-split export."""
        deps(missing=("tensorflow",))
        tf_platform(True)
        out = tmp_dir / "splits"
        with pytest.raises(ImportError):
            BagFrame(sample_bag).export(
                topics=["/imu/data"], format="rlds", output=str(out),
                split={"train": 0.5, "val": 0.5},
            )
        assert not out.exists()

    def test_dataset_version_export_creates_nothing(self, deps, tf_platform, tmp_dir, sample_bag):
        from resurrector.core.dataset import BagRef, DatasetManager
        deps(missing=("tensorflow",))
        tf_platform(True)
        mgr = DatasetManager(tmp_dir / "ds.db")
        try:
            mgr.create("rl")
            mgr.create_version(
                dataset_name="rl", version="1.0",
                bag_refs=[BagRef(path=str(sample_bag))],
                topics=["/imu/data"], export_format="rlds",
            )
            with pytest.raises(ImportError):
                mgr.export_version("rl", "1.0", str(tmp_dir / "datasets"))
        finally:
            mgr.close()
        assert not (tmp_dir / "datasets").exists()


# ---------------------------------------------------------------- doctor + CLI

class TestDoctorRldsRow:
    def test_fix_points_at_extra(self, deps, tf_platform):
        """Would catch: the 0.8.4 hint `pip install tensorflow`, which skips
        the extra (and its pins) that now carries tensorflow."""
        from resurrector.cli import doctor
        deps(missing=("tensorflow",))
        tf_platform(True)
        row = doctor._check_rlds()
        assert row.name == "RLDS export (tensorflow)"
        assert row.status == "warn" and row.tier == "optional"
        assert row.fix_hint == EXTRA_CMD

    def test_unsupported_platform_explains(self, deps, tf_platform):
        from resurrector.cli import doctor
        deps(missing=("tensorflow",))
        tf_platform(False)
        row = doctor._check_rlds()
        v = sys.version_info
        assert f"Python {v[0]}.{v[1]}" in row.detail
        assert "3.10-3.13" in row.fix_hint and "'rosbag-resurrector[all-exports]'" in row.fix_hint

    def test_pass_without_importing_tensorflow(self, deps):
        from resurrector.cli import doctor
        deps(no_import=("tensorflow",))
        assert doctor._check_rlds().status == "pass"
        assert "tensorflow" not in sys.modules


class TestCli:
    def test_list_presets_footer_points_rlds_at_extra(self):
        """Would catch: the footer still saying `pip install tensorflow`."""
        from typer.testing import CliRunner
        from resurrector.cli.main import app
        result = CliRunner().invoke(app, ["export", "--list-presets"], env={"COLUMNS": "200"})
        assert result.exit_code == 0, result.output
        out = " ".join(result.output.split())
        assert "pip install tensorflow" not in out
        assert "rosbag-resurrector[all-exports]" in out and "3.10-3.13" in out

    def test_export_without_tensorflow_prints_extra_and_writes_nothing(
        self, deps, tf_platform, tmp_dir, sample_bag,
    ):
        from typer.testing import CliRunner
        from resurrector.cli.main import app
        deps(missing=("tensorflow",))
        tf_platform(True)
        out = tmp_dir / "cli_out"
        result = CliRunner().invoke(
            app, ["export", str(sample_bag), "-f", "rlds", "-t", "/imu/data", "-o", str(out)],
            env={"COLUMNS": "200"},
        )
        assert result.exit_code == 1
        assert "rosbag-resurrector[all-exports]" in " ".join(result.output.split())
        assert not out.exists()
