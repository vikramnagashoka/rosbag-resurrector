"""RLDS export needs tensorflow: the [all-exports] extra, the capability
report, the dashboard preset list, `doctor`, and the export pre-flight must
all agree on that. LeRobot export gets the same pre-flight and preset check
for the [lerobot] extra.

Before this was fixed, [all-exports] installed zarr + tensorflow-datasets
but not tensorflow, so the capability and the dashboard's RLDS preset read
"available" and the export then failed, leaving an empty output directory.

Every test fakes the import state through ``sys.modules`` / a meta-path
finder, and pins the interpreter the tensorflow platform table sees
(``export._running_platform``) wherever wording depends on it, so the suite
gives the same answers whether or not tensorflow is really installed (CI's
all-exports job has it; the default job doesn't) and on any host.
"""

from __future__ import annotations

import importlib.abc
import importlib.machinery
import itertools
import re
import shlex
import sys
import tempfile
from pathlib import Path

import pytest

from resurrector.core.bag_frame import BagFrame
from tests.fixtures.generate_test_bags import BagConfig, generate_bag

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"
EXTRA_CMD = "pip install 'rosbag-resurrector[all-exports]'"
TF_WHERE = (
    "Python 3.10-3.13 on x86_64/aarch64 Linux, Apple-silicon macOS or x64 "
    "Windows, or Python 3.10-3.12 on Intel macOS"
)
CAP_BASE = "Zarr and RLDS (TFRecord) export formats"
LEROBOT_HINT = (
    "LeRobot export needs the [lerobot] extra (Python 3.12+): "
    "pip install 'rosbag-resurrector[lerobot]'"
)
BROKEN_TF = "libtensorflow_framework.2.dylib: cannot open shared object file"


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def sample_bag(tmp_dir):
    return generate_bag(tmp_dir / "sample.mcap", BagConfig(duration_sec=1.0))


class _FakeLoader(importlib.abc.Loader):
    def __init__(self, mode: str, exploded: list[str]):
        self.mode = mode
        self.exploded = exploded

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        if self.mode == "explode":
            self.exploded.append(module.__name__)
            raise AssertionError(
                f"{module.__name__} was imported; availability checks must "
                "use importlib.util.find_spec instead"
            )
        if self.mode == "broken":
            raise ImportError(BROKEN_TF)


class _FakeFinder(importlib.abc.MetaPathFinder):
    """Reports chosen top-level modules as installed.

    ``importable`` modules import as empty stubs. ``exploding`` modules are
    findable but raise if anything actually imports them, which is how a
    test proves a check never paid for ``import tensorflow``. ``broken``
    modules are findable but raise ImportError on import, like a
    tensorflow whose native library is missing. ``exploded`` lists every
    attempt to import an exploding module, so a check that swallows the
    error (``except Exception``) is still caught.
    """

    def __init__(self, importable=(), exploding=(), broken=()):
        self.exploded: list[str] = []
        self.loaders = {n: _FakeLoader("ok", self.exploded) for n in importable}
        self.loaders.update({n: _FakeLoader("explode", self.exploded) for n in exploding})
        self.loaders.update({n: _FakeLoader("broken", self.exploded) for n in broken})

    def find_spec(self, fullname, path=None, target=None):
        loader = self.loaders.get(fullname)
        if loader is None:
            return None
        return importlib.machinery.ModuleSpec(fullname, loader)


@pytest.fixture
def deps(monkeypatch):
    """``deps(installed=..., missing=..., no_import=..., broken=...)`` fakes
    module state and returns the :class:`_FakeFinder`."""

    def _set(installed=(), missing=(), no_import=(), broken=()):
        for name in (*installed, *no_import, *broken):
            # setitem records the original entry (or its absence) so a stub
            # imported during the test never leaks into later tests.
            monkeypatch.setitem(sys.modules, name, None)
            del sys.modules[name]
        for name in missing:
            # A None entry makes find_spec return None and import raise.
            monkeypatch.setitem(sys.modules, name, None)
        finder = _FakeFinder(importable=installed, exploding=no_import, broken=broken)
        monkeypatch.setattr(sys, "meta_path", [finder, *sys.meta_path])
        return finder

    return _set


@pytest.fixture
def platform_as(monkeypatch):
    """``platform_as((3, 12), "Darwin", "x86_64")`` pins the interpreter the
    tensorflow platform table (and every message built from it) sees."""
    from resurrector.core import export

    def _set(python, system, machine):
        monkeypatch.setattr(export, "_running_platform", lambda: (python, system, machine))

    return _set


SUPPORTED = ((3, 12), "Linux", "x86_64")
UNSUPPORTED = ((3, 14), "Linux", "x86_64")  # no stable cp314 tensorflow wheel


@pytest.fixture
def tf_platform(platform_as):
    """``tf_platform(True/False)``: a platform where [all-exports] does /
    doesn't install tensorflow (Linux x86_64 on Python 3.12 / 3.14)."""

    def _set(supported: bool):
        platform_as(*(SUPPORTED if supported else UNSUPPORTED))

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

    def test_extra_lists_only_packages_the_code_imports(self):
        """Would catch: a dependency nothing imports riding along in the
        extra (tensorflow-datasets did: a large install, with markers to
        keep in sync, that the RLDS writer never used)."""
        sources = "\n".join(
            p.read_text() for p in (PYPROJECT.parent / "resurrector").rglob("*.py")
        )
        for req in _all_exports_requirements():
            module = req.name.replace("-", "_")
            assert re.search(rf"^\s*(import|from) {module}\b", sources, re.M), (
                f"[all-exports] installs {req.name}, but nothing under resurrector/ imports it"
            )

    def test_tensorflow_marker_matches_runtime_platform_table(self):
        """The pyproject markers decide where pip installs tensorflow; the
        runtime table decides what `doctor` / the dashboard say. Would catch
        the two drifting apart (e.g. a Python bump in one but not the other,
        which would tell users to install an extra that can't deliver)."""
        from packaging.markers import default_environment
        from resurrector.core.export import tensorflow_wheels_available

        tf_reqs = [r for r in _all_exports_requirements() if r.name == "tensorflow"]
        assert all(r.marker is not None for r in tf_reqs), (
            "tensorflow must be marker-gated so installs don't break where it has no wheel"
        )

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
            expected = tensorflow_wheels_available((3, minor), system, machine)
            applicable = [r for r in tf_reqs if r.marker.evaluate(env)]
            assert bool(applicable) == expected, (minor, system, machine)
            assert len(applicable) <= 1, (minor, system, machine)  # never two ranges at once

    def test_no_prerelease_only_python_is_admitted(self):
        """tensorflow 2.21 (latest stable) ships cp310-cp313. On 3.14 pip
        would silently fall back to a release candidate, so 3.14 stays out
        until a stable cp314 wheel exists."""
        from resurrector.core.export import tensorflow_wheels_available
        assert tensorflow_wheels_available((3, 13), "Linux", "x86_64")
        assert not tensorflow_wheels_available((3, 14), "Linux", "x86_64")

    def test_intel_macos_admitted_through_python_312(self):
        """tensorflow 2.16.2, its last Intel-macOS release, ships
        macosx_10_15_x86_64 wheels for cp310-cp312 and resolves with this
        package's numpy>=1.24 (it pins numpy<2). Would catch: Intel macOS
        being excluded (and told tensorflow has no release for it) when the
        extra can install it."""
        from resurrector.core.export import tensorflow_wheels_available
        assert tensorflow_wheels_available((3, 10), "Darwin", "x86_64")
        assert tensorflow_wheels_available((3, 12), "Darwin", "x86_64")
        assert not tensorflow_wheels_available((3, 13), "Darwin", "x86_64")

    def test_intel_macos_requirement_pins_the_last_intel_release(self):
        """Wheels for Intel macOS stop at 2.16, so its line must admit 2.16
        and nothing newer (pip would otherwise backtrack through every later
        release looking for an Intel wheel)."""
        from packaging.markers import default_environment
        env = default_environment()
        env.update(
            python_version="3.12", python_full_version="3.12.0",
            platform_system="Darwin", platform_machine="x86_64",
        )
        (req,) = [
            r for r in _all_exports_requirements()
            if r.name == "tensorflow" and r.marker.evaluate(env)
        ]
        assert req.specifier.contains("2.16.2")
        assert not req.specifier.contains("2.17.0")


_PY_RANGE = re.compile(r"\b(3\.\d+)-(3\.\d+)\b")


def _py_ranges(text: str) -> set[str]:
    return {f"{lo}-{hi}" for lo, hi in _PY_RANGE.findall(text)}


def _table_ranges() -> set[str]:
    """``"3.10-3.13"``-style ranges the tensorflow platform table implies."""
    from resurrector.core.export import TENSORFLOW_MIN_PYTHON, TENSORFLOW_PLATFORMS
    lo = "%d.%d" % TENSORFLOW_MIN_PYTHON
    return {f"{lo}-{major}.{minor}" for major, minor in TENSORFLOW_PLATFORMS.values()}


class TestTensorflowWhere:
    """TENSORFLOW_WHERE, the README and the CLI tell users which Pythons get
    tensorflow. Before this was fixed they were hand-written copies of
    TENSORFLOW_PLATFORMS that only a reader would notice going stale."""

    def test_every_max_python_is_in_where(self):
        """Would catch: a hand-written TENSORFLOW_WHERE left quoting the old
        Pythons after a table change."""
        from resurrector.core.export import TENSORFLOW_WHERE
        assert _table_ranges()
        assert _py_ranges(TENSORFLOW_WHERE) == _table_ranges()

    def test_wording_for_the_current_table(self):
        from resurrector.core.export import TENSORFLOW_WHERE
        assert TENSORFLOW_WHERE == TF_WHERE

    def test_where_follows_a_changed_table(self):
        """Would catch: a hand-written TENSORFLOW_WHERE that keeps saying
        3.13 after the table admits 3.14."""
        from resurrector.core.export import describe_tensorflow_platforms
        table = {
            ("Linux", "x86_64"): (3, 14),
            ("Windows", "AMD64"): (3, 14),
            ("Windows", "x86_64"): (3, 14),
            ("Darwin", "arm64"): (3, 13),
            ("Darwin", "x86_64"): (3, 12),
        }
        assert describe_tensorflow_platforms(table) == (
            "Python 3.10-3.14 on x86_64 Linux or x64 Windows, "
            "or Python 3.10-3.13 on Apple-silicon macOS, "
            "or Python 3.10-3.12 on Intel macOS"
        )

    def test_floor_is_the_package_floor(self):
        tomllib = pytest.importorskip("tomllib")
        from resurrector.core.export import TENSORFLOW_MIN_PYTHON
        requires = tomllib.loads(PYPROJECT.read_text())["project"]["requires-python"]
        assert requires == ">=%d.%d" % TENSORFLOW_MIN_PYTHON

    def test_readme_install_text_matches_the_table(self):
        """Would catch: a table bump (or drop) that leaves the README's
        install line or its RLDS paragraph quoting the old Pythons."""
        readme = (PYPROJECT.parent / "README.md").read_text()
        lines = [ln for ln in readme.splitlines() if "tensorflow" in ln and _py_ranges(ln)]
        assert len(lines) >= 2, "expected the install line and the RLDS formats paragraph"
        assert any("pip install 'rosbag-resurrector[all-exports]'" in ln for ln in lines)
        assert any("RLDS writes TFRecords with tensorflow" in ln for ln in lines)
        for ln in lines:
            assert _py_ranges(ln) == _table_ranges(), ln


class TestPlatformMessages:
    """Exact wording, per platform, of what `doctor`, the export pre-flight,
    the CLI and /api/export-presets say about a missing tensorflow."""

    @pytest.mark.parametrize("python", [(3, 10), (3, 12)])
    def test_intel_macos_is_pointed_at_the_extra(self, deps, platform_as, python):
        """Would catch: every surface telling an Intel-macOS user on Python
        3.10-3.12 that tensorflow 'has no stable release' there, when
        tensorflow 2.16 ships Intel wheels and the extra installs it."""
        from resurrector.cli import doctor
        from resurrector.core import export
        deps(missing=("tensorflow",))
        platform_as(python, "Darwin", "x86_64")
        assert export.tensorflow_missing_detail() == "tensorflow isn't installed"
        assert export.tensorflow_install_hint() == EXTRA_CMD
        assert export.export_dependency_problem("rlds") == (
            f"RLDS export needs tensorflow, which isn't installed. Install with: {EXTRA_CMD}"
        )
        row = doctor._check_rlds()
        assert (row.status, row.detail, row.fix_hint) == (
            "warn", "tensorflow isn't installed", EXTRA_CMD,
        )

    @pytest.mark.parametrize("plat, label", [
        (((3, 13), "Darwin", "x86_64"), "Python 3.13 on Intel macOS"),
        (((3, 14), "Darwin", "arm64"), "Python 3.14 on Apple-silicon macOS"),
        (((3, 14), "Linux", "x86_64"), "Python 3.14 on Linux x86_64"),
        (((3, 12), "Windows", "ARM64"), "Python 3.12 on Windows ARM64"),
    ])
    def test_unsupported_platform_wording(self, deps, platform_as, plat, label):
        from resurrector.cli import doctor
        from resurrector.core import export
        deps(missing=("tensorflow",))
        platform_as(*plat)
        detail = f"tensorflow publishes no stable wheel for {label}"
        hint = f"Use {TF_WHERE}, then: {EXTRA_CMD}"
        assert export.tensorflow_missing_detail() == detail
        assert export.tensorflow_install_hint() == hint
        assert export.export_dependency_problem("rlds") == (
            f"RLDS export needs tensorflow, which publishes no stable wheel for {label}. {hint}"
        )
        row = doctor._check_rlds()
        assert (row.detail, row.fix_hint) == (detail, hint)


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

    def test_unsupported_platform_explains_in_description(self, deps, tf_platform):
        """Would catch: telling a Python 3.14 user only to install an extra
        that can't install tensorflow on their interpreter. The reason goes
        in ``description``; ``install_command`` stays the pip command, which
        still delivers Zarr."""
        from resurrector.core.capabilities import get_capabilities
        deps(missing=("zarr", "tensorflow"))
        tf_platform(False)
        cap = get_capabilities()["all_exports"]
        assert cap.install_command == EXTRA_CMD
        assert cap.description == (
            f"{CAP_BASE}. On this interpreter the extra installs Zarr only: "
            "tensorflow publishes no stable wheel for Python 3.14 on Linux x86_64. "
            f"For RLDS, use {TF_WHERE}."
        )

    @pytest.mark.parametrize("tf", ["installed", "missing"])
    @pytest.mark.parametrize("zarr", ["installed", "missing"])
    @pytest.mark.parametrize("plat", [
        ((3, 12), "Linux", "x86_64"),
        ((3, 12), "Darwin", "x86_64"),
        ((3, 13), "Darwin", "x86_64"),
        ((3, 13), "Darwin", "arm64"),
        ((3, 14), "Linux", "x86_64"),
        ((3, 12), "Windows", "ARM64"),
    ], ids=lambda p: f"py{p[0][0]}{p[0][1]}-{p[1]}-{p[2]}")
    def test_install_command_is_always_a_runnable_command(
        self, deps, platform_as, plat, zarr, tf,
    ):
        """Would catch: prose such as "(Zarr only on this platform). RLDS
        export needs ..." in install_command. The dashboard renders it in a
        copy block, so pasting it into a shell was a syntax error."""
        from resurrector.core.capabilities import get_capabilities
        from resurrector.core.export import tensorflow_wheels_available
        deps(
            installed=("zarr",) if zarr == "installed" else (),
            no_import=("tensorflow",) if tf == "installed" else (),
            missing=tuple(
                name for name, state in (("zarr", zarr), ("tensorflow", tf))
                if state == "missing"
            ),
        )
        platform_as(*plat)
        cap = get_capabilities()["all_exports"]
        assert cap.install_command == EXTRA_CMD
        assert shlex.split(cap.install_command) == [
            "pip", "install", "rosbag-resurrector[all-exports]",
        ]
        assert not any(c in cap.install_command for c in "()\n.:")
        assert cap.available is (zarr == "installed" and tf == "installed")
        if tf == "missing" and not tensorflow_wheels_available(*plat):
            assert cap.description.startswith(f"{CAP_BASE}. On this interpreter")
        else:
            assert cap.description == CAP_BASE


class TestLeRobotCapability:
    def test_available_without_importing_lerobot(self, deps, platform_as):
        """Would catch: /api/system/capabilities importing LeRobot's writer
        (and torch) on every call, and disagreeing with the preset list."""
        from resurrector.core.capabilities import get_capabilities
        finder = deps(no_import=("lerobot",))
        platform_as((3, 12), "Linux", "x86_64")
        assert get_capabilities()["lerobot"].available is True
        assert finder.exploded == []

    def test_unavailable_below_python_312(self, deps, platform_as):
        from resurrector.core.capabilities import get_capabilities
        finder = deps(no_import=("lerobot",))
        platform_as((3, 11), "Linux", "x86_64")
        assert get_capabilities()["lerobot"].available is False
        assert finder.exploded == []


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

    @pytest.mark.parametrize("supported", [True, False])
    def test_reason_tells_the_dashboard_whether_pip_fixes_it(
        self, deps, tf_platform, supported,
    ):
        """Would catch: rewording the rlds reason so the export dialogs
        (installWontFix in dashboard/app/src/exportOptions.ts) can no
        longer tell "run the extra's command" from "switch interpreter,
        then run it", and go back to offering pip on Python 3.14."""
        from resurrector.core.capabilities import get_capabilities
        deps(installed=("zarr",), missing=("tensorflow",))
        tf_platform(supported)
        reason = self._presets()["rlds"]["unavailable_reason"]
        command = get_capabilities()["all_exports"].install_command
        assert command in reason
        assert ("then: " in reason) is (not supported)

    def test_rlds_preset_available_without_importing_tensorflow(self, deps):
        deps(installed=("zarr",), no_import=("tensorflow",))
        assert self._presets()["rlds"]["available"] is True
        assert "tensorflow" not in sys.modules

    def test_lerobot_preset_available_without_importing_lerobot(self, deps, platform_as):
        """Would catch: the preset list importing LeRobot's writer (and so
        torch) on every request to decide availability."""
        finder = deps(no_import=("lerobot",))
        platform_as((3, 12), "Linux", "x86_64")
        preset = self._presets()["lerobot"]
        assert (preset["available"], preset["unavailable_reason"]) == (True, None)
        assert finder.exploded == []

    @pytest.mark.parametrize("python, lerobot", [
        ((3, 12), "missing"),
        # An older LeRobot can be installed below 3.12, but not one we can drive.
        ((3, 11), "installed"),
    ])
    def test_lerobot_preset_unavailable_names_the_extra(
        self, deps, platform_as, python, lerobot,
    ):
        finder = deps(**(
            {"missing": ("lerobot",)} if lerobot == "missing" else {"no_import": ("lerobot",)}
        ))
        platform_as(python, "Linux", "x86_64")
        preset = self._presets()["lerobot"]
        assert (preset["available"], preset["unavailable_reason"]) == (False, LEROBOT_HINT)
        assert finder.exploded == []


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
        with pytest.raises(ImportError) as ei:
            BagFrame(sample_bag).export(
                topics=["/imu/data"], format="rlds", output=str(tmp_dir / "o"),
            )
        msg = str(ei.value)
        assert "Python 3.14 on Linux x86_64" in msg and TF_WHERE in msg

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


class TestLeRobotPreflight:
    """[lerobot] gets the same pre-flight as zarr and rlds. Before it did,
    ``require_export_dependencies("lerobot")`` was a no-op, so a split
    export or a dataset version in LeRobot format created its output
    directory and then raised, leaving an empty directory behind."""

    @pytest.fixture(autouse=True)
    def _python_312(self, platform_as):
        platform_as((3, 12), "Linux", "x86_64")

    def test_problem_is_the_install_hint(self, deps):
        from resurrector.core import export
        deps(missing=("lerobot",))
        assert export.export_dependency_problem("lerobot") == LEROBOT_HINT

    def test_problem_check_never_imports_lerobot(self, deps):
        from resurrector.core import export
        finder = deps(no_import=("lerobot",))
        assert export.export_dependency_problem("lerobot") is None
        assert finder.exploded == []

    def test_python_floor(self, deps, platform_as):
        from resurrector.core import export
        finder = deps(no_import=("lerobot",))
        platform_as((3, 11), "Linux", "x86_64")
        assert export.export_dependency_problem("lerobot") == LEROBOT_HINT
        assert finder.exploded == []

    def test_exporter_creates_nothing(self, deps, tmp_dir, sample_bag):
        from resurrector.core.export import Exporter
        deps(missing=("lerobot",))
        out = tmp_dir / "lr_out"
        with pytest.raises(ImportError, match=r"\[lerobot\]"):
            Exporter().export(BagFrame(sample_bag), ["/imu/data"], format="lerobot",
                              output_dir=str(out))
        assert not out.exists()

    def test_split_export_creates_nothing(self, deps, tmp_dir, sample_bag):
        """Would catch: split_export mkdir-ing ``<out>/train`` before the
        LeRobot writer finds LeRobot missing."""
        deps(missing=("lerobot",))
        out = tmp_dir / "splits"
        with pytest.raises(ImportError, match=r"\[lerobot\]"):
            BagFrame(sample_bag).export(
                topics=["/imu/data"], format="lerobot", output=str(out),
                split={"train": 0.5, "val": 0.5},
            )
        assert not out.exists()

    def test_dataset_version_export_creates_nothing(self, deps, tmp_dir, sample_bag):
        """Would catch: export_version creating ``datasets/<name>/<ver>``
        before the LeRobot writer finds LeRobot missing."""
        from resurrector.core.dataset import BagRef, DatasetManager
        deps(missing=("lerobot",))
        mgr = DatasetManager(tmp_dir / "ds.db")
        try:
            mgr.create("lr")
            mgr.create_version(
                dataset_name="lr", version="1.0",
                bag_refs=[BagRef(path=str(sample_bag))],
                topics=["/imu/data"], export_format="lerobot",
            )
            with pytest.raises(ImportError, match=r"\[lerobot\]"):
                mgr.export_version("lr", "1.0", str(tmp_dir / "datasets"))
        finally:
            mgr.close()
        assert not (tmp_dir / "datasets").exists()

    def test_unimportable_writer_caught_before_anything_is_written(
        self, deps, monkeypatch, tmp_dir, sample_bag,
    ):
        """find_spec sees lerobot but its dataset writer won't import (e.g.
        lerobot installed without its [dataset] extra). The dashboard's
        presence check can't tell; the export-time check imports the writer,
        so the split still leaves nothing behind."""
        from resurrector.core.export import require_export_dependencies
        deps(broken=("lerobot",))
        # Independent of whether a real LeRobot was imported earlier.
        monkeypatch.setitem(sys.modules, "lerobot.datasets.lerobot_dataset", None)
        with pytest.raises(ImportError) as ei:
            require_export_dependencies("lerobot")
        assert str(ei.value) == LEROBOT_HINT
        assert isinstance(ei.value.__cause__, ImportError)
        out = tmp_dir / "splits"
        with pytest.raises(ImportError, match=r"\[lerobot\]"):
            BagFrame(sample_bag).export(
                topics=["/imu/data"], format="lerobot", output=str(out),
                split={"train": 0.5, "val": 0.5},
            )
        assert not out.exists()

    def test_min_python_matches_the_extra_marker(self):
        """The [lerobot] extra installs LeRobot only from LEROBOT_MIN_PYTHON
        up; would catch the marker and the runtime floor drifting apart."""
        tomllib = pytest.importorskip("tomllib")
        from packaging.markers import default_environment
        from packaging.requirements import Requirement
        from resurrector.core.lerobot_export import LEROBOT_MIN_PYTHON
        extra = tomllib.loads(PYPROJECT.read_text())["project"]["optional-dependencies"]["lerobot"]
        (req,) = [r for r in map(Requirement, extra) if r.name == "lerobot"]
        for minor in range(10, 16):
            env = default_environment()
            env.update(python_version=f"3.{minor}", python_full_version=f"3.{minor}.0")
            assert req.marker.evaluate(env) == ((3, minor) >= LEROBOT_MIN_PYTHON), minor


class TestRldsWriterImportError:
    """``_stream_rlds`` raises its own ImportError. Exporter.export's
    pre-flight normally fires first, so without calling the writer directly
    a regression in its message survives every other test."""

    def test_names_the_extra(self, deps, tf_platform, tmp_dir):
        """Would catch: the writer reverting to the 0.8.4 text ("the
        [all-exports] extra does not install [tensorflow]. Install with: pip
        install tensorflow"), wrong now that the extra carries it."""
        from resurrector.core.export import _stream_rlds
        deps(missing=("tensorflow",))
        tf_platform(True)
        out = tmp_dir / "rlds_out"
        with pytest.raises(ImportError) as ei:
            _stream_rlds(iter(()), out, "episode")
        assert str(ei.value) == (
            f"RLDS export needs tensorflow, which isn't installed. Install with: {EXTRA_CMD}"
        )
        assert not out.exists()

    def test_broken_install_reports_the_import_failure(self, deps, tmp_dir):
        """find_spec sees tensorflow, so there's no packaging reason to give;
        the writer must pass the real import error through instead of None."""
        from resurrector.core.export import _stream_rlds
        deps(broken=("tensorflow",))
        out = tmp_dir / "rlds_out"
        with pytest.raises(ImportError) as ei:
            _stream_rlds(iter(()), out, "episode")
        assert str(ei.value) == f"RLDS export needs tensorflow, which failed to import: {BROKEN_TF}"
        assert str(ei.value.__cause__) == BROKEN_TF
        assert not out.exists()


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
        assert "Python 3.14 on Linux x86_64" in row.detail
        assert TF_WHERE in row.fix_hint and row.fix_hint.endswith(EXTRA_CMD)

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

    def test_cli_strings_quote_tensorflow_where(self):
        """Would catch: the --format help, the export command's help and the
        --list-presets footer hard-coding Python ranges instead of using
        TENSORFLOW_WHERE (they drift when the table changes)."""
        import typer
        from typer.testing import CliRunner
        from resurrector.cli.main import app
        from resurrector.core.export import TENSORFLOW_WHERE

        cmd = typer.main.get_command(app).commands["export"]
        format_help = next(p.help for p in cmd.params if p.name == "format")
        command_help = " ".join(f"{cmd.help or ''} {cmd.epilog or ''}".split())
        footer = CliRunner().invoke(app, ["export", "--list-presets"], env={"COLUMNS": "200"})
        for where, text in (
            ("--format help", format_help),
            ("command help", command_help),
            ("--list-presets footer", " ".join(footer.output.split())),
        ):
            assert TENSORFLOW_WHERE in text, where

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
