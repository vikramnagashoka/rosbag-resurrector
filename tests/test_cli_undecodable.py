"""CLI commands on a bag with a topic Resurrector can't decode.

With a terminal the user chooses: load decoders from a path, go on
without the topic, or stop. Without one the command exits 1 naming
--decoders and --skip-undecodable, so it never waits for input.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from resurrector.cli import undecodable
from resurrector.cli.main import app
from resurrector.ingest import custom_decoders
from tests.fixtures.undecodable_bag import write_msg_folder, write_undecodable_bag

runner = CliRunner()


@pytest.fixture(autouse=True)
def _clean_decoders(monkeypatch, tmp_path):
    monkeypatch.delenv(custom_decoders.ENV_VAR, raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    custom_decoders._reset_for_tests()
    yield
    custom_decoders._reset_for_tests()


@pytest.fixture
def bag(tmp_path):
    return write_undecodable_bag(tmp_path / "gizmo.mcap", n=20)


@pytest.fixture
def msg_dir(tmp_path):
    return write_msg_folder(tmp_path / "ws")


def _files(path):
    return sorted(p.name for p in path.iterdir()) if path.exists() else []


class TestExportWithoutATerminal:
    def test_stops_and_names_the_flags(self, bag, tmp_path):
        out = tmp_path / "out"
        r = runner.invoke(app, ["export", str(bag), "-o", str(out)])
        assert r.exit_code == 1, r.output
        assert "/gizmo" in r.output and "acme_msgs/msg/Gizmo" in r.output
        assert "--decoders PATH" in r.output and "--skip-undecodable" in r.output
        assert _files(out) == []

    def test_skip_undecodable(self, bag, tmp_path):
        out = tmp_path / "out"
        r = runner.invoke(app, ["export", str(bag), "-o", str(out), "--skip-undecodable"])
        assert r.exit_code == 0, r.output
        assert "Left out /gizmo" in r.output
        assert _files(out) == ["widget.parquet"]

    def test_skip_with_nothing_left(self, bag, tmp_path):
        r = runner.invoke(app, ["export", str(bag), "-t", "/gizmo", "-o", str(tmp_path / "o"),
                                "--skip-undecodable"])
        assert r.exit_code == 1
        assert "nothing to export" in r.output

    def test_decoders_flag(self, bag, msg_dir, tmp_path):
        out = tmp_path / "out"
        r = runner.invoke(app, ["export", str(bag), "-o", str(out), "--decoders", str(msg_dir)])
        assert r.exit_code == 0, r.output
        assert "Loaded 1 message type" in r.output
        assert _files(out) == ["gizmo.parquet", "widget.parquet"]

    def test_bad_decoders_path(self, bag, tmp_path):
        r = runner.invoke(app, ["export", str(bag), "-o", str(tmp_path / "o"),
                                "--decoders", str(tmp_path / "nowhere")])
        assert r.exit_code == 1
        assert "--decoders" in r.output and "No such file or folder" in r.output


class TestExportPrompt:
    @pytest.fixture(autouse=True)
    def _terminal(self, monkeypatch):
        monkeypatch.setattr(undecodable, "_interactive", lambda: True)

    def test_go_on_without_it(self, bag, tmp_path):
        out = tmp_path / "out"
        r = runner.invoke(app, ["export", str(bag), "-o", str(out)], input="2\n")
        assert r.exit_code == 0, r.output
        assert "Go on without it" in r.output
        assert _files(out) == ["widget.parquet"]

    def test_load_from_a_path(self, bag, msg_dir, tmp_path):
        out = tmp_path / "out"
        r = runner.invoke(app, ["export", str(bag), "-o", str(out)], input=f"1\n{msg_dir}\n")
        assert r.exit_code == 0, r.output
        assert f"Next time pass --decoders {msg_dir}" in r.output.replace("\n", "")
        assert _files(out) == ["gizmo.parquet", "widget.parquet"]

    def test_a_bad_path_asks_again(self, bag, msg_dir, tmp_path):
        out = tmp_path / "out"
        r = runner.invoke(app, ["export", str(bag), "-o", str(out)],
                          input=f"1\n{tmp_path / 'nowhere'}\n{msg_dir}\n")
        assert r.exit_code == 0, r.output
        assert "No such file or folder" in r.output
        assert _files(out) == ["gizmo.parquet", "widget.parquet"]

    def test_a_path_that_leaves_it_undecodable_asks_again(self, bag, tmp_path):
        incomplete = tmp_path / "ws2" / "acme_msgs" / "msg"
        incomplete.mkdir(parents=True)
        (incomplete / "Gizmo.msg").write_text("acme_msgs/Part part\n")
        r = runner.invoke(app, ["export", str(bag), "-o", str(tmp_path / "o")],
                          input=f"1\n{tmp_path / 'ws2'}\n3\n")
        assert r.exit_code == 1
        # Asked again, with why it still can't be decoded.
        assert "definition of acme_msgs/Part is missing" in " ".join(r.output.split())
        assert r.output.count("Choice [1/2/3]") == 2

    def test_stop(self, bag, tmp_path):
        out = tmp_path / "out"
        r = runner.invoke(app, ["export", str(bag), "-o", str(out)], input="3\n")
        assert r.exit_code == 1
        assert _files(out) == []


def test_env_var(bag, msg_dir, tmp_path, monkeypatch):
    monkeypatch.setenv(custom_decoders.ENV_VAR, str(msg_dir))
    out = tmp_path / "out"
    r = runner.invoke(app, ["export", str(bag), "-o", str(out)])
    assert r.exit_code == 0, r.output
    assert _files(out) == ["gizmo.parquet", "widget.parquet"]


@pytest.mark.parametrize("command", ["info", "quicklook"])
def test_info_and_quicklook_name_the_topic(bag, command):
    r = runner.invoke(app, [command, str(bag)])
    assert r.exit_code == 0, r.output
    text = " ".join(r.output.split())
    assert "1 topic can't be decoded" in text
    assert "/gizmo acme_msgs/msg/Gizmo: the bag has only a ros2idl definition of it" in text


def test_info_says_nothing_when_all_decode(bag, msg_dir):
    r = runner.invoke(app, ["info", str(bag)])
    assert "can't be decoded" in r.output
    custom_decoders.load_decoders(msg_dir)
    r = runner.invoke(app, ["info", str(bag)])
    assert "can't be decoded" not in r.output
