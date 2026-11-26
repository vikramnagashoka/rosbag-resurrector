"""Topics whose message type has no decoder, and decoders the user loads.

APIs that read fields raise UndecodableTopicError up front instead of
returning frames with only timestamps; load_decoders() makes the type
decodable from a .msg folder or a Python file. See
resurrector/ingest/custom_decoders.py.
"""

from __future__ import annotations

import logging
import struct
from pathlib import Path

import polars as pl
import pytest
from mcap.writer import Writer

import resurrector
from resurrector import BagFrame, UndecodableTopicError, load_decoders
from resurrector.core.exceptions import EmptyExportError
from resurrector.core.export import Exporter
from resurrector.ingest import custom_decoders
from resurrector.ingest.parser import flatten_into, parse_bag, unregister_decoder
from tests.fixtures.undecodable_bag import (
    FRAME_ID, GIZMO_TYPE, gizmo_values, write_msg_folder, write_undecodable_bag,
)


@pytest.fixture(autouse=True)
def _clean_decoders(monkeypatch):
    monkeypatch.delenv(custom_decoders.ENV_VAR, raising=False)
    custom_decoders._reset_for_tests()
    yield
    custom_decoders._reset_for_tests()
    unregister_decoder(GIZMO_TYPE)


@pytest.fixture
def bag(tmp_path) -> Path:
    return write_undecodable_bag(tmp_path / "gizmo.mcap", n=40)


@pytest.fixture
def msg_dir(tmp_path) -> Path:
    return write_msg_folder(tmp_path / "ws" / "src")


def _bag_with(tmp_path, name, schema_encoding, schema_data, payload=b"\x00\x01\x00\x00\x00\x00\x00\x00"):
    path = tmp_path / f"{name.replace('/', '_')}.mcap"
    with open(path, "wb") as f:
        w = Writer(f)
        w.start(profile="ros2")
        sid = w.register_schema(name, schema_encoding, schema_data) if schema_encoding else 0
        cid = w.register_channel("/t", "cdr", sid)
        for i in range(5):
            w.add_message(cid, log_time=i + 1, publish_time=i + 1, data=payload)
        w.finish()
    return path


class TestFindingThem:
    def test_lists_the_topic_with_its_reason(self, bag):
        found = BagFrame(bag).undecodable_topics()
        assert [(t.topic, t.message_type) for t in found] == [("/gizmo", GIZMO_TYPE)]
        assert found[0].reason == "the bag has only a ros2idl definition of it"
        assert BagFrame(bag)["/gizmo"].undecodable_reason == found[0].reason
        assert BagFrame(bag)["/widget"].undecodable_reason is None

    def test_filters_by_topic(self, bag):
        assert BagFrame(bag).undecodable_topics(["/widget"]) == []

    def test_no_definition(self, tmp_path):
        path = _bag_with(tmp_path, "acme_msgs/msg/Blank", None, b"")
        (t,) = BagFrame(path).undecodable_topics()
        assert t.message_type == "unknown"
        assert t.reason == "the bag has no definition of it"

    def test_definition_the_compiler_cant_read(self, tmp_path):
        path = _bag_with(tmp_path, "acme_msgs/msg/Name", "ros2msg", b"wstring name\n")
        (t,) = BagFrame(path).undecodable_topics()
        assert "wstring is not supported" in t.reason
        assert t.reason.startswith("its definition can't be decoded")

    def test_registered_decoder_counts(self, bag):
        resurrector.register_decoder(GIZMO_TYPE, lambda raw: {"len": len(raw)})
        assert BagFrame(bag).undecodable_topics() == []


class TestReadingFieldsRaises:
    def test_to_polars_and_iter_chunks_raise_on_the_call(self, bag):
        view = BagFrame(bag)["/gizmo"]
        with pytest.raises(UndecodableTopicError) as exc:
            view.iter_chunks()  # not iterated: the call itself raises
        assert [t.topic for t in exc.value.topics] == ["/gizmo"]
        msg = str(exc.value)
        assert "/gizmo" in msg and GIZMO_TYPE in msg and "ros2idl" in msg
        assert "load_decoders" in msg and "health_report()" in msg
        with pytest.raises(UndecodableTopicError):
            view.to_polars()
        assert isinstance(exc.value, ValueError)

    def test_sync_raises_before_reading(self, bag):
        with pytest.raises(UndecodableTopicError) as exc:
            BagFrame(bag).sync(["/widget", "/gizmo"])
        assert [t.topic for t in exc.value.topics] == ["/gizmo"]

    def test_raw_messages_and_health_still_work(self, bag):
        bf = BagFrame(bag)
        msgs = list(bf["/gizmo"].iter_messages())
        assert len(msgs) == 40
        assert msgs[0].data["_unparsed"] is True and msgs[0].raw_data
        assert "/gizmo" in bf.health_report().topic_scores

    def test_the_decodable_topic_is_unaffected(self, bag):
        df = BagFrame(bag)["/widget"].to_polars()
        assert "y" in df.columns and df.height == 40


class TestExport:
    def test_export_raises_and_writes_nothing(self, bag, tmp_path):
        out = tmp_path / "out"
        with pytest.raises(UndecodableTopicError) as exc:
            BagFrame(bag).export(output=str(out))
        assert "skip_undecodable=True" in str(exc.value)
        assert not out.exists()

    def test_skip_exports_the_rest(self, bag, tmp_path):
        out = BagFrame(bag).export(output=str(tmp_path / "out"), skip_undecodable=True)
        assert sorted(p.name for p in Path(out).iterdir()) == ["widget.parquet"]

    def test_skip_with_nothing_left_raises(self, bag, tmp_path):
        with pytest.raises(UndecodableTopicError):
            BagFrame(bag).export(topics=["/gizmo"], output=str(tmp_path / "o"), skip_undecodable=True)

    def test_exporter_checks_too(self, bag, tmp_path):
        # Dataset exports call the Exporter directly.
        with pytest.raises(UndecodableTopicError) as exc:
            Exporter().export(BagFrame(bag), ["/gizmo"], "parquet", str(tmp_path / "o"))
        assert "skip_undecodable" not in str(exc.value)

    def test_unknown_topic_is_still_topic_not_found(self, bag, tmp_path):
        with pytest.raises(KeyError):
            BagFrame(bag).export(topics=["/nope"], output=str(tmp_path / "o"), skip_undecodable=True)


class TestLoadMsgFolder:
    def test_folder_makes_the_topic_decodable(self, bag, msg_dir):
        assert load_decoders(msg_dir) == [GIZMO_TYPE]
        bf = BagFrame(bag)
        assert bf.undecodable_topics() == []
        df = bf["/gizmo"].to_polars()
        expected = [gizmo_values(i) for i in range(40)]
        assert df["x"].to_list() == [x for x, _ in expected]
        assert df["n"].to_list() == [n for _, n in expected]
        # std_msgs/Header isn't in the folder: it came from /widget's definition.
        assert set(df["header.frame_id"].to_list()) == {FRAME_ID}

    def test_rows_match_flattened_messages(self, bag, msg_dir):
        load_decoders(msg_dir)
        view = BagFrame(bag)["/gizmo"]
        rows = []
        for m in view.iter_messages():
            row = {"timestamp_ns": m.timestamp_ns}
            flatten_into(m.data, row)
            rows.append(row)
        assert view.to_polars().equals(pl.DataFrame(rows))

    def test_single_msg_file(self, bag, msg_dir):
        load_decoders(msg_dir / "acme_msgs" / "msg" / "Gizmo.msg")
        assert BagFrame(bag).undecodable_topics() == []

    def test_type_it_uses_missing_everywhere(self, bag, tmp_path):
        d = tmp_path / "ws2" / "acme_msgs" / "msg"
        d.mkdir(parents=True)
        (d / "Gizmo.msg").write_text("acme_msgs/Part part\nfloat64 x\nint32 n\n")
        load_decoders(tmp_path / "ws2")
        (t,) = BagFrame(bag).undecodable_topics()
        assert "definition of acme_msgs/Part is missing" in t.reason

    def test_completes_an_embedded_definition_missing_a_type(self, tmp_path):
        # The bag's own definition names acme_msgs/Part but leaves it out.
        payload = b"\x00\x01\x00\x00" + struct.pack("<d", 2.5)
        bag = _bag_with(tmp_path, "acme_msgs/msg/Holder", "ros2msg", b"acme_msgs/Part part\n", payload)
        assert "definition of acme_msgs/Part is missing" in BagFrame(bag).undecodable_topics()[0].reason
        d = tmp_path / "ws3" / "acme_msgs" / "msg"
        d.mkdir(parents=True)
        (d / "Part.msg").write_text("float64 weight\n")
        load_decoders(tmp_path / "ws3")
        assert BagFrame(bag)["/t"].to_polars()["part.weight"].to_list() == [2.5] * 5

    def test_records_the_path(self, msg_dir):
        load_decoders(msg_dir)
        assert custom_decoders.loaded_paths() == [msg_dir.resolve()]

    @pytest.mark.parametrize("make, error", [
        (lambda p: p / "missing", FileNotFoundError),
        (lambda p: (p / "empty").mkdir() or p / "empty", ValueError),
        (lambda p: (p / "x.txt").write_text("hi") and p / "x.txt", ValueError),
        (lambda p: (p / "Loose.msg").write_text("int32 a\n") and p / "Loose.msg", ValueError),
    ], ids=["missing", "no-msg-files", "other-file", "msg-outside-msg-dir"])
    def test_bad_paths(self, tmp_path, make, error):
        with pytest.raises(error):
            load_decoders(make(tmp_path))
        assert custom_decoders.loaded_paths() == []


class TestLoadPythonFile:
    def test_file_that_registers(self, bag, tmp_path):
        f = tmp_path / "decoders.py"
        f.write_text(
            "import struct\n"
            "from dataclasses import dataclass\n"
            "import resurrector\n"
            "@dataclass\n"
            "class Unused:\n"
            "    a: int = 0\n"
            "def decode(raw):\n"
            "    return {'size': len(raw)}\n"
            f"resurrector.register_decoder({GIZMO_TYPE!r}, decode)\n"
        )
        assert load_decoders(f) == [GIZMO_TYPE]
        df = BagFrame(bag)["/gizmo"].to_polars()
        assert df["size"].n_unique() == 1

    def test_file_that_registers_nothing(self, tmp_path):
        f = tmp_path / "nothing.py"
        f.write_text("x = 1\n")
        with pytest.raises(ValueError, match="registered no decoder"):
            load_decoders(f)

    def test_file_that_raises(self, tmp_path):
        f = tmp_path / "broken.py"
        f.write_text("raise RuntimeError('boom')\n")
        with pytest.raises(ValueError, match="RuntimeError while loading: boom"):
            load_decoders(f)


def test_env_var_loads_on_first_need(bag, msg_dir, monkeypatch):
    monkeypatch.setenv(custom_decoders.ENV_VAR, str(msg_dir))
    custom_decoders._reset_for_tests()
    assert BagFrame(bag).undecodable_topics() == []


def test_env_var_bad_path_only_warns(bag, tmp_path, monkeypatch, caplog):
    monkeypatch.setenv(custom_decoders.ENV_VAR, str(tmp_path / "nowhere"))
    custom_decoders._reset_for_tests()
    with caplog.at_level(logging.WARNING):
        assert len(BagFrame(bag).undecodable_topics()) == 1
    assert "RESURRECTOR_DECODERS" in caplog.text


def test_concatenated_bags(bag, tmp_path):
    other = write_undecodable_bag(tmp_path / "b.mcap", n=10)
    cbf = resurrector.concatenate_bags([bag, other])
    assert [t.topic for t in cbf.undecodable_topics()] == ["/gizmo"]


def test_a_failing_decoder_logs_twice_not_per_message(bag, caplog):
    def broken(raw):
        raise ValueError("nope")
    resurrector.register_decoder(GIZMO_TYPE, broken)
    with caplog.at_level(logging.WARNING, logger="resurrector.ingest.parser"):
        msgs = list(parse_bag(bag).read_messages(topics=["/gizmo"]))
    assert len(msgs) == 40 and all(m.data.get("_parse_error") for m in msgs)
    lines = [r.getMessage() for r in caplog.records]
    assert len(lines) == 2
    assert "nope" in lines[0]
    assert lines[1].startswith("40 messages of type 'acme_msgs/msg/Gizmo' failed to decode")


def test_empty_export_error_still_applies(tmp_path):
    # An undecodable topic with no messages is reported as undecodable,
    # not as an empty export: the type is the problem the user can fix.
    path = tmp_path / "empty.mcap"
    with open(path, "wb") as f:
        w = Writer(f)
        w.start(profile="ros2")
        sid = w.register_schema("acme_msgs/msg/Gizmo", "ros2idl", b"x")
        w.register_channel("/gizmo", "cdr", sid)
        w.finish()
    with pytest.raises(UndecodableTopicError):
        BagFrame(path).export(output=str(tmp_path / "o"))
    assert issubclass(UndecodableTopicError, ValueError)
    assert not issubclass(UndecodableTopicError, EmptyExportError)
