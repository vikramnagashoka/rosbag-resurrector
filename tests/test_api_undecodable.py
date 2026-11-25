"""Dashboard API on a bag with a topic Resurrector can't decode.

The notebook asks GET /api/bags/{id}/decoders which topics can't be
decoded, loads a .msg folder with POST /api/decoders, and every endpoint
that reads fields answers 422 ``undecodable_topics`` until then.
"""

from __future__ import annotations

import importlib

import pytest
from fastapi.testclient import TestClient

from resurrector.ingest import custom_decoders
from resurrector.ingest.indexer import BagIndex
from resurrector.ingest.parser import parse_bag
from resurrector.ingest.scanner import scan_path
from tests.fixtures.undecodable_bag import write_msg_folder, write_undecodable_bag


@pytest.fixture(autouse=True)
def _clean_decoders(monkeypatch):
    monkeypatch.delenv(custom_decoders.ENV_VAR, raising=False)
    custom_decoders._reset_for_tests()
    yield
    custom_decoders._reset_for_tests()


@pytest.fixture
def setup(tmp_path, monkeypatch):
    bag = write_undecodable_bag(tmp_path / "gizmo.mcap", n=30)
    msg_dir = write_msg_folder(tmp_path / "ws")
    db = tmp_path / "index.db"
    index = BagIndex(db)
    bag_id = index.upsert_bag(scan_path(bag)[0], parse_bag(bag).get_metadata())
    index.close()
    monkeypatch.setenv("RESURRECTOR_DB_PATH", str(db))
    monkeypatch.setenv("RESURRECTOR_ALLOWED_ROOTS", str(tmp_path))
    from resurrector.dashboard import api
    importlib.reload(api)
    return TestClient(api.app), bag_id, msg_dir, tmp_path


def test_lists_undecodable_topics(setup):
    client, bag_id, _, _ = setup
    r = client.get(f"/api/bags/{bag_id}/decoders")
    assert r.status_code == 200
    assert r.json() == {
        "undecodable": [{
            "topic": "/gizmo",
            "message_type": "acme_msgs/msg/Gizmo",
            "reason": "the bag has only a ros2idl definition of it",
        }],
        "loaded_paths": [],
    }


def test_reading_fields_is_a_422_naming_the_topic(setup):
    client, bag_id, _, _ = setup
    r = client.get(f"/api/bags/{bag_id}/topics/gizmo")
    assert r.status_code == 422, r.text
    detail = r.json()["detail"]
    assert detail["code"] == "undecodable_topics"
    assert detail["topics"][0]["topic"] == "/gizmo"
    assert "notice above the notebook" in detail["message"]
    r = client.get(f"/api/bags/{bag_id}/sync", params={"topics": "/gizmo,/widget"})
    assert r.status_code == 422, r.text
    assert client.get(f"/api/bags/{bag_id}/topics/widget").status_code == 200


def test_export_422_then_skip(setup):
    client, bag_id, _, tmp = setup
    out = tmp / "out"
    r = client.post(f"/api/bags/{bag_id}/export", params={"output_dir": str(out)})
    assert r.status_code == 422, r.text
    assert r.json()["detail"]["code"] == "undecodable_topics"
    r = client.post(f"/api/bags/{bag_id}/export",
                    params={"output_dir": str(out), "skip_undecodable": "true"})
    assert r.status_code == 200, r.text
    assert sorted(p.name for p in out.iterdir()) == ["widget.parquet"]


def test_load_a_msg_folder(setup):
    client, bag_id, msg_dir, _ = setup
    r = client.post("/api/decoders", json={"path": str(msg_dir)})
    assert r.status_code == 200, r.text
    assert r.json()["message_types"] == ["acme_msgs/msg/Gizmo"]
    assert r.json()["loaded_paths"] == [str(msg_dir.resolve())]
    assert client.get(f"/api/bags/{bag_id}/decoders").json()["undecodable"] == []
    r = client.get(f"/api/bags/{bag_id}/topics/gizmo")
    assert r.status_code == 200, r.text
    assert "x" in r.json()["columns"]


@pytest.mark.parametrize("make, status, text", [
    (lambda t: t / "nowhere", 404, "No such file or folder"),
    (lambda t: (t / "empty").mkdir() or t / "empty", 400, "No .msg files"),
    (lambda t: (t / "dec.py").write_text("x = 1\n") and t / "dec.py", 400, ".msg folders only"),
    (lambda t: t.parent, 403, "outside the allowed roots"),
], ids=["missing", "no-msg-files", "python-file", "outside-allowed-roots"])
def test_bad_paths(setup, make, status, text):
    client, _, _, tmp = setup
    r = client.post("/api/decoders", json={"path": str(make(tmp))})
    assert r.status_code == status, r.text
    assert text in r.json()["detail"]
    assert custom_decoders.loaded_paths() == []


def test_python_file_is_never_run(setup):
    client, _, _, tmp = setup
    marker = tmp / "ran"
    (tmp / "evil.py").write_text(f"open({str(marker)!r}, 'w').write('x')\n")
    assert client.post("/api/decoders", json={"path": str(tmp / "evil.py")}).status_code == 400
    assert not marker.exists()


def test_missing_path_field(setup):
    client, _, _, _ = setup
    assert client.post("/api/decoders", json={}).status_code == 400


def test_bridge_streams_what_it_can_with_the_loaded_definitions(setup, tmp_path, monkeypatch):
    # The bridge subprocess has no terminal to ask in: without
    # --skip-undecodable it would exit 1 at startup on this bag.
    import socket

    from resurrector.dashboard import api

    monkeypatch.setenv("RESURRECTOR_BRIDGE_LOG_DIR", str(tmp_path / "logs"))
    client, _, msg_dir, tmp = setup
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    assert client.post("/api/decoders", json={"path": str(msg_dir)}).status_code == 200
    api._get_bridge_state().update(process=None, port=None, mode=None)
    try:
        r = client.post("/api/bridge/start", json={
            "mode": "playback", "bag_path": str(tmp / "gizmo.mcap"), "port": port,
        })
        assert r.status_code == 200, r.text
        args = api._get_bridge_state()["process"].args
        assert "--skip-undecodable" in args
        assert args[args.index("--decoders") + 1] == str(msg_dir.resolve())
    finally:
        proc = api._get_bridge_state()["process"]
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        api._get_bridge_state().update(process=None, port=None, mode=None)
