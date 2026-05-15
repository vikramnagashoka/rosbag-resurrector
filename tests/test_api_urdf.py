"""Tests for the v0.6.0 URDF endpoints — Sub-feature A.2.

- GET /api/scene/urdf serves a URDF file from a sandboxed location
- Path validation rejects traversal / out-of-sandbox paths
- File-type, size, encoding gates work
- GET /api/bags/{id}/scene/urdf-from-bag extracts URDF from /robot_description
- Topic missing / no payload / non-URDF payload all return clean 404s

The URDF parsing itself is client-side via urdf-loader; these tests
only cover the backend serving + sandboxing + extraction logic.
"""

from __future__ import annotations

import os
import struct
import tempfile
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from resurrector.core.bag_frame import BagFrame
from resurrector.ingest.indexer import BagIndex
from resurrector.ingest.parser import parse_bag
from resurrector.ingest.scanner import scan_path


SIMPLE_URDF = """<?xml version="1.0"?>
<robot name="test_robot">
  <link name="base_link">
    <visual>
      <geometry>
        <box size="1 1 1"/>
      </geometry>
    </visual>
  </link>
  <link name="arm_link">
    <visual>
      <geometry>
        <cylinder length="0.5" radius="0.05"/>
      </geometry>
    </visual>
  </link>
  <joint name="shoulder" type="revolute">
    <parent link="base_link"/>
    <child link="arm_link"/>
    <origin xyz="0 0 0.5" rpy="0 0 0"/>
    <axis xyz="0 0 1"/>
    <limit lower="-3.14" upper="3.14" effort="10" velocity="1"/>
  </joint>
</robot>
"""


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def urdf_file(tmp_dir):
    p = tmp_dir / "robot.urdf"
    p.write_text(SIMPLE_URDF)
    return p


@pytest.fixture
def app_client(tmp_dir, monkeypatch):
    db_path = tmp_dir / "test.db"
    BagIndex(db_path).close()  # initialize empty
    monkeypatch.setenv("RESURRECTOR_DB_PATH", str(db_path))
    monkeypatch.setenv(
        "RESURRECTOR_ALLOWED_ROOTS",
        os.pathsep.join([str(tmp_dir), tempfile.gettempdir(), str(Path.home())]),
    )
    import importlib
    from resurrector.dashboard import api as api_module
    importlib.reload(api_module)
    transport = ASGITransport(app=api_module.app)
    return AsyncClient(transport=transport, base_url="http://test")


# ---------------------------------------------------------------------------
# GET /api/scene/urdf — file-system-served URDF
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestServeUrdfByPath:
    async def test_serves_urdf_xml(self, app_client, urdf_file):
        async with app_client as client:
            r = await client.get("/api/scene/urdf", params={"path": str(urdf_file)})
        assert r.status_code == 200
        body = r.json()
        assert "urdf" in body
        assert "<robot" in body["urdf"]
        assert body["urdf"].count("<link") == 2

    async def test_returns_resolved_path(self, app_client, urdf_file):
        async with app_client as client:
            r = await client.get("/api/scene/urdf", params={"path": str(urdf_file)})
        body = r.json()
        # Path is the resolved (canonical) form
        assert body["path"] == str(urdf_file.resolve())

    async def test_missing_file_returns_404(self, app_client, tmp_dir):
        async with app_client as client:
            r = await client.get(
                "/api/scene/urdf",
                params={"path": str(tmp_dir / "does_not_exist.urdf")},
            )
        assert r.status_code == 404

    async def test_directory_path_returns_404(self, app_client, tmp_dir):
        async with app_client as client:
            r = await client.get("/api/scene/urdf", params={"path": str(tmp_dir)})
        assert r.status_code == 404

    async def test_wrong_extension_returns_400(self, app_client, tmp_dir):
        wrong = tmp_dir / "robot.txt"
        wrong.write_text(SIMPLE_URDF)
        async with app_client as client:
            r = await client.get("/api/scene/urdf", params={"path": str(wrong)})
        assert r.status_code == 400
        assert "urdf" in r.text.lower() or "xml" in r.text.lower()

    async def test_xml_extension_accepted(self, app_client, tmp_dir):
        # .xml is a valid alternate extension for URDFs
        xml_file = tmp_dir / "robot.xml"
        xml_file.write_text(SIMPLE_URDF)
        async with app_client as client:
            r = await client.get("/api/scene/urdf", params={"path": str(xml_file)})
        assert r.status_code == 200

    async def test_oversized_file_returns_413(self, app_client, tmp_dir):
        big = tmp_dir / "big.urdf"
        # 6 MB > 5 MB cap
        big.write_text("<robot>" + "x" * (6 * 1024 * 1024) + "</robot>")
        async with app_client as client:
            r = await client.get("/api/scene/urdf", params={"path": str(big)})
        assert r.status_code == 413

    async def test_outside_sandbox_returns_403(self, app_client, monkeypatch, tmp_dir):
        # Re-narrow the allowed roots to NOT include /etc
        monkeypatch.setenv("RESURRECTOR_ALLOWED_ROOTS", str(tmp_dir))
        # Re-import the api module to pick up the new env
        import importlib
        from resurrector.dashboard import api as api_module
        importlib.reload(api_module)
        transport = ASGITransport(app=api_module.app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            r = await client.get(
                "/api/scene/urdf", params={"path": "/etc/passwd"},
            )
        # _validate_path raises HTTPException(403) on out-of-sandbox
        assert r.status_code in (403, 404)  # depends on whether the file exists

    async def test_non_utf8_file_returns_400(self, app_client, tmp_dir):
        # Write a binary file with .urdf extension that isn't valid UTF-8
        bad = tmp_dir / "binary.urdf"
        bad.write_bytes(b"\x80\x81\x82\x83\x84\x85" * 50)
        async with app_client as client:
            r = await client.get("/api/scene/urdf", params={"path": str(bad)})
        assert r.status_code == 400
        assert "utf-8" in r.text.lower()


# ---------------------------------------------------------------------------
# GET /api/bags/{id}/scene/urdf-from-bag — extract from /robot_description
# ---------------------------------------------------------------------------


def _build_string_msg(payload: str) -> bytes:
    """Build a std_msgs/String CDR payload."""
    body = payload.encode("utf-8") + b"\x00"
    cdr = b"\x00\x01\x00\x00"
    return cdr + struct.pack("<I", len(body)) + body


def _generate_bag_with_robot_description(
    output_path: Path, urdf_text: str, topic: str = "/robot_description",
) -> Path:
    """Write a tiny MCAP with one std_msgs/String message on the given topic."""
    from mcap.writer import Writer

    output_path.parent.mkdir(parents=True, exist_ok=True)
    base_ns = 1_700_000_000_000_000_000

    with open(output_path, "wb") as f:
        writer = Writer(f)
        writer.start(profile="ros2", library="urdf-test")
        schema_id = writer.register_schema(
            name="std_msgs/msg/String",
            encoding="ros2msg",
            data=b"string data",
        )
        chan_id = writer.register_channel(
            topic=topic, message_encoding="cdr", schema_id=schema_id,
        )
        writer.add_message(
            channel_id=chan_id,
            log_time=base_ns, publish_time=base_ns,
            data=_build_string_msg(urdf_text),
        )
        writer.finish()
    return output_path


@pytest.fixture
def indexed_bag_with_urdf(tmp_dir):
    bag_path = tmp_dir / "robot.mcap"
    _generate_bag_with_robot_description(bag_path, SIMPLE_URDF)
    db_path = tmp_dir / "test.db"
    index = BagIndex(db_path)
    scanned = scan_path(bag_path)[0]
    parser = parse_bag(bag_path)
    metadata = parser.get_metadata()
    bag_id = index.upsert_bag(scanned, metadata)
    bf = BagFrame(bag_path)
    index.update_health_score(bag_id, bf.health_report().score)
    index.close()
    return bag_path, db_path, bag_id


@pytest.fixture
def indexed_bag_without_urdf(tmp_dir):
    """Bag that has /robot_description topic but the payload doesn't contain a URDF."""
    bag_path = tmp_dir / "noturdf.mcap"
    _generate_bag_with_robot_description(bag_path, "this is not a URDF")
    db_path = tmp_dir / "test2.db"
    index = BagIndex(db_path)
    scanned = scan_path(bag_path)[0]
    parser = parse_bag(bag_path)
    metadata = parser.get_metadata()
    bag_id = index.upsert_bag(scanned, metadata)
    bf = BagFrame(bag_path)
    index.update_health_score(bag_id, bf.health_report().score)
    index.close()
    return bag_path, db_path, bag_id


def make_client(db_path: Path, tmp_dir: Path, monkeypatch):
    monkeypatch.setenv("RESURRECTOR_DB_PATH", str(db_path))
    monkeypatch.setenv(
        "RESURRECTOR_ALLOWED_ROOTS",
        os.pathsep.join([str(tmp_dir), tempfile.gettempdir(), str(Path.home())]),
    )
    import importlib
    from resurrector.dashboard import api as api_module
    importlib.reload(api_module)
    transport = ASGITransport(app=api_module.app)
    return AsyncClient(transport=transport, base_url="http://test")


@pytest.mark.asyncio
class TestUrdfFromBag:
    async def test_extracts_urdf_from_default_topic(
        self, indexed_bag_with_urdf, tmp_dir, monkeypatch,
    ):
        _, db_path, bag_id = indexed_bag_with_urdf
        client = make_client(db_path, tmp_dir, monkeypatch)
        async with client:
            r = await client.get(f"/api/bags/{bag_id}/scene/urdf-from-bag")
        assert r.status_code == 200
        body = r.json()
        assert "<robot" in body["urdf"]
        assert body["topic"] == "/robot_description"

    async def test_unknown_bag_returns_404(self, tmp_dir, monkeypatch):
        db_path = tmp_dir / "empty.db"
        BagIndex(db_path).close()
        client = make_client(db_path, tmp_dir, monkeypatch)
        async with client:
            r = await client.get("/api/bags/9999/scene/urdf-from-bag")
        assert r.status_code == 404

    async def test_topic_not_in_bag_returns_404(
        self, indexed_bag_with_urdf, tmp_dir, monkeypatch,
    ):
        _, db_path, bag_id = indexed_bag_with_urdf
        client = make_client(db_path, tmp_dir, monkeypatch)
        async with client:
            r = await client.get(
                f"/api/bags/{bag_id}/scene/urdf-from-bag",
                params={"topic": "/nonexistent_topic"},
            )
        assert r.status_code == 404

    async def test_topic_present_but_no_urdf_payload(
        self, indexed_bag_without_urdf, tmp_dir, monkeypatch,
    ):
        _, db_path, bag_id = indexed_bag_without_urdf
        client = make_client(db_path, tmp_dir, monkeypatch)
        async with client:
            r = await client.get(f"/api/bags/{bag_id}/scene/urdf-from-bag")
        # Topic exists but payload doesn't contain <robot — 404 with clear msg
        assert r.status_code == 404
        assert "robot" in r.text.lower() or "urdf" in r.text.lower()

    async def test_custom_topic_name(
        self, tmp_dir, monkeypatch,
    ):
        # Bag with URDF on a custom topic name
        bag_path = tmp_dir / "custom.mcap"
        _generate_bag_with_robot_description(
            bag_path, SIMPLE_URDF, topic="/my_robot/urdf",
        )
        db_path = tmp_dir / "custom.db"
        index = BagIndex(db_path)
        scanned = scan_path(bag_path)[0]
        parser = parse_bag(bag_path)
        metadata = parser.get_metadata()
        bag_id = index.upsert_bag(scanned, metadata)
        index.update_health_score(bag_id, BagFrame(bag_path).health_report().score)
        index.close()

        client = make_client(db_path, tmp_dir, monkeypatch)
        async with client:
            r = await client.get(
                f"/api/bags/{bag_id}/scene/urdf-from-bag",
                params={"topic": "/my_robot/urdf"},
            )
        assert r.status_code == 200
        assert r.json()["topic"] == "/my_robot/urdf"

    async def test_response_is_cached(
        self, indexed_bag_with_urdf, tmp_dir, monkeypatch,
    ):
        # Cache-Control header lets the browser skip re-fetching during scrub
        _, db_path, bag_id = indexed_bag_with_urdf
        client = make_client(db_path, tmp_dir, monkeypatch)
        async with client:
            r = await client.get(f"/api/bags/{bag_id}/scene/urdf-from-bag")
        assert "cache-control" in {k.lower() for k in r.headers.keys()}
