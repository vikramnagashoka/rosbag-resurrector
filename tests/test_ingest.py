"""Tests for the ingest layer: scanner, parser, indexer."""

import tempfile
from pathlib import Path

import pytest

from tests.fixtures.generate_test_bags import generate_bag, BagConfig
from resurrector.ingest.scanner import scan_path, ScannedFile
from resurrector.ingest.parser import MCAPParser, parse_bag
from resurrector.ingest.indexer import BagIndex


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def healthy_bag(tmp_dir):
    return generate_bag(tmp_dir / "healthy.mcap", BagConfig(duration_sec=2.0))


@pytest.fixture
def short_bag(tmp_dir):
    return generate_bag(tmp_dir / "short.mcap", BagConfig(duration_sec=1.0))


class TestScanner:
    def test_scan_single_file(self, healthy_bag):
        results = scan_path(healthy_bag)
        assert len(results) == 1
        assert results[0].extension == ".mcap"
        assert results[0].size_bytes > 0
        assert len(results[0].fingerprint) == 64
        # fast fingerprint by default — no full hash
        assert results[0].sha256_full is None

    def test_scan_full_hash(self, healthy_bag):
        """--full-hash mode populates a real SHA256 alongside the fingerprint."""
        import hashlib
        results = scan_path(healthy_bag, full_hash=True)
        assert results[0].sha256_full is not None
        assert len(results[0].sha256_full) == 64
        # Verify it matches a direct hashlib.sha256 over the file bytes.
        h = hashlib.sha256()
        with open(healthy_bag, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        assert results[0].sha256_full == h.hexdigest()

    def test_scan_directory(self, tmp_dir, healthy_bag, short_bag):
        results = scan_path(tmp_dir)
        assert len(results) == 2
        names = {r.path.name for r in results}
        assert "healthy.mcap" in names
        assert "short.mcap" in names

    def test_scan_nonexistent_path(self):
        with pytest.raises(FileNotFoundError):
            scan_path("/nonexistent/path")

    def test_scan_empty_directory(self, tmp_dir):
        results = scan_path(tmp_dir)
        assert len(results) == 0

    def test_scanned_file_format(self, healthy_bag):
        results = scan_path(healthy_bag)
        assert results[0].format == "mcap"


class TestParser:
    def test_parse_metadata(self, healthy_bag):
        parser = MCAPParser(healthy_bag)
        meta = parser.get_metadata()
        assert meta.format == "mcap"
        assert meta.duration_sec > 0
        assert meta.message_count > 0
        assert len(meta.topics) >= 4  # IMU, joints, camera, lidar

    def test_topic_names(self, healthy_bag):
        parser = MCAPParser(healthy_bag)
        meta = parser.get_metadata()
        topic_names = [t.name for t in meta.topics]
        assert "/imu/data" in topic_names
        assert "/joint_states" in topic_names
        assert "/camera/rgb" in topic_names
        assert "/lidar/scan" in topic_names

    def test_topic_frequencies(self, healthy_bag):
        parser = MCAPParser(healthy_bag)
        meta = parser.get_metadata()
        topic_map = {t.name: t for t in meta.topics}
        # IMU should be ~200Hz
        assert topic_map["/imu/data"].frequency_hz is not None
        assert topic_map["/imu/data"].frequency_hz > 100

    def test_read_messages(self, healthy_bag):
        parser = MCAPParser(healthy_bag)
        messages = list(parser.read_messages(topics=["/imu/data"]))
        assert len(messages) > 0
        msg = messages[0]
        assert msg.topic == "/imu/data"
        assert "linear_acceleration" in msg.data
        assert "angular_velocity" in msg.data
        assert "orientation" in msg.data

    def test_read_messages_filtered_by_topic(self, healthy_bag):
        parser = MCAPParser(healthy_bag)
        imu_msgs = list(parser.read_messages(topics=["/imu/data"]))
        joint_msgs = list(parser.read_messages(topics=["/joint_states"]))
        assert all(m.topic == "/imu/data" for m in imu_msgs)
        assert all(m.topic == "/joint_states" for m in joint_msgs)

    def test_parse_joint_state(self, healthy_bag):
        parser = MCAPParser(healthy_bag)
        messages = list(parser.read_messages(topics=["/joint_states"]))
        msg = messages[0]
        assert "name" in msg.data
        assert "position" in msg.data
        assert len(msg.data["name"]) == 6  # 6-DOF

    def test_parse_laser_scan(self, healthy_bag):
        parser = MCAPParser(healthy_bag)
        messages = list(parser.read_messages(topics=["/lidar/scan"]))
        msg = messages[0]
        assert "ranges" in msg.data
        assert len(msg.data["ranges"]) == 360

    def test_parse_image(self, healthy_bag):
        parser = MCAPParser(healthy_bag)
        messages = list(parser.read_messages(topics=["/camera/rgb"]))
        msg = messages[0]
        assert msg.data["width"] == 64
        assert msg.data["height"] == 48

    def test_parse_bag_factory(self, healthy_bag):
        parser = parse_bag(healthy_bag)
        assert isinstance(parser, MCAPParser)

    def test_unsupported_format(self, tmp_dir):
        fake = tmp_dir / "test.xyz"
        fake.write_text("not a bag")
        with pytest.raises(ValueError):
            parse_bag(fake)


class TestIndexer:
    def test_create_index(self, tmp_dir):
        index = BagIndex(tmp_dir / "test.db")
        assert index.count() == 0
        index.close()

    def test_upsert_bag(self, tmp_dir, healthy_bag):
        index = BagIndex(tmp_dir / "test.db")
        scanned = scan_path(healthy_bag)[0]
        parser = parse_bag(healthy_bag)
        meta = parser.get_metadata()
        bag_id = index.upsert_bag(scanned, meta)
        assert bag_id > 0
        assert index.count() == 1

        # Get bag back
        bag = index.get_bag(bag_id)
        assert bag is not None
        assert bag["format"] == "mcap"
        assert len(bag["topics"]) >= 4
        index.close()

    def test_upsert_idempotent(self, tmp_dir, healthy_bag):
        index = BagIndex(tmp_dir / "test.db")
        scanned = scan_path(healthy_bag)[0]
        parser = parse_bag(healthy_bag)
        meta = parser.get_metadata()
        id1 = index.upsert_bag(scanned, meta)
        id2 = index.upsert_bag(scanned, meta)
        assert id1 == id2
        assert index.count() == 1
        index.close()

    def test_tags(self, tmp_dir, healthy_bag):
        index = BagIndex(tmp_dir / "test.db")
        scanned = scan_path(healthy_bag)[0]
        parser = parse_bag(healthy_bag)
        meta = parser.get_metadata()
        bag_id = index.upsert_bag(scanned, meta)

        index.add_tag(bag_id, "task", "pick_and_place")
        index.add_tag(bag_id, "robot", "digit")

        bag = index.get_bag(bag_id)
        tags = {t["key"]: t["value"] for t in bag["tags"]}
        assert tags["task"] == "pick_and_place"
        assert tags["robot"] == "digit"

        index.remove_tag(bag_id, "robot")
        bag = index.get_bag(bag_id)
        assert len(bag["tags"]) == 1
        index.close()

    def test_search(self, tmp_dir, healthy_bag):
        index = BagIndex(tmp_dir / "test.db")
        scanned = scan_path(healthy_bag)[0]
        parser = parse_bag(healthy_bag)
        meta = parser.get_metadata()
        index.upsert_bag(scanned, meta)

        results = index.search("topic:/imu/data")
        assert len(results) == 1

        results = index.search("topic:/nonexistent")
        assert len(results) == 0
        index.close()

    def test_list_bags(self, tmp_dir, healthy_bag, short_bag):
        index = BagIndex(tmp_dir / "test.db")
        for bag_path in [healthy_bag, short_bag]:
            scanned = scan_path(bag_path)[0]
            parser = parse_bag(bag_path)
            meta = parser.get_metadata()
            index.upsert_bag(scanned, meta)

        bags = index.list_bags()
        assert len(bags) == 2
        index.close()
