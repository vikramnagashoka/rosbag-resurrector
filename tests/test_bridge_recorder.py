"""Tests for BridgeRecorder (Sub-feature 2.2 — record-while-streaming)."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from resurrector.bridge.recorder import BridgeRecorder
from resurrector.ingest.parser import MCAPParser, TopicInfo


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def synth_bag_with_metadata(tmp_dir):
    """Synthetic bag + extracted TopicInfo with schema data populated."""
    from resurrector.demo.sample_bag import generate_bag, BagConfig
    bag_path = tmp_dir / "synth.mcap"
    generate_bag(bag_path, BagConfig(duration_sec=0.5))
    parser = MCAPParser(bag_path)
    metadata = parser.get_metadata()
    return bag_path, metadata.topics


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

class TestLifecycle:
    def test_construct_does_not_open_file(self, tmp_dir, synth_bag_with_metadata):
        _, topics = synth_bag_with_metadata
        out = tmp_dir / "rec.mcap"
        rec = BridgeRecorder(output_path=out, topic_info=topics)
        # File should NOT exist yet — open is lazy
        assert not out.exists()
        rec.close()

    def test_close_idempotent(self, tmp_dir, synth_bag_with_metadata):
        _, topics = synth_bag_with_metadata
        rec = BridgeRecorder(output_path=tmp_dir / "rec.mcap", topic_info=topics)
        rec.close()
        rec.close()  # Should not raise

    def test_record_after_close_raises(self, tmp_dir, synth_bag_with_metadata):
        _, topics = synth_bag_with_metadata
        rec = BridgeRecorder(output_path=tmp_dir / "rec.mcap", topic_info=topics)
        rec.close()
        with pytest.raises(RuntimeError, match="after close"):
            rec.record("/imu/data", 0, b"x")

    def test_context_manager(self, tmp_dir, synth_bag_with_metadata):
        bag_path, topics = synth_bag_with_metadata
        out = tmp_dir / "rec.mcap"
        with BridgeRecorder(output_path=out, topic_info=topics) as rec:
            for msg in MCAPParser(bag_path).read_messages(topics=["/imu/data"]):
                if msg.raw_data:
                    rec.record(msg.topic, msg.timestamp_ns, msg.raw_data)
                    break
        # File should be valid MCAP after context exit
        assert out.exists()
        assert out.stat().st_size > 0


# ---------------------------------------------------------------------------
# Recording produces valid MCAP
# ---------------------------------------------------------------------------

class TestRecording:
    def test_records_one_topic_round_trip(self, tmp_dir, synth_bag_with_metadata):
        bag_path, topics = synth_bag_with_metadata
        out = tmp_dir / "rec.mcap"

        # Record /imu/data from the synth bag
        rec = BridgeRecorder(output_path=out, topic_info=topics)
        n_recorded = 0
        for msg in MCAPParser(bag_path).read_messages(topics=["/imu/data"]):
            if msg.raw_data:
                rec.record(msg.topic, msg.timestamp_ns, msg.raw_data)
                n_recorded += 1
        rec.close()

        assert rec.messages_written == n_recorded > 0

        # Read back: the recorded MCAP should contain only /imu/data
        new_parser = MCAPParser(out)
        new_meta = new_parser.get_metadata()
        topic_names = {t.name for t in new_meta.topics}
        assert topic_names == {"/imu/data"}
        # Message count should match
        imu_topic = next(t for t in new_meta.topics if t.name == "/imu/data")
        assert imu_topic.message_count == n_recorded

    def test_records_multi_topic(self, tmp_dir, synth_bag_with_metadata):
        bag_path, topics = synth_bag_with_metadata
        out = tmp_dir / "multi.mcap"

        rec = BridgeRecorder(output_path=out, topic_info=topics)
        target_topics = ["/imu/data", "/joint_states"]
        for msg in MCAPParser(bag_path).read_messages(topics=target_topics):
            if msg.raw_data:
                rec.record(msg.topic, msg.timestamp_ns, msg.raw_data)
        rec.close()

        new_meta = MCAPParser(out).get_metadata()
        topic_names = {t.name for t in new_meta.topics}
        assert topic_names == set(target_topics)

    def test_dropped_unknown_topic_doesnt_crash(self, tmp_dir, synth_bag_with_metadata):
        _, topics = synth_bag_with_metadata
        rec = BridgeRecorder(
            output_path=tmp_dir / "drop.mcap",
            topic_info=topics,
        )
        # Record a message on a topic not in topic_info — should silently drop
        rec.record("/totally/unknown/topic", 0, b"x")
        # Should not have written anything yet
        assert rec.messages_written == 0
        rec.close()

    def test_dropped_empty_payload(self, tmp_dir, synth_bag_with_metadata):
        _, topics = synth_bag_with_metadata
        rec = BridgeRecorder(output_path=tmp_dir / "empty.mcap", topic_info=topics)
        rec.record("/imu/data", 0, b"")  # Empty payload
        assert rec.messages_written == 0
        rec.close()

    def test_message_without_schema_data_dropped(self, tmp_dir):
        """Topic without schema_data → record() drops + warns once."""
        # Construct a TopicInfo without schema_data populated
        topics = [
            TopicInfo(
                name="/no/schema",
                message_type="custom/msgs/MyMsg",
                message_count=0,
                schema_encoding="",  # missing
                schema_data="",
            )
        ]
        rec = BridgeRecorder(output_path=tmp_dir / "noschema.mcap", topic_info=topics)
        rec.record("/no/schema", 0, b"some_payload")
        assert rec.messages_written == 0
        rec.close()


# ---------------------------------------------------------------------------
# Sequence handling
# ---------------------------------------------------------------------------

class TestSequencing:
    def test_per_topic_sequence_increments(self, tmp_dir, synth_bag_with_metadata):
        bag_path, topics = synth_bag_with_metadata
        out = tmp_dir / "seq.mcap"
        rec = BridgeRecorder(
            output_path=out,
            topic_info=topics,
            sequence_per_topic=True,
        )
        for msg in MCAPParser(bag_path).read_messages(topics=["/imu/data"]):
            if msg.raw_data:
                rec.record(msg.topic, msg.timestamp_ns, msg.raw_data)
        rec.close()

        # Read back: sequences should be 0, 1, 2, ... (monotonically increasing)
        new_parser = MCAPParser(out)
        seqs = []
        for msg in new_parser.read_messages(topics=["/imu/data"]):
            seqs.append(msg.sequence)
        assert seqs == list(range(len(seqs)))


# ---------------------------------------------------------------------------
# Integration with the bridge's per-message callback
# ---------------------------------------------------------------------------

class TestBridgeIntegration:
    def test_recorder_can_be_attached_to_playback_engine(self, tmp_dir, synth_bag_with_metadata):
        """Smoke test: construct the recorder, attach to PlaybackEngine.message_callback,
        run playback, verify the recorded MCAP contains the same messages.
        """
        import asyncio
        from resurrector.bridge.playback import PlaybackEngine

        bag_path, topics = synth_bag_with_metadata
        out = tmp_dir / "integration.mcap"
        rec = BridgeRecorder(output_path=out, topic_info=topics)

        def on_message(msg):
            if msg.raw_data:
                rec.record(msg.topic, msg.timestamp_ns, msg.raw_data)

        engine = PlaybackEngine(
            bag_path=bag_path,
            speed=20.0,
            topics=["/imu/data"],
            message_callback=on_message,
        )

        async def run():
            await engine.play()
            # Wait for the bag (0.5s / 20x = 25ms) plus some buffer
            await asyncio.sleep(0.2)
            await engine.stop()
            rec.close()

        asyncio.run(run())

        assert rec.messages_written > 0
        new_meta = MCAPParser(out).get_metadata()
        assert any(t.name == "/imu/data" for t in new_meta.topics)
