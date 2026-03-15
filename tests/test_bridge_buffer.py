"""Tests for the ring buffer."""

import threading

import pytest

from resurrector.bridge.buffer import RingBuffer, BufferedMessage


def _make_msg(topic: str, ts: float) -> BufferedMessage:
    return BufferedMessage(topic=topic, timestamp_sec=ts, encoded={}, raw_json=f'{{"t":{ts}}}')


class TestRingBuffer:
    def test_basic_put_get(self):
        buf = RingBuffer(capacity=100)
        buf.register_consumer("c1")
        buf.put(_make_msg("/imu", 1.0))
        buf.put(_make_msg("/imu", 2.0))

        messages = buf.get_since("c1")
        assert len(messages) == 2
        assert messages[0].timestamp_sec == 1.0
        assert messages[1].timestamp_sec == 2.0

    def test_consumer_catches_up(self):
        buf = RingBuffer(capacity=100)
        buf.register_consumer("c1")
        buf.put(_make_msg("/imu", 1.0))

        # First read
        msgs = buf.get_since("c1")
        assert len(msgs) == 1

        # No new messages — should be empty
        msgs = buf.get_since("c1")
        assert len(msgs) == 0

        # New message arrives
        buf.put(_make_msg("/imu", 2.0))
        msgs = buf.get_since("c1")
        assert len(msgs) == 1
        assert msgs[0].timestamp_sec == 2.0

    def test_overflow_eviction(self):
        buf = RingBuffer(capacity=5)
        buf.register_consumer("c1")

        for i in range(10):
            buf.put(_make_msg("/imu", float(i)))

        assert buf.size == 5

        # Consumer should get the latest 5 (skipping evicted ones)
        msgs = buf.get_since("c1")
        assert len(msgs) == 5
        assert msgs[0].timestamp_sec == 5.0
        assert msgs[-1].timestamp_sec == 9.0

    def test_multi_consumer(self):
        buf = RingBuffer(capacity=100)
        buf.register_consumer("c1")
        buf.register_consumer("c2")

        buf.put(_make_msg("/imu", 1.0))
        buf.put(_make_msg("/imu", 2.0))

        # Both consumers get all messages independently
        msgs_c1 = buf.get_since("c1")
        msgs_c2 = buf.get_since("c2")
        assert len(msgs_c1) == 2
        assert len(msgs_c2) == 2

        # c1 reads next message, c2 doesn't
        buf.put(_make_msg("/imu", 3.0))
        msgs_c1 = buf.get_since("c1")
        assert len(msgs_c1) == 1

        msgs_c2 = buf.get_since("c2")
        assert len(msgs_c2) == 1

    def test_unregister_consumer(self):
        buf = RingBuffer(capacity=100)
        buf.register_consumer("c1")
        assert buf.consumer_count == 1
        buf.unregister_consumer("c1")
        assert buf.consumer_count == 0
        # Should return empty for unregistered consumer
        msgs = buf.get_since("c1")
        assert len(msgs) == 0

    def test_max_count_limit(self):
        buf = RingBuffer(capacity=100)
        buf.register_consumer("c1")

        for i in range(20):
            buf.put(_make_msg("/imu", float(i)))

        # Request only 5
        msgs = buf.get_since("c1", max_count=5)
        assert len(msgs) == 5
        assert msgs[0].timestamp_sec == 0.0

        # Next batch
        msgs = buf.get_since("c1", max_count=5)
        assert len(msgs) == 5
        assert msgs[0].timestamp_sec == 5.0

    def test_thread_safety(self):
        """Basic thread safety test — concurrent put and get."""
        buf = RingBuffer(capacity=1000)
        buf.register_consumer("c1")
        errors = []

        def writer():
            try:
                for i in range(500):
                    buf.put(_make_msg("/imu", float(i)))
            except Exception as e:
                errors.append(e)

        def reader():
            try:
                total = 0
                for _ in range(100):
                    msgs = buf.get_since("c1", max_count=50)
                    total += len(msgs)
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=writer)
        t2 = threading.Thread(target=reader)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(errors) == 0
