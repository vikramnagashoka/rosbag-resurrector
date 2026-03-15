"""Tests for smart topic grouping."""

import pytest

from resurrector.core.topic_groups import classify_topics, get_topic_group, TopicGroup


class TestClassifyTopics:
    def test_perception_group(self):
        topics = ["/camera/rgb", "/lidar/scan", "/camera/depth"]
        groups = classify_topics(topics)
        names = {g.name for g in groups}
        assert "Perception" in names
        perception = next(g for g in groups if g.name == "Perception")
        assert len(perception.topics) == 3

    def test_state_group(self):
        topics = ["/imu/data", "/joint_states", "/odom"]
        groups = classify_topics(topics)
        names = {g.name for g in groups}
        assert "State" in names

    def test_navigation_group(self):
        topics = ["/cmd_vel", "/nav/path", "/costmap"]
        groups = classify_topics(topics)
        names = {g.name for g in groups}
        assert "Navigation" in names

    def test_other_group(self):
        topics = ["/my_custom_topic", "/another_unknown"]
        groups = classify_topics(topics)
        names = {g.name for g in groups}
        assert "Other" in names
        other = next(g for g in groups if g.name == "Other")
        assert len(other.topics) == 2

    def test_mixed_topics(self):
        topics = ["/camera/rgb", "/imu/data", "/cmd_vel", "/custom"]
        groups = classify_topics(topics)
        names = {g.name for g in groups}
        assert "Perception" in names
        assert "State" in names
        assert "Navigation" in names
        assert "Other" in names

    def test_empty_list(self):
        groups = classify_topics([])
        assert groups == []

    def test_custom_patterns(self):
        topics = ["/my_sensor/data"]
        groups = classify_topics(topics, custom_patterns={"MySensors": ["/my_sensor"]})
        names = {g.name for g in groups}
        assert "MySensors" in names

    def test_tf_group(self):
        topics = ["/tf", "/tf_static"]
        groups = classify_topics(topics)
        names = {g.name for g in groups}
        assert "Transforms" in names


class TestGetTopicGroup:
    def test_imu(self):
        assert get_topic_group("/imu/data") == "State"

    def test_camera(self):
        assert get_topic_group("/camera/rgb") == "Perception"

    def test_cmd_vel(self):
        assert get_topic_group("/cmd_vel") == "Navigation"

    def test_unknown(self):
        assert get_topic_group("/random_thing") == "Other"
