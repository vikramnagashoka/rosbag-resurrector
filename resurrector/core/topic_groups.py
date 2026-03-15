"""Smart topic grouping — auto-detect and label topic groups.

Categorizes ROS topics into semantic groups based on naming patterns:
Perception, State, Navigation, Control, Diagnostics, etc.
"""

from __future__ import annotations

from dataclasses import dataclass

# Default topic group patterns — matched against topic names (case-insensitive)
DEFAULT_GROUP_PATTERNS: dict[str, list[str]] = {
    "Perception": [
        "/camera", "/image", "/rgb", "/depth", "/stereo",
        "/lidar", "/velodyne", "/points", "/pointcloud",
        "/radar", "/sonar", "/ultrasonic",
    ],
    "State": [
        "/imu", "/joint_states", "/joint_state",
        "/odom", "/odometry", "/pose", "/twist",
        "/battery", "/temperature", "/pressure",
    ],
    "Navigation": [
        "/cmd_vel", "/nav", "/path", "/plan",
        "/goal", "/costmap", "/map", "/amcl",
        "/move_base", "/waypoint",
    ],
    "Control": [
        "/joint_command", "/joint_trajectory", "/effort",
        "/controller", "/pid", "/servo", "/actuator",
        "/gripper", "/hand",
    ],
    "Transforms": [
        "/tf", "/tf_static",
    ],
    "Diagnostics": [
        "/diagnostics", "/rosout", "/parameter_events",
        "/clock", "/statistics",
    ],
}


@dataclass
class TopicGroup:
    """A named group of related topics."""
    name: str
    topics: list[str]


def classify_topics(
    topic_names: list[str],
    custom_patterns: dict[str, list[str]] | None = None,
) -> list[TopicGroup]:
    """Classify a list of topic names into semantic groups.

    Args:
        topic_names: List of ROS topic names (e.g., "/camera/rgb", "/imu/data").
        custom_patterns: Optional custom patterns to override/extend defaults.

    Returns:
        List of TopicGroups. Topics not matching any pattern go into "Other".
    """
    patterns = dict(DEFAULT_GROUP_PATTERNS)
    if custom_patterns:
        patterns.update(custom_patterns)

    groups: dict[str, list[str]] = {}
    classified = set()

    for topic in topic_names:
        topic_lower = topic.lower()
        matched = False
        for group_name, prefixes in patterns.items():
            for prefix in prefixes:
                if prefix.lower() in topic_lower:
                    groups.setdefault(group_name, []).append(topic)
                    classified.add(topic)
                    matched = True
                    break
            if matched:
                break

    # Unclassified topics go to "Other"
    unclassified = [t for t in topic_names if t not in classified]
    if unclassified:
        groups["Other"] = unclassified

    # Return in a stable order: known groups first, then "Other"
    ordered_names = [g for g in patterns if g in groups]
    if "Other" in groups:
        ordered_names.append("Other")

    return [TopicGroup(name=n, topics=sorted(groups[n])) for n in ordered_names]


def get_topic_group(
    topic_name: str,
    custom_patterns: dict[str, list[str]] | None = None,
) -> str:
    """Return the group name for a single topic."""
    patterns = dict(DEFAULT_GROUP_PATTERNS)
    if custom_patterns:
        patterns.update(custom_patterns)

    topic_lower = topic_name.lower()
    for group_name, prefixes in patterns.items():
        for prefix in prefixes:
            if prefix.lower() in topic_lower:
                return group_name
    return "Other"
