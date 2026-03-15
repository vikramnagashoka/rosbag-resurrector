"""Resurrector Bridge — stream bag data over WebSocket for live visualization.

Supports PlotJuggler-compatible protocol and includes a built-in web viewer.

Modes:
- Playback: replay recorded MCAP bags at configurable speed
- Live: relay ROS2 topics in real-time (requires rclpy)

Usage:
    resurrector bridge playback experiment.mcap --port 9090
    resurrector bridge live --port 9090
"""
