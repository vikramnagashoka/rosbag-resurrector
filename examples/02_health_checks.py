"""Health checks — automatic 0–100 quality score per bag.

Demonstrates: ``BagFrame.health_report()`` and
``HealthChecker / HealthConfig`` for per-robot threshold tuning.

Run:
    python examples/02_health_checks.py

What you'll see: a healthy bag's report, then the same checker run
with stricter thresholds, then the issues categorized by severity.
"""

from __future__ import annotations

from _common import ensure_sample_bag, header, section

from resurrector import BagFrame
from resurrector.ingest.health_check import HealthChecker, HealthConfig


def main() -> None:
    header("02 — Health checks")
    bag_path = ensure_sample_bag()
    bf = BagFrame(bag_path)

    section("Default health report")
    report = bf.health_report()
    print(f"  Overall score: {report.score}/100")
    print(f"  Issues:        {len(report.issues)}")
    print(f"  Warnings:      {len(report.warnings)}")
    print(f"  Topic scores:  {len(report.topic_scores)} topics analyzed")

    if report.recommendations:
        print(f"\n  Recommendations:")
        for r in report.recommendations[:3]:
            print(f"    • {r}")

    section("Per-topic scores")
    for topic_name, topic_score in sorted(
        report.topic_scores.items(), key=lambda kv: kv[1].score,
    )[:5]:
        print(f"  {topic_score.score:>3}/100  {topic_name:<30}  "
              f"{len(topic_score.issues)} issue(s)")

    section("Issues by severity")
    by_severity: dict[str, list] = {}
    for issue in report.issues:
        by_severity.setdefault(issue.severity.value, []).append(issue)
    for sev in ("critical", "error", "warning", "info"):
        items = by_severity.get(sev, [])
        if items:
            print(f"  {sev.upper()}: {len(items)}")
            for i in items[:3]:
                print(f"    • {i.check_name}: {i.message[:70]}")

    section("Custom thresholds — be stricter about gaps")
    # Default flags gaps when one is >2x the expected period.
    # Tighten to 1.5x for a noisier robot platform.
    strict_config = HealthConfig(
        rate_drop_threshold=0.10,      # 10% drop is suspicious (default 25%)
        gap_multiplier=1.5,            # 1.5x period flags a gap (default 2x)
        completeness_threshold=0.02,   # only 2% start/end delay tolerated
        size_deviation_threshold=0.3,  # 30% size variance flags it
    )
    strict_checker = HealthChecker(strict_config)
    strict_report = bf.health_report_with(strict_checker) \
        if hasattr(bf, "health_report_with") else None

    # Fallback: run checker manually if BagFrame doesn't expose the helper.
    if strict_report is None:
        from resurrector.ingest.parser import parse_bag
        topic_timestamps: dict[str, list[int]] = {}
        topic_sizes: dict[str, list[int]] = {}
        parser = parse_bag(bag_path)
        for msg in parser.read_messages():
            topic_timestamps.setdefault(msg.topic, []).append(msg.timestamp_ns)
            if msg.raw_data:
                topic_sizes.setdefault(msg.topic, []).append(len(msg.raw_data))
        strict_report = strict_checker.run_all_checks(
            topic_timestamps=topic_timestamps,
            topic_message_sizes=topic_sizes,
            bag_start_ns=bf.metadata.start_time_ns,
            bag_end_ns=bf.metadata.end_time_ns,
        )

    print(f"  Default score:  {report.score}/100  ({len(report.issues)} issues)")
    print(f"  Strict score:   {strict_report.score}/100  ({len(strict_report.issues)} issues)")

    print(
        "\n  ✓ Use HealthConfig to tune thresholds for your robot platform.\n"
        "    A 200Hz IMU on a quadruped is different from a 10Hz lidar on\n"
        "    a logistics bot; one set of defaults can't fit both.\n"
    )


if __name__ == "__main__":
    main()
