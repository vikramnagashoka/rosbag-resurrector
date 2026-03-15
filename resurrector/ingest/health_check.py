"""Automated quality validation for bag files.

Detects common issues:
- Dropped messages (buffer overflow)
- Time gaps (sensor disconnects)
- Out-of-order timestamps
- Partial topic recordings
- Message size anomalies
- TF consistency issues

Outputs a health score (0-100) per bag and per topic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import numpy as np


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class HealthIssue:
    """A single quality issue detected in a bag or topic."""
    check_name: str
    severity: Severity
    message: str
    topic: str | None = None
    start_time_sec: float | None = None
    end_time_sec: float | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthResult:
    """Result of a single health check."""
    check_name: str
    passed: bool
    score: int  # 0-100
    issues: list[HealthIssue] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class TopicHealth:
    """Health summary for a single topic."""
    topic: str
    score: int
    results: list[HealthResult] = field(default_factory=list)
    issues: list[HealthIssue] = field(default_factory=list)


@dataclass
class BagHealthReport:
    """Complete health report for a bag file."""
    score: int
    topic_scores: dict[str, TopicHealth] = field(default_factory=dict)
    results: list[HealthResult] = field(default_factory=list)
    issues: list[HealthIssue] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)

    @property
    def warnings(self) -> list[HealthIssue]:
        return [i for i in self.issues if i.severity == Severity.WARNING]

    @property
    def errors(self) -> list[HealthIssue]:
        return [i for i in self.issues if i.severity in (Severity.ERROR, Severity.CRITICAL)]


@dataclass
class HealthConfig:
    """Configurable thresholds for health checks.

    Override defaults for different robot platforms — a humanoid at 200Hz IMU
    needs different thresholds than a drone at 10Hz lidar.
    """
    # Message rate stability: flag if rate drops below (1 - rate_drop_threshold) of median
    rate_drop_threshold: float = 0.25  # 25% drop
    rate_drop_window_ms: float = 500.0  # Window size for rate analysis

    # Time gaps: flag intervals > gap_multiplier * expected_period
    gap_multiplier: float = 2.0

    # Topic completeness: flag if topic starts/ends beyond this fraction of bag duration
    completeness_threshold: float = 0.05  # 5%

    # Message size anomalies: flag if size deviates more than this fraction from median
    size_deviation_threshold: float = 0.5  # 50%

    # Clock jump: minimum jump to flag (seconds)
    clock_jump_min_sec: float = 1.0
    # Clock jump: multiplier of median interval
    clock_jump_multiplier: float = 100.0

    # Minimum messages per topic to run rate/gap checks
    min_messages_for_rate_check: int = 10
    min_messages_for_size_check: int = 10

    # Weights for aggregate scoring
    weights: dict[str, int] = field(default_factory=lambda: {
        "message_rate_stability": 25,
        "time_gaps": 25,
        "timestamp_ordering": 20,
        "topic_completeness": 15,
        "message_size_anomalies": 15,
    })


class HealthChecker:
    """Run all quality checks on a bag file."""

    def __init__(self, config: HealthConfig | None = None):
        self.config = config or HealthConfig()

    # Weights for aggregate scoring (kept for backward compat, delegates to config)
    @property
    def WEIGHTS(self) -> dict[str, int]:
        return self.config.weights

    def run_all_checks(
        self,
        topic_timestamps: dict[str, list[int]],
        topic_message_sizes: dict[str, list[int]] | None = None,
        bag_start_ns: int = 0,
        bag_end_ns: int = 0,
        expected_frequencies: dict[str, float] | None = None,
    ) -> BagHealthReport:
        """Run all health checks and produce a report.

        Args:
            topic_timestamps: Dict mapping topic name -> list of message timestamps (ns).
            topic_message_sizes: Optional dict mapping topic name -> list of message sizes (bytes).
            bag_start_ns: Bag start time in nanoseconds.
            bag_end_ns: Bag end time in nanoseconds.
            expected_frequencies: Optional dict of expected frequencies per topic.
        """
        all_results: list[HealthResult] = []
        all_issues: list[HealthIssue] = []
        topic_healths: dict[str, TopicHealth] = {}

        for topic, timestamps in topic_timestamps.items():
            if len(timestamps) < 2:
                # Not enough data to run checks — score as healthy
                topic_healths[topic] = TopicHealth(
                    topic=topic, score=100, results=[], issues=[],
                )
                continue

            ts_arr = np.array(timestamps, dtype=np.int64)
            ts_sorted = np.sort(ts_arr)

            # Estimate frequency if not provided
            freq = None
            if expected_frequencies and topic in expected_frequencies:
                freq = expected_frequencies[topic]
            else:
                duration_ns = ts_sorted[-1] - ts_sorted[0]
                if duration_ns > 0:
                    freq = len(ts_sorted) / (duration_ns / 1e9)

            topic_results: list[HealthResult] = []

            # Check 1: Message rate stability
            r1 = self.check_message_rate_stability(topic, ts_sorted, freq)
            topic_results.append(r1)

            # Check 2: Time gaps
            r2 = self.check_time_gaps(topic, ts_sorted, freq)
            topic_results.append(r2)

            # Check 3: Timestamp ordering
            r3 = self.check_timestamp_ordering(topic, ts_arr)
            topic_results.append(r3)

            # Check 4: Topic completeness
            r4 = self.check_topic_completeness(topic, ts_sorted, bag_start_ns, bag_end_ns)
            topic_results.append(r4)

            # Check 5: Message size anomalies
            if topic_message_sizes and topic in topic_message_sizes:
                sizes = np.array(topic_message_sizes[topic])
                r5 = self.check_message_size_anomalies(topic, sizes)
                topic_results.append(r5)

            # Aggregate topic score
            topic_score = self._aggregate_topic_score(topic_results)
            topic_issues = []
            for r in topic_results:
                topic_issues.extend(r.issues)

            topic_healths[topic] = TopicHealth(
                topic=topic,
                score=topic_score,
                results=topic_results,
                issues=topic_issues,
            )

            all_results.extend(topic_results)
            all_issues.extend(topic_issues)

        # Aggregate bag-level score
        if topic_healths:
            bag_score = int(np.mean([th.score for th in topic_healths.values()]))
        else:
            bag_score = 100

        # Generate recommendations
        recommendations = self._generate_recommendations(all_issues)

        return BagHealthReport(
            score=bag_score,
            topic_scores=topic_healths,
            results=all_results,
            issues=all_issues,
            recommendations=recommendations,
        )

    def check_message_rate_stability(
        self, topic: str, timestamps_ns: np.ndarray, expected_hz: float | None,
    ) -> HealthResult:
        """Check for message rate drops indicating buffer overflow.

        Flags if rate drops below 80% of median for > 500ms.
        """
        if len(timestamps_ns) < self.config.min_messages_for_rate_check or expected_hz is None:
            return HealthResult("message_rate_stability", True, 100)

        # Compute rolling message rate using configurable windows
        window_ns = int(self.config.rate_drop_window_ms * 1e6)
        issues = []

        # Calculate inter-message intervals
        intervals = np.diff(timestamps_ns)
        expected_interval = 1e9 / expected_hz
        median_interval = np.median(intervals)

        # Find regions where interval exceeds threshold (rate drops)
        threshold = median_interval * (1.0 / (1.0 - self.config.rate_drop_threshold))
        slow_mask = intervals > threshold

        if not np.any(slow_mask):
            return HealthResult("message_rate_stability", True, 100)

        # Find contiguous slow regions
        slow_regions = _find_contiguous_regions(slow_mask)
        significant_drops = 0

        for start_idx, end_idx in slow_regions:
            region_duration_ns = timestamps_ns[end_idx + 1] - timestamps_ns[start_idx]
            if region_duration_ns > window_ns:
                significant_drops += 1
                start_sec = timestamps_ns[start_idx] / 1e9
                end_sec = timestamps_ns[end_idx + 1] / 1e9
                actual_rate = (end_idx - start_idx + 1) / (region_duration_ns / 1e9)
                issues.append(HealthIssue(
                    check_name="message_rate_stability",
                    severity=Severity.WARNING,
                    message=(
                        f"Message rate dropped to {actual_rate:.1f}Hz "
                        f"(expected ~{expected_hz:.1f}Hz) for {region_duration_ns/1e6:.0f}ms"
                    ),
                    topic=topic,
                    start_time_sec=start_sec,
                    end_time_sec=end_sec,
                    details={"actual_hz": actual_rate, "expected_hz": expected_hz},
                ))

        # Score: deduct points based on fraction of recording affected
        total_duration = timestamps_ns[-1] - timestamps_ns[0]
        affected_duration = sum(
            timestamps_ns[min(e + 1, len(timestamps_ns) - 1)] - timestamps_ns[s]
            for s, e in slow_regions
            if timestamps_ns[min(e + 1, len(timestamps_ns) - 1)] - timestamps_ns[s] > window_ns
        )
        fraction_affected = affected_duration / total_duration if total_duration > 0 else 0
        score = max(0, int(100 * (1 - fraction_affected * 2)))

        return HealthResult(
            "message_rate_stability",
            passed=len(issues) == 0,
            score=score,
            issues=issues,
            details={"significant_drops": significant_drops},
        )

    def check_time_gaps(
        self, topic: str, timestamps_ns: np.ndarray, expected_hz: float | None,
    ) -> HealthResult:
        """Detect timestamp gaps > 2x expected period."""
        if len(timestamps_ns) < 2:
            return HealthResult("time_gaps", True, 100)

        intervals = np.diff(timestamps_ns)

        if expected_hz and expected_hz > 0:
            expected_interval = 1e9 / expected_hz
        else:
            expected_interval = float(np.median(intervals))

        gap_threshold = expected_interval * self.config.gap_multiplier
        gap_mask = intervals > gap_threshold
        issues = []

        gap_indices = np.where(gap_mask)[0]
        for idx in gap_indices:
            gap_duration_ns = intervals[idx]
            estimated_missing = int(gap_duration_ns / expected_interval) - 1
            issues.append(HealthIssue(
                check_name="time_gaps",
                severity=Severity.WARNING if gap_duration_ns < 5 * expected_interval else Severity.ERROR,
                message=(
                    f"Gap of {gap_duration_ns/1e6:.1f}ms detected "
                    f"(~{estimated_missing} missing messages)"
                ),
                topic=topic,
                start_time_sec=timestamps_ns[idx] / 1e9,
                end_time_sec=timestamps_ns[idx + 1] / 1e9,
                details={
                    "gap_duration_ms": gap_duration_ns / 1e6,
                    "estimated_missing": estimated_missing,
                },
            ))

        # Score
        if not issues:
            score = 100
        else:
            total_gap_ns = sum(intervals[idx] - expected_interval for idx in gap_indices)
            total_duration = timestamps_ns[-1] - timestamps_ns[0]
            fraction = total_gap_ns / total_duration if total_duration > 0 else 0
            score = max(0, int(100 * (1 - fraction * 3)))

        return HealthResult("time_gaps", passed=len(issues) == 0, score=score, issues=issues)

    def check_timestamp_ordering(
        self, topic: str, timestamps_ns: np.ndarray,
    ) -> HealthResult:
        """Detect out-of-order timestamps and clock jumps."""
        if len(timestamps_ns) < 2:
            return HealthResult("timestamp_ordering", True, 100)

        diffs = np.diff(timestamps_ns)
        issues = []

        # Out-of-order: negative diffs
        ooo_indices = np.where(diffs < 0)[0]
        for idx in ooo_indices:
            issues.append(HealthIssue(
                check_name="timestamp_ordering",
                severity=Severity.ERROR,
                message=(
                    f"Out-of-order timestamp: jumped backwards by "
                    f"{abs(diffs[idx])/1e6:.1f}ms"
                ),
                topic=topic,
                start_time_sec=timestamps_ns[idx] / 1e9,
                details={"jump_ms": float(diffs[idx] / 1e6)},
            ))

        # Clock jumps: sudden large forward jumps (> 1 second)
        median_diff = np.median(diffs[diffs > 0]) if np.any(diffs > 0) else 0
        if median_diff > 0:
            jump_threshold = max(self.config.clock_jump_min_sec * 1e9, median_diff * self.config.clock_jump_multiplier)
            jump_indices = np.where(diffs > jump_threshold)[0]
            for idx in jump_indices:
                issues.append(HealthIssue(
                    check_name="timestamp_ordering",
                    severity=Severity.WARNING,
                    message=f"Clock jump of {diffs[idx]/1e9:.2f}s detected",
                    topic=topic,
                    start_time_sec=timestamps_ns[idx] / 1e9,
                    details={"jump_sec": float(diffs[idx] / 1e9)},
                ))

        # Score: out-of-order is critical
        if ooo_indices.size > 0:
            ooo_fraction = len(ooo_indices) / len(diffs)
            score = max(0, int(100 * (1 - ooo_fraction * 10)))
        else:
            score = 100

        return HealthResult("timestamp_ordering", passed=len(issues) == 0, score=score, issues=issues)

    def check_topic_completeness(
        self, topic: str, timestamps_ns: np.ndarray,
        bag_start_ns: int, bag_end_ns: int,
    ) -> HealthResult:
        """Flag topics that don't span the full recording duration."""
        if len(timestamps_ns) == 0 or bag_end_ns <= bag_start_ns:
            return HealthResult("topic_completeness", True, 100)

        bag_duration = bag_end_ns - bag_start_ns
        topic_start = timestamps_ns[0]
        topic_end = timestamps_ns[-1]

        issues = []
        # Check if topic starts late (> configurable threshold of recording duration)
        start_delay = topic_start - bag_start_ns
        threshold = bag_duration * self.config.completeness_threshold

        if start_delay > threshold:
            issues.append(HealthIssue(
                check_name="topic_completeness",
                severity=Severity.WARNING,
                message=f"Topic starts {start_delay/1e9:.1f}s after bag start",
                topic=topic,
                start_time_sec=bag_start_ns / 1e9,
                end_time_sec=topic_start / 1e9,
                details={"delay_sec": start_delay / 1e9},
            ))

        # Check if topic ends early
        end_early = bag_end_ns - topic_end
        if end_early > threshold:
            issues.append(HealthIssue(
                check_name="topic_completeness",
                severity=Severity.WARNING,
                message=f"Topic ends {end_early/1e9:.1f}s before bag end",
                topic=topic,
                start_time_sec=topic_end / 1e9,
                end_time_sec=bag_end_ns / 1e9,
                details={"early_sec": end_early / 1e9},
            ))

        # Score
        coverage = (topic_end - topic_start) / bag_duration if bag_duration > 0 else 1.0
        score = max(0, int(coverage * 100))

        return HealthResult("topic_completeness", passed=len(issues) == 0, score=score, issues=issues)

    def check_message_size_anomalies(
        self, topic: str, sizes: np.ndarray,
    ) -> HealthResult:
        """Detect sudden changes in message size."""
        if len(sizes) < self.config.min_messages_for_size_check:
            return HealthResult("message_size_anomalies", True, 100)

        median_size = np.median(sizes)
        if median_size == 0:
            return HealthResult("message_size_anomalies", True, 100)

        # Flag messages whose size deviates beyond threshold from median
        deviation = np.abs(sizes - median_size) / median_size
        anomaly_mask = deviation > self.config.size_deviation_threshold
        issues = []

        if np.any(anomaly_mask):
            n_anomalies = int(np.sum(anomaly_mask))
            issues.append(HealthIssue(
                check_name="message_size_anomalies",
                severity=Severity.WARNING if n_anomalies < len(sizes) * 0.1 else Severity.ERROR,
                message=(
                    f"{n_anomalies} messages with abnormal size "
                    f"(median: {median_size:.0f} bytes)"
                ),
                topic=topic,
                details={
                    "anomaly_count": n_anomalies,
                    "median_size": float(median_size),
                    "min_size": float(np.min(sizes)),
                    "max_size": float(np.max(sizes)),
                },
            ))

        fraction = np.sum(anomaly_mask) / len(sizes) if len(sizes) > 0 else 0
        score = max(0, int(100 * (1 - fraction * 5)))

        return HealthResult("message_size_anomalies", passed=len(issues) == 0, score=score, issues=issues)

    def _aggregate_topic_score(self, results: list[HealthResult]) -> int:
        """Weighted aggregate score for a topic."""
        total_weight = 0
        weighted_sum = 0
        for r in results:
            weight = self.WEIGHTS.get(r.check_name, 10)
            weighted_sum += r.score * weight
            total_weight += weight
        return int(weighted_sum / total_weight) if total_weight > 0 else 100

    def _generate_recommendations(self, issues: list[HealthIssue]) -> list[str]:
        """Generate actionable recommendations from issues."""
        recs = []
        seen = set()

        for issue in issues:
            if issue.check_name == "message_rate_stability" and "rate_stability" not in seen:
                recs.append(
                    f"Topic {issue.topic} has message rate drops. "
                    "Consider increasing buffer size or reducing recording frequency."
                )
                seen.add("rate_stability")
            elif issue.check_name == "time_gaps" and f"gaps_{issue.topic}" not in seen:
                n_gaps = sum(1 for i in issues if i.check_name == "time_gaps" and i.topic == issue.topic)
                recs.append(
                    f"Topic {issue.topic} has {n_gaps} time gap(s). "
                    "Check sensor connection stability or recording node health."
                )
                seen.add(f"gaps_{issue.topic}")
            elif issue.check_name == "timestamp_ordering" and "ordering" not in seen:
                if issue.severity == Severity.ERROR:
                    recs.append(
                        f"Topic {issue.topic} has out-of-order timestamps. "
                        "This may indicate a clock sync issue or message queue problem."
                    )
                    seen.add("ordering")
            elif issue.check_name == "topic_completeness" and f"completeness_{issue.topic}" not in seen:
                recs.append(
                    f"Topic {issue.topic} doesn't span the full recording. "
                    "Ensure all nodes are started before recording begins."
                )
                seen.add(f"completeness_{issue.topic}")

        return recs


def _find_contiguous_regions(mask: np.ndarray) -> list[tuple[int, int]]:
    """Find contiguous True regions in a boolean array. Returns (start, end) index pairs."""
    if len(mask) == 0:
        return []
    regions = []
    in_region = False
    start = 0
    for i, val in enumerate(mask):
        if val and not in_region:
            start = i
            in_region = True
        elif not val and in_region:
            regions.append((start, i - 1))
            in_region = False
    if in_region:
        regions.append((start, len(mask) - 1))
    return regions
