"""Automated quality validation for bag files.

Detects common issues:
- Dropped messages (buffer overflow)
- Time gaps (sensor disconnects)
- Out-of-order timestamps
- Partial topic recordings
- Message size anomalies
- TF consistency issues

Outputs a health score (0-100) per bag and per topic.

v0.4.0 streaming refactor: the v0.3.x version accumulated full
``list[int]`` of per-topic timestamps (and message sizes) before
running the checks, which on a 100M-message bag was ~800 MB just for
the int64s. This version maintains a small fixed-size ``TopicHealthState``
per topic (running first/last/prev/count/Welford accumulators + an
inline gap/rate/ordering issue collector) and processes messages
one at a time. Memory becomes O(num_topics * constant), independent of
bag size.

Backward compatibility: the v0.3.x ``HealthChecker.run_all_checks(
topic_timestamps=..., topic_message_sizes=...)`` signature is preserved.
The new streaming entry point is
``HealthChecker.run_streaming(states_dict, bag_start_ns, bag_end_ns,
expected_frequencies)``. Both produce the same ``BagHealthReport`` shape.
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


# ---------------------------------------------------------------------------
# Streaming state object — the v0.4.0 path
# ---------------------------------------------------------------------------

@dataclass
class TopicHealthState:
    """Running per-topic state for streaming health analysis.

    All fields are O(1) memory except ``rate_drop_regions`` and
    ``gap_issues`` which collect detected anomalies inline as they're
    spotted. Anomalies are bounded by the number of *gaps in the data*,
    not by the number of *messages* — even a pathologically broken
    100M-message recording typically has < 1000 distinct gaps.

    Attributes:
        count: Total messages seen on this topic.
        first_ts: Timestamp (ns) of the first message.
        last_ts: Timestamp (ns) of the most recent message.
        prev_ts: Same as last_ts; tracked separately for clarity.
        out_of_order_count: Number of times we saw ts < prev_ts.
        max_gap_ns: Largest single inter-message interval seen.
        ooo_issues: Out-of-order anomalies emitted inline (capped at
            MAX_INLINE_ISSUES per category).
        gap_issues: Time-gap anomalies emitted inline.
        rate_drop_regions: Contiguous slow regions tracked via a
            simple state machine; finalized into HealthIssue objects.
        size_count, size_mean, size_m2: Welford accumulators for
            message-size statistics. NaN-safe via the count guard.
        size_min, size_max: For the size-anomaly check's reporting.
        size_anomaly_count: Number of messages whose size exceeded the
            running deviation threshold (compared against running
            mean ± size_deviation * mean).
    """
    count: int = 0
    first_ts: int = 0
    last_ts: int = 0
    prev_ts: int = 0
    out_of_order_count: int = 0
    max_gap_ns: int = 0

    ooo_issues: list[HealthIssue] = field(default_factory=list)
    gap_issues: list[HealthIssue] = field(default_factory=list)
    clock_jump_issues: list[HealthIssue] = field(default_factory=list)

    # Rate-drop region tracking (state machine)
    in_slow_region: bool = False
    slow_region_start_ts: int = 0
    slow_region_count: int = 0
    rate_drop_issues: list[HealthIssue] = field(default_factory=list)

    # Welford accumulators for message size
    size_count: int = 0
    size_mean: float = 0.0
    size_m2: float = 0.0
    size_min: int = 0
    size_max: int = 0
    size_anomaly_count: int = 0


# Cap on how many inline issues to emit per category per topic.
# Beyond this we stop appending — the issue count alone is useful and
# we don't want to OOM on a pathologically broken bag.
MAX_INLINE_ISSUES = 100


def update_state(
    state: TopicHealthState,
    topic: str,
    timestamp_ns: int,
    message_size: int | None,
    config: HealthConfig,
    expected_interval_ns: float | None = None,
) -> None:
    """Process a single message into the topic's running state.

    Time complexity: O(1). Memory complexity: also O(1) (we only ever
    append to bounded issue lists).

    Args:
        state: Mutable per-topic state object.
        topic: Topic name (carried into HealthIssue.topic).
        timestamp_ns: Message timestamp.
        message_size: Optional payload size in bytes; if None, size
            check is skipped for this message.
        config: Health thresholds.
        expected_interval_ns: Optional pre-computed expected interval
            (1e9 / expected_hz). If None, we use a running estimate.
    """
    if state.count == 0:
        state.count = 1
        state.first_ts = timestamp_ns
        state.last_ts = timestamp_ns
        state.prev_ts = timestamp_ns
        if message_size is not None:
            state.size_count = 1
            state.size_mean = float(message_size)
            state.size_min = message_size
            state.size_max = message_size
        return

    # Inter-message interval based on the previous timestamp.
    interval_ns = timestamp_ns - state.prev_ts

    # --- Out-of-order check ---
    if interval_ns < 0:
        state.out_of_order_count += 1
        if len(state.ooo_issues) < MAX_INLINE_ISSUES:
            state.ooo_issues.append(HealthIssue(
                check_name="timestamp_ordering",
                severity=Severity.ERROR,
                message=(
                    f"Out-of-order timestamp: jumped backwards by "
                    f"{abs(interval_ns)/1e6:.1f}ms"
                ),
                topic=topic,
                start_time_sec=state.prev_ts / 1e9,
                details={"jump_ms": float(interval_ns / 1e6)},
            ))
    else:
        # Update max_gap only on forward intervals.
        if interval_ns > state.max_gap_ns:
            state.max_gap_ns = interval_ns

        # --- Time gaps (forward jumps) check ---
        # Use expected_interval if provided, otherwise wait for enough
        # data to estimate from the running rate.
        if expected_interval_ns is None:
            # Running estimate: total_duration / count
            running_dur_ns = timestamp_ns - state.first_ts
            if state.count >= config.min_messages_for_rate_check:
                expected_interval_ns = running_dur_ns / state.count
            # else: skip the gap check until we have data

        if expected_interval_ns is not None and expected_interval_ns > 0:
            # Time gap: interval > gap_multiplier * expected
            gap_threshold = expected_interval_ns * config.gap_multiplier
            if interval_ns > gap_threshold:
                if len(state.gap_issues) < MAX_INLINE_ISSUES:
                    estimated_missing = max(
                        0,
                        int(interval_ns / expected_interval_ns) - 1,
                    )
                    state.gap_issues.append(HealthIssue(
                        check_name="time_gaps",
                        severity=(
                            Severity.WARNING
                            if interval_ns < 5 * expected_interval_ns
                            else Severity.ERROR
                        ),
                        message=(
                            f"Gap of {interval_ns/1e6:.1f}ms detected "
                            f"(~{estimated_missing} missing messages)"
                        ),
                        topic=topic,
                        start_time_sec=state.prev_ts / 1e9,
                        end_time_sec=timestamp_ns / 1e9,
                        details={
                            "gap_duration_ms": interval_ns / 1e6,
                            "estimated_missing": estimated_missing,
                        },
                    ))

            # --- Clock jump check (large forward jumps) ---
            jump_threshold_ns = max(
                config.clock_jump_min_sec * 1e9,
                expected_interval_ns * config.clock_jump_multiplier,
            )
            if interval_ns > jump_threshold_ns:
                if len(state.clock_jump_issues) < MAX_INLINE_ISSUES:
                    state.clock_jump_issues.append(HealthIssue(
                        check_name="timestamp_ordering",
                        severity=Severity.WARNING,
                        message=f"Clock jump of {interval_ns/1e9:.2f}s detected",
                        topic=topic,
                        start_time_sec=state.prev_ts / 1e9,
                        details={"jump_sec": float(interval_ns / 1e9)},
                    ))

            # --- Rate-drop region tracking (state machine) ---
            # A "slow" interval is one where interval > median * (1 / (1 - drop_threshold))
            # We use expected_interval as the median proxy.
            slow_threshold_ns = expected_interval_ns * (
                1.0 / (1.0 - config.rate_drop_threshold)
            )
            window_ns = config.rate_drop_window_ms * 1e6
            is_slow = interval_ns > slow_threshold_ns

            if is_slow and not state.in_slow_region:
                # Enter slow region.
                state.in_slow_region = True
                state.slow_region_start_ts = state.prev_ts
                state.slow_region_count = 1
            elif is_slow and state.in_slow_region:
                state.slow_region_count += 1
            elif not is_slow and state.in_slow_region:
                # Exit slow region — emit issue if it lasted long enough.
                region_dur = state.prev_ts - state.slow_region_start_ts
                if region_dur > window_ns:
                    if len(state.rate_drop_issues) < MAX_INLINE_ISSUES:
                        actual_rate = (
                            state.slow_region_count / (region_dur / 1e9)
                            if region_dur > 0 else 0.0
                        )
                        expected_hz = 1e9 / expected_interval_ns
                        state.rate_drop_issues.append(HealthIssue(
                            check_name="message_rate_stability",
                            severity=Severity.WARNING,
                            message=(
                                f"Message rate dropped to {actual_rate:.1f}Hz "
                                f"(expected ~{expected_hz:.1f}Hz) for "
                                f"{region_dur/1e6:.0f}ms"
                            ),
                            topic=topic,
                            start_time_sec=state.slow_region_start_ts / 1e9,
                            end_time_sec=state.prev_ts / 1e9,
                            details={
                                "actual_hz": actual_rate,
                                "expected_hz": expected_hz,
                            },
                        ))
                state.in_slow_region = False
                state.slow_region_count = 0

    state.prev_ts = timestamp_ns
    state.last_ts = timestamp_ns
    state.count += 1

    # --- Message size: Welford running mean + variance + min/max ---
    if message_size is not None:
        state.size_count += 1
        if state.size_count == 1:
            state.size_mean = float(message_size)
            state.size_min = message_size
            state.size_max = message_size
        else:
            delta = message_size - state.size_mean
            state.size_mean += delta / state.size_count
            delta2 = message_size - state.size_mean
            state.size_m2 += delta * delta2
            if message_size < state.size_min:
                state.size_min = message_size
            if message_size > state.size_max:
                state.size_max = message_size
        # Anomaly: deviation from running mean exceeds threshold.
        # Skip until we have enough data for a stable mean.
        if state.size_count >= config.min_messages_for_size_check:
            mean = state.size_mean
            if mean > 0:
                dev = abs(message_size - mean) / mean
                if dev > config.size_deviation_threshold:
                    state.size_anomaly_count += 1


def finalize_state(
    state: TopicHealthState,
    topic: str,
    bag_start_ns: int,
    bag_end_ns: int,
    config: HealthConfig,
    expected_hz: float | None = None,
) -> TopicHealth:
    """Convert a streaming state into a TopicHealth result.

    Closes any in-progress slow region, runs the topic-completeness
    check (which only needs first_ts / last_ts), and aggregates per-
    check scores into a single topic score.
    """
    # Close out any in-progress slow region.
    if state.in_slow_region:
        region_dur = state.last_ts - state.slow_region_start_ts
        window_ns = config.rate_drop_window_ms * 1e6
        if region_dur > window_ns:
            actual_rate = (
                state.slow_region_count / (region_dur / 1e9)
                if region_dur > 0 else 0.0
            )
            # Estimate expected rate from total messages / total duration.
            total_dur = state.last_ts - state.first_ts
            expected_rate_est = (
                state.count / (total_dur / 1e9) if total_dur > 0 else 0.0
            )
            if len(state.rate_drop_issues) < MAX_INLINE_ISSUES:
                state.rate_drop_issues.append(HealthIssue(
                    check_name="message_rate_stability",
                    severity=Severity.WARNING,
                    message=(
                        f"Message rate dropped to {actual_rate:.1f}Hz "
                        f"(expected ~{expected_rate_est:.1f}Hz) for "
                        f"{region_dur/1e6:.0f}ms"
                    ),
                    topic=topic,
                    start_time_sec=state.slow_region_start_ts / 1e9,
                    end_time_sec=state.last_ts / 1e9,
                    details={
                        "actual_hz": actual_rate,
                        "expected_hz": expected_rate_est,
                    },
                ))

    if state.count < 2:
        return TopicHealth(topic=topic, score=100, results=[], issues=[])

    # Score each check based on inline-collected issues + counters.
    total_dur_ns = state.last_ts - state.first_ts
    bag_dur_ns = bag_end_ns - bag_start_ns

    # message_rate_stability
    if state.rate_drop_issues:
        # Approximate fraction-affected: sum of distinct drop spans.
        affected_ns = sum(
            int((iss.end_time_sec - iss.start_time_sec) * 1e9)
            for iss in state.rate_drop_issues
            if iss.end_time_sec is not None and iss.start_time_sec is not None
        )
        frac = (affected_ns / total_dur_ns) if total_dur_ns > 0 else 0.0
        rate_score = max(0, int(100 * (1 - frac * 2)))
    else:
        rate_score = 100
    rate_result = HealthResult(
        "message_rate_stability",
        passed=not state.rate_drop_issues,
        score=rate_score,
        issues=list(state.rate_drop_issues),
        details={"significant_drops": len(state.rate_drop_issues)},
    )

    # time_gaps
    if state.gap_issues:
        # Sum of excess time across all gaps (interval - expected).
        # We don't have per-issue interval; use max_gap_ns as a proxy
        # and the issue count for severity.
        # Use a coarse estimate: total excess ≈ sum of gap_duration_ms / 1e3.
        total_excess_ns = sum(
            int(iss.details.get("gap_duration_ms", 0) * 1e6)
            for iss in state.gap_issues
        )
        frac = (total_excess_ns / total_dur_ns) if total_dur_ns > 0 else 0.0
        gap_score = max(0, int(100 * (1 - frac * 3)))
    else:
        gap_score = 100
    gap_result = HealthResult(
        "time_gaps",
        passed=not state.gap_issues,
        score=gap_score,
        issues=list(state.gap_issues),
    )

    # timestamp_ordering = OOO + clock jumps
    ordering_issues = list(state.ooo_issues) + list(state.clock_jump_issues)
    if state.out_of_order_count > 0:
        ooo_frac = state.out_of_order_count / max(state.count - 1, 1)
        ordering_score = max(0, int(100 * (1 - ooo_frac * 10)))
    else:
        ordering_score = 100
    ordering_result = HealthResult(
        "timestamp_ordering",
        passed=not ordering_issues,
        score=ordering_score,
        issues=ordering_issues,
    )

    # topic_completeness
    completeness_issues: list[HealthIssue] = []
    if bag_dur_ns > 0:
        threshold_ns = bag_dur_ns * config.completeness_threshold
        start_delay = state.first_ts - bag_start_ns
        end_early = bag_end_ns - state.last_ts
        if start_delay > threshold_ns:
            completeness_issues.append(HealthIssue(
                check_name="topic_completeness",
                severity=Severity.WARNING,
                message=f"Topic starts {start_delay/1e9:.1f}s after bag start",
                topic=topic,
                start_time_sec=bag_start_ns / 1e9,
                end_time_sec=state.first_ts / 1e9,
                details={"delay_sec": start_delay / 1e9},
            ))
        if end_early > threshold_ns:
            completeness_issues.append(HealthIssue(
                check_name="topic_completeness",
                severity=Severity.WARNING,
                message=f"Topic ends {end_early/1e9:.1f}s before bag end",
                topic=topic,
                start_time_sec=state.last_ts / 1e9,
                end_time_sec=bag_end_ns / 1e9,
                details={"early_sec": end_early / 1e9},
            ))
        coverage = (state.last_ts - state.first_ts) / bag_dur_ns
        completeness_score = max(0, int(coverage * 100))
    else:
        completeness_score = 100
    completeness_result = HealthResult(
        "topic_completeness",
        passed=not completeness_issues,
        score=completeness_score,
        issues=completeness_issues,
    )

    # message_size_anomalies
    size_issues: list[HealthIssue] = []
    if state.size_count >= config.min_messages_for_size_check:
        if state.size_anomaly_count > 0:
            severity = (
                Severity.WARNING
                if state.size_anomaly_count < state.size_count * 0.1
                else Severity.ERROR
            )
            size_issues.append(HealthIssue(
                check_name="message_size_anomalies",
                severity=severity,
                message=(
                    f"{state.size_anomaly_count} messages with abnormal size "
                    f"(mean: {state.size_mean:.0f} bytes)"
                ),
                topic=topic,
                details={
                    "anomaly_count": state.size_anomaly_count,
                    "mean_size": state.size_mean,
                    "min_size": float(state.size_min),
                    "max_size": float(state.size_max),
                },
            ))
        frac = state.size_anomaly_count / state.size_count
        size_score = max(0, int(100 * (1 - frac * 5)))
    else:
        size_score = 100
    size_result = HealthResult(
        "message_size_anomalies",
        passed=not size_issues,
        score=size_score,
        issues=size_issues,
    )

    results = [
        rate_result, gap_result, ordering_result,
        completeness_result, size_result,
    ]
    issues = (
        rate_result.issues + gap_result.issues + ordering_result.issues
        + completeness_result.issues + size_result.issues
    )

    # Weighted aggregate score.
    total_weight = 0
    weighted_sum = 0
    for r in results:
        weight = config.weights.get(r.check_name, 10)
        weighted_sum += r.score * weight
        total_weight += weight
    score = int(weighted_sum / total_weight) if total_weight > 0 else 100

    return TopicHealth(
        topic=topic,
        score=score,
        results=results,
        issues=issues,
    )


# ---------------------------------------------------------------------------
# HealthChecker — keeps the v0.3.x bulk API for backward compat
# ---------------------------------------------------------------------------

class HealthChecker:
    """Run all quality checks on a bag file."""

    def __init__(self, config: HealthConfig | None = None):
        self.config = config or HealthConfig()

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
        """v0.3.x bulk API: feed pre-collected timestamps + sizes.

        Runs the legacy per-topic numpy checks. Preserved for tests
        and external callers; new code should use the streaming
        ``update_state`` / ``finalize_state`` helpers via
        ``BagFrame.health_report()``.
        """
        all_results: list[HealthResult] = []
        all_issues: list[HealthIssue] = []
        topic_healths: dict[str, TopicHealth] = {}

        for topic, timestamps in topic_timestamps.items():
            if len(timestamps) < 2:
                topic_healths[topic] = TopicHealth(
                    topic=topic, score=100, results=[], issues=[],
                )
                continue

            ts_arr = np.array(timestamps, dtype=np.int64)
            ts_sorted = np.sort(ts_arr)

            freq = None
            if expected_frequencies and topic in expected_frequencies:
                freq = expected_frequencies[topic]
            else:
                duration_ns = ts_sorted[-1] - ts_sorted[0]
                if duration_ns > 0:
                    freq = len(ts_sorted) / (duration_ns / 1e9)

            topic_results: list[HealthResult] = []
            topic_results.append(
                self.check_message_rate_stability(topic, ts_sorted, freq)
            )
            topic_results.append(
                self.check_time_gaps(topic, ts_sorted, freq)
            )
            topic_results.append(
                self.check_timestamp_ordering(topic, ts_arr)
            )
            topic_results.append(
                self.check_topic_completeness(
                    topic, ts_sorted, bag_start_ns, bag_end_ns,
                )
            )
            if topic_message_sizes and topic in topic_message_sizes:
                sizes = np.array(topic_message_sizes[topic])
                topic_results.append(
                    self.check_message_size_anomalies(topic, sizes)
                )

            topic_score = self._aggregate_topic_score(topic_results)
            topic_issues = []
            for r in topic_results:
                topic_issues.extend(r.issues)

            topic_healths[topic] = TopicHealth(
                topic=topic, score=topic_score,
                results=topic_results, issues=topic_issues,
            )
            all_results.extend(topic_results)
            all_issues.extend(topic_issues)

        if topic_healths:
            bag_score = int(np.mean([th.score for th in topic_healths.values()]))
        else:
            bag_score = 100

        recommendations = self._generate_recommendations(all_issues)

        return BagHealthReport(
            score=bag_score,
            topic_scores=topic_healths,
            results=all_results,
            issues=all_issues,
            recommendations=recommendations,
        )

    def run_streaming(
        self,
        states: dict[str, TopicHealthState],
        bag_start_ns: int,
        bag_end_ns: int,
        expected_frequencies: dict[str, float] | None = None,
    ) -> BagHealthReport:
        """v0.4.0 streaming API: finalize already-updated states.

        Caller is responsible for having already processed every
        message via ``update_state(state, ...)``. This method walks
        each state, computes per-topic results, and assembles the
        final report.
        """
        all_results: list[HealthResult] = []
        all_issues: list[HealthIssue] = []
        topic_healths: dict[str, TopicHealth] = {}

        for topic, state in states.items():
            expected_hz = (
                expected_frequencies.get(topic) if expected_frequencies else None
            )
            th = finalize_state(
                state, topic,
                bag_start_ns=bag_start_ns,
                bag_end_ns=bag_end_ns,
                config=self.config,
                expected_hz=expected_hz,
            )
            topic_healths[topic] = th
            all_results.extend(th.results)
            all_issues.extend(th.issues)

        if topic_healths:
            bag_score = int(np.mean([th.score for th in topic_healths.values()]))
        else:
            bag_score = 100

        recommendations = self._generate_recommendations(all_issues)

        return BagHealthReport(
            score=bag_score,
            topic_scores=topic_healths,
            results=all_results,
            issues=all_issues,
            recommendations=recommendations,
        )

    # -----------------------------------------------------------------
    # Legacy per-check methods — kept so existing tests work unchanged.
    # -----------------------------------------------------------------

    def check_message_rate_stability(
        self, topic: str, timestamps_ns: np.ndarray, expected_hz: float | None,
    ) -> HealthResult:
        """Check for message rate drops indicating buffer overflow."""
        if (
            len(timestamps_ns) < self.config.min_messages_for_rate_check
            or expected_hz is None
        ):
            return HealthResult("message_rate_stability", True, 100)

        window_ns = int(self.config.rate_drop_window_ms * 1e6)
        issues = []

        intervals = np.diff(timestamps_ns)
        median_interval = np.median(intervals)
        threshold = median_interval * (1.0 / (1.0 - self.config.rate_drop_threshold))
        slow_mask = intervals > threshold

        if not np.any(slow_mask):
            return HealthResult("message_rate_stability", True, 100)

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

        median_diff = np.median(diffs[diffs > 0]) if np.any(diffs > 0) else 0
        if median_diff > 0:
            jump_threshold = max(
                self.config.clock_jump_min_sec * 1e9,
                median_diff * self.config.clock_jump_multiplier,
            )
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
        threshold = bag_duration * self.config.completeness_threshold

        start_delay = topic_start - bag_start_ns
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
