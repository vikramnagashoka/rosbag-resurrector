"""Bag-side QC tool (v0.6.0 — Sub-feature B.3).

Upstream-side data quality checks for ROS 2 bag files. Complements
``lerobot-doctor`` (which works on already-converted LeRobot datasets)
by catching problems before conversion / training. Distinct lane.

Two surfaces:

- **Single-bag QC** — wraps `bf.health_report()` plus a few extra
  checks (filename plausibility, file age, etc.). One bag at a time.

- **Fleet QC** — N bags treated as a logical campaign. Detects:
  - **Schema drift**: same topic name, different message_type or
    schema_data across bags
  - **Topic-set divergence**: bag A has /topic_x but bag B doesn't
  - **Rate anomaly**: same topic at very different frequencies
    (>50% deviation from the median)
  - **Coverage**: total time covered, time gaps between bags,
    overlapping time ranges

Output is a :class:`QCReport` dict-of-issues that can be rendered as
a console table OR serialized as JSON for CI use.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from resurrector.core.bag_frame import BagFrame
from resurrector.ingest.parser import parse_bag


Severity = Literal["info", "warning", "error"]


@dataclass
class QCIssue:
    """A single QC finding on a bag or across bags."""
    severity: Severity
    code: str  # short machine-readable identifier
    bag: str | None  # bag path; None for cross-bag issues
    topic: str | None  # topic name; None if not topic-specific
    message: str  # human-readable detail

    def to_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "code": self.code,
            "bag": self.bag,
            "topic": self.topic,
            "message": self.message,
        }


@dataclass
class BagQCResult:
    """Per-bag QC summary."""
    bag_path: str
    health_score: int
    duration_sec: float
    message_count: int
    n_topics: int
    issues: list[QCIssue] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "bag_path": self.bag_path,
            "health_score": self.health_score,
            "duration_sec": self.duration_sec,
            "message_count": self.message_count,
            "n_topics": self.n_topics,
            "issues": [i.to_dict() for i in self.issues],
        }


@dataclass
class QCReport:
    """Full QC report — per-bag + cross-bag findings."""
    bags: list[BagQCResult] = field(default_factory=list)
    fleet_issues: list[QCIssue] = field(default_factory=list)

    @property
    def n_bags(self) -> int:
        return len(self.bags)

    @property
    def all_issues(self) -> list[QCIssue]:
        out: list[QCIssue] = []
        for r in self.bags:
            out.extend(r.issues)
        out.extend(self.fleet_issues)
        return out

    @property
    def n_errors(self) -> int:
        return sum(1 for i in self.all_issues if i.severity == "error")

    @property
    def n_warnings(self) -> int:
        return sum(1 for i in self.all_issues if i.severity == "warning")

    def to_dict(self) -> dict[str, Any]:
        return {
            "bags": [b.to_dict() for b in self.bags],
            "fleet_issues": [i.to_dict() for i in self.fleet_issues],
            "summary": {
                "n_bags": self.n_bags,
                "n_errors": self.n_errors,
                "n_warnings": self.n_warnings,
            },
        }

    def to_json(self, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


# ---------------------------------------------------------------------------
# Single-bag checks
# ---------------------------------------------------------------------------


def qc_single_bag(bag_path: str | Path) -> BagQCResult:
    """Run QC on one bag. Wraps health_report + adds bag-level checks."""
    bag_path = Path(bag_path)
    if not bag_path.exists():
        # Encode the missing-file as an issue rather than raising — fleet
        # QC continues with the other bags.
        return BagQCResult(
            bag_path=str(bag_path),
            health_score=0,
            duration_sec=0,
            message_count=0,
            n_topics=0,
            issues=[QCIssue(
                severity="error",
                code="bag_missing",
                bag=str(bag_path),
                topic=None,
                message="Bag file does not exist",
            )],
        )

    issues: list[QCIssue] = []

    try:
        bf = BagFrame(bag_path)
        report = bf.health_report()
        meta = bf.metadata
    except Exception as e:
        return BagQCResult(
            bag_path=str(bag_path),
            health_score=0,
            duration_sec=0,
            message_count=0,
            n_topics=0,
            issues=[QCIssue(
                severity="error",
                code="bag_unreadable",
                bag=str(bag_path),
                topic=None,
                message=f"Could not open bag: {type(e).__name__}: {e}",
            )],
        )

    # Convert health_report's per-issue findings into QCIssues. The
    # health report exposes a flat issues list at the top level plus
    # per-topic TopicHealth entries — we convert the flat list since
    # it dedupes already.
    for issue in report.issues:
        # HealthIssue.severity is a Severity enum with values "info",
        # "warning", "error" — the strings already match our typing.
        sev_value = (
            issue.severity.value
            if hasattr(issue.severity, "value")
            else str(issue.severity)
        )
        sev: Severity = sev_value if sev_value in (
            "info", "warning", "error",
        ) else "warning"
        issues.append(QCIssue(
            severity=sev,
            code=issue.check_name,
            bag=str(bag_path),
            topic=issue.topic,
            message=issue.message,
        ))

    # Empty bag check
    if meta.message_count == 0:
        issues.append(QCIssue(
            severity="error",
            code="empty_bag",
            bag=str(bag_path),
            topic=None,
            message="Bag has zero messages",
        ))

    # Very-short-duration check (< 1 second)
    if 0 < meta.duration_sec < 1.0:
        issues.append(QCIssue(
            severity="warning",
            code="very_short_bag",
            bag=str(bag_path),
            topic=None,
            message=f"Bag duration only {meta.duration_sec:.2f}s — likely incomplete recording",
        ))

    return BagQCResult(
        bag_path=str(bag_path),
        health_score=report.score,
        duration_sec=meta.duration_sec,
        message_count=meta.message_count,
        n_topics=len(meta.topics),
        issues=issues,
    )


# ---------------------------------------------------------------------------
# Cross-bag checks
# ---------------------------------------------------------------------------


_RATE_DRIFT_THRESHOLD = 0.5  # 50% deviation from median is a warning


def _check_schema_drift(bag_metas: list[tuple[str, Any]]) -> list[QCIssue]:
    """Detect topics whose message_type or schema_data differs across bags."""
    issues: list[QCIssue] = []
    # topic_name -> first (path, message_type, schema_data) seen
    seen: dict[str, tuple[str, str, str]] = {}
    for path, meta in bag_metas:
        for ti in meta.topics:
            if ti.name not in seen:
                seen[ti.name] = (path, ti.message_type, ti.schema_data or "")
                continue
            first_path, first_type, first_schema = seen[ti.name]
            if ti.message_type != first_type:
                issues.append(QCIssue(
                    severity="error",
                    code="schema_drift_type",
                    bag=path, topic=ti.name,
                    message=(
                        f"message_type mismatch: this bag has "
                        f"{ti.message_type!r} but {first_path} has "
                        f"{first_type!r}"
                    ),
                ))
            elif (ti.schema_data or "") != first_schema and (ti.schema_data and first_schema):
                # Only flag if both have a schema; "" vs "..." isn't drift,
                # it's a missing schema in one bag (separate issue)
                issues.append(QCIssue(
                    severity="warning",
                    code="schema_drift_data",
                    bag=path, topic=ti.name,
                    message=(
                        f"schema_data differs from {first_path} (same type "
                        f"{ti.message_type}, but the .msg definition strings "
                        f"don't match — possible message-version drift)"
                    ),
                ))
    return issues


def _check_topic_set_divergence(bag_metas: list[tuple[str, Any]]) -> list[QCIssue]:
    """Find topics present in some bags but not others."""
    if len(bag_metas) < 2:
        return []
    # bag_path -> set of topic names
    per_bag = {p: {ti.name for ti in m.topics} for p, m in bag_metas}
    union = set().union(*per_bag.values())
    issues: list[QCIssue] = []
    for topic in sorted(union):
        present_in = [p for p, topics in per_bag.items() if topic in topics]
        missing_from = [p for p, topics in per_bag.items() if topic not in topics]
        if missing_from and present_in:
            sev: Severity = "info" if len(missing_from) < len(per_bag) // 2 else "warning"
            issues.append(QCIssue(
                severity=sev,
                code="topic_set_divergence",
                bag=None, topic=topic,
                message=(
                    f"Topic present in {len(present_in)}/{len(per_bag)} bags. "
                    f"Missing from: {', '.join(missing_from[:3])}"
                    f"{'...' if len(missing_from) > 3 else ''}"
                ),
            ))
    return issues


def _check_rate_anomaly(bag_metas: list[tuple[str, Any]]) -> list[QCIssue]:
    """Find topics whose recording rate differs >50% from the cross-bag median."""
    if len(bag_metas) < 2:
        return []
    # topic_name -> list of (bag_path, frequency_hz)
    rates: dict[str, list[tuple[str, float]]] = {}
    for path, meta in bag_metas:
        for ti in meta.topics:
            if ti.frequency_hz is not None and ti.frequency_hz > 0:
                rates.setdefault(ti.name, []).append((path, ti.frequency_hz))

    issues: list[QCIssue] = []
    for topic, observations in rates.items():
        if len(observations) < 2:
            continue
        sorted_rates = sorted(r for _, r in observations)
        n = len(sorted_rates)
        median = (
            sorted_rates[n // 2]
            if n % 2 else (sorted_rates[n // 2 - 1] + sorted_rates[n // 2]) / 2
        )
        if median <= 0:
            continue
        for path, hz in observations:
            deviation = abs(hz - median) / median
            if deviation > _RATE_DRIFT_THRESHOLD:
                issues.append(QCIssue(
                    severity="warning",
                    code="rate_anomaly",
                    bag=path, topic=topic,
                    message=(
                        f"Frequency {hz:.1f} Hz deviates {deviation * 100:.0f}% "
                        f"from cross-bag median {median:.1f} Hz "
                        f"(across {n} bags)"
                    ),
                ))
    return issues


def _check_coverage_gaps(bag_metas: list[tuple[str, Any]]) -> list[QCIssue]:
    """Detect time gaps + overlaps between sequential bags (sorted by start time)."""
    if len(bag_metas) < 2:
        return []
    sorted_meta = sorted(bag_metas, key=lambda pm: pm[1].start_time_ns)
    issues: list[QCIssue] = []
    for prev, curr in zip(sorted_meta, sorted_meta[1:]):
        prev_end_ns = prev[1].end_time_ns
        curr_start_ns = curr[1].start_time_ns
        gap_sec = (curr_start_ns - prev_end_ns) / 1e9
        if gap_sec > 0:
            sev: Severity = "info" if gap_sec < 60 else "warning"
            issues.append(QCIssue(
                severity=sev,
                code="coverage_gap",
                bag=None, topic=None,
                message=(
                    f"Gap of {gap_sec:.1f}s between {Path(prev[0]).name} and "
                    f"{Path(curr[0]).name}"
                ),
            ))
        elif gap_sec < 0:
            issues.append(QCIssue(
                severity="warning",
                code="coverage_overlap",
                bag=None, topic=None,
                message=(
                    f"Overlap of {-gap_sec:.1f}s between {Path(prev[0]).name} and "
                    f"{Path(curr[0]).name} — bags share a wall-clock window"
                ),
            ))
    return issues


def run_qc(bags: list[str | Path]) -> QCReport:
    """Run bag-side QC across N bags.

    Args:
        bags: List of bag paths (or directory paths — directories are
            recursed for .mcap files).

    Returns:
        :class:`QCReport` with per-bag results + fleet-level findings.

    The function never raises for missing/unreadable bags — those become
    `bag_missing`/`bag_unreadable` issues so a partial fleet still yields
    a useful report.
    """
    # Resolve directories to lists of bag paths
    resolved: list[Path] = []
    for b in bags:
        p = Path(b)
        if p.is_dir():
            resolved.extend(sorted(p.rglob("*.mcap")))
        else:
            resolved.append(p)

    # Per-bag QC
    bag_results = [qc_single_bag(p) for p in resolved]

    # Cross-bag QC — only on bags that opened cleanly
    bag_metas: list[tuple[str, Any]] = []
    for p in resolved:
        try:
            parser = parse_bag(p)
            meta = parser.get_metadata()
            bag_metas.append((str(p), meta))
        except Exception:
            continue

    fleet_issues: list[QCIssue] = []
    fleet_issues.extend(_check_schema_drift(bag_metas))
    fleet_issues.extend(_check_topic_set_divergence(bag_metas))
    fleet_issues.extend(_check_rate_anomaly(bag_metas))
    fleet_issues.extend(_check_coverage_gaps(bag_metas))

    return QCReport(bags=bag_results, fleet_issues=fleet_issues)
