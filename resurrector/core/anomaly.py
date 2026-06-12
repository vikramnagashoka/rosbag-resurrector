"""Cross-bag anomaly surfacing (v0.7 — Feature C).

The QC tool (``resurrector.core.qc``) answers "is this bag broken?" against
fixed thresholds. This module answers a different question: **"which runs in
this fleet are weird relative to the others?"** — without the user having to
know what to look for.

The detection is *relative*, not absolute. We never say "30 Hz is wrong"; we
say "every other bag records this topic at ~10 Hz and this one is at 30 Hz,
which makes it a 4.2σ outlier." That distinction matters: fleets have their
own normal, and the useful signal is deviation from the cluster, not from a
hardcoded constant.

Method: **robust z-scores** via the median and MAD (median absolute
deviation), using the Iglewicz-Hoaglin modified z-score:

    modified_z = 0.6745 * (x - median) / MAD

A common cutoff is |modified_z| > 3.5. Robust statistics (median/MAD rather
than mean/std) are deliberate — a single broken bag shouldn't move the mean
enough to hide itself. With MAD, one outlier can't mask another.

Metrics scored per topic across the fleet:
- recording frequency (Hz)
- message count

Plus per-bag scalar metrics:
- duration
- total message count
- topic-set size

Each bag accumulates an **anomaly score** = the count of metrics on which it
is a flagged outlier, so the fleet can be ranked "most suspicious first."
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from resurrector.core.qc import QCIssue, Severity

# Iglewicz-Hoaglin modified z-score cutoff. 3.5 is the textbook default;
# we expose it so callers (and tests) can tighten/loosen.
MODIFIED_Z_CUTOFF = 3.5

# 0.6745 is the 0.75 quantile of the standard normal — it makes the MAD a
# consistent estimator of the standard deviation for normally distributed
# data, so the modified z-score is comparable to an ordinary z-score.
_MAD_SCALE = 0.6745


def _median(values: list[float]) -> float:
    s = sorted(values)
    n = len(s)
    if n == 0:
        return 0.0
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def _modified_zscores(values: list[float]) -> list[float]:
    """Iglewicz-Hoaglin modified z-scores for a 1-D sample.

    Returns 0.0 for every element when the sample has no spread (MAD == 0),
    which is the correct behaviour: if every bag agrees, nothing is an
    outlier. Falls back to a mean-absolute-deviation scale when the MAD is
    exactly zero but the values still differ (e.g. [10, 10, 10, 11] — the
    MAD is 0 because the majority cluster dominates, yet the 11 is mildly
    anomalous; the meanAD fallback surfaces it without over-flagging).
    """
    n = len(values)
    if n < 3:
        # Need at least 3 points for an outlier to mean anything.
        return [0.0] * n
    med = _median(values)
    abs_dev = [abs(v - med) for v in values]
    mad = _median(abs_dev)
    if mad > 0:
        return [_MAD_SCALE * (v - med) / mad for v in values]
    # MAD == 0: the majority share the median exactly. Use mean absolute
    # deviation as a softer scale so a lone differing value still scores.
    mean_ad = sum(abs_dev) / n
    if mean_ad == 0:
        return [0.0] * n
    # 1.253314 makes meanAD a consistent std estimator under normality.
    return [(v - med) / (1.253314 * mean_ad) for v in values]


@dataclass
class BagAnomalyScore:
    """Per-bag anomaly summary for ranking the fleet."""
    bag_path: str
    score: int  # number of metrics on which this bag is a flagged outlier
    reasons: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "bag_path": self.bag_path,
            "name": Path(self.bag_path).name,
            "score": self.score,
            "reasons": self.reasons,
        }


@dataclass
class AnomalyReport:
    """Fleet anomaly findings + a ranked list of suspicious bags."""
    issues: list[QCIssue] = field(default_factory=list)
    ranking: list[BagAnomalyScore] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "issues": [i.to_dict() for i in self.issues],
            "ranking": [r.to_dict() for r in self.ranking],
        }


def _flag(
    scores: dict[str, BagAnomalyScore],
    path: str,
    reason: str,
) -> None:
    """Record an outlier hit against a bag's running anomaly score."""
    entry = scores.setdefault(path, BagAnomalyScore(bag_path=path, score=0))
    entry.score += 1
    entry.reasons.append(reason)


def detect_fleet_anomalies(bag_metas: list[tuple[str, Any]]) -> AnomalyReport:
    """Run cross-bag anomaly detection over a list of (path, BagMetadata).

    Args:
        bag_metas: ``(bag_path, BagMetadata)`` for each bag that opened
            cleanly. Caller is responsible for filtering unreadable bags
            (mirrors the contract used by ``qc.run_qc``).

    Returns:
        :class:`AnomalyReport`. Empty (no issues, no ranking) when there are
        fewer than 3 bags — relative outlier detection is meaningless on 1-2
        samples, and forcing it would just produce noise.
    """
    if len(bag_metas) < 3:
        return AnomalyReport()

    issues: list[QCIssue] = []
    scores: dict[str, BagAnomalyScore] = {}

    # --- Per-topic metrics: frequency + message count -------------------
    # topic -> list of (path, hz, msg_count)
    per_topic: dict[str, list[tuple[str, float | None, int]]] = {}
    for path, meta in bag_metas:
        for ti in meta.topics:
            per_topic.setdefault(ti.name, []).append(
                (path, ti.frequency_hz, ti.message_count)
            )

    for topic, observations in per_topic.items():
        # Only score topics present in (almost) the whole fleet — a topic
        # in 2 of 10 bags is a topic-set-divergence issue (qc handles that),
        # not a per-topic rate outlier.
        if len(observations) < 3:
            continue

        # Frequency outliers
        freq_obs = [(p, hz) for p, hz, _ in observations if hz is not None and hz > 0]
        if len(freq_obs) >= 3:
            paths_f = [p for p, _ in freq_obs]
            vals_f = [hz for _, hz in freq_obs]
            zs = _modified_zscores(vals_f)
            med = _median(vals_f)
            for p, hz, z in zip(paths_f, vals_f, zs):
                if abs(z) > MODIFIED_Z_CUTOFF:
                    direction = "faster" if hz > med else "slower"
                    issues.append(QCIssue(
                        severity="warning",
                        code="anomaly_rate",
                        bag=p, topic=topic,
                        message=(
                            f"{hz:.1f} Hz is a {abs(z):.1f}σ outlier ({direction}) "
                            f"vs fleet median {med:.1f} Hz across {len(vals_f)} bags"
                        ),
                    ))
                    _flag(scores, p, f"{topic} rate {hz:.1f}Hz ({abs(z):.1f}σ)")

        # Message-count outliers
        count_obs = [(p, c) for p, _, c in observations if c > 0]
        if len(count_obs) >= 3:
            paths_c = [p for p, _ in count_obs]
            vals_c = [float(c) for _, c in count_obs]
            zs = _modified_zscores(vals_c)
            med = _median(vals_c)
            for p, c, z in zip(paths_c, vals_c, zs):
                if abs(z) > MODIFIED_Z_CUTOFF:
                    direction = "more" if c > med else "fewer"
                    issues.append(QCIssue(
                        severity="info",
                        code="anomaly_msg_count",
                        bag=p, topic=topic,
                        message=(
                            f"{int(c):,} messages is a {abs(z):.1f}σ outlier "
                            f"({direction}) vs fleet median {int(med):,}"
                        ),
                    ))
                    _flag(scores, p, f"{topic} count {int(c):,} ({abs(z):.1f}σ)")

    # --- Per-bag scalar metrics: duration, total messages, topic count --
    scalar_specs = [
        ("duration", "anomaly_duration", "s",
         lambda m: float(m.duration_sec)),
        ("total messages", "anomaly_total_messages", "",
         lambda m: float(m.message_count)),
        ("topic count", "anomaly_topic_count", " topics",
         lambda m: float(len(m.topics))),
    ]
    for label, code, unit, getter in scalar_specs:
        paths_s = [p for p, _ in bag_metas]
        vals_s = [getter(m) for _, m in bag_metas]
        zs = _modified_zscores(vals_s)
        med = _median(vals_s)
        for p, v, z in zip(paths_s, vals_s, zs):
            if abs(z) > MODIFIED_Z_CUTOFF:
                direction = "high" if v > med else "low"
                pretty = f"{v:,.1f}{unit}" if unit == "s" else f"{int(v):,}{unit}"
                pretty_med = f"{med:,.1f}{unit}" if unit == "s" else f"{int(med):,}{unit}"
                issues.append(QCIssue(
                    severity="warning",
                    code=code,
                    bag=p, topic=None,
                    message=(
                        f"{label} {pretty} is a {abs(z):.1f}σ outlier "
                        f"({direction}) vs fleet median {pretty_med}"
                    ),
                ))
                _flag(scores, p, f"{label} {pretty} ({abs(z):.1f}σ)")

    ranking = sorted(scores.values(), key=lambda s: -s.score)
    return AnomalyReport(issues=issues, ranking=ranking)
