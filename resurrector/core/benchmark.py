"""CI-for-robots: metric extraction + regression gating (v0.7 — Feature D).

Turns a bag into a set of scalar metrics, compares them against a committed
baseline, and flags regressions — so a robotics PR can gate on "did this
change make the robot measurably worse?" the way a software PR gates on tests.

Metric grammar (a metric id is a string):

- ``duration``                — bag duration (seconds)
- ``message_count``           — total messages
- ``health_score``            — overall health 0-100
- ``rate:<topic>``            — recording frequency (Hz) from metadata
- ``count:<topic>``           — per-topic message count
- ``mean:<topic>:<col>``      — mean of a numeric column
- ``min:<topic>:<col>``       — min of a numeric column
- ``max:<topic>:<col>``       — max of a numeric column

The column aggregates stream the topic via ``iter_chunks`` and accumulate
running sum/count/min/max — bounded memory, no full materialization, per the
project's performance contract.

A metric spec adds a *direction* and *tolerance*:

- ``lower_is_better``  — a regression is the metric going UP beyond tolerance
- ``higher_is_better`` — a regression is the metric going DOWN beyond tolerance
- ``stable``           — a regression is any change beyond tolerance (either way)

Tolerance is a fraction of the baseline (default 0.10 = 10%). A metric with a
baseline of 0 uses an absolute epsilon to avoid divide-by-zero.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

Direction = Literal["lower_is_better", "higher_is_better", "stable"]

_DEFAULT_TOLERANCE = 0.10
_ZERO_EPS = 1e-9


@dataclass
class MetricSpec:
    """How to evaluate one metric: its id, what direction is good, tolerance."""
    name: str
    direction: Direction = "stable"
    tolerance: float = _DEFAULT_TOLERANCE

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "MetricSpec":
        direction = d.get("direction", "stable")
        if direction not in ("lower_is_better", "higher_is_better", "stable"):
            raise ValueError(
                f"metric {d.get('name')!r}: invalid direction {direction!r}"
            )
        return cls(
            name=d["name"],
            direction=direction,
            tolerance=float(d.get("tolerance", _DEFAULT_TOLERANCE)),
        )


@dataclass
class MetricCheck:
    """The evaluation of one metric: current vs baseline + regression verdict."""
    name: str
    current: float
    baseline: float | None
    direction: Direction
    tolerance: float
    regressed: bool
    delta: float
    delta_pct: float | None
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "current": self.current,
            "baseline": self.baseline,
            "direction": self.direction,
            "tolerance": self.tolerance,
            "regressed": self.regressed,
            "delta": self.delta,
            "delta_pct": self.delta_pct,
            "note": self.note,
        }


@dataclass
class BenchmarkReport:
    """Full benchmark run: every metric check + whether anything regressed."""
    bag_path: str
    checks: list[MetricCheck] = field(default_factory=list)

    @property
    def regressed(self) -> bool:
        return any(c.regressed for c in self.checks)

    @property
    def n_regressions(self) -> int:
        return sum(1 for c in self.checks if c.regressed)

    def to_dict(self) -> dict[str, Any]:
        return {
            "bag_path": self.bag_path,
            "checks": [c.to_dict() for c in self.checks],
            "summary": {
                "n_metrics": len(self.checks),
                "n_regressions": self.n_regressions,
                "regressed": self.regressed,
            },
        }

    def to_json(self, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


# ---------------------------------------------------------------------------
# Metric extraction
# ---------------------------------------------------------------------------


class UnknownMetricError(ValueError):
    """Raised when a metric id can't be parsed or its target isn't in the bag."""


def _column_aggregate(bf, topic: str, column: str, agg: str) -> float:
    """Stream a topic and compute mean/min/max of one numeric column.

    Bounded memory: accumulates running sum/count/min/max over chunks rather
    than materializing the topic.
    """
    try:
        view = bf[topic]
    except KeyError:
        raise UnknownMetricError(f"topic {topic!r} not in bag")

    total = 0.0
    n = 0
    cur_min = math.inf
    cur_max = -math.inf
    seen_columns: list[str] = []

    for chunk in view.iter_chunks():
        if column not in chunk.columns:
            seen_columns = list(chunk.columns)
            break
        col = chunk[column]
        # Drop nulls; coerce to float via polars sum/min/max which ignore nulls.
        c_sum = col.sum()
        c_len = col.len() - col.null_count()
        if c_sum is not None and c_len:
            total += float(c_sum)
            n += int(c_len)
            cmn = col.min()
            cmx = col.max()
            if cmn is not None:
                cur_min = min(cur_min, float(cmn))
            if cmx is not None:
                cur_max = max(cur_max, float(cmx))

    if n == 0:
        if seen_columns:
            raise UnknownMetricError(
                f"column {column!r} not found in topic {topic!r}. "
                f"Available: {', '.join(seen_columns[:12])}"
            )
        raise UnknownMetricError(
            f"topic {topic!r} produced no rows for column {column!r}"
        )

    if agg == "mean":
        return total / n
    if agg == "min":
        return cur_min
    if agg == "max":
        return cur_max
    raise UnknownMetricError(f"unknown aggregate {agg!r}")


def compute_metric(bf, meta, report, metric_id: str) -> float:
    """Compute one metric id against an open BagFrame + cached metadata/report."""
    if metric_id == "duration":
        return float(meta.duration_sec)
    if metric_id == "message_count":
        return float(meta.message_count)
    if metric_id == "health_score":
        return float(report.score)

    parts = metric_id.split(":")
    kind = parts[0]

    if kind in ("rate", "count") and len(parts) == 2:
        topic = parts[1]
        ti = next((t for t in meta.topics if t.name == topic), None)
        if ti is None:
            raise UnknownMetricError(f"topic {topic!r} not in bag")
        if kind == "rate":
            return float(ti.frequency_hz or 0.0)
        return float(ti.message_count)

    if kind in ("mean", "min", "max") and len(parts) == 3:
        _, topic, column = parts
        return _column_aggregate(bf, topic, column, kind)

    raise UnknownMetricError(f"unrecognized metric id: {metric_id!r}")


def compute_metrics(bag_path: str | Path, metric_ids: list[str]) -> dict[str, float]:
    """Compute a set of metric ids for a bag. Opens the bag once.

    Raises:
        UnknownMetricError: if any metric id is malformed or its target is
            absent. (Benchmarks should fail loud on a typo'd metric, not
            silently skip it.)
    """
    from resurrector.core.bag_frame import BagFrame

    bf = BagFrame(bag_path)
    meta = bf.metadata
    # health_score is only needed if requested — compute lazily.
    report = None
    needs_health = "health_score" in metric_ids
    if needs_health:
        report = bf.health_report()

    out: dict[str, float] = {}
    for mid in metric_ids:
        if mid == "health_score" and report is None:
            report = bf.health_report()
        out[mid] = compute_metric(bf, meta, report, mid)
    return out


# ---------------------------------------------------------------------------
# Benchmark evaluation
# ---------------------------------------------------------------------------


def _evaluate(spec: MetricSpec, current: float, baseline: float | None) -> MetricCheck:
    if baseline is None:
        return MetricCheck(
            name=spec.name, current=current, baseline=None,
            direction=spec.direction, tolerance=spec.tolerance,
            regressed=False, delta=0.0, delta_pct=None,
            note="no baseline — recorded current value",
        )

    delta = current - baseline
    if abs(baseline) > _ZERO_EPS:
        delta_pct = delta / abs(baseline)
    else:
        # Baseline ~0: treat any movement past the absolute tolerance as a
        # full deviation so we don't divide by zero.
        delta_pct = math.inf if abs(delta) > spec.tolerance else 0.0

    regressed = False
    if spec.direction == "lower_is_better":
        regressed = delta_pct > spec.tolerance
    elif spec.direction == "higher_is_better":
        regressed = delta_pct < -spec.tolerance
    else:  # stable
        regressed = abs(delta_pct) > spec.tolerance

    return MetricCheck(
        name=spec.name, current=current, baseline=baseline,
        direction=spec.direction, tolerance=spec.tolerance,
        regressed=regressed, delta=delta,
        delta_pct=(None if delta_pct == math.inf else delta_pct),
    )


def run_benchmark(
    bag_path: str | Path,
    specs: list[MetricSpec],
    baseline: dict[str, float] | None = None,
) -> BenchmarkReport:
    """Compute each spec's metric and evaluate it against the baseline.

    Args:
        bag_path: Bag to benchmark.
        specs: Metric specs (id + direction + tolerance).
        baseline: Map of metric name -> baseline value. Missing entries are
            treated as "no baseline" (recorded, never a regression).

    Returns:
        :class:`BenchmarkReport`.
    """
    baseline = baseline or {}
    metric_ids = [s.name for s in specs]
    current = compute_metrics(bag_path, metric_ids)
    checks = [
        _evaluate(spec, current[spec.name], baseline.get(spec.name))
        for spec in specs
    ]
    return BenchmarkReport(bag_path=str(bag_path), checks=checks)


def capture_baseline(bag_path: str | Path, specs: list[MetricSpec]) -> dict[str, float]:
    """Compute current metric values to use as a baseline. Returns {name: value}."""
    return compute_metrics(bag_path, [s.name for s in specs])


def load_specs(path: str | Path) -> list[MetricSpec]:
    """Load a metric-spec file (JSON list of {name, direction, tolerance})."""
    data = json.loads(Path(path).read_text())
    if isinstance(data, dict) and "metrics" in data:
        data = data["metrics"]
    if not isinstance(data, list):
        raise ValueError("metrics spec must be a JSON list of metric objects")
    return [MetricSpec.from_dict(d) for d in data]
