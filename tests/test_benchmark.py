"""Tests for CI-for-robots benchmark / regression gating — v0.7 Feature D.

Covers:
- Metric extraction: scalar (duration/message_count/health_score),
  metadata (rate/count), and streaming column aggregates (mean/min/max)
- Direction logic: lower_is_better / higher_is_better / stable
- Tolerance boundary (just-within vs just-over)
- No-baseline metrics are recorded, never regressions
- Baseline-of-zero divide-by-zero guard
- Unknown metric ids raise
- capture_baseline round-trip + spec loading
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from resurrector.core.benchmark import (
    MetricSpec,
    UnknownMetricError,
    capture_baseline,
    compute_metrics,
    load_specs,
    run_benchmark,
    _evaluate,
)
from resurrector.demo.sample_bag import generate_bag, BagConfig


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def bag(tmp_dir):
    p = tmp_dir / "b.mcap"
    generate_bag(p, BagConfig(duration_sec=2.0, imu_hz=100.0))
    return p


# ---------------------------------------------------------------------------
# Metric extraction
# ---------------------------------------------------------------------------


class TestComputeMetrics:
    def test_scalar_metrics(self, bag):
        m = compute_metrics(bag, ["duration", "message_count", "health_score"])
        assert m["duration"] > 1.5
        assert m["message_count"] > 0
        assert 0 <= m["health_score"] <= 100

    def test_rate_and_count(self, bag):
        m = compute_metrics(bag, ["rate:/imu/data", "count:/imu/data"])
        assert m["rate:/imu/data"] > 0
        assert m["count:/imu/data"] > 0

    def test_column_aggregate_mean(self, bag):
        # /imu/data has a timestamp_ns column at minimum; mean should compute.
        from resurrector.core.bag_frame import BagFrame
        cols = BagFrame(bag)["/imu/data"].iter_chunks().__next__().columns
        # pick a numeric column present in the chunk
        numeric = [c for c in cols if c == "timestamp_ns"]
        assert numeric, f"expected timestamp_ns in {cols}"
        m = compute_metrics(bag, [f"mean:/imu/data:{numeric[0]}"])
        assert m[f"mean:/imu/data:{numeric[0]}"] > 0

    def test_unknown_topic_raises(self, bag):
        with pytest.raises(UnknownMetricError):
            compute_metrics(bag, ["rate:/nope"])

    def test_unknown_column_raises(self, bag):
        with pytest.raises(UnknownMetricError):
            compute_metrics(bag, ["mean:/imu/data:not_a_column"])

    def test_malformed_metric_id_raises(self, bag):
        with pytest.raises(UnknownMetricError):
            compute_metrics(bag, ["frobnicate"])


# ---------------------------------------------------------------------------
# Direction + tolerance logic (pure _evaluate)
# ---------------------------------------------------------------------------


class TestEvaluate:
    def test_lower_is_better_regresses_when_up(self):
        spec = MetricSpec("latency", "lower_is_better", 0.1)
        check = _evaluate(spec, current=120.0, baseline=100.0)  # +20%
        assert check.regressed

    def test_lower_is_better_ok_when_down(self):
        spec = MetricSpec("latency", "lower_is_better", 0.1)
        check = _evaluate(spec, current=80.0, baseline=100.0)
        assert not check.regressed

    def test_higher_is_better_regresses_when_down(self):
        spec = MetricSpec("coverage", "higher_is_better", 0.1)
        check = _evaluate(spec, current=80.0, baseline=100.0)  # -20%
        assert check.regressed

    def test_stable_regresses_either_direction(self):
        spec = MetricSpec("rate", "stable", 0.1)
        assert _evaluate(spec, 120.0, 100.0).regressed
        assert _evaluate(spec, 80.0, 100.0).regressed
        assert not _evaluate(spec, 105.0, 100.0).regressed  # within 10%

    def test_tolerance_boundary(self):
        spec = MetricSpec("x", "lower_is_better", 0.10)
        # exactly +10% is NOT over tolerance (strict >)
        assert not _evaluate(spec, 110.0, 100.0).regressed
        # +10.1% is over
        assert _evaluate(spec, 110.1, 100.0).regressed

    def test_no_baseline_never_regresses(self):
        spec = MetricSpec("x", "lower_is_better", 0.1)
        check = _evaluate(spec, 999.0, None)
        assert not check.regressed
        assert check.baseline is None
        assert "no baseline" in check.note

    def test_zero_baseline_guard(self):
        spec = MetricSpec("x", "stable", 0.1)
        # baseline 0, current 0.05 within abs tolerance -> ok
        assert not _evaluate(spec, 0.05, 0.0).regressed
        # baseline 0, current 5 -> regressed (no divide-by-zero crash)
        assert _evaluate(spec, 5.0, 0.0).regressed


# ---------------------------------------------------------------------------
# run_benchmark + baseline round-trip
# ---------------------------------------------------------------------------


class TestRunBenchmark:
    def test_no_regression_against_self(self, bag):
        specs = [MetricSpec("duration", "stable", 0.1),
                 MetricSpec("rate:/imu/data", "stable", 0.1)]
        base = capture_baseline(bag, specs)
        report = run_benchmark(bag, specs, base)
        assert not report.regressed
        assert report.n_regressions == 0

    def test_regression_detected(self, tmp_dir):
        good = tmp_dir / "good.mcap"
        bad = tmp_dir / "bad.mcap"
        generate_bag(good, BagConfig(duration_sec=2.0, imu_hz=100.0))
        generate_bag(bad, BagConfig(duration_sec=2.0, imu_hz=300.0))
        specs = [MetricSpec("rate:/imu/data", "stable", 0.1)]
        base = capture_baseline(good, specs)
        report = run_benchmark(bad, specs, base)
        assert report.regressed
        assert report.to_dict()["summary"]["n_regressions"] == 1

    def test_report_json_round_trip(self, bag):
        specs = [MetricSpec("duration", "stable", 0.1)]
        report = run_benchmark(bag, specs, {"duration": 1.0})
        parsed = json.loads(report.to_json())
        assert parsed["summary"]["n_metrics"] == 1
        assert "checks" in parsed


class TestLoadSpecs:
    def test_load_list_form(self, tmp_dir):
        p = tmp_dir / "m.json"
        p.write_text(json.dumps([
            {"name": "duration", "direction": "stable", "tolerance": 0.2},
            {"name": "health_score", "direction": "higher_is_better"},
        ]))
        specs = load_specs(p)
        assert len(specs) == 2
        assert specs[0].tolerance == 0.2
        assert specs[1].direction == "higher_is_better"
        assert specs[1].tolerance == 0.1  # default

    def test_load_wrapped_form(self, tmp_dir):
        p = tmp_dir / "m.json"
        p.write_text(json.dumps({"metrics": [{"name": "duration"}]}))
        specs = load_specs(p)
        assert len(specs) == 1

    def test_invalid_direction_raises(self, tmp_dir):
        p = tmp_dir / "m.json"
        p.write_text(json.dumps([{"name": "x", "direction": "sideways"}]))
        with pytest.raises(ValueError):
            load_specs(p)
