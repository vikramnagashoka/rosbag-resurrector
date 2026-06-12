"""Tests for cross-bag anomaly surfacing — v0.7 Feature C.

Covers:
- Modified z-score math (the Iglewicz-Hoaglin robust estimator)
- detect_fleet_anomalies happy path (one bag with an off rate is flagged)
- Per-bag ranking ("most suspicious first")
- Rainy day: <3 bags is a no-op, all-identical fleet yields nothing,
  robustness (one big outlier doesn't mask a second)
- Integration through run_qc(detect_anomalies=True)
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from resurrector.core.anomaly import (
    MODIFIED_Z_CUTOFF,
    AnomalyReport,
    _median,
    _modified_zscores,
    detect_fleet_anomalies,
)
from resurrector.core.qc import run_qc
from resurrector.demo.sample_bag import generate_bag, BagConfig
from resurrector.ingest.parser import parse_bag


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


# ---------------------------------------------------------------------------
# z-score math
# ---------------------------------------------------------------------------


class TestModifiedZScores:
    def test_returns_zeros_for_under_three_samples(self):
        assert _modified_zscores([1.0]) == [0.0]
        assert _modified_zscores([1.0, 2.0]) == [0.0, 0.0]

    def test_identical_values_score_zero(self):
        assert _modified_zscores([5.0, 5.0, 5.0, 5.0]) == [0.0, 0.0, 0.0, 0.0]

    def test_obvious_outlier_exceeds_cutoff(self):
        # Tight cluster around 10 with one value at 100.
        vals = [10.0, 10.1, 9.9, 10.2, 9.8, 100.0]
        zs = _modified_zscores(vals)
        assert abs(zs[-1]) > MODIFIED_Z_CUTOFF
        # The in-cluster points stay well under the cutoff.
        assert all(abs(z) < MODIFIED_Z_CUTOFF for z in zs[:-1])

    def test_mad_zero_fallback_surfaces_lone_differ(self):
        # Majority identical -> MAD is 0; the meanAD fallback should still
        # give the lone differing value a non-zero score.
        vals = [10.0, 10.0, 10.0, 10.0, 18.0]
        zs = _modified_zscores(vals)
        assert zs[-1] != 0.0
        assert abs(zs[-1]) > abs(zs[0])

    def test_robust_to_a_second_outlier(self):
        # Two outliers in a cluster with natural spread (so MAD > 0 and the
        # true robust path runs, not the meanAD fallback). Robust stats mean
        # neither outlier masks the other — the mean/std approach would let
        # the pair inflate the scale and hide themselves.
        vals = [10.0, 11.0, 9.0, 10.5, 9.5, 50.0, 55.0]
        zs = _modified_zscores(vals)
        assert abs(zs[-1]) > MODIFIED_Z_CUTOFF
        assert abs(zs[-2]) > MODIFIED_Z_CUTOFF

    def test_median_even_and_odd(self):
        assert _median([1.0, 2.0, 3.0]) == 2.0
        assert _median([1.0, 2.0, 3.0, 4.0]) == 2.5


# ---------------------------------------------------------------------------
# detect_fleet_anomalies via real bags
# ---------------------------------------------------------------------------


def _meta(path):
    return (str(path), parse_bag(path).get_metadata())


class TestDetectFleetAnomalies:
    def test_under_three_bags_is_noop(self, tmp_dir):
        paths = []
        for i in range(2):
            p = tmp_dir / f"b{i}.mcap"
            generate_bag(p, BagConfig(duration_sec=0.5))
            paths.append(p)
        report = detect_fleet_anomalies([_meta(p) for p in paths])
        assert isinstance(report, AnomalyReport)
        assert report.issues == []
        assert report.ranking == []

    def test_consistent_fleet_has_no_anomalies(self, tmp_dir):
        metas = []
        for i in range(5):
            p = tmp_dir / f"b{i}.mcap"
            generate_bag(p, BagConfig(duration_sec=0.5, imu_hz=100.0))
            metas.append(_meta(p))
        report = detect_fleet_anomalies(metas)
        # A perfectly consistent fleet should rank nobody as suspicious.
        flagged = [r for r in report.ranking if r.score > 0]
        assert flagged == []

    def test_one_off_rate_bag_is_flagged_and_ranked_first(self, tmp_dir):
        metas = []
        # Four bags at 100 Hz IMU, one at 400 Hz (clear outlier).
        for i in range(4):
            p = tmp_dir / f"normal{i}.mcap"
            generate_bag(p, BagConfig(duration_sec=0.5, imu_hz=100.0))
            metas.append(_meta(p))
        odd = tmp_dir / "odd.mcap"
        generate_bag(odd, BagConfig(duration_sec=0.5, imu_hz=400.0))
        metas.append(_meta(odd))

        report = detect_fleet_anomalies(metas)
        # There should be at least one anomaly_rate issue on the odd bag.
        rate_issues = [i for i in report.issues if i.code == "anomaly_rate"]
        assert any("odd.mcap" in (i.bag or "") for i in rate_issues)
        # The odd bag should rank first (highest anomaly score).
        assert report.ranking
        assert "odd.mcap" in report.ranking[0].bag_path
        assert report.ranking[0].score >= 1

    def test_short_duration_bag_flagged_as_scalar_outlier(self, tmp_dir):
        metas = []
        for i in range(4):
            p = tmp_dir / f"full{i}.mcap"
            generate_bag(p, BagConfig(duration_sec=2.0, imu_hz=100.0))
            metas.append(_meta(p))
        short = tmp_dir / "short.mcap"
        generate_bag(short, BagConfig(duration_sec=0.2, imu_hz=100.0))
        metas.append(_meta(short))

        report = detect_fleet_anomalies(metas)
        dur_issues = [i for i in report.issues if i.code == "anomaly_duration"]
        assert any("short.mcap" in (i.bag or "") for i in dur_issues)


# ---------------------------------------------------------------------------
# Integration through run_qc
# ---------------------------------------------------------------------------


class TestRunQCIntegration:
    def test_anomalies_off_by_default(self, tmp_dir):
        for i in range(3):
            generate_bag(tmp_dir / f"b{i}.mcap", BagConfig(duration_sec=0.5))
        report = run_qc([tmp_dir])
        assert report.anomaly_ranking == []

    def test_anomalies_on_populates_ranking(self, tmp_dir):
        for i in range(4):
            generate_bag(tmp_dir / f"normal{i}.mcap", BagConfig(duration_sec=0.5, imu_hz=100.0))
        generate_bag(tmp_dir / "odd.mcap", BagConfig(duration_sec=0.5, imu_hz=400.0))
        report = run_qc([tmp_dir], detect_anomalies=True)
        assert report.anomaly_ranking  # non-empty
        # Serializes cleanly for JSON / API consumption.
        d = report.to_dict()
        assert "anomaly_ranking" in d
        assert any(r["score"] > 0 for r in d["anomaly_ranking"])

    def test_anomaly_ranking_round_trips_json(self, tmp_dir):
        for i in range(4):
            generate_bag(tmp_dir / f"normal{i}.mcap", BagConfig(duration_sec=0.5, imu_hz=100.0))
        generate_bag(tmp_dir / "odd.mcap", BagConfig(duration_sec=0.5, imu_hz=400.0))
        report = run_qc([tmp_dir], detect_anomalies=True)
        import json
        parsed = json.loads(report.to_json())
        assert isinstance(parsed["anomaly_ranking"], list)
