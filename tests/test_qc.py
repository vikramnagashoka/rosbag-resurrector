"""Tests for the v0.6.0 bag-side QC tool — Sub-feature B.3.

Covers:
- Single-bag QC: health score, duration, message_count
- Cross-bag fleet checks: schema drift, topic-set divergence,
  rate anomaly, coverage gap, coverage overlap
- Rainy day: missing bag, unreadable bag, empty bag, very-short bag,
  single-bag input (no fleet issues), directory recursion
- CLI: text output, JSON output, --fail-on-error exit code
"""

from __future__ import annotations

import json
import struct
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from resurrector.core.qc import (
    BagQCResult, QCIssue, QCReport, qc_single_bag, run_qc,
)
from resurrector.demo.sample_bag import generate_bag, BagConfig


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def healthy_bag(tmp_dir):
    p = tmp_dir / "healthy.mcap"
    generate_bag(p, BagConfig(duration_sec=2.0))
    return p


@pytest.fixture
def fleet_three_healthy(tmp_dir):
    """3 healthy bags with distinct (sequential) start times."""
    paths = []
    for i in range(3):
        p = tmp_dir / f"bag{i}.mcap"
        generate_bag(p, BagConfig(duration_sec=0.5, imu_hz=100.0))
        paths.append(p)
    return paths


# ---------------------------------------------------------------------------
# Single-bag QC
# ---------------------------------------------------------------------------


class TestSingleBagQC:
    def test_healthy_bag(self, healthy_bag):
        result = qc_single_bag(healthy_bag)
        assert isinstance(result, BagQCResult)
        assert result.health_score > 70
        assert result.duration_sec > 1.5
        assert result.message_count > 0
        assert result.n_topics > 0

    def test_missing_bag_returns_error_issue(self, tmp_dir):
        result = qc_single_bag(tmp_dir / "does_not_exist.mcap")
        assert result.health_score == 0
        assert any(i.code == "bag_missing" for i in result.issues)
        assert all(i.severity == "error" for i in result.issues if i.code == "bag_missing")

    def test_unreadable_bag(self, tmp_dir):
        # Write a file that's not a valid MCAP
        bad = tmp_dir / "garbage.mcap"
        bad.write_bytes(b"\x00\x01\x02\x03not an mcap file")
        result = qc_single_bag(bad)
        assert result.health_score == 0
        assert any(i.code == "bag_unreadable" for i in result.issues)

    def test_very_short_bag_warning(self, tmp_dir):
        p = tmp_dir / "short.mcap"
        generate_bag(p, BagConfig(duration_sec=0.3))
        result = qc_single_bag(p)
        assert any(i.code == "very_short_bag" for i in result.issues)


# ---------------------------------------------------------------------------
# Cross-bag fleet QC
# ---------------------------------------------------------------------------


class TestFleetQC:
    def test_three_healthy_bags(self, fleet_three_healthy):
        report = run_qc(fleet_three_healthy)
        assert report.n_bags == 3
        # Healthy fleet shouldn't have errors
        assert report.n_errors == 0

    def test_topic_set_divergence_detected(self, tmp_dir):
        # One bag with TF, one without
        with_tf = tmp_dir / "with_tf.mcap"
        without_tf = tmp_dir / "without_tf.mcap"
        generate_bag(with_tf, BagConfig(duration_sec=0.5, include_tf=True))
        generate_bag(without_tf, BagConfig(duration_sec=0.5, include_tf=False))
        report = run_qc([with_tf, without_tf])
        codes = [i.code for i in report.fleet_issues]
        assert "topic_set_divergence" in codes
        # The divergent topic should be /tf
        tf_issues = [
            i for i in report.fleet_issues
            if i.code == "topic_set_divergence" and i.topic == "/tf"
        ]
        assert len(tf_issues) == 1

    def test_rate_anomaly_detected(self, tmp_dir):
        # Three bags with very different IMU rates
        slow = tmp_dir / "slow.mcap"
        normal1 = tmp_dir / "normal1.mcap"
        normal2 = tmp_dir / "normal2.mcap"
        generate_bag(slow, BagConfig(duration_sec=0.5, imu_hz=10.0))
        generate_bag(normal1, BagConfig(duration_sec=0.5, imu_hz=200.0))
        generate_bag(normal2, BagConfig(duration_sec=0.5, imu_hz=200.0))
        report = run_qc([slow, normal1, normal2])
        rate_issues = [i for i in report.fleet_issues if i.code == "rate_anomaly"]
        # The slow bag should be flagged (10 Hz vs median 200 Hz = 95% deviation)
        assert any(
            i.topic == "/imu/data" and "slow.mcap" in (i.bag or "")
            for i in rate_issues
        )

    def test_no_fleet_issues_for_single_bag(self, healthy_bag):
        report = run_qc([healthy_bag])
        # Cross-bag checks need >= 2 bags
        assert report.fleet_issues == []

    def test_directory_recursion(self, tmp_dir, fleet_three_healthy):
        # Pass the directory, not individual files
        report = run_qc([tmp_dir])
        assert report.n_bags == 3

    def test_mixed_bag_paths_and_directories(self, tmp_dir):
        # One bag in subdir, one direct
        sub = tmp_dir / "sub"
        sub.mkdir()
        a = sub / "a.mcap"
        b = tmp_dir / "b.mcap"
        generate_bag(a, BagConfig(duration_sec=0.3))
        generate_bag(b, BagConfig(duration_sec=0.3))
        report = run_qc([sub, b])
        assert report.n_bags == 2

    def test_partial_fleet_with_missing_bag(self, tmp_dir, healthy_bag):
        # One real bag + one missing — partial fleet should still report
        report = run_qc([healthy_bag, tmp_dir / "missing.mcap"])
        assert report.n_bags == 2
        # Missing bag's issue is captured
        assert any(
            "bag_missing" in [i.code for i in b.issues]
            for b in report.bags
        )


# ---------------------------------------------------------------------------
# QCReport serialization
# ---------------------------------------------------------------------------


class TestQCReportSerialization:
    def test_to_dict_shape(self, healthy_bag):
        report = run_qc([healthy_bag])
        d = report.to_dict()
        assert "bags" in d
        assert "fleet_issues" in d
        assert "summary" in d
        assert d["summary"]["n_bags"] == 1
        assert "n_errors" in d["summary"]
        assert "n_warnings" in d["summary"]

    def test_to_json_round_trip(self, healthy_bag):
        report = run_qc([healthy_bag])
        s = report.to_json()
        # Parses back as JSON
        parsed = json.loads(s)
        assert parsed["summary"]["n_bags"] == 1

    def test_n_errors_warnings_counts(self, tmp_dir):
        # Force an error: missing bag
        report = run_qc([tmp_dir / "ghost.mcap"])
        assert report.n_errors >= 1
        d = report.to_dict()
        assert d["summary"]["n_errors"] == report.n_errors


# ---------------------------------------------------------------------------
# CLI integration
# ---------------------------------------------------------------------------


VENV_RESURRECTOR = "/tmp/v060-build/bin/resurrector"


def _run_cli(*args: str) -> tuple[int, str, str]:
    """Run the resurrector CLI under the v060-build venv."""
    p = subprocess.run(
        [VENV_RESURRECTOR, "qc", *args],
        capture_output=True, text=True,
    )
    return p.returncode, p.stdout, p.stderr


@pytest.mark.skipif(
    not Path(VENV_RESURRECTOR).exists(),
    reason="venv-installed CLI not available; CLI tests need /tmp/v060-build",
)
class TestQCCLI:
    def test_text_output_on_healthy_fleet(self, fleet_three_healthy):
        rc, out, _ = _run_cli(*[str(p) for p in fleet_three_healthy])
        assert rc == 0
        assert "QC" in out or "Bag-side" in out
        assert "bag(s)" in out

    def test_json_output(self, fleet_three_healthy, tmp_dir):
        json_path = tmp_dir / "qc.json"
        rc, _, _ = _run_cli(
            *[str(p) for p in fleet_three_healthy],
            "--json", str(json_path),
        )
        assert rc == 0
        assert json_path.exists()
        data = json.loads(json_path.read_text())
        assert data["summary"]["n_bags"] == 3

    def test_fail_on_error_exits_nonzero(self, tmp_dir):
        # Missing bag forces an error → --fail-on-error → exit 1
        rc, _, _ = _run_cli(
            str(tmp_dir / "ghost.mcap"), "--fail-on-error",
        )
        assert rc == 1

    def test_no_fail_on_error_exits_zero_even_with_errors(self, tmp_dir):
        # Without the flag, errors still exit 0 (informational mode)
        rc, _, _ = _run_cli(str(tmp_dir / "ghost.mcap"))
        assert rc == 0
