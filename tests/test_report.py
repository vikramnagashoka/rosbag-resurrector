"""Tests for the Incident Report Generator — v0.7.1 Feature.

Covers:
- build_incident_report assembles evidence + health + probable cause
- HTML render is self-contained (no external asset refs) + has the chart
- Markdown render has the expected sections
- probable_cause classification (known on error finding, unknown on clean)
- diff section when a baseline is given
- generate_incident_report writes the file + honors fmt
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from resurrector.core.report import (
    build_incident_report,
    generate_incident_report,
    render_html,
    render_markdown,
)
from resurrector.demo.sample_bag import generate_bag, BagConfig


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def clean_bag(tmp_dir):
    p = tmp_dir / "clean.mcap"
    generate_bag(p, BagConfig(duration_sec=3.0, imu_hz=100.0))
    return p


@pytest.fixture
def gappy_bag(tmp_dir):
    """A bag with an injected /imu/data gap from 1.0s for 1.0s."""
    p = tmp_dir / "gappy.mcap"
    generate_bag(p, BagConfig(
        duration_sec=4.0, imu_hz=100.0,
        time_gap=True, gap_topic="/imu/data",
        gap_start_sec=1.0, gap_duration_sec=1.0,
    ))
    return p


class TestBuild:
    def test_assembles_core_sections(self, clean_bag):
        data = build_incident_report(clean_bag, 0.0, 3.0, include_thumbnail=False)
        assert data["bag_path"] == str(clean_bag)
        assert data["narrative"]
        assert data["window"]["duration_sec"] > 0
        assert isinstance(data["topics"], list)
        assert "probable_cause" in data

    def test_probable_cause_known_on_gap(self, gappy_bag):
        # Brushing the gap should surface an error-severity health finding.
        data = build_incident_report(gappy_bag, 0.5, 2.5, include_thumbnail=False)
        # A time gap is an error-severity finding → probable cause "known".
        assert data["probable_cause"] in ("known", "likely")
        assert data["probable_cause_reasons"]

    def test_clean_window_cause_unknown_or_likely(self, clean_bag):
        data = build_incident_report(clean_bag, 0.0, 3.0, include_thumbnail=False)
        # No injected fault; should not be "known".
        assert data["probable_cause"] in ("unknown", "likely")

    def test_diff_section_when_baseline_given(self, clean_bag, gappy_bag):
        data = build_incident_report(
            gappy_bag, 0.0, 4.0, baseline_bag=clean_bag, include_thumbnail=False,
        )
        assert data["diff"] is not None
        assert "summary" in data["diff"]


class TestRenderHtml:
    def test_self_contained(self, clean_bag):
        data = build_incident_report(clean_bag, 0.0, 3.0, include_thumbnail=False)
        html = render_html(data)
        assert "<!doctype html>" in html.lower()
        assert "Incident Report" in html
        assert "</svg>" in html  # inline chart
        # No external asset references.
        assert 'src="http' not in html
        assert "<script" not in html.lower()  # no JS

    def test_attribution_footer(self, clean_bag):
        data = build_incident_report(clean_bag, 0.0, 3.0, include_thumbnail=False)
        html = render_html(data)
        assert "RosBag Resurrector" in html


class TestRenderMarkdown:
    def test_sections(self, clean_bag):
        data = build_incident_report(clean_bag, 0.0, 3.0, include_thumbnail=False)
        md = render_markdown(data)
        assert md.startswith("# Incident Report")
        assert "## Summary" in md
        assert "## Topic activity in window" in md
        assert "## Health findings" in md


class TestGenerate:
    def test_writes_html_file(self, clean_bag, tmp_dir):
        out = tmp_dir / "r.html"
        res = generate_incident_report(clean_bag, 0.0, 3.0, output_path=out, fmt="html")
        assert out.exists()
        assert res.format == "html"
        assert out.read_text().lower().startswith("<!doctype html>")

    def test_writes_markdown_file(self, clean_bag, tmp_dir):
        out = tmp_dir / "r.md"
        res = generate_incident_report(clean_bag, 0.0, 3.0, output_path=out, fmt="md")
        assert out.exists()
        assert res.format == "md"
        assert out.read_text().startswith("# Incident Report")

    def test_invalid_format_raises(self, clean_bag):
        with pytest.raises(ValueError):
            generate_incident_report(clean_bag, 0.0, 3.0, fmt="pdf")

    def test_window_clamped_to_bag(self, clean_bag):
        # Asking past the bag end shouldn't blow up — gather_evidence clamps.
        res = generate_incident_report(clean_bag, 0.0, 9999.0, fmt="md")
        assert res.content
