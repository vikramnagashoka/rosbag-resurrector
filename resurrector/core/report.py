"""Incident Report Generator (v0.7.1 — demand-validated from outreach).

Brush a bad time window (or pick an anomaly), generate a single shareable
report: the grounded evidence from the copilot, the health findings in that
window, a self-contained activity chart, an optional camera thumbnail, an
optional diff against a known-good baseline, and a probable-cause verdict.

Design goals:
- **Self-contained output.** One HTML file — inline CSS, inline SVG, base64
  images. No external assets, no JS. It survives being dropped into a Slack
  message, a GitHub issue, or an email attachment. That portability is the
  whole point: every shared report carries attribution and travels on its own.
- **Composition, not new analysis.** Everything here is assembled from
  primitives that already exist (copilot evidence, health, bag diff). This
  module is a renderer, not an analyzer.

Two render targets: HTML (the shareable artifact) and Markdown (for pasting
into issue trackers / PR descriptions).
"""

from __future__ import annotations

import base64
import html as _html
import io
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from resurrector.core.copilot import explain_time_range


@dataclass
class IncidentReportResult:
    """Outcome of generating a report."""
    content: str
    format: str  # "html" | "md"
    probable_cause: str  # "known" | "likely" | "unknown"
    output_path: str | None = None
    sections: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------


def _probable_cause(evidence: dict[str, Any]) -> tuple[str, list[str]]:
    """Classify the window. 'known' if any error-severity health finding,
    'likely' if warnings, else 'unknown'. Returns (verdict, reasons)."""
    issues = evidence.get("health_issues", [])
    errors = [i for i in issues if i.get("severity") == "error"]
    warnings = [i for i in issues if i.get("severity") == "warning"]
    if errors:
        return "known", [f"{i['check']}: {i['message']}" for i in errors]
    if warnings:
        return "likely", [f"{i['check']}: {i['message']}" for i in warnings]
    return "unknown", []


def _camera_thumbnail(bag_path: str | Path, mid_sec: float) -> str | None:
    """Best-effort base64 PNG of the camera frame nearest ``mid_sec``.

    Returns a ``data:image/png;base64,...`` URI, or None if there's no image
    topic, no decodable frame, or PIL isn't installed. Never raises — the
    thumbnail is a nicety, not a requirement.
    """
    try:
        from PIL import Image as PILImage  # noqa: F401
    except ImportError:
        return None
    try:
        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(bag_path)
        img_topic = next(
            (t.name for t in bf.metadata.topics if bf[t.name].is_image_topic),
            None,
        )
        if img_topic is None:
            return None
        bag_start_ns = bf.metadata.start_time_ns
        target_ns = bag_start_ns + int(mid_sec * 1e9)
        best = None
        best_dt = None
        # Scan frames, keep the one closest to the window midpoint. Bounded by
        # iterating images (already downsampled by the reader path).
        for ts, arr in bf[img_topic].iter_images():
            dt = abs(ts - target_ns)
            if best_dt is None or dt < best_dt:
                best, best_dt = arr, dt
            elif ts > target_ns and best_dt is not None:
                break  # past the target and getting worse — stop
        if best is None:
            return None
        from PIL import Image
        im = Image.fromarray(best)
        im.thumbnail((320, 240))
        buf = io.BytesIO()
        im.save(buf, format="PNG")
        b64 = base64.b64encode(buf.getvalue()).decode("ascii")
        return f"data:image/png;base64,{b64}"
    except Exception:
        return None


def build_incident_report(
    bag_path: str | Path,
    start_sec: float,
    end_sec: float,
    *,
    baseline_bag: str | Path | None = None,
    use_llm: bool = False,
    include_thumbnail: bool = True,
) -> dict[str, Any]:
    """Assemble all report sections from existing primitives. Pure data."""
    explain = explain_time_range(bag_path, start_sec, end_sec, use_llm=use_llm)
    evidence = explain.evidence
    verdict, reasons = _probable_cause(evidence)

    diff_data = None
    if baseline_bag is not None:
        from resurrector.core.bag_diff import diff_bags
        # Baseline = A (good), candidate = B (the incident bag).
        diff_data = diff_bags(baseline_bag, bag_path).to_dict()

    thumbnail = None
    if include_thumbnail:
        mid = (explain.start_sec + explain.end_sec) / 2.0
        thumbnail = _camera_thumbnail(bag_path, mid)

    return {
        "bag_path": str(bag_path),
        "narrative": explain.narrative,
        "narrative_source": explain.source,
        "window": evidence.get("window", {}),
        "totals": evidence.get("totals", {}),
        "topics": evidence.get("topics", []),
        "health_issues": evidence.get("health_issues", []),
        "health_score": evidence.get("health_score"),
        "probable_cause": verdict,
        "probable_cause_reasons": reasons,
        "diff": diff_data,
        "thumbnail": thumbnail,
    }


# ---------------------------------------------------------------------------
# Activity chart (self-contained inline SVG — no JS, no deps)
# ---------------------------------------------------------------------------


def _activity_svg(topics: list[dict[str, Any]], width: int = 520) -> str:
    """Horizontal bar chart of per-topic in-window message counts."""
    if not topics:
        return "<p style='color:#8b949e'>No topic activity in this window.</p>"
    rows = topics[:12]
    max_count = max((t["count_in_window"] for t in rows), default=1) or 1
    bar_h, gap, label_w = 18, 8, 200
    chart_w = width - label_w - 60
    height = len(rows) * (bar_h + gap) + gap
    parts = [f"<svg width='{width}' height='{height}' xmlns='http://www.w3.org/2000/svg' font-family='monospace' font-size='11'>"]
    y = gap
    for t in rows:
        w = max(1, int(chart_w * t["count_in_window"] / max_count))
        name = _html.escape(t["topic"][:28])
        parts.append(
            f"<text x='0' y='{y + bar_h - 5}' fill='#8b949e'>{name}</text>"
            f"<rect x='{label_w}' y='{y}' width='{w}' height='{bar_h}' fill='#1f6feb' rx='2'/>"
            f"<text x='{label_w + w + 6}' y='{y + bar_h - 5}' fill='#e1e4e8'>"
            f"{t['count_in_window']:,} ({t['rate_in_window_hz']:.0f}Hz)</text>"
        )
        y += bar_h + gap
    parts.append("</svg>")
    return "".join(parts)


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------


_CAUSE_BADGE = {
    "known": ("#f85149", "PROBABLE CAUSE KNOWN"),
    "likely": ("#d29922", "PROBABLE CAUSE LIKELY"),
    "unknown": ("#8b949e", "PROBABLE CAUSE UNKNOWN"),
}


def render_html(data: dict[str, Any]) -> str:
    e = _html.escape
    win = data["window"]
    color, label = _CAUSE_BADGE[data["probable_cause"]]

    topic_rows = "".join(
        f"<tr><td style='font-family:monospace;color:#58a6ff'>{e(t['topic'])}</td>"
        f"<td style='text-align:right'>{t['count_in_window']:,}</td>"
        f"<td style='text-align:right'>{t['rate_in_window_hz']:.1f} Hz</td>"
        f"<td style='text-align:right;color:#8b949e'>"
        f"{(str(round(t['overall_rate_hz'],1)) + ' Hz') if t.get('overall_rate_hz') else '—'}</td></tr>"
        for t in data["topics"]
    )

    health_rows = "".join(
        f"<li><strong style='color:"
        f"{'#f85149' if h['severity']=='error' else '#d29922'}'>{e(h['severity'].upper())}</strong> "
        f"{e(h['check'])}{(' [' + e(h['topic']) + ']') if h.get('topic') else ''}: {e(h['message'])}</li>"
        for h in data["health_issues"]
    ) or "<li style='color:#8b949e'>No health findings overlap this window.</li>"

    reasons_html = ""
    if data["probable_cause_reasons"]:
        items = "".join(f"<li>{e(r)}</li>" for r in data["probable_cause_reasons"])
        reasons_html = f"<ul>{items}</ul>"

    thumb_html = (
        f"<h3>Camera frame</h3><img src='{data['thumbnail']}' "
        f"style='max-width:320px;border:1px solid #30363d;border-radius:6px'/>"
        if data.get("thumbnail") else ""
    )

    diff_html = ""
    if data.get("diff"):
        d = data["diff"]
        s = d["summary"]
        diff_html = (
            "<h3>Diff vs baseline</h3>"
            f"<p>{s['topics_added']} topic(s) added, {s['topics_removed']} removed, "
            f"{s['topic_changes']} changed. "
            f"Duration {d['duration_delta_sec']:+.2f}s, "
            f"messages {d['message_count_delta']:+,}.</p>"
        )

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Incident Report — {e(Path(data['bag_path']).name)}</title>
<style>
body{{background:#0d1117;color:#e1e4e8;font-family:-apple-system,system-ui,sans-serif;max-width:780px;margin:32px auto;padding:0 16px;line-height:1.5}}
h1{{font-size:22px}} h3{{font-size:15px;color:#e1e4e8;margin-top:24px}}
.badge{{display:inline-block;padding:4px 12px;border-radius:12px;font-size:12px;font-weight:600;color:#fff;background:{color}}}
.meta{{color:#8b949e;font-size:13px}}
.card{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;margin:12px 0}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{text-align:left;color:#8b949e;font-weight:500;padding:4px 8px}} td{{padding:4px 8px;border-top:1px solid #21262d}}
.narrative{{white-space:pre-wrap;background:#0d1117;border:1px solid #30363d;border-radius:8px;padding:14px}}
footer{{color:#6e7681;font-size:12px;margin-top:28px;border-top:1px solid #21262d;padding-top:12px}}
a{{color:#58a6ff}}
</style></head><body>
<h1>Incident Report</h1>
<p class="meta">{e(Path(data['bag_path']).name)} &middot; window {win.get('start_sec',0):.2f}s–{win.get('end_sec',0):.2f}s
of {win.get('bag_duration_sec',0):.1f}s &middot; health score {data.get('health_score') if data.get('health_score') is not None else '—'}</p>
<p><span class="badge">{label}</span></p>
{reasons_html}
<div class="card"><h3 style="margin-top:0">Summary <span class="meta">({e(data['narrative_source'])})</span></h3>
<div class="narrative">{e(data['narrative'])}</div></div>
<h3>Topic activity in window</h3>
{_activity_svg(data['topics'])}
<table><thead><tr><th>Topic</th><th style="text-align:right">Count</th><th style="text-align:right">Rate (window)</th><th style="text-align:right">Rate (overall)</th></tr></thead>
<tbody>{topic_rows}</tbody></table>
<h3>Health findings</h3><ul>{health_rows}</ul>
{diff_html}
{thumb_html}
<footer>Generated by <a href="https://github.com/vikramnagashoka/rosbag-resurrector">RosBag Resurrector</a> — <code>resurrector report</code></footer>
</body></html>"""


def render_markdown(data: dict[str, Any]) -> str:
    win = data["window"]
    _, label = _CAUSE_BADGE[data["probable_cause"]]
    lines = [
        f"# Incident Report — {Path(data['bag_path']).name}",
        "",
        f"**Window:** {win.get('start_sec',0):.2f}s–{win.get('end_sec',0):.2f}s "
        f"of {win.get('bag_duration_sec',0):.1f}s  |  "
        f"**Health score:** {data.get('health_score') if data.get('health_score') is not None else '—'}",
        "",
        f"**{label}**",
    ]
    for r in data["probable_cause_reasons"]:
        lines.append(f"- {r}")
    lines += ["", f"## Summary ({data['narrative_source']})", "", data["narrative"], ""]
    lines += ["## Topic activity in window", "", "| Topic | Count | Rate (window) | Rate (overall) |", "|---|---:|---:|---:|"]
    for t in data["topics"]:
        overall = f"{t['overall_rate_hz']:.1f} Hz" if t.get("overall_rate_hz") else "—"
        lines.append(f"| `{t['topic']}` | {t['count_in_window']:,} | {t['rate_in_window_hz']:.1f} Hz | {overall} |")
    lines += ["", "## Health findings", ""]
    if data["health_issues"]:
        for h in data["health_issues"]:
            topic = f" [{h['topic']}]" if h.get("topic") else ""
            lines.append(f"- **{h['severity'].upper()}** {h['check']}{topic}: {h['message']}")
    else:
        lines.append("- No health findings overlap this window.")
    if data.get("diff"):
        d = data["diff"]; s = d["summary"]
        lines += ["", "## Diff vs baseline", "",
                  f"{s['topics_added']} added, {s['topics_removed']} removed, "
                  f"{s['topic_changes']} changed; duration {d['duration_delta_sec']:+.2f}s, "
                  f"messages {d['message_count_delta']:+,}."]
    lines += ["", "---", "*Generated by [RosBag Resurrector](https://github.com/vikramnagashoka/rosbag-resurrector) — `resurrector report`*"]
    return "\n".join(lines)


def generate_incident_report(
    bag_path: str | Path,
    start_sec: float,
    end_sec: float,
    *,
    output_path: str | Path | None = None,
    fmt: str = "html",
    baseline_bag: str | Path | None = None,
    use_llm: bool = False,
    include_thumbnail: bool = True,
) -> IncidentReportResult:
    """Build + render an incident report. Writes to ``output_path`` if given."""
    if fmt not in ("html", "md"):
        raise ValueError("fmt must be 'html' or 'md'")
    data = build_incident_report(
        bag_path, start_sec, end_sec,
        baseline_bag=baseline_bag, use_llm=use_llm,
        include_thumbnail=include_thumbnail,
    )
    content = render_html(data) if fmt == "html" else render_markdown(data)
    out = None
    if output_path is not None:
        out = str(output_path)
        Path(out).write_text(content, encoding="utf-8")
    return IncidentReportResult(
        content=content, format=fmt,
        probable_cause=data["probable_cause"],
        output_path=out, sections=data,
    )
