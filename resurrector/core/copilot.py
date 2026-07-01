"""'Ask your bag' — grounded time-range explanation (v0.7 — Feature B).

The wedge is deliberately NOT open-ended chat. It's one bounded operation:
**"explain what happened in this time window"**. The user brushes a range on
a plot; the tool gathers *real facts* from the bag for that window and turns
them into a narrative.

The non-negotiable design rule: **the narrative may only state facts that were
queried from the bag.** We never hand a model the raw bag and ask it to
free-associate. Instead:

1. ``gather_evidence`` runs deterministic queries (per-topic activity in the
   window, health issues overlapping it, drops/gaps) and returns a structured
   fact base. Pure, no network, fully testable.
2. The narrative is generated *from that fact base*:
   - ``summarize_rule_based`` — a deterministic template summary. Always
     available, always grounded, no dependencies. This is the floor.
   - When the ``[copilot]`` extra (anthropic) is installed and a key is
     present, the same evidence is handed to the model with strict
     "only use these facts" instructions for a more fluent narrative.

Either way the returned result carries the raw ``evidence`` so the UI can show
the citations behind every claim. If the model is unavailable, the user still
gets a correct (if drier) answer — the feature degrades, it doesn't break.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Model used when the anthropic SDK + key are available.
_DEFAULT_MODEL = "claude-sonnet-4-6"


@dataclass
class ExplainResult:
    """A grounded explanation of a time window."""
    narrative: str
    source: str  # "llm" | "rule_based"
    start_sec: float
    end_sec: float
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "narrative": self.narrative,
            "source": self.source,
            "start_sec": self.start_sec,
            "end_sec": self.end_sec,
            "evidence": self.evidence,
        }


def gather_evidence(
    bag_path: str | Path,
    start_sec: float,
    end_sec: float,
) -> dict[str, Any]:
    """Collect grounded facts about ``[start_sec, end_sec]`` (relative to bag start).

    Pure: opens the bag, runs deterministic queries, returns a JSON-able fact
    base. No LLM. The facts:

    - ``window``: the requested + clamped bounds
    - ``topics``: per-topic message count + approx rate within the window
    - ``health_issues``: health findings whose time range overlaps the window
    - ``totals``: window message count, most/least active topic

    Memory-bounded: per-topic counting streams ``iter_chunks`` and tallies
    in-window rows; nothing is materialized whole.
    """
    from resurrector.core.bag_frame import BagFrame

    bf = BagFrame(bag_path)
    meta = bf.metadata
    bag_start_ns = meta.start_time_ns
    bag_dur = meta.duration_sec

    # Clamp the window to the bag.
    start_sec = max(0.0, min(start_sec, bag_dur))
    end_sec = max(start_sec, min(end_sec, bag_dur))
    win_start_ns = bag_start_ns + int(start_sec * 1e9)
    win_end_ns = bag_start_ns + int(end_sec * 1e9)
    win_dur = max(end_sec - start_sec, 1e-9)

    # Per-topic in-window activity.
    topic_activity: list[dict[str, Any]] = []
    total_in_window = 0
    for ti in meta.topics:
        count = 0
        try:
            view = bf[ti.name]
        except KeyError:
            continue
        for chunk in view.iter_chunks():
            if "timestamp_ns" not in chunk.columns:
                continue
            ts = chunk["timestamp_ns"]
            # Count rows within the window.
            in_win = ((ts >= win_start_ns) & (ts <= win_end_ns)).sum()
            count += int(in_win)
        if count > 0:
            topic_activity.append({
                "topic": ti.name,
                "message_type": ti.message_type,
                "count_in_window": count,
                "rate_in_window_hz": round(count / win_dur, 2),
                "overall_rate_hz": round(ti.frequency_hz, 2) if ti.frequency_hz else None,
            })
            total_in_window += count

    topic_activity.sort(key=lambda t: -t["count_in_window"])

    # Health issues overlapping the window.
    overlapping: list[dict[str, Any]] = []
    try:
        report = bf.health_report()
        for issue in report.issues:
            s = issue.start_time_sec
            e = issue.end_time_sec
            # An issue with no time range is bag-global → include it.
            if s is None and e is None:
                in_range = True
            else:
                # Issue times are absolute seconds (bag epoch); convert window
                # to the same basis by adding bag start.
                abs_start = bag_start_ns / 1e9 + start_sec
                abs_end = bag_start_ns / 1e9 + end_sec
                iss_s = s if s is not None else abs_start
                iss_e = e if e is not None else abs_end
                in_range = not (iss_e < abs_start or iss_s > abs_end)
            if in_range:
                overlapping.append({
                    "check": issue.check_name,
                    "severity": (
                        issue.severity.value
                        if hasattr(issue.severity, "value") else str(issue.severity)
                    ),
                    "topic": issue.topic,
                    "message": issue.message,
                })
    except Exception:
        # Health is best-effort context; never let it break evidence-gathering.
        report = None

    # Flag topics that went quiet: present overall but absent in-window.
    silent_topics = [
        ti.name for ti in meta.topics
        if ti.message_count > 0
        and ti.name not in {t["topic"] for t in topic_activity}
    ]

    return {
        "bag_path": str(bag_path),
        "window": {
            "start_sec": round(start_sec, 3),
            "end_sec": round(end_sec, 3),
            "duration_sec": round(win_dur, 3),
            "bag_duration_sec": round(bag_dur, 3),
        },
        "totals": {
            "messages_in_window": total_in_window,
            "active_topics": len(topic_activity),
            "silent_topics": silent_topics,
        },
        "topics": topic_activity,
        "health_issues": overlapping,
        "health_score": (report.score if report is not None else None),
    }


def summarize_rule_based(evidence: dict[str, Any]) -> str:
    """Deterministic, fully-grounded narrative built from the evidence dict.

    The floor that's always available — no LLM, no network. Every sentence is
    a direct restatement of a queried fact.
    """
    w = evidence["window"]
    totals = evidence["totals"]
    lines = [
        f"In the window {w['start_sec']:.2f}s–{w['end_sec']:.2f}s "
        f"({w['duration_sec']:.2f}s of a {w['bag_duration_sec']:.1f}s bag), "
        f"{totals['messages_in_window']:,} messages were recorded across "
        f"{totals['active_topics']} topic(s)."
    ]

    topics = evidence["topics"]
    if topics:
        top = topics[0]
        lines.append(
            f"Most active: {top['topic']} "
            f"({top['count_in_window']:,} msgs, {top['rate_in_window_hz']:.1f} Hz)."
        )
        # Call out topics running notably below their overall rate.
        slowed = [
            t for t in topics
            if t["overall_rate_hz"] and t["rate_in_window_hz"] < 0.5 * t["overall_rate_hz"]
        ]
        for t in slowed[:3]:
            lines.append(
                f"⚠ {t['topic']} ran at {t['rate_in_window_hz']:.1f} Hz here vs "
                f"{t['overall_rate_hz']:.1f} Hz overall — a slowdown in this window."
            )

    if totals["silent_topics"]:
        st = totals["silent_topics"]
        lines.append(
            f"Silent in this window (no messages): "
            f"{', '.join(st[:5])}{'…' if len(st) > 5 else ''}."
        )

    issues = evidence["health_issues"]
    if issues:
        lines.append(f"{len(issues)} health finding(s) overlap this window:")
        for iss in issues[:5]:
            topic_str = f" [{iss['topic']}]" if iss.get("topic") else ""
            lines.append(f"  - {iss['severity'].upper()} {iss['check']}{topic_str}: {iss['message']}")
    else:
        lines.append("No health findings overlap this window.")

    return "\n".join(lines)


def _llm_narrative(evidence: dict[str, Any], model: str, client: Any = None) -> str:
    """Ask the model to narrate the window using ONLY the provided evidence.

    Raises ImportError if anthropic isn't installed and no client is injected.
    """
    if client is None:
        try:
            import anthropic
        except ImportError:
            raise ImportError(
                "The copilot needs the anthropic SDK. "
                "Install with: pip install 'rosbag-resurrector[copilot]'"
            )
        client = anthropic.Anthropic()

    import json as _json
    system = (
        "You are a robotics data analyst. You will be given a JSON 'evidence' "
        "object describing what was recorded in a time window of a ROS 2 bag. "
        "Write a concise (3-6 sentence) explanation of what happened in this "
        "window for an engineer debugging the run. STRICT RULE: only state "
        "facts present in the evidence JSON. Do not invent topics, rates, "
        "causes, or events that aren't in the data. If the evidence is "
        "insufficient to explain a cause, say so. Reference concrete numbers "
        "(rates, counts, topic names) from the evidence."
    )
    user = (
        "Evidence JSON:\n```json\n"
        + _json.dumps(evidence, indent=2)
        + "\n```\nExplain this window, grounded only in the evidence above."
    )
    resp = client.messages.create(
        model=model,
        max_tokens=600,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    # Anthropic returns a list of content blocks; concatenate text blocks.
    parts = []
    for block in resp.content:
        text = getattr(block, "text", None)
        if text:
            parts.append(text)
    return "".join(parts).strip()


def explain_time_range(
    bag_path: str | Path,
    start_sec: float,
    end_sec: float,
    use_llm: bool = True,
    model: str = _DEFAULT_MODEL,
    client: Any = None,
) -> ExplainResult:
    """Explain a time window, grounded in queried evidence.

    Gathers evidence, then narrates it. Prefers the LLM when available
    (``use_llm`` + anthropic installed + ``ANTHROPIC_API_KEY`` set, or an
    injected ``client``); otherwise falls back to the deterministic
    rule-based summary. The result always includes the raw evidence.
    """
    evidence = gather_evidence(bag_path, start_sec, end_sec)

    want_llm = use_llm and (
        client is not None or os.environ.get("ANTHROPIC_API_KEY")
    )
    if want_llm:
        try:
            narrative = _llm_narrative(evidence, model=model, client=client)
            source = "llm"
        except Exception:
            # Any LLM failure (missing dep, API error) → graceful fallback.
            narrative = summarize_rule_based(evidence)
            source = "rule_based"
    else:
        narrative = summarize_rule_based(evidence)
        source = "rule_based"

    return ExplainResult(
        narrative=narrative,
        source=source,
        start_sec=evidence["window"]["start_sec"],
        end_sec=evidence["window"]["end_sec"],
        evidence=evidence,
    )
