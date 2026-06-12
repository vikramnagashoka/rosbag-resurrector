"""Semantic bag diff (v0.7 — Feature F).

Answers "what changed between these two runs?" — the bag equivalent of a
git diff. Compares two bags' *structure and statistics*, not their raw bytes:
topics added/removed, message-type changes, schema (`.msg` definition) drift,
recording-rate shifts, message-count deltas, and duration change.

This is metadata-level by default (cheap — reads summaries, not messages), so
it works on huge bags without loading them. The intended uses:

- "Did my code change alter what the robot records?" (run N vs run N-1)
- "Why does this bag behave differently from the known-good one?"
- A reviewable artifact attached to a robotics PR.

Distinct from the cross-bag QC/anomaly passes: those score a *fleet* for
outliers; this is a focused pairwise A-vs-B comparison with a clear baseline
(A) and candidate (B).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from resurrector.ingest.parser import parse_bag

# A rate change below this fraction is treated as noise, not a real shift.
# Recording rates wobble run-to-run; only flag meaningful deltas.
_RATE_CHANGE_THRESHOLD = 0.10  # 10%


@dataclass
class TopicChange:
    """A per-topic difference between bag A and bag B."""
    topic: str
    kind: str  # message_type | schema | rate | count
    detail: str
    a_value: Any
    b_value: Any

    def to_dict(self) -> dict[str, Any]:
        return {
            "topic": self.topic,
            "kind": self.kind,
            "detail": self.detail,
            "a_value": self.a_value,
            "b_value": self.b_value,
        }


@dataclass
class BagDiff:
    """Structured A-vs-B difference. A is the baseline, B the candidate."""
    bag_a: str
    bag_b: str
    topics_added: list[str] = field(default_factory=list)    # in B, not A
    topics_removed: list[str] = field(default_factory=list)  # in A, not B
    topic_changes: list[TopicChange] = field(default_factory=list)
    duration_a_sec: float = 0.0
    duration_b_sec: float = 0.0
    message_count_a: int = 0
    message_count_b: int = 0

    @property
    def duration_delta_sec(self) -> float:
        return self.duration_b_sec - self.duration_a_sec

    @property
    def message_count_delta(self) -> int:
        return self.message_count_b - self.message_count_a

    @property
    def has_changes(self) -> bool:
        return bool(
            self.topics_added
            or self.topics_removed
            or self.topic_changes
        )

    @property
    def n_changes(self) -> int:
        return (
            len(self.topics_added)
            + len(self.topics_removed)
            + len(self.topic_changes)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "bag_a": self.bag_a,
            "bag_b": self.bag_b,
            "topics_added": self.topics_added,
            "topics_removed": self.topics_removed,
            "topic_changes": [c.to_dict() for c in self.topic_changes],
            "duration_a_sec": round(self.duration_a_sec, 3),
            "duration_b_sec": round(self.duration_b_sec, 3),
            "duration_delta_sec": round(self.duration_delta_sec, 3),
            "message_count_a": self.message_count_a,
            "message_count_b": self.message_count_b,
            "message_count_delta": self.message_count_delta,
            "summary": {
                "topics_added": len(self.topics_added),
                "topics_removed": len(self.topics_removed),
                "topic_changes": len(self.topic_changes),
                "has_changes": self.has_changes,
            },
        }

    def to_json(self, indent: int | None = 2) -> str:
        import json
        return json.dumps(self.to_dict(), indent=indent)


def diff_bags(bag_a: str | Path, bag_b: str | Path) -> BagDiff:
    """Compute the structural + statistical diff from bag A (baseline) to bag B.

    Args:
        bag_a: Baseline bag path.
        bag_b: Candidate bag path.

    Returns:
        :class:`BagDiff`. Raises FileNotFoundError if either path is missing
        (unlike fleet QC, a pairwise diff with a missing operand is a hard
        error — there's no partial result worth returning).
    """
    bag_a, bag_b = Path(bag_a), Path(bag_b)
    if not bag_a.exists():
        raise FileNotFoundError(f"Baseline bag not found: {bag_a}")
    if not bag_b.exists():
        raise FileNotFoundError(f"Candidate bag not found: {bag_b}")

    meta_a = parse_bag(bag_a).get_metadata()
    meta_b = parse_bag(bag_b).get_metadata()

    topics_a = {ti.name: ti for ti in meta_a.topics}
    topics_b = {ti.name: ti for ti in meta_b.topics}

    added = sorted(set(topics_b) - set(topics_a))
    removed = sorted(set(topics_a) - set(topics_b))

    changes: list[TopicChange] = []
    for name in sorted(set(topics_a) & set(topics_b)):
        ta, tb = topics_a[name], topics_b[name]

        # Message type change (the most significant — same topic, new type)
        if ta.message_type != tb.message_type:
            changes.append(TopicChange(
                topic=name, kind="message_type",
                detail=f"type changed {ta.message_type} → {tb.message_type}",
                a_value=ta.message_type, b_value=tb.message_type,
            ))
            # If the type changed, schema/rate comparisons are meaningless.
            continue

        # Schema (.msg definition) drift
        sa = ta.schema_data or ""
        sb = tb.schema_data or ""
        if sa and sb and sa != sb:
            changes.append(TopicChange(
                topic=name, kind="schema",
                detail="schema_data differs (same type, different .msg definition)",
                a_value=None, b_value=None,
            ))

        # Recording-rate shift
        ra = ta.frequency_hz
        rb = tb.frequency_hz
        if ra and rb and ra > 0:
            frac = abs(rb - ra) / ra
            if frac >= _RATE_CHANGE_THRESHOLD:
                changes.append(TopicChange(
                    topic=name, kind="rate",
                    detail=f"rate {ra:.1f} Hz → {rb:.1f} Hz ({(rb - ra) / ra * 100:+.0f}%)",
                    a_value=round(ra, 2), b_value=round(rb, 2),
                ))

        # Message-count change
        if ta.message_count != tb.message_count:
            changes.append(TopicChange(
                topic=name, kind="count",
                detail=f"count {ta.message_count:,} → {tb.message_count:,} "
                       f"({tb.message_count - ta.message_count:+,})",
                a_value=ta.message_count, b_value=tb.message_count,
            ))

    return BagDiff(
        bag_a=str(bag_a),
        bag_b=str(bag_b),
        topics_added=added,
        topics_removed=removed,
        topic_changes=changes,
        duration_a_sec=meta_a.duration_sec,
        duration_b_sec=meta_b.duration_sec,
        message_count_a=meta_a.message_count,
        message_count_b=meta_b.message_count,
    )
