"""Bag Contracts (v0.7.1 — demand-validated from outreach).

Turn "does this run look sane?" into a **versioned spec teams enforce in CI.**

A contract declares what a good bag looks like: expected topics, their
message types, acceptable rate ranges, and required TF frames. ``init``
infers a contract from known-good bags; ``check`` validates a candidate bag
against it and exits non-zero on violation.

Relationship to ``benchmark`` (v0.7): benchmark is numeric-metrics-vs-baseline;
a contract is the *declarative, human-editable, git-versionable* framing of
the same CI-gating job — readable enough that a team owns it as a spec file.

Format: YAML when pyyaml is importable (nicer to hand-edit), JSON always
(no new hard dependency). Detected by file extension.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from resurrector.ingest.parser import parse_bag

# How many /tf messages to sample when discovering frames. Frames are
# near-static, so a sample is plenty and keeps discovery memory-bounded.
_TF_SAMPLE_LIMIT = 500
# Default rate tolerance when inferring: widen the observed [min,max] by ±20%
# so normal run-to-run jitter doesn't trip the contract.
_DEFAULT_RATE_TOLERANCE = 0.2


@dataclass
class TopicContract:
    message_type: str | None = None
    min_rate_hz: float | None = None
    max_rate_hz: float | None = None

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {}
        if self.message_type is not None:
            d["type"] = self.message_type
        if self.min_rate_hz is not None or self.max_rate_hz is not None:
            d["rate_hz"] = {
                "min": round(self.min_rate_hz, 2) if self.min_rate_hz is not None else None,
                "max": round(self.max_rate_hz, 2) if self.max_rate_hz is not None else None,
            }
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "TopicContract":
        rate = d.get("rate_hz") or {}
        return cls(
            message_type=d.get("type"),
            min_rate_hz=rate.get("min"),
            max_rate_hz=rate.get("max"),
        )


@dataclass
class Contract:
    version: int = 1
    topics: dict[str, TopicContract] = field(default_factory=dict)
    tf_frames: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "topics": {name: tc.to_dict() for name, tc in self.topics.items()},
            "tf_frames": sorted(self.tf_frames),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Contract":
        return cls(
            version=int(d.get("version", 1)),
            topics={
                name: TopicContract.from_dict(tc or {})
                for name, tc in (d.get("topics") or {}).items()
            },
            tf_frames=list(d.get("tf_frames") or []),
        )


@dataclass
class ContractViolation:
    code: str  # missing_topic | wrong_type | rate_low | rate_high | missing_tf_frame
    topic: str | None
    message: str
    severity: str = "error"

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code, "topic": self.topic,
            "message": self.message, "severity": self.severity,
        }


@dataclass
class ContractResult:
    bag_path: str
    violations: list[ContractViolation] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.violations

    def to_dict(self) -> dict[str, Any]:
        return {
            "bag_path": self.bag_path,
            "passed": self.passed,
            "n_violations": len(self.violations),
            "violations": [v.to_dict() for v in self.violations],
        }

    def to_json(self, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


# ---------------------------------------------------------------------------
# TF frame discovery
# ---------------------------------------------------------------------------


def _discover_tf_frames(bag_path: str | Path) -> set[str]:
    """Collect TF frame names (parent + child) from /tf and /tf_static.

    Bounded: reads all of /tf_static (tiny) and samples up to
    ``_TF_SAMPLE_LIMIT`` /tf messages. Best-effort — returns whatever it
    parses, never raises (TF parsing failures shouldn't sink a contract run).
    """
    from resurrector.core.scene import parse_tf_message

    frames: set[str] = set()
    try:
        parser = parse_bag(bag_path)
    except Exception:
        return frames

    topic_names = {ti.name for ti in parser.get_metadata().topics}
    for topic, is_static, limit in (
        ("/tf_static", True, None),
        ("/tf", False, _TF_SAMPLE_LIMIT),
    ):
        if topic not in topic_names:
            continue
        n = 0
        try:
            for msg in parser.read_messages(topics=[topic]):
                if msg.raw_data is None:
                    continue
                for tf in parse_tf_message(msg.raw_data, is_static=is_static):
                    frames.add(tf.parent_frame)
                    frames.add(tf.child_frame)
                n += 1
                if limit is not None and n >= limit:
                    break
        except Exception:
            continue
    frames.discard("")
    return frames


# ---------------------------------------------------------------------------
# Infer
# ---------------------------------------------------------------------------


def infer_contract(
    bags: list[str | Path],
    rate_tolerance: float = _DEFAULT_RATE_TOLERANCE,
) -> Contract:
    """Infer a contract from known-good bags.

    A topic is **required** if it appears in *every* provided bag (the strict,
    useful default — a topic missing from one good run isn't a guarantee). Its
    rate range is the observed [min, max] across bags, widened by
    ``rate_tolerance`` so normal jitter passes. Required TF frames are those
    present in every bag.

    Raises ``ValueError`` if no bags are given.
    """
    paths = [Path(b) for b in bags]
    if not paths:
        raise ValueError("infer_contract needs at least one good bag")

    # Per-bag topic→(type, rate); per-bag frame sets.
    per_bag_topics: list[dict[str, tuple[str, float | None]]] = []
    per_bag_frames: list[set[str]] = []
    for p in paths:
        meta = parse_bag(p).get_metadata()
        per_bag_topics.append({
            ti.name: (ti.message_type, ti.frequency_hz) for ti in meta.topics
        })
        per_bag_frames.append(_discover_tf_frames(p))

    # Required topics = intersection across all bags.
    common = set(per_bag_topics[0])
    for d in per_bag_topics[1:]:
        common &= set(d)

    topics: dict[str, TopicContract] = {}
    for name in sorted(common):
        types = {d[name][0] for d in per_bag_topics}
        rates = [d[name][1] for d in per_bag_topics if d[name][1] and d[name][1] > 0]
        tc = TopicContract(
            message_type=next(iter(types)) if len(types) == 1 else None,
        )
        if rates:
            lo, hi = min(rates), max(rates)
            tc.min_rate_hz = max(0.0, lo * (1 - rate_tolerance))
            tc.max_rate_hz = hi * (1 + rate_tolerance)
        topics[name] = tc

    # Required frames = intersection (only meaningful if at least one bag had TF).
    frames_with_tf = [f for f in per_bag_frames if f]
    if frames_with_tf:
        common_frames = set(frames_with_tf[0])
        for f in frames_with_tf[1:]:
            common_frames &= f
    else:
        common_frames = set()

    return Contract(version=1, topics=topics, tf_frames=sorted(common_frames))


# ---------------------------------------------------------------------------
# Check
# ---------------------------------------------------------------------------


def check_contract(bag_path: str | Path, contract: Contract) -> ContractResult:
    """Validate a bag against a contract. Collects all violations (doesn't
    stop at the first), so a CI run reports everything wrong at once."""
    result = ContractResult(bag_path=str(bag_path))
    try:
        meta = parse_bag(bag_path).get_metadata()
    except Exception as e:
        result.violations.append(ContractViolation(
            code="bag_unreadable", topic=None,
            message=f"Could not open bag: {type(e).__name__}: {e}",
        ))
        return result

    by_name = {ti.name: ti for ti in meta.topics}

    for name, tc in contract.topics.items():
        ti = by_name.get(name)
        if ti is None:
            result.violations.append(ContractViolation(
                code="missing_topic", topic=name,
                message=f"Required topic {name!r} is absent",
            ))
            continue
        if tc.message_type and ti.message_type != tc.message_type:
            result.violations.append(ContractViolation(
                code="wrong_type", topic=name,
                message=(
                    f"{name}: type {ti.message_type!r} != "
                    f"contract {tc.message_type!r}"
                ),
            ))
        hz = ti.frequency_hz
        if hz is not None and hz > 0:
            if tc.min_rate_hz is not None and hz < tc.min_rate_hz:
                result.violations.append(ContractViolation(
                    code="rate_low", topic=name,
                    message=(
                        f"{name}: {hz:.1f} Hz below contract min "
                        f"{tc.min_rate_hz:.1f} Hz"
                    ),
                ))
            if tc.max_rate_hz is not None and hz > tc.max_rate_hz:
                result.violations.append(ContractViolation(
                    code="rate_high", topic=name,
                    message=(
                        f"{name}: {hz:.1f} Hz above contract max "
                        f"{tc.max_rate_hz:.1f} Hz"
                    ),
                ))

    if contract.tf_frames:
        present = _discover_tf_frames(bag_path)
        for frame in contract.tf_frames:
            if frame not in present:
                result.violations.append(ContractViolation(
                    code="missing_tf_frame", topic=None,
                    message=f"Required TF frame {frame!r} not found",
                ))

    return result


# ---------------------------------------------------------------------------
# Load / save (YAML if available, else JSON; by extension)
# ---------------------------------------------------------------------------


def _is_yaml(path: Path) -> bool:
    return path.suffix.lower() in (".yaml", ".yml")


def save_contract(contract: Contract, path: str | Path) -> Path:
    path = Path(path)
    data = contract.to_dict()
    if _is_yaml(path):
        try:
            import yaml
        except ImportError as e:
            raise RuntimeError(
                "Writing a .yaml contract needs pyyaml (pip install pyyaml), "
                "or use a .json path instead."
            ) from e
        path.write_text(yaml.safe_dump(data, sort_keys=False))
    else:
        path.write_text(json.dumps(data, indent=2))
    return path


def load_contract(path: str | Path) -> Contract:
    path = Path(path)
    text = path.read_text()
    if _is_yaml(path):
        try:
            import yaml
        except ImportError as e:
            raise RuntimeError(
                "Reading a .yaml contract needs pyyaml (pip install pyyaml)."
            ) from e
        data = yaml.safe_load(text)
    else:
        data = json.loads(text)
    return Contract.from_dict(data or {})
