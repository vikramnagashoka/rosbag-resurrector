"""One-click HuggingFace dataset publishing (v0.7 — Feature A).

Takes a dataset directory (typically produced by ``resurrector export`` /
``DatasetManager.materialize``) and publishes it to the HuggingFace Hub with
an **auto-generated dataset card** — the README with YAML frontmatter that HF
renders on the dataset page.

The card is the point. Every published dataset becomes a public HF page that:
- documents the sensor inventory + per-topic stats,
- embeds a QC / quality grade so consumers know if the data is clean,
- credits rosbag-resurrector (top-of-funnel from the exact audience we want).

Design split:
- ``build_dataset_card(...)`` is a **pure function** — no network, fully
  testable. It reads whatever the dataset dir contains (manifest, config,
  LeRobot's ``meta/info.json``) and an optional QC summary, and returns the
  card markdown string.
- ``publish_dataset(...)`` is the thin push: build the card, write it into
  the dir as README.md, upload via ``huggingface_hub``. For a LeRobot
  dataset it then tags the Hub repo with the dataset's codebase version
  (``v3.0``), moving the tag on re-publish, because ``LeRobotDataset``
  refuses an untagged repo. ``dry_run=True`` does everything except the
  network calls (upload and tag), so the path is testable offline.

``huggingface_hub`` is an optional dependency (the ``[publish]`` extra). It's
imported lazily inside ``publish_dataset`` so the card builder — and the rest
of resurrector — never require it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class PublishResult:
    """Outcome of a publish (or dry run)."""
    repo_id: str
    url: str
    card_path: str
    n_files: int
    dry_run: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "repo_id": self.repo_id,
            "url": self.url,
            "card_path": self.card_path,
            "n_files": self.n_files,
            "dry_run": self.dry_run,
        }


def _load_json(path: Path) -> dict[str, Any]:
    try:
        # JSON is UTF-8; the locale default (cp1252 on Windows) would drop
        # a hand-written config whose description isn't ASCII.
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def read_dataset_config(dataset_dir: str | Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return ``(config, metadata)`` from a dataset dir's ``dataset_config.json``.

    ``DatasetManager.export_version`` nests the version config (format,
    topics, bag_refs, ...) under ``"config"`` and the card fields under
    ``"metadata"``. A flat hand-written file is read as the config itself.
    A missing or unreadable file gives ``({}, {})``.
    """
    raw = _load_json(Path(dataset_dir) / "dataset_config.json")
    config = raw.get("config")
    metadata = raw.get("metadata")
    return (
        config if isinstance(config, dict) else raw,
        metadata if isinstance(metadata, dict) else {},
    )


def _lerobot_codebase_version(dataset_dir: Path) -> str | None:
    """Codebase version of a LeRobot dataset dir (``"v3.0"``), else None."""
    version = _load_json(dataset_dir / "meta" / "info.json").get("codebase_version")
    return version if isinstance(version, str) and version else None


def _tag_codebase_version(api: Any, repo_id: str, tag: str) -> None:
    """Point ``tag`` at the commit just uploaded.

    ``LeRobotDataset(repo_id)`` picks its Hub revision from the repo's
    codebase-version tags and refuses a repo that has none. Re-publishing
    has to move the tag, or LeRobot keeps loading the previous upload.
    Same steps as LeRobot's own ``push_to_hub``.
    """
    existing = {t.name for t in api.list_repo_refs(repo_id, repo_type="dataset").tags}
    if tag in existing:
        api.delete_tag(repo_id, tag=tag, repo_type="dataset")
    api.create_tag(repo_id, tag=tag, repo_type="dataset")


# Per-frame index columns LeRobot adds to every dataset; the card lists the
# features the export actually chose (state, action, cameras).
_LEROBOT_INDEX_FEATURES = frozenset(
    {"timestamp", "frame_index", "episode_index", "index", "task_index"}
)


def _exported_topics(config: dict[str, Any], export_format: str) -> tuple[list[str], int, int]:
    """Topics the export actually wrote, as ``DatasetManager.export_version`` picks them.

    A bag's own ``topics`` wins over the version's, and an empty or missing
    filter means every topic in the bag. LeRobot ignores per-bag filters
    and applies the version's list to every episode.

    Returns:
        ``(named, n_unfiltered, n_bags)``: the union of the filters in
        effect (first-seen order), how many bags were exported with every
        topic, and how many bags there are. Without bag refs the version's
        list stands alone as one selection.
    """
    default = list(config.get("topics") or [])
    refs = config.get("bag_refs") or []
    if export_format == "lerobot" or not refs:
        selections = [default]
    else:
        selections = [
            list((ref.get("topics") if isinstance(ref, dict) else None) or default)
            for ref in refs
        ]
    named = list(dict.fromkeys(t for sel in selections for t in sel))
    n_unfiltered = sum(1 for sel in selections if not sel)
    return named, n_unfiltered, len(selections)


def _shape_str(shape: Any) -> str:
    if isinstance(shape, (list, tuple)):
        return "x".join(str(d) for d in shape)
    return str(shape)


def _data_files(dataset_dir: Path, manifest: dict[str, Any]) -> list[str]:
    """Data files from ``manifest.json``, else from a walk of the directory.

    ``resurrector export`` writes no manifest; only ``DatasetManager`` does.
    JSON metadata and markdown aren't counted either way.
    """
    if manifest:
        names = list(manifest)
    else:
        names = [
            f.relative_to(dataset_dir).as_posix()
            for f in dataset_dir.rglob("*")
            if f.is_file()
            and not any(part.startswith(".") for part in f.relative_to(dataset_dir).parts)
        ]
    return [f for f in names if not f.endswith(".json") and not f.endswith(".md")]


def _grade_from_score(score: int) -> str:
    if score >= 90:
        return "A (excellent)"
    if score >= 75:
        return "B (good)"
    if score >= 50:
        return "C (usable — review issues)"
    return "D (problems — inspect before training)"


def build_dataset_card(
    dataset_dir: str | Path,
    repo_id: str,
    qc_summary: dict[str, Any] | None = None,
    license: str = "apache-2.0",
    task_categories: list[str] | None = None,
    extra_description: str | None = None,
) -> str:
    """Build the HuggingFace dataset-card markdown for a dataset directory.

    Pure function — no network, no filesystem writes. Reads ``manifest.json``,
    ``dataset_config.json`` and (for LeRobot datasets) ``meta/info.json``
    from the directory if present; tolerates their absence. Without a
    manifest, data files are counted from the directory itself. A LeRobot
    dataset gets episode/frame/fps rows and a feature table from
    ``meta/info.json`` (a bare ``resurrector export --preset lerobot`` dir
    has nothing else to go on) and a ``LeRobotDataset`` loading snippet;
    everything else gets ``datasets.load_dataset``. Topics are the ones
    the export used: a bag's own filter wins over the version's, and a bag
    with neither was exported, and is shown, with "all topics".

    Args:
        dataset_dir: Path to the materialized dataset.
        repo_id: Target HF repo, e.g. ``myorg/pick-place-v1``.
        qc_summary: Optional dict from ``QCReport.to_dict()`` — its
            ``summary`` + worst-bag info drive the Quality section.
        license: SPDX license id for the YAML frontmatter.
        task_categories: HF task tags; defaults to ``["robotics"]``.
        extra_description: Free-text prepended to the body.

    Returns:
        The full card markdown (YAML frontmatter + body).
    """
    dataset_dir = Path(dataset_dir)
    manifest = _load_json(dataset_dir / "manifest.json")
    config, metadata = read_dataset_config(dataset_dir)
    task_categories = task_categories or ["robotics"]
    # Attribution / description precedence: explicit arg > config description.
    # For re-hosted CC-BY data this is where the required credit lands.
    if extra_description is None:
        extra_description = metadata.get("description") or config.get("description")

    data_files = _data_files(dataset_dir, manifest)
    # `resurrector export --preset lerobot` writes no dataset_config.json;
    # the LeRobot layout identifies itself.
    export_format = config.get("export_format") or (
        "lerobot" if _lerobot_codebase_version(dataset_dir) else "unknown"
    )
    if config:
        topics, n_unfiltered, n_selections = _exported_topics(config, export_format)
    else:
        topics, n_unfiltered, n_selections = [], 0, 0
    # meta/info.json is what LeRobot wrote: episode/frame totals and features.
    info = _load_json(dataset_dir / "meta" / "info.json") if export_format == "lerobot" else {}
    bag_refs = config.get("bag_refs") or []
    name = repo_id.split("/")[-1]

    # --- YAML frontmatter (what HF renders into the dataset header) ------
    fm = [
        "---",
        f"license: {license}",
        "tags:",
        "- robotics",
        "- ros2",
        "- rosbag",
        "task_categories:",
    ]
    for tc in task_categories:
        fm.append(f"- {tc}")
    fm += ["---", ""]

    # --- Body -----------------------------------------------------------
    body = [
        f"# {name}",
        "",
        f"> Published with [RosBag Resurrector]"
        f"(https://github.com/vikramnagashoka/rosbag-resurrector) "
        f"on {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]
    if extra_description:
        body += [extra_description, ""]

    # Overview table
    overview = [("Format", f"`{export_format}`")]
    # A bare LeRobot export has no config, so its bags and topics are
    # unknown; info.json's totals below say what's in it instead of "0".
    if config or not info:
        overview.append(("Source bags", str(len(bag_refs))))
        if n_unfiltered == 0:
            topics_cell = str(len(topics))
        elif n_unfiltered == n_selections:
            topics_cell = "all topics"
        else:
            topics_cell = (
                f"{len(topics)} listed + all topics from "
                f"{n_unfiltered} of {n_selections} bags"
            )
        overview.append(("Topics", topics_cell))
    if info.get("total_episodes") is not None:
        overview.append(("Episodes", str(info["total_episodes"])))
    if info.get("total_frames") is not None:
        overview.append(("Frames", str(info["total_frames"])))
    if info.get("fps"):
        overview.append(("Frame rate", f"{info['fps']} fps"))
    overview.append(("Data files", str(len(data_files))))
    body += [
        "## Overview",
        "",
        "| | |",
        "|---|---|",
        *[f"| {k} | {v} |" for k, v in overview],
        "",
    ]

    # Quality section (the differentiator — consumers see the grade up front)
    if qc_summary:
        summary = qc_summary.get("summary", {})
        n_err = summary.get("n_errors", 0)
        n_warn = summary.get("n_warnings", 0)
        n_bags = summary.get("n_bags", len(bag_refs))
        # Derive a coarse grade from error/warning density.
        if n_err > 0:
            grade = "D (errors present — inspect before training)"
        elif n_warn > n_bags:
            grade = "C (usable — review warnings)"
        elif n_warn > 0:
            grade = "B (good — minor warnings)"
        else:
            grade = "A (clean — no QC issues)"
        body += [
            "## Data quality",
            "",
            f"**Grade: {grade}**",
            "",
            f"- Bags checked: {n_bags}",
            f"- Errors: {n_err}",
            f"- Warnings: {n_warn}",
            "",
            "Quality assessed by `resurrector qc` before publishing "
            "(upstream bag-side checks: schema drift, rate anomalies, "
            "coverage gaps, message drops).",
            "",
        ]

    # Topics
    if topics:
        body += ["## Topics", ""]
        for t in topics[:50]:
            body.append(f"- `{t}`")
        if len(topics) > 50:
            body.append(f"- … and {len(topics) - 50} more")
        if n_unfiltered:
            body.append(
                f"- plus every topic in {n_unfiltered} of {n_selections} "
                "source bags (no topic filter)"
            )
        body.append("")

    features = info.get("features")
    if isinstance(features, dict):
        rows = [
            f"| `{key}` | {ft.get('dtype', '?')} | {_shape_str(ft.get('shape', '?'))} |"
            for key, ft in features.items()
            if key not in _LEROBOT_INDEX_FEATURES and isinstance(ft, dict)
        ]
        if rows:
            body += [
                "## Features",
                "",
                "| Feature | Type | Shape |",
                "|---|---|---|",
                *rows,
                "",
            ]

    # Load snippet. A LeRobot v3 dataset (episode metadata, MP4 cameras)
    # isn't a plain HF table; LeRobot's own loader is the way in.
    if export_format == "lerobot":
        load = [
            "from lerobot.datasets.lerobot_dataset import LeRobotDataset",
            f'ds = LeRobotDataset("{repo_id}")',
        ]
    else:
        load = [
            "from datasets import load_dataset",
            f'ds = load_dataset("{repo_id}")',
        ]
    body += [
        "## Loading",
        "",
        "```python",
        *load,
        "```",
        "",
        "---",
        "",
        "*Card auto-generated. Edit freely — re-publishing overwrites it.*",
    ]

    return "\n".join(fm + body)


def publish_dataset(
    dataset_dir: str | Path,
    repo_id: str,
    token: str | None = None,
    private: bool = False,
    qc_summary: dict[str, Any] | None = None,
    license: str = "apache-2.0",
    dry_run: bool = False,
    extra_description: str | None = None,
) -> PublishResult:
    """Publish a dataset directory to the HuggingFace Hub.

    Builds the dataset card, writes it as ``README.md`` into the directory,
    and uploads the whole folder. A LeRobot dataset's repo is then tagged
    with its codebase version (e.g. ``v3.0``), which ``LeRobotDataset``
    requires before it will load from the Hub. With ``dry_run=True`` it does
    everything except the network calls (writes the card, counts files) so
    the flow is verifiable offline.

    Args:
        dataset_dir: Materialized dataset directory.
        repo_id: Target repo, e.g. ``myorg/pick-place-v1``.
        token: HF token. If None, ``huggingface_hub`` falls back to the
            cached login / ``HF_TOKEN`` env var.
        private: Create the repo private.
        qc_summary: Optional ``QCReport.to_dict()`` for the quality section.
        license: SPDX license id.
        dry_run: Skip the upload (card is still written locally).

    Returns:
        :class:`PublishResult`.

    Raises:
        FileNotFoundError: dataset_dir doesn't exist.
        ImportError: huggingface_hub not installed (only when not dry_run).
    """
    dataset_dir = Path(dataset_dir)
    if not dataset_dir.is_dir():
        raise FileNotFoundError(f"Dataset directory not found: {dataset_dir}")

    card = build_dataset_card(
        dataset_dir, repo_id, qc_summary=qc_summary, license=license,
        extra_description=extra_description,
    )
    card_path = dataset_dir / "README.md"
    # The card carries user-written descriptions; the locale default
    # (cp1252 on Windows) can't encode most non-Latin text.
    card_path.write_text(card, encoding="utf-8")

    n_files = sum(1 for f in dataset_dir.rglob("*") if f.is_file())
    url = f"https://huggingface.co/datasets/{repo_id}"

    if dry_run:
        return PublishResult(
            repo_id=repo_id, url=url, card_path=str(card_path),
            n_files=n_files, dry_run=True,
        )

    try:
        from huggingface_hub import HfApi
    except ImportError:
        raise ImportError(
            "Publishing needs the huggingface_hub package. "
            "Install with: pip install 'rosbag-resurrector[publish]'"
        )

    api = HfApi(token=token)
    api.create_repo(
        repo_id=repo_id, repo_type="dataset",
        private=private, exist_ok=True,
    )
    api.upload_folder(
        folder_path=str(dataset_dir),
        repo_id=repo_id,
        repo_type="dataset",
    )
    codebase_version = _lerobot_codebase_version(dataset_dir)
    if codebase_version:
        _tag_codebase_version(api, repo_id, codebase_version)
    return PublishResult(
        repo_id=repo_id, url=url, card_path=str(card_path),
        n_files=n_files, dry_run=False,
    )
