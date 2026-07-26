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
  testable. It reads whatever the dataset dir contains (manifest, config)
  and an optional QC summary, and returns the card markdown string.
- ``publish_dataset(...)`` is the thin push: build the card, write it into
  the dir as README.md, upload via ``huggingface_hub``. ``dry_run=True``
  does everything except the upload, so the path is testable offline.

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
        return json.loads(path.read_text())
    except Exception:
        return {}


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

    Pure function — no network, no filesystem writes. Reads ``manifest.json``
    and ``dataset_config.json`` from the directory if present; tolerates their
    absence.

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
    config = _load_json(dataset_dir / "dataset_config.json")
    task_categories = task_categories or ["robotics"]
    # Attribution / description precedence: explicit arg > config description.
    # For re-hosted CC-BY data this is where the required credit lands.
    if extra_description is None:
        extra_description = config.get("description")

    data_files = [f for f in manifest if not f.endswith(".json") and not f.endswith(".md")]
    topics = config.get("topics") or []
    export_format = config.get("export_format", "unknown")
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
    body += [
        "## Overview",
        "",
        "| | |",
        "|---|---|",
        f"| Format | `{export_format}` |",
        f"| Source bags | {len(bag_refs)} |",
        f"| Topics | {len(topics)} |",
        f"| Data files | {len(data_files)} |",
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
        body.append("")

    # Load snippet
    body += [
        "## Loading",
        "",
        "```python",
        "from datasets import load_dataset",
        f'ds = load_dataset("{repo_id}")',
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
    and uploads the whole folder. With ``dry_run=True`` it does everything
    except the network upload (writes the card, counts files) so the flow is
    verifiable offline.

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
    card_path.write_text(card)

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
    return PublishResult(
        repo_id=repo_id, url=url, card_path=str(card_path),
        n_files=n_files, dry_run=False,
    )
