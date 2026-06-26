"""RosBag Resurrector CLI — Typer-based command line interface."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Optional

import typer
from rich.console import Console

app = typer.Typer(
    name="resurrector",
    help="RosBag Resurrector — Stop letting your rosbag data rot.",
    add_completion=False,
    # Use Markdown rendering for docstrings instead of the default Rich
    # markup mode. In Rich mode, brackets like `[vision]` (in our pip
    # extras strings) get parsed as markup tags and silently stripped.
    # Markdown treats brackets as plain text AND preserves lists / code
    # blocks / paragraphs in the docstrings, which the plain-text
    # fallback (mode=None) flattens into one wall of text.
    rich_markup_mode="markdown",
)
console = Console()


def _print_version_and_exit(value: bool) -> None:
    """`--version` callback. Typer doesn't add this automatically."""
    if not value:
        return
    from resurrector import __version__
    console.print(f"resurrector {__version__}")
    raise typer.Exit()


@app.callback()
def _root_callback(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-V",
            help="Show the installed resurrector version and exit.",
            callback=_print_version_and_exit,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """Root-level options shared across every subcommand."""
    # Only here so Typer wires up the --version flag at the top level;
    # the actual handling lives in _print_version_and_exit.
    pass


def _setup_logging(verbose: bool = False, log_file: str | None = None):
    """Initialize logging based on CLI flags."""
    from resurrector.logging_config import setup_logging
    setup_logging(
        level="DEBUG" if verbose else "WARNING",
        log_file=log_file,
        verbose=verbose,
    )


@app.command()
def scan(
    path: Annotated[Path, typer.Argument(
        help="Directory or file to scan for bag files. "
             "e.g. resurrector scan ~/recordings",
    )],
    db: Annotated[Optional[Path], typer.Option("--db",
        help="Path to a non-default index database. Defaults to "
             "~/.resurrector/index.db. e.g. --db /data/myindex.db",
    )] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v",
        help="Enable verbose logging. e.g. -v",
    )] = False,
    log_file: Annotated[Optional[str], typer.Option("--log-file",
        help="Also write logs to a file. e.g. --log-file ./scan.log",
    )] = None,
    skip_frame_index: Annotated[bool, typer.Option(
        "--skip-frame-index",
        help="Skip pre-building the frame-offset cache for image topics. "
             "Dashboard/search will build it lazily on first access. "
             "e.g. --skip-frame-index",
    )] = False,
    full_hash: Annotated[bool, typer.Option(
        "--full-hash",
        help="Also compute a real SHA256 over every byte of each bag and "
             "store it in the index (column: sha256_full). Slow on large "
             "bags. Default behavior uses a fast fingerprint (first 1 MB + "
             "size) which is sufficient for change detection but is NOT "
             "a cryptographic digest. e.g. --full-hash",
    )] = False,
):
    """Scan a directory for bag files and index them.

    Pre-builds the frame-offset cache for image topics during indexing,
    so the dashboard's ImageViewer and semantic search thumbnails are
    fast on the first access. Use --skip-frame-index to defer that work.
    """
    _setup_logging(verbose, log_file)
    from resurrector.ingest.scanner import scan_path
    from resurrector.ingest.parser import parse_bag
    from resurrector.ingest.indexer import BagIndex
    from resurrector.ingest.frame_index import build_frame_offsets, image_topics
    from resurrector.cli.formatters import create_progress, console as fmt_console

    files = scan_path(path, full_hash=full_hash)
    if not files:
        console.print("[yellow]No bag files found.[/yellow]")
        raise typer.Exit()

    console.print(f"Found [cyan]{len(files)}[/cyan] bag file(s)")

    index = BagIndex(db) if db else BagIndex()
    with create_progress() as progress:
        task = progress.add_task("Indexing bags...", total=len(files))
        for scanned_file in files:
            try:
                parser = parse_bag(scanned_file.path)
                metadata = parser.get_metadata()
                bag_id = index.upsert_bag(scanned_file, metadata)

                # Run health check
                from resurrector.core.bag_frame import BagFrame
                bf = BagFrame(scanned_file.path)
                report = bf.health_report()
                index.update_health_score(bag_id, report.score)
                for topic_name, th in report.topic_scores.items():
                    index.update_topic_health(bag_id, topic_name, th.score)

                # Pre-build frame offsets for image topics unless asked
                # to skip. This is cheap because we already have the
                # parser warm; amortizes the thundering-herd cost when
                # semantic search returns N thumbnails for the bag.
                if not skip_frame_index:
                    img_topics = [
                        t.name for t in metadata.topics
                        if t.message_type in {
                            "sensor_msgs/msg/Image",
                            "sensor_msgs/msg/CompressedImage",
                        }
                    ]
                    if img_topics:
                        build_frame_offsets(
                            index, bag_id, scanned_file.path, topics=img_topics,
                        )

                progress.advance(task)
            except Exception as e:
                console.print(f"[red]Error indexing {scanned_file.path.name}: {e}[/red]")
                progress.advance(task)

    console.print(f"[green]Indexed {index.count()} bag(s) total.[/green]")
    index.close()


@app.command()
def info(
    path: Annotated[Path, typer.Argument(
        help="Path to a bag file (.mcap, .bag, or .db3). "
             "e.g. resurrector info experiment.mcap",
    )],
):
    """Print a detailed summary of a single bag: topics, message counts, duration, health.

    Reads the bag's metadata, computes a health report, and renders both
    as a formatted table. Useful as a "what's in this file?" first
    inspection before scanning into the index.

    Example:
      resurrector info experiment.mcap
    """
    from resurrector.core.bag_frame import BagFrame
    from resurrector.cli.formatters import print_bag_info

    bf = BagFrame(path)
    health = bf.health_report()
    print_bag_info(bf.metadata, health, path.stat().st_size)


@app.command()
def health(
    path: Annotated[Path, typer.Argument(
        help="A single bag file or a directory of bags. Directories are "
             "scanned recursively and every found bag gets its own report. "
             "e.g. resurrector health experiment.mcap"
    )],
    format: Annotated[str, typer.Option("--format", "-f",
        help="Output format. 'rich' (default) for human-readable tables in "
             "the terminal; 'json' for machine-readable output (pipe-friendly, "
             "stable schema). e.g. -f json",
    )] = "rich",
    output: Annotated[Optional[Path], typer.Option("--output", "-o",
        help="Write the report to this path instead of stdout. Only used "
             "with --format json. e.g. -o report.json",
    )] = None,
):
    """Score every bag for data-quality issues — dropped messages, time gaps, anomalies.

    Each bag gets a 0-100 health score plus per-topic breakdowns and a
    list of specific findings (out-of-order timestamps, frequency drift,
    missing-topic gaps, oversized messages, etc.). Use this before
    training to avoid feeding bad data into your model.

    Examples:
      Single bag, formatted for the terminal:
          resurrector health experiment.mcap

      Whole directory, machine-readable JSON output:
          resurrector health ./bags --format json --output report.json
    """
    from resurrector.core.bag_frame import BagFrame
    from resurrector.ingest.scanner import scan_path
    from resurrector.cli.formatters import print_health_report

    if path.is_dir():
        files = scan_path(path)
        paths = [f.path for f in files]
    else:
        paths = [path]

    all_reports = {}
    for bag_path in paths:
        bf = BagFrame(bag_path)
        report = bf.health_report()
        all_reports[str(bag_path)] = report

        if format == "rich":
            print_health_report(report, bag_path.name)

    if format == "json":
        json_data = {}
        for bag_path_str, report in all_reports.items():
            json_data[bag_path_str] = {
                "score": report.score,
                "issues": [
                    {
                        "check": i.check_name,
                        "severity": i.severity.value,
                        "message": i.message,
                        "topic": i.topic,
                        "start_time": i.start_time_sec,
                    }
                    for i in report.issues
                ],
                "recommendations": report.recommendations,
                "topic_scores": {
                    k: v.score for k, v in report.topic_scores.items()
                },
            }
        json_str = json.dumps(json_data, indent=2)
        if output:
            output.write_text(json_str)
            console.print(f"[green]Report saved to {output}[/green]")
        else:
            console.print(json_str)


@app.command()
def qc(
    paths: Annotated[list[Path], typer.Argument(
        help="One or more bag files or directories. Directories are scanned "
             "recursively for .mcap. e.g. resurrector qc bag_a.mcap bag_b.mcap "
             "or resurrector qc /data/bags",
    )],
    json_output: Annotated[Optional[Path], typer.Option("--json",
        help="Write the QC report as JSON to this path (machine-readable, "
             "stable schema). Suitable for CI gating. e.g. --json qc.json",
    )] = None,
    fail_on_error: Annotated[bool, typer.Option("--fail-on-error",
        help="Exit non-zero if the report contains any 'error'-severity "
             "issues. Useful for CI pipelines. e.g. --fail-on-error",
    )] = False,
    anomalies: Annotated[bool, typer.Option("--anomalies",
        help="Also run relative cross-bag outlier detection and rank the "
             "fleet 'most suspicious first'. Needs >=3 bags. e.g. --anomalies",
    )] = False,
):
    """Bag-side data-quality checks for a single bag or a fleet of bags.

    Runs upstream QC before bags become training datasets:

    - **Per-bag**: health score (drops, gaps, frequency drift, oversized
      messages) plus empty / very-short-bag detection.
    - **Cross-bag fleet**: schema drift (same topic with different
      message_type / .msg definition across bags), topic-set divergence
      (topic present in some bags but not others), recording-rate
      anomalies (>50% deviation from cross-bag median), coverage gaps
      and overlaps.

    Complementary to lerobot-doctor — that tool QCs converted LeRobot
    datasets; this one QCs the bags BEFORE conversion. Catches
    bag-collection problems (drift across recording sessions, missing
    sensors on one robot, etc.) that a single-dataset check wouldn't
    surface.

    Examples:
      Single bag, console output:
          resurrector qc experiment.mcap

      Whole campaign with JSON for CI:
          resurrector qc /data/campaign --json campaign_qc.json --fail-on-error
    """
    from resurrector.core.qc import run_qc
    from rich.table import Table

    report = run_qc(paths, detect_anomalies=anomalies)

    if json_output:
        json_output.write_text(report.to_json())
        console.print(f"[green]QC report saved to {json_output}[/green]")
    else:
        # Render to console
        console.print(
            f"\n[bold]Bag-side QC: {report.n_bags} bag(s)[/bold]  "
            f"[red]{report.n_errors} error(s)[/red]  "
            f"[yellow]{report.n_warnings} warning(s)[/yellow]\n"
        )
        # Per-bag summary
        if report.bags:
            table = Table(title="Per-bag")
            table.add_column("Bag")
            table.add_column("Score", justify="right")
            table.add_column("Duration", justify="right")
            table.add_column("Messages", justify="right")
            table.add_column("Topics", justify="right")
            table.add_column("Issues", justify="right")
            for b in report.bags:
                score_color = "green" if b.health_score >= 80 else (
                    "yellow" if b.health_score >= 50 else "red"
                )
                table.add_row(
                    Path(b.bag_path).name,
                    f"[{score_color}]{b.health_score}[/{score_color}]",
                    f"{b.duration_sec:.1f}s",
                    f"{b.message_count:,}",
                    str(b.n_topics),
                    str(len(b.issues)),
                )
            console.print(table)

        # Anomaly ranking (only populated with --anomalies)
        if report.anomaly_ranking:
            from rich.markup import escape as rich_escape
            console.print("\n[bold]Most suspicious bags[/bold] [dim](relative outliers)[/dim]")
            ranked = [r for r in report.anomaly_ranking if r["score"] > 0]
            if not ranked:
                console.print("  [green]No relative outliers — the fleet looks consistent.[/green]")
            for r in ranked[:10]:
                console.print(
                    f"  [yellow]{r['score']}×[/yellow] {rich_escape(r['name'])} "
                    f"[dim]{rich_escape('; '.join(r['reasons'][:3]))}"
                    f"{'...' if len(r['reasons']) > 3 else ''}[/dim]"
                )
        elif anomalies:
            console.print(
                "\n[dim]Anomaly detection needs >=3 readable bags; "
                "skipped.[/dim]"
            )

        # Fleet issues
        from rich.markup import escape as rich_escape

        if report.fleet_issues:
            console.print("\n[bold]Cross-bag findings[/bold]")
            for issue in report.fleet_issues:
                color = {"error": "red", "warning": "yellow", "info": "cyan"}[issue.severity]
                # rich_escape protects against topic / message strings that
                # contain brackets (e.g. /camera/compressed) being mis-parsed
                # as markup tags
                topic_str = (
                    f" \\[{rich_escape(issue.topic)}]" if issue.topic else ""
                )
                console.print(
                    f"  [{color}]{issue.severity.upper()}[/{color}] "
                    f"\\[{rich_escape(issue.code)}]{topic_str} "
                    f"{rich_escape(issue.message)}"
                )

        # Per-bag issues
        any_bag_issues = any(b.issues for b in report.bags)
        if any_bag_issues:
            console.print("\n[bold]Per-bag findings[/bold]")
            for b in report.bags:
                if not b.issues:
                    continue
                console.print(f"\n  [dim]{Path(b.bag_path).name}[/dim]")
                for issue in b.issues:
                    color = {"error": "red", "warning": "yellow", "info": "cyan"}[issue.severity]
                    topic_str = (
                        f" \\[{rich_escape(issue.topic)}]" if issue.topic else ""
                    )
                    console.print(
                        f"    [{color}]{issue.severity.upper()}[/{color}] "
                        f"\\[{rich_escape(issue.code)}]{topic_str} "
                        f"{rich_escape(issue.message)}"
                    )

        if not report.all_issues:
            console.print("[green]No issues found across the fleet.[/green]")

    if fail_on_error and report.n_errors > 0:
        raise typer.Exit(code=1)


@app.command(name="list")
def list_bags(
    after: Annotated[Optional[str], typer.Option(
        help="Show only bags recorded after this date (YYYY-MM-DD). "
             "e.g. --after 2026-04-01",
    )] = None,
    before: Annotated[Optional[str], typer.Option(
        help="Show only bags recorded before this date (YYYY-MM-DD). "
             "e.g. --before 2026-04-30",
    )] = None,
    has_topic: Annotated[Optional[str], typer.Option("--has-topic",
        help="Show only bags that contain this topic name (exact match). "
             "e.g. --has-topic /imu/data",
    )] = None,
    min_health: Annotated[Optional[int], typer.Option("--min-health",
        help="Show only bags with a health score >= this value (0-100). "
             "Useful for filtering down to clean training data. "
             "e.g. --min-health 80",
    )] = None,
    db: Annotated[Optional[Path], typer.Option("--db",
        help="Path to a non-default index database. Defaults to "
             "~/.resurrector/index.db. e.g. --db /data/myindex.db",
    )] = None,
):
    """List bags in the index, optionally filtered by date, topic, or health score.

    Filters compose with AND — passing --after, --has-topic, and
    --min-health together returns bags matching all three.

    Examples:
      Everything in the index:
          resurrector list

      Recent clean bags with IMU data:
          resurrector list --after 2026-01-01 --has-topic /imu/data --min-health 80
    """
    from resurrector.ingest.indexer import BagIndex
    from resurrector.cli.formatters import print_bag_list

    index = BagIndex(db) if db else BagIndex()
    bags = index.list_bags(
        after=after,
        before=before,
        has_topic=has_topic,
        min_health=min_health,
    )
    print_bag_list(bags)
    index.close()


@app.command()
def export(
    path: Annotated[Path, typer.Argument(
        help="Path to a bag file (.mcap). "
             "e.g. resurrector export experiment.mcap --preset lerobot",
    )],
    topics: Annotated[Optional[list[str]], typer.Option("--topics", "-t",
        help="Topics to export. Pass --topics multiple times for multi-topic "
             "exports. When omitted, every topic is exported (or, with --preset, "
             "the preset's topic filter applies). "
             "e.g. -t /imu/data -t /joint_states",
    )] = None,
    format: Annotated[Optional[str], typer.Option("--format", "-f",
        help="Output format. parquet (default), hdf5, csv, numpy "
             "(capped at 1 M rows per topic), zarr (needs [all-exports]), "
             "lerobot / rlds (training-ready, needs [all-exports]). "
             "Overrides the preset's format if --preset is set. "
             "e.g. -f hdf5",
    )] = None,
    output: Annotated[Path, typer.Option("--output", "-o",
        help="Output directory. Created if missing. e.g. -o ./training_data",
    )] = Path("./export"),
    sync: Annotated[Optional[str], typer.Option("--sync",
        help="Sync method to time-align selected topics: 'nearest', "
             "'interpolate', 'sample_and_hold'. Overrides the preset's "
             "sync_method if --preset is set. e.g. --sync nearest",
    )] = None,
    downsample: Annotated[Optional[float], typer.Option("--downsample",
        help="Resample to this rate in Hz before writing. Overrides the "
             "preset's downsample_hz if --preset is set. e.g. --downsample 50",
    )] = None,
    preset: Annotated[Optional[str], typer.Option("--preset",
        help="Use a named export preset that bundles format + sync + "
             "downsample for common workflows. Available: lerobot, rlds, "
             "training-tabular, camera-only, multimodal. Run "
             "`resurrector export --list-presets` to see details. Any "
             "user-supplied flags override the preset's values. "
             "e.g. --preset lerobot",
    )] = None,
    list_presets_flag: Annotated[bool, typer.Option("--list-presets",
        help="List available export presets and their settings, then exit. "
             "e.g. --list-presets",
    )] = False,
    split: Annotated[Optional[list[str]], typer.Option("--split",
        help="Train/val/test split spec. Pass multiple times: "
             "--split train=0.8 --split val=0.1 --split test=0.1. "
             "Each split writes to its own subdirectory under -o. "
             "Ratios must sum to 1.0. "
             "e.g. --split train=0.8 --split val=0.1 --split test=0.1",
    )] = None,
    split_strategy: Annotated[str, typer.Option("--split-strategy",
        help="How to assign rows to splits when --split is set. "
             "'time' (default) — chronological, best for time-series. "
             "'random' — uniform random per row. 'stratified' is a "
             "v0.6+ candidate (raises NotImplementedError). "
             "e.g. --split-strategy random",
    )] = "time",
):
    """Export bag data to ML-ready formats — Parquet, HDF5, NumPy, Zarr, LeRobot, RLDS.

    All chunk-streaming formats are memory-bounded by chunk size, not topic
    size — open a 100 GB bag without OOMing. NumPy `.npz` is the exception:
    it materializes the full topic and refuses topics over 1 M messages.

    **Presets** (--preset NAME) bundle format/sync/downsample for common
    workflows. User-supplied flags always override preset values, so a
    preset is a baseline, not a constraint.

    Examples:
      Manual config:
          resurrector export bag.mcap -t /imu/data -t /joint_states \\
              --sync nearest --downsample 50 --format hdf5 -o ./training

      One-line LeRobot dataset (synced 30 Hz, all topics):
          resurrector export bag.mcap --preset lerobot -o ./lerobot_data

      Preset with override (LeRobot defaults but at 60 Hz):
          resurrector export bag.mcap --preset lerobot --downsample 60 \\
              -o ./lerobot_60hz

    Format support note: zarr/rlds (and the lerobot/rlds presets) need
    `pip install 'rosbag-resurrector[all-exports]'`.
    """
    from resurrector.core.export import PRESETS

    # --list-presets short-circuits to a table dump and exits
    if list_presets_flag:
        from rich.table import Table
        t = Table(title="Export presets", show_header=True, header_style="bold")
        t.add_column("Name", style="cyan")
        t.add_column("Format")
        t.add_column("Sync")
        t.add_column("Hz")
        t.add_column("Topics")
        t.add_column("Description", style="dim", max_width=44)
        for p in PRESETS.values():
            t.add_row(
                p.name,
                p.format,
                f"{p.sync_method}" if p.sync else "—",
                f"{p.downsample_hz:.0f}" if p.downsample_hz else "native",
                p.topic_filter or "all",
                p.description,
            )
        console.print(t)
        if any(p.extras_required for p in PRESETS.values()):
            console.print(
                "\n[dim]Extras required for some presets — install via "
                "`pip install 'rosbag-resurrector[all-exports]'`.[/dim]"
            )
        raise typer.Exit()

    from resurrector.core.bag_frame import BagFrame

    bf = BagFrame(path)
    # `sync` arg in CLI is the method string (or None); BagFrame.export
    # takes a bool sync + str sync_method separately. Resolve here.
    do_sync = sync is not None if preset is None else None  # let preset decide
    sync_method = sync if sync is not None else None

    # Parse --split entries (each like "train=0.8") into a dict
    split_dict: dict[str, float] | None = None
    if split:
        split_dict = {}
        for item in split:
            if "=" not in item:
                console.print(
                    f"[red]Invalid --split entry {item!r}; expected NAME=RATIO "
                    f"(e.g. train=0.8)[/red]"
                )
                raise typer.Exit(2)
            k, _, v = item.partition("=")
            try:
                split_dict[k.strip()] = float(v.strip())
            except ValueError:
                console.print(f"[red]--split {item!r}: ratio must be numeric[/red]")
                raise typer.Exit(2)

    try:
        result_path = bf.export(
            topics=topics,
            format=format,
            output=str(output),
            sync=do_sync,
            sync_method=sync_method,
            downsample_hz=downsample,
            preset=preset,
            split=split_dict,
            split_strategy=split_strategy,
        )
    except ValueError as e:
        console.print(f"[red]Export failed: {e}[/red]")
        raise typer.Exit(1)
    except NotImplementedError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(1)

    console.print(f"[green]Exported to {result_path}[/green]")
    if preset:
        console.print(f"[dim]Used preset: {preset}[/dim]")
    if split_dict:
        names = ", ".join(split_dict.keys())
        console.print(f"[dim]Split ({split_strategy}): {names}[/dim]")


@app.command()
def publish(
    dataset_dir: Annotated[Path, typer.Argument(
        help="A materialized dataset directory (from `resurrector export` or "
             "`dataset export`). e.g. ./datasets/pick-place/1.0",
    )],
    repo_id: Annotated[str, typer.Option("--repo-id",
        help="Target HuggingFace dataset repo, `owner/name`. "
             "e.g. --repo-id myorg/pick-place-v1",
    )],
    private: Annotated[bool, typer.Option("--private",
        help="Create the HF repo as private. e.g. --private",
    )] = False,
    license_id: Annotated[str, typer.Option("--license",
        help="SPDX license id for the dataset card. e.g. --license mit",
    )] = "apache-2.0",
    qc: Annotated[bool, typer.Option("--qc/--no-qc",
        help="Run bag-side QC on the source bags first and embed a quality "
             "grade in the dataset card. On by default when source bags are "
             "resolvable. e.g. --no-qc to skip",
    )] = True,
    dry_run: Annotated[bool, typer.Option("--dry-run",
        help="Build + write the dataset card locally but skip the upload. "
             "Lets you preview README.md before publishing. e.g. --dry-run",
    )] = False,
    token: Annotated[Optional[str], typer.Option("--token",
        help="HuggingFace token. Falls back to a cached login or HF_TOKEN. "
             "e.g. --token hf_xxx",
    )] = None,
):
    """Publish a dataset directory to the HuggingFace Hub with an auto card.

    Builds a HuggingFace dataset card (README with YAML frontmatter) from the
    directory's manifest + config, optionally embeds a `resurrector qc`
    quality grade, writes it into the directory, and uploads the folder.

    The published page documents the sensor inventory, topic list, and data
    quality so consumers can judge the dataset before training — and credits
    rosbag-resurrector.

    Needs the publish extra:  `pip install rosbag-resurrector[publish]`

    Examples:
      Preview the card without uploading:
          resurrector publish ./datasets/pick-place/1.0 --repo-id me/pick-place --dry-run

      Publish for real:
          resurrector publish ./datasets/pick-place/1.0 --repo-id me/pick-place
    """
    from resurrector.core.publish import publish_dataset

    if not dataset_dir.is_dir():
        console.print(f"[red]Not a directory: {dataset_dir}[/red]")
        raise typer.Exit(1)

    # Optional QC pass over the source bags, if the config records them.
    qc_summary = None
    if qc:
        import json as _json
        cfg_path = dataset_dir / "dataset_config.json"
        bag_paths = []
        if cfg_path.exists():
            try:
                cfg = _json.loads(cfg_path.read_text())
                # bag_refs may be plain path strings (resurrector export) or
                # {path: ...} dicts (dataset export) — handle both.
                raw_refs = cfg.get("bag_refs") or []
                candidates = [
                    r["path"] if isinstance(r, dict) else r
                    for r in raw_refs
                ]
                bag_paths = [c for c in candidates if c and Path(c).exists()]
            except Exception:
                bag_paths = []
        if bag_paths:
            from resurrector.core.qc import run_qc
            report = run_qc(bag_paths)
            qc_summary = report.to_dict()
            console.print(
                f"[dim]QC: {report.n_errors} error(s), "
                f"{report.n_warnings} warning(s) across {report.n_bags} bag(s)[/dim]"
            )
        else:
            console.print("[dim]QC skipped — no resolvable source bags in config.[/dim]")

    try:
        result = publish_dataset(
            dataset_dir, repo_id, token=token, private=private,
            qc_summary=qc_summary, license=license_id, dry_run=dry_run,
        )
    except ImportError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(1)

    if result.dry_run:
        console.print(
            f"[yellow]Dry run[/yellow] — card written to {result.card_path}. "
            f"{result.n_files} file(s) would upload to {result.url}"
        )
    else:
        console.print(f"[green]Published {result.n_files} file(s) → {result.url}[/green]")


@app.command()
def benchmark(
    bag: Annotated[Path, typer.Argument(
        help="Bag to benchmark. e.g. resurrector benchmark run_043.mcap",
    )],
    metrics: Annotated[Path, typer.Option("--metrics",
        help="JSON metric-spec file: a list of {name, direction, tolerance}. "
             "name is a metric id like 'rate:/imu/data' or 'health_score'; "
             "direction is lower_is_better|higher_is_better|stable. "
             "e.g. --metrics metrics.json",
    )],
    baseline: Annotated[Optional[Path], typer.Option("--baseline",
        help="JSON baseline file mapping metric name -> value. Regressions "
             "are measured against this. e.g. --baseline baseline.json",
    )] = None,
    update_baseline: Annotated[bool, typer.Option("--update-baseline",
        help="Write the current metric values to the --baseline path and exit "
             "(captures a new baseline). e.g. --update-baseline",
    )] = False,
    json_output: Annotated[Optional[Path], typer.Option("--json",
        help="Write the benchmark report as JSON. e.g. --json bench.json",
    )] = None,
    fail_on_regression: Annotated[bool, typer.Option("--fail-on-regression",
        help="Exit non-zero if any metric regressed past its tolerance. "
             "The CI gate. e.g. --fail-on-regression",
    )] = False,
):
    """Extract scalar metrics from a bag and gate on regressions vs a baseline.

    CI-for-robots: define metrics (recording rates, health, message counts,
    numeric column aggregates), commit a baseline, and fail a build when a
    change makes the robot measurably worse.

    Metric ids: `duration`, `message_count`, `health_score`,
    `rate:<topic>`, `count:<topic>`, `mean|min|max:<topic>:<column>`.

    Examples:
      Capture a baseline:
          resurrector benchmark good.mcap --metrics metrics.json --baseline baseline.json --update-baseline

      Gate a candidate in CI:
          resurrector benchmark candidate.mcap --metrics metrics.json --baseline baseline.json --fail-on-regression
    """
    from resurrector.core.benchmark import (
        load_specs, run_benchmark, capture_baseline, UnknownMetricError,
    )
    from rich.table import Table

    try:
        specs = load_specs(metrics)
    except (ValueError, OSError) as e:
        console.print(f"[red]Bad metrics spec: {e}[/red]")
        raise typer.Exit(1)

    # Capture-baseline mode: compute current values, write them, exit.
    if update_baseline:
        if baseline is None:
            console.print("[red]--update-baseline requires --baseline <path>[/red]")
            raise typer.Exit(1)
        try:
            values = capture_baseline(bag, specs)
        except UnknownMetricError as e:
            console.print(f"[red]Metric error: {e}[/red]")
            raise typer.Exit(1)
        baseline.write_text(json.dumps(values, indent=2))
        console.print(f"[green]Baseline ({len(values)} metrics) written to {baseline}[/green]")
        return

    baseline_values = None
    if baseline is not None and baseline.exists():
        baseline_values = json.loads(baseline.read_text())

    try:
        report = run_benchmark(bag, specs, baseline_values)
    except UnknownMetricError as e:
        console.print(f"[red]Metric error: {e}[/red]")
        raise typer.Exit(1)

    if json_output:
        json_output.write_text(report.to_json())
        console.print(f"[green]Benchmark report saved to {json_output}[/green]")
    else:
        table = Table(title=f"Benchmark: {bag.name}")
        table.add_column("Metric")
        table.add_column("Current", justify="right")
        table.add_column("Baseline", justify="right")
        table.add_column("Δ%", justify="right")
        table.add_column("Status")
        for c in report.checks:
            base_str = "—" if c.baseline is None else f"{c.baseline:.3g}"
            pct_str = "—" if c.delta_pct is None else f"{c.delta_pct * 100:+.1f}%"
            status = (
                "[red]REGRESSED[/red]" if c.regressed
                else ("[dim]baseline set[/dim]" if c.baseline is None else "[green]ok[/green]")
            )
            table.add_row(c.name, f"{c.current:.3g}", base_str, pct_str, status)
        console.print(table)
        if report.regressed:
            console.print(f"\n[red]{report.n_regressions} metric(s) regressed.[/red]")
        else:
            console.print("\n[green]No regressions.[/green]")

    if fail_on_regression and report.regressed:
        raise typer.Exit(code=1)


@app.command()
def diff(
    bag1: Annotated[Path, typer.Argument(
        help="First bag (the baseline / 'before'). e.g. baseline.mcap",
    )],
    bag2: Annotated[Path, typer.Argument(
        help="Second bag (the comparison / 'after'). e.g. experiment.mcap",
    )],
    json_output: Annotated[Optional[Path], typer.Option("--json",
        help="Write the structured diff as JSON to this path (stable schema, "
             "CI-friendly): topics added/removed, type/schema/rate/count "
             "changes, duration + message deltas. e.g. --json diff.json",
    )] = None,
    fail_on_change: Annotated[bool, typer.Option("--fail-on-change",
        help="Exit non-zero if any structural change is detected (topic "
             "added/removed, type/schema/rate change). A CI gate that a "
             "recording's shape stays stable. e.g. --fail-on-change",
    )] = False,
):
    """Compare topic lists, message counts, and durations across two bags.

    Useful for "did this run record what the previous run did?" sanity
    checks and for diagnosing setup drift between recordings (a topic
    silently dropped, a frequency change, a duration mismatch).

    With **--json** you get a machine-readable structured diff (the same
    shape the dashboard's /api/diff endpoint returns); with
    **--fail-on-change** the command becomes a CI gate.

    For visual / numeric trace overlays, use the dashboard's Compare or
    Cross-bag Overlay pages instead.

    Examples:
      Human-readable table:
          resurrector diff baseline.mcap experiment.mcap

      CI gate that the recording shape stays stable:
          resurrector diff baseline.mcap candidate.mcap --fail-on-change
    """
    from resurrector.core.bag_diff import diff_bags

    # --json / --fail-on-change use the structured engine; the default
    # human view keeps the existing rich-table formatter unchanged.
    if json_output is not None or fail_on_change:
        result = diff_bags(bag1, bag2)
        if json_output is not None:
            json_output.write_text(result.to_json())
            console.print(f"[green]Diff saved to {json_output}[/green]")
        else:
            from resurrector.cli.formatters import print_diff
            from resurrector.ingest.parser import parse_bag
            print_diff(parse_bag(bag1).get_metadata(), parse_bag(bag2).get_metadata())
        if fail_on_change and result.has_changes:
            raise typer.Exit(code=1)
        return

    from resurrector.ingest.parser import parse_bag
    from resurrector.cli.formatters import print_diff

    parser1 = parse_bag(bag1)
    parser2 = parse_bag(bag2)
    print_diff(parser1.get_metadata(), parser2.get_metadata())


@app.command()
def report(
    bag: Annotated[Path, typer.Argument(
        help="Bag file to report on. e.g. experiment.mcap",
    )],
    start: Annotated[float, typer.Option("--start", "-s",
        help="Window start, seconds from bag start. e.g. --start 12.5")] = 0.0,
    end: Annotated[Optional[float], typer.Option("--end", "-e",
        help="Window end, seconds. Defaults to bag end. e.g. --end 18.0")] = None,
    output: Annotated[Optional[Path], typer.Option("--output", "-o",
        help="Write the report here. Extension picks the format (.html / .md). "
             "e.g. -o incident.html")] = None,
    fmt: Annotated[str, typer.Option("--format",
        help="Output format if --output has no extension. html | md")] = "html",
    baseline: Annotated[Optional[Path], typer.Option("--baseline",
        help="A known-good bag to diff against. Adds a 'diff vs baseline' "
             "section. e.g. --baseline good_run.mcap")] = None,
    use_llm: Annotated[bool, typer.Option("--llm",
        help="Use the AI copilot for the narrative (needs [copilot] + "
             "ANTHROPIC_API_KEY). Falls back to rule-based otherwise.")] = False,
):
    """Generate a shareable incident report for a time window.

    Brush a bad window in the dashboard (or pick one from `qc --anomalies`),
    then produce a single self-contained HTML (or Markdown) report: the
    grounded evidence table, health findings, a per-topic activity chart, an
    optional camera thumbnail, an optional diff vs a known-good baseline, and
    a probable-cause verdict (known / likely / unknown).

    The HTML is self-contained (inline CSS/SVG/base64) — drop it in a Slack
    thread, a GitHub issue, or an email and it just works.

    Examples:
      Whole-bag HTML report:
          resurrector report run.mcap -o incident.html

      A specific bad window, with a baseline diff:
          resurrector report run.mcap -s 12 -e 18 --baseline good.mcap -o incident.html

      Markdown for a GitHub issue:
          resurrector report run.mcap -s 12 -e 18 -o incident.md
    """
    from resurrector.core.report import generate_incident_report
    from resurrector.ingest.parser import parse_bag

    end_sec = end if end is not None else parse_bag(bag).get_metadata().duration_sec

    # Resolve format from the output extension when present.
    resolved_fmt = fmt
    if output is not None:
        ext = output.suffix.lower()
        if ext in (".html", ".htm"):
            resolved_fmt = "html"
        elif ext in (".md", ".markdown"):
            resolved_fmt = "md"

    out_path = output
    if out_path is None:
        out_path = Path(f"{bag.stem}_incident.{ 'md' if resolved_fmt == 'md' else 'html'}")

    result = generate_incident_report(
        bag, start, end_sec, output_path=out_path, fmt=resolved_fmt,
        baseline_bag=baseline, use_llm=use_llm,
    )
    cause_color = {"known": "red", "likely": "yellow", "unknown": "dim"}[result.probable_cause]
    console.print(
        f"[green]Report written to {out_path}[/green]  "
        f"probable cause: [{cause_color}]{result.probable_cause}[/{cause_color}]"
    )


# Bag Contracts — versioned 'what a good run looks like' spec, enforced in CI.
contract_app = typer.Typer(help="Author + enforce bag contracts (CI-friendly).")
app.add_typer(contract_app, name="contract")


@contract_app.command("init")
def contract_init(
    bags: Annotated[list[Path], typer.Argument(
        help="One or more known-good bags (or directories). The contract is "
             "inferred from these. e.g. resurrector contract init good_*.mcap",
    )],
    output: Annotated[Path, typer.Option("--output", "-o",
        help="Where to write the contract. .yaml (needs pyyaml) or .json. "
             "e.g. -o contract.yaml")] = Path("contract.yaml"),
    tolerance: Annotated[float, typer.Option("--rate-tolerance",
        help="Widen inferred rate ranges by this fraction (0.2 = ±20%).")] = 0.2,
):
    """Infer a contract from known-good bags.

    A topic is required if it appears in every input bag; its rate range is
    the observed [min, max] widened by --rate-tolerance. Required TF frames
    are those present in every bag. Edit the output by hand, commit it, and
    enforce with `resurrector contract check` in CI.
    """
    from resurrector.core.contract import infer_contract, save_contract

    resolved: list[Path] = []
    for b in bags:
        resolved.extend(sorted(b.rglob("*.mcap")) if b.is_dir() else [b])
    if not resolved:
        console.print("[red]No bags found.[/red]")
        raise typer.Exit(code=1)

    c = infer_contract(resolved, rate_tolerance=tolerance)
    try:
        save_contract(c, output)
    except RuntimeError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=1)
    console.print(
        f"[green]Contract written to {output}[/green] — "
        f"{len(c.topics)} topic(s), {len(c.tf_frames)} required TF frame(s), "
        f"inferred from {len(resolved)} bag(s)."
    )


@contract_app.command("check")
def contract_check(
    bag: Annotated[Path, typer.Argument(help="Bag to validate. e.g. candidate.mcap")],
    contract_path: Annotated[Path, typer.Option("--contract", "-c",
        help="Contract file (.yaml / .json). e.g. -c contract.yaml")] = Path("contract.yaml"),
    json_output: Annotated[Optional[Path], typer.Option("--json",
        help="Write the result as JSON. e.g. --json result.json")] = None,
    fail_on_violation: Annotated[bool, typer.Option("--fail-on-violation",
        help="Exit non-zero if any violation is found. The CI gate.")] = False,
):
    """Validate a bag against a contract.

    Reports every violation (missing topic, wrong type, rate out of range,
    missing TF frame). With --fail-on-violation it's a CI gate.

    Example:
        resurrector contract check candidate.mcap -c contract.yaml --fail-on-violation
    """
    from resurrector.core.contract import load_contract, check_contract
    from rich.markup import escape as rich_escape

    if not contract_path.exists():
        console.print(f"[red]Contract not found: {contract_path}[/red]")
        raise typer.Exit(code=1)

    contract = load_contract(contract_path)
    result = check_contract(bag, contract)

    if json_output is not None:
        json_output.write_text(result.to_json())
        console.print(f"[green]Result saved to {json_output}[/green]")
    elif result.passed:
        console.print(f"[green]PASS[/green] {bag.name} satisfies {contract_path.name}")
    else:
        console.print(
            f"[red]FAIL[/red] {bag.name}: {len(result.violations)} violation(s)"
        )
        for v in result.violations:
            topic_str = f" \\[{rich_escape(v.topic)}]" if v.topic else ""
            console.print(
                f"  [red]{rich_escape(v.code)}[/red]{topic_str} "
                f"{rich_escape(v.message)}"
            )

    if fail_on_violation and not result.passed:
        raise typer.Exit(code=1)


@app.command()
def tag(
    path: Annotated[Path, typer.Argument(
        help="Path to an indexed bag file. "
             "e.g. resurrector tag experiment.mcap --add task:pick_and_place",
    )],
    add: Annotated[Optional[list[str]], typer.Option("--add",
        help="Tag to add, in `key:value` form. Pass --add multiple times to "
             "add several tags at once. Tags are stored in the index and can "
             "be filtered on later (dashboard Library page, custom queries). "
             "e.g. --add task:pick_and_place --add robot:digit",
    )] = None,
    remove: Annotated[Optional[list[str]], typer.Option("--remove",
        help="Tag to remove. Use `key` alone to remove every value for that "
             "key, or `key:value` to remove a specific entry. Pass --remove "
             "multiple times to remove several. "
             "e.g. --remove task  or  --remove task:pick_and_place",
    )] = None,
    db: Annotated[Optional[Path], typer.Option("--db",
        help="Path to a non-default index database. Defaults to "
             "~/.resurrector/index.db. e.g. --db /data/myindex.db",
    )] = None,
):
    """Add or remove tags on an indexed bag for later filtering / organization.

    Tags are simple key:value pairs (free-form strings). After tagging,
    tagged bags surface in `resurrector list --has-topic` filters and the
    dashboard's Library page filter chips. Without --add or --remove,
    just shows the bag's current tags.

    Preconditions:
      The bag must already be in the index (`resurrector scan ...` first).

    Examples:
      resurrector tag bag.mcap --add task:pick_and_place --add robot:digit
      resurrector tag bag.mcap --remove task          # remove all task tags
      resurrector tag bag.mcap                        # show current tags
    """
    from resurrector.ingest.indexer import BagIndex

    index = BagIndex(db) if db else BagIndex()
    bag = index.get_bag_by_path(path.resolve())
    if bag is None:
        console.print(f"[red]Bag not found in index. Run 'resurrector scan' first.[/red]")
        index.close()
        raise typer.Exit(1)

    bag_id = bag["id"]

    if add:
        for tag_str in add:
            key, _, value = tag_str.partition(":")
            index.add_tag(bag_id, key, value)
            console.print(f"[green]Added tag: {tag_str}[/green]")

    if remove:
        for tag_str in remove:
            key, _, value = tag_str.partition(":")
            index.remove_tag(bag_id, key, value if value else None)
            console.print(f"[yellow]Removed tag: {tag_str}[/yellow]")

    # Show current tags
    bag = index.get_bag(bag_id)
    tags = bag.get("tags", [])
    if tags:
        # Pre-format outside the f-string so older Pythons (PEP 701 < 3.12)
        # don't choke on nested same-style quotes.
        tag_pairs = ", ".join("{}:{}".format(t["key"], t["value"]) for t in tags)
        console.print(f"Current tags: {tag_pairs}")
    else:
        console.print("[dim]No tags.[/dim]")

    index.close()


@app.command()
def quicklook(
    path: Annotated[Path, typer.Argument(
        help="Path to a bag file (.mcap, .bag, or .db3). "
             "e.g. resurrector quicklook experiment.mcap",
    )],
):
    """At-a-glance bag summary in the terminal: health, topics, sparklines, anomalies.

    Mid-weight inspection: heavier than `info`, lighter than `health`.
    Renders a single-screen overview with the bag's health badge,
    topic-grouped table with relative-message-count sparklines, and a
    short bullet list of any data-quality issues. Best for triaging a
    folder of bags one at a time.

    Example:
      resurrector quicklook experiment.mcap
    """
    from resurrector.core.bag_frame import BagFrame
    from resurrector.core.topic_groups import classify_topics
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text
    from resurrector.cli.formatters import health_badge, format_size

    bf = BagFrame(path)
    meta = bf.metadata
    health = bf.health_report()
    groups = classify_topics(bf.topic_names)

    # Header panel
    badge = health_badge(health.score)
    console.print()
    header = Text()
    header.append(f"  {meta.path.name}\n", style="bold")
    header.append(f"  Health: ", style="dim")
    header.append_text(badge)
    header.append(
        f"  Duration: {meta.duration_sec:.1f}s  "
        f"Size: {format_size(path.stat().st_size)}  "
        f"Topics: {len(meta.topics)}  "
        f"Messages: {meta.message_count:,}",
        style="dim",
    )
    console.print(Panel(header, title="[bold blue]quicklook[/bold blue]", border_style="blue"))

    # Topic table grouped
    table = Table(show_header=True, header_style="bold", show_lines=False, pad_edge=False)
    table.add_column("Group", style="dim", width=14)
    table.add_column("Topic", style="cyan", min_width=25)
    table.add_column("Hz", justify="right", width=8)
    table.add_column("Count", justify="right", width=10)
    table.add_column("Rate", width=20)  # sparkline
    table.add_column("Health", justify="center", width=8)

    for group in groups:
        for i, topic_name in enumerate(group.topics):
            topic = bf._find_topic(topic_name)
            freq = f"{topic.frequency_hz:.0f}" if topic.frequency_hz else "?"
            th = health.topic_scores.get(topic_name)
            if th:
                if th.score >= 90:
                    h = "[green]OK[/green]"
                elif th.score >= 70:
                    h = f"[yellow]{th.score}[/yellow]"
                else:
                    h = f"[red]{th.score}[/red]"
            else:
                h = "[dim]?[/dim]"

            # Simple sparkline based on message count relative to max
            max_count = max(t.message_count for t in meta.topics) if meta.topics else 1
            bar_len = int(15 * topic.message_count / max_count)
            bar = "█" * bar_len + "░" * (15 - bar_len)

            g_label = group.name if i == 0 else ""
            table.add_row(g_label, topic_name, freq, f"{topic.message_count:,}", f"[cyan]{bar}[/cyan]", h)

    console.print(table)

    # Highlight anomalies
    if health.issues:
        console.print()
        n_errors = len([i for i in health.issues if i.severity.value in ("error", "critical")])
        n_warns = len([i for i in health.issues if i.severity.value == "warning"])
        summary = []
        if n_errors:
            summary.append(f"[red]{n_errors} error(s)[/red]")
        if n_warns:
            summary.append(f"[yellow]{n_warns} warning(s)[/yellow]")
        console.print(f"  Issues: {', '.join(summary)}")
        for rec in health.recommendations[:3]:
            console.print(f"  [dim]→ {rec}[/dim]")

    console.print()


@app.command()
def watch(
    path: Annotated[Path, typer.Argument(
        help="Directory to monitor. Subdirectories are NOT watched recursively. "
             "e.g. resurrector watch ~/recordings",
    )],
    db: Annotated[Optional[Path], typer.Option("--db",
        help="Path to a non-default index database. Defaults to "
             "~/.resurrector/index.db. e.g. --db /data/myindex.db",
    )] = None,
    interval: Annotated[float, typer.Option("--interval", "-i",
        help="How often (in seconds) to poll for new files. Default 5.0. "
             "Lower for more responsive detection at the cost of more "
             "filesystem reads. e.g. --interval 2",
    )] = 5.0,
):
    """Watch a folder for newly-recorded bags and index each one as it appears.

    Long-running command: starts a poll loop, indexes every existing bag
    once, then watches for new files. When a new bag is detected, scans
    metadata, computes a health report, and writes both to the index.
    Press Ctrl+C to stop.

    Preconditions:
      `pip install 'rosbag-resurrector[watch]'` (or just rely on the
      built-in poll loop — the [watch] extra adds inotify-based detection
      via watchdog for instant pickup).

    Example:
      resurrector watch ~/recordings --interval 2
    """
    import time
    from resurrector.ingest.scanner import scan_path, BAG_EXTENSIONS
    from resurrector.ingest.parser import parse_bag
    from resurrector.ingest.indexer import BagIndex
    from resurrector.core.bag_frame import BagFrame
    from resurrector.cli.formatters import health_badge

    if not path.is_dir():
        console.print(f"[red]Not a directory: {path}[/red]")
        raise typer.Exit(1)

    index = BagIndex(db) if db else BagIndex()
    seen: set[str] = set()

    # Index existing files first
    existing = scan_path(path)
    for f in existing:
        seen.add(str(f.path))

    console.print(f"[bold]Watching[/bold] {path} (poll every {interval}s, {len(seen)} existing files)")
    console.print("[dim]Press Ctrl+C to stop[/dim]\n")

    try:
        while True:
            time.sleep(interval)
            current = scan_path(path)
            for scanned in current:
                key = str(scanned.path)
                if key not in seen:
                    seen.add(key)
                    console.print(f"[cyan]New bag detected:[/cyan] {scanned.path.name}")
                    try:
                        parser = parse_bag(scanned.path)
                        metadata = parser.get_metadata()
                        bag_id = index.upsert_bag(scanned, metadata)

                        bf = BagFrame(scanned.path)
                        report = bf.health_report()
                        index.update_health_score(bag_id, report.score)

                        badge = health_badge(report.score)
                        console.print(f"  Indexed: {len(metadata.topics)} topics, health: ", end="")
                        console.print(badge)
                    except Exception as e:
                        console.print(f"  [red]Error: {e}[/red]")
    except KeyboardInterrupt:
        console.print("\n[dim]Stopped watching.[/dim]")
    finally:
        index.close()


# --- Dataset commands ---

dataset_app = typer.Typer(help="Manage reproducible datasets.")
app.add_typer(dataset_app, name="dataset")


@dataset_app.command("create")
def dataset_create(
    name: Annotated[str, typer.Argument(
        help="Unique dataset name. e.g. pick-place-experiments",
    )],
    description: Annotated[str, typer.Option("--desc", "-d",
        help="Free-text description, stored with the dataset. Shows up in "
             "`dataset list` and the auto-generated README on export. "
             "e.g. --desc \"Pick-and-place runs across April\"",
    )] = "",
    db: Annotated[Optional[Path], typer.Option("--db",
        help="Path to a non-default index database. e.g. --db /data/myindex.db",
    )] = None,
):
    """Create a new versioned dataset (an empty container — add versions next).

    A "dataset" here is a named collection of bag references plus
    sync/export config. Versions are added separately via `dataset
    add-version`. On export, the toolchain writes a SHA256 manifest +
    README.md alongside the data so the dataset is reproducible.

    Example:
      resurrector dataset create pick-place-experiments \\
          --desc "Pick-and-place runs across April"
    """
    from resurrector.core.dataset import DatasetManager
    mgr = DatasetManager(db)
    did = mgr.create(name, description)
    console.print(f"[green]Created dataset '{name}' (id={did})[/green]")
    mgr.close()


@dataset_app.command("add-version")
def dataset_add_version(
    name: Annotated[str, typer.Argument(
        help="Dataset name (must already exist via `dataset create`). "
             "e.g. pick-place-experiments",
    )],
    version: Annotated[str, typer.Argument(
        help="Version string for this configuration. Free-form. "
             "e.g. 1.0  or  2026-04-28",
    )],
    bags: Annotated[list[Path], typer.Option("--bag", "-b",
        help="Bag file path. Pass --bag multiple times for multi-bag datasets. "
             "e.g. -b session_001.mcap -b session_002.mcap",
    )],
    topics: Annotated[Optional[list[str]], typer.Option("--topic", "-t",
        help="Topic to include. Pass --topic multiple times for multi-topic. "
             "When omitted, every non-image topic is included. "
             "e.g. -t /imu/data -t /joint_states",
    )] = None,
    format: Annotated[str, typer.Option("--format", "-f",
        help="Export format applied at materialization time. parquet (default), "
             "hdf5, csv, lerobot, rlds, etc. e.g. -f hdf5",
    )] = "parquet",
    sync_method: Annotated[Optional[str], typer.Option("--sync",
        help="Optional time-alignment method for the included topics: "
             "nearest, interpolate, sample_and_hold. e.g. --sync nearest",
    )] = None,
    downsample: Annotated[Optional[float], typer.Option("--downsample",
        help="Resample to this Hz before export. e.g. --downsample 50",
    )] = None,
    db: Annotated[Optional[Path], typer.Option("--db",
        help="Path to a non-default index database. e.g. --db /data/myindex.db",
    )] = None,
):
    """Pin a set of bags + sync/export config to a named version of a dataset.

    The dataset itself is just metadata; this command attaches concrete
    bags. Re-export the same version later and you get the same data.

    Example:
      resurrector dataset add-version pick-place-experiments 1.0 \\
          -b session_001.mcap -b session_002.mcap \\
          -t /imu/data -t /joint_states \\
          --sync nearest --downsample 50 --format parquet
    """
    from resurrector.core.dataset import DatasetManager, BagRef, SyncConfig
    mgr = DatasetManager(db)
    bag_refs = [BagRef(path=str(b.resolve())) for b in bags]
    sync_cfg = SyncConfig(method=sync_method) if sync_method else None
    vid = mgr.create_version(
        dataset_name=name,
        version=version,
        bag_refs=bag_refs,
        topics=topics,
        sync_config=sync_cfg,
        export_format=format,
        downsample_hz=downsample,
    )
    console.print(f"[green]Added version '{version}' (id={vid}) to dataset '{name}'[/green]")
    mgr.close()


@dataset_app.command("export")
def dataset_export(
    name: Annotated[str, typer.Argument(
        help="Dataset name. e.g. pick-place-experiments",
    )],
    version: Annotated[str, typer.Argument(
        help="Version to export (must exist via `dataset add-version`). e.g. 1.0",
    )],
    output: Annotated[Path, typer.Option("--output", "-o",
        help="Output directory; the version writes into <output>/<name>/<version>/. "
             "e.g. -o ./datasets",
    )] = Path("./datasets"),
    db: Annotated[Optional[Path], typer.Option("--db",
        help="Path to a non-default index database. e.g. --db /data/myindex.db",
    )] = None,
):
    """Materialize a dataset version to disk with auto-generated README + SHA256 manifest.

    Reads each bag listed in the version, applies the version's sync /
    downsample / format settings, and writes the result to --output. A
    `manifest.json` records SHA256 hashes of every file written so the
    export is verifiable.

    Example:
      resurrector dataset export pick-place-experiments 1.0 -o ./datasets
    """
    from resurrector.core.dataset import DatasetManager
    mgr = DatasetManager(db)
    result = mgr.export_version(name, version, str(output))
    console.print(f"[green]Exported to {result}[/green]")
    mgr.close()


@dataset_app.command("list")
def dataset_list(
    db: Annotated[Optional[Path], typer.Option("--db",
        help="Path to a non-default index database. e.g. --db /data/myindex.db",
    )] = None,
):
    """Show every dataset in the index, with name, description, and version count.

    Example:
      resurrector dataset list
    """
    from resurrector.core.dataset import DatasetManager
    from rich.table import Table

    mgr = DatasetManager(db)
    datasets = mgr.list_datasets()
    if not datasets:
        console.print("[dim]No datasets found.[/dim]")
        mgr.close()
        return

    table = Table(title="Datasets", show_header=True, header_style="bold")
    table.add_column("Name", style="cyan")
    table.add_column("Description")
    table.add_column("Versions", justify="right")
    table.add_column("Updated", style="dim")

    for ds in datasets:
        table.add_row(
            ds["name"],
            ds.get("description", ""),
            str(len(ds.get("versions", []))),
            str(ds.get("updated_at", "")),
        )

    console.print(table)
    mgr.close()


@app.command(name="export-frames")
def export_frames_cmd(
    path: Annotated[Path, typer.Argument(
        help="Path to a bag file (.mcap). "
             "e.g. resurrector export-frames experiment.mcap -t /camera/rgb",
    )],
    topic: Annotated[str, typer.Option("--topic", "-t",
        help="Image topic to extract. Required. "
             "e.g. -t /camera/rgb/image_raw",
    )],
    output: Annotated[Path, typer.Option("--output", "-o",
        help="Where to write. With default behavior, treated as a directory "
             "(created if missing) with one image file per frame. With "
             "--video, treated as the path to an MP4 file. "
             "e.g. -o ./frames  or  -o preview.mp4",
    )] = Path("./frames"),
    format: Annotated[str, typer.Option("--format", "-f",
        help="Image format: 'png' (lossless, larger) or 'jpeg' (smaller, "
             "lossy). Default png. Ignored when --video is set. "
             "e.g. -f jpeg",
    )] = "png",
    video: Annotated[bool, typer.Option("--video",
        help="Encode the frame sequence to a single MP4 video instead of "
             "writing individual images. Requires opencv (in the [vision-lite] "
             "extra). e.g. --video -o preview.mp4",
    )] = False,
    fps: Annotated[Optional[float], typer.Option("--fps",
        help="Frames per second for the output video. When omitted, uses "
             "the topic's recorded frequency from the bag metadata. Only "
             "meaningful with --video. e.g. --fps 30",
    )] = None,
    max_frames: Annotated[Optional[int], typer.Option("--max-frames",
        help="Stop after this many frames. Useful for spot-checking a long "
             "bag without writing every frame. Default: no limit. "
             "e.g. --max-frames 1000",
    )] = None,
    every_n: Annotated[int, typer.Option("--every-n",
        help="Sample every Nth frame. Default 1 (every frame). Set to 5 to "
             "thin a 30 Hz camera to 6 Hz output. Combine with --max-frames "
             "to bound both the rate and the count. e.g. --every-n 5",
    )] = 1,
):
    """Extract images from a single image topic to a folder or MP4 video.

    Differs from `search-frames`: this command writes ALL frames (or a
    rate-limited subset) of one topic; `search-frames` writes only frames
    matching a query. Use this to give an external tool (a labeler, a
    custom training pipeline) raw frame data.

    Modes:
      - Default: writes individual PNG/JPEG files into --output directory.
        Filenames include the frame index and timestamp for stable ordering.
      - --video: encodes the same frame stream as a single MP4 at --output.
        Useful for quick visual review of a long camera recording.

    Examples:
      Dump every frame of /camera/rgb to ./frames as PNGs:
          resurrector export-frames bag.mcap -t /camera/rgb -o ./frames

      Make a 10 Hz preview video, capped at 5 minutes worth of frames:
          resurrector export-frames bag.mcap -t /camera/rgb \
              --video -o preview.mp4 --fps 10 --max-frames 3000

      Thin a 30 Hz topic to 6 Hz JPEGs:
          resurrector export-frames bag.mcap -t /camera/rgb \
              -f jpeg --every-n 5 -o ./frames_thin
    """
    from resurrector.core.bag_frame import BagFrame
    from resurrector.core.export import Exporter

    bf = BagFrame(path)
    view = bf[topic]
    if not view.is_image_topic:
        console.print(f"[red]Topic '{topic}' is not an image topic.[/red]")
        raise typer.Exit(1)

    exporter = Exporter()
    if video:
        result = exporter.export_video(view, output, fps=fps)
        console.print(f"[green]Video exported to {result}[/green]")
    else:
        result = exporter.export_frames(
            view, output, format=format, max_frames=max_frames, every_n=every_n,
        )
        console.print(f"[green]Frames exported to {result}[/green]")


@app.command(name="index-frames")
def index_frames_cmd(
    path: Annotated[Path, typer.Argument(
        help="Bag file (.mcap) or a directory of bags. Directories are "
             "indexed recursively; bags already in the index are processed "
             "in place — pass --force to re-index them. "
             "e.g. resurrector index-frames experiment.mcap"
    )],
    topic: Annotated[Optional[str], typer.Option("--topic", "-t",
        help="Image topic to index. When omitted, every image topic in the "
             "bag is auto-detected and indexed. "
             "e.g. -t /camera/rgb/image_raw",
    )] = None,
    sample_hz: Annotated[float, typer.Option("--sample-hz",
        help="Frames per second to sample for embedding. Default 5.0 — a "
             "30 Hz camera at 5 Hz means 1 in 6 frames is embedded. Lower "
             "sample rate is faster + smaller index but coarser search "
             "resolution. Raise for finer scrubbing in matched clips. "
             "e.g. --sample-hz 10",
    )] = 5.0,
    batch_size: Annotated[int, typer.Option("--batch-size",
        help="How many frames to feed the CLIP model per forward pass. "
             "Default 32 — increase on a GPU for throughput, decrease if "
             "running out of memory on CPU-only machines. "
             "e.g. --batch-size 64",
    )] = 32,
    force: Annotated[bool, typer.Option("--force",
        help="Re-index from scratch even if embeddings already exist for "
             "the (bag, topic) pair. Without this, a second `index-frames` "
             "run on the same bag is a no-op. e.g. --force",
    )] = False,
    db: Annotated[Optional[Path], typer.Option("--db",
        help="Path to a non-default index database. Defaults to "
             "~/.resurrector/index.db. Use the same value here, in `scan`, "
             "and in `search-frames`. e.g. --db /data/myindex.db",
    )] = None,
):
    """Compute CLIP embeddings for image frames so `search-frames` can find them.

    For each frame sampled at --sample-hz, runs the CLIP image encoder and
    stores the resulting 512-d vector in the index database. This is a
    one-time cost per bag: typical timing is roughly 1-2 minutes per
    minute of video on CPU, an order of magnitude faster on a GPU.

    Preconditions:
      1. `pip install 'rosbag-resurrector[vision]'` (local CLIP model;
         downloads ~600 MB on first use, cached at ~/.cache/huggingface)
         OR `pip install 'rosbag-resurrector[vision-openai]'` for the
         OpenAI-backed embedding backend.
      2. `resurrector scan <dir>` so the bag exists in the index.

    The index stores one row per sampled frame: bag_id, topic, timestamp,
    frame_index, and the embedding. Search uses DuckDB's
    list_cosine_similarity at query time.

    Examples:
      Index every image topic in a single bag:
          resurrector index-frames experiment.mcap

      Index just the front camera at high resolution:
          resurrector index-frames experiment.mcap -t /camera/front --sample-hz 10

      Re-index after the bag was edited or after upgrading the CLIP backend:
          resurrector index-frames experiment.mcap --force
    """
    from resurrector.ingest.scanner import scan_path
    from resurrector.ingest.indexer import BagIndex
    from resurrector.core.vision import FrameSearchEngine
    from resurrector.cli.formatters import create_progress

    # Collect bag files
    if path.is_dir():
        files = scan_path(path)
        bag_paths = [f.path for f in files]
    else:
        bag_paths = [path]

    if not bag_paths:
        console.print("[yellow]No bag files found.[/yellow]")
        raise typer.Exit()

    index = BagIndex(db) if db else BagIndex()
    engine = FrameSearchEngine(index)
    total_frames = 0

    with create_progress() as progress:
        task = progress.add_task("Indexing frames...", total=len(bag_paths))
        for bag_path in bag_paths:
            bag = index.get_bag_by_path(bag_path)
            if bag is None:
                console.print(f"[yellow]Bag not indexed: {bag_path.name}. Run 'resurrector scan' first.[/yellow]")
                progress.advance(task)
                continue
            try:
                n = engine.index_bag(
                    bag_id=bag["id"], bag_path=bag_path,
                    topic=topic, sample_hz=sample_hz,
                    batch_size=batch_size, force=force,
                )
                total_frames += n
            except Exception as e:
                console.print(f"[red]Error: {bag_path.name}: {e}[/red]")
            progress.advance(task)

    console.print(f"[green]Indexed {total_frames} frames from {len(bag_paths)} bag(s).[/green]")
    index.close()


@app.command(name="search-frames")
def search_frames_cmd(
    query: Annotated[str, typer.Argument(
        help="Natural-language description of what to find. Plain English; "
             "no special syntax. e.g. resurrector search-frames "
             "\"robot dropping object\""
    )],
    top_k: Annotated[int, typer.Option("--top-k", "-k",
        help="Maximum number of matching frames (or clips, with --clips) to "
             "return. Default 20. Increase for broader recall; lower to focus "
             "on the highest-similarity matches. e.g. --top-k 50",
    )] = 20,
    clips: Annotated[bool, typer.Option("--clips",
        help="Group consecutive matching frames into temporal clips instead "
             "of returning isolated frames. Useful when a query matches a "
             "continuous scene — you get one entry per scene with start/end "
             "time and frame count. See also --clip-duration. e.g. --clips",
    )] = False,
    clip_duration: Annotated[float, typer.Option("--clip-duration",
        help="When --clips is on, frames within this many seconds of each "
             "other are merged into the same clip. Default 5.0 s. "
             "e.g. --clip-duration 3.0",
    )] = 5.0,
    min_similarity: Annotated[float, typer.Option("--min-sim",
        help="Minimum cosine similarity (0.0–1.0) for a frame to be returned. "
             "0.15 is a permissive default that surfaces dim/partial matches; "
             "raise to ~0.25 for stricter results. e.g. --min-sim 0.30",
    )] = 0.15,
    save: Annotated[Optional[Path], typer.Option("--save",
        help="Directory to save matched frames as image files (and a "
             "`results.json` with per-match metadata: rank, similarity, "
             "timestamp, bag, topic). With --clips, saves short clips per "
             "match instead. Useful for visually validating the search. "
             "The directory is created if it does not exist. "
             "e.g. --save ./search_results",
    )] = None,
    db: Annotated[Optional[Path], typer.Option("--db",
        help="Path to a non-default index database. Defaults to "
             "~/.resurrector/index.db. Match the value used by `scan` and "
             "`index-frames`. e.g. --db /data/myindex.db",
    )] = None,
):
    """Find image frames in indexed bags using natural-language queries.

    Backed by CLIP embeddings stored in the DuckDB index. The query is
    embedded once, then compared against every frame embedding via cosine
    similarity. Results are ranked by similarity and printed as a table.

    Preconditions:
      1. `pip install 'rosbag-resurrector[vision]'` to install the local
         CLIP model dependency (or [vision-openai] for the OpenAI backend).
      2. `resurrector scan <dir>` to index the bag(s).
      3. `resurrector index-frames <bag>` to compute the per-frame
         embeddings — this is the slow step (~5 min for a 1 GB bag at the
         default sample rate). One-time cost per bag.

    Output modes:
      - Default (frame mode): a table with one row per matching frame
        showing rank, similarity, bag, topic, timestamp, and frame index.
      - --clips: rows are temporal clips (consecutive matching frames
        merged) showing time range and frame count. Better for "find a
        scene" queries than "find a still".
      - --save DIR: writes the actual frame images (or clip videos with
        --clips) to DIR, plus a results.json with full metadata. Open the
        directory in Finder/Explorer to validate matches visually.

    Examples:
      Inspect matches visually, then pick the most useful query:
          resurrector search-frames "person walking" --top-k 10 --save ./hits

      Find a continuous scene rather than individual stills:
          resurrector search-frames "robot turning left" --clips --save ./scenes

      Restrict to high-confidence matches:
          resurrector search-frames "collision" --min-sim 0.30
    """
    from rich.table import Table
    from resurrector.ingest.indexer import BagIndex
    from resurrector.core.vision import FrameSearchEngine, save_search_results

    index = BagIndex(db) if db else BagIndex()
    engine = FrameSearchEngine(index)

    if clips:
        results = engine.search_temporal(
            query, clip_duration_sec=clip_duration,
            top_k=top_k, min_similarity=min_similarity,
        )
        if not results:
            console.print("[dim]No matching clips found.[/dim]")
            index.close()
            return

        table = Table(title=f"Clip Search: \"{query}\"", show_header=True, header_style="bold")
        table.add_column("Rank", justify="right", width=5)
        table.add_column("Similarity", justify="right", width=12)
        table.add_column("Bag", style="cyan", max_width=30)
        table.add_column("Topic", style="dim")
        table.add_column("Time Range", justify="right")
        table.add_column("Frames", justify="right")

        for i, r in enumerate(results, 1):
            table.add_row(
                str(i),
                f"{r.avg_similarity:.3f} avg",
                Path(r.bag_path).name,
                r.topic,
                f"{r.start_sec:.1f}s - {r.end_sec:.1f}s",
                str(r.frame_count),
            )
        console.print(table)

        if save:
            save_search_results(results, query, save, extract_clips=True)
            console.print(f"[green]Results saved to {save}[/green]")
    else:
        results = engine.search(
            query, top_k=top_k, min_similarity=min_similarity,
        )
        if not results:
            console.print("[dim]No matching frames found.[/dim]")
            index.close()
            return

        table = Table(title=f"Frame Search: \"{query}\"", show_header=True, header_style="bold")
        table.add_column("Rank", justify="right", width=5)
        table.add_column("Similarity", justify="right", width=12)
        table.add_column("Bag", style="cyan", max_width=30)
        table.add_column("Topic", style="dim")
        table.add_column("Time", justify="right")
        table.add_column("Frame", justify="right")

        for i, r in enumerate(results, 1):
            table.add_row(
                str(i),
                f"{r.similarity:.3f}",
                Path(r.bag_path).name,
                r.topic,
                f"{r.timestamp_sec:.2f}s",
                f"#{r.frame_index}",
            )
        console.print(table)

        if save:
            save_search_results(results, query, save, extract_clips=False)
            console.print(f"[green]Results saved to {save}[/green]")

    index.close()


# --- Bridge commands ---

bridge_app = typer.Typer(help="Resurrector Bridge — stream bag data over WebSocket.")
app.add_typer(bridge_app, name="bridge")


@bridge_app.command("playback")
def bridge_playback(
    bag: Annotated[Path, typer.Argument(
        help="Path to MCAP bag file. e.g. resurrector bridge playback experiment.mcap",
    )],
    port: Annotated[int, typer.Option("--port", "-p",
        help="WebSocket server port. Default 9090 (PlotJuggler's expected port). "
             "e.g. -p 9091",
    )] = 9090,
    host: Annotated[str, typer.Option("--host",
        help="Bind address. Default 0.0.0.0 — accepts connections from any "
             "host on the LAN, since PlotJuggler often runs on a different "
             "machine. e.g. --host 127.0.0.1",
    )] = "0.0.0.0",
    speed: Annotated[float, typer.Option("--speed", "-s",
        help="Playback speed multiplier. 1.0 = real-time, 2.0 = 2× faster, "
             "0.5 = half-speed. Range 0.1–20. e.g. -s 2.0",
    )] = 1.0,
    topics: Annotated[Optional[list[str]], typer.Option("--topic", "-t",
        help="Topic to stream. Pass --topic multiple times for several. "
             "When omitted, all topics are streamed. "
             "e.g. -t /imu/data -t /joint_states",
    )] = None,
    loop: Annotated[bool, typer.Option("--loop",
        help="Restart playback from the beginning when the bag ends, "
             "indefinitely. Useful for live demos. e.g. --loop",
    )] = False,
    no_browser: Annotated[bool, typer.Option("--no-browser",
        help="Skip opening the built-in viewer in the default browser at "
             "startup. e.g. --no-browser",
    )] = False,
    max_rate: Annotated[float, typer.Option("--max-rate",
        help="Per-topic maximum message rate in Hz, applied as a sliding "
             "window. Caps a 1 kHz topic at the given rate so the WebSocket "
             "doesn't saturate. e.g. --max-rate 100",
    )] = 50.0,
    record: Annotated[Optional[Path], typer.Option("--record",
        help="Write every relayed message to a fresh MCAP at this path "
             "(in addition to streaming over WebSocket). Useful when the "
             "bridge applies server-side filters/transforms and you want "
             "to capture the post-processing stream. Parent dir is "
             "created. e.g. --record session.mcap",
    )] = None,
):
    """Stream a recorded bag over WebSocket — PlotJuggler-compatible.

    Replays the bag's messages over WebSocket at the requested speed.
    The built-in HTML viewer at http://host:port/ shows a simple
    plot for sanity checks. PlotJuggler users connect via "WebSocket
    Client" → ws://host:port/ws.

    Pass ``--record path.mcap`` to also write every relayed message to
    a fresh MCAP file. The recording is closed cleanly on shutdown
    (Ctrl+C); if the process dies hard, the file is still readable
    but lacks a summary index.
    """
    import uvicorn
    from resurrector.bridge.server import create_bridge_app

    bridge = create_bridge_app(
        mode="playback", bag_path=bag, speed=speed,
        topics=topics, loop_playback=loop, max_rate_hz=max_rate,
        record_path=record,
    )

    console.print(f"[bold]Resurrector Bridge — Playback Mode[/bold]")
    console.print(f"  WebSocket: [cyan]ws://{host}:{port}/ws[/cyan]")
    console.print(f"  Viewer:    [cyan]http://{host}:{port}/[/cyan]")
    console.print(f"  PlotJuggler: connect WebSocket Client to ws://{host}:{port}/ws")
    if record:
        console.print(f"  Recording: [cyan]{record}[/cyan]")
    console.print(f"  Speed: {speed}x | Loop: {loop}")
    console.print()

    if not no_browser:
        try:
            import webbrowser
            webbrowser.open(f"http://localhost:{port}/")
        except Exception:
            pass

    uvicorn.run(bridge, host=host, port=port, log_level="info")


@bridge_app.command("live")
def bridge_live(
    port: Annotated[int, typer.Option("--port", "-p",
        help="WebSocket server port. Default 9090 (PlotJuggler's default). "
             "e.g. -p 9091",
    )] = 9090,
    host: Annotated[str, typer.Option("--host",
        help="Bind address. Default 0.0.0.0 — accepts connections from any "
             "host on the LAN. e.g. --host 127.0.0.1",
    )] = "0.0.0.0",
    topics: Annotated[Optional[list[str]], typer.Option("--topic", "-t",
        help="ROS 2 topic to subscribe to. Pass --topic multiple times for "
             "several. When omitted, every active topic is auto-discovered. "
             "e.g. -t /imu/data -t /joint_states",
    )] = None,
    max_rate: Annotated[float, typer.Option("--max-rate",
        help="Per-topic max forward rate in Hz to throttle high-frequency "
             "publishers (e.g. a 1 kHz IMU). e.g. --max-rate 100",
    )] = 50.0,
    no_browser: Annotated[bool, typer.Option("--no-browser",
        help="Accepted for API parity with `bridge playback`; live mode "
             "never opens a browser. e.g. --no-browser",
    )] = False,
):
    """Relay LIVE ROS 2 topics over WebSocket — PlotJuggler-compatible.

    Subscribes to the requested topics on a running ROS 2 system and
    forwards messages over WebSocket to a connected PlotJuggler (or the
    built-in viewer). Requires rclpy in the active Python — install via
    `pip install 'rosbag-resurrector[bridge-live]'` AND have a ROS 2
    distribution sourced in the shell. For replaying recorded bags
    instead, use `bridge playback`.
    """
    from resurrector.bridge.live import is_rclpy_available

    if not is_rclpy_available():
        console.print("[red]Live mode requires rclpy (ROS2). Use 'bridge playback' instead.[/red]")
        raise typer.Exit(1)

    import uvicorn
    from resurrector.bridge.server import create_bridge_app

    bridge = create_bridge_app(mode="live", topics=topics, max_rate_hz=max_rate)

    console.print(f"[bold]Resurrector Bridge — Live Mode[/bold]")
    console.print(f"  WebSocket: [cyan]ws://{host}:{port}/ws[/cyan]")
    console.print(f"  Topics: {topics or 'all (auto-discover)'}")

    uvicorn.run(bridge, host=host, port=port, log_level="info")


@app.command()
def dashboard(
    port: Annotated[int, typer.Option("--port", "-p",
        help="Port to bind the web server on. Default 8080. e.g. -p 9090",
    )] = 8080,
    host: Annotated[str, typer.Option("--host",
        help="Address to bind. Default 127.0.0.1 (localhost only — "
             "intentional, since the dashboard has no auth). Set to "
             "0.0.0.0 to expose on the LAN; understand the security "
             "implications first. e.g. --host 0.0.0.0",
    )] = "127.0.0.1",
    db: Annotated[Optional[Path], typer.Option("--db",
        help="Path to a non-default index database. Defaults to "
             "~/.resurrector/index.db. e.g. --db /data/myindex.db",
    )] = None,
):
    """Launch the local web dashboard at http://localhost:8080.

    Pages: Library (browse / scan), Explorer (Plotly per-topic plots
    with brush-zoom and click-to-annotate), Health, Compare, Cross-bag
    Overlay, Search (semantic frame search), Datasets, Bridge.

    Scope of what the dashboard can scan is controlled by the
    RESURRECTOR_ALLOWED_ROOTS environment variable (os.pathsep-separated
    list of directory roots). Defaults to the user's home directory.

    Example:
      resurrector dashboard
      RESURRECTOR_ALLOWED_ROOTS=/data/bags resurrector dashboard --port 9090
    """
    import uvicorn

    console.print(f"[bold]Starting RosBag Resurrector Dashboard[/bold]")
    console.print(f"Open [cyan]http://{host}:{port}[/cyan] in your browser")

    # Set DB path as environment variable for the dashboard to pick up
    import os
    if db:
        os.environ["RESURRECTOR_DB_PATH"] = str(db)

    uvicorn.run(
        "resurrector.dashboard.api:app",
        host=host,
        port=port,
        log_level="info",
    )


@app.command()
def doctor():
    """Verify the install: prints a pass/warn/fail grid for every dependency.

    Two tables: "Core install" (Python version, MCAP parser, DuckDB
    index, Polars, FastAPI — all required and bundled) and "Optional
    extras" (image parsing, video export, CLIP local + OpenAI search,
    live ROS 2 bridge, watch mode, Zarr export, mcap CLI, ros2 CLI).
    Each row tells you exactly what to install if missing — for example:

      pip install 'rosbag-resurrector[vision]'
      pip install 'rosbag-resurrector[all-exports]'

    Run this after `pip install` to confirm what's working before
    chasing import errors. Exits with code 1 if any core check fails.

    Example:
      resurrector doctor
    """
    from resurrector.cli.doctor import run_all_checks, render
    results = run_all_checks()
    passed, warned, failed = render(results)
    if failed:
        raise typer.Exit(code=1)


@app.command()
def demo(
    output: Annotated[Optional[Path], typer.Option(
        "--output", "-o",
        help="Where to write the sample bag. Defaults to "
             "~/.resurrector/demo_sample.mcap (synth) or "
             "~/.resurrector/samples/hku2.mcap (--download). "
             "e.g. -o /tmp/demo.mcap",
    )] = None,
    run_full: Annotated[bool, typer.Option(
        "--full",
        help="Also run scan + health + export on the bag, showing the "
             "full pipeline end-to-end. e.g. --full",
    )] = False,
    download: Annotated[bool, typer.Option(
        "--download",
        help="Download a real-data MCAP bag (HKU FAST-LIVO dataset, "
             "~844 MB, CC-BY-NC-4.0) instead of generating the synthetic "
             "sample. Real footage is needed for the dashboard's CLIP "
             "semantic frame search to return visually meaningful results. "
             "Idempotent — skips download if the file already exists. "
             "e.g. --download",
    )] = False,
    download_url: Annotated[Optional[str], typer.Option(
        "--download-url",
        help="Override the default download URL (HKU FAST-LIVO hku2). "
             "Use to fetch a different public sample bag. "
             "e.g. --download-url https://example.com/sample.mcap",
    )] = None,
    force: Annotated[bool, typer.Option(
        "--force",
        help="Re-download (or re-generate) even if the target file already "
             "exists. e.g. --force",
    )] = False,
):
    """Generate or download a sample bag and walk through the basic workflow.

    Default: generates a 5-second synthetic bag (fast, good for smoke tests
    but cameras contain colored noise — bad for visual demos like CLIP
    search). Pass --download to fetch a real-data MCAP bag with actual
    camera footage instead.

    Examples:
      resurrector demo --full                          # synthetic + walkthrough
      resurrector demo --download                      # fetch real-data sample
      resurrector demo --download --full               # download + walkthrough
      resurrector demo --download -o /data/sample.mcap # custom path
    """
    if download:
        _demo_download(output=output, run_full=run_full, url=download_url, force=force)
        return

    # Default path: generate synthetic bag
    from resurrector.demo.sample_bag import generate_bag, BagConfig

    output = output or Path.home() / ".resurrector" / "demo_sample.mcap"
    output.parent.mkdir(parents=True, exist_ok=True)

    if output.exists() and not force:
        console.print(f"[dim]Sample already exists at {output} (use --force to regenerate)[/dim]")
    else:
        console.print(f"[cyan]Generating demo bag at {output}...[/cyan]")
        generate_bag(output, BagConfig(duration_sec=5.0))
        console.print(f"[green][OK] Created {output.stat().st_size // 1024} KB bag[/green]\n")

    console.print("[cyan]Opening with BagFrame...[/cyan]")
    from resurrector.core.bag_frame import BagFrame
    bf = BagFrame(output)
    bf.info()
    console.print()

    if run_full:
        console.print("[cyan]Running health check...[/cyan]")
        report = bf.health_report()
        console.print(f"Health score: [bold]{report.score}/100[/bold]")
        console.print(f"Warnings: {len(report.warnings)}\n")

        export_dir = output.parent / "demo_export"
        console.print(f"[cyan]Exporting /imu/data to Parquet at {export_dir}...[/cyan]")
        bf.export(topics=["/imu/data"], format="parquet", output=str(export_dir))
        console.print(f"[green][OK] Exported[/green]\n")

    console.print(
        "[dim]Next steps:[/dim]\n"
        f"  [cyan]resurrector scan {output.parent}[/cyan]       # index the bag\n"
        f"  [cyan]resurrector health {output}[/cyan]    # detailed health report\n"
        f"  [cyan]resurrector dashboard[/cyan]                   # open the web UI\n"
    )


def _demo_download(
    output: Optional[Path],
    run_full: bool,
    url: Optional[str],
    force: bool,
) -> None:
    """Implementation of `resurrector demo --download`."""
    from resurrector.demo.download import (
        DEFAULT_SAMPLE_URL,
        LICENSE_NOTE,
        download_sample,
    )
    from rich.progress import (
        BarColumn, DownloadColumn, Progress, TextColumn,
        TimeRemainingColumn, TransferSpeedColumn,
    )

    target = output  # may be None — download_sample handles default
    download_url = url or DEFAULT_SAMPLE_URL

    console.print(
        f"[cyan]Downloading real-data sample bag (HKU FAST-LIVO, ~844 MB)...[/cyan]"
    )
    console.print(f"[dim]URL: {download_url}[/dim]")
    if target:
        console.print(f"[dim]Target: {target}[/dim]")
    console.print()

    progress = Progress(
        TextColumn("[bold blue]hku2.mcap"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
    )

    task_id = None
    try:
        with progress:
            def cb(bytes_so_far: int, total: int) -> None:
                nonlocal task_id
                if task_id is None:
                    # First chunk — set the bar's total now that we know it.
                    # If server didn't send Content-Length (total=0), the bar
                    # will show indeterminate spinner mode, which is fine.
                    task_id = progress.add_task("download", total=total or None)
                progress.update(task_id, completed=bytes_so_far)

            result = download_sample(
                target=target,
                url=download_url,
                progress_callback=cb,
                force=force,
            )
    except Exception as e:
        console.print(f"[red]Download failed: {type(e).__name__}: {e}[/red]")
        raise typer.Exit(1)

    if result.skipped:
        size_mb = result.bytes_downloaded / (1024 * 1024)
        console.print(
            f"[green][OK] Already downloaded at {result.path} "
            f"({size_mb:.0f} MB) — use --force to re-download[/green]\n"
        )
    else:
        size_mb = result.bytes_downloaded / (1024 * 1024)
        console.print(
            f"[green][OK] Downloaded {size_mb:.0f} MB to {result.path}[/green]\n"
        )

    console.print(f"[yellow]{LICENSE_NOTE}[/yellow]\n")

    console.print("[cyan]Opening with BagFrame...[/cyan]")
    from resurrector.core.bag_frame import BagFrame
    bf = BagFrame(result.path)
    bf.info()
    console.print()

    if run_full:
        console.print("[cyan]Running health check (real bags can take a moment)...[/cyan]")
        report = bf.health_report()
        console.print(f"Health score: [bold]{report.score}/100[/bold]")
        console.print(f"Warnings: {len(report.warnings)}\n")

    console.print(
        "[dim]Next steps:[/dim]\n"
        f"  [cyan]resurrector scan {result.path.parent}[/cyan]\n"
        f"  [cyan]resurrector index-frames {result.path}[/cyan]    # build CLIP embeddings\n"
        f"  [cyan]resurrector search-frames \"person walking\" --save ./hits[/cyan]\n"
        f"  [cyan]resurrector dashboard[/cyan]                   # open localhost:8080\n"
    )


if __name__ == "__main__":
    app()
