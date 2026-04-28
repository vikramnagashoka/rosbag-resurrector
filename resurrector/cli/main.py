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
             "e.g. resurrector export experiment.mcap -t /imu/data -f parquet",
    )],
    topics: Annotated[Optional[list[str]], typer.Option("--topics", "-t",
        help="Topics to export. Pass --topics multiple times for multi-topic "
             "exports. When omitted, every non-image topic is exported. "
             "e.g. -t /imu/data -t /joint_states",
    )] = None,
    format: Annotated[str, typer.Option("--format", "-f",
        help="Output format. parquet (default, columnar, best for ML), "
             "hdf5 (numerical workflows), csv (readable, large), "
             "numpy (.npz, capped at 1 M rows per topic — refuses larger), "
             "zarr (chunked, requires [all-exports]), "
             "lerobot / rlds (training-ready, requires [all-exports]). "
             "e.g. -f hdf5",
    )] = "parquet",
    output: Annotated[Path, typer.Option("--output", "-o",
        help="Output directory. Created if missing. One file (or sub-directory "
             "for multi-file formats) per exported topic. "
             "e.g. -o ./training_data",
    )] = Path("./export"),
    sync: Annotated[Optional[str], typer.Option("--sync",
        help="When set, all selected topics are time-aligned to one another "
             "before export. Methods: 'nearest' (closest-in-time match), "
             "'interpolate' (linear interp on numeric fields), "
             "'sample_and_hold' (carry last value forward). Default: no sync; "
             "each topic exported at its native rate. e.g. --sync nearest",
    )] = None,
    downsample: Annotated[Optional[float], typer.Option("--downsample",
        help="Resample exported topics to this rate in Hz before writing. "
             "Useful for shrinking dataset size when full sensor rate is "
             "more than your model needs. e.g. --downsample 50",
    )] = None,
):
    """Export bag data to ML-ready formats — Parquet, HDF5, NumPy, Zarr, LeRobot, RLDS.

    All chunk-streaming formats (Parquet, HDF5, CSV, Zarr, LeRobot, RLDS)
    are memory-bounded by chunk size, not topic size — open a 100 GB bag
    without OOMing. NumPy `.npz` is the exception: it materializes the
    full topic and refuses topics over 1 M messages with a clear error
    pointing at Parquet.

    Examples:
      Quick Parquet export of two topics, native rates:
          resurrector export bag.mcap -t /imu/data -t /joint_states

      Time-synced export at 50 Hz for downstream ML training:
          resurrector export bag.mcap -t /imu/data -t /joint_states \
              --sync nearest --downsample 50 --format hdf5 -o ./training

      LeRobot-format dataset for direct use in robot-learning pipelines:
          resurrector export bag.mcap --format lerobot -o ./lerobot_data

    Format support note: zarr/rlds need `pip install
    'rosbag-resurrector[all-exports]'`. The export will fail with a
    clear message if the extra isn't installed.
    """
    from resurrector.core.bag_frame import BagFrame

    bf = BagFrame(path)
    do_sync = sync is not None
    sync_method = sync or "nearest"

    result_path = bf.export(
        topics=topics,
        format=format,
        output=str(output),
        sync=do_sync,
        sync_method=sync_method,
        downsample_hz=downsample,
    )
    console.print(f"[green]Exported to {result_path}[/green]")


@app.command()
def diff(
    bag1: Annotated[Path, typer.Argument(
        help="First bag (the baseline / 'before'). e.g. baseline.mcap",
    )],
    bag2: Annotated[Path, typer.Argument(
        help="Second bag (the comparison / 'after'). e.g. experiment.mcap",
    )],
):
    """Compare topic lists, message counts, and durations across two bags.

    Useful for "did this run record what the previous run did?" sanity
    checks and for diagnosing setup drift between recordings (a topic
    silently dropped, a frequency change, a duration mismatch).

    For visual / numeric trace overlays, use the dashboard's Compare or
    Cross-bag Overlay pages instead.

    Example:
      resurrector diff baseline.mcap experiment.mcap
    """
    from resurrector.ingest.parser import parse_bag
    from resurrector.cli.formatters import print_diff

    parser1 = parse_bag(bag1)
    parser2 = parse_bag(bag2)
    print_diff(parser1.get_metadata(), parser2.get_metadata())


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
):
    """Stream a recorded bag over WebSocket — PlotJuggler-compatible.

    Replays the bag's messages over WebSocket at the requested speed.
    The built-in HTML viewer at http://host:port/ shows a simple
    plot for sanity checks. PlotJuggler users connect via "WebSocket
    Client" → ws://host:port/ws.
    """
    import uvicorn
    from resurrector.bridge.server import create_bridge_app

    bridge = create_bridge_app(
        mode="playback", bag_path=bag, speed=speed,
        topics=topics, loop_playback=loop, max_rate_hz=max_rate,
    )

    console.print(f"[bold]Resurrector Bridge — Playback Mode[/bold]")
    console.print(f"  WebSocket: [cyan]ws://{host}:{port}/ws[/cyan]")
    console.print(f"  Viewer:    [cyan]http://{host}:{port}/[/cyan]")
    console.print(f"  PlotJuggler: connect WebSocket Client to ws://{host}:{port}/ws")
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
             "never opens a browser.",
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
             "~/.resurrector/demo_sample.mcap. e.g. -o /tmp/demo.mcap",
    )] = None,
    run_full: Annotated[bool, typer.Option(
        "--full",
        help="Also run scan + health + export on the generated bag, "
             "showing the full pipeline end-to-end. e.g. --full",
    )] = False,
):
    """Generate a synthetic sample bag and walk through the basic workflow.

    Useful as a smoke test or to show what the tool can do without
    needing your own data.
    """
    from resurrector.demo.sample_bag import generate_bag, BagConfig

    output = output or Path.home() / ".resurrector" / "demo_sample.mcap"
    output.parent.mkdir(parents=True, exist_ok=True)

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


if __name__ == "__main__":
    app()
