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
)
console = Console()


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
    path: Annotated[Path, typer.Argument(help="Directory or file to scan for bag files")],
    db: Annotated[Optional[Path], typer.Option("--db", help="Path to index database")] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Enable verbose logging")] = False,
    log_file: Annotated[Optional[str], typer.Option("--log-file", help="Write logs to file")] = None,
):
    """Scan a directory for bag files and index them."""
    _setup_logging(verbose, log_file)
    from resurrector.ingest.scanner import scan_path
    from resurrector.ingest.parser import parse_bag
    from resurrector.ingest.indexer import BagIndex
    from resurrector.cli.formatters import create_progress, console as fmt_console

    files = scan_path(path)
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

                progress.advance(task)
            except Exception as e:
                console.print(f"[red]Error indexing {scanned_file.path.name}: {e}[/red]")
                progress.advance(task)

    console.print(f"[green]Indexed {index.count()} bag(s) total.[/green]")
    index.close()


@app.command()
def info(
    path: Annotated[Path, typer.Argument(help="Path to a bag file")],
):
    """Show detailed info about a bag file."""
    from resurrector.core.bag_frame import BagFrame
    from resurrector.cli.formatters import print_bag_info

    bf = BagFrame(path)
    health = bf.health_report()
    print_bag_info(bf.metadata, health, path.stat().st_size)


@app.command()
def health(
    path: Annotated[Path, typer.Argument(help="Path to a bag file or directory")],
    format: Annotated[str, typer.Option("--format", "-f", help="Output format: rich, json")] = "rich",
    output: Annotated[Optional[Path], typer.Option("--output", "-o", help="Output file path")] = None,
):
    """Run health checks on a bag file or directory."""
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
    after: Annotated[Optional[str], typer.Option(help="Show bags after this date (YYYY-MM-DD)")] = None,
    before: Annotated[Optional[str], typer.Option(help="Show bags before this date (YYYY-MM-DD)")] = None,
    has_topic: Annotated[Optional[str], typer.Option("--has-topic", help="Filter to bags with this topic")] = None,
    min_health: Annotated[Optional[int], typer.Option("--min-health", help="Minimum health score")] = None,
    db: Annotated[Optional[Path], typer.Option("--db", help="Path to index database")] = None,
):
    """List all indexed bags with optional filtering."""
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
    path: Annotated[Path, typer.Argument(help="Path to a bag file")],
    topics: Annotated[Optional[list[str]], typer.Option("--topics", "-t", help="Topics to export")] = None,
    format: Annotated[str, typer.Option("--format", "-f", help="Export format: parquet, hdf5, csv, numpy, zarr")] = "parquet",
    output: Annotated[Path, typer.Option("--output", "-o", help="Output directory")] = Path("./export"),
    sync: Annotated[Optional[str], typer.Option("--sync", help="Sync method: nearest, interpolate, sample_and_hold")] = None,
    downsample: Annotated[Optional[float], typer.Option("--downsample", help="Target frequency in Hz")] = None,
):
    """Export bag data to ML-friendly formats."""
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
    bag1: Annotated[Path, typer.Argument(help="First bag file")],
    bag2: Annotated[Path, typer.Argument(help="Second bag file")],
):
    """Compare two bag files side by side."""
    from resurrector.ingest.parser import parse_bag
    from resurrector.cli.formatters import print_diff

    parser1 = parse_bag(bag1)
    parser2 = parse_bag(bag2)
    print_diff(parser1.get_metadata(), parser2.get_metadata())


@app.command()
def tag(
    path: Annotated[Path, typer.Argument(help="Path to a bag file")],
    add: Annotated[Optional[list[str]], typer.Option("--add", help="Tags to add (key:value)")] = None,
    remove: Annotated[Optional[list[str]], typer.Option("--remove", help="Tags to remove")] = None,
    db: Annotated[Optional[Path], typer.Option("--db", help="Path to index database")] = None,
):
    """Tag bags for organization."""
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
        console.print(f"Current tags: {', '.join(f'{t['key']}:{t['value']}' for t in tags)}")
    else:
        console.print("[dim]No tags.[/dim]")

    index.close()


@app.command()
def quicklook(
    path: Annotated[Path, typer.Argument(help="Path to a bag file")],
):
    """Quick rich summary: health badge, topic sparklines, anomaly highlights."""
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
    path: Annotated[Path, typer.Argument(help="Directory to watch for new bag files")],
    db: Annotated[Optional[Path], typer.Option("--db", help="Path to index database")] = None,
    interval: Annotated[float, typer.Option("--interval", "-i", help="Poll interval in seconds")] = 5.0,
):
    """Watch a directory for new bag files and auto-index them."""
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
    name: Annotated[str, typer.Argument(help="Dataset name")],
    description: Annotated[str, typer.Option("--desc", "-d", help="Description")] = "",
    db: Annotated[Optional[Path], typer.Option("--db", help="Path to index database")] = None,
):
    """Create a new dataset."""
    from resurrector.core.dataset import DatasetManager
    mgr = DatasetManager(db)
    did = mgr.create(name, description)
    console.print(f"[green]Created dataset '{name}' (id={did})[/green]")
    mgr.close()


@dataset_app.command("add-version")
def dataset_add_version(
    name: Annotated[str, typer.Argument(help="Dataset name")],
    version: Annotated[str, typer.Argument(help="Version string (e.g., '1.0')")],
    bags: Annotated[list[Path], typer.Option("--bag", "-b", help="Bag file paths")],
    topics: Annotated[Optional[list[str]], typer.Option("--topic", "-t", help="Topics to include")] = None,
    format: Annotated[str, typer.Option("--format", "-f", help="Export format")] = "parquet",
    sync_method: Annotated[Optional[str], typer.Option("--sync", help="Sync method")] = None,
    downsample: Annotated[Optional[float], typer.Option("--downsample", help="Target Hz")] = None,
    db: Annotated[Optional[Path], typer.Option("--db", help="Path to index database")] = None,
):
    """Add a version to a dataset with bag references and config."""
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
    name: Annotated[str, typer.Argument(help="Dataset name")],
    version: Annotated[str, typer.Argument(help="Version to export")],
    output: Annotated[Path, typer.Option("--output", "-o", help="Output directory")] = Path("./datasets"),
    db: Annotated[Optional[Path], typer.Option("--db", help="Path to index database")] = None,
):
    """Export a dataset version to disk with README and manifest."""
    from resurrector.core.dataset import DatasetManager
    mgr = DatasetManager(db)
    result = mgr.export_version(name, version, str(output))
    console.print(f"[green]Exported to {result}[/green]")
    mgr.close()


@dataset_app.command("list")
def dataset_list(
    db: Annotated[Optional[Path], typer.Option("--db", help="Path to index database")] = None,
):
    """List all datasets."""
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
    path: Annotated[Path, typer.Argument(help="Path to a bag file")],
    topic: Annotated[str, typer.Option("--topic", "-t", help="Image topic name")],
    output: Annotated[Path, typer.Option("--output", "-o", help="Output directory or video path")] = Path("./frames"),
    format: Annotated[str, typer.Option("--format", "-f", help="Image format: png or jpeg")] = "png",
    video: Annotated[bool, typer.Option("--video", help="Export as MP4 video instead of frames")] = False,
    fps: Annotated[Optional[float], typer.Option("--fps", help="Video FPS (default: topic frequency)")] = None,
    max_frames: Annotated[Optional[int], typer.Option("--max-frames", help="Maximum frames to export")] = None,
    every_n: Annotated[int, typer.Option("--every-n", help="Export every Nth frame")] = 1,
):
    """Export image topic as frame sequence (PNG/JPEG) or MP4 video."""
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
    path: Annotated[Path, typer.Argument(help="Path to a bag file or directory")],
    topic: Annotated[Optional[str], typer.Option("--topic", "-t", help="Image topic (auto-detects if omitted)")] = None,
    sample_hz: Annotated[float, typer.Option("--sample-hz", help="Frame sampling rate")] = 5.0,
    batch_size: Annotated[int, typer.Option("--batch-size", help="Embedding batch size")] = 32,
    force: Annotated[bool, typer.Option("--force", help="Re-index even if embeddings exist")] = False,
    db: Annotated[Optional[Path], typer.Option("--db", help="Path to index database")] = None,
):
    """Generate CLIP embeddings for image frames in bags for semantic search.

    Requires: pip install rosbag-resurrector[vision]
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
    query: Annotated[str, typer.Argument(help="Natural language search query")],
    top_k: Annotated[int, typer.Option("--top-k", "-k", help="Number of results")] = 20,
    clips: Annotated[bool, typer.Option("--clips", help="Group results into temporal clips")] = False,
    clip_duration: Annotated[float, typer.Option("--clip-duration", help="Clip grouping window (seconds)")] = 5.0,
    min_similarity: Annotated[float, typer.Option("--min-sim", help="Minimum cosine similarity")] = 0.15,
    save: Annotated[Optional[Path], typer.Option("--save", help="Save results (frames/clips + results.json)")] = None,
    db: Annotated[Optional[Path], typer.Option("--db", help="Path to index database")] = None,
):
    """Search bag frames by natural language description.

    Examples:
        resurrector search-frames "robot picks up red ball"
        resurrector search-frames "collision event" --clips
        resurrector search-frames "robot arm" --save ./results
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
    bag: Annotated[Path, typer.Argument(help="Path to MCAP bag file")],
    port: Annotated[int, typer.Option("--port", "-p", help="Server port")] = 9090,
    host: Annotated[str, typer.Option("--host", help="Bind host")] = "0.0.0.0",
    speed: Annotated[float, typer.Option("--speed", "-s", help="Playback speed")] = 1.0,
    topics: Annotated[Optional[list[str]], typer.Option("--topic", "-t", help="Topics to stream")] = None,
    loop: Annotated[bool, typer.Option("--loop", help="Loop playback")] = False,
    no_browser: Annotated[bool, typer.Option("--no-browser", help="Don't open browser")] = False,
    max_rate: Annotated[float, typer.Option("--max-rate", help="Max message rate (Hz)")] = 50.0,
):
    """Stream bag playback over WebSocket (PlotJuggler compatible).

    Connect PlotJuggler → WebSocket Client → ws://host:port/ws
    Or open http://host:port/ for the built-in viewer.
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
    port: Annotated[int, typer.Option("--port", "-p", help="Server port")] = 9090,
    host: Annotated[str, typer.Option("--host", help="Bind host")] = "0.0.0.0",
    topics: Annotated[Optional[list[str]], typer.Option("--topic", "-t", help="Topics to subscribe")] = None,
    max_rate: Annotated[float, typer.Option("--max-rate", help="Max message rate (Hz)")] = 50.0,
):
    """Relay live ROS2 topics over WebSocket (requires rclpy).

    Connect PlotJuggler → WebSocket Client → ws://host:port/ws
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
    port: Annotated[int, typer.Option("--port", "-p", help="Port to run the dashboard on")] = 8080,
    host: Annotated[str, typer.Option("--host", help="Host to bind to")] = "127.0.0.1",
    db: Annotated[Optional[Path], typer.Option("--db", help="Path to index database")] = None,
):
    """Launch the interactive web dashboard."""
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
    """Check that your environment is ready — shows what works with the current install."""
    from resurrector.cli.doctor import run_all_checks, render
    results = run_all_checks()
    passed, warned, failed = render(results)
    if failed:
        raise typer.Exit(code=1)


@app.command()
def demo(
    output: Annotated[Optional[Path], typer.Option(
        "--output", "-o", help="Where to write the sample bag",
    )] = None,
    run_full: Annotated[bool, typer.Option(
        "--full", help="Also run scan + health + export on the sample",
    )] = False,
):
    """Generate a synthetic sample bag and walk through the basic workflow.

    Useful as a smoke test or to show what the tool can do without
    needing your own data.
    """
    from tests.fixtures.generate_test_bags import generate_bag, BagConfig

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
