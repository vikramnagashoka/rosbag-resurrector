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


if __name__ == "__main__":
    app()
