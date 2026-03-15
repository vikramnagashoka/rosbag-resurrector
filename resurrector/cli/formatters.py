"""Rich console output formatting for the CLI."""

from __future__ import annotations

from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

from resurrector.ingest.health_check import BagHealthReport, HealthIssue, Severity, TopicHealth
from resurrector.ingest.parser import BagMetadata

console = Console()


def format_size(size_bytes: int) -> str:
    """Format byte size to human-readable string."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def health_badge(score: int) -> Text:
    """Create a colored health score badge."""
    if score >= 90:
        return Text(f" {score}/100 ", style="bold white on green")
    elif score >= 70:
        return Text(f" {score}/100 ", style="bold black on yellow")
    elif score >= 50:
        return Text(f" {score}/100 ", style="bold white on dark_orange")
    else:
        return Text(f" {score}/100 ", style="bold white on red")


def topic_health_icon(score: int | None) -> str:
    """Return a health status icon for a topic."""
    if score is None:
        return "?"
    if score >= 90:
        return "[green]OK[/green]"
    elif score >= 70:
        return f"[yellow]WARN({score})[/yellow]"
    else:
        return f"[red]BAD({score})[/red]"


def severity_style(severity: Severity) -> str:
    """Return a Rich style for a severity level."""
    return {
        Severity.INFO: "dim",
        Severity.WARNING: "yellow",
        Severity.ERROR: "red",
        Severity.CRITICAL: "bold red",
    }.get(severity, "")


def print_bag_info(metadata: BagMetadata, health: BagHealthReport, file_size: int):
    """Print a formatted bag info summary."""
    # Header
    console.print()
    console.print(Panel(
        f"[bold]{metadata.path.name}[/bold]\n"
        f"Health: {_score_text(health.score)} | "
        f"Duration: [cyan]{metadata.duration_sec:.1f}s[/cyan] | "
        f"Size: [cyan]{format_size(file_size)}[/cyan] | "
        f"Topics: [cyan]{len(metadata.topics)}[/cyan] | "
        f"Messages: [cyan]{metadata.message_count:,}[/cyan]",
        title="RosBag Resurrector",
        border_style="blue",
    ))

    # Topics table
    table = Table(title="Topics", show_header=True, header_style="bold")
    table.add_column("Topic", style="cyan", min_width=25)
    table.add_column("Type", style="dim")
    table.add_column("Count", justify="right")
    table.add_column("Freq (Hz)", justify="right")
    table.add_column("Health", justify="center")

    for topic in metadata.topics:
        freq = f"{topic.frequency_hz:.1f}" if topic.frequency_hz else "?"
        th = health.topic_scores.get(topic.name)
        health_str = topic_health_icon(th.score if th else None)
        table.add_row(
            topic.name,
            topic.message_type,
            f"{topic.message_count:,}",
            freq,
            health_str,
        )

    console.print(table)


def print_health_report(health: BagHealthReport, bag_name: str):
    """Print a detailed health report."""
    console.print()
    console.print(Panel(
        f"Health Score: {_score_text(health.score)}",
        title=f"Health Report — {bag_name}",
        border_style="blue",
    ))

    if not health.issues:
        console.print("[green]No issues detected. All checks passed.[/green]")
        return

    # Issues table
    table = Table(title="Issues", show_header=True, header_style="bold")
    table.add_column("Severity", justify="center", width=10)
    table.add_column("Topic", style="cyan")
    table.add_column("Check", style="dim")
    table.add_column("Message")
    table.add_column("Time", justify="right")

    for issue in sorted(health.issues, key=lambda i: i.severity.value, reverse=True):
        sev_text = f"[{severity_style(issue.severity)}]{issue.severity.value.upper()}[/{severity_style(issue.severity)}]"
        time_str = f"{issue.start_time_sec:.2f}s" if issue.start_time_sec else "-"
        table.add_row(
            sev_text,
            issue.topic or "-",
            issue.check_name,
            issue.message,
            time_str,
        )

    console.print(table)

    # Recommendations
    if health.recommendations:
        console.print()
        console.print("[bold]Recommendations:[/bold]")
        for rec in health.recommendations:
            console.print(f"  [dim]-[/dim] {rec}")

    # Per-topic breakdown
    console.print()
    topic_table = Table(title="Per-Topic Scores", show_header=True, header_style="bold")
    topic_table.add_column("Topic", style="cyan")
    topic_table.add_column("Score", justify="center")
    topic_table.add_column("Issues", justify="right")

    for topic, th in sorted(health.topic_scores.items()):
        topic_table.add_row(
            topic,
            _score_text(th.score),
            str(len(th.issues)),
        )

    console.print(topic_table)


def print_bag_list(bags: list[dict[str, Any]]):
    """Print a table of indexed bags."""
    if not bags:
        console.print("[dim]No bags found.[/dim]")
        return

    table = Table(title="Indexed Bags", show_header=True, header_style="bold")
    table.add_column("ID", justify="right", style="dim")
    table.add_column("Path", style="cyan", max_width=50)
    table.add_column("Duration", justify="right")
    table.add_column("Size", justify="right")
    table.add_column("Topics", justify="right")
    table.add_column("Health", justify="center")
    table.add_column("Tags", style="dim")

    for bag in bags:
        from pathlib import Path
        path = Path(bag["path"]).name
        duration = f"{bag['duration_sec']:.1f}s" if bag.get("duration_sec") else "?"
        size = format_size(bag.get("size_bytes", 0))
        n_topics = str(len(bag.get("topics", [])))
        score = bag.get("health_score")
        health_str = _score_text(score) if score is not None else "[dim]?[/dim]"
        tags = ", ".join(f"{t['key']}:{t['value']}" for t in bag.get("tags", []))

        table.add_row(
            str(bag["id"]),
            path,
            duration,
            size,
            n_topics,
            health_str,
            tags or "-",
        )

    console.print(table)


def print_diff(meta1: BagMetadata, meta2: BagMetadata):
    """Print a side-by-side comparison of two bags."""
    table = Table(title="Bag Comparison", show_header=True, header_style="bold")
    table.add_column("Property", style="bold")
    table.add_column(meta1.path.name, style="cyan")
    table.add_column(meta2.path.name, style="green")

    table.add_row("Duration", f"{meta1.duration_sec:.1f}s", f"{meta2.duration_sec:.1f}s")
    table.add_row("Messages", f"{meta1.message_count:,}", f"{meta2.message_count:,}")
    table.add_row("Topics", str(len(meta1.topics)), str(len(meta2.topics)))

    console.print(table)

    # Topic overlap
    topics1 = {t.name for t in meta1.topics}
    topics2 = {t.name for t in meta2.topics}
    shared = topics1 & topics2
    only1 = topics1 - topics2
    only2 = topics2 - topics1

    if shared:
        console.print(f"\n[bold]Shared topics ({len(shared)}):[/bold]")
        topic_table = Table(show_header=True, header_style="bold")
        topic_table.add_column("Topic", style="cyan")
        topic_table.add_column(f"Count ({meta1.path.name})", justify="right")
        topic_table.add_column(f"Count ({meta2.path.name})", justify="right")

        t1_map = {t.name: t for t in meta1.topics}
        t2_map = {t.name: t for t in meta2.topics}
        for name in sorted(shared):
            topic_table.add_row(
                name,
                f"{t1_map[name].message_count:,}",
                f"{t2_map[name].message_count:,}",
            )
        console.print(topic_table)

    if only1:
        console.print(f"\n[yellow]Only in {meta1.path.name}:[/yellow] {', '.join(sorted(only1))}")
    if only2:
        console.print(f"\n[yellow]Only in {meta2.path.name}:[/yellow] {', '.join(sorted(only2))}")


def _score_text(score: int | None) -> str:
    """Format a score with color."""
    if score is None:
        return "[dim]?[/dim]"
    if score >= 90:
        return f"[green]{score}/100[/green]"
    elif score >= 70:
        return f"[yellow]{score}/100[/yellow]"
    else:
        return f"[red]{score}/100[/red]"


def create_progress() -> Progress:
    """Create a Rich progress bar for scanning/processing."""
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    )
