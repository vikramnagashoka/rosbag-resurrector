"""What a CLI command does when a bag has topics it can't decode.

Commands that read message fields (``export``, ``dataset export``,
``bridge playback``) stop before reading. With a terminal, the user
chooses: load decoders from a path on their machine, go on without those
topics (the health checks still cover them), or stop. Without one (CI, a
pipe, the dashboard's bridge subprocess) the command exits with the
flags that answer the question up front, so it never waits for input
nobody will type.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.markup import escape as rich_escape
from rich.prompt import Prompt
from rich.table import Table

from resurrector.core.exceptions import UndecodableTopic
from resurrector.ingest.custom_decoders import ENV_VAR, load_decoders

console = Console()
err_console = Console(stderr=True, soft_wrap=True)

DECODERS_HELP = (
    "Decoders for message types the bag can't be decoded from by itself: a "
    "folder of .msg files (e.g. ~/ros2_ws/src or /opt/ros/humble/share) or a "
    "Python file that calls register_decoder. Repeatable. "
    f"{ENV_VAR}=PATH does the same for every command. "
    "e.g. --decoders ~/ros2_ws/src"
)
SKIP_HELP = (
    "Leave out topics whose message type can't be decoded, instead of "
    "asking (or, without a terminal, failing). The health checks still "
    "cover them. e.g. --skip-undecodable"
)

RETRY = "retry"
SKIP = "skip"


def load_paths(paths: Optional[list[Path]]) -> None:
    """Load each ``--decoders`` path, or exit 1 naming the one that failed."""
    for p in paths or []:
        try:
            types = load_decoders(p)
        except (OSError, ValueError) as e:
            err_console.print(f"[red]--decoders {rich_escape(str(p))}: {rich_escape(str(e))}[/red]")
            raise typer.Exit(1)
        console.print(f"[dim]Loaded {_count(len(types), 'message type')} from {rich_escape(str(p))}[/dim]")


def ask(problems: list[UndecodableTopic], *, can_skip: bool = True) -> str:
    """Show what can't be decoded and ask what to do.

    Returns :data:`RETRY` after loading decoders from a path (the caller
    checks again: a definition can still be missing a type it uses) or
    :data:`SKIP` when the user chose to go on without these topics. Exits
    1 when the user stops, and when there's no terminal to ask in.
    """
    if not _interactive():
        _print_problems(problems, err_console)
        how = "--decoders PATH (a folder of .msg files, or a Python file that calls register_decoder)"
        tail = f" or --skip-undecodable to go on without {_them(problems)}," if can_skip else ","
        err_console.print(
            f"Pass {how} to decode {_them(problems)},{tail} or set {ENV_VAR}=PATH "
            f"for every command."
        )
        raise typer.Exit(1)

    _print_problems(problems, console)
    options = [("1", "Load decoders from a path (a folder of .msg files, or a Python file)")]
    if can_skip:
        options.append(("2", f"Go on without {_them(problems)}; the health checks still cover {_them(problems)}"))
    options.append(("3", "Stop"))
    console.print("\nThe built-in decoders can't read these message types. Choose:")
    for key, text in options:
        console.print(f"  [bold]{key}[/bold]  {text}")
    choice = Prompt.ask("Choice", choices=[k for k, _ in options], default="1", console=console)
    if choice == "2":
        return SKIP
    if choice == "3":
        raise typer.Exit(1)

    while True:
        raw = Prompt.ask("Path to a .msg folder or a Python file", console=console).strip()
        if not raw:
            raise typer.Exit(1)
        try:
            types = load_decoders(raw)
        except (OSError, ValueError) as e:
            console.print(f"[red]{rich_escape(str(e))}[/red]")
            continue
        console.print(
            f"[green]Loaded {_count(len(types), 'message type')} from {rich_escape(raw)}.[/green] "
            f"[dim]Next time pass --decoders {rich_escape(raw)}, or export {ENV_VAR}={rich_escape(raw)}.[/dim]"
        )
        return RETRY


def left_out_note(problems: list[UndecodableTopic]) -> None:
    names = ", ".join(t.topic for t in problems)
    console.print(
        f"[yellow]Left out {rich_escape(names)}: the message type can't be decoded. "
        f"The health checks still cover {_them(problems)}.[/yellow]"
    )


def info_note(problems: list[UndecodableTopic]) -> None:
    """The footnote ``info`` and ``quicklook`` print under the topic table."""
    if not problems:
        return
    console.print(
        f"[yellow]{_count(len(problems), 'topic')} can't be decoded, so exports and "
        f"plots can't read {'its' if len(problems) == 1 else 'their'} fields "
        f"(health still covers {_them(problems)}):[/yellow]"
    )
    for t in problems:
        console.print(f"  [cyan]{rich_escape(t.topic)}[/cyan]  {rich_escape(t.message_type)}: {rich_escape(t.reason)}")
    console.print(f"[dim]  Decode {_them(problems)} with --decoders PATH or {ENV_VAR}=PATH "
                  f"(a folder of .msg files, or a Python file that calls register_decoder).[/dim]")


def _print_problems(problems: list[UndecodableTopic], out: Console) -> None:
    out.print(f"[bold yellow]{_count(len(problems), 'topic')} use{'s' if len(problems) == 1 else ''} "
              f"a message type Resurrector can't decode:[/bold yellow]")
    table = Table(show_header=True, header_style="bold", box=None, pad_edge=False)
    table.add_column("Topic", style="cyan")
    table.add_column("Type")
    table.add_column("Why", style="dim")
    for t in problems:
        table.add_row(rich_escape(t.topic), rich_escape(t.message_type), rich_escape(t.reason))
    out.print(table)


def _interactive() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


def _them(problems: list) -> str:
    return "it" if len(problems) == 1 else "them"


def _count(n: int, noun: str) -> str:
    return f"{n} {noun}{'' if n == 1 else 's'}"
