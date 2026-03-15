"""SQL-like query interface for searching across indexed bag files."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from resurrector.ingest.indexer import BagIndex, DEFAULT_INDEX_PATH


def search(
    query: str,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Search indexed bags using a query string.

    Query syntax:
        topic:/camera/rgb         — has this topic
        health:>80                — health score above 80
        tag:task:pick_and_place   — has this tag
        after:2025-01-01          — recorded after date
        before:2025-06-01         — recorded before date
        Free text                 — matches against file path

    Example:
        results = search("topic:/camera/rgb health:>80 after:2025-01")
    """
    index = BagIndex(db_path or DEFAULT_INDEX_PATH)
    try:
        return index.search(query)
    finally:
        index.close()
