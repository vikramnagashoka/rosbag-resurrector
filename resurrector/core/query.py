"""SQL-like query interface for searching across indexed bag files."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from resurrector.ingest.indexer import BagIndex, DEFAULT_INDEX_PATH


def search(
    query: str,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Search bags in the index with a compact filter-string DSL.

    Query terms are space-separated. Multiple terms compose with AND.

    Supported terms:

      ===============================  ===========================================
      Term                             Meaning
      ===============================  ===========================================
      ``topic:<name>``                 Has this exact topic name
      ``health:>N``                    Health score strictly greater than N
      ``health:<N``                    Health score strictly less than N
      ``health:>=N`` / ``health:<=N``  Inclusive variants
      ``tag:<key>:<value>``            Has this exact tag
      ``after:YYYY-MM-DD``             Recorded on or after this date
      ``before:YYYY-MM-DD``            Recorded on or before this date
      ``<plain text>``                 Substring match against the file path
      ===============================  ===========================================

    Args:
        query: The filter string. Empty string returns every indexed bag.
        db_path: Custom index DB path. ``None`` uses ``~/.resurrector/index.db``.

    Returns:
        List of bag dicts (each has ``id``, ``path``, ``health_score``,
        ``duration_sec``, ``recorded_at``, etc.). Empty list if no matches.

    Example::

        from resurrector import search

        # Clean recent bags with IMU data
        hits = search("topic:/imu/data health:>=80 after:2026-04-01")
        for bag in hits:
            print(bag["path"], "score:", bag["health_score"])
    """
    index = BagIndex(db_path or DEFAULT_INDEX_PATH)
    try:
        return index.search(query)
    finally:
        index.close()
