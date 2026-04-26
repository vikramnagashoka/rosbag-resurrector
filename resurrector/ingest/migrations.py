"""DuckDB schema migrations for the bag index.

Each entry in MIGRATIONS is applied once, in order, to bring an existing
index forward. New migrations append to the end and bump the file's
top-level SCHEMA_VERSION constant by one. Never edit a past migration —
add a new one instead.

The framework is intentionally simple: a single-column `_meta` table
tracks `schema_version`, every migration runs in its own implicit
transaction (DuckDB autocommits per statement), and `_init_schema()` in
indexer.py applies whatever's missing on every connect.

Migration 1 was the v0.3.x → v0.4.0 SHA fingerprint rename — it renames
`bags.sha256` to `bags.fingerprint` and adds a nullable `bags.sha256_full`
column for users who opt into real full-file hashing via `scan --full-hash`.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Migration:
    version: int
    description: str
    sql: str  # one or more statements, separated by ';'


# Append new migrations to the end. Never reorder. Never rewrite history.
MIGRATIONS: list[Migration] = [
    Migration(
        version=1,
        description="Rename bags.sha256 -> bags.fingerprint; add bags.sha256_full",
        sql="""
            ALTER TABLE bags RENAME COLUMN sha256 TO fingerprint;
            ALTER TABLE bags ADD COLUMN sha256_full VARCHAR;
        """,
    ),
]


SCHEMA_VERSION = max((m.version for m in MIGRATIONS), default=0)


def apply_pending(conn) -> list[int]:
    """Apply every migration with version > current schema_version.

    Returns the list of versions that were applied (empty if already up
    to date). Caller must hold any locks; this function does not lock.
    """
    # Ensure _meta exists. We use INSERT-if-empty rather than DEFAULT so
    # that brand-new databases start at 0 (and then have all migrations
    # applied) — same code path as upgrading from v0.3.x.
    conn.execute(
        "CREATE TABLE IF NOT EXISTS _meta ("
        "  schema_version INTEGER NOT NULL"
        ")"
    )
    row = conn.execute("SELECT schema_version FROM _meta").fetchone()
    if row is None:
        conn.execute("INSERT INTO _meta (schema_version) VALUES (0)")
        current = 0
    else:
        current = int(row[0])

    applied: list[int] = []
    for migration in MIGRATIONS:
        if migration.version <= current:
            continue
        for stmt in _split_statements(migration.sql):
            conn.execute(stmt)
        conn.execute(
            "UPDATE _meta SET schema_version = ?",
            [migration.version],
        )
        applied.append(migration.version)
    return applied


def _split_statements(sql: str) -> list[str]:
    """Split a multi-statement SQL string on ';' and strip whitespace.

    DuckDB's Python driver requires one statement per execute() call.
    """
    return [s.strip() for s in sql.split(";") if s.strip()]
