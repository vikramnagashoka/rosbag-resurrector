"""Tests for the DuckDB schema migration framework.

The bag index has to forward-migrate older databases without losing
data. Each migration runs once, in order, gated by a `_meta.schema_version`
counter. These tests cover:

- A v0.3.x-format database (with `bags.sha256` column) is migrated to
  the v0.4.0 schema (`bags.fingerprint` + nullable `bags.sha256_full`).
- Existing rows survive the rename — no data loss.
- Re-opening an already-current database is a no-op.
- A brand-new database lands at SCHEMA_VERSION on first init.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import duckdb
import pytest

from resurrector.ingest.indexer import BagIndex
from resurrector.ingest.migrations import MIGRATIONS, SCHEMA_VERSION, apply_pending


@pytest.fixture
def tmp_db():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d) / "index.db"


def _create_v0_3_x_schema(db_path: Path) -> None:
    """Build a database that looks like one written by v0.3.x.

    Critically: the bags table has the old `sha256` column name and
    no `_meta` table. Insert a sample row so we can verify it survives
    the rename.
    """
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE bags (
            id INTEGER PRIMARY KEY,
            path VARCHAR UNIQUE,
            format VARCHAR,
            sha256 VARCHAR,
            size_bytes BIGINT,
            duration_sec DOUBLE,
            start_time_ns BIGINT,
            end_time_ns BIGINT,
            message_count INTEGER,
            health_score INTEGER,
            mtime DOUBLE,
            indexed_at TIMESTAMP DEFAULT current_timestamp
        )
    """)
    conn.execute(
        "INSERT INTO bags (id, path, format, sha256, size_bytes, mtime) "
        "VALUES (1, '/tmp/old.mcap', 'mcap', 'abc123', 100, 1700000000.0)"
    )
    conn.close()


def test_brand_new_database_lands_at_current_version(tmp_db):
    """First-time BagIndex(...) on an empty file applies all migrations."""
    index = BagIndex(tmp_db)
    row = index.conn.execute("SELECT schema_version FROM _meta").fetchone()
    assert row is not None
    assert int(row[0]) == SCHEMA_VERSION
    index.close()


def test_v0_3_x_database_is_migrated(tmp_db):
    """A v0.3.x database with `bags.sha256` is renamed to `bags.fingerprint`."""
    _create_v0_3_x_schema(tmp_db)

    # Sanity: pre-migration state has the old column name.
    conn = duckdb.connect(str(tmp_db))
    cols_before = {row[1] for row in conn.execute("PRAGMA table_info('bags')").fetchall()}
    assert "sha256" in cols_before
    assert "fingerprint" not in cols_before
    conn.close()

    # Open via BagIndex — triggers _init_schema -> apply_pending.
    index = BagIndex(tmp_db)

    cols_after = {row[1] for row in index.conn.execute("PRAGMA table_info('bags')").fetchall()}
    assert "fingerprint" in cols_after
    assert "sha256_full" in cols_after
    assert "sha256" not in cols_after

    # The pre-existing row survived the rename.
    row = index.conn.execute(
        "SELECT path, fingerprint, sha256_full FROM bags WHERE id = 1"
    ).fetchone()
    assert row[0] == "/tmp/old.mcap"
    assert row[1] == "abc123"  # the value moved with the column
    assert row[2] is None  # newly added nullable column
    index.close()


def test_reopening_current_database_is_noop(tmp_db):
    """Opening an already-migrated database doesn't re-apply migrations."""
    BagIndex(tmp_db).close()

    # Re-open — apply_pending should report nothing was applied.
    conn = duckdb.connect(str(tmp_db))
    applied = apply_pending(conn)
    assert applied == []
    row = conn.execute("SELECT schema_version FROM _meta").fetchone()
    assert int(row[0]) == SCHEMA_VERSION
    conn.close()


def test_migrations_have_unique_versions():
    """Defensive: every migration in the list has a distinct version."""
    versions = [m.version for m in MIGRATIONS]
    assert len(versions) == len(set(versions))


def test_schema_version_matches_max_migration():
    """SCHEMA_VERSION should always be the largest migration version."""
    assert SCHEMA_VERSION == max(m.version for m in MIGRATIONS)
