"""Build and manage a DuckDB index of all scanned bag files.

The index persists at ~/.resurrector/index.db and enables fast
SQL-based searching and filtering across large bag collections.

Thread safety: a single ``threading.Lock`` serializes all access to the
underlying DuckDB connection. DuckDB allows a connection to be used
from multiple threads as long as calls don't interleave; the lock
enforces that contract so the bridge, dashboard, and scanner can all
safely share one BagIndex instance.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import duckdb

from resurrector.ingest.migrations import apply_pending
from resurrector.ingest.parser import BagMetadata, TopicInfo
from resurrector.ingest.scanner import ScannedFile

DEFAULT_INDEX_PATH = Path.home() / ".resurrector" / "index.db"


class BagIndex:
    """Persistent DuckDB index of bag files and their topics.

    All public methods are thread-safe; they acquire a single lock
    around the shared connection.
    """

    def __init__(self, db_path: str | Path | None = None):
        self.db_path = Path(db_path) if db_path else DEFAULT_INDEX_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))
        self._lock = threading.Lock()
        self._init_schema()

    def _init_schema(self):
        """Create tables if they don't exist, then apply any pending migrations.

        Order matters: CREATE the v0.3.x base schema first (so an
        upgrading user's existing tables are referenced unchanged),
        then run apply_pending() to ALTER them forward. Brand-new
        databases hit the same code path — they get the v0.3.x base
        and then the migrations promote them to current.
        """
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS bags (
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
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS topics (
                id INTEGER PRIMARY KEY,
                bag_id INTEGER,
                name VARCHAR,
                message_type VARCHAR,
                message_count INTEGER,
                frequency_hz DOUBLE,
                health_score INTEGER,
                UNIQUE(bag_id, name)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY,
                bag_id INTEGER,
                key VARCHAR,
                value VARCHAR,
                UNIQUE(bag_id, key, value)
            )
        """)
        # Create sequences for auto-incrementing IDs
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS bag_id_seq START 1
        """)
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS topic_id_seq START 1
        """)
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS tag_id_seq START 1
        """)
        # Frame embeddings for semantic image search
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS frame_embeddings (
                id INTEGER PRIMARY KEY,
                bag_id INTEGER NOT NULL,
                topic VARCHAR NOT NULL,
                timestamp_ns BIGINT NOT NULL,
                frame_index INTEGER NOT NULL,
                embedding DOUBLE[512] NOT NULL,
                indexed_at TIMESTAMP DEFAULT current_timestamp,
                UNIQUE(bag_id, topic, timestamp_ns)
            )
        """)
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS frame_embedding_id_seq START 1
        """)

        # Frame offset cache — enables O(1) seek to any image frame.
        # Built during `resurrector scan` for image topics, and lazily on
        # first dashboard/search access for bags scanned on older versions.
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS frame_offsets (
                bag_id INTEGER NOT NULL,
                topic VARCHAR NOT NULL,
                frame_index INTEGER NOT NULL,
                timestamp_ns BIGINT NOT NULL,
                PRIMARY KEY (bag_id, topic, frame_index)
            )
        """)

        # Persistent annotations on timestamps in topic data.
        # Rendered in the dashboard Plotly explorer as pinned notes.
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS annotations (
                id INTEGER PRIMARY KEY,
                bag_id INTEGER NOT NULL,
                topic VARCHAR,
                timestamp_ns BIGINT NOT NULL,
                text VARCHAR NOT NULL,
                created_at TIMESTAMP DEFAULT current_timestamp,
                updated_at TIMESTAMP DEFAULT current_timestamp
            )
        """)
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS annotation_id_seq START 1
        """)

        # Apply any schema migrations (e.g. sha256 -> fingerprint rename
        # for users upgrading from v0.3.x).
        apply_pending(self.conn)

    def upsert_bag(self, scanned: ScannedFile, metadata: BagMetadata) -> int:
        """Insert or update a bag in the index. Returns the bag ID."""
        with self._lock:
            existing = self.conn.execute(
                "SELECT id, fingerprint, mtime FROM bags WHERE path = ?",
                [str(scanned.path)],
            ).fetchone()

            if existing and existing[1] == scanned.fingerprint and existing[2] == scanned.mtime:
                return existing[0]

            if existing:
                bag_id = existing[0]
                self.conn.execute("""
                    UPDATE bags SET
                        format = ?, fingerprint = ?, sha256_full = ?, size_bytes = ?,
                        duration_sec = ?, start_time_ns = ?, end_time_ns = ?,
                        message_count = ?, mtime = ?, indexed_at = current_timestamp
                    WHERE id = ?
                """, [
                    metadata.format, scanned.fingerprint, scanned.sha256_full,
                    scanned.size_bytes,
                    metadata.duration_sec, metadata.start_time_ns, metadata.end_time_ns,
                    metadata.message_count, scanned.mtime, bag_id,
                ])
                self.conn.execute("DELETE FROM topics WHERE bag_id = ?", [bag_id])
            else:
                bag_id = self.conn.execute("SELECT nextval('bag_id_seq')").fetchone()[0]
                self.conn.execute("""
                    INSERT INTO bags (id, path, format, fingerprint, sha256_full,
                        size_bytes, duration_sec, start_time_ns, end_time_ns,
                        message_count, mtime)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    bag_id, str(scanned.path), metadata.format,
                    scanned.fingerprint, scanned.sha256_full,
                    scanned.size_bytes, metadata.duration_sec, metadata.start_time_ns,
                    metadata.end_time_ns, metadata.message_count, scanned.mtime,
                ])

            for topic in metadata.topics:
                tid = self.conn.execute("SELECT nextval('topic_id_seq')").fetchone()[0]
                self.conn.execute("""
                    INSERT INTO topics (id, bag_id, name, message_type, message_count, frequency_hz)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, [tid, bag_id, topic.name, topic.message_type, topic.message_count, topic.frequency_hz])

            return bag_id

    def update_health_score(self, bag_id: int, score: int):
        """Update the health score for a bag."""
        with self._lock:
            self.conn.execute("UPDATE bags SET health_score = ? WHERE id = ?", [score, bag_id])

    def update_topic_health(self, bag_id: int, topic_name: str, score: int):
        """Update the health score for a specific topic."""
        with self._lock:
            self.conn.execute(
                "UPDATE topics SET health_score = ? WHERE bag_id = ? AND name = ?",
                [score, bag_id, topic_name],
            )

    def add_tag(self, bag_id: int, key: str, value: str = ""):
        """Add a tag to a bag."""
        with self._lock:
            tid = self.conn.execute("SELECT nextval('tag_id_seq')").fetchone()[0]
            self.conn.execute("""
                INSERT INTO tags (id, bag_id, key, value)
                VALUES (?, ?, ?, ?)
                ON CONFLICT DO NOTHING
            """, [tid, bag_id, key, value])

    def remove_tag(self, bag_id: int, key: str, value: str | None = None):
        """Remove a tag from a bag."""
        with self._lock:
            if value is not None:
                self.conn.execute(
                    "DELETE FROM tags WHERE bag_id = ? AND key = ? AND value = ?",
                    [bag_id, key, value],
                )
            else:
                self.conn.execute(
                    "DELETE FROM tags WHERE bag_id = ? AND key = ?",
                    [bag_id, key],
                )

    def get_bag(self, bag_id: int) -> dict[str, Any] | None:
        """Get a bag by ID."""
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM bags WHERE id = ?", [bag_id]
            ).fetchone()
            if row is None:
                return None
            cols = [desc[0] for desc in self.conn.description]
            bag = dict(zip(cols, row))
            bag["topics"] = self._get_topics_locked(bag_id)
            bag["tags"] = self._get_tags_locked(bag_id)
            return bag

    def get_bag_by_path(self, path: str | Path) -> dict[str, Any] | None:
        """Get a bag by file path."""
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM bags WHERE path = ?", [str(path)]
            ).fetchone()
            if row is None:
                return None
            cols = [desc[0] for desc in self.conn.description]
            bag = dict(zip(cols, row))
            bag["topics"] = self._get_topics_locked(bag["id"])
            bag["tags"] = self._get_tags_locked(bag["id"])
            return bag

    def _get_topics_locked(self, bag_id: int) -> list[dict[str, Any]]:
        """Return topics for a bag. Caller must already hold the lock."""
        rows = self.conn.execute(
            "SELECT * FROM topics WHERE bag_id = ? ORDER BY name", [bag_id]
        ).fetchall()
        cols = [desc[0] for desc in self.conn.description]
        return [dict(zip(cols, row)) for row in rows]

    def _get_tags_locked(self, bag_id: int) -> list[dict[str, str]]:
        """Return tags for a bag. Caller must already hold the lock."""
        rows = self.conn.execute(
            "SELECT key, value FROM tags WHERE bag_id = ? ORDER BY key", [bag_id]
        ).fetchall()
        return [{"key": r[0], "value": r[1]} for r in rows]

    # Backwards-compat wrappers for any external callers of the old names.
    def _get_topics(self, bag_id: int) -> list[dict[str, Any]]:
        with self._lock:
            return self._get_topics_locked(bag_id)

    def _get_tags(self, bag_id: int) -> list[dict[str, str]]:
        with self._lock:
            return self._get_tags_locked(bag_id)

    def list_bags(
        self,
        after: str | None = None,
        before: str | None = None,
        has_topic: str | None = None,
        min_health: int | None = None,
        tag_filter: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """List bags with optional filtering."""
        conditions = []
        params: list[Any] = []

        if after:
            conditions.append("b.start_time_ns >= ?")
            params.append(_parse_date_to_ns(after))
        if before:
            conditions.append("b.start_time_ns <= ?")
            params.append(_parse_date_to_ns(before))
        if has_topic:
            conditions.append("EXISTS (SELECT 1 FROM topics t WHERE t.bag_id = b.id AND t.name = ?)")
            params.append(has_topic)
        if min_health is not None:
            conditions.append("(b.health_score IS NULL OR b.health_score >= ?)")
            params.append(min_health)
        if tag_filter:
            key, _, value = tag_filter.partition(":")
            if value:
                conditions.append("EXISTS (SELECT 1 FROM tags tg WHERE tg.bag_id = b.id AND tg.key = ? AND tg.value = ?)")
                params.extend([key, value])
            else:
                conditions.append("EXISTS (SELECT 1 FROM tags tg WHERE tg.bag_id = b.id AND tg.key = ?)")
                params.append(key)

        # SAFETY: every entry in `conditions` is a string literal built
        # from the parameterized branches above; no user input is ever
        # interpolated into the SQL string itself. All values flow
        # through the `params` list. Treat `where` as trusted-by-construction.
        where = " AND ".join(conditions) if conditions else "1=1"
        params.extend([limit, offset])

        with self._lock:
            rows = self.conn.execute(f"""
                SELECT * FROM bags b
                WHERE {where}
                ORDER BY b.start_time_ns DESC
                LIMIT ? OFFSET ?
            """, params).fetchall()

            cols = [desc[0] for desc in self.conn.description]
            results = []
            for row in rows:
                bag = dict(zip(cols, row))
                bag["topics"] = self._get_topics_locked(bag["id"])
                bag["tags"] = self._get_tags_locked(bag["id"])
                results.append(bag)
            return results

    def search(self, query: str) -> list[dict[str, Any]]:
        """Search bags using a simple query language.

        Supported syntax:
            - topic:/camera/rgb — has this topic
            - health:>80 — health score above 80
            - tag:task:pick_and_place — has this tag
            - after:2025-01-01 — recorded after date
            - before:2025-06-01 — recorded before date
            - Free text — matches against path
        """
        conditions = []
        params: list[Any] = []

        for token in query.split():
            if token.startswith("topic:") or token.startswith("has_topic:"):
                topic = token.split(":", 1)[1]
                conditions.append("EXISTS (SELECT 1 FROM topics t WHERE t.bag_id = b.id AND t.name = ?)")
                params.append(topic)
            elif token.startswith("health:"):
                expr = token[7:]
                if expr.startswith(">"):
                    conditions.append("b.health_score > ?")
                    params.append(int(expr[1:]))
                elif expr.startswith("<"):
                    conditions.append("b.health_score < ?")
                    params.append(int(expr[1:]))
                elif expr.startswith(">="):
                    conditions.append("b.health_score >= ?")
                    params.append(int(expr[2:]))
                else:
                    conditions.append("b.health_score = ?")
                    params.append(int(expr))
            elif token.startswith("tag:"):
                tag = token[4:]
                key, _, value = tag.partition(":")
                if value:
                    conditions.append("EXISTS (SELECT 1 FROM tags tg WHERE tg.bag_id = b.id AND tg.key = ? AND tg.value = ?)")
                    params.extend([key, value])
                else:
                    conditions.append("EXISTS (SELECT 1 FROM tags tg WHERE tg.bag_id = b.id AND tg.key = ?)")
                    params.append(key)
            elif token.startswith("after:"):
                conditions.append("b.start_time_ns >= ?")
                params.append(_parse_date_to_ns(token[6:]))
            elif token.startswith("before:"):
                conditions.append("b.start_time_ns <= ?")
                params.append(_parse_date_to_ns(token[7:]))
            else:
                conditions.append("b.path LIKE ?")
                params.append(f"%{token}%")

        # SAFETY: see list_bags — `conditions` is constructed from string
        # literals only; user input flows through `params`.
        where = " AND ".join(conditions) if conditions else "1=1"
        with self._lock:
            rows = self.conn.execute(f"""
                SELECT * FROM bags b WHERE {where} ORDER BY b.start_time_ns DESC LIMIT 100
            """, params).fetchall()

            cols = [desc[0] for desc in self.conn.description]
            results = []
            for row in rows:
                bag = dict(zip(cols, row))
                bag["topics"] = self._get_topics_locked(bag["id"])
                bag["tags"] = self._get_tags_locked(bag["id"])
                results.append(bag)
            return results

    def count(self) -> int:
        """Return total number of indexed bags."""
        with self._lock:
            return self.conn.execute("SELECT COUNT(*) FROM bags").fetchone()[0]

    def validate_paths(self) -> list[dict[str, Any]]:
        """Check all indexed bags for stale paths (file moved/deleted).

        Returns list of bags with missing files. Marks them in the index
        with health_score = -1 to indicate unavailable.
        """
        stale: list[dict[str, Any]] = []
        with self._lock:
            rows = self.conn.execute("SELECT id, path FROM bags").fetchall()
            for bag_id, path_str in rows:
                if not Path(path_str).exists():
                    stale.append({"id": bag_id, "path": path_str})
                    self.conn.execute(
                        "UPDATE bags SET health_score = -1 WHERE id = ?", [bag_id]
                    )
        return stale

    def remove_stale(self) -> int:
        """Remove all bags whose files no longer exist on disk. Returns count removed."""
        stale = self.validate_paths()
        for entry in stale:
            self.remove_bag(entry["id"])
        return len(stale)

    def remove_bag(self, bag_id: int):
        """Remove a bag and its associated topics/tags/embeddings from the index."""
        with self._lock:
            self.conn.execute("DELETE FROM frame_embeddings WHERE bag_id = ?", [bag_id])
            self.conn.execute("DELETE FROM topics WHERE bag_id = ?", [bag_id])
            self.conn.execute("DELETE FROM tags WHERE bag_id = ?", [bag_id])
            self.conn.execute("DELETE FROM bags WHERE id = ?", [bag_id])

    # --- Frame embedding methods ---

    def upsert_frame_embeddings(
        self,
        bag_id: int,
        topic: str,
        timestamps_ns: list[int],
        frame_indices: list[int],
        embeddings: list[list[float]],
    ) -> int:
        """Bulk insert frame embeddings. Returns count inserted."""
        with self._lock:
            count = 0
            for ts, idx, emb in zip(timestamps_ns, frame_indices, embeddings):
                eid = self.conn.execute("SELECT nextval('frame_embedding_id_seq')").fetchone()[0]
                self.conn.execute("""
                    INSERT INTO frame_embeddings (id, bag_id, topic, timestamp_ns, frame_index, embedding)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT DO NOTHING
                """, [eid, bag_id, topic, ts, idx, emb])
                count += 1
            return count

    def has_frame_embeddings(self, bag_id: int, topic: str | None = None) -> bool:
        """Check if a bag already has frame embeddings indexed."""
        with self._lock:
            if topic:
                row = self.conn.execute(
                    "SELECT COUNT(*) FROM frame_embeddings WHERE bag_id = ? AND topic = ?",
                    [bag_id, topic],
                ).fetchone()
            else:
                row = self.conn.execute(
                    "SELECT COUNT(*) FROM frame_embeddings WHERE bag_id = ?",
                    [bag_id],
                ).fetchone()
            return row[0] > 0

    def count_frame_embeddings(self, bag_id: int | None = None) -> int:
        """Count frame embeddings, optionally filtered to a bag."""
        with self._lock:
            if bag_id is not None:
                return self.conn.execute(
                    "SELECT COUNT(*) FROM frame_embeddings WHERE bag_id = ?", [bag_id]
                ).fetchone()[0]
            return self.conn.execute("SELECT COUNT(*) FROM frame_embeddings").fetchone()[0]

    def search_embeddings(
        self,
        query_embedding: list[float],
        top_k: int = 20,
        bag_id: int | None = None,
        min_similarity: float = 0.15,
    ) -> list[dict[str, Any]]:
        """Cosine similarity search against frame embeddings."""
        # Params must match the textual order of `?` placeholders in the SQL
        # below. The SELECT's similarity expression comes first textually,
        # then WHERE's similarity expression, then the threshold, then the
        # optional bag_id filter, then LIMIT.
        params: list[Any] = [
            query_embedding,    # SELECT  list_cosine_similarity(fe.embedding, ?::DOUBLE[512])
            query_embedding,    # WHERE   list_cosine_similarity(fe.embedding, ?::DOUBLE[512])
            min_similarity,     # WHERE   ... >= ?
        ]
        conditions = ["list_cosine_similarity(fe.embedding, ?::DOUBLE[512]) >= ?"]

        if bag_id is not None:
            conditions.append("fe.bag_id = ?")
            params.append(bag_id)

        params.append(top_k)    # LIMIT  ?
        where = " AND ".join(conditions)

        with self._lock:
            rows = self.conn.execute(f"""
                SELECT
                    fe.bag_id, fe.topic, fe.timestamp_ns, fe.frame_index,
                    list_cosine_similarity(fe.embedding, ?::DOUBLE[512]) AS similarity,
                    b.path AS bag_path
                FROM frame_embeddings fe
                JOIN bags b ON b.id = fe.bag_id
                WHERE {where}
                ORDER BY similarity DESC
                LIMIT ?
            """, params).fetchall()

            cols = [desc[0] for desc in self.conn.description]
            return [dict(zip(cols, row)) for row in rows]

    def delete_frame_embeddings(self, bag_id: int, topic: str | None = None):
        """Delete embeddings for a bag (used for re-indexing)."""
        with self._lock:
            if topic:
                self.conn.execute(
                    "DELETE FROM frame_embeddings WHERE bag_id = ? AND topic = ?",
                    [bag_id, topic],
                )
            else:
                self.conn.execute(
                    "DELETE FROM frame_embeddings WHERE bag_id = ?", [bag_id]
                )

    # ----- Frame offset cache (for fast image seek) -----

    def has_frame_offsets(self, bag_id: int, topic: str) -> bool:
        """True if frame offsets are already cached for this (bag, topic)."""
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) FROM frame_offsets WHERE bag_id = ? AND topic = ?",
                [bag_id, topic],
            ).fetchone()
            return row[0] > 0

    def insert_frame_offsets(
        self, bag_id: int, topic: str, offsets: list[tuple[int, int]],
    ) -> None:
        """Bulk-insert (frame_index, timestamp_ns) pairs for a topic.

        Idempotent — existing rows are left untouched.
        """
        if not offsets:
            return
        with self._lock:
            self.conn.executemany(
                "INSERT OR IGNORE INTO frame_offsets "
                "(bag_id, topic, frame_index, timestamp_ns) VALUES (?, ?, ?, ?)",
                [(bag_id, topic, idx, ts) for (idx, ts) in offsets],
            )

    def get_frame_timestamp(
        self, bag_id: int, topic: str, frame_index: int,
    ) -> int | None:
        """Return the timestamp_ns for a specific frame, or None if not cached."""
        with self._lock:
            row = self.conn.execute(
                "SELECT timestamp_ns FROM frame_offsets "
                "WHERE bag_id = ? AND topic = ? AND frame_index = ?",
                [bag_id, topic, frame_index],
            ).fetchone()
            return row[0] if row else None

    def count_frames(self, bag_id: int, topic: str) -> int:
        """Return how many frames are cached for this (bag, topic)."""
        with self._lock:
            row = self.conn.execute(
                "SELECT COUNT(*) FROM frame_offsets WHERE bag_id = ? AND topic = ?",
                [bag_id, topic],
            ).fetchone()
            return row[0] if row else 0

    def clear_frame_offsets(self, bag_id: int, topic: str | None = None) -> None:
        """Drop cached frame offsets for a bag (optionally scoped to one topic)."""
        with self._lock:
            if topic:
                self.conn.execute(
                    "DELETE FROM frame_offsets WHERE bag_id = ? AND topic = ?",
                    [bag_id, topic],
                )
            else:
                self.conn.execute(
                    "DELETE FROM frame_offsets WHERE bag_id = ?", [bag_id],
                )

    # ----- Annotations (persistent user notes on plot timestamps) -----

    def add_annotation(
        self, bag_id: int, timestamp_ns: int, text: str, topic: str | None = None,
    ) -> int:
        """Create an annotation. Returns the new annotation id."""
        with self._lock:
            aid = self.conn.execute(
                "SELECT nextval('annotation_id_seq')"
            ).fetchone()[0]
            self.conn.execute(
                "INSERT INTO annotations (id, bag_id, topic, timestamp_ns, text) "
                "VALUES (?, ?, ?, ?, ?)",
                [aid, bag_id, topic, timestamp_ns, text],
            )
            return aid

    def list_annotations(
        self, bag_id: int, topic: str | None = None,
    ) -> list[dict[str, Any]]:
        """Return annotations for a bag, optionally filtered by topic."""
        with self._lock:
            if topic is None:
                rows = self.conn.execute(
                    "SELECT id, bag_id, topic, timestamp_ns, text, "
                    "created_at, updated_at FROM annotations "
                    "WHERE bag_id = ? ORDER BY timestamp_ns",
                    [bag_id],
                ).fetchall()
            else:
                rows = self.conn.execute(
                    "SELECT id, bag_id, topic, timestamp_ns, text, "
                    "created_at, updated_at FROM annotations "
                    "WHERE bag_id = ? AND (topic = ? OR topic IS NULL) "
                    "ORDER BY timestamp_ns",
                    [bag_id, topic],
                ).fetchall()
            return [
                {
                    "id": r[0], "bag_id": r[1], "topic": r[2],
                    "timestamp_ns": r[3], "text": r[4],
                    "created_at": str(r[5]) if r[5] else None,
                    "updated_at": str(r[6]) if r[6] else None,
                }
                for r in rows
            ]

    def update_annotation(self, annotation_id: int, text: str) -> bool:
        """Update an annotation's text. Returns True if it existed."""
        with self._lock:
            result = self.conn.execute(
                "UPDATE annotations SET text = ?, updated_at = current_timestamp "
                "WHERE id = ?",
                [text, annotation_id],
            )
            # duckdb returns a cursor; check affected rows via subsequent SELECT
            exists = self.conn.execute(
                "SELECT 1 FROM annotations WHERE id = ?", [annotation_id],
            ).fetchone()
            return exists is not None

    def delete_annotation(self, annotation_id: int) -> bool:
        """Delete an annotation. Returns True if it existed before deletion."""
        with self._lock:
            existed = self.conn.execute(
                "SELECT 1 FROM annotations WHERE id = ?", [annotation_id],
            ).fetchone()
            self.conn.execute(
                "DELETE FROM annotations WHERE id = ?", [annotation_id],
            )
            return existed is not None

    def close(self):
        """Close the database connection."""
        with self._lock:
            self.conn.close()


def _parse_date_to_ns(date_str: str) -> int:
    """Parse a date string to nanoseconds since epoch."""
    from datetime import datetime, timezone
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m"):
        try:
            dt = datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1e9)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {date_str}")
