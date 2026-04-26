"""FastAPI backend for the RosBag Resurrector web dashboard."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import asyncio
import json

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.responses import StreamingResponse

app = FastAPI(
    title="RosBag Resurrector",
    description="Interactive dashboard for rosbag analysis",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Configurable allowed roots for path operations (scan, export). When the
# RESURRECTOR_ALLOWED_ROOTS env var is unset we default to a SAFE set: the
# user's home directory, the OS temp dir, AND the cwd of the dashboard
# process. The cwd is included because if the user launched
# `resurrector dashboard` from inside their data folder (e.g. on WSL where
# bags often live under /mnt/c/...), they clearly trust that location and
# refusing to write there is just user-hostile.
#
# We never want a curl-able endpoint to expose arbitrary paths like
# /etc/passwd, so unset never means "allow everything."
#
# Read the env var on every call rather than at import time so tests (and
# users) can override it after the module is loaded.
def _resolve_allowed_roots() -> list[Path]:
    raw = os.environ.get("RESURRECTOR_ALLOWED_ROOTS", "")
    parts = [r for r in raw.split(os.pathsep) if r]
    if parts:
        return [Path(r).resolve() for r in parts]
    import tempfile
    defaults = {
        Path.home().resolve(),
        Path(tempfile.gettempdir()).resolve(),
        Path.cwd().resolve(),
    }
    return list(defaults)


def _resolved_export_paths() -> dict[str, str]:
    """Concrete absolute paths the dashboard can write to.

    Used by the frontend to construct outputs without hardcoding
    things like ``~/.resurrector`` (which the browser can't expand
    and the path validator then rejects).
    """
    import tempfile
    home = Path.home().resolve()
    return {
        "home": str(home),
        "tmp": str(Path(tempfile.gettempdir()).resolve()),
        "cwd": str(Path.cwd().resolve()),
        "resurrector_cache": str((home / ".resurrector").resolve()),
        "allowed_roots": [str(r) for r in _resolve_allowed_roots()],
    }


def _validate_path(path_str: str) -> Path:
    """Validate a path is safe to operate on.

    Rejects:
      - paths containing ``..`` traversal components, and
      - paths that resolve outside the configured allowed roots.

    Allowed roots default to {home, OS temp, dashboard cwd}; override
    via the ``RESURRECTOR_ALLOWED_ROOTS`` environment variable
    (os.pathsep-separated).
    """
    if ".." in Path(path_str).parts:
        raise HTTPException(400, "Path must not contain '..' components")
    resolved = Path(path_str).resolve()
    allowed_roots = _resolve_allowed_roots()
    if not any(_is_within(resolved, root) for root in allowed_roots):
        # Echo the allowed roots in the error message so users don't have
        # to reverse-engineer them from the env var name.
        roots_str = ", ".join(str(r) for r in allowed_roots)
        raise HTTPException(
            403,
            f"Path '{resolved}' is outside the allowed roots. "
            f"Currently allowed: {roots_str}. "
            f"Set RESURRECTOR_ALLOWED_ROOTS (os.pathsep-separated) to broaden.",
        )
    return resolved


def _is_within(child: Path, parent: Path) -> bool:
    """True if child resolves under parent (no symlink escape)."""
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _get_index():
    from resurrector.ingest.indexer import BagIndex
    db_path = os.environ.get("RESURRECTOR_DB_PATH")
    return BagIndex(db_path) if db_path else BagIndex()


# --- API Routes ---


@app.get("/api/system/paths")
async def get_system_paths() -> dict[str, Any]:
    """Return absolute paths the frontend can use to construct outputs.

    Lets the browser ask the server "where can I write?" instead of
    guessing with ``~/...`` strings the browser can't expand.
    """
    return _resolved_export_paths()


@app.post("/api/system/generate-demo-bag")
async def generate_demo_bag_api(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Generate a synthetic demo bag and index it.

    Mirrors the ``resurrector demo`` CLI but works for users running
    from source who don't have the ``resurrector`` shell command on
    PATH. Particularly useful from the Library and CompareRuns pages
    where users hit the "I have one bag, can't compare" wall.

    Body (all optional):
      {"name": "demo_2", "duration_sec": 5.0}

    Returns the indexed bag's id + path.
    """
    payload = payload or {}
    name = str(payload.get("name", f"demo_{int(__import__('time').time())}")).strip()
    duration_sec = float(payload.get("duration_sec", 5.0))
    if duration_sec < 0.5 or duration_sec > 60:
        raise HTTPException(400, "duration_sec must be between 0.5 and 60")
    if not name or any(c in name for c in r'\/:*?"<>|'):
        raise HTTPException(400, "name must be a simple filename without separators")

    output = (Path.home() / ".resurrector" / f"{name}.mcap").resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    # Inline-import the test fixture generator so we don't pull it
    # eagerly when nobody calls this endpoint.
    try:
        from tests.fixtures.generate_test_bags import BagConfig, generate_bag
    except ImportError as e:
        raise HTTPException(
            500,
            "Demo bag generator not available — make sure the project is "
            f"installed with `pip install -e \".[dev]\"`. ({e})",
        )

    try:
        generate_bag(output, BagConfig(duration_sec=duration_sec))
    except Exception as e:
        raise HTTPException(500, f"Failed to generate demo bag: {e}")

    # Index the new bag so it shows up in the Library + CompareRuns
    # without requiring a separate scan.
    from resurrector.ingest.scanner import scan_path
    from resurrector.ingest.parser import parse_bag
    from resurrector.core.bag_frame import BagFrame
    from resurrector.ingest.frame_index import build_frame_offsets, image_topics

    index = _get_index()
    try:
        scanned_files = scan_path(output)
        if not scanned_files:
            raise HTTPException(500, "Generated bag was not scannable")
        scanned = scanned_files[0]
        parser = parse_bag(output)
        metadata = parser.get_metadata()
        bag_id = index.upsert_bag(scanned, metadata)
        bf = BagFrame(output)
        index.update_health_score(bag_id, bf.health_report().score)

        # Pre-build frame offsets so the dashboard's image viewer + frame
        # endpoint are O(1) when the user explores this bag.
        img_topics = image_topics(output)
        if img_topics:
            build_frame_offsets(index, bag_id, output, topics=img_topics)
    finally:
        index.close()

    return {
        "bag_id": bag_id,
        "path": str(output),
        "duration_sec": duration_sec,
    }


@app.get("/api/bags")
async def list_bags(
    search: str | None = None,
    after: str | None = None,
    before: str | None = None,
    has_topic: str | None = None,
    min_health: int | None = None,
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
) -> list[dict[str, Any]]:
    """List all indexed bags with optional filtering."""
    index = _get_index()
    try:
        if search:
            return index.search(search)
        return index.list_bags(
            after=after,
            before=before,
            has_topic=has_topic,
            min_health=min_health,
            limit=limit,
            offset=offset,
        )
    finally:
        index.close()


@app.get("/api/bags/{bag_id}")
async def get_bag(bag_id: int) -> dict[str, Any]:
    """Get bag metadata and topic list."""
    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")
        return bag
    finally:
        index.close()


@app.get("/api/bags/{bag_id}/health")
async def get_bag_health(bag_id: int) -> dict[str, Any]:
    """Get health report for a bag."""
    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")

        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(bag["path"])
        report = bf.health_report()

        return {
            "score": report.score,
            "issues": [
                {
                    "check": i.check_name,
                    "severity": i.severity.value,
                    "message": i.message,
                    "topic": i.topic,
                    "start_time": i.start_time_sec,
                    "end_time": i.end_time_sec,
                    "details": i.details,
                }
                for i in report.issues
            ],
            "recommendations": report.recommendations,
            "topic_scores": {
                k: {"score": v.score, "issue_count": len(v.issues)}
                for k, v in report.topic_scores.items()
            },
        }
    finally:
        index.close()


_FRAME_SUBROUTE = __import__("re").compile(r"^(?P<topic>.+)/frame/(?P<idx>\d+)$")
_THUMB_SUBROUTE = "/thumbnail"


@app.get("/api/bags/{bag_id}/topics/{topic_name:path}")
async def get_topic_data(
    bag_id: int,
    topic_name: str,
    start_sec: float | None = None,
    end_sec: float | None = None,
    limit: int = Query(default=1000, le=10000),
    offset: int = Query(default=0, ge=0),
    max_points: int | None = Query(
        default=None, ge=3, le=10000,
        description="If set, downsample to ~this many points via LTTB. "
                    "Overrides pagination — returns the full time range "
                    "visually summarized for plotting.",
    ),
    width: int | None = Query(default=None, description="Image resize width (sub-routes only)"),
) -> Any:
    """Get topic data as JSON.

    Two modes:
      1. Paginated (default): returns up to ``limit`` raw rows starting
         at ``offset``. For table views and exact inspection.
      2. Downsampled (``max_points`` set): returns ~``max_points`` rows
         LTTB-downsampled across the full [start_sec, end_sec] window.
         For plotting. Cached in memory keyed on (bag, topic, window,
         max_points, bag mtime) so panning the plot hits RAM.

    The ``{topic_name:path}`` wildcard is greedy, so it also matches
    sub-routes like ``.../frame/N`` and ``.../thumbnail``. We dispatch
    those inline here so FastAPI doesn't need two separate handlers to
    disambiguate.
    """
    # Dispatch sub-routes before running the full topic-data query.
    m = _FRAME_SUBROUTE.match(topic_name)
    if m:
        return await get_frame_image(
            bag_id=bag_id,
            topic_name=m.group("topic"),
            frame_index=int(m.group("idx")),
            width=width,
        )
    if topic_name.endswith(_THUMB_SUBROUTE):
        return await get_topic_thumbnail(
            bag_id=bag_id,
            topic_name=topic_name.removesuffix(_THUMB_SUBROUTE),
        )

    from resurrector.dashboard.cache import (
        get_topic_cache, set_topic_cache, topic_cache_key,
    )

    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")

        from resurrector.core.bag_frame import BagFrame
        topic_name = "/" + topic_name if not topic_name.startswith("/") else topic_name

        # Downsampled path: consult cache first.
        if max_points is not None:
            cache_key = topic_cache_key(
                bag_id, topic_name, start_sec, end_sec, max_points, bag["path"],
            )
            cached = get_topic_cache(cache_key)
            if cached is not None:
                return cached

        bf = BagFrame(bag["path"])
        try:
            if start_sec is not None and end_sec is not None:
                view = bf.time_slice(start_sec, end_sec)[topic_name]
            else:
                view = bf[topic_name]
        except KeyError:
            raise HTTPException(404, f"Topic '{topic_name}' not found")

        df = view.to_polars()
        total = df.height

        if max_points is not None:
            from resurrector.core.downsample import downsample_dataframe
            df = downsample_dataframe(df, max_points=max_points)
            response = {
                "topic": topic_name,
                "total": total,
                "downsampled": True,
                "max_points": max_points,
                "columns": df.columns,
                "data": df.to_dicts(),
            }
            set_topic_cache(cache_key, response)
            return response
        else:
            df = df.slice(offset, limit)
            return {
                "topic": topic_name,
                "total": total,
                "offset": offset,
                "limit": limit,
                "downsampled": False,
                "columns": df.columns,
                "data": df.to_dicts(),
            }
    finally:
        index.close()


@app.get("/api/bags/{bag_id}/sync")
async def get_synced_data(
    bag_id: int,
    topics: str = Query(description="Comma-separated topic names"),
    method: str = Query(default="nearest"),
    tolerance_ms: float = Query(default=50.0),
    limit: int = Query(default=1000, le=10000),
) -> dict[str, Any]:
    """Get synchronized multi-topic data."""
    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")

        topic_list = [t.strip() for t in topics.split(",")]

        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(bag["path"])
        df = bf.sync(topic_list, method=method, tolerance_ms=tolerance_ms)

        total = df.height
        df = df.head(limit)

        return {
            "topics": topic_list,
            "method": method,
            "total": total,
            "columns": df.columns,
            "data": df.to_dicts(),
        }
    finally:
        index.close()


@app.post("/api/bags/{bag_id}/export")
async def export_bag(
    bag_id: int,
    topics: str | None = Query(default=None, description="Comma-separated topics"),
    format: str = Query(default="parquet"),
    sync: bool = Query(default=False),
    output_dir: str = Query(default="./export", description="Output directory"),
) -> dict[str, str]:
    """Trigger an export job."""
    validated_output = _validate_path(output_dir)
    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")

        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(bag["path"])
        topic_list = [t.strip() for t in topics.split(",")] if topics else None
        output_path = bf.export(
            topics=topic_list,
            format=format,
            output=str(validated_output),
            sync=sync,
        )
        return {"status": "completed", "output_path": str(output_path)}
    finally:
        index.close()


@app.get("/api/search")
async def search_bags(q: str = Query(description="Search query")) -> list[dict[str, Any]]:
    """Full-text search across bags."""
    index = _get_index()
    try:
        return index.search(q)
    finally:
        index.close()


@app.post("/api/scan")
async def trigger_scan(
    path: str = Query(description="Directory path to scan"),
    stream: bool = Query(default=False, description="Stream progress via SSE"),
) -> Any:
    """Trigger a directory scan. Set stream=true for Server-Sent Events progress."""
    scan_path_obj = _validate_path(path)
    if not scan_path_obj.exists():
        raise HTTPException(400, f"Path does not exist: {path}")

    if stream:
        return StreamingResponse(
            _scan_stream(scan_path_obj),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # Non-streaming fallback
    return await _scan_blocking(scan_path_obj)


async def _scan_blocking(scan_path_obj: Path) -> dict[str, Any]:
    """Blocking scan (original behavior)."""
    from resurrector.ingest.scanner import scan_path
    from resurrector.ingest.parser import parse_bag
    from resurrector.core.bag_frame import BagFrame

    files = scan_path(scan_path_obj)
    index = _get_index()
    indexed = 0
    errors = []

    try:
        for scanned in files:
            try:
                parser = parse_bag(scanned.path)
                metadata = parser.get_metadata()
                bag_id = index.upsert_bag(scanned, metadata)

                bf = BagFrame(scanned.path)
                report = bf.health_report()
                index.update_health_score(bag_id, report.score)
                indexed += 1
            except Exception as e:
                errors.append({"file": str(scanned.path), "error": str(e)})
    finally:
        index.close()

    return {
        "scanned": len(files),
        "indexed": indexed,
        "errors": errors,
    }


async def _scan_stream(scan_path_obj: Path):
    """Stream scan progress as Server-Sent Events."""
    from resurrector.ingest.scanner import scan_path
    from resurrector.ingest.parser import parse_bag
    from resurrector.core.bag_frame import BagFrame

    files = scan_path(scan_path_obj)
    total = len(files)
    index = _get_index()
    indexed = 0
    errors = []

    yield f"data: {json.dumps({'event': 'start', 'total': total})}\n\n"

    try:
        for i, scanned in enumerate(files):
            try:
                parser = parse_bag(scanned.path)
                metadata = parser.get_metadata()
                bag_id = index.upsert_bag(scanned, metadata)

                bf = BagFrame(scanned.path)
                report = bf.health_report()
                index.update_health_score(bag_id, report.score)
                indexed += 1

                yield f"data: {json.dumps({'event': 'indexed', 'file': scanned.path.name, 'health': report.score, 'progress': i + 1, 'total': total})}\n\n"
            except Exception as e:
                errors.append({"file": str(scanned.path), "error": str(e)})
                yield f"data: {json.dumps({'event': 'error', 'file': scanned.path.name, 'error': str(e), 'progress': i + 1, 'total': total})}\n\n"

            # Yield control to event loop so SSE messages flush
            await asyncio.sleep(0)
    finally:
        index.close()

    yield f"data: {json.dumps({'event': 'complete', 'scanned': total, 'indexed': indexed, 'errors': errors})}\n\n"


@app.get("/api/bags/{bag_id}/timeline")
async def get_timeline(bag_id: int) -> dict[str, Any]:
    """Get timeline data for visualization (topic spans and message density)."""
    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")

        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(bag["path"])
        meta = bf.metadata

        timeline_data = {
            "start_time_ns": meta.start_time_ns,
            "end_time_ns": meta.end_time_ns,
            "duration_sec": meta.duration_sec,
            "topics": [],
        }

        for topic in meta.topics:
            timeline_data["topics"].append({
                "name": topic.name,
                "type": topic.message_type,
                "count": topic.message_count,
                "frequency_hz": topic.frequency_hz,
            })

        return timeline_data
    finally:
        index.close()


# --- Frame / Vision endpoints ---

@app.get("/api/bags/{bag_id}/topics/{topic_name:path}/frame/{frame_index}")
async def get_frame_image(
    bag_id: int,
    topic_name: str,
    frame_index: int,
    width: int | None = Query(default=None, description="Resize width"),
) -> Any:
    """Serve a single frame as JPEG.

    Uses a DuckDB-cached (frame_index -> timestamp_ns) lookup so the
    second request for the same bag/topic is O(1) instead of
    re-scanning the MCAP. Build is serialized per (bag, topic) to avoid
    thundering-herd on semantic-search thumbnail bursts.
    """
    from resurrector.dashboard.cache import get_frame_build_lock
    from resurrector.ingest.frame_index import (
        get_frame_timestamp, read_single_frame,
    )

    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")

        topic_name = "/" + topic_name if not topic_name.startswith("/") else topic_name

        # Confirm topic exists and is image-typed before we try to build offsets.
        topic_info = next(
            (t for t in bag.get("topics", []) if t["name"] == topic_name),
            None,
        )
        if topic_info is None:
            raise HTTPException(404, f"Topic '{topic_name}' not found")
        from resurrector.ingest.frame_index import IMAGE_TOPIC_TYPES
        if topic_info["message_type"] not in IMAGE_TOPIC_TYPES:
            raise HTTPException(
                400, f"Topic '{topic_name}' is not an image topic "
                     f"(type: {topic_info['message_type']})",
            )

        # Build offsets under a per-(bag, topic) lock so concurrent
        # requests for the same topic deduplicate the scan cost.
        lock = get_frame_build_lock(bag_id, topic_name)
        async with lock:
            ts = get_frame_timestamp(
                index, bag_id, bag["path"], topic_name, frame_index,
            )
        if ts is None:
            raise HTTPException(
                404,
                f"Frame {frame_index} not found on '{topic_name}' "
                f"(bag has {index.count_frames(bag_id, topic_name)} frames)",
            )

        arr, _ = read_single_frame(bag["path"], topic_name, ts)
        if arr is None:
            raise HTTPException(
                500, f"Could not decode frame {frame_index} on '{topic_name}'",
            )

        from PIL import Image as PILImage
        import io
        img = PILImage.fromarray(arr)
        if width:
            ratio = width / img.width
            img = img.resize((width, int(img.height * ratio)))
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        from starlette.responses import Response
        return Response(content=buf.getvalue(), media_type="image/jpeg")
    finally:
        index.close()


@app.get("/api/bags/{bag_id}/topics/{topic_name:path}/thumbnail")
async def get_topic_thumbnail(bag_id: int, topic_name: str) -> Any:
    """Serve a thumbnail of the first frame from an image topic."""
    return await get_frame_image(bag_id, topic_name, frame_index=0, width=320)


@app.get("/api/search/frames")
async def search_frames_api(
    q: str = Query(description="Natural language search query"),
    top_k: int = Query(default=20, le=100),
    bag_id: int | None = Query(default=None),
    min_similarity: float = Query(default=0.15),
    clips: bool = Query(default=False),
    clip_duration: float = Query(default=5.0),
) -> dict[str, Any]:
    """Semantic search across frame embeddings."""
    index = _get_index()
    try:
        from resurrector.core.vision import FrameSearchEngine

        engine = FrameSearchEngine(index)

        if clips:
            results = engine.search_temporal(
                q, clip_duration_sec=clip_duration,
                top_k=top_k, bag_id=bag_id, min_similarity=min_similarity,
            )
            return {
                "query": q,
                "mode": "clips",
                "results": [
                    {
                        "bag_id": r.bag_id,
                        "bag_path": r.bag_path,
                        "topic": r.topic,
                        "start_timestamp_sec": round(r.start_sec, 2),
                        "end_timestamp_sec": round(r.end_sec, 2),
                        "duration_sec": round(r.duration_sec, 2),
                        "avg_similarity": round(r.avg_similarity, 4),
                        "peak_similarity": round(r.peak_similarity, 4),
                        "frame_count": r.frame_count,
                    }
                    for r in results
                ],
            }
        else:
            results = engine.search(
                q, top_k=top_k, bag_id=bag_id, min_similarity=min_similarity,
            )
            return {
                "query": q,
                "mode": "frames",
                "results": [
                    {
                        "bag_id": r.bag_id,
                        "bag_path": r.bag_path,
                        "topic": r.topic,
                        "timestamp_sec": round(r.timestamp_sec, 2),
                        "frame_index": r.frame_index,
                        "similarity": round(r.similarity, 4),
                        "thumbnail_url": f"/api/bags/{r.bag_id}/topics/{r.topic.lstrip('/')}/frame/{r.frame_index}?width=320",
                    }
                    for r in results
                ],
            }
    finally:
        index.close()


@app.get("/api/bags/{bag_id}/frame-index-status")
async def get_frame_index_status(bag_id: int) -> dict[str, Any]:
    """Check if frames have been indexed for a bag."""
    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")

        count = index.count_frame_embeddings(bag_id)
        # Get indexed topics
        rows = index.conn.execute(
            "SELECT DISTINCT topic FROM frame_embeddings WHERE bag_id = ?", [bag_id]
        ).fetchall()
        topics = [r[0] for r in rows]

        return {
            "bag_id": bag_id,
            "indexed": count > 0,
            "frame_count": count,
            "topics_indexed": topics,
        }
    finally:
        index.close()


# ============================================================================
# Annotations — persistent user notes on plot timestamps (v0.3.0)
# ============================================================================


@app.get("/api/bags/{bag_id}/annotations")
async def list_annotations_api(
    bag_id: int,
    topic: str | None = Query(default=None),
) -> dict[str, Any]:
    """List annotations for a bag, optionally scoped to a topic.

    Topic-scoped queries include bag-global annotations (topic IS NULL)
    so users see their general notes alongside per-topic notes.
    """
    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")
        return {"annotations": index.list_annotations(bag_id, topic=topic)}
    finally:
        index.close()


@app.post("/api/bags/{bag_id}/annotations")
async def create_annotation_api(
    bag_id: int,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Create an annotation. Body: {timestamp_ns, text, topic?}"""
    if "timestamp_ns" not in payload or "text" not in payload:
        raise HTTPException(400, "Body must include 'timestamp_ns' and 'text'")
    text = str(payload["text"]).strip()
    if not text:
        raise HTTPException(400, "Annotation text cannot be empty")
    try:
        ts = int(payload["timestamp_ns"])
    except (TypeError, ValueError):
        raise HTTPException(400, "'timestamp_ns' must be an integer")

    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")
        aid = index.add_annotation(
            bag_id, ts, text, topic=payload.get("topic"),
        )
        return {"id": aid, "bag_id": bag_id, "timestamp_ns": ts, "text": text,
                "topic": payload.get("topic")}
    finally:
        index.close()


@app.patch("/api/annotations/{annotation_id}")
async def update_annotation_api(
    annotation_id: int, payload: dict[str, Any],
) -> dict[str, Any]:
    """Update an annotation's text."""
    text = str(payload.get("text", "")).strip()
    if not text:
        raise HTTPException(400, "Annotation text cannot be empty")
    index = _get_index()
    try:
        if not index.update_annotation(annotation_id, text):
            raise HTTPException(404, "Annotation not found")
        return {"id": annotation_id, "text": text}
    finally:
        index.close()


@app.delete("/api/annotations/{annotation_id}")
async def delete_annotation_api(annotation_id: int) -> dict[str, Any]:
    """Delete an annotation."""
    index = _get_index()
    try:
        if not index.delete_annotation(annotation_id):
            raise HTTPException(404, "Annotation not found")
        return {"deleted": annotation_id}
    finally:
        index.close()


# ============================================================================
# Datasets — full CRUD for versioned dataset collections (v0.3.0)
# ============================================================================


def _get_dataset_manager():
    from resurrector.core.dataset import DatasetManager
    db_path = os.environ.get("RESURRECTOR_DB_PATH")
    return DatasetManager(db_path=Path(db_path)) if db_path else DatasetManager()


@app.get("/api/datasets")
async def list_datasets_api() -> dict[str, Any]:
    """List every dataset with its version count."""
    mgr = _get_dataset_manager()
    try:
        items = mgr.list_datasets()
        return {"datasets": items}
    finally:
        mgr.close()


@app.post("/api/datasets")
async def create_dataset_api(payload: dict[str, Any]) -> dict[str, Any]:
    """Create a dataset. Body: {name, description?}"""
    name = str(payload.get("name", "")).strip()
    if not name:
        raise HTTPException(400, "'name' is required")
    mgr = _get_dataset_manager()
    try:
        # duckdb raises ConstraintException on unique-name collision; any
        # integrity / constraint error here is a 409 to the caller.
        try:
            ds_id = mgr.create(name, description=payload.get("description", ""))
        except Exception as e:
            msg = str(e)
            if "Constraint" in msg or "UNIQUE" in msg or "duplicate" in msg.lower():
                raise HTTPException(409, f"Dataset '{name}' already exists")
            raise
        return {"name": name, "description": payload.get("description", ""), "id": ds_id}
    finally:
        mgr.close()


@app.get("/api/datasets/{name}")
async def get_dataset_api(name: str) -> dict[str, Any]:
    """Get a dataset plus its versions."""
    mgr = _get_dataset_manager()
    try:
        ds = mgr.get_dataset(name)
        if ds is None:
            raise HTTPException(404, f"Dataset '{name}' not found")
        return ds
    finally:
        mgr.close()


@app.delete("/api/datasets/{name}")
async def delete_dataset_api(name: str) -> dict[str, Any]:
    """Delete a dataset and all its versions."""
    mgr = _get_dataset_manager()
    try:
        if not mgr.delete_dataset(name):
            raise HTTPException(404, f"Dataset '{name}' not found")
        return {"deleted": name}
    finally:
        mgr.close()


@app.post("/api/datasets/{name}/versions")
async def create_dataset_version_api(
    name: str, payload: dict[str, Any],
) -> dict[str, Any]:
    """Create a version of a dataset.

    Body:
      {
        "version": "1.0",
        "bag_refs": [{"path": "...", "topics": [...], "start_time": "...", "end_time": "..."}, ...],
        "sync_config": {"method": "nearest", "tolerance_ms": 25},
        "export_format": "parquet",
        "downsample_hz": 50,
        "metadata": {"description": "...", "license": "MIT", ...}
      }
    """
    from resurrector.core.dataset import BagRef, SyncConfig, DatasetMetadata

    version = str(payload.get("version", "")).strip()
    if not version:
        raise HTTPException(400, "'version' is required")
    if "bag_refs" not in payload:
        raise HTTPException(400, "'bag_refs' is required")

    try:
        bag_refs = [BagRef(**b) for b in payload["bag_refs"]]
    except TypeError as e:
        raise HTTPException(400, f"Invalid bag_refs: {e}")

    sync_cfg = None
    if payload.get("sync_config"):
        sync_cfg = SyncConfig(**payload["sync_config"])
    metadata = DatasetMetadata(**(payload.get("metadata") or {}))

    mgr = _get_dataset_manager()
    try:
        try:
            mgr.create_version(
                dataset_name=name,
                version=version,
                bag_refs=bag_refs,
                sync_config=sync_cfg,
                export_format=payload.get("export_format", "parquet"),
                downsample_hz=payload.get("downsample_hz"),
                metadata=metadata,
            )
        except KeyError as e:
            raise HTTPException(404, str(e))
        except ValueError as e:
            raise HTTPException(409, str(e))
        return {"name": name, "version": version}
    finally:
        mgr.close()


@app.post("/api/datasets/{name}/versions/{version}/export")
async def export_dataset_version_api(
    name: str, version: str, payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Export a dataset version to disk. Body: {"output_dir": "..."}"""
    payload = payload or {}
    output_dir = payload.get("output_dir", "./datasets")
    # Validate the output path against allowed roots.
    validated = _validate_path(str(Path(output_dir).resolve().parent))  # dir may not exist yet
    mgr = _get_dataset_manager()
    try:
        try:
            path = mgr.export_version(name, version, output_dir=output_dir)
        except ValueError as e:
            raise HTTPException(404, str(e))
        except Exception as e:
            # Transactional cleanup: user sees the error; partial files may exist
            # but live under a dataset-named subdir that we don't remove to avoid
            # clobbering unrelated data.
            raise HTTPException(
                500, f"Export failed: {e}. Partial output may exist at {output_dir}.",
            )
        return {"name": name, "version": version, "output": str(path)}
    finally:
        mgr.close()


@app.delete("/api/datasets/{name}/versions/{version}")
async def delete_dataset_version_api(
    name: str, version: str,
) -> dict[str, Any]:
    """Delete a specific version of a dataset."""
    mgr = _get_dataset_manager()
    try:
        if not mgr.delete_version(name, version):
            raise HTTPException(404, f"Dataset '{name}' version '{version}' not found")
        return {"deleted": {"name": name, "version": version}}
    finally:
        mgr.close()


# ============================================================================
# Bridge subprocess lifecycle (v0.3.0) — spawn, proxy, stop.
# ============================================================================


_BRIDGE_DEFAULT_PORT = 9090


def _get_bridge_state():
    """Singleton dict tracking the bridge subprocess."""
    if not hasattr(app.state, "bridge"):
        app.state.bridge = {"process": None, "port": None, "mode": None}
    return app.state.bridge


@app.post("/api/bridge/start")
async def start_bridge_api(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Start a bridge subprocess.

    Body:
      {
        "mode": "playback" | "live",
        "bag_path": "..."          # required for playback
        "topics": ["/imu/data"]    # required for live
        "speed": 1.0               # optional for playback
        "port": 9090               # optional
      }
    """
    import subprocess
    import socket
    payload = payload or {}
    mode = payload.get("mode")
    if mode not in {"playback", "live"}:
        raise HTTPException(400, "mode must be 'playback' or 'live'")

    port = int(payload.get("port", _BRIDGE_DEFAULT_PORT))

    state = _get_bridge_state()
    if state["process"] and state["process"].poll() is None:
        raise HTTPException(
            409,
            f"Bridge already running in mode '{state['mode']}' on port {state['port']}. "
            f"Stop it first.",
        )

    # Pre-flight: is the port already bound by something else?
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("127.0.0.1", port))
        except OSError as e:
            raise HTTPException(
                409, f"Port {port} is already in use: {e}",
            )

    import sys
    cmd = [sys.executable, "-m", "resurrector.cli.main", "bridge", mode]
    if mode == "playback":
        bag_path = payload.get("bag_path")
        if not bag_path:
            raise HTTPException(400, "'bag_path' is required for playback mode")
        _validate_path(bag_path)
        cmd.append(str(bag_path))
        if "speed" in payload:
            cmd.extend(["--speed", str(payload["speed"])])
    else:
        topics = payload.get("topics") or []
        if not topics:
            raise HTTPException(400, "'topics' is required for live mode")
        for t in topics:
            cmd.extend(["--topic", str(t)])

    cmd.extend(["--port", str(port)])
    cmd.append("--no-browser")  # don't open a viewer; the dashboard IS the viewer

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except OSError as e:
        raise HTTPException(500, f"Failed to start bridge: {e}")

    # Wait briefly for the port to accept connections.
    import time
    deadline = time.time() + 10
    ready = False
    while time.time() < deadline:
        if proc.poll() is not None:
            stderr = (proc.stderr.read() if proc.stderr else b"").decode(errors="replace")
            raise HTTPException(
                500, f"Bridge exited during startup: {stderr[:500]}",
            )
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                ready = True
                break
        except OSError:
            await asyncio.sleep(0.2)

    if not ready:
        proc.terminate()
        raise HTTPException(504, f"Bridge did not start listening on port {port} within 10s")

    state["process"] = proc
    state["port"] = port
    state["mode"] = mode
    return {"mode": mode, "port": port, "pid": proc.pid}


@app.post("/api/bridge/stop")
async def stop_bridge_api() -> dict[str, Any]:
    """Stop the running bridge subprocess, if any."""
    state = _get_bridge_state()
    proc = state["process"]
    if proc is None or proc.poll() is not None:
        state["process"] = None
        return {"stopped": False, "reason": "no running bridge"}
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except Exception:
        proc.kill()
        proc.wait(timeout=5)
    state["process"] = None
    state["port"] = None
    state["mode"] = None
    return {"stopped": True}


@app.get("/api/bridge/status")
async def bridge_status_api() -> dict[str, Any]:
    """Report bridge subprocess state. Polled by the Bridge page."""
    state = _get_bridge_state()
    proc = state["process"]
    if proc is None:
        return {"running": False}
    rc = proc.poll()
    if rc is not None:
        # Process died; clean up state so future polls don't report a ghost.
        state["process"] = None
        state["port"] = None
        state["mode"] = None
        return {"running": False, "exited": True, "return_code": rc}
    return {
        "running": True, "mode": state["mode"], "port": state["port"], "pid": proc.pid,
    }


@app.api_route(
    "/api/bridge/proxy/{rest_path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
)
async def bridge_proxy(rest_path: str, request: Any) -> Any:
    """Forward requests to the running bridge's REST API.

    Frontend calls `POST /api/bridge/proxy/api/playback/play` and we
    relay it to `http://127.0.0.1:9090/api/playback/play` so the user
    never needs to know about the bridge's real port.
    """
    import httpx
    from starlette.responses import Response

    state = _get_bridge_state()
    proc = state["process"]
    if proc is None or proc.poll() is not None:
        raise HTTPException(503, "Bridge not running — start it first.")

    port = state["port"]
    url = f"http://127.0.0.1:{port}/{rest_path}"
    method = request.method
    body = await request.body()
    params = dict(request.query_params)

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.request(
                method, url, content=body, params=params,
                headers={"Accept": "application/json"},
            )
        except httpx.ConnectError as e:
            raise HTTPException(502, f"Cannot reach bridge at {url}: {e}")

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        media_type=resp.headers.get("content-type"),
    )


@app.on_event("shutdown")
async def _cleanup_bridge_on_shutdown() -> None:
    """Kill the bridge subprocess when the dashboard shuts down.

    Without this, Ctrl+C on the dashboard leaves the bridge orphaned
    on port 9090 and a subsequent dashboard restart can't reclaim it.
    """
    state = _get_bridge_state()
    proc = state["process"]
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=3)
    except Exception:
        proc.kill()
        proc.wait(timeout=3)


# ============================================================================
# v0.4.0 power features — density, trim, transform preview, cross-bag overlay
# ============================================================================


# Density results are cached per (bag_id, mtime) since they're computed
# from a full bag scan. Reuses the same in-memory LRU as topic data.
_DENSITY_CACHE: dict[tuple, dict[str, Any]] = {}
_DENSITY_ORDER: list[tuple] = []
_DENSITY_MAX = 32


def _density_cache_get(key: tuple) -> dict[str, Any] | None:
    v = _DENSITY_CACHE.get(key)
    if v is not None:
        try:
            _DENSITY_ORDER.remove(key)
        except ValueError:
            pass
        _DENSITY_ORDER.append(key)
    return v


def _density_cache_set(key: tuple, value: dict[str, Any]) -> None:
    _DENSITY_CACHE[key] = value
    try:
        _DENSITY_ORDER.remove(key)
    except ValueError:
        pass
    _DENSITY_ORDER.append(key)
    while len(_DENSITY_ORDER) > _DENSITY_MAX:
        evict = _DENSITY_ORDER.pop(0)
        _DENSITY_CACHE.pop(evict, None)


@app.get("/api/bags/{bag_id}/density")
async def get_bag_density_api(
    bag_id: int,
    bins: int = Query(default=200, ge=10, le=1000),
    topic: str | None = Query(default=None, description="Single topic; defaults to all"),
) -> dict[str, Any]:
    """Per-topic message-count histograms for the timeline ribbon.

    Cached per (bag_id, bins, topic, bag mtime) so repeated dashboard
    visits hit RAM. Bag-file edits invalidate via the mtime component.
    """
    from resurrector.ingest.density import compute_density

    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")

        try:
            mtime_ns = Path(bag["path"]).stat().st_mtime_ns
        except OSError:
            mtime_ns = -1
        cache_key = (bag_id, bins, topic, mtime_ns)
        cached = _density_cache_get(cache_key)
        if cached is not None:
            return cached

        topics = [topic] if topic else None
        try:
            result = compute_density(bag["path"], topics=topics, bins=bins)
        except FileNotFoundError as e:
            raise HTTPException(404, str(e))
        response = {"bag_id": bag_id, "bins": bins, "density": result}
        _density_cache_set(cache_key, response)
        return response
    finally:
        index.close()


@app.post("/api/bags/{bag_id}/trim")
async def trim_bag_api(bag_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    """Trim a time range from a bag and export to MCAP / Parquet / CSV / etc.

    Body:
      {
        "start_sec": 1.0,
        "end_sec": 3.0,
        "topics": ["/imu/data", "/joint_states"],
        "format": "mcap" | "parquet" | "csv" | "hdf5" | "numpy" | "zarr" | "mp4",
        "output_path": "/path/to/output"
      }
    """
    from resurrector.core.trim import trim_to_format

    required = {"start_sec", "end_sec", "topics", "format", "output_path"}
    missing = required - set(payload)
    if missing:
        raise HTTPException(400, f"Missing required fields: {sorted(missing)}")

    try:
        start_sec = float(payload["start_sec"])
        end_sec = float(payload["end_sec"])
    except (TypeError, ValueError):
        raise HTTPException(400, "start_sec and end_sec must be numbers")

    topics_in = payload["topics"]
    if not isinstance(topics_in, list) or not all(isinstance(t, str) for t in topics_in):
        raise HTTPException(400, "'topics' must be a list of strings")
    if not topics_in:
        raise HTTPException(400, "'topics' must contain at least one topic")

    format_str = str(payload["format"])
    output_path = Path(str(payload["output_path"])).resolve()

    # Validate output directory is within allowed roots so dashboard
    # users can't write to /etc.
    _validate_path(str(output_path.parent if output_path.suffix else output_path))

    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")

        try:
            result_path = trim_to_format(
                source_path=bag["path"],
                output_path=output_path,
                start_sec=start_sec,
                end_sec=end_sec,
                topics=topics_in,
                format=format_str,
            )
        except ValueError as e:
            raise HTTPException(400, str(e))
        except FileNotFoundError as e:
            raise HTTPException(404, str(e))
        return {
            "bag_id": bag_id,
            "format": format_str,
            "start_sec": start_sec,
            "end_sec": end_sec,
            "output": str(result_path),
        }
    finally:
        index.close()


@app.post("/api/transforms/preview")
async def preview_transform_api(payload: dict[str, Any]) -> dict[str, Any]:
    """Apply a transform to one topic column and return downsampled values.

    Body shape — two modes:

      Common menu:
      {"bag_id": 1, "topic": "/imu/data", "column": "linear_acceleration.x",
       "op": "derivative", "params": {}, "max_points": 1000}

      Expression:
      {"bag_id": 1, "topic": "/imu/data", "expression": "pl.col(\\"x\\")*2",
       "max_points": 1000}
    """
    from resurrector.core.bag_frame import BagFrame
    from resurrector.core.downsample import downsample_dataframe
    from resurrector.core.transforms import (
        apply_polars_expression,
        apply_transform,
    )
    import polars as pl

    bag_id = payload.get("bag_id")
    topic = payload.get("topic")
    if bag_id is None or not topic:
        raise HTTPException(400, "'bag_id' and 'topic' are required")
    max_points = int(payload.get("max_points", 1000))
    if max_points < 3:
        raise HTTPException(400, "max_points must be >= 3")

    index = _get_index()
    try:
        bag = index.get_bag(int(bag_id))
        if bag is None:
            raise HTTPException(404, "Bag not found")
        bf = BagFrame(bag["path"])
        try:
            view = bf[str(topic)]
        except KeyError:
            raise HTTPException(404, f"Topic '{topic}' not found in bag")
        df = view.to_polars()

        # Mode 1: common menu op.
        if "op" in payload:
            op = str(payload["op"])
            column = payload.get("column")
            if not column:
                raise HTTPException(400, "'column' is required for menu transforms")
            params = payload.get("params") or {}
            try:
                series = apply_transform(df, str(column), op, **params)
            except ValueError as e:
                raise HTTPException(400, str(e))
            result_df = pl.DataFrame({"timestamp_ns": df["timestamp_ns"], series.name: series})
        elif "expression" in payload:
            expr = str(payload["expression"])
            try:
                series = apply_polars_expression(df, expr, alias="result")
            except ValueError as e:
                raise HTTPException(400, str(e))
            result_df = pl.DataFrame({"timestamp_ns": df["timestamp_ns"], "result": series})
        else:
            raise HTTPException(400, "Provide either 'op' (menu) or 'expression'")

        if result_df.height > max_points:
            result_df = downsample_dataframe(result_df, max_points=max_points)

        return {
            "topic": topic,
            "label": result_df.columns[1],
            "total": df.height,
            "downsampled": True if df.height > max_points else False,
            "data": result_df.to_dicts(),
        }
    finally:
        index.close()


@app.post("/api/compare/topics")
async def compare_topics_api(payload: dict[str, Any]) -> dict[str, Any]:
    """Cross-bag overlay: same topic on N bags, aligned by relative time.

    Body:
      {
        "bag_ids": [1, 2, 3],
        "topic": "/imu/data",
        "offsets_sec": [0.0, 1.5, 0.0],   // optional, defaults to zeros
        "labels": ["a", "b", "c"],         // optional, defaults to bag stem
        "max_points_per_bag": 2000          // optional
      }

    Returns rows in long format with bag_label + relative_t_sec columns,
    ready for one Plotly trace per bag.
    """
    from resurrector.core.cross_bag import align_bags_by_offset

    bag_ids = payload.get("bag_ids")
    topic = payload.get("topic")
    if not isinstance(bag_ids, list) or not bag_ids:
        raise HTTPException(400, "'bag_ids' must be a non-empty list of bag IDs")
    if not topic:
        raise HTTPException(400, "'topic' is required")
    offsets_sec = payload.get("offsets_sec")
    labels = payload.get("labels")
    max_points_per_bag = int(payload.get("max_points_per_bag", 2000))

    index = _get_index()
    try:
        paths: list[str] = []
        resolved_labels: list[str] = []
        for bid in bag_ids:
            bag = index.get_bag(int(bid))
            if bag is None:
                raise HTTPException(404, f"Bag {bid} not found")
            paths.append(bag["path"])
            resolved_labels.append(Path(bag["path"]).stem)
        if labels:
            resolved_labels = list(labels)

        try:
            df = align_bags_by_offset(
                paths,
                topic=str(topic),
                offsets_sec=offsets_sec,
                labels=resolved_labels,
                max_points_per_bag=max_points_per_bag,
            )
        except ValueError as e:
            raise HTTPException(400, str(e))

        return {
            "topic": topic,
            "bag_ids": bag_ids,
            "labels": resolved_labels,
            "columns": df.columns,
            "data": df.to_dicts(),
        }
    finally:
        index.close()


# Serve static frontend files
_static_dir = Path(__file__).parent / "static"
if _static_dir.exists():
    app.mount("/", StaticFiles(directory=str(_static_dir), html=True), name="static")
else:
    @app.get("/")
    async def root():
        return {
            "message": "RosBag Resurrector API",
            "docs": "/docs",
            "note": "Frontend not built. Run from the dashboard/app directory: npm run build",
        }
