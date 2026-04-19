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
# RESURRECTOR_ALLOWED_ROOTS env var is unset we default to the user's home
# directory rather than allowing any path on disk — the dashboard ships in
# distribution packages and we never want a curl-able endpoint to expose
# arbitrary paths like /etc/passwd.
def _resolve_allowed_roots() -> list[Path]:
    raw = os.environ.get("RESURRECTOR_ALLOWED_ROOTS", "")
    parts = [r for r in raw.split(os.pathsep) if r]
    if not parts:
        return [Path.home().resolve()]
    return [Path(r).resolve() for r in parts]


_ALLOWED_ROOTS: list[Path] = _resolve_allowed_roots()


def _validate_path(path_str: str) -> Path:
    """Validate a path is safe to operate on.

    Rejects:
      - paths containing ``..`` traversal components, and
      - paths that resolve outside the configured allowed roots.

    Allowed roots default to the user's home directory; override via
    the ``RESURRECTOR_ALLOWED_ROOTS`` environment variable
    (os.pathsep-separated).
    """
    if ".." in Path(path_str).parts:
        raise HTTPException(400, "Path must not contain '..' components")
    resolved = Path(path_str).resolve()
    if not any(_is_within(resolved, root) for root in _ALLOWED_ROOTS):
        raise HTTPException(
            403,
            f"Path '{resolved}' is outside allowed roots. "
            f"Set RESURRECTOR_ALLOWED_ROOTS to allow more directories.",
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


@app.get("/api/bags/{bag_id}/topics/{topic_name:path}")
async def get_topic_data(
    bag_id: int,
    topic_name: str,
    start_sec: float | None = None,
    end_sec: float | None = None,
    limit: int = Query(default=1000, le=10000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    """Get topic data as JSON (paginated)."""
    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")

        from resurrector.core.bag_frame import BagFrame
        topic_name = "/" + topic_name if not topic_name.startswith("/") else topic_name

        bf = BagFrame(bag["path"])
        try:
            if start_sec is not None and end_sec is not None:
                view = bf.time_slice(start_sec, end_sec)[topic_name]
            else:
                view = bf[topic_name]
        except KeyError:
            raise HTTPException(404, f"Topic '{topic_name}' not found")

        df = view.to_polars()

        # Apply pagination
        total = df.height
        df = df.slice(offset, limit)

        return {
            "topic": topic_name,
            "total": total,
            "offset": offset,
            "limit": limit,
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
    """Serve a single frame as JPEG."""
    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")

        from resurrector.core.bag_frame import BagFrame
        topic_name = "/" + topic_name if not topic_name.startswith("/") else topic_name
        bf = BagFrame(bag["path"])
        try:
            view = bf[topic_name]
        except KeyError:
            raise HTTPException(404, f"Topic '{topic_name}' not found")

        if not view.is_image_topic:
            raise HTTPException(400, f"Topic '{topic_name}' is not an image topic")

        # Seek to the frame
        for i, (ts, arr) in enumerate(view.iter_images()):
            if i == frame_index:
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

        raise HTTPException(404, f"Frame {frame_index} not found")
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
