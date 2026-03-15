"""FastAPI backend for the RosBag Resurrector web dashboard."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(
    title="RosBag Resurrector",
    description="Interactive dashboard for rosbag analysis",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
) -> dict[str, str]:
    """Trigger an export job."""
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
) -> dict[str, Any]:
    """Trigger a directory scan."""
    from resurrector.ingest.scanner import scan_path
    from resurrector.ingest.parser import parse_bag
    from resurrector.core.bag_frame import BagFrame

    scan_path_obj = Path(path)
    if not scan_path_obj.exists():
        raise HTTPException(400, f"Path does not exist: {path}")

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
