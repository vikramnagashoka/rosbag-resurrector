"""FastAPI backend for the RosBag Resurrector web dashboard."""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Any

import asyncio
import json

logger = logging.getLogger("resurrector.dashboard.api")

import polars as pl
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.requests import Request
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


@app.get("/api/system/capabilities")
async def get_system_capabilities() -> dict[str, Any]:
    """Report the runtime state of every optional capability.

    The dashboard's Search / Bridge / Library / Export surfaces each
    pre-check the relevant capability on mount and render an install
    banner (with copy-to-clipboard command) when one is missing,
    instead of leaving the user to discover the gap by hitting a 500.

    Response shape: ``{<name>: {available: bool, install_command, description, name}}``.
    See ``resurrector.core.capabilities`` for the source of truth.
    """
    from resurrector.core.capabilities import get_capabilities
    return {name: c.to_dict() for name, c in get_capabilities().items()}


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

    # Inline-import so the heavy mcap.writer dep doesn't load eagerly
    # for users who never call this endpoint.
    try:
        from resurrector.demo.sample_bag import BagConfig, generate_bag
    except ImportError as e:
        raise HTTPException(
            500,
            f"Demo bag generator not available: {e}",
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
    """List indexed bags. The Library page's primary data source.

    Filters compose with AND. ``search`` (when set) overrides the others
    and runs the full DSL described in ``GET /api/search``. Pagination
    is via ``limit`` (max 200) and ``offset``.

    Returns a list of bag records; each has ``id``, ``path``,
    ``health_score``, ``duration_sec``, ``recorded_at``, topic count,
    and tags.
    """
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
    """Return one indexed bag's metadata + topic list. 404 if no such bag.

    Used by the Explorer and Library pages to populate the bag's
    sidebar (topics, message counts, health, tags). Cheap — reads from
    the index, doesn't open the bag file.
    """
    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")
        return bag
    finally:
        index.close()


def _aggregate_checks(results: list[Any]) -> list[dict[str, Any]]:
    """Collapse per-topic-per-check HealthResults into one row per check
    dimension. Score = worst (min) across topics so the failing dimension
    is visible; issue_count = total across topics. Order is preserved by
    first appearance (matches the configured check order)."""
    agg: dict[str, dict[str, Any]] = {}
    for r in results:
        e = agg.get(r.check_name)
        if e is None:
            e = {"check": r.check_name, "score": r.score,
                 "passed": r.passed, "issue_count": 0}
            agg[r.check_name] = e
        else:
            e["score"] = min(e["score"], r.score)
            e["passed"] = e["passed"] and r.passed
        e["issue_count"] += len(r.issues)
    return list(agg.values())


@app.get("/api/bags/{bag_id}/health")
async def get_bag_health(bag_id: int) -> dict[str, Any]:
    """Run the full health report for one bag — score, issues, recommendations.

    Streams the bag once and returns the same data the CLI's
    ``resurrector health`` would print. Backs the dashboard's Health
    page. Not cached server-side; the BagFrame in-instance cache makes
    the second call within a session free.
    """
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
            # Per-check breakdown (the 5 health dimensions). report.results
            # is per-topic-per-check, so aggregate by check name: worst score
            # across topics (the informative one — surfaces the failing
            # dimension) + total issues for that dimension.
            "checks": _aggregate_checks(report.results),
            # Cheap aggregate counts so the UI doesn't recompute them.
            "summary": {
                "errors": len(report.errors),
                "warnings": len(report.warnings),
                "topics_checked": len(report.topic_scores),
            },
        }
    finally:
        index.close()


@app.get("/api/diff")
async def diff_bags_api(
    bag_a_id: int = Query(description="Baseline bag id"),
    bag_b_id: int = Query(description="Candidate bag id"),
) -> dict[str, Any]:
    """Semantic diff between two indexed bags (A = baseline, B = candidate).

    Backs a future Compare-page "what changed" panel. Metadata-level —
    reads summaries, not messages — so it's cheap even on large bags.
    Returns the same shape as ``BagDiff.to_dict()``.
    """
    index = _get_index()
    try:
        bag_a = index.get_bag(bag_a_id)
        bag_b = index.get_bag(bag_b_id)
        if bag_a is None:
            raise HTTPException(404, f"Bag {bag_a_id} not found")
        if bag_b is None:
            raise HTTPException(404, f"Bag {bag_b_id} not found")

        from resurrector.core.bag_diff import diff_bags
        result = diff_bags(bag_a["path"], bag_b["path"])
        return result.to_dict()
    finally:
        index.close()


@app.get("/api/bags/{bag_id}/explain")
async def explain_time_range_api(
    bag_id: int,
    start_sec: float = Query(description="Window start, seconds from bag start"),
    end_sec: float = Query(description="Window end, seconds from bag start"),
    use_llm: bool = Query(default=True, description="Use the LLM narrative if available"),
) -> dict[str, Any]:
    """Grounded 'explain this time range' for the Scene/Plot brush selection.

    Gathers real per-topic activity + overlapping health findings for the
    window and returns a narrative grounded in that evidence (LLM when the
    [copilot] extra + ANTHROPIC_API_KEY are present, otherwise a deterministic
    rule-based summary). The raw evidence is included for citation display.
    """
    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")
        from resurrector.core.copilot import explain_time_range
        result = explain_time_range(
            bag["path"], start_sec, end_sec, use_llm=use_llm,
        )
        return result.to_dict()
    finally:
        index.close()


@app.get("/api/bags/{bag_id}/report")
async def incident_report_api(
    bag_id: int,
    start_sec: float = Query(description="Window start, seconds from bag start"),
    end_sec: float = Query(description="Window end, seconds from bag start"),
    fmt: str = Query(default="html", description="html | md"),
    use_llm: bool = Query(default=True, description="Use the LLM narrative if available"),
) -> Any:
    """Generate a shareable incident report for the brushed window.

    Returns the self-contained report as a downloadable attachment (HTML by
    default, or Markdown). Backs the Explain panel's 'Download report' button.
    """
    from starlette.responses import Response
    if fmt not in ("html", "md"):
        raise HTTPException(400, "fmt must be 'html' or 'md'")
    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")
        from resurrector.core.report import generate_incident_report
        result = generate_incident_report(
            bag["path"], start_sec, end_sec, fmt=fmt, use_llm=use_llm,
        )
        media = "text/html" if fmt == "html" else "text/markdown"
        stem = Path(bag["path"]).stem
        fname = f"{stem}_incident_{int(start_sec)}-{int(end_sec)}s.{fmt}"
        return Response(
            content=result.content,
            media_type=media,
            headers={"Content-Disposition": f'attachment; filename="{fname}"'},
        )
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

        # Use the index-recorded message_count as `total` instead of
        # materializing the topic just to count rows. This is the
        # streaming hot path — never call view.to_polars() here.
        total = view.message_count

        if max_points is not None:
            # Stream-aggregate to ~max_points via bucketed min/max. Memory
            # bounded by num_buckets, NOT by topic size. Replaces the
            # v0.3.x `view.to_polars(); downsample_dataframe(...)` path.
            from resurrector.core.streaming import (
                stream_bucketed_minmax_from_view,
            )
            num_buckets = max(1, max_points // 2)
            df = stream_bucketed_minmax_from_view(
                view,
                num_buckets=num_buckets,
                bag_start_ns=int(bag["start_time_ns"] or 0),
                bag_end_ns=int(bag["end_time_ns"] or 0),
            )
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
            # Paginated mode — collect the requested page via iter_chunks
            # so we never materialize beyond the page size.
            collected: list[pl.DataFrame] = []
            rows_collected = 0
            rows_skipped = 0
            for chunk in view.iter_chunks(chunk_size=max(1000, limit)):
                if chunk.height == 0:
                    continue
                if rows_skipped + chunk.height <= offset:
                    rows_skipped += chunk.height
                    continue
                # The page intersects this chunk — slice into it.
                start_in_chunk = max(0, offset - rows_skipped)
                take = min(chunk.height - start_in_chunk, limit - rows_collected)
                collected.append(chunk.slice(start_in_chunk, take))
                rows_collected += take
                rows_skipped += chunk.height
                if rows_collected >= limit:
                    break
            df = (
                pl.concat(collected, how="diagonal_relaxed")
                if collected else pl.DataFrame({"timestamp_ns": []})
            )
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
    """Time-align multiple topics and return up to ``limit`` synced rows.

    Wraps :meth:`BagFrame.sync` over the given comma-separated topics.
    Backs the Sync tab on the Explorer page. ``method`` is one of
    ``nearest`` / ``interpolate`` / ``sample_and_hold``; ``tolerance_ms``
    is the maximum match window. Returns ``total`` (full sync size) +
    the first ``limit`` rows for inspection.
    """
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


@app.get("/api/export-presets")
async def list_export_presets() -> list[dict[str, Any]]:
    """Return the registered export presets for the dashboard's preset picker.

    Each entry has the same shape as ``ExportPreset`` plus a flag
    ``available`` indicating whether the required pip extras are installed
    (e.g. ``zarr`` / ``rlds`` need ``[all-exports]``).

    Used by ExportDialog to populate the preset dropdown and disable
    presets whose required extras are missing on this install.
    """
    from resurrector.core.export import list_presets

    def _extra_available(extra: str) -> bool:
        # Only one extra currently ships in this codebase: all-exports
        # (zarr, tensorflow-datasets). Detect via package import.
        if extra == "all-exports":
            try:
                import zarr  # noqa: F401
                return True
            except ImportError:
                return False
        return True  # Unknown extras default to available

    out: list[dict[str, Any]] = []
    for p in list_presets():
        out.append({
            "name": p.name,
            "format": p.format,
            "sync": p.sync,
            "sync_method": p.sync_method,
            "downsample_hz": p.downsample_hz,
            "topic_filter": p.topic_filter,
            "description": p.description,
            "extras_required": list(p.extras_required),
            "available": all(_extra_available(e) for e in p.extras_required),
        })
    return out


@app.post("/api/bags/{bag_id}/export")
async def export_bag(
    bag_id: int,
    topics: str | None = Query(default=None, description="Comma-separated topics"),
    format: str | None = Query(default=None, description="Output format; overrides preset"),
    sync: bool | None = Query(default=None, description="Time-align before export; overrides preset"),
    output_dir: str = Query(default="./export", description="Output directory"),
    preset: str | None = Query(default=None, description="Named export preset (lerobot, rlds, etc.)"),
    downsample_hz: float | None = Query(default=None, description="Downsample rate; overrides preset"),
) -> dict[str, str]:
    """Run a synchronous bag export. Returns the output path on completion.

    The dashboard's ExportDialog hits this. Streams chunks through the
    chosen format (parquet / hdf5 / csv / numpy / zarr / lerobot / rlds)
    so memory stays bounded. ``output_dir`` is validated against
    ``RESURRECTOR_ALLOWED_ROOTS`` to prevent writing outside trusted
    locations.

    Pass ``preset`` to use a named bundle (LeRobot, RLDS, etc.); any
    other query params override the preset's values. See
    ``GET /api/export-presets`` for the available preset list.

    No async / job system: large exports block the request. A user
    cancel hangs up the connection but the server continues writing.
    """
    validated_output = _validate_path(output_dir)
    index = _get_index()
    try:
        bag = index.get_bag(bag_id)
        if bag is None:
            raise HTTPException(404, "Bag not found")

        from resurrector.core.bag_frame import BagFrame
        bf = BagFrame(bag["path"])
        topic_list = [t.strip() for t in topics.split(",")] if topics else None
        try:
            output_path = bf.export(
                topics=topic_list,
                format=format,
                output=str(validated_output),
                sync=sync,
                downsample_hz=downsample_hz,
                preset=preset,
            )
        except ValueError as e:
            # Unknown preset, etc. — surface as 400 not 500.
            raise HTTPException(400, str(e))
        return {"status": "completed", "output_path": str(output_path)}
    finally:
        index.close()


@app.get("/api/search")
async def search_bags(q: str = Query(description="Search query")) -> list[dict[str, Any]]:
    """Filter the index with the search DSL — ``topic:``, ``health:>N``, etc.

    Same query syntax as the Python ``resurrector.search()`` API:
    space-separated terms, ANDed together. Supported terms include
    ``topic:/imu/data``, ``health:>=80``, ``tag:robot:digit``,
    ``after:2026-04-01``, free text against the file path.

    Returns a list of bag records.
    """
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
    """Scan a directory for bag files and index them. Synchronous unless ``stream=true``.

    With ``stream=false`` (default), runs the full scan and returns a
    summary on completion. With ``stream=true``, returns a Server-Sent
    Events stream where each event is a per-bag progress record — used
    by the dashboard's "Scan folder" button to render a live progress
    bar.

    ``path`` is validated against ``RESURRECTOR_ALLOWED_ROOTS`` so the
    UI can't trigger scans outside trusted locations.
    """
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


def _classify_scan_error(file_path: Path, exc: Exception) -> dict[str, Any]:
    """Tag scan errors with a ``kind`` so the frontend can route the right
    install banner. Returns the error dict the scan endpoint surfaces.

    Recognized kinds: ``ros1_convert_unavailable`` (.bag file but the
    ``mcap`` CLI is missing), ``ros2_convert_unavailable`` (.db3 / dir
    bag but no ROS 2 install on PATH), ``unknown`` (everything else).
    """
    msg = str(exc)
    suffix = file_path.suffix.lower()
    kind = "unknown"
    if suffix == ".bag" and ("mcap" in msg or "convert" in msg.lower()):
        kind = "ros1_convert_unavailable"
    elif (suffix == ".db3" or file_path.is_dir()) and "ros2" in msg.lower():
        kind = "ros2_convert_unavailable"
    return {"file": str(file_path), "error": msg, "kind": kind}


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
                errors.append(_classify_scan_error(scanned.path, e))
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
                err = _classify_scan_error(scanned.path, e)
                errors.append(err)
                yield f"data: {json.dumps({'event': 'error', 'file': scanned.path.name, 'error': err['error'], 'kind': err['kind'], 'progress': i + 1, 'total': total})}\n\n"

            # Yield control to event loop so SSE messages flush
            await asyncio.sleep(0)
    finally:
        index.close()

    yield f"data: {json.dumps({'event': 'complete', 'scanned': total, 'indexed': indexed, 'errors': errors})}\n\n"


@app.post("/api/bags/upload")
async def upload_bag(request: Request, filename: str = Query(description="Original bag filename")) -> dict[str, Any]:
    """Upload a single bag file and index it, returning the indexed bag.

    For the notebook "Import a new bag" flow: a browser can't hand the
    server a filesystem path, so it POSTs the raw file bytes (not
    multipart — avoids the python-multipart dependency). We stream the
    body to ``~/.resurrector/uploads/<uuid>/<name>`` (under the home
    allowed-root, so later frame/export path validation passes) in
    bounded-memory chunks, then scan + index that one file.

    ``filename`` (query) carries the original name + extension so we can
    reject non-bag files up front and keep a human-readable path.
    """
    from resurrector.ingest.scanner import scan_path, BAG_EXTENSIONS
    from resurrector.ingest.parser import parse_bag
    from resurrector.core.bag_frame import BagFrame
    import uuid

    name = Path(filename).name
    if not name:
        raise HTTPException(400, "No filename provided")
    if Path(name).suffix.lower() not in BAG_EXTENSIONS:
        raise HTTPException(
            400,
            f"Unsupported file type '{Path(name).suffix}'. "
            f"Expected one of: {', '.join(sorted(BAG_EXTENSIONS))}",
        )

    # Unique subdir per upload avoids clobbering same-named files and lets
    # scan_path target exactly this one bag. Base defaults to the home cache
    # (always inside the allowed roots so later frame/export path validation
    # passes); override with RESURRECTOR_UPLOADS_DIR.
    uploads_base = Path(
        os.environ.get("RESURRECTOR_UPLOADS_DIR", str(Path.home() / ".resurrector" / "uploads"))
    )
    dest_dir = (uploads_base / uuid.uuid4().hex).resolve()
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / name

    # Stream the raw body to disk. request.stream() yields chunks as they
    # arrive — never materializes the whole (possibly multi-GB) bag in RAM.
    wrote = 0
    try:
        with dest.open("wb") as out:
            async for chunk in request.stream():
                out.write(chunk)
                wrote += len(chunk)
    except Exception as e:
        shutil.rmtree(dest_dir, ignore_errors=True)
        raise HTTPException(500, f"Failed to save upload: {e}")

    if wrote == 0:
        shutil.rmtree(dest_dir, ignore_errors=True)
        raise HTTPException(400, "Empty upload — no bytes received")

    scanned_files = scan_path(dest)
    if not scanned_files:
        shutil.rmtree(dest_dir, ignore_errors=True)
        raise HTTPException(400, "Uploaded file is not a readable bag")

    index = _get_index()
    try:
        scanned = scanned_files[0]
        try:
            parser = parse_bag(scanned.path)
            metadata = parser.get_metadata()
            bag_id = index.upsert_bag(scanned, metadata)
            bf = BagFrame(scanned.path)
            report = bf.health_report()
            index.update_health_score(bag_id, report.score)
        except Exception as e:
            shutil.rmtree(dest_dir, ignore_errors=True)
            err = _classify_scan_error(scanned.path, e)
            raise HTTPException(422, err["error"])
        bag = index.get_bag(bag_id)
    finally:
        index.close()

    if bag is None:
        raise HTTPException(500, "Bag indexed but could not be read back")
    return bag


# ============================================================================
# Async job system (v0.7 — Feature E)
# ============================================================================


def _scan_job_worker(scan_path_str: str):
    """Build a synchronous scan worker for the job manager.

    Returns a callable ``worker(progress)`` that scans + indexes a directory,
    reporting per-bag progress. Runs on a background thread (the job pool),
    so it must be fully synchronous — no asyncio here.
    """
    def worker(progress) -> dict[str, Any]:
        from resurrector.ingest.scanner import scan_path
        from resurrector.ingest.parser import parse_bag
        from resurrector.core.bag_frame import BagFrame

        scan_path_obj = _validate_path(scan_path_str)
        files = scan_path(scan_path_obj)
        total = len(files)
        index = _get_index()
        indexed = 0
        errors: list[dict[str, Any]] = []
        progress(0.0, f"scanning {total} file(s)")
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
                except Exception as e:  # noqa: BLE001
                    errors.append(_classify_scan_error(scanned.path, e))
                frac = (i + 1) / total if total else 1.0
                progress(frac, f"{i + 1}/{total} — {scanned.path.name}")
        finally:
            index.close()
        return {"scanned": total, "indexed": indexed, "errors": errors}

    return worker


@app.post("/api/jobs/scan")
async def submit_scan_job(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Submit a directory scan as a background job. Returns ``{job_id}``.

    The async alternative to ``POST /api/scan`` for TB-scale fleets where a
    synchronous scan would time out the request. Poll ``GET /api/jobs/{id}``
    for progress.
    """
    from resurrector.core.jobs import get_job_manager

    payload = payload or {}
    path = payload.get("path")
    if not path:
        raise HTTPException(400, "'path' is required")
    # Validate up front so a bad path fails the request, not silently the job.
    scan_path_obj = _validate_path(path)
    if not scan_path_obj.exists():
        raise HTTPException(400, f"Path does not exist: {path}")

    manager = get_job_manager()
    job_id = manager.submit("scan", _scan_job_worker(path))
    return {"job_id": job_id}


@app.get("/api/jobs")
async def list_jobs_api(kind: str | None = Query(default=None)) -> dict[str, Any]:
    """List background jobs, newest first. Optional ``?kind=scan`` filter."""
    from resurrector.core.jobs import get_job_manager
    jobs = get_job_manager().list_jobs(kind=kind)
    return {"jobs": [j.to_dict() for j in jobs]}


@app.get("/api/jobs/{job_id}")
async def get_job_api(job_id: str) -> dict[str, Any]:
    """Poll one job's status / progress / result. 404 if unknown id."""
    from resurrector.core.jobs import get_job_manager
    job = get_job_manager().get(job_id)
    if job is None:
        raise HTTPException(404, "Job not found")
    return job.to_dict()


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job_api(job_id: str) -> dict[str, Any]:
    """Request cooperative cancellation of a job."""
    from resurrector.core.jobs import get_job_manager
    ok = get_job_manager().cancel(job_id)
    if not ok:
        raise HTTPException(409, "Job not cancellable (unknown or already finished)")
    return {"cancelled": True, "job_id": job_id}


@app.get("/api/bags/{bag_id}/timeline")
async def get_timeline(bag_id: int) -> dict[str, Any]:
    """Per-topic message density across the bag's duration. Backs the timeline strip.

    Returns one entry per topic with a binned count vector + start/end
    timestamps. Memory bounded by num_topics × num_bins, regardless of
    bag size — uses the streaming density compute, not message lists.
    """
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
    """Return a small JPEG of the first frame on an image topic. For Library cards.

    Cached on disk under ``~/.resurrector/thumbnails/`` keyed by
    ``(bag_id, topic, mtime)`` so repeat hits are O(1). Quality and
    max-dimension are tuned to keep payloads small enough for grid
    rendering.
    """
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
    """CLIP-powered natural-language search across indexed frame embeddings.

    Backs the Search page. ``q`` is plain English; results are ranked
    by cosine similarity. ``clips=true`` groups consecutive matches
    into temporal segments instead of returning isolated frames.

    Preconditions: bags must be ``index-frames``-d first; the
    ``[vision]`` or ``[vision-openai]`` extra must be installed.
    """
    index = _get_index()
    try:
        try:
            from resurrector.core.vision import FrameSearchEngine
        except ImportError as e:
            raise HTTPException(
                status_code=503,
                detail={
                    "kind": "vision_not_installed",
                    "message": str(e),
                    "install_command": "pip install 'rosbag-resurrector[vision]'",
                },
            )

        engine = FrameSearchEngine(index)

        # If no bag has indexed frames yet, return early with a clear hint
        # so the frontend can render an actionable empty state (which bags
        # need indexing) instead of an opaque "no results" message.
        if index.count_frame_embeddings() == 0:
            raise HTTPException(
                status_code=409,
                detail={
                    "kind": "no_indexed_frames",
                    "message": "No bags have CLIP frame embeddings yet. Index a bag with `resurrector index-frames <bag-path>`.",
                },
            )

        try:
            if clips:
                results = engine.search_temporal(
                    q, clip_duration_sec=clip_duration,
                    top_k=top_k, bag_id=bag_id, min_similarity=min_similarity,
                )
            else:
                results = engine.search(
                    q, top_k=top_k, bag_id=bag_id, min_similarity=min_similarity,
                )
        except ImportError as e:
            raise HTTPException(
                status_code=503,
                detail={
                    "kind": "vision_not_installed",
                    "message": str(e),
                    "install_command": "pip install 'rosbag-resurrector[vision]'",
                },
            )

        if clips:
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


@app.get("/api/search/index-status")
async def get_search_index_status() -> dict[str, Any]:
    """Cross-bag summary used by the Search page to render a useful empty
    state: shows which bags already have CLIP embeddings vs which have
    image topics but no embeddings yet (the "ready to index" set).

    Also reports whether the optional vision deps are importable so the
    UI can suggest the right install command up front.
    """
    try:
        from resurrector.core.vision import CLIPEmbedder  # noqa: F401
        try:
            import sentence_transformers  # noqa: F401
            vision_available = True
        except ImportError:
            try:
                import openai  # noqa: F401
                vision_available = True
            except ImportError:
                vision_available = False
    except ImportError:
        vision_available = False

    index = _get_index()
    try:
        IMAGE_TYPES = ("sensor_msgs/msg/Image", "sensor_msgs/msg/CompressedImage")

        # Pull every bag with its topics; classify by whether it has frame
        # embeddings already vs has image topics that could be indexed.
        rows = index.conn.execute(
            "SELECT id, path FROM bags ORDER BY path"
        ).fetchall()

        indexed: list[dict[str, Any]] = []
        unindexed: list[dict[str, Any]] = []

        for bag_id, path in rows:
            embed_count = index.count_frame_embeddings(bag_id)
            image_topics = [
                t["name"] for t in index._get_topics(bag_id)
                if t["message_type"] in IMAGE_TYPES
            ]

            entry = {
                "bag_id": bag_id,
                "name": Path(path).name,
                "path": path,
                "image_topics": image_topics,
                "frame_count": embed_count,
            }

            if embed_count > 0:
                indexed.append(entry)
            elif image_topics:
                unindexed.append(entry)
            # Bags with no image topics aren't surfaced — nothing to index.

        return {
            "vision_available": vision_available,
            "install_command": "pip install 'rosbag-resurrector[vision]'",
            "indexed_bags": indexed,
            "unindexed_bags": unindexed,
        }
    finally:
        index.close()


@app.get("/api/bags/{bag_id}/frame-index-status")
async def get_frame_index_status(bag_id: int) -> dict[str, Any]:
    """Report whether a bag has CLIP frame embeddings, and how many.

    Used by the Search page to decide whether to show "index this bag
    first" vs querying directly. Returns ``{indexed: bool, count: int,
    topics: [...]}``.
    """
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
    """Pin a text annotation at a specific timestamp on a bag.

    Body: ``{timestamp_ns: int, text: str, topic: Optional[str]}``.
    Annotations show up as Plotly markers on the Explorer page and
    persist across reloads.
    """
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
    """Edit an existing annotation's text. Body: ``{text: str}``.

    Returns 404 if the annotation id doesn't exist.
    """
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
    """Permanently remove one annotation by id. 404 if not found."""
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
    """List every dataset in the index, with version metadata. Backs the Datasets page.

    Returns a list of dicts: id, name, description, version count,
    timestamps. Sorted by most-recently-updated first.
    """
    mgr = _get_dataset_manager()
    try:
        items = mgr.list_datasets()
        return {"datasets": items}
    finally:
        mgr.close()


@app.post("/api/datasets")
async def create_dataset_api(payload: dict[str, Any]) -> dict[str, Any]:
    """Create an empty dataset container. Body: ``{name: str, description?: str}``.

    The dataset starts empty — add versions via the
    ``POST /api/datasets/{name}/versions`` endpoint. Names must be
    unique; collisions return 409.
    """
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
    """Return a single dataset by name with its full version list. 404 if not found."""
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
    """Remove a dataset and every version from the index. Doesn't touch exported files.

    Idempotent — deleting a non-existent dataset returns 404.
    """
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
    """Materialize a dataset version. Body: ``{"output_dir": str}``.

    Reads each bag in the version, applies the recorded sync /
    downsample / format settings, and writes data + manifest +
    auto-README + reproducibility config under
    ``<output_dir>/<dataset>/<version>/``. Synchronous; large datasets
    block the request.
    """
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
    """Drop one version from a dataset (the dataset itself stays). Files on disk are untouched."""
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

    # Live mode needs rclpy; fail fast with a structured detail the
    # frontend can render as an install banner instead of letting the
    # subprocess die silently after spawn.
    if mode == "live":
        from resurrector.core.capabilities import get_capabilities
        cap = get_capabilities()["bridge_live"]
        if not cap.available:
            raise HTTPException(
                status_code=503,
                detail={
                    "kind": "capability_unavailable",
                    "capability": "bridge_live",
                    "install_command": cap.install_command,
                    "description": cap.description,
                    "message": (
                        "Live mode requires rclpy (ROS 2 Python client). "
                        "Install ROS 2 first."
                    ),
                },
            )

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
    """Terminate the WebSocket bridge subprocess. No-op if none is running.

    Always returns 200 with the new state. Used by the Bridge page's
    Stop button.
    """
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
    """Return current state of the WebSocket bridge subprocess.

    Polled every few seconds by the Bridge page. Reports ``running``,
    ``pid``, mode (``playback`` / ``live``), bound port, and (if
    playback) the bag path being replayed.
    """
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
async def bridge_proxy(rest_path: str, request: Request) -> Any:
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

    # Log non-2xx so we can diagnose proxied failures without having to
    # sniff WebSocket frames. The bridge subprocess otherwise hides
    # behind the proxy.
    if not (200 <= resp.status_code < 300):
        logger.warning(
            "bridge proxy %s %s -> %s body=%s",
            method, url, resp.status_code, resp.content[:300],
        )

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
        # Transform preview applies stateful transforms (derivative,
        # integral, IIR, rolling) which can't be cleanly chunk-streamed
        # without carrying state across chunks — out of scope for the
        # v0.4.0 dashboard endpoint. Gate on LARGE_TOPIC_THRESHOLD with
        # a clear actionable error so users on big topics see why and
        # know what to do instead.
        from resurrector.core.exceptions import LargeTopicError
        try:
            df = view.to_polars()
        except LargeTopicError as e:
            raise HTTPException(
                413,
                f"Transform preview is only supported for topics under "
                f"{e.threshold:,} messages. {e.topic_name!r} has "
                f"{e.message_count:,}. For larger topics, use "
                f"bf[{e.topic_name!r}].iter_chunks() and apply_transform "
                f"per chunk in a notebook.",
            )

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
# ---------------------------------------------------------------------------
# 3D scene endpoints (Option 3 / v0.5.0)
# ---------------------------------------------------------------------------


def _build_tf_tree(bag_path: str, time_ns: int | None = None):
    """Read /tf and /tf_static from a bag and return a populated TFTree.

    ``time_ns`` caps the dynamic TF samples loaded (so a long bag
    doesn't read every frame for a single time-anchored query). Static
    transforms are always loaded in full since they're typically tiny.
    """
    from resurrector.core.bag_frame import BagFrame
    from resurrector.core.scene import TFTree, parse_tf_message

    tree = TFTree()
    bf = BagFrame(bag_path)
    available = {ti.name for ti in bf.metadata.topics}
    parser = bf._parser  # already constructed by BagFrame
    for topic, is_static in (("/tf_static", True), ("/tf", False)):
        if topic not in available:
            continue
        # MCAP's end_time is exclusive — bump by 1 ns so a query exactly
        # at the bag's start time still captures the first TF sample.
        end_time_ns = (
            None if is_static or time_ns is None
            else int(time_ns) + 1
        )
        for msg in parser.read_messages(
            topics=[topic], end_time_ns=end_time_ns,
        ):
            if msg.raw_data is None:
                continue
            for tf in parse_tf_message(msg.raw_data, is_static=is_static):
                tree.add(tf)
    return tree


@app.get("/api/bags/{bag_id}/scene/tf-tree")
async def get_scene_tf_tree(
    bag_id: int, time_ns: int | None = None,
) -> dict[str, Any]:
    """Resolve the TF tree at a given timestamp.

    Returns frames, root frames, and one resolved transform per known
    edge. ``time_ns`` defaults to the bag's end time so the response
    includes every TF sample (most-recent state, suitable for an
    initial render). Pass an explicit ``time_ns`` for time-anchored
    scrubbing. Unknown bag → 404; bag with no /tf or /tf_static →
    empty tree (200 OK).
    """
    index = _get_index()
    try:
        bag = index.get_bag(int(bag_id))
        if bag is None:
            raise HTTPException(404, "Bag not found")
        from resurrector.core.bag_frame import BagFrame

        if time_ns is None:
            bf = BagFrame(bag["path"])
            time_ns = bf.metadata.end_time_ns
        tree = _build_tf_tree(bag["path"], time_ns=time_ns)
        return tree.to_dict(int(time_ns))
    finally:
        index.close()


@app.get("/api/bags/{bag_id}/scene/pointcloud")
async def get_scene_pointcloud(
    bag_id: int,
    topic: str = Query(..., description="PointCloud2 topic name, e.g. /lidar/points"),
    time_ns: int | None = None,
    max_points: int = Query(default=20_000, ge=10, le=200_000),
) -> dict[str, Any]:
    """Return the decoded (x, y, z) points of one PointCloud2 message.

    Picks the message nearest to ``time_ns`` (or the first message if
    unspecified). Decimates to at most ``max_points`` to keep wire size
    bounded — a 200k-point sweep at ~12 bytes/point is already 2.4 MB.

    Returns ``{frame_id, time_ns, n_points, points: [[x,y,z],...]}``.
    """
    index = _get_index()
    try:
        bag = index.get_bag(int(bag_id))
        if bag is None:
            raise HTTPException(404, "Bag not found")
        from resurrector.core.bag_frame import BagFrame
        from resurrector.core.scene import (
            decode_pointcloud2_xyz, parse_pointcloud2_meta,
        )

        bf = BagFrame(bag["path"])
        if topic not in {ti.name for ti in bf.metadata.topics}:
            raise HTTPException(404, f"Topic {topic!r} not found")

        chosen_msg = None
        chosen_dt = None
        for msg in bf._parser.read_messages(topics=[topic]):
            if msg.raw_data is None:
                continue
            if time_ns is None:
                chosen_msg = msg
                break
            dt = abs(msg.timestamp_ns - int(time_ns))
            if chosen_dt is None or dt < chosen_dt:
                chosen_msg = msg
                chosen_dt = dt
            if msg.timestamp_ns > int(time_ns) and chosen_dt is not None:
                # Past target with a candidate already — stop scanning
                break
        if chosen_msg is None or chosen_msg.raw_data is None:
            raise HTTPException(404, f"No PointCloud2 messages on {topic!r}")
        meta = parse_pointcloud2_meta(chosen_msg.raw_data)
        if meta is None:
            raise HTTPException(500, "Failed to parse PointCloud2 metadata")
        pts = decode_pointcloud2_xyz(
            chosen_msg.raw_data, meta, max_points=max_points,
        )
        if pts is None:
            return {
                "frame_id": meta.frame_id,
                "time_ns": chosen_msg.timestamp_ns,
                "n_points": 0,
                "points": [],
                "warning": "No xyz float32 fields found — non-standard layout",
            }
        return {
            "frame_id": meta.frame_id,
            "time_ns": chosen_msg.timestamp_ns,
            "n_points": int(pts.shape[0]),
            "points": pts.tolist(),
        }
    finally:
        index.close()


@app.get("/api/bags/{bag_id}/scene/topics")
async def list_scene_topics(bag_id: int) -> dict[str, Any]:
    """Categorize bag topics by their relevance to the 3D scene viewer.

    Returns ``{tf, tf_static, pointclouds, images, markers}`` lists so
    the frontend can show only the topics the viewer can actually
    consume.
    """
    index = _get_index()
    try:
        bag = index.get_bag(int(bag_id))
        if bag is None:
            raise HTTPException(404, "Bag not found")
        topics = bag.get("topics") or []
        out: dict[str, list[str]] = {
            "tf": [], "tf_static": [], "pointclouds": [],
            "images": [], "markers": [],
        }
        for t in topics:
            name = t["name"]
            mtype = t.get("message_type", "")
            if name == "/tf":
                out["tf"].append(name)
            elif name == "/tf_static":
                out["tf_static"].append(name)
            elif "PointCloud2" in mtype:
                out["pointclouds"].append(name)
            elif "Image" in mtype or "CompressedImage" in mtype:
                out["images"].append(name)
            elif "Marker" in mtype:
                out["markers"].append(name)
        return out
    finally:
        index.close()


@app.get("/api/bags/{bag_id}/scene/camera-frame-at")
async def get_camera_frame_at(
    bag_id: int,
    topic: str = Query(..., description="Image topic name"),
    time_ns: int = Query(..., description="Target timestamp ns"),
) -> dict[str, Any]:
    """Return the frame_index nearest to ``time_ns`` for an image topic.

    Used by the SceneViewer's camera-image overlay to keep the displayed
    frame synced to the time slider. Frontend then fetches the actual
    image bytes via the existing ``/topics/{name}/frame/{idx}`` endpoint
    so we don't double-pay the JPEG-encode cost when the frame is cached.

    Returns ``{frame_index, frame_time_ns, dt_ns}`` (dt_ns = signed
    difference frame_time - target_time, useful for "frame is N ms old"
    UI hints).
    """
    from resurrector.ingest.frame_index import (
        IMAGE_TOPIC_TYPES, build_frame_offsets,
    )

    index = _get_index()
    try:
        bag = index.get_bag(int(bag_id))
        if bag is None:
            raise HTTPException(404, "Bag not found")
        topic_info = next(
            (t for t in bag.get("topics", []) if t["name"] == topic), None,
        )
        if topic_info is None:
            raise HTTPException(404, f"Topic {topic!r} not found")
        if topic_info["message_type"] not in IMAGE_TOPIC_TYPES:
            raise HTTPException(
                400, f"Topic {topic!r} is not an image topic "
                     f"(type: {topic_info['message_type']})",
            )

        # Lazy-build the frame offsets if not already indexed
        if not index.has_frame_offsets(int(bag_id), topic):
            build_frame_offsets(index, int(bag_id), bag["path"], topics=[topic])

        # Direct DuckDB query: pick the row with smallest |timestamp - target|
        with index._lock:
            row = index.conn.execute(
                """
                SELECT frame_index, timestamp_ns
                FROM frame_offsets
                WHERE bag_id = ? AND topic = ?
                ORDER BY ABS(timestamp_ns - ?) ASC
                LIMIT 1
                """,
                [int(bag_id), topic, int(time_ns)],
            ).fetchone()
        if row is None:
            raise HTTPException(404, f"No frames indexed for topic {topic!r}")
        frame_index, frame_ts = row
        return {
            "frame_index": int(frame_index),
            "frame_time_ns": int(frame_ts),
            "dt_ns": int(frame_ts - int(time_ns)),
        }
    finally:
        index.close()


@app.get("/api/bags/{bag_id}/scene/markers")
async def get_scene_markers(
    bag_id: int,
    topic: str = Query(..., description="Marker or MarkerArray topic name"),
    time_ns: int | None = None,
) -> dict[str, Any]:
    """Return the decoded markers from one Marker / MarkerArray message.

    Picks the message nearest to ``time_ns`` (or the first message if
    unspecified). Auto-detects whether the topic is `Marker` (returns
    a single-element list) or `MarkerArray` (returns the full list).

    Returns ``{frame_id, time_ns, n_markers, markers: [...]}``.
    """
    index = _get_index()
    try:
        bag = index.get_bag(int(bag_id))
        if bag is None:
            raise HTTPException(404, "Bag not found")
        from resurrector.core.bag_frame import BagFrame
        from resurrector.core.scene import parse_marker, parse_marker_array

        bf = BagFrame(bag["path"])
        topic_info = next(
            (ti for ti in bf.metadata.topics if ti.name == topic), None,
        )
        if topic_info is None:
            raise HTTPException(404, f"Topic {topic!r} not found")
        is_array = "MarkerArray" in topic_info.message_type

        chosen_msg = None
        chosen_dt = None
        for msg in bf._parser.read_messages(topics=[topic]):
            if msg.raw_data is None:
                continue
            if time_ns is None:
                chosen_msg = msg
                break
            dt = abs(msg.timestamp_ns - int(time_ns))
            if chosen_dt is None or dt < chosen_dt:
                chosen_msg = msg
                chosen_dt = dt
            if msg.timestamp_ns > int(time_ns) and chosen_dt is not None:
                break
        if chosen_msg is None or chosen_msg.raw_data is None:
            raise HTTPException(404, f"No marker messages on {topic!r}")

        if is_array:
            markers = parse_marker_array(chosen_msg.raw_data)
        else:
            single = parse_marker(chosen_msg.raw_data)
            markers = [single] if single is not None else []

        return {
            "topic": topic,
            "time_ns": chosen_msg.timestamp_ns,
            "n_markers": len(markers),
            "markers": [m.to_dict() for m in markers],
        }
    finally:
        index.close()


@app.get("/api/scene/urdf")
async def get_urdf_file(path: str) -> Any:
    """Serve a URDF file from a sandboxed location for the SceneViewer.

    Path validation: must resolve under one of the allowed roots
    (RESURRECTOR_ALLOWED_ROOTS or the safe default set). Refuses
    symlinks pointing outside the sandbox. URDF files are typically
    small (< 1 MB), so we read into memory and return as text.

    Returns the raw URDF XML with Content-Type ``application/xml``.
    The frontend's urdf-loader parses the string client-side and walks
    relative `<mesh filename="…">` references via the loader's package://
    resolver — those mesh files would need a separate endpoint or to be
    inlined; v0.6.0 supports primitive-shape URDFs (box / cylinder /
    sphere) without external mesh assets.
    """
    try:
        resolved = _validate_path(path)
    except HTTPException:
        raise
    if not resolved.exists() or not resolved.is_file():
        raise HTTPException(404, f"URDF file not found: {path}")
    if resolved.suffix.lower() not in (".urdf", ".xml"):
        raise HTTPException(400, "Only .urdf or .xml files are accepted")
    if resolved.stat().st_size > 5 * 1024 * 1024:  # 5 MB cap
        raise HTTPException(413, "URDF file exceeds 5 MB cap")
    try:
        text = resolved.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        raise HTTPException(400, "URDF file is not valid UTF-8")
    return JSONResponse(
        content={"urdf": text, "path": str(resolved)},
        headers={"Cache-Control": "public, max-age=300"},
    )


@app.get("/api/bags/{bag_id}/scene/urdf-from-bag")
async def get_urdf_from_bag(bag_id: int, topic: str = "/robot_description") -> Any:
    """Try to extract a URDF string from a bag's `/robot_description` topic.

    Many ROS 2 bags include the URDF as a string published to a latched
    topic (commonly ``/robot_description``). If found, we return it the
    same way as ``/api/scene/urdf`` so the frontend treats both sources
    interchangeably.

    Returns 404 if the topic isn't present or no message has the URDF
    payload (we look for ``<robot`` in the decoded string field).
    """
    index = _get_index()
    try:
        bag = index.get_bag(int(bag_id))
        if bag is None:
            raise HTTPException(404, "Bag not found")
        from resurrector.core.bag_frame import BagFrame

        bf = BagFrame(bag["path"])
        if topic not in {ti.name for ti in bf.metadata.topics}:
            raise HTTPException(
                404,
                f"Topic {topic!r} not found in bag (typical robot_description "
                f"location is /robot_description)",
            )
        # Try to read the first message; std_msgs/String CDR is just a
        # length-prefixed string after the 4-byte CDR header.
        for msg in bf._parser.read_messages(topics=[topic]):
            if msg.raw_data is None or len(msg.raw_data) < 8:
                continue
            import struct
            try:
                (n,) = struct.unpack_from("<I", msg.raw_data, 4)
                payload = msg.raw_data[8:8 + n].decode("utf-8", errors="replace")
            except (struct.error, UnicodeDecodeError):
                continue
            if "<robot" in payload:
                return JSONResponse(
                    content={"urdf": payload, "topic": topic},
                    headers={"Cache-Control": "public, max-age=300"},
                )
        raise HTTPException(
            404,
            f"No URDF-like message found on {topic!r} (no <robot tag in payloads)",
        )
    finally:
        index.close()


_static_dir = Path(__file__).parent / "static"
if _static_dir.exists():
    # SPA fallback: any path that isn't a real file in /static gets
    # index.html so React Router can resolve it client-side. Without
    # this, /help / /search / /datasets etc. all 404 on direct
    # navigation or page reload — they only work via in-app Link clicks.
    # /api/* routes are registered above this mount, so they keep
    # their normal handlers.
    from starlette.exceptions import HTTPException as _StarletteHTTPException

    class _SPAStaticFiles(StaticFiles):
        async def get_response(self, path, scope):
            try:
                return await super().get_response(path, scope)
            except _StarletteHTTPException as exc:
                if exc.status_code == 404:
                    return await super().get_response("index.html", scope)
                raise

    app.mount("/", _SPAStaticFiles(directory=str(_static_dir), html=True), name="static")
else:
    @app.get("/")
    async def root():
        """Fallback root response when the React frontend bundle isn't built.

        In a normal install (pip wheel) the React SPA is mounted at /
        by StaticFiles; this handler runs only for source checkouts that
        haven't run ``npm run build``.
        """
        return {
            "message": "RosBag Resurrector API",
            "docs": "/docs",
            "note": "Frontend not built. Run from the dashboard/app directory: npm run build",
        }
