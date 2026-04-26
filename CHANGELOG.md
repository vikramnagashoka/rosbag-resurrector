# Changelog

All notable changes to RosBag Resurrector are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to [Semantic Versioning](https://semver.org/).

Each release has a **What's New** one-liner summary followed by feature lists grouped by category so readers can scan changes without reading diffs.

---

## [Unreleased]

## [0.3.1] — 2026-04-25

### What's new

Power-features release. Six new dashboard surfaces plus a complete set of runnable exploration scripts so you can try every feature in your terminal before opening the dashboard. The strategically headline feature: **cross-bag overlay** — pick 2+ bags, pick a topic, see them aligned on one chart with per-bag offset sliders. Neither Foxglove nor Rerun does this cleanly today.

### Dashboard — new surfaces

- **Bookmarks panel** (right rail in Explorer) — searchable list of every annotation on the current bag with click-to-jump. Sets a 1-second window in Explorer's zoom around the bookmark.
- **Per-topic message-density ribbon** — Plotly heatmap above the chart showing message counts in N time bins per topic. Drops, bursts, and missing data are visible at a glance. Highlighted topic floats to the top; current zoom range overlaid as a translucent box; click any cell to jump there.
- **Math/transform editor** — modal with two tabs:
  - **Common**: derivative, integral, moving average, low-pass, scale, abs, shift — with appropriate parameter inputs and live preview
  - **Expression**: free-form Polars expression with sandboxed evaluation (allowlisted `pl.col()` chains, no imports, no dunder access)
  - Saved transforms append a new dashed-purple subplot to the parent chart so you can compare original and derived
- **Trim & export popover** — shift-drag a region on the chart, popover appears, pick MCAP / Parquet / CSV / HDF5 / NumPy / Zarr / MP4. MCAP output is byte-identical to a recording over that window (no decode/re-encode round-trip).
- **Open in Jupyter** button — trims selection (or whole bag) to Parquet under `~/.resurrector/`, copies a Polars `read_parquet(...)` snippet to your clipboard, opens `localhost:8888` in a new tab.
- **Compare runs page** (`/compare-runs`) — overlay the same topic across N bags. Pick bags as chips, pick a shared topic, see one Plotly trace per bag colored by `bag_label`. Per-bag offset sliders below the chart for sub-second alignment fine-tuning.

### Backend

- **`compute_density(bag_path, topics, bins)`** — computes per-topic message-count histograms over a bag-wide time axis. Powers the dashboard ribbon; cached server-side keyed on `(bag_id, bins, topic, mtime)` so panning is instant.
- **`trim_to_mcap` / `trim_to_format`** in `core/trim.py` — time-range trim with byte-identical MCAP output (preserves schemas, channels, raw message bytes via `mcap.writer`); other formats delegate to the existing streaming Exporter.
- **`apply_transform` and `apply_polars_expression`** in `core/transforms.py` — common math ops + AST-walked expression sandbox. Sandbox rejects names other than `pl`, dunder attribute access, imports, and unallowed `pl.*` functions.
- **`align_bags_by_offset`** in `core/cross_bag.py` — long-format DataFrame builder for cross-bag overlay; LTTB-downsamples each bag's series independently so sparse and dense bags both render smoothly.

### API

- `GET /api/bags/{id}/density?bins=N&topic=...` — per-topic histograms
- `POST /api/bags/{id}/trim` — time-range trim with format dispatch
- `POST /api/transforms/preview` — preview a menu op or expression on real topic data, returns LTTB-downsampled result
- `POST /api/compare/topics` — cross-bag overlay; aligned long-format JSON ready for one trace per bag

### Exploration scripts (new)

Eight runnable scripts under `examples/` that demo every v0.3.1 feature in the terminal — no docs reading required. First run auto-generates a synthetic sample bag at `~/.resurrector/explore_sample.mcap`; subsequent scripts reuse it.

```
examples/01_density_ribbon.py       — sparkline density per topic
examples/02_trim_to_mcap.py         — trim a 2-second window 4 ways
examples/03_math_transforms.py      — derivatives + Polars expression sandbox demo
examples/04_cross_bag_overlay.py    — overlay two synthetic bags with per-bag offsets
examples/05_dashboard_walkthrough.py— boots dashboard, opens browser, prints UI tour
examples/06_bookmarks_via_api.py    — programmatic CRUD on the annotations API
examples/07_jupyter_export.py       — Parquet + paste-ready Polars snippet
examples/08_polars_lazy_filter.py   — lazy filter/projection vs. eager comparison
```

Each script is self-contained; run them in any order. They also serve as a smoke-test suite — running all 8 in sequence exercises every v0.3.1 surface end-to-end.

### Bundle

- Frontend: lazy-loaded `CompareRuns` and `Explorer` keep Plotly out of the main bundle. Main `index.js` is **63KB gz** (no change from v0.3.0); Plotly chunk loads on demand when an Explorer or CompareRuns route is visited.

### Tests

- 70 new Python tests across `test_density`, `test_trim`, `test_transforms_v040`, `test_cross_bag`, `test_api_v040`. Total: **348 passing** (up from 278 in v0.3.0), zero regressions.

### Compatibility

- All v0.3.0 endpoints and behaviors unchanged. v0.3.0 dashboard pages (Library, Explorer, Health, Compare, Search, Datasets, Bridge) all still work as before; v0.3.1 adds onto Explorer and adds the new Compare Runs page next to the existing Compare page.

---

## [0.3.0] — 2026-04-19

### What's new

Unified dashboard release. Every advanced feature that used to be CLI-only now lives in the web UI: semantic search, datasets management, bridge control, and a Plotly-based Explorer with brush-to-zoom and click-to-annotate. Three previously-dormant components (ExportDialog, SyncView, ImageViewer) are wired up and functional.

### Dashboard — new pages

- **Search** — semantic frame search by natural language. Thumbnails link back to the Explorer at the matched frame. Supports clip mode (temporal groups) and actionable "no results" guidance when a bag isn't indexed.
- **Datasets** — full CRUD management of versioned dataset collections. Create, inspect versions, delete, and export directly from the UI.
- **Bridge** — start/stop the PlotJuggler-compatible WebSocket bridge as a subprocess. Dashboard polls bridge status every 3 seconds so unexpected subprocess death surfaces as a toast instead of a broken page.

### Dashboard — rewritten Explorer

- **Plotly subplots** with shared x-axis replace the old SVG mini-charts. Click-and-drag to zoom; server re-downsamples the narrower window via LTTB and returns ~2k points regardless of source density.
- **Linked cursors** across multiple series via Plotly's unified hovermode.
- **Click-to-annotate** — click any point, add a note, persists via new annotations API; renders as dashed lines with labels on subsequent visits.
- **Tab UX** — Plot / Sync / Images. ExportDialog, SyncView, and ImageViewer are now mounted and functional. Images tab automatically opens when an image topic is selected.

### Dashboard infrastructure

- **`src/api.ts`** — typed client for every REST endpoint with a shared `ApiError` class.
- **`<ErrorToast>`** at app root surfaces 4xx/5xx as dismissable banners. All pages (existing Library/Explorer/Health/Compare + new pages) retrofit to use it; no more silent fetch failures.
- **Lazy-loaded Explorer** via `React.lazy()` so the Plotly bundle (~4.7MB) only loads when a user opens a bag; Library/Health/Compare/Search/Datasets/Bridge pages pay just 200KB.

### Backend

- **`GET /api/bags/{id}/topics/{t}?max_points=N`** — new query param triggers LTTB downsampling and caches results keyed on `(bag, topic, window, max_points, mtime)`. Panning or zooming the plot re-requests the narrower window; file edits auto-invalidate via mtime.
- **Frame endpoint** (`/api/bags/{id}/topics/{t}/frame/{n}`) — now uses a DuckDB-cached `(frame_index -> timestamp_ns)` map for O(1) seek. Previously re-scanned the entire bag on every request. Cache is built per (bag, topic) during `resurrector scan` and lazily on demand under a per-(bag, topic) lock for older bags.
- **`/api/bags/{id}/annotations`** + **`/api/annotations/{id}`** — CRUD for persistent plot annotations.
- **`/api/datasets`** + `/versions` + `/export` — full CRUD for dataset management.
- **`/api/bridge/start|stop|status|proxy/*`** — subprocess-managed bridge with cross-origin avoidance via proxy. FastAPI shutdown hook kills the subprocess cleanly.

### Ingest

- **Pre-built frame offset cache during `resurrector scan`** for image topics. Opt out with `--skip-frame-index`. Makes the first Explorer visit on a fresh bag instant instead of doing a cold scan per frame request.
- New `frame_offsets` table in the DuckDB index.
- New `annotations` table in the DuckDB index.

### New tests

- 13 for frame offsets + annotations (indexer CRUD)
- 11 for frame_index pipeline (build, lookup, read)
- 14 for LTTB downsampling
- 23 for new API endpoints (annotations, datasets, frame, downsampled data)

### Compatibility

- Legacy bags scanned on older versions keep working — frame offsets are built lazily on first dashboard/search access if absent.

---

## [0.2.2] — 2026-04-19

### What's new

Onboarding and honesty release. `resurrector doctor` verifies your setup in one command, legacy `.bag` and `.db3` files now offer a one-click conversion helper instead of a blunt error, and streaming claims have been tightened to match reality.

### Onboarding

- **`resurrector doctor`** — single-command environment check. Verifies Python version, MCAP parser, DuckDB index path, optional vision/bridge/watch dependencies, and dashboard allowed-roots configuration. Prints a pass/warn/fail grid so you know exactly which features are ready before you use them.
- **Sample bag demo mode** — `resurrector demo` generates a synthetic bag with realistic IMU/joint/camera data so new users can try the full pipeline (scan → health → export → search) without needing their own data.
- **Dashboard "Scan folder" onboarding** — the library page now shows a one-click scan-folder button on empty state instead of a blank table, guiding first-time users from dashboard launch to indexed bags.

### Format support

- **Auto-convert helper for `.bag` and `.db3`** — opening a legacy file now offers to run `mcap convert` (ROS 1) or `ros2 bag convert` (ROS 2 SQLite) in a subprocess and reopen the converted MCAP, instead of raising `NotImplementedError`. Tested for both CLI (`resurrector info old.bag`) and Python API paths.

### Core

- **`to_lazy_polars` honesty fix** — the method previously materialized all chunks before returning, despite the "lazy" name. It now uses `pl.scan_ipc` on a temporary Arrow stream so filter/projection pushdown actually works. Benchmarked: a `.filter(pl.col("x") > 0).head(10)` on a 500k-message topic runs in ~200ms instead of ~4s.

### Docs

- New `CHANGELOG.md` (this file) tracking releases with "What's new" summaries and category-grouped features.

### Fixed

- Dashboard path validation now reads `RESURRECTOR_ALLOWED_ROOTS` per-call instead of at import, so tests and CLI overrides take effect immediately.

---

## [0.2.1] — 2026-04-18

### What's new

Pre-launch hardening. Every export format now streams to disk (peak memory bounded regardless of topic size), CDR parsing rejects malformed buffers with a typed error instead of crashing, and the dashboard path validator defaults to a safe root.

### Core / export

- **Streaming for all formats** — Parquet, HDF5, CSV, NumPy, and Zarr now write chunk-by-chunk. Peak memory is bounded by `CHUNK_SIZE=50k` rows regardless of total topic size.
- **`TopicView.iter_chunks(chunk_size)`** — new streaming primitive returning an iterator of Polars DataFrames.
- **`TopicView.to_lazy_polars()`** — returns a `pl.LazyFrame` for filter/projection pushdown.
- **`ExportError`** — new typed exception. Per-column serialization failures are collected and surfaced instead of silently swallowed.
- **LeRobot and RLDS export formats** — new `--format lerobot` and `--format rlds` options for ML training pipelines (RT-2, OpenX, LeRobot).

### Ingest

- **`CDRParseError`** — new typed exception raised on malformed/truncated CDR messages. Inflated field counts (e.g., `n_names = 50M`) are now rejected early with a clear error instead of crashing with an opaque `struct.error` or silently truncating.
- **Bounds checks** — every `struct.unpack_from` in the CDR parser now validates offset + size ≤ buffer length before reading.

### Bridge

- WebSocket send loop now logs errors cleanly and closes the client connection on failure instead of silently exiting.
- Ring buffer warns when a consumer falls behind by >50% of buffer capacity.

### Dashboard

- Path validation defaults to `Path.home()` when `RESURRECTOR_ALLOWED_ROOTS` is unset. Previously, unset env meant any path was allowed.

### Concurrency

- `BagIndex` writes are now serialized by a shared lock; the bridge, dashboard, and scanner can safely share one index.

### Docs

- README now surfaces ROS 2 / MCAP support as the primary format in the hero section.
- Added architectural overview in `PLAN.md` and pre-launch work list in `LAUNCH_BLOCKERS.md`.

### Tests

- 164 → 202 passing (+38 new), 2 skipped.

### Fixed

- Linux CI regression: `_validate_path` now reads `RESURRECTOR_ALLOWED_ROOTS` per-call so test fixtures can override roots after module import.

---

## [0.2.0] — 2026-03-15

### What's new

First public release. Bags can be scanned, health-checked, synchronized, searched by natural language, and exported to ML formats — all without installing ROS.

### Core data engine

- **`BagFrame`** — pandas-like handle on a bag file with `.info()`, `.health_report()`, `.time_slice()`, `.sync()`, `.export()`.
- **`TopicView`** — per-topic selection with `.to_polars()`, `.to_pandas()`, `.to_numpy()`, `.iter_messages()`, `.iter_images()`.
- **Multi-stream sync** — three methods: `nearest`, `interpolate`, `sample_and_hold`.
- **Smart topic grouping** — Perception/State/Navigation/Control/Transforms/Diagnostics classifier.

### Health checks

- Automatic 0–100 quality score per bag.
- Detectors: dropped messages, time gaps, out-of-order timestamps, partial topics, message size anomalies.
- `HealthConfig` for per-robot threshold tuning.

### Ingest

- `.mcap` parsing (primary, schema-aware via `mcap-ros2-support`).
- DuckDB index at `~/.resurrector/index.db` with stale-path detection.

### Export

- Formats: Parquet, HDF5, CSV, NumPy (`.npz`), Zarr.
- Synchronization + downsampling baked in.

### Datasets

- `DatasetManager` with versioned dataset collections.
- Auto-generated `manifest.json` (SHA256), `dataset_config.json`, and per-dataset `README.md`.

### Video / image

- `sensor_msgs/Image` and `sensor_msgs/CompressedImage` parsing.
- `iter_images()` yields `(timestamp_ns, np.ndarray)`.
- MP4 video + PNG/JPEG sequence export.

### Semantic search (CLIP)

- Frame indexing at 5Hz into DuckDB.
- `resurrector search-frames "robot drops object"` with natural-language query.
- Two backends: local `sentence-transformers` (`[vision]`) or OpenAI API (`[vision-openai]`).
- Temporal clip grouping for multi-frame results.

### Resurrector Bridge

- PlotJuggler-compatible WebSocket streaming (`ws://localhost:9090/ws`).
- Playback mode: replay bags at 0.1x–20x with play/pause/seek.
- Live mode: relay ROS 2 topics via rclpy (optional `[bridge-live]` extra).

### CLI

- Commands: `scan`, `quicklook`, `info`, `health`, `list`, `export`, `diff`, `tag`, `watch`, `dataset`, `index-frames`, `search-frames`, `export-frames`, `bridge`, `dashboard`.

### Dashboard

- FastAPI + React (Vite) with Library, Explorer, Health, Compare pages.
- Server-Sent Events for real-time scan progress.

### Packaging

- PyArmor + PyInstaller builds.
- GitHub Actions: tag push → builds macOS DMG and Ubuntu DEB, attaches to draft Release.
- PyPI: `pip install rosbag-resurrector`.

---

## [0.1.0] — 2026-02-10 (internal)

Initial engineering preview. Core MCAP parsing, BagFrame prototype, basic CLI.
