# Changelog

All notable changes to RosBag Resurrector are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to [Semantic Versioning](https://semver.org/).

Each release has a **What's New** one-liner summary followed by feature lists grouped by category so readers can scan changes without reading diffs.

---

## [Unreleased]

### Dashboard

- **New "Help & Docs" page** at `/help`, linked from the top nav. Single in-app reference covering: a quick-start, a tour of every dashboard page (with what it does and when to use it), CLI reference table, Python API code snippets (open / iter_chunks / sync / health / export / search), links to the FastAPI auto-generated `/docs` (Swagger UI), `/redoc`, and `/openapi.json`, a troubleshooting section (zsh extras quoting, search-blank-frames, scan-403, etc.), and external links (GitHub, README, CHANGELOG, PyPI). Right-aligned in the nav so it reads as a "secondary" reference link, separate from the workspace pages.

### Docs

- **Beefed up `--help` output across the CLI.** Every command's docstring now explains what it does, when to use it (vs. similar commands), what each flag actually means with units / examples, and shows runnable examples. Each option also has an inline ``e.g.`` snippet showing the flag in a real command (e.g. ``--top-k 50``, ``-t /imu/data``, ``--sync nearest``, ``-o ./training_data``) so devs can copy-paste from ``--help`` without reading the README.
- **Switched Typer's docstring renderer to `rich_markup_mode="markdown"`.** The default Rich-markup mode silently strips `[vision]`-style brackets in help text (interprets them as markup tags), which made every documented `pip install 'rosbag-resurrector[vision]'` command read as `pip install 'rosbag-resurrector'`. Markdown mode preserves the brackets and renders lists, code, and headings cleanly in the terminal.
- **Python API docstrings filled in across the public surface.** ``BagFrame``, ``TopicView``, ``Exporter``, ``DatasetManager``, ``BagRef``, ``SyncConfig``, ``DatasetMetadata``, ``scan()``, and ``search()`` now all have full Args / Returns / Raises / Example blocks. ``help(bf.export)`` in a REPL or hovering in an IDE actually teaches you what the method does, what each parameter accepts, and what the call looks like in practice. Previously most public methods had a one-line summary and no example.
- **FastAPI dashboard backend route docs.** Every route in ``resurrector/dashboard/api.py`` now has a docstring that populates the auto-generated OpenAPI spec at ``localhost:8080/docs``. Each entry explains what the endpoint returns, what page on the dashboard uses it, and any preconditions (e.g. ``[vision]`` extra needed for semantic search). Power users hitting the backend directly no longer have to read the source.

## [0.4.1] — 2026-04-28

### What's new

Patch release. Fixes a critical bug in semantic frame search and a few launch-prep paper cuts in the install / doctor flow. Also closes a CI coverage gap that allowed those bugs to ship.

### Fixed

- **Semantic frame search was completely broken on every install** — `BagIndex.search_embeddings()` had a parameter-order bug that fed `min_similarity` (a scalar float) to the WHERE clause's embedding placeholder and the embedding list to the threshold comparison. Every search raised `duckdb.BinderException: Cannot compare values of type DOUBLE and type DOUBLE[]`. The end-to-end test in `test_vision.py` would have caught it but is gated behind `pytest.importorskip("sentence_transformers")` and CI didn't install the `[vision]` extra (avoiding the 2 GB CLIP model download), so the test was always skipped. New SQL-level regression tests in `test_indexer_concurrency.py::TestSearchEmbeddings` insert synthetic 512-d embeddings directly so the param binding is exercised on every CI run, no CLIP model needed.
- **`resurrector doctor` install hints** — the "Install" column for optional extras (vision, all-exports, watch, bridge-live) was rendering as bare `pip install rosbag-resurrector`, eating the `[extras]` brackets because Rich was parsing them as markup tags. Now escapes detail/fix_hint so users see the actual command they need to run.
- **Shell-safe pip extras commands everywhere** — `pip install rosbag-resurrector[vision]` and friends now print and document as `pip install 'rosbag-resurrector[vision]'` (single-quoted package spec). Without quotes, zsh — the default shell on macOS since 10.15 — treats `[vision]` as a glob pattern and refuses the command (`zsh: no matches found`). Quoted form works on zsh, bash, fish, and PowerShell. Updated in `doctor`'s output, README install commands, and example scripts.

### CI

- **New `extras-test` job covers every optional extras bundle.** Previously CI only installed `[dev]`, so any code path gated by `pytest.importorskip("sentence_transformers")` / `import zarr` / `import watchdog` etc. was silently skipped — that's how the broken search SQL above shipped. The new job is a matrix over `vision-lite`, `vision`, `all-exports`, `watch`, and `ros1` — each entry installs the extra and runs the whole suite, so existing importorskip gates surface their tests for real. HuggingFace cache makes the CLIP-model download a one-time cost. Excluded by design: `bridge-live` (rclpy needs a real ROS 2 system install), `vision-openai` (needs API key), `packaging` (build tooling).
- **Main `test` job no longer ignores `tests/test_vision.py`.** With the new extras-test job, the main job's importorskip gates handle the CLIP-dependent classes cleanly, and the non-CLIP `TestFrameSampler` tests now actually run there.

### Docs

- **README install section now surfaces MCAP as a hard dependency.** Adds an explicit `pip install mcap mcap-ros2-support` block + a callout clarifying the separate Go-based `mcap` CLI is only required for legacy ROS 1 `.bag` conversion. Previously you had to grep `pyproject.toml` to see what came bundled.
- **`Performance contract` section now has a tabular "Tuning the bounds" subsection** listing every per-call OOM knob (`chunk_size`, `max_buffer_messages`, `max_lateness_ms`, `tolerance_ms`, `engine`, `force=True`) with its default and when to change, plus a separate "Hard limits (not configurable)" table for `LARGE_TOPIC_THRESHOLD` / `NUMPY_HARD_CAP`. Surfaces what was previously only in code docstrings.

## [0.4.0] — 2026-04-26

### What's new

Reliability release. No new user-facing features — every change in v0.4.0 is about fixing multiple reliability issues.

The headline change is to improve the **OOM-safe** capabilities. Memory is bounded by a configured chunk size — not by bag size, topic size, or export size — for dashboard plotting, sync, health, density, cross-bag overlay, and the streaming export formats. There's a [Performance contract](README.md#performance-contract) section in the README that states the rule plainly and points at `tests/test_streaming_oom.py` as the verification.

This is a **minor-version bump with one breaking change**: `TopicView.to_lazy_polars()` was removed and replaced with `materialize_ipc_cache()`. See the migration snippet below.

### Performance contract — OOM-safe verified

- **`stream_bucketed_minmax`** (new in `resurrector/core/streaming.py`) — single-pass time-bucketed min/max aggregation. Replaces eager `view.to_polars(); downsample_dataframe(...)` on the dashboard plot endpoint, transform preview, and cross-bag overlay. Memory is `O(num_buckets × num_columns)`, independent of topic size.
- **Streaming health checks** — `BagFrame.health_report()` now maintains a small `TopicHealthState` per topic (Welford accumulators for size stats, running counters for gaps and ordering, bounded inline issue lists capped at 100 per category) instead of accumulating per-topic timestamp lists. Bag-size memory disappears.
- **Streaming density** — `compute_density()` increments per-topic bin counters in-place rather than accumulating timestamps. ~100 KB regardless of bag size.
- **Streaming sync engine** — new `engine="streaming"` and `engine="auto"` (the default). Lookahead-window buffer for `nearest`, prev/next pair tracking for `interpolate`, single-sample carry for `sample_and_hold`. Memory bounded by `tolerance_ms × topic_rate`. The eager engine is still available as `engine="eager"`.
- **NumPy `.npz` hard-cap** — refuses topics > 1 M rows up front with `LargeTopicError` (use Parquet for larger). The format can't append; capping is more honest than a multi-GB silent spike.
- **RLDS TFRecord streaming** — uses `total_rows` from the index instead of `list(chunks)` to derive `is_last`. Memory bounded by chunk size.
- **`LargeTopicError` guards** on the eager `to_polars` / `to_pandas` / `to_numpy` (threshold 1 M messages, `force=True` to opt in). Replaces the v0.3.x silent log warning that nobody read.
- **Memory regression test suite** (`tests/test_streaming_oom.py`, marked `@pytest.mark.slow`, run via `pytest -m slow`) — 8 tests assert peak RSS delta stays within budget for every advertised-as-bounded workflow on a synthetic bag. Wired into a new CI job.
- **Tuning table in README** — the "Performance contract" section now has a tabular "Tuning the bounds" subsection listing every per-call knob (`chunk_size`, `max_buffer_messages`, `max_lateness_ms`, `tolerance_ms`, `engine`, `force=True`) with its default and a "Hard limits (not configurable)" table for `LARGE_TOPIC_THRESHOLD` / `NUMPY_HARD_CAP`. Surfaces what was previously only in docstrings.

### Streaming sync — explicit-contract design

The new streaming sync engine exposes three orthogonal policies that the eager engine doesn't need (because it has perfect global state):

```python
bf.sync(
    topics=["/joint_states", "/imu/data"],
    method="nearest",
    tolerance_ms=50,
    engine="auto",                # eager | streaming | auto
    out_of_order="error",         # error | warn_drop | reorder
    boundary="null",              # null | drop | hold | error  (interpolate only)
    max_buffer_messages=100_000,  # per-topic cap; tripped raises SyncBufferExceededError
    max_lateness_ms=0,            # watermark window for out_of_order="reorder"
)
```

Failures are surfaced as typed exceptions with actionable messages: `SyncBufferExceededError`, `SyncOutOfOrderError`, `SyncBoundaryError`, `LargeTopicError`. All live in `resurrector/core/exceptions.py`.

The streaming engine was tested for equivalence against the eager engine on 9 synthetic timing-pathology fixtures (fast-vs-slow, tie-at-anchor, missing-before-first, missing-after-last, out-of-order-within-topic, bursty-fast, sparse-no-match, duplicate-timestamps, topic-stops-halfway).

### Breaking change — `to_lazy_polars()` removed

The v0.3.x `TopicView.to_lazy_polars()` claimed temp-file cleanup happens "when the LazyFrame is dropped" but had no cleanup hook anywhere — every call leaked an Arrow IPC file in the OS temp dir. Replaced with explicit-lifecycle `materialize_ipc_cache()`:

```python
# v0.3.x — broken (leaks temp files on every call)
lazy = bf["/imu/data"].to_lazy_polars()
filtered = lazy.filter(pl.col("x") > 0).collect()

# v0.4.0 — explicit lifecycle, file deleted on block exit
with bf["/imu/data"].materialize_ipc_cache() as cache:
    filtered = cache.scan().filter(pl.col("x") > 0).collect()
```

The new `IpcCache` supports context-manager usage, idempotent `close()`, raises if `scan()` is called after close, and emits a `ResourceWarning` on `__del__` if it wasn't closed (so notebook leaks become visible instead of silent).

### Honesty fixes

- **Renamed `_compute_sha256` to `_fingerprint_fast`** in `scanner.py`. The function only hashed the first 1 MB plus the file size — fine as a fast change-detection fingerprint, dishonest as a "SHA256". The DuckDB column `bags.sha256` is renamed to `bags.fingerprint`. Users who need a real cryptographic digest pass `--full-hash` to `resurrector scan`, which populates a new nullable `bags.sha256_full` column with a real full-file SHA256.
- **DuckDB schema migration framework** (`resurrector/ingest/migrations.py`) — versioned forward-only migrations applied on first connect. The SHA rename is migration 1; existing v0.3.x indexes are upgraded transparently with row-level data preserved. Future schema changes append new migrations; never reorder, never rewrite.

### Format support

- **ROS 2 directory-format bags** — the scanner now recognizes directories containing `metadata.yaml` as ROS 2 bag candidates. Real ROS 2 bags are commonly directories with one or more `.db3` shards plus a `metadata.yaml`; the v0.3.x scanner treated each shard as a separate bag, which would index a single recording N times.
- **`convert_to_mcap` accepts directory inputs** for ROS 2 bags. Forwards the directory path to `ros2 bag convert -i` as expected.
- The mcap CLI (Go binary, distributed via Homebrew/apt/GitHub releases — not a PyPI package) is still required for `.bag` → `.mcap` conversion. `resurrector doctor` warns when it's missing. The Python `mcap` library that ships with the wheel handles all native MCAP read/write.

### CI

- **New `.github/workflows/ci.yml`** runs on every push and PR with four parallel jobs:
  - `test` — pytest matrix on Python 3.10 / 3.11 / 3.12 / 3.13 (Ubuntu)
  - `wheel-smoke` — `python -m build`, install the wheel into a fresh venv, run `resurrector --version`, `resurrector doctor`, and `pytest -m smoke`. This is the regression gate for packaging bugs like the v0.3.2 demo-import break that would have been caught here.
  - `frontend-build` — `npm ci && npm run build` in the dashboard app to catch frontend regressions
  - `lint` — `python -m compileall` syntax-error catcher (ruff/mypy is a v0.5+ project)
  - `memory-regression` — runs `pytest -m slow` against the synthetic-bag fixture
- The one-shot `backfill-latest-assets.yml` workflow (used to attach `_latest` filenames to the v0.3.2 release) was deleted.

### Other

- New `resurrector/core/exceptions.py` houses the typed exception hierarchy (`ResurrectorError`, `LargeTopicError`, `SyncBufferExceededError`, `SyncOutOfOrderError`, `SyncBoundaryError`).
- `tests/test_streaming_oom.py` introduces `@pytest.mark.slow` (excluded by default via `pyproject.toml addopts = "-m 'not slow'"`).
- `tests/test_python_api_smoke.py` introduces `@pytest.mark.smoke` for the wheel-install CI job.
- `psutil>=5.9.0` added to the `[dev]` extras for the memory regression tests.
- 415 tests passing in the default suite, 8 more under `pytest -m slow`, 423 total.

---

## [0.3.2] — 2026-04-26

### Critical fix — `resurrector demo` was broken on PyPI installs

v0.3.1 worked from source (`pip install -e .`) but the `resurrector demo` command and the dashboard's "Generate demo bag" button both crashed on PyPI installs with `ModuleNotFoundError: No module named 'tests'`. The synthetic-bag generator lived under `tests/fixtures/` which doesn't ship in the wheel.

This release moves the generator to `resurrector/demo/sample_bag.py` so it ships with the package. A back-compat shim under `tests/fixtures/generate_test_bags.py` re-exports it so historical test imports keep working.

### `resurrector --version`

Added a top-level `--version` / `-V` flag that prints the installed version and exits. Typer doesn't add this automatically.

### Other

- 348 tests passing.

---

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

### Exploration scripts (new) — 17 runnable demos covering every important feature

A new `examples/` directory with 17 standalone scripts that demo every important feature in the terminal — no docs reading required. First run auto-generates a synthetic sample bag at `~/.resurrector/explore_sample.mcap`; subsequent scripts reuse it.

**Core features (start here):**

```
examples/01_bag_frame_basics.py        — pandas-like API, time slice, message iter
examples/02_health_checks.py           — 0-100 quality score + custom HealthConfig
examples/03_multi_stream_sync.py       — nearest / interpolate / sample-and-hold
examples/04_image_video_export.py      — iter_images, PNG sequence, MP4 encode
examples/05_ml_export_formats.py       — Parquet / HDF5 / NumPy / LeRobot / RLDS
examples/06_index_search_query_dsl.py  — DuckDB index + query DSL + stale paths
examples/07_semantic_frame_search.py   — CLIP embed + cosine search + clips
examples/08_datasets_versioning.py     — versioned datasets with manifest + auto-README
examples/09_plotjuggler_bridge.py      — start bridge subprocess + connection URLs
```

**v0.3.1 power features:**

```
examples/11_density_ribbon.py          — sparkline density per topic
examples/12_trim_to_mcap.py            — trim a 2-second window 4 ways
examples/13_math_transforms.py         — derivatives + Polars expression sandbox
examples/14_cross_bag_overlay.py       — overlay two synthetic bags with per-bag offsets
examples/15_dashboard_walkthrough.py   — boots dashboard, opens browser, prints UI tour
examples/16_bookmarks_via_api.py       — programmatic CRUD on the annotations API
examples/17_jupyter_export.py          — Parquet + paste-ready Polars snippet
examples/18_polars_lazy_filter.py      — lazy filter/projection vs eager comparison
```

Each script is self-contained, runs in <10 seconds, and auto-skips with install instructions when an optional extra (CLIP, OpenCV, etc.) isn't installed. They also serve as a smoke-test suite — running them in sequence exercises every major surface of the toolkit.

### Bugfixes shipped during smoke-testing

- **`dataset_readme.py` Windows encoding crash** — README.md was written without an explicit encoding, so the cp1252 default on Windows couldn't represent the `→` and em-dashes in the auto-generated README. Forced `encoding="utf-8"` so dataset export works on every platform.

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
