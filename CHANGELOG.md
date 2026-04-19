# Changelog

All notable changes to RosBag Resurrector are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to [Semantic Versioning](https://semver.org/).

Each release has a **What's New** one-liner summary followed by feature lists grouped by category so readers can scan changes without reading diffs.

---

## [Unreleased]

### Planned for 0.3.0 — "Unified dashboard"

- Wire unfinished UI components (ExportDialog, SyncView, ImageViewer) into the main dashboard flow
- Plotly-based Explorer with linked cursors, brushing, and annotations
- Semantic frame search page in the dashboard
- Datasets management page
- Bridge control panel

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
