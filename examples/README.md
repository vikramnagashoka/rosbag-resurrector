# Exploration Scripts

Standalone runnable scripts that demonstrate every important feature.
The goal: **`python examples/01_*.py` should give you working output you
can inspect in 30 seconds** — no docs reading required.

Every script is self-contained and uses a synthetic demo bag (created
on first run at `~/.resurrector/explore_sample.mcap`) so you don't
need your own data.

## Setup (once)

```bash
pip install -e ".[dev]"   # if running from source
# OR
pip install rosbag-resurrector

# Then verify your install:
resurrector doctor
```

## Run any script

```bash
python examples/01_bag_frame_basics.py
```

The first script you run also generates the sample bag. Subsequent
scripts reuse it.

## What each script demonstrates

### Core features (start here)

| # | Script | Feature | What you'll see |
|---|--------|---------|-----------------|
| 01 | `01_bag_frame_basics.py` | BagFrame API — pandas-like topic access | Open, list topics, slice by time, convert to Polars/Pandas, iterate raw messages |
| 02 | `02_health_checks.py` | Quality scoring (0-100) per bag | Default report, per-topic scores, issues by severity, custom HealthConfig thresholds |
| 03 | `03_multi_stream_sync.py` | Topic alignment across rates | All three sync methods (nearest / interpolate / sample-and-hold) on real topics |
| 04 | `04_image_video_export.py` | Image + video frame iteration | Iterate frames from raw + JPEG-compressed image topics, export PNGs and MP4 |
| 05 | `05_ml_export_formats.py` | ML training pipeline outputs | Same data exported to Parquet, HDF5, NumPy, LeRobot, RLDS |
| 06 | `06_index_search_query_dsl.py` | DuckDB-backed bag index | Index multiple bags, search by topic / health, query DSL, stale-path detection |
| 07 | `07_semantic_frame_search.py` | CLIP-based natural language search | Embed video frames, search by text, group results into clips |
| 08 | `08_datasets_versioning.py` | Reproducible dataset collections | Create dataset, add a version, export with manifest + auto-README |
| 09 | `09_plotjuggler_bridge.py` | WebSocket bridge for live viz | Start bridge subprocess, print connection URL, stream for 10s |

### v0.3.1 power features

| # | Script | Feature | What you'll see |
|---|--------|---------|-----------------|
| 11 | `11_density_ribbon.py` | Per-topic message density | Sparkline ribbon per topic showing where messages cluster vs drop |
| 12 | `12_trim_to_mcap.py` | Time-range trim to multiple formats | A 2-second window exported 4 ways (MCAP, Parquet, CSV, NumPy) |
| 13 | `13_math_transforms.py` | Math/transform editor backend | Common ops (derivative, MA, low-pass) + Polars expression sandbox |
| 14 | `14_cross_bag_overlay.py` | Cross-bag overlay alignment | Two bags overlaid on relative time axis with per-bag offsets |
| 15 | `15_dashboard_walkthrough.py` | Full dashboard, end-to-end | Boots dashboard, opens browser, prints UI tour |
| 16 | `16_bookmarks_via_api.py` | Annotations REST API | Programmatic CRUD on bag bookmarks |
| 17 | `17_jupyter_export.py` | Parquet + Polars snippet | Writes a Parquet file plus a paste-ready snippet |
| 18 | `18_polars_lazy_filter.py` | Lazy Polars query optimization | Lazy filter / projection vs eager comparison |

## Why these exist

Reading docs about features is one thing; seeing them produce output
in your own terminal is another. These scripts also serve as a
ground-truth "is anything broken on a fresh install" smoke test —
running them in sequence exercises every major surface of the toolkit.

If a script fails, please open an issue with the full traceback;
something in your environment doesn't match the assumptions in
`resurrector doctor`.

## Tips

- Scripts that need optional extras (`07_semantic_frame_search.py`,
  parts of `04_image_video_export.py` and `05_ml_export_formats.py`)
  auto-skip with install instructions when the dependency is missing.
- Scripts write outputs under `./_exploration_output/` so you can
  inspect what got produced. Safe to delete between runs.
- The dashboard walkthrough (`15`) starts a long-running subprocess.
  Press Ctrl+C in the terminal to stop it.
- The bridge demo (`09`) runs for ~10 seconds and shuts down cleanly.
- Want the same flow from the CLI? Many of these have a
  `resurrector` subcommand equivalent — `resurrector --help` lists
  them all.
