# Exploration Scripts

Standalone runnable scripts that demonstrate every v0.3.1 feature. The
goal: **`python examples/01_*.py` should give you working output you can
inspect in 30 seconds** — no docs reading required.

Every script is self-contained and uses the synthetic demo bag (created
on first run) so you don't need your own data.

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
python examples/01_density_ribbon.py
```

The first script you run also generates a small sample bag at
`~/.resurrector/explore_sample.mcap`. Subsequent scripts reuse it.

## What each script demonstrates

| # | Script | Feature | What you'll see |
|---|--------|---------|-----------------|
| 01 | `01_density_ribbon.py` | Per-topic message-density histograms | A text ribbon per topic showing where messages cluster vs. drop |
| 02 | `02_trim_to_mcap.py` | Time-range trim to MCAP / Parquet / CSV | Output files appear in `./_exploration_output/` and are re-openable |
| 03 | `03_math_transforms.py` | Common math ops + Polars expressions | Original vs. derivative vs. moving-average plotted as ASCII spark lines |
| 04 | `04_cross_bag_overlay.py` | Cross-bag overlay of one topic | Two synthetic bags overlaid on a relative time axis |
| 05 | `05_dashboard_walkthrough.py` | Dashboard end-to-end | Boots the dashboard, opens your browser to the right page, prints what to click |
| 06 | `06_bookmarks_via_api.py` | Annotations REST API | Creates 3 bookmarks programmatically, lists, deletes |
| 07 | `07_jupyter_export.py` | Jupyter-friendly Parquet export | Writes a Parquet file + prints the snippet to copy into a notebook |
| 08 | `08_polars_lazy_filter.py` | Lazy polars on a giant topic | Demonstrates filter pushdown without loading the whole topic |

## Why these exist

Reading docs about features is one thing; seeing them produce output
in your own terminal is another. These scripts also serve as a
ground-truth "is anything broken on a fresh install" smoke test —
running all 8 in sequence exercises every v0.3.1 surface.

If a script fails, please open an issue with the full traceback;
something in your environment doesn't match the assumptions in
`resurrector doctor`.
