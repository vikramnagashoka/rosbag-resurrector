# RosBag Resurrector — System Architecture & Shipped Features

> Status as of 2026-04-18 — v0.2.0 released, ~6,500 lines of Python across 22 modules, 164+ tests, packaging + distribution live.

This document captures the architecture of the system, every feature shipped to date, and the deferred work remaining. It's the baseline for CEO / engineering plan reviews on what to build next.

---

## 1. Product Goal

**Problem:** Robotics teams record terabytes of rosbag data during experiments, then dump it on a NAS and never analyze it again. The tooling to work with bag files hasn't meaningfully evolved since 2015. ROS-based workflows require a full ROS install, custom scripts, and manual scrubbing to find anything useful.

**Solution:** A pandas-like Python library + web dashboard + WebSocket bridge that treats rosbags as queryable, searchable, ML-ready datasets. **No ROS installation required.** Works on Linux, macOS, Windows via `pip install`.

**Primary persona:** Robotics engineers and ML researchers working with ROS 2 / MCAP bags who need to analyze, validate, search, and export data for training pipelines.

---

## 2. System Architecture

```
                    ┌──────────────────────────────────────────────┐
                    │                   USER                        │
                    │   (CLI, Python API, Web Dashboard, PlotJug)  │
                    └───┬──────────┬─────────┬──────────┬──────────┘
                        │          │         │          │
                  ┌─────▼───┐ ┌────▼────┐ ┌──▼───┐ ┌────▼──────┐
                  │   CLI   │ │  Python │ │ Dash │ │  Bridge    │
                  │ (Typer) │ │   API   │ │board │ │(WebSocket) │
                  └─────┬───┘ └────┬────┘ └──┬───┘ └────┬──────┘
                        │          │         │          │
                        └──────────┼─────────┼──────────┘
                                   │         │
                    ┌──────────────▼─────────▼───────────────┐
                    │          CORE LAYER                    │
                    │  BagFrame · Sync · Transforms · Export │
                    │  Dataset · TopicGroups · Vision (CLIP) │
                    └──────────────┬─────────────────────────┘
                                   │
                    ┌──────────────▼─────────────────────────┐
                    │         INGEST LAYER                   │
                    │  Scanner · Parser · Indexer · Health   │
                    └──────────────┬─────────────────────────┘
                                   │
                    ┌──────────────▼─────────────────────────┐
                    │         STORAGE                        │
                    │  MCAP files + DuckDB index             │
                    │  (~/.resurrector/index.db)             │
                    └────────────────────────────────────────┘
```

### Layer responsibilities

**Ingest layer** — find bag files, parse them, compute health scores, maintain the DuckDB index.

**Core layer** — the analytical primitives: BagFrame, sync, transforms, export, datasets, topic grouping, semantic search.

**Interface layer** — four distinct user surfaces that all wrap the core: CLI, Python API, web dashboard, WebSocket bridge.

**Storage** — bag files stay on disk (we never copy). A DuckDB index at `~/.resurrector/index.db` holds metadata and CLIP embeddings.

### Directory layout

```
resurrector/
├── __init__.py              # Public API surface
├── logging_config.py        # Structured logging
├── ingest/                  # "Find, parse, score, index"
│   ├── scanner.py           # (92 loc) walk filesystem, find .mcap/.bag/.db3
│   ├── parser.py            # (473 loc) MCAPParser, BagMetadata, Message
│   ├── indexer.py           # (473 loc) DuckDB index, stale detection
│   └── health_check.py      # (541 loc) HealthChecker, HealthConfig, scoring
├── core/                    # "Analyze, transform, export"
│   ├── bag_frame.py         # (528 loc) BagFrame, TopicView, Jupyter repr
│   ├── sync.py              # (205 loc) nearest/interpolate/sample-and-hold
│   ├── transforms.py        # (161 loc) quaternion→euler, laser scan, downsample
│   ├── export.py            # (314 loc) Parquet/HDF5/CSV/NumPy/Zarr
│   ├── dataset.py           # (340 loc) DatasetManager, BagRef, versioned datasets
│   ├── dataset_readme.py    # (192 loc) auto-generate dataset README
│   ├── topic_groups.py      # (110 loc) Perception/State/Nav/Control classifier
│   ├── query.py             # (32 loc) search() top-level helper
│   └── vision.py            # (558 loc) FrameSampler, CLIPEmbedder, search engine
├── cli/                     # Typer CLI + Rich formatting
│   ├── main.py              # (770 loc) all commands
│   └── formatters.py        # (259 loc) Rich tables, sparklines, output styles
├── dashboard/               # FastAPI + React
│   ├── api.py               # (545 loc) REST + SSE endpoints
│   ├── static/              # built React bundle
│   └── app/                 # React + Vite source
└── bridge/                  # PlotJuggler WebSocket bridge
    ├── protocol.py          # (101 loc) PlotJuggler flat JSON format
    ├── buffer.py            # (102 loc) thread-safe ring buffer
    ├── playback.py          # (198 loc) recorded bag replay engine
    ├── live.py              # (210 loc) rclpy → WebSocket relay
    └── server.py            # (272 loc) FastAPI REST + WebSocket endpoint
```

Total: **~6,500 lines Python**, 22 modules, 19 test suites (164+ tests).

---

## 3. Core Abstraction: BagFrame

The BagFrame is the central abstraction — a pandas-like handle on a bag file.

```
  BagFrame("experiment.mcap")           ← lazy (metadata only)
        │
        ├── .info()                     ← print summary
        ├── .health_report()            ← HealthReport (scores + issues)
        ├── .topic_names                ← list all topics
        ├── .time_slice("10s", "30s")   ← returns BagFrame scoped to window
        ├── .sync([topics], method=)    ← align streams by timestamp
        ├── .export(topics=, format=)   ← Parquet/HDF5/CSV/NumPy/Zarr
        │
        └── bf["/imu/data"]             ← TopicView
               ├── .to_polars()          ← Polars DataFrame (flattened)
               ├── .to_pandas()          ← Pandas DataFrame
               ├── .to_numpy()           ← numpy array
               ├── .iter_messages()      ← message stream
               ├── .iter_images()        ← (timestamp, np.ndarray) for image topics
               └── .is_image_topic       ← bool
```

**Design principles (enforced across the codebase):**
1. **Lazy by default** — never load a full bag into memory.
2. **Batteries included** — health checks, sync, transforms, export all work with zero config.
3. **Escape hatches** — `.to_polars()` / `.to_pandas()` / `.to_numpy()` drop users into familiar tools.
4. **ROS-aware but not ROS-dependent** — parses MCAP directly; no ROS install needed.
5. **Fast** — Polars for processing, DuckDB for queries, lazy evaluation throughout.
6. **Reproducible** — versioned datasets with manifests (SHA256) and auto-generated docs.

---

## 4. Features Shipped

### 4.1 Format support — Ingest

| Format | Ext | Status | Notes |
|--------|-----|--------|-------|
| **MCAP (ROS 2 default)** | `.mcap` | ✅ Fully supported | Primary format, optimized path |
| ROS 2 SQLite | `.db3` | ❌ Stub (raises) | Users convert with `ros2 bag convert` |
| ROS 1 bag | `.bag` | ❌ Stub (raises) | Users convert with `mcap convert` |

MCAP parsing is schema-aware via `mcap-ros2-support`. Messages are decoded to nested dicts, timestamps preserved in nanoseconds.

### 4.2 Automatic health checks

Every bag gets a **0–100 quality score**. Detectors in `HealthChecker` (`resurrector/ingest/health_check.py`):

- **Dropped messages** — rate drop vs. declared publication rate
- **Time gaps** — gaps larger than `gap_multiplier × expected_period`
- **Out-of-order timestamps** — clock sync / replay issues
- **Partial topics** — topics that don't span the full recording
- **Message size anomalies** — sudden size changes indicating corruption

Thresholds are **configurable** via `HealthConfig`:
```python
HealthConfig(
    rate_drop_threshold=0.25,      # 25% drop before flagging
    gap_multiplier=2.0,             # 2x period for gap detection
    completeness_threshold=0.05,    # 5% start/end delay tolerance
    size_deviation_threshold=0.5,   # 50% size variance tolerance
)
```

Tests: 7 base + 5 config edge-case tests.

### 4.3 Multi-stream synchronization

Three methods in `resurrector/core/sync.py`:

```
  Method          | Best for                       | How it works
  ----------------|--------------------------------|---------------------------
  nearest         | Event-driven, discrete topics  | Match by closest timestamp
  interpolate     | Numeric streams (IMU, joints)  | Linear interpolation
  sample_and_hold | Slow topics (camera, config)   | Carry forward last value
```

Tolerance-based matching on `nearest` (`tolerance_ms` parameter). Tests: 6 covering all three methods + edge cases.

### 4.4 ML-ready export

Handled by `resurrector/core/export.py`:

| Format | Extension | Use case |
|--------|-----------|----------|
| Parquet | `.parquet` | Spark/Polars pipelines, tabular sensor data |
| HDF5 | `.h5` | Mixed numeric+image, MATLAB compatibility |
| NumPy | `.npz` | Jupyter workflows |
| CSV | `.csv` | Quick inspection |
| Zarr | `.zarr/` | Cloud-native, chunked, very large datasets |
| **LeRobot** | dataset/ | Hugging Face LeRobot training pipelines |
| **RLDS** | `.tfrecord` | OpenX / RT-2 / robotic foundation models |

**OOM-safe streaming**: every export format streams chunk-by-chunk via the new `TopicView.iter_chunks()` primitive. Peak memory is bounded by chunk size (50k rows by default) regardless of topic size. Tests: 8 (existing) + 13 (streaming) + 6 (LeRobot/RLDS) = 27.

### 4.5 Video & image support

Both `sensor_msgs/Image` and `sensor_msgs/CompressedImage` are decoded. `iter_images()` yields `(timestamp_ns, np.ndarray)`. CLI exports:

```bash
resurrector export-frames bag.mcap --topic /camera/rgb --output ./frames   # PNGs
resurrector export-frames bag.mcap --topic /camera/rgb --video --output out.mp4 --fps 30
```

Uses OpenCV for decoding (compressed) and MP4 encoding. Tests: 7 (CompressedImage CDR parsing) + 5 (export).

### 4.6 Semantic frame search (CLIP)

Indexes video frames at 5Hz into DuckDB as 512-dim CLIP embeddings. Searches via cosine similarity.

```
  ┌──────────┐      ┌─────────────┐      ┌──────────┐      ┌──────────┐
  │  bag.mcap│──┬──▶│ FrameSampler│─────▶│ CLIPEmbed│─────▶│ DuckDB   │
  │  (video) │  │   │   (5 Hz)    │      │   (ViT)  │      │ frames + │
  └──────────┘  │   └─────────────┘      └──────────┘      │ embeds   │
                │                                           └─────┬────┘
                │                                                 │
  "robot misses catch" ──▶ CLIPEmbed ──▶ cosine sim search ◀─────┘
                                                │
                                                ▼
                                      top-k frames / temporal clips
```

Two backends:
- `[vision]` — local CLIP via `sentence-transformers` (~2GB model)
- `[vision-openai]` — OpenAI embedding API (lighter install, API key required)
- `[vision-lite]` — just image parsing + video export (no ML)

Tests: 8 (auto-skip when ML deps absent).

### 4.7 Resurrector Bridge (PlotJuggler-compatible)

WebSocket bridge that streams bag data in PlotJuggler's flat JSON format. Two modes:

```
  PLAYBACK MODE                       LIVE MODE
  ─────────────                       ──────────
  bag.mcap                            ROS 2 DDS network
      │                                    │
      ▼                                    ▼
  PlaybackEngine              rclpy subscription (requires rclpy)
  (speed 0.1x–20x,                         │
   play/pause/seek)                        ▼
      │                              LiveEngine
      ▼                                    │
  RingBuffer◀─────────────────────────────┘
      │
      ├─▶ WebSocket clients (PlotJuggler, Plotly viewer)
      └─▶ REST API (play/pause/seek/speed/status)
```

REST endpoints: `/api/playback/{play,pause,seek,speed}`, `/api/topics`, `/api/status`. WebSocket at `/ws`. Ring buffer is thread-safe and multi-consumer.

Tests: 6 protocol + 7 buffer + 6 playback + 6 server = **25 bridge tests**.

### 4.8 Reproducible datasets

Versioned dataset collections for ML pipelines. `DatasetManager` in `resurrector/core/dataset.py`.

```
  DatasetManager
      │
      ├── create("pick-and-place-v1")
      ├── create_version(
      │     bag_refs=[BagRef, ...],      ← which bags + topics + time windows
      │     sync_config=SyncConfig(...),
      │     export_format="parquet",
      │     downsample_hz=50,
      │     metadata=DatasetMetadata(    ← license, robot_type, task, tags
      │       ...))
      └── export_version(output_dir)
            │
            ▼
      output_dir/
      ├── data/*.parquet
      ├── manifest.json            ← SHA256 of every file
      ├── dataset_config.json      ← full recreation config
      └── README.md                ← auto-generated docs + load snippet
```

Tests: 14.

### 4.9 Smart topic grouping

`classify_topics()` buckets topic names into semantic groups:

```
  Perception   → /camera/*, /lidar/*, /depth/*
  State        → /imu/*, /joint_states, /odom
  Navigation   → /cmd_vel, /map, /path, /goal_pose
  Control      → /controller/*, /effort/*
  Transforms   → /tf, /tf_static
  Diagnostics  → /rosout, /diagnostics
```

Custom patterns override built-ins. Tests: 12.

### 4.10 DuckDB searchable index

`~/.resurrector/index.db` stores:
- `bags` — path, duration, start/end time, health score, message count
- `topics` — per-bag topic catalog with types and message counts
- `frames` — CLIP embeddings for semantic search
- `tags` — user-applied organizational tags
- `datasets` + `dataset_versions` — dataset manager metadata

Features:
- **Stale detection** — `validate_paths()` finds moved/deleted bags, `remove_stale()` cleans up
- **Query DSL** — `search("topic:/camera/rgb health:>80 after:2025-01")`
- **Incremental indexing** — only new bags get processed on re-scan

### 4.11 CLI (Typer + Rich)

22 commands across `scan`, `quicklook`, `info`, `health`, `list`, `export`, `diff`, `tag`, `watch`, `dataset`, `index-frames`, `search-frames`, `export-frames`, `bridge`, `dashboard`.

Key UX:
- **Rich output** — sparklines for time distributions, colored health badges, grouped topic tables
- **Async scan with SSE** — real-time progress in dashboard
- **Watch mode** — auto-index new bags as they appear in a directory

Tests: 14.

### 4.12 Web dashboard (FastAPI + React)

REST + Server-Sent Events backend, React + Vite frontend. Pages:
- **Library** — browse, filter, search all indexed bags
- **Explorer** — plot topics, time-slice, sync multiple streams
- **Health** — visual reports with recommendations
- **Compare** — side-by-side bag comparison

Backend: 545 lines in `dashboard/api.py`. Tests: 13 (FastAPI endpoints).

### 4.13 Packaging & distribution

Source-protected binary distribution via PyArmor + PyInstaller:
- **macOS DMG** — built via `packaging/macos/create_dmg.sh`
- **Ubuntu DEB** — built via `packaging/ubuntu/build_deb.sh`
- **GitHub Actions** — `.github/workflows/build-packages.yml` triggers on version tag push, creates draft Release with DMG + DEB attached

PyPI: `pip install rosbag-resurrector` (source, not obfuscated).

---

## 5. Test Coverage Summary

```
  test_integration          5    Full pipeline: scan → index → health → sync → export
  test_cli                 14    All CLI commands
  test_api                 13    FastAPI endpoints
  test_dataset             14    Dataset manager
  test_bag_frame           13    BagFrame API
  test_ingest              17    Scanner, parser, indexer
  test_sync                 6    Three sync methods
  test_health               7    Health checks, recommendations
  test_health_config        5    Configurable thresholds
  test_export               8    Five export formats
  test_topic_groups        12    Classification, custom patterns
  test_compressed_image     7    CDR parsing, iter_images
  test_export_frames        5    PNG/JPEG/MP4 export
  test_vision               8    Frame search (auto-skip when deps absent)
  test_bridge_protocol      6    PlotJuggler JSON format
  test_bridge_buffer        7    Ring buffer (overflow, threading)
  test_bridge_playback      6    Playback engine
  test_bridge_server        6    Bridge REST API
  ─────────────────────────────
  TOTAL                   164+
```

Fixtures: `tests/fixtures/generate_test_bags.py` produces synthetic MCAP files with realistic sensor data (IMU, lidar, camera, joint states).

---

## 6. Deferred / Not Yet Built

Items that exist as stubs or were considered and postponed:

| Item | Status | Why deferred |
|------|--------|--------------|
| ROS 2 `.db3` (SQLite) parser | Stub — raises error | Users can convert via `ros2 bag convert`; low priority |
| ROS 1 `.bag` parser | Stub — raises error | `rosbags` dep is heavy; users can `mcap convert` |
| Streaming export for HDF5/Zarr | Not implemented | Only Parquet streams today; OOM risk on 100k+ topics |
| Dataset split generators (train/val/test) | Not implemented | Users do this externally |
| ~~RLDS / LeRobot export formats~~ | ✅ DONE | Built as part of pre-launch hardening (2026-04-18) |
| Live ROS 1 bridge (via rosbags) | Not implemented | Only ROS 2 live bridge works |
| Structured eval harness for health thresholds | Not implemented | Thresholds are currently "trust the defaults" |
| Distributed indexing | Not implemented | Single-machine only, OK for now |
| Auth / multi-tenant dashboard | Not implemented | Single-user local tool |

**~~NOTE~~ RESOLVED:** RLDS and LeRobot exports are now implemented (see [resurrector/core/export.py](resurrector/core/export.py)). The marketing copy is now truthful.

---

## 7. Distribution Posture

Shipped for user acquisition:
- GitHub repo: public, topics set, MIT license, demo GIF, full README
- PyPI package: `rosbag-resurrector` v0.2.0
- GitHub Releases: draft DMG + DEB artifacts per tag
- Marketing plan: `marketing/launch_plan.md` — HN + Reddit + Twitter rollout prepared, not yet executed

Open question: **is the current feature set enough to launch**, or does something critical need to land first (e.g., RLDS/LeRobot export that marketing is already claiming, or `.db3` support for users with older ROS 2 bags)?

---

## 8. Open Strategic Questions (for CEO-level review)

1. **Truthfulness gap:** Marketing materials claim RLDS + LeRobot export, but these aren't implemented. Build them now, or correct the copy?
2. **ROS 1 vs. ROS 2 priority:** `.bag` is a stub. Which audience — legacy ROS 1 teams, or new ROS 2 teams — do we win first?
3. **Hosted vs. local:** The tool is 100% local today. Is there a hosted/SaaS version that would move the business further, faster?
4. **Platform potential:** CLIP search is the "wow" feature. Should we double down on semantic search as the wedge (e.g., build a searchable catalog service) or stay a general-purpose data tool?
5. **Launch timing:** Do we ship now with current features, or hold for one more release that closes the RLDS/LeRobot gap?

These are the questions `plan-ceo-review` is actually well-suited to stress-test.
