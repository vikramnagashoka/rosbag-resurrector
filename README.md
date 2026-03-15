# RosBag Resurrector

**Stop letting your rosbag data rot. Analyze it.**

A pandas-like data analysis tool for robotics bag files with automatic quality validation, multi-stream synchronization, ML-ready export, and an interactive web dashboard.

> "We have terabytes of rosbag data and no good way to work with it after recording. Every time someone wants to analyze something, they write throwaway scripts to convert to CSV. Most bags never get analyzed at all."
>
> — [The Rosbag Graveyard](https://discourse.ros.org/), a shared frustration across the robotics community

## Quick Start

```bash
pip install rosbag-resurrector
```

```python
from resurrector import BagFrame

# Load a bag (lazy — doesn't read all data into memory)
bf = BagFrame("experiment.mcap")

# See what's inside
bf.info()

# Get IMU data as a Polars DataFrame
imu_df = bf["/imu/data"].to_polars()

# Or as Pandas
imu_pd = bf["/imu/data"].to_pandas()

# Synchronize multiple topics by timestamp
synced = bf.sync(["/imu/data", "/joint_states", "/camera/rgb"],
                 method="nearest", tolerance_ms=50)

# Export to ML-ready formats
bf.export(topics=["/imu/data", "/joint_states"],
          format="parquet", output="training_data/", sync=True)
```

## Features

### Automatic Health Checks

Every bag gets a quality score (0-100) detecting real-world problems:

- **Dropped messages** — catches the classic rosbag buffer overflow
- **Time gaps** — detects sensor disconnects and recording interruptions
- **Out-of-order timestamps** — flags clock sync issues
- **Partial topics** — topics that don't span the full recording
- **Message size anomalies** — sudden changes indicating corruption or config changes

```python
report = bf.health_report()
# Health Score: 87/100
# /lidar/points has 47 gaps > 200ms
# Recommendation: increase buffer size or reduce recording frequency
```

### Pandas-Like API

Work with robotics data the way you work with any tabular data:

```python
# Select topics
imu = bf["/imu/data"]
joints = bf["/joint_states"]

# Time slicing
segment = bf.time_slice("10s", "30s")

# Get as DataFrame with flattened columns
df = imu.to_polars()
# Columns: timestamp_ns, linear_acceleration.x, .y, .z,
#           angular_velocity.x, .y, .z, orientation.x, .y, .z, .w
```

### Multi-Stream Synchronization

Topics publish at independent rates. Resurrector aligns them:

```python
# Nearest-timestamp matching
synced = bf.sync(["/imu/data", "/joint_states"], method="nearest", tolerance_ms=50)

# Linear interpolation for numeric streams
synced = bf.sync(["/imu/data", "/joint_states"], method="interpolate")

# Sample-and-hold for slow topics
synced = bf.sync(["/imu/data", "/camera/rgb"], method="sample_and_hold")
```

### ML-Ready Export

Export directly to the formats your training pipeline expects:

```python
bf.export(topics=["/imu/data", "/joint_states"],
          format="parquet",    # Also: hdf5, csv, numpy, zarr
          sync=True,
          downsample_hz=10)
```

| Format | Best For |
|--------|----------|
| Parquet | Tabular sensor data, Spark/Polars pipelines |
| HDF5 | Mixed numeric/image data, MATLAB compatibility |
| NumPy (.npz) | Jupyter notebook workflows |
| CSV | Quick inspection, sharing with non-technical team members |
| Zarr | Cloud-native, chunked, very large datasets |

### Robotics Transforms

Common operations built in:

```python
from resurrector.core.transforms import quaternion_to_euler, add_euler_columns

# Add roll/pitch/yaw from quaternion columns
df = add_euler_columns(imu_df, prefix="orientation")

# Laser scan to Cartesian coordinates
from resurrector.core.transforms import laser_scan_to_cartesian
points = laser_scan_to_cartesian(ranges, angle_min, angle_max)

# Temporal downsampling
from resurrector.core.transforms import downsample_temporal
df_10hz = downsample_temporal(df, target_hz=10)
```

### Interactive Web Dashboard

```bash
resurrector dashboard --port 8080
```

- **Library** — Browse, search, and filter all indexed bags
- **Explorer** — Deep-dive into topics with interactive plots
- **Health** — Visual quality reports with recommendations
- **Compare** — Side-by-side bag comparison

### Searchable Index

DuckDB-powered index for fast queries across your entire bag collection:

```python
from resurrector import search

results = search("topic:/camera/rgb health:>80 after:2025-01")
```

## CLI Reference

```bash
# Scan and index a directory
resurrector scan /path/to/bags/

# Show bag info
resurrector info experiment.mcap

# Health check
resurrector health experiment.mcap
resurrector health /path/to/bags/ --format json --output report.json

# List indexed bags with filtering
resurrector list --after 2025-01-01 --has-topic /camera/rgb --min-health 70

# Export
resurrector export experiment.mcap \
  --topics /imu/data /joint_states \
  --format parquet \
  --sync nearest \
  --output ./training_data/

# Compare two bags
resurrector diff bag1.mcap bag2.mcap

# Tag bags for organization
resurrector tag experiment.mcap --add "task:pick_and_place" "robot:digit"

# Launch web dashboard
resurrector dashboard --port 8080
```

## Comparison

| Feature | Resurrector | Foxglove | PlotJuggler | rosbag2_py |
|---------|------------|----------|-------------|------------|
| Automatic health checks | Yes | No | No | No |
| Pandas/Polars API | Yes | No | No | Partial |
| Multi-stream sync | Yes | Visual only | Visual only | No |
| ML-ready export | Yes | No | CSV only | No |
| Web dashboard | Yes | Yes (paid) | No | No |
| No ROS install needed | Yes | Yes | Needs ROS | Needs ROS |
| DuckDB search index | Yes | No | No | No |
| Batch processing | Yes | No | No | Yes |

## Supported Formats

| Format | Extension | Status |
|--------|-----------|--------|
| MCAP (ROS2 default) | `.mcap` | Fully supported |
| ROS1 bag | `.bag` | Planned (`pip install rosbag-resurrector[ros1]`) |
| ROS2 SQLite | `.db3` | Planned |

## Architecture

```
resurrector/
  ingest/          # Scanner, parser, indexer, health checks
  core/            # BagFrame, sync engine, transforms, export
  cli/             # Typer CLI with Rich formatting
  dashboard/       # FastAPI backend + React frontend
```

**Design principles:**
1. **Lazy by default** — never loads full bags into memory
2. **Batteries included** — health checks, sync, transforms, export with zero config
3. **Escape hatches** — `.to_polars()` / `.to_pandas()` / `.to_numpy()` to drop into familiar tools
4. **ROS-aware but not ROS-dependent** — parses MCAP directly, no ROS installation needed
5. **Fast** — Polars for processing, DuckDB for queries, lazy evaluation

## Development

```bash
git clone https://github.com/your-org/rosbag-resurrector.git
cd rosbag-resurrector
pip install -e ".[dev]"

# Generate test bags
python tests/fixtures/generate_test_bags.py

# Run tests
pytest tests/ -v

# Build dashboard frontend
cd resurrector/dashboard/app
npm install && npm run build
```

## Contributing

Contributions welcome! Key extension points:

- **New export formats**: Add a method to `resurrector/core/export.py`
- **New health checks**: Add a method to `resurrector/ingest/health_check.py`
- **New transforms**: Add to `resurrector/core/transforms.py`
- **ROS1 support**: Implement a `ROS1Parser` in `resurrector/ingest/parser.py`

## License

MIT
