"""3D scene primitives — TF tree + PointCloud2 decoder (v0.5.0).

Demonstrates the building blocks behind the dashboard's new Scene tab:

  - ``parse_tf_message()`` — decode ``tf2_msgs/TFMessage`` CDR into a
    list of :class:`Transform` records
  - ``TFTree`` — accumulate transforms, lookup at a timestamp, walk
    parent chains via lowest-common-ancestor composition
  - ``parse_pointcloud2_meta()`` + ``decode_pointcloud2_xyz()`` —
    decode ``sensor_msgs/PointCloud2`` CDR + extract Nx3 float32 points
    with decimation and NaN filtering

The default sample bag has TF + PointCloud2 schemas registered but
no actual messages — the test fixture generator in
``tests/fixtures/scene_bag.py`` writes a tiny scene-rich bag, which
this example reuses.

Run:
    python examples/23_scene_tf_and_pointcloud.py

What you'll see: a 3-frame TF tree (world → base_link → camera_link)
resolved at three different timestamps, then a 100-point cloud
decoded + decimated to 25 points.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

from _common import ensure_output_dir, header, section

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from resurrector.core.scene import (
    TFTree,
    decode_pointcloud2_xyz,
    parse_pointcloud2_meta,
    parse_tf_message,
)
from resurrector.ingest.parser import parse_bag


def main() -> None:
    header("23 — v0.5.0: TF tree + PointCloud2 decoder")

    out = ensure_output_dir()
    scene_bag = out / "v05_scene_demo.mcap"
    if not scene_bag.exists():
        print(f"  Generating scene bag at {scene_bag}...")
        from tests.fixtures.scene_bag import generate_scene_bag
        generate_scene_bag(scene_bag)
        print(f"  [OK] Wrote {scene_bag.stat().st_size // 1024} KB scene bag\n")
    else:
        print(f"  Reusing scene bag at {scene_bag}\n")

    parser = parse_bag(scene_bag)
    meta = parser.get_metadata()
    print(f"  topics: {[ti.name for ti in meta.topics]}")

    # ----------------------------------------------------------------
    # TF tree
    # ----------------------------------------------------------------
    section("Build a TFTree by walking /tf and /tf_static")
    tree = TFTree()
    for topic, is_static in (("/tf_static", True), ("/tf", False)):
        for msg in parser.read_messages(topics=[topic]):
            if msg.raw_data is None:
                continue
            for tf in parse_tf_message(msg.raw_data, is_static=is_static):
                tree.add(tf)

    print(f"  frames: {sorted(tree.frames())}")
    print(f"  roots:  {tree.root_frames()}")
    print(f"  static edges: {len(tree._static)}   "
          f"dynamic edges: {sum(len(h) for h in tree._dynamic.values())}")

    section("Resolve world ← base_link at three timestamps")
    base_ns = meta.start_time_ns
    for label, t_ns in (
        ("t = bag start (i=0)", base_ns),
        ("t = bag start + 250 ms", base_ns + 250_000_000),
        ("t = bag start + 400 ms (latest dynamic)", base_ns + 400_000_000),
    ):
        m = tree.chain("world", "base_link", t_ns)
        if m is None:
            print(f"  {label}: (chain unresolved)")
        else:
            x, y, z = m[0:3, 3]
            print(f"  {label}:  translation = ({x:.3f}, {y:.3f}, {z:.3f})")

    section("Sibling chain: camera_link ← world (composes static + dynamic)")
    m = tree.chain("camera_link", "world", base_ns + 200_000_000)
    if m is not None:
        np.set_printoptions(precision=3, suppress=True)
        print(f"  4×4 matrix:\n{m}")

    # ----------------------------------------------------------------
    # PointCloud2
    # ----------------------------------------------------------------
    section("Decode the first PointCloud2 message")
    pc_msg = next(
        (m for m in parser.read_messages(topics=["/lidar/points"]) if m.raw_data),
        None,
    )
    assert pc_msg is not None, "scene fixture should always have a pointcloud"

    pc_meta = parse_pointcloud2_meta(pc_msg.raw_data)
    assert pc_meta is not None
    print(f"  frame_id:    {pc_meta.frame_id}")
    print(f"  height×width: {pc_meta.height}×{pc_meta.width}  "
          f"({pc_meta.height * pc_meta.width} points)")
    print(f"  point_step:  {pc_meta.point_step} bytes")
    print(f"  fields:      {[f['name'] for f in pc_meta.fields]}")

    section("Full decode → Nx3 float32 array")
    pts = decode_pointcloud2_xyz(pc_msg.raw_data, pc_meta)
    print(f"  shape: {pts.shape}   dtype: {pts.dtype}")
    print(f"  bbox:  x ∈ [{pts[:, 0].min():.2f}, {pts[:, 0].max():.2f}]   "
          f"y ∈ [{pts[:, 1].min():.2f}, {pts[:, 1].max():.2f}]   "
          f"z ∈ [{pts[:, 2].min():.2f}, {pts[:, 2].max():.2f}]")

    section("Decimated decode (max_points=25)")
    pts_small = decode_pointcloud2_xyz(pc_msg.raw_data, pc_meta, max_points=25)
    print(f"  shape: {pts_small.shape}   "
          f"(stride = {pc_meta.height * pc_meta.width // pts_small.shape[0]})")

    print(
        "\n  ✓ TFTree.lookup_at() picks the time-nearest dynamic sample;\n"
        "    static transforms (from /tf_static) always win for their child.\n"
        "  ✓ chain(target, source, time_ns) resolves via lowest-common-\n"
        "    ancestor walk + per-edge composition — handles parent / child\n"
        "    / sibling / inverse cases without bookkeeping in the caller.\n"
        "  ✓ PointCloud2 decoder uses field-offset stride reads, so an\n"
        "    intensity / RGB-augmented cloud still decodes cleanly to\n"
        "    just (x, y, z) at native float32.\n"
    )


if __name__ == "__main__":
    main()
