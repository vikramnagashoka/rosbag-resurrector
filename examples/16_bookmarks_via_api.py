"""Annotations REST API — programmatic bookmarks on a bag.

Demonstrates: ``BagIndex.add_annotation / list_annotations / update / delete``.

Run:
    python examples/06_bookmarks_via_api.py

What you'll see: three bookmarks created, listed, updated, and deleted —
all from Python. Same operations the dashboard's BookmarksPanel uses
under the hood.
"""

from __future__ import annotations

from _common import ensure_sample_bag, header, section

from resurrector.core.bag_frame import BagFrame
from resurrector.ingest.indexer import BagIndex
from resurrector.ingest.parser import parse_bag
from resurrector.ingest.scanner import scan_path


def main() -> None:
    header("06 — Bookmarks via the API")
    bag_path = ensure_sample_bag()

    # Index the bag so we have a bag_id to attach bookmarks to.
    print(f"  Indexing {bag_path.name}...")
    index = BagIndex()
    scanned = scan_path(bag_path)[0]
    parser = parse_bag(bag_path)
    metadata = parser.get_metadata()
    bag_id = index.upsert_bag(scanned, metadata)
    bf = BagFrame(bag_path)
    index.update_health_score(bag_id, bf.health_report().score)
    print(f"  ✓ bag_id = {bag_id}\n")

    # Compute three timestamps inside the bag's time window.
    start_ns = metadata.start_time_ns
    duration_ns = metadata.end_time_ns - metadata.start_time_ns
    bookmark_specs = [
        (start_ns + duration_ns * 1 // 4, "robot starts moving", "/joint_states"),
        (start_ns + duration_ns * 2 // 4, "spike on imu accel", "/imu/data"),
        (start_ns + duration_ns * 3 // 4, "global note", None),
    ]

    section("Create three bookmarks")
    created_ids = []
    for ts_ns, text, topic in bookmark_specs:
        aid = index.add_annotation(bag_id, ts_ns, text, topic=topic)
        created_ids.append(aid)
        rel = (ts_ns - start_ns) / 1e9
        topic_label = topic or "(global)"
        print(f"  ✓ id={aid}  t={rel:.2f}s  topic={topic_label}  text={text!r}")

    section("List all bookmarks for this bag")
    for a in index.list_annotations(bag_id):
        rel = (a["timestamp_ns"] - start_ns) / 1e9
        print(f"    [{a['id']}] t={rel:.2f}s  {a['topic'] or '(global)':<20} {a['text']}")

    section("List bookmarks visible to a specific topic (includes globals)")
    for a in index.list_annotations(bag_id, topic="/imu/data"):
        rel = (a["timestamp_ns"] - start_ns) / 1e9
        print(f"    [{a['id']}] t={rel:.2f}s  {a['topic'] or '(global)':<20} {a['text']}")

    section("Update one bookmark")
    target_id = created_ids[0]
    index.update_annotation(target_id, "joint motion begins")
    updated = next(a for a in index.list_annotations(bag_id) if a["id"] == target_id)
    print(f"  ✓ id={target_id}  text now = {updated['text']!r}")

    section("Delete the bookmarks we created")
    for aid in created_ids:
        deleted = index.delete_annotation(aid)
        print(f"  ✓ id={aid}  deleted={deleted}")

    print(
        "\n  ✓ Same operations live behind /api/bags/{id}/annotations\n"
        "    (POST/GET) and /api/annotations/{id} (PATCH/DELETE) for the\n"
        "    dashboard's right-rail BookmarksPanel.\n"
    )

    index.close()


if __name__ == "__main__":
    main()
