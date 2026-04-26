"""DuckDB-backed index — search across all your bags with a query DSL.

Demonstrates: ``BagIndex`` + the ``topic:`` / ``health:`` / ``after:``
query syntax surfaced by ``resurrector list`` and the dashboard
search bar.

Run:
    python examples/06_index_search_query_dsl.py

What you'll see: index two synthetic bags with different topic sets,
then run several queries against the index — by topic name, by health
score, by message-count threshold.
"""

from __future__ import annotations

from _common import ensure_output_dir, ensure_sample_bag, header, section

from resurrector import BagFrame
from resurrector.ingest.indexer import BagIndex
from resurrector.ingest.parser import parse_bag
from resurrector.ingest.scanner import scan_path


def index_one(index: BagIndex, bag_path) -> int:
    scanned = scan_path(bag_path)[0]
    parser = parse_bag(bag_path)
    bag_id = index.upsert_bag(scanned, parser.get_metadata())
    bf = BagFrame(bag_path)
    index.update_health_score(bag_id, bf.health_report().score)
    return bag_id


def main() -> None:
    header("06 — DuckDB index + query DSL")
    out = ensure_output_dir()

    # Make a second bag so the index has more than one row to filter.
    primary = ensure_sample_bag()
    second = out / "second_bag.mcap"
    if not second.exists():
        from resurrector.demo.sample_bag import BagConfig, generate_bag
        generate_bag(second, BagConfig(duration_sec=4.0))

    # Use a scratch DB so we don't touch the user's real index.
    db_path = out / "scratch_index.db"
    if db_path.exists():
        db_path.unlink()

    section("Index two bags into a fresh DuckDB")
    index = BagIndex(db_path)
    a_id = index_one(index, primary)
    b_id = index_one(index, second)
    print(f"  Indexed {primary.name} as id {a_id}")
    print(f"  Indexed {second.name}  as id {b_id}")
    print(f"  Total in index: {index.count()} bag(s)")

    section("List everything")
    rows = index.list_bags(limit=10)
    for r in rows:
        print(f"  [{r['id']}] {r['path'].split('/')[-1]:<30}  "
              f"health={r['health_score']}  msgs={r['message_count']:,}")

    section("Filter by minimum health score")
    healthy = index.list_bags(min_health=80)
    print(f"  bags with health >= 80: {len(healthy)}")

    section("Filter by topic name (must contain /imu)")
    has_imu = index.list_bags(has_topic="/imu/data")
    print(f"  bags containing /imu/data: {len(has_imu)}")
    for r in has_imu:
        print(f"    [{r['id']}] {r['path'].split('/')[-1]}")

    section("Query DSL — same surface the dashboard search bar uses")
    # 'health:>80' style queries are supported by index.search()
    results = index.search("health:>80")
    print(f"  query 'health:>80' -> {len(results)} bag(s)")
    results = index.search("topic:/joint_states")
    print(f"  query 'topic:/joint_states' -> {len(results)} bag(s)")

    section("Stale-path detection")
    # Move one of the bags away so the index can spot it.
    moved = second.with_suffix(".moved.mcap")
    second.rename(moved)
    stale = index.validate_paths()
    print(f"  Stale paths after rename: {len(stale)}")
    if stale:
        print(f"    {stale[0]['path']}")
    # Restore for next time the user runs this script.
    moved.rename(second)

    index.close()
    print(
        "\n  ✓ The same index powers `resurrector list`, the dashboard\n"
        "    library page, and the dashboard's search bar. CLIP frame\n"
        "    embeddings live in this DB too (see semantic search example).\n"
    )


if __name__ == "__main__":
    main()
