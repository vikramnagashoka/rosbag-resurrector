"""Semantic frame search — find clips by describing them in English.

Demonstrates: ``FrameSearchEngine`` (resurrector/core/vision.py).

Run:
    python examples/07_semantic_frame_search.py

What you'll see: index the demo bag's video frames as 512-dim CLIP
embeddings into DuckDB, then search by natural language. Returns the
top-K matching frames with similarity scores, then groups consecutive
matches into temporal clips.

Requires the [vision] (local CLIP) or [vision-openai] extra. Auto-
skips with install instructions if neither is available.
"""

from __future__ import annotations

import sys

from _common import ensure_output_dir, ensure_sample_bag, header, section

from resurrector import BagFrame
from resurrector.ingest.indexer import BagIndex
from resurrector.ingest.parser import parse_bag
from resurrector.ingest.scanner import scan_path


def main() -> None:
    header("07 — Semantic frame search")
    bag_path = ensure_sample_bag()
    out = ensure_output_dir()

    # Probe for either backend before doing the heavy work.
    has_local = False
    has_openai = False
    try:
        import sentence_transformers  # noqa: F401
        has_local = True
    except ImportError:
        pass
    try:
        import openai  # noqa: F401
        has_openai = True
    except ImportError:
        pass

    if not (has_local or has_openai):
        print(
            "\n  [SKIP] Semantic search needs a CLIP backend. Install one:\n"
            "    pip install 'rosbag-resurrector[vision]'         # local CLIP, ~2GB model\n"
            "    pip install 'rosbag-resurrector[vision-openai]'  # OpenAI API, lighter\n"
        )
        return

    # Use a scratch index so we don't pollute the real one.
    db_path = out / "scratch_search.db"
    if db_path.exists():
        db_path.unlink()
    index = BagIndex(db_path)

    section("Index a bag")
    scanned = scan_path(bag_path)[0]
    parser = parse_bag(bag_path)
    bag_id = index.upsert_bag(scanned, parser.get_metadata())
    bf = BagFrame(bag_path)
    index.update_health_score(bag_id, bf.health_report().score)
    print(f"  bag_id = {bag_id}")

    section("Embed video frames at 5Hz (this is the slow part — first time only)")
    from resurrector.core.vision import FrameSearchEngine
    engine = FrameSearchEngine(index)
    try:
        engine.index_bag(bag_id=bag_id, bag_path=bag_path, sample_hz=5.0)
    except Exception as e:
        msg = str(e)
        if "api_key" in msg.lower() or "OPENAI_API_KEY" in msg:
            print(f"  [SKIP] OpenAI backend needs an API key:\n"
                  f"           export OPENAI_API_KEY=sk-...\n"
                  f"         Or install the local CLIP backend:\n"
                  f"           pip install 'rosbag-resurrector[vision]'")
        else:
            print(f"  [SKIP] Frame indexing failed: {e}")
        index.close()
        return
    print(f"  Frames indexed: {index.count_frame_embeddings(bag_id)}")

    section("Search by text")
    queries = [
        "robot arm extending forward",
        "bright outdoor scene",
        "objects on a table",
    ]
    for q in queries:
        try:
            results = engine.search(q, top_k=3)
        except Exception as e:
            print(f"  [SKIP] '{q}' -> {e}")
            continue
        print(f"\n  query: {q!r}")
        for r in results:
            print(f"    sim={r.similarity:.3f}  bag_id={r.bag_id}  "
                  f"topic={r.topic}  t={r.timestamp_sec:.2f}s  "
                  f"frame={r.frame_index}")

    section("Group consecutive matches into clips")
    try:
        clips = engine.search_temporal(
            "robot arm extending forward",
            clip_duration_sec=2.0,
            top_k=3,
        )
        for c in clips:
            print(f"  [{c.start_sec:.2f}s — {c.end_sec:.2f}s]  "
                  f"{c.frame_count} frames  avg_sim={c.avg_similarity:.3f}")
    except Exception as e:
        print(f"  [SKIP] {e}")

    index.close()
    print(
        "\n  ✓ The same index powers the Search page in the dashboard.\n"
        "    Embeddings persist; subsequent queries are pure DuckDB cosine\n"
        "    similarity (sub-second even on millions of frames).\n"
    )


if __name__ == "__main__":
    main()
