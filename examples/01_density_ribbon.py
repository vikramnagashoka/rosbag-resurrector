"""Per-topic message-density histogram (the dashboard timeline ribbon).

Demonstrates: ``resurrector/ingest/density.py`` :func:`compute_density`.

Run:
    python examples/01_density_ribbon.py

What you'll see: one sparkline per topic showing how messages
distribute across the bag's duration. Gaps and bursts are visible at a
glance. This is exactly what the dashboard ribbon renders.
"""

from __future__ import annotations

from _common import ensure_sample_bag, header, sparkline

from resurrector.ingest.density import compute_density


def main() -> None:
    header("01 — Per-topic message-density histogram")
    bag_path = ensure_sample_bag()

    print(f"  Computing density across 50 bins for every topic...\n")
    result = compute_density(bag_path, bins=50)

    # Sort topics by total message count, descending.
    sorted_topics = sorted(
        result.items(), key=lambda kv: kv[1]["total"], reverse=True,
    )

    print(f"  {'topic':<30} {'total':>8}  density (50 bins, full duration)")
    print(f"  {'-' * 30} {'-' * 8}  {'-' * 50}")
    for topic, info in sorted_topics:
        ribbon = sparkline(info["bins"])
        print(f"  {topic:<30} {info['total']:>8}  {ribbon}")

    print(
        "\n  ✓ Density computed without parsing message bodies — only timestamps.\n"
        "  In the dashboard this renders as a heatmap; in this script it's a\n"
        "  Unicode ribbon. Each row is normalized to its own [0,1] range so\n"
        "  sparse topics still show their pattern.\n"
    )


if __name__ == "__main__":
    main()
