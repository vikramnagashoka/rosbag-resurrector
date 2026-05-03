"""Multi-bag synchronized playback with per-bag offsets (v0.5.0).

Demonstrates ``resurrector.bridge.multibag.MultiBagPlayback`` — N
playback engines sharing one play/pause/stop/seek/set_speed control
surface. Each bag can be staggered by a wall-clock offset so you can
align "trial A" against "trial B + 2.5s lag" without rewriting bags.

Topics are namespaced as ``<bag_id>:<original_topic>`` so PlotJuggler
and the cross-bag overlay UI handle multi-bag streams without any
WS protocol changes.

Run:
    python examples/20_multi_bag_playback.py

What you'll see: two synthetic bags played at high speed; bag "a"
starts immediately, bag "b" starts ~0.3s later. Their messages
interleave through one callback in real time.
"""

from __future__ import annotations

import asyncio
from collections import Counter

from _common import ensure_sample_bag, header, section

from resurrector.bridge.multibag import BagPlaybackConfig, MultiBagPlayback


async def run() -> None:
    header("20 — v0.5.0: multi-bag synchronized playback")
    bag_path = ensure_sample_bag()
    # We use the same synthetic bag twice for the demo; in a real
    # workflow you'd use two different bags from two different runs.
    bag_a = bag_path
    bag_b = bag_path
    print(f"  bag a: {bag_a}")
    print(f"  bag b: {bag_b}  (same file, simulating two runs)\n")

    counts: Counter = Counter()
    first_seen: dict[str, float] = {}
    loop_started = asyncio.get_event_loop().time()

    def on_message(bag_id: str, msg) -> None:
        counts[bag_id] += 1
        if bag_id not in first_seen:
            first_seen[bag_id] = asyncio.get_event_loop().time() - loop_started

    section("Constructing MultiBagPlayback (b offset by 0.3s)")
    mp = MultiBagPlayback(
        configs=[
            BagPlaybackConfig(bag_path=bag_a, bag_id="a", label="Baseline run"),
            BagPlaybackConfig(
                bag_path=bag_b, bag_id="b", label="Lagged run", offset_sec=0.3,
            ),
        ],
        speed=20.0,  # 20× real-time so the demo wraps quickly
        message_callback=on_message,
    )

    info = mp.get_discovery_info()
    print(f"  bags: {[(b['id'], b['label'], b['offset_sec']) for b in info['bags']]}")
    sample_topics = info["namespaced_topics"][:6]
    print(f"  namespaced topics (first 6): {sample_topics}")
    print(f"  ... {len(info['namespaced_topics'])} total\n")

    section("Playing both bags concurrently for 1.5s wall-clock")
    await mp.play()
    await asyncio.sleep(1.5)
    await mp.stop()

    section("Per-bag delivery summary")
    for bid in ("a", "b"):
        print(f"  {bid}:  {counts[bid]:>5} messages   "
              f"first_seen_at_offset={first_seen.get(bid, float('nan')):.3f}s "
              f"wall-clock from play()")
    print(
        "\n  ✓ Bag b's first message arrives ~0.3s/speed = ~15 ms after play()\n"
        "    at speed=20×. Topic namespacing keeps a's '/imu/data' from\n"
        "    colliding with b's '/imu/data' downstream.\n"
    )


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
