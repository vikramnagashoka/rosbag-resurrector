"""Custom message types: decoded from the bag's definition, or by your decoder.

rosbag2 embeds each topic's message definition in the MCAP file, and
Resurrector decodes any type from it, custom ones included. For a bag
that carries no definition, ``resurrector.register_decoder()`` plugs in your
own, and it also overrides the built-in or embedded one when you want a
different shape.

Run:
    python examples/24_custom_type_decoders.py

What you'll see:
  1. A bag that embeds the definition of ``my_pkg/msg/Vec3`` (3 float64
     fields): typed x/y/z columns with no setup
  2. The same messages in a bag without a definition: ``_unparsed``
  3. After ``register_decoder()``: that bag decodes too
"""

from __future__ import annotations

import struct
from pathlib import Path

import polars as pl

from _common import ensure_output_dir, header, section

import resurrector
from resurrector.core.bag_frame import BagFrame
from resurrector.ingest.parser import parse_bag


def _generate_bag_with_custom_type(
    out_path: Path, n_messages: int = 100, definition: bytes = b"float64 x\nfloat64 y\nfloat64 z",
) -> Path:
    """Write a tiny MCAP with N messages of a custom my_pkg/msg/Vec3 type.
    ``definition=b""`` writes the schema without a definition."""
    from mcap.writer import Writer
    base_ns = 1_700_000_000_000_000_000

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as f:
        writer = Writer(f)
        writer.start(profile="ros2", library="custom-decoder-example")
        sid = writer.register_schema(
            name="my_pkg/msg/Vec3", encoding="ros2msg",
            data=definition,
        )
        cid = writer.register_channel(
            topic="/my_vec", message_encoding="cdr", schema_id=sid,
        )
        for i in range(n_messages):
            ts = base_ns + i * 10_000_000  # 100 Hz
            cdr = b"\x00\x01\x00\x00"
            body = struct.pack("<3d", float(i), float(i * 2), float(i * 3))
            writer.add_message(
                channel_id=cid, log_time=ts, publish_time=ts,
                data=cdr + body,
            )
        writer.finish()
    return out_path


def main() -> None:
    header("24 — v0.6.0: pluggable custom-type CDR decoders")
    out = ensure_output_dir()
    with_def = out / "v06_custom_type.mcap"
    bag_path = out / "v06_custom_type_no_definition.mcap"
    for path in (with_def, bag_path):
        if path.exists():
            path.unlink()
    print("  Generating two synthetic bags with my_pkg/msg/Vec3 messages:")
    print(f"    {with_def}  (embeds the definition, as rosbag2 does)")
    print(f"    {bag_path}  (no definition)\n")
    _generate_bag_with_custom_type(with_def, n_messages=100)
    _generate_bag_with_custom_type(bag_path, n_messages=100, definition=b"")

    section("Built-in decoders (hand-written, for the most common types)")
    print(f"  Total registered: {len(resurrector.list_decoders())}")
    for d in resurrector.list_decoders():
        print(f"    {d}")

    section("A bag that embeds the definition decodes with no setup")
    msgs = list(parse_bag(with_def).read_messages(topics=["/my_vec"]))
    print(f"  First message data: {msgs[0].data}")
    df = BagFrame(with_def)["/my_vec"].to_polars()
    print(f"  to_polars() columns: {df.columns}")

    section("Without a definition or a decoder — message body is _unparsed")
    parser = parse_bag(bag_path)
    msgs = list(parser.read_messages(topics=["/my_vec"]))
    print(f"  Messages on /my_vec: {len(msgs)}")
    print(f"  First message data: {msgs[0].data}")
    print(f"  Note: _unparsed=True; raw_data is still preserved on every Message")

    section("Register a custom decoder")
    print("  def decode_vec3(raw):")
    print("      x, y, z = struct.unpack_from('<3d', raw, 4)  # skip CDR header")
    print("      return {'x': x, 'y': y, 'z': z}")
    print()
    print("  resurrector.register_decoder('my_pkg/msg/Vec3', decode_vec3)")

    def decode_vec3(raw: bytes) -> dict:
        x, y, z = struct.unpack_from("<3d", raw, 4)
        return {"x": x, "y": y, "z": z}

    resurrector.register_decoder("my_pkg/msg/Vec3", decode_vec3)
    print(f"\n  Total decoders now: {len(resurrector.list_decoders())}")
    print(f"  'my_pkg/msg/Vec3' present: {'my_pkg/msg/Vec3' in resurrector.list_decoders()}")

    section("Re-iterate the same bag — messages now decoded into typed dicts")
    parser = parse_bag(bag_path)
    msgs = list(parser.read_messages(topics=["/my_vec"]))
    print(f"  First message data: {msgs[0].data}")
    print(f"  Last message data:  {msgs[-1].data}")

    section("BagFrame.to_polars() now produces a real DataFrame with x/y/z columns")
    bf = BagFrame(bag_path)
    df = bf["/my_vec"].to_polars()
    print(f"  Shape: {df.shape}")
    print(f"  Columns: {df.columns}")
    print(f"  Head:")
    print(df.head(5))

    section("Decoder failures fail-closed (don't crash the whole iteration)")
    print("  resurrector.register_decoder('my_pkg/msg/Vec3', lambda raw: 1/0)")
    resurrector.register_decoder("my_pkg/msg/Vec3", lambda raw: 1 / 0)
    parser = parse_bag(bag_path)
    msgs = list(parser.read_messages(topics=["/my_vec"]))
    print(f"  Messages: {len(msgs)} (no crash)")
    print(f"  First message data: {msgs[0].data}")
    print(f"  Note: _parse_error=True; iteration continues with the placeholder")

    # Restore the working decoder before exit so any later examples that
    # share state aren't broken.
    resurrector.register_decoder("my_pkg/msg/Vec3", decode_vec3)

    print(
        "\n  ✓ Any type whose definition the bag embeds decodes automatically.\n"
        "  ✓ register_decoder() covers bags without one, and overrides either way.\n"
        "  ✓ Decoder errors fail-closed — one bad message doesn't abort the bag.\n"
        "  ✓ unregister_decoder() removes a decoder; can also override built-ins.\n"
    )


if __name__ == "__main__":
    main()
