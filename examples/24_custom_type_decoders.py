"""Custom message types: decoded from the bag's definition, your .msg files, or your decoder.

rosbag2 embeds each topic's message definition in the MCAP file, and
Resurrector decodes any type from it, custom ones included. For a bag
that carries no usable definition, ``resurrector.load_decoders()`` reads
the type from a folder of ``.msg`` files (your ROS workspace), and
``resurrector.register_decoder()`` plugs in a Python decoder, which also
overrides the built-in or embedded one when you want a different shape.

Run:
    python examples/24_custom_type_decoders.py

What you'll see:
  1. A bag that embeds the definition of ``my_pkg/msg/Vec3`` (3 float64
     fields): typed x/y/z columns with no setup
  2. The same messages in a bag without a definition: ``undecodable_topics()``
     names the topic, and ``to_polars()`` raises ``UndecodableTopicError``
  3. After ``load_decoders()`` on a folder with ``my_pkg/msg/Vec3.msg``: it decodes
  4. ``register_decoder()``, the Python alternative
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
    header("24 — custom message types: embedded definitions + register_decoder")
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

    section("Without a definition or a decoder, reading fields stops with the reason")
    bf = BagFrame(bag_path)
    for t in bf.undecodable_topics():
        print(f"  undecodable_topics(): {t.topic}  {t.message_type}: {t.reason}")
    try:
        bf["/my_vec"].to_polars()
    except resurrector.UndecodableTopicError as e:
        print(f"  to_polars() raised UndecodableTopicError:\n    {str(e).splitlines()[0]}")
    msgs = list(parse_bag(bag_path).read_messages(topics=["/my_vec"]))
    print(f"  iter_messages() still yields all {len(msgs)}, raw bytes included: {msgs[0].data}")

    section("Load the type from a folder of .msg files (your ROS workspace)")
    ws = out / "msg_ws" / "src"
    msg_file = ws / "my_pkg" / "msg" / "Vec3.msg"
    msg_file.parent.mkdir(parents=True, exist_ok=True)
    msg_file.write_text("float64 x\nfloat64 y\nfloat64 z\n")
    print(f"  Wrote {msg_file}")
    print(f"  resurrector.load_decoders({str(ws)!r}) -> {resurrector.load_decoders(ws)}")
    df = BagFrame(bag_path)["/my_vec"].to_polars()
    print(f"  to_polars() columns now: {df.columns}")
    print("  (CLI: --decoders PATH, or RESURRECTOR_DECODERS=PATH for every command)")

    section("Or register a Python decoder (also overrides any other)")
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
        "  ✓ load_decoders() reads types a bag lacks from your .msg files.\n"
        "  ✓ register_decoder() plugs in Python, and overrides either way.\n"
        "  ✓ Decoder errors fail-closed — one bad message doesn't abort the bag.\n"
        "  ✓ unregister_decoder() removes a decoder; can also override built-ins.\n"
    )


if __name__ == "__main__":
    main()
