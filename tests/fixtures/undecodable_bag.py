"""A bag with a topic Resurrector can't decode by itself, plus the .msg
folder that makes it decodable.

``/gizmo`` (``acme_msgs/msg/Gizmo``) records only an IDL definition
(schema encoding ``ros2idl``), which the schema compiler doesn't read.
``/widget`` (``acme_msgs/msg/Widget``) embeds a normal ``ros2msg``
definition, including ``std_msgs/Header``. The .msg folder holds only
``acme_msgs/msg/Gizmo.msg``; the ``std_msgs/Header`` it uses has to come
from the bag's ``/widget`` definition, which is the case a ROS workspace
without the ROS install hits.

Messages are encoded by mcap_ros2 from the .msg text, so a decoder that
reads them back is checked against an independent encoder.

Run (the e2e launcher does)::

    python tests/fixtures/undecodable_bag.py OUT.mcap MSG_DIR
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

from mcap.writer import Writer
from mcap_ros2._dynamic import serialize_dynamic

GIZMO_TYPE = "acme_msgs/msg/Gizmo"
WIDGET_TYPE = "acme_msgs/msg/Widget"
GIZMO_MSG = "std_msgs/Header header\nfloat64 x\nint32 n\n"
WIDGET_MSG = "std_msgs/Header header\nfloat64 y\n"
_SEP = "=" * 80
_HEADER_BLOCKS = (
    f"{_SEP}\nMSG: std_msgs/Header\nbuiltin_interfaces/Time stamp\nstring frame_id\n"
    f"{_SEP}\nMSG: builtin_interfaces/Time\nint32 sec\nuint32 nanosec\n"
)
GIZMO_IDL = b"""\
module acme_msgs { module msg {
  struct Gizmo { std_msgs::msg::Header header; double x; int32 n; };
}; };
"""
FRAME_ID = "gizmo_frame"
T0 = 1_700_000_000_000_000_000


def gizmo_values(i: int) -> tuple[float, int]:
    """``(x, n)`` of the i-th /gizmo message."""
    return i * 0.25, i


def write_undecodable_bag(path: Path, n: int = 200, rate_hz: float = 50.0, t0: int = T0) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    encode_gizmo = serialize_dynamic("acme_msgs/Gizmo", GIZMO_MSG + _HEADER_BLOCKS)["acme_msgs/Gizmo"]
    encode_widget = serialize_dynamic("acme_msgs/Widget", WIDGET_MSG + _HEADER_BLOCKS)["acme_msgs/Widget"]
    step = int(1e9 / rate_hz)
    with open(path, "wb") as f:
        w = Writer(f)
        w.start(profile="ros2", library="resurrector-tests")
        gizmo_schema = w.register_schema(GIZMO_TYPE, "ros2idl", GIZMO_IDL)
        widget_schema = w.register_schema(
            WIDGET_TYPE, "ros2msg", (WIDGET_MSG + _HEADER_BLOCKS).encode(),
        )
        gizmo = w.register_channel("/gizmo", "cdr", gizmo_schema)
        widget = w.register_channel("/widget", "cdr", widget_schema)
        for i in range(n):
            t = t0 + i * step
            stamp = SimpleNamespace(sec=t // 10**9, nanosec=t % 10**9)
            x, count = gizmo_values(i)
            g = SimpleNamespace(header=SimpleNamespace(stamp=stamp, frame_id=FRAME_ID), x=x, n=count)
            wd = SimpleNamespace(header=SimpleNamespace(stamp=stamp, frame_id="widget_frame"), y=-x)
            w.add_message(gizmo, log_time=t, publish_time=t, data=encode_gizmo(g), sequence=i)
            w.add_message(widget, log_time=t, publish_time=t, data=encode_widget(wd), sequence=i)
        w.finish()
    return path


def write_msg_folder(root: Path) -> Path:
    """``root/acme_msgs/msg/Gizmo.msg``, laid out like a ROS workspace's src/."""
    msg_dir = Path(root) / "acme_msgs" / "msg"
    msg_dir.mkdir(parents=True, exist_ok=True)
    (msg_dir / "Gizmo.msg").write_text("# A gizmo reading\n" + GIZMO_MSG)
    return Path(root)


if __name__ == "__main__":
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("undecodable_demo.mcap")
    # Recorded at 1 s past the epoch: the oldest bag in the e2e index, so it
    # lists last (newest first) and specs that open the first notebook still
    # get the scene demo.
    write_undecodable_bag(out, t0=1_000_000_000)
    if len(sys.argv) > 2:
        write_msg_folder(Path(sys.argv[2]))
