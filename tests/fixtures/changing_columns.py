"""A JointState bag whose fields come and go, so ``iter_chunks`` yields
chunks with different column sets.

Message ``i`` is at ``BASE_NS + i`` ms with position ``[i / 2, -i]``,
velocity ``[1000 + i, 2000 + i]`` for ``i`` in ``velocity`` (empty
otherwise) and effort ``[5 + i, 6]`` before ``effort_until`` (empty
from there on). An empty list flattens to no column, so a chunk whose
messages all lack velocity has no ``velocity.*`` columns.
"""

from __future__ import annotations

from pathlib import Path

BASE_NS = 1_700_000_000_000_000_000


def write_joint_state_bag(
    path: Path, rows: int, velocity: range, effort_until: int,
) -> Path:
    from mcap.writer import Writer

    from resurrector.demo.sample_bag import SCHEMAS, _encode_joint_state

    info = SCHEMAS["sensor_msgs/msg/JointState"]
    with open(path, "wb") as f:
        writer = Writer(f)
        writer.start(profile="ros2", library="resurrector-test")
        sid = writer.register_schema(
            name="sensor_msgs/msg/JointState", encoding=info["encoding"],
            data=info["data"].encode(),
        )
        cid = writer.register_channel(
            topic="/joint_states", message_encoding="cdr", schema_id=sid,
        )
        for i in range(rows):
            t = BASE_NS + i * 1_000_000
            data = _encode_joint_state(
                t // 10**9, t % 10**9, ["a", "b"],
                [i * 0.5, -float(i)],
                [1000.0 + i, 2000.0 + i] if i in velocity else [],
                [5.0 + i, 6.0] if i < effort_until else [],
            )
            writer.add_message(cid, log_time=t, publish_time=t, sequence=i, data=data)
        writer.finish()
    return path
