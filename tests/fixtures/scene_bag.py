"""Back-compat shim — the scene bag generator now lives in
``resurrector.demo.scene_bag`` so it ships with the installed package
(examples/23_scene_tf_and_pointcloud.py needs it without a repo
checkout).

New code should import directly:

    from resurrector.demo.scene_bag import generate_scene_bag
"""

from resurrector.demo.scene_bag import (
    encode_pointcloud2,
    encode_tf_message,
    generate_scene_bag,
)

__all__ = ["encode_pointcloud2", "encode_tf_message", "generate_scene_bag"]
