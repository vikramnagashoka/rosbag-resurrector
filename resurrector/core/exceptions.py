"""Custom exceptions raised by core APIs.

Centralized so callers can catch a small, well-named hierarchy
instead of TypeError / RuntimeError / ValueError grab-bags. Every
exception here corresponds to a documented contract violation; if
you're catching one, you should be able to point at the contract
that was crossed.
"""

from __future__ import annotations

import os
import shlex


class ResurrectorError(Exception):
    """Base class for resurrector-specific errors.

    Catch this to handle anything resurrector raises explicitly,
    without swallowing generic Python errors.
    """


class LargeTopicError(ResurrectorError):
    """Raised when an eager API would materialize a too-large topic.

    The bag-as-dataframe contract is "memory bounded by chunk size,
    not topic size" — see the README "Performance contract" section.
    Eager APIs (``to_polars``, ``to_pandas``, ``to_numpy``) refuse
    topics above ``LARGE_TOPIC_THRESHOLD`` (default 1_000_000 messages)
    unless the caller passes ``force=True`` to opt in.

    Attributes
    ----------
    topic_name : str
    message_count : int
    threshold : int
    """

    def __init__(self, topic_name: str, message_count: int, threshold: int):
        self.topic_name = topic_name
        self.message_count = message_count
        self.threshold = threshold
        super().__init__(
            f"Topic {topic_name!r} has {message_count:,} messages "
            f"(threshold: {threshold:,}). Eager materialization would "
            f"likely OOM. Use one of:\n"
            f"  - bf[{topic_name!r}].iter_chunks(chunk_size=...)\n"
            f"  - with bf[{topic_name!r}].materialize_ipc_cache() as cache: "
            f"cache.scan().filter(...).collect()\n"
            f"or pass force=True to opt in (and accept the memory cost)."
        )


class SyncBufferExceededError(ResurrectorError):
    """Raised when a streaming sync buffer fills up.

    Happens when a non-anchor topic produces more than
    ``max_buffer_messages`` samples within the lookahead window — a
    likely sign of a pathological rate mismatch between the anchor
    topic and this one.
    """

    def __init__(
        self,
        topic_name: str,
        buffer_size: int,
        max_buffer_messages: int,
        suggestion: str = "",
    ):
        self.topic_name = topic_name
        self.buffer_size = buffer_size
        self.max_buffer_messages = max_buffer_messages
        suggestion = suggestion or (
            "Pick an anchor topic with a closer publication rate, "
            "or raise max_buffer_messages= if the rate mismatch is intentional."
        )
        super().__init__(
            f"Sync buffer for topic {topic_name!r} hit "
            f"{buffer_size:,} messages "
            f"(max_buffer_messages={max_buffer_messages:,}). {suggestion}"
        )


class SyncOutOfOrderError(ResurrectorError):
    """Raised when streaming sync sees a backwards-in-time timestamp.

    Only raised when ``out_of_order='error'`` (the streaming default).
    Use ``out_of_order='warn_drop'`` to silently drop regressing
    samples, or ``out_of_order='reorder'`` for a watermark-bounded
    reorder buffer.
    """

    def __init__(self, topic_name: str, prev_ts: int, regressing_ts: int):
        self.topic_name = topic_name
        self.prev_ts = prev_ts
        self.regressing_ts = regressing_ts
        delta_ms = (prev_ts - regressing_ts) / 1e6
        super().__init__(
            f"Topic {topic_name!r} produced an out-of-order timestamp: "
            f"{regressing_ts} after {prev_ts} ({delta_ms:.2f} ms backwards). "
            f"Pass out_of_order='reorder' (with max_lateness_ms=) to "
            f"tolerate, or 'warn_drop' to drop regressing samples."
        )


class SyncSchemaDriftError(ResurrectorError):
    """Raised when a topic's column changes dtype partway through a
    streaming sync.

    The streaming engine fixes every output column's dtype from the
    topic's first chunk (so each output chunk fits one writer). A later
    chunk whose values that dtype can't hold (an all-null first chunk
    followed by values, floats after ints in an integer column, ...)
    would otherwise fail inside polars or be silently truncated.
    """

    def __init__(self, topic_name: str, column: str, expected: str, got: str):
        self.topic_name = topic_name
        self.column = column
        self.expected = expected
        self.got = got
        super().__init__(
            f"Topic {topic_name!r} column {column!r} changed dtype mid-stream: "
            f"{expected} in its first chunk, {got} later. The streaming sync "
            f"engine fixes each column's dtype from the topic's first chunk "
            f"and can't hold these values. Pass engine='eager' if the topics "
            f"fit in memory (it unifies dtypes across the whole topic), or "
            f"leave {topic_name!r} out of the sync."
        )


class LeRobotFrameShapeError(ResurrectorError, ValueError):
    """A camera topic's frames are a size LeRobot can't store faithfully.

    For the first bag, raised before the dataset directory is created; a
    later bag's error removes the partial dataset. Each rule below is a
    failure measured against LeRobot 0.6.1, not a guess:

    - Height 1: LeRobot reads ``(1, W, 3)`` as single-channel, its image
      writer drops every frame, and ``save_episode()`` dies with
      FileNotFoundError. The 1x1 placeholder demo bags hit this.
    - Height 3: read as channels-first ``(3, H, W)`` and transposed, so
      image mode silently stores wrong pixels and video mode crashes.
    - Video mode, height under ``min_height`` or width under 4: the
      encoder refuses it. Width 4 to ``min_width - 1``: the encoder
      never returns.

    ``min_width`` / ``min_height`` are the video encoder's limits, measured
    and defined in :mod:`resurrector.core.lerobot_export`. ``bag`` is the
    source bag's path, named in the 1x1 hint when given.

    Also a ``ValueError``, so callers that already map ValueError to a
    clean error (the CLI, the dashboard's 400) keep doing so.
    """

    def __init__(
        self,
        topic: str,
        shape: tuple[int, ...],
        use_videos: bool,
        *,
        min_width: int,
        min_height: int,
        bag: str | os.PathLike | None = None,
    ):
        self.topic = topic
        self.shape = tuple(shape)
        self.use_videos = use_videos
        self.bag = bag
        h, w = self.shape[:2]
        # What image mode does with this height; None when it stores it fine.
        image_problem = {
            1: "LeRobot reads a 1-pixel-high frame as single-channel and fails to write it",
            3: "LeRobot reads a 3-pixel-high frame as channels-first and stores it transposed",
        }.get(h)
        alternative = ""
        if use_videos:
            need = (f"LeRobot's AV1 video encoder needs frames at least {min_width} "
                    f"pixels wide and {min_height} pixels high")
            if image_problem:
                need += f", and PNG images can't hold them either: {image_problem}"
            else:
                # The CLI, dashboard and BagFrame.export always encode video;
                # only a direct export_lerobot() call can choose PNG.
                alternative = (" PNG images take frames this size; to store them instead, "
                               "call the Python API: from resurrector import BagFrame; "
                               "from resurrector.core.lerobot_export import export_lerobot; "
                               "export_lerobot([BagFrame(bag)], topics, output_dir, "
                               "use_videos=False).")
        else:
            need = f"{image_problem}; PNG images need a height of 2 or at least 4 pixels"
        hint = ""
        if (h, w) == (1, 1):
            regen = (f"`resurrector demo -o {shlex.quote(os.fspath(bag))}`" if bag
                     else "`resurrector demo` (or `resurrector demo -o <bag>` for another path)")
            hint = (" 1x1 frames usually mean a demo bag written by an install without "
                    f"Pillow: regenerate it with {regen}.")
        super().__init__(
            f"Camera topic {topic!r} has {h}x{w} (height x width) frames: "
            f"{need}. Leave the topic out of the export or resize its "
            f"images.{alternative}{hint}"
        )


class LeRobotFrameFormatError(ResurrectorError, ValueError):
    """A camera topic's pixels can't become 8-bit RGB without losing values.

    LeRobot export stores every camera as 8-bit RGB (LeRobot 0.6.1's image
    and video features crash on a uint16 frame and on a 2-channel one).
    Gray, gray+alpha, RGB(A)/BGR(A) and 1-bit frames convert losslessly
    (alpha is dropped); anything else raises this before the dataset
    directory is created. A plain cast would wrap 16-bit values (300 -> 44),
    and scaling 16-bit or float data to 8 bits turns metric depth (16UC1
    millimetres, 32FC1 metres) into a near-black, coarsely stepped image,
    so neither is done silently.

    Also a ``ValueError``, for the same reason as :class:`LeRobotFrameShapeError`.
    """

    def __init__(self, topic: str | None, dtype: str, shape: tuple[int, ...]):
        self.topic = topic
        self.dtype = str(dtype)
        self.shape = tuple(shape)
        who = f"Camera topic {topic!r}" if topic else "A camera topic"
        if self.dtype != "uint8":
            problem = (
                f"has {self.dtype} frames (shape {self.shape}): LeRobot export "
                "stores cameras as 8-bit RGB, and converting these pixels to "
                "8 bits would wrap or flatten their values (16-bit depth in "
                "millimetres, for example, becomes a near-black image)"
            )
        else:
            problem = (
                f"has frames of shape {self.shape}, which isn't height x width "
                "with 1 to 4 channels"
            )
        super().__init__(
            f"{who} {problem}. Leave the topic out of the export or convert "
            "its images to 8-bit gray or RGB first."
        )


class SyncBoundaryError(ResurrectorError):
    """Raised when interpolation can't bracket an anchor timestamp.

    Only raised when ``boundary='error'``. Default is ``boundary='null'``
    which emits a missing value at the boundaries instead (NaN in
    numeric columns).
    """

    def __init__(self, topic_name: str, anchor_ts: int, position: str):
        self.topic_name = topic_name
        self.anchor_ts = anchor_ts
        self.position = position  # "before_first" | "after_last" | "no_data"
        super().__init__(
            f"Interpolation failed for topic {topic_name!r} at anchor "
            f"timestamp {anchor_ts}: {position}. "
            f"Pass boundary='null' (default), 'drop', or 'hold' to tolerate."
        )
