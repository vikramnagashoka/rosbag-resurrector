"""Custom exceptions raised by core APIs.

Centralized so callers can catch a small, well-named hierarchy
instead of TypeError / RuntimeError / ValueError grab-bags. Every
exception here corresponds to a documented contract violation; if
you're catching one, you should be able to point at the contract
that was crossed.
"""

from __future__ import annotations


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


class SyncBoundaryError(ResurrectorError):
    """Raised when interpolation can't bracket an anchor timestamp.

    Only raised when ``boundary='error'``. Default is ``boundary='null'``
    which emits None/NaN at the boundaries instead.
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
