"""Decoders the user supplies, for message types a bag can't be decoded from by itself.

rosbag2 embeds each type's ``ros2msg`` definition and
:mod:`resurrector.ingest.ros2msg` compiles it. Some bags carry no usable
one: the definition is IDL only (``ros2idl``), there is none, or it uses
something the compiler can't read. :func:`load_decoders` fills the gap
from a path on the user's machine:

- a folder of ``.msg`` files (a ROS workspace, or an installed ``share/``
  folder): each ``<pkg>/msg/<Type>.msg`` becomes a definition, compiled
  like an embedded one. Types it uses that the folder doesn't have
  (``std_msgs/Header``, say) come from the definitions the bag embeds for
  its other topics.
- a Python file that calls :func:`~resurrector.ingest.parser.register_decoder`.

Loaded definitions are process-wide, like registered decoders. Paths in
``RESURRECTOR_DECODERS`` (``os.pathsep``-separated) load the first time a
type has no other decoder.

``.msg`` text is data: it goes through the same compiler as embedded
definitions, which treats it as untrusted. A Python file runs as code, so
only the CLI, the Python API and the environment variable load one; the
dashboard accepts ``.msg`` folders only.
"""

from __future__ import annotations

import importlib.util
import logging
import os
import sys
import threading
from pathlib import Path
from typing import Callable

from resurrector.ingest.ros2msg import (
    UnsupportedSchema, normalize_type_name, referenced_types, split_definitions,
)

logger = logging.getLogger("resurrector.ingest.custom_decoders")

ENV_VAR = "RESURRECTOR_DECODERS"

# Folders a .msg search never needs to enter.
_SKIP_DIRS = {"node_modules", "__pycache__", "build", "log", "venv", "site-packages"}
_MAX_DIRS = 50_000          # a search of a whole home folder stops here
_MAX_MSG_BYTES = 1 << 20    # a .msg file is a few KB; anything this big isn't one

_lock = threading.Lock()
_msg_files: dict[str, Path] = {}    # "pkg/Type" -> its .msg file
_loaded: list[Path] = []
_python_modules = 0
_env_loaded = False


def load_decoders(path: str | os.PathLike) -> list[str]:
    """Make message types decodable from a path on this machine.

    Args:
        path: A folder of ``.msg`` files (searched recursively for
            ``<package>/msg/<Type>.msg``, e.g. ``~/ros2_ws/src`` or
            ``/opt/ros/humble/share``), a single ``.msg`` file in such a
            ``msg/`` folder, or a Python file that calls
            :func:`~resurrector.ingest.parser.register_decoder`.

    Returns:
        The message types it provides, as ``"pkg/msg/Type"``, sorted.

    Raises:
        FileNotFoundError: ``path`` doesn't exist.
        ValueError: It holds no ``.msg`` files, is some other kind of
            file, or the Python file failed or registered nothing.

    Example::

        import resurrector
        resurrector.load_decoders("~/ros2_ws/src")
        df = resurrector.BagFrame("run.mcap")["/gizmo"].to_polars()
    """
    p = Path(path).expanduser()
    if not p.exists():
        raise FileNotFoundError(f"No such file or folder: {p}")
    p = p.resolve()
    if p.is_file() and p.suffix == ".py":
        return _load_python(p)
    if p.is_file() and p.suffix == ".msg":
        name = _type_name(p)
        if name is None:
            raise ValueError(
                f"{p.name} isn't in a <package>/msg/ folder, so its package is unknown"
            )
        found = {name: p}
    elif p.is_dir():
        found = _find_msg_files(p)
        if not found:
            raise ValueError(f"No .msg files under {p} (looked for <package>/msg/<Type>.msg)")
    else:
        raise ValueError(
            f"{p.name}: expected a folder of .msg files, a .msg file, or a Python "
            f"file that calls register_decoder"
        )
    with _lock:
        _msg_files.update(found)
        _remember(p)
    return sorted(_public_name(n) for n in found)


def loaded_paths() -> list[Path]:
    """Every path :func:`load_decoders` has loaded in this process, in order."""
    with _lock:
        return list(_loaded)


def definition_for(msg_type: str, bag_bodies: Callable[[], dict[str, str]]) -> str | None:
    """A full ``ros2msg`` definition of ``msg_type`` built from loaded
    ``.msg`` files, or None when they add nothing for it.

    ``bag_bodies()`` returns ``{"pkg/Type": fields}`` from the definitions
    the bag embeds; those take priority (they match what was recorded),
    and loaded files fill in what the bag lacks. The main type may come
    from the bag too, when its embedded definition failed only because a
    type it uses was left out. Types neither has stay missing, and
    compiling the result says which.
    """
    _load_env_paths()
    with _lock:
        if not _msg_files:
            return None
        files = dict(_msg_files)
    main = normalize_type_name(msg_type)
    bodies = bag_bodies()
    if main not in files and main not in bodies:
        return None

    texts: dict[str, str] = {}
    order: list[str] = []
    from_files = False
    stack = [main]
    while stack:
        name = stack.pop()
        if name in texts:
            continue
        body = bodies.get(name)
        if body is None and name in files:
            body = _read_msg(files[name])
            from_files = from_files or body is not None
        if body is None:
            continue
        texts[name] = body
        order.append(name)
        try:
            stack.extend(referenced_types(name, body))
        except UnsupportedSchema:
            pass  # compiling the whole definition reports it
    if not from_files:
        return None  # the bag's own text, unchanged: it already failed
    parts = [texts[main].rstrip("\n") + "\n"]
    for name in order[1:]:
        parts.append("=" * 80 + f"\nMSG: {name}\n" + texts[name].rstrip("\n") + "\n")
    return "".join(parts)


def bodies_from_schemas(schemas) -> dict[str, str]:
    """``{"pkg/Type": fields}`` for every type in a bag's ``ros2msg`` schemas."""
    out: dict[str, str] = {}
    for schema in schemas:
        if schema.encoding != "ros2msg" or not schema.data:
            continue
        try:
            blocks = split_definitions(schema.name, schema.data.decode("utf-8", "replace"))
        except UnsupportedSchema:
            continue
        for name, body in blocks.items():
            out.setdefault(name, body)
    return out


def _load_env_paths() -> None:
    global _env_loaded
    if _env_loaded:
        return
    _env_loaded = True
    for entry in os.environ.get(ENV_VAR, "").split(os.pathsep):
        if not entry.strip():
            continue
        try:
            load_decoders(entry.strip())
        except (OSError, ValueError) as exc:
            logger.warning("%s: can't load %s: %s", ENV_VAR, entry, exc)


def _load_python(p: Path) -> list[str]:
    global _python_modules
    from resurrector.ingest.parser import _DECODER_REGISTRY

    before = dict(_DECODER_REGISTRY)
    with _lock:
        _python_modules += 1
        module_name = f"resurrector_user_decoders_{_python_modules}"
    spec = importlib.util.spec_from_file_location(module_name, p)
    if spec is None or spec.loader is None:
        raise ValueError(f"{p} can't be loaded as a Python module")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module  # dataclasses and pickling look modules up here
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        sys.modules.pop(module_name, None)
        raise ValueError(f"{p.name} raised {type(exc).__name__} while loading: {exc}") from exc
    added = sorted(t for t, fn in _DECODER_REGISTRY.items() if before.get(t) is not fn)
    if not added:
        raise ValueError(
            f"{p.name} ran but registered no decoder. Call "
            f"resurrector.register_decoder('pkg/msg/Type', decode_fn) in it."
        )
    with _lock:
        _remember(p)
    return added


def _remember(p: Path) -> None:
    if p not in _loaded:
        _loaded.append(p)


def _find_msg_files(root: Path) -> dict[str, Path]:
    found: dict[str, Path] = {}
    visited = 0
    for dirpath, dirnames, filenames in os.walk(root):
        visited += 1
        if visited > _MAX_DIRS:
            raise ValueError(
                f"{root} has more than {_MAX_DIRS:,} folders; point at your "
                f"workspace's src/ or an installed share/ folder instead"
            )
        dirnames[:] = [d for d in dirnames if not d.startswith(".") and d not in _SKIP_DIRS]
        if os.path.basename(dirpath) != "msg":
            continue
        for fname in filenames:
            if fname.endswith(".msg"):
                p = Path(dirpath) / fname
                name = _type_name(p)
                if name is not None:
                    found.setdefault(name, p)
    return found


def _type_name(p: Path) -> str | None:
    """``"pkg/Type"`` for ``.../pkg/msg/Type.msg``."""
    if p.parent.name != "msg" or not p.parent.parent.name:
        return None
    return f"{p.parent.parent.name}/{p.stem}"


def _public_name(name: str) -> str:
    pkg, _, typ = name.partition("/")
    return f"{pkg}/msg/{typ}"


def _read_msg(p: Path) -> str | None:
    try:
        if p.stat().st_size > _MAX_MSG_BYTES:
            return None
        return p.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def _reset_for_tests() -> None:
    global _env_loaded
    with _lock:
        _msg_files.clear()
        _loaded.clear()
        _env_loaded = False
