"""Decode any ROS 2 message from the definition embedded in its MCAP file.

rosbag2 stores each topic's full message definition in the file (schema
encoding ``ros2msg``: the type's fields, then every type it uses, each after
a ``MSG: pkg/Type`` line). This module compiles that text into Python
functions, once per type, so a type with no built-in decoder still comes out
as typed fields instead of raw bytes. Built-in and user-registered decoders
keep priority; this is the fallback.

Speed comes from code generation: fixed-size fields that sit next to each
other are read with a single precompiled ``struct`` call, and alignment
padding is worked out at compile time wherever the offset is known (it
stops being known after a string or a sequence, where a runtime align is
emitted instead). Nested types are inlined, so a ``geometry_msgs/Pose``
costs one ``unpack`` of seven doubles.

Two functions come out of the same generated reads:

- :func:`compile_decoder` returns nested dicts (what ``iter_messages``
  yields), shaped like the built-in decoders' output so columns line up:
  ``std_msgs/Header`` as ``{"stamp_sec", "stamp_nsec", "frame_id"}``;
  numeric arrays as lists, or as read-only NumPy views over the message
  bytes past ``LIST_MAX`` elements; ``uint8[]``/``byte[]``/``char[]`` as
  ``bytes``; arrays of strings or messages as lists.
- :func:`compile_row_decoder` returns the flat row ``iter_chunks`` builds
  from that dict (dotted keys, numeric lists of up to 20 values expanded to
  ``field.0``, ``field.1`` ..., everything else that ``_flatten_dict``
  skips left out, same key order), without building the dict first.

The definition comes from the bag file, so it's untrusted input. Names
from it reach the generated source only through ``repr()`` (type names
must also match a strict identifier pattern), so a crafted definition
can't inject code; array sizes and empty element types are bounded so it
can't make a decoder loop without consuming bytes either.

Wire format: CDR little-endian (XCDR1), the encoding every common ROS 2
middleware writes. Alignment is relative to the byte after the 4-byte
encapsulation header, and a sequence's elements are aligned only if it has
any (as Fast-CDR writes them and mcap_ros2 reads them). Other encodings
raise, and the parser turns that into a ``_parse_error`` placeholder.
"""

from __future__ import annotations

import re
import struct
from dataclasses import dataclass
from typing import Any, Callable

import numpy as np

LIST_MAX = 64
ROW_LIST_MAX = 20           # _flatten_dict expands numeric lists up to this length
MAX_ARRAY_LEN = 10_000_000  # past this a length prefix is corrupt, not data

# name: (struct code, size)
_PRIMITIVES = {
    "bool": ("?", 1), "byte": ("B", 1), "char": ("B", 1),
    "int8": ("b", 1), "uint8": ("B", 1),
    "int16": ("h", 2), "uint16": ("H", 2),
    "int32": ("i", 4), "uint32": ("I", 4),
    "int64": ("q", 8), "uint64": ("Q", 8),
    "float32": ("f", 4), "float64": ("d", 8),
}
_BLOB = {"byte", "char", "uint8"}
_NUMPY = {"b": "<i1", "B": "<u1", "?": "?", "h": "<i2", "H": "<u2", "i": "<i4",
          "I": "<u4", "q": "<i8", "Q": "<u8", "f": "<f4", "d": "<f8"}

# Types some writers leave out of the embedded text.
_BUILTIN_TYPES = {
    "builtin_interfaces/Time": "int32 sec\nuint32 nanosec\n",
    "builtin_interfaces/Duration": "int32 sec\nuint32 nanosec\n",
}

_FIELD_TYPE = re.compile(r"([A-Za-z_][\w/]*)(?:<=\d+)?(\[(<=)?(\d*)\])?")
_SEPARATOR = re.compile(r"^=+\s*$", re.M)


class UnsupportedSchema(ValueError):
    """The definition uses something this compiler can't decode."""


class CDRDecodeError(ValueError):
    """A message's bytes don't fit its definition (truncated or corrupt)."""


@dataclass
class _Field:
    name: str
    base: str          # primitive name, "string", or a full "pkg/Type"
    array: str         # "", "fixed" or "seq"
    size: int = 0      # element count for fixed arrays


def _normalize(name: str, package: str = "") -> str:
    parts = name.split("/")
    if len(parts) == 3 and parts[1] == "msg":
        return f"{parts[0]}/{parts[2]}"
    if len(parts) == 2:
        return name
    if len(parts) == 1:
        if name == "Header":
            return "std_msgs/Header"
        return f"{package}/{name}" if package else name
    raise UnsupportedSchema(f"bad type name {name!r}")


def parse_definitions(type_name: str, text: str) -> dict[str, list[_Field]]:
    """``{"pkg/Type": [fields]}`` for the main type and every type it uses."""
    blocks = _SEPARATOR.split(text)
    raw = {_normalize(type_name): blocks[0]}
    for block in blocks[1:]:
        block = block.strip("\n")
        first, _, body = block.partition("\n")
        if not first.startswith("MSG:"):
            raise UnsupportedSchema(f"definition block without a MSG: line: {first!r}")
        raw[_normalize(first[4:].strip())] = body
    for name, body in _BUILTIN_TYPES.items():
        raw.setdefault(name, body)

    defs = {}
    for name, body in raw.items():
        package = name.split("/")[0]
        fields = []
        for line in body.splitlines():
            line = line.split("#", 1)[0].strip()
            if not line:
                continue
            tokens = line.split()
            if len(tokens) < 2 or "=" in tokens[1] or (len(tokens) > 2 and tokens[2].startswith("=")):
                continue  # a constant, not a field
            ftype, fname = tokens[0], tokens[1]
            m = _FIELD_TYPE.fullmatch(ftype)
            if not m:
                raise UnsupportedSchema(f"{name}: can't read field type {ftype!r}")
            base = m.group(1)
            if base == "wstring":
                raise UnsupportedSchema(f"{name}.{fname}: wstring is not supported")
            if base not in _PRIMITIVES and base != "string":
                base = _normalize(base, package)
            if not m.group(2):
                array, size = "", 0
            elif m.group(4) and not m.group(3):
                array, size = "fixed", int(m.group(4))
                if size > MAX_ARRAY_LEN:
                    raise UnsupportedSchema(f"{name}.{fname}: array size {size} is implausible")
            else:
                array, size = "seq", 0
            fields.append(_Field(fname, base, array, size))
        defs[name] = fields
    return defs


# A decoded value, before rendering it as a nested dict or a flat row:
#   ("scalar", expr)            one value
#   ("list", expr, n)           numeric list; n = its length if fixed, else None
#   ("opaque", expr)            kept in the dict, left out of rows (bytes,
#                               NumPy arrays, lists of strings or messages)
#   ("struct", [(key, node)])   nested message
_Node = tuple


class _Gen:
    """Emits the reads of one decode function and the tree of what they hold.

    ``mode`` is "nested" (decode everything, for ``iter_messages``), "row"
    (skip what a row leaves out: arrays of strings or messages, numeric
    arrays past 20 values, byte blobs) or "skip" (decode nothing, just move
    the offset past one value; for elements of skipped arrays).
    """

    def __init__(self, defs: dict[str, list[_Field]], env: dict, counter: list[int],
                 mode: str = "nested"):
        self.mode = mode
        self.defs = defs
        self.env = env
        self.counter = counter
        self.lines: list[str] = []
        self.mod, self.res = 8, 0    # offset is known modulo `mod`; it equals `res`
        self.run: list[tuple[str, str, int]] = []  # queued (temp, kind, count) of a fixed run
        self.run_fmt = ""

    def fresh(self, prefix: str) -> str:
        self.counter[0] += 1
        return f"{prefix}{self.counter[0]}"

    def const(self, value: Any, prefix: str) -> str:
        name = self.fresh(prefix)
        self.env[name] = value
        return name

    # -- alignment -------------------------------------------------------
    def align(self, a: int) -> None:
        """Bring the offset to a multiple of ``a`` (relative to the payload)."""
        if a == 1:
            return
        if a <= self.mod:
            pad = (-self.res) % a
            if pad:
                if self.run:
                    self.run_fmt += f"{pad}x"
                else:
                    self.lines.append(f"o += {pad}")
                self.res = (self.res + pad) % self.mod
            return
        self.flush()
        self.lines.append(_runtime_align(a))
        self.mod, self.res = a, 0

    def advance(self, n: int) -> None:
        self.res = (self.res + n) % self.mod

    def unknown(self) -> None:
        self.mod, self.res = 1, 0

    # -- fixed-size runs -------------------------------------------------
    def fixed(self, code: str, size: int, count: int = 1, kind: str = "scalar") -> str:
        """Queue a fixed-size read and return the temp that will hold it.

        ``kind`` is "scalar" (one value), "list" (``count`` values) or
        "bytes" (``count`` bytes as one ``bytes``).
        """
        self.align(size)
        temp = self.fresh("v")
        self.run.append((temp, kind, count))
        self.run_fmt += code if kind == "scalar" else f"{count}{code}"
        self.advance(size * count)
        return temp

    def skip(self, size: int, count: int) -> None:
        """Move past ``count`` fixed-size values without reading them."""
        if not count:
            return
        self.align(size)
        n = size * count
        if self.run:
            self.run_fmt += f"{n}x"
        else:
            self.lines.append(f"o += {n}")
        self.advance(n)

    def skip_string(self) -> None:
        n = self.fixed("I", 4)
        self.flush()
        self.lines.append(f"if o + {n} > L: raise E('string runs past the message')")
        self.lines.append(f"o += {n}")
        self.unknown()

    def skip_loop(self, base: str, count: str) -> None:
        if base != "string" and not self.defs.get(base):
            raise UnsupportedSchema(f"array of field-less type {base}")
        self.flush()
        elem = _skip_function(base, self.defs, self.env, self.counter)
        self.lines.append(f"for _ in range({count}):")
        self.lines.append(f"    o = {elem}(b, o, L)")
        self.unknown()

    def flush(self) -> None:
        if not self.run:
            return
        s = struct.Struct("<" + self.run_fmt)
        name = self.const(s, "S")
        tup = self.fresh("t")
        self.lines.append(f"{tup} = {name}.unpack_from(b, o)")
        self.lines.append(f"o += {s.size}")
        i = 0
        for temp, kind, count in self.run:
            if kind == "list":
                self.lines.append(f"{temp} = list({tup}[{i}:{i + count}])")
                i += count
            else:  # scalar, or bytes (one item for an "Ns" code)
                self.lines.append(f"{temp} = {tup}[{i}]")
                i += 1
        self.run, self.run_fmt = [], ""

    # -- fields ------------------------------------------------------------
    def struct_node(self, type_name: str) -> _Node:
        if type_name not in self.defs:
            raise UnsupportedSchema(f"definition of {type_name} is missing")
        fields = self.defs[type_name]
        if type_name == "std_msgs/Header" and _is_standard_header(fields, self.defs):
            sec, nsec = (self.field_node(f) for f in self.defs["builtin_interfaces/Time"])
            frame_id = self.field_node(fields[1])
            return ("struct", [("stamp_sec", sec), ("stamp_nsec", nsec), ("frame_id", frame_id)])
        return ("struct", [(f.name, self.field_node(f)) for f in fields])

    def field_node(self, f: _Field) -> _Node:
        dropping = self.mode != "nested"   # rows don't need it
        everything = self.mode == "skip"   # nothing is needed
        none = ("opaque", "None")
        if f.array == "":
            if f.base in _PRIMITIVES:
                code, size = _PRIMITIVES[f.base]
                if everything:
                    self.skip(size, 1)
                    return none
                return ("scalar", self.fixed(code, size))
            if f.base == "string":
                if everything:
                    self.skip_string()
                    return none
                return ("scalar", self.string())
            return self.struct_node(f.base)
        if f.array == "fixed":
            if f.base in _BLOB:
                if dropping:
                    self.skip(1, f.size)
                    return none
                return ("opaque", self.fixed("s", 1, f.size, kind="bytes") if f.size else "b''")
            if f.base in _PRIMITIVES:
                code, size = _PRIMITIVES[f.base]
                if f.size == 0:
                    return ("list", "[]", 0)
                if dropping and (everything or f.size > ROW_LIST_MAX):
                    self.skip(size, f.size)
                    return none
                if f.size > LIST_MAX:
                    return ("opaque", self.numpy_array(code, size, f.size))
                return ("list", self.fixed(code, size, f.size, kind="list"), f.size)
            if dropping:
                self.skip_loop(f.base, str(f.size))
                return none
            return ("opaque", self.loop(f, str(f.size)))
        # sequence: uint32 count, then the elements
        count = self.fixed("I", 4)
        self.flush()
        self.lines.append(f"if {count} > {MAX_ARRAY_LEN}: raise E('sequence length %d' % {count})")
        if f.base in _BLOB:
            self.lines.append(f"if o + {count} > L: raise E('sequence runs past the message')")
            if dropping:
                self.lines.append(f"o += {count}")
                self.unknown()
                return none
            out = self.fresh("v")
            self.lines.append(f"{out} = b[o:o + {count}]")
            self.lines.append(f"o += {count}")
            self.unknown()
            return ("opaque", out)
        if f.base in _PRIMITIVES:
            code, size = _PRIMITIVES[f.base]
            align = _runtime_align(size) or "pass"
            bounds = f"if o + {count} * {size} > L: raise E('sequence runs past the message')"
            if everything:
                self.lines += [f"if {count}:", f"    {align}", f"    {bounds}",
                               f"    o += {count} * {size}"]
                self.unknown()
                return none
            out = self.fresh("v")
            if dropping:
                # Rows expand only lists of up to ROW_LIST_MAX numbers.
                self.lines += [
                    f"if {count}:",
                    f"    {align}",
                    f"    if {count} > {ROW_LIST_MAX}:",
                    f"        {bounds}",
                    f"        {out} = ()",
                    "    else:",
                    f"        {out} = unpack_from('<%d{code}' % {count}, b, o)",
                    f"    o += {count} * {size}",
                    "else:",
                    f"    {out} = ()",
                ]
                self.unknown()
                return ("list", out, None)
            dt = self.const(np.dtype(_NUMPY[code]), "D")
            self.lines += [
                f"if {count}:",
                f"    {align}",
                f"    if {count} > {LIST_MAX}:",
                f"        {out} = np.frombuffer(b, {dt}, {count}, o)",
                "    else:",
                f"        {out} = list(unpack_from('<%d{code}' % {count}, b, o))",
                f"    o += {count} * {size}",
                "else:",
                f"    {out} = []",
            ]
            self.unknown()
            return ("list", out, None)
        if dropping:
            self.skip_loop(f.base, count)
            return none
        return ("opaque", self.loop(f, count))

    def string(self) -> str:
        n = self.fixed("I", 4)
        self.flush()
        out = self.fresh("v")
        self.lines.append(f"if o + {n} > L: raise E('string runs past the message')")
        self.lines.append(f"{out} = b[o:o + {n}].decode('utf-8', 'replace').rstrip('\\x00')")
        self.lines.append(f"o += {n}")
        self.unknown()
        return out

    def numpy_array(self, code: str, size: int, count: int) -> str:
        self.align(size)
        self.flush()
        out = self.fresh("v")
        dt = self.const(np.dtype(_NUMPY[code]), "D")
        self.lines.append(f"{out} = np.frombuffer(b, {dt}, {count}, o)")
        self.lines.append(f"o += {count * size}")
        self.advance(size * count)
        return out

    def loop(self, f: _Field, count: str) -> str:
        """An array of strings or messages, decoded element by element."""
        if f.base != "string" and not self.defs.get(f.base):
            # Each element must consume bytes, or a count from the data
            # could spin the loop without ever running out of message.
            raise UnsupportedSchema(f"array of field-less type {f.base}")
        self.flush()
        elem = _element_function(f.base, self.defs, self.env, self.counter)
        out = self.fresh("v")
        self.lines += [
            f"{out} = []",
            f"for _ in range({count}):",
            f"    e, o = {elem}(b, o, L)",
            f"    {out}.append(e)",
        ]
        self.unknown()
        return out


def _runtime_align(a: int) -> str:
    """A statement aligning ``o`` to ``a`` relative to the payload start (4)."""
    if a == 1:
        return ""
    if a == 8:
        return "o = ((o + 3) & -8) + 4"
    return f"o = (o + {a - 1}) & -{a}"


def _is_standard_header(fields: list[_Field], defs) -> bool:
    time = defs.get("builtin_interfaces/Time", [])
    return (
        [(f.name, f.base, f.array) for f in fields]
        == [("stamp", "builtin_interfaces/Time", ""), ("frame_id", "string", "")]
        and [(f.name, f.base, f.array) for f in time]
        == [("sec", "int32", ""), ("nanosec", "uint32", "")]
    )


def _render_nested(node: _Node) -> str:
    kind = node[0]
    if kind == "struct":
        return "{" + ", ".join(f"{k!r}: {_render_nested(v)}" for k, v in node[1]) + "}"
    return node[1]


def _render_row(node: _Node, prefix: str, out: list[str]) -> None:
    """Statements filling ``row`` the way ``_flatten_dict`` fills it."""
    kind = node[0]
    if kind == "struct":
        for key, child in node[1]:
            if key.startswith("_"):
                continue
            _render_row(child, f"{prefix}.{key}" if prefix else key, out)
    elif kind == "scalar":
        out.append(f"row[{prefix!r}] = {node[1]}")
    elif kind == "list":
        expr, n = node[1], node[2]
        if n is not None:
            if n <= ROW_LIST_MAX:
                out.extend(f"row[{prefix + '.' + str(i)!r}] = {expr}[{i}]" for i in range(n))
        else:
            out += [
                f"if len({expr}) <= {ROW_LIST_MAX}:",
                f"    for i, x in enumerate({expr}):",
                f"        row[{prefix!r} + '.' + str(i)] = x",
            ]
    # opaque values stay out of rows


def _skip_function(base: str, defs, env, counter) -> str:
    """Name of a compiled ``f(b, o, L) -> o`` that moves past one array element."""
    key = ("skip", base)
    if key in env:
        return env[key]
    counter[0] += 1
    name = f"_skip{counter[0]}"
    env[key] = name
    g = _Gen(defs, env, counter, mode="skip")
    g.mod, g.res = 1, 0
    if base == "string":
        g.skip_string()
    else:
        g.struct_node(base)
    g.flush()
    body = "".join("    " + line + "\n" for line in g.lines)
    src = f"def {name}(b, o, L):\n{body}    return o\n"
    exec(compile(src, f"<ros2msg skip {base}>", "exec"), env)
    return name


def _element_function(base: str, defs, env, counter) -> str:
    """Name of a compiled ``f(b, o, L) -> (value, o)`` for one array element."""
    key = ("elem", base)
    if key in env:
        return env[key]
    counter[0] += 1
    name = f"_elem{counter[0]}"
    env[key] = name
    g = _Gen(defs, env, counter)
    g.mod, g.res = 1, 0  # an element can start anywhere
    node = ("scalar", g.string()) if base == "string" else g.struct_node(base)
    g.flush()
    body = "\n".join("    " + line for line in g.lines)
    src = f"def {name}(b, o, L):\n{body}\n    return {_render_nested(node)}, o\n"
    exec(compile(src, f"<ros2msg {base}>", "exec"), env)
    return name


_HEAD = (
    "    if len(b) < 4 or b[1] != 1:\n"
    "        raise E('not little-endian CDR (encapsulation %r)' % bytes(b[:2]))\n"
    "    L = len(b)\n"
    "    o = 4\n"
)
# Real messages end within 3 bytes of padding. Much more left over means
# the definition doesn't describe these bytes.
_TAIL = (
    "    if o > L:\n"
    "        raise E('message is %d bytes shorter than its definition' % (o - L))\n"
    "    if L - o > 7:\n"
    "        raise E('%d bytes left after decoding: the definition does not match' % (L - o))\n"
)

_cache: dict[tuple, Any] = {}


def compile_decoder(type_name: str, definition: str | bytes) -> Callable[[bytes], dict]:
    """``f(cdr_bytes) -> dict`` for one message (encapsulation header included).

    Compiled once per (type, definition) and cached. Raises
    :class:`UnsupportedSchema` if the definition can't be decoded; the
    function itself raises :class:`CDRDecodeError` (or ``struct.error``)
    on bytes that don't fit.
    """
    return _compiled(type_name, definition)[0]


def compile_row_decoder(type_name: str, definition: str | bytes) -> Callable[[bytes, int], dict]:
    """``f(cdr_bytes, timestamp_ns) -> row``: the flat row ``iter_chunks``
    makes from :func:`compile_decoder`'s dict, built directly.

    ``row`` starts with ``timestamp_ns`` and has the same keys, values and
    key order as ``{"timestamp_ns": ts}`` filled by ``_flatten_dict``.
    """
    return _compiled(type_name, definition)[1]


def _compiled(type_name: str, definition: str | bytes) -> tuple[Callable, Callable]:
    raw = definition if isinstance(definition, bytes) else definition.encode()
    key = (type_name, raw)
    hit = _cache.get(key)
    if isinstance(hit, UnsupportedSchema):
        raise hit
    if hit is not None:
        return hit
    try:
        fns = _compile(type_name, raw.decode("utf-8", "replace"))
    except UnsupportedSchema as exc:
        _cache[key] = exc
        raise
    _cache[key] = fns
    return fns


def _compile(type_name: str, text: str) -> tuple[Callable, Callable]:
    defs = parse_definitions(type_name, text)
    main = _normalize(type_name)
    if not defs[main]:
        # A field-less definition (a placeholder, or std_msgs/Empty) says
        # nothing about the bytes, so leave the message undecoded.
        raise UnsupportedSchema(f"definition of {type_name} has no fields")
    env: dict[str, Any] = {"np": np, "unpack_from": struct.unpack_from, "E": CDRDecodeError}
    counter = [0]
    g = _Gen(defs, env, counter)
    r = _Gen(defs, env, counter, mode="row")
    try:
        node = g.struct_node(main)
        g.flush()
        rnode = r.struct_node(main)
        r.flush()
    except RecursionError:
        raise UnsupportedSchema(f"{type_name} contains itself") from None
    body = "".join("    " + line + "\n" for line in g.lines)
    row_reads = "".join("    " + line + "\n" for line in r.lines)

    row_lines: list[str] = []
    _render_row(rnode, "", row_lines)
    row_body = "".join("    " + line + "\n" for line in row_lines)
    src = (
        "def decode(b):\n" + _HEAD + body + _TAIL
        + f"    return {_render_nested(node)}\n\n"
        "def decode_row(b, ts):\n" + _HEAD + row_reads + _TAIL
        + "    row = {'timestamp_ns': ts}\n" + row_body + "    return row\n"
    )
    exec(compile(src, f"<ros2msg {type_name}>", "exec"), env)
    decode, decode_row = env["decode"], env["decode_row"]
    decode.__doc__ = f"Decode {type_name} (generated from its embedded definition)."
    decode_row.__doc__ = f"Decode {type_name} straight to an iter_chunks row."
    decode.source = decode_row.source = src  # for debugging
    return decode, decode_row
