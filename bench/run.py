#!/usr/bin/env python3
"""Benchmark harness: speed, peak memory and decode coverage on real bags.

One command runs the benchmarks and writes one CSV row per measured run::

    PY=~/.venvs/resurrector/bin/python
    $PY bench/run.py                        # week 1 baseline suite
    $PY bench/run.py --suite memory         # benchmark 2: memory vs bag size
    $PY bench/run.py --all --ops coverage   # decode coverage of every downloaded bag
    $PY bench/run.py ~/rosdata/vla/grasp_cup/garmi_demo_010 --ops read,mcap_read
    $PY bench/run.py --dry-run              # show what would run, run nothing

Get the data with ``bash bench/download_datasets.sh`` and the comparison
readers with ``pip install -r bench/requirements.txt``. macOS and Linux.

How it measures, and why:

- Each measured run is a fresh subprocess, timed from after its imports.
  Peak memory is that process's own ``ru_maxrss`` high-water mark, so a
  spike freed before the operation returns still counts.
  ``tests/test_streaming_oom.py`` reads RSS after the call returns and can
  miss one.
- Cold runs drop the OS file cache first (``purge`` on macOS,
  ``drop_caches`` on Linux, both through ``sudo -n``). Warm runs read the
  bag once beforehand. A bag bigger than RAM can't be cached whole, so its
  warm numbers are close to cold; ``bag_vs_ram`` shows when that applies.
- Every row records the git commit, power and thermal state, swap in use
  and load average, and ``run.json`` adds the machine and package versions,
  so a number can be traced back to the conditions it was measured in.
- Resurrector has built-in decoders for 9 message types. Any other type
  comes back as raw bytes (``_unparsed``), and the Resurrector ops count
  those messages. Skipping a message is faster than decoding it, so compare
  throughput against mcap or rosbags only next to that count.

Data lives in $ROSDATA (default ~/rosdata), which must not be a synced
folder. Results go to $ROSDATA/bench-results/<run id>/:

    runs.csv       one row per measured run
    summary.csv    medians per bag, op and cache mode
    coverage.csv   per-topic decode coverage (the ``coverage`` op)
    run.json       machine, versions, git commit and arguments
    logs/          each run's output; holds full local paths, so publish
                   the CSVs and run.json, not the logs
"""

from __future__ import annotations

import argparse
import csv
import getpass
import json
import os
import platform
import re
import resource
import shutil
import statistics
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
ROSDATA = Path(os.environ.get("ROSDATA", "~/rosdata")).expanduser()
SYNCED_MARKERS = ("/Library/CloudStorage/", "/Mobile Documents/", "/Dropbox")

# Bags relative to $ROSDATA. A bag is a rosbag2 directory (metadata.yaml plus
# one .mcap) or a bare .mcap file.
SUITES = {
    # Week 1 baseline: one bag per dataset.
    "baseline": [
        "vla/grasp_cup/garmi_demo_010",                              # 1.6 GB
        "lidar-tactile/mcap/bucket/teleop/2026-05-09-17-08-12/001",  # 0.5 GB
        "construction/site1/site1_session02",                        # 17.5 GB
    ],
    # Benchmark 2: one dataset at three sizes.
    "memory": [
        "construction/site1/site1_session02",                        # 17.5 GB
        "construction/site2/site2_session01",                        # 34.0 GB
        "construction/site1/site1_session01",                        # 55.7 GB
    ],
}

OPS = {
    "coverage": "Resurrector: share of messages with a built-in decoder (summary only, once per bag)",
    "read": "Resurrector: decode every message in one pass (parser.read_messages)",
    "mcap_read": "mcap + mcap-ros2-support: decode every message from the embedded schemas",
    "rosbags_read": "rosbags AnyReader: decode every message",
    "chunks": "Resurrector: every topic as DataFrame chunks (iter_chunks, one pass per topic)",
    "health": "Resurrector: BagFrame.health_report()",
    "export_parquet": "Resurrector: export every topic to Parquet",
}

FIELDS = [
    "run_id", "label", "time_utc", "git_commit", "git_dirty", "resurrector", "machine",
    "dataset", "dataset_rev", "bag", "bag_gb", "bag_vs_ram",
    "op", "cache", "repeat", "status", "error",
    "wall_s", "cpu_user_s", "cpu_sys_s", "mb_per_s", "msgs", "msgs_per_s", "msgs_unparsed",
    "rss_base_mb", "rss_peak_mb", "rss_op_mb",
    "power", "battery_pct", "low_power_mode", "thermal", "load_1m", "swap_used_mb", "mem_free_pct",
    "extra",
]

COLD_HELP = """\
Cold-cache runs drop the OS file cache with `sudo -n`, which failed: {err}

Run `sudo -v` just before starting. If one run can take longer than sudo's
5-minute window, allow that single command without a password instead:

    sudo visudo -f /etc/sudoers.d/purge
    # add this line:  {user} ALL=(root) NOPASSWD: /usr/sbin/purge
"""


# ---------------------------------------------------------------------------
# Child side: one process per measured run. Runs exactly one op.

def _peak_rss_bytes() -> int:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024  # Linux reports KiB


class Meter:
    """Starts the measured region once an op has done its imports."""

    def __init__(self) -> None:
        self.t0: float | None = None

    def start(self) -> None:
        ru = resource.getrusage(resource.RUSAGE_SELF)
        self.cpu0 = (ru.ru_utime, ru.ru_stime)
        self.rss0 = _peak_rss_bytes()
        self.t0 = time.perf_counter()

    def stop(self) -> dict:
        if self.t0 is None:
            return {}
        wall = time.perf_counter() - self.t0
        ru = resource.getrusage(resource.RUSAGE_SELF)
        peak = _peak_rss_bytes()
        return {
            "wall_s": wall,
            "cpu_user_s": ru.ru_utime - self.cpu0[0],
            "cpu_sys_s": ru.ru_stime - self.cpu0[1],
            "rss_base_mb": self.rss0 / 1e6,
            "rss_peak_mb": peak / 1e6,
            "rss_op_mb": (peak - self.rss0) / 1e6,
        }


def _decodable_types(summary) -> set[str]:
    """Types this resurrector checkout decodes: registered decoders, plus any
    type its schema compiler (if it has one) accepts from the bag."""
    from resurrector.ingest.parser import list_decoders

    known = set(list_decoders())
    try:
        from resurrector.ingest.ros2msg import UnsupportedSchema, compile_decoder
    except ImportError:  # checkouts before schema decoding
        return known
    for schema in summary.schemas.values():
        if schema.name not in known and schema.encoding == "ros2msg":
            try:
                compile_decoder(schema.name, schema.data)
                known.add(schema.name)
            except UnsupportedSchema:
                pass
    return known


def op_coverage(m: Meter, mcap: Path, bag_dir: Path | None, scratch: Path) -> dict:
    from mcap.reader import make_reader
    import resurrector.ingest.parser  # noqa: F401  (registers the built-in decoders)

    m.start()
    with open(mcap, "rb") as f:
        summary = make_reader(f).get_summary()
    if summary is None:
        raise RuntimeError("no summary section; the recording may have been cut off")
    known = _decodable_types(summary)
    counts = summary.statistics.channel_message_counts if summary.statistics else {}
    topics = []
    for ch in summary.channels.values():
        schema = summary.schemas.get(ch.schema_id)
        msg_type = schema.name if schema else "unknown"
        topics.append({
            "topic": ch.topic,
            "msg_type": msg_type,
            "messages": counts.get(ch.id, 0),
            "decodable": msg_type in known,
        })
    return {
        "msgs": sum(t["messages"] for t in topics),
        "msgs_unparsed": sum(t["messages"] for t in topics if not t["decodable"]),
        "extra": {
            "topics": len(topics),
            "topics_decodable": sum(t["decodable"] for t in topics),
        },
        "coverage": topics,
    }


def op_read(m: Meter, mcap: Path, bag_dir: Path | None, scratch: Path) -> dict:
    from resurrector.ingest.parser import parse_bag

    m.start()
    n = unparsed = errors = 0
    for msg in parse_bag(mcap).read_messages():
        n += 1
        if msg.data.get("_unparsed"):
            unparsed += 1
        elif msg.data.get("_parse_error"):
            errors += 1
    return {"msgs": n, "msgs_unparsed": unparsed, "extra": {"parse_errors": errors}}


def op_mcap_read(m: Meter, mcap: Path, bag_dir: Path | None, scratch: Path) -> dict:
    from mcap.reader import make_reader
    from mcap_ros2.decoder import DecoderFactory

    m.start()
    n = 0
    with open(mcap, "rb") as f:
        reader = make_reader(f, decoder_factories=[DecoderFactory()])
        for _ in reader.iter_decoded_messages():
            n += 1
    return {"msgs": n}


def op_rosbags_read(m: Meter, mcap: Path, bag_dir: Path | None, scratch: Path) -> dict:
    from rosbags.highlevel import AnyReader
    from rosbags.typesys import Stores, get_typestore

    typestore = get_typestore(Stores.ROS2_HUMBLE)  # only for types a bag doesn't embed
    m.start()
    n = 0
    # The .mcap file itself, as the other readers get it. rosbags 0.11.10's
    # bag-directory mode (metadata.yaml) yields no messages at all from the
    # construction bags; file mode reads them in full.
    with AnyReader([mcap], default_typestore=typestore) as reader:
        for conn, _ts, raw in reader.messages(connections=reader.connections):
            reader.deserialize(raw, conn.msgtype)
            n += 1
    return {"msgs": n}


def op_chunks(m: Meter, mcap: Path, bag_dir: Path | None, scratch: Path) -> dict:
    from resurrector.core.bag_frame import BagFrame

    m.start()
    bf = BagFrame(mcap)
    rows = unparsed = 0
    for topic in bf.topic_names:
        for chunk in bf[topic].iter_chunks():
            rows += chunk.height
            # Rows with nothing but a timestamp: undecoded messages (their
            # placeholder keys start with "_" and are dropped), and decoded
            # ones whose fields don't flatten (lists of transforms).
            if chunk.width == 1:
                unparsed += chunk.height
    return {"msgs": rows, "msgs_unparsed": unparsed, "extra": {"topics": len(bf.topic_names)}}


def op_health(m: Meter, mcap: Path, bag_dir: Path | None, scratch: Path) -> dict:
    from resurrector.core.bag_frame import BagFrame

    m.start()
    bf = BagFrame(mcap)
    report = bf.health_report()
    return {
        "msgs": bf.message_count,
        "extra": {"score": report.score, "issues": len(report.issues)},
    }


def op_export_parquet(m: Meter, mcap: Path, bag_dir: Path | None, scratch: Path) -> dict:
    from resurrector.core.bag_frame import BagFrame
    from resurrector.core.export import Exporter

    m.start()
    bf = BagFrame(mcap)
    out = scratch / "parquet"
    Exporter().export(bf, topics=bf.topic_names, format="parquet", output_dir=str(out))
    size = sum(p.stat().st_size for p in out.rglob("*") if p.is_file())
    return {"msgs": bf.message_count, "extra": {"output_mb": round(size / 1e6, 1)}}


CHILD_OPS = {
    "coverage": op_coverage,
    "read": op_read,
    "mcap_read": op_mcap_read,
    "rosbags_read": op_rosbags_read,
    "chunks": op_chunks,
    "health": op_health,
    "export_parquet": op_export_parquet,
}


def child_main(spec_file: str) -> int:
    spec = json.loads(Path(spec_file).read_text())
    # Import resurrector from the tree under test, ahead of the editable install.
    sys.path.insert(0, spec["src"])
    meter = Meter()
    bag_dir = Path(spec["bag_dir"]) if spec["bag_dir"] else None
    try:
        op = CHILD_OPS[spec["op"]]
        out = {"status": "ok", **op(meter, Path(spec["mcap"]), bag_dir, Path(spec["scratch"]))}
    except ImportError as exc:
        out = {"status": "skipped", "error": f"{exc}. Run: pip install -r bench/requirements.txt"}
    except Exception as exc:
        traceback.print_exc()
        out = {"status": "error", "error": f"{type(exc).__name__}: {exc}"[:500]}
    out.update(meter.stop())
    Path(spec["result"]).write_text(json.dumps(out, default=str))
    return 0


# ---------------------------------------------------------------------------
# Parent side: plans runs, controls the cache, records conditions, writes CSVs.

@dataclass
class Bag:
    label: str
    mcap: Path | None
    bag_dir: Path | None
    dataset: str
    rev: str
    size: int
    problem: str = ""
    messages: int | None = None  # from the bag's index, to check full reads


# Ops that must read every message; a different count means a silent short read.
FULL_READ_OPS = {"read", "mcap_read", "rosbags_read", "chunks"}


def indexed_message_count(mcap: Path) -> int | None:
    try:
        from mcap.reader import make_reader
        with open(mcap, "rb") as f:
            summary = make_reader(f).get_summary()
        return summary.statistics.message_count if summary and summary.statistics else None
    except Exception:
        return None


def _sh(*cmd: str) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=60).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def in_synced_folder(path: Path) -> bool:
    p = str(path.expanduser().resolve())
    return any(marker in p for marker in SYNCED_MARKERS)


def dataset_revision(mcap: Path) -> str:
    """Commit the file was downloaded at, from the metadata `hf download` leaves."""
    for root in mcap.parents:
        meta = root / ".cache" / "huggingface" / "download" / f"{mcap.relative_to(root)}.metadata"
        if meta.is_file():
            return meta.read_text().split("\n", 1)[0].strip()[:12]
        if root == ROSDATA:
            break
    return ""


def resolve_bag(path: Path) -> Bag:
    path = path.expanduser().resolve()
    try:
        rel = path.relative_to(ROSDATA.resolve())
        label, dataset = str(rel), rel.parts[0]
    except ValueError:
        label, dataset = path.name, "-"
    mcap, problem = None, ""
    if path.is_dir():
        found = sorted(path.glob("*.mcap"))
        if len(found) > 1:
            problem = f"{len(found)} .mcap files (split bag); pass them one at a time"
        elif found:
            mcap = found[0]
    elif path.suffix == ".mcap" and path.is_file():
        mcap = path
    if mcap is None:
        return Bag(label, None, None, dataset, "", 0, problem or "not downloaded")
    bag_dir = mcap.parent if (mcap.parent / "metadata.yaml").is_file() else None
    return Bag(label, mcap, bag_dir, dataset, dataset_revision(mcap), mcap.stat().st_size,
               messages=indexed_message_count(mcap))


def discover_all() -> list[Path]:
    skip = {".cache", "bench-results", "bench-scratch"}
    seen: list[Path] = []
    for mcap in sorted(ROSDATA.rglob("*.mcap")):
        if skip & set(mcap.relative_to(ROSDATA).parts):
            continue
        bag = mcap.parent if (mcap.parent / "metadata.yaml").is_file() else mcap
        if bag not in seen:
            seen.append(bag)
    return seen


def machine_info() -> dict:
    info = {
        "os": f"{platform.system()} {platform.release()}",
        "arch": platform.machine(),
        "cpus": os.cpu_count(),
        "python": platform.python_version(),
        "model": "",
        "chip": platform.processor(),
        "ram_bytes": 0,
    }
    if sys.platform == "darwin":
        info["os"] = "macOS " + _sh("sw_vers", "-productVersion")
        info["chip"] = _sh("sysctl", "-n", "machdep.cpu.brand_string")
        info["ram_bytes"] = int(_sh("sysctl", "-n", "hw.memsize") or 0)
        try:
            hw = json.loads(_sh("system_profiler", "SPHardwareDataType", "-json"))
            # Only the model name: the same record holds the serial number.
            info["model"] = hw["SPHardwareDataType"][0].get("machine_name", "")
        except (ValueError, KeyError, IndexError):
            info["model"] = _sh("sysctl", "-n", "hw.model")
    elif sys.platform.startswith("linux"):
        m = re.search(r"MemTotal:\s+(\d+) kB", Path("/proc/meminfo").read_text())
        info["ram_bytes"] = int(m.group(1)) * 1024 if m else 0
        m = re.search(r"model name\s*:\s*(.+)", Path("/proc/cpuinfo").read_text())
        if m:
            info["chip"] = m.group(1).strip()
    return info


def resurrector_import(src: Path) -> tuple[str, str]:
    """(version, package dir) of the resurrector the children will import.

    An editable install's metadata can lag the code (it said 0.7.1 while the
    code was 0.8.7), so this asks the package itself, the same way a child
    imports it.
    """
    out = _sh(sys.executable, "-I", "-c",
              f"import sys; sys.path.insert(0, {str(src)!r}); import resurrector, os; "
              "print(resurrector.__version__); print(os.path.dirname(resurrector.__file__))")
    lines = out.splitlines()
    return (lines[0], lines[1]) if len(lines) == 2 else ("", "")


def package_versions(src: Path) -> dict:
    from importlib.metadata import PackageNotFoundError, version

    out = {}
    for dist in ("mcap", "mcap-ros2-support", "rosbags", "polars", "numpy",
                 "pyarrow", "duckdb", "lz4", "zstandard"):
        try:
            out[dist] = version(dist)
        except PackageNotFoundError:
            out[dist] = ""
    out["rosbag-resurrector"] = resurrector_import(src)[0]
    return out


def git_info(src: Path) -> dict:
    commit = _sh("git", "-C", str(src), "rev-parse", "HEAD")
    branch = _sh("git", "-C", str(src), "rev-parse", "--abbrev-ref", "HEAD")
    status = _sh("git", "-C", str(src), "status", "--porcelain", "--untracked-files=no")
    return {"commit": commit, "branch": branch, "dirty": bool(status)}


def conditions() -> dict:
    c = {
        "power": "", "battery_pct": "", "low_power_mode": "", "thermal": "",
        "load_1m": round(os.getloadavg()[0], 2), "swap_used_mb": "", "mem_free_pct": "",
    }
    if sys.platform == "darwin":
        m = re.search(r"used = ([\d.]+)M", _sh("sysctl", "-n", "vm.swapusage"))
        c["swap_used_mb"] = round(float(m.group(1))) if m else ""
        c["mem_free_pct"] = _sh("sysctl", "-n", "kern.memorystatus_level")
        batt = _sh("pmset", "-g", "batt")
        m = re.search(r"'([^']+)'", batt)
        c["power"] = m.group(1) if m else ""
        m = re.search(r"(\d+)%", batt)
        c["battery_pct"] = m.group(1) if m else ""
        m = re.search(r"lowpowermode\s+(\d)", _sh("pmset", "-g"))
        c["low_power_mode"] = (m.group(1) == "1") if m else ""
        notes = [ln.strip() for ln in _sh("pmset", "-g", "therm").splitlines()
                 if ln.strip() and "has been recorded" not in ln]
        c["thermal"] = "; ".join(notes) or "none"
    elif sys.platform.startswith("linux"):
        mi = dict(re.findall(r"(\w+):\s+(\d+) kB", Path("/proc/meminfo").read_text()))
        c["swap_used_mb"] = round((int(mi.get("SwapTotal", 0)) - int(mi.get("SwapFree", 0))) / 1024)
        if mi.get("MemTotal"):
            c["mem_free_pct"] = round(100 * int(mi.get("MemAvailable", 0)) / int(mi["MemTotal"]))
    return c


def condition_warnings(c: dict) -> list[str]:
    w = []
    if c["power"] and c["power"] != "AC Power":
        w.append(f"running on {c['power']}: plug in the charger")
    if c["low_power_mode"] is True:
        w.append("Low Power Mode is on, which slows the CPU: turn it off")
    if c["thermal"] not in ("", "none"):
        w.append(f"thermal warning: {c['thermal']}")
    if isinstance(c["swap_used_mb"], (int, float)) and c["swap_used_mb"] > 2000:
        w.append(f"{c['swap_used_mb'] / 1000:.1f} GB of swap in use: "
                 "reboot and close other apps before a run you'll publish")
    if c["load_1m"] > 1.5:
        w.append(f"load average {c['load_1m']}: something else is using the CPU")
    return w


def drop_caches() -> str | None:
    """Drop the OS file cache. Returns an error message, or None on success."""
    if sys.platform == "darwin":
        cmd = ["sudo", "-n", "/usr/sbin/purge"]
    elif sys.platform.startswith("linux"):
        cmd = ["sudo", "-n", "sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"]
    else:
        return f"not supported on {sys.platform}"
    r = subprocess.run(cmd, capture_output=True, text=True)
    return None if r.returncode == 0 else (r.stderr.strip() or f"exit code {r.returncode}")


def fits_in_cache(bag: Bag, ram: int) -> bool:
    return not ram or bag.size < 0.8 * ram


def prime_cache(path: Path) -> None:
    buf = bytearray(16 << 20)
    with open(path, "rb", buffering=0) as f:
        while f.readinto(buf):
            pass


def _num(value, digits: int):
    if value in (None, ""):
        return ""
    rounded = round(float(value), digits)
    return int(rounded) if digits == 0 else rounded


class Runner:
    def __init__(self, args: argparse.Namespace, run_id: str, out_dir: Path, git: dict) -> None:
        self.args = args
        self.run_id = run_id
        self.out_dir = out_dir
        self.logs = out_dir / "logs"
        self.scratch_root = ROSDATA / "bench-scratch" / run_id
        self.machine = machine_info()
        self.ram = self.machine["ram_bytes"]
        self.git = git
        self.versions = package_versions(args.src)
        self.machine_label = " ".join(
            x for x in (self.machine["model"], self.machine["chip"],
                        f"{self.ram / 2**30:.0f}GB" if self.ram else "") if x)
        self.rows: list[dict] = []

    def measure(self, bag: Bag, op: str, cache: str, repeat: int) -> tuple[dict, dict]:
        scratch = self.scratch_root / f"{op}-{cache}-{repeat}"
        scratch.mkdir(parents=True, exist_ok=True)
        spec_file, result_file = scratch / "spec.json", scratch / "result.json"
        spec_file.write_text(json.dumps({
            "op": op, "mcap": str(bag.mcap), "bag_dir": str(bag.bag_dir or ""),
            "scratch": str(scratch), "result": str(result_file), "src": str(self.args.src),
        }))
        cond = conditions()
        safe = re.sub(r"[^A-Za-z0-9._-]+", "_", bag.label)
        log_path = self.logs / f"{safe}__{op}__{cache}__{repeat}.log"

        if op == "export_parquet" and shutil.disk_usage(self.scratch_root).free < 2 * bag.size + 10e9:
            res = {"status": "skipped", "error": "not enough free disk for the export"}
        else:
            cmd = [sys.executable, "-I", str(Path(__file__).resolve()), "--_child", str(spec_file)]
            with open(log_path, "w") as log:
                try:
                    proc = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT,
                                          timeout=self.args.timeout)
                    if result_file.is_file():
                        res = json.loads(result_file.read_text())
                    else:
                        hint = " (SIGKILL: out of memory?)" if proc.returncode == -9 else ""
                        res = {"status": "crashed", "error": f"exit code {proc.returncode}{hint}"}
                except subprocess.TimeoutExpired:
                    res = {"status": "timeout", "error": f"stopped after {self.args.timeout:.0f} s"}
        shutil.rmtree(scratch, ignore_errors=True)
        if (res["status"] == "ok" and op in FULL_READ_OPS and bag.messages is not None
                and res.get("msgs") != bag.messages):
            res["status"] = "short_read"
            res["error"] = f"read {res.get('msgs')} of the {bag.messages} messages the bag's index lists"

        wall = res.get("wall_s")
        ok = res["status"] == "ok" and wall
        msgs = res.get("msgs", "")
        row = {
            "run_id": self.run_id,
            "label": self.args.label,
            "time_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "git_commit": self.git["commit"][:12],
            "git_dirty": self.git["dirty"],
            "resurrector": self.versions.get("rosbag-resurrector", ""),
            "machine": self.machine_label,
            "dataset": bag.dataset,
            "dataset_rev": bag.rev,
            "bag": bag.label,
            "bag_gb": round(bag.size / 1e9, 3),
            "bag_vs_ram": round(bag.size / self.ram, 2) if self.ram else "",
            "op": op,
            "cache": cache,
            "repeat": repeat,
            "status": res["status"],
            # CSVs get published; keep the home directory out of them.
            "error": res.get("error", "").replace(str(Path.home()), "~"),
            "wall_s": _num(wall, 3),
            "cpu_user_s": _num(res.get("cpu_user_s"), 3),
            "cpu_sys_s": _num(res.get("cpu_sys_s"), 3),
            "mb_per_s": round(bag.size / 1e6 / wall, 1) if ok and op != "coverage" else "",
            "msgs": msgs,
            "msgs_per_s": round(msgs / wall) if ok and msgs and op != "coverage" else "",
            "msgs_unparsed": res.get("msgs_unparsed", ""),
            "rss_base_mb": _num(res.get("rss_base_mb"), 1),
            "rss_peak_mb": _num(res.get("rss_peak_mb"), 1),
            "rss_op_mb": _num(res.get("rss_op_mb"), 1),
            **cond,
            "extra": json.dumps(res["extra"], separators=(",", ":")) if res.get("extra") else "",
        }
        return row, res


def summarize(rows: list[dict]) -> list[dict]:
    groups: dict[tuple, list[dict]] = {}
    for r in rows:
        if r["status"] == "ok" and r["op"] != "coverage":
            groups.setdefault((r["bag"], r["op"], r["cache"]), []).append(r)

    def med(rs: list[dict], key: str):
        vals = [float(r[key]) for r in rs if r[key] != ""]
        return statistics.median(vals) if vals else ""

    out = []
    for (bag, op, cache), rs in groups.items():
        out.append({
            "bag": bag, "op": op, "cache": cache, "runs": len(rs), "bag_gb": rs[0]["bag_gb"],
            "wall_s": _num(med(rs, "wall_s"), 2),
            "mb_per_s": _num(med(rs, "mb_per_s"), 1),
            "msgs_per_s": _num(med(rs, "msgs_per_s"), 0),
            "rss_peak_mb": _num(med(rs, "rss_peak_mb"), 0),
            "rss_peak_mb_max": _num(max(float(r["rss_peak_mb"]) for r in rs), 0),
            "msgs": rs[0]["msgs"], "msgs_unparsed": rs[0]["msgs_unparsed"],
        })
    return out


def _write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def _progress(i: int, total: int, row: dict) -> str:
    head = f"[{i:>3}/{total}] {row['bag'][-44:]:<44} {row['op']:<14} {row['cache']:<4} #{row['repeat']}"
    if row["status"] != "ok":
        return f"{head}  {row['status'].upper()}: {row['error'][:120]}"
    parts = [f"{row['wall_s']:>8.1f} s"]
    if row["mb_per_s"] != "":
        parts.append(f"{row['mb_per_s']:>7.1f} MB/s")
    if row["rss_peak_mb"] != "":
        parts.append(f"peak {row['rss_peak_mb']:>7.0f} MB")
    if row["msgs"] and row["msgs_unparsed"] != "":
        # For chunks this counts rows with nothing but a timestamp: undecoded
        # messages, and decoded ones whose fields don't flatten (TF's lists).
        what = "timestamp-only rows" if row["op"] == "chunks" else "undecoded"
        parts.append(f"{100 * row['msgs_unparsed'] / row['msgs']:.0f}% {what}")
    return f"{head}  " + "  ".join(parts)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Benchmark Resurrector on real bags. See the module docstring for the method.")
    ap.add_argument("bags", nargs="*", type=Path,
                    help="bag directories or .mcap files (default: the --suite)")
    ap.add_argument("--suite", choices=sorted(SUITES), default="baseline")
    ap.add_argument("--all", action="store_true", help="every bag under $ROSDATA")
    ap.add_argument("--ops", default=",".join(OPS),
                    help="comma-separated, from: " + ", ".join(OPS))
    ap.add_argument("--repeats", type=int, default=3, help="runs per bag, op and cache mode")
    ap.add_argument("--cache", choices=("warm", "cold", "both"), default="warm",
                    help="cold runs drop the OS file cache first and need sudo")
    ap.add_argument("--cooldown", type=float, default=30.0,
                    help="max seconds to rest after a run; rests as long as the run took, "
                         "up to this, so a fanless laptop cools between runs")
    ap.add_argument("--timeout", type=float, default=3600.0, help="seconds before a run is stopped")
    ap.add_argument("--out", type=Path, default=ROSDATA / "bench-results")
    ap.add_argument("--src", type=Path, default=REPO,
                    help="source tree to benchmark, e.g. a `git worktree` of main "
                         "(default: this checkout)")
    ap.add_argument("--label", default="", help="free-text tag stored with every row")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--_child", help=argparse.SUPPRESS)
    args = ap.parse_args(argv)

    if args._child:
        return child_main(args._child)

    ops = [o.strip() for o in args.ops.split(",") if o.strip()]
    unknown = [o for o in ops if o not in OPS]
    if unknown:
        ap.error(f"unknown op(s): {', '.join(unknown)}")
    timed_ops = [o for o in ops if o != "coverage"]
    modes = ["warm", "cold"] if args.cache == "both" else [args.cache]

    for d in (ROSDATA, args.out):
        if in_synced_folder(d):
            print(f"{d} is inside a synced folder (Dropbox, iCloud). Use a local path.", file=sys.stderr)
            return 2

    if args.bags:
        paths = args.bags
    elif args.all:
        paths = discover_all()
    else:
        paths = [ROSDATA / b for b in SUITES[args.suite]]
    bags = [resolve_bag(p) for p in paths]
    ready = [b for b in bags if not b.problem]
    total = len(ready) * (("coverage" in ops) + len(timed_ops) * args.repeats * len(modes))

    source = "given bags" if args.bags else ("every bag under $ROSDATA" if args.all else f"suite '{args.suite}'")
    print(f"{source}: {len(ready)} of {len(bags)} bags ready, {total} runs "
          f"({', '.join(ops)}; cache {'+'.join(modes)}; {args.repeats} repeats)")
    for b in bags:
        state = b.problem.upper() if b.problem else f"{b.size / 1e9:6.2f} GB"
        print(f"  {state:>15}  {b.label}")
    if args.dry_run or not ready:
        return 0 if args.dry_run else 1

    if "cold" in modes:
        err = drop_caches()
        if err:
            sys.stdout.flush()
            print(COLD_HELP.format(err=err, user=getpass.getuser()), file=sys.stderr)
            return 2

    args.src = args.src.expanduser().resolve()
    _, pkg_dir = resurrector_import(args.src)
    if not pkg_dir or Path(pkg_dir).resolve().parent != args.src:
        print(f"resurrector imports from {pkg_dir or '(nowhere)'}, not from --src {args.src}",
              file=sys.stderr)
        return 2

    git = git_info(args.src)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + (f"-{git['commit'][:7]}" if git["commit"] else "")
    out_dir = args.out / run_id
    runner = Runner(args, run_id, out_dir, git)
    runner.logs.mkdir(parents=True, exist_ok=True)
    runner.scratch_root.mkdir(parents=True, exist_ok=True)

    # Keep the Mac awake until this process exits (an idle sleep stalls a run).
    if sys.platform == "darwin" and shutil.which("caffeinate"):
        subprocess.Popen(["caffeinate", "-i", "-w", str(os.getpid())])

    cond = conditions()
    (out_dir / "run.json").write_text(json.dumps({
        "run_id": run_id,
        "label": args.label,
        "started_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "git": runner.git,
        "machine": runner.machine,
        "versions": runner.versions,
        "conditions_at_start": cond,
        "args": {"ops": ops, "cache": modes, "repeats": args.repeats,
                 "cooldown": args.cooldown, "timeout": args.timeout},
        "bags": [{"bag": b.label, "dataset": b.dataset, "dataset_rev": b.rev,
                  "bytes": b.size} for b in ready],
        "ops": {o: OPS[o] for o in ops},
    }, indent=2))

    print(f"machine: {runner.machine_label}, {runner.machine['os']}, Python {runner.machine['python']}")
    print(f"git: {runner.git['branch']} {runner.git['commit'][:12]}{' (uncommitted changes)' if runner.git['dirty'] else ''}"
          f", resurrector {runner.versions['rosbag-resurrector']}")
    for w in condition_warnings(cond):
        print(f"  WARNING: {w}")
    print(f"results: {out_dir}\n")

    runs_f = open(out_dir / "runs.csv", "w", newline="")
    writer = csv.DictWriter(runs_f, fieldnames=FIELDS)
    writer.writeheader()
    coverage_rows: list[dict] = []
    i = 0

    def record(row: dict) -> None:
        nonlocal i
        i += 1
        writer.writerow(row)
        runs_f.flush()  # partial results survive an interrupted run
        runner.rows.append(row)
        print(_progress(i, total, row), flush=True)

    try:
        for bag in ready:
            if "coverage" in ops:
                row, res = runner.measure(bag, "coverage", "-", 1)
                record(row)
                for t in res.get("coverage", []):
                    coverage_rows.append({"run_id": run_id, "dataset": bag.dataset, "bag": bag.label, **t})
            for mode in modes:
                primed = False
                for rep in range(1, args.repeats + 1):
                    for op in timed_ops:
                        if mode == "cold":
                            err = drop_caches()
                            if err:
                                record({**dict.fromkeys(FIELDS, ""), "run_id": run_id, "bag": bag.label,
                                        "op": op, "cache": mode, "repeat": rep, "status": "error",
                                        "error": f"cache drop failed: {err}"})
                                continue
                        elif not primed:
                            if not fits_in_cache(bag, runner.ram):
                                print(f"  note: {bag.label} is bigger than 80% of RAM; warm runs are close to cold")
                            else:
                                prime_cache(bag.mcap)
                            primed = True
                        row, _ = runner.measure(bag, op, mode, rep)
                        record(row)
                        if row["wall_s"] != "":
                            time.sleep(min(args.cooldown, float(row["wall_s"])))
    except KeyboardInterrupt:
        print("\ninterrupted; summarizing the runs that finished")
    finally:
        runs_f.close()
        shutil.rmtree(runner.scratch_root, ignore_errors=True)

    _write_csv(out_dir / "coverage.csv", coverage_rows)
    summary = summarize(runner.rows)
    _write_csv(out_dir / "summary.csv", summary)

    if coverage_rows:
        print("\ndecode coverage (messages with a built-in decoder):")
        for bag in ready:
            ts = [t for t in coverage_rows if t["bag"] == bag.label]
            n = sum(t["messages"] for t in ts)
            if not n:
                continue
            ok = sum(t["messages"] for t in ts if t["decodable"])
            missing = sorted({t["msg_type"] for t in ts if not t["decodable"]})
            print(f"  {bag.label}: {ok:,} of {n:,} ({100 * ok / n:.0f}%); "
                  f"no decoder for {len(missing)} type(s): {', '.join(missing[:6])}"
                  f"{' ...' if len(missing) > 6 else ''}")
    if summary:
        print(f"\n{'bag':<44} {'op':<14} {'cache':<5} {'n':>2} {'median s':>9} {'MB/s':>8} {'peak MB':>8}")
        for s in summary:
            print(f"{s['bag'][-44:]:<44} {s['op']:<14} {s['cache']:<5} {s['runs']:>2} "
                  f"{s['wall_s']:>9} {s['mb_per_s']:>8} {s['rss_peak_mb']:>8}")
    failed = [r for r in runner.rows if r["status"] not in ("ok",)]
    if failed:
        print(f"\n{len(failed)} run(s) did not finish cleanly; see the status and error columns in runs.csv")
    print(f"\nresults: {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
