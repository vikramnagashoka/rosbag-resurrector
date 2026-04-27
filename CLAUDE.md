# CLAUDE.md — orientation for AI assistants working in this repo

This file is read automatically by Claude Code at the start of every session. It exists so that an assistant on any machine — fresh session, no prior context — can be useful immediately without having to re-derive the project's architecture, conventions, and unwritten rules from the codebase.

If you're a human, this is also a fast project tour.

---

## What this project is

**RosBag Resurrector** — a pandas-like data analysis tool for ROS 2 (MCAP) bag files. Health validation, multi-stream sync, ML-ready export (LeRobot/RLDS), CLIP-powered semantic frame search, cross-bag overlay, and a PlotJuggler-compatible WebSocket bridge. No ROS install required.

Positioned as a **single-user localhost developer tool**. Security model is "127.0.0.1 by default + path validation" — sufficient for the use case. Don't introduce auth/RBAC/multi-tenant patterns; if/when team-mode is on the roadmap, that's a separate project.

It is **not** trying to displace Foxglove (3D scenes), PlotJuggler (super-fast OpenGL plotting), or rosbags (pure-Python custom-message read/write). It's a *bag-as-dataframe* analysis workbench. Keep claims narrowed to that lane.

---

## The performance contract (read this first)

> **Memory must be bounded by a configured chunk/window size, not by bag size, topic size, or export size.**

This is the rule. It applies to: dashboard plotting, sync, health checks, density, cross-bag overlay, `iter_chunks()`, `materialize_ipc_cache()`, and the chunk-streaming export formats (Parquet, HDF5, CSV, Zarr, LeRobot, RLDS).

Two explicit exceptions, both documented to users:

- **NumPy `.npz` export** is bounded by total converted-array size. Hard-capped at 1 M rows (`NUMPY_HARD_CAP` in `resurrector/core/export.py`). Past that → `LargeTopicError` pointing at Parquet.
- **Eager `bf["/topic"].to_polars()` / `.to_pandas()` / `.to_numpy()`** materializes the full topic. Refuses topics > `LARGE_TOPIC_THRESHOLD` (1 M, in `resurrector/core/bag_frame.py`) unless caller passes `force=True`.

The contract is verified by [tests/test_streaming_oom.py](tests/test_streaming_oom.py), gated behind `@pytest.mark.slow` (run via `pytest -m slow`). CI runs the slow tier in the `memory-regression` job. **If you add a new hot path, add a corresponding regression test or you're back to "hopes" instead of "verified."**

### Things that violate the contract (don't do these on hot paths)

- `view.to_polars()` then post-process — defeats the contract; the materialization already happened.
- `list(view.iter_chunks())` — same problem in disguise.
- `dict[str, list[int]]` of per-topic timestamps — was the v0.3.x health/density anti-pattern.
- Any function that takes a `pl.DataFrame` of "the whole topic" as input is a yellow flag — make sure the caller can produce it streaming, or refuse big inputs.

### Things that are fine

- `view.iter_chunks(chunk_size=N)` — the streaming primitive everything else routes through.
- `view.materialize_ipc_cache()` — for filter/projection pushdown via Polars LazyFrame; explicit lifecycle (context manager).
- Per-topic running aggregators (Welford, bin counters, lookahead deques bounded by `tolerance × rate`).
- `bf.health_report()`, `compute_density()`, `bf.sync(engine="streaming")`, `stream_bucketed_minmax(...)` — all already streaming.

---

## Architecture quick tour

```
resurrector/
  ingest/          Scanner, parser, indexer, health checks, density, format conversion, schema migrations
  core/            BagFrame, sync, transforms, export, datasets, streaming aggregators, exceptions
  dashboard/       FastAPI backend + React/Vite frontend (built bundle in dashboard/static/, source in dashboard/app/)
  bridge/          PlotJuggler-compatible WebSocket bridge; static viewer in bridge/web/
  cli/             Typer CLI with Rich formatting (entry point: resurrector)
  demo/            Synthetic bag generator (sample_bag.py) — ships in the wheel
```

### Entry points worth knowing

| File | What lives there |
|---|---|
| [resurrector/core/bag_frame.py](resurrector/core/bag_frame.py) | `BagFrame`, `TopicView`, `IpcCache`, `LARGE_TOPIC_THRESHOLD`. The pandas-like front door. |
| [resurrector/core/sync.py](resurrector/core/sync.py) | Streaming + eager sync engines. Engine="auto" picks based on threshold. |
| [resurrector/core/streaming.py](resurrector/core/streaming.py) | `stream_bucketed_minmax` — the bounded-memory plot aggregator. |
| [resurrector/core/exceptions.py](resurrector/core/exceptions.py) | `ResurrectorError` base + `LargeTopicError` + 3 sync exceptions. Every contract violation surfaces here. |
| [resurrector/core/export.py](resurrector/core/export.py) | Streaming exporters per format. NumPy hard-cap pre-flight lives in `Exporter.export()`. |
| [resurrector/ingest/scanner.py](resurrector/ingest/scanner.py) | File + ROS 2 directory bag discovery; `_fingerprint_fast` (NOT a real SHA), `_compute_sha256_full` (real). |
| [resurrector/ingest/indexer.py](resurrector/ingest/indexer.py) | DuckDB index. Calls `apply_pending(conn)` to run schema migrations on every connect. |
| [resurrector/ingest/migrations.py](resurrector/ingest/migrations.py) | Forward-only migration list. **Never reorder, never edit past entries — append only.** |
| [resurrector/ingest/health_check.py](resurrector/ingest/health_check.py) | Streaming `TopicHealthState` + Welford. Legacy bulk `run_all_checks` API kept for backward compat. |
| [resurrector/ingest/density.py](resurrector/ingest/density.py) | Single-pass per-topic bin counters. |
| [resurrector/dashboard/api.py](resurrector/dashboard/api.py) | FastAPI app. ~1500 lines — splitting it is a v0.5+ task. |
| [resurrector/cli/main.py](resurrector/cli/main.py) | Typer app. Top-level `--version` callback lives here. |
| [resurrector/cli/doctor.py](resurrector/cli/doctor.py) | Core/optional check tiers. Don't add core checks lightly — false positives erode trust. |

---

## The streaming sync engine — explicit-contract API

The v0.4.0 `bf.sync(...)` API exposes orthogonal policies the eager engine doesn't need (because eager has perfect global state):

```python
bf.sync(
    topics=["/joint_states", "/imu/data"],
    method="nearest",                 # "nearest" | "interpolate" | "sample_and_hold"
    tolerance_ms=50,
    anchor="/joint_states",           # default: highest-frequency topic
    engine="auto",                    # "eager" | "streaming" | "auto" (default)
    out_of_order="error",             # "error" | "warn_drop" | "reorder"
    boundary="null",                  # "null" | "drop" | "hold" | "error"  (interpolate only)
    max_buffer_messages=100_000,
    max_lateness_ms=0.0,              # only meaningful when out_of_order="reorder"
)
```

**Engine selection rules:**
- `auto` → eager when every topic has < 1M messages, streaming otherwise.
- `eager` → load every topic via `to_polars(force=True)`, match via `np.searchsorted`. Globally correct, O(N) memory per topic.
- `streaming` → per-topic lookahead-window buffers. Memory bound: `O(rate × 2 × tolerance)`.

**Tie-break** (for `nearest` when two non-anchor samples are equidistant from the anchor): prefer the **later** sample. This matches eager's `np.searchsorted` upper-bound rule. Don't flip this without updating the equivalence tests.

**Equivalence tests live at** [tests/test_sync_streaming.py](tests/test_sync_streaming.py). 9 fixture bags in [tests/fixtures/sync_fixtures.py](tests/fixtures/sync_fixtures.py) cover every documented edge case (fast-vs-slow, tie-at-anchor, missing-before-first, missing-after-last, out-of-order-within-topic, bursty-fast, sparse-no-match, duplicate-timestamps, topic-stops-halfway). If you change sync behavior, run these.

**MCAP gotcha for tests:** the MCAP reader returns time-sorted messages even when you write out-of-order to the file. The `out_of_order` policy is therefore tested at the `_row_iter` helper level (using a `FakeView` with chunks containing regressing timestamps), not via a fixture bag.

---

## SHA fingerprint vs full hash

`_compute_sha256` in v0.3.x was a misnomer — it only hashed the first 1 MB plus the file size. v0.4.0:

- `_fingerprint_fast(path)` — same algorithm, honest name. Used by default for change detection (good enough since bag files are typically write-once).
- `_compute_sha256_full(path)` — real full-file SHA256. Computed only when the user passes `resurrector scan --full-hash`.
- `bags.fingerprint` (DuckDB column) — fast fingerprint, always populated.
- `bags.sha256_full` (DuckDB column, nullable) — populated only when `--full-hash` was used.

The rename was migration 1 in `resurrector/ingest/migrations.py`. Existing v0.3.x indexes are upgraded transparently on first open with v0.4.0.

**Don't call the fingerprint a SHA in user-facing strings or column names.** That's the credibility issue the rename was built to fix.

---

## Schema migrations — append only

`resurrector/ingest/migrations.py` is a forward-only list. Rules:

- Append new migrations to the end. Never reorder.
- Never edit a past migration. If migration 3 was wrong, fix it with migration 4.
- Bump the file's `SCHEMA_VERSION` constant (it's `max(m.version for m in MIGRATIONS)` automatically — no manual bump needed).
- Each migration's `sql` is one or more semicolon-separated statements, each runs in its own implicit transaction.
- A test in [tests/test_migrations.py](tests/test_migrations.py) verifies a v0.3.x-format DB upgrades cleanly. **If you add a migration, extend this test.**

---

## Conventions

### Docstrings & comments

This codebase keeps comments minimal and explanatory, not narrative. Conventions:

- Module docstrings explain *why* the module exists and the contract it upholds — see `streaming.py` and `sync.py` for the shape.
- Function/class docstrings describe args, return values, and any non-obvious invariants. ASCII art is fine when it clarifies dataflow (see `density.py`).
- Inline comments explain WHY, not WHAT. If a comment restates what the code obviously does, delete it.
- Performance-sensitive code that looks weird gets a comment explaining the constraint (e.g. "Avoids np.clip per-message overhead" in density.py).

### Tests

- `tests/test_*.py` for everything. Pytest config in `pyproject.toml`.
- Markers: `smoke` (the wheel-install CI flow, runs against installed package only), `slow` (memory regression). Both registered in `pyproject.toml`.
- The `slow` tier is excluded by default via `addopts = "-m 'not slow'"`. Run with `pytest -m slow` or `pytest -m ''` for everything.
- Equivalence tests for streaming-vs-eager paths: when you write a streaming version of a function that has an eager equivalent, write a test that runs both on real fixtures and asserts the outputs match (modulo float drift). See `test_health_streaming.py` and `test_sync_streaming.py` for the pattern.
- Fixtures in `tests/fixtures/`. The synthetic bag generator is `resurrector.demo.sample_bag` (it ships in the wheel — `tests/fixtures/generate_test_bags.py` is a back-compat shim). Don't import from `tests.fixtures` in non-test code.

### Errors

- Custom exceptions in `resurrector/core/exceptions.py`, all inheriting from `ResurrectorError`.
- Every contract violation should raise a typed exception with a message that names the bound and points at the alternative. Pattern:

  ```python
  raise LargeTopicError(
      topic_name=...,
      message_count=...,
      threshold=...,
  )
  # The exception class formats the user-facing message itself —
  # don't re-format at the raise site.
  ```

### Dashboard frontend

- React + Vite + Plotly. Source in [resurrector/dashboard/app/](resurrector/dashboard/app/), built bundle in [resurrector/dashboard/static/](resurrector/dashboard/static/) (gitignored — built fresh by CI / packaging).
- Mostly inline-styled. Cleanup is a v0.5+ task — don't introduce a design system in unrelated PRs.
- The dashboard frontend is included in the wheel via `[tool.setuptools.package-data]` in `pyproject.toml`. CI's `frontend-build` job catches build regressions.

---

## CI

[.github/workflows/ci.yml](.github/workflows/ci.yml) runs on every push and PR with five parallel jobs:

- `test` — pytest matrix on Python 3.10/3.11/3.12/3.13 (Ubuntu)
- `wheel-smoke` — build wheel, install in fresh venv, `resurrector --version` + `pytest -m smoke`
- `frontend-build` — `npm ci && npm run build` in the dashboard app
- `lint` — `python -m compileall` syntax-error catcher (ruff/mypy is a v0.5+ project)
- `memory-regression` — `pytest -m slow` against the synthetic-bag fixture

The `wheel-smoke` job is the gate for packaging bugs. The v0.3.2 demo-import break (which slipped to PyPI) would have been caught here. Don't disable this job to skip a flake — fix the root cause.

[.github/workflows/build-packages.yml](.github/workflows/build-packages.yml) builds DMG (macOS) + DEB (Ubuntu) on tag push. [.github/workflows/publish-pypi.yml](.github/workflows/publish-pypi.yml) publishes to PyPI via Trusted Publishing.

---

## Things that are intentionally NOT done (don't add them in unrelated PRs)

These are deferred to v0.5+ per the v0.4.0 plan. They're real, they're known, and they're out-of-scope for now:

- **Async job system** for long scans/exports/indexing. Currently request-bound. Will fail on truly massive bags.
- **Dashboard `api.py` split** — it's 1500 lines. Cosmetic next to streaming.
- **Real-world bag corpus + benchmark suite** — v0.4.0 ships memory regression on synthetic bags. v0.5+ adds public-dataset fixtures.
- **Frontend design system / virtualization** — inline styles, no virtualization for thousands of bags.
- **Ruff / mypy** — too noisy on existing code; revisit in v0.5.
- **Stream extensibility** (multi-bag sync playback, recording during live streaming, time-anchored event markers in WS protocol) — captured in `FUTURE_FEATURES.md` (gitignored).
- **3D scene panel** — Three.js + URDF + TF + point clouds. ~2-3 weeks of work; in `FUTURE_FEATURES.md`.

---

## Local-only files (gitignored, don't commit)

- `PLAN.md`, `TODOS.md`, `LAUNCH_BLOCKERS.md`, `FUTURE_FEATURES.md` — working docs
- `marketing/` — launch posts and threads (HN/Reddit/Twitter drafts)
- `docs/usability_review.md` — internal review notes
- `_exploration_output/`, `test_log.txt`, `trimmed.mcap`, `.claude/` — scratch

---

## Quick smell tests for "is this PR honest?"

1. Did you add a hot path that materializes a full topic? Either add a `LargeTopicError` guard or refactor to streaming.
2. Did you add a numpy/list accumulator over per-message data? Probably needs to become a running aggregator.
3. Are you claiming "OOM-safe" anywhere new? Add a `tests/test_streaming_oom.py` case to back it up.
4. Did you change a public API (`bf.sync`, `bf.health_report`, etc.)? Update CHANGELOG and check that the equivalence tests still pass.
5. Did you add a DB column? Add a migration. Don't ALTER inline in `_init_schema`.
6. Did you add a comparison claim in README? Make sure the competitor actually doesn't do that thing — Foxglove and PlotJuggler are excellent and the comparison table got tightened in v0.4.0 specifically because previous claims overreached.

If any of these answer "yes" without the stated remediation, fix before landing.
