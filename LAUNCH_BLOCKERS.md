# Launch Blockers — Pre-Launch Work List

> Generated 2026-04-18 from `plan-eng-review` tech debt audit on PLAN.md.
> Total estimate: 3-5 days of focused work.

All items must land before HN / Reddit / Twitter launch.

---

## Status

- [x] **#3 — Full streaming for all export formats + LazyTopicView** — DONE (177 tests pass)
- [x] **#2 — DRY'd export error collection + ExportError** — DONE as part of #3
- [x] **#1 — CDR parser bounds checks + typed errors + tests** — DONE (12 new tests)
- [ ] **#4 — RLDS + LeRobot export formats** (1-2 days)
- [x] **#5 — DuckDB thread safety** — DONE (4 new concurrency tests)
- [x] **#6 — Dashboard path validation default** — DONE (default = ~/, opt-in to broaden)
- [x] **#7 — Hardening PR: SQL f-strings, WebSocket error swallow, ring buffer sizing** — DONE (2 new tests, ws send-loop logs+closes, lag warning)

Recommended order: #3 first (largest, touches the core), then #1/#2/#5/#6/#7 in parallel, #4 last (builds on the streaming refactor).

---

## #3 — Full streaming for all export formats + LazyTopicView

**Why:** PLAN claims "lazy by default" and "OOM-safe streaming" — but only Parquet export streams today. `TopicView.to_polars()` and HDF5/CSV/NumPy/Zarr exports load full topics into memory. A user calling `bf["/camera/rgb"].to_polars()` on a 10GB camera topic OOMs.

**Files:**
- `resurrector/core/bag_frame.py:104-124` — `TopicView.to_polars()` loads all messages as list of dicts
- `resurrector/core/export.py:79` — only Parquet streams; HDF5/CSV/NumPy/Zarr call full `to_polars()` first

**What to build:**
1. `TopicView.to_lazy_polars()` — returns `pl.LazyFrame`, backed by batch iterator over MCAP messages
2. Chunked writers for HDF5 (append mode), CSV (line-streaming), NumPy (use `np.memmap` or document limitation), Zarr (already chunked, just wire it)
3. Keep eager `to_polars()` working but add size guard — log warning if loading >100k messages eagerly

**Tests:**
- Synthetic bag with 200k+ messages on one topic, assert peak memory stays under 500MB during each export format
- `to_lazy_polars()` correctness: collect a lazy frame and compare to eager for small topic
- Filter pushdown works through lazy

**Done when:**
- All 5 export formats stream chunk-by-chunk
- `to_lazy_polars()` exists and is documented
- README + PLAN.md "OOM-safe" claim is actually true across the board

---

## #1 — CDR parser bounds checks + typed errors + tests

**Why:** `resurrector/ingest/parser.py:224-343` has no buffer bounds checks before `struct.unpack_from()` calls. A corrupt MCAP message with inflated field counts (e.g., `n_names=1000000` when buffer is 1KB) either crashes with `struct.error` or silently truncates data via `errors="replace"` in decode.

**Fix:**
1. Add `_safe_unpack(fmt, buf, offset)` helper that validates `offset + struct.calcsize(fmt) <= len(buf)` before calling `struct.unpack_from`
2. Raise `CDRParseError(topic, offset, expected_size, actual_size)` on underflow
3. Replace all direct `struct.unpack_from` calls with the helper
4. In string reads: check `offset + str_len <= len(buf)` before slice

**Tests:**
- Truncated string-length header
- `n_names` > buffer size in `_parse_joint_state`
- `n_ranges` > buffer size in `_parse_laser_scan`
- Empty buffer

---

## #2 — DRY'd export error collection + ExportError

**Why:** Same `except Exception: pass` pattern repeated 3x in `resurrector/core/export.py:179, 193, 213` (HDF5, NumPy, Zarr). Silently drops columns that fail serialization.

**Fix:**
1. Add `_serialize_column(name, values, writer)` helper used by all 3 exporters
2. Collect failed columns into a list; at end of export, raise `ExportError(failed_columns=[...], partial_output_path=...)`
3. Alternative: `ExportResult` dataclass with `warnings: list[str]` so CLI can surface warnings without aborting

**Tests:**
- Mock a column that raises during serialization; assert ExportError raised with column name
- Verify successful columns still landed on disk

---

## #4 — RLDS + LeRobot export formats

**Why:** Marketing (Twitter thread, launch_plan.md) claims both formats exist. They don't. Closing the truthfulness gap AND adding a real ML-pipeline wedge.

**What to build:**
- **LeRobot format:** Parquet shards + `meta/info.json` + `meta/episodes.jsonl` + `meta/tasks.jsonl` per the LeRobot dataset schema. Each bag → one episode. Joint states / images / actions mapped to LeRobot feature dict.
- **RLDS format:** TFRecord with RLDS step structure (observation, action, reward, discount, is_first/is_last/is_terminal). Use `tensorflow-datasets` (already in `all-exports` optional dep group).

**Registration:**
- Add `"lerobot"` and `"rlds"` to the format enum in `export.py`
- CLI: `resurrector export bag.mcap --format lerobot --output ./dataset`

**Tests:**
- Round-trip: export small bag → load with `lerobot.common.datasets.lerobot_dataset.LeRobotDataset` (at least validate metadata JSON structure)
- RLDS: export → `tfds.builder_from_directory(path).as_dataset()` returns steps

**Builds on #3:** Needs streaming so large bags don't OOM during RLDS TFRecord write.

---

## #5 — DuckDB thread safety

**Why:** `resurrector/ingest/indexer.py` shares a single DuckDB connection across threads. Bridge (threaded) + dashboard (async) + CLI scan (threaded) all touch the index concurrently. Undefined behavior, possible index corruption.

**Fix options (pick one during implementation):**
- **Option A:** Wrap all writes in `threading.Lock()`. Simpler, but serializes write throughput.
- **Option B:** Connection-per-thread using `threading.local()`. DuckDB supports multiple connections to the same DB file. Higher concurrency.

Recommendation: **Option B** for writes AND reads. DuckDB's own docs recommend one connection per thread.

**Tests:**
- Spawn 8 threads, each scanning different bags concurrently; assert index is consistent afterward
- Thread + async: run scan thread while dashboard queries in parallel

---

## #6 — Dashboard path validation default

**Why:** `resurrector/dashboard/api.py:43-57` checks `RESURRECTOR_ALLOWED_ROOTS` env var — if unset, **ANY path is allowed**. Dashboard ships in DMG/DEB packages; users won't set the env var. Path traversal disclosure at launch would be embarrassing.

**Fix:**
- If env var unset, default `ALLOWED_ROOTS = [Path.home()]`
- Document the env var in README for users who want broader access
- Continue rejecting `".."` in path parts (already done)

**Tests:**
- Unset env, attempt to access `/etc/passwd` → 403
- Set env to a specific dir, attempt outside → 403
- Set env to specific dir, attempt inside → 200

---

## #7 — Pre-launch hardening bundle

Four small fixes, one PR:

### 7a — SQL f-strings in indexer
**Where:** `resurrector/ingest/indexer.py:254, 323` and `resurrector/dashboard/api.py:254-259`
**Fix:** Use parameterized where-clause composition. Not exploitable today (where is built from parameterized conditions) but fragile. ~30 min.

### 7b — WebSocket send loop silent failure
**Where:** `resurrector/bridge/server.py:181`
**Current:** `except Exception: return` — no log, no cleanup.
**Fix:** `except Exception as e: logger.warning("ws send failed", extra={...}); await ws.close(code=1011)`. ~20 min.

### 7c — Ring buffer sizing warning
**Where:** `resurrector/bridge/buffer.py`
**Issue:** Default capacity + high message rate → buffer wraps every ~20s; slow clients lose messages silently.
**Fix:** Expose capacity in bridge CLI flags; log warning when a consumer falls behind by > 50% of buffer. ~1 hr.

### 7d — CompressedImage decode logs vs raises
**Where:** `resurrector/ingest/parser.py:446`
**Current:** Bare except returns None. Caller can't tell "no data" from "decode failed".
**Fix:** Return `DecodeResult(frame=..., error=...)` or raise `ImageDecodeError`. Pick one, use consistently. ~1 hr.

---

## Post-launch (not blockers)

Items from PLAN.md section 6 that can wait:
- ROS 2 `.db3` parser
- ROS 1 `.bag` parser
- Dataset split generators (train/val/test)
- Structured eval harness for health thresholds
- Distributed indexing
- Auth / multi-tenant dashboard
