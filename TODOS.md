# TODOs

Deferred work captured from plan reviews. Each item has context enough to pick up cold in 3 months.

---

## Deferred from v0.3.0 plan review

### TODO-3: Remote bridge URL config

- **What:** Allow the dashboard to connect to a bridge running on a different machine instead of always spawning a local subprocess.
- **Why:** Teams running the dashboard on a lab server while the bridge runs on a robot can't currently use the Bridge page without SSH tunneling.
- **Pros:** Unlocks distributed/multi-machine deployments.
- **Cons:** Adds a configuration surface (env var + UI input). Edge-case audience today.
- **Context:** v0.3.0 added `POST /api/bridge/start` that spawns a subprocess and proxies calls through `/api/bridge/proxy/*`. The dashboard assumes bridge is always localhost:9090. To support remote: check for `RESURRECTOR_BRIDGE_URL` env var OR a user-set value in dashboard settings; if set, skip subprocess spawn and proxy directly to that URL.
- **Depends on / blocked by:** v0.3.0 bridge infrastructure must ship first.

---

### TODO-5: Dataset export progress streaming

- **What:** SSE-based progress updates during `POST /api/datasets/{name}/versions/{v}/export`.
- **Why:** Multi-bag exports can take minutes. Currently the dashboard shows an indefinite spinner with no feedback.
- **Pros:** Users see per-bag progress, can cancel, trust the tool isn't frozen.
- **Cons:** Requires SSE generator pattern; blocking version is simpler and works fine for small datasets.
- **Context:** The existing `_scan_stream()` in `resurrector/dashboard/api.py:348` is the template — accept `?stream=true`, yield `data: {...}` events as each bag exports. `DatasetManager.export_version()` already iterates bag-by-bag, so it's a matter of switching to a generator and emitting events.
- **Depends on / blocked by:** v0.3.0 dataset CRUD API must ship first.
