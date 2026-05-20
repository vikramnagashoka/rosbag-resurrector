#!/usr/bin/env bash
# Boot a hermetic dashboard for Playwright. Creates a temp data dir
# with a synthetic bag (the scene demo) pre-indexed, then runs the
# dashboard on port 8765 — separate from the dev instance on 8080.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"
VENV_BIN="/tmp/v060-build/bin"

if [ ! -x "${VENV_BIN}/resurrector" ]; then
  echo "Could not find ${VENV_BIN}/resurrector." >&2
  echo "Run: python3 -m venv /tmp/v060-build && /tmp/v060-build/bin/pip install -e ." >&2
  exit 1
fi

E2E_ROOT="$(mktemp -d -t resurrector-e2e-XXXXXX)"
export RESURRECTOR_ALLOWED_ROOTS="${E2E_ROOT}"

# Make sure the demo bag generator has been run at least once.
DEMO_BAG="${HOME}/rosbag-demo/scene_demo.mcap"
if [ ! -f "${DEMO_BAG}" ]; then
  "${VENV_BIN}/python" "${REPO_ROOT}/marketing/build-notes/make_scene_demo_bag.py"
fi
cp "${DEMO_BAG}" "${E2E_ROOT}/scene_demo.mcap"

# Pre-index so the Library page shows the bag immediately.
"${VENV_BIN}/resurrector" scan "${E2E_ROOT}" --db "${E2E_ROOT}/index.db" >/dev/null 2>&1 || true

cleanup() { rm -rf "${E2E_ROOT}"; }
trap cleanup EXIT

exec "${VENV_BIN}/resurrector" dashboard --port 8967 --db "${E2E_ROOT}/index.db"
