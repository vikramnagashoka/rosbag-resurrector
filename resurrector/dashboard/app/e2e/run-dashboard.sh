#!/usr/bin/env bash
# Boot a hermetic dashboard for Playwright. Creates a temp data dir
# with a synthetic bag (the scene demo) pre-indexed, then runs the
# dashboard on port 8765 — separate from the dev instance on 8080.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../../../.." && pwd)"

# Find the resurrector entrypoint. Priority:
# 1. CI / user-provided override via RESURRECTOR_BIN
# 2. PATH (set by CI's `pip install -e .` step)
# 3. Local dev venv at /tmp/v060-build (matches CLAUDE.md convention)
if [ -n "${RESURRECTOR_BIN:-}" ] && [ -x "${RESURRECTOR_BIN}" ]; then
  RESURRECTOR_CMD="${RESURRECTOR_BIN}"
  PYTHON_CMD="$(dirname "${RESURRECTOR_BIN}")/python"
elif command -v resurrector >/dev/null 2>&1; then
  # Use the python sitting next to the resurrector binary so we
  # inherit its environment (matters in venvs / Docker layers where
  # `python` on PATH is system, not the one with our deps).
  RESURRECTOR_CMD="$(command -v resurrector)"
  PYTHON_CMD="$(dirname "${RESURRECTOR_CMD}")/python"
  [ -x "${PYTHON_CMD}" ] || PYTHON_CMD="$(dirname "${RESURRECTOR_CMD}")/python3"
elif [ -x "/tmp/v060-build/bin/resurrector" ]; then
  RESURRECTOR_CMD="/tmp/v060-build/bin/resurrector"
  PYTHON_CMD="/tmp/v060-build/bin/python"
else
  echo "Could not find a resurrector binary." >&2
  echo "Either install: pip install -e .   (CI) " >&2
  echo "Or create the dev venv: python3 -m venv /tmp/v060-build && /tmp/v060-build/bin/pip install -e .  (local)" >&2
  exit 1
fi

E2E_ROOT="$(mktemp -d -t resurrector-e2e-XXXXXX)"
export RESURRECTOR_ALLOWED_ROOTS="${E2E_ROOT}"
# Keep uploaded bags inside the hermetic root (cleaned up on exit, and within
# the allowed roots so post-upload path validation passes).
export RESURRECTOR_UPLOADS_DIR="${E2E_ROOT}/uploads"

# Generate the scene demo bag directly into the hermetic root. Script
# accepts an output path arg so we don't touch the user's $HOME cache.
"${PYTHON_CMD}" "${REPO_ROOT}/tests/fixtures/make_scene_demo_bag.py" \
  "${E2E_ROOT}/scene_demo.mcap"

# Pre-index so the Library page shows the bag immediately.
"${RESURRECTOR_CMD}" scan "${E2E_ROOT}" --db "${E2E_ROOT}/index.db" >/dev/null 2>&1 || true

cleanup() { rm -rf "${E2E_ROOT}"; }
trap cleanup EXIT

exec "${RESURRECTOR_CMD}" dashboard --port 8967 --db "${E2E_ROOT}/index.db"
