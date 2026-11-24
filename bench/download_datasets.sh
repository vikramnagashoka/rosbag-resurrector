#!/usr/bin/env bash
# Download the benchmark datasets, pinned to fixed commits, into $ROSDATA
# (default ~/rosdata).
#
# Usage:
#   bash bench/download_datasets.sh                 # all three
#   bash bench/download_datasets.sh lidar-tactile   # just one
#
# Re-running resumes partial downloads and skips finished files.
#
# Lidar-Tactile is gated: accept its terms at
# https://huggingface.co/datasets/GeorgiaTech/Lidar-Tactile and run
# `hf auth login` first.
#
# ROSDATA must not be inside Dropbox, iCloud or another synced folder. The
# sync client would upload ~100 GB, and a file it moves to online-only gets
# downloaded again the next time a benchmark reads it.
set -uo pipefail

ROSDATA="${ROSDATA:-$HOME/rosdata}"
HF="${HF:-$HOME/.venvs/resurrector/bin/hf}"

# Dataset commits as of 2026-11-21. Bump these deliberately, and say so in
# BENCHMARKS.md, so old and new numbers are never compared silently.
VLA_REV=1610810e5f9c16ec312a395f6936a2fce1690a34
LIDAR_TACTILE_REV=81bfe665289ba48bad7e783e42b93623fc12d5bc
CONSTRUCTION_REV=4d1cf64785aaf8eec839ae167c80e1e4ed8c8b4b

case "$ROSDATA" in
  *"/Library/CloudStorage/"* | *"/Mobile Documents/"* | *"/Dropbox"*)
    echo "ROSDATA=$ROSDATA is inside a synced folder. Pick a local path." >&2
    exit 2
    ;;
esac

if [ ! -x "$HF" ]; then
  echo "hf CLI not found at $HF. Set HF=/path/to/hf or pip install huggingface_hub." >&2
  exit 2
fi

# Progress bars flood a log file; keep them only in a terminal.
[ -t 1 ] || export HF_HUB_DISABLE_PROGRESS_BARS=1

mkdir -p "$ROSDATA"
failed=()

# Raw manipulation episodes, 5 of 70 (~7 GB), covering all three tasks.
download_vla() {
  "$HF" download danielkha/VLA_pick_and_place --repo-type dataset \
    --revision "$VLA_REV" \
    --include "grasp_bottle/garmi_demo_001/*" \
    --include "grasp_bottle/garmi_demo_002/*" \
    --include "grasp_cup/garmi_demo_010/*" \
    --include "grasp_objects_to_box_and_move_box/garmi_demo_019/*" \
    --include "grasp_objects_to_box_and_move_box/garmi_demo_020/*" \
    --local-dir "$ROSDATA/vla"
}

# Humanoid recordings, all 71 bags (~19 GB) plus the authors' derived Zarr
# training data. The model checkpoints are skipped: nothing here loads them.
download_lidar_tactile() {
  "$HF" download GeorgiaTech/Lidar-Tactile --repo-type dataset \
    --revision "$LIDAR_TACTILE_REV" \
    --exclude "checkpoints/*" \
    --local-dir "$ROSDATA/lidar-tactile"
}

# Mobile robot bags, three of the four (~107 GB): 17.5, 34.0 and 55.7 GB, one
# dataset at three sizes for the memory chart. The largest is more than twice
# a 24 GB laptop's RAM, which is the point of benchmark 2.
download_construction() {
  "$HF" download manojkarnekar/construction-rosbags --repo-type dataset \
    --revision "$CONSTRUCTION_REV" \
    --include "site1/*" \
    --include "site2/site2_session01/*" \
    --local-dir "$ROSDATA/construction"
}

targets=("$@")
[ ${#targets[@]} -eq 0 ] && targets=(vla lidar-tactile construction)

for t in "${targets[@]}"; do
  echo "=== $t  $(date '+%Y-%m-%d %H:%M:%S')"
  case "$t" in
    vla) download_vla || failed+=("$t") ;;
    lidar-tactile) download_lidar_tactile || failed+=("$t") ;;
    construction) download_construction || failed+=("$t") ;;
    *) echo "unknown dataset: $t (vla, lidar-tactile, construction)" >&2; failed+=("$t") ;;
  esac
done

echo "=== done  $(date '+%Y-%m-%d %H:%M:%S')"
du -sh "$ROSDATA"/*/ 2>/dev/null
if [ ${#failed[@]} -gt 0 ]; then
  echo "FAILED: ${failed[*]}  (re-run with those names to resume)" >&2
  exit 1
fi
