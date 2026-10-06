#!/bin/bash
# Create an Ubuntu .deb package from the PyInstaller output.
#
# Prerequisites:
#   sudo apt-get install ruby-dev build-essential
#   sudo gem install fpm
#
# Usage:
#   bash packaging/ubuntu/build_deb.sh [version]   # default: pyproject.toml's

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
VERSION="${1:-$(sed -n 's/^version = "\(.*\)"$/\1/p' "$ROOT_DIR/pyproject.toml" | head -n 1)}"
if [ -z "$VERSION" ]; then
    echo "Error: no version in $ROOT_DIR/pyproject.toml; pass one as the first argument."
    exit 1
fi
DIST_DIR="$ROOT_DIR/dist/ubuntu"

echo "=== Building Ubuntu DEB: rosbag-resurrector_${VERSION} ==="

# Check that the binary exists
if [ ! -f "$DIST_DIR/resurrector" ]; then
    echo "Error: $DIST_DIR/resurrector not found."
    echo "Run 'make build-base' first."
    exit 1
fi

# Ensure binary is executable
chmod +x "$DIST_DIR/resurrector"

# Create staging directory
STAGING="$DIST_DIR/deb-staging"
rm -rf "$STAGING"
mkdir -p "$STAGING/usr/local/bin"
cp "$DIST_DIR/resurrector" "$STAGING/usr/local/bin/resurrector"

# Build .deb with fpm
fpm \
    -s dir \
    -t deb \
    -n rosbag-resurrector \
    -v "$VERSION" \
    --description "RosBag Resurrector — pandas-like analysis for robotics bag files. Includes health checks, multi-stream sync, ML export, semantic search, and WebSocket bridge." \
    --url "https://github.com/vikramnagashoka/rosbag-resurrector" \
    --maintainer "RosBag Resurrector Contributors" \
    --license "MIT" \
    --architecture amd64 \
    --depends "libc6 >= 2.17" \
    --after-install "$SCRIPT_DIR/postinst.sh" \
    --category "science" \
    -p "$DIST_DIR/rosbag-resurrector_${VERSION}_amd64.deb" \
    -C "$STAGING" \
    .

echo "=== DEB created: $DIST_DIR/rosbag-resurrector_${VERSION}_amd64.deb ==="
echo ""
echo "Install with:"
echo "  sudo dpkg -i $DIST_DIR/rosbag-resurrector_${VERSION}_amd64.deb"
echo ""
echo "Test with:"
echo "  resurrector --help"
