#!/bin/bash
# Create an Ubuntu .deb package from the PyInstaller output.
#
# Prerequisites:
#   sudo apt-get install ruby-dev build-essential
#   sudo gem install fpm
#
# Usage:
#   bash packaging/ubuntu/build_deb.sh [version]

set -euo pipefail

VERSION="${1:-0.2.0}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
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
    --license "Proprietary" \
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
