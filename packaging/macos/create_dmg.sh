#!/bin/bash
# Create a macOS DMG from the PyInstaller output.
#
# Prerequisites:
#   brew install create-dmg
#
# Usage:
#   bash packaging/macos/create_dmg.sh [version]

set -euo pipefail

VERSION="${1:-0.2.0}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
DIST_DIR="$ROOT_DIR/dist/macos"
DMG_NAME="RosBag-Resurrector-v${VERSION}-macos"

echo "=== Building macOS DMG: ${DMG_NAME} ==="

# Check that the binary exists
if [ ! -f "$DIST_DIR/resurrector" ]; then
    echo "Error: $DIST_DIR/resurrector not found."
    echo "Run 'make build-base' first."
    exit 1
fi

# Create a temporary .app bundle structure
APP_DIR="$DIST_DIR/${DMG_NAME}.app"
mkdir -p "$APP_DIR/Contents/MacOS"
mkdir -p "$APP_DIR/Contents/Resources"

# Copy binary
cp "$DIST_DIR/resurrector" "$APP_DIR/Contents/MacOS/resurrector"
chmod +x "$APP_DIR/Contents/MacOS/resurrector"

# Copy Info.plist
cp "$SCRIPT_DIR/Info.plist" "$APP_DIR/Contents/Info.plist"
# Replace version placeholder
sed -i '' "s/__VERSION__/${VERSION}/g" "$APP_DIR/Contents/Info.plist" 2>/dev/null || \
    sed -i "s/__VERSION__/${VERSION}/g" "$APP_DIR/Contents/Info.plist"

# Create DMG
if command -v create-dmg &>/dev/null; then
    create-dmg \
        --volname "RosBag Resurrector" \
        --window-size 600 400 \
        --icon "${DMG_NAME}.app" 150 200 \
        --app-drop-link 450 200 \
        --no-internet-enable \
        "$DIST_DIR/${DMG_NAME}.dmg" \
        "$APP_DIR"
else
    # Fallback: use hdiutil directly
    echo "create-dmg not found, using hdiutil..."
    hdiutil create -volname "RosBag Resurrector" \
        -srcfolder "$APP_DIR" \
        -ov -format UDZO \
        "$DIST_DIR/${DMG_NAME}.dmg"
fi

echo "=== DMG created: $DIST_DIR/${DMG_NAME}.dmg ==="
echo ""
echo "To code-sign (optional, requires Apple Developer ID):"
echo "  codesign --deep --force --sign 'Developer ID Application: Your Name' $APP_DIR"
echo ""
echo "To notarize (optional):"
echo "  xcrun notarytool submit $DIST_DIR/${DMG_NAME}.dmg --apple-id YOUR_ID --team-id YOUR_TEAM"
