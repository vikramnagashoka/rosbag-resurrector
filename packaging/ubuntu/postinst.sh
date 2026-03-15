#!/bin/bash
# Post-install script for the .deb package.
# Creates the resurrector data directory.

set -e

# Create default index directory
RESURRECTOR_DIR="${HOME}/.resurrector"
if [ ! -d "$RESURRECTOR_DIR" ]; then
    mkdir -p "$RESURRECTOR_DIR"
    echo "Created $RESURRECTOR_DIR"
fi

echo "RosBag Resurrector installed successfully."
echo "Run 'resurrector --help' to get started."
