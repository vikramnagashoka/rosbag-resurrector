#!/bin/bash
# Demo script for recording with asciinema.
# Usage: asciinema rec demo.cast -c "bash packaging/record_demo.sh"
# Then:  agg demo.cast demo.gif --theme monokai

set -e

# Simulate typing effect
type_cmd() {
    echo ""
    echo -n "$ "
    for ((i=0; i<${#1}; i++)); do
        echo -n "${1:$i:1}"
        sleep 0.04
    done
    echo ""
    sleep 0.3
    eval "$1"
    sleep 1.5
}

clear
echo "╔══════════════════════════════════════════════════════════╗"
echo "║        RosBag Resurrector — Quick Demo                  ║"
echo "╚══════════════════════════════════════════════════════════╝"
sleep 2

# 1. Scan and index
type_cmd "resurrector scan tests/fixtures/"

# 2. Quicklook
type_cmd "resurrector quicklook tests/fixtures/healthy.mcap"

# 3. Health check
type_cmd "resurrector health tests/fixtures/time_gap.mcap"

# 4. Export
type_cmd "resurrector export tests/fixtures/healthy.mcap --topics /imu/data --format parquet --output /tmp/demo_export"

# 5. List indexed bags
type_cmd "resurrector list"

sleep 1
echo ""
echo "✨ pip install rosbag-resurrector"
echo "📖 github.com/vikramnagashoka/rosbag-resurrector"
sleep 3
