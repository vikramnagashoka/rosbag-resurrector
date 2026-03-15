"""Pyarmor obfuscation configuration for RosBag Resurrector.

Usage:
    python packaging/pyarmor_config.py

This script runs pyarmor to obfuscate all resurrector source modules
before PyInstaller bundles them. The obfuscated output goes to
dist/obfuscated/ which is then fed to PyInstaller.
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
SRC = ROOT / "resurrector"
OUTPUT = ROOT / "dist" / "obfuscated"

# Modules to obfuscate
PACKAGES = [
    "resurrector",
]

# Files/patterns to exclude from obfuscation
EXCLUDES = [
    "tests/",
    "*.pyc",
    "__pycache__/",
]


def obfuscate():
    """Run pyarmor to obfuscate the resurrector package."""
    OUTPUT.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, "-m", "pyarmor", "gen",
        "--output", str(OUTPUT),
        "--recursive",
        "--no-wrap",  # Don't wrap scripts — PyInstaller handles entry point
        str(SRC),
    ]

    print(f"Obfuscating {SRC} → {OUTPUT}")
    print(f"Command: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        sys.exit(1)

    print(f"Obfuscation complete: {OUTPUT}")
    print(result.stdout)


if __name__ == "__main__":
    obfuscate()
