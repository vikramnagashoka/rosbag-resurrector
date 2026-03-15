"""Build script for the resurrector-vision extras package.

This creates a separate distributable that includes:
- sentence-transformers + PyTorch
- Pillow + OpenCV
- The resurrector.core.vision module

The base resurrector binary detects this package at runtime.

Usage:
    python packaging/build_vision.py [--version 0.2.0]

Note: This builds a pip-installable wheel with obfuscated source,
NOT a standalone executable. Users install it alongside the base:

    pip install resurrector-vision-0.2.0-cp312-linux_x86_64.whl
"""

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent


def build_vision_wheel(version: str = "0.2.0"):
    """Build an obfuscated wheel for the vision extras."""
    print(f"=== Building resurrector-vision v{version} ===")

    # Step 1: Create a temporary setup for vision-only package
    vision_setup = ROOT / "dist" / "vision_pkg"
    vision_setup.mkdir(parents=True, exist_ok=True)

    # Create a minimal pyproject.toml for the vision package
    pyproject = vision_setup / "pyproject.toml"
    pyproject.write_text(f"""[build-system]
requires = ["setuptools>=68.0", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "resurrector-vision"
version = "{version}"
description = "Vision extras for RosBag Resurrector (CLIP semantic search + image export)"
requires-python = ">=3.10"
dependencies = [
    "sentence-transformers>=2.2.0",
    "Pillow>=10.0.0",
    "opencv-python-headless>=4.8.0",
]
""")

    print(f"Vision wheel config written to {pyproject}")
    print("")
    print("To build:")
    print(f"  cd {vision_setup}")
    print("  pip wheel . --no-deps --wheel-dir ../wheels/")
    print("")
    print("To install alongside base:")
    print("  pip install resurrector-vision-*.whl")
    print("")
    print("Note: For obfuscated builds, run pyarmor on resurrector/core/vision.py first,")
    print("then include the obfuscated module in the wheel.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build vision extras package")
    parser.add_argument("--version", default="0.2.0", help="Version number")
    args = parser.parse_args()
    build_vision_wheel(args.version)
