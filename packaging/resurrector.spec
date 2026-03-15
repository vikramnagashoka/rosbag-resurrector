# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec file for RosBag Resurrector base package.

Build with:
    pyinstaller packaging/resurrector.spec

Or via the Makefile:
    make build-base
"""

import os
import sys
from pathlib import Path

block_cipher = None

# Project root
ROOT = Path(os.path.abspath(os.path.join(SPECPATH, '..')))

# Collect data files (web viewer, dashboard static)
datas = []

bridge_web = ROOT / 'resurrector' / 'bridge' / 'web'
if bridge_web.exists():
    datas.append((str(bridge_web), 'resurrector/bridge/web'))

dashboard_static = ROOT / 'resurrector' / 'dashboard' / 'static'
if dashboard_static.exists():
    datas.append((str(dashboard_static), 'resurrector/dashboard/static'))

# Dashboard app HTML (for the bridge viewer)
dashboard_app = ROOT / 'resurrector' / 'dashboard' / 'app'
if dashboard_app.exists():
    datas.append((str(dashboard_app / 'index.html'), 'resurrector/dashboard/app'))

a = Analysis(
    [str(ROOT / 'resurrector' / 'cli' / 'main.py')],
    pathex=[str(ROOT)],
    binaries=[],
    datas=datas,
    hiddenimports=[
        # Uvicorn internals
        'uvicorn.workers',
        'uvicorn.logging',
        'uvicorn.protocols.http.auto',
        'uvicorn.protocols.http.h11_impl',
        'uvicorn.protocols.http.httptools_impl',
        'uvicorn.protocols.websockets.auto',
        'uvicorn.protocols.websockets.wsproto_impl',
        'uvicorn.protocols.websockets.websockets_impl',
        'uvicorn.lifespan.on',
        'uvicorn.lifespan.off',
        # FastAPI / Starlette
        'starlette.responses',
        'starlette.routing',
        'starlette.middleware.cors',
        # Core dependencies
        'mcap',
        'mcap.reader',
        'mcap.writer',
        'polars',
        'duckdb',
        'h5py',
        'pyarrow',
        'numpy',
        'pandas',
        'rich',
        'typer',
        'typer.main',
        # Resurrector modules
        'resurrector',
        'resurrector.cli',
        'resurrector.cli.main',
        'resurrector.cli.formatters',
        'resurrector.core',
        'resurrector.core.bag_frame',
        'resurrector.core.sync',
        'resurrector.core.transforms',
        'resurrector.core.export',
        'resurrector.core.query',
        'resurrector.core.dataset',
        'resurrector.core.dataset_readme',
        'resurrector.core.topic_groups',
        'resurrector.ingest',
        'resurrector.ingest.scanner',
        'resurrector.ingest.parser',
        'resurrector.ingest.indexer',
        'resurrector.ingest.health_check',
        'resurrector.dashboard',
        'resurrector.dashboard.api',
        'resurrector.bridge',
        'resurrector.bridge.server',
        'resurrector.bridge.playback',
        'resurrector.bridge.protocol',
        'resurrector.bridge.buffer',
        'resurrector.logging_config',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Exclude heavy optional deps from base package
        'torch',
        'torchvision',
        'sentence_transformers',
        'transformers',
        'openai',
        'cv2',
        'PIL',
        'rclpy',
        # Exclude test infrastructure
        'pytest',
        'httpx',
        '_pytest',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='resurrector',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
