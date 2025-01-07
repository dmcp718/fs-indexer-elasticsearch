#!/usr/bin/env python3

import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

def run_command(cmd):
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

def build_app():
    # Determine the operating system
    system = platform.system().lower()
    if system not in ('darwin', 'linux'):
        print(f"Unsupported operating system: {system}")
        sys.exit(1)

    # Setup paths
    root_dir = Path(__file__).parent.absolute()
    dist_dir = root_dir / 'dist'
    build_dir = root_dir / 'build'
    config_dir = root_dir / 'config'
    docker_compose_dir = root_dir / 'fs-indexer-elasticsearch-docker-compose'
    pyinstaller_path = root_dir / 'venv' / 'bin' / 'pyinstaller'

    # Clean previous builds
    for dir_path in (dist_dir, build_dir):
        if dir_path.exists():
            shutil.rmtree(dir_path)

    # Create spec file content
    spec_content = f'''
# -*- mode: python ; coding: utf-8 -*-

a = Analysis(
    ['fs_indexer_elasticsearch/main.py'],
    pathex=['{root_dir}'],
    binaries=[],
    datas=[
        ('config/*', 'config'),
        ('fs-indexer-elasticsearch-docker-compose/*', 'docker-compose'),
    ],
    hiddenimports=[
        'elasticsearch',
        'elasticsearch.client',
        'elasticsearch.connection',
        'elasticsearch.serializer',
        'elasticsearch.transport',
        'duckdb',
        'xxhash',
        'yaml',
        'dateutil',
        'pytz',
        'aiohttp',
        'requests',
        'pyarrow',
        'fs_indexer_elasticsearch.elasticsearch',
        'fs_indexer_elasticsearch.lucidlink',
        'fs_indexer_elasticsearch.scanner',
    ],
    hookspath=[],
    hooksconfig={{}},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='fs-indexer-elasticsearch',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='fs-indexer-elasticsearch',
)
'''

    # Write spec file
    spec_file = root_dir / 'fs-indexer-elasticsearch.spec'
    spec_file.write_text(spec_content)

    # Build the application
    cmd = [
        str(pyinstaller_path),
        '--clean',
        str(spec_file),
    ]
    run_command(cmd)

    # Copy docker-compose files to a more accessible location in dist
    docker_dist_dir = dist_dir / 'fs-indexer-elasticsearch' / 'docker-compose'
    if not docker_dist_dir.exists():
        docker_dist_dir.mkdir(parents=True)
    
    for file in docker_compose_dir.glob('*'):
        shutil.copy2(file, docker_dist_dir)

    print("\nBuild complete! Executable can be found in the 'dist' directory.")
    print("Docker Compose files are available in 'dist/fs-indexer-elasticsearch/docker-compose/'")

if __name__ == '__main__':
    build_app()
