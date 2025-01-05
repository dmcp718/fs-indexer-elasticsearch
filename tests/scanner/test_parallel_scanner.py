#!/usr/bin/env python3

import os
import pytest
import tempfile
from datetime import datetime
from pathlib import Path
from fs_indexer_elasticsearch.scanner.parallel_scanner import ParallelFindScanner
from fs_indexer_elasticsearch.scanner.batch_processor import BatchProcessor

@pytest.fixture
def test_config():
    return {
        'performance': {
            'parallel_processing': {
                'enabled': True,
                'max_workers': 2,
                'batch_size': 100
            }
        },
        'skip_patterns': {
            'hidden_files': True,
            'hidden_dirs': True,
            'patterns': [
                '*.tmp',
                'test_skip'
            ]
        }
    }

@pytest.fixture
def test_directory():
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test directory structure
        dirs = ['dir1', 'dir2', '.hidden_dir', 'test_skip']
        files = ['file1.txt', 'file2.txt', '.hidden_file', 'test.tmp']
        
        for d in dirs:
            os.makedirs(os.path.join(tmpdir, d), exist_ok=True)
            
        for f in files:
            for d in [''] + dirs:  # Create in root and all subdirs
                file_path = os.path.join(tmpdir, d, f)
                if d:  # Skip if directory should be skipped
                    Path(file_path).touch()
                    
        yield tmpdir

def test_batch_processor_skip_patterns(test_config, test_directory):
    """Test that BatchProcessor correctly applies skip patterns."""
    processor = BatchProcessor(test_config)
    results = list(processor.process_directory(test_directory))
    
    # Check that hidden and skip pattern files are excluded
    paths = [r['filepath'] for r in results]
    assert not any(p.endswith('.hidden_file') for p in paths)
    assert not any(p.endswith('.tmp') for p in paths)
    assert not any('test_skip' in p for p in paths)
    
    # Check that normal files are included
    assert any(p.endswith('file1.txt') for p in paths)
    assert any(p.endswith('file2.txt') for p in paths)

def test_batch_processor_file_info(test_config, test_directory):
    """Test that BatchProcessor returns correct file information."""
    processor = BatchProcessor(test_config)
    results = list(processor.process_directory(test_directory))
    
    for entry in results:
        assert set(entry.keys()) == {
            'name', 'relative_path', 'filepath', 'size_bytes', 
            'modified_time', 'creation_time', 'type', 'extension',
            'checksum', 'direct_link', 'last_seen'
        }
        assert isinstance(entry['modified_time'], datetime)
        assert isinstance(entry['size_bytes'], int)
        assert entry['type'] in ('file', 'directory')

def test_parallel_scanner_directory_split(test_config, test_directory):
    """Test that ParallelFindScanner correctly splits directories."""
    scanner = ParallelFindScanner(test_config)
    dirs = scanner.split_directories(test_directory)
    
    # Should find dir1 and dir2, but not hidden or skip dirs
    assert len(dirs) == 2
    assert all(os.path.basename(d) in ['dir1', 'dir2'] for d in dirs)
    assert not any('.hidden_dir' in d for d in dirs)
    assert not any('test_skip' in d for d in dirs)

def test_parallel_scanner_batch_size(test_config, test_directory):
    """Test that ParallelFindScanner respects batch size limits."""
    # Set very small batch size
    test_config['performance']['parallel_processing']['batch_size'] = 2
    
    scanner = ParallelFindScanner(test_config)
    results = list(scanner.scan(test_directory))
    
    # Should only get 2 results per directory (4 total from dir1 and dir2)
    assert len(results) <= 4

def test_parallel_scanner_workers(test_config, test_directory):
    """Test that ParallelFindScanner uses correct number of workers."""
    scanner = ParallelFindScanner(test_config)
    
    # This is a basic test - in reality we'd want to use mock or spy
    # to verify the actual number of processes created
    results = list(scanner.scan(test_directory))
    assert len(results) > 0  # Just verify it works with specified workers

def test_parallel_scanner_error_handling(test_config, test_directory):
    """Test that ParallelFindScanner handles errors gracefully."""
    scanner = ParallelFindScanner(test_config)
    
    # Make a directory unreadable
    os.chmod(os.path.join(test_directory, 'dir1'), 0o000)
    try:
        results = list(scanner.scan(test_directory))
        # Should still get results from dir2
        assert len(results) > 0
        assert all('dir1' not in r['filepath'] for r in results)
    finally:
        # Restore permissions for cleanup
        os.chmod(os.path.join(test_directory, 'dir1'), 0o755)
