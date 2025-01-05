#!/usr/bin/env python3

import os
import time
import pytest
import tempfile
from pathlib import Path
from fs_indexer_elasticsearch.scanner.scanner import FileScanner

def create_test_directory(base_dir: str, depth: int, files_per_dir: int, size_mb: float):
    """Create a test directory structure with specified characteristics.
    
    Args:
        base_dir: Base directory to create structure in
        depth: Maximum directory depth
        files_per_dir: Number of files to create in each directory
        size_mb: Size of each file in megabytes
    """
    def _create_level(current_dir: str, current_depth: int):
        # Create files in current directory
        for i in range(files_per_dir):
            file_path = os.path.join(current_dir, f'file_{i}.txt')
            with open(file_path, 'wb') as f:
                f.write(os.urandom(int(size_mb * 1024 * 1024)))
                
        # Create subdirectories if not at max depth
        if current_depth < depth:
            for i in range(2):  # Create 2 subdirs at each level
                subdir = os.path.join(current_dir, f'dir_{i}')
                os.makedirs(subdir, exist_ok=True)
                _create_level(subdir, current_depth + 1)
                
    _create_level(base_dir, 0)

@pytest.fixture(scope='module')
def large_test_directory():
    """Create a large test directory structure."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a moderate-sized test structure:
        # - Depth 3 (root + 2 levels)
        # - 5 files per directory
        # - 0.1MB per file
        create_test_directory(tmpdir, depth=3, files_per_dir=5, size_mb=0.1)
        yield tmpdir

@pytest.fixture
def performance_config():
    return {
        'performance': {
            'parallel_processing': {
                'enabled': True,
                'max_workers': 4,
                'batch_size': 1000
            },
            'scan_chunk_size': 1000
        },
        'skip_patterns': {
            'hidden_files': True,
            'hidden_dirs': True,
            'patterns': []
        }
    }

def measure_scan_performance(scanner: FileScanner, directory: str):
    """Measure scanning performance metrics.
    
    Returns:
        Dict with performance metrics
    """
    start_time = time.time()
    results = list(scanner.scan(directory))
    end_time = time.time()
    
    total_size = sum(r['size_bytes'] for r in results)
    duration = end_time - start_time
    
    return {
        'duration_seconds': duration,
        'file_count': len(results),
        'total_size_bytes': total_size,
        'files_per_second': len(results) / duration,
        'mb_per_second': (total_size / 1024 / 1024) / duration
    }

def test_parallel_vs_sequential(performance_config, large_test_directory):
    """Compare performance of parallel vs sequential scanning."""
    # Test sequential scanning
    sequential_config = performance_config.copy()
    sequential_config['performance']['parallel_processing']['enabled'] = False
    sequential_scanner = FileScanner(sequential_config)
    sequential_metrics = measure_scan_performance(sequential_scanner, large_test_directory)
    
    # Test parallel scanning
    parallel_scanner = FileScanner(performance_config)
    parallel_metrics = measure_scan_performance(parallel_scanner, large_test_directory)
    
    # Log performance comparison
    print("\nPerformance Comparison:")
    print(f"Sequential: {sequential_metrics['files_per_second']:.2f} files/sec, "
          f"{sequential_metrics['mb_per_second']:.2f} MB/sec")
    print(f"Parallel: {parallel_metrics['files_per_second']:.2f} files/sec, "
          f"{parallel_metrics['mb_per_second']:.2f} MB/sec")
    
    # Parallel should be faster (allowing some overhead for very small datasets)
    if sequential_metrics['duration_seconds'] > 1.0:  # Only compare if test is non-trivial
        assert parallel_metrics['files_per_second'] > sequential_metrics['files_per_second']

def test_worker_count_scaling(performance_config, large_test_directory):
    """Test how performance scales with different worker counts."""
    results = []
    
    # Test with different worker counts
    for workers in [1, 2, 4]:
        config = performance_config.copy()
        config['performance']['parallel_processing']['max_workers'] = workers
        scanner = FileScanner(config)
        metrics = measure_scan_performance(scanner, large_test_directory)
        results.append((workers, metrics))
        
    # Log scaling results
    print("\nWorker Count Scaling:")
    for workers, metrics in results:
        print(f"{workers} workers: {metrics['files_per_second']:.2f} files/sec, "
              f"{metrics['mb_per_second']:.2f} MB/sec")
        
    # More workers should generally be faster (though might plateau)
    # Compare 1 worker vs max workers
    min_workers_fps = results[0][1]['files_per_second']
    max_workers_fps = results[-1][1]['files_per_second']
    if results[0][1]['duration_seconds'] > 1.0:  # Only compare if test is non-trivial
        assert max_workers_fps > min_workers_fps

def test_batch_size_impact(performance_config, large_test_directory):
    """Test impact of different batch sizes on performance."""
    results = []
    
    # Test with different batch sizes
    for batch_size in [100, 1000, 10000]:
        config = performance_config.copy()
        config['performance']['parallel_processing']['batch_size'] = batch_size
        scanner = FileScanner(config)
        metrics = measure_scan_performance(scanner, large_test_directory)
        results.append((batch_size, metrics))
        
    # Log batch size impact
    print("\nBatch Size Impact:")
    for batch_size, metrics in results:
        print(f"Batch size {batch_size}: {metrics['files_per_second']:.2f} files/sec, "
              f"{metrics['mb_per_second']:.2f} MB/sec")
