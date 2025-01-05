#!/usr/bin/env python3

import os
import time
import pytest
import tempfile
import subprocess
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

def create_large_test_directory(base_dir: str):
    """Create a larger test directory structure for meaningful performance testing."""
    # Create a much larger structure for better parallel vs sequential comparison
    create_test_directory(base_dir, depth=5, files_per_dir=20, size_mb=1.0)

@pytest.fixture(scope='module')
def large_test_directory():
    """Create a large test directory structure."""
    with tempfile.TemporaryDirectory() as tmpdir:
        create_large_test_directory(tmpdir)
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
        },
        'database': {
            'enabled': False  # Disable database for performance testing
        }
    }

def measure_scan_performance(scanner: FileScanner, directory: str):
    """Measure scanning performance metrics.
    
    Returns:
        Dict with performance metrics
    """
    start_time = time.time()
    start_cpu = time.process_time()
    
    results = list(scanner.scan(directory))
    
    end_time = time.time()
    end_cpu = time.process_time()
    
    total_size = sum(r['size_bytes'] for r in results)
    wall_duration = end_time - start_time
    cpu_duration = end_cpu - start_cpu
    
    return {
        'wall_duration_seconds': wall_duration,
        'cpu_duration_seconds': cpu_duration,
        'file_count': len(results),
        'total_size_bytes': total_size,
        'files_per_second': len(results) / wall_duration,
        'mb_per_second': (total_size / 1024 / 1024) / wall_duration,
        'cpu_utilization': (cpu_duration / wall_duration) * 100 if wall_duration > 0 else 0
    }

def test_parallel_vs_sequential(performance_config, large_test_directory):
    """Compare performance of parallel vs sequential scanning."""
    print("\nDirectory Structure:")
    subprocess.run(['tree', '-h', large_test_directory], check=False)
    
    # Test with different worker counts
    worker_counts = [1, 2, 4]
    results = []
    
    for workers in worker_counts:
        config = performance_config.copy()
        config['performance']['parallel_processing'].update({
            'enabled': True,
            'max_workers': workers
        })
        scanner = FileScanner(config)
        metrics = measure_scan_performance(scanner, large_test_directory)
        results.append((workers, metrics))
    
    # Test sequential scanning
    sequential_config = performance_config.copy()
    sequential_config['performance']['parallel_processing']['enabled'] = False
    sequential_scanner = FileScanner(sequential_config)
    sequential_metrics = measure_scan_performance(sequential_scanner, large_test_directory)
    
    # Print detailed comparison
    print("\nPerformance Comparison:")
    print(f"\nSequential Scanning:")
    print(f"  Wall Time: {sequential_metrics['wall_duration_seconds']:.2f}s")
    print(f"  CPU Time: {sequential_metrics['cpu_duration_seconds']:.2f}s")
    print(f"  CPU Utilization: {sequential_metrics['cpu_utilization']:.1f}%")
    print(f"  Files/sec: {sequential_metrics['files_per_second']:.1f}")
    print(f"  MB/sec: {sequential_metrics['mb_per_second']:.1f}")
    print(f"  Total Files: {sequential_metrics['file_count']}")
    print(f"  Total Size: {sequential_metrics['total_size_bytes']/1024/1024:.1f} MB")
    
    print("\nParallel Scanning:")
    for workers, metrics in results:
        speedup = sequential_metrics['wall_duration_seconds'] / metrics['wall_duration_seconds']
        efficiency = speedup / workers * 100
        
        print(f"\n{workers} Workers:")
        print(f"  Wall Time: {metrics['wall_duration_seconds']:.2f}s")
        print(f"  CPU Time: {metrics['cpu_duration_seconds']:.2f}s")
        print(f"  CPU Utilization: {metrics['cpu_utilization']:.1f}%")
        print(f"  Files/sec: {metrics['files_per_second']:.1f}")
        print(f"  MB/sec: {metrics['mb_per_second']:.1f}")
        print(f"  Speedup: {speedup:.2f}x")
        print(f"  Efficiency: {efficiency:.1f}%")
    
    # Verify parallel is faster with multiple workers
    fastest_parallel = min(m['wall_duration_seconds'] for _, m in results)
    assert fastest_parallel < sequential_metrics['wall_duration_seconds']

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
    if results[0][1]['wall_duration_seconds'] > 1.0:  # Only compare if test is non-trivial
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
