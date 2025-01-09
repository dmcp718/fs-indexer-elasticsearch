#!/usr/bin/env python3

import logging
import time
import psutil
import os
import json
from typing import Dict, Any, List
from pathlib import Path
import yaml

logger = logging.getLogger(__name__)

class PerformanceTest:
    """Performance testing utility for fs-indexer-elasticsearch."""
    
    def __init__(self, config_path: str, root_path: str):
        """Initialize performance test with config path.
        
        Args:
            config_path: Path to config file to test
            root_path: Root path to scan
        """
        self.config_path = config_path
        self.root_path = root_path
        self.results = []
        
    def _load_config(self, overrides: Dict[str, Any] = None) -> Dict[str, Any]:
        """Load config with optional overrides.
        
        Args:
            overrides: Dictionary of config overrides
            
        Returns:
            Config dictionary
        """
        with open(self.config_path) as f:
            config = yaml.safe_load(f)
            
        if overrides:
            # Deep merge overrides
            def deep_update(d, u):
                for k, v in u.items():
                    if isinstance(v, dict):
                        d[k] = deep_update(d.get(k, {}), v)
                    else:
                        d[k] = v
                return d
            
            deep_update(config, overrides)
            
        return config
        
    def _get_system_metrics(self) -> Dict[str, float]:
        """Get current system metrics.
        
        Returns:
            Dictionary of metrics
        """
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_io_counters()
        
        return {
            'cpu_percent': cpu_percent,
            'memory_percent': memory.percent,
            'disk_read_mb': disk.read_bytes / 1024 / 1024,
            'disk_write_mb': disk.write_bytes / 1024 / 1024
        }
        
    def run_test(self, 
                test_name: str,
                config_overrides: Dict[str, Any],
                duration_secs: int = 60) -> Dict[str, Any]:
        """Run a single performance test.
        
        Args:
            test_name: Name of the test
            config_overrides: Config values to override
            duration_secs: How long to run test
            
        Returns:
            Test results dictionary
        """
        # Load config with overrides
        config = self._load_config(config_overrides)
        
        # Write temporary config
        test_config_path = f'/tmp/test_config_{int(time.time())}.yaml'
        with open(test_config_path, 'w') as f:
            yaml.dump(config, f)
            
        try:
            # Start metrics collection
            start_metrics = self._get_system_metrics()
            start_time = time.time()
            
            # Run indexer with test config
            cmd = [
                'venv/bin/python', '-m', 'fs_indexer_elasticsearch.main',
                '--config', test_config_path,
                '--root-path', self.root_path,
                '--mode', 'elasticsearch'
            ]
            
            logger.info(f"Running test '{test_name}' with config overrides: {json.dumps(config_overrides, indent=2)}")
            
            # Run process and capture output
            import subprocess
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            # Monitor process for specified duration
            elapsed = 0
            metrics_samples = []
            while elapsed < duration_secs and process.poll() is None:
                metrics_samples.append(self._get_system_metrics())
                time.sleep(5)
                elapsed = time.time() - start_time
                
            # Get final metrics
            end_metrics = self._get_system_metrics()
            end_time = time.time()
            
            # Calculate averages
            avg_metrics = {
                k: sum(s[k] for s in metrics_samples) / len(metrics_samples)
                for k in metrics_samples[0].keys()
            }
            
            # Parse output for file count and processing rate
            stdout, stderr = process.communicate()
            
            # Extract metrics from output
            import re
            files_processed = 0
            processing_rate = 0
            
            rate_match = re.search(r'Processing Rate:\s+([\d.]+)\s+files/second', stderr)
            if rate_match:
                processing_rate = float(rate_match.group(1))
                
            files_match = re.search(r'Total Files:\s+(\d+)', stderr)
            if files_match:
                files_processed = int(files_match.group(1))
                
            # Compile results
            results = {
                'test_name': test_name,
                'config_overrides': config_overrides,
                'duration_secs': end_time - start_time,
                'files_processed': files_processed,
                'processing_rate': processing_rate,
                'avg_metrics': avg_metrics,
                'peak_metrics': {
                    k: max(s[k] for s in metrics_samples)
                    for k in metrics_samples[0].keys()
                }
            }
            
            self.results.append(results)
            return results
            
        finally:
            # Cleanup
            if os.path.exists(test_config_path):
                os.remove(test_config_path)
                
    def save_results(self, output_path: str):
        """Save test results to file.
        
        Args:
            output_path: Path to save results
        """
        with open(output_path, 'w') as f:
            json.dump({
                'results': self.results,
                'system_info': {
                    'cpu_count': psutil.cpu_count(),
                    'memory_gb': psutil.virtual_memory().total / 1024 / 1024 / 1024,
                    'disk_type': 'SSD' if self._is_ssd() else 'HDD'
                }
            }, f, indent=2)
            
    def _is_ssd(self) -> bool:
        """Detect if system disk is SSD.
        
        Returns:
            True if SSD, False if HDD
        """
        # Simple heuristic - check if rotational
        try:
            disk = psutil.disk_partitions()[0].device
            with open('/sys/block/{}/queue/rotational'.format(disk.split('/')[-1])) as f:
                return f.read().strip() == '0'
        except:
            return False  # Default to HDD if can't detect
