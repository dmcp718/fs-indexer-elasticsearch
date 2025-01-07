#!/usr/bin/env python3

import logging
import itertools
from typing import Dict, Any, List
import json
from pathlib import Path
from .perf_test import PerformanceTest

logger = logging.getLogger(__name__)

class ConfigOptimizer:
    """Optimize configuration settings through systematic testing."""
    
    def __init__(self, base_config_path: str, root_path: str):
        """Initialize optimizer with base config path.
        
        Args:
            base_config_path: Path to base config file
            root_path: Root path to scan
        """
        self.base_config_path = base_config_path
        self.root_path = root_path
        self.perf_test = PerformanceTest(base_config_path, root_path)
        
    def optimize(self, output_path: str):
        """Run optimization tests and save results.
        
        Args:
            output_path: Path to save results
        """
        # Define parameter ranges to test - focused on most impactful parameters
        param_ranges = {
            'performance.batch_sizes.elasticsearch': [25000, 50000, 100000],
            'performance.parallel_processing.max_workers': [8, 12, 16],
            'performance.parallel_processing.batch_size': [50000, 100000],
            'database.connection.options.memory_limit': ['4GB', '8GB']
        }
        
        # Test combinations strategically
        test_configs = self._generate_test_configs(param_ranges)
        
        logger.info(f"Running {len(test_configs)} test configurations...")
        
        # Run tests
        for i, config in enumerate(test_configs):
            test_name = f"config_test_{i+1}"
            self.perf_test.run_test(test_name, config, duration_secs=30)  # Reduced duration
            
        # Save results
        self.perf_test.save_results(output_path)
        
        # Analyze and recommend optimal config
        optimal_config = self._analyze_results(self.perf_test.results)
        
        return optimal_config
        
    def _generate_test_configs(self, param_ranges: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
        """Generate test configurations strategically.
        
        Instead of testing all combinations (which would be too many),
        use a smart sampling approach:
        
        1. Test baseline config
        2. Test each parameter independently
        3. Test some strategic combinations
        
        Args:
            param_ranges: Dictionary of parameter ranges to test
            
        Returns:
            List of config override dictionaries
        """
        configs = []
        
        # Add baseline (middle values)
        baseline = {}
        for param, values in param_ranges.items():
            baseline[param] = values[len(values)//2]
        configs.append(baseline)
        
        # Test each parameter independently
        for param, values in param_ranges.items():
            for value in values:
                if value != baseline[param]:
                    config = baseline.copy()
                    config[param] = value
                    configs.append(config)
                    
        # Add some strategic combinations
        # Focus on parameters that are likely to interact:
        # - batch sizes
        # - workers and threads
        # - memory and batch sizes
        strategic_groups = [
            ['performance.batch_sizes.elasticsearch', 
             'performance.parallel_processing.max_workers'],
            ['performance.parallel_processing.batch_size',
             'database.connection.options.memory_limit']
        ]
        
        for group in strategic_groups:
            # Get relevant param ranges
            group_ranges = {
                param: param_ranges[param]
                for param in group
            }
            
            # Generate some combinations
            keys = list(group_ranges.keys())
            for values in itertools.product(*group_ranges.values()):
                config = baseline.copy()
                for k, v in zip(keys, values):
                    config[k] = v
                configs.append(config)
                
        return configs
        
    def _analyze_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze test results and recommend optimal config.
        
        Args:
            results: List of test results
            
        Returns:
            Optimal config overrides
        """
        # Sort by processing rate
        sorted_results = sorted(
            results,
            key=lambda x: x['processing_rate'],
            reverse=True
        )
        
        # Get top 3 configs
        top_configs = sorted_results[:3]
        
        # Analyze patterns in top configs
        param_patterns = {}
        for param in top_configs[0]['config_overrides'].keys():
            values = [c['config_overrides'][param] for c in top_configs]
            
            # If all top configs agree on a value, use it
            if len(set(values)) == 1:
                param_patterns[param] = values[0]
            else:
                # Otherwise use the value from the best config
                param_patterns[param] = values[0]
                
        return param_patterns
