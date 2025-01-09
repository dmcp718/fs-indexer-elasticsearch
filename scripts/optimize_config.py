#!/usr/bin/env python3

import argparse
import logging
import json
import os
from pathlib import Path
from fs_indexer_elasticsearch.utils.optimize_config import ConfigOptimizer

def main():
    parser = argparse.ArgumentParser(description='Optimize fs-indexer-elasticsearch configuration')
    parser.add_argument('--config', required=True, help='Path to base config file')
    parser.add_argument('--output', required=True, help='Path to save optimization results')
    parser.add_argument('--root-path', required=True, help='Root path to scan')
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Ensure output directory exists
    output_dir = os.path.dirname(os.path.abspath(args.output))
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Run optimization
        optimizer = ConfigOptimizer(args.config, args.root_path)
        optimal_config = optimizer.optimize(args.output)
        
        # Print recommendations
        print("\nRecommended Configuration Settings:")
        print(json.dumps(optimal_config, indent=2))
        print(f"\nDetailed results saved to: {args.output}")
        
    except Exception as e:
        logging.error(f"Error during optimization: {e}", exc_info=True)
        raise
    
if __name__ == '__main__':
    main()
