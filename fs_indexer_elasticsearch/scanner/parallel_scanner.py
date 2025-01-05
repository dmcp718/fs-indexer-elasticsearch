#!/usr/bin/env python3

import logging
import os
import multiprocessing
import subprocess
from typing import Dict, Any, Generator, List
from concurrent.futures import ProcessPoolExecutor
from .batch_processor import BatchProcessor

logger = logging.getLogger(__name__)

class ParallelFindScanner:
    """Scanner that uses parallel find commands to process directories."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize parallel scanner with configuration.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        parallel_config = config.get('performance', {}).get('parallel_processing', {})
        self.max_workers = parallel_config.get('max_workers', multiprocessing.cpu_count())
        self.batch_processor = BatchProcessor(config)
        
    def split_directories(self, root_path: str) -> List[str]:
        """Split root directory into subdirectories for parallel processing.
        
        Args:
            root_path: Root directory to split
            
        Returns:
            List of directory paths for parallel processing
        """
        cmd = [
            'find',
            os.path.expanduser(root_path),
            '-maxdepth', '1',
            '-type', 'd',
            '-not', '-path', '*/.*'  # Exclude hidden directories
        ]
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            # Get list of directories, excluding the root directory itself
            directories = [
                d.strip() for d in result.stdout.splitlines()
                if d.strip() != root_path
            ]
            
            if not directories:
                # If no subdirectories found, use root path itself
                return [root_path]
                
            return directories
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Error splitting directories: {e}")
            # Fall back to using root path
            return [root_path]
            
    def _process_directory_wrapper(self, directory: str) -> List[Dict[str, Any]]:
        """Wrapper to process a directory and return results as list.
        
        Args:
            directory: Directory to process
            
        Returns:
            List of file entries
        """
        try:
            return list(self.batch_processor.process_directory(directory))
        except Exception as e:
            logger.error(f"Error processing directory {directory}: {e}")
            return []
            
    def scan(self, root_path: str) -> Generator[Dict[str, Any], None, None]:
        """Scan filesystem in parallel using multiple find commands.
        
        Args:
            root_path: Root path to scan
            
        Yields:
            Dictionaries containing file information
        """
        # Get list of directories to process
        directories = self.split_directories(root_path)
        logger.info(f"Split into {len(directories)} directories for parallel processing")
        
        # Process directories in parallel
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all directories for processing
            future_to_dir = {
                executor.submit(self._process_directory_wrapper, d): d
                for d in directories
            }
            
            # Process results as they complete
            for future in future_to_dir:
                directory = future_to_dir[future]
                try:
                    results = future.result()
                    logger.info(f"Processed directory {directory}: {len(results)} entries")
                    yield from results
                except Exception as e:
                    logger.error(f"Error processing directory {directory}: {e}")
                    continue
