#!/usr/bin/env python3

import time
import logging

logger = logging.getLogger(__name__)

class WorkflowStats:
    """Class to track workflow statistics"""
    
    def __init__(self):
        """Initialize workflow stats"""
        self.start_time = time.time()
        self.total_files = 0
        self.total_dirs = 0
        self.files_updated = 0
        self.files_removed = 0
        self.files_skipped = 0
        self.total_size = 0
        self.errors = []
        
    def format_size(self, size_bytes: int) -> str:
        """Format size in bytes to human readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} PB"
        
    def update_stats(self, file_count: int = 0, updated: int = 0, size: int = 0, 
                    dirs: int = 0, skipped: int = 0):
        """Update workflow statistics"""
        self.total_files += file_count
        self.total_dirs += dirs
        self.files_updated += updated
        self.files_skipped += skipped
        self.total_size += size
        
    def add_removed_files(self, count: int, paths: list = None):
        """Add removed files to stats"""
        self.files_removed += count
        if paths:
            self.errors.extend([f"File removed: {path}" for path in paths])
            
    def add_error(self, error: str):
        """Add an error message to stats"""
        self.errors.append(error)
        
    def log_summary(self):
        """Log workflow summary with statistics."""
        elapsed_time = time.time() - self.start_time
        processing_rate = self.total_files / elapsed_time if elapsed_time > 0 else 0
        
        logger.info("=" * 80)
        logger.info("Indexer Summary:")
        logger.info(f"Time Elapsed:     {elapsed_time:.2f} seconds")
        logger.info(f"Processing Rate:  {processing_rate:.1f} files/second")
        logger.info(f"Total Size:       {self.format_size(self.total_size)}")
        logger.info(f"Total Files:      {self.total_files:,}")
        logger.info(f"Total Dirs:       {self.total_dirs:,}")
        logger.info(f"Items Updated:    {self.files_updated:,}")
        logger.info(f"Files Skipped:    {self.files_skipped:,}")
        logger.info(f"Files Removed:    {self.files_removed:,}")
        logger.info(f"Total Errors:     {len(self.errors):,}")
        logger.info("=" * 80)
        
        if self.errors:
            logger.info("\nErrors encountered:")
            for error in self.errors:
                logger.error(error)
