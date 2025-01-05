#!/usr/bin/env python3

import time
import logging
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)

class IndexerStats:
    """Tracks statistics for the indexing process."""
    
    def __init__(self):
        """Initialize statistics tracking."""
        self.start_time = time.time()
        self.total_size = 0
        self.total_files = 0
        self.total_dirs = 0
        self.items_updated = 0
        self.files_skipped = 0
        self.files_removed = 0
        self.total_errors = 0
        
    def format_size(self, size_bytes: int) -> str:
        """Format size in bytes to human readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} PB"
        
    def update(self, size_bytes: int = 0, is_file: bool = True, is_dir: bool = False, 
              updated: bool = True, skipped: bool = False, removed: bool = False, 
              error: bool = False):
        """Update statistics with a single item."""
        self.total_size += size_bytes
        if is_file:
            self.total_files += 1
        if is_dir:
            self.total_dirs += 1
        if updated:
            self.items_updated += 1
        if skipped:
            self.files_skipped += 1
        if removed:
            self.files_removed += 1
        if error:
            self.total_errors += 1
            
    def log_summary(self):
        """Log indexer summary with statistics."""
        elapsed_time = time.time() - self.start_time
        processing_rate = self.total_files / elapsed_time if elapsed_time > 0 else 0
        
        logger.info("=" * 80)
        logger.info("Indexer Summary:")
        logger.info(f"Time Elapsed:     {elapsed_time:.2f} seconds")
        logger.info(f"Processing Rate:  {processing_rate:.1f} files/second")
        logger.info(f"Total Size:       {self.format_size(self.total_size)}")
        logger.info(f"Total Files:      {self.total_files:,}")
        logger.info(f"Total Dirs:       {self.total_dirs:,}")
        logger.info(f"Items Updated:    {self.items_updated:,}")
        logger.info(f"Files Skipped:    {self.files_skipped:,}")
        logger.info(f"Files Removed:    {self.files_removed:,}")
        logger.info(f"Total Errors:     {self.total_errors:,}")
        logger.info("=" * 80)
