"""Module for calculating directory sizes efficiently."""

import logging
from typing import Dict, List, Any, Optional
import duckdb
from pathlib import Path

logger = logging.getLogger(__name__)

class DirectorySizeCalculator:
    """Handles efficient calculation of directory sizes using DuckDB."""
    
    def __init__(self, session: duckdb.DuckDBPyConnection):
        """Initialize with DuckDB session."""
        self.session = session
        
    def calculate_directory_sizes(self, items: List[Dict[str, Any]]) -> Dict[str, int]:
        """Calculate sizes for all directories in a batch of items.
        
        Uses DuckDB's efficient column operations to calculate directory sizes
        by summing file sizes within each directory path.
        
        Args:
            items: List of file/directory items from LucidLink API
            
        Returns:
            Dict mapping directory paths to their total size in bytes
        """
        if not items:
            return {}
        
        try:
            # Extract directory paths using relative_path instead of name
            dir_paths = [item['relative_path'] for item in items if item['type'] == 'directory']
            if not dir_paths:
                return {}
            
            # Use a single query to calculate sizes for all directories
            query = """
                WITH RECURSIVE
                directory_paths(path) AS (
                    -- Start with all directories we want to calculate
                    SELECT DISTINCT unnest(?::VARCHAR[]) as path
                ),
                directory_sizes AS (
                    SELECT 
                        d.path,
                        COALESCE(SUM(CASE WHEN f.type = 'file' THEN f.size ELSE 0 END), 0) as total_size
                    FROM directory_paths d
                    LEFT JOIN lucidlink_files f ON (
                        -- Match files directly in this directory or in subdirectories
                        f.relative_path LIKE d.path || '/%'
                        -- Match files that are the directory itself (for empty dirs)
                        OR f.relative_path = d.path
                    )
                    GROUP BY d.path
                )
                SELECT path, total_size
                FROM directory_sizes;
            """
            
            # Execute query and collect results
            result = self.session.execute(query, [dir_paths]).fetchall()
            sizes = {path: size for path, size in result}
            
            return sizes
            
        except Exception as e:
            logger.error(f"Error calculating directory sizes: {str(e)}")
            return {}
            
    def update_directory_items(self, items: List[Dict[str, Any]], calculate_sizes: bool = True) -> None:
        """Update directory items with their calculated sizes.
        
        Args:
            items: List of file/directory items to update
            calculate_sizes: If False, sets directory sizes to None instead of calculating
        """
        try:
            if calculate_sizes:
                # Calculate sizes for all directories
                sizes = self.calculate_directory_sizes(items)
                
                # Update directory items with their sizes
                for item in items:
                    if item['type'] == 'directory':
                        # Use relative_path to look up size
                        size = sizes.get(item['relative_path'], 0)
                        item['size'] = size
            else:
                # Set directory sizes to None when calculation is disabled
                for item in items:
                    if item['type'] == 'directory':
                        item['size'] = None
                    
        except Exception as e:
            logger.error(f"Error updating directory sizes: {str(e)}")
