#!/usr/bin/env python3

class WorkflowStats:
    """Class to track workflow statistics"""
    
    def __init__(self):
        """Initialize workflow stats"""
        self.total_files = 0
        self.files_updated = 0
        self.files_removed = 0
        self.total_size = 0
        self.errors = []
        
    def update_stats(self, file_count: int, updated: int, size: int):
        """Update workflow statistics"""
        self.total_files += file_count
        self.files_updated += updated
        self.total_size += size
        
    def add_removed_files(self, count: int, paths: list):
        """Add removed files to stats"""
        self.files_removed += count
        if paths:
            self.errors.extend([f"File removed: {path}" for path in paths])
            
    def add_error(self, error: str):
        """Add an error message to stats"""
        self.errors.append(error)
