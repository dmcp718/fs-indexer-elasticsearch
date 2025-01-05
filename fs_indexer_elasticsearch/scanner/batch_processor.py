#!/usr/bin/env python3

import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Generator, List, Optional
import fnmatch

logger = logging.getLogger(__name__)

class BatchProcessor:
    """Processes a single directory batch using find command."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize batch processor with configuration.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.batch_size = config.get('performance', {}).get('parallel_processing', {}).get('batch_size', 100000)
        
        # Get mount point and paths
        self.mount_point = config.get('lucidlink_filespace', {}).get('mount_point', '')
        self.root_path = config.get('root_path', '/')
        
    def _build_find_command(self, directory: str) -> List[str]:
        """Build find command with skip patterns and batch size limit."""
        cmd = [
            'find',
            os.path.expanduser(directory),
            '-not', '-path', '*/.*'  # Skip hidden files and directories
        ]
        
        # Add skip patterns
        skip_patterns = self.config.get('skip_patterns', {}).get('patterns', [])
        for pattern in skip_patterns:
            cmd.extend(['-not', '-path', f'*/{pattern}'])
            cmd.extend(['-not', '-path', f'*/{pattern}/*'])  # Also skip files in matching directories
            
        # Add -ls for detailed listing
        cmd.append('-ls')
        
        return cmd
        
    def _parse_find_line(self, line: str, exclude_hidden: bool = False) -> Optional[Dict[str, Any]]:
        """Parse a line of find -ls output.
        
        Args:
            line: Line from find -ls output
            exclude_hidden: Whether to exclude hidden files
            
        Returns:
            Dictionary with file information or None if line should be skipped
        """
        try:
            # Skip empty lines
            line = line.strip()
            if not line:
                return None
                
            # Parse find -ls output format
            parts = line.split()
            if len(parts) < 11:
                return None
                
            # Get the relevant parts
            perms = parts[2]
            size_str = parts[6]
            month = parts[7]
            day = parts[8]
            time_or_year = parts[9]
            name = ' '.join(parts[10:])
            
            # Skip hidden files/dirs if requested
            if exclude_hidden and (name.startswith('.') or '/..' in name):
                return None
                
            # Parse size
            try:
                size = int(size_str)
            except ValueError:
                return None
                
            # Determine type from permissions
            entry_type = 'directory' if perms.startswith('d') else 'file'
            
            # Parse timestamp
            current_year = datetime.now().year
            try:
                if ':' in time_or_year:
                    # Recent file: "Mon DD HH:MM"
                    timestamp = datetime.strptime(f"{month} {day} {time_or_year} {current_year}", 
                                               "%b %d %H:%M %Y")
                    if timestamp > datetime.now():
                        timestamp = timestamp.replace(year=current_year - 1)
                else:
                    # Old file: "Mon DD YYYY"
                    timestamp = datetime.strptime(f"{month} {day} {time_or_year}", 
                                               "%b %d %Y")
            except ValueError as e:
                logger.error(f"Error parsing date: {month} {day} {time_or_year} - {e}")
                timestamp = datetime.now()
                
            # Get file extension
            extension = Path(name).suffix[1:].lower() if Path(name).suffix else ''
                
            # Get filepath (without mount point)
            filepath = name
            if self.mount_point and name.startswith(self.mount_point):
                # Remove mount point prefix
                filepath = name[len(self.mount_point):]
                if not filepath.startswith('/'):
                    filepath = '/' + filepath
                
            # Generate relative path
            relative_path = filepath
            if self.root_path and self.root_path != '/':
                # Remove root path prefix if it exists
                if relative_path.startswith(self.root_path):
                    relative_path = relative_path[len(self.root_path):]
                    if not relative_path.startswith('/'):
                        relative_path = '/' + relative_path
                
            return {
                'name': Path(filepath).name,
                'relative_path': relative_path,  # Full path relative to mount point
                'filepath': filepath,  # Clean path for external use
                'size_bytes': size,
                'modified_time': timestamp,
                'creation_time': timestamp,  # Use modified time as creation time if not available
                'type': entry_type,
                'extension': extension,
                'checksum': '',  # Empty checksum for now
                'direct_link': '',  # Will be populated by DirectLinkManager
                'last_seen': datetime.now()
            }
            
        except Exception as e:
            logger.error(f"Error parsing find output line: {e}")
            return None
            
    def process_directory(self, directory: str) -> Generator[Dict[str, Any], None, None]:
        """Process a single directory using find command.
        
        Args:
            directory: Directory to process
            
        Yields:
            Dictionaries containing file information
        """
        exclude_hidden = self.config.get('skip_patterns', {}).get('hidden_files', True)
        cmd = self._build_find_command(directory)
        
        logger.debug(f"Running find command: {' '.join(cmd)}")
        
        try:
            # Run find command
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                encoding='utf-8'
            )
            
            count = 0
            for line in process.stdout:
                entry = self._parse_find_line(line.strip(), exclude_hidden)
                if entry:
                    yield entry
                    count += 1
                    
                    if count >= self.batch_size:
                        logger.info(f"Reached batch size limit of {self.batch_size} for directory {directory}")
                        break
                        
            process.terminate()
            
        except Exception as e:
            logger.error(f"Error processing directory {directory}: {e}")
            raise
