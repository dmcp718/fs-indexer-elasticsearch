#!/usr/bin/env python3

import logging
import os
import subprocess
import multiprocessing
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Generator, List
from concurrent.futures import ProcessPoolExecutor

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
        self.max_workers = parallel_config.get('max_workers', min(4, multiprocessing.cpu_count()))
        self.mount_point = config.get('lucidlink_filespace', {}).get('mount_point', '')
        self.root_path = config.get('root_path', '/')
        
    def split_directories(self, root_path: str) -> List[str]:
        """Split root directory into subdirectories for parallel processing.
        
        Args:
            root_path: Root directory to split
            
        Returns:
            List of directory paths for parallel processing
        """
        # For small directory structures, just return the root path
        if not os.path.isdir(root_path):
            return [root_path]
            
        # Build find command with skip patterns
        skip_patterns = self.config.get('skip_patterns', {}).get('patterns', [])
        cmd = [
            'find',
            os.path.expanduser(root_path),
            '-maxdepth', '1',
            '-type', 'd',
            '-not', '-path', '*/.*'  # Exclude hidden directories
        ]
        
        # Add skip patterns
        for pattern in skip_patterns:
            cmd.extend(['-not', '-path', f'*/{pattern}'])
            cmd.extend(['-not', '-path', f'*/{pattern}/*'])
            
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
                return [root_path]
                
            return directories
            
        except Exception as e:
            logger.error(f"Error splitting directories: {e}")
            return [root_path]
            
    def _parse_find_line(self, line: str, exclude_hidden: bool = False) -> Dict[str, Any]:
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
            if self.mount_point:
                if name.startswith(self.mount_point):
                    # Remove mount point prefix
                    filepath = name[len(self.mount_point):]
                    if not filepath.startswith('/'):
                        filepath = '/' + filepath
                
            # Generate relative path
            relative_path = filepath
            if self.root_path:
                # Remove root path prefix if it exists
                if relative_path.startswith(self.root_path):
                    relative_path = relative_path[len(self.root_path):]
                    if not relative_path.startswith('/'):
                        relative_path = '/' + relative_path
                
            return {
                'id': None,  # Will be generated later
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

    def _process_directory(self, directory: str) -> Generator[Dict[str, Any], None, None]:
        """Process a directory using find command.
        
        Args:
            directory: Directory to process
            
        Yields:
            File entries
        """
        # Build find command with exclusions
        exclude_hidden = self.config.get('skip_patterns', {}).get('hidden_files', True)
        exclude_hidden_dirs = self.config.get('skip_patterns', {}).get('hidden_dirs', True)
        skip_patterns = self.config.get('skip_patterns', {}).get('patterns', [])
        
        cmd = ['find', os.path.expanduser(directory)]
        
        # Add exclusions for hidden files/dirs
        if exclude_hidden or exclude_hidden_dirs:
            cmd.extend(['-not', '-path', '*/.*'])
            
        # Add skip patterns
        for pattern in skip_patterns:
            find_pattern = f"*/{pattern}"
            if not find_pattern.startswith('*/'):
                find_pattern = f"*/{find_pattern}"
            cmd.extend(['-not', '-path', find_pattern])
            
        # Add -ls for detailed listing
        cmd.append('-ls')
        
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                encoding='utf-8'
            )
            
            # Process output directly
            for line in process.stdout:
                entry = self._parse_find_line(line.strip(), exclude_hidden)
                if entry:
                    yield entry
                    
        except Exception as e:
            logger.error(f"Error processing directory {directory}: {e}")
            
        finally:
            if process:
                process.terminate()
                
    def _process_directory_wrapper(self, directory: str) -> List[Dict[str, Any]]:
        """Wrapper to process a directory and return results as list.
        
        Args:
            directory: Directory to process
            
        Returns:
            List of file entries
        """
        try:
            return list(self._process_directory(directory))
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
        
        # If only one directory, process it directly
        if len(directories) == 1:
            yield from self._process_directory(directories[0])
            return
            
        logger.info(f"Split into {len(directories)} directories for parallel processing")
        
        # Process directories in parallel
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for directory in directories:
                future = executor.submit(self._process_directory_wrapper, directory)
                futures.append(future)
                
            # Process results as they complete
            for future in futures:
                try:
                    results = future.result()
                    yield from results
                except Exception as e:
                    logger.error(f"Error processing directory: {e}")
                    continue
