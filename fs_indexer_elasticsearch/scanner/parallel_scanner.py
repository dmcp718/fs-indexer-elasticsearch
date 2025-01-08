#!/usr/bin/env python3

import logging
import os
import subprocess
import multiprocessing
import platform
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Generator, List
from concurrent.futures import ProcessPoolExecutor
from multiprocessing.context import BaseContext

logger = logging.getLogger(__name__)

class ParallelFindScanner:
    """Scanner that uses parallel find commands to process directories."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize parallel scanner with configuration.
        
        Args:
            config: Configuration dictionary
        """
        parallel_config = config.get('performance', {}).get('parallel_processing', {})
        # Default to CPU count - 2, minimum of 1
        default_workers = max(1, multiprocessing.cpu_count() - 2)
        self.max_workers = parallel_config.get('max_workers', default_workers)
        self.mount_point = config.get('lucidlink_filespace', {}).get('mount_point', '')
        self.root_path = config.get('root_path', '/')
        
        # Extract config values needed for scanning
        self.exclude_hidden = config.get('skip_patterns', {}).get('hidden_files', True)
        self.exclude_hidden_dirs = config.get('skip_patterns', {}).get('hidden_dirs', True)
        self.skip_patterns = config.get('skip_patterns', {}).get('patterns', [])

    @staticmethod
    def _process_directory_static(directory: str, exclude_hidden: bool, exclude_hidden_dirs: bool, skip_patterns: List[str], mount_point: str, root_path: str) -> List[Dict[str, Any]]:
        """Static method to process a directory, making it picklable for multiprocessing.
        
        Args:
            directory: Directory to process
            exclude_hidden: Whether to exclude hidden files
            exclude_hidden_dirs: Whether to exclude hidden directories
            skip_patterns: List of patterns to skip
            mount_point: Mount point path
            root_path: Root path
            
        Returns:
            List of file entries
        """
        try:
            entries = []
            for entry in ParallelFindScanner._process_directory_inner(directory, exclude_hidden, exclude_hidden_dirs, skip_patterns, mount_point, root_path):
                # Generate file ID
                relative_path = entry.get('relative_path', '')
                if relative_path:
                    entry['id'] = ParallelFindScanner._generate_file_id(relative_path)
                entries.append(entry)
            return entries
        except Exception as e:
            logger.error(f"Error processing directory {directory}: {e}")
            return []

    @staticmethod
    def _process_directory_inner(directory: str, exclude_hidden: bool, exclude_hidden_dirs: bool, skip_patterns: List[str], mount_point: str, root_path: str) -> Generator[Dict[str, Any], None, None]:
        """Static method to process a directory using find command.
        
        Args:
            directory: Directory to process
            exclude_hidden: Whether to exclude hidden files
            exclude_hidden_dirs: Whether to exclude hidden directories
            skip_patterns: List of patterns to skip
            mount_point: Mount point path
            root_path: Root path
            
        Yields:
            File entries
        """
        # Build find command with exclusions
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
                entry = ParallelFindScanner._parse_find_line(line.strip(), exclude_hidden, mount_point, root_path)
                if entry:
                    yield entry
                    
        except Exception as e:
            logger.error(f"Error processing directory {directory}: {e}")
            
        finally:
            if process:
                process.terminate()

    @staticmethod
    def _parse_find_line(line: str, exclude_hidden: bool, mount_point: str, root_path: str) -> Dict[str, Any]:
        """Parse a line of find -ls output.
        
        Args:
            line: Line from find -ls output
            exclude_hidden: Whether to exclude hidden files
            mount_point: Mount point path
            root_path: Root path
            
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
            if mount_point:
                if name.startswith(mount_point):
                    # Remove mount point prefix
                    filepath = name[len(mount_point):]
                    if not filepath.startswith('/'):
                        filepath = '/' + filepath
                
            # Generate relative path
            relative_path = filepath
            if root_path:
                # Remove root path prefix if it exists
                if relative_path.startswith(root_path):
                    relative_path = relative_path[len(root_path):]
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

    @staticmethod
    def _generate_file_id(relative_path: str) -> str:
        """Generate a consistent file ID from the relative path.
        
        Args:
            relative_path: Relative path to the file
            
        Returns:
            SHA-256 hash of the path as hex string
        """
        import hashlib
        # Normalize path and encode as UTF-8
        normalized_path = os.path.normpath(relative_path).encode('utf-8')
        # Generate SHA-256 hash
        return hashlib.sha256(normalized_path).hexdigest()

    def _get_multiprocessing_context(self) -> BaseContext:
        """Get the appropriate multiprocessing context based on the platform.
        
        Returns:
            Multiprocessing context optimized for the current platform
        """
        system = platform.system().lower()
        try:
            if system == 'darwin':
                # Use 'spawn' on macOS to avoid issues with forking
                return multiprocessing.get_context('spawn')
            elif system == 'linux':
                # Use 'fork' on Linux for better performance
                return multiprocessing.get_context('fork')
            else:
                # Default to 'spawn' for other platforms
                logger.warning(f"Unknown platform {system}, defaulting to 'spawn' method")
                return multiprocessing.get_context('spawn')
        except Exception as e:
            logger.error(f"Error creating multiprocessing context: {e}")
            logger.warning("Falling back to default context")
            return multiprocessing.get_context()

    def get_optimal_workers(self, directories: List[str]) -> int:
        """Calculate optimal number of workers based on directories and config max.
        
        The number of workers is dynamically adjusted to match the number of top-level
        directories, but will not exceed the configured maximum. This approach showed
        ~5% better performance in testing, as it eliminates idle workers and reduces
        resource contention.
        
        Args:
            directories: List of directories to process
            
        Returns:
            Optimal number of workers (min of directory count and configured max)
        """
        return min(len(directories), self.max_workers)
        
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
        cmd = [
            'find',
            os.path.expanduser(root_path),
            '-maxdepth', '1',
            '-type', 'd',
            '-not', '-path', '*/.*'  # Exclude hidden directories
        ]
        
        # Add skip patterns
        for pattern in self.skip_patterns:
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
            
    def scan(self, root_path: str) -> Generator[Dict[str, Any], None, None]:
        """Scan filesystem in parallel using multiple find commands.
        
        Args:
            root_path: Root path to scan
            
        Yields:
            Dictionaries containing file information
        """
        # Get list of directories to process
        directories = self.split_directories(root_path)
        worker_count = self.get_optimal_workers(directories)
        logger.info(f"Using {worker_count} workers for {len(directories)} directories")
        
        # Get platform-specific multiprocessing context
        ctx = self._get_multiprocessing_context()
        
        # Process directories in parallel
        with ProcessPoolExecutor(max_workers=worker_count,
                               mp_context=ctx) as executor:
            # Submit all directories for processing
            futures = []
            for directory in directories:
                try:
                    future = executor.submit(self._process_directory_static, directory, 
                                          self.exclude_hidden, self.exclude_hidden_dirs, 
                                          self.skip_patterns, self.mount_point, self.root_path)
                    futures.append(future)
                except Exception as e:
                    logger.error(f"Error submitting directory {directory} for processing: {e}")
                    continue
            
            # Process results as they complete
            for future in futures:
                try:
                    result = future.result(timeout=300)  # 5 minute timeout per directory
                    for entry in result:
                        yield entry
                except TimeoutError:
                    logger.error("Directory processing timed out after 5 minutes")
                    continue
                except Exception as e:
                    logger.error(f"Error processing directory batch: {e}")
                    if hasattr(e, '__cause__') and e.__cause__:
                        logger.error(f"Caused by: {e.__cause__}")
                    continue
