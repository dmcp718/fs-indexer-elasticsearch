#!/usr/bin/env python3

import logging
import os
import subprocess
import multiprocessing
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Generator, List
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

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
        
    def analyze_directory_structure(self, root_path: str) -> Dict[str, Any]:
        """Analyze directory structure to optimize splitting and worker allocation.
        
        Args:
            root_path: Root path to analyze
            
        Returns:
            Dictionary with directory structure metrics
        """
        cmd = [
            'find',
            os.path.expanduser(root_path),
            '-type', 'd',
            '-not', '-path', '*/.*'
        ]
        
        # Add skip patterns
        skip_patterns = self.config.get('skip_patterns', {}).get('patterns', [])
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
            
            directories = [d.strip() for d in result.stdout.splitlines()]
            if not directories:
                return {'max_depth': 0, 'total_dirs': 0, 'dirs_per_level': {}}
                
            # Calculate directory metrics
            depths = [d.count(os.sep) for d in directories]
            max_depth = max(depths)
            dirs_per_level = {}
            for depth in range(max_depth + 1):
                dirs_per_level[depth] = sum(1 for d in depths if d == depth)
                
            return {
                'max_depth': max_depth,
                'total_dirs': len(directories),
                'dirs_per_level': dirs_per_level
            }
            
        except Exception as e:
            logger.error(f"Error analyzing directory structure: {e}")
            return {'max_depth': 0, 'total_dirs': 1, 'dirs_per_level': {0: 1}}
            
    def calculate_optimal_workers(self, metrics: Dict[str, Any]) -> int:
        """Calculate optimal number of workers based on directory metrics.
        
        Args:
            metrics: Directory structure metrics
            
        Returns:
            Optimal number of workers
        """
        max_workers = self.config.get('performance', {}).get('parallel_processing', {}).get('max_workers', multiprocessing.cpu_count())
        
        # Base number of workers on directory density and depth
        total_dirs = metrics['total_dirs']
        max_depth = metrics['max_depth']
        
        if max_depth <= 1 or total_dirs <= 1:
            return 1
            
        # Calculate directory density score (0-2)
        # Scale based on total directories, allowing for larger scores
        density_score = min(total_dirs / 10000, 2.0)  # Up to 20k dirs for max score
        
        # Calculate depth score (0-2)
        depth_score = min(max_depth / 20, 2.0)  # Up to depth 40 for max score
        
        # Calculate size-based score from config if available
        size_threshold_tb = 5  # 5 TB threshold for scaling
        total_size_tb = self.config.get('total_size_tb', 0)
        size_score = min(total_size_tb / size_threshold_tb, 2.0) if total_size_tb > 0 else 0
        
        # Combine scores to get worker multiplier (0.5-2.0)
        # Weight density more heavily for better parallelization
        worker_multiplier = 0.5 + ((density_score * 1.5 + depth_score + size_score) / 4)
        
        # Calculate base workers from CPU count
        cpu_count = multiprocessing.cpu_count()
        base_workers = max(4, cpu_count)  # Minimum 4 workers
        
        # Calculate recommended workers
        recommended = min(
            max(4, int(base_workers * worker_multiplier)),  # At least 4 workers
            max_workers,  # Respect configured max
            total_dirs // 10 + 1  # Don't exceed 1 worker per 10 directories
        )
        
        logger.info(
            f"Directory metrics: {total_dirs} dirs, depth {max_depth}, "
            f"size {total_size_tb:.1f}TB"
        )
        logger.info(
            f"Worker calculation: density={density_score:.2f}, depth={depth_score:.2f}, "
            f"size={size_score:.2f}, multiplier={worker_multiplier:.2f}"
        )
        logger.info(f"Using {recommended} workers (base={base_workers}, max={max_workers})")
        
        return recommended
            
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
                
            # Calculate optimal workers based on directory count
            dir_count = len(directories)
            original_workers = self.max_workers
            
            if dir_count <= 20:
                # For small directory counts (<=20), use 2-3 dirs per worker
                optimal_workers = max(1, min(dir_count // 2, self.max_workers))
            else:
                # For larger directory counts, scale up more aggressively
                # but maintain reasonable dir/worker ratio
                optimal_workers = max(1, min(dir_count // 4, self.max_workers))
                
            # Always log worker allocation for transparency
            dirs_per_worker = dir_count / (optimal_workers or 1)
            if optimal_workers != original_workers:
                logger.info(f"Adjusting worker count to {optimal_workers} based on {dir_count} directories ({dirs_per_worker:.1f} dirs/worker)")
            else:
                logger.info(f"Using {optimal_workers} workers for {dir_count} directories ({dirs_per_worker:.1f} dirs/worker)")
                
            self.max_workers = optimal_workers
            logger.info(f"Split into {len(directories)} directories for parallel processing")
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
            
    def _scan_directory(self, directory: str) -> List[Dict[str, Any]]:
        """Scan a single directory and return file entries.
        
        Args:
            directory: Directory path to scan
            
        Returns:
            List of file entry dictionaries
        """
        try:
            # Build find command
            cmd = [
                'find',
                os.path.expanduser(directory),
                '-type', 'f',
                '-not', '-path', '*/.*'
            ]
            
            # Add skip patterns
            skip_patterns = self.config.get('skip_patterns', {}).get('patterns', [])
            for pattern in skip_patterns:
                cmd.extend(['-not', '-path', f'*/{pattern}'])
                cmd.extend(['-not', '-path', f'*/{pattern}/*'])
                
            # Run find command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=1800  # 30 minute timeout
            )
            
            # Process results
            entries = []
            for line in result.stdout.splitlines():
                try:
                    path = line.strip()
                    if path:
                        entry = self._get_file_info(path)
                        if entry:
                            entries.append(entry)
                except Exception as e:
                    logger.error(f"Error processing file {line.strip()}: {e}")
                    continue
                    
            return entries
            
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout running find command on {directory}")
            raise
        except Exception as e:
            logger.error(f"Error scanning directory {directory}: {e}")
            raise

    def scan(self, root_path: str) -> Generator[Dict[str, Any], None, None]:
        """Scan filesystem in parallel and yield file entries.
        
        Args:
            root_path: Root directory to scan
            
        Yields:
            Dict containing file metadata
        """
        directories = self.split_directories(root_path)
        if not directories:
            return

        # Track failed directories for retry
        failed_dirs = []
        retry_count = 0
        max_retries = 2  # Maximum number of retry attempts
        
        while directories and retry_count <= max_retries:
            # Create thread pool for parallel processing
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                
                # Submit scan jobs
                for directory in directories:
                    future = executor.submit(self._scan_directory, directory)
                    futures.append((directory, future))
                    
                # Process results as they complete
                for directory, future in futures:
                    try:
                        # Get results with timeout
                        results = future.result(timeout=3600)  # 1 hour timeout per directory
                        for result in results:
                            yield result
                            
                    except TimeoutError:
                        logger.error(f"Timeout scanning directory: {directory}")
                        failed_dirs.append(directory)
                    except Exception as e:
                        logger.error(f"Error scanning directory {directory}: {e}")
                        failed_dirs.append(directory)
                        
            # If we have failed directories, retry with reduced concurrency
            if failed_dirs:
                retry_count += 1
                if retry_count <= max_retries:
                    reduced_workers = max(1, self.max_workers // (retry_count * 2))
                    logger.warning(
                        f"Retry #{retry_count}: Retrying {len(failed_dirs)} failed directories "
                        f"with {reduced_workers} workers"
                    )
                    directories = failed_dirs
                    failed_dirs = []
                    self.max_workers = reduced_workers
                else:
                    logger.error(
                        f"Giving up on {len(failed_dirs)} directories after {max_retries} retries: "
                        f"{', '.join(failed_dirs)}"
                    )
                    break
            else:
                break
                
    def _get_file_info(self, path: str) -> Dict[str, Any]:
        """Get file information.
        
        Args:
            path: File path
            
        Returns:
            Dictionary with file information
        """
        try:
            # Parse find -ls output format
            parts = path.split()
            if len(parts) < 11:
                return None
                
            # Get the relevant parts
            perms = parts[2]
            size_str = parts[6]
            month = parts[7]
            day = parts[8]
            time_or_year = parts[9]
            name = ' '.join(parts[10:])
            
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
