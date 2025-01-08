#!/usr/bin/env python3

import logging
import os
import time
import subprocess
import multiprocessing
import concurrent.futures
import platform
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Generator
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import hashlib
import duckdb
import pyarrow as pa

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
        # Default to CPU count - 2, minimum of 1
        default_workers = max(1, multiprocessing.cpu_count() - 2)
        self.max_workers = parallel_config.get('max_workers', default_workers)
        self.mount_point = config.get('lucidlink_filespace', {}).get('mount_point', '')
        self.root_path = config.get('root_path', '/')
        self.debug = config.get('debug', False)
        self.batch_size = config.get('performance', {}).get('scan_chunk_size', 25000)
        
        # New: Top-level parallelization settings
        self.use_top_level = parallel_config.get('use_top_level', False)
        top_level_settings = parallel_config.get('top_level_settings', {})
        self.min_workers = top_level_settings.get('min_workers', 4)
        self.max_memory_per_worker = top_level_settings.get('max_memory_per_worker', '2GB')
        self.size_threshold = top_level_settings.get('size_threshold', '1TB')
        
        # Progress tracking
        self._completed_dirs = 0
        self._total_dirs = 0
        self._total_files = 0
        self._total_bytes = 0
        self._last_progress = 0
        self._start_time = 0
        self._progress_interval = 5  # Log progress every 5 seconds
        
        # Initialize database in main process
        self._db_conn = None
        if config.get('database', {}).get('enabled', True):
            self._init_db()
            
    def __getstate__(self):
        """Return state for pickling, excluding database connection."""
        state = self.__dict__.copy()
        state['_db_conn'] = None  # Don't pickle the database connection
        return state
        
    def __setstate__(self, state):
        """Restore state after unpickling."""
        self.__dict__.update(state)
        
    def _log_progress(self, completed_files: int = 0):
        """Log progress if enough time has elapsed."""
        now = time.time()
        if now - self._last_progress >= self._progress_interval:
            progress = (self._completed_dirs / self._total_dirs) * 100 if self._total_dirs else 0
            rate = self._total_files / max(1, now - self._last_progress)  # Files per second
            logger.info(
                f"Progress: {self._completed_dirs}/{self._total_dirs} directories "
                f"({progress:.1f}%), {self._total_files:,} files processed "
                f"({rate:.1f} files/sec)"
            )
            self._last_progress = now
            
    def _format_size(self, size_bytes: int) -> str:
        """Format size in bytes to human readable string."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} PB"

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
                
            # Adjust worker count based on directory count
            dirs_per_worker = dir_count / optimal_workers
            if dirs_per_worker < 1:
                optimal_workers = max(1, dir_count)
                dirs_per_worker = dir_count / optimal_workers
                
            self.max_workers = optimal_workers
            return directories
            
        except Exception as e:
            logger.error(f"Error splitting directories: {e}")
            return [root_path]
            
    def _get_top_level_directories(self, root_path: str) -> List[str]:
        """Get list of top-level directories for parallel processing.
        
        Args:
            root_path: Root path to analyze
            
        Returns:
            List of top-level directory paths
        """
        if not self.use_top_level:
            return []
            
        cmd = [
            'find',
            os.path.expanduser(root_path),
            '-mindepth', '1',
            '-maxdepth', '1',
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
                check=True,
                timeout=300  # 5 minute timeout
            )
            
            directories = [d.strip() for d in result.stdout.splitlines()]
            if not directories:
                logger.info("No top-level directories found, using root path")
                return [root_path]
                
            # Calculate optimal workers based on directory count
            dir_count = len(directories)
            original_workers = self.max_workers
            
            # Use configuration settings
            min_workers = self.min_workers
            optimal_workers = max(min_workers, min(dir_count, self.max_workers))
            
            # Log worker allocation
            dirs_per_worker = dir_count / (optimal_workers or 1)
            if optimal_workers != original_workers:
                logger.info(f"Adjusting worker count to {optimal_workers} based on {dir_count} directories ({dirs_per_worker:.1f} dirs/worker)")
            else:
                logger.info(f"Using {optimal_workers} workers for {dir_count} directories ({dirs_per_worker:.1f} dirs/worker)")
                
            self.max_workers = optimal_workers
            logger.info(f"Split into {len(directories)} directories for parallel processing")
            return directories
            
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout analyzing directory structure of {root_path}")
            return [root_path]
        except Exception as e:
            logger.error(f"Error analyzing directory structure: {e}")
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

    def _process_directory(self, directory: str) -> List[Dict[str, Any]]:
        """Process a single directory.
        
        Args:
            directory: Directory path to process
            
        Returns:
            List of file entry dictionaries
        """
        try:
            # Build find command with -ls for detailed output
            cmd = [
                'find',
                os.path.expanduser(directory),
                '-ls',
                '-not', '-path', '*/.*',  # Skip hidden files
                '-not', '-name', '.*',    # Skip hidden files by name
                '-type', 'f'              # Only files
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
                timeout=300  # 5 minute timeout
            )
            
            # Process results
            entries = []
            skipped = 0
            for line in result.stdout.splitlines():
                try:
                    entry = self._parse_find_line(line)
                    if entry:
                        entries.append(entry)
                    else:
                        skipped += 1
                except Exception as e:
                    logger.error(f"Error parsing line: {e}")
                    skipped += 1
                    continue
                    
            if self.debug and skipped > 0:
                logger.debug(f"Skipped {skipped} entries in {directory}")
                
            return entries
            
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout running find command on {directory}")
            return []
        except Exception as e:
            logger.error(f"Error processing directory {directory}: {e}")
            return []

    def _process_directory_wrapper(self, directory: str) -> List[Dict[str, Any]]:
        """Wrapper to process a directory and return results as list.
        
        Args:
            directory: Directory to process
            
        Returns:
            List of file entries
        """
        try:
            # Process directory and collect results without database
            return self._process_directory(directory)
        except Exception as e:
            logger.error(f"Error processing directory {directory}: {e}")
            return []
            
    def _scan_directory(self, root_path: str) -> List[Dict[str, Any]]:
        """Scan a single directory and return file entries.
        
        Args:
            root_path: Root path to scan
            
        Returns:
            List of file entry dictionaries
        """
        try:
            # Build find command with -ls for detailed output
            cmd = [
                'find',
                os.path.expanduser(root_path),
                '-ls',
                '-not', '-path', '*/.*',  # Skip hidden files
                '-not', '-name', '.*',    # Skip hidden files by name
                '-not', '-type', 's',     # Skip sockets
                '-not', '-type', 'p',     # Skip pipes
            ]
            
            # Add skip patterns
            skip_patterns = self.config.get('skip_patterns', {}).get('patterns', [])
            for pattern in skip_patterns:
                cmd.extend(['-not', '-path', f'*/{pattern}'])
                cmd.extend(['-not', '-path', f'*/{pattern}/*'])
                
            # Run find command
            if self.debug:
                logger.debug(f"Running find command: {' '.join(cmd)}")
                
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=1800  # 30 minute timeout
            )
            
            # Process results
            entries = []
            skipped = 0
            for line in result.stdout.splitlines():
                try:
                    entry = self._parse_find_output(line)
                    if entry:
                        entries.append(entry)
                    else:
                        skipped += 1
                except Exception as e:
                    if self.debug:
                        logger.debug(f"Error processing line: {line.strip()}")
                        logger.debug(f"Error details: {e}")
                    skipped += 1
                    continue
                    
            if self.debug and skipped > 0:
                logger.debug(f"Skipped {skipped} entries in {root_path}")
                
            return entries
            
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout running find command on {root_path}")
            raise
        except Exception as e:
            logger.error(f"Error scanning directory {root_path}: {e}")
            raise
            
    def _generate_file_id(self, relative_path: str) -> str:
        """Generate a consistent file ID from the relative path.
        
        Args:
            relative_path: Relative path to the file
            
        Returns:
            SHA-256 hash of the path as hex string
        """
        return hashlib.sha256(relative_path.encode()).hexdigest()

    def _parse_find_output(self, line: str) -> Dict[str, Any]:
        """Parse find -ls output line.
        
        Args:
            line: Output line from find -ls
            
        Returns:
            Dictionary with file information or None if parsing fails
        """
        try:
            parts = line.split()
            if len(parts) < 11:
                return None
                
            # Parse standard find -ls format
            perms = parts[2]
            size_str = parts[6]
            month = parts[7]
            day = parts[8]
            time_or_year = parts[9]
            name = ' '.join(parts[10:])
            
            try:
                size = int(size_str)
            except ValueError:
                return None
                
            entry_type = 'directory' if perms.startswith('d') else 'file'
            
            # Parse timestamp with fallback
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
            except ValueError:
                # Use current time for malformed dates
                timestamp = datetime.now()
            
            # Get file extension
            extension = Path(name).suffix[1:].lower() if Path(name).suffix else ''
            
            # Clean up paths
            filepath = name
            if self.mount_point and filepath.startswith(self.mount_point):
                filepath = filepath[len(self.mount_point):]
                if not filepath.startswith('/'):
                    filepath = '/' + filepath
                    
            relative_path = filepath
            if self.root_path and relative_path.startswith(self.root_path):
                relative_path = relative_path[len(self.root_path):]
                if not relative_path.startswith('/'):
                    relative_path = '/' + relative_path
                
            return {
                'id': self._generate_file_id(relative_path),
                'name': Path(filepath).name,
                'relative_path': relative_path,
                'filepath': filepath,
                'size_bytes': size,
                'modified_time': timestamp,
                'creation_time': timestamp,
                'type': entry_type,
                'extension': extension,
                'checksum': '',
                'direct_link': '',
                'last_seen': datetime.now()
            }
            
        except Exception as e:
            logger.debug(f"Error parsing find output: {e}")
            return None

    def _get_file_info(self, path: str) -> Dict[str, Any]:
        """Get file information.
        
        Args:
            path: File path
            
        Returns:
            Dictionary with file information or None if parsing fails
        """
        try:
            # For simple path, just get basic info
            if '\n' not in path and len(path.split()) == 1:
                filepath = path.strip()
                if not filepath:
                    return None
                    
                try:
                    stat = os.stat(filepath)
                    timestamp = datetime.fromtimestamp(stat.st_mtime)
                    size = stat.st_size
                except (OSError, ValueError) as e:
                    logger.debug(f"Error getting file stats for {filepath}: {e}")
                    timestamp = datetime.now()
                    size = 0
                    
                # Get file extension
                extension = Path(filepath).suffix[1:].lower() if Path(filepath).suffix else ''
                    
                # Get filepath (without mount point)
                if self.mount_point and filepath.startswith(self.mount_point):
                    filepath = filepath[len(self.mount_point):]
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
                    'id': self._generate_file_id(relative_path),
                    'name': Path(filepath).name,
                    'relative_path': relative_path,  # Full path relative to mount point
                    'filepath': filepath,  # Clean path for external use
                    'size_bytes': size,
                    'modified_time': timestamp,
                    'creation_time': timestamp,
                    'type': 'file',
                    'extension': extension,
                    'checksum': '',
                    'direct_link': '',
                    'last_seen': datetime.now()
                }
                
            return None
            
        except Exception as e:
            logger.debug(f"Error parsing file info: {e}")
            return None

    def _process_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Process a batch of file entries."""
        if not batch or not self._db_conn:
            return
            
        try:
            # Convert batch to Arrow table
            arrow_table = pa.Table.from_pylist(batch)
            
            # Register Arrow table with DuckDB
            self._db_conn.execute("BEGIN TRANSACTION")
            try:
                self._db_conn.register('arrow_table', arrow_table)
                
                # Insert new records
                self._db_conn.execute("""
                    INSERT INTO files 
                    SELECT * FROM arrow_table
                    WHERE NOT EXISTS (
                        SELECT 1 FROM files 
                        WHERE files.relative_path = arrow_table.relative_path
                    )
                    ON CONFLICT(relative_path) DO UPDATE SET
                        name = excluded.name,
                        filepath = excluded.filepath,
                        size_bytes = excluded.size_bytes,
                        modified_time = excluded.modified_time,
                        creation_time = excluded.creation_time,
                        type = excluded.type,
                        extension = excluded.extension,
                        checksum = excluded.checksum,
                        direct_link = excluded.direct_link,
                        last_seen = excluded.last_seen
                    WHERE excluded.modified_time > files.modified_time
                """)
                self._db_conn.execute("COMMIT")
            except Exception as e:
                self._db_conn.execute("ROLLBACK")
                raise e
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            raise

    def _init_db(self):
        """Setup DuckDB database with optimizations."""
        db_path = self._get_db_path()
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Always connect in read-write mode initially
        self._db_conn = duckdb.connect(db_path)
        
        # Create tables if they don't exist
        if not self._db_conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files'").fetchone():
            self._db_conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id VARCHAR,
                name VARCHAR,
                relative_path VARCHAR PRIMARY KEY,  -- Keep for internal use
                filepath VARCHAR,  -- Clean path for external use
                size_bytes BIGINT,
                modified_time TIMESTAMP,
                creation_time TIMESTAMP,
                type VARCHAR,
                extension VARCHAR,
                checksum VARCHAR,
                direct_link VARCHAR,
                last_seen TIMESTAMP
            )
            """)
        
        # Enable optimizations
        self._db_conn.execute("SET enable_progress_bar=false")
        self._db_conn.execute("SET temp_directory='data'")
        self._db_conn.execute("SET memory_limit='4GB'")
        self._db_conn.execute("PRAGMA threads=8")
        
    def _get_db_path(self) -> str:
        """Get database path based on filespace name."""
        if self.config.get('lucidlink_filespace', {}).get('enabled', False):
            filespace = self.config.get('lucidlink_filespace', {}).get('name', 'default')
            return f"data/{filespace}_index.duckdb"
        return "data/fs_index.duckdb"

    def _get_multiprocessing_context(self) -> multiprocessing.context.BaseContext:
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

    def scan(self, root_path: str) -> Generator[Dict[str, Any], None, None]:
        """Scan filesystem in parallel and yield file entries.
        
        Args:
            root_path: Root path to scan
            
        Returns:
            Generator yielding dictionaries containing file information
        """
        # Get list of directories to process
        directories = self._get_top_level_directories(root_path) if self.use_top_level else self.split_directories(root_path)
        
        if not directories:
            logger.warning("No directories to process")
            return
            
        logger.info(f"Processing {len(directories)} directories with {self.max_workers} workers")
        
        # Initialize progress tracking
        self._start_time = time.time()
        self._completed_dirs = 0
        self._total_dirs = len(directories)
        
        # Process directories in parallel
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            try:
                # Submit all directories for processing
                future_to_dir = {
                    executor.submit(self._process_directory, directory): directory
                    for directory in directories
                }
                
                # Process results as they complete
                for future in concurrent.futures.as_completed(future_to_dir):
                    directory = future_to_dir[future]
                    try:
                        results = future.result()
                        self._completed_dirs += 1
                        
                        # Process and yield results
                        for entry in results:
                            self._total_files += 1
                            self._total_bytes += entry.get('size', 0)
                            yield entry
                            
                        # Log progress periodically
                        self._log_progress()
                        
                    except Exception as e:
                        logger.error(f"Error processing directory {directory}: {e}")
                        continue
                        
            except KeyboardInterrupt:
                logger.info("Scan interrupted by user")
                executor.shutdown(wait=False)
                return
                
        # Log final statistics
        duration = time.time() - self._start_time
        logger.info(
            f"Scan completed in {duration:.1f}s: "
            f"{self._total_files:,} files, "
            f"{self._total_bytes / (1024**3):.1f}GB"
        )
