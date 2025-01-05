#!/usr/bin/env python3

import logging
import subprocess
import threading
from datetime import datetime
from pathlib import Path
from queue import Queue, Empty
from typing import Dict, Any, Optional, Generator, List
import duckdb
import pyarrow as pa
import hashlib
import os
import fnmatch

logger = logging.getLogger(__name__)

class FileScanner:
    """Optimized file scanner using find command and DuckDB."""
    
    def __init__(self, config: Dict[str, Any], mode: str = 'default'):
        """Initialize scanner with configuration."""
        self.config = config
        self.mode = mode
        self.batch_size = config.get('performance', {}).get('scan_chunk_size', 25000)
        
        # Initialize database only if enabled
        self.conn = None
        if config.get('database', {}).get('enabled', True):
            self._init_db()
        
        # Get mount point and paths
        self.mount_point = config.get('lucidlink_filespace', {}).get('mount_point', '')
        self.root_path = config.get('root_path', '/')
        if not self.mount_point:
            logger.warning("No mount point configured, using absolute paths")
            
        # Initialize parallel scanner if enabled
        parallel_config = config.get('performance', {}).get('parallel_processing', {})
        self.parallel_enabled = parallel_config.get('enabled', False)
        if self.parallel_enabled:
            from .parallel_scanner import ParallelFindScanner
            self.parallel_scanner = ParallelFindScanner(config)
            
    def _get_db_path(self) -> str:
        """Get database path based on filespace name."""
        if self.config.get('lucidlink_filespace', {}).get('enabled', False):
            filespace = self.config.get('lucidlink_filespace', {}).get('name', 'default')
            return f"data/{filespace}_index.duckdb"
        return "data/fs_index.duckdb"
        
    def _init_db(self):
        """Setup DuckDB database with optimizations."""
        # Connect to database
        self.conn = duckdb.connect(self._get_db_path())
        
        # Handle database setup based on mode
        check_missing_files = self.config.get('check_missing_files', True)
        if not check_missing_files:
            # Drop and recreate mode - close connection and delete file
            self.conn.close()
            try:
                os.remove(self._get_db_path())
            except FileNotFoundError:
                pass
            self.conn = duckdb.connect(self._get_db_path())
            
        # Create table if not exists
        self.conn.execute("""
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
        self.conn.execute("SET enable_progress_bar=false")
        self.conn.execute("SET temp_directory='data'")
        self.conn.execute("SET memory_limit='4GB'")
        self.conn.execute("PRAGMA threads=8")
        
    def setup_database(self):
        """Setup database if not already initialized."""
        if not self.conn and self.config.get('database', {}).get('enabled', True):
            self._init_db()
            
    def _should_skip_path(self, path: str) -> bool:
        """Check if a path should be skipped based on config patterns.
        
        Args:
            path: Path to check
            
        Returns:
            True if path should be skipped, False otherwise
        """
        path_obj = Path(path)
        
        # Get skip patterns from config
        skip_patterns = self.config.get('skip_patterns', {})
        patterns = skip_patterns.get('patterns', [])
        
        # Check if path matches any skip pattern
        for pattern in patterns:
            if fnmatch.fnmatch(path, pattern):
                return True
            
            # Also check each part of the path for directory patterns
            path_parts = path.split('/')
            for part in path_parts:
                if fnmatch.fnmatch(part, pattern):
                    return True
                
        return False
        
    def _generate_file_id(self, relative_path: str) -> str:
        """Generate a consistent file ID from the relative path.
        
        Args:
            relative_path: Relative path to the file
            
        Returns:
            SHA-256 hash of the path as hex string
        """
        return hashlib.sha256(relative_path.encode()).hexdigest()
        
    def parse_find_line(self, line: str, exclude_hidden: bool = False) -> Optional[Dict[str, Any]]:
        """Parse a line of find -ls output."""
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
                
            # Check skip patterns
            if self._should_skip_path(name):
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
                'id': self._generate_file_id(relative_path),
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
            
    def _process_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Process a batch of file entries."""
        if not batch:
            return
            
        if not self.config.get('database', {}).get('enabled', True):
            return  # Skip database operations if disabled
            
        try:
            if not self.conn:
                self._init_db()
                
            # Convert batch to Arrow table
            arrow_table = pa.Table.from_pylist(batch)
            
            # Register Arrow table with DuckDB
            self.conn.execute("BEGIN TRANSACTION")
            try:
                self.conn.register('arrow_table', arrow_table)
                
                # Insert new records
                self.conn.execute("""
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
                self.conn.execute("COMMIT")
            except Exception as e:
                self.conn.execute("ROLLBACK")
                raise e
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            raise
            
    def _parse_find_output(self, line: str) -> Optional[Dict[str, Any]]:
        """Parse a line of find output into a file entry."""
        try:
            # Split line by tabs
            parts = line.strip().split('\t')
            if len(parts) < 4:
                return None
                
            # Parse path and get components
            abs_path = parts[0]
            name = os.path.basename(abs_path)
            extension = os.path.splitext(name)[1].lower()[1:] if '.' in name else ''
            
            # Handle paths:
            # - relative_path: Full path relative to mount point (for internal use)
            # - filepath: Clean path for external use
            if self.mount_point and abs_path.startswith(self.mount_point):
                relative_path = abs_path[len(self.mount_point):]
                filepath = relative_path
            else:
                relative_path = abs_path
                filepath = abs_path
                
            # Ensure paths start with /
            if not relative_path.startswith('/'):
                relative_path = '/' + relative_path
            if not filepath.startswith('/'):
                filepath = '/' + filepath
            
            # Parse size and times
            size_bytes = int(parts[1])
            modified_time = datetime.fromtimestamp(int(parts[2]))
            creation_time = datetime.fromtimestamp(int(parts[3]))
            
            # Generate unique ID
            id_str = f"{relative_path}:{size_bytes}:{modified_time.timestamp()}"
            file_id = hashlib.sha256(id_str.encode()).hexdigest()
            
            # Return file entry
            return {
                'id': file_id,
                'name': name,
                'relative_path': relative_path,  # Full path relative to mount point
                'filepath': filepath,  # Clean path for external use
                'size_bytes': size_bytes,
                'modified_time': modified_time,
                'creation_time': creation_time,
                'type': 'file',
                'extension': extension,
                'checksum': None,  # Will be calculated later if needed
                'direct_link': None,  # Will be populated by DirectLinkManager
                'last_seen': datetime.now()
            }
            
        except Exception as e:
            logger.error(f"Error parsing find output line: {e}")
            return None

    def reader_thread(self, process: subprocess.Popen, queue: Queue, stop_event: threading.Event):
        """Read lines from find output and put them in the queue."""
        try:
            for line in process.stdout:
                if stop_event.is_set():
                    break
                queue.put(line.strip())
        finally:
            queue.put(None)  # Signal end of data
            
    def scan(self, root_path: str) -> Generator[Dict[str, Any], None, None]:
        """Scan filesystem and yield file entries."""
        # Use parallel scanner if enabled
        if self.parallel_enabled:
            yield from self.parallel_scanner.scan(root_path)
            return
            
        # Fall back to sequential scanning
        exclude_hidden = self.config.get('skip_patterns', {}).get('hidden_files', True)
        exclude_hidden_dirs = self.config.get('skip_patterns', {}).get('hidden_dirs', True)
        skip_patterns = self.config.get('skip_patterns', {}).get('patterns', [])
        
        # Start find command with exclusions
        cmd = ['find', os.path.expanduser(root_path)]
        
        # Add exclusions for hidden files/dirs
        if exclude_hidden or exclude_hidden_dirs:
            cmd.extend(['-not', '-path', '*/.*'])
            
        # Add skip patterns to find command
        for pattern in skip_patterns:
            # Convert glob pattern to find -path pattern
            find_pattern = f"*/{pattern}"  # Add */ prefix to match anywhere in path
            if not find_pattern.startswith('*/'):
                find_pattern = f"*/{find_pattern}"
            cmd.extend(['-not', '-path', find_pattern])
            
        # Add -ls for detailed listing
        cmd.append('-ls')
        
        # Only log at debug level
        logger.debug(f"Running find command: {' '.join(cmd)}")
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,  # Use text mode instead of binary
            bufsize=1,  # Line buffering
            encoding='utf-8'
        )
        
        # Setup queue and events
        queue = Queue(maxsize=10000)
        stop_event = threading.Event()
        
        # Start reader thread
        reader = threading.Thread(
            target=self.reader_thread,
            args=(process, queue, stop_event)
        )
        reader.start()
        
        # Process entries
        batch = []
        try:
            while True:
                try:
                    line = queue.get(timeout=1)
                    if line is None:  # End of data
                        break
                        
                    entry = self.parse_find_line(line, exclude_hidden)
                    if entry:
                        batch.append(entry)
                        
                        # Process batch if needed
                        if len(batch) >= self.batch_size:
                            try:
                                self._process_batch(batch)
                                yield from batch
                            except Exception as e:
                                logger.error(f"Error processing batch: {e}")
                            batch = []
                            
                except Empty:
                    if process.poll() is not None:  # Process finished
                        break
                        
        finally:
            stop_event.set()
            reader.join()
            process.terminate()
            
            # Process remaining entries
            if batch:
                try:
                    self._process_batch(batch)
                    yield from batch
                except Exception as e:
                    logger.error(f"Error processing final batch: {e}")
                
    def cleanup_missing_files(self, current_files: set) -> int:
        """Clean up files that no longer exist from the database.
        
        Args:
            current_files: Set of current file paths
            
        Returns:
            Number of files removed
        """
        if not self.conn:
            return 0
            
        try:
            # Create temp table for current files
            self.conn.execute("""
                CREATE TEMP TABLE current_files (
                    relative_path VARCHAR PRIMARY KEY
                )
            """)
            
            # Insert current files in batches using VALUES
            batch_size = 1000
            current_files_list = list(current_files)
            for i in range(0, len(current_files_list), batch_size):
                batch = current_files_list[i:i + batch_size]
                # Build VALUES clause manually
                values = ",".join(f"(?)" for _ in batch)
                self.conn.execute(
                    f"INSERT INTO current_files (relative_path) VALUES {values}",
                    batch
                )
            
            # Delete files that don't exist in current_files
            removed = self.conn.execute("""
                WITH removed AS (
                    DELETE FROM files 
                    WHERE relative_path NOT IN (SELECT relative_path FROM current_files)
                    RETURNING id
                )
                SELECT count(*) FROM removed
            """).fetchone()[0]
            
            # Cleanup
            self.conn.execute("DROP TABLE current_files")
            
            return removed
            
        except Exception as e:
            logger.error(f"Error cleaning up missing files: {e}")
            return 0
            
    def get_all_file_paths(self) -> list:
        """Get all file paths from the database.
        
        Returns:
            List of relative file paths
        """
        try:
            result = self.conn.execute("""
                SELECT relative_path 
                FROM files
            """).fetchall()
            return [row[0] for row in result]
            
        except Exception as e:
            logger.error(f"Error getting file paths: {e}")
            return []
            
    def get_file_info(self, relative_path: str) -> Optional[Dict[str, Any]]:
        """Get file information for a specific path.
        
        Args:
            relative_path: Relative path to the file
            
        Returns:
            Dictionary with file information or None if not found
        """
        try:
            result = self.conn.execute("""
                SELECT name, relative_path, filepath, size_bytes, modified_time, 
                       creation_time, type, extension, checksum, direct_link, last_seen
                FROM files
                WHERE relative_path = ?
            """, [relative_path]).fetchone()
            
            if result:
                return {
                    'name': result[0],
                    'relative_path': result[1],
                    'filepath': result[2],
                    'size_bytes': result[3],
                    'modified_time': result[4],
                    'creation_time': result[5],
                    'type': result[6],
                    'extension': result[7],
                    'checksum': result[8],
                    'direct_link': result[9],
                    'last_seen': result[10]
                }
            return None
            
        except Exception as e:
            logger.error(f"Error getting file info: {e}")
            return None
            
    def get_removed_file_ids(self) -> List[str]:
        """Get IDs of files that were removed in the last cleanup.
        
        Returns:
            List of file IDs
        """
        try:
            result = self.conn.execute("""
                SELECT id FROM files 
                WHERE last_seen < (
                    SELECT MAX(last_seen) FROM files
                )
            """).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error getting removed file IDs: {e}")
            return []
