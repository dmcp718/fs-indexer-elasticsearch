#!/usr/bin/env python3

import logging
import subprocess
import threading
from datetime import datetime
from pathlib import Path
from queue import Queue, Empty
from typing import Dict, Any, Optional, Generator
import duckdb
import pyarrow as pa
import hashlib

logger = logging.getLogger(__name__)

class FileScanner:
    """Optimized file scanner using find command and DuckDB."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize scanner with configuration."""
        self.config = config
        self.batch_size = config.get('performance', {}).get('batch_size', 100000)
        self.queue_size = config.get('performance', {}).get('queue_size', 10000)
        self.db_path = self._get_db_path()
        self.conn = None
        
    def _get_db_path(self) -> str:
        """Get database path based on filespace name and mode."""
        mode = self.config.get('mode', 'index-only')
        if mode == 'lucidlink':
            filespace = self.config.get('lucidlink_filespace', {}).get('name', 'default')
            return f"data/{filespace}_index.duckdb"
        return "data/fs_index.duckdb"
        
    def setup_database(self):
        """Setup DuckDB database with optimizations."""
        # Ensure data directory exists
        Path('data').mkdir(exist_ok=True)
        
        # Connect to database
        self.conn = duckdb.connect(self.db_path)
        
        # Create tables if they don't exist
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id VARCHAR PRIMARY KEY,
                name VARCHAR,
                relative_path VARCHAR,
                size_bytes BIGINT,
                modified_time TIMESTAMP,
                creation_time TIMESTAMP,
                type VARCHAR,
                extension VARCHAR,
                checksum VARCHAR,
                last_seen TIMESTAMP
            )
        """)
        
        # Create indexes for better performance
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_relative_path ON files(relative_path)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_type_name ON files(type, name)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_last_seen ON files(last_seen)")
        
        # Enable optimizations
        self.conn.execute("SET enable_progress_bar=false")
        self.conn.execute("SET temp_directory='data'")
        self.conn.execute("SET memory_limit='4GB'")
        self.conn.execute("PRAGMA threads=8")
        
        return self.conn
        
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
        skip_extensions = skip_patterns.get('extensions', [])
        skip_directories = skip_patterns.get('directories', [])
        
        # Check if path is in skip directories
        for dir_pattern in skip_directories:
            if dir_pattern in path.split('/'):
                return True
        
        # Check file extension
        if path_obj.suffix:
            # Handle both formats: with and without dot
            extension = path_obj.suffix  # With dot
            extension_no_dot = path_obj.suffix[1:]  # Without dot
            
            for pattern in skip_extensions:
                # Handle glob patterns
                if pattern.startswith('*.'):
                    pattern = pattern[2:]  # Remove *. from pattern
                    if extension_no_dot == pattern:
                        return True
                # Handle direct matches (with or without dot)
                elif extension == pattern or extension_no_dot == pattern:
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
                
            return {
                'id': self._generate_file_id(name),
                'name': Path(name).name,
                'relative_path': name,
                'size_bytes': size,
                'modified_time': timestamp,
                'creation_time': timestamp,  # Use modified time as creation time if not available
                'type': entry_type,
                'extension': extension,
                'last_seen': datetime.now()
            }
            
        except Exception as e:
            logger.error(f"Error parsing find output line: {e}")
            return None
            
    def process_batch(self, entries: list):
        """Process a batch of entries into the database.
        
        The bulk insert flow:
        1. Convert batch of entries to Arrow table for efficient data transfer
        2. Register Arrow table with DuckDB
        3. Perform upsert operation:
           - For new files: Insert them
           - For existing files: Only update if the file was modified
           - Use a transaction for atomicity
        """
        if not entries:
            return
            
        try:
            # Convert to Arrow table for efficient bulk insert
            arrow_table = pa.Table.from_pylist(entries)
            
            # Register Arrow table with DuckDB
            self.conn.execute("BEGIN TRANSACTION")
            try:
                self.conn.register('arrow_table', arrow_table)
                
                # Bulk upsert using DuckDB
                # 1. Insert new records
                # 2. Update existing records only if modified
                self.conn.execute("""
                    INSERT INTO files 
                    SELECT * FROM arrow_table
                    WHERE NOT EXISTS (
                        SELECT 1 FROM files 
                        WHERE files.relative_path = arrow_table.relative_path
                        AND files.modified_time >= arrow_table.modified_time
                    )
                    ON CONFLICT(id) DO UPDATE SET
                        name = excluded.name,
                        relative_path = excluded.relative_path,
                        size_bytes = excluded.size_bytes,
                        modified_time = excluded.modified_time,
                        creation_time = excluded.creation_time,
                        type = excluded.type,
                        extension = excluded.extension,
                        checksum = excluded.checksum,
                        last_seen = excluded.last_seen
                    WHERE excluded.modified_time > files.modified_time
                """)
                self.conn.execute("COMMIT")
            except Exception as e:
                self.conn.execute("ROLLBACK")
                raise e
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            
    def reader_thread(self, process: subprocess.Popen, queue: Queue, stop_event: threading.Event):
        """Read lines from find output and put them in the queue."""
        try:
            for line in process.stdout:
                if stop_event.is_set():
                    break
                queue.put(line.decode().strip())
        finally:
            queue.put(None)  # Signal end of data
            
    def scan(self, root_path: str) -> Generator[Dict[str, Any], None, None]:
        """Scan filesystem and yield file entries."""
        exclude_hidden = self.config.get('skip_patterns', {}).get('hidden_files', True)
        
        # Start find command
        cmd = [
            'find', root_path,
            '-ls'  # Get detailed listing
        ]
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=1,
            universal_newlines=False
        )
        
        # Setup queue and events
        queue = Queue(maxsize=self.queue_size)
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
                        
                        if len(batch) >= self.batch_size:
                            self.process_batch(batch)
                            yield from batch
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
                self.process_batch(batch)
                yield from batch
                
    def cleanup_missing_files(self, current_files: set) -> int:
        """Clean up files that no longer exist from the database.
        
        Args:
            current_files: Set of current file paths
            
        Returns:
            Number of files removed
        """
        try:
            # Mark files as removed that are not in current_files
            self.conn.execute("""
                DELETE FROM files 
                WHERE relative_path NOT IN (
                    SELECT unnest(?::VARCHAR[])
                )
            """, [list(current_files)])
            
            count = self.conn.execute("SELECT changes()").fetchone()[0]
            logger.info(f"Removed {count} missing files from database")
            return count
            
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
                SELECT name, relative_path, size_bytes, modified_time, 
                       creation_time, type, extension, checksum, last_seen
                FROM files
                WHERE relative_path = ?
            """, [relative_path]).fetchone()
            
            if result:
                return {
                    'name': result[0],
                    'relative_path': result[1],
                    'size_bytes': result[2],
                    'modified_time': result[3],
                    'creation_time': result[4],
                    'type': result[5],
                    'extension': result[6],
                    'checksum': result[7],
                    'last_seen': result[8]
                }
            return None
            
        except Exception as e:
            logger.error(f"Error getting file info: {e}")
            return None
