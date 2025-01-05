#!/usr/bin/env python3

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
import duckdb
import pyarrow as pa
from ..lucidlink.lucidlink_api import LucidLinkAPI

logger = logging.getLogger(__name__)

class DirectLinkManager:
    """Manages direct links in a separate database."""
    
    def __init__(self, config: Dict[str, Any], lucidlink_api: LucidLinkAPI):
        """Initialize with configuration and LucidLink API client."""
        self.config = config
        self.lucidlink_api = lucidlink_api
        self.batch_size = config.get('performance', {}).get('direct_link_batch_size', 1000)
        self.db_path = self._get_db_path()
        self.conn = None
        
    def _get_db_path(self) -> str:
        """Get database path based on filespace name."""
        filespace = self.config.get('lucidlink_filespace', {}).get('name', 'default')
        return f"data/{filespace}_directlinks.duckdb"
        
    def setup_database(self):
        """Setup direct links database with optimizations."""
        # Ensure data directory exists
        Path('data').mkdir(exist_ok=True)
        
        # Connect to database
        self.conn = duckdb.connect(self.db_path)
        
        # Create tables if they don't exist
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS direct_links (
                file_id VARCHAR PRIMARY KEY,
                direct_link VARCHAR,
                link_type VARCHAR,  -- v2 or v3
                fsentry_id VARCHAR,  -- For v2 caching
                last_updated TIMESTAMP
            )
        """)
        
        # Create indexes for better performance
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_last_updated ON direct_links(last_updated)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_fsentry_id ON direct_links(fsentry_id)")
        
        # Enable optimizations
        self.conn.execute("SET enable_progress_bar=false")
        self.conn.execute("SET temp_directory='data'")
        self.conn.execute("SET memory_limit='4GB'")
        self.conn.execute("PRAGMA threads=4")  # API bound, so fewer threads
        
        return self.conn
        
    async def process_batch(self, files: List[Dict[str, Any]]):
        """Process a batch of files to get their direct links.
        
        The bulk insert flow:
        1. Get direct links for each file using LucidLink API
        2. Convert results to Arrow table for efficient data transfer
        3. Perform upsert operation in a transaction
        """
        try:
            results = []
            for file in files:
                if file['type'] != 'file':
                    continue
                    
                # Get direct link based on version
                if self.lucidlink_api.version == 3:
                    # V3: Simple direct API call
                    direct_link = await self.lucidlink_api.get_direct_link_v3(file['relative_path'])
                    if direct_link:
                        results.append({
                            'file_id': file['id'],
                            'direct_link': direct_link,
                            'link_type': 'v3',
                            'fsentry_id': None,  # Not used in v3
                            'last_updated': datetime.now()
                        })
                else:
                    # V2: Try fast path first using cached fsentry_id
                    fsentry_id = None
                    if 'fsentry_id' in file:
                        fsentry_id = file['fsentry_id']
                    else:
                        # Check cache in direct_links table
                        cache_result = self.conn.execute("""
                            SELECT fsentry_id 
                            FROM direct_links 
                            WHERE file_id = ? 
                            AND last_updated > CURRENT_TIMESTAMP - INTERVAL 1 HOUR
                        """, [file['id']]).fetchone()
                        if cache_result:
                            fsentry_id = cache_result[0]
                    
                    # Get direct link (will use fsentry_id if available)
                    direct_link = await self.lucidlink_api.get_direct_link_v2(
                        file['relative_path'], 
                        fsentry_id=fsentry_id
                    )
                    
                    if direct_link:
                        # Extract fsentry_id from link for caching
                        new_fsentry_id = direct_link.split('/')[-1]
                        results.append({
                            'file_id': file['id'],
                            'direct_link': direct_link,
                            'link_type': 'v2',
                            'fsentry_id': new_fsentry_id,
                            'last_updated': datetime.now()
                        })
                    
            if results:
                # Convert to Arrow table for efficient bulk insert
                arrow_table = pa.Table.from_pylist(results)
                
                # Perform upsert in transaction
                self.conn.execute("BEGIN TRANSACTION")
                try:
                    self.conn.register('arrow_table', arrow_table)
                    self.conn.execute("""
                        INSERT INTO direct_links 
                        SELECT * FROM arrow_table
                        ON CONFLICT(file_id) DO UPDATE SET
                            direct_link = excluded.direct_link,
                            link_type = excluded.link_type,
                            fsentry_id = excluded.fsentry_id,
                            last_updated = excluded.last_updated
                    """)
                    self.conn.execute("COMMIT")
                except Exception as e:
                    self.conn.execute("ROLLBACK")
                    raise e
                
        except Exception as e:
            logger.error(f"Error processing direct links batch: {e}")
            
    async def update_direct_links(self, files_db_path: str):
        """Update direct links for files in the main database.
        
        Args:
            files_db_path: Path to the files database
        """
        try:
            # Attach files database
            self.conn.execute(f"ATTACH '{files_db_path}' AS files")
            
            # Get files that need direct link updates
            query = """
                SELECT f.* 
                FROM files.files f
                LEFT JOIN direct_links dl ON f.id = dl.file_id
                WHERE 
                    f.type = 'file'
                    AND (
                        dl.file_id IS NULL 
                        OR dl.last_updated < f.modified_time
                    )
                ORDER BY f.modified_time DESC  -- Process newest files first
            """
            
            # Use Arrow for efficient data handling
            result = self.conn.execute(query)
            files = result.fetch_arrow_table()
            records = files.to_pylist()
            
            # Process in batches
            total_files = len(records)
            processed = 0
            last_progress_time = time.time()
            start_time = time.time()
            
            for i in range(0, total_files, self.batch_size):
                batch = records[i:i + self.batch_size]
                await self.process_batch(batch)
                
                # Update progress
                processed += len(batch)
                current_time = time.time()
                if current_time - last_progress_time >= 5:
                    elapsed = current_time - start_time
                    rate = processed / elapsed if elapsed > 0 else 0
                    logger.info(f"Processed {processed}/{total_files} direct links - {rate:.1f} files/s")
                    last_progress_time = current_time
            
            duration = time.time() - start_time
            logger.info(f"Direct link update completed in {duration:.2f}s - {processed} files")
            
        finally:
            # Detach files database
            self.conn.execute("DETACH files")
            
    def combine_with_files(self, files_conn: duckdb.DuckDBPyConnection) -> pa.Table:
        """Combine file and direct link data for export.
        
        Args:
            files_conn: Connection to the files database
            
        Returns:
            Arrow table with combined data
        """
        query = """
            SELECT 
                f.*,
                dl.direct_link,
                dl.link_type
            FROM 
                files f
            LEFT JOIN 
                direct_links dl ON f.id = dl.file_id
            ORDER BY 
                f.relative_path
        """
        return files_conn.execute(query).fetch_arrow_table()
