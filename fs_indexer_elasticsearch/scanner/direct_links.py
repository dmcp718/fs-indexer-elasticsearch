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
        self.version = config.get('lucidlink_filespace', {}).get('lucidlink_version', 3)
        
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
        
        # Drop existing table and indexes
        self.conn.execute("DROP TABLE IF EXISTS direct_links")
        
        # Create tables if they don't exist
        self.conn.execute("""
            CREATE TABLE direct_links (
                file_id VARCHAR PRIMARY KEY,
                direct_link VARCHAR,
                link_type VARCHAR,  -- v2 or v3
                fsentry_id VARCHAR NULL,  -- For v2 caching
                last_updated TIMESTAMP
            )
        """)
        
        # Create indexes for better performance
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_last_updated ON direct_links(last_updated)")
        
        # Enable optimizations
        self.conn.execute("SET enable_progress_bar=false")
        self.conn.execute("SET temp_directory='data'")
        self.conn.execute("SET memory_limit='4GB'")
        self.conn.execute("PRAGMA threads=4")  # API bound, so fewer threads
        
        return self.conn
        
    async def process_batch(self, items: List[Dict[str, Any]]):
        """Process a batch of files and directories to get their direct links.
        
        The bulk insert flow:
        1. Get direct links for each file using LucidLink API
        2. Convert results to Arrow table for efficient data transfer
        3. Perform upsert operation in a transaction
        """
        if not items:
            return

        # Log only for first batch or if batch size changes
        if not hasattr(self, '_last_batch_size') or self._last_batch_size != len(items):
            logger.info(f"Processing direct links in batches of {len(items)}...")
            self._last_batch_size = len(items)

        try:
            results = []
            for item in items:
                try:
                    # Log item type for debugging
                    logger.debug(f"Processing direct link for {item['type']}: {item['filepath']}")
                    
                    # Get direct link based on version
                    if self.version == 3:
                        # V3: Simple direct API call
                        direct_link = await self.lucidlink_api.get_direct_link_v3(item['filepath'])
                        if direct_link:
                            logger.debug(f"Generated direct link for {item['type']}: {item['filepath']}")
                            results.append({
                                'file_id': item['id'],
                                'direct_link': direct_link,
                                'link_type': 'v3',
                                'fsentry_id': None,  # Not used in v3
                                'last_updated': datetime.now()
                            })
                            # Update the item object for Elasticsearch
                            item['direct_link'] = direct_link
                        else:
                            logger.warning(f"Failed to generate direct link for {item['type']}: {item['filepath']}")
                    else:
                        # V2: Try fast path first using cached fsentry_id
                        fsentry_id = None
                        if 'fsentry_id' in item:
                            fsentry_id = item['fsentry_id']
                        else:
                            # Check cache in direct_links table
                            cache_result = self.conn.execute("""
                                SELECT fsentry_id 
                                FROM direct_links 
                                WHERE file_id = ? 
                                AND last_updated > CURRENT_TIMESTAMP - INTERVAL 1 HOUR
                            """, [item['id']]).fetchone()
                            if cache_result:
                                fsentry_id = cache_result[0]
                        
                        # Get direct link (will use fsentry_id if available)
                        direct_link = await self.lucidlink_api.get_direct_link_v2(
                            item['filepath'], 
                            fsentry_id=fsentry_id
                        )
                        if direct_link:
                            logger.debug(f"Generated direct link for {item['type']}: {item['filepath']}")
                            results.append({
                                'file_id': item['id'],
                                'direct_link': direct_link,
                                'link_type': 'v2',
                                'fsentry_id': fsentry_id,
                                'last_updated': datetime.now()
                            })
                            # Update the item object for Elasticsearch
                            item['direct_link'] = direct_link
                        else:
                            logger.warning(f"Failed to generate direct link for {item['type']}: {item['filepath']}")
                except Exception as e:
                    logger.error(f"Error generating direct link for {item['filepath']}: {e}")
                    
            if results:
                # Convert to Arrow table for efficient bulk insert
                arrow_table = pa.Table.from_pylist(results)
                
                # Perform upsert in transaction
                self.conn.execute("BEGIN TRANSACTION")
                try:
                    self.conn.register('arrow_table', arrow_table)
                    self.conn.execute("""
                        INSERT OR REPLACE INTO direct_links 
                        SELECT * FROM arrow_table
                    """)
                    self.conn.execute("COMMIT")
                    logger.info(f"Generated {len(results)} direct links")
                except Exception as e:
                    self.conn.execute("ROLLBACK")
                    logger.error(f"Error updating direct links database: {e}")
                    raise
        except Exception as e:
            logger.error(f"Error processing direct links batch: {e}")
            
    async def update_direct_links(self, files_db_path: str):
        """Update direct links for files and directories in the main database.
        
        Args:
            files_db_path: Path to the files database
        """
        logger.info("Starting direct link update process...")
        try:
            # Attach files database
            self.conn.execute(f"ATTACH '{files_db_path}' AS files")
            
            # Get files and directories that need direct link updates
            query = """
                SELECT f.* 
                FROM files.files f
                LEFT JOIN direct_links dl ON f.id = dl.file_id
                WHERE 
                    dl.file_id IS NULL 
                    OR dl.last_updated < f.modified_time
                ORDER BY f.modified_time DESC  -- Process newest items first
            """
            
            # Use Arrow for efficient data handling
            result = self.conn.execute(query)
            items = result.fetch_arrow_table()
            records = items.to_pylist()
            
            # Log number of items by type
            type_counts = {}
            for record in records:
                type_counts[record['type']] = type_counts.get(record['type'], 0) + 1
            logger.debug(f"Found items to process by type: {type_counts}")
            
            # Process in batches
            total_items = len(records)
            processed = 0
            last_progress_time = time.time()
            start_time = time.time()
            
            for i in range(0, total_items, self.batch_size):
                batch = records[i:i + self.batch_size]
                await self.process_batch(batch)
                
                # Update progress
                processed += len(batch)
                current_time = time.time()
                if current_time - last_progress_time >= 5:
                    elapsed = current_time - start_time
                    rate = processed / elapsed if elapsed > 0 else 0
                    logger.debug(f"Processed {processed}/{total_items} direct links - {rate:.1f} items/s")
                    last_progress_time = current_time
            
            duration = time.time() - start_time
            logger.info(f"Direct link update completed in {duration:.2f}s - {processed} items")
            
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

    def get_direct_link(self, file_id: str) -> str:
        """Get direct link for a file by its ID.
        
        Args:
            file_id: File ID to get direct link for
            
        Returns:
            Direct link string or empty string if not found
        """
        try:
            result = self.conn.execute("""
                SELECT direct_link
                FROM direct_links
                WHERE file_id = ?
            """, [file_id]).fetchone()
            
            return result[0] if result else ''
        except Exception as e:
            logger.error(f"Error getting direct link for file {file_id}: {e}")
            return ''
