#!/usr/bin/env python3

import logging
import time
from datetime import datetime
from typing import Dict, Any, List
import duckdb
import pyarrow as pa
import os
import fnmatch
from ..lucidlink.lucidlink_api import LucidLinkAPI
from ..database.db_duckdb import init_database, close_database

logger = logging.getLogger(__name__)

class DirectLinkManager:
    """Manages direct links in a separate database."""
    
    def __init__(self, config: Dict[str, Any], db_path: str, lucidlink_api: LucidLinkAPI):
        """Initialize with configuration, database path, and LucidLink API client."""
        self.config = config
        self.db_path = db_path
        self.lucidlink_api = lucidlink_api
        self.version = config.get('lucidlink_filespace', {}).get('lucidlink_version', 3)
        
        # Get batch size from performance settings
        perf_settings = config.get('performance', {}).get('batch_sizes', {})
        self.batch_size = perf_settings.get('direct_links', 50000)
        
        # Single persistent connection
        self.conn = None
        
    def setup_database(self):
        """Setup the DuckDB database and create necessary tables."""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            # Initialize database with parent config
            self.conn = init_database(f'duckdb:///{self.db_path}', self.config)
            
            # Create tables
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS direct_links (
                    file_id VARCHAR PRIMARY KEY,
                    direct_link VARCHAR,
                    link_type VARCHAR,  -- v2 or v3
                    fsentry_id VARCHAR NULL,  -- For v2 caching
                    last_updated TIMESTAMP
                )
            """)
        except Exception as e:
            logger.error(f"Error setting up direct links database: {str(e)}")
            raise
            
    def _reconnect_database(self):
        """Reconnect to the database after a fatal error."""
        try:
            if self.conn:
                close_database(self.conn, self.config)
            self.setup_database()
        except Exception as err:
            logger.error(f"Error reconnecting to database: {str(err)}")
            raise
            
    async def cleanup(self):
        """Clean up database connection when done."""
        if self.conn:
            try:
                close_database(self.conn, self.config)
                self.conn = None
            except Exception as e:
                logger.error(f"Error closing database connection: {str(e)}")
                raise

    async def process_batch(self, items: List[Dict[str, Any]]):
        """Process a batch of files and directories to get their direct links.
        
        The bulk insert flow:
        1. Get direct links for each file using LucidLink API
        2. Convert results to Arrow table for efficient data transfer
        3. Perform upsert operation in a transaction
        """
        if not items:
            return
            
        logger.info(f"Processing direct links in batches of {len(items)}...")
        
        try:
            results = []
            # Get skip patterns
            skip_patterns = self.config.get('skip_patterns', {}).get('patterns', [])
            
            # Process items in smaller sub-batches to avoid overwhelming the API
            for i in range(0, len(items), self.batch_size):
                batch = items[i:i + self.batch_size]
                for item in batch:
                    try:
                        # Validate required fields
                        if not item.get('id') or not item.get('filepath'):
                            logger.warning(f"Skipping direct link generation for item missing required fields: {item}")
                            continue
                            
                        # Skip if path matches skip patterns
                        if any(fnmatch.fnmatch(item.get('filepath', ''), pattern) for pattern in skip_patterns):
                            logger.debug(f"Skipping direct link generation for {item.get('filepath', '')} due to skip pattern")
                            continue
                            
                        # Generate direct link based on version
                        if self.version == 2:
                            # Check cache in direct_links table
                            fsentry_id = None
                            try:
                                if 'fsentry_id' in item:
                                    fsentry_id = item['fsentry_id']
                                else:
                                    cache_result = self.conn.execute("""
                                        SELECT fsentry_id 
                                        FROM direct_links 
                                        WHERE file_id = ? 
                                        AND last_updated > CURRENT_TIMESTAMP - INTERVAL 1 HOUR
                                    """, [item['id']]).fetchone()
                                    if cache_result:
                                        fsentry_id = cache_result[0]
                            except duckdb.duckdb.ConnectionException:
                                self._reconnect_database()
                                continue  # Skip this item and retry in next batch
                                    
                            direct_link, fsentry_id = await self.lucidlink_api.get_direct_link_v2(
                                item['filepath'],
                                fsentry_id=fsentry_id
                            )
                        else:
                            direct_link = await self.lucidlink_api.get_direct_link_v3(item['filepath'])
                            fsentry_id = None
                            
                        if direct_link:
                            results.append({
                                'file_id': item['id'],
                                'direct_link': direct_link,
                                'link_type': f'v{self.version}',
                                'fsentry_id': fsentry_id,
                                'last_updated': datetime.now()
                            })
                            item['direct_link'] = direct_link
                        else:
                            logger.warning(f"Failed to generate direct link for {item['type']}: {item.get('filepath', '')}")
                            
                    except Exception as err:
                        logger.error(f"Error generating direct link for {item.get('filepath', '')}: {str(err)}")
                        if "400" in str(err):  # Skip 400 errors as they're likely for unsupported files
                            logger.warning(f"Failed to generate direct link for {'directory' if item.get('is_dir', False) else 'file'}: {item.get('filepath', '')}")
                            continue
                        raise
                        
            if results:
                max_retries = 3
                retry_count = 0
                while retry_count < max_retries:
                    try:
                        # Convert to Arrow table for efficient bulk insert
                        arrow_table = pa.Table.from_pylist(results)
                        
                        # Begin transaction
                        self.conn.execute("BEGIN TRANSACTION")
                        
                        try:
                            self.conn.register('arrow_table', arrow_table)
                            self.conn.execute("""
                                INSERT OR REPLACE INTO direct_links 
                                SELECT * FROM arrow_table
                            """)
                            self.conn.execute("COMMIT")
                            logger.info(f"Generated {len(results)} direct links")
                            break  # Success, exit retry loop
                        except Exception:
                            self.conn.execute("ROLLBACK")
                            raise
                            
                    except duckdb.duckdb.ConnectionException:
                        retry_count += 1
                        if retry_count >= max_retries:
                            raise
                        logger.warning(f"Database connection lost, retrying ({retry_count}/{max_retries})...")
                        self._reconnect_database()
                    except Exception as err:
                        logger.error(f"Error processing direct links batch: {str(err)}")
                        raise
                        
        except Exception as err:
            logger.error(f"Error processing direct links batch: {str(err)}")
            raise

    async def update_direct_links(self, files_db_path: str):
        """Update direct links for files and directories in the main database.
        
        Args:
            files_db_path: Path to the files database
        """
        logger.info("Starting direct link update process...")
        try:
            # Attach files database
            self.conn.execute(f"ATTACH '{files_db_path}' AS files")
            
            # Get total count first
            count_query = """
                SELECT COUNT(*) as total
                FROM files.files f
                LEFT JOIN direct_links dl ON f.id = dl.file_id
                WHERE 
                    dl.file_id IS NULL 
                    OR dl.last_updated < f.modified_time
            """
            total_items = self.conn.execute(count_query).fetchone()[0]
            
            # Process in batches using OFFSET/LIMIT
            processed = 0
            last_progress_time = time.time()
            start_time = time.time()
            
            while processed < total_items:
                # Get next batch
                query = f"""
                    SELECT f.* 
                    FROM files.files f
                    LEFT JOIN direct_links dl ON f.id = dl.file_id
                    WHERE 
                        dl.file_id IS NULL 
                        OR dl.last_updated < f.modified_time
                    ORDER BY f.modified_time DESC  -- Process newest items first
                    LIMIT {self.batch_size}
                    OFFSET {processed}
                """
                
                # Use Arrow for efficient data handling
                result = self.conn.execute(query)
                items = result.fetch_arrow_table()
                records = items.to_pylist()
                
                if not records:
                    break
                    
                # Process batch
                await self.process_batch(records)
                
                # Update progress
                processed += len(records)
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

    async def combine_with_files(self, files_conn: duckdb.DuckDBPyConnection) -> pa.Table:
        """Combine file and direct link data for export.
        
        Args:
            files_conn: Connection to files database
            
        Returns:
            Arrow table with combined data
        """
        try:
            # Attach files database
            files_conn.execute(f"ATTACH '{self.db_path}' AS direct_links")
            
            try:
                # Join files and direct links
                result = files_conn.execute("""
                    SELECT f.*, dl.direct_link
                    FROM files f
                    LEFT JOIN direct_links.direct_links dl ON f.id = dl.file_id
                """)
                return result.fetch_arrow_table()
            finally:
                files_conn.execute("DETACH direct_links")
        except Exception as err:
            logger.error(f"Error combining files and direct links: {str(err)}")
            raise

    async def get_direct_link(self, file_id: str) -> str:
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
        except Exception as err:
            logger.error(f"Error getting direct link for file {file_id}: {str(err)}")
            return ''
