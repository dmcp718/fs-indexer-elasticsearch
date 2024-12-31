#!/usr/bin/env python3

import os
import sys
import time
import yaml
import logging
import logging.handlers
import argparse
import asyncio
import requests
import pytz
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Event
from queue import Queue, Empty
import uuid
import duckdb
import pandas as pd
from elasticsearch import helpers
import signal

from .db_duckdb import (
    init_database,
    bulk_upsert_files,
    cleanup_missing_files,
)
from .elasticsearch_integration import ElasticsearchClient
from .lucidlink_api import LucidLinkAPI
from .filespace_prompt import get_filespace_info

# Initialize basic logging first
logger = logging.getLogger(__name__)

# Define UUID namespace for consistent IDs
NAMESPACE_DNS = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')

# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    if not shutdown_requested:
        logger.info("\nShutdown requested, completing current batch...")
        shutdown_requested = True
    else:
        logger.info("\nForce shutdown requested, exiting immediately...")
        sys.exit(1)

# Load configuration
def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration from file."""
    if not config_path:
        # Try locations in order:
        config_locations = [
            'config/indexer-config.yaml',  # Project config directory
            'indexer-config.yaml',         # Current directory
            os.path.join(os.path.dirname(__file__), 'indexer-config.yaml')  # Package directory
        ]
        
        for loc in config_locations:
            if os.path.exists(loc):
                config_path = loc
                break
    
    if not config_path or not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found in any of the expected locations: {config_locations}")
    
    logger.info(f"Loading configuration from: {config_path}")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
        
    # Log the loaded configuration
    logger.info(f"Loaded configuration: {config}")
    
    # Set default values
    config.setdefault('database', {})
    config['database'].setdefault('connection', {})
    config['database']['connection'].setdefault('url', 'duckdb:///fs_index.duckdb')
    config['database']['connection'].setdefault('options', {})
    
    # Create data directory if needed
    db_path = config['database']['connection']['url'].replace('duckdb:///', '')
    if db_path and os.path.dirname(db_path):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    return config

# Configure logging
def configure_logging(config: Dict[str, Any]) -> None:
    """Configure logging with both file and console handlers."""
    global logger
    
    # Remove any existing handlers from the root logger
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    logger = logging.getLogger(__name__)
    logger.handlers.clear()  # Clear any existing handlers
    logger.setLevel(getattr(logging, config["logging"]["level"]))
    logger.propagate = False  # Prevent propagation to avoid duplicate logs
    
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(config["logging"]["file"])
    os.makedirs(log_dir, exist_ok=True)
    
    # Add file handler
    log_file = config["logging"]["file"]
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=config["logging"]["max_size_mb"] * 1024 * 1024,  # Convert MB to bytes
        backupCount=config["logging"]["backup_count"],
    )
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    
    # Add console handler if enabled
    if config["logging"].get("console", True):
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(console_handler)

# Constants
def get_constants(config: Dict[str, Any]) -> Dict[str, Any]:
    """Get configuration constants."""
    return {
        "BATCH_SIZE": config["performance"]["batch_size"],
        "READ_BUFFER_SIZE": config["read_buffer_size"]
    }

class WorkflowStats:
    """Track workflow statistics."""
    def __init__(self):
        self.start_time = time.time()
        self.end_time = None
        self.total_files = 0
        self.total_dirs = 0  # Track number of directories
        self.files_updated = 0
        self.files_skipped = 0
        self.files_removed = 0
        self.removed_paths = []
        self.errors = []
        self.total_size = 0  # Track total size in bytes
        self.lock = Lock()  # Add lock for thread safety
        self._batch_size = 1000
        self._batch = {
            'files': 0,
            'dirs': 0,
            'updated': 0,
            'skipped': 0,
            'size': 0
        }
        
    def _flush_batch(self):
        """Flush accumulated stats to main counters."""
        if any(self._batch.values()):
            with self.lock:
                self.total_files += self._batch['files']
                self.total_dirs += self._batch['dirs']
                self.files_updated += self._batch['updated']
                self.files_skipped += self._batch['skipped']
                self.total_size += self._batch['size']
            self._batch = {k: 0 for k in self._batch}

    def update_stats(self, file_count=0, dir_count=0, updated=0, skipped=0, size=0):
        """Batch update multiple stats at once."""
        self._batch['files'] += file_count
        self._batch['dirs'] += dir_count
        self._batch['updated'] += updated
        self._batch['skipped'] += skipped
        self._batch['size'] += size
        
        if sum(self._batch.values()) >= self._batch_size:
            self._flush_batch()
            
    def add_error(self, error: str):
        """Add an error to the stats."""
        with self.lock:
            self.errors.append(error)
            
    def add_removed_files(self, count: int, paths: List[str]):
        """Add information about removed files."""
        with self.lock:
            self.files_removed = count
            self.removed_paths = paths
        
    def finish(self):
        """Mark the workflow as finished and set end time."""
        self._flush_batch()  # Ensure all stats are flushed
        self.end_time = time.time()

def should_skip_file(file_info: Dict[str, Any], skip_patterns: Dict[str, List[str]]) -> bool:
    """Check if a file should be skipped based on skip patterns."""
    filename = file_info['name'].split('/')[-1]
    full_path = file_info['name']  # Use full path from API
    
    # Debug logging
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Checking skip patterns for: {full_path} (type: {file_info['type']})")
    
    # Check directory patterns first
    for dir_pattern in skip_patterns.get('directories', []):
        # Remove leading/trailing slashes for consistent matching
        dir_pattern = dir_pattern.strip('/')
        clean_path = full_path.strip('/')
        
        # Skip if path starts with or equals the pattern
        if clean_path.startswith(dir_pattern + '/') or clean_path == dir_pattern:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Skipping {full_path} due to directory pattern {dir_pattern}")
            return True
    
    # Check file extensions
    for ext in skip_patterns.get('extensions', []):
        # Remove leading dot if present in the extension pattern
        ext = ext.lstrip('.')
        if filename.endswith(f".{ext}") or filename == ext:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Skipping {full_path} due to extension pattern {ext}")
            return True
    
    return False

def calculate_checksum(file_path: str, buffer_size: int = 131072) -> str:
    """Calculate xxHash checksum of a file."""
    try:
        if not os.access(file_path, os.R_OK):
            logger.error(f"Permission denied: Unable to access file for checksum calculation: {file_path}")
            raise PermissionError(f"No read access to file: {file_path}")
            
        # hasher = xxhash.xxh64()
        # with open(file_path, "rb") as f:
        #     try:
        #         while True:
        #             data = f.read(buffer_size)
        #             if not data:
        #                 break
        #             hasher.update(data)
        #         return hasher.hexdigest()
        #     except IOError as e:
        #         logger.error(f"Error reading file during checksum calculation: {file_path}: {str(e)}")
        #         raise
    except Exception as e:
        logger.error(f"Failed to calculate checksum for {file_path}: {str(e)}")
        raise

def scan_directory_parallel(root_path: Path, config: Dict[str, Any], max_workers: int = 16) -> Generator[Dict[str, Any], None, None]:
    """Scan directory in parallel for better performance."""
    start_time = time.time()
    scanned_files = 0
    
    def scan_directory(dir_path: Path) -> List[Dict[str, Any]]:
        """Scan a single directory using LucidLink API."""
        files = []
        try:
            # Get directory listing from API
            api_response = requests.get(f"{api_base_url}/list", params={
                'path': str(dir_path.relative_to(root_path))
            }, timeout=30)
            api_response.raise_for_status()
            
            # Process files and directories
            for entry in api_response.json():
                if entry['type'] == 'file' and not should_skip_file(file_info={'name': entry['name'], 'type': 'file'}, skip_patterns=config.get('skip_patterns', {})):
                    files.append({
                        'id': str(uuid.uuid4()),
                        'name': entry['name'],
                        'relative_path': str(Path(dir_path, entry['name']).relative_to(root_path)),
                        'type': 'file',
                        'size': entry['size'],
                        'creation_time': datetime.fromtimestamp(entry['creationTime'] / 1e9, tz=pytz.utc),
                        'update_time': datetime.fromtimestamp(entry['updateTime'] / 1e9, tz=pytz.utc)
                    })
                
        except Exception as e:
            logger.error(f"Error scanning directory {dir_path}: {e}")
        return files
    
    # Get initial directory listing
    try:
        api_response = requests.get(f"{api_base_url}/list", timeout=30)
        api_response.raise_for_status()
        root_entries = api_response.json()
    except Exception as e:
        logger.error(f"Error getting root directory listing: {e}")
        return
    
    # Process root entries
    dirs_to_scan = []
    for entry in root_entries:
        try:
            path = Path(entry['name'])
            if entry['type'] == 'directory' and not should_skip_file(file_info={'name': entry['name'], 'type': 'directory'}, skip_patterns=config.get('skip_patterns', {})):
                dirs_to_scan.append(root_path / path)
            elif entry['type'] == 'file' and not should_skip_file(file_info={'name': entry['name'], 'type': 'file'}, skip_patterns=config.get('skip_patterns', {})):
                scanned_files += 1
                yield {
                    'id': str(uuid.uuid4()),
                    'name': entry['name'],
                    'relative_path': str(path),
                    'type': 'file',
                    'size': entry['size'],
                    'creation_time': datetime.fromtimestamp(entry['creationTime'] / 1e9, tz=pytz.utc),
                    'update_time': datetime.fromtimestamp(entry['updateTime'] / 1e9, tz=pytz.utc)
                }
        except Exception as e:
            logger.error(f"Error processing root entry {entry['name']}: {e}")
    
    # Process directories in parallel chunks
    chunk_size = config["performance"]["scan_chunk_size"]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while dirs_to_scan:
            # Take next chunk of directories
            chunk = dirs_to_scan[:chunk_size]
            dirs_to_scan = dirs_to_scan[chunk_size:]
            
            # Process current chunk
            futures = []
            for dir_path in chunk:
                future = executor.submit(scan_directory, dir_path)
                futures.append(future)
            
            # Get results and subdirectories
            for future in as_completed(futures):
                try:
                    dir_files = future.result()
                    scanned_files += len(dir_files)
                    for file_info in dir_files:
                        yield file_info
                except Exception as e:
                    logger.error(f"Error processing directory chunk: {e}")
            
            # Get subdirectories from processed directories
            for dir_path in chunk:
                try:
                    api_response = requests.get(f"{api_base_url}/list", params={
                        'path': str(dir_path.relative_to(root_path))
                    }, timeout=30)
                    api_response.raise_for_status()
                    
                    for entry in api_response.json():
                        if entry['type'] == 'directory' and not should_skip_file(file_info={'name': entry['name'], 'type': 'directory'}, skip_patterns=config.get('skip_patterns', {})):
                            dirs_to_scan.append(dir_path / entry['name'])
                except Exception as e:
                    logger.error(f"Error getting subdirectories for {dir_path}: {e}")
            
            # Log progress
            scan_duration = time.time() - start_time
            scan_rate = scanned_files / scan_duration if scan_duration > 0 else 0
            if logger.isEnabledFor(logging.INFO):
                logger.info(f"Scanned {scanned_files} files so far ({scan_rate:.1f} files/s)")

def process_files_worker(queue: Queue, stop_event: Event, session: duckdb.DuckDBPyConnection, stats: WorkflowStats, config: Dict[str, Any]):
    """Worker thread to process files from queue."""
    files_chunk = []
    chunk_size = config["performance"]["batch_size"]
    chunk_start_time = time.time()
    
    while not stop_event.is_set() or not queue.empty():
        try:
            file_info = queue.get(timeout=1)  # 1 second timeout
            files_chunk.append(file_info)
            stats.update_stats(size=file_info["size"])
            
            # Process chunk when it reaches batch size
            if len(files_chunk) >= chunk_size:
                try:
                    # Bulk upsert the chunk
                    processed = bulk_upsert_files(session, files_chunk)
                    stats.update_stats(updated=processed)
                    chunk_duration = time.time() - chunk_start_time
                    chunk_rate = processed / chunk_duration if chunk_duration > 0 else 0
                    if logger.isEnabledFor(logging.INFO):
                        logger.info(f"Database batch: {processed} files committed")
                    if logger.isEnabledFor(logging.INFO):
                        logger.info(f"Processed {stats.files_updated}/{stats.total_files} files ({chunk_rate:.1f} files/s in last chunk)")
                    
                    # Reset for next chunk
                    files_chunk = []
                    chunk_start_time = time.time()
                except Exception as e:
                    logger.error(f"Error in batch update: {e}")
                    # Keep retrying with smaller batches
                    if len(files_chunk) > 1000:
                        files_chunk = files_chunk[:1000]
                    else:
                        files_chunk = []
                    chunk_start_time = time.time()
                
        except Empty:
            # No files available, wait for more
            if not stop_event.is_set():
                continue
            break
        except Exception as e:
            logger.error(f"Error processing files: {e}")
            break
    
    # Process remaining files
    if files_chunk:
        try:
            processed = bulk_upsert_files(session, files_chunk)
            stats.update_stats(updated=processed)
            chunk_duration = time.time() - chunk_start_time
            chunk_rate = processed / chunk_duration if chunk_duration > 0 else 0
            if logger.isEnabledFor(logging.INFO):
                logger.info(f"Database batch: {processed} files committed")
            if logger.isEnabledFor(logging.INFO):
                logger.info(f"Processed {stats.files_updated}/{stats.total_files} files ({chunk_rate:.1f} files/s in last chunk)")
        except Exception as e:
            logger.error(f"Error processing final chunk: {e}")

def batch_update_database(session: duckdb.DuckDBPyConnection, files_batch: List[Dict[str, Any]]) -> None:
    """Update database with a batch of files."""
    try:
        processed = bulk_upsert_files(session, files_batch)
        if processed != len(files_batch):
            logger.warning(f"Processed {processed} files out of {len(files_batch)} in batch")
            
    except Exception as e:
        error_msg = f"Error updating database: {e}"
        logger.error(error_msg)
        raise

def process_files(session: duckdb.DuckDBPyConnection, files: List[Dict], stats: WorkflowStats, config: Dict[str, Any]) -> None:
    """Process a list of files and update the database."""
    try:
        total_files = len(files)
        stats.update_stats(file_count=total_files)
        
        # Process files in batches
        total_processed = 0
        start_time = time.time()
        last_progress_time = start_time
        last_progress_count = 0
        
        for i in range(0, len(files), config["performance"]["batch_size"]):
            batch = files[i:i + config["performance"]["batch_size"]]
            processed = bulk_upsert_files(session, batch)
            total_processed += processed
            stats.update_stats(updated=processed)
            
            # Calculate and log processing rate
            current_time = time.time()
            elapsed = current_time - last_progress_time
            files_in_chunk = total_processed - last_progress_count
            rate = files_in_chunk / elapsed if elapsed > 0 else 0
            if logger.isEnabledFor(logging.INFO):
                logger.info(f"Processed {total_processed}/{total_files} files ({rate:.1f} files/s in last chunk)")
            last_progress_time = current_time
            last_progress_count = total_processed
            
    except Exception as e:
        error_msg = f"Critical error in process_files: {str(e)}"
        logger.error(error_msg)
        stats.add_error(error_msg)
        raise

def process_lucidlink_files(session: duckdb.DuckDBPyConnection, stats: WorkflowStats, config: Dict[str, Any], client: ElasticsearchClient) -> None:
    """Process files from LucidLink and index them."""
    try:
        # Initialize LucidLink API
        logger.info("Initializing LucidLink API...")
        logger.info(f"LucidLink config: {config['lucidlink_filespace']}")
        
        # Initialize API with parallel workers
        max_workers = config.get('performance', {}).get('max_workers', 10)
        port = config.get('lucidlink_filespace', {}).get('port', 9778)
        mount_point = config.get('lucidlink_filespace', {}).get('mount_point', '')

        async def process_batch(session: duckdb.DuckDBPyConnection, batch: List[Dict], stats: WorkflowStats, lucidlink_api: LucidLinkAPI) -> int:
            """Process a batch of files by inserting them into DuckDB."""
            try:
                # Process batch
                skip_direct_links = config.get('lucidlink_filespace', {}).get('skip_direct_links', False)
                
                if not skip_direct_links:
                    # Process batch
                    direct_link_tasks = []  # Initialize the list
                    if logger.isEnabledFor(logging.INFO):
                        logger.info(f"Processing batch of {len(batch)} items for direct links")
                    for item in batch:
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"Generating direct link for: {item['relative_path']}")
                        task = lucidlink_api.get_direct_link(item['relative_path'])
                        direct_link_tasks.append(task)
                            
                    # Wait for all direct link generations
                    if direct_link_tasks:
                        if logger.isEnabledFor(logging.INFO):
                            logger.info(f"Waiting for {len(direct_link_tasks)} direct link tasks")
                        direct_links = await asyncio.gather(*direct_link_tasks, return_exceptions=True)
                        
                        # Add direct links to batch items
                        for i, (item, direct_link) in enumerate(zip(batch, direct_links)):
                            if isinstance(direct_link, Exception):
                                logger.error(f"Failed to generate direct link for {item['relative_path']}: {direct_link}")
                                item['direct_link'] = None
                            else:
                                if logger.isEnabledFor(logging.DEBUG):
                                    logger.debug(f"Got direct link for {item['relative_path']}: {direct_link}")
                                item['direct_link'] = direct_link
                
                processed = bulk_upsert_files(session, batch)
                return processed
                
            except Exception as e:
                error_msg = f"Error processing batch: {str(e)}"
                logger.error(error_msg)
                stats.add_error(error_msg)
                raise
                
        async def process_files_async():
            try:
                async with LucidLinkAPI(port=port, mount_point=mount_point, max_workers=max_workers) as lucidlink_api:
                    # Check API health first
                    if not await lucidlink_api.health_check():
                        raise RuntimeError("LucidLink API is not available")
                    
                    logger.info("Starting filesystem traversal...")
                    
                    # Get full and relative root paths
                    full_root_path = config.get('root_path', '')
                    if not full_root_path:
                        raise ValueError("Root path is required")
                        
                    # Get relative path by removing mount point prefix
                    if not full_root_path.startswith(mount_point):
                        raise ValueError(f"Root path '{full_root_path}' must be under mount point '{mount_point}'")
                        
                    relative_root_path = full_root_path[len(mount_point):].lstrip('/')
                    # For logging only: if root path equals mount point, show as "/"
                    display_relative_path = "/" if full_root_path == mount_point else relative_root_path
                    logger.info(f"Full root path: {full_root_path}")
                    logger.info(f"Mount point: {mount_point}")
                    logger.info(f"Relative root path: {display_relative_path}")
                    
                    # Initialize variables for batch processing
                    batch_dirs = 0
                    batch_files = 0
                    batch_size = 0
                    batch = []
                    current_files = []  # Track all current files for cleanup
                    files_in_batch = 0
                    skip_patterns = config.get('skip_patterns', {})
                    batch_size_limit = config['performance']['batch_size']
                    
                    # Process files from LucidLink API using relative path
                    async for item in lucidlink_api.traverse_filesystem(relative_root_path):
                        # Check for shutdown request
                        if shutdown_requested:
                            logger.info("Gracefully stopping directory scan...")
                            # Process any remaining items in the batch
                            if batch:
                                await process_batch(session, batch, stats, lucidlink_api)
                            break

                        # Skip files based on patterns
                        if should_skip_file(item, skip_patterns):
                            stats.update_stats(skipped=1)
                            continue
                        
                        # Copy file_info to avoid modifying the original
                        item = item.copy()
                        
                        # Set required fields
                        item['relative_path'] = item['name']
                        item['id'] = str(uuid.uuid5(NAMESPACE_DNS, item['relative_path']))
                        
                        # Initialize LucidLink fields
                        item['lucidlink'] = {
                            'direct_link': None,
                            'filespace': config['lucidlink_filespace']['name'],
                            'port': config['lucidlink_filespace']['port']
                        }
                        
                        # Add to batch
                        batch.append(item)
                        batch_files += 1 if item["type"] == "file" else 0
                        batch_dirs += 1 if item["type"] == "directory" else 0
                        
                        # Collect batch stats
                        batch_size += item.get("size", 0)
                        current_files.append({'id': item['id'], 'relative_path': item['relative_path']})
                        files_in_batch += 1
                        
                        # Process batch if we've reached the batch size
                        if files_in_batch >= batch_size_limit:
                            if logger.isEnabledFor(logging.INFO):
                                logger.info(f"Processing batch of {len(batch)} items...")
                            processed = await process_batch(session, batch, stats, lucidlink_api)
                            stats.update_stats(updated=processed)
                            
                            # Update stats in one lock acquisition
                            stats.update_stats(file_count=batch_files, dir_count=batch_dirs, size=batch_size)
                            
                            # Reset batch and stats
                            batch = []
                            files_in_batch = 0
                            batch_dirs = 0
                            batch_files = 0
                            batch_size = 0
                
                # Process remaining files in the last batch
                if files_in_batch > 0:
                    if logger.isEnabledFor(logging.INFO):
                        logger.info(f"Processing batch of {files_in_batch} items...")
                    processed = await process_batch(session, batch, stats, lucidlink_api)
                    stats.update_stats(updated=processed)
                    # Update final stats
                    stats.update_stats(file_count=batch_files, dir_count=batch_dirs, size=batch_size)
                
                # Clean up items that no longer exist
                if current_files:  # Only clean up if we have files to check against
                    logger.info("Cleaning up removed files...")
                    removed_files = cleanup_missing_files(session, current_files)
                    if removed_files and client is not None:
                        removed_ids, removed_paths = zip(*removed_files)
                        client.delete_by_ids(list(removed_ids))
                    if removed_files:
                        # Split IDs and paths
                        removed_ids, removed_paths = zip(*removed_files)
                        stats.add_removed_files(len(removed_files), removed_paths)
                        logger.info(f"Removed {len(removed_files)} files from DuckDB")
            except Exception as e:
                error_msg = f"Error in async file processing: {str(e)}"
                logger.error(error_msg)
                stats.add_error(error_msg)
                raise
        
        # Run the async function to complete DuckDB indexing
        asyncio.run(process_files_async())
        
    except Exception as e:
        error_msg = f"Error in LucidLink file processing: {str(e)}"
        logger.error(error_msg)
        stats.add_error(error_msg)
        raise

def clean_elasticsearch_records(client: ElasticsearchClient, root_path: str) -> None:
    """Clean up Elasticsearch records under the given path"""
    try:
        logger.info("Cleaning up Elasticsearch records...")
        clean_elasticsearch_records(client, root_path)
    except Exception as e:
        logger.error(f"Error cleaning up Elasticsearch records: {str(e)}")
        raise

def send_data_to_elasticsearch(session: duckdb.DuckDBPyConnection, config: Dict[str, Any]) -> None:
    """Send file data from DuckDB to Elasticsearch."""
    logger.info("DuckDB indexing complete, sending data to Elasticsearch...")
    start_time = time.time()
    
    try:
        # Initialize Elasticsearch client
        es_config = config.get('elasticsearch', {})
        host = es_config.get('host', 'localhost')
        port = es_config.get('port', 9200)
        username = es_config.get('username', '')
        password = es_config.get('password', '')
        index_name = es_config.get('index_name', 'filesystem')
        filespace_name = config.get('lucidlink_filespace', {}).get('name', '')
        if filespace_name:
            index_name = f"{index_name}-{filespace_name}"
            
        client = ElasticsearchClient(
            host=host,
            port=port,
            username=username,
            password=password,
            index_name=index_name,
            filespace=filespace_name or ''
        )

        # Get all files from DuckDB
        query = """
        SELECT id, name, relative_path as filepath, size, 
               type,
               creation_time,
               update_time as modified_time,
               CASE WHEN type = 'directory' THEN true ELSE false END as is_directory,
               NULL as checksum,  -- Checksum not available in table
               '.' as root_path,
               direct_link
        FROM lucidlink_files
        WHERE indexed_at >= (
            SELECT MAX(indexed_at) - INTERVAL 1 MINUTE
            FROM lucidlink_files
        )
        ORDER BY filepath
        """
        result = session.execute(query).fetchdf()
        total_docs = len(result)
        
        if total_docs == 0:
            logger.info("No files to index in Elasticsearch")
            return
            
        # Log sample of records for debugging
        if logger.isEnabledFor(logging.INFO):
            logger.info("Sample of first 5 records:")
        for _, row in result.head().iterrows():
            if logger.isEnabledFor(logging.INFO):
                logger.info("  %s: %s" % (row['type'], row['filepath']))
            
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Preparing to index {total_docs:,} documents to Elasticsearch...")
        
        # Convert DataFrame to list of dicts for bulk indexing
        docs = []
        for _, row in result.iterrows():
            # Handle NaN values
            size = row['size']
            if pd.isna(size):
                size = 0
            
            doc = {
                "_index": client.index_name,
                "_id": str(row['id']),  # Ensure ID is string
                "_source": {
                    "name": row['name'],
                    "extension": Path(row['filepath']).suffix.lstrip('.'),
                    "filepath": row['filepath'],
                    "size_bytes": int(size),
                    "size": format_size(int(size)),
                    "type": row['type'],
                    "is_directory": bool(row['is_directory']),
                    "checksum": row['checksum'] if pd.notnull(row['checksum']) else None,
                    "creation_time": row['creation_time'].isoformat() if pd.notnull(row['creation_time']) else None,
                    "modified_time": row['modified_time'].isoformat() if pd.notnull(row['modified_time']) else None,
                    "direct_link": row['direct_link'] if pd.notnull(row['direct_link']) else None
                }
            }
            docs.append(doc)
            
        # Bulk index in larger batches for better performance
        batch_size = 5000
        total_indexed = 0
        total_success = 0
        total_failed = 0
        
        for i in range(0, len(docs), batch_size):
            batch = docs[i:i + batch_size]
            batch_start = time.time()
            try:
                success_count = 0
                failed_count = 0
                
                # Process bulk response
                response = helpers.bulk(
                    client.client.options(
                        request_timeout=300,
                        ignore_status=400,
                        retry_on_timeout=True,
                        max_retries=3
                    ),
                    batch,
                    chunk_size=batch_size,
                    raise_on_error=False,
                    raise_on_exception=False
                )
                
                # Response is (success_count, errors_list)
                if isinstance(response, tuple):
                    success_count = response[0]
                    failed_count = len(response[1]) if len(response) > 1 and response[1] else 0
                
                total_success += success_count
                total_failed += failed_count
                total_indexed += len(batch)
                
                batch_time = time.time() - batch_start
                docs_per_sec = len(batch) / batch_time
                progress = (total_indexed / total_docs) * 100
                
                if failed_count > 0:
                    logger.warning(f"Batch indexing completed with errors - Success: {success_count:,}, Failed: {failed_count:,}")
                if logger.isEnabledFor(logging.INFO):
                    logger.info(f"Progress: {progress:.1f}% ({total_indexed:,}/{total_docs:,}) - {docs_per_sec:.0f} docs/sec")
                
            except Exception as e:
                logger.error(f"Error indexing batch: {str(e)}")
                total_failed += len(batch)
        
        # Log final statistics
        total_time = time.time() - start_time
        avg_rate = total_docs / total_time
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Elasticsearch indexing complete in {total_time:.1f}s ({avg_rate:.0f} docs/sec)")
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"Total documents: {total_docs:,}, Success: {total_success:,}, Failed: {total_failed:,}")
                
    except Exception as e:
        logger.error(f"Error sending data to Elasticsearch: {str(e)}")
        raise

def format_size(size_bytes: int) -> str:
    """Convert size in bytes to human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            # Use 2 decimal places, but strip trailing zeros and decimal point if not needed
            return f"{size_bytes:.2f}".rstrip('0').rstrip('.') + f" {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

def log_workflow_summary(stats: WorkflowStats) -> None:
    """Log a summary of the workflow execution."""
    # Set end time if not already set
    if stats.end_time is None:
        stats.finish()
        
    elapsed_time = stats.end_time - stats.start_time
    processing_rate = stats.total_files / elapsed_time if elapsed_time > 0 else 0
    
    logger.info("=" * 80)
    logger.info("Indexer Summary:")
    logger.info(f"Time Elapsed:     {elapsed_time:.2f} seconds")
    logger.info(f"Processing Rate:  {processing_rate:.1f} files/second")
    logger.info(f"Total Size:       {format_size(stats.total_size)}")
    logger.info(f"Total Files:      {stats.total_files:,}")
    logger.info(f"Total Dirs:       {stats.total_dirs:,}")
    logger.info(f"Items Updated:    {stats.files_updated:,}")
    logger.info(f"Files Skipped:    {stats.files_skipped:,}")
    logger.info(f"Files Removed:    {stats.files_removed:,}")
    logger.info(f"Total Errors:     {len(stats.errors)}")
    
    if stats.files_removed > 0:
        logger.info("Removed Files:")
        for path in stats.removed_paths:
            logger.info(f"  - {path}")
    
    if stats.errors:
        logger.info("Errors:")
        for error in stats.errors:
            logger.info(f"  - {error}")
    
    logger.info("=" * 80)

def get_database_stats(session: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """Get database statistics."""
    return get_database_stats(session)

# Initialize direct link API client
def get_api_base_url(config: Dict[str, Any]) -> str:
    """Get API base URL using port from configuration."""
    port = config.get('lucidlink_filespace', {}).get('port', 9778)  # Default to 9778 if not specified
    return f"http://localhost:{port}"

api_base_url = None  # Will be initialized in main()

def main():
    """Main entry point."""
    try:
        # Set up signal handling
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Parse command line arguments
        parser = argparse.ArgumentParser(description='Index filesystem metadata')
        parser.add_argument('--config', help='Path to config file')
        parser.add_argument('--root-path', help='Root path to index')
        parser.add_argument('--mode', choices=['elasticsearch', 'index-only'], 
                          help='Operation mode: elasticsearch (default) or index-only')
        args = parser.parse_args()

        # Load configuration
        config = load_config(args.config)
        if args.root_path:
            config['root_path'] = args.root_path  # Set root path from command line
            logger.info(f"Using root path from command line: {args.root_path}")
        if args.mode:
            config['mode'] = args.mode

        # Configure logging
        configure_logging(config)
        
        # Get filespace info if enabled
        filespace_name = None
        filespace_port = None
        if config.get('lucidlink_filespace', {}).get('enabled', False):
            filespace_info = get_filespace_info(config)
            if filespace_info:
                filespace_name, filespace_port, mount_point = filespace_info
                logger.info(f"Using filespace '{filespace_name}' on port {filespace_port}")
                logger.info(f"Mount point: {mount_point}")
                config['lucidlink_filespace']['port'] = filespace_port
                config['lucidlink_filespace']['name'] = filespace_name
                config['lucidlink_filespace']['mount_point'] = mount_point
                # Update root_path if it's relative to mount point
                if config.get('root_path', '').startswith(mount_point):
                    config['root_path'] = config['root_path']
                    logger.info(f"Using absolute root path: {config['root_path']}")
        
        # Initialize database connection
        db_url = config['database']['connection']['url']
        session = init_database(db_url)

        # Initialize workflow stats
        stats = WorkflowStats()

        # Initialize Elasticsearch client only if needed
        client = None
        if config.get('mode', 'elasticsearch') == 'elasticsearch':
            es_config = config.get('elasticsearch', {})
            host = es_config.get('host', 'localhost')
            port = es_config.get('port', 9200)
            username = es_config.get('username', '')
            password = es_config.get('password', '')
            index_name = es_config.get('index_name', 'filesystem')
            filespace_name = config.get('lucidlink_filespace', {}).get('name', '')
            if filespace_name:
                index_name = f"{index_name}-{filespace_name}"
                
            client = ElasticsearchClient(
                host=host,
                port=port,
                username=username,
                password=password,
                index_name=index_name,
                filespace=filespace_name or ''
            )

            logger.info(f"Connected to Elasticsearch at {host}:{port}")
        else:
            logger.info("Skipping Elasticsearch connection (index-only mode)")

        logger.info(f"Starting indexer in {config.get('mode', 'elasticsearch')} mode...")

        try:
            # Process files
            if config.get('lucidlink_filespace', {}).get('enabled', False):
                process_lucidlink_files(session, stats, config, client)
            else:
                root_path = config.get('root_path')
                if not root_path:
                    raise ValueError("Root path not specified")
                    
                process_files(session, [{'path': root_path}], stats, config)
                
            # Send data to Elasticsearch if in elasticsearch mode
            if config.get('mode', 'elasticsearch') == 'elasticsearch' and client is not None:
                send_data_to_elasticsearch(session, config)
            else:
                logger.info("Skipping Elasticsearch indexing (index-only mode)")
                
            # Log summary
            log_workflow_summary(stats)

        except Exception as e:
            logger.error(f"Error in main workflow: {str(e)}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()