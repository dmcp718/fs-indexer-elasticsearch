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
from fs_indexer_elasticsearch.directory_size import DirectorySizeCalculator
from .db_duckdb import (
    init_database,
    bulk_upsert_files,
    cleanup_missing_files,
)
from .elasticsearch_integration import ElasticsearchClient
from .lucidlink_api import LucidLinkAPI
from .filespace_prompt import get_filespace_info
from .kibana_data_views import KibanaDataViewManager

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
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_locations = [
            os.path.join(base_dir, 'config', 'indexer-config.yaml'),  # Project config directory
            os.path.join(base_dir, 'indexer-config.yaml'),         # Current directory
            os.path.join(os.path.dirname(__file__), 'indexer-config.yaml')  # Package directory
        ]
        
        logger.info(f"Base directory: {base_dir}")
        logger.info("Searching for config in locations:")
        for loc in config_locations:
            logger.info(f"  - {loc}")
            if os.path.exists(loc):
                config_path = loc
                logger.info(f"Found config at: {loc}")
                break
            else:
                logger.info(f"  Not found at: {loc}")
    
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

def should_skip_file(file_info: Dict[str, Any], skip_patterns: Dict[str, List[str]]) -> bool:
    """Check if a file should be skipped based on skip patterns."""
    import fnmatch
    
    filename = file_info['name'].split('/')[-1]
    full_path = file_info['name']  # Use full path from API
    is_directory = file_info.get('type') == 'directory'
    
    # Debug logging
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Checking skip patterns for: {full_path} (type: {file_info['type']})")
    
    # Check directory patterns first
    for dir_pattern in skip_patterns.get('directories', []):
        # Remove leading/trailing slashes for consistent matching
        dir_pattern = dir_pattern.strip('/')
        clean_path = full_path.strip('/')
        
        # For directories, check if the name matches directly
        if is_directory and (fnmatch.fnmatch(filename, dir_pattern) or 
                           fnmatch.fnmatch(clean_path, dir_pattern) or 
                           fnmatch.fnmatch(clean_path, f"*/{dir_pattern}")):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Skipping directory {full_path} due to pattern {dir_pattern}")
            return True
        
        # For files, check if they're in a skipped directory
        elif not is_directory and (fnmatch.fnmatch(clean_path, f"{dir_pattern}/*") or 
                                 fnmatch.fnmatch(clean_path, f"*/{dir_pattern}/*")):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Skipping file {full_path} due to directory pattern {dir_pattern}")
            return True
    
    # Only check file extensions for files, not directories
    if not is_directory:
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
            # Check if this directory should be skipped
            relative_path = str(dir_path.relative_to(root_path))
            if should_skip_file({'name': relative_path, 'type': 'directory'}, skip_patterns=config.get('skip_patterns', {})):
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Skipping directory tree: {relative_path}")
                return files
                
            # Get directory listing from API
            api_response = requests.get(f"{api_base_url}/list", params={
                'path': relative_path
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
            if entry['type'] == 'directory':
                # Check if this directory should be skipped
                if not should_skip_file(file_info={'name': entry['name'], 'type': 'directory'}, skip_patterns=config.get('skip_patterns', {})):
                    dirs_to_scan.append(root_path / path)
                elif logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Skipping directory tree: {entry['name']}")
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
                    # Skip this directory if it matches skip patterns
                    relative_path = str(dir_path.relative_to(root_path))
                    if should_skip_file({'name': relative_path, 'type': 'directory'}, skip_patterns=config.get('skip_patterns', {})):
                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"Skipping directory tree: {relative_path}")
                        continue
                        
                    api_response = requests.get(f"{api_base_url}/list", params={
                        'path': relative_path
                    }, timeout=30)
                    api_response.raise_for_status()
                    
                    for entry in api_response.json():
                        if entry['type'] == 'directory':
                            # Check if subdirectory should be skipped
                            subdir_path = dir_path / entry['name']
                            relative_subdir = str(subdir_path.relative_to(root_path))
                            if not should_skip_file({'name': relative_subdir, 'type': 'directory'}, skip_patterns=config.get('skip_patterns', {})):
                                dirs_to_scan.append(subdir_path)
                            elif logger.isEnabledFor(logging.DEBUG):
                                logger.debug(f"Skipping directory tree: {relative_subdir}")
                except Exception as e:
                    logger.error(f"Error getting subdirectories for {dir_path}: {e}")
            
            # Log progress
            scan_duration = time.time() - start_time
            scan_rate = scanned_files / scan_duration if scan_duration > 0 else 0
            if logger.isEnabledFor(logging.INFO):
                logger.info(f"Scanned {scanned_files} files so far ({scan_rate:.1f} files/s)")

def process_files_worker(queue: Queue, stop_event: Event, session: duckdb.DuckDBPyConnection, config: Dict[str, Any]):
    """Worker thread to process files from queue."""
    files_chunk = []
    chunk_size = config["performance"]["batch_size"]
    chunk_start_time = time.time()
    
    while not stop_event.is_set() or not queue.empty():
        try:
            file_info = queue.get(timeout=1)  # 1 second timeout
            files_chunk.append(file_info)
            
            # Process chunk when it reaches batch size
            if len(files_chunk) >= chunk_size:
                try:
                    # Bulk upsert the chunk
                    bulk_upsert_files(session, files_chunk)
                    chunk_duration = time.time() - chunk_start_time
                    if logger.isEnabledFor(logging.INFO):
                        logger.info(f"Database batch: {len(files_chunk)} files committed")
                    
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
            bulk_upsert_files(session, files_chunk)
            chunk_duration = time.time() - chunk_start_time
            if logger.isEnabledFor(logging.INFO):
                logger.info(f"Database batch: {len(files_chunk)} files committed")
        except Exception as e:
            logger.error(f"Error processing final chunk: {e}")

def batch_update_database(session: duckdb.DuckDBPyConnection, files_batch: List[Dict[str, Any]]) -> None:
    """Update database with a batch of files."""
    try:
        bulk_upsert_files(session, files_batch)
            
    except Exception as e:
        error_msg = f"Error updating database: {e}"
        logger.error(error_msg)
        raise

def process_files(session: duckdb.DuckDBPyConnection, files: List[Dict], config: Dict[str, Any]) -> None:
    """Process a list of files and update the database."""
    try:
        total_files = len(files)
        
        # Process files in batches
        start_time = time.time()
        last_progress_time = start_time
        last_progress_count = 0
        
        for i in range(0, len(files), config["performance"]["batch_size"]):
            batch = files[i:i + config["performance"]["batch_size"]]
            bulk_upsert_files(session, batch)
            
            # Calculate and log processing rate
            current_time = time.time()
            elapsed = current_time - last_progress_time
            files_in_chunk = len(batch)
            rate = files_in_chunk / elapsed if elapsed > 0 else 0
            if logger.isEnabledFor(logging.INFO):
                logger.info(f"Processed {i+len(batch)}/{total_files} files ({rate:.1f} files/s in last chunk)")
            last_progress_time = current_time
            last_progress_count = i+len(batch)
                
    except Exception as e:
        error_msg = f"Critical error in process_files: {str(e)}"
        logger.error(error_msg)
        raise

def process_lucidlink_files(session: duckdb.DuckDBPyConnection, config: Dict[str, Any], client: ElasticsearchClient) -> None:
    """Process files from LucidLink and index them."""
    try:
        # Initialize LucidLink API
        logger.info("Initializing LucidLink API...")
        logger.info(f"LucidLink config: {config['lucidlink_filespace']}")
        
        # Initialize API with parallel workers
        max_workers = config.get('performance', {}).get('max_workers', 10)
        port = config.get('lucidlink_filespace', {}).get('port', 9778)
        mount_point = config.get('lucidlink_filespace', {}).get('mount_point', '')
        filespace_raw = config.get('lucidlink_filespace', {}).get('filespace_raw', '')

        async def process_batch(session: duckdb.DuckDBPyConnection, batch: List[Dict], lucidlink_api: LucidLinkAPI, config: Dict[str, Any]) -> int:
            """Process a batch of files by inserting them into DuckDB."""
            try:
                logger.info(f"Processing batch of {len(batch)} items...")
                
                # Handle directory sizes based on configuration
                calculate_sizes = config.get('lucidlink_filespace', {}).get('calculate_directory_sizes', True)
                size_calculator = DirectorySizeCalculator(session)
                size_calculator.update_directory_items(batch, calculate_sizes=calculate_sizes)
                
                # Get skip patterns from config
                skip_patterns = config.get('skip_patterns', {}).get('directories', [])
                
                # Process direct links sequentially to avoid session issues
                for item in batch:
                    try:
                        # Skip direct link generation for files in skipped directories
                        if skip_patterns and lucidlink_api._should_skip_path(item['relative_path'], skip_patterns):
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug(f"Skipping direct link generation for {item['relative_path']} due to skip pattern")
                            continue

                        if logger.isEnabledFor(logging.DEBUG):
                            logger.debug(f"Generating direct link for: {item['relative_path']}")
                        # Use the LucidLink fsEntry ID directly for v2 direct links
                        if lucidlink_api.version == 2:
                            direct_link = await lucidlink_api.get_direct_link_v2(item['relative_path'], fsentry_id=item.get('fsentry_id'))
                        else:
                            direct_link = await lucidlink_api.get_direct_link_v3(item['relative_path'])
                        if direct_link:
                            if logger.isEnabledFor(logging.DEBUG):
                                logger.debug(f"Got direct link for {item['relative_path']}: {direct_link}")
                            item['direct_link'] = direct_link
                        else:
                            logger.warning(f"No direct link generated for {item['relative_path']}")
                    except Exception as e:
                        logger.error(f"Error generating direct link for {item.get('relative_path', 'unknown')}: {e}")
                bulk_upsert_files(session, batch)
                return len(batch)
                
            except Exception as e:
                error_msg = f"Error processing batch: {str(e)}"
                logger.error(error_msg)
                raise

        async def process_files_async():
            try:
                lucidlink_version = config.get('lucidlink_filespace', {}).get('lucidlink_version', 3)
                async with LucidLinkAPI(port=port, mount_point=mount_point, version=lucidlink_version, filespace=filespace_raw) as lucidlink_api:
                    # Check API health first
                    if not await lucidlink_api.health_check():
                        raise RuntimeError("LucidLink API is not available")
                    
                    logger.info("Starting filesystem traversal...")
                    
                    # Get full and relative root paths
                    full_root_path = config.get('root_path', '')
                    if not full_root_path:
                        raise ValueError("Root path is required. Please provide it using the --root-path argument or root_path: in config/indexer-config.yaml\n\nExamples:\n\nCommand line argument:\n--root-path /path/to/index\n\nconfig/indexer-config.yaml:\nroot_path: \"/path/to/index\"\n")
                        
                    # Get relative path by removing mount point prefix
                    if not full_root_path.startswith(mount_point):
                        raise ValueError(f"Root path '{full_root_path}' must be under mount point '{mount_point}'")
                        
                    relative_root_path = full_root_path[len(mount_point):].lstrip('/')
                    # For logging only: if root path equals mount point, show as "/"
                    display_relative_path = "/" if full_root_path == mount_point else relative_root_path
                    logger.info(f"Absolute path: {full_root_path}")
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
                    
                    try:
                        # Process files from LucidLink API using relative path
                        async for item in lucidlink_api.traverse_filesystem(relative_root_path):
                            # Check for shutdown request frequently
                            if shutdown_requested:
                                logger.info("Gracefully stopping directory scan...")
                                # Process any remaining items in the batch
                                if batch:
                                    await process_batch(session, batch, lucidlink_api, config)
                                return  # Exit cleanly
                                
                            # Skip files based on patterns
                            if should_skip_file(item, skip_patterns):
                                continue
                            
                            # Copy file_info to avoid modifying the original
                            item = item.copy()
                            
                            # Set required fields
                            item['relative_path'] = item['name']
                            item['name'] = os.path.basename(item['name'])
                            item['id'] = str(uuid.uuid5(NAMESPACE_DNS, item['relative_path']))
                            
                            # Add to batch
                            batch.append(item)
                            batch_files += 1 if item["type"] == "file" else 0
                            batch_dirs += 1 if item["type"] == "directory" else 0
                            
                            # Collect batch stats
                            batch_size += item.get("size", 0)
                            current_files.append({'id': item['id'], 'relative_path': item['relative_path']})
                            files_in_batch += 1
                            
                            # Process batch if it reaches the size limit
                            if files_in_batch >= batch_size_limit:
                                if logger.isEnabledFor(logging.INFO):
                                    logger.info(f"Processing batch of {files_in_batch} items...")
                                processed = await process_batch(session, batch, lucidlink_api, config)
                                
                                # Reset batch
                                batch = []
                                files_in_batch = 0
                                batch_dirs = 0
                                batch_files = 0
                                batch_size = 0
                                
                                # Check for shutdown after each batch
                                if shutdown_requested:
                                    logger.info("Gracefully stopping after batch completion...")
                                    return
                    except asyncio.CancelledError:
                        logger.info("Operation cancelled, cleaning up...")
                        if batch:
                            await process_batch(session, batch, lucidlink_api, config)
                        raise
                    
                    # Process remaining files in the last batch
                    if files_in_batch > 0 and not shutdown_requested:
                        if logger.isEnabledFor(logging.INFO):
                            logger.info(f"Processing final batch of {files_in_batch} items...")
                        processed = await process_batch(session, batch, lucidlink_api, config)
                    
                    # Clean up items that no longer exist
                    if current_files and not shutdown_requested:
                        logger.info("Cleaning up removed files...")
                        removed_files = cleanup_missing_files(session, current_files)
                        if removed_files and client is not None:
                            removed_ids, removed_paths = zip(*removed_files)
                            client.delete_by_ids(list(removed_ids))
                        if removed_files:
                            # Split IDs and paths
                            removed_ids, removed_paths = zip(*removed_files)
                            logger.info(f"Removed {len(removed_files)} files from DuckDB")
            except Exception as e:
                error_msg = f"Error in async file processing: {str(e)}"
                logger.error(error_msg)
                raise

        # Create a new event loop for the async processing
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(process_files_async())
        finally:
            loop.close()
            
    except Exception as e:
        error_msg = f"Error in LucidLink file processing: {str(e)}"
        logger.error(error_msg)
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
               NULL as checksum,  -- Checksum not available in table
               '.' as root_path,
               direct_link,
               fsentry_id
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
                    "fsEntryId": row['fsentry_id'],  # Use LucidLink fsEntry ID
                    "size_bytes": int(size),
                    "size": format_size(int(size)),
                    "type": row['type'],
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
        parser = argparse.ArgumentParser(
            description='Index filesystem metadata',
            formatter_class=argparse.RawTextHelpFormatter
        )
        parser.add_argument('--config', 
                          metavar='CONFIG',
                          help='Path to configuration file (default: config/indexer-config.yaml)')
        parser.add_argument('--root-path',
                          metavar='PATH',
                          help='Root directory path to index')
        parser.add_argument('--mode',
                          choices=['elasticsearch', 'index-only'],
                          help='Operation mode:\n\n  elasticsearch: index and send to ES (default)\n  index-only: only update local index')

        args = parser.parse_args()

        # Load configuration
        config = load_config(args.config)
        
        # CLI args override config values
        if args.root_path:
            config['root_path'] = args.root_path  # Set root path from command line
            logger.info(f"Using root path from command line: {args.root_path}")
        elif config.get('root_path') and config['root_path'].strip():  # Check for non-empty string
            logger.info(f"Using root path from config file: {config['root_path']}")
        else:
            raise ValueError("Root path is required. Please provide it using the --root-path argument or root_path: in config/indexer-config.yaml\n\nExamples:\n\nCommand line argument:\n--root-path /path/to/index\n\nconfig/indexer-config.yaml:\nroot_path: \"/path/to/index\"\n")
            
        if args.mode:
            config['mode'] = args.mode
            logger.info(f"Using mode from command line: {args.mode}")
        elif config.get('mode'):
            logger.info(f"Using mode from config file: {config['mode']}")
        else:
            config['mode'] = 'elasticsearch'  # Default if not specified in CLI or config
            logger.info("Using default mode: elasticsearch")
            
        # Configure logging
        configure_logging(config)
        
        # Get filespace info if enabled
        filespace_info = None
        filespace_name = None
        filespace_port = None
        if config.get('lucidlink_filespace', {}).get('enabled', False):
            filespace_info = get_filespace_info(config)
            if filespace_info:
                filespace_raw, filespace_name, filespace_port, mount_point = filespace_info
                logger.info(f"Using filespace '{filespace_name}' on port {filespace_port}")
                logger.info(f"Mount point: {mount_point}")
                config['lucidlink_filespace']['port'] = filespace_port
                config['lucidlink_filespace']['name'] = filespace_name
                config['lucidlink_filespace']['mount_point'] = mount_point
                config['lucidlink_filespace']['filespace_raw'] = filespace_raw
                # Update root_path if it's relative to mount point
                if config.get('root_path', '').startswith(mount_point):
                    config['root_path'] = config['root_path']
                    logger.info(f"Using absolute root path: {config['root_path']}")
        
        # Initialize database connection
        db_url = config['database']['connection']['url']
        
        # Use filespace name for database file if lucidlink is enabled
        if filespace_info:  # Use the already retrieved filespace_info
            # Create data directory if it doesn't exist
            os.makedirs("data", exist_ok=True)
            # Update database URL to use filespace name in data directory
            db_url = f"duckdb:///data/{filespace_name}_index.duckdb"
            logger.info(f"Using database: {db_url}")
        
        session = init_database(db_url)

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
            
            # Create Kibana data views
            kibana_manager = KibanaDataViewManager(config)
            if not kibana_manager.create_data_views():
                logger.error("Failed to create Kibana data views")
                return
        else:
            logger.info("Skipping Elasticsearch connection (index-only mode)")

        logger.info(f"Starting indexer in {config.get('mode', 'elasticsearch')} mode...")

        try:
            # Process files
            if config.get('lucidlink_filespace', {}).get('enabled', False):
                process_lucidlink_files(session, config, client)
            else:
                root_path = config.get('root_path')
                if not root_path:
                    raise ValueError("Root path not specified")
                    
                process_files(session, [{'path': root_path}], config)
                
            # Send data to Elasticsearch if in elasticsearch mode
            if config.get('mode', 'elasticsearch') == 'elasticsearch' and client is not None:
                send_data_to_elasticsearch(session, config)
                
                # Setup Kibana data views after elasticsearch indexing
                logger.info("Setting up Kibana data views...")
                kibana_manager = KibanaDataViewManager(config)
                if kibana_manager.setup_kibana_views():
                    logger.info("Successfully created Kibana data views and saved layout")
                else:
                    logger.error("Failed to create Kibana data views")
            else:
                logger.info("Skipping Elasticsearch indexing (index-only mode)")
                
        except Exception as e:
            logger.error(f"Error in main workflow: {str(e)}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()