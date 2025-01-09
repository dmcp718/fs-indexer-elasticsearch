#!/usr/bin/env python3

import sys
import logging
import time
import signal
import argparse
import asyncio
from typing import Dict, Any, Optional, Tuple
import os
import subprocess

from .config.config import load_config
from .config.logging import configure_logging
from .elasticsearch.elasticsearch_integration import ElasticsearchClient
from .elasticsearch.kibana_data_views import KibanaDataViewManager
from .scanner import FileScanner, DirectLinkManager
from .lucidlink.directory_size import DirectorySizeCalculator
from .lucidlink.lucidlink_api import LucidLinkAPI
from .lucidlink.filespace_prompt import get_filespace_info
from .utils.workflow_stats import WorkflowStats

# Initialize basic logging
logger = logging.getLogger(__name__)

# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle interrupt signals for graceful shutdown."""
    global shutdown_requested
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested = True

async def main() -> int:
    """Main entry point for the file indexer."""
    parser = argparse.ArgumentParser(
        description='LucidLink File System Indexer for Elasticsearch',
        prog='fs-indexer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Index files using config file settings
  %(prog)s --config config/indexer-config.yaml

  # Index specific directory with LucidLink v2
  %(prog)s --root-path /Volumes/filespace/path --version 2

  # Index files without Elasticsearch (index-only mode)
  %(prog)s --root-path /path/to/index --mode index-only
""")

    # Required arguments
    parser.add_argument('--config', type=str, default='config/indexer-config.yaml',
                       help='Path to configuration file (default: %(default)s)')
    
    # Optional arguments
    group = parser.add_argument_group('indexing options')
    group.add_argument('--root-path', type=str, metavar='PATH',
                      help='Root path to start indexing from. If not provided, will use path from config')
    group.add_argument('--version', type=int, choices=[2, 3], metavar='VER',
                      help='LucidLink version to use (2 or 3). Overrides config setting')
    group.add_argument('--mode', type=str, choices=['elasticsearch', 'index-only'],
                      default='elasticsearch', metavar='MODE',
                      help='Operating mode: elasticsearch (default) or index-only')
    args, unknown = parser.parse_known_args()

    try:
        # Load configuration
        config = load_config(args.config)
        
        # Override lucidlink_version if provided via CLI
        if args.version is not None:
            if 'lucidlink_filespace' not in config:
                config['lucidlink_filespace'] = {}
            config['lucidlink_filespace']['lucidlink_version'] = args.version
        
        # Configure logging
        configure_logging(config)
        
        # Register signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Initialize workflow statistics
        workflow_stats = WorkflowStats()

        # Initialize components
        mode = args.mode  # Use mode from command line arguments
        logger.info(f"Running in {mode} mode")
        scanner = None
        es_client = None
        lucidlink_api = None
        direct_link_manager = None
        direct_links_db = None  # Track direct_links database path

        # Get and normalize root path
        root_path = args.root_path or config.get('root_path', '')

        # Get mount point if LucidLink is enabled
        mount_point = None
        if config.get('lucidlink_filespace', {}).get('enabled', False):
            # Get filespace info if not provided
            if not config.get('lucidlink_filespace', {}).get('name'):
                filespace_raw, filespace_name, port, mount_point = get_filespace_info(config)
                if not filespace_raw:
                    logger.error("No filespace selected")
                    return 1
                config['lucidlink_filespace'].update({
                    'name': filespace_name,
                    'raw_name': filespace_raw,
                    'port': port,
                    'mount_point': mount_point,
                    'lucidlink_version': config.get('lucidlink_filespace', {}).get('lucidlink_version', 3)
                })

            # Get filespace info
            filespace_name = config['lucidlink_filespace']['name']
            mount_point = config['lucidlink_filespace']['mount_point']
            port = config['lucidlink_filespace']['port']
            
            # Set direct_links database path
            if mode == 'elasticsearch':
                data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
                os.makedirs(data_dir, exist_ok=True)
                direct_links_db = os.path.join(data_dir, f'{filespace_name}_directlinks.duckdb')
                logger.info(f"Using direct_links database: {os.path.relpath(direct_links_db)}")
            
            # Log mount point and root path
            if mount_point:
                logger.info(f"Mount point: {mount_point}")
                if root_path.startswith(mount_point):
                    # Strip mount point to get relative path
                    display_path = root_path[len(mount_point):].lstrip('/')
                    logger.info(f"Root path (relative to mount point): {display_path or '/'}")
                else:
                    logger.info(f"Root path: {root_path}")
            else:
                logger.info(f"Root path: {root_path}")

            # Validate root path exists and is accessible
            if not os.path.exists(root_path):
                logger.error(f"Root path does not exist: {root_path}")
                return 1
                
            # Verify root path access with ls
            try:
                subprocess.run(['ls', '-la', root_path], check=True, capture_output=True)
            except subprocess.CalledProcessError as e:
                logger.error(f"Cannot access root path {root_path}: {e.stderr.decode()}")
                return 1

            # Initialize LucidLink API if in elasticsearch mode
            if mode == 'elasticsearch' and config.get('lucidlink_filespace', {}).get('enabled', False):
                # Get v3 settings
                v3_settings = config.get('lucidlink_filespace', {}).get('v3_settings', {})
                if not v3_settings:
                    config['lucidlink_filespace']['v3_settings'] = {
                        'max_concurrent_requests': 4,
                        'retry_attempts': 5,
                        'retry_delay_seconds': 0.5
                    }
                
                # Get performance settings
                perf_settings = config.get('performance', {})
                batch_sizes = perf_settings.get('batch_sizes', {})
                
                # Add performance settings to v3_settings for compatibility
                v3_settings['batch_size'] = batch_sizes.get('direct_links', 100000)  # Default to 100K
                v3_settings['queue_size'] = v3_settings.get('batch_size', 100000)  # Match batch size
                
                # Log config without mount point and root path
                config_log = config.get('lucidlink_filespace', {}).copy()
                config_log.pop('mount_point', None)
                config_log.pop('root_path', None)
                logger.info(f"LucidLink config: {config_log}")
                logger.info("Initializing LucidLink API...")
                
                lucidlink_api = LucidLinkAPI(
                    port=config['lucidlink_filespace'].get('port', 9778),
                    mount_point=mount_point,
                    version=config['lucidlink_filespace'].get('lucidlink_version', 3),
                    filespace=config['lucidlink_filespace'].get('raw_name'),
                    v3_settings=config['lucidlink_filespace'].get('v3_settings'),
                    max_workers=config['lucidlink_filespace'].get('max_concurrent_requests', 10)
                )

                # Initialize DirectLinkManager
                if mode == 'elasticsearch':
                    direct_link_manager = DirectLinkManager(config, direct_links_db, lucidlink_api)
                    direct_link_manager.setup_database()

        # Initialize scanner
        scanner = FileScanner(config)
        scanner.setup_database()

        # Initialize Elasticsearch if needed
        if mode == 'elasticsearch':
            try:
                # Get filespace name for index
                filespace_name = None
                if config.get('lucidlink_filespace', {}).get('enabled'):
                    filespace_name = config['lucidlink_filespace'].get('name')  # Use the name field which is already formatted
                
                # Use filespace name for index if available
                root_name = filespace_name if filespace_name else ''
                
                # Create index name
                es_config = config.get('elasticsearch', {})
                index_name = f"{es_config.get('index_name', 'filespace')}-{root_name}" if root_name else es_config.get('index_name', 'filespace')
                
                # Initialize Elasticsearch client
                es_client = ElasticsearchClient(
                    host=es_config.get('host', 'localhost'),
                    port=es_config.get('port', 9200),
                    username=es_config.get('username', ''),
                    password=es_config.get('password', ''),
                    index_name=index_name,
                    filespace=config.get('lucidlink_filespace', {}).get('name', ''),
                    config=config
                )
            except Exception as e:
                logger.error(f"Error connecting to Elasticsearch: {e}")
                return 1

        # Create Kibana data views
        try:
            kibana_manager = KibanaDataViewManager(config)
            if not kibana_manager.setup_kibana_views():
                logger.error("Failed to create Kibana data views")
                return 1
        except Exception as e:
            logger.error(f"Failed to create Kibana data views: {e}")
            return 1

        # Start scanning
        try:
            # Initialize tracking variables
            check_missing_files = config.get('database', {}).get('check_missing_files', True)
            current_files = set() if check_missing_files else None
            logging.debug("Initialized file tracking")
            
            # Process files
            if lucidlink_api:
                async with lucidlink_api:
                    # Initialize batches
                    batch = []
                    direct_link_batch = []
                    
                    logger.info("Scanning root path...")
                    for entry in scanner.scan(root_path=root_path):
                        # Update statistics based on entry type
                        is_file = entry.get('type') == 'file'
                        is_dir = entry.get('type') == 'directory'
                        workflow_stats.update_stats(
                            file_count=1 if is_file else 0,
                            dirs=1 if is_dir else 0,
                            updated=1,
                            size=entry.get('size_bytes', 0)
                        )
                        
                        # Track current files if needed
                        if current_files is not None and entry.get('id'):
                            current_files.add(entry.get('id'))
                        
                        # Skip direct links and ES in index-only mode
                        if mode != 'elasticsearch':
                            continue
                        
                        # Add to direct link batch if needed
                        if direct_link_manager:  
                            direct_link_batch.append(entry)
                            if len(direct_link_batch) >= config['performance']['batch_sizes']['direct_links']:
                                await direct_link_manager.process_batch(direct_link_batch)
                                direct_link_batch = []
                        
                        # Add to Elasticsearch batch
                        if es_client:
                            batch.append(entry)
                            if len(batch) >= config['performance']['batch_sizes']['elasticsearch']:
                                es_client.bulk_index(batch, direct_links_db)
                                batch = []
                    
                    # Skip remaining batches in index-only mode
                    if mode == 'elasticsearch':
                        # Process remaining direct links
                        if direct_link_manager and direct_link_batch:
                            await direct_link_manager.process_batch(direct_link_batch)
                        
                        # Process remaining Elasticsearch batch
                        if es_client and batch:
                            es_client.bulk_index(batch, direct_links_db)
            else:
                # Process files without LucidLink
                batch = []
                logger.info("Scanning root path...")
                for entry in scanner.scan(root_path=root_path):
                    # Update statistics based on entry type
                    is_file = entry.get('type') == 'file'
                    is_dir = entry.get('type') == 'directory'
                    workflow_stats.update_stats(
                        file_count=1 if is_file else 0,
                        dirs=1 if is_dir else 0,
                        updated=1,
                        size=entry.get('size_bytes', 0)
                    )
                    
                    # Track current files if needed
                    if current_files is not None and entry.get('id'):
                        current_files.add(entry.get('id'))
                    
                    # Skip ES in index-only mode
                    if mode != 'elasticsearch':
                        continue
                    
                    # Add to Elasticsearch batch
                    if es_client:
                        batch.append(entry)
                        if len(batch) >= config['performance']['batch_sizes']['elasticsearch']:
                            es_client.bulk_index(batch, direct_links_db)
                            batch = []
                
                # Skip remaining batch in index-only mode
                if mode == 'elasticsearch':
                    # Process remaining Elasticsearch batch
                    if es_client and batch:
                        es_client.bulk_index(batch, direct_links_db)
            
            # Clean up missing files if enabled in config
            if config.get('database', {}).get('check_missing_files', True):
                try:
                    if mode == 'elasticsearch' and es_client:
                        # Use elasticsearch client for cleanup
                        if current_files:
                            logging.debug(f"Found {len(current_files)} files in current scan")
                        es_client.cleanup_missing_files(direct_links_db)
                        
                        # Get removed file IDs
                        removed_ids = es_client.get_removed_file_ids(direct_links_db)
                        if removed_ids:
                            logging.info(f"Found {len(removed_ids)} removed files")
                            workflow_stats.add_removed_files(len(removed_ids))
                    elif mode == 'index-only':
                        # In index-only mode, just track the file count difference
                        last_count = workflow_stats.get_last_file_count()
                        if last_count is not None and last_count > workflow_stats.file_count:
                            removed = last_count - workflow_stats.file_count
                            logging.info(f"Found {removed} removed files")
                            workflow_stats.add_removed_files(removed)
                            
                except Exception as e:
                    logging.error(f"Error during cleanup: {e}")
            
            # Log final statistics
            workflow_stats.log_summary()

            return 0
        except Exception as e:
            logger.error(f"Error during scanning: {e}", exc_info=True)
            workflow_stats.add_error(str(e))
            return 1

    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        return 1

if __name__ == '__main__':
    sys.exit(asyncio.run(main()))