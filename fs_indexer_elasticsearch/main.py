#!/usr/bin/env python3

import sys
import logging
import time
import signal
import argparse
import asyncio
from typing import Dict, Any, Optional, Tuple

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
    parser = argparse.ArgumentParser(description='File System Indexer')
    parser.add_argument('--config', type=str, default='config/indexer-config.yaml',
                       help='Path to configuration file')
    parser.add_argument('--root-path', type=str,
                       help='Root path to start indexing from')
    args = parser.parse_args()

    try:
        # Load configuration
        config = load_config(args.config)
        
        # Configure logging
        configure_logging(config)
        
        # Register signal handlers
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Initialize workflow statistics
        workflow_stats = WorkflowStats()

        # Initialize components based on mode
        mode = config.get('mode', 'elasticsearch')
        scanner = None
        es_client = None

        if mode == 'lucidlink':
            # Get filespace info if not provided
            if not config.get('lucidlink_filespace', {}).get('name'):
                filespace_raw, filespace_name, port, mount_point = get_filespace_info(config)
                if not filespace_raw:
                    logger.error("No filespace selected")
                    return 1
                config['lucidlink_filespace'] = {
                    'name': filespace_name,
                    'raw_name': filespace_raw,
                    'port': port,
                    'mount_point': mount_point,
                    'lucidlink_version': config.get('lucidlink_filespace', {}).get('lucidlink_version', 3)
                }

            # Initialize LucidLink API if needed
            lucidlink_api = None
            if mode == 'lucidlink' and not config.get('lucidlink_filespace', {}).get('skip_direct_links', False):
                logger.info("Initializing LucidLink API...")
                logger.info(f"LucidLink config: {config.get('lucidlink_filespace', {})}")
                lucidlink_api = LucidLinkAPI(
                    port=config['lucidlink_filespace'].get('port', 9778),
                    mount_point=config['lucidlink_filespace'].get('mount_point', ''),
                    version=config['lucidlink_filespace'].get('lucidlink_version', 3),
                    filespace=config['lucidlink_filespace'].get('raw_name')
                )
                
                # Initialize DirectLinkManager outside the context
                direct_link_manager = None
                if not config.get('lucidlink_filespace', {}).get('skip_direct_links', False):
                    direct_link_manager = DirectLinkManager(
                        config=config,
                        lucidlink_api=lucidlink_api
                    )
                    direct_link_manager.setup_database()
                    
                # Initialize scanner
                scanner = FileScanner(config)
                scanner.setup_database()
                
                # Initialize Elasticsearch if configured
                es_config = config.get('elasticsearch', {})
                if es_config:
                    filespace_name = config['lucidlink_filespace'].get('name', '')
                    # Construct index name based on filespace
                    index_name = f"{es_config.get('index_name', 'filespace')}-{filespace_name}" if filespace_name else es_config.get('index_name', 'filespace')
                    
                    es_client = ElasticsearchClient(
                        host=es_config.get('host', 'localhost'),
                        port=es_config.get('port', 9200),
                        username=es_config.get('username', ''),
                        password=es_config.get('password', ''),
                        index_name=index_name,
                        filespace=filespace_name
                    )
                    kibana_manager = KibanaDataViewManager(config)
                    kibana_manager.setup_kibana_views()
                
                # Run scanner with LucidLink session
                async with lucidlink_api:
                    # Initialize tracking variables
                    file_count = 0
                    total_size = 0
                    updated_count = 0
                    batch = []
                    direct_link_batch = []
                    check_missing_files = config.get('check_missing_files', True)
                    current_files = set() if check_missing_files else None
                    
                    for entry in scanner.scan(root_path=args.root_path or config.get('root_path')):
                        file_count += 1
                        total_size += entry.get('size_bytes', 0)
                        updated_count += 1
                        
                        # Track current files if needed
                        if current_files is not None:
                            current_files.add(entry.get('relative_path'))
                        
                        # Add to direct link batch if needed
                        if direct_link_manager and entry.get('type') == 'file':
                            direct_link_batch.append(entry)
                            if len(direct_link_batch) >= direct_link_manager.batch_size:
                                await direct_link_manager.process_batch(direct_link_batch)
                                direct_link_batch = []
                        
                        # Add to Elasticsearch batch
                        if es_client:
                            batch.append(entry)
                            if len(batch) >= config.get('elasticsearch', {}).get('bulk_size', 5000):
                                es_client.bulk_index(batch)
                                batch = []
                    
                    # Process remaining direct links
                    if direct_link_manager and direct_link_batch:
                        await direct_link_manager.process_batch(direct_link_batch)
                    
                    # Send remaining files to Elasticsearch
                    if es_client and batch:
                        es_client.bulk_index(batch)
                    
                    # Update workflow statistics
                    workflow_stats.update_stats(
                        file_count=file_count,
                        updated=updated_count,
                        size=total_size
                    )
            elif mode == 'elasticsearch':
                # Initialize Elasticsearch client
                es_config = config.get('elasticsearch', {})
                if es_config:
                    filespace_name = config['lucidlink_filespace'].get('name', '')
                    # Construct index name based on filespace
                    index_name = f"{es_config.get('index_name', 'filespace')}-{filespace_name}" if filespace_name else es_config.get('index_name', 'filespace')
                    
                    es_client = ElasticsearchClient(
                        host=es_config.get('host', 'localhost'),
                        port=es_config.get('port', 9200),
                        username=es_config.get('username', ''),
                        password=es_config.get('password', ''),
                        index_name=index_name,
                        filespace=filespace_name
                    )
                    kibana_manager = KibanaDataViewManager(config)
                    kibana_manager.setup_kibana_views()
                    
                    # Initialize scanner
                    scanner = FileScanner(config)
                    scanner.setup_database()
                    
                    # Run scanner
                    await scanner.scan_with_elasticsearch(es_client)
                else:
                    logger.error("Elasticsearch configuration missing")
                    return 1
            else:
                # Initialize scanner without LucidLink components
                scanner = FileScanner(config)
                scanner.setup_database()
                
                # Run scanner
                await scanner.scan()

        elif mode == 'elasticsearch':
            # Initialize Elasticsearch client
            es_config = config.get('elasticsearch', {})
            # Get root path name for index
            root_path = args.root_path or config.get('root_path', '')
            root_name = root_path.strip('/').replace('/', '-').lower() if root_path else ''
            # Construct index name based on root path
            index_name = f"{es_config.get('index_name', 'filespace')}-{root_name}" if root_name else es_config.get('index_name', 'filespace')
            es_client = ElasticsearchClient(
                host=es_config.get('host', 'localhost'),
                port=es_config.get('port', 9200),
                username=es_config.get('username', ''),
                password=es_config.get('password', ''),
                index_name=index_name,
                filespace=''
            )
            kibana_manager = KibanaDataViewManager(config)
            kibana_manager.setup_kibana_views()

            # Initialize FileScanner without LucidLink components
            scanner = FileScanner(config=config)

            # Start scanning
            if scanner:
                try:
                    # Setup database
                    scanner.setup_database()
                    
                    # Get current file paths before scanning
                    check_missing_files = config.get('check_missing_files', True)
                    current_files = set() if check_missing_files else None
                    
                    # Scan files
                    file_count = 0
                    total_size = 0
                    updated_count = 0
                    batch = []
                    
                    for entry in scanner.scan(root_path=args.root_path or config.get('root_path')):
                        file_count += 1
                        total_size += entry.get('size_bytes', 0)
                        updated_count += 1
                        
                        # Track current files if needed
                        if current_files is not None:
                            current_files.add(entry.get('relative_path'))
                        
                        # Add to batch for Elasticsearch
                        if es_client:
                            batch.append(entry)
                            if len(batch) >= config.get('elasticsearch', {}).get('bulk_size', 5000):
                                es_client.bulk_index(batch)
                                batch = []
                    
                    # Send remaining files to Elasticsearch
                    if es_client and batch:
                        es_client.bulk_index(batch)
                    
                    # Clean up missing files if needed
                    if check_missing_files and current_files:
                        removed_count = scanner.cleanup_missing_files(current_files)
                        if es_client:
                            # Get removed file IDs and delete from Elasticsearch
                            removed_ids = scanner.get_removed_file_ids()
                            if removed_ids:
                                es_client.delete_by_ids(removed_ids)
                    
                    workflow_stats.update_stats(
                        file_count=file_count,
                        updated=updated_count,
                        size=total_size
                    )
                except Exception as e:
                    logger.error(f"Error during scanning: {e}", exc_info=True)
                    workflow_stats.add_error(str(e))
                    return 1

        else:
            logger.error(f"Unsupported mode: {mode}")
            return 1

        # Log final statistics
        logger.info("Final workflow statistics:")
        logger.info(f"Total files processed: {workflow_stats.total_files}")
        logger.info(f"Files updated: {workflow_stats.files_updated}")
        logger.info(f"Files removed: {workflow_stats.files_removed}")
        logger.info(f"Total size: {workflow_stats.total_size} bytes")
        if workflow_stats.errors:
            logger.info("\nErrors encountered:")
            for error in workflow_stats.errors:
                logger.error(error)

        return 0

    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        return 1

if __name__ == '__main__':
    sys.exit(asyncio.run(main()))