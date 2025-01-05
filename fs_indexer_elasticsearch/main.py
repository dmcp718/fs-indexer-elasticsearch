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

            # Initialize LucidLink API client
            lucidlink_api = LucidLinkAPI(
                port=config['lucidlink_filespace']['port'],
                mount_point=config['lucidlink_filespace']['mount_point']
            )

            # Initialize DirectLinkManager
            direct_link_manager = DirectLinkManager(
                config=config,
                lucidlink_api=lucidlink_api
            )

            # Initialize DirectorySizeCalculator if enabled
            dir_size_calculator = None
            if config['lucidlink_filespace'].get('calculate_directory_sizes', False):
                dir_size_calculator = DirectorySizeCalculator()

            # Initialize FileScanner
            scanner = FileScanner(config=config)

            # Initialize Elasticsearch client if needed
            if not config.get('skip_elasticsearch', False):
                es_client = ElasticsearchClient(config.get('elasticsearch', {}))
                kibana_manager = KibanaDataViewManager(es_client)
                await kibana_manager.setup_data_views()

        elif mode == 'elasticsearch':
            # Initialize Elasticsearch client
            es_client = ElasticsearchClient(config.get('elasticsearch', {}))
            kibana_manager = KibanaDataViewManager(es_client)
            await kibana_manager.setup_data_views()

            # Initialize FileScanner without LucidLink components
            scanner = FileScanner(config=config)

        else:
            logger.error(f"Unsupported mode: {mode}")
            return 1

        # Start scanning
        if scanner:
            try:
                result = await scanner.scan(
                    root_path=args.root_path or config.get('root_path'),
                    es_client=es_client
                )
                workflow_stats.update_stats(
                    file_count=result.get('file_count', 0),
                    updated=result.get('updated_count', 0),
                    size=result.get('total_size', 0)
                )
            except Exception as e:
                logger.error(f"Error during scanning: {e}", exc_info=True)
                workflow_stats.add_error(str(e))
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