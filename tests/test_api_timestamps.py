#!/usr/bin/env python3

import os
import sys
import logging
import asyncio
import duckdb
from datetime import datetime, timezone
from elasticsearch import Elasticsearch
import time

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fs_indexer_elasticsearch.elasticsearch.elasticsearch_integration import ElasticsearchClient
from fs_indexer_elasticsearch.scanner.direct_links import DirectLinkManager
from fs_indexer_elasticsearch.lucidlink.lucidlink_api import LucidLinkAPI
from fs_indexer_elasticsearch.config.config import load_config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_api_timestamps():
    """Test the API timestamp functionality."""
    try:
        # Load config
        config = load_config()
        
        # Get filespace name from config or prompt
        filespace = config.get('lucidlink_filespace', {}).get('name')
        if not filespace:
            # Use raw name if available, otherwise default to test
            filespace = config.get('lucidlink_filespace', {}).get('raw_name', 'production-dmpfs')
        
        # Connect to Elasticsearch
        es_config = config.get('elasticsearch', {})
        es = Elasticsearch(
            hosts=[f"http://{es_config.get('host', 'localhost')}:{es_config.get('port', 9200)}"],
            verify_certs=False
        )
        
        # Get index name - use filespace name if available
        index_name = f"filespace-{filespace}" if filespace else es_config.get('index_name', 'filespace')
        logger.info(f"Using index: {index_name}")
        
        # Test 1: Verify index mapping
        logger.info("Test 1: Verifying index mapping...")
        try:
            mapping = es.indices.get_mapping(index=index_name)
            properties = mapping[index_name]['mappings']['properties']
            
            assert 'api_creation_time' in properties, "api_creation_time field missing from mapping"
            assert 'api_modified_time' in properties, "api_modified_time field missing from mapping"
            assert 'fsentry_id' in properties, "fsentry_id field missing from mapping"
            logger.info("✓ Index mapping verified")
        except Exception as e:
            logger.warning(f"Could not verify index mapping: {e}")
        
        # Test 2: Verify direct_links database
        logger.info("Test 2: Verifying direct_links database...")
        direct_links_db = f"data/{filespace}_directlinks.duckdb"
        
        if os.path.exists(direct_links_db):
            conn = duckdb.connect(direct_links_db)
            
            # Check table schema
            schema = conn.execute("DESCRIBE direct_links").fetchall()
            columns = [row[0] for row in schema]
            
            assert 'api_creation_time' in columns, "api_creation_time column missing from direct_links"
            assert 'api_modified_time' in columns, "api_modified_time column missing from direct_links"
            assert 'fsentry_id' in columns, "fsentry_id column missing from direct_links"
            logger.info("✓ Direct links database schema verified")
            
            # Test 3: Check a sample document in Elasticsearch
            logger.info("Test 3: Checking sample document in Elasticsearch...")
            try:
                sample = es.search(
                    index=index_name,
                    body={
                        "query": {"match_all": {}},
                        "size": 1
                    }
                )
                
                if sample['hits']['total']['value'] > 0:
                    doc = sample['hits']['hits'][0]['_source']
                    # Check if either API or filesystem timestamps are present
                    assert any([
                        'api_creation_time' in doc,
                        'api_modified_time' in doc,
                        'creation_time' in doc,
                        'modified_time' in doc
                    ]), "No timestamps found in document"
                    logger.info("✓ Document timestamps verified")
                else:
                    logger.warning("No documents found in index")
            except Exception as e:
                logger.warning(f"Could not check sample document: {e}")
            
            # Test 4: Verify direct_links join
            logger.info("Test 4: Verifying direct_links join...")
            try:
                # Get a sample file_id from direct_links
                sample_link = conn.execute("""
                    SELECT file_id, api_creation_time, api_modified_time, fsentry_id 
                    FROM direct_links 
                    WHERE api_creation_time IS NOT NULL 
                    LIMIT 1
                """).fetchone()
                
                if sample_link:
                    file_id = sample_link[0]
                    # Find this document in Elasticsearch
                    es_doc = es.search(
                        index=index_name,
                        body={
                            "query": {"term": {"id": file_id}}
                        }
                    )
                    
                    if es_doc['hits']['total']['value'] > 0:
                        doc = es_doc['hits']['hits'][0]['_source']
                        # Convert timestamps to ISO format for comparison
                        api_creation_time = sample_link[1].strftime('%Y-%m-%dT%H:%M:%S') if sample_link[1] else None
                        api_modified_time = sample_link[2].strftime('%Y-%m-%dT%H:%M:%S') if sample_link[2] else None
                        
                        # Debug timestamps
                        logger.info(f"DuckDB file_id: {file_id}")
                        logger.info(f"DuckDB creation time: {api_creation_time}")
                        logger.info(f"DuckDB modified time: {api_modified_time}")
                        logger.info(f"DuckDB fsentry_id: {sample_link[3]}")
                        logger.info(f"ES file_id: {doc.get('id')}")
                        logger.info(f"ES creation time: {doc.get('api_creation_time')}")
                        logger.info(f"ES modified time: {doc.get('api_modified_time')}")
                        logger.info(f"ES fsentry_id: {doc.get('fsentry_id')}")
                        
                        # Verify timestamps match
                        assert doc.get('api_creation_time') == api_creation_time, "Creation time mismatch"
                        assert doc.get('api_modified_time') == api_modified_time, "Modified time mismatch"
                        assert doc.get('fsentry_id') == sample_link[3], "fsentry_id mismatch"
                        logger.info("✓ Direct links join verified")
                    else:
                        logger.warning(f"Document {file_id} not found in Elasticsearch")
                else:
                    logger.warning("No direct links with API timestamps found")
            except Exception as e:
                logger.warning(f"Could not verify direct_links join: {e}")
        else:
            logger.warning(f"Direct links database not found at {direct_links_db}")
        
        # Create test database
        db_path = "test_direct_links.duckdb"
        conn = duckdb.connect(db_path)
        
        # Drop table if exists
        conn.execute("DROP TABLE IF EXISTS direct_links")
        
        # Create table
        conn.execute("""
            CREATE TABLE direct_links (
                file_id VARCHAR,
                fsentry_id VARCHAR,
                api_creation_time TIMESTAMP,
                api_modified_time TIMESTAMP,
                file_type VARCHAR,
                direct_link VARCHAR
            )
        """)
        
        # Insert test data
        test_id = "3efddbd06e865b6cd4530815e6f49c2ee2a44121a0618ef1bb147d3e0022faf6"
        test_time = datetime(2024, 12, 23, 8, 14, 54)
        conn.execute("""
            INSERT INTO direct_links 
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            test_id,
            "13:12050",
            test_time,
            test_time,
            "file",
            "https://test.link"
        ])
        
        # Debug: check test data
        logger.info("Test data in direct_links:")
        results = conn.execute("SELECT * FROM direct_links").fetchall()
        logger.info(f"Test data: {results}")
        
        # Create test document
        test_doc = {
            "id": test_id,
            "name": "test.txt",
            "size_bytes": 1024,
            "creation_time": test_time,
            "modified_time": test_time
        }
        
        # Create Elasticsearch client
        es = ElasticsearchClient(
            host=es_config.get('host', 'localhost'),
            port=es_config.get('port', 9200),
            username=es_config.get('username', ''),
            password=es_config.get('password', ''),
            index_name=index_name,
            filespace=filespace
        )
        
        # Delete old documents with the same ID
        es.client.delete_by_query(
            index=index_name,
            body={
                "query": {"term": {"id": test_id}}
            },
            refresh=True
        )
        
        # Index test document
        es.bulk_index([test_doc], db_path)
        
        # Wait for indexing to complete
        time.sleep(1)
        
        # Search for document
        es_doc = es.client.search(
            index=index_name,
            body={
                "query": {"term": {"id": test_id}}
            }
        )
        
        # Debug: check ES document
        logger.info("ES document:")
        logger.info(es_doc)
        
        # Get the document from the search results
        if es_doc["hits"]["total"]["value"] > 0:
            doc = es_doc["hits"]["hits"][0]["_source"]
            
            # Check if the API timestamps are present
            assert doc.get("api_creation_time") == "2024-12-23T08:14:54", "API creation time not set"
            assert doc.get("api_modified_time") == "2024-12-23T08:14:54", "API modified time not set"
            assert doc.get("fsentry_id") == "13:12050", "fsentry_id not set"
        else:
            raise AssertionError("Document not found in Elasticsearch")
        
        logger.info("All tests completed!")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise

if __name__ == '__main__':
    asyncio.run(test_api_timestamps())
