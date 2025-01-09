from elasticsearch import Elasticsearch, helpers
import logging
from typing import List, Dict, Any, Tuple
import urllib3
from datetime import datetime
from dateutil import tz
from ..utils.size_formatter import format_size
import os
import duckdb
import pyarrow as pa
from ..database.db_duckdb import init_database, close_database

class ElasticsearchClient:
    def __init__(self, host: str, port: int, username: str, password: str, index_name: str, filespace: str, config: Dict[str, Any]):
        """Initialize Elasticsearch client."""
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.index_name = index_name
        self.filespace = filespace
        self.config = config
        self.client = Elasticsearch(
            hosts=[f'http://{host}:{port}'],
            basic_auth=(username, password) if username and password else None,
            verify_certs=False,
            timeout=300,  # 5 minutes
            retry_on_timeout=True,
            max_retries=3
        )
        self._ensure_index_exists()
        logging.info(f"Connected to Elasticsearch at {host}:{port}")

    def _ensure_index_exists(self):
        """Ensure the index exists with proper mapping."""
        try:
            if not self.client.indices.exists(index=self.index_name):
                self.client.indices.create(
                    index=self.index_name,
                    body=self._create_index_mapping()
                )
                logging.info(f"Created index {self.index_name}")
        except Exception as e:
            logging.error(f"Failed to ensure index exists: {e}")
            raise

    def _create_index_mapping(self):
        """Create the index mapping for filesystem data."""
        return {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": "30s",
                "analysis": {
                    "analyzer": {
                        "path_analyzer": {
                            "tokenizer": "path_tokenizer",
                            "filter": ["lowercase"]
                        }
                    },
                    "tokenizer": {
                        "path_tokenizer": {
                            "type": "path_hierarchy",
                            "delimiter": "/"
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "name": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "filepath": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        },
                        "analyzer": "path_analyzer"
                    },
                    "size_bytes": {"type": "long"},
                    "size": {"type": "keyword"},
                    "modified_time": {"type": "date"},
                    "creation_time": {"type": "date"},
                    "api_modified_time": {"type": "date"},  # Accurate timestamp from LucidLink API
                    "api_creation_time": {"type": "date"},  # Accurate timestamp from LucidLink API
                    "type": {"type": "keyword"},
                    "extension": {"type": "keyword"},
                    "checksum": {"type": "keyword"},
                    "direct_link": {"type": "keyword"},
                    "fsentry_id": {"type": "keyword"},  # LucidLink fsEntry ID
                    "last_seen": {"type": "date"},
                    "relative_path": {"type": "keyword"}
                }
            }
        }

    def _format_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Format document for Elasticsearch."""
        # Convert timestamps to ISO format
        if 'creation_time' in doc:
            doc['creation_time'] = doc['creation_time'].isoformat() if doc['creation_time'] else None
        if 'modified_time' in doc:
            doc['modified_time'] = doc['modified_time'].isoformat() if doc['modified_time'] else None
        if 'api_creation_time' in doc:
            doc['api_creation_time'] = doc['api_creation_time']
        if 'api_modified_time' in doc:
            doc['api_modified_time'] = doc['api_modified_time']
        if 'last_seen' in doc:
            doc['last_seen'] = doc['last_seen'].isoformat() if doc['last_seen'] else None
            
        return doc

    def _prepare_documents_with_join(self, documents: List[Dict[str, Any]], direct_links_db: str) -> List[Dict[str, Any]]:
        """Prepare documents for indexing by joining with direct_links database."""
        conn = None
        try:
            # Initialize database with parent config
            conn = init_database(f'duckdb:///{direct_links_db}', self.config)
            
            # Convert documents to Arrow table
            table = pa.Table.from_pylist(documents)
            
            # Register Arrow table
            conn.register("documents_table", table)
            
            # First, calculate directory sizes by summing size_bytes of all files within each directory
            dir_sizes_query = """
                WITH RECURSIVE
                file_paths AS (
                    SELECT 
                        filepath,
                        size_bytes,
                        type
                    FROM documents_table
                ),
                directory_sizes AS (
                    SELECT 
                        d1.filepath as directory_path,
                        SUM(CASE 
                            WHEN f.type = 'file' THEN f.size_bytes 
                            ELSE 0 
                        END) as total_size
                    FROM documents_table d1
                    LEFT JOIN file_paths f 
                    ON f.filepath LIKE d1.filepath || '/%' OR f.filepath = d1.filepath
                    WHERE d1.type = 'directory'
                    GROUP BY d1.filepath
                )
                SELECT * FROM directory_sizes
            """
            
            # Execute directory sizes query
            dir_sizes_table = conn.execute(dir_sizes_query).fetch_arrow_table()
            conn.register("directory_sizes", dir_sizes_table)
            
            # Join with direct_links table and update directory sizes
            query = """
                SELECT 
                    d.*,
                    COALESCE(dl.direct_link, '') as direct_link,
                    COALESCE(dl.link_type, '') as link_type,
                    CASE 
                        WHEN d.type = 'directory' THEN COALESCE(ds.total_size, 0)
                        ELSE d.size_bytes 
                    END as size_bytes
                FROM documents_table d
                LEFT JOIN direct_links dl ON d.id = dl.file_id
                LEFT JOIN directory_sizes ds ON d.filepath = ds.directory_path
            """
            
            # Execute query and fetch results as Arrow table
            result_table = conn.execute(query).fetch_arrow_table()
            
            # Convert Arrow table to list of dictionaries
            docs = result_table.to_pylist()
            
            # Format each document
            formatted_docs = []
            for doc in docs:
                formatted_doc = self._format_document(doc.copy())
                if 'size_bytes' in formatted_doc:
                    formatted_doc['size'] = format_size(formatted_doc['size_bytes'])
                formatted_docs.append(formatted_doc)
            
            # Debug logging
            if formatted_docs:
                logging.debug(f"Join results sample: {formatted_docs[0]}")
            
            return formatted_docs
            
        except Exception as e:
            logging.error(f"Error joining with direct_links database: {e}")
            # Return original documents if join fails
            return documents
        finally:
            if conn:
                try:
                    close_database(conn, self.config)
                except Exception as e:
                    logging.warning(f"Error closing database connection: {e}")

    def _prepare_documents_without_join(self, entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare documents without joining with direct_links database."""
        formatted_docs = []
        for entry in entries:
            # Format document and add size
            doc = self._format_document(entry.copy())
            if 'size_bytes' in doc:
                doc['size'] = format_size(doc['size_bytes'])
            formatted_docs.append(doc)
        return formatted_docs

    def _prepare_documents(self, entries: List[Dict[str, Any]], direct_links_db: str = None) -> List[Dict[str, Any]]:
        """Prepare documents for Elasticsearch indexing.
        
        Args:
            entries: List of file entries from scanner
            direct_links_db: Path to direct links database (optional)
        """
        if direct_links_db and os.path.exists(direct_links_db):
            return self._prepare_documents_with_join(entries, direct_links_db)
        else:
            return self._prepare_documents_without_join(entries)

    def send_data(self, data: list):
        try:
            if not data:
                return
                
            # Prepare actions for bulk indexing
            actions = []
            for doc in data:
                action = {
                    '_index': self.index_name,
                    '_source': doc,
                }
                actions.append(action)
                
            # Send to ES
            success, failed = helpers.bulk(
                self.client,
                actions,
                refresh=True,
                raise_on_error=False
            )
            logging.info(f"Indexed {success} documents, {len(failed) if failed else 0} failed")
            
        except Exception as e:
            logging.error(f"Failed to send data to Elasticsearch: {e}")
            raise

    def delete_by_ids(self, ids: List[str]) -> None:
        """Delete documents from Elasticsearch by their IDs."""
        if not ids:
            return
            
        # Prepare bulk delete actions
        actions = []
        for doc_id in ids:
            actions.extend([
                {"delete": {"_index": self.index_name, "_id": doc_id}}
            ])
            
        if actions:
            try:
                # Send bulk delete request
                response = self.client.bulk(operations=actions, refresh=True)
                if response.get('errors', False):
                    logging.error(f"Bulk delete operation had errors: {response}")
                else:
                    logging.info(f"Successfully deleted {len(ids)} documents")
            except Exception as e:
                logging.error(f"Failed to delete documents from Elasticsearch: {e}")
                raise

    def search_files(self, query: str, size: int = 100) -> List[Dict[str, Any]]:
        """Search for files using a text query."""
        try:
            search_body = {
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["filepath^2", "name^3"],
                        "type": "best_fields"
                    }
                },
                "size": size
            }
            
            response = self.client.search(index=self.index_name, body=search_body)
            hits = response.get('hits', {}).get('hits', [])
            return [hit['_source'] for hit in hits]
        except Exception as e:
            logging.error(f"Failed to search Elasticsearch: {e}")
            raise

    def bulk_index(self, documents: List[Dict[str, Any]], direct_links_db: str) -> Tuple[int, int]:
        """Bulk index documents into Elasticsearch.
        
        Args:
            documents: List of documents to index
            direct_links_db: Path to direct links database
            
        Returns:
            Tuple of (success_count, failure_count)
        """
        if not documents:
            return 0, 0
            
        # Prepare documents
        prepared_docs = self._prepare_documents(documents, direct_links_db)
            
        # Create bulk request
        bulk_data = []
        for doc in prepared_docs:
            # Get document ID
            doc_id = doc.get('id')
            if not doc_id:
                logging.warning(f"Document missing ID: {doc}")
                continue
                
            # Add index action
            bulk_data.append({
                "index": {
                    "_index": self.index_name,
                    "_id": doc_id
                }
            })
            bulk_data.append(doc)
            
        if not bulk_data:
            return 0, 0
            
        try:
            # Send bulk request
            response = self.client.bulk(
                operations=bulk_data,
                refresh=True
            )
            
            # Count successes and failures
            success_count = 0
            failure_count = 0
            for item in response['items']:
                if 'index' in item and item['index'].get('status') in [200, 201]:
                    success_count += 1
                else:
                    failure_count += 1
                    error = item.get('index', {}).get('error')
                    if error:
                        logging.warning(f"Failed to index document: {error}")
                        
            # Log summary
            if success_count > 0:
                logging.info(f"Indexed {success_count} documents, {failure_count} failed")
                
            return success_count, failure_count
            
        except Exception as e:
            logging.error(f"Error during bulk indexing: {e}")
            return 0, len(documents)

    def cleanup_missing_files(self, direct_links_db: str) -> None:
        """Remove documents for files that no longer exist in the current scan.
        
        This function:
        1. Gets the list of file IDs and paths from the current scan
        2. Gets the list of file IDs and paths from Elasticsearch
        3. Removes files that exist in Elasticsearch but not in current scan
        """
        conn = None
        total_deleted = 0
        try:
            # Initialize database connection
            conn = init_database(f'duckdb:///{direct_links_db}', self.config)
            
            # Get list of file IDs from current scan
            query = """
                SELECT 
                    DISTINCT file_id,
                    direct_link,
                    link_type
                FROM direct_links 
                WHERE file_id IS NOT NULL
            """
            result = conn.execute(query).fetch_arrow_table()
            current_files = result.to_pylist()
            
            if not current_files:
                logging.debug("No files in current scan")
                return
                
            # Convert to list of current IDs
            current_ids = set(doc['file_id'] for doc in current_files)
            logging.debug(f"Found {len(current_ids)} files in current scan")
            
            # Get all documents from Elasticsearch
            es_query = {
                "query": {"match_all": {}},
                "_source": ["id", "filepath"],
                "size": 10000  # Adjust based on your index size
            }
            
            try:
                result = self.client.search(
                    index=self.index_name,
                    body=es_query
                )
                
                # Extract IDs from Elasticsearch
                es_hits = result['hits']['hits']
                es_docs = [(hit['_source']['id'], hit['_source'].get('filepath', '')) for hit in es_hits]
                es_ids = set(doc[0] for doc in es_docs)
                logging.debug(f"Found {len(es_ids)} files in Elasticsearch")
                
                # Find IDs that are in Elasticsearch but not in current scan
                ids_to_remove = list(es_ids - current_ids)
                
                if not ids_to_remove:
                    logging.debug("No files need to be removed")
                    return
                    
                logging.info(f"Found {len(ids_to_remove)} files to remove from Elasticsearch")
                if ids_to_remove:
                    # Log some examples of files being removed
                    examples = [doc[1] for doc in es_docs if doc[0] in ids_to_remove][:5]
                    logging.debug(f"Example files being removed: {examples}")
                
                # Delete the missing files in batches
                batch_size = 1000
                for i in range(0, len(ids_to_remove), batch_size):
                    batch = ids_to_remove[i:i + batch_size]
                    query = {
                        "query": {
                            "terms": {
                                "id": batch
                            }
                        }
                    }
                    
                    try:
                        result = self.client.delete_by_query(
                            index=self.index_name,
                            body=query,
                            conflicts="proceed",
                            wait_for_completion=True,
                            refresh=True
                        )
                        deleted = result.get('deleted', 0)
                        if deleted > 0:
                            total_deleted += deleted
                            logging.info(f"Removed {deleted} files in batch {i//batch_size + 1}")
                    except Exception as e:
                        logging.error(f"Error removing files in batch {i//batch_size + 1}: {e}")
                
                if total_deleted > 0:
                    logging.info(f"Total files removed from index: {total_deleted}")
                
            except Exception as e:
                logging.error(f"Error querying Elasticsearch: {e}")
                
        except Exception as e:
            logging.error(f"Error during cleanup: {e}")
        finally:
            if conn:
                try:
                    close_database(conn, self.config)
                except Exception as e:
                    logging.warning(f"Error closing database connection during cleanup: {e}")

    def get_removed_file_ids(self, direct_links_db: str) -> List[str]:
        """Get list of file IDs that exist in Elasticsearch but not in current scan.
        
        This function:
        1. Gets the list of file IDs from the current scan
        2. Gets the list of file IDs from Elasticsearch
        3. Returns files that exist in Elasticsearch but not in current scan
        
        Returns:
            List of file IDs that should be removed from Elasticsearch
        """
        conn = None
        try:
            # Initialize database connection
            conn = init_database(f'duckdb:///{direct_links_db}', self.config)
            
            # Get list of current file IDs
            query = """
                SELECT 
                    DISTINCT file_id,
                    direct_link,
                    link_type
                FROM direct_links 
                WHERE file_id IS NOT NULL 
                ORDER BY file_id
            """
            result = conn.execute(query).fetch_arrow_table()
            current_files = result.to_pylist()
            
            if not current_files:
                logging.debug("No files in current scan")
                return []
                
            # Get current IDs
            current_ids = set(doc['file_id'] for doc in current_files)
            logging.debug(f"Found {len(current_ids)} files in current scan")
            
            # Get all documents from Elasticsearch
            query = {
                "query": {"match_all": {}},
                "_source": ["id", "filepath"],
                "size": 10000  # Adjust based on your index size
            }
            
            try:
                result = self.client.search(
                    index=self.index_name,
                    body=query
                )
                
                # Extract IDs from Elasticsearch
                es_hits = result['hits']['hits']
                es_docs = [(hit['_source']['id'], hit['_source'].get('filepath', '')) for hit in es_hits]
                es_ids = set(doc[0] for doc in es_docs)
                logging.debug(f"Found {len(es_ids)} files in Elasticsearch")
                
                # Find IDs that are in Elasticsearch but not in current scan
                removed_ids = list(es_ids - current_ids)
                
                if removed_ids:
                    # Log some examples of files being removed
                    examples = [doc[1] for doc in es_docs if doc[0] in removed_ids][:5]
                    logging.debug(f"Example files being removed: {examples}")
                    logging.debug(f"Found {len(removed_ids)} files that need to be removed")
                else:
                    logging.debug("No files need to be removed")
                    
                return removed_ids
                
            except Exception as e:
                logging.error(f"Error querying Elasticsearch: {e}")
                return []
            
        except Exception as e:
            logging.error(f"Error getting removed file IDs: {e}")
            return []
        finally:
            if conn:
                try:
                    close_database(conn, self.config)
                except Exception as e:
                    logging.warning(f"Error closing database connection: {e}")
