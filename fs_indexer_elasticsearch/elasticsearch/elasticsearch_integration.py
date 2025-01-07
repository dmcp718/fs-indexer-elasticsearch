from elasticsearch import Elasticsearch, helpers
import logging
from typing import List, Dict, Any
import urllib3
from datetime import datetime
from dateutil import tz
from ..utils.size_formatter import format_size
import os
import duckdb
import pyarrow as pa

class ElasticsearchClient:
    def __init__(self, host: str, port: int, username: str, password: str, index_name: str, filespace: str):
        """Initialize Elasticsearch client with connection details."""
        self.client = Elasticsearch(
            hosts=[f'http://{host}:{port}'],
            basic_auth=(username, password) if username and password else None,
            verify_certs=False,
            timeout=300,  # 5 minutes
            retry_on_timeout=True,
            max_retries=3
        )
        self.index_name = index_name
        self.filespace = filespace
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
                    "last_seen": {"type": "date"}
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
            
        # Format size for display
        if 'size_bytes' in doc:
            doc['size'] = format_size(doc['size_bytes'])
            
        # Remove internal fields
        doc.pop('relative_path', None)
        
        return doc

    def _prepare_documents_with_join(self, documents: List[Dict[str, Any]], direct_links_db: str) -> List[Dict[str, Any]]:
        """Prepare documents for indexing by joining with direct_links database."""
        try:
            # Convert documents to Arrow table
            table = pa.Table.from_pylist(documents)
            
            # Connect to direct_links database
            conn = duckdb.connect(direct_links_db)
            
            # Register Arrow table
            conn.register("documents_table", table)
            
            # Create a temporary table with our documents
            conn.execute("CREATE TEMP TABLE temp_files AS SELECT * FROM documents_table")
            
            # Debug: check direct_links table
            results = conn.execute("SELECT * FROM direct_links LIMIT 5").fetchall()
            logging.debug(f"Direct links sample: {results}")
            
            # Join with direct_links to get API timestamps
            results = conn.execute("""
                WITH direct_links_latest AS (
                    SELECT 
                        file_id,
                        fsentry_id,
                        last_updated as api_creation_time,
                        link_type,
                        ROW_NUMBER() OVER (PARTITION BY file_id ORDER BY last_updated DESC) as rn
                    FROM direct_links
                )
                SELECT 
                    f.*,
                    dl.api_creation_time,
                    dl.fsentry_id,
                    dl.link_type
                FROM 
                    temp_files f
                LEFT JOIN 
                    direct_links_latest dl ON f.id = dl.file_id AND dl.rn = 1
            """).arrow()
            
            # Debug: check join results
            docs = results.to_pylist()
            logging.debug(f"Join results sample: {docs[:1]}")
            
            # Convert to list of dicts
            return docs
            
        except Exception as e:
            logging.error(f"Error joining with direct_links database: {e}")
            return documents

    def _prepare_documents_without_join(self, entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare documents without joining with direct_links database."""
        documents = []
        for entry in entries:
            # Create a copy of the entry to avoid modifying the original
            doc = entry.copy()
            
            # Remove internal fields
            doc.pop('relative_path', None)
            
            # Format size for display
            if 'size_bytes' in doc:
                doc['size'] = format_size(doc['size_bytes'])
            
            documents.append(doc)
            
        return documents

    def _prepare_documents(self, entries: List[Dict[str, Any]], direct_links_db: str = None) -> List[Dict[str, Any]]:
        """Prepare documents for Elasticsearch indexing.
        
        Args:
            entries: List of file entries from scanner
            direct_links_db: Path to direct_links database (optional)
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

    def bulk_index(self, documents: List[Dict[str, Any]], direct_links_db: str = None):
        """Index documents in bulk.
        
        Args:
            documents: List of documents to index
            direct_links_db: Path to direct_links database (optional)
        """
        try:
            # Prepare documents
            prepared_docs = self._prepare_documents(documents, direct_links_db)
            
            # Format documents for ES
            formatted_docs = []
            for doc in prepared_docs:
                formatted_docs.append(self._format_document(doc))
            
            # Send to ES
            self.send_data(formatted_docs)
            
        except Exception as e:
            logging.error(f"Error during bulk indexing: {e}")
            raise
