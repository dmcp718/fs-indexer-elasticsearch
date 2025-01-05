from elasticsearch import Elasticsearch, helpers
import logging
from typing import List, Dict, Any
import urllib3
from datetime import datetime
from dateutil import tz
from ..utils.size_formatter import format_size

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
                    "type": {"type": "keyword"},
                    "extension": {"type": "keyword"},
                    "checksum": {"type": "keyword"},
                    "direct_link": {"type": "keyword"},
                    "last_seen": {"type": "date"}
                }
            }
        }

    def _format_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Format document for Elasticsearch."""
        # Convert datetime objects to ISO format
        for field in ['modified_time', 'creation_time', 'last_seen']:
            if isinstance(doc.get(field), datetime):
                doc[field] = doc[field].isoformat()

        # Add human readable size
        if 'size_bytes' in doc:
            doc['size'] = format_size(doc['size_bytes'])

        return doc

    def _prepare_documents(self, entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Prepare documents for Elasticsearch indexing."""
        documents = []
        for entry in entries:
            # Create a copy of the entry to avoid modifying the original
            doc = entry.copy()
            
            # Remove internal fields
            doc.pop('relative_path', None)  # Remove relative_path as it's only for internal use
            
            # Format size for display
            if 'size_bytes' in doc:
                doc['size'] = format_size(doc['size_bytes'])
            
            documents.append(doc)
            
        return documents

    def send_data(self, data: list):
        try:
            if not data:
                logging.warning("No data to send to Elasticsearch")
                return
                
            response = self.client.bulk(index=self.index_name, operations=data, refresh=True)
            if response.get('errors', False):
                logging.error(f"Bulk operation had errors: {response}")
            else:
                logging.info(f"Successfully indexed {len(data)//2} documents")
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

    def bulk_index(self, documents: List[Dict[str, Any]]) -> None:
        """Index multiple documents in bulk."""
        if not documents:
            return

        actions = []
        for doc in documents:
            # Format document
            formatted_doc = self._format_document(doc)
            
            action = {
                '_index': self.index_name,
                '_source': formatted_doc,
            }
            actions.append(action)

        try:
            success, failed = helpers.bulk(
                self.client,
                actions,
                refresh=True,
                raise_on_error=False
            )
            logging.info(f"Indexed {success} documents, {len(failed)} failed")
        except Exception as e:
            logging.error(f"Failed to bulk index documents: {e}")
            raise
