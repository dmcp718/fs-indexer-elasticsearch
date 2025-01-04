from elasticsearch import Elasticsearch, helpers
import logging
from typing import List, Dict, Any
import urllib3
from datetime import datetime
from dateutil import tz

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
                            "type": "pattern",
                            "pattern": "[/\\\\_\\.\\s]"
                        }
                    }
                },
                "mapping": {
                    "total_fields": {
                        "limit": 2000
                    }
                }
            },
            "mappings": {
                "properties": {
                    "name": {
                        "type": "text",
                        "analyzer": "path_analyzer",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "extension": {
                        "type": "keyword"
                    },
                    "filepath": {
                        "type": "text",
                        "analyzer": "path_analyzer",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 512
                            }
                        }
                    },
                    "fsEntryId": {
                        "type": "keyword"
                    },
                    "size_bytes": {
                        "type": "long"  # Use long to handle large directory sizes
                    },
                    "size": {
                        "type": "keyword"
                    },
                    "type": {
                        "type": "keyword"
                    },
                    "checksum": {
                        "type": "keyword",
                        "null_value": "NULL"
                    },
                    "creation_time": {
                        "type": "date"
                    },
                    "modified_time": {
                        "type": "date"
                    },
                    "direct_link": {
                        "type": "keyword",
                        "null_value": "NULL"
                    },
                    "indexed_time": {
                        "type": "date",
                        "format": "strict_date_optional_time||epoch_millis"
                    }
                }
            }
        }

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

    def bulk_index(self, docs: List[Dict[str, Any]]) -> None:
        """Bulk index documents into Elasticsearch.
        
        Args:
            docs: List of documents to index
        """
        if not docs:
            return
            
        try:
            # Bulk index
            success, failed = helpers.bulk(
                client=self.client,
                actions=docs,
                refresh=True,
                request_timeout=300,
                raise_on_error=False,
                stats_only=True
            )
            
            logging.info(f"Indexed {success} documents, {failed} failed")
                    
        except Exception as e:
            logging.error(f"Error in bulk_index: {str(e)}")
            raise
