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
        mapping = self._create_index_mapping()
        stats_mapping = self._create_stats_index_mapping()
        
        if not self.client.indices.exists(index=self.index_name):
            self.client.indices.create(index=self.index_name, body=mapping)
            logging.info(f"Created index {self.index_name} with mapping")
            
        stats_index = f"{self.index_name}-stats"
        if not self.client.indices.exists(index=stats_index):
            self.client.indices.create(index=stats_index, body=stats_mapping)
            logging.info(f"Created index {stats_index} with mapping")

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

    def _create_stats_index_mapping(self):
        """Create the index mapping for filesystem stats data."""
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
                        "limit": 100
                    }
                }
            },
            "mappings": {
                "properties": {
                    "relative_root_path": {
                        "type": "text",
                        "analyzer": "path_analyzer",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 512
                            }
                        }
                    },
                    "total_size_bytes": {
                        "type": "long"
                    },
                    "total_size": {
                        "type": "keyword"
                    },
                    "total_files": {
                        "type": "long"
                    },
                    "total_dirs": {
                        "type": "long"
                    },
                    "indexed_at": {
                        "type": "date"
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

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the indexed files."""
        try:
            # Get total count
            count_query = {"query": {"match_all": {}}}
            count_response = self.client.count(index=self.index_name, body=count_query)
            total_files = count_response.get('count', 0)
            
            # Get aggregations for file types and total size
            aggs_query = {
                "size": 0,
                "aggs": {
                    "total_size": {"sum": {"field": "size_bytes"}},
                    "by_type": {"terms": {"field": "type", "size": 100}},
                    "by_extension": {"terms": {"field": "extension", "size": 100}}
                }
            }
            
            aggs_response = self.client.search(index=self.index_name, body=aggs_query)
            aggs = aggs_response.get('aggregations', {})
            
            return {
                "total_files": total_files,
                "total_size_bytes": int(aggs.get('total_size', {}).get('value', 0)),
                "by_type": {
                    bucket['key']: bucket['doc_count']
                    for bucket in aggs.get('by_type', {}).get('buckets', [])
                },
                "by_extension": {
                    bucket['key']: bucket['doc_count']
                    for bucket in aggs.get('by_extension', {}).get('buckets', [])
                }
            }
        except Exception as e:
            logging.error(f"Failed to get stats from Elasticsearch: {e}")
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
