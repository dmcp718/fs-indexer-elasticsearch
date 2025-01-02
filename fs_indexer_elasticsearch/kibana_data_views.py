import json
import logging
import os
from typing import Dict, Optional, List
import datetime
import requests
from requests.auth import HTTPBasicAuth
import base64
import uuid

logger = logging.getLogger(__name__)

class KibanaDataViewManager:
    """Manages creation of Kibana data views and saved layouts."""

    def __init__(self, config: Dict):
        """Initialize with elasticsearch config and lucidlink version."""
        self.es_config = config.get('elasticsearch', {})
        self.lucidlink_version = config.get('lucidlink_filespace', {}).get('lucidlink_version')

        # Get the base index name and filespace name
        base_index_name = self.es_config.get('index_name', 'filespace')
        filespace_name = config.get('lucidlink_filespace', {}).get('name', '')
        
        # Construct full index name with filespace if provided
        self.index_name = f"{base_index_name}-{filespace_name}" if filespace_name else base_index_name

        host = self.es_config.get('host', 'localhost')
        self.kibana_url = f"http://{host}:5601"

        self.auth = None
        username = self.es_config.get('username')
        password = self.es_config.get('password')
        if username and password:
            self.auth = HTTPBasicAuth(username, password)

    def _load_json_file(self, file_path: str) -> Optional[Dict]:
        """Load a JSON file."""
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load JSON file {file_path}: {e}")
            return None

    def _get_default_version(self) -> str:
        """Generate a default base64 encoded version string."""
        version_array = [1, 1]
        version_bytes = json.dumps(version_array).encode('utf-8')
        return base64.b64encode(version_bytes).decode('utf-8')

    def _create_config_object(self, search_id: str) -> Dict:
        """Create a config object with default settings."""
        return {
            "type": "config",
            "id": "7.11.0",
            "attributes": {
                "defaultIndex": search_id,
                "search:defaultSearch": search_id,
                "discover": {
                    "defaultSearch": search_id,
                    "defaultIndex": search_id
                },
                "defaultRoute": f"/app/discover#/view/{search_id}",
                "page:defaultIndex": search_id,
                "page:defaultSearch": search_id
            },
            "migrationVersion": {
                "config": "7.11.0"
            },
            "coreMigrationVersion": "7.11.0",
            "version": self._get_default_version(),
            "references": [
                {
                    "id": search_id,
                    "name": "search_default",
                    "type": "search"
                },
                {
                    "id": search_id,
                    "name": "index_pattern_default",
                    "type": "index-pattern"
                }
            ]
        }

    def _create_field_formats(self) -> Dict:
        """Create field formats based on version."""
        if self.lucidlink_version == 3:
            return {
                "direct_link": {
                    "id": "url",
                    "params": {
                        "labelTemplate": "link to asset"
                    }
                }
            }
        return {
            "direct_link": {
                "id": "string"
            }
        }

    def _create_index_pattern(self, search_id: str) -> Dict:
        """Create an index pattern object."""
        return {
            "type": "index-pattern",
            "id": search_id,
            "attributes": {
                "title": self.index_name,
                "timeFieldName": None,
                "fields": "[]",
                "fieldFormatMap": json.dumps(self._create_field_formats()),
                "typeMeta": "{}",
                "defaultSearchId": search_id
            },
            "migrationVersion": {
                "index-pattern": "7.11.0"
            },
            "coreMigrationVersion": "7.11.0",
            "version": self._get_default_version(),
            "references": [
                {
                    "id": search_id,
                    "name": "default_search",
                    "type": "search"
                }
            ]
        }

    def _create_search_object(self, search_id: str) -> Dict:
        """Create a search object with layout settings."""
        search_attrs = {
            "title": f"{self.index_name} Layout",
            "description": "Default layout for file browser",
            "hits": 0,
            "columns": [
                "name",
                "creation_time",
                "modified_time",
                "size",
                "type",
                "direct_link",
                "filepath",
                "fsEntryId",
                "checksum"
            ],
            "sort": [],
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "query": {"query": "", "language": "kuery"},
                    "filter": [],
                    "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    "highlightAll": False,
                    "version": True
                })
            }
        }

        # Add grid settings for v3
        if self.lucidlink_version == 3:
            search_attrs["grid"] = {
                "columns": {
                    "creation_time": {"width": 149},
                    "modified_time": {"width": 152},
                    "size": {"width": 92},
                    "type": {"width": 96},
                    "fsEntryId": {"width": 129},
                    "direct_link": {"width": 162}
                }
            }

        return {
            "type": "search",
            "id": search_id,
            "attributes": search_attrs,
            "references": [
                {
                    "id": search_id,
                    "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    "type": "index-pattern"
                }
            ],
            "migrationVersion": {
                "search": "7.11.0"
            },
            "coreMigrationVersion": "7.11.0",
            "version": self._get_default_version()
        }

    def _get_existing_data_views(self) -> List[Dict]:
        """Get list of existing data views from Kibana."""
        endpoint = "/api/saved_objects/_find"
        params = {
            "type": "index-pattern",
            "fields": ["title"],
            "per_page": 1000
        }
        try:
            response = requests.get(
                url=f"{self.kibana_url}{endpoint}",
                headers={"kbn-xsrf": "true"},
                auth=self.auth,
                params=params
            )
            if response.status_code == 200:
                data = response.json()
                return data.get("saved_objects", [])
            else:
                logger.error(f"Failed to get data views: {response.text}")
                return []
        except requests.exceptions.RequestException as err:
            logger.error(f"Failed to get data views: {err}")
            return []

    def _send_objects_to_kibana(self, objects: List[Dict]) -> bool:
        """Send objects to Kibana using the import endpoint."""
        endpoint = f"/api/saved_objects/_import"
        
        try:
            # Convert objects to NDJSON format
            ndjson_content = '\n'.join(json.dumps(obj) for obj in objects)
            
            files = {
                'file': ('export.ndjson', ndjson_content, 'application/ndjson')
            }
            
            # Remove Content-Type from headers
            headers = {"kbn-xsrf": "true"}
            
            # Log the exact data being sent
            logger.debug("=== Request Details ===")
            logger.debug(f"URL: {self.kibana_url}{endpoint}")
            logger.debug("Headers:")
            logger.debug(json.dumps(headers, indent=2))
            logger.debug("Objects:")
            logger.debug(json.dumps(objects, indent=2))
            
            response = requests.post(
                url=f"{self.kibana_url}{endpoint}",
                headers=headers,
                auth=self.auth,
                files=files,
                params={"overwrite": "true"}
            )
            
            logger.debug("=== Response Details ===")
            logger.debug(f"Status Code: {response.status_code}")
            logger.debug(f"Response Headers: {dict(response.headers)}")
            logger.debug(f"Response Body: {response.text}")
            
            response_data = response.json()
            if response_data.get("success", False):
                return True
            else:
                logger.error(f"Import failed: {response_data}")
                return False
                
        except requests.exceptions.RequestException as err:
            logger.error(f"Kibana API request failed: {err}")
            return False

    def setup_kibana_views(self) -> bool:
        """Create both data view and saved layout if they don't exist."""
        try:
            logger.info("Setting up Kibana data views...")
            
            # Check for existing data views
            existing_views = self._get_existing_data_views()
            for view in existing_views:
                if view.get("attributes", {}).get("title") == self.index_name:
                    logger.info(f"Data view for {self.index_name} already exists")
                    return True

            objects_to_import = []

            # Generate a unique ID for our data view
            search_id = str(uuid.uuid4())

            # Create all objects from scratch
            objects_to_import.extend([
                self._create_config_object(search_id),
                self._create_index_pattern(search_id),
                self._create_search_object(search_id)
            ])

            # Send objects to Kibana
            return self._send_objects_to_kibana(objects_to_import)

        except Exception as e:
            logger.error(f"Failed to setup Kibana data views: {e}")
            return False
