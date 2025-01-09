import json
import logging
import os
from typing import Dict, Optional, List, Any
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
        field_formats = self._create_field_formats()
        data_view = self._create_data_view_object(search_id)
        
        return {
            "type": "index-pattern",
            "id": search_id,
            "attributes": {
                "title": self.index_name,
                "fields": json.dumps(data_view["fields"]),
                "fieldFormatMap": json.dumps(field_formats),
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

    def _create_data_view_object(self, index_pattern: str) -> Dict[str, Any]:
        """Create data view object with field mappings."""
        return {
            "title": index_pattern,
            "name": index_pattern,
            "fields": [
                {"name": "id", "type": "keyword", "searchable": True, "aggregatable": True},
                {"name": "name", "type": "text", "searchable": True, "aggregatable": False},
                {"name": "name.keyword", "type": "keyword", "searchable": True, "aggregatable": True},
                {"name": "filepath", "type": "text", "searchable": True, "aggregatable": False},
                {"name": "filepath.keyword", "type": "keyword", "searchable": True, "aggregatable": True},
                {"name": "size_bytes", "type": "long", "searchable": True, "aggregatable": True},
                {"name": "size", "type": "keyword", "searchable": True, "aggregatable": True},
                {"name": "modified_time", "type": "date", "searchable": True, "aggregatable": True},
                {"name": "creation_time", "type": "date", "searchable": True, "aggregatable": True},
                {"name": "api_modified_time", "type": "date", "searchable": True, "aggregatable": True},
                {"name": "api_creation_time", "type": "date", "searchable": True, "aggregatable": True},
                {"name": "type", "type": "keyword", "searchable": True, "aggregatable": True},
                {"name": "extension", "type": "keyword", "searchable": True, "aggregatable": True},
                {"name": "checksum", "type": "keyword", "searchable": True, "aggregatable": True},
                {"name": "direct_link", "type": "keyword", "searchable": True, "aggregatable": True},
                {"name": "fsentry_id", "type": "keyword", "searchable": True, "aggregatable": True},
                {"name": "file_type", "type": "keyword", "searchable": True, "aggregatable": True}
            ]
        }

    def _create_search_object(self, search_id: str) -> Dict:
        """Create a search object with layout settings."""
        title = f"{self.index_name} Layout"
        
        search_attrs = {
            "title": title,
            "description": "Default layout for file browser",
            "hits": 0,
            "columns": [
                "name",
                "creation_time",
                "modified_time",
                "size",
                "extension",
                "type",
                "direct_link",
                "filepath",
                "fsentry_id",
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
            },
            "grid": {
                "columns": {
                    "creation_time": {"width": 149},
                    "modified_time": {"width": 152},
                    "size": {"width": 92},
                    "extension": {"width": 120},
                    "type": {"width": 96},
                    "fsentry_id": {"width": 129},
                    "direct_link": {"width": 162}
                }
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
            # Convert objects to NDJSON format and write to temp file
            import tempfile
            temp = tempfile.NamedTemporaryFile(delete=False, suffix='.ndjson')
            try:
                # Write each object as a line of NDJSON
                for obj in objects:
                    temp.write(json.dumps(obj).encode('utf-8'))
                    temp.write(b'\n')
                temp.close()
                
                # Open file in binary mode for upload
                files = {
                    'file': ('export.ndjson', open(temp.name, 'rb'), 'application/ndjson')
                }
                
                # Make request
                response = requests.post(
                    url=f"{self.kibana_url}{endpoint}",
                    headers={"kbn-xsrf": "true"},
                    auth=self.auth,
                    files=files,
                    params={"overwrite": "true"}
                )
                
                if response.status_code == 200:
                    response_data = response.json()
                    if response_data.get("success", False):
                        logger.info("Successfully imported objects to Kibana")
                        return True
                
                logger.error(f"Import failed: {response.text}")
                return False
                
            finally:
                # Clean up temp file
                os.unlink(temp.name)
                
        except Exception as err:
            logger.error(f"Kibana API request failed: {err}")
            return False

    def create_data_views(self) -> bool:
        """Create Kibana data views for the index."""
        try:
            # Check if data views already exist
            existing_views = self._get_existing_data_views()
            main_view_exists = any(view['attributes']['title'] == self.index_name for view in existing_views)
            
            if main_view_exists:
                logger.info("Data view already exists")
                return True
                
            # Generate a unique search ID for both views
            search_id = str(uuid.uuid4())
            
            # Create objects to import
            objects_to_import = [
                self._create_config_object(search_id),
                self._create_index_pattern(search_id),
                self._create_search_object(search_id)
            ]
            
            # Send objects to Kibana
            return self._send_objects_to_kibana(objects_to_import)
            
        except Exception as e:
            logger.error(f"Failed to create data views: {e}")
            return False
            
    def setup_kibana_views(self) -> bool:
        """Create both data view and saved layout if they don't exist."""
        try:
            # Create data views (both main and stats)
            if not self.create_data_views():
                logger.error("Failed to create data views")
                return False
                
            logger.info("Successfully created Kibana data views")
            return True
            
        except Exception as e:
            logger.error(f"Failed to setup Kibana views: {e}")
            return False
