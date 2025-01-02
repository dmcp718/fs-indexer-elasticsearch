import json
import logging
import os
from typing import Dict, Optional, List
import datetime
import requests
from requests.auth import HTTPBasicAuth
import base64

logger = logging.getLogger(__name__)

class KibanaDataViewManager:
    """Manages creation of Kibana data views and saved layouts."""

    def __init__(self, config: Dict):
        """Initialize with elasticsearch config and lucidlink version."""
        self.es_config = config.get('elasticsearch', {})
        self.lucidlink_version = config.get('lucidlink_filespace', {}).get('lucidlink_version')

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

    def _create_search_object(self, saved_layout: Dict) -> Dict:
        """Create a search object from saved layout."""
        # Create search object attributes
        attributes = {
            "title": saved_layout["attributes"].get("title", "Default_layout"),
            "description": saved_layout["attributes"].get("description", ""),
            "hits": 0,
            "columns": saved_layout["attributes"].get("columns", []),
            "sort": saved_layout["attributes"].get("sort", []),
            "version": 1,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps({
                    "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
                    "filter": [],
                    "query": {
                        "query": "",
                        "language": "kuery"
                    }
                })
            }
        }
        
        # Create references using our index pattern ID
        references = [
            {
                "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                "type": "index-pattern",
                "id": self.index_pattern_id  # Use our actual index pattern ID
            }
        ]
        
        # Add additional references if they exist, but ensure they use our index pattern ID
        if saved_layout.get("references"):
            for ref in saved_layout["references"]:
                if ref["type"] == "index-pattern":
                    ref["id"] = self.index_pattern_id
                if ref not in references:
                    references.append(ref)
        
        # Create the complete search object
        search_object = {
            "type": "search",
            "id": saved_layout["id"],
            "attributes": attributes,
            "references": references,
            "migrationVersion": {
                "search": "7.11.0"
            },
            "coreMigrationVersion": "7.11.0",
            "version": self._get_default_version()
        }
        
        return search_object

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
        """Create both data view and saved layout."""
        try:
            logger.info("Setting up Kibana data views...")
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            objects_to_import = []

            # Load index pattern config
            index_pattern_file = os.path.join(base_dir, 'kibana_data_view', 'index_pattern.json')
            logger.debug(f"Loading index pattern from: {index_pattern_file}")
            
            index_pattern_config = self._load_json_file(index_pattern_file)
            if not index_pattern_config:
                return False

            # Store index pattern ID for later use
            self.index_pattern_id = index_pattern_config["id"]

            # Load version-specific saved layout config
            layout_file = os.path.join(
                base_dir, 
                'kibana_data_view', 
                f'v{self.lucidlink_version}_saved_layout.json'
            )
            logger.debug(f"Loading saved layout from: {layout_file}")
            
            layout_config = self._load_json_file(layout_file)
            if not layout_config:
                return False

            search_id = layout_config["id"]

            # Create config object first
            config_object = {
                "type": "config",
                "id": "7.11.0",
                "attributes": {
                    "defaultIndex": self.index_pattern_id,
                    "search:defaultSearch": search_id,
                    "discover": {
                        "defaultSearch": search_id,
                        "defaultIndex": self.index_pattern_id
                    },
                    "defaultRoute": f"/app/discover#/view/{search_id}",
                    "page:defaultIndex": self.index_pattern_id,
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
                        "id": self.index_pattern_id,
                        "name": "index_pattern_default",
                        "type": "index-pattern"
                    }
                ]
            }
            objects_to_import.append(config_object)

            # Load version-specific data view config
            data_view_file = os.path.join(
                base_dir,
                'kibana_data_view',
                f'v{self.lucidlink_version}_kibana_data_view.json'
            )
            data_view_config = self._load_json_file(data_view_file)
            if not data_view_config:
                return False

            # Create index pattern with version-specific fields
            if self.lucidlink_version == 3:
                # Use v3's URL format for direct_link
                field_formats = {
                    "direct_link": {
                        "id": "url",
                        "params": {
                            "labelTemplate": "link to asset"
                        }
                    }
                }
            else:
                # Use v2's simple string format
                field_formats = {
                    "direct_link": {
                        "id": "string"
                    }
                }

            index_pattern = {
                "type": "index-pattern",
                "id": self.index_pattern_id,
                "attributes": {
                    "title": index_pattern_config["attributes"]["title"],
                    "timeFieldName": None,
                    "fields": "[]",
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
            objects_to_import.append(index_pattern)

            # Create search object with version-specific attributes
            search_source = {
                "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
                "filter": [],
                "query": {
                    "query": "",
                    "language": "kuery"
                },
                "highlightAll": True,
                "version": True
            }

            search_attrs = {
                "title": layout_config["attributes"].get("title", "Default_layout"),
                "description": layout_config["attributes"].get("description", ""),
                "hits": 0,
                "columns": layout_config["attributes"].get("columns", ["_source"]),
                "sort": layout_config["attributes"].get("sort", []),
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps(search_source)
                },
                "refreshInterval": {
                    "pause": True,
                    "value": 0
                }
            }

            # Add grid settings for v3
            if self.lucidlink_version == 3 and "grid" in layout_config["attributes"]:
                search_attrs["grid"] = layout_config["attributes"]["grid"]

            search_object = {
                "type": "search",
                "id": search_id,
                "attributes": search_attrs,
                "references": [
                    {
                        "id": self.index_pattern_id,
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
            objects_to_import.append(search_object)

            # Send all objects to Kibana
            return self._send_objects_to_kibana(objects_to_import)

        except Exception as e:
            logger.error(f"Failed to create Kibana data views: {e}")
            return False
