import json
import requests
import sys
import base64

def load_json_files():
    """Load the JSON files containing Kibana objects."""
    objects = []
    
    # Migration version for Kibana 7.11.0
    migration_version = {
        "index-pattern": "7.11.0",
        "search": "7.11.0",
        "config": "7.11.0"
    }
    core_migration_version = "7.11.0"
    
    index_pattern_id = "504363f3-0815-42ba-99ff-0b7481811a6e"
    search_id = "cb3b071c-3732-40c5-b702-dba71edb71b0"
    
    # Create config object with more specific settings
    config_object = {
        "type": "config",
        "id": "7.11.0",
        "attributes": {
            "defaultIndex": index_pattern_id,
            "search:defaultSearch": search_id,  # Changed from discover:defaultSearch
            "discover": {
                "defaultSearch": search_id,
                "defaultIndex": index_pattern_id
            },
            "defaultRoute": f"/app/discover#/view/{search_id}",  # Modified route format
            "page:defaultIndex": index_pattern_id,
            "page:defaultSearch": search_id
        },
        "migrationVersion": migration_version,
        "coreMigrationVersion": core_migration_version,
        "version": get_default_version(),
        "references": [
            {
                "id": search_id,
                "name": "search_default",
                "type": "search"
            },
            {
                "id": index_pattern_id,
                "name": "index_pattern_default",
                "type": "index-pattern"
            }
        ]
    }
    objects.append(config_object)
    
    # Create index pattern
    index_pattern = {
        "type": "index-pattern",
        "id": index_pattern_id,
        "attributes": {
            "title": "filespace-production-cloudstream",
            "timeFieldName": None,
            "fields": "[]",
            "fieldFormatMap": "{}",
            "typeMeta": "{}",
            "defaultSearchId": search_id  # Add default search reference
        },
        "migrationVersion": migration_version,
        "coreMigrationVersion": core_migration_version,
        "version": get_default_version(),
        "references": [
            {
                "id": search_id,
                "name": "default_search",
                "type": "search"
            }
        ]
    }
    objects.append(index_pattern)
    
    # Load saved layout
    print("\nLoading saved layout...", file=sys.stderr, flush=True)
    with open('v2_saved_layout.json', 'r') as f:
        saved_layout = json.load(f)
        print(f"Saved layout type: {saved_layout.get('type')}", file=sys.stderr, flush=True)
        
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
        
        attributes = {
            "title": saved_layout["attributes"].get("title", "Default_layout"),
            "description": saved_layout["attributes"].get("description", ""),
            "hits": 0,
            "columns": saved_layout["attributes"].get("columns", ["_source"]),
            "sort": saved_layout["attributes"].get("sort", []),
            "version": 1,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps(search_source)
            },
            "refreshInterval": {
                "pause": True,
                "value": 0
            }
        }
        
        references = [
            {
                "id": index_pattern_id,
                "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                "type": "index-pattern"
            }
        ]
        
        search_object = {
            "type": "search",
            "id": search_id,
            "attributes": attributes,
            "references": references,
            "migrationVersion": migration_version,
            "coreMigrationVersion": core_migration_version,
            "version": get_default_version()
        }
        
        objects.append(search_object)
    
    return objects

def get_default_version():
    """Generate a default base64 encoded version string."""
    version_array = [1, 1]  # Using a lower version number
    version_bytes = json.dumps(version_array).encode('utf-8')
    return base64.b64encode(version_bytes).decode('utf-8')

def send_to_kibana(objects, kibana_url, headers):
    """Send objects to Kibana."""
    endpoint = f"{kibana_url}/api/saved_objects/_import"
    
    try:
        # Convert objects to NDJSON format
        ndjson_content = '\n'.join(json.dumps(obj) for obj in objects)
        
        files = {
            'file': ('export.ndjson', ndjson_content, 'application/ndjson')
        }
        
        headers.pop('Content-Type', None)
        
        params = {
            'overwrite': 'true',
            'createNewCopies': 'false'
        }
        
        print("\nSending request to Kibana...", file=sys.stderr, flush=True)
        print(f"Objects being sent: {len(objects)}", file=sys.stderr, flush=True)
        for obj in objects:
            print(f"Object type: {obj['type']}, id: {obj['id']}", file=sys.stderr, flush=True)
        
        response = requests.post(
            endpoint,
            headers=headers,
            files=files,
            params=params,
            verify=True
        )
        
        response_data = response.json()
        print("\nResponse from Kibana:", file=sys.stderr, flush=True)
        print(json.dumps(response_data, indent=2), file=sys.stderr, flush=True)
        
        return response.status_code == 200 and response_data.get("success", False)
            
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}", file=sys.stderr, flush=True)
        return False

def main():
    print("\nStarting main function...", file=sys.stderr, flush=True)
    
    kibana_url = "http://localhost:5601"
    print(f"\nUsing Kibana URL: {kibana_url}", file=sys.stderr, flush=True)
    
    headers = {
        "kbn-xsrf": "true"
    }
    
    try:
        print("\nLoading JSON files...", file=sys.stderr, flush=True)
        objects = load_json_files()
        print(f"\nLoaded {len(objects)} objects", file=sys.stderr, flush=True)
        
        print("\nSending objects to Kibana...", file=sys.stderr, flush=True)
        success = send_to_kibana(objects, kibana_url, headers)
        
        print(f"\nImport process completed with success={success}", file=sys.stderr, flush=True)
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"\nError in main: {str(e)}", file=sys.stderr, flush=True)
        sys.exit(1)

if __name__ == "__main__":
    print("\nScript starting...", file=sys.stderr, flush=True)
    main()