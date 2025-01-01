# Kibana Bulk Import Script

This Python script allows you to import multiple Kibana objects (index patterns, data views, and saved layouts) in bulk via the Kibana API.

## Usage

1. Make sure you have the required JSON files in the same directory:
   - `index_pattern.json`
   - `v2_kibana_data_view.json`
   - `v2_saved_layout.json`

2. Install the required Python package:
   ```bash
   pip install requests
   ```

3. Update the `kibana_url` variable in the script to point to your Kibana instance.

4. Run the script:
   ```bash
   python kibana_bulk_import.py
   ```

The script will:
- Load the JSON files
- Format them appropriately for bulk import
- Send them to Kibana's `_bulk_create` API endpoint
- Print the success message or any errors that occur

## Expected Response

On success (HTTP 200):
```json
{
    "success": true,
    "successCount": 3
}
```