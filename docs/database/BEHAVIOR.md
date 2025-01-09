# DuckDB Database Behavior Documentation

## Database Files and Locations

### Main Database
- **Purpose**: Stores file system metadata and scan results
- **Default Location**: `data/fs_index.duckdb`
- **Configuration**: Set via `database.connection.url` in config
- **Creation**: Created on first run if doesn't exist
- **Deletion Triggers**:
  - Schema migration needed
  - `check_missing_files` set to false
  - Database reset requested

### Direct Links Database
- **Purpose**: Stores LucidLink direct link cache
- **Location**: `data/{filespace_name}_directlinks.duckdb`
- **Creation**: Created when direct link manager initialized
- **Lifecycle**: Managed by DirectLinkManager

## Schema Structure

### lucidlink_files Table
```sql
CREATE TABLE IF NOT EXISTS lucidlink_files (
    id VARCHAR,
    name VARCHAR,
    relative_path VARCHAR PRIMARY KEY,  -- Keep for internal use
    filepath VARCHAR,  -- Clean path for external use
    size_bytes BIGINT,
    modified_time TIMESTAMP,
    direct_link VARCHAR,
    last_seen TIMESTAMP
)
```

### direct_links Table
```sql
CREATE TABLE IF NOT EXISTS direct_links (
    file_id VARCHAR PRIMARY KEY,
    direct_link VARCHAR,
    link_type VARCHAR,  -- v2 or v3
    fsentry_id VARCHAR NULL,  -- For v2 caching
    last_updated TIMESTAMP
)
```

## Critical Behaviors

### Schema Migration
1. Triggered by `needs_schema_update()`:
   - Missing required columns
   - Non-nullable size column
   - Missing tables
2. Current behavior is destructive:
   - Drops and recreates tables
   - Loses existing data
   - No backup mechanism

### File Cleanup
1. Controlled by `check_missing_files`:
   - When true: Removes entries for missing files
   - When false: Recreates database from scratch
2. Implementation in scanner.py:
   ```python
   check_missing_files = self.config.get('check_missing_files', True)
   if not check_missing_files:
       # Drop and recreate mode
       self.conn.close()
       try:
           os.remove(self._get_db_path())
       except FileNotFoundError:
           pass
   ```

### Temporary Files
1. Multiple temp directory settings:
   - Config: `./.tmp`
   - Scanner: `data`
   - Inconsistent handling
2. Created by:
   - DuckDB operations
   - Bulk inserts
   - Query optimization

## Configuration Dependencies

### Database Settings
```yaml
database:
  connection:
    url: "duckdb:///data/fs_index.duckdb"
    options:
      memory_limit: "8GB"
      threads: 10
      temp_directory: "./.tmp"
      checkpoint_on_shutdown: true
      compression: "lz4"
```

### Critical Options
1. `check_missing_files`: Controls database reset behavior
2. `temp_directory`: Affects query performance and disk usage
3. `memory_limit`: Impacts query performance
4. `threads`: Affects parallel processing

## Known Issues

1. Database Deletion
   - Destructive schema migration
   - Inconsistent cleanup behavior
   - No backup mechanism

2. Temp Directory
   - Inconsistent paths
   - No cleanup mechanism
   - Potential disk space issues

3. Configuration
   - Misplaced settings
   - Inconsistent defaults
   - Missing validation

## Dependencies and Interactions

1. Scanner Dependencies
   - Requires database for file tracking
   - Uses temp storage for batching
   - Manages file cleanup

2. Direct Link Manager
   - Separate database file
   - Caches link information
   - Handles v2/v3 API differences

3. Performance Impact
   - Schema changes affect query speed
   - Temp directory location affects I/O
   - Memory settings impact bulk operations
