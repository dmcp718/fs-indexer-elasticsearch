"""DuckDB database layer for file indexer."""

import logging
import os
from typing import Dict, Any, List, Tuple
import duckdb
import pyarrow as pa
from datetime import datetime
import pytz
import json

logger = logging.getLogger(__name__)

# Global connection registry to track open connections
_connections = {}

# Default configuration
DEFAULT_CONFIG = {
    'threads': 10,
    'memory_limit': '32GB',
    'temp_directory': 'tmp'
}

def get_db_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Get standardized database configuration.
    
    Args:
        config: User configuration dictionary
        
    Returns:
        Dict[str, Any]: Standardized database configuration
    """
    db_config = DEFAULT_CONFIG.copy()
    
    # Update with user config if provided
    user_config = config.get('database', {}).get('connection', {}).get('options', {})
    if user_config:
        for key in ['threads', 'memory_limit', 'temp_directory']:
            if key in user_config:
                db_config[key] = user_config[key]
    
    logger.debug(f"Database config: {json.dumps(db_config, indent=2)}")
    return db_config

def _close_existing_connection(db_path: str) -> None:
    """Close any existing connection to the database."""
    if db_path in _connections:
        try:
            existing_conn = _connections[db_path]
            try:
                existing_conn.execute("CHECKPOINT")
            except Exception as e:
                if "Connection already closed" not in str(e):
                    logger.warning(f"Error during checkpoint: {e}")
            try:
                existing_conn.close()
            except Exception as e:
                if "Connection already closed" not in str(e):
                    logger.warning(f"Error closing connection: {e}")
            del _connections[db_path]
            logger.debug(f"Closed existing connection to {db_path}")
        except Exception as e:
            logger.warning(f"Error closing existing connection to {db_path}: {e}")

def get_temp_dir(config: Dict[str, Any]) -> str:
    """Get the temp directory path from config or use default.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        str: Path to temp directory
    """
    # Get temp directory from config or use default
    temp_dir = config.get('database', {}).get('connection', {}).get('options', {}).get('temp_directory', 'tmp')
    
    # Make it relative to database location if not absolute
    if not os.path.isabs(temp_dir):
        db_path = config.get('database', {}).get('connection', {}).get('url', '').replace('duckdb:///', '')
        db_dir = os.path.dirname(db_path) if db_path else 'data'
        temp_dir = os.path.join(db_dir, temp_dir)
    
    # Create directory if it doesn't exist
    os.makedirs(temp_dir, exist_ok=True)
    
    return temp_dir

def cleanup_temp_dir(temp_dir: str) -> None:
    """Clean up temporary files in the specified directory.
    
    Only removes files with .tmp extension to avoid accidental deletions.
    """
    if not os.path.exists(temp_dir):
        return
        
    for filename in os.listdir(temp_dir):
        if filename.endswith('.tmp'):
            try:
                os.remove(os.path.join(temp_dir, filename))
            except OSError:
                # Log but don't fail if cleanup fails
                logger.warning(f"Failed to remove temp file: {filename}")

def init_database(db_url: str, config: Dict[str, Any]) -> duckdb.DuckDBPyConnection:
    """Initialize the database connection and create tables if they don't exist."""
    # Extract the actual file path from the URL
    db_path = db_url.replace('duckdb:///', '')
    
    # Log connection attempt
    logger.debug(f"Initializing database connection to {db_path}")
    logger.debug(f"Active connections: {list(_connections.keys())}")
    
    # Close any existing connection first
    _close_existing_connection(db_path)
    
    # Get standardized config
    db_config = get_db_config(config)
    
    # Get and create temp directory
    temp_dir = get_temp_dir(config)
    db_config['temp_directory'] = temp_dir
    
    # Clean up any existing temp files before connecting
    cleanup_temp_dir(temp_dir)
    
    # Log final configuration
    logger.debug(f"Connecting with config: {json.dumps(db_config, indent=2)}")
    
    # Connect with configuration
    conn = duckdb.connect(db_path, config=db_config)
    _connections[db_path] = conn
    
    # Set additional configuration
    conn.execute(f"SET threads={db_config['threads']}")
    conn.execute(f"SET external_threads={max(1, int(db_config['threads']) // 2)}")
    conn.execute(f"SET memory_limit='{db_config['memory_limit']}'")
    conn.execute(f"SET temp_directory='{temp_dir}'")
    
    # Load Arrow extension for optimal performance with pyarrow
    conn.execute("INSTALL arrow;")
    conn.execute("LOAD arrow;")
    
    logger.debug(f"Successfully connected to {db_path}")
    
    # Check if we need to migrate the schema
    needs_migration = needs_schema_update(conn)
    
    if needs_migration:
        logger.info("Migrating database schema...")
        migrate_schema(conn)
    
    # Create a cursor for this operation
    cursor = conn.cursor()
    
    try:
        # Create files table with optimized schema
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS lucidlink_files (
                id VARCHAR PRIMARY KEY,
                fsentry_id VARCHAR,
                name VARCHAR,
                relative_path VARCHAR,
                type VARCHAR,
                size BIGINT,  -- Make size nullable for directories when calculation is disabled
                creation_time TIMESTAMP WITH TIME ZONE,
                update_time TIMESTAMP WITH TIME ZONE,
                direct_link VARCHAR,
                indexed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                error_count INTEGER DEFAULT 0,
                last_error VARCHAR
            );
        """)
        
        # Create indexes for common queries with performance hints
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_path ON lucidlink_files(relative_path) WITH (index_type='art');")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_type ON lucidlink_files(type) WITH (index_type='art');")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_update ON lucidlink_files(update_time) WITH (index_type='art');")
        
    finally:
        cursor.close()
    
    return conn

def bulk_upsert_files(conn: duckdb.DuckDBPyConnection, files_batch: List[Dict[str, Any]]) -> int:
    """Bulk upsert files into the database using a cursor for thread safety."""
    if not files_batch:
        return 0
        
    try:
        # Create a cursor for this operation
        cursor = conn.cursor()
        
        # Set thread configuration for this cursor
        cursor.execute(f"SET threads={10}")
        cursor.execute(f"SET external_threads={max(1, 10 // 2)}")
        cursor.execute("SET preserve_insertion_order=false")  # Allow parallel inserts
        
        # Pre-process data to avoid per-row operations
        processed_data = []
        now = datetime.now(pytz.utc)
        
        # Process data in chunks for better memory efficiency
        chunk_size = 10000  
        for i in range(0, len(files_batch), chunk_size):
            chunk = files_batch[i:i + chunk_size]
            chunk_data = [{
                'id': f['id'],
                'fsentry_id': f.get('fsentry_id', None),
                'name': f['name'],
                'relative_path': f['relative_path'],
                'type': f['type'],
                'size': f.get('size', None),  # Ensure size is never NULL
                'creation_time': f.get('creation_time', now),
                'update_time': f.get('update_time', now),
                'direct_link': f.get('direct_link', None),
                'indexed_at': now,
                'error_count': 0,
                'last_error': None
            } for f in chunk]
            processed_data.extend(chunk_data)
        
        # Convert to Arrow table with optimized schema
        table = pa.Table.from_pylist(processed_data, schema=pa.schema([
            ('id', pa.string()),
            ('fsentry_id', pa.string()),
            ('name', pa.string()),
            ('relative_path', pa.string()),
            ('type', pa.string()),
            ('size', pa.int64()),  # Make size nullable for directories when calculation is disabled
            ('creation_time', pa.timestamp('us', tz='UTC')),
            ('update_time', pa.timestamp('us', tz='UTC')),
            ('direct_link', pa.string()),
            ('indexed_at', pa.timestamp('us', tz='UTC')),
            ('error_count', pa.int32()),
            ('last_error', pa.string())
        ]))
        
        # Register table and perform parallel upsert in a single transaction
        cursor.execute("BEGIN TRANSACTION")
        try:
            cursor.register("batch_table", table)
            cursor.execute("""
                INSERT OR REPLACE INTO lucidlink_files 
                SELECT * FROM batch_table
            """)
            cursor.execute("COMMIT")
        except Exception:
            cursor.execute("ROLLBACK")
            raise
        
        # Log sample of data after upsert
        logger.debug("Sample of data after upsert:")
        result = cursor.execute("""
            SELECT type, relative_path, size 
            FROM lucidlink_files 
            WHERE type = 'directory'
            LIMIT 5;
        """).fetchall()
        for row in result:
            logger.debug(f"Directory in main: {row[1]} (size: {row[2]} bytes)")
        
        return len(files_batch)
        
    except Exception as e:
        logger.error(f"Bulk upsert failed: {str(e)}")
        raise
    finally:
        cursor.close()

def cleanup_missing_files(session: duckdb.DuckDBPyConnection, current_files: List[Dict[str, str]]) -> List[Tuple[str, str]]:
    """Remove files from the database that no longer exist in the filesystem.
    Returns a list of tuples (id, path) that were removed."""
    try:
        # Create a cursor for this operation
        cursor = session.cursor()
        
        # Convert current files to Arrow table
        table = pa.Table.from_pylist([{'id': f['id']} for f in current_files])
        
        if len(table) == 0:
            logger.warning("No current files provided for cleanup")
            return []
            
        # Log current state
        total_files = cursor.execute("SELECT COUNT(*) FROM lucidlink_files").fetchone()[0]
        logger.info(f"Total files in database before cleanup: {total_files}")
        logger.info(f"Current files provided for cleanup: {len(current_files)}")
        
        # Create temporary table with current file IDs
        cursor.execute("DROP TABLE IF EXISTS current_files")
        cursor.execute("CREATE TEMP TABLE current_files (id STRING)")
        cursor.register("current_files_table", table)
        cursor.execute("INSERT INTO current_files SELECT id FROM current_files_table")
        
        # Get list of files to be removed with their paths
        removed_files = cursor.execute("""
            SELECT id, name, type FROM lucidlink_files
            WHERE id NOT IN (SELECT id FROM current_files)
        """).fetchall()
        
        if removed_files:
            logger.info(f"Found {len(removed_files)} files to remove:")
            for row in removed_files[:5]:  # Show first 5 for debugging
                logger.info(f"  - {row[1]} (type: {row[2]})")
            if len(removed_files) > 5:
                logger.info(f"  ... and {len(removed_files) - 5} more")
        
        # Delete files that don't exist in current_files
        cursor.execute("""
            DELETE FROM lucidlink_files
            WHERE id NOT IN (SELECT id FROM current_files)
        """)
        
        logger.info(f"Deleted {len(removed_files)} files from database")
        
        # Log final state
        remaining_files = cursor.execute("SELECT COUNT(*) FROM lucidlink_files").fetchone()[0]
        logger.info(f"Files remaining after cleanup: {remaining_files}")
        
        # Drop temporary table
        cursor.execute("DROP TABLE IF EXISTS current_files")
        
        # Return list of removed file IDs and paths
        return [(row[0], row[1]) for row in removed_files]
        
    except Exception as e:
        logger.error(f"Cleanup failed: {str(e)}")
        raise
    finally:
        # Close the cursor
        cursor.close()

def get_database_stats(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """Get database statistics."""
    try:
        # Create a cursor for this operation
        cursor = conn.cursor()
        
        stats = {}
        
        # Get total rows
        stats['total_rows'] = cursor.execute("""
            SELECT COUNT(*) FROM lucidlink_files
        """).fetchone()[0]
        
        # Get total size
        stats['total_size'] = cursor.execute("""
            SELECT SUM(size) FROM lucidlink_files
        """).fetchone()[0]
        
        # Get file count by type
        type_counts = cursor.execute("""
            SELECT type, COUNT(*) as count
            FROM lucidlink_files
            GROUP BY type
        """).fetchall()
        
        stats['type_counts'] = {t: c for t, c in type_counts}
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to get database stats: {str(e)}")
        raise
    finally:
        # Close the cursor
        cursor.close()

def needs_schema_update(conn: duckdb.DuckDBPyConnection) -> bool:
    """Check if the database needs a schema update by checking for required columns."""
    try:
        # Create a cursor for this operation
        cursor = conn.cursor()
        
        # First check if the table exists
        result = cursor.execute("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_name = 'lucidlink_files'
            );
        """).fetchone()
        
        if not result or not result[0]:
            logger.debug("Table does not exist, needs schema update")
            return True
            
        # Get column names and properties from lucidlink_files table
        result = cursor.execute("""
            SELECT column_name, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'lucidlink_files'
        """).fetchall()
        
        if not result:
            return True
            
        columns = {row[0].lower(): row[1] for row in result}
        
        # Check if required columns exist and size is nullable
        needs_update = (
            'relative_path' not in columns or 
            'direct_link' not in columns or 
            'fsentry_id' not in columns or
            (columns.get('size', 'NO') == 'NO')  # Check if size column is not nullable
        )
        
        if needs_update:
            logger.debug("Schema needs update: missing required columns or incorrect column types")
            
        return needs_update
        
    except Exception as e:
        logger.error(f"Error checking schema: {str(e)}")
        # If there's an error (like table doesn't exist), assume we need an update
        return True
    finally:
        # Close the cursor
        cursor.close()

def migrate_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Migrate database schema while preserving data."""
    try:
        # Create a cursor for this operation
        cursor = conn.cursor()
        
        # Check if old table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE table_name = 'lucidlink_files'
            );
        """)
        old_table_exists = cursor.fetchone()[0]
        
        if old_table_exists:
            # Create temporary table with new schema
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS lucidlink_files_new (
                    id VARCHAR PRIMARY KEY,
                    fsentry_id VARCHAR,
                    name VARCHAR,
                    relative_path VARCHAR,
                    type VARCHAR,
                    size BIGINT,  -- Make size nullable for directories when calculation is disabled
                    creation_time TIMESTAMP WITH TIME ZONE,
                    update_time TIMESTAMP WITH TIME ZONE,
                    direct_link VARCHAR,
                    indexed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    error_count INTEGER DEFAULT 0,
                    last_error VARCHAR
                );
            """)
            
            # Copy data from old table
            cursor.execute("""
                INSERT INTO lucidlink_files_new (
                    id, fsentry_id, name, relative_path, type, size,
                    creation_time, update_time, direct_link, indexed_at,
                    error_count, last_error
                )
                SELECT 
                    id, fsentry_id, name, relative_path, type, size,
                    creation_time, update_time, direct_link, indexed_at,
                    error_count, last_error
                FROM lucidlink_files;
            """)
            
            # Drop old table and rename new one
            cursor.execute("DROP TABLE IF EXISTS lucidlink_files")
            cursor.execute("ALTER TABLE lucidlink_files_new RENAME TO lucidlink_files")
        else:
            # First time setup - create table directly
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS lucidlink_files (
                    id VARCHAR PRIMARY KEY,
                    fsentry_id VARCHAR,
                    name VARCHAR,
                    relative_path VARCHAR,
                    type VARCHAR,
                    size BIGINT,  -- Make size nullable for directories when calculation is disabled
                    creation_time TIMESTAMP WITH TIME ZONE,
                    update_time TIMESTAMP WITH TIME ZONE,
                    direct_link VARCHAR,
                    indexed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    error_count INTEGER DEFAULT 0,
                    last_error VARCHAR
                );
            """)
        
        # Create indexes for common queries with performance hints
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_path ON lucidlink_files(relative_path) WITH (index_type='art');")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_type ON lucidlink_files(type) WITH (index_type='art');")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_update ON lucidlink_files(update_time) WITH (index_type='art');")
        
        logger.info("Database schema migrated successfully")
        
    except Exception as e:
        logger.error(f"Error migrating schema: {str(e)}")
        raise
    finally:
        cursor.close()

def close_database(conn: duckdb.DuckDBPyConnection, config: Dict[str, Any]) -> None:
    """Properly close database connection and clean up temp files."""
    if conn:
        try:
            # Get database path from connection
            db_path = None
            for path, existing_conn in _connections.items():
                if existing_conn == conn:
                    db_path = path
                    break
            
            # Ensure all changes are written
            try:
                conn.execute("CHECKPOINT")
            except Exception as e:
                if "Connection already closed" not in str(e):
                    logger.warning(f"Error during checkpoint: {e}")
            
            # Close connection
            try:
                conn.close()
            except Exception:
                logger.error("Error closing database connection")
                # Don't raise, just log the error
            
            # Remove from registry
            if db_path:
                _connections.pop(db_path, None)
            
            # Clean up temp files
            try:
                temp_dir = get_temp_dir(config)
                cleanup_temp_dir(temp_dir)
            except Exception as e:
                logger.warning(f"Error cleaning up temp files: {e}")
        except Exception as e:
            logger.error(f"Error closing database: {e}")
            # Ensure all changes are written
            try:
                conn.execute("CHECKPOINT")
            except Exception as e:
                if "Connection already closed" not in str(e):
                    logger.warning(f"Error during checkpoint: {e}")
            
            # Close connection
            try:
                conn.close()
            except Exception:
                logger.error("Error closing database connection")
                # Don't raise, just log the error
            
            # Remove from registry
            if db_path:
                _connections.pop(db_path, None)
            
            # Clean up temp files
            try:
                temp_dir = get_temp_dir(config)
                cleanup_temp_dir(temp_dir)
            except Exception as e:
                logger.warning(f"Error cleaning up temp files: {e}")
        except Exception as e:
            logger.error(f"Error closing database: {e}")
