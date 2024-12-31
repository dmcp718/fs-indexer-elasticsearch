"""DuckDB database layer for file indexer."""

import logging
import os
from typing import Dict, List, Tuple, Any

import duckdb
import pyarrow as pa
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)

# Configure optimal thread count for available memory
thread_count = 10
external_thread_count = max(1, thread_count // 2)

def init_database(db_url: str) -> duckdb.DuckDBPyConnection:
    """Initialize the database connection and create tables if they don't exist."""
    # Extract the actual file path from the URL
    db_path = db_url.replace('duckdb:///', '')
    
    # Create temp directory if it doesn't exist
    os.makedirs("./.tmp", exist_ok=True)
    
    # Connect with optimized settings
    config = {
        'threads': thread_count,
        'memory_limit': '32GB',
        'temp_directory': './.tmp'
    }
    
    # Connect with configuration
    conn = duckdb.connect(db_path, config=config)
    
    # Set thread configuration
    conn.execute(f"SET threads={thread_count}")
    conn.execute(f"SET external_threads={external_thread_count}")
    conn.execute("SET memory_limit='32GB'")
    
    # Load Arrow extension for optimal performance with pyarrow
    conn.execute("INSTALL arrow;")
    conn.execute("LOAD arrow;")
    
    # Create files table with optimized schema
    conn.execute("""
        CREATE TABLE IF NOT EXISTS lucidlink_files (
            id VARCHAR NOT NULL,
            name VARCHAR NOT NULL,
            relative_path VARCHAR NOT NULL,
            type VARCHAR NOT NULL,
            size BIGINT NOT NULL,
            creation_time TIMESTAMP WITH TIME ZONE NOT NULL,
            update_time TIMESTAMP WITH TIME ZONE NOT NULL,
            direct_link VARCHAR,
            indexed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
            error_count INTEGER NOT NULL DEFAULT 0,
            last_error VARCHAR,
            PRIMARY KEY (id)
        ) WITH (
            checkpoint_threshold='1GB'
        );
    """)
    
    # Create indexes for common queries with performance hints
    conn.execute("CREATE INDEX IF NOT EXISTS idx_path ON lucidlink_files(relative_path) WITH (index_type='art');")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_type ON lucidlink_files(type) WITH (index_type='art');")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_update ON lucidlink_files(update_time) WITH (index_type='art');")
    
    return conn

def bulk_upsert_files(conn: duckdb.DuckDBPyConnection, files_batch: List[Dict[str, Any]]) -> int:
    """Bulk upsert files into the database using a cursor for thread safety."""
    if not files_batch:
        return 0
        
    try:
        # Create a cursor for this operation
        cursor = conn.cursor()
        
        # Set thread configuration for this cursor
        cursor.execute(f"SET threads={thread_count}")
        cursor.execute(f"SET external_threads={external_thread_count}")
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
                'name': f['name'],
                'relative_path': f['relative_path'],
                'type': f['type'],
                'size': f['size'],
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
            ('name', pa.string()),
            ('relative_path', pa.string()),
            ('type', pa.string()),
            ('size', pa.int64()),
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
        except Exception as e:
            cursor.execute("ROLLBACK")
            raise
        
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
        
        # Get column names from lucidlink_files table
        result = cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'lucidlink_files'
        """).fetchall()
        columns = [row[0].lower() for row in result]
        
        # Check if relative_path and direct_link columns exist
        return 'relative_path' not in columns or 'direct_link' not in columns
        
    except Exception as e:
        logger.error(f"Error checking schema: {str(e)}")
        # If there's an error (like table doesn't exist), assume we need an update
        return True
    finally:
        # Close the cursor
        cursor.close()

def reset_database(conn: duckdb.DuckDBPyConnection) -> None:
    """Drop and recreate all tables."""
    try:
        # Create a cursor for this operation
        cursor = conn.cursor()
        
        # Drop existing tables
        cursor.execute("DROP TABLE IF EXISTS lucidlink_files")
        cursor.execute("DROP TABLE IF EXISTS temp_batch")
        
        # Recreate tables with new schema
        cursor.execute("""
            CREATE TABLE lucidlink_files (
                id VARCHAR PRIMARY KEY,
                name VARCHAR,
                relative_path VARCHAR,
                type VARCHAR,
                size BIGINT,
                creation_time TIMESTAMP WITH TIME ZONE,
                update_time TIMESTAMP WITH TIME ZONE,
                direct_link VARCHAR,
                indexed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                error_count INTEGER DEFAULT 0,
                last_error VARCHAR
            );
        """)
        logger.info("Database tables reset successfully")
        
        logger.info("Database reset completed successfully")
        
    except Exception as e:
        logger.error(f"Error resetting database: {str(e)}")
        raise
    finally:
        # Close the cursor
        cursor.close()
