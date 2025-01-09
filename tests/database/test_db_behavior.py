import os
import pytest
import tempfile
from datetime import datetime
from pathlib import Path
from fs_indexer_elasticsearch.database.db_duckdb import DuckDBManager
from fs_indexer_elasticsearch.config import Config

class TestDuckDBBehavior:
    @pytest.fixture
    def temp_dir(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            yield tmp_dir

    @pytest.fixture
    def config(self, temp_dir):
        return {
            'database': {
                'connection': {
                    'url': f'duckdb:///{temp_dir}/test.duckdb',
                    'options': {
                        'memory_limit': '1GB',
                        'threads': 2,
                        'temp_directory': f'{temp_dir}/tmp',
                        'checkpoint_on_shutdown': True
                    }
                }
            },
            'check_missing_files': True
        }

    @pytest.fixture
    def db_manager(self, config):
        manager = DuckDBManager(Config(config))
        yield manager
        manager.close()

    def test_database_creation(self, db_manager, temp_dir):
        """Test database is created with correct schema"""
        # Verify database file exists
        db_path = Path(temp_dir) / 'test.duckdb'
        assert db_path.exists()

        # Verify tables exist
        tables = db_manager.get_tables()
        assert 'lucidlink_files' in tables
        assert 'direct_links' in tables

        # Verify schema
        schema = db_manager.get_schema('lucidlink_files')
        required_columns = {
            'id', 'name', 'relative_path', 'filepath',
            'size_bytes', 'modified_time', 'direct_link',
            'last_seen'
        }
        assert all(col in schema for col in required_columns)

    def test_file_entry_crud(self, db_manager):
        """Test basic CRUD operations for file entries"""
        test_file = {
            'id': 'test123',
            'name': 'test.txt',
            'relative_path': '/test/path',
            'filepath': '/clean/test/path',
            'size_bytes': 1024,
            'modified_time': datetime.now(),
            'direct_link': None,
            'last_seen': datetime.now()
        }

        # Create
        db_manager.insert_file(test_file)
        
        # Read
        result = db_manager.get_file_by_path('/test/path')
        assert result['id'] == test_file['id']
        assert result['name'] == test_file['name']
        
        # Update
        updated_size = 2048
        db_manager.update_file_size('/test/path', updated_size)
        result = db_manager.get_file_by_path('/test/path')
        assert result['size_bytes'] == updated_size
        
        # Delete
        db_manager.delete_file('/test/path')
        result = db_manager.get_file_by_path('/test/path')
        assert result is None

    def test_check_missing_files_behavior(self, db_manager, config):
        """Test behavior of check_missing_files option"""
        # Add a test file
        test_file = {
            'id': 'test123',
            'name': 'test.txt',
            'relative_path': '/test/path',
            'filepath': '/clean/test/path',
            'size_bytes': 1024,
            'modified_time': datetime.now(),
            'direct_link': None,
            'last_seen': datetime.now()
        }
        db_manager.insert_file(test_file)

        # Test with check_missing_files = True
        config['check_missing_files'] = True
        db_manager.cleanup_missing_files([])  # Empty list means all files are missing
        result = db_manager.get_file_by_path('/test/path')
        assert result is None  # File should be removed

        # Test with check_missing_files = False
        config['check_missing_files'] = False
        db_path = db_manager.db_path
        db_manager.cleanup_missing_files([])
        assert not os.path.exists(db_path)  # Database should be deleted

    def test_schema_migration(self, db_manager):
        """Test schema migration behavior"""
        # Test adding new column
        db_manager.execute("ALTER TABLE lucidlink_files DROP COLUMN IF EXISTS test_col")
        assert 'test_col' not in db_manager.get_schema('lucidlink_files')
        
        needs_update = db_manager.needs_schema_update()
        if needs_update:
            db_manager.update_schema()
        
        # Verify schema is intact after update
        schema = db_manager.get_schema('lucidlink_files')
        required_columns = {
            'id', 'name', 'relative_path', 'filepath',
            'size_bytes', 'modified_time', 'direct_link',
            'last_seen'
        }
        assert all(col in schema for col in required_columns)

    def test_temp_directory_behavior(self, db_manager, temp_dir):
        """Test temporary directory handling"""
        temp_path = Path(temp_dir) / 'tmp'
        assert temp_path.exists()  # Temp directory should be created

        # Test bulk insert to trigger temp file creation
        test_files = [
            {
                'id': f'test{i}',
                'name': f'test{i}.txt',
                'relative_path': f'/test/path{i}',
                'filepath': f'/clean/test/path{i}',
                'size_bytes': 1024 * i,
                'modified_time': datetime.now(),
                'direct_link': None,
                'last_seen': datetime.now()
            }
            for i in range(1000)
        ]
        db_manager.bulk_insert(test_files)

        # Verify temp files are created and cleaned up
        temp_files = list(temp_path.glob('*'))
        assert len(temp_files) > 0  # Should have temp files during operation

        db_manager.cleanup()
        temp_files = list(temp_path.glob('*'))
        assert len(temp_files) == 0  # Should clean up temp files

    def test_concurrent_access(self, db_manager):
        """Test database behavior under concurrent access"""
        import threading
        
        def worker(worker_id):
            for i in range(100):
                test_file = {
                    'id': f'test{worker_id}_{i}',
                    'name': f'test{i}.txt',
                    'relative_path': f'/test/path{worker_id}_{i}',
                    'filepath': f'/clean/test/path{worker_id}_{i}',
                    'size_bytes': 1024,
                    'modified_time': datetime.now(),
                    'direct_link': None,
                    'last_seen': datetime.now()
                }
                db_manager.insert_file(test_file)

        threads = []
        for i in range(5):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Verify all records were inserted
        count = db_manager.execute("SELECT COUNT(*) FROM lucidlink_files")[0][0]
        assert count == 500  # 5 threads * 100 inserts each
