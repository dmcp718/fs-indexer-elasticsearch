import os
import pytest
from unittest.mock import patch, Mock, AsyncMock, MagicMock
from fs_indexer_elasticsearch.main import main

class MockAsyncIterator:
    """Mock async iterator for scan results."""
    def __init__(self, entries):
        self.entries = entries
        self.index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.index >= len(self.entries):
            raise StopAsyncIteration
        entry = self.entries[self.index]
        self.index += 1
        return entry

async def mock_scan_generator(root_path):
    """Create an async generator for mock scan results."""
    entries = [
        {"type": "file", "relative_path": "file1.txt", "size_bytes": 100},
        {"type": "directory", "relative_path": "dir1", "size_bytes": 0},
        {"type": "file", "relative_path": "dir1/file2.txt", "size_bytes": 200},
        {"type": "directory", "relative_path": "dir2", "size_bytes": 0},
        {"type": "file", "relative_path": "dir2/file3.txt", "size_bytes": 300}
    ]
    return MockAsyncIterator(entries)

class AsyncIterator:
    def __init__(self, entries):
        self.entries = entries
        self.index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            result = self.entries[self.index]
            self.index += 1
            return result
        except IndexError:
            raise StopAsyncIteration

@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory with some test files."""
    # Create test files and directories
    file1 = tmp_path / "file1.txt"
    file1.write_text("test content")
    
    dir1 = tmp_path / "dir1"
    dir1.mkdir()
    file2 = dir1 / "file2.txt"
    file2.write_text("test content")
    
    dir2 = tmp_path / "dir2"
    dir2.mkdir()
    file3 = dir2 / "file3.txt"
    file3.write_text("test content")
    
    return str(tmp_path)

@pytest.fixture
def mock_elasticsearch():
    """Mock ElasticsearchClient."""
    with patch("fs_indexer_elasticsearch.main.ElasticsearchClient") as mock:
        mock_instance = Mock()
        mock_instance.bulk_index = Mock()
        mock_instance.delete_by_ids = Mock()
        mock.return_value = mock_instance
        yield mock_instance

@pytest.fixture
def mock_lucidlink():
    """Mock LucidLinkAPI."""
    with patch("fs_indexer_elasticsearch.main.LucidLinkAPI") as mock:
        mock_instance = AsyncMock()
        mock_instance.__aenter__.return_value = mock_instance
        mock.return_value = mock_instance
        yield mock_instance

@pytest.fixture
def mock_args():
    """Mock command line arguments."""
    with patch("argparse.ArgumentParser.parse_args") as mock:
        args = Mock()
        args.config = "config/indexer-config.yaml"
        args.root_path = None
        mock.return_value = args
        yield mock

def mock_scan_generator(root_path):
    """Create a generator for mock scan results."""
    entries = [
        {"type": "file", "relative_path": "file1.txt", "size_bytes": 100},
        {"type": "directory", "relative_path": "dir1", "size_bytes": 0},
        {"type": "file", "relative_path": "dir1/file2.txt", "size_bytes": 200},
        {"type": "directory", "relative_path": "dir2", "size_bytes": 0},
        {"type": "file", "relative_path": "dir2/file3.txt", "size_bytes": 300}
    ]
    for entry in entries:
        yield entry

@pytest.fixture
def mock_scanner():
    """Mock FileScanner."""
    with patch("fs_indexer_elasticsearch.main.FileScanner") as mock:
        mock_instance = AsyncMock()
        mock_instance.setup_database = Mock()
        mock_instance.scan = Mock()
        mock_instance.scan.return_value = mock_scan_generator(None)
        mock_instance.cleanup_missing_files.return_value = 0
        mock_instance.get_removed_file_ids.return_value = []
        mock.return_value = mock_instance
        yield mock_instance

@pytest.fixture
def mock_workflow_stats():
    """Mock WorkflowStats."""
    with patch("fs_indexer_elasticsearch.main.WorkflowStats") as mock:
        mock_instance = Mock()
        mock_instance.update_stats = Mock()
        mock_instance.add_removed_files = Mock()
        mock.return_value = mock_instance
        yield mock_instance

@pytest.fixture
def mock_direct_link_manager():
    """Mock DirectLinkManager."""
    with patch("fs_indexer_elasticsearch.main.DirectLinkManager") as mock:
        mock_instance = AsyncMock()
        mock_instance.process_batch = AsyncMock()
        mock_instance.setup_database = AsyncMock()
        mock.return_value = mock_instance
        yield mock_instance

def create_config(
    lucidlink_enabled=False,
    get_direct_links=False,
    lucidlink_version=3,
    mode="elasticsearch",
    root_path=None
):
    """Create a test configuration."""
    config = {
        "mode": mode,
        "root_path": root_path,
        "lucidlink_filespace": {
            "enabled": lucidlink_enabled,
            "get_direct_links": get_direct_links,
            "lucidlink_version": lucidlink_version,
            "name": "test-filespace",
            "raw_name": "test_filespace",
            "port": 9778 if lucidlink_version == 3 else 8280,
            "mount_point": "/mnt/lucidlink",
            "v3_settings": {
                "max_concurrent_requests": 4,
                "retry_attempts": 5,
                "retry_delay_seconds": 0.5,
                "batch_size": 50000,
                "queue_size": 50000
            }
        },
        "elasticsearch": {
            "host": "localhost",
            "port": 9200,
            "index_name": "filespace",
            "username": "",
            "password": ""
        },
        "performance": {
            "batch_sizes": {
                "direct_links": 50000,
                "elasticsearch": 1000
            },
            "queue_sizes": {
                "direct_links": 50000
            }
        }
    }
    return config

@pytest.mark.asyncio
async def test_lucidlink_disabled_elasticsearch_mode(temp_dir, mock_elasticsearch, mock_scanner, mock_args, mock_workflow_stats):
    """Test with LucidLink disabled in elasticsearch mode."""
    config = create_config(
        lucidlink_enabled=False,
        mode="elasticsearch",
        root_path=temp_dir
    )
    
    with patch("fs_indexer_elasticsearch.main.load_config", return_value=config), \
         patch("fs_indexer_elasticsearch.main.KibanaDataViewManager"):
        result = await main()
        
    assert result == 0
    mock_elasticsearch.bulk_index.assert_called_once()
    mock_elasticsearch.delete_by_ids.assert_called_once()
    mock_scanner.scan.assert_called_once_with(root_path=temp_dir)

@pytest.mark.asyncio
async def test_lucidlink_disabled_index_only_mode(temp_dir, mock_elasticsearch, mock_scanner, mock_args, mock_workflow_stats):
    """Test with LucidLink disabled in index-only mode."""
    config = create_config(
        lucidlink_enabled=False,
        mode="index-only",
        root_path=temp_dir
    )
    
    with patch("fs_indexer_elasticsearch.main.load_config", return_value=config):
        result = await main()
        
    assert result == 0
    mock_elasticsearch.bulk_index.assert_not_called()
    mock_elasticsearch.delete_by_ids.assert_not_called()
    mock_scanner.scan.assert_called_once_with(root_path=temp_dir)

@pytest.mark.asyncio
async def test_lucidlink_v2_with_direct_links(temp_dir, mock_elasticsearch, mock_lucidlink, mock_scanner, mock_args, mock_workflow_stats, mock_direct_link_manager):
    """Test with LucidLink v2 enabled with direct links."""
    config = create_config(
        lucidlink_enabled=True,
        get_direct_links=True,
        lucidlink_version=2,
        mode="elasticsearch",
        root_path=temp_dir
    )
    
    with patch("fs_indexer_elasticsearch.main.load_config", return_value=config), \
         patch("fs_indexer_elasticsearch.main.KibanaDataViewManager"):
        result = await main()
        
    assert result == 0
    mock_elasticsearch.bulk_index.assert_called_once()
    mock_elasticsearch.delete_by_ids.assert_called_once()
    mock_scanner.scan.assert_called_once_with(root_path=temp_dir)
    assert mock_lucidlink.__aenter__.called
    assert mock_lucidlink.__aexit__.called

@pytest.mark.asyncio
async def test_lucidlink_v2_without_direct_links(temp_dir, mock_elasticsearch, mock_lucidlink, mock_scanner, mock_args, mock_workflow_stats):
    """Test with LucidLink v2 enabled without direct links."""
    config = create_config(
        lucidlink_enabled=True,
        get_direct_links=False,
        lucidlink_version=2,
        mode="elasticsearch",
        root_path=temp_dir
    )
    
    with patch("fs_indexer_elasticsearch.main.load_config", return_value=config), \
         patch("fs_indexer_elasticsearch.main.KibanaDataViewManager"):
        result = await main()
        
    assert result == 0
    mock_elasticsearch.bulk_index.assert_called_once()
    mock_elasticsearch.delete_by_ids.assert_called_once()
    mock_scanner.scan.assert_called_once_with(root_path=temp_dir)
    assert not mock_lucidlink.__aenter__.called
    assert not mock_lucidlink.__aexit__.called

@pytest.mark.asyncio
async def test_lucidlink_v3_with_direct_links(temp_dir, mock_elasticsearch, mock_lucidlink, mock_scanner, mock_args, mock_workflow_stats, mock_direct_link_manager):
    """Test with LucidLink v3 enabled with direct links."""
    config = create_config(
        lucidlink_enabled=True,
        get_direct_links=True,
        lucidlink_version=3,
        mode="elasticsearch",
        root_path=temp_dir
    )
    
    with patch("fs_indexer_elasticsearch.main.load_config", return_value=config), \
         patch("fs_indexer_elasticsearch.main.KibanaDataViewManager"):
        result = await main()
        
    assert result == 0
    mock_elasticsearch.bulk_index.assert_called_once()
    mock_elasticsearch.delete_by_ids.assert_called_once()
    mock_scanner.scan.assert_called_once_with(root_path=temp_dir)
    assert mock_lucidlink.__aenter__.called
    assert mock_lucidlink.__aexit__.called

@pytest.mark.asyncio
async def test_lucidlink_v3_without_direct_links(temp_dir, mock_elasticsearch, mock_lucidlink, mock_scanner, mock_args, mock_workflow_stats):
    """Test with LucidLink v3 enabled without direct links."""
    config = create_config(
        lucidlink_enabled=True,
        get_direct_links=False,
        lucidlink_version=3,
        mode="elasticsearch",
        root_path=temp_dir
    )
    
    with patch("fs_indexer_elasticsearch.main.load_config", return_value=config), \
         patch("fs_indexer_elasticsearch.main.KibanaDataViewManager"):
        result = await main()
        
    assert result == 0
    mock_elasticsearch.bulk_index.assert_called_once()
    mock_elasticsearch.delete_by_ids.assert_called_once()
    mock_scanner.scan.assert_called_once_with(root_path=temp_dir)
    assert not mock_lucidlink.__aenter__.called
    assert not mock_lucidlink.__aexit__.called

@pytest.mark.asyncio
async def test_lucidlink_v2_index_only_mode(temp_dir, mock_elasticsearch, mock_lucidlink, mock_scanner, mock_args, mock_workflow_stats, mock_direct_link_manager):
    """Test with LucidLink v2 in index-only mode."""
    config = create_config(
        lucidlink_enabled=True,
        get_direct_links=True,
        lucidlink_version=2,
        mode="index-only",
        root_path=temp_dir
    )
    
    with patch("fs_indexer_elasticsearch.main.load_config", return_value=config):
        result = await main()
        
    assert result == 0
    mock_elasticsearch.bulk_index.assert_not_called()
    mock_elasticsearch.delete_by_ids.assert_not_called()
    mock_scanner.scan.assert_called_once_with(root_path=temp_dir)
    assert mock_lucidlink.__aenter__.called
    assert mock_lucidlink.__aexit__.called

@pytest.mark.asyncio
async def test_lucidlink_v3_index_only_mode(temp_dir, mock_elasticsearch, mock_lucidlink, mock_scanner, mock_args, mock_workflow_stats, mock_direct_link_manager):
    """Test with LucidLink v3 in index-only mode."""
    config = create_config(
        lucidlink_enabled=True,
        get_direct_links=True,
        lucidlink_version=3,
        mode="index-only",
        root_path=temp_dir
    )
    
    with patch("fs_indexer_elasticsearch.main.load_config", return_value=config):
        result = await main()
        
    assert result == 0
    mock_elasticsearch.bulk_index.assert_not_called()
    mock_elasticsearch.delete_by_ids.assert_not_called()
    mock_scanner.scan.assert_called_once_with(root_path=temp_dir)
    assert mock_lucidlink.__aenter__.called
    assert mock_lucidlink.__aexit__.called
