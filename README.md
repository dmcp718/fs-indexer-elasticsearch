# FS Indexer

A high-performance file system indexer with DuckDB backend. This tool efficiently scans and indexes file system metadata using asynchronous I/O and smart caching strategies. File index and metadata are sent to Elasticsearch for search and analysis. 

## Features

- Fast file system scanning with async/await
- DuckDB database for high-performance storage and querying
- Connection pooling and rate limiting
- Configurable batch processing
- Detailed progress tracking and performance metrics

## Installation

1. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate
```

2. Install the package:
```bash
pip install -e .
```

## Configuration

Configure the indexer using `indexer-config.yaml`. Key settings include:

### Performance Settings
- DuckDB memory limit (default: 85% of system RAM)
- Number of threads (default: 16)
- Batch size for database operations (default: 100,000)
- Read buffer size and mmap settings
- LZ4 compression for better space efficiency

### API Settings
- Maximum concurrent requests (default: 5)
- Cache TTL for directory contents (default: 60s)
- Prefetch depth and batch size
- Retry attempts and backoff delay

### Skip Patterns
- Directory patterns to skip
- File patterns to skip
- Extension-based filtering

### LucidLink Configuration

The indexer can be configured to work with LucidLink filespaces:

```yaml
lucidlink_filespace:
  enabled: true  # Enable LucidLink integration
  skip_direct_links: false  # Set to true to skip generating direct links (faster indexing)
```

#### Direct Links

By default, the indexer generates direct links for all files. This allows for direct access to files but can slow down the indexing process. You can disable direct link generation by setting `skip_direct_links: true` in the config file.

Benefits of skipping direct links:
- Faster indexing performance
- Reduced API load
- Lower memory usage

Note that when `skip_direct_links` is enabled, the `direct_link` field in the database will be set to `null` for all files.

## Usage

### Basic Usage

```bash
# Show help
python -m fs_indexer_elasticsearch.main --help

# Index a directory
python -m fs_indexer_elasticsearch.main --root-path /path/to/index

# Use a custom config file
python -m fs_indexer_elasticsearch.main --config /path/to/config.yaml --root-path /path/to/index
```

The indexer will display progress and performance metrics during operation, including:
- Files processed per second (~25K/s on modern hardware)
- Total size of indexed files
- Number of files updated/skipped/removed
- Any errors encountered

## Performance Optimizations

The indexer uses several strategies to achieve high performance:

1. **Async I/O Operations**
   - Async/await for API calls
   - Connection pooling and rate limiting
   - Retry logic with exponential backoff

2. **Smart Caching**
   - Directory content caching with TTL
   - Intelligent prefetching of child directories
   - Memory-efficient batch processing

3. **DuckDB Optimizations**
   - Memory-mapped I/O
   - LZ4 compression
   - Parallel query execution
   - Optimized batch upserts

4. **Resource Management**
   - Platform-optimized multiprocessing (fork on Linux, spawn on macOS)
   - Dynamic batch sizing based on directory depth
   - Controlled prefetching to prevent API overload
   - Efficient memory usage with generators
   - Automatic worker count adjustment based on system resources
   - Robust error handling and process recovery

### Command Line Arguments

- `--root-path PATH`: Specify the root directory to index
- `--config PATH`: Optional path to custom config file
- `--mode MODE`: Operation mode (choices: 'elasticsearch', 'index-only')
  - `elasticsearch`: Run indexer and send records to Elasticsearch (default)
  - `index-only`: Run indexer only, do not send records to Elasticsearch

### Operation Modes

The indexer can run in two modes:

1. **Elasticsearch Mode** (default)
   - Indexes files into DuckDB
   - Sends indexed records to Elasticsearch
   - Enables full-text search and analytics

2. **Index-Only Mode**
   - Only indexes files into DuckDB
   - Skips Elasticsearch integration
   - Useful for:
     - Initial indexing of large directories
     - Testing indexing performance
     - Scenarios where Elasticsearch is not needed

You can set the mode in two ways:
1. In the config file:
   ```yaml
   mode: elasticsearch  # or 'index-only'
   ```
2. Via command line argument (overrides config file):
   ```bash
   ./fs-indexer-es --mode index-only
   ```

## Building Standalone Executables

To create a self-contained executable for your platform:

1. Install development dependencies:
```bash
pip install -e ".[dev]"
```

2. Run the build script:
```bash
./build_app.py
```

This will create a platform-specific release package in the `dist/fs-indexer-es-{platform}` directory containing:
- The standalone executable
- Configuration file (indexer-config.yaml)
- Run script (run-indexer.sh)

The executable can be run on any machine of the same platform without requiring Python or any dependencies to be installed.

To run the standalone version:
```bash
cd dist/fs-indexer-es-{platform}
./run-indexer.sh
