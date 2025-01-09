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

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Verify installation:
```bash
python -m fs_indexer_elasticsearch.main --help
```

## Configuration

Configure the indexer using `indexer-config.yaml`. Key settings include:

### Performance Settings
- DuckDB memory limit (default: 85% of system RAM)
- Number of threads (default: 10)
- Batch size for database operations (default: 100,000)
- Read buffer size and mmap settings

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
  lucidlink_version: 3  # LucidLink API version (2 or 3)
  port: 9778  # LucidLink API port
```

#### Direct Links

Direct link generation is controlled by the operating mode:
- `elasticsearch` mode (default): Generates direct links and sends data to Elasticsearch
- `index-only` mode: Skips direct link generation and Elasticsearch integration

Benefits of using `index-only` mode:
- Faster indexing performance
- Reduced API load
- Lower memory usage

Note that in `index-only` mode, the `direct_link` field in the database will be set to `null` for all files.

## Usage

The indexer provides a simple command-line interface:

```bash
usage: fs-indexer [-h] [--config CONFIG] [--root-path PATH] [--version VER] [--mode MODE]

options:
  -h, --help        show this help message and exit
  --config CONFIG   Path to configuration file (default: config/indexer-config.yaml)

indexing options:
  --root-path PATH  Root path to start indexing from. If not provided, will use path from config
  --version VER     LucidLink version to use (2 or 3). Overrides config setting
  --mode MODE       Operating mode: elasticsearch (default) or index-only
```

### Examples

```bash
# Index files using config file settings
fs-indexer --config config/indexer-config.yaml

# Index specific directory with LucidLink v2
fs-indexer --root-path /Volumes/filespace/path --version 2

# Index files without Elasticsearch (index-only mode)
fs-indexer --root-path /path/to/index --mode index-only
```

The indexer will display progress and performance metrics during operation, including:
- Files processed per second (~25K/s on modern hardware)
- Total size of indexed files
- Number of files updated/skipped/removed
- Any errors encountered

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

## Running with Docker Compose

For development and testing, you can use Docker Compose to run Elasticsearch and Kibana:

```bash
# Start Elasticsearch and Kibana
docker compose -f docker-compose/docker-compose.yml up -d

# Wait for services to be ready (usually takes about 30 seconds)
# You can check the status at:
# - Elasticsearch: http://localhost:9200
# - Kibana: http://localhost:5601

# Run the indexer with Elasticsearch integration
python -m fs_indexer_elasticsearch.main --root-path /path/to/index --mode elasticsearch
```

The Docker Compose setup includes:
- Elasticsearch with proper settings for development
- Kibana with pre-configured data views
- Automatic index pattern creation
- Health checks for both services

To stop the services:
```bash
docker compose -f docker-compose/docker-compose.yml down
```

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
   - Parallel query execution
   - Optimized batch upserts

4. **Resource Management**
   - Platform-optimized multiprocessing (fork on Linux, spawn on macOS)
   - Dynamic batch sizing based on directory depth
   - Controlled prefetching to prevent API overload
   - Efficient memory usage with generators
   - Automatic worker count adjustment based on system resources
   - Robust error handling and process recovery

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
