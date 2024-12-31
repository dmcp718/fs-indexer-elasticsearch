# fs-indexer-elasticsearch

A filesystem indexer that stores file metadata in Elasticsearch for fast searching and analysis.

## Features

- Indexes file metadata into Elasticsearch
- Supports incremental updates
- Tracks indexing metrics and errors
- Configurable logging and error handling
- Integration with LucidLink API

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure Elasticsearch:
- Edit `fs_indexer_elasticsearch/config/indexer-config.yaml` with your Elasticsearch connection details
- Default configuration assumes Elasticsearch running on localhost:9200 without authentication

3. Start Elasticsearch:
```bash
docker-compose up -d
```

## Usage

The indexer can be used as a library or run directly:

```python
from fs_indexer_elasticsearch import ElasticsearchClient

# Initialize client
client = ElasticsearchClient(
    host='localhost',
    port=9200,
    username='',
    password='',
    index_name='filespace',
    filespace='default'
)

# Index files
client.index_directory('/path/to/files')
```

## Configuration

See `indexer-config.yaml` for available configuration options:

- Elasticsearch connection settings
- Logging configuration
- Indexing batch size and intervals
- Excluded directories and files
