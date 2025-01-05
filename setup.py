from setuptools import setup, find_packages

setup(
    name="fs_indexer_elasticsearch",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "elasticsearch>=8.11.1",
        "duckdb>=0.9.0",
        "xxhash>=3.0.0",
        "pyyaml>=6.0.0",
        "python-dateutil>=2.8.2",
        "pytz>=2023.3",
        "aiohttp>=3.9.0",
        "requests>=2.31.0",
        "pyarrow>=14.0.1",
    ],
    entry_points={
        "console_scripts": [
            "fs-indexer=fs_indexer_elasticsearch.main:main",
        ],
    },
)
