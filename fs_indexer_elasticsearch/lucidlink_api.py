#!/usr/bin/env python3

import logging
import asyncio
import aiohttp
from datetime import datetime, timezone
from typing import Dict, List, Generator, Any, Optional
from urllib.parse import quote
import time

logger = logging.getLogger(__name__)

class LucidLinkAPI:
    """Handler for LucidLink Filespace API interactions"""
    
    def __init__(self, port: int, mount_point: str, max_workers: int = 10):
        """Initialize the API handler with the filespace port and mount point"""
        self.base_url = f"http://127.0.0.1:{port}/files"
        self.mount_point = mount_point
        self.max_workers = max_workers
        self.session = None
        self._seen_paths = set()  # Track seen paths to avoid duplicates
        self._dir_cache = {}  # Cache for directory contents
        self._cache_ttl = 300  # Cache TTL in seconds
        self._request_semaphore = None  # For rate limiting
        self._max_concurrent_requests = 20  # Increased for directory-heavy structure
        self._retry_attempts = 3
        self._retry_delay = 1  # seconds
        
    async def __aenter__(self):
        """Async context manager entry"""
        conn = aiohttp.TCPConnector(
            limit=30,  # Increased for top-level parallelism
            ttl_dns_cache=300,
            limit_per_host=30
        )
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self.session = aiohttp.ClientSession(
            connector=conn,
            timeout=timeout,
            raise_for_status=True
        )
        self._request_semaphore = asyncio.Semaphore(30)  # Match connector limit
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
            
    def _convert_timestamp(self, ns_timestamp: int) -> datetime:
        """Convert nanosecond epoch timestamp to datetime object"""
        seconds = ns_timestamp / 1e9
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
        
    def _is_cache_valid(self, cache_entry):
        """Check if a cache entry is still valid"""
        if not cache_entry:
            return False
        cache_time, update_time, _ = cache_entry
        current_time = time.time()
        return (current_time - cache_time) < self._cache_ttl
        
    async def _make_request(self, path: str = "") -> Dict[str, Any]:
        """Make async HTTP request to the API with retries and rate limiting"""
        # Clean up path - remove leading/trailing slashes and normalize
        clean_path = path.strip('/').replace('//', '/')
        url = f"{self.base_url}/{quote(clean_path)}" if clean_path else self.base_url
        
        for attempt in range(self._retry_attempts):
            try:
                async with self._request_semaphore:
                    async with self.session.get(url) as response:
                        if response.status == 400:
                            # Log the problematic path and skip it
                            logger.warning(f"Skipping invalid path: {path}")
                            return {'items': []}  # Return empty result
                        response.raise_for_status()
                        data = await response.json()
                        logger.debug(f"API response for {path}: {data}")
                        return data
            except aiohttp.ClientError as e:
                if attempt == self._retry_attempts - 1:
                    logger.error(f"API request failed for path {path}: {str(e)}")
                    raise
                await asyncio.sleep(self._retry_delay * (attempt + 1))
                
    async def get_directory_contents(self, directory: str) -> List[Dict[str, Any]]:
        """Get contents of a specific directory with caching"""
        # Check cache first
        cache_entry = self._dir_cache.get(directory)
        if self._is_cache_valid(cache_entry):
            _, update_time, contents = cache_entry
            return contents
            
        try:
            data = await self._make_request(directory)
            for item in data:
                item['creation_time'] = self._convert_timestamp(item['creationTime'])
                item['update_time'] = self._convert_timestamp(item['updateTime'])
                item['type'] = item.get('type', '').lower()
                
            # Cache the results
            self._dir_cache[directory] = (time.time(), time.time(), data)
            
            return data
        except Exception as e:
            logger.error(f"Failed to get contents of directory {directory}: {str(e)}")
            raise
            
    async def get_top_level_directories(self) -> List[Dict[str, Any]]:
        """Get list of top-level directories"""
        try:
            data = await self._make_request()
            for item in data:
                item['creation_time'] = self._convert_timestamp(item['creationTime'])
                item['update_time'] = self._convert_timestamp(item['updateTime'])
                item['type'] = item.get('type', '').lower()
            return data
        except Exception as e:
            logger.error(f"Failed to get top-level directories: {str(e)}")
            raise
            
    async def _batch_get_directories(self, directories: List[str], semaphore: asyncio.Semaphore) -> Dict[str, List[Dict[str, Any]]]:
        """Get contents of multiple directories concurrently with rate limiting"""
        results = {}
        tasks = []
        
        for directory in directories:
            if directory in self._seen_paths:
                continue
            self._seen_paths.add(directory)
            
            tasks.append(self._get_directory_with_semaphore(directory, semaphore))
            
        if tasks:
            completed = await asyncio.gather(*tasks, return_exceptions=True)
            
            for directory, result in zip(directories, completed):
                if isinstance(result, Exception):
                    logger.error(f"Failed to get contents of {directory}: {str(result)}")
                    results[directory] = []
                else:
                    results[directory] = result
                    
        return results
        
    async def _get_directory_with_semaphore(self, directory: str, semaphore: asyncio.Semaphore) -> List[Dict[str, Any]]:
        """Get directory contents with rate limiting"""
        async with semaphore:
            return await self.get_directory_contents(directory)
            
    def _get_chunk_size(self, path: str) -> int:
        """Get optimal chunk size based on directory depth"""
        depth = path.count('/')
        if depth <= 1:
            return 30  # More parallel at top level
        elif depth <= 3:
            return 20  # Medium parallelism for middle levels
        else:
            return 10  # Less parallelism for deep directories
            
    async def traverse_filesystem(self, root_path: str = None, skip_directories: List[str] = None):
        """Traverse the filesystem and yield file/directory info"""
        skip_directories = skip_directories or []
        
        try:
            # Clean up root path
            if root_path:
                root_path = root_path.strip('/')
            
            # Start from root if no path specified
            current_path = root_path if root_path else ""
            
            # Get initial directory contents
            contents = await self.get_directory_contents(current_path)
            
            # Process all items first
            for item in contents:
                try:
                    # Clean up item path
                    item_path = item['name'].strip('/')
                    
                    # Skip if in skip patterns
                    if skip_directories and any(pattern in item_path for pattern in skip_directories):
                        logger.debug(f"Skipping {item_path} due to skip pattern")
                        continue
                        
                    yield item
                except Exception as e:
                    logger.error(f"Error processing item {item.get('name', 'unknown')}: {str(e)}")
                    continue
            
            # Then process directories in parallel
            directories = [
                item['name'] for item in contents 
                if item['type'] == 'directory' and 
                not any(pattern in item['name'] for pattern in skip_directories)
            ]
            
            if directories:
                # Process directories in parallel with adaptive chunk size
                chunk_size = self._get_chunk_size(current_path)
                for i in range(0, len(directories), chunk_size):
                    chunk = directories[i:i + chunk_size]
                    tasks = []
                    for directory in chunk:
                        if directory not in self._seen_paths:
                            self._seen_paths.add(directory)
                            tasks.append(asyncio.create_task(self._traverse_subdir(directory, skip_directories)))
                    
                    # Wait for chunk to complete
                    if tasks:
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        for items in results:
                            if isinstance(items, Exception):
                                logger.error(f"Error in directory traversal: {str(items)}")
                                continue
                            for item in items:
                                yield item
                
        except Exception as e:
            logger.error(f"Error traversing filesystem: {str(e)}")
            raise
            
    async def _traverse_subdir(self, directory: str, skip_directories: List[str]) -> List[Dict[str, Any]]:
        """Traverse a subdirectory and return all items"""
        try:
            items = []
            contents = await self.get_directory_contents(directory)
            
            # Process all items first
            for item in contents:
                try:
                    # Clean up item path
                    item_path = item['name'].strip('/')
                    
                    # Skip if in skip patterns
                    if skip_directories and any(pattern in item_path for pattern in skip_directories):
                        logger.debug(f"Skipping {item_path} due to skip pattern")
                        continue
                        
                    items.append(item)
                except Exception as e:
                    logger.error(f"Error processing item {item.get('name', 'unknown')}: {str(e)}")
                    continue
            
            # Then process subdirectories in parallel
            directories = [
                item['name'] for item in contents 
                if item['type'] == 'directory' and 
                not any(pattern in item['name'] for pattern in skip_directories)
            ]
            
            if directories:
                # Process directories in parallel in chunks
                chunk_size = self._get_chunk_size(directory)
                for i in range(0, len(directories), chunk_size):
                    chunk = directories[i:i + chunk_size]
                    tasks = []
                    for directory in chunk:
                        if directory not in self._seen_paths:
                            self._seen_paths.add(directory)
                            tasks.append(asyncio.create_task(self._traverse_subdir(directory, skip_directories)))
                    
                    # Wait for chunk to complete
                    if tasks:
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        for result in results:
                            if isinstance(result, Exception):
                                logger.error(f"Error in directory traversal: {str(result)}")
                                continue
                            items.extend(result)
            
            return items
            
        except Exception as e:
            logger.error(f"Error traversing subdirectory {directory}: {str(e)}")
            raise

    async def health_check(self) -> bool:
        """Check if the LucidLink API is available"""
        try:
            # Use shorter timeout for health check
            url = self.base_url
            timeout = aiohttp.ClientTimeout(total=1)  # 1 second timeout
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    await response.json()
                    return True
        except Exception as e:
            logger.error(f"LucidLink API health check failed: {str(e)}")
            return False
            
    async def get_direct_link(self, file_path: str) -> Optional[str]:
        """Get a direct link for a file"""
        try:
            if not self.session:
                raise RuntimeError("Session not initialized")

            file_path = self._get_relative_path(file_path)  # Convert to relative path
            encoded_path = quote(file_path)
            url = f"{self.base_url}/direct-link/{encoded_path}"
            
            async with self._request_semaphore:
                async with self.session.get(url) as response:
                    if response.status == 400:
                        logger.warning(f"Failed to generate direct link for: {file_path} - Bad Request")
                        return None
                    
                    response.raise_for_status()
                    data = await response.json()
                    
                    # Extract the 'result' field
                    if 'result' not in data:
                        logger.warning(f"No result field in response for: {file_path}")
                        return None
                        
                    return data['result']
                    
        except Exception as e:
            logger.error(f"Error generating direct link for {file_path}: {str(e)}")
            return None

    def _get_relative_path(self, path: str) -> str:
        """Convert absolute path to relative path using mount point"""
        if path.startswith(self.mount_point):
            return path[len(self.mount_point):].lstrip('/')
        return path

    async def scan_directory(self, path: str) -> List[Dict[str, Any]]:
        """Scan a directory and return its contents"""
        try:
            if not self.session:
                raise RuntimeError("Session not initialized")

            # Ensure path is relative to mount point
            rel_path = self._get_relative_path(path)
            logger.debug(f"Converting path '{path}' to relative path '{rel_path}'")
            encoded_path = quote(rel_path)
            url = f"{self.base_url}/list/{encoded_path}"
            
            async with self._request_semaphore:
                async with self.session.get(url) as response:
                    if response.status == 400:
                        logger.warning(f"Failed to scan directory: {path} - Bad Request")
                        return []
                    
                    response.raise_for_status()
                    data = await response.json()
                    
                    # Extract the 'result' field
                    if 'result' not in data:
                        logger.warning(f"No result field in response for: {path}")
                        return []
                        
                    return data['result']
                    
        except Exception as e:
            logger.error(f"Error scanning directory {path}: {str(e)}")
            return []

    def get_all_files(self) -> List[Dict[str, Any]]:
        """Get all files and directories that were traversed"""
        return self._all_files
