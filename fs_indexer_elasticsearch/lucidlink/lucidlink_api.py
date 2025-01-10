#!/usr/bin/env python3

import logging
import asyncio
import aiohttp
from datetime import datetime, timezone
from typing import Dict, List, Generator, Any, Optional
from urllib.parse import quote
import time
import fnmatch

logger = logging.getLogger(__name__)

class LucidLinkAPI:
    """Handler for LucidLink Filespace API interactions"""
    
    def __init__(self, port: int, mount_point: str, max_workers: int = 10, version: int = 1, filespace: str = None, v3_settings: Dict[str, Any] = None, get_direct_links: bool = True):
        """Initialize the API handler with the filespace port and mount point"""
        self.base_url = f"http://127.0.0.1:{port}/files"
        self.mount_point = mount_point
        self.port = port
        self._request_semaphore = None
        self.session = None
        self._max_workers = max_workers
        self.get_direct_links = get_direct_links
        
        # Use v3 settings if provided
        if v3_settings:
            self._max_concurrent_requests = v3_settings.get('max_concurrent_requests', 10)  # Default to 10 for API limits
            self._retry_attempts = v3_settings.get('retry_attempts', 5)
            self._retry_delay = v3_settings.get('retry_delay_seconds', 0.5)
            self._batch_size = v3_settings.get('batch_size', 100000)  # Match config default
            self._queue_size = v3_settings.get('queue_size', 100000)  # Match batch size
        else:
            self._max_concurrent_requests = 10  # Default to 10 for API limits
            self._retry_attempts = 5
            self._retry_delay = 0.5  # seconds
            self._batch_size = 100000  # Match config default
            self._queue_size = 100000  # Match batch size
            
        self.version = version
        self._filespace = filespace  # Store raw filespace name
        self._seen_paths = set()  # Track seen paths to avoid duplicates
        self._dir_cache = {}  # Cache for directory contents
        self._cache_ttl = 60  # Cache TTL in seconds
        
    async def __aenter__(self):
        """Async context manager entry"""
        # Initialize session if not already done
        if not self.session:
            conn = aiohttp.TCPConnector(
                limit=50,
                ttl_dns_cache=300,
                limit_per_host=50
            )
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self.session = aiohttp.ClientSession(
                connector=conn,
                timeout=timeout,
                raise_for_status=True
            )
            self._request_semaphore = asyncio.Semaphore(50)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
            self.session = None
            
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
                        return data
            except Exception as e:
                if attempt == self._retry_attempts - 1:
                    logger.error(f"Failed to make request to {url} after {self._retry_attempts} attempts: {e}")
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
                item['fsentry_id'] = item.get('id')  # Store the LucidLink fsEntry ID
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
            return 50  # More parallel at top level
        elif depth <= 3:
            return 30  # Medium parallelism for middle levels
        else:
            return 20  # Less parallelism for deep directories
            
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
                    if self._should_skip_path(item_path, skip_directories):
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
                not self._should_skip_path(item['name'], skip_directories)
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
                    if self._should_skip_path(item_path, skip_directories):
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
                not self._should_skip_path(item['name'], skip_directories)
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
        """Get direct link for a file based on the configured version"""
        if not self.get_direct_links:
            logger.debug(f"Direct link generation disabled, returning None for: {file_path}")
            return None
            
        if self.version == 2:
            return await self.get_direct_link_v2(file_path)
        else:
            return await self.get_direct_link_v3(file_path)
            
    async def get_direct_link_v2(self, file_path: str, fsentry_id: str = None) -> Optional[str]:
        """Get direct link for a file using v2 API endpoint
        
        Args:
            file_path: Path to the file
            fsentry_id: Optional DuckDB ID to use directly instead of making an API call
        """
        try:
            if fsentry_id:
                # Use provided fsentry_id directly - fast path
                if not self._filespace:
                    logger.error("Filespace name not set")
                    return None
                    
                direct_link = f"lucid://{self._filespace}/file/{fsentry_id}"
                logger.debug(f"Generated v2 direct link using provided ID for {file_path}: {direct_link}")
                return direct_link
                
            # Fallback to API call if no ID provided - slow path
            file_path = self._get_relative_path(file_path)
            encoded_path = quote(file_path)
            
            # Get the fsEntry ID from the API
            url = f"http://127.0.0.1:{self.port}/fsEntry?path={encoded_path}"
            async with self.session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                
                if not data or 'id' not in data:
                    logger.error(f"Failed to get fsEntry ID for {file_path}")
                    return None
                    
                # Construct the direct link using the fsEntry ID
                fsentry_id = data['id']
                if not self._filespace:
                    logger.error("Filespace name not set")
                    return None
                    
                direct_link = f"lucid://{self._filespace}/file/{fsentry_id}"
                logger.debug(f"Generated v2 direct link via API for {file_path}: {direct_link}")
                return direct_link
                
        except Exception as e:
            logger.error(f"Error generating v2 direct link for {file_path}: {e}")
            return None

    async def get_direct_link_v3(self, file_path: str) -> Optional[str]:
        """Get direct link for a file or directory using v3 API endpoint"""
        try:
            if not self.session:
                # Initialize session if not already done
                conn = aiohttp.TCPConnector(
                    limit=50,
                    ttl_dns_cache=300,
                    limit_per_host=50
                )
                timeout = aiohttp.ClientTimeout(total=30, connect=10)
                self.session = aiohttp.ClientSession(
                    connector=conn,
                    timeout=timeout,
                    raise_for_status=True
                )
                self._request_semaphore = asyncio.Semaphore(50)

            file_path = self._get_relative_path(file_path)  # Convert to relative path
            encoded_path = quote(file_path)
            # Use v3 API format with query parameter
            url = f"http://127.0.0.1:{self.port}/fsEntry/direct-link?path={encoded_path}"
            
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

    @property
    def filespace(self) -> Optional[str]:
        """Get the filespace name"""
        return self._filespace

    @filespace.setter
    def filespace(self, value: str):
        """Set the filespace name"""
        self._filespace = value

    def get_all_files(self) -> List[Dict[str, Any]]:
        """Get all files and directories that were traversed"""
        return self._all_files

    def _should_skip_path(self, path: str, skip_patterns: List[str]) -> bool:
        """Check if a path should be skipped based on skip patterns"""
        if not skip_patterns:
            return False
            
        # Clean up path for consistent matching
        clean_path = path.strip('/')
        
        for pattern in skip_patterns:
            pattern = pattern.strip('/')
            if fnmatch.fnmatch(clean_path, pattern) or \
               fnmatch.fnmatch(clean_path, f"*/{pattern}") or \
               fnmatch.fnmatch(clean_path, f"{pattern}/*") or \
               fnmatch.fnmatch(clean_path, f"*/{pattern}/*"):
                logger.debug(f"Skipping {path} due to pattern {pattern}")
                return True
        return False
