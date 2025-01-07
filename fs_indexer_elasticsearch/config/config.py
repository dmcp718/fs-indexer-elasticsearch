import os
import sys
import yaml
import logging
import logging.handlers
from pathlib import Path

def get_base_dir():
    """Get the base directory for the application."""
    if getattr(sys, 'frozen', False):
        # If the application is run as a bundle (PyInstaller)
        base_dir = Path(sys._MEIPASS)
    else:
        # If the application is run from source
        base_dir = Path(__file__).parent.parent.parent

    return base_dir

def load_config(config_path=None):
    """Load configuration from YAML file."""
    if config_path:
        # If a specific config path is provided, use it
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found at specified path: {config_path}")
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    # Otherwise, look in default locations
    base_dir = get_base_dir()
    config_paths = [
        base_dir / 'config' / 'indexer-config.yaml',  # Look in config directory first
        base_dir / 'indexer-config.yaml',  # Then look at root level
    ]

    for path in config_paths:
        if path.exists():
            with open(path, 'r') as f:
                return yaml.safe_load(f)

    raise FileNotFoundError(f"Config file not found in any of these locations: {[str(p) for p in config_paths]}")

def init_logging(config):
    """Initialize logging configuration."""
    log_config = config.get('logging', {})
    log_level = getattr(logging, log_config.get('level', 'INFO'))
    log_format = log_config.get('format', '%(asctime)s - %(levelname)s - %(message)s')
    
    # Create formatter
    formatter = logging.Formatter(log_format)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Add file handler if configured
    log_file = log_config.get('file')
    if log_file:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=log_config.get('max_size', 10*1024*1024),  # Default 10MB
            backupCount=log_config.get('backup_count', 5)
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
