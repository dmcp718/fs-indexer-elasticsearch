import os
import yaml
import logging
import logging.handlers

def load_config(config_path=None):
    """Load configuration from file."""
    # Define config locations upfront
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    config_locations = [
        os.path.join(base_dir, 'config', 'indexer-config.yaml'),  # Project config directory
        os.path.join(base_dir, 'indexer-config.yaml'),         # Current directory
        os.path.join(os.path.dirname(__file__), 'indexer-config.yaml')  # Package directory
    ]

    if not config_path:
        # Try locations in order
        for loc in config_locations:
            if os.path.exists(loc):
                config_path = loc
                break
    
    if not config_path or not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found in any of the expected locations: {config_locations}")
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

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
