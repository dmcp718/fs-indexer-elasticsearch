def format_size(size_bytes: int) -> str:
    """Format size in bytes to human readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Human readable size string (e.g., "1.5 GB")
    """
    if not size_bytes:
        return "0 B"
        
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    size = float(size_bytes)
    unit_index = 0
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    # Format with 1 decimal place if size is not whole number
    if size.is_integer():
        return f"{int(size)} {units[unit_index]}"
    else:
        return f"{size:.1f} {units[unit_index]}"
