#!/usr/bin/env python3

import json
import subprocess
import sys
import os
import yaml
import argparse

def load_config(config_path: str = None):
    """Load configuration from file."""
    if not config_path:
        # Try locations in order:
        config_locations = [
            'config/indexer-config.yaml',  # Project config directory
            'indexer-config.yaml',         # Current directory
            os.path.join(os.path.dirname(__file__), 'indexer-config.yaml')  # Package directory
        ]
        
        for loc in config_locations:
            if os.path.exists(loc):
                config_path = loc
                break
    
    if not config_path or not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found in any of the expected locations: {config_locations}")
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def get_lucidlink_mount(lucidlink_bin, instance_id):
    """
    Get the LucidLink mount point by running the status command.
    Returns the mount point path.
    """
    try:
        # Run the status command with instance ID
        result = subprocess.run([lucidlink_bin, '--instance', str(instance_id), 'status'], 
                              capture_output=True, 
                              text=True, 
                              check=True)
        
        # Parse the output to find mount point
        mount_point = None
        for line in result.stdout.split('\n'):
            if line.startswith('Mount point:'):
                mount_point = line.split(':', 1)[1].strip()
                break
                
        if not mount_point:
            print(f"Error: Could not find mount point in {lucidlink_bin} status output")
            sys.exit(1)
            
        return mount_point
        
    except subprocess.CalledProcessError as e:
        print(f"Error running '{lucidlink_bin} status': {e}")
        print(f"stderr: {e.stderr}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

def get_filespace_info(config=None, version=None):
    """
    Run lucidlink list command and process the output to get filespace info.
    Returns tuple of (filespace_raw, filespace_name, port, mount_point) where:
    - filespace_raw: Original filespace name with dots
    - filespace_name: Filespace name with dots replaced by dashes
    - port: Port number
    - mount_point: Mount point path
    
    Args:
        config (dict): Configuration dictionary (optional)
        version (int): LucidLink API version (2 or 3) (optional, overrides config)
    """
    try:
        # 1. Get version from config or use default
        if config is not None:
            version = config.get('lucidlink_filespace', {}).get('lucidlink_version', 3)
        else:
            version = version or 3
            
        # 2. Formulate lucidlink binary command
        lucidlink_bin = f"lucid{version}"
        
        # 3. Get list of filespaces
        result = subprocess.run([lucidlink_bin, 'list', '--json'], 
                              capture_output=True, 
                              text=True, 
                              check=True)
        
        # Parse the JSON output
        filespaces = json.loads(result.stdout)
        
        # 3.5 Check if empty
        if not filespaces:
            print("Error: No filespaces found")
            sys.exit(1)
            
        # 4. Handle single filespace case
        if len(filespaces) == 1:
            filespace = filespaces[0]
            filespace_raw = filespace['filespace']  # Keep original with dots
            filespace_name = filespace_raw.replace('.', '-')  # Replace dots with dashes
            port = filespace['port']
            instance_id = filespace['instanceId']  
            # 4.5 Get mount point
            mount_point = get_lucidlink_mount(lucidlink_bin, instance_id)
            # 5. Return values
            return filespace_raw, filespace_name, port, mount_point
        else:
            # 6. Handle multiple filespaces
            print("\nAvailable filespaces:")
            for idx, fs in enumerate(filespaces, 1):
                print(f"{idx}. {fs['filespace']} (port: {fs['port']})")
                
            while True:
                try:
                    choice = input("\nEnter the number of the filespace to index (1-{}): ".format(len(filespaces)))
                    choice_idx = int(choice) - 1
                    
                    if 0 <= choice_idx < len(filespaces):
                        # 7. Process user choice
                        selected = filespaces[choice_idx]
                        filespace_raw = selected['filespace']  # Keep original with dots
                        filespace_name = filespace_raw.replace('.', '-')  # Replace dots with dashes
                        port = selected['port']
                        instance_id = selected['instanceId']  
                        mount_point = get_lucidlink_mount(lucidlink_bin, instance_id)
                        return filespace_raw, filespace_name, port, mount_point
                    else:
                        print("Invalid choice. Please enter a number between 1 and", len(filespaces))
                except ValueError:
                    print("Invalid input. Please enter a number.")
                    
    except subprocess.CalledProcessError as e:
        print(f"Error running '{lucidlink_bin} list': {e}")
        print(f"stderr: {e.stderr}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON output: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='Get LucidLink filespace information')
    parser.add_argument('--version', type=int, choices=[2, 3], default=None,
                      help='LucidLink API version (2 or 3)')
    parser.add_argument('--config', type=str, default=None,
                      help='Path to config file')
    args = parser.parse_args()
    
    try:
        config = load_config(args.config) if not args.version else None
        filespace_raw, filespace_name, port, mount_point = get_filespace_info(config, args.version)
        print("\nSelected filespace:")
        print(f"filespace: {filespace_raw}")
        print(f"filespace-1: {filespace_name}")
        print(f"port: {port}")
        print(f"mount-point: {mount_point}")
        if args.version:
            print(f"version: {args.version}")
        elif config:
            print(f"version: {config.get('lucidlink_filespace', {}).get('lucidlink_version', 3)}")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
