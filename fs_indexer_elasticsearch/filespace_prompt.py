#!/usr/bin/env python3

import json
import subprocess
import sys

def get_filespace_info():
    """
    Run 'lucid list --json' command and process the output to get filespace info.
    Returns tuple of (filespace_name, port) where filespace_name has '.' replaced with '-'
    """
    try:
        # Run the lucid list command and capture output
        result = subprocess.run(['lucid', 'list', '--json'], 
                              capture_output=True, 
                              text=True, 
                              check=True)
        
        # Parse the JSON output
        filespaces = json.loads(result.stdout)
        
        if not filespaces:
            print("Error: No filespaces found")
            sys.exit(1)
            
        if len(filespaces) == 1:
            # Single filespace case
            filespace = filespaces[0]
            filespace_name = filespace['filespace'].replace('.', '-')
            port = filespace['port']
            return filespace_name, port
        else:
            # Multiple filespaces - prompt user to choose
            print("\nAvailable filespaces:")
            for idx, fs in enumerate(filespaces, 1):
                print(f"{idx}. {fs['filespace']} (port: {fs['port']})")
                
            while True:
                try:
                    choice = input("\nEnter the number of the filespace to index (1-{}): ".format(len(filespaces)))
                    choice_idx = int(choice) - 1
                    
                    if 0 <= choice_idx < len(filespaces):
                        selected = filespaces[choice_idx]
                        filespace_name = selected['filespace'].replace('.', '-')
                        port = selected['port']
                        return filespace_name, port
                    else:
                        print("Invalid choice. Please enter a number between 1 and", len(filespaces))
                except ValueError:
                    print("Invalid input. Please enter a number.")
                    
    except subprocess.CalledProcessError as e:
        print(f"Error running 'lucid list': {e}")
        print(f"stderr: {e.stderr}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON output: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

def main():
    filespace_name, port = get_filespace_info()
    print(f"\nSelected filespace:")
    print(f"filespace-1: {filespace_name}")
    print(f"port: {port}")

if __name__ == "__main__":
    main()
