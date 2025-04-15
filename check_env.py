"""
Environment Variable Checker

This script prints out all environment variables, focusing on OANDA-related ones.
Use this for debugging environment variable issues.
"""

import os
import sys
from pprint import pprint

def main():
    """Main function to print environment variables"""
    print("=" * 50)
    print("ENVIRONMENT VARIABLES DEBUG")
    print("=" * 50)
    
    # Get all environment variables
    env_vars = dict(os.environ)
    
    # Print OANDA-specific variables first (if they exist)
    oanda_vars = {k: v for k, v in env_vars.items() if 'OANDA' in k}
    if oanda_vars:
        print("\nOANDA-SPECIFIC VARIABLES:")
        print("-" * 30)
        for key, value in oanda_vars.items():
            # Mask part of the value if it's likely to be a token/key
            if 'TOKEN' in key or 'KEY' in key or 'SECRET' in key:
                # Show just the first and last 4 characters
                if len(value) > 8:
                    masked_value = value[:4] + '*' * (len(value) - 8) + value[-4:]
                else:
                    masked_value = '****'
                print(f"{key}: {masked_value}")
            else:
                print(f"{key}: {value}")
    else:
        print("\nNO OANDA-SPECIFIC VARIABLES FOUND!")
    
    # Print total count of environment variables
    print(f"\nTotal environment variables: {len(env_vars)}")
    
    # Print a few other important variables if they exist
    important_vars = ['PORT', 'RENDER', 'PYTHON_VERSION', 'PYTHONPATH', 'PATH']
    print("\nOTHER IMPORTANT VARIABLES:")
    print("-" * 30)
    for var in important_vars:
        if var in env_vars:
            print(f"{var}: {env_vars[var]}")
        else:
            print(f"{var}: NOT SET")
    
    # Option to print all variables if requested
    if len(sys.argv) > 1 and sys.argv[1] == '--all':
        print("\nALL ENVIRONMENT VARIABLES:")
        print("-" * 30)
        pprint(env_vars)
    else:
        print("\nRun with --all flag to see all environment variables")
    
    print("=" * 50)

if __name__ == "__main__":
    main() 