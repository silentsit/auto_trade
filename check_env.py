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
    
    # Print all environment variables
    print("=== All Environment Variables ===")
    for key, value in os.environ.items():
        # Mask sensitive values
        if any(sensitive in key.lower() for sensitive in ['token', 'key', 'secret', 'password', 'api']):
            masked_value = value[:4] + '*' * (len(value) - 8) + value[-4:] if len(value) > 12 else '****'
            print(f"{key}: {masked_value}")
        else:
            print(f"{key}: {value}")

    # Specifically check for OANDA variables
    print("\n=== OANDA Variables ===")
    oanda_api_token = os.environ.get('OANDA_API_TOKEN')
    oanda_account_id = os.environ.get('OANDA_ACCOUNT_ID')
    oanda_api_url = os.environ.get('OANDA_API_URL')

    print(f"OANDA_API_TOKEN exists: {oanda_api_token is not None}")
    if oanda_api_token:
        masked_token = oanda_api_token[:4] + '*' * (len(oanda_api_token) - 8) + oanda_api_token[-4:] if len(oanda_api_token) > 12 else '****'
        print(f"OANDA_API_TOKEN: {masked_token}")

    print(f"OANDA_ACCOUNT_ID exists: {oanda_account_id is not None}")
    if oanda_account_id:
        print(f"OANDA_ACCOUNT_ID: {oanda_account_id}")

    print(f"OANDA_API_URL exists: {oanda_api_url is not None}")
    if oanda_api_url:
        print(f"OANDA_API_URL: {oanda_api_url}")

    # Check other common environment variables
    print("\n=== Common Environment Variables ===")
    print(f"PORT: {os.environ.get('PORT')}")
    print(f"HOST: {os.environ.get('HOST')}")
    print(f"ENVIRONMENT: {os.environ.get('ENVIRONMENT')}")
    print(f"DEBUG: {os.environ.get('DEBUG')}")

    print("\nPython version:", sys.version)
    print("Python path:", sys.executable)
    
    # Print total count of environment variables
    print(f"\nTotal environment variables: {len(os.environ)}")
    
    # Print a few other important variables if they exist
    important_vars = ['PORT', 'RENDER', 'PYTHON_VERSION', 'PYTHONPATH', 'PATH']
    print("\nOTHER IMPORTANT VARIABLES:")
    print("-" * 30)
    for var in important_vars:
        if var in os.environ:
            print(f"{var}: {os.environ[var]}")
        else:
            print(f"{var}: NOT SET")
    
    # Option to print all variables if requested
    if len(sys.argv) > 1 and sys.argv[1] == '--all':
        print("\nALL ENVIRONMENT VARIABLES:")
        print("-" * 30)
        pprint(os.environ)
    else:
        print("\nRun with --all flag to see all environment variables")
    
    print("=" * 50)

if __name__ == "__main__":
    main() 