import os
import json
import sys
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("env-checker")

def check_environment():
    """
    Check and print all environment variables for debugging
    """
    # Load .env file if present
    load_dotenv()
    
    print("\n=== Environment Variables ===")
    env_vars = {k: v for k, v in os.environ.items() 
               if not k.startswith('_') and 
                  not k.startswith('SYSTEMROOT') and
                  not k.startswith('COMPUTERNAME') and
                  not k.startswith('USERNAME')}
    
    # Look specifically for OANDA variables
    oanda_vars = {k: v for k, v in env_vars.items() if 'OANDA' in k.upper()}
    
    print("\n--- OANDA Environment Variables ---")
    if oanda_vars:
        for k, v in oanda_vars.items():
            # Mask sensitive values
            if 'TOKEN' in k.upper() or 'KEY' in k.upper() or 'SECRET' in k.upper():
                v = v[:4] + '*' * (len(v) - 8) + v[-4:] if len(v) > 8 else '*' * len(v)
            print(f"{k}: {v}")
    else:
        print("No OANDA environment variables found!")
    
    print("\n--- All Other Environment Variables ---")
    for k, v in env_vars.items():
        if k not in oanda_vars:
            # Mask sensitive values
            if 'TOKEN' in k.upper() or 'KEY' in k.upper() or 'SECRET' in k.upper() or 'PASSWORD' in k.upper():
                v = v[:4] + '*' * (len(v) - 8) + v[-4:] if len(v) > 8 else '*' * len(v)
            print(f"{k}: {v}")
    
    # Check for .env file
    print("\n--- .env File Contents ---")
    try:
        with open('.env', 'r') as f:
            env_file = f.read()
            lines = env_file.strip().split('\n')
            for line in lines:
                if line and not line.startswith('#'):
                    if '=' in line:
                        k, v = line.split('=', 1)
                        # Mask sensitive values
                        if 'TOKEN' in k.upper() or 'KEY' in k.upper() or 'SECRET' in k.upper() or 'PASSWORD' in k.upper():
                            v = v[:4] + '*' * (len(v) - 8) + v[-4:] if len(v) > 8 else '*' * len(v)
                        print(f"{k}: {v}")
    except FileNotFoundError:
        print("No .env file found!")
    except Exception as e:
        print(f"Error reading .env file: {str(e)}")

if __name__ == "__main__":
    logger.info("Checking environment variables...")
    check_environment()
    logger.info("Environment check completed!")
    sys.exit(0) 