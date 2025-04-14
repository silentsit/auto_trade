import os
import sys
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger("env-checker")

def check_environment():
    """Check if all required environment variables are properly set"""
    
    # Load .env file if present
    load_dotenv()
    
    # Required environment variables
    required_vars = [
        "OANDA_API_TOKEN",
        "OANDA_ACCOUNT_ID"
    ]
    
    # Optional environment variables with defaults
    optional_vars = {
        "OANDA_API_URL": "https://api-fxtrade.oanda.com/v3",
        "OANDA_ENVIRONMENT": "practice",
        "PORT": "8000"
    }
    
    # Check required variables
    missing_vars = []
    for var in required_vars:
        value = os.environ.get(var)
        if not value:
            missing_vars.append(var)
            logger.error(f"Missing required environment variable: {var}")
        else:
            # Show first few characters of sensitive information
            if "TOKEN" in var or "KEY" in var:
                masked_value = value[:4] + "*" * (len(value) - 4)
                logger.info(f"{var}: {masked_value}")
            else:
                logger.info(f"{var}: {value}")
    
    # Check optional variables
    for var, default in optional_vars.items():
        value = os.environ.get(var, default)
        logger.info(f"{var}: {value} {'(default)' if os.environ.get(var) is None else ''}")
    
    # Check for .env file
    env_files = [".env", ".env.example", ".env.txt"]
    found_env_files = []
    
    for env_file in env_files:
        if os.path.exists(env_file):
            found_env_files.append(env_file)
            logger.info(f"Found {env_file} file")
    
    if not found_env_files:
        logger.warning("No .env files found")
    
    # Return status
    if missing_vars:
        logger.error(f"Missing {len(missing_vars)} required environment variables")
        return False
    else:
        logger.info("All required environment variables are set")
        return True

if __name__ == "__main__":
    logger.info("Checking environment variables...")
    if check_environment():
        logger.info("Environment check passed!")
        sys.exit(0)
    else:
        logger.error("Environment check failed!")
        sys.exit(1) 