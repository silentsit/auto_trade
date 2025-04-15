"""
Debug utility for checking component initialization and configuration
"""

import os
import sys
import logging
import asyncio

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("debug")

async def check_environment():
    """Check environment variables"""
    logger.info("=== CHECKING ENVIRONMENT VARIABLES ===")
    required_vars = [
        "OANDA_API_TOKEN", 
        "OANDA_ACCOUNT_ID"
    ]
    
    for var in required_vars:
        value = os.environ.get(var)
        if value:
            # Mask sensitive information
            if var == "OANDA_API_TOKEN":
                masked = value[:4] + "..." + value[-4:] if len(value) > 8 else "***"
                logger.info(f"{var}: {masked}")
            else:
                logger.info(f"{var}: {value}")
        else:
            logger.error(f"Missing required environment variable: {var}")
    
    # Print all environment variables
    logger.info("\n=== ALL ENVIRONMENT VARIABLES ===")
    for key, value in os.environ.items():
        # Skip sensitive variables or show masked version
        if key in ["OANDA_API_TOKEN"]:
            masked = value[:4] + "..." + value[-4:] if len(value) > 8 else "***"
            logger.info(f"{key}: {masked}")
        else:
            logger.info(f"{key}: {value}")

async def check_components():
    """Check if components are initialized properly"""
    # Import here to avoid circular imports
    from enhanced_trading import (
        position_tracker, multi_stage_tp_manager, dynamic_exit_manager,
        advanced_loss_management, market_structure_analyzer, settings
    )
    
    logger.info("\n=== CHECKING COMPONENTS ===")
    
    components = {
        "position_tracker": position_tracker,
        "multi_stage_tp_manager": multi_stage_tp_manager,
        "dynamic_exit_manager": dynamic_exit_manager,
        "advanced_loss_management": advanced_loss_management,
        "market_structure_analyzer": market_structure_analyzer,
        "settings": settings
    }
    
    for name, component in components.items():
        status = "Initialized" if component is not None else "NOT INITIALIZED"
        logger.info(f"{name}: {status}")
        if component is not None:
            if hasattr(component, 'running'):
                logger.info(f"  Running: {component.running}")
            if hasattr(component, 'initialized'):
                logger.info(f"  Initialized: {component.initialized}")

async def main():
    """Main entry point"""
    logger.info("Starting debug utility...")
    
    await check_environment()
    await check_components()
    
    logger.info("Debug completed")

if __name__ == "__main__":
    asyncio.run(main()) 