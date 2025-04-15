# This patch adds a null check for the advanced_loss_management object
# Used to fix NoneType errors when the object is not properly initialized

import logging
import asyncio
import traceback
from advanced_loss_management import AdvancedLossManagement

logger = logging.getLogger("advanced-loss-patch")

async def start_advanced_loss_management_safely(advanced_loss_management):
    """Safely start the advanced loss management system with null checks"""
    if advanced_loss_management is None:
        logger.warning("Advanced loss management is None, cannot start")
        return False
    
    try:
        # Check if start method exists before calling it
        if hasattr(advanced_loss_management, 'start') and callable(getattr(advanced_loss_management, 'start')):
            await advanced_loss_management.start()
            logger.info("Advanced loss management started successfully")
            return True
        else:
            logger.warning("AdvancedLossManagement has no start() method, skipping initialization")
            return False
    except Exception as e:
        logger.error(f"Error starting advanced loss management: {str(e)}")
        logger.error(traceback.format_exc())
        return False

async def stop_advanced_loss_management_safely(advanced_loss_management):
    """Safely stop the advanced loss management system with null checks"""
    if advanced_loss_management is None:
        logger.warning("Advanced loss management is None, nothing to stop")
        return
    
    try:
        # Check if stop method exists before calling it
        if hasattr(advanced_loss_management, 'stop') and callable(getattr(advanced_loss_management, 'stop')):
            await advanced_loss_management.stop()
            logger.info("Advanced loss management stopped successfully")
        else:
            logger.warning("AdvancedLossManagement has no stop() method, skipping cleanup")
    except Exception as e:
        logger.error(f"Error stopping advanced loss management: {str(e)}")
        logger.error(traceback.format_exc())

# Example usage:
# In your application, replace:
#   await advanced_loss_management.start()
# with:
#   from advanced_loss_management_null_check import start_advanced_loss_management_safely
#   await start_advanced_loss_management_safely(advanced_loss_management) 