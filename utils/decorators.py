import functools
import logging
import traceback

logger = logging.getLogger(__name__)

def handle_async_errors(func):
    """Decorator to catch and log errors in async functions."""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"[async] Error in {func.__name__}: {str(e)}", exc_info=True)
            return None
    return wrapper

def handle_sync_errors(func):
    """Decorator to catch and log errors in synchronous functions."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"[sync] Error in {func.__name__}: {str(e)}", exc_info=True)
            return None
    return wrapper
