# utils/decorators.py
import functools
import traceback
import logging

logger = logging.getLogger(__name__)

def handle_async_errors(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Async error in {func.__name__}: {str(e)}", exc_info=True)
            return None
    return wrapper
