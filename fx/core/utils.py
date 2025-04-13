"""
Utility functions for the FX Trading Bridge application.

This module provides helper functions used throughout the application.
"""

import logging
import functools
import time
from typing import Any, Callable, Dict, Optional, TypeVar, cast

from fx.core.exceptions import TradeError

T = TypeVar('T')

logger = logging.getLogger(__name__)

def safe_execute(func: Callable[..., T]) -> Callable[..., Optional[T]]:
    """
    Decorator to safely execute a function and handle exceptions.
    
    Args:
        func: Function to be wrapped
        
    Returns:
        Wrapped function that catches and logs exceptions
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Optional[T]:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error executing {func.__name__}: {str(e)}")
            return None
    return wrapper

def format_price(price: float, digits: int = 5) -> str:
    """
    Format a price to a specified number of decimal places.
    
    Args:
        price: The price to format
        digits: Number of decimal places
        
    Returns:
        Formatted price string
    """
    return f"{price:.{digits}f}"

def calculate_pip_value(symbol: str, lot_size: float = 1.0) -> float:
    """
    Calculate the pip value for a given symbol and lot size.
    
    Args:
        symbol: Currency pair (e.g., "EURUSD")
        lot_size: Size of the lot in standard lots
        
    Returns:
        Value of a single pip in account currency
    """
    # Simplified implementation - in a real system would need broker-specific logic
    base_pip_values = {
        "EURUSD": 10.0,
        "GBPUSD": 10.0,
        "USDJPY": 9.30,
        "AUDUSD": 10.0,
        "USDCHF": 9.50,
        "EURGBP": 12.50,
    }
    
    # Default if symbol not found
    default_value = 10.0
    
    pip_value = base_pip_values.get(symbol.upper(), default_value)
    return pip_value * lot_size

def retry(max_attempts: int = 3, delay: float = 1.0) -> Callable:
    """
    Retry decorator for functions that might fail temporarily.
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Delay between retries in seconds
        
    Returns:
        Decorator function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempts = 0
            last_error = None
            
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    last_error = e
                    if attempts < max_attempts:
                        logger.warning(f"Retry {attempts}/{max_attempts} for {func.__name__} after error: {str(e)}")
                        time.sleep(delay)
            
            # If we get here, all attempts failed
            logger.error(f"All {max_attempts} attempts failed for {func.__name__}")
            raise last_error if last_error else TradeError("Function failed after multiple retry attempts")
            
        return wrapper
    return decorator 
