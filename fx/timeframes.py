"""
Timeframe standardization utilities for the FX Trading Bridge application.
"""
import logging
import re
from typing import Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

# Define standard timeframe mappings
TIMEFRAME_MAPPINGS: Dict[str, str] = {
    # Minutes
    "M1": "1m",
    "M5": "5m", 
    "M15": "15m",
    "M30": "30m",
    "1M": "1m",
    "5M": "5m",
    "15M": "15m",
    "30M": "30m",
    "1MIN": "1m",
    "5MIN": "5m",
    "15MIN": "15m",
    "30MIN": "30m",
    "MINUTE": "1m",
    "MINUTE1": "1m",
    "MINUTE5": "5m",
    "MINUTE15": "15m",
    "MINUTE30": "30m",
    
    # Hours
    "H1": "1h",
    "H4": "4h",
    "1H": "1h",
    "4H": "4h",
    "HOUR": "1h",
    "HOUR1": "1h",
    "HOUR4": "4h",
    "HOURLY": "1h",
    
    # Days
    "D": "1d",
    "D1": "1d",
    "1D": "1d",
    "DAY": "1d",
    "DAILY": "1d",
    
    # Weeks
    "W": "1w",
    "W1": "1w",
    "1W": "1w",
    "WEEK": "1w",
    "WEEKLY": "1w",
    
    # Months
    "MN": "1M",
    "MN1": "1M",
    "1MN": "1M",
    "MONTH": "1M",
    "MONTHLY": "1M"
}

# For converting from standard format to minutes
TIMEFRAME_TO_MINUTES: Dict[str, int] = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
    "1d": 1440,   # 24 * 60
    "1w": 10080,  # 7 * 24 * 60
    "1M": 43200   # 30 * 24 * 60 (approximate)
}

# Order timeframes from smallest to largest for comparison
TIMEFRAME_ORDER: List[str] = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1M"]

def standardize_timeframe(timeframe: str) -> str:
    """
    Standardize a trading timeframe to a common format.
    
    Args:
        timeframe: The timeframe to standardize (e.g., 'M5', '5m', '5min')
        
    Returns:
        Standardized timeframe format (e.g., '5m', '1h', '1d')
    """
    if not timeframe:
        logger.error("Received empty timeframe for standardization")
        return "1h"  # Default to 1-hour timeframe if nothing specified
    
    # Remove any whitespace and convert to uppercase for matching
    clean_timeframe = timeframe.strip().upper()
    
    # Check direct mapping first
    if clean_timeframe in TIMEFRAME_MAPPINGS:
        return TIMEFRAME_MAPPINGS[clean_timeframe]
    
    # If it's already in standard format (lowercase with appropriate suffix)
    lower_tf = timeframe.lower()
    if any(lower_tf == tf for tf in TIMEFRAME_TO_MINUTES.keys()):
        return lower_tf
    
    # Try to parse timeframe with regex
    # Match patterns like: 1m, 5min, 1hour, 1h, 1d, 1day, etc.
    match = re.match(r'^(\d+)([mMhHdDwW]|min|hour|day|week|month).*$', clean_timeframe)
    if match:
        value, unit = match.groups()
        unit = unit.lower()
        if unit in ('m', 'min', 'minute'):
            if value == '1':
                return "1m"
            elif value == '5':
                return "5m"
            elif value == '15':
                return "15m"
            elif value == '30':
                return "30m"
            else:
                logger.warning(f"Non-standard minute timeframe: {timeframe}")
                return f"{value}m"  # Custom minute timeframe
        elif unit in ('h', 'hour'):
            if value == '1':
                return "1h"
            elif value == '4':
                return "4h"
            else:
                logger.warning(f"Non-standard hour timeframe: {timeframe}")
                return f"{value}h"  # Custom hour timeframe
        elif unit in ('d', 'day'):
            return "1d"
        elif unit in ('w', 'week'):
            return "1w"
        elif unit in ('m', 'month'):
            return "1M"
    
    # If we can't standardize it properly, log a warning and return a default
    logger.warning(f"Unable to standardize timeframe: {timeframe}. Using default 1h.")
    return "1h"  # Default to 1 hour

def timeframe_to_minutes(timeframe: str) -> int:
    """Convert a standardized timeframe to minutes"""
    std_tf = standardize_timeframe(timeframe)
    return TIMEFRAME_TO_MINUTES.get(std_tf, 60)  # Default to 60 if not found

def is_higher_timeframe(tf1: str, tf2: str) -> bool:
    """Check if tf1 is a higher timeframe than tf2"""
    std_tf1 = standardize_timeframe(tf1)
    std_tf2 = standardize_timeframe(tf2)
    
    try:
        idx1 = TIMEFRAME_ORDER.index(std_tf1)
        idx2 = TIMEFRAME_ORDER.index(std_tf2)
        return idx1 > idx2
    except ValueError:
        # If either timeframe is not in our list, compare by minutes
        minutes1 = timeframe_to_minutes(std_tf1)
        minutes2 = timeframe_to_minutes(std_tf2)
        return minutes1 > minutes2

def get_higher_timeframes(timeframe: str) -> List[str]:
    """Get all timeframes higher than the given timeframe"""
    std_tf = standardize_timeframe(timeframe)
    try:
        idx = TIMEFRAME_ORDER.index(std_tf)
        return TIMEFRAME_ORDER[idx+1:]
    except ValueError:
        # If timeframe is not in our list, use minutes comparison
        minutes = timeframe_to_minutes(std_tf)
        return [tf for tf in TIMEFRAME_ORDER if timeframe_to_minutes(tf) > minutes]

def get_lower_timeframes(timeframe: str) -> List[str]:
    """Get all timeframes lower than the given timeframe"""
    std_tf = standardize_timeframe(timeframe)
    try:
        idx = TIMEFRAME_ORDER.index(std_tf)
        return TIMEFRAME_ORDER[:idx]
    except ValueError:
        # If timeframe is not in our list, use minutes comparison
        minutes = timeframe_to_minutes(std_tf)
        return [tf for tf in TIMEFRAME_ORDER if timeframe_to_minutes(tf) < minutes]

def get_next_higher_timeframe(timeframe: str) -> str:
    """Get the next higher timeframe from the given timeframe"""
    std_tf = standardize_timeframe(timeframe)
    try:
        idx = TIMEFRAME_ORDER.index(std_tf)
        if idx < len(TIMEFRAME_ORDER) - 1:
            return TIMEFRAME_ORDER[idx + 1]
        return std_tf  # Already at highest timeframe
    except ValueError:
        # If timeframe is not in our list, find the next higher by minutes
        minutes = timeframe_to_minutes(std_tf)
        higher_tfs = [tf for tf in TIMEFRAME_ORDER if timeframe_to_minutes(tf) > minutes]
        if higher_tfs:
            return min(higher_tfs, key=lambda tf: timeframe_to_minutes(tf))
        return std_tf  # No higher timeframe found 