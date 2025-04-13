"""
Constants for the FX Trading Bridge application.

This module defines constants used throughout the application.
"""

# Trading constants
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
DEFAULT_TIMEOUT = 30  # seconds

# Order types
ORDER_TYPE_MARKET = "MARKET"
ORDER_TYPE_LIMIT = "LIMIT"
ORDER_TYPE_STOP = "STOP"
ORDER_TYPE_STOP_LIMIT = "STOP_LIMIT"

# Order sides
ORDER_SIDE_BUY = "BUY"
ORDER_SIDE_SELL = "SELL"

# Order timeframes
ORDER_TIF_GTC = "GTC"  # Good Till Cancelled
ORDER_TIF_IOC = "IOC"  # Immediate Or Cancel
ORDER_TIF_FOK = "FOK"  # Fill Or Kill
ORDER_TIF_GTD = "GTD"  # Good Till Date

# Position status
POSITION_STATUS_OPEN = "OPEN"
POSITION_STATUS_CLOSED = "CLOSED"
POSITION_STATUS_PENDING = "PENDING"

# Pip values for major currency pairs (simplified)
PIP_VALUES = {
    "EUR/USD": 0.0001,
    "GBP/USD": 0.0001,
    "USD/JPY": 0.01,
    "USD/CHF": 0.0001,
    "USD/CAD": 0.0001,
    "AUD/USD": 0.0001,
    "NZD/USD": 0.0001,
}

# Connection status
CONNECTION_STATUS_CONNECTED = "CONNECTED"
CONNECTION_STATUS_DISCONNECTED = "DISCONNECTED"
CONNECTION_STATUS_CONNECTING = "CONNECTING"

# Log levels
LOG_LEVEL_DEBUG = "DEBUG"
LOG_LEVEL_INFO = "INFO"
LOG_LEVEL_WARNING = "WARNING"
LOG_LEVEL_ERROR = "ERROR"
LOG_LEVEL_CRITICAL = "CRITICAL" 
