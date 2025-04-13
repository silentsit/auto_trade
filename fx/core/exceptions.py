"""
Exceptions for the FX Trading Bridge application.

This module defines custom exceptions used throughout the application.
"""

class BridgeError(Exception):
    """Base class for all FX Bridge exceptions."""
    pass

class ConnectionError(BridgeError):
    """Exception raised when there are issues connecting to the trading platform."""
    pass

class AuthenticationError(ConnectionError):
    """Exception raised when authentication with the trading platform fails."""
    pass

class TradeError(BridgeError):
    """Exception raised when a trade operation fails."""
    pass

class ValidationError(BridgeError):
    """Exception raised when trade parameters fail validation."""
    pass

class OrderError(TradeError):
    """Exception raised when order creation or modification fails."""
    pass

class PositionError(TradeError):
    """Exception raised when position operations fail."""
    pass

class ConfigError(BridgeError):
    """Exception raised when there are issues with the configuration."""
    pass

class TimeoutError(BridgeError):
    """Exception raised when an operation times out."""
    pass 
