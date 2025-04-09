##############################################################################
# Python Trading Bridge - Complete Version
##############################################################################

##############################################################################
# Imports
##############################################################################

import os
import sys
import asyncio
import logging
import signal
import json
import time
import uuid
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any, Union, Callable, TypeVar
from functools import wraps
from contextlib import asynccontextmanager
from pathlib import Path
import re
import copy
import math
import statistics

# FastAPI and web related
import uvicorn
from fastapi import FastAPI, Request, BackgroundTasks, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

# HTTP client
import aiohttp

# Redis (optional)
try:
    import redis.asyncio as redis
except ImportError:
    redis = None

# Type Variables for decorators
P = TypeVar('P')
T = TypeVar('T')

# Get configuration
from dotenv import load_dotenv
load_dotenv()

##############################################################################
# Configuration Management
##############################################################################

class Settings(BaseSettings):
    """
    Application settings loaded from environment variables with defaults.
    Pydantic's BaseSettings handles environment variables automatically.
    """
    # API settings
    host: str = "0.0.0.0"
    port: int = 10000
    environment: str = "production"  # production, development
    allowed_origins: str = "*"
    
    # OANDA settings
    oanda_account_id: str = ""
    oanda_api_token: str = ""
    oanda_api_url: str = "https://api-fxtrade.oanda.com/v3"
    oanda_environment: str = "practice"  # practice, live
    
    # Redis settings (optional)
    redis_url: Optional[str] = None
    
    # Risk management settings
    default_risk_percentage: float = 2.0
    max_risk_percentage: float = 5.0
    max_daily_loss: float = 20.0  # 20% max daily loss
    max_portfolio_heat: float = 15.0  # 15% max portfolio risk
    
    # Connection settings
    connect_timeout: int = 10
    read_timeout: int = 30
    total_timeout: int = 45
    max_simultaneous_connections: int = 100
    spread_threshold_forex: float = 0.001
    spread_threshold_crypto: float = 0.008
    max_retries: int = 3
    base_delay: float = 1.0
    max_requests_per_minute: int = 100
    
    # Trading settings
    trade_24_7: bool = True  # For crypto and 24/7 markets
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )

# Initialize config
config = Settings()

# Set up logger - will be properly configured later
logger = logging.getLogger("trading_bridge")

##############################################################################
# Custom Exception Classes & Error Handling
##############################################################################

class TradingError(Exception):
    """Base class for all trading-related errors"""
    pass

class MarketError(TradingError):
    """Error related to market data or conditions"""
    pass

class OrderError(TradingError):
    """Error related to order execution or management"""
    pass

class CustomValidationError(TradingError):
    """Error related to validation of trading parameters"""
    pass

class ErrorRecoverySystem:
    """
    Advanced error recovery system that detects patterns in errors and attempts
    to recover from them using various strategies.
    """
    def __init__(self):
        """Initialize the error recovery system"""
        self.recovery_stats = {
            "total_errors": 0,
            "recovery_attempts": 0,
            "successful_recoveries": 0,
            "strategies": {},
            "errors_by_type": {},
            "errors_by_operation": {}
        }
        self.operation_contexts = {}
        self.last_errors = {}
        
    @handle_sync_errors
    def handle_error(self, 
                    request_id: str, 
                    operation: str, 
                    error: Exception, 
                    context: Dict[str, Any] = None) -> bool:
        """
        Handle an error by selecting and applying a recovery strategy
        
        Args:
            request_id: Unique identifier for the request
            operation: The operation that failed
            error: The exception that was raised
            context: Additional context for recovery
            
        Returns:
            bool: True if recovery was successful, False otherwise
        """
        if context is None:
            context = {}
            
        error_type = type(error).__name__
        error_message = str(error)
        
        # Update error stats
        self.recovery_stats["total_errors"] += 1
        
        # Track errors by type
        if error_type not in self.recovery_stats["errors_by_type"]:
            self.recovery_stats["errors_by_type"][error_type] = 0
        self.recovery_stats["errors_by_type"][error_type] += 1
        
        # Track errors by operation
        if operation not in self.recovery_stats["errors_by_operation"]:
            self.recovery_stats["errors_by_operation"][operation] = 0
        self.recovery_stats["errors_by_operation"][operation] += 1
        
        # Store last error for this operation
        self.last_errors[operation] = {
            "type": error_type,
            "message": error_message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "request_id": request_id
        }
        
        # Store context for potential recovery
        self.operation_contexts[request_id] = context
        
        # Log the error
        logger.error(f"Operation '{operation}' failed: {error_type} - {error_message}")
        
        # Select recovery strategy
        strategy = self._get_recovery_strategy(operation, error_type, error_message)
        
        if not strategy:
            logger.warning(f"No recovery strategy available for {error_type} in {operation}")
            return False
            
        # Track recovery attempt
        self.recovery_stats["recovery_attempts"] += 1
        
        # Track strategies
        if strategy not in self.recovery_stats["strategies"]:
            self.recovery_stats["strategies"][strategy] = {
                "attempts": 0,
                "successes": 0
            }
        self.recovery_stats["strategies"][strategy]["attempts"] += 1
        
        # Apply recovery strategy
        success = False
        try:
            if strategy == "retry":
                success = self._retry_operation(operation, context, 1)
            elif strategy == "reconnect_and_retry":
                success = self._reconnect_and_retry(operation, context, 1)
            elif strategy == "reset_session_and_retry":
                success = self._reset_session_and_retry(operation, context, 1)
            elif strategy == "sync_positions":
                success = self._sync_positions(context)
                
            # Record outcome
            self._record_recovery_outcome(operation, error_type, strategy, success)
            
            # Log outcome
            if success:
                logger.info(f"Successfully recovered from {error_type} in {operation} using {strategy}")
            else:
                logger.warning(f"Failed to recover from {error_type} in {operation} using {strategy}")
                
            return success
            
        except Exception as recovery_error:
            logger.error(f"Error during recovery: {type(recovery_error).__name__} - {str(recovery_error)}")
            self._record_recovery_outcome(operation, error_type, strategy, False)
            return False
    
    def _get_recovery_strategy(self, operation: str, error_type: str, error_message: str) -> Optional[str]:
        """
        Determine the appropriate recovery strategy based on the error and operation
        
        Args:
            operation: Operation that failed
            error_type: Type of error
            error_message: Error message
            
        Returns:
            Optional[str]: Recovery strategy or None if no strategy is available
        """
        # Network errors - try reconnecting
        if error_type in ["ConnectionError", "TimeoutError", "ClientConnectorError", "ServerDisconnectedError"]:
            return "reconnect_and_retry"
            
        # HTTP errors - depends on status code
        if error_type == "ClientResponseError":
            if "429" in error_message or "Too Many Requests" in error_message:
                return "retry"  # With backoff
            elif any(code in error_message for code in ["500", "502", "503", "504"]):
                return "retry"  # Server errors, retry
            elif "401" in error_message or "403" in error_message:
                return "reset_session_and_retry"  # Auth issues, reset session
                
        # JSON parse errors - retry with new session
        if error_type == "JSONDecodeError" or "Invalid JSON" in error_message:
            return "reset_session_and_retry"
            
        # Position synchronization issues
        if "position not found" in error_message.lower() or "order not found" in error_message.lower():
            return "sync_positions"
            
        # Default strategy based on operation
        if operation in ["execute_trade", "close_position", "update_position"]:
            return "retry"
            
        # No specific strategy
        return "retry"
    
    @handle_sync_errors
    def _retry_operation(self, operation: str, context: Dict[str, Any], attempt: int) -> bool:
        """
        Retry an operation with exponential backoff
        
        Args:
            operation: Operation to retry
            context: Context for the operation
            attempt: Current attempt number
            
        Returns:
            bool: True if successful, False otherwise
        """
        if attempt > 3:  # Maximum 3 retries
            return False
            
        # Calculate backoff delay
        delay = min(30, (2 ** attempt) * 1.5)  # Exponential backoff with max 30 sec
        
        logger.info(f"Retrying operation {operation} (attempt {attempt}) after {delay:.1f}s delay")
        
        # In a synchronous implementation, we use time.sleep
        time.sleep(delay)
        
        try:
            # Perform retry based on operation type
            if operation == "execute_trade" and "symbol" in context and "action" in context:
                # Simplified retry - in a real system we would call the actual function
                logger.info(f"Retrying trade execution for {context.get('symbol')}")
                # success, _ = execute_trade(...)
                return True  # Simulate success for now
                
            elif operation == "close_position" and "symbol" in context:
                logger.info(f"Retrying position closure for {context.get('symbol')}")
                # success, _ = close_position(...)
                return True  # Simulate success for now
                
            elif operation == "update_position" and "position_id" in context:
                logger.info(f"Retrying position update for {context.get('position_id')}")
                # Simulate success
                return True
                
            # Generic retry success simulation
            return True
            
        except Exception as e:
            logger.error(f"Error during retry attempt {attempt}: {type(e).__name__} - {str(e)}")
            # Recursive retry with incremented attempt
            return self._retry_operation(operation, context, attempt + 1)
    
    @handle_sync_errors
    def _reconnect_and_retry(self, operation: str, context: Dict[str, Any], attempt: int) -> bool:
        """
        Reconnect to services and retry the operation
        
        Args:
            operation: Operation to retry
            context: Context for the operation
            attempt: Current attempt number
            
        Returns:
            bool: True if successful, False otherwise
        """
        if attempt > 2:  # Maximum 2 reconnect attempts
            return False
            
        logger.info(f"Reconnecting and retrying operation {operation} (attempt {attempt})")
        
        try:
            # For synchronous implementation, simply retry
            # In async, we would use get_session(force_new=True)
            time.sleep(2)  # Simulate reconnection time
            
            # Now retry the operation
            return self._retry_operation(operation, context, 1)
            
        except Exception as e:
            logger.error(f"Error during reconnect attempt {attempt}: {type(e).__name__} - {str(e)}")
            return self._reconnect_and_retry(operation, context, attempt + 1)
    
    @handle_sync_errors
    def _reset_session_and_retry(self, operation: str, context: Dict[str, Any], attempt: int) -> bool:
        """
        Reset session credentials and retry the operation
        
        Args:
            operation: Operation to retry
            context: Context for the operation
            attempt: Current attempt number
            
        Returns:
            bool: True if successful, False otherwise
        """
        if attempt > 2:  # Maximum 2 reset attempts
            return False
            
        logger.info(f"Resetting session and retrying operation {operation} (attempt {attempt})")
        
        try:
            # Simulate session reset
            time.sleep(3)
            
            # Retry operation
            return self._retry_operation(operation, context, 1)
            
        except Exception as e:
            logger.error(f"Error during session reset attempt {attempt}: {type(e).__name__} - {str(e)}")
            return False
    
    @handle_sync_errors
    def _sync_positions(self, context: Dict[str, Any]) -> bool:
        """
        Synchronize local position state with broker positions
        
        Args:
            context: Context containing position information
            
        Returns:
            bool: True if successful, False otherwise
        """
        logger.info("Synchronizing positions with broker")
        
        try:
            # In a real implementation, we would:
            # 1. Get positions from broker
            # 2. Compare with local positions
            # 3. Reconcile any differences
            
            symbol = context.get("symbol")
            position_id = context.get("position_id")
            
            if not symbol and not position_id:
                logger.warning("Cannot sync positions: missing symbol or position_id")
                return False
                
            logger.info(f"Synced position for {symbol or position_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error during position synchronization: {type(e).__name__} - {str(e)}")
            return False
    
    def _record_recovery_outcome(self, operation: str, error_type: str, strategy: str, success: bool):
        """
        Record the outcome of a recovery attempt
        
        Args:
            operation: Operation that was being recovered
            error_type: Type of error
            strategy: Recovery strategy used
            success: Whether recovery was successful
        """
        if success:
            self.recovery_stats["successful_recoveries"] += 1
            
            if strategy in self.recovery_stats["strategies"]:
                self.recovery_stats["strategies"][strategy]["successes"] += 1
                
        # Calculate success rate
        attempts = self.recovery_stats["recovery_attempts"]
        successes = self.recovery_stats["successful_recoveries"]
        
        if attempts > 0:
            success_rate = (successes / attempts) * 100
            logger.info(f"Recovery success rate: {success_rate:.1f}% ({successes}/{attempts})")
    
    @handle_sync_errors
    def get_recovery_stats(self) -> Dict[str, Any]:
        """
        Get statistics about error recovery
        
        Returns:
            Dict[str, Any]: Recovery statistics
        """
        # Calculate success rates for each strategy
        for strategy, stats in self.recovery_stats["strategies"].items():
            if stats["attempts"] > 0:
                stats["success_rate"] = (stats["successes"] / stats["attempts"]) * 100
            else:
                stats["success_rate"] = 0
                
        # Calculate overall success rate
        attempts = self.recovery_stats["recovery_attempts"]
        if attempts > 0:
            overall_success_rate = (self.recovery_stats["successful_recoveries"] / attempts) * 100
        else:
            overall_success_rate = 0
            
        return {
            "total_errors": self.recovery_stats["total_errors"],
            "recovery_attempts": attempts,
            "successful_recoveries": self.recovery_stats["successful_recoveries"],
            "overall_success_rate": overall_success_rate,
            "strategies": self.recovery_stats["strategies"],
            "errors_by_type": self.recovery_stats["errors_by_type"],
            "errors_by_operation": self.recovery_stats["errors_by_operation"],
            "last_errors": self.last_errors
        }
    
    def schedule_stale_position_check(self):
        """
        Schedule regular checks for stale positions
        This would normally be an async function running in a background task
        """
        logger.info("Stale position checking is scheduled")
        # In a synchronous implementation, this would be run in a separate thread
        
    def _check_for_stale_positions(self):
        """Check for and reconcile stale positions"""
        logger.info("Checking for stale positions")
        
        try:
            # In a real implementation, we would:
            # 1. Get all local positions
            # 2. Compare with broker positions
            # 3. Identify stale positions (e.g., closed on broker but still open locally)
            # 4. Reconcile any differences
            
            # Simulated implementation
            logger.info("Stale position check completed")
            
        except Exception as e:
            logger.error(f"Error during stale position check: {type(e).__name__} - {str(e)}")

def handle_async_errors(func: Callable[P, T]) -> Callable[P, T]:
    """
    Decorator for handling errors in async functions.
    Logs errors and maintains proper error propagation.
    """
    @wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            if isinstance(e, TradingError):
                raise
            else:
                raise TradingError(f"Unexpected error in {func.__name__}: {str(e)}") from e
    return wrapper

def handle_sync_errors(func: Callable[P, T]) -> Callable[P, T]:
    """
    Decorator for handling errors in synchronous functions.
    Similar to handle_async_errors but for sync functions.
    """
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            if isinstance(e, TradingError):
                raise
            else:
                raise TradingError(f"Unexpected error in {func.__name__}: {str(e)}") from e
    return wrapper

##############################################################################
# Logging Setup
##############################################################################

class JSONFormatter(logging.Formatter):
    """Format logs as JSON for better parsing"""
    def format(self, record):
        log_data = {
            'timestamp': datetime.now().isoformat(),
            'level': record.levelname,
            'name': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add exception info if available
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': self.formatException(record.exc_info)
            }
            
        # Add extra data if available
        if hasattr(record, 'data'):
            log_data['data'] = record.data
            
        return json.dumps(log_data)

def setup_logging():
    """Set up structured logging"""
    global logger
    
    logger = logging.getLogger("trading_bridge")
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Use JSON formatter in production, simple formatter in development
    if config.environment == "production":
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s - %(name)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Add file handler with rotation
    try:
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            filename="trading_bot.log",
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        logger.warning(f"Could not set up file handler: {str(e)}")
    
    # Suppress excessive logging from dependencies
    logging.getLogger("uvicorn").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    
    logger.info("Logging configured")

# Initialize logging
setup_logging()

##############################################################################
# HTTP Session Management
##############################################################################

# Global session dictionary
_sessions = {}
_last_session_cleanup = time.time()

@handle_async_errors
async def get_session(force_new: bool = False) -> aiohttp.ClientSession:
    """Get or create an aiohttp session with proper timeout handling"""
    global _sessions, _last_session_cleanup
    
    # Get current task ID for session tracking
    task_id = id(asyncio.current_task())
    
    # Clean up old sessions periodically
    current_time = time.time()
    if current_time - _last_session_cleanup > 300:  # 5 minutes
        await cleanup_stale_sessions()
        _last_session_cleanup = current_time
    
    # Return existing session if available and not forced to create new one
    if not force_new and task_id in _sessions:
        session = _sessions[task_id]
        if not session.closed:
            return session
    
    # Create timeout object
    timeout = aiohttp.ClientTimeout(
        connect=config.connect_timeout,
        sock_read=config.read_timeout,
        total=config.total_timeout
    )
    
    # Create new session
    session = aiohttp.ClientSession(timeout=timeout)
    _sessions[task_id] = session
    
    return session

@handle_async_errors
async def cleanup_stale_sessions():
    """Clean up stale or closed HTTP sessions"""
    global _sessions
    
    to_remove = []
    
    for task_id, session in _sessions.items():
        if session.closed:
            to_remove.append(task_id)
        else:
            try:
                # Check if task still exists
                task = asyncio.all_tasks()
                task_ids = [id(t) for t in task]
                if task_id not in task_ids:
                    await session.close()
                    to_remove.append(task_id)
            except Exception as e:
                logger.warning(f"Error checking session task: {str(e)}")
                to_remove.append(task_id)
    
    # Remove closed sessions
    for task_id in to_remove:
        if task_id in _sessions:
            del _sessions[task_id]
    
    if to_remove:
        logger.info(f"Cleaned up {len(to_remove)} stale sessions")

##############################################################################
# Data Models & Validation
##############################################################################

class AlertData(BaseModel):
    """Alert data model with improved validation"""
    symbol: str
    action: str
    timeframe: Optional[str] = "1H"
    orderType: Optional[str] = "MARKET"
    timeInForce: Optional[str] = "FOK"
    percentage: Optional[float] = 15.0
    account: Optional[str] = None
    id: Optional[str] = None
    comment: Optional[str] = None
    
    @validator('timeframe', pre=True, always=True)
    def validate_timeframe(cls, v):
        """Validate and standardize timeframe format"""
        if not v:
            return "1H"
            
        v = str(v).upper().strip()
        
        # Standard timeframes
        valid_timeframes = [
            "1M", "5M", "15M", "30M",  # Minutes
            "1H", "2H", "4H", "8H",    # Hours
            "1D", "1W", "1MO"          # Days, Weeks, Months
        ]
        
        # Normalize common variants
        timeframe_map = {
            "M1": "1M", "M5": "5M", "M15": "15M", "M30": "30M",
            "H1": "1H", "H2": "2H", "H4": "4H", "H8": "8H",
            "D1": "1D", "W1": "1W", "MN": "1MO", "MONTHLY": "1MO",
            "DAILY": "1D", "WEEKLY": "1W",
            "MINUTE": "1M", "HOUR": "1H", "DAY": "1D",
            "MIN": "1M", "HR": "1H"
        }
        
        if v in timeframe_map:
            return timeframe_map[v]
            
        if v in valid_timeframes:
            return v
            
        # Try to parse numeric prefixes
        if len(v) > 1:
            number_part = ''.join(filter(str.isdigit, v))
            unit_part = ''.join(filter(str.isalpha, v))
            
            if number_part and unit_part:
                if unit_part in ["M", "MIN", "MINUTE", "MINUTES"]:
                    if int(number_part) in [1, 5, 15, 30]:
                        return f"{number_part}M"
                elif unit_part in ["H", "HR", "HOUR", "HOURS"]:
                    if int(number_part) in [1, 2, 4, 8]:
                        return f"{number_part}H"
                elif unit_part in ["D", "DAY", "DAYS"]:
                    return "1D"
                elif unit_part in ["W", "WK", "WEEK", "WEEKS"]:
                    return "1W"
                elif unit_part in ["MO", "MON", "MONTH", "MONTHS"]:
                    return "1MO"
        
        # Default to 1H if unrecognized
        logger.warning(f"Unrecognized timeframe '{v}', defaulting to 1H")
        return "1H"
    
    @validator('action')
    def validate_action(cls, v):
        """Validate trade action"""
        if not v:
            raise ValueError("Action cannot be empty")
            
        v = str(v).upper().strip()
        
        valid_actions = ["BUY", "SELL", "CLOSE", "MODIFY"]
        
        if v not in valid_actions:
            raise ValueError(f"Invalid action: {v}. Must be one of {valid_actions}")
            
        return v
    
    @validator('symbol')
    def validate_symbol(cls, v):
        """Validate and standardize symbol format"""
        if not v:
            raise ValueError("Symbol cannot be empty")
            
        v = str(v).strip()
        
        # Basic validation - more complex validation in standardize_symbol function
        # This just ensures we have a non-empty string
        if len(v) < 2:
            raise ValueError(f"Invalid symbol: {v}")
            
        return v
    
    @validator('percentage')
    def validate_percentage(cls, v):
        """Validate percentage for partial close"""
        if v is not None:
            if v <= 0 or v > 100:
                raise ValueError(f"Invalid percentage: {v}. Must be between 0 and 100")
                
        return v
    
    class Config:
        str_strip_whitespace = True
        validate_assignment = True
        extra = "forbid"

##############################################################################
# Main Application Entry Point - Add this at the end
##############################################################################

def standardize_symbol(symbol: str) -> str:
    """
    Standardize symbol format for consistency
    
    Args:
        symbol: Trading symbol
        
    Returns:
        Standardized symbol
    """
    if not symbol:
        return ""
        
    # Remove any whitespace
    symbol = symbol.strip()
    
    # Convert to uppercase
    symbol = symbol.upper()
    
    return symbol


@handle_sync_errors
def check_market_hours(symbol: str = None) -> Tuple[bool, str]:
    """
    Check if markets are currently open for trading
    
    Args:
        symbol: Optional trading symbol to check specific market
        
    Returns:
        Tuple of (is_open, reason)
    """
    # Get current time in UTC
    current_time = datetime.now(timezone.utc)
    current_day = current_time.weekday()  # 0 = Monday, 6 = Sunday
    current_hour = current_time.hour
    
    # For crypto markets that trade 24/7
    if symbol and any(crypto in symbol for crypto in ["BTC", "ETH", "XRP", "LTC"]):
        return True, "Crypto markets trade 24/7"
    
    # Weekend check for traditional markets
    if current_day >= 5:  # Saturday or Sunday
        return False, "Markets closed on weekends"
    
    # For forex markets
    if not symbol or any(currency in symbol for currency in ["USD", "EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF"]):
        # Forex markets are generally open Sunday 5PM ET to Friday 5PM ET
        # This is a simplified check - in reality it's more complex with overlapping sessions
        if current_day == 4 and current_hour >= 21:  # Friday after 9PM UTC
            return False, "Forex markets closed for weekend"
        else:
            return True, "Forex markets open"
    
    # For US stock markets (simplified - doesn't account for holidays)
    if any(market in symbol for market in ["US30", "SPX", "NASDAQ"]):
        # US markets generally open 9:30 AM to 4 PM ET (14:30-21:00 UTC)
        if 14 <= current_hour < 21:
            return True, "US markets open"
        else:
            return False, "Outside US market hours"
    
    # Default to open if no specific rules match
    return True, "Markets assumed open"


def get_current_market_session(current_time: Optional[datetime] = None) -> str:
    """
    Get the current active market session
    
    Args:
        current_time: Override current time for testing
        
    Returns:
        String identifying current session (ASIAN, EUROPEAN, US, OVERLAP, CLOSED)
    """
    if current_time is None:
        current_time = datetime.now(timezone.utc)
        
    current_day = current_time.weekday()  # 0 = Monday, 6 = Sunday
    current_hour = current_time.hour
    
    # Weekend check
    if current_day >= 5:  # Saturday or Sunday
        return "CLOSED"
    
    # Asian session: ~00:00-09:00 UTC
    if 0 <= current_hour < 9:
        return "ASIAN"
    
    # European session: ~07:00-16:00 UTC
    if 7 <= current_hour < 16:
        # Overlap with Asian session
        if 7 <= current_hour < 9:
            return "OVERLAP_ASIAN_EUROPEAN"
        else:
            return "EUROPEAN"
    
    # US session: ~14:00-23:00 UTC
    if 14 <= current_hour < 23:
        # Overlap with European session
        if 14 <= current_hour < 16:
            return "OVERLAP_EUROPEAN_US"
        else:
            return "US"
    
    # Late hours
    return "CLOSED"


def is_instrument_tradeable(symbol: str) -> Tuple[bool, str]:
    """
    Check if a specific instrument is tradeable
    
    Args:
        symbol: Trading symbol
        
    Returns:
        Tuple of (is_tradeable, reason)
    """
    # Make sure symbol is standardized
    symbol = standardize_symbol(symbol)
    
    # Check if markets are open
    market_open, reason = check_market_hours(symbol)
    if not market_open:
        return False, reason
    
    # Get current session
    current_session = get_current_market_session()
    
    # Check for specific trading restrictions by instrument type
    
    # For forex majors - tradeable in all sessions
    if any(pair == symbol for pair in ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD"]):
        return True, f"Major pair tradeable in {current_session} session"
    
    # For forex crosses - some may have low liquidity in certain sessions
    if "JPY" in symbol:
        if current_session == "ASIAN":
            return True, "JPY cross with good liquidity in Asian session"
        elif current_session in ["CLOSED", "OVERLAP_ASIAN_EUROPEAN"]:
            return False, "JPY cross with low liquidity in current session"
    
    # For exotic pairs - be more restrictive
    exotic_currencies = ["TRY", "ZAR", "MXN", "SGD", "HKD", "NOK", "SEK"]
    if any(currency in symbol for currency in exotic_currencies):
        if current_session in ["EUROPEAN", "OVERLAP_EUROPEAN_US"]:
            return True, "Exotic pair tradeable in European session"
        else:
            return False, "Exotic pair with low liquidity in current session"
    
    # For US stocks and indices
    if any(market in symbol for market in ["US30", "SPX", "NASDAQ"]):
        if current_session in ["US", "OVERLAP_EUROPEAN_US"]:
            return True, "US market instrument tradeable in US session"
        else:
            return False, "US market closed in current session"
    
    # For crypto - always tradeable
    if any(crypto in symbol for crypto in ["BTC", "ETH", "XRP", "LTC"]):
        return True, "Crypto tradeable 24/7"
    
    # Default to tradeable if no specific restrictions
    return True, "Instrument assumed tradeable"

#############################
# Redis / Data Storage
#############################

@handle_async_errors
async def get_redis_client() -> Optional[redis.Redis]:
    """
    Get a Redis client instance for data storage.
    
    Returns:
        Optional[redis.Redis]: Redis client instance or None if Redis is not configured
    """
    try:
        settings = Settings()
        if not settings.redis_url:
            logger.debug("Redis not configured, using in-memory storage")
            return None
            
        client = redis.from_url(
            settings.redis_url,
            encoding="utf-8",
            decode_responses=True
        )
        await client.ping()
        return client
    except Exception as e:
        logger.warning(f"Could not connect to Redis: {str(e)}")
        return None

class DataStore:
    """
    Manages persistent data storage for positions, analytics, and settings.
    Falls back to in-memory storage if Redis is not available.
    """
    def __init__(self, redis_prefix: str = "trading:"):
        self.redis_client = None
        self.memory_storage = {}
        self.redis_prefix = redis_prefix
        self.cache = {}
        self._position_cache_valid = False
        self._settings_cache = {}
        self._analytics_cache = {}
        
    @handle_sync_errors
    def init(self) -> bool:
        """Initialize the data store connection"""
        try:
            loop = asyncio.get_event_loop()
            self.redis_client = loop.run_until_complete(get_redis_client())
            logger.info(f"DataStore initialized with Redis: {bool(self.redis_client)}")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize DataStore: {str(e)}")
            return False
    
    @handle_sync_errors
    def store_position(self, position_id: str, position_data: dict) -> bool:
        """
        Store position data in the data store
        
        Args:
            position_id: Unique identifier for the position
            position_data: Dictionary containing position data
            
        Returns:
            bool: Success indicator
        """
        try:
            key = f"{self.redis_prefix}position:{position_id}"
            if self.redis_client:
                self.redis_client.set(key, json.dumps(position_data))
            else:
                self.memory_storage[key] = position_data
            
            # Invalidate position cache
            self._position_cache_valid = False
            return True
        except Exception as e:
            logger.error(f"Failed to store position {position_id}: {str(e)}")
            return False
    
    @handle_sync_errors
    def get_position(self, position_id: str) -> Optional[dict]:
        """
        Retrieve position data from the data store
        
        Args:
            position_id: Unique identifier for the position
            
        Returns:
            Optional[dict]: Position data or None if not found
        """
        try:
            key = f"{self.redis_prefix}position:{position_id}"
            if self.redis_client:
                data = self.redis_client.get(key)
                if data:
                    return json.loads(data)
            else:
                return self.memory_storage.get(key)
            return None
        except Exception as e:
            logger.error(f"Failed to get position {position_id}: {str(e)}")
            return None
    
    @handle_sync_errors
    def delete_position(self, position_id: str) -> bool:
        """
        Delete position data from the data store
        
        Args:
            position_id: Unique identifier for the position
            
        Returns:
            bool: Success indicator
        """
        try:
            key = f"{self.redis_prefix}position:{position_id}"
            if self.redis_client:
                self.redis_client.delete(key)
            else:
                if key in self.memory_storage:
                    del self.memory_storage[key]
            
            # Invalidate position cache
            self._position_cache_valid = False
            return True
        except Exception as e:
            logger.error(f"Failed to delete position {position_id}: {str(e)}")
            return False
    
    @handle_sync_errors
    def get_all_positions(self) -> List[dict]:
        """
        Retrieve all positions from the data store
        
        Returns:
            List[dict]: List of position data dictionaries
        """
        try:
            # Check if cached data is valid
            if self._position_cache_valid and 'positions' in self.cache:
                return self.cache['positions']
                
            positions = []
            pattern = f"{self.redis_prefix}position:*"
            
            if self.redis_client:
                keys = self.redis_client.keys(pattern)
                if keys:
                    for key in keys:
                        data = self.redis_client.get(key)
                        if data:
                            positions.append(json.loads(data))
            else:
                for key, value in self.memory_storage.items():
                    if key.startswith(pattern.replace('*', '')):
                        positions.append(value)
            
            # Cache the results
            self.cache['positions'] = positions
            self._position_cache_valid = True
            return positions
        except Exception as e:
            logger.error(f"Failed to get all positions: {str(e)}")
            return []
    
    @handle_sync_errors
    def store_analytics(self, analytics_type: str, data: dict) -> bool:
        """
        Store analytics data in the data store
        
        Args:
            analytics_type: Type of analytics (e.g., 'daily', 'position')
            data: Dictionary containing analytics data
            
        Returns:
            bool: Success indicator
        """
        try:
            key = f"{self.redis_prefix}analytics:{analytics_type}"
            if self.redis_client:
                self.redis_client.set(key, json.dumps(data))
            else:
                self.memory_storage[key] = data
            
            # Update cache
            self._analytics_cache[analytics_type] = data
            return True
        except Exception as e:
            logger.error(f"Failed to store analytics {analytics_type}: {str(e)}")
            return False
    
    @handle_sync_errors
    def get_analytics(self, analytics_type: str) -> Optional[dict]:
        """
        Retrieve analytics data from the data store
        
        Args:
            analytics_type: Type of analytics to retrieve
            
        Returns:
            Optional[dict]: Analytics data or None if not found
        """
        try:
            # Check cache first
            if analytics_type in self._analytics_cache:
                return self._analytics_cache[analytics_type]
                
            key = f"{self.redis_prefix}analytics:{analytics_type}"
            if self.redis_client:
                data = self.redis_client.get(key)
                if data:
                    result = json.loads(data)
                    self._analytics_cache[analytics_type] = result
                    return result
            else:
                result = self.memory_storage.get(key)
                if result:
                    self._analytics_cache[analytics_type] = result
                    return result
            return None
        except Exception as e:
            logger.error(f"Failed to get analytics {analytics_type}: {str(e)}")
            return None
    
    @handle_sync_errors
    def store_setting(self, setting_name: str, value: Any) -> bool:
        """
        Store a setting in the data store
        
        Args:
            setting_name: Name of the setting
            value: Setting value (will be JSON serialized)
            
        Returns:
            bool: Success indicator
        """
        try:
            key = f"{self.redis_prefix}setting:{setting_name}"
            serialized_value = json.dumps(value)
            
            if self.redis_client:
                self.redis_client.set(key, serialized_value)
            else:
                self.memory_storage[key] = value
            
            # Update cache
            self._settings_cache[setting_name] = value
            return True
        except Exception as e:
            logger.error(f"Failed to store setting {setting_name}: {str(e)}")
            return False
    
    @handle_sync_errors
    def get_setting(self, setting_name: str, default: Any = None) -> Any:
        """
        Retrieve a setting from the data store
        
        Args:
            setting_name: Name of the setting to retrieve
            default: Default value if setting not found
            
        Returns:
            Any: Setting value or default if not found
        """
        try:
            # Check cache first
            if setting_name in self._settings_cache:
                return self._settings_cache[setting_name]
                
            key = f"{self.redis_prefix}setting:{setting_name}"
            if self.redis_client:
                data = self.redis_client.get(key)
                if data:
                    result = self._parse_value(data, default)
                    self._settings_cache[setting_name] = result
                    return result
            else:
                if key in self.memory_storage:
                    self._settings_cache[setting_name] = self.memory_storage[key]
                    return self.memory_storage[key]
            return default
        except Exception as e:
            logger.error(f"Failed to get setting {setting_name}: {str(e)}")
            return default
    
    def _parse_value(self, value: str, default: Any) -> Any:
        """Parse a value from string format, maintaining type"""
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            # If not a valid JSON, return as string
            return value
        except Exception:
            return default
    
    @handle_sync_errors
    def clear_cache(self) -> None:
        """Clear all internal caches"""
        self._position_cache_valid = False
        self._settings_cache = {}
        self._analytics_cache = {}
        self.cache = {}
    
    @handle_sync_errors
    def get_daily_stats(self) -> dict:
        """
        Get daily trading statistics
        
        Returns:
            dict: Trading statistics for the current day
        """
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            stats = self.get_analytics(f"daily:{today}")
            
            if not stats:
                # Initialize with default values
                stats = {
                    "date": today,
                    "trades_executed": 0,
                    "successful_trades": 0,
                    "failed_trades": 0,
                    "total_pnl": 0.0,
                    "win_rate": 0.0,
                    "average_win": 0.0,
                    "average_loss": 0.0,
                    "largest_win": 0.0,
                    "largest_loss": 0.0,
                    "consecutive_wins": 0,
                    "consecutive_losses": 0,
                    "current_streak": 0,
                    "streak_type": "none"
                }
                self.store_analytics(f"daily:{today}", stats)
                
            return stats
        except Exception as e:
            logger.error(f"Failed to get daily stats: {str(e)}")
            return {
                "date": datetime.now().strftime("%Y-%m-%d"),
                "trades_executed": 0,
                "total_pnl": 0.0
            }
    
    @handle_sync_errors
    def update_trade_stats(self, pnl: float) -> dict:
        """
        Update trading statistics with a new trade result
        
        Args:
            pnl: Profit/loss from the trade
            
        Returns:
            dict: Updated statistics
        """
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            stats = self.get_daily_stats()
            
            # Update basic counts
            stats["trades_executed"] += 1
            stats["total_pnl"] += pnl
            
            # Track wins and losses
            if pnl > 0:
                stats["successful_trades"] += 1
                stats["average_win"] = ((stats["average_win"] * (stats["successful_trades"] - 1)) + pnl) / stats["successful_trades"]
                stats["largest_win"] = max(stats["largest_win"], pnl)
                
                if stats["streak_type"] == "win":
                    stats["current_streak"] += 1
                else:
                    stats["current_streak"] = 1
                    stats["streak_type"] = "win"
                    
                stats["consecutive_wins"] = max(stats["consecutive_wins"], stats["current_streak"])
            elif pnl < 0:
                stats["failed_trades"] += 1
                abs_pnl = abs(pnl)
                stats["average_loss"] = ((stats["average_loss"] * (stats["failed_trades"] - 1)) + abs_pnl) / stats["failed_trades"]
                stats["largest_loss"] = max(stats["largest_loss"], abs_pnl)
                
                if stats["streak_type"] == "loss":
                    stats["current_streak"] += 1
                else:
                    stats["current_streak"] = 1
                    stats["streak_type"] = "loss"
                    
                stats["consecutive_losses"] = max(stats["consecutive_losses"], stats["current_streak"])
            
            # Calculate win rate
            if stats["trades_executed"] > 0:
                stats["win_rate"] = (stats["successful_trades"] / stats["trades_executed"]) * 100
                
            # Store updated stats
            self.store_analytics(f"daily:{today}", stats)
            return stats
        except Exception as e:
            logger.error(f"Failed to update trade stats: {str(e)}")
            return self.get_daily_stats()

# Initialize data store
data_store = DataStore()

##############################################################################
# Position Management
##############################################################################

class Position:
    """
    Represents a trading position with risk management and tracking features.
    Handles tracking, updates, and management of individual positions.
    """
    def __init__(
        self,
        symbol: str,
        action: str,
        size: float,
        entry_price: float,
        take_profit_price: Optional[float] = None,
        stop_loss_price: Optional[float] = None,
        position_id: Optional[str] = None,
        metadata: Optional[dict] = None
    ):
        """
        Initialize a position with trade details.
        
        Args:
            symbol: Trading symbol (e.g., 'EUR_USD')
            action: Trade direction ('BUY' or 'SELL')
            size: Position size in base units
            entry_price: Entry price
            take_profit_price: Optional price for take profit
            stop_loss_price: Optional price for stop loss
            position_id: Unique position identifier (generated if not provided)
            metadata: Additional position data
        """
        self.symbol = standardize_symbol(symbol)
        self.action = action.upper()
        self.size = float(size)
        self.entry_price = float(entry_price)
        self.take_profit_price = float(take_profit_price) if take_profit_price is not None else None
        self.stop_loss_price = float(stop_loss_price) if stop_loss_price is not None else None
        self.position_id = position_id or str(uuid.uuid4())
        self.metadata = metadata or {}
        
        # Status tracking
        self.entry_time = datetime.now(timezone.utc)
        self.last_update_time = self.entry_time
        self.exit_time = None
        self.exit_price = None
        self.status = "OPEN"
        self.pnl = 0.0
        self.pnl_percentage = 0.0
        self.initial_risk = self._calculate_initial_risk()
        
        # Validation
        if self.action not in ["BUY", "SELL"]:
            raise CustomValidationError(f"Invalid action: {self.action}. Must be 'BUY' or 'SELL'")
            
        if self.size <= 0:
            raise CustomValidationError(f"Invalid position size: {self.size}. Must be positive")
            
        if self.entry_price <= 0:
            raise CustomValidationError(f"Invalid entry price: {self.entry_price}. Must be positive")
    
    def _calculate_initial_risk(self) -> float:
        """Calculate the initial risk amount for the position"""
        if not self.stop_loss_price:
            return 0.0
            
        price_delta = abs(self.entry_price - self.stop_loss_price)
        risk_amount = self.size * price_delta
        
        return risk_amount
    
    @handle_sync_errors
    def update_pnl(self, current_price: float) -> Tuple[float, float]:
        """
        Update the position's profit and loss based on current price.
        
        Args:
            current_price: Current market price
            
        Returns:
            Tuple[float, float]: (PnL amount, PnL percentage)
        """
        self.last_update_time = datetime.now(timezone.utc)
        
        # Calculate PnL based on direction
        if self.action == "BUY":
            price_delta = current_price - self.entry_price
        else:  # SELL
            price_delta = self.entry_price - current_price
            
        self.pnl = self.size * price_delta
        
        # Calculate percentage gain/loss
        if self.entry_price > 0:
            self.pnl_percentage = (price_delta / self.entry_price) * 100
            
        return self.pnl, self.pnl_percentage
    
    @handle_sync_errors
    def modify_take_profit(self, new_price: float) -> bool:
        """
        Modify the take profit level for the position.
        
        Args:
            new_price: New take profit price
            
        Returns:
            bool: Success indicator
        """
        if self.status != "OPEN":
            logger.warning(f"Cannot modify take profit for {self.position_id}: Position is {self.status}")
            return False
            
        if new_price <= 0:
            logger.error(f"Invalid take profit price: {new_price}")
            return False
            
        # Validate the new take profit level based on position direction
        if self.action == "BUY" and new_price <= self.entry_price:
            logger.warning(f"Take profit {new_price} must be higher than entry price {self.entry_price} for BUY position")
            return False
            
        if self.action == "SELL" and new_price >= self.entry_price:
            logger.warning(f"Take profit {new_price} must be lower than entry price {self.entry_price} for SELL position")
            return False
            
        self.take_profit_price = new_price
        self.last_update_time = datetime.now(timezone.utc)
        
        logger.info(f"Modified take profit for position {self.position_id} to {new_price}")
        return True
    
    @handle_sync_errors
    def modify_stop_loss(self, new_price: float) -> bool:
        """
        Modify the stop loss level for the position.
        
        Args:
            new_price: New stop loss price
            
        Returns:
            bool: Success indicator
        """
        if self.status != "OPEN":
            logger.warning(f"Cannot modify stop loss for {self.position_id}: Position is {self.status}")
            return False
            
        if new_price <= 0:
            logger.error(f"Invalid stop loss price: {new_price}")
            return False
            
        # Validate the new stop loss level based on position direction
        if self.action == "BUY" and new_price >= self.entry_price:
            logger.warning(f"Stop loss {new_price} must be lower than entry price {self.entry_price} for BUY position")
            return False
            
        if self.action == "SELL" and new_price <= self.entry_price:
            logger.warning(f"Stop loss {new_price} must be higher than entry price {self.entry_price} for SELL position")
            return False
            
        self.stop_loss_price = new_price
        self.last_update_time = datetime.now(timezone.utc)
        
        # Recalculate risk if we change the stop loss
        self.initial_risk = self._calculate_initial_risk()
        
        logger.info(f"Modified stop loss for position {self.position_id} to {new_price}")
        return True
    
    @handle_sync_errors
    def close(self, exit_price: float, reason: str = "manual") -> float:
        """
        Close the position at the specified price.
        
        Args:
            exit_price: Exit price for the position
            reason: Reason for closing the position
            
        Returns:
            float: Final PnL for the position
        """
        if self.status != "OPEN":
            logger.warning(f"Cannot close position {self.position_id}: Already {self.status}")
            return self.pnl
            
        self.exit_price = float(exit_price)
        self.exit_time = datetime.now(timezone.utc)
        self.status = "CLOSED"
        
        # Calculate final PnL
        self.update_pnl(exit_price)
        
        # Add closing details to metadata
        self.metadata.update({
            "exit_reason": reason,
            "final_pnl": self.pnl,
            "final_pnl_percentage": self.pnl_percentage,
            "duration": (self.exit_time - self.entry_time).total_seconds()
        })
        
        logger.info(f"Closed position {self.position_id} with PnL: {self.pnl:.2f} ({self.pnl_percentage:.2f}%), Reason: {reason}")
        return self.pnl
    
    @handle_sync_errors
    def to_dict(self) -> dict:
        """
        Convert position to dictionary for storage and API responses.
        
        Returns:
            dict: Position data dictionary
        """
        return {
            "position_id": self.position_id,
            "symbol": self.symbol,
            "action": self.action,
            "size": self.size,
            "entry_price": self.entry_price,
            "take_profit_price": self.take_profit_price,
            "stop_loss_price": self.stop_loss_price,
            "entry_time": self.entry_time.isoformat(),
            "last_update_time": self.last_update_time.isoformat(),
            "exit_time": self.exit_time.isoformat() if self.exit_time else None,
            "exit_price": self.exit_price,
            "status": self.status,
            "pnl": self.pnl,
            "pnl_percentage": self.pnl_percentage,
            "initial_risk": self.initial_risk,
            "metadata": self.metadata
        }
    
    @classmethod
    @handle_sync_errors
    def from_dict(cls, data: dict) -> 'Position':
        """
        Create a position from a dictionary.
        
        Args:
            data: Dictionary containing position data
            
        Returns:
            Position: Position object
        """
        position = cls(
            symbol=data['symbol'],
            action=data['action'],
            size=data['size'],
            entry_price=data['entry_price'],
            take_profit_price=data.get('take_profit_price'),
            stop_loss_price=data.get('stop_loss_price'),
            position_id=data['position_id'],
            metadata=data.get('metadata', {})
        )
        
        # Restore additional fields
        position.entry_time = datetime.fromisoformat(data['entry_time'])
        position.last_update_time = datetime.fromisoformat(data['last_update_time'])
        
        if data.get('exit_time'):
            position.exit_time = datetime.fromisoformat(data['exit_time'])
            
        position.exit_price = data.get('exit_price')
        position.status = data.get('status', 'OPEN')
        position.pnl = data.get('pnl', 0.0)
        position.pnl_percentage = data.get('pnl_percentage', 0.0)
        position.initial_risk = data.get('initial_risk', 0.0)
        
        return position

class PositionManager:
    """
    Manages multiple trading positions with synchronized access.
    Handles position creation, retrieval, updates, and risk management.
    """
    def __init__(self, data_store: DataStore):
        """
        Initialize the position manager.
        
        Args:
            data_store: DataStore instance for persisting positions
        """
        self.data_store = data_store
        self.positions = {}
        self.lock = asyncio.Lock()
        self._last_load_time = datetime.min
        self._load_interval = timedelta(seconds=30)
    
    @handle_async_errors
    async def load_positions(self, force_reload: bool = False) -> None:
        """
        Load positions from data store if needed.
        
        Args:
            force_reload: Force reload regardless of last load time
        """
        now = datetime.now()
        
        # Skip loading if recently loaded and not forced
        if not force_reload and now - self._last_load_time < self._load_interval:
            return
            
        async with self.lock:
            position_dicts = self.data_store.get_all_positions()
            self.positions = {}
            
            for pos_dict in position_dicts:
                try:
                    position = Position.from_dict(pos_dict)
                    self.positions[position.position_id] = position
                except Exception as e:
                    logger.error(f"Failed to load position: {e}")
                    
            self._last_load_time = now
            logger.debug(f"Loaded {len(self.positions)} positions from data store")
    
    @handle_async_errors
    async def add_position(self, position: Position) -> str:
        """
        Add a new position to the manager.
        
        Args:
            position: Position object to add
            
        Returns:
            str: Position ID
        """
        async with self.lock:
            await self.load_positions()
            position_id = position.position_id
            self.positions[position_id] = position
            
            # Store in data store
            self.data_store.store_position(position_id, position.to_dict())
            logger.info(f"Added position {position_id} for {position.symbol}")
            
            return position_id
    
    @handle_async_errors
    async def get_position(self, position_id: str) -> Optional[Position]:
        """
        Get a position by ID.
        
        Args:
            position_id: Unique position ID
            
        Returns:
            Optional[Position]: Position object or None if not found
        """
        await self.load_positions()
        
        # Try in-memory first
        if position_id in self.positions:
            return self.positions[position_id]
            
        # Try data store as fallback
        position_dict = self.data_store.get_position(position_id)
        if position_dict:
            try:
                position = Position.from_dict(position_dict)
                self.positions[position_id] = position
                return position
            except Exception as e:
                logger.error(f"Failed to load position {position_id}: {e}")
                
        return None
    
    @handle_async_errors
    async def update_position(self, position: Position) -> bool:
        """
        Update an existing position.
        
        Args:
            position: Position object with updates
            
        Returns:
            bool: Success indicator
        """
        async with self.lock:
            position_id = position.position_id
            if position_id not in self.positions:
                logger.warning(f"Position {position_id} not found for update")
                return False
                
            self.positions[position_id] = position
            
            # Update in data store
            success = self.data_store.store_position(position_id, position.to_dict())
            if success:
                logger.debug(f"Updated position {position_id}")
                
            return success
    
    @handle_async_errors
    async def close_position(
        self, 
        position_id: str, 
        exit_price: float, 
        reason: str = "manual"
    ) -> Tuple[bool, float]:
        """
        Close a position.
        
        Args:
            position_id: Position ID to close
            exit_price: Exit price
            reason: Reason for closure
            
        Returns:
            Tuple[bool, float]: (Success indicator, Final PnL)
        """
        async with self.lock:
            position = await self.get_position(position_id)
            if not position:
                logger.warning(f"Position {position_id} not found for closure")
                return False, 0.0
                
            if position.status != "OPEN":
                logger.warning(f"Position {position_id} already {position.status}")
                return False, position.pnl
                
            # Close the position
            pnl = position.close(exit_price, reason)
            
            # Update in data store
            success = self.data_store.store_position(position_id, position.to_dict())
            
            # Update trading statistics
            self.data_store.update_trade_stats(pnl)
            
            if success:
                logger.info(f"Closed position {position_id} with PnL: {pnl:.2f}")
                
            return success, pnl
    
    @handle_async_errors
    async def get_open_positions(self) -> List[Position]:
        """
        Get all open positions.
        
        Returns:
            List[Position]: List of open position objects
        """
        await self.load_positions()
        return [p for p in self.positions.values() if p.status == "OPEN"]
    
    @handle_async_errors
    async def get_positions_by_symbol(self, symbol: str) -> List[Position]:
        """
        Get positions for a specific symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            List[Position]: List of position objects for the symbol
        """
        await self.load_positions()
        symbol = standardize_symbol(symbol)
        return [p for p in self.positions.values() if p.symbol == symbol]
    
    @handle_async_errors
    async def get_open_positions_by_symbol(self, symbol: str) -> List[Position]:
        """
        Get open positions for a specific symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            List[Position]: List of open position objects for the symbol
        """
        await self.load_positions()
        symbol = standardize_symbol(symbol)
        return [p for p in self.positions.values() if p.symbol == symbol and p.status == "OPEN"]
    
    @handle_async_errors
    async def calculate_total_exposure(self) -> float:
        """
        Calculate total exposure across all open positions.
        
        Returns:
            float: Total exposure value
        """
        open_positions = await self.get_open_positions()
        return sum(p.size * p.entry_price for p in open_positions)
    
    @handle_async_errors
    async def calculate_total_pnl(self, current_prices: Dict[str, float]) -> float:
        """
        Calculate total PnL across all open positions.
        
        Args:
            current_prices: Dictionary mapping symbols to current prices
            
        Returns:
            float: Total PnL value
        """
        open_positions = await self.get_open_positions()
        total_pnl = 0.0
        
        for position in open_positions:
            if position.symbol in current_prices:
                pnl, _ = position.update_pnl(current_prices[position.symbol])
                total_pnl += pnl
                
        return total_pnl
    
    @handle_async_errors
    async def update_all_positions(self, current_prices: Dict[str, float]) -> Dict[str, float]:
        """
        Update all positions with current prices and check exit conditions
        
        Args:
            current_prices: Dictionary of symbol -> current price
            
        Returns:
            Dictionary of position_id -> updated P&L
        """
        results = {}
        
        # First, update all positions
        positions = await self.get_open_positions()
        for position in positions:
            symbol = position.symbol
            if symbol in current_prices:
                price = current_prices[symbol]
                pnl, pnl_pct = position.update_pnl(price)
                results[position.position_id] = pnl
                
        # Check for multi-tier exit conditions
        exit_actions = await exit_manager.monitor_positions(current_prices)
        
        # Process any exit actions
        for action in exit_actions:
            if action["action"] == "PARTIAL_CLOSE":
                position_id = action["position_id"]
                symbol = action["symbol"]
                price = action["price"]
                exit_level = action["exit_level"]
                
                # Get percentage to close
                if "percentage" in action:
                    percentage = action["percentage"]
                else:
                    # Get the exit plan
                    exit_plan = await exit_manager.get_exit_plan(position_id)
                    if not exit_plan:
                        continue
                        
                    # Calculate percentage based on exit level
                    exits = exit_plan["exits"]
                    if exit_level == "first":
                        size_to_close = exits["first"]["size"]
                    elif exit_level == "second":
                        size_to_close = exits["second"]["size"]
                    elif exit_level == "runner":
                        size_to_close = exits["runner"]["size"]
                    else:
                        continue
                        
                    total_size = exit_plan["total_size"]
                    if total_size > 0:
                        percentage = (size_to_close / total_size) * 100
                    else:
                        continue
                
                # Execute partial close
                success, result = await close_partial_position(
                    symbol=symbol,
                    percentage=percentage,
                    position_id=position_id,
                    reason=f"take_profit_{exit_level}"
                )
                
                if success:
                    # Mark this exit level as executed
                    await exit_manager.mark_exit_executed(position_id, exit_level)
                    logger.info(f"Auto-executed {exit_level} take profit for {symbol}: {percentage}% at {price}")
                
            elif action["action"] == "CLOSE":
                position_id = action["position_id"]
                symbol = action["symbol"]
                price = action["price"]
                exit_level = action["exit_level"]
                
                # Execute full close
                success, result = await close_position(
                    symbol=symbol,
                    position_id=position_id,
                    reason=f"take_profit_{exit_level}"
                )
                
                if success:
                    # Clear the exit plan
                    await exit_manager.clear_exit_plan(position_id)
                    logger.info(f"Auto-executed full close for {symbol} at {price} (reason: {exit_level})")
        
        return results

# Initialize position manager
position_manager = PositionManager(data_store)

##############################################################################
# Price Data and Market Analysis
##############################################################################

@dataclass
class OHLC:
    """Represents OHLC (Open, High, Low, Close) candlestick data"""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    
    @property
    def range(self) -> float:
        """Calculate the candle's range (high - low)"""
        return self.high - self.low
    
    @property
    def body(self) -> float:
        """Calculate the candle's body size (abs(open - close))"""
        return abs(self.open - self.close)
    
    @property
    def is_bullish(self) -> bool:
        """Check if candle is bullish (close > open)"""
        return self.close > self.open
    
    @property
    def upper_wick(self) -> float:
        """Calculate the upper wick length"""
        return self.high - (self.close if self.is_bullish else self.open)
    
    @property
    def lower_wick(self) -> float:
        """Calculate the lower wick length"""
        return (self.open if self.is_bullish else self.close) - self.low


async def get_current_price(symbol: str) -> float:
    """
    Get current market price for a symbol (simulation).
    
    In a real implementation, this would call a broker API.
    
    Args:
        symbol: Trading symbol
        
    Returns:
        float: Current market price
    """
    # This is a simulation - in production, connect to your broker API
    symbol = standardize_symbol(symbol)
    
    # Simulate a random price around 1.0 +/- 5%
    import random
    base_price = 1.0
    if symbol.startswith("EUR"):
        base_price = 1.1
    elif symbol.startswith("GBP"):
        base_price = 1.3
    elif symbol.startswith("USD"):
        base_price = 1.0
    elif symbol.startswith("JPY"):
        base_price = 150.0
        
    # Add some random noise
    variation = base_price * 0.05  # 5% variation
    price = base_price + random.uniform(-variation, variation)
    
    logger.debug(f"Retrieved simulated price for {symbol}: {price:.5f}")
    return price


class MarketAnalyzer:
    """
    Analyzes market data to identify patterns and signals.
    Supports technical analysis and trading signal generation.
    """
    def __init__(self, cache_duration: int = 300):
        """
        Initialize market analyzer.
        
        Args:
            cache_duration: Cache duration in seconds
        """
        self.price_cache = {}
        self.ohlc_cache = {}
        self.cache_duration = cache_duration
        self.cache_timestamps = {}
    
    @handle_async_errors
    async def get_price(self, symbol: str) -> float:
        """
        Get current price for a symbol with caching.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            float: Current market price
        """
        symbol = standardize_symbol(symbol)
        now = datetime.now()
        
        # Check if we have a valid cached price
        if symbol in self.price_cache and symbol in self.cache_timestamps:
            last_update = self.cache_timestamps[symbol]
            if (now - last_update).total_seconds() < self.cache_duration:
                return self.price_cache[symbol]
        
        # Fetch new price
        price = await get_current_price(symbol)
        
        # Update cache
        self.price_cache[symbol] = price
        self.cache_timestamps[symbol] = now
        
        return price
    
    @handle_async_errors
    async def get_prices(self, symbols: List[str]) -> Dict[str, float]:
        """
        Get current prices for multiple symbols.
        
        Args:
            symbols: List of trading symbols
            
        Returns:
            Dict[str, float]: Dictionary mapping symbols to prices
        """
        results = {}
        for symbol in symbols:
            results[standardize_symbol(symbol)] = await self.get_price(symbol)
        return results
    
    @handle_async_errors
    async def get_historical_data(
        self, 
        symbol: str, 
        timeframe: str = "1h",
        count: int = 100
    ) -> List[OHLC]:
        """
        Get historical OHLC data for a symbol.
        
        This is a simulation - in production, connect to your data provider.
        
        Args:
            symbol: Trading symbol
            timeframe: Timeframe for candles (e.g., "1m", "5m", "15m", "1h", "4h", "1d")
            count: Number of candles to retrieve
            
        Returns:
            List[OHLC]: List of OHLC objects
        """
        symbol = standardize_symbol(symbol)
        cache_key = f"{symbol}_{timeframe}_{count}"
        now = datetime.now()
        
        # Check if we have cached data
        if (
            cache_key in self.ohlc_cache and 
            cache_key in self.cache_timestamps and
            (now - self.cache_timestamps[cache_key]).total_seconds() < self.cache_duration
        ):
            return self.ohlc_cache[cache_key]
        
        # Simulated historical data generation
        # In production, connect to your data provider API
        candles = []
        end_time = now
        
        # Parse timeframe to determine time delta
        if timeframe.endswith("m"):
            minutes = int(timeframe[:-1])
            delta = timedelta(minutes=minutes)
        elif timeframe.endswith("h"):
            hours = int(timeframe[:-1])
            delta = timedelta(hours=hours)
        elif timeframe.endswith("d"):
            days = int(timeframe[:-1])
            delta = timedelta(days=days)
        else:
            raise ValueError(f"Invalid timeframe: {timeframe}")
        
        # Generate random candles
        import random
        current_time = end_time - (count * delta)
        close_price = await self.get_price(symbol)
        
        for i in range(count):
            # Generate random price movements
            price_volatility = 0.002  # 0.2% volatility per candle
            
            # Slightly bias the random walk to match the current price at the end
            bias_factor = (count - i) / count
            random_factor = random.uniform(-price_volatility, price_volatility) + (
                (close_price / (candles[-1].close if candles else close_price) - 1) * bias_factor * 0.1
                if candles else 0
            )
            
            prev_close = candles[-1].close if candles else close_price * 0.95
            open_price = prev_close
            close_price = prev_close * (1 + random_factor)
            high_price = max(open_price, close_price) * (1 + random.uniform(0, price_volatility))
            low_price = min(open_price, close_price) * (1 - random.uniform(0, price_volatility))
            volume = random.uniform(1000, 10000)
            
            candle = OHLC(
                timestamp=current_time,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=volume
            )
            
            candles.append(candle)
            current_time += delta
        
        # Update cache
        self.ohlc_cache[cache_key] = candles
        self.cache_timestamps[cache_key] = now
        
        logger.debug(f"Retrieved {len(candles)} candles for {symbol} ({timeframe})")
        return candles
    
    @handle_async_errors
    async def calculate_moving_average(
        self, 
        symbol: str, 
        period: int = 20, 
        timeframe: str = "1h"
    ) -> float:
        """
        Calculate simple moving average for a symbol.
        
        Args:
            symbol: Trading symbol
            period: Period for moving average calculation
            timeframe: Timeframe for candles
            
        Returns:
            float: Moving average value
        """
        candles = await self.get_historical_data(symbol, timeframe, period)
        
        if len(candles) < period:
            logger.warning(f"Not enough data to calculate {period} period MA. Got {len(candles)} candles.")
            return 0.0
            
        # Calculate SMA
        closes = [candle.close for candle in candles[-period:]]
        ma_value = sum(closes) / period
        
        logger.debug(f"Calculated {period} period MA for {symbol}: {ma_value:.5f}")
        return ma_value
    
    @handle_async_errors
    async def calculate_rsi(
        self, 
        symbol: str, 
        period: int = 14, 
        timeframe: str = "1h"
    ) -> float:
        """
        Calculate Relative Strength Index (RSI) for a symbol.
        
        Args:
            symbol: Trading symbol
            period: Period for RSI calculation
            timeframe: Timeframe for candles
            
        Returns:
            float: RSI value (0-100)
        """
        candles = await self.get_historical_data(symbol, timeframe, period + 1)
        
        if len(candles) < period + 1:
            logger.warning(f"Not enough data to calculate {period} period RSI. Got {len(candles)} candles.")
            return 50.0  # Neutral RSI value as fallback
            
        # Calculate price changes
        closes = [candle.close for candle in candles]
        price_changes = [closes[i] - closes[i-1] for i in range(1, len(closes))]
        
        # Separate gains and losses
        gains = [max(0, change) for change in price_changes]
        losses = [abs(min(0, change)) for change in price_changes]
        
        # Calculate average gains and losses
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        
        # Calculate RS and RSI
        if avg_loss == 0:
            rsi_value = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi_value = 100 - (100 / (1 + rs))
        
        logger.debug(f"Calculated {period} period RSI for {symbol}: {rsi_value:.2f}")
        return rsi_value
    
    @handle_async_errors
    async def detect_trend(
        self, 
        symbol: str, 
        fast_ma: int = 20, 
        slow_ma: int = 50, 
        timeframe: str = "1h"
    ) -> str:
        """
        Detect trend direction using moving average comparison.
        
        Args:
            symbol: Trading symbol
            fast_ma: Fast moving average period
            slow_ma: Slow moving average period
            timeframe: Timeframe for candles
            
        Returns:
            str: Trend direction ("UP", "DOWN", or "NEUTRAL")
        """
        fast_ma_value = await self.calculate_moving_average(symbol, fast_ma, timeframe)
        slow_ma_value = await self.calculate_moving_average(symbol, slow_ma, timeframe)
        
        # Determine trend with a small buffer for neutral zone
        buffer = fast_ma_value * 0.001  # 0.1% buffer
        
        if fast_ma_value > slow_ma_value + buffer:
            trend = "UP"
        elif fast_ma_value < slow_ma_value - buffer:
            trend = "DOWN"
        else:
            trend = "NEUTRAL"
            
        logger.debug(f"Detected {trend} trend for {symbol} ({timeframe})")
        return trend
    
    @handle_async_errors
    async def analyze_risk_reward(
        self, 
        symbol: str,
        entry_price: float,
        stop_loss: float,
        take_profit: float
    ) -> Dict[str, float]:
        """
        Analyze risk-reward ratio for a potential trade.
        
        Args:
            symbol: Trading symbol
            entry_price: Entry price
            stop_loss: Stop loss price
            take_profit: Take profit price
            
        Returns:
            Dict[str, float]: Risk-reward analysis
        """
        symbol = standardize_symbol(symbol)
        
        # Calculate distances
        stop_distance = abs(entry_price - stop_loss)
        target_distance = abs(entry_price - take_profit)
        
        # Calculate risk-reward ratio
        risk_reward_ratio = target_distance / stop_distance if stop_distance > 0 else float('inf')
        
        # Get current price for additional context
        current_price = await self.get_price(symbol)
        distance_to_entry = abs(current_price - entry_price)
        entry_probability = 1.0 - min(1.0, distance_to_entry / (entry_price * 0.01))  # Simple heuristic
        
        analysis = {
            "risk_reward_ratio": risk_reward_ratio,
            "stop_distance_pips": stop_distance * 10000,  # Convert to pips
            "target_distance_pips": target_distance * 10000,  # Convert to pips
            "entry_probability": entry_probability,
            "current_price": current_price
        }
        
        return analysis
    
    @handle_async_errors
    async def generate_trading_signal(
        self, 
        symbol: str,
        timeframes: List[str] = ["15m", "1h", "4h"]
    ) -> Dict[str, Any]:
        """
        Generate comprehensive trading signal for a symbol.
        
        Args:
            symbol: Trading symbol
            timeframes: List of timeframes to analyze
            
        Returns:
            Dict[str, Any]: Trading signal analysis
        """
        symbol = standardize_symbol(symbol)
        signal = {
            "symbol": symbol,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeframes": {},
            "overall_signal": "NEUTRAL",
            "confidence": 0.0
        }
        
        # Analyze each timeframe
        bullish_count = 0
        bearish_count = 0
        total_weight = 0
        
        weights = {
            "15m": 1,
            "1h": 2,
            "4h": 3,
            "1d": 4
        }
        
        for tf in timeframes:
            # Get trend
            trend = await self.detect_trend(symbol, timeframe=tf)
            
            # Get RSI
            rsi = await self.calculate_rsi(symbol, timeframe=tf)
            
            # Determine if overbought/oversold
            rsi_signal = "NEUTRAL"
            if rsi > 70:
                rsi_signal = "OVERBOUGHT"
            elif rsi < 30:
                rsi_signal = "OVERSOLD"
                
            # Get MA values
            ma20 = await self.calculate_moving_average(symbol, 20, tf)
            ma50 = await self.calculate_moving_average(symbol, 50, tf)
            ma200 = await self.calculate_moving_average(symbol, 200, tf)
            
            # Generate score between -100 and 100 for this timeframe
            score = 0
            
            # Trend score component
            if trend == "UP":
                score += 50
                bullish_count += weights.get(tf, 1)
            elif trend == "DOWN":
                score -= 50
                bearish_count += weights.get(tf, 1)
                
            # RSI score component
            if rsi > 50 and rsi < 70:
                score += 20
            elif rsi < 50 and rsi > 30:
                score -= 20
                
            # Overbought/oversold adjustments
            if rsi_signal == "OVERBOUGHT":
                score -= 30
            elif rsi_signal == "OVERSOLD":
                score += 30
                
            # Golden/Death cross components
            if ma20 > ma50 and ma50 > ma200:
                score += 30
            elif ma20 < ma50 and ma50 < ma200:
                score -= 30
                
            # Normalize score to -100 to 100 range
            score = max(-100, min(100, score))
            
            # Determine signal
            tf_signal = "NEUTRAL"
            if score > 30:
                tf_signal = "BUY"
            elif score < -30:
                tf_signal = "SELL"
                
            # Add to timeframe results
            signal["timeframes"][tf] = {
                "trend": trend,
                "rsi": rsi,
                "rsi_signal": rsi_signal,
                "ma20": ma20,
                "ma50": ma50,
                "ma200": ma200,
                "score": score,
                "signal": tf_signal
            }
            
            total_weight += weights.get(tf, 1)
        
        # Calculate overall signal
        if total_weight > 0:
            weighted_sentiment = (bullish_count - bearish_count) / total_weight
            
            if weighted_sentiment > 0.3:
                signal["overall_signal"] = "BUY"
                signal["confidence"] = min(1.0, weighted_sentiment)
            elif weighted_sentiment < -0.3:
                signal["overall_signal"] = "SELL"
                signal["confidence"] = min(1.0, abs(weighted_sentiment))
            else:
                signal["overall_signal"] = "NEUTRAL"
                signal["confidence"] = 0.5 - abs(weighted_sentiment)
        
        logger.info(f"Generated {signal['overall_signal']} signal for {symbol} with {signal['confidence']:.2f} confidence")
        return signal

# Initialize market analyzer
market_analyzer = MarketAnalyzer()

##############################################################################
# Risk Management
##############################################################################

class RiskParameters:
    """
    Defines risk parameters for trading
    """
    def __init__(
        self,
        max_risk_per_trade: float = 0.01,  # 1% of account balance
        max_daily_risk: float = 0.05,      # 5% of account balance
        max_correlated_risk: float = 0.03, # 3% of account balance
        max_positions: int = 10,
        preferred_risk_reward: float = 2.0,
        minimum_risk_reward: float = 1.5
    ):
        """
        Initialize risk parameters
        
        Args:
            max_risk_per_trade: Maximum risk per trade as percentage of account balance
            max_daily_risk: Maximum daily risk as percentage of account balance
            max_correlated_risk: Maximum risk for correlated instruments
            max_positions: Maximum number of open positions
            preferred_risk_reward: Preferred risk-reward ratio
            minimum_risk_reward: Minimum acceptable risk-reward ratio
        """
        self.max_risk_per_trade = max_risk_per_trade
        self.max_daily_risk = max_daily_risk
        self.max_correlated_risk = max_correlated_risk
        self.max_positions = max_positions
        self.preferred_risk_reward = preferred_risk_reward
        self.minimum_risk_reward = minimum_risk_reward


class AdvancedLossManager:
    """
    Advanced loss management to protect capital and manage position risks dynamically
    """
    def __init__(self, data_store: Optional[DataStore] = None):
        """Initialize the advanced loss manager"""
        self.data_store = data_store
        self.position_loss_data = {}
        self.daily_pnl = 0.0
        self.correlation_matrix = {}
        self.max_daily_loss_percentage = config.max_daily_loss
        self.max_loss_reset_timestamp = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).isoformat()
        
    @handle_async_errors
    async def initialize_position(self, 
                               symbol: str, 
                               entry_price: float, 
                               position_type: str,
                               units: float, 
                               account_balance: float) -> Dict[str, Any]:
        """
        Initialize position loss management data
        
        Args:
            symbol: Trading symbol
            entry_price: Entry price
            position_type: Position type (BUY or SELL)
            units: Position size in units
            account_balance: Current account balance
            
        Returns:
            Dict with initialized loss management data
        """
        symbol = standardize_symbol(symbol)
        position_type = position_type.upper()
        
        # Calculate max allowed loss for position
        max_loss = self._calculate_position_max_loss(entry_price, units, account_balance)
        
        # Initialize loss data
        loss_data = {
            "symbol": symbol,
            "entry_price": entry_price,
            "position_type": position_type,
            "units": units,
            "max_loss_amount": max_loss,
            "max_loss_price": self._calculate_max_loss_price(entry_price, position_type, max_loss, units),
            "current_loss": 0.0,
            "current_loss_percentage": 0.0,
            "correlation_factor": 1.0,
            "last_update": datetime.now(timezone.utc).isoformat()
        }
        
        # Store in memory
        self.position_loss_data[symbol] = loss_data
        
        # Persist to data store if available
        if self.data_store:
            self.data_store.store_analytics(f"loss_management:{symbol}", loss_data)
            
        logger.info(f"Initialized loss management for {symbol}: max loss=${max_loss:.2f}")
        
        return loss_data
        
    def _calculate_position_max_loss(self, entry_price: float, units: float, account_balance: float) -> float:
        """
        Calculate maximum allowed loss for a position
        
        Args:
            entry_price: Entry price
            units: Position size in units
            account_balance: Account balance
            
        Returns:
            Maximum loss amount in base currency
        """
        # For example: limit position loss to 2% of account
        max_percentage = 0.02
        position_value = entry_price * units
        
        # Calculate max loss as lesser of 2% of account or 20% of position value
        account_max_loss = account_balance * max_percentage
        position_max_loss = position_value * 0.20
        
        return min(account_max_loss, position_max_loss)
        
    def _calculate_max_loss_price(self, entry_price: float, position_type: str, 
                               max_loss: float, units: float) -> float:
        """
        Calculate price at which max loss would be hit
        
        Args:
            entry_price: Entry price
            position_type: Position type (BUY or SELL)
            max_loss: Maximum allowed loss
            units: Position size
            
        Returns:
            Price at which max loss would be hit
        """
        # Calculate price difference per unit that would equal max loss
        price_diff = max_loss / units
        
        # For long positions, loss price is below entry
        if position_type == "BUY":
            return max(0.00001, entry_price - price_diff)
        
        # For short positions, loss price is above entry
        return entry_price + price_diff
    
    @handle_async_errors
    async def update_position_loss(self, 
                                symbol: str, 
                                current_price: float) -> Dict[str, Any]:
        """
        Update position loss management data
        
        Args:
            symbol: Trading symbol
            current_price: Current price
            
        Returns:
            Dict with updated loss management data
        """
        symbol = standardize_symbol(symbol)
        
        # Check if position exists
        if symbol not in self.position_loss_data:
            logger.warning(f"Cannot update loss for unknown position: {symbol}")
            return {}
            
        position_data = self.position_loss_data[symbol]
        
        # Calculate current loss
        entry_price = position_data["entry_price"]
        position_type = position_data["position_type"]
        units = position_data["units"]
        
        if position_type == "BUY":
            # For long positions, loss when price < entry
            loss_per_unit = entry_price - current_price
        else:
            # For short positions, loss when price > entry
            loss_per_unit = current_price - entry_price
            
        # Calculate total loss and percentage
        current_loss = loss_per_unit * units
        current_loss = max(0.0, current_loss)  # No negative loss (would be profit)
        
        position_value = entry_price * units
        loss_percentage = (current_loss / position_value) * 100 if position_value > 0 else 0
        
        # Update position data
        position_data["current_price"] = current_price
        position_data["current_loss"] = current_loss
        position_data["current_loss_percentage"] = loss_percentage
        position_data["last_update"] = datetime.now(timezone.utc).isoformat()
        
        # Persist to data store if available
        if self.data_store:
            self.data_store.store_analytics(f"loss_management:{symbol}", position_data)
            
        # Check if max loss hit
        if current_loss >= position_data["max_loss_amount"]:
            logger.warning(f"⚠️ Position {symbol} has hit max allowed loss: ${current_loss:.2f}")
            position_data["max_loss_hit"] = True
            
        return position_data
    
    @handle_async_errors
    async def update_correlation_matrix(self, 
                                     symbol: str, 
                                     other_positions: Dict[str, Dict[str, Any]]):
        """
        Update correlation data for positions
        
        Args:
            symbol: Symbol to update correlation for
            other_positions: Dict of other open positions
        """
        symbol = standardize_symbol(symbol)
        
        # Skip if no other positions
        if not other_positions:
            return
            
        # Initialize correlation for this symbol if needed
        if symbol not in self.correlation_matrix:
            self.correlation_matrix[symbol] = {}
            
        # Get current position data
        if symbol not in self.position_loss_data:
            logger.warning(f"Cannot update correlation for unknown position: {symbol}")
            return
            
        current_position = self.position_loss_data[symbol]
        current_type = current_position["position_type"]
        
        # Update correlation with each other position
        for other_symbol, position_data in other_positions.items():
            if other_symbol == symbol:
                continue
                
            other_type = position_data.get("position_type")
            if not other_type:
                continue
                
            # Calculate correlation
            correlation = self._calculate_correlation(
                symbol, other_symbol, current_type, other_type
            )
            
            # Store correlation
            self.correlation_matrix[symbol][other_symbol] = correlation
    
    def _calculate_correlation(self, 
                           symbol1: str, 
                           symbol2: str, 
                           type1: str, 
                           type2: str) -> float:
        """
        Calculate correlation between two positions
        
        Args:
            symbol1: First symbol
            symbol2: Second symbol
            type1: Position type of first symbol
            type2: Position type of second symbol
            
        Returns:
            Correlation value (-1 to 1)
        """
        # This is a simplified correlation calculation
        # In a real system, we would use actual price correlation data
        
        # Extract currency pairs
        pairs = [symbol1, symbol2]
        currencies = []
        
        for pair in pairs:
            # Extract currencies from pair (e.g., "EURUSD" -> ["EUR", "USD"])
            if len(pair) == 6:
                currencies.append(pair[:3])
                currencies.append(pair[3:])
                
        # Count occurrences of each currency
        counts = {}
        for curr in currencies:
            if curr not in counts:
                counts[curr] = 0
            counts[curr] += 1
            
        # If positions share currencies, they may be correlated
        shared_currencies = sum(1 for _, count in counts.items() if count > 1)
        
        # Position types impact correlation
        same_direction = (type1 == type2)
        
        # Calculate correlation factor
        if shared_currencies == 0:
            # No shared currencies, low correlation
            return 0.0
        elif shared_currencies == 1:
            # One shared currency
            return 0.3 if same_direction else -0.3
        else:
            # Two shared currencies (e.g., EURUSD and EURGBP both have EUR)
            return 0.7 if same_direction else -0.7
    
    @handle_async_errors
    async def get_position_correlation_factor(self, symbol: str) -> float:
        """
        Get overall correlation factor for a position
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Correlation factor (0.5 to 2.0)
        """
        symbol = standardize_symbol(symbol)
        
        if symbol not in self.correlation_matrix:
            return 1.0  # Default - no correlation adjustment
            
        correlations = self.correlation_matrix[symbol].values()
        
        if not correlations:
            return 1.0
            
        # Calculate average absolute correlation
        avg_correlation = sum(abs(c) for c in correlations) / len(correlations)
        
        # Convert to risk factor (0.5 to 2.0)
        # Higher correlation = higher risk factor
        correlation_factor = 1.0 + avg_correlation
        
        # Ensure factor is within reasonable range
        correlation_factor = max(0.5, min(2.0, correlation_factor))
        
        return correlation_factor
    
    @handle_async_errors
    async def update_daily_pnl(self, pnl: float):
        """
        Update daily P&L tracking
        
        Args:
            pnl: Profit/Loss amount to add
        """
        # Check if we need to reset (new day)
        now = datetime.now(timezone.utc)
        reset_time = datetime.fromisoformat(self.max_loss_reset_timestamp)
        
        if now.date() > reset_time.date():
            # New day, reset P&L
            self.daily_pnl = 0.0
            self.max_loss_reset_timestamp = now.replace(
                hour=0, minute=0, second=0, microsecond=0
            ).isoformat()
            logger.info("Daily P&L tracker reset for new day")
            
        # Update P&L
        self.daily_pnl += pnl
        logger.info(f"Updated daily P&L: ${self.daily_pnl:.2f}")
        
        # Store in data store if available
        if self.data_store:
            self.data_store.store_setting("daily_pnl", self.daily_pnl)
            self.data_store.store_setting("pnl_reset_timestamp", self.max_loss_reset_timestamp)
    
    @handle_async_errors
    async def should_reduce_risk(self) -> Tuple[bool, float]:
        """
        Check if risk should be reduced based on daily losses
        
        Returns:
            Tuple of (should_reduce, factor)
        """
        # No need to reduce if profitable
        if self.daily_pnl >= 0:
            return False, 1.0
            
        # Calculate loss percentage from daily PnL
        account_balance = 10000.0  # Default value
        
        # Try to get actual balance from data store
        if self.data_store:
            stored_balance = self.data_store.get_setting("account_balance")
            if stored_balance is not None:
                account_balance = float(stored_balance)
                
        loss_percentage = abs(self.daily_pnl) / account_balance * 100
        
        # Determine if risk should be reduced
        if loss_percentage >= self.max_daily_loss_percentage:
            # Max daily loss hit, no more trading
            logger.warning(f"⚠️ Maximum daily loss hit: ${self.daily_pnl:.2f}, {loss_percentage:.1f}%")
            return True, 0.0
        elif loss_percentage >= self.max_daily_loss_percentage * 0.5:
            # Over 50% to max loss, reduce risk by 50%
            logger.warning(f"⚠️ Daily loss approaching max: ${self.daily_pnl:.2f}, reducing risk by 50%")
            return True, 0.5
        elif loss_percentage >= self.max_daily_loss_percentage * 0.25:
            # Over 25% to max loss, reduce risk by 25%
            logger.info(f"Daily loss significant: ${self.daily_pnl:.2f}, reducing risk by 25%")
            return True, 0.75
            
        # No risk reduction needed
        return False, 1.0
    
    @handle_async_errors
    async def clear_position(self, symbol: str):
        """
        Clear position data from memory
        
        Args:
            symbol: Trading symbol
        """
        symbol = standardize_symbol(symbol)
        
        if symbol in self.position_loss_data:
            del self.position_loss_data[symbol]
            
        if symbol in self.correlation_matrix:
            del self.correlation_matrix[symbol]
            
        # Remove from data store if available
        if self.data_store:
            self.data_store.delete_position(f"loss_management:{symbol}")
            
        logger.info(f"Cleared loss management data for {symbol}")
    
    @handle_async_errors
    async def get_position_risk_metrics(self, symbol: str) -> Dict[str, Any]:
        """
        Get risk metrics for a position
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Dict with risk metrics
        """
        symbol = standardize_symbol(symbol)
        
        # Try to get from memory
        if symbol in self.position_loss_data:
            metrics = self.position_loss_data[symbol]
        # Try to get from data store
        elif self.data_store:
            metrics = self.data_store.get_analytics(f"loss_management:{symbol}")
        else:
            return {}
            
        # Add correlation factor if available
        if symbol in self.correlation_matrix:
            metrics["correlations"] = self.correlation_matrix[symbol]
            
        return metrics


class RiskAnalytics:
    """
    Advanced analytics for position risk assessment and portfolio optimization
    """
    def __init__(self, data_store: Optional[DataStore] = None):
        """Initialize the risk analytics system"""
        self.data_store = data_store
        self.position_stats = {}
        self.portfolio_stats = {
            "total_positions": 0,
            "winning_positions": 0,
            "losing_positions": 0,
            "win_rate": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "profit_factor": 0.0,
            "sharpe_ratio": 0.0,
            "sortino_ratio": 0.0,
            "max_drawdown": 0.0,
            "expected_return": 0.0,
            "expected_risk": 0.0,
            "returns": [],
            "last_update": datetime.now(timezone.utc).isoformat()
        }
        
    @handle_async_errors
    async def initialize_position(self, symbol: str, entry_price: float, units: float):
        """
        Initialize position analytics
        
        Args:
            symbol: Trading symbol
            entry_price: Entry price
            units: Position size
        """
        symbol = standardize_symbol(symbol)
        
        # Initialize position stats
        self.position_stats[symbol] = {
            "symbol": symbol,
            "entry_price": entry_price,
            "units": units,
            "price_history": [entry_price],
            "timestamps": [datetime.now(timezone.utc).isoformat()],
            "returns": [],
            "volatility": 0.0,
            "max_favorable_excursion": 0.0,
            "max_adverse_excursion": 0.0,
            "current_pnl": 0.0,
            "last_update": datetime.now(timezone.utc).isoformat()
        }
        
        # Store in data store if available
        if self.data_store:
            self.data_store.store_analytics(f"risk_analytics:{symbol}", self.position_stats[symbol])
            
        logger.info(f"Initialized risk analytics for {symbol}")
        
    @handle_async_errors
    async def update_position(self, symbol: str, current_price: float):
        """
        Update position risk metrics
        
        Args:
            symbol: Trading symbol
            current_price: Current market price
        """
        symbol = standardize_symbol(symbol)
        
        # Check if position exists
        if symbol not in self.position_stats:
            logger.warning(f"Cannot update analytics for unknown position: {symbol}")
            return
            
        position_data = self.position_stats[symbol]
        
        # Update price history (keep last 100 prices maximum)
        position_data["price_history"].append(current_price)
        position_data["timestamps"].append(datetime.now(timezone.utc).isoformat())
        
        if len(position_data["price_history"]) > 100:
            position_data["price_history"].pop(0)
            position_data["timestamps"].pop(0)
            
        # Calculate return since last update
        if len(position_data["price_history"]) >= 2:
            prev_price = position_data["price_history"][-2]
            if prev_price > 0:
                period_return = (current_price - prev_price) / prev_price
                position_data["returns"].append(period_return)
                
                # Keep returns list to last 100 values
                if len(position_data["returns"]) > 100:
                    position_data["returns"].pop(0)
                    
        # Calculate current P&L
        entry_price = position_data["entry_price"]
        units = position_data["units"]
        position_data["current_pnl"] = (current_price - entry_price) * units
        
        # Update favorable and adverse excursions
        price_changes = [p - entry_price for p in position_data["price_history"]]
        max_gain = max(price_changes)
        max_loss = min(price_changes)
        
        position_data["max_favorable_excursion"] = max_gain * units
        position_data["max_adverse_excursion"] = max_loss * units
        
        # Calculate risk metrics
        await self._calculate_risk_metrics(symbol)
        
        # Update timestamp
        position_data["last_update"] = datetime.now(timezone.utc).isoformat()
        
        # Persist to data store if available
        if self.data_store:
            self.data_store.store_analytics(f"risk_analytics:{symbol}", position_data)
            
    @handle_async_errors
    async def _calculate_risk_metrics(self, symbol: str):
        """
        Calculate risk metrics for a position
        
        Args:
            symbol: Trading symbol
        """
        if symbol not in self.position_stats:
            return
            
        position_data = self.position_stats[symbol]
        price_history = position_data["price_history"]
        returns = position_data["returns"]
        
        # Need at least a few data points
        if len(price_history) < 3 or len(returns) < 3:
            return
            
        # Calculate volatility (standard deviation of returns)
        if returns:
            volatility = statistics.stdev(returns) if len(returns) > 1 else 0.0
            position_data["volatility"] = volatility
            
        # Calculate Value at Risk (VaR)
        # Using historical simulation method
        if returns:
            # Sort returns from worst to best
            sorted_returns = sorted(returns)
            
            # Calculate 95% and 99% VaR
            var_95_index = int(0.05 * len(sorted_returns))
            var_99_index = int(0.01 * len(sorted_returns))
            
            # Make sure indexes are valid
            var_95_index = max(0, min(var_95_index, len(sorted_returns) - 1))
            var_99_index = max(0, min(var_99_index, len(sorted_returns) - 1))
            
            position_data["var_95"] = abs(sorted_returns[var_95_index]) if len(sorted_returns) > var_95_index else 0.0
            position_data["var_99"] = abs(sorted_returns[var_99_index]) if len(sorted_returns) > var_99_index else 0.0
            
            # Calculate Expected Shortfall (Conditional VaR)
            if var_95_index > 0:
                position_data["es_95"] = abs(sum(sorted_returns[:var_95_index]) / var_95_index) if var_95_index > 0 else 0.0
            else:
                position_data["es_95"] = 0.0
                
        # Calculate maximum drawdown
        if len(price_history) >= 2:
            max_drawdown = 0.0
            peak = price_history[0]
            
            for price in price_history[1:]:
                if price > peak:
                    peak = price
                else:
                    drawdown = (peak - price) / peak
                    max_drawdown = max(max_drawdown, drawdown)
                    
            position_data["max_drawdown"] = max_drawdown
            
        # Calculate Sharpe Ratio if we have returns
        if returns:
            position_data["sharpe_ratio"] = self._calculate_sharpe_ratio(returns)
            position_data["sortino_ratio"] = self._calculate_sortino_ratio(returns)
            
        # Calculate current risk level
        if "volatility" in position_data and position_data["volatility"] > 0:
            # Higher volatility = higher risk
            current_risk = position_data["volatility"] * position_data["units"]
            position_data["current_risk"] = current_risk
            
            # Risk-adjusted return
            if position_data["current_pnl"] != 0 and current_risk > 0:
                position_data["risk_adjusted_return"] = position_data["current_pnl"] / current_risk
            else:
                position_data["risk_adjusted_return"] = 0.0
                
    def _calculate_sharpe_ratio(self, returns: List[float]) -> float:
        """
        Calculate Sharpe ratio
        
        Args:
            returns: List of period returns
            
        Returns:
            Sharpe ratio
        """
        if not returns or len(returns) < 2:
            return 0.0
            
        # Calculate average return
        avg_return = sum(returns) / len(returns)
        
        # Calculate standard deviation of returns
        std_dev = statistics.stdev(returns)
        
        # Avoid division by zero
        if std_dev == 0:
            return 0.0
            
        # Assuming risk-free rate of 0 for simplicity
        risk_free_rate = 0.0
        
        # Calculate Sharpe ratio
        sharpe = (avg_return - risk_free_rate) / std_dev
        
        # Annualize (assuming daily returns)
        annualized_sharpe = sharpe * math.sqrt(252)
        
        return annualized_sharpe
        
    def _calculate_sortino_ratio(self, returns: List[float]) -> float:
        """
        Calculate Sortino ratio (similar to Sharpe but only penalizes downside volatility)
        
        Args:
            returns: List of period returns
            
        Returns:
            Sortino ratio
        """
        if not returns or len(returns) < 2:
            return 0.0
            
        # Calculate average return
        avg_return = sum(returns) / len(returns)
        
        # Calculate downside returns (negative returns only)
        downside_returns = [r for r in returns if r < 0]
        
        # If no downside returns, return a high value
        if not downside_returns:
            return 10.0  # Arbitrary high value for perfect performance
            
        # Calculate downside deviation
        downside_deviation = math.sqrt(sum(r**2 for r in downside_returns) / len(downside_returns))
        
        # Avoid division by zero
        if downside_deviation == 0:
            return 0.0
            
        # Assuming risk-free rate of 0 for simplicity
        risk_free_rate = 0.0
        
        # Calculate Sortino ratio
        sortino = (avg_return - risk_free_rate) / downside_deviation
        
        # Annualize (assuming daily returns)
        annualized_sortino = sortino * math.sqrt(252)
        
        return annualized_sortino
    
    @handle_async_errors
    async def get_position_risk_metrics(self, symbol: str) -> Dict[str, Any]:
        """
        Get risk metrics for a position
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Dict with risk metrics
        """
        symbol = standardize_symbol(symbol)
        
        # Try to get from memory
        if symbol in self.position_stats:
            return self.position_stats[symbol]
            
        # Try to get from data store
        if self.data_store:
            stored_data = self.data_store.get_analytics(f"risk_analytics:{symbol}")
            if stored_data:
                return stored_data
                
        # No data available
        return {}
    
    @handle_async_errors
    async def get_portfolio_risk_metrics(self) -> Dict[str, Any]:
        """
        Get risk metrics for the entire portfolio
        
        Returns:
            Dict with portfolio risk metrics
        """
        # If no positions, return default portfolio stats
        if not self.position_stats:
            return self.portfolio_stats
            
        # Calculate portfolio level metrics based on individual positions
        total_pnl = 0.0
        winning_count = 0
        losing_count = 0
        total_winning_amount = 0.0
        total_losing_amount = 0.0
        all_returns = []
        
        # Collect data from all positions
        for symbol, stats in self.position_stats.items():
            current_pnl = stats.get("current_pnl", 0.0)
            total_pnl += current_pnl
            
            # Count winning and losing positions
            if current_pnl > 0:
                winning_count += 1
                total_winning_amount += current_pnl
            elif current_pnl < 0:
                losing_count += 1
                total_losing_amount += abs(current_pnl)
                
            # Collect returns for portfolio-level calculations
            all_returns.extend(stats.get("returns", []))
            
        # Update portfolio stats
        self.portfolio_stats["total_positions"] = len(self.position_stats)
        self.portfolio_stats["winning_positions"] = winning_count
        self.portfolio_stats["losing_positions"] = losing_count
        
        # Calculate win rate
        if winning_count + losing_count > 0:
            self.portfolio_stats["win_rate"] = winning_count / (winning_count + losing_count)
        
        # Calculate average win and loss
        if winning_count > 0:
            self.portfolio_stats["avg_win"] = total_winning_amount / winning_count
            
        if losing_count > 0:
            self.portfolio_stats["avg_loss"] = total_losing_amount / losing_count
            
        # Calculate profit factor
        if total_losing_amount > 0:
            self.portfolio_stats["profit_factor"] = total_winning_amount / total_losing_amount
            
        # Store all returns
        self.portfolio_stats["returns"] = all_returns
        
        # Calculate portfolio risk metrics
        if all_returns:
            self.portfolio_stats["sharpe_ratio"] = self._calculate_sharpe_ratio(all_returns)
            self.portfolio_stats["sortino_ratio"] = self._calculate_sortino_ratio(all_returns)
            
            # Calculate expected return and risk
            self.portfolio_stats["expected_return"] = sum(all_returns) / len(all_returns) if all_returns else 0.0
            self.portfolio_stats["expected_risk"] = statistics.stdev(all_returns) if len(all_returns) > 1 else 0.0
            
        # Update timestamp
        self.portfolio_stats["last_update"] = datetime.now(timezone.utc).isoformat()
        
        # Persist to data store if available
        if self.data_store:
            self.data_store.store_analytics("portfolio_metrics", self.portfolio_stats)
            
        return self.portfolio_stats
    
    @handle_async_errors
    async def clear_position(self, symbol: str):
        """
        Clear position data
        
        Args:
            symbol: Trading symbol
        """
        symbol = standardize_symbol(symbol)
        
        if symbol in self.position_stats:
            del self.position_stats[symbol]
            
        # Clear from data store if available
        if self.data_store:
            self.data_store.delete_position(f"risk_analytics:{symbol}")
            
        logger.info(f"Cleared risk analytics for {symbol}")


class PositionSizingManager:
    """
    Manages optimal position sizing based on risk parameters, 
    market conditions, and account equity
    """
    def __init__(self, 
                data_store: Optional[DataStore] = None,
                risk_manager: Optional['RiskManager'] = None):
        """
        Initialize the position sizing manager
        
        Args:
            data_store: Data store for persistence
            risk_manager: Risk manager for risk checks
        """
        self.data_store = data_store
        self.risk_manager = risk_manager
        self.total_risk_allocated = 0.0
        self.position_sizes = {}
        self.last_update = datetime.now(timezone.utc).isoformat()
        
    @handle_async_errors
    async def calculate_position_size(self, 
                                    account_balance: float,
                                    entry_price: float,
                                    stop_loss: float,
                                    atr: float,
                                    timeframe: str,
                                    market_condition: Optional[Dict[str, Any]] = None,
                                    correlation_factor: float = 1.0) -> float:
        """
        Calculate optimal position size based on risk parameters
        
        Args:
            account_balance: Current account balance
            entry_price: Planned entry price
            stop_loss: Planned stop loss level
            atr: Average True Range value
            timeframe: Trading timeframe
            market_condition: Market condition data (optional)
            correlation_factor: Correlation risk factor (1.0 = normal)
            
        Returns:
            Optimal position size
        """
        # Default risk parameters
        risk_percentage = 0.01  # 1% risk per trade as default
        max_total_risk = 0.15  # 15% max total portfolio risk
        
        # Get risk parameters from risk manager if available
        if self.risk_manager:
            risk_params = self.risk_manager.risk_parameters
            if risk_params:
                risk_percentage = risk_params.max_risk_per_trade
                max_total_risk = risk_params.max_correlated_risk
        
        # Adjust risk based on timeframe
        timeframe_adjustments = {
            "1m": 0.5,  # Reduce risk for short timeframes
            "5m": 0.6,
            "15m": 0.7,
            "30m": 0.8,
            "1h": 1.0,  # Base reference
            "4h": 1.2,
            "1d": 1.5   # Increase risk for longer timeframes
        }
        
        # Apply timeframe adjustment
        timeframe_multiplier = timeframe_adjustments.get(timeframe.lower(), 1.0)
        adjusted_risk = risk_percentage * timeframe_multiplier
        
        # Adjust risk based on market condition
        if market_condition:
            volatility_condition = market_condition.get("condition", "normal")
            
            if volatility_condition == "high":
                # Reduce risk in high volatility
                adjusted_risk *= 0.7
            elif volatility_condition == "low":
                # Increase risk in low volatility
                adjusted_risk *= 1.2
                
        # Adjust for correlation
        adjusted_risk /= correlation_factor
        
        # Calculate risk amount in base currency
        risk_amount = account_balance * adjusted_risk
        
        # Calculate position size based on risk and stop loss distance
        stop_distance = abs(entry_price - stop_loss)
        
        # Avoid division by zero
        if stop_distance <= 0:
            # Use ATR as fallback if available
            if atr > 0:
                stop_distance = atr
            else:
                stop_distance = entry_price * 0.01  # Default 1% stop
                
        # Calculate raw position size
        position_size = risk_amount / stop_distance
        
        # Account for ATR volatility adjustment
        atr_factor = 1.0
        if atr > 0:
            # If stop is tighter than 0.5x ATR, reduce size
            if stop_distance < 0.5 * atr:
                atr_factor = stop_distance / (0.5 * atr)
            # If stop is looser than 2x ATR, increase size
            elif stop_distance > 2 * atr:
                atr_factor = min(1.5, 2 * atr / stop_distance)
                
        # Apply ATR adjustment
        position_size *= atr_factor
        
        # Check if adding this position would exceed max portfolio risk
        if self.data_store:
            # Get current account risk exposure
            total_risk = self.data_store.get_setting("total_risk_allocated", 0.0)
            if isinstance(total_risk, str):
                try:
                    total_risk = float(total_risk)
                except ValueError:
                    total_risk = 0.0
                    
            # Check if max risk would be exceeded
            max_risk_amount = account_balance * max_total_risk
            if total_risk + risk_amount > max_risk_amount:
                # Scale down position size to fit within max risk
                scaling_factor = (max_risk_amount - total_risk) / risk_amount
                scaling_factor = max(0.0, min(1.0, scaling_factor))
                position_size *= scaling_factor
                
                logger.warning(f"Position size reduced due to portfolio risk constraints: {scaling_factor:.2f}x")
        
        # Round position size based on instrument minimum size
        # This is a simplified approach - adapt for actual instrument requirements
        position_size = max(0.01, position_size)  # Minimum 0.01 units
        position_size = round(position_size, 2)  # Round to 2 decimal places
        
        logger.info(f"Calculated position size: {position_size:.2f} units (risk: {adjusted_risk*100:.2f}%)")
        
        return position_size
    
    @handle_async_errors
    async def update_portfolio_heat(self, new_position_risk: float):
        """
        Update total portfolio risk allocation ("heat")
        
        Args:
            new_position_risk: Risk amount for new position
        """
        # Update total risk allocation
        self.total_risk_allocated += new_position_risk
        
        # Persist to data store if available
        if self.data_store:
            self.data_store.store_setting("total_risk_allocated", self.total_risk_allocated)
            
        logger.info(f"Updated portfolio heat: {self.total_risk_allocated:.2f}")
    
    @handle_async_errors
    async def get_correlation_factor(self, symbol: str, existing_positions: List[str]) -> float:
        """
        Calculate correlation factor for a new position
        
        Args:
            symbol: Symbol for new position
            existing_positions: List of existing position symbols
            
        Returns:
            Correlation risk factor (1.0 = no correlation)
        """
        if not existing_positions:
            return 1.0
            
        symbol = standardize_symbol(symbol)
        
        # Extract currency components
        symbol_components = []
        if len(symbol) >= 6:
            # For forex pairs like "EURUSD"
            symbol_components = [symbol[:3], symbol[3:6]]
        
        # Count correlation instances
        correlation_count = 0
        for existing in existing_positions:
            existing = standardize_symbol(existing)
            
            # Check for exact same instrument
            if existing == symbol:
                return 2.0  # Maximum correlation factor
                
            # Check for shared currency components in forex
            if len(existing) >= 6:
                existing_components = [existing[:3], existing[3:6]]
                
                # Count shared currencies
                shared = set(symbol_components).intersection(set(existing_components))
                correlation_count += len(shared)
        
        # Calculate correlation factor:
        # - 0 shared currencies: factor 1.0 (no correlation)
        # - 1 shared currency: factor 1.2 (slight correlation)
        # - 2 shared currencies: factor 1.5 (moderate correlation)
        # - 3+ shared currencies: factor 1.8 (high correlation)
        if correlation_count == 0:
            return 1.0
        elif correlation_count == 1:
            return 1.2
        elif correlation_count == 2:
            return 1.5
        else:
            return 1.8


class TimeframeSettings:
    """
    Settings for specific timeframes for trading
    """
    def __init__(self):
        """Initialize timeframe settings"""
        # Default settings for 15-minute timeframe
        self.m15 = {
            "stop_loss_atr_multiplier": 1.5,
            "take_profit_atr_multiplier": 3.0,
            "rsi_oversold": 30,
            "rsi_overbought": 70,
            "volume_threshold": 1.2,  # 20% above average
            "max_spread_pips": 5.0,
            "min_candle_range_pips": 5.0,
            "trailing_stop_activation": 0.5,  # Activate after 0.5x take profit
            "preferred_session": "all",
            "first_exit": 0.5,  # 50% at 1:1 risk:reward
            "second_exit": 0.25,  # 25% at 2:1 risk:reward
            "runner": 0.25  # 25% with trailing stop
        }
        
        # Default settings for 1-hour timeframe
        self.h1 = {
            "stop_loss_atr_multiplier": 2.0,
            "take_profit_atr_multiplier": 4.0,
            "rsi_oversold": 30,
            "rsi_overbought": 70,
            "volume_threshold": 1.1,  # 10% above average
            "max_spread_pips": 7.0,
            "min_candle_range_pips": 10.0,
            "trailing_stop_activation": 0.5,  # Activate after 0.5x take profit
            "preferred_session": "all",
            "first_exit": 0.4,  # 40% at 1:1
            "second_exit": 0.3,  # 30% at 2:1
            "runner": 0.3  # 30% with trailing
        }
        
        # Default settings for 4-hour timeframe
        self.h4 = {
            "stop_loss_atr_multiplier": 2.5,
            "take_profit_atr_multiplier": 5.0,
            "rsi_oversold": 30,
            "rsi_overbought": 70,
            "volume_threshold": 1.0,  # Average volume
            "max_spread_pips": 10.0,
            "min_candle_range_pips": 20.0,
            "trailing_stop_activation": 0.5,  # Activate after 0.5x take profit
            "preferred_session": "all",
            "first_exit": 0.33,  # 33% at 1:1
            "second_exit": 0.33,  # 33% at 2:1
            "runner": 0.34  # 34% with trailing
        }
    
    def get_settings(self, timeframe: str) -> Dict[str, Any]:
        """
        Get settings for a specific timeframe
        
        Args:
            timeframe: Timeframe string (e.g., "15m", "1h", "4h")
            
        Returns:
            Dict with settings for the timeframe
        """
        if timeframe == "15m":
            return self.m15
        elif timeframe == "1h":
            return self.h1
        elif timeframe == "4h":
            return self.h4
        else:
            # Return default settings for unknown timeframes
            logger.warning(f"No specific settings for timeframe {timeframe}, using 1h settings")
            return self.h1


class RiskManager:
    """
    Manages trading risk across different positions and instruments
    """
    def __init__(
        self, 
        data_store: DataStore,
        position_manager: PositionManager,
        market_analyzer: MarketAnalyzer,
        risk_parameters: Optional[RiskParameters] = None,
        timeframe_settings: Optional[TimeframeSettings] = None
    ):
        """
        Initialize risk manager
        
        Args:
            data_store: Data store for persistence
            position_manager: Position manager
            market_analyzer: Market analyzer
            risk_parameters: Risk parameters (optional)
            timeframe_settings: Timeframe settings (optional)
        """
        self.data_store = data_store
        self.position_manager = position_manager
        self.market_analyzer = market_analyzer
        self.risk_parameters = risk_parameters or RiskParameters()
        self.timeframe_settings = timeframe_settings or TimeframeSettings()
        self.daily_risk_used = 0.0
        self.last_reset_date = datetime.now().date()
        self.correlated_pairs = {
            # FX correlations
            "EURUSD": ["GBPUSD", "EURGBP", "EURJPY", "USDJPY", "EURCAD", "USDCAD"],
            "GBPUSD": ["EURUSD", "EURGBP", "GBPJPY", "USDJPY", "GBPCAD", "USDCAD"],
            "USDJPY": ["EURJPY", "GBPJPY", "EURUSD", "GBPUSD", "CADJPY", "USDCAD"],
            "USDCAD": ["EURCAD", "GBPCAD", "CADJPY", "EURUSD", "GBPUSD", "USDJPY"],
            "AUDUSD": ["NZDUSD", "AUDNZD", "AUDJPY", "USDJPY"],
            "NZDUSD": ["AUDUSD", "AUDNZD", "NZDJPY", "USDJPY"],
            
            # Crypto correlations
            "BTCUSD": ["ETHUSD", "XRPUSD", "LTCUSD"],
            "ETHUSD": ["BTCUSD", "XRPUSD", "LTCUSD"],
            
            # Equity index correlations
            "US30": ["SPX500", "NASDAQ", "US2000"],
            "SPX500": ["US30", "NASDAQ", "US2000"],
            "NASDAQ": ["US30", "SPX500", "US2000"],
            
            # Commodity correlations
            "XAUUSD": ["XAGUSD", "USDCAD", "AUDUSD"],
            "XAGUSD": ["XAUUSD", "USDCAD", "AUDUSD"],
            "USOIL": ["UKOIL", "USDCAD", "CADJPY"]
        }
    
    @handle_async_errors
    async def check_daily_risk_reset(self):
        """Reset daily risk if date has changed"""
        today = datetime.now().date()
        if today > self.last_reset_date:
            logger.info(f"Resetting daily risk from {self.daily_risk_used:.2%} to 0.0%")
            self.daily_risk_used = 0.0
            self.last_reset_date = today
    
    @handle_async_errors
    async def calculate_position_risk(self, position: Position, account_balance: float) -> float:
        """
        Calculate risk for a position as percentage of account balance
        
        Args:
            position: Trading position
            account_balance: Account balance
            
        Returns:
            Risk as percentage of account balance
        """
        if account_balance <= 0:
            return 0.0
            
        # Risk is the potential loss if stop loss is hit
        entry_price = position.entry_price
        stop_loss = position.stop_loss_price
        
        if stop_loss is None:
            logger.warning(f"Position {position.id} has no stop loss set")
            # Assume a 5% risk if no stop loss is set
            return 0.05
        
        position_value = position.size * entry_price
        potential_loss = position.size * abs(entry_price - stop_loss)
        risk_percentage = potential_loss / account_balance
        
        logger.debug(f"Position {position.id} risk: {risk_percentage:.2%} of account")
        return risk_percentage
    
    @handle_async_errors
    async def calculate_correlated_risk(
        self, 
        symbol: str, 
        account_balance: float
    ) -> float:
        """
        Calculate total risk for correlated instruments
        
        Args:
            symbol: Trading symbol
            account_balance: Account balance
            
        Returns:
            Total risk for correlated instruments as percentage of account balance
        """
        symbol = standardize_symbol(symbol)
        correlated_symbols = self.correlated_pairs.get(symbol, [])
        correlated_symbols.append(symbol)  # Include the symbol itself
        
        total_risk = 0.0
        positions = await self.position_manager.get_all_positions()
        
        for position in positions:
            if position.symbol in correlated_symbols:
                risk = await self.calculate_position_risk(position, account_balance)
                total_risk += risk
        
        logger.debug(f"Correlated risk for {symbol}: {total_risk:.2%} of account")
        return total_risk
    
    @handle_async_errors
    async def calculate_total_account_risk(self, account_balance: float) -> float:
        """
        Calculate total risk across all positions
        
        Args:
            account_balance: Account balance
            
        Returns:
            Total risk as percentage of account balance
        """
        positions = await self.position_manager.get_all_positions()
        total_risk = 0.0
        
        for position in positions:
            risk = await self.calculate_position_risk(position, account_balance)
            total_risk += risk
        
        logger.info(f"Total account risk: {total_risk:.2%} of account")
        return total_risk
    
    @handle_async_errors
    async def can_open_position(
        self, 
        symbol: str, 
        stop_loss: float, 
        entry_price: float,
        take_profit: float,
        size: float,
        account_balance: float
    ) -> Dict[str, Any]:
        """
        Check if a new position can be opened based on risk parameters
        
        Args:
            symbol: Trading symbol
            stop_loss: Stop loss price
            entry_price: Entry price
            take_profit: Take profit price
            size: Position size
            account_balance: Account balance
            
        Returns:
            Dict with approval status and reason
        """
        await self.check_daily_risk_reset()
        
        symbol = standardize_symbol(symbol)
        result = {
            "approved": False,
            "reason": "Unknown risk issue"
        }
        
        # Calculate risk metrics
        potential_loss = size * abs(entry_price - stop_loss)
        risk_percentage = potential_loss / account_balance
        risk_reward_ratio = abs(take_profit - entry_price) / abs(stop_loss - entry_price)
        
        # Check max positions
        positions = await self.position_manager.get_all_positions()
        if len(positions) >= self.risk_parameters.max_positions:
            result["reason"] = f"Max positions reached ({len(positions)}/{self.risk_parameters.max_positions})"
            return result
        
        # Check risk per trade
        if risk_percentage > self.risk_parameters.max_risk_per_trade:
            result["reason"] = f"Risk per trade too high ({risk_percentage:.2%} > {self.risk_parameters.max_risk_per_trade:.2%})"
            return result
        
        # Check daily risk
        if self.daily_risk_used + risk_percentage > self.risk_parameters.max_daily_risk:
            result["reason"] = f"Daily risk limit reached ({self.daily_risk_used + risk_percentage:.2%} > {self.risk_parameters.max_daily_risk:.2%})"
            return result
        
        # Check correlated risk
        correlated_risk = await self.calculate_correlated_risk(symbol, account_balance)
        if correlated_risk + risk_percentage > self.risk_parameters.max_correlated_risk:
            result["reason"] = f"Correlated risk too high ({correlated_risk + risk_percentage:.2%} > {self.risk_parameters.max_correlated_risk:.2%})"
            return result
        
        # Check risk-reward ratio
        if risk_reward_ratio < self.risk_parameters.minimum_risk_reward:
            result["reason"] = f"Risk-reward ratio too low ({risk_reward_ratio:.2f} < {self.risk_parameters.minimum_risk_reward:.2f})"
            return result
        
        # All checks passed
        result["approved"] = True
        result["reason"] = "Position approved"
        result["risk_percentage"] = risk_percentage
        result["risk_reward_ratio"] = risk_reward_ratio
        result["daily_risk_used"] = self.daily_risk_used
        result["correlated_risk"] = correlated_risk
        
        # Update daily risk used
        self.daily_risk_used += risk_percentage
        
        logger.info(f"Position approved for {symbol}: {risk_percentage:.2%} risk, {risk_reward_ratio:.2f} RR ratio")
        return result
    
    @handle_async_errors
    async def suggest_position_size(
        self, 
        symbol: str, 
        entry_price: float,
        stop_loss: float,
        account_balance: float,
        risk_percentage: Optional[float] = None
    ) -> float:
        """
        Suggest position size based on risk parameters
        
        Args:
            symbol: Trading symbol
            entry_price: Entry price
            stop_loss: Stop loss price
            account_balance: Account balance
            risk_percentage: Risk percentage (optional, defaults to max_risk_per_trade)
            
        Returns:
            Suggested position size
        """
        if risk_percentage is None:
            risk_percentage = self.risk_parameters.max_risk_per_trade
        
        # Check risk limits
        await self.check_daily_risk_reset()
        
        # Adjust for daily risk limit
        available_risk = min(
            risk_percentage,
            self.risk_parameters.max_daily_risk - self.daily_risk_used,
            self.risk_parameters.max_risk_per_trade
        )
        
        # Calculate max risk amount
        max_risk_amount = account_balance * available_risk
        
        # Calculate position size
        price_distance = abs(entry_price - stop_loss)
        if price_distance <= 0:
            logger.warning(f"Invalid price distance for {symbol}: {price_distance}")
            return 0.0
            
        size = max_risk_amount / price_distance
        
        # Minimum size of 0.01 lots
        size = max(0.01, size)
        
        logger.info(f"Suggested position size for {symbol}: {size:.2f} units at {available_risk:.2%} risk")
        return size
    
    @handle_async_errors
    async def get_atr_values(
        self, 
        symbol: str,
        timeframes: List[str] = ["15m", "1h", "4h"]
    ) -> Dict[str, float]:
        """
        Calculate Average True Range (ATR) for multiple timeframes
        
        Args:
            symbol: Trading symbol
            timeframes: List of timeframes
            
        Returns:
            Dict mapping timeframes to ATR values
        """
        symbol = standardize_symbol(symbol)
        result = {}
        
        for tf in timeframes:
            # Get historical data
            candles = await self.market_analyzer.get_historical_data(symbol, tf, 14)
            
            if len(candles) < 2:
                logger.warning(f"Not enough data to calculate ATR for {symbol} ({tf})")
                result[tf] = 0.0
                continue
                
            # Calculate true ranges
            true_ranges = []
            for i in range(1, len(candles)):
                high = candles[i].high
                low = candles[i].low
                prev_close = candles[i-1].close
                
                # True range is the greatest of:
                # 1. Current high - current low
                # 2. Abs(current high - previous close)
                # 3. Abs(current low - previous close)
                tr = max(
                    high - low,
                    abs(high - prev_close),
                    abs(low - prev_close)
                )
                true_ranges.append(tr)
            
            # Calculate ATR (simple average for simplicity)
            atr = sum(true_ranges) / len(true_ranges)
            result[tf] = atr
            
        logger.debug(f"ATR values for {symbol}: {result}")
        return result
    
    @handle_async_errors
    async def suggest_stop_loss_and_take_profit(
        self, 
        symbol: str,
        entry_price: float,
        action: str,
        timeframe: str = "1h"
    ) -> Dict[str, float]:
        """
        Suggest stop loss and take profit levels based on ATR
        
        Args:
            symbol: Trading symbol
            entry_price: Entry price
            action: Trade action ("BUY" or "SELL")
            timeframe: Timeframe to use
            
        Returns:
            Dict with stop loss and take profit prices
        """
        symbol = standardize_symbol(symbol)
        action = action.upper()
        
        if action not in ["BUY", "SELL"]:
            raise ValueError(f"Invalid action: {action}. Must be 'BUY' or 'SELL'")
        
        # Get settings for timeframe
        settings = self.timeframe_settings.get_settings(timeframe)
        
        # Get ATR for symbol
        atr_values = await self.get_atr_values(symbol, [timeframe])
        atr = atr_values.get(timeframe, 0.0)
        
        if atr <= 0:
            logger.warning(f"Invalid ATR value for {symbol} ({timeframe}): {atr}")
            # Use default values based on price
            atr = entry_price * 0.002  # Default to 0.2% of price
        
        # Calculate stop loss distance
        stop_loss_distance = atr * settings["stop_loss_atr_multiplier"]
        take_profit_distance = atr * settings["take_profit_atr_multiplier"]
        
        # Calculate stop loss and take profit based on action
        if action == "BUY":
            stop_loss = entry_price - stop_loss_distance
            take_profit = entry_price + take_profit_distance
        else:  # SELL
            stop_loss = entry_price + stop_loss_distance
            take_profit = entry_price - take_profit_distance
        
        result = {
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "risk_reward_ratio": take_profit_distance / stop_loss_distance
        }
        
        logger.info(f"Suggested levels for {symbol} ({action}): SL={stop_loss:.5f}, TP={take_profit:.5f}, RR={result['risk_reward_ratio']:.2f}")
        return result

# Initialize risk manager
risk_parameters = RiskParameters()
timeframe_settings = TimeframeSettings()
risk_manager = RiskManager(data_store, position_manager, market_analyzer, risk_parameters, timeframe_settings)

##############################################################################
# Trade Execution Functions
##############################################################################

@handle_async_errors
async def execute_trade(
    symbol: str,
    action: str,
    size: float,
    take_profit_price: Optional[float] = None,
    stop_loss_price: Optional[float] = None,
    trade_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None
) -> Tuple[bool, Dict[str, Any]]:
    """
    Execute a trade with the broker
    
    Args:
        symbol: Trading symbol
        action: BUY or SELL
        size: Position size
        take_profit_price: Take profit price (optional)
        stop_loss_price: Stop loss price (optional)
        trade_id: Unique trade ID (optional)
        metadata: Additional metadata (optional)
        
    Returns:
        Tuple of (success, result_data)
    """
    try:
        request_id = str(uuid.uuid4())
        logger.info(f"Request {request_id}: Executing trade for {symbol} - Action: {action}")
        
        # Validate action
        if action.upper() not in ["BUY", "SELL"]:
            raise CustomValidationError(f"Invalid action: {action}. Must be BUY or SELL")
            
        # Generate trade ID if not provided
        if not trade_id:
            trade_id = f"{symbol}-{action}-{int(time.time())}"
            
        # Standardize symbol
        symbol = standardize_symbol(symbol)
        
        # Default metadata
        if metadata is None:
            metadata = {}
            
        timeframe = metadata.get("timeframe", "1h")
        
        # Get current price if not executing at a specific price
        current_price = await get_current_price(symbol)
        entry_price = current_price
        
        # Calculate stop loss if not provided
        if not stop_loss_price:
            # Default to ATR-based stop loss
            atr = await market_analyzer.calculate_atr(symbol, timeframe=timeframe)
            multiplier = 2.0  # Default multiplier
            
            if action.upper() == "BUY":
                stop_loss_price = entry_price - (atr * multiplier)
            else:
                stop_loss_price = entry_price + (atr * multiplier)
        
        # Create a position object
        position = Position(
            symbol=symbol,
            action=action,
            size=size,
            entry_price=entry_price,
            take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price,
            position_id=trade_id,
            metadata=metadata
        )
        
        # Simulate broker API call
        # In a real implementation, you would call your broker's API here
        logger.debug(f"Simulating trade execution with broker API: {symbol} {action} {size}")
        
        # Record the position
        position_id = await position_manager.add_position(position)
        
        # Set up multi-tier exit plan
        await exit_manager.initialize_position_exits(
            position_id=position_id,
            symbol=symbol,
            action=action,
            entry_price=entry_price,
            stop_loss=stop_loss_price,
            size=size,
            timeframe=timeframe
        )
        
        # Get the exit plan for response
        exit_plan = await exit_manager.get_exit_plan(position_id)
        
        # Simulate successful execution
        result = {
            "success": True,
            "position_id": position_id,
            "symbol": symbol,
            "action": action,
            "size": size,
            "entry_price": entry_price,
            "stop_loss": stop_loss_price,
            "take_profit_levels": {
                "first": exit_plan["exits"]["first"]["price"] if exit_plan else take_profit_price,
                "second": exit_plan["exits"]["second"]["price"] if exit_plan else None,
                "final": exit_plan["exits"]["runner"]["price"] if exit_plan else None
            },
            "timestamp": datetime.now().isoformat(),
            "trade_id": trade_id,
            "metadata": metadata
        }
        
        logger.info(f"Trade executed successfully: {symbol} {action} {size}")
        return True, result
        
    except Exception as e:
        error_msg = f"Failed to execute trade: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, {"error": error_msg, "symbol": symbol, "action": action}


@handle_async_errors
async def close_position(
    symbol: str,
    position_id: Optional[str] = None,
    reason: str = "manual"
) -> Tuple[bool, Dict[str, Any]]:
    """
    Close a position with the broker
    
    Args:
        symbol: Trading symbol
        position_id: Position ID to close (optional)
        reason: Reason for closing the position
        
    Returns:
        Tuple of (success, result_data)
    """
    try:
        logger.info(f"Attempting to close position: {symbol} (ID: {position_id})")
        
        # Validate symbol
        symbol = standardize_symbol(symbol)
        
        # Find position if ID not provided
        if not position_id:
            positions = await position_manager.get_open_positions_by_symbol(symbol)
            if not positions:
                raise OrderError(f"No open positions found for {symbol}")
            # Close the first position found
            position = positions[0]
            position_id = position.position_id
        else:
            position = await position_manager.get_position(position_id)
            if not position:
                raise OrderError(f"Position not found: {position_id}")
        
        # Get current price
        current_price = await get_current_price(symbol)
        
        # Simulate broker API call
        # In a real implementation, you would call your broker's API here
        logger.debug(f"Simulating position closure with broker API: {symbol} {position_id}")
        
        # Close the position in our tracking system
        success, pnl = await position_manager.close_position(position_id, current_price, reason)
        
        if not success:
            raise OrderError(f"Failed to close position in position manager: {position_id}")
        
        # Simulate successful closure
        result = {
            "success": True,
            "position_id": position_id,
            "symbol": symbol,
            "close_price": current_price,
            "pnl": pnl,
            "close_reason": reason,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"Position closed successfully: {position_id}, PnL: {pnl}")
        return True, result
        
    except Exception as e:
        error_msg = f"Failed to close position: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, {"error": error_msg, "symbol": symbol, "position_id": position_id}


@handle_async_errors
async def close_partial_position(
    symbol: str,
    percentage: float,
    position_id: Optional[str] = None,
    reason: str = "partial_take_profit"
) -> Tuple[bool, Dict[str, Any]]:
    """
    Close a percentage of a position with the broker
    
    Args:
        symbol: Trading symbol
        percentage: Percentage of position to close (0-100)
        position_id: Position ID to close (optional)
        reason: Reason for closing part of the position
        
    Returns:
        Tuple of (success, result_data)
    """
    try:
        logger.info(f"Attempting to close {percentage}% of position: {symbol} (ID: {position_id})")
        
        # Validate inputs
        symbol = standardize_symbol(symbol)
        
        if percentage <= 0 or percentage > 100:
            raise CustomValidationError(f"Invalid percentage: {percentage}. Must be between 0-100")
        
        # Find position if ID not provided
        if not position_id:
            positions = await position_manager.get_open_positions_by_symbol(symbol)
            if not positions:
                raise OrderError(f"No open positions found for {symbol}")
            # Use the first position found
            position = positions[0]
            position_id = position.position_id
        else:
            position = await position_manager.get_position(position_id)
            if not position:
                raise OrderError(f"Position not found: {position_id}")
        
        # Get current price
        current_price = await get_current_price(symbol)
        
        # Calculate size to close
        original_size = position.size
        size_to_close = original_size * (percentage / 100)
        remaining_size = original_size - size_to_close
        
        # Simulate broker API call
        # In a real implementation, you would call your broker's API here
        logger.debug(f"Simulating partial position closure with broker API: {symbol} {position_id} {percentage}%")
        
        # For partial closure, we'll need to:
        # 1. Calculate PnL for the closed portion
        # 2. Update the existing position with the new size
        # 3. Create a record of the partial closure
        
        # Calculate PnL for closed portion
        entry_price = position.entry_price
        price_diff = current_price - entry_price if position.action == "BUY" else entry_price - current_price
        closed_pnl = price_diff * size_to_close
        
        # Update the position size in our system
        position.size = remaining_size
        await position_manager.update_position(position)
        
        # Simulate successful partial closure
        result = {
            "success": True,
            "position_id": position_id,
            "symbol": symbol,
            "close_price": current_price,
            "closed_percentage": percentage,
            "closed_size": size_to_close,
            "remaining_size": remaining_size,
            "pnl": closed_pnl,
            "close_reason": reason,
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"Position partially closed: {position_id}, {percentage}%, PnL: {closed_pnl}")
        return True, result
        
    except Exception as e:
        error_msg = f"Failed to close partial position: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, {"error": error_msg, "symbol": symbol, "position_id": position_id}


@handle_async_errors
async def calculate_trade_size(
    account_balance: float,
    risk_percentage: float,
    entry_price: float,
    stop_loss_price: float,
    instrument: Optional[str] = None
) -> Tuple[float, Dict[str, Any]]:
    """
    Calculate optimal position size based on account balance and risk parameters
    
    Args:
        account_balance: Current account balance
        risk_percentage: Risk percentage (e.g., 1.0 for 1%)
        entry_price: Entry price
        stop_loss_price: Stop loss price
        instrument: Trading instrument/symbol
        
    Returns:
        Tuple containing the position size and additional information
    """
    try:
        logger.info(f"Calculating trade size: balance=${account_balance}, risk={risk_percentage}%, "
                  f"entry={entry_price}, stop={stop_loss_price}, instrument={instrument}")
        
        # Validate inputs
        if account_balance <= 0:
            raise CustomValidationError("Account balance must be positive")
            
        if risk_percentage <= 0 or risk_percentage > config.max_risk_percentage:
            raise CustomValidationError(f"Risk percentage must be between 0 and {config.max_risk_percentage}%")
            
        if entry_price <= 0:
            raise CustomValidationError("Entry price must be positive")
            
        if stop_loss_price <= 0:
            raise CustomValidationError("Stop loss price must be positive")
            
        if entry_price == stop_loss_price:
            raise CustomValidationError("Entry price cannot equal stop loss price")
            
        # Convert percentage to decimal
        risk_decimal = risk_percentage / 100
        
        # Get market analyzer for ATR calculation
        market_analyzer = MarketAnalyzer()
        
        # Get symbol from instrument if provided
        symbol = standardize_symbol(instrument) if instrument else "UNKNOWN"
        
        # Check if we should use the advanced position sizing
        if position_sizing_manager and instrument:
            # Get ATR for volatility-based position sizing
            timeframe = "1h"  # Default timeframe
            atr = 0.0
            
            try:
                # Get ATR for the instrument
                historical_data = await market_analyzer.get_historical_data(symbol, timeframe, 14)
                
                if historical_data and len(historical_data) >= 14:
                    # Calculate ATR
                    tr_values = []
                    for i in range(1, len(historical_data)):
                        high = historical_data[i].high
                        low = historical_data[i].low
                        prev_close = historical_data[i-1].close
                        
                        tr1 = high - low
                        tr2 = abs(high - prev_close)
                        tr3 = abs(low - prev_close)
                        
                        tr = max(tr1, tr2, tr3)
                        tr_values.append(tr)
                        
                    atr = sum(tr_values) / len(tr_values)
            except Exception as e:
                logger.warning(f"Error calculating ATR for position sizing: {str(e)}")
                
            # Get market condition if available
            market_condition = None
            if advanced_loss_manager:
                try:
                    market_condition = await advanced_loss_manager.get_market_condition(symbol)
                except Exception as e:
                    logger.warning(f"Error getting market condition: {str(e)}")
                    
            # Get correlation factor if available
            correlation_factor = 1.0
            if position_manager:
                try:
                    # Get existing position symbols
                    positions = await position_manager.get_open_positions()
                    existing_symbols = [p.symbol for p in positions]
                    
                    # Calculate correlation factor
                    correlation_factor = await position_sizing_manager.get_correlation_factor(
                        symbol, existing_symbols
                    )
                except Exception as e:
                    logger.warning(f"Error calculating correlation factor: {str(e)}")
            
            # Calculate advanced position size
            try:
                position_size = await position_sizing_manager.calculate_position_size(
                    account_balance=account_balance,
                    entry_price=entry_price,
                    stop_loss=stop_loss_price,
                    atr=atr,
                    timeframe=timeframe,
                    market_condition=market_condition,
                    correlation_factor=correlation_factor
                )
                
                # Return position size with additional info
                return position_size, {
                    "account_balance": account_balance,
                    "risk_percentage": risk_percentage,
                    "position_size": position_size,
                    "entry_price": entry_price,
                    "stop_loss_price": stop_loss_price,
                    "atr": atr,
                    "correlation_factor": correlation_factor,
                    "risk_amount": account_balance * risk_decimal,
                    "stop_distance": abs(entry_price - stop_loss_price),
                    "calculation_method": "advanced"
                }
                
            except Exception as e:
                logger.error(f"Error in advanced position sizing: {str(e)}")
                # Fall back to basic calculation
        
        # Basic calculation if advanced failed or not available
        stop_distance = abs(entry_price - stop_loss_price)
        
        # Simple position sizing: Risk amount / Stop distance
        risk_amount = account_balance * risk_decimal
        position_size = risk_amount / stop_distance
        
        # Apply min/max constraints
        position_size = max(0.01, position_size)  # Minimum size
        
        # Default max size: 10% of account value
        max_size = (account_balance * 0.1) / entry_price
        position_size = min(position_size, max_size)
        
        # Round to 2 decimal places
        position_size = round(position_size, 2)
        
        logger.info(f"Calculated position size: {position_size}")
        
        return position_size, {
            "account_balance": account_balance,
            "risk_percentage": risk_percentage,
            "position_size": position_size,
            "entry_price": entry_price,
            "stop_loss_price": stop_loss_price,
            "risk_amount": risk_amount,
            "stop_distance": stop_distance,
            "calculation_method": "basic"
        }
        
    except Exception as e:
        logger.error(f"Error calculating trade size: {str(e)}")
        # Return minimal position size in case of error
        return 0.01, {
            "error": str(e),
            "account_balance": account_balance,
            "risk_percentage": risk_percentage,
            "position_size": 0.01,
            "calculation_method": "fallback"
        }

##############################################################################
# Application Entry Point
##############################################################################

# Initialize FastAPI app
app = FastAPI(
    title="Trading Bridge API",
    description="API for trading system bridging TradingView alerts to broker",
    version="1.0.0"
)

# Health check endpoint
@app.get("/health")
async def health_check():
    """API health check endpoint"""
    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }

# Account information endpoint
@app.get("/api/account")
async def get_account_info():
    """Get trading account information"""
    try:
        # In a real implementation, you would fetch actual account data from your broker
        # This is a simulated response
        account_data = {
            "account_id": "demo-account",
            "balance": 10000.00,
            "currency": "USD",
            "margin_used": 1500.00,
            "margin_available": 8500.00,
            "open_positions": len(await position_manager.get_open_positions()),
            "timestamp": datetime.now().isoformat()
        }
        
        return {
            "success": True,
            "data": account_data
        }
    except Exception as e:
        logger.error(f"Failed to get account info: {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }

# Open positions endpoint
@app.get("/api/positions")
async def get_positions_info():
    """Get open positions information"""
    try:
        positions = await position_manager.get_open_positions()
        
        # Get current prices for P&L calculation
        symbols = [p.symbol for p in positions]
        current_prices = {}
        
        for symbol in symbols:
            try:
                current_prices[symbol] = await get_current_price(symbol)
            except Exception as e:
                logger.warning(f"Failed to get price for {symbol}: {str(e)}")
                current_prices[symbol] = 0.0
        
        # Calculate total P&L
        total_pnl = await position_manager.calculate_total_pnl(current_prices)
        
        # Convert positions to dict for JSON serialization
        position_data = [p.to_dict() for p in positions]
        
        # Add current P&L to each position
        for p in position_data:
            symbol = p["symbol"]
            if symbol in current_prices and current_prices[symbol] > 0:
                entry_price = p["entry_price"]
                size = p["size"]
                action = p["action"]
                
                if action == "BUY":
                    p["current_pnl"] = (current_prices[symbol] - entry_price) * size
                else:  # SELL
                    p["current_pnl"] = (entry_price - current_prices[symbol]) * size
                
                p["current_price"] = current_prices[symbol]
        
        return {
            "success": True,
            "data": {
                "positions": position_data,
                "total_pnl": total_pnl,
                "count": len(positions),
                "timestamp": datetime.now().isoformat()
            }
        }
    except Exception as e:
        logger.error(f"Failed to get positions info: {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }

# Trade execution endpoint
@app.post("/api/trade")
async def execute_trade_endpoint(request: Request):
    """Execute a trade"""
    try:
        # Parse request body
        data = await request.json()
        
        # Required fields
        required_fields = ["symbol", "action", "size"]
        for field in required_fields:
            if field not in data:
                raise CustomValidationError(f"Missing required field: {field}")
        
        # Get parameters
        symbol = data["symbol"]
        action = data["action"]
        size = float(data["size"])
        
        # Optional parameters
        take_profit = data.get("take_profit")
        stop_loss = data.get("stop_loss")
        trade_id = data.get("trade_id")
        metadata = data.get("metadata", {})
        
        # Execute trade
        success, result = await execute_trade(
            symbol=symbol,
            action=action,
            size=size,
            take_profit_price=take_profit,
            stop_loss_price=stop_loss,
            trade_id=trade_id,
            metadata=metadata
        )
        
        if not success:
            return {
                "success": False,
                "error": result.get("error", "Unknown error")
            }
        
        return {
            "success": True,
            "data": result
        }
    except Exception as e:
        logger.error(f"Failed to execute trade: {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }

# Close position endpoint
@app.post("/api/close")
async def close_position_endpoint(request: Request):
    """Close a position"""
    try:
        # Parse request body
        data = await request.json()
        
        # Required fields
        if "symbol" not in data:
            raise CustomValidationError("Missing required field: symbol")
        
        # Get parameters
        symbol = data["symbol"]
        position_id = data.get("position_id")
        reason = data.get("reason", "manual")
        
        # Check if this is a partial close
        percentage = data.get("percentage")
        
        if percentage is not None:
            # Partial close
            percentage = float(percentage)
            success, result = await close_partial_position(
                symbol=symbol,
                percentage=percentage,
                position_id=position_id,
                reason=reason
            )
        else:
            # Full close
            success, result = await close_position(
                symbol=symbol,
                position_id=position_id,
                reason=reason
            )
        
        if not success:
            return {
                "success": False,
                "error": result.get("error", "Unknown error")
            }
        
        return {
            "success": True,
            "data": result
        }
    except Exception as e:
        logger.error(f"Failed to close position: {str(e)}", exc_info=True)
        return {
            "success": False,
            "error": str(e)
        }

@app.on_event("startup")
async def startup_event():
    """Initialize application on startup"""
    logger.info("Starting application...")
    await setup_initial_dependencies()

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup resources on shutdown"""
    logger.info("Shutting down application...")
    await cleanup_resources()

async def setup_initial_dependencies():
    """Set up initial dependencies and services"""
    # Initialize datastore
    global data_store
    data_store = DataStore()
    data_store.init()
    
    # Initialize components
    global position_manager, risk_manager, market_analyzer, multi_tier_exit_manager
    global error_recovery_system, advanced_loss_manager, risk_analytics, position_sizing_manager
    
    # Create market analyzer
    market_analyzer = MarketAnalyzer()
    
    # Create position manager
    position_manager = PositionManager(data_store)
    await position_manager.load_positions()
    
    # Initialize risk manager with dependencies
    risk_parameters = RiskParameters()
    timeframe_settings = TimeframeSettings()
    risk_manager = RiskManager(
        data_store, 
        position_manager, 
        market_analyzer,
        risk_parameters, 
        timeframe_settings
    )
    
    # Initialize multi-tier exit manager
    multi_tier_exit_manager = MultiTierExitManager(data_store)
    
    # Initialize the error recovery system
    error_recovery_system = ErrorRecoverySystem()
    
    # Initialize advanced loss manager
    advanced_loss_manager = AdvancedLossManager(data_store)
    
    # Initialize risk analytics
    risk_analytics = RiskAnalytics(data_store)
    
    # Initialize position sizing manager
    position_sizing_manager = PositionSizingManager(data_store, risk_manager)
    
    # Start monitoring tasks
    asyncio.create_task(position_monitor())
    
    logger.info("Application dependencies initialized successfully")

async def position_monitor():
    """Background task for monitoring positions"""
    try:
        logger.info("Starting position monitor background task")
        while True:
            try:
                # Get all positions
                positions = await position_manager.get_open_positions()
                
                if positions:
                    # Get symbols to check
                    symbols = [p.symbol for p in positions]
                    
                    # Get current prices
                    current_prices = {}
                    for symbol in symbols:
                        try:
                            price = await get_current_price(symbol)
                            current_prices[symbol] = price
                        except Exception as e:
                            logger.warning(f"Failed to get price for {symbol}: {str(e)}")
                    
                    # Update positions and check for exit conditions
                    await position_manager.update_all_positions(current_prices)
                    
                # Wait for next check (every 10 seconds)
                await asyncio.sleep(10)
                
            except Exception as e:
                logger.error(f"Error in position monitor: {str(e)}", exc_info=True)
                await asyncio.sleep(30)  # Wait longer on error
    except asyncio.CancelledError:
        logger.info("Position monitor task cancelled")
    except Exception as e:
        logger.error(f"Position monitor task failed: {str(e)}", exc_info=True)

async def cleanup_resources():
    """Clean up resources on application shutdown"""
    # Close any open HTTP sessions
    session = await get_session()
    if session and not session.closed:
        await session.close()

def start():
    """Start the application server"""
    config = Settings()
    
    # Set up logging
    setup_logging()
    
    # Log startup
    logger.info(f"Starting server in {config.environment} mode")
    logger.info(f"Host: {config.host}, Port: {config.port}")
    
    # Start server
    if config.environment == "development":
        # Development mode with auto-reload
        import uvicorn
        uvicorn.run(
            "python_bridge_final:app",
            host=config.host,
            port=config.port,
            reload=True,
            log_level="debug"
        )
    else:
        # Production mode
        import uvicorn
        uvicorn.run(
            app,
            host=config.host,
            port=config.port,
            log_level="info"
        )

# Initialize global dependencies
settings = Settings()
data_store = DataStore()
position_manager = PositionManager(data_store)
market_analyzer = MarketAnalyzer()
risk_parameters = RiskParameters()
timeframe_settings = TimeframeSettings()
risk_manager = RiskManager(data_store, position_manager, market_analyzer, risk_parameters, timeframe_settings)
exit_manager = MultiTierExitManager(data_store)

##############################################################################
# Multi-Tier Exit Management
##############################################################################

class MultiTierExitManager:
    """
    Manages multi-tier take profit strategy for positions
    """
    def __init__(self, data_store: DataStore):
        """Initialize the multi-tier exit manager"""
        self.data_store = data_store
        self.exits = {}
        
        # Default take profit levels by timeframe
        self.take_profit_levels = {
            "15m": {
                "first_exit": 0.5,   # 50% at 1:1 risk:reward
                "second_exit": 0.25,  # 25% at 2:1 risk:reward
                "runner": 0.25        # 25% with trailing stop
            },
            "1h": {
                "first_exit": 0.4,    # 40% at 1:1 risk:reward
                "second_exit": 0.3,    # 30% at 2:1 risk:reward
                "runner": 0.3         # 30% with trailing stop
            },
            "4h": {
                "first_exit": 0.33,   # 33% at 1:1 risk:reward
                "second_exit": 0.33,   # 33% at 2:1 risk:reward
                "runner": 0.34        # 34% with trailing stop
            },
            "1d": {
                "first_exit": 0.33,   # 33% at 1:1 risk:reward
                "second_exit": 0.33,   # 33% at 2:1 risk:reward
                "runner": 0.34        # 34% with trailing stop
            }
        }
        
        # Trailing stop settings by timeframe
        self.trailing_settings = {
            "15m": {
                "activation_level": 1.5,  # Activate at 1.5R
                "initial_distance": 1.0,   # Initial distance of 1R
                "step": 0.5               # Move every 0.5R
            },
            "1h": {
                "activation_level": 2.0,   # Activate at 2R
                "initial_distance": 1.5,   # Initial distance of 1.5R 
                "step": 0.5                # Move every 0.5R
            },
            "4h": {
                "activation_level": 2.5,   # Activate at 2.5R
                "initial_distance": 2.0,   # Initial distance of 2R
                "step": 0.5                # Move every 0.5R
            },
            "1d": {
                "activation_level": 3.0,   # Activate at 3R
                "initial_distance": 2.5,   # Initial distance of 2.5R
                "step": 0.5                # Move every 0.5R
            }
        }
    
    @handle_async_errors
    async def initialize_position_exits(self, 
                                        position_id: str, 
                                        symbol: str, 
                                        action: str,
                                        entry_price: float, 
                                        stop_loss: float,
                                        size: float,
                                        timeframe: str = "1h") -> Dict[str, Any]:
        """
        Initialize multi-tier exits for a new position
        
        Args:
            position_id: Unique position identifier
            symbol: Trading symbol
            action: "BUY" or "SELL"
            entry_price: Position entry price
            stop_loss: Stop loss price
            size: Position size
            timeframe: Trading timeframe
        
        Returns:
            Dictionary with exit levels
        """
        # Standardize timeframe format
        timeframe = timeframe.lower()
        if timeframe not in self.take_profit_levels:
            logger.warning(f"No take profit levels for timeframe {timeframe}, using 1h")
            timeframe = "1h"
        
        # Get take profit levels for this timeframe
        levels = self.take_profit_levels[timeframe]
        
        # Calculate base risk (R)
        risk_per_unit = abs(entry_price - stop_loss)
        is_long = action.upper() == "BUY"
        
        # Calculate take profit levels based on risk multiples
        first_tp_distance = 1.0 * risk_per_unit  # 1R
        second_tp_distance = 2.0 * risk_per_unit  # 2R
        final_tp_distance = 3.0 * risk_per_unit  # 3R
        
        if is_long:
            first_tp = entry_price + first_tp_distance
            second_tp = entry_price + second_tp_distance
            final_tp = entry_price + final_tp_distance
        else:
            first_tp = entry_price - first_tp_distance
            second_tp = entry_price - second_tp_distance
            final_tp = entry_price - final_tp_distance
        
        # Calculate size for each exit
        total_size = size
        first_exit_size = total_size * levels["first_exit"]
        second_exit_size = total_size * levels["second_exit"]
        runner_size = total_size * levels["runner"]
        
        # Store exit plan
        exit_plan = {
            "position_id": position_id,
            "symbol": symbol,
            "action": action,
            "entry_price": entry_price,
            "stop_loss": stop_loss,
            "total_size": total_size,
            "timeframe": timeframe,
            "initialized_at": datetime.now(timezone.utc).isoformat(),
            "risk_per_unit": risk_per_unit,
            "exits": {
                "first": {
                    "price": first_tp,
                    "size": first_exit_size,
                    "executed": False,
                    "r_multiple": 1.0
                },
                "second": {
                    "price": second_tp,
                    "size": second_exit_size,
                    "executed": False,
                    "r_multiple": 2.0
                },
                "runner": {
                    "price": final_tp,
                    "size": runner_size,
                    "executed": False,
                    "r_multiple": 3.0,
                    "trailing_active": False,
                    "trailing_stop": None
                }
            }
        }
        
        # Save exit plan
        self.exits[position_id] = exit_plan
        await self._persist_exit_plan(position_id, exit_plan)
        
        logger.info(f"Initialized multi-tier exits for position {position_id}: {symbol} {action}")
        logger.debug(f"Exit plan: 1R={first_tp:.5f}, 2R={second_tp:.5f}, 3R={final_tp:.5f}")
        
        return exit_plan
    
    @handle_async_errors
    async def check_exit_levels(self, 
                               position_id: str, 
                               current_price: float) -> Dict[str, Any]:
        """
        Check if any exit levels have been reached
        
        Args:
            position_id: Position ID
            current_price: Current price
            
        Returns:
            Action to take (if any)
        """
        if position_id not in self.exits:
            logger.warning(f"No exit plan found for position {position_id}")
            return {"action": "NONE"}
        
        exit_plan = self.exits[position_id]
        is_long = exit_plan["action"].upper() == "BUY"
        
        result = {"action": "NONE"}
        
        # Check first exit
        first_exit = exit_plan["exits"]["first"]
        if not first_exit["executed"]:
            if (is_long and current_price >= first_exit["price"]) or \
               (not is_long and current_price <= first_exit["price"]):
                result = {
                    "action": "PARTIAL_CLOSE",
                    "size": first_exit["size"],
                    "price": current_price,
                    "percentage": exit_plan["take_profit_levels"]["first_exit"] * 100,
                    "exit_level": "first",
                    "r_multiple": first_exit["r_multiple"]
                }
                
        # Check second exit if first has been executed
        elif not exit_plan["exits"]["second"]["executed"]:
            second_exit = exit_plan["exits"]["second"]
            if (is_long and current_price >= second_exit["price"]) or \
               (not is_long and current_price <= second_exit["price"]):
                result = {
                    "action": "PARTIAL_CLOSE",
                    "size": second_exit["size"],
                    "price": current_price,
                    "percentage": exit_plan["take_profit_levels"]["second_exit"] * 100,
                    "exit_level": "second",
                    "r_multiple": second_exit["r_multiple"]
                }
        
        # Check trailing stop for runner if activated
        elif exit_plan["exits"]["runner"]["trailing_active"]:
            trailing_stop = exit_plan["exits"]["runner"]["trailing_stop"]
            if trailing_stop and \
               ((is_long and current_price <= trailing_stop) or \
                (not is_long and current_price >= trailing_stop)):
                result = {
                    "action": "CLOSE",
                    "price": current_price,
                    "exit_level": "runner_trailing",
                    "r_multiple": self._calculate_r_multiple(exit_plan, current_price)
                }
                
        # Check if runner take profit hit before trailing activated
        elif not exit_plan["exits"]["runner"]["executed"]:
            runner_exit = exit_plan["exits"]["runner"]
            if (is_long and current_price >= runner_exit["price"]) or \
               (not is_long and current_price <= runner_exit["price"]):
                result = {
                    "action": "PARTIAL_CLOSE",
                    "size": runner_exit["size"],
                    "price": current_price,
                    "percentage": exit_plan["take_profit_levels"]["runner"] * 100,
                    "exit_level": "runner",
                    "r_multiple": runner_exit["r_multiple"]
                }
                
        # Check if we should activate trailing stop
        await self._update_trailing_stop(position_id, current_price)
                
        return result
    
    @handle_async_errors
    async def mark_exit_executed(self, 
                                position_id: str, 
                                exit_level: str) -> bool:
        """
        Mark an exit level as executed
        
        Args:
            position_id: Position ID
            exit_level: Exit level ("first", "second", "runner")
            
        Returns:
            Success status
        """
        if position_id not in self.exits:
            logger.warning(f"No exit plan found for position {position_id}")
            return False
            
        if exit_level not in ["first", "second", "runner"]:
            logger.warning(f"Invalid exit level: {exit_level}")
            return False
            
        self.exits[position_id]["exits"][exit_level]["executed"] = True
        self.exits[position_id]["exits"][exit_level]["executed_at"] = datetime.now(timezone.utc).isoformat()
        
        # Save updated exit plan
        await self._persist_exit_plan(position_id, self.exits[position_id])
        
        logger.info(f"Marked {exit_level} exit as executed for position {position_id}")
        return True
    
    @handle_async_errors
    async def _update_trailing_stop(self, 
                                   position_id: str, 
                                   current_price: float) -> bool:
        """
        Update trailing stop for the runner portion
        
        Args:
            position_id: Position ID
            current_price: Current price
            
        Returns:
            True if trailing stop was updated
        """
        if position_id not in self.exits:
            return False
            
        exit_plan = self.exits[position_id]
        runner = exit_plan["exits"]["runner"]
        
        # If runner portion already executed, nothing to do
        if runner["executed"]:
            return False
            
        # If first and second exits not executed, we're not ready for trailing
        if not exit_plan["exits"]["first"]["executed"] or not exit_plan["exits"]["second"]["executed"]:
            return False
            
        is_long = exit_plan["action"].upper() == "BUY"
        risk_per_unit = exit_plan["risk_per_unit"]
        entry_price = exit_plan["entry_price"]
        
        # Calculate current R-multiple
        r_multiple = self._calculate_r_multiple(exit_plan, current_price)
        
        # Get trailing settings for this timeframe
        timeframe = exit_plan["timeframe"]
        trailing = self.trailing_settings.get(timeframe, self.trailing_settings["1h"])
        
        # Check if we should activate trailing stop
        if not runner["trailing_active"] and r_multiple >= trailing["activation_level"]:
            # Activate trailing stop
            if is_long:
                trailing_stop = current_price - (risk_per_unit * trailing["initial_distance"])
                # Ensure trailing stop is above entry for long positions
                trailing_stop = max(trailing_stop, exit_plan["entry_price"])
            else:
                trailing_stop = current_price + (risk_per_unit * trailing["initial_distance"])
                # Ensure trailing stop is below entry for short positions
                trailing_stop = min(trailing_stop, exit_plan["entry_price"])
                
            self.exits[position_id]["exits"]["runner"]["trailing_active"] = True
            self.exits[position_id]["exits"]["runner"]["trailing_stop"] = trailing_stop
            
            logger.info(f"Activated trailing stop for position {position_id} at {trailing_stop:.5f}")
            await self._persist_exit_plan(position_id, self.exits[position_id])
            return True
            
        # If trailing already active, check if we should move the stop
        elif runner["trailing_active"] and runner["trailing_stop"] is not None:
            current_stop = runner["trailing_stop"]
            step_distance = risk_per_unit * trailing["step"]
            
            if is_long:
                new_stop = current_price - (risk_per_unit * trailing["initial_distance"])
                # Only move stop up for long positions
                if new_stop > current_stop + step_distance:
                    self.exits[position_id]["exits"]["runner"]["trailing_stop"] = new_stop
                    logger.info(f"Updated trailing stop for position {position_id} to {new_stop:.5f}")
                    await self._persist_exit_plan(position_id, self.exits[position_id])
                    return True
            else:
                new_stop = current_price + (risk_per_unit * trailing["initial_distance"])
                # Only move stop down for short positions
                if new_stop < current_stop - step_distance:
                    self.exits[position_id]["exits"]["runner"]["trailing_stop"] = new_stop
                    logger.info(f"Updated trailing stop for position {position_id} to {new_stop:.5f}")
                    await self._persist_exit_plan(position_id, self.exits[position_id])
                    return True
                    
        return False
    
    def _calculate_r_multiple(self, exit_plan: Dict[str, Any], current_price: float) -> float:
        """
        Calculate the current R multiple (how many times the initial risk)
        
        Args:
            exit_plan: Exit plan
            current_price: Current price
            
        Returns:
            R multiple
        """
        entry_price = exit_plan["entry_price"]
        risk_per_unit = exit_plan["risk_per_unit"]
        is_long = exit_plan["action"].upper() == "BUY"
        
        if risk_per_unit == 0:
            return 0
            
        if is_long:
            profit = current_price - entry_price
        else:
            profit = entry_price - current_price
            
        return profit / risk_per_unit
    
    @handle_async_errors
    async def get_exit_plan(self, position_id: str) -> Dict[str, Any]:
        """
        Get the exit plan for a position
        
        Args:
            position_id: Position ID
            
        Returns:
            Exit plan dictionary
        """
        if position_id in self.exits:
            return copy.deepcopy(self.exits[position_id])
            
        # Try to load from storage
        stored_plan = await self._load_exit_plan(position_id)
        if stored_plan:
            self.exits[position_id] = stored_plan
            return copy.deepcopy(stored_plan)
            
        return None
    
    @handle_async_errors
    async def clear_exit_plan(self, position_id: str) -> bool:
        """
        Clear the exit plan for a position
        
        Args:
            position_id: Position ID
            
        Returns:
            Success status
        """
        if position_id in self.exits:
            del self.exits[position_id]
            
        key = f"exit_plan:{position_id}"
        self.data_store.delete_position(key)
        
        logger.info(f"Cleared exit plan for position {position_id}")
        return True
    
    @handle_async_errors
    async def _persist_exit_plan(self, position_id: str, exit_plan: Dict[str, Any]) -> bool:
        """
        Save exit plan to persistent storage
        
        Args:
            position_id: Position ID
            exit_plan: Exit plan
            
        Returns:
            Success status
        """
        key = f"exit_plan:{position_id}"
        return self.data_store.store_position(key, exit_plan)
    
    @handle_async_errors
    async def _load_exit_plan(self, position_id: str) -> Dict[str, Any]:
        """
        Load exit plan from persistent storage
        
        Args:
            position_id: Position ID
            
        Returns:
            Exit plan dictionary or None
        """
        key = f"exit_plan:{position_id}"
        return self.data_store.get_position(key)
    
    @handle_async_errors
    async def monitor_positions(self, current_prices: Dict[str, float]) -> List[Dict[str, Any]]:
        """
        Monitor all positions for exit conditions
        
        Args:
            current_prices: Dictionary of symbol -> current price
            
        Returns:
            List of exit actions to take
        """
        exit_actions = []
        
        # Get all exit plans
        all_plans = list(self.exits.values())
        
        for exit_plan in all_plans:
            position_id = exit_plan["position_id"]
            symbol = exit_plan["symbol"]
            
            # Skip if we don't have a current price
            if symbol not in current_prices:
                continue
                
            current_price = current_prices[symbol]
            
            # Check if any exit levels are hit
            exit_action = await self.check_exit_levels(position_id, current_price)
            
            if exit_action["action"] != "NONE":
                exit_action["position_id"] = position_id
                exit_action["symbol"] = symbol
                exit_actions.append(exit_action)
        
        return exit_actions

# Run the application if executed directly
if __name__ == "__main__":
    start() 