##############################################################################
# Core Setup - Block 1: Imports, Error Handling, Configuration
##############################################################################

import os
import uuid
import asyncio
import aiohttp
import logging
import logging.handlers
import re
import time
import json
import signal
import holidays
import statistics
import numpy as np
from typing import Dict, Any, Tuple
import re
from datetime import datetime, timedelta
from pytz import timezone
from fastapi import FastAPI, Request, HTTPException, status, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any, Union, List, Tuple, Callable, TypeVar, ParamSpec
from contextlib import asynccontextmanager
from pydantic import BaseModel, validator, ValidationError, Field
from functools import wraps
from redis.asyncio import Redis
from prometheus_client import Counter, Histogram
from pydantic_settings import BaseSettings

# Type variables for type hints
P = ParamSpec('P')
T = TypeVar('T')

# Prometheus metrics
TRADE_REQUESTS = Counter('trade_requests', 'Total trade requests')
TRADE_LATENCY = Histogram('trade_latency', 'Trade processing latency')

# Redis for shared state
redis = Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379'))

# Configure logger
logger = logging.getLogger("trading_bot")
if not logger.handlers:
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Global lock for thread safety
_lock = asyncio.Lock()

# These would normally be initialized elsewhere but for now let's declare them
error_recovery = None
position_tracker = None
risk_manager = None
dynamic_exit_manager = None
loss_manager = None
risk_analytics = None
market_structure_analyzer = None
volatility_monitor = None
position_sizing = None

# Import configuration (assuming it exists elsewhere)
class config:
    oanda_account = "your_oanda_account"
    oanda_api_url = "https://api-fxpractice.oanda.com/v3"
    max_retries = 3
    base_delay = 1

# Constants
HTTP_REQUEST_TIMEOUT = 10  # seconds

##############################################################################
# Error Handling Infrastructure
##############################################################################
##############################################################################
# Circuit Breaker Pattern (Without Notifications)
##############################################################################

class CircuitBreaker:
    """
    Circuit breaker to temporarily disable trading when too many errors occur.
    Uses sliding window to track errors and can auto-reset after a cooldown period.
    """
    def __init__(self, error_threshold: int = 5, window_seconds: int = 300, cooldown_seconds: int = 600):
        """Initialize circuit breaker with configurable thresholds"""
        self.error_threshold = error_threshold  # Number of errors before tripping
        self.window_seconds = window_seconds    # Time window to count errors (seconds)
        self.cooldown_seconds = cooldown_seconds  # Cooldown period after tripping (seconds)
        
        self.error_timestamps = []  # Timestamps of recent errors
        self.tripped = False        # Current circuit state
        self.tripped_time = None    # When circuit was last tripped
        self._lock = asyncio.Lock() # Thread safety
        
    async def record_error(self) -> bool:
        """
        Record an error and check if circuit should trip
        Returns True if circuit is now tripped
        """
        async with self._lock:
            # Auto-reset if cooldown period has passed
            await self._check_auto_reset()
            
            if self.tripped:
                return True
                
            # Record current error
            current_time = time.time()
            self.error_timestamps.append(current_time)
            
            # Remove errors outside the window
            window_start = current_time - self.window_seconds
            self.error_timestamps = [t for t in self.error_timestamps if t >= window_start]
            
            # Check if threshold exceeded
            if len(self.error_timestamps) >= self.error_threshold:
                logger.warning(f"Circuit breaker tripped: {len(self.error_timestamps)} errors in last {self.window_seconds} seconds")
                self.tripped = True
                self.tripped_time = current_time
                return True
                
            return False
            
    async def is_open(self) -> bool:
        """Check if circuit is open (i.e., trading disabled)"""
        async with self._lock:
            await self._check_auto_reset()
            return self.tripped
            
    async def reset(self) -> None:
        """Manually reset the circuit breaker"""
        async with self._lock:
            was_tripped = self.tripped
            self.tripped = False
            self.error_timestamps = []
            self.tripped_time = None
            
            if was_tripped:
                logger.info("Circuit breaker manually reset")
            
    async def _check_auto_reset(self) -> None:
        """Check if circuit should auto-reset after cooldown"""
        if not self.tripped or not self.tripped_time:
            return
            
        current_time = time.time()
        if current_time - self.tripped_time >= self.cooldown_seconds:
            self.tripped = False
            self.error_timestamps = []
            .info(f"Circuit breaker auto-reset after {self.cooldown_seconds} seconds cooldown")
            
    def get_status(self) -> Dict[str, Any]:
        """Get current status of the circuit breaker"""
        current_time = time.time()
        recent_errors = len([t for t in self.error_timestamps 
                            if t >= current_time - self.window_seconds])
                            
        cooldown_remaining = 0
        if self.tripped and self.tripped_time:
            elapsed = current_time - self.tripped_time
            cooldown_remaining = max(0, self.cooldown_seconds - elapsed)
            
        return {
            "state": "OPEN" if self.tripped else "CLOSED",
            "recent_errors": recent_errors,
            "error_threshold": self.error_threshold,
            "cooldown_remaining_seconds": int(cooldown_remaining),
            "window_seconds": self.window_seconds,
            "cooldown_seconds": self.cooldown_seconds
        }

class ErrorRecoverySystem:
    """
    System to recover from common errors and retry operations
    with exponential backoff and state recovery
    """
    def __init__(self):
        self.circuit_breaker = CircuitBreaker()
        self.recovery_attempts = {}  # Track recovery attempts by request ID
        self.recovery_history = {}   # Track recovery history
        self.stale_position_checks = {}  # Track stale position check timestamps
        self._lock = asyncio.Lock()
        
    async def handle_error(self, request_id: str, operation: str, 
                          error: Exception, context: Dict[str, Any] = None) -> bool:
        """
        Handle an error with potential recovery
        Returns True if error was handled and recovery attempt was made
        """
        error_str = str(error)
        error_type = type(error).__name__
        
        .error(f"Error in {operation} (request {request_id}): {error_str}")
        
        # Check if circuit breaker is open
        if await self.circuit_breaker.is_open():
            logger.warning(f"Circuit breaker open, skipping recovery for {operation}")
            return False
            
        # Record error in circuit breaker
        circuit_tripped = await self.circuit_breaker.record_error()
        if circuit_tripped:
            logger.warning(f"Circuit breaker tripped due to error in {operation}")
            return False
            
        # Get recovery strategy based on error type and operation
        recovery_strategy = self._get_recovery_strategy(operation, error_type, error_str)
        
        if not recovery_strategy:
            logger.warning(f"No recovery strategy for {error_type} in {operation}")
            return False
            
        # Apply recovery strategy
        async with self._lock:
            # Track recovery attempts
            if request_id not in self.recovery_attempts:
                self.recovery_attempts[request_id] = {
                    "count": 0,
                    "last_attempt": time.time(),
                    "operation": operation
                }
                
            attempt_info = self.recovery_attempts[request_id]
            attempt_info["count"] += 1
            attempt_info["last_attempt"] = time.time()
            
            # Limit number of recovery attempts
            if attempt_info["count"] > 3:
                logger.error(f"Maximum recovery attempts reached for {request_id} ({operation})")
                
                # Remove from tracking
                del self.recovery_attempts[request_id]
                return False
                
        # Log recovery attempt
        logger.info(f"Attempting recovery for {operation} (attempt {attempt_info['count']})")
        
        # Process recovery
        recovery_success = False
        try:
            if recovery_strategy == "retry":
                recovery_success = await self._retry_operation(operation, context, attempt_info["count"])
            elif recovery_strategy == "reconnect":
                recovery_success = await self._reconnect_and_retry(operation, context, attempt_info["count"])
            elif recovery_strategy == "position_sync":
                recovery_success = await self._sync_positions(context)
            elif recovery_strategy == "session_reset":
                recovery_success = await self._reset_session_and_retry(operation, context, attempt_info["count"])
                
            # Record recovery outcome
            self._record_recovery_outcome(operation, error_type, recovery_strategy, recovery_success)
            
            # Clean up tracking if successful
            if recovery_success:
                async with self._lock:
                    if request_id in self.recovery_attempts:
                        del self.recovery_attempts[request_id]
                        
            return recovery_success
            
        except Exception as recovery_error:
            logger.error(f"Error during recovery attempt: {str(recovery_error)}")
            return False
            
    def _get_recovery_strategy(self, operation: str, error_type: str, error_message: str) -> Optional[str]:
        """Determine appropriate recovery strategy based on error"""
        # Network/connection errors
        if any(term in error_type for term in ["Timeout", "Connection", "ClientError"]):
            return "reconnect"
            
        # Session errors
        if "session" in error_message.lower() or "closed" in error_message.lower():
            return "session_reset"
            
        # Position-related errors
        if operation in ["close_position", "_handle_position_actions", "update_position"]:
            if "not defined" in error_message or "not found" in error_message:
                return "position_sync"
                
        # Type errors that might be fixed with a retry after clean state
        if error_type in ["TypeError", "AttributeError", "KeyError"]:
            if "subscriptable" in error_message or "not defined" in error_message:
                return "position_sync"
                
        # Default strategy for most operations is retry
        if operation in ["execute_trade", "close_position", "get_account_balance", "get_current_price"]:
            return "retry"
            
        return None
        
    async def _retry_operation(self, operation: str, context: Dict[str, Any], attempt: int) -> bool:
        """Retry an operation with exponential backoff"""
        if not context or not context.get("func"):
            logger.error(f"Cannot retry operation {operation}: missing context or function")
            return False
            
        # Calculate backoff delay
        delay = min(30, config.base_delay * (2 ** (attempt - 1)))
        logger.info(f"Retrying {operation} after {delay}s delay (attempt {attempt})")
        
        # Wait before retry
        await asyncio.sleep(delay)
        
        # Retry the operation
        func = context.get("func")
        args = context.get("args", [])
        kwargs = context.get("kwargs", {})
        
        try:
            result = await func(*args, **kwargs)
            logger.info(f"Retry successful for {operation}")
            return True
        except Exception as e:
            logger.error(f"Retry failed for {operation}: {str(e)}")
            return False
            
    async def _reconnect_and_retry(self, operation: str, context: Dict[str, Any], attempt: int) -> bool:
        """Reconnect session and retry operation"""
        logger.info(f"Reconnecting session before retrying {operation}")
        
        try:
            # Force new session
            await get_session(force_new=True)
            
            # Now retry the operation
            return await self._retry_operation(operation, context, attempt)
        except Exception as e:
            logger.error(f"Reconnection failed: {str(e)}")
            return False
            
    async def _reset_session_and_retry(self, operation: str, context: Dict[str, Any], attempt: int) -> bool:
        """Reset session and retry with clean state"""
        logger.info(f"Resetting session for {operation}")
        
        try:
            # Close and recreate session
            await cleanup_stale_sessions()
            await get_session(force_new=True)
            
            # Add delay for external services to recognize the reset
            await asyncio.sleep(5)
            
            # Now retry
            return await self._retry_operation(operation, context, attempt)
        except Exception as e:
            logger.error(f"Session reset failed: {str(e)}")
            return False
            
    async def _sync_positions(self, context: Dict[str, Any]) -> bool:
        """Synchronize position state with broker"""
        logger.info("Synchronizing positions with broker")
        
        try:
            # Get handler reference
            handler = context.get("handler")
            if not handler or not hasattr(handler, "position_tracker"):
                logger.error("Cannot sync positions: missing handler or tracker")
                return False
                
            # Get current positions from broker
            success, positions_data = await get_open_positions()
            if not success:
                logger.error("Failed to get positions from broker")
                return False
                
            # Get tracked positions
            tracked_positions = await handler.position_tracker.get_all_positions()
            
            # Extract broker positions
            broker_positions = {
                p["instrument"]: p for p in positions_data.get("positions", [])
            }
            
            # Sync any missing positions
            for symbol in tracked_positions:
                if symbol not in broker_positions:
                    logger.warning(f"Position {symbol} exists in tracker but not with broker - removing")
                    await handler.position_tracker.clear_position(symbol)
                    await handler.risk_manager.clear_position(symbol)
                    
            # Log results
            logger.info(f"Position sync complete. Removed {len(tracked_positions) - len(broker_positions)} stale positions")
            
            return True
            
        except Exception as e:
            logger.error(f"Position sync failed: {str(e)}")
            return False
            
    def _record_recovery_outcome(self, operation: str, error_type: str, strategy: str, success: bool):
        """Record recovery outcome for analytics"""
        if operation not in self.recovery_history:
            self.recovery_history[operation] = {
                "attempts": 0,
                "successes": 0,
                "strategies": {}
            }
            
        history = self.recovery_history[operation]
        history["attempts"] += 1
        if success:
            history["successes"] += 1
            
        # Track strategy effectiveness
        if strategy not in history["strategies"]:
            history["strategies"][strategy] = {
                "attempts": 0,
                "successes": 0,
                "error_types": {}
            }
            
        strategy_stats = history["strategies"][strategy]
        strategy_stats["attempts"] += 1
        if success:
            strategy_stats["successes"] += 1
            
        # Track error types
        if error_type not in strategy_stats["error_types"]:
            strategy_stats["error_types"][error_type] = {
                "attempts": 0,
                "successes": 0
            }
            
        error_stats = strategy_stats["error_types"][error_type]
        error_stats["attempts"] += 1
        if success:
            error_stats["successes"] += 1
            
    async def get_recovery_stats(self) -> Dict[str, Any]:
        """Get recovery statistics"""
        async with self._lock:
            total_attempts = sum(h["attempts"] for h in self.recovery_history.values())
            total_successes = sum(h["successes"] for h in self.recovery_history.values())
            
            return {
                "total_recovery_attempts": total_attempts,
                "successful_recoveries": total_successes,
                "success_rate": round((total_successes / total_attempts) * 100, 2) if total_attempts > 0 else 0,
                "active_recovery_count": len(self.recovery_attempts),
                "operation_stats": self.recovery_history,
                "circuit_breaker": await self.get_circuit_breaker_status()
            }
            
    async def get_circuit_breaker_status(self) -> Dict[str, Any]:
        """Get current circuit breaker status"""
        return self.circuit_breaker.get_status()
        
    async def reset_circuit_breaker(self) -> bool:
        """Reset the circuit breaker"""
        await self.circuit_breaker.reset()
        return True
        
    async def schedule_stale_position_check(self):
        """Schedule regular stale position checks"""
        while True:
            try:
                await self._check_for_stale_positions()
                await asyncio.sleep(900)  # Check every 15 minutes
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in stale position check: {str(e)}")
                await asyncio.sleep(300)  # Shorter retry on error
                
    async def _check_for_stale_positions(self):
        """Check for and clean up stale positions"""
        logger.info("Running scheduled stale position check")
        
        try:
            # Ensure we have a handler reference
            if not alert_handler:
                logger.warning("Cannot check stale positions: alert handler not initialized")
                return
                
            # Context for position sync
            context = {
                "handler": alert_handler
            }
            
            # Run position sync
            success = await self._sync_positions(context)
            logger.info(f"Stale position check completed with status: {success}")
            
        except Exception as e:
            logger.error(f"Stale position check failed: {str(e)}")

# Create the error recovery system
error_recovery = ErrorRecoverySystem()

class TradingError(Exception):
    """Base exception for trading-related errors"""
    pass

class MarketError(TradingError):
    """Errors related to market conditions"""
    pass

class OrderError(TradingError):
    """Errors related to order execution"""
    pass

class CustomValidationError(TradingError):
    """Errors related to data validation"""
    pass

# Placeholder for the handle_async_errors decorator
def handle_async_errors(func):
    """Decorator to handle async errors"""
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
            return False, {"error": str(e)}
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
        except TradingError as e:
            logger.error(f"Trading error in {func.__name__}: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {str(e)}", exc_info=True)
            raise TradingError(f"Internal error in {func.__name__}: {str(e)}") from e
    return wrapper

##############################################################################
# Configuration & Constants
##############################################################################

class Settings(BaseSettings):
    """Centralized configuration management"""
    oanda_account: str = Field(alias='OANDA_ACCOUNT_ID')
    oanda_token: str = Field(alias='OANDA_API_TOKEN')
    oanda_api_url: str = Field(
        default="https://api-fxtrade.oanda.com/v3",
        alias='OANDA_API_URL'
    )
    oanda_environment: str = Field(
        default="practice",
        alias='OANDA_ENVIRONMENT'
    )
    allowed_origins: str = "http://localhost"
    connect_timeout: int = 10
    read_timeout: int = 30
    total_timeout: int = 45
    max_simultaneous_connections: int = 100
    spread_threshold_forex: float = 0.001
    spread_threshold_crypto: float = 0.008
    max_retries: int = 3
    base_delay: float = 1.0
    base_position: int = 5000  # Updated from 300000 to 3000
    max_daily_loss: float = 0.20  # 20% max daily loss
    host: str = "0.0.0.0"
    port: int = 8000
    environment: str = "production"
    max_requests_per_minute: int = 100  # Added missing config parameter

    trade_24_7: bool = True  # Set to True for exchanges trading 24/7

    class Config:
        env_file = '.env'
        case_sensitive = True
        
config = Settings()

# Add this back for monitoring purposes
MAX_DAILY_LOSS = config.max_daily_loss

# Session Configuration
HTTP_REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=config.total_timeout,
    connect=config.connect_timeout,
    sock_read=config.read_timeout
)

# Market Session Configuration
MARKET_SESSIONS = {
    "FOREX": {
        "hours": "24/5",
        "timezone": "Asia/Bangkok",
        "holidays": "US"
    },
    "XAU_USD": {
        "hours": "23:00-21:59",
        "timezone": "UTC",
        "holidays": []
    },
    "CRYPTO": {
        "hours": "24/7",
        "timezone": "UTC",
        "holidays": []
    }
}

# 1. Update INSTRUMENT_LEVERAGES based on Singapore MAS regulations and your full pair list
INSTRUMENT_LEVERAGES = {
    # Forex - major pairs
    "USD_CHF": 33.3, "EUR_USD": 50, "GBP_USD": 20,
    "USD_JPY": 20, "AUD_USD": 33.3, "USD_THB": 20,
    "CAD_CHF": 33.3, "NZD_USD": 33.3, "AUD_CAD": 33.3,
    # Additional forex pairs
    "AUD_JPY": 20, "USD_SGD": 20, "EUR_JPY": 20,
    "GBP_JPY": 20, "USD_CAD": 50, "NZD_JPY": 20,
    # Crypto - 2:1 leverage
    "BTC_USD": 2, "ETH_USD": 2, "XRP_USD": 2, "LTC_USD": 2, "BTCUSD": 2,
    # Gold - 10:1 leverage
    "XAU_USD": 10
    # Add more pairs from your forex list as needed
}

# TradingView Field Mapping
TV_FIELD_MAP = {
    'symbol': 'symbol',           # Your TradingView is sending 'symbol' directly
    'action': 'action',           # Your TradingView is sending 'action' directly  
    'timeframe': 'timeframe',     # Your TradingView is sending 'timeframe' directly
    'orderType': 'orderType',     # Your TradingView is sending 'orderType' directly
    'timeInForce': 'timeInForce', # Your TradingView is sending 'timeInForce' directly
    'percentage': 'percentage',   # Your TradingView is sending 'percentage' directly
    'account': 'account',         # Your TradingView is sending 'account' directly
    'id': 'id',                   # Not in your payload but keep it anyway
    'comment': 'comment'          # Your TradingView is sending 'comment' directly
}

# Error Mapping
ERROR_MAP = {
    "INSUFFICIENT_MARGIN": (True, "Insufficient margin", 400),
    "ACCOUNT_NOT_TRADEABLE": (True, "Account restricted", 403),
    "MARKET_HALTED": (False, "Market is halted", 503),
    "RATE_LIMIT": (True, "Rate limit exceeded", 429)
}

# Risk management settings for different timeframes
TIMEFRAME_TAKE_PROFIT_LEVELS = {
    "15M": {
        "first_exit": 0.5,  # 50% at 1:1
        "second_exit": 0.25,  # 25% at 2:1
        "runner": 0.25  # 25% with trailing
    },
    "1H": {
        "first_exit": 0.4,  # 40% at 1:1
        "second_exit": 0.3,  # 30% at 2:1
        "runner": 0.3  # 30% with trailing
    },
    "4H": {
        "first_exit": 0.33,  # 33% at 1:1
        "second_exit": 0.33,  # 33% at 2:1
        "runner": 0.34  # 34% with trailing
    },
    "1D": {
        "first_exit": 0.33,  # 33% at 1:1
        "second_exit": 0.33,  # 33% at 2:1
        "runner": 0.34  # 34% with trailing
    }
}

TIMEFRAME_TRAILING_SETTINGS = {
    "15M": {
        "initial_multiplier": 2.5,  # Tighter initial stop
        "profit_levels": [
            {"threshold": 2.0, "multiplier": 2.0},
            {"threshold": 3.0, "multiplier": 1.5}
        ]
    },
    "1H": {
        "initial_multiplier": 3.0,
        "profit_levels": [
            {"threshold": 2.5, "multiplier": 2.5},
            {"threshold": 4.0, "multiplier": 2.0}
        ]
    },
    "4H": {
        "initial_multiplier": 3.5,
        "profit_levels": [
            {"threshold": 3.0, "multiplier": 3.0},
            {"threshold": 5.0, "multiplier": 2.5}
        ]
    },
    "1D": {
        "initial_multiplier": 4.0,
        "profit_levels": [
            {"threshold": 3.5, "multiplier": 3.5},
            {"threshold": 6.0, "multiplier": 3.0}
        ]
    }
}

TIMEFRAME_TIME_STOPS = {
    "15M": {
        "optimal_duration": 4,  # hours
        "max_duration": 8,  # hours
        "stop_adjustment": 0.5  # tighten by 50% after max duration
    },
    "1H": {
        "optimal_duration": 8,  # hours
        "max_duration": 24,  # hours
        "stop_adjustment": 0.5
    },
    "4H": {
        "optimal_duration": 24,  # hours
        "max_duration": 72,  # hours
        "stop_adjustment": 0.5
    },
    "1D": {
        "optimal_duration": 72,  # hours
        "max_duration": 168,  # hours
        "stop_adjustment": 0.5
    }
}

##############################################################################
# Block 2: Models, Logging, and Session Management
##############################################################################

##############################################################################
# Logging Setup
##############################################################################

class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging"""
    def format(self, record):
        return json.dumps({
            "ts": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": getattr(record, 'request_id', None),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        })

def setup_logging():
    """Setup logging with improved error handling and rotation"""
    try:
        log_dir = '/opt/render/project/src/logs'
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'trading_bot.log')
    except Exception as e:
        log_file = 'trading_bot.log'
        logging.warning(f"Using default log file due to error: {str(e)}")

    formatter = JSONFormatter()
    
    # Configure file handler with proper encoding and rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Clear existing handlers
    root_logger = logging.getLogger()
    for hdlr in root_logger.handlers[:]:
        root_logger.removeHandler(hdlr)
    
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return logging.getLogger('trading_bot')

logger = setup_logging()

##############################################################################
# Models
##############################################################################

class AlertData(BaseModel):
    """Alert data model with improved validation"""
    symbol: str
    action: str
    timeframe: Optional[str] = "1M"
    orderType: Optional[str] = "MARKET"
    timeInForce: Optional[str] = "FOK"
    percentage: Optional[float] = 15.0
    account: Optional[str] = None
    id: Optional[str] = None
    comment: Optional[str] = None

    @validator('timeframe', pre=True, always=True)
    def validate_timeframe(cls, v):
        """Validate timeframe with improved error handling and None checking"""
        if v is None:
            return "15M"  # Default value if timeframe is None

        if not isinstance(v, str):
            v = str(v)

        # Handle TradingView-style timeframes
        if v.upper() in ["1D", "D", "DAILY"]:
            return "1440"  # Daily in minutes
        elif v.upper() in ["W", "1W", "WEEKLY"]:
            return "10080"  # Weekly in minutes
        elif v.upper() in ["MN", "1MN", "MONTHLY"]:
            return "43200"  # Monthly in minutes (30 days)

        if v.isdigit():
            mapping = {1: "1H", 4: "4H", 12: "12H", 5: "5M", 15: "15M", 30: "30M"}
            try:
                num = int(v)
                v = mapping.get(num, f"{v}M")
            except ValueError as e:
                raise ValueError("Invalid timeframe value") from e

        pattern = re.compile(r'^(\d+)([mMhH])$')
        match = pattern.match(v)
        if not match:
            # Handle case where v is just a number like "15"
            if v.isdigit():
                return f"{v}M"
            raise ValueError("Invalid timeframe format. Use '15M' or '1H' format")
        
        value, unit = match.groups()
        value = int(value)
        if unit.upper() == 'H':
            if value > 24:
                raise ValueError("Maximum timeframe is 24H")
            return str(value * 60)
        if unit.upper() == 'M':
            if value > 1440:
                raise ValueError("Maximum timeframe is 1440M (24H)")
            return str(value)
        raise ValueError("Invalid timeframe format")

    @validator('action')
    def validate_action(cls, v):
        """Validate action with strict checking"""
        valid_actions = ['BUY', 'SELL', 'CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']
        v = v.upper()
        if v not in valid_actions:
            raise ValueError(f"Action must be one of {valid_actions}")
        return v

    @validator('symbol')
    def validate_symbol(cls, v):
        """Validate symbol with improved checks"""
        if not v or len(v) < 3:  # Allow shorter symbols like "BTC" if needed
            raise ValueError("Symbol must be at least 3 characters")
        
        # Use the standardized format
        instrument = standardize_symbol(v)
        
        # Check if it's a cryptocurrency with special handling
        is_crypto = False
        for crypto in ["BTC", "ETH", "XRP", "LTC"]:
            if crypto in instrument.upper():
                is_crypto = True
                break
        
        # More lenient validation for crypto
        if is_crypto:
            return v  # Accept crypto symbols more liberally
        
        # Verify against available instruments
        if instrument not in INSTRUMENT_LEVERAGES:
            # More flexible crypto validation
            is_crypto = any(crypto in instrument for crypto in ["BTC", "ETH", "XRP", "LTC"])
            crypto_with_usd = ("USD" in instrument and is_crypto)
            
            if crypto_with_usd:
                # It's a cryptocurrency with USD, so it's valid
                pass
            else:
                # Try to check if there are any similarly formatted instruments before failing
                alternate_formats = [
                    instrument.replace("_", ""),
                    instrument[:3] + "_" + instrument[3:] if len(instrument) >= 6 else instrument
                ]
                
                if any(alt in INSTRUMENT_LEVERAGES for alt in alternate_formats):
                    # Found an alternate format that works
                    pass
                else:
                    # Unknown instrument - should properly report the error
                    raise ValueError(f"Invalid instrument: {instrument}")
        
        return v  # Return original value to maintain compatibility

    @validator('percentage')
    def validate_percentage(cls, v):
        """Validate percentage with proper bounds checking"""
        if v is None:
            return 1.0
        if not 0 < v <= 100:
            raise ValueError("Percentage must be between 0 and 100")
        return float(v)

    class Config:
        str_strip_whitespace = True
        validate_assignment = True
        extra = "forbid"

##############################################################################
# Session Management
##############################################################################

_session: Optional[aiohttp.ClientSession] = None

async def get_session() -> aiohttp.ClientSession:
    """Get or create an HTTP session"""
    # In a real implementation, this would maintain a singleton session
    # But for simplicity in this example, we'll create a new one each time
    
    # Set up authentication headers
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer YOUR_OANDA_API_KEY"  # Replace with actual API key mechanism
    }
    
    # Create a ClientSession with the headers
    session = aiohttp.ClientSession(headers=headers)
    
    return session
    
async def cleanup_stale_sessions():
    """Cleanup stale sessions"""
    try:
        if _session and not _session.closed:
            await _session.close()
    except Exception as e:
        logger.error(f"Error cleaning up sessions: {str(e)}")

##############################################################################
# Market Utilities
##############################################################################

def standardize_symbol(symbol: str) -> str:
    """Standardize trading symbols to a consistent format"""
    # Remove any whitespace and convert to uppercase
    symbol = symbol.strip().upper()
    
    # Convert TradingView style symbols to OANDA format
    if '_' not in symbol and '/' in symbol:
        # Convert "EUR/USD" format to "EUR_USD"
        symbol = symbol.replace('/', '_')
    
    # Handle some common symbol variations
    if symbol == 'XAUUSD':
        return 'XAU_USD'
    elif symbol == 'XAGUSD':
        return 'XAG_USD'
    elif symbol == 'BTCUSD':
        return 'BTC_USD'
    
    # Check if it's a forex pair without proper formatting
    forex_currencies = ['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'NZD', 'CAD', 'CHF']
    if '_' not in symbol and len(symbol) == 6:
        for i in range(3, 6):
            base = symbol[:i]
            quote = symbol[i:]
            if base in forex_currencies and quote in forex_currencies:
                return f"{base}_{quote}"
    
    return symbol

@handle_sync_errors
def check_market_hours(session_config: dict) -> bool:
    """Check market hours with handling for ranges crossing midnight and 24/7 mode."""
    try:
        # If the exchange trades 24/7, bypass time restrictions.
        if config.trade_24_7:
            return True
        
        tz = timezone(session_config['timezone'])
        now = datetime.now(tz)
        
        # Check holidays
        if session_config['holidays']:
            holiday_cal = getattr(holidays, session_config['holidays'])()
            if now.date() in holiday_cal:
                return False
        
        # Special cases for continuous trading sessions (for non-24/7 mode)
        if "24/7" in session_config['hours']:
            return True
        if "24/5" in session_config['hours']:
            return now.weekday() < 5
        
        time_ranges = session_config['hours'].split('|')
        for time_range in time_ranges:
            start_str, end_str = time_range.split('-')
            start = datetime.strptime(start_str, "%H:%M").time()
            end = datetime.strptime(end_str, "%H:%M").time()
            
            if start <= end:
                if start <= now.time() <= end:
                    return True
            else:
                # For ranges crossing midnight.
                if now.time() >= start or now.time() <= end:
                    return True
        return False
    except Exception as e:
        logger.error(f"Error checking market hours: {str(e)}")
        raise

def is_instrument_tradeable(instrument: str) -> Tuple[bool, str]:
    """Check if an instrument is tradeable based on market hours and conditions"""
    try:
        # Get current time in UTC
        current_time = datetime.utcnow()
        current_day = current_time.weekday()  # Monday is 0, Sunday is 6
        current_hour = current_time.hour
        
        # Check if it's weekend (forex markets closed)
        if current_day >= 5:  # Saturday or Sunday
            return False, "Weekend - forex markets are closed"
            
        # Handle forex specific rules
        if "JPY" in instrument or any(x in instrument for x in ["USD", "EUR", "GBP", "AUD", "NZD", "CAD", "CHF"]):
            # Check for forex market hours
            # Forex markets are closed from Friday 22:00 UTC to Sunday 22:00 UTC
            if current_day == 4 and current_hour >= 22:  # Friday after 22:00
                return False, "Forex markets closed for the weekend"
            if current_day == 6 and current_hour < 22:  # Sunday before 22:00
                return False, "Forex markets closed for the weekend"
                
            # Check for low liquidity periods
            if 22 <= current_hour or current_hour <= 1:
                return True, "Forex market open (low liquidity period)"
                
            return True, "Forex market open"
            
        # Handle stock market hours (assuming US stocks)
        if "_" in instrument and any(x in instrument for x in ["US", "NYSE", "NASDAQ"]):
            # US stock markets are open 9:30 AM to 4:00 PM Eastern Time
            # Convert to UTC (Eastern Time is UTC-5 or UTC-4 during daylight saving)
            # For simplicity, we'll use a rough approximation
            if current_day >= 0 and current_day <= 4:  # Monday to Friday
                if 14 <= current_hour < 21:  # 9:30 AM to 4:00 PM ET is roughly 14:30 to 21:00 UTC
                    return True, "Stock market open"
            return False, "Stock market closed"
            
        # For other instruments (commodities, etc.), check specific rules
        # For simplicity, we'll just return true
        return True, "Market is open"
        
    except Exception as e:
        logger.error(f"Error checking if instrument is tradeable: {str(e)}")
        # Default to closed when there's an error, to be safe
        return False, f"Error checking market hours: {str(e)}"

async def get_atr(instrument: str, timeframe: str) -> float:
    """Get ATR value with timeframe normalization"""
    # Normalize the timeframe format
    normalized_timeframe = ensure_proper_timeframe(timeframe)
    logger.debug(f"ATR calculation: Normalized timeframe from {timeframe} to {normalized_timeframe}")
    
    instrument_type = get_instrument_type(instrument)
    
    # Default ATR values by timeframe and instrument type
    default_atr_values = {
        "FOREX": {
            "15M": 0.0010,  # 10 pips
            "1H": 0.0025,   # 25 pips
            "4H": 0.0050,   # 50 pips
            "D": 0.0100     # 100 pips
        },
        "STOCK": {
            "15M": 0.01,    # 1% for stocks
            "1H": 0.02,     # 2% for stocks
            "4H": 0.03,     # 3% for stocks
            "D": 0.05       # 5% for stocks
        },
        "COMMODITY": {
            "15M": 0.05,    # 0.05% for commodities
            "1H": 0.10,     # 0.1% for commodities
            "4H": 0.20,     # 0.2% for commodities
            "D": 0.50       # 0.5% for commodities
        }
    }
    
    # Get the ATR value for this instrument and timeframe
    return default_atr_values[instrument_type].get(normalized_timeframe, default_atr_values[instrument_type]["1H"])

def get_atr_multiplier(instrument_type: str, timeframe: str) -> float:
    """Get the appropriate ATR multiplier based on instrument type and timeframe"""
    # Normalize the timeframe format
    normalized_timeframe = ensure_proper_timeframe(timeframe)
    logger.debug(f"ATR multiplier: Normalized timeframe from {timeframe} to {normalized_timeframe}")
    
    # Default multipliers by instrument type and timeframe
    default_multipliers = {
        "FOREX": {
            "15M": 1.5,
            "1H": 2.0,
            "4H": 2.5,
            "D": 3.0
        },
        "STOCK": {
            "15M": 2.0,
            "1H": 2.5,
            "4H": 3.0,
            "D": 3.5
        },
        "COMMODITY": {
            "15M": 1.8,
            "1H": 2.2,
            "4H": 2.7,
            "D": 3.2
        }
    }
    
    # Return the appropriate multiplier or default to 1H if timeframe not found
    return default_multipliers[instrument_type].get(normalized_timeframe, default_multipliers[instrument_type]["1H"])
    
async def get_current_price(instrument: str, action: str) -> float:
    """Get the current price for an instrument"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Getting current price for {instrument}, action: {action}")
    
    try:
        # Get session and API URL
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{config.oanda_account}/pricing"
        
        # Set up the query parameters
        params = {
            "instruments": instrument,
            "includeUnitsAvailable": "true"
        }
        
        # Make the API request with retries
        retries = 0
        while retries < config.max_retries:
            try:
                async with session.get(url, params=params, timeout=HTTP_REQUEST_TIMEOUT) as response:
                    if response.status == 200:
                        response_data = await response.json()
                        prices = response_data.get("prices", [])
                        
                        if not prices:
                            logger.warning(f"[{request_id}] No pricing data found for {instrument}")
                            raise ValueError(f"No pricing data found for {instrument}")
                            
                        price_data = prices[0]
                        
                        # Get the appropriate price based on the action
                        if action.upper() == "BUY":
                            # For buying, use the ask price
                            current_price = float(price_data.get("asks", [{}])[0].get("price", 0))
                        else:
                            # For selling, use the bid price
                            current_price = float(price_data.get("bids", [{}])[0].get("price", 0))
                            
                        logger.info(f"[{request_id}] Retrieved {action} price for {instrument}: {current_price}")
                        return current_price
                    
                    # Handle error responses
                    response_text = await response.text()
                    logger.error(f"[{request_id}] Error getting price: {response.status}, Response: {response_text}")
                    
                    if "RATE_LIMIT" in response_text:
                        await asyncio.sleep(60)  # Longer wait for rate limits
                    else:
                        delay = config.base_delay * (2 ** retries)
                        await asyncio.sleep(delay)
                    
                    retries += 1
                    
            except aiohttp.ClientError as e:
                logger.error(f"[{request_id}] Network error getting price: {str(e)}")
                if retries < config.max_retries - 1:
                    await asyncio.sleep(config.base_delay * (2 ** retries))
                    retries += 1
                    continue
                raise
        
        # If we've exhausted all retries without success
        raise ValueError(f"Failed to get price for {instrument} after {config.max_retries} attempts")
        
    except Exception as e:
        logger.error(f"[{request_id}] Error getting current price: {str(e)}", exc_info=True)
        # Return a placeholder value in case of error
        return 1.1234  # Placeholder value

def get_current_market_session(current_time: datetime) -> str:
    """Get current market session based on time"""
    hour = current_time.hour
    weekday = current_time.weekday()
    
    # Define sessions based on time
    if weekday >= 5:  # Weekend
        return "WEEKEND"
    elif 0 <= hour < 8:  # Asian session
        return "ASIAN"
    elif 8 <= hour < 12:  # Asian-European overlap
        return "ASIAN_EUROPEAN_OVERLAP"
    elif 12 <= hour < 16:  # European session
        return "EUROPEAN"
    elif 16 <= hour < 20:  # European-American overlap
        return "EUROPEAN_AMERICAN_OVERLAP"
    else:  # American session
        return "AMERICAN"

##############################################################################
# Risk Management Classes
##############################################################################

class VolatilityMonitor:
    def __init__(self):
        self.volatility_history = {}
        self.volatility_thresholds = {
            "15M": {"std_dev": 2.0, "lookback": 20},
            "1H": {"std_dev": 2.5, "lookback": 24},
            "4H": {"std_dev": 3.0, "lookback": 30},
            "1D": {"std_dev": 3.5, "lookback": 20}
        }
        self.market_conditions = {}
        
    async def initialize_market_condition(self, symbol: str, timeframe: str):
        """Initialize market condition tracking for a symbol"""
        if symbol not in self.market_conditions:
            self.market_conditions[symbol] = {
                'timeframe': timeframe,
                'volatility_state': 'normal',
                'last_update': datetime.now(timezone('Asia/Bangkok')),
                'volatility_ratio': 1.0
            }
            
    async def update_volatility(self, symbol: str, current_atr: float, timeframe: str):
        """Update volatility history and calculate current state"""
        if symbol not in self.volatility_history:
            self.volatility_history[symbol] = []
            
        settings = self.volatility_thresholds.get(timeframe, self.volatility_thresholds["1H"])
        self.volatility_history[symbol].append(current_atr)
        
        # Maintain lookback period
        if len(self.volatility_history[symbol]) > settings['lookback']:
            self.volatility_history[symbol].pop(0)
            
        # Calculate volatility metrics
        if len(self.volatility_history[symbol]) >= settings['lookback']:
            mean_atr = sum(self.volatility_history[symbol]) / len(self.volatility_history[symbol])
            std_dev = statistics.stdev(self.volatility_history[symbol])
            current_ratio = current_atr / mean_atr
            
            # Update market condition
            self.market_conditions[symbol] = self.market_conditions.get(symbol, {})
            self.market_conditions[symbol]['volatility_ratio'] = current_ratio
            self.market_conditions[symbol]['last_update'] = datetime.now(timezone('Asia/Bangkok'))
            
            if current_atr > (mean_atr + settings['std_dev'] * std_dev):
                self.market_conditions[symbol]['volatility_state'] = 'high'
            elif current_atr < (mean_atr - settings['std_dev'] * std_dev):
                self.market_conditions[symbol]['volatility_state'] = 'low'
            else:
                self.market_conditions[symbol]['volatility_state'] = 'normal'
                
    async def get_market_condition(self, symbol: str) -> Dict[str, Any]:
        """Get current market condition for a symbol"""
        return self.market_conditions.get(symbol, {
            'volatility_state': 'unknown',
            'volatility_ratio': 1.0
        })
        
    async def should_adjust_risk(self, symbol: str, timeframe: str) -> Tuple[bool, float]:
        """Determine if risk parameters should be adjusted based on volatility"""
        condition = await self.get_market_condition(symbol)
        
        if condition['volatility_state'] == 'high':
            return True, 0.75  # Reduce risk by 25%
        elif condition['volatility_state'] == 'low':
            return True, 1.25  # Increase risk by 25%
        return False, 1.0

class LorentzianDistanceClassifier:
    def __init__(self, lookback_period: int = 20):
        self.lookback_period = lookback_period
        self.price_history = {}
        self.regime_history = {}
        self.volatility_history = {}
        self.atr_history = {}
        
    async def calculate_lorentzian_distance(self, price: float, history: List[float]) -> float:
        """Calculate true Lorentzian distance using logarithmic scaling"""
        if not history:
            return 0.0
            
        distances = []
        for hist_price in history:
            # Proper Lorentzian distance formula with log scaling
            distance = np.log(1 + abs(price - hist_price))
            distances.append(distance)
            
        return float(np.mean(distances))
        
    async def classify_market_regime(self, symbol: str, current_price: float, atr: float = None) -> Dict[str, Any]:
        """Classify current market regime using multiple factors"""
        if symbol not in self.price_history:
            self.price_history[symbol] = []
            self.regime_history[symbol] = []
            self.volatility_history[symbol] = []
            self.atr_history[symbol] = []
            
        # Update price history
        self.price_history[symbol].append(current_price)
        if len(self.price_history[symbol]) > self.lookback_period:
            self.price_history[symbol].pop(0)
            
        # Need at least 2 prices for calculation
        if len(self.price_history[symbol]) < 2:
            return {"regime": "UNKNOWN", "volatility": 0.0, "momentum": 0.0, "price_distance": 0.0}
            
        # Calculate price-based metrics
        price_distance = await self.calculate_lorentzian_distance(
            current_price, self.price_history[symbol][:-1]  # Compare current to history
        )
        
        # Calculate returns and volatility
        returns = [self.price_history[symbol][i] / self.price_history[symbol][i-1] - 1 
                  for i in range(1, len(self.price_history[symbol]))]
        volatility = statistics.stdev(returns) if len(returns) > 1 else 0.0
        
        # Calculate momentum (percentage change over lookback period)
        momentum = (current_price - self.price_history[symbol][0]) / self.price_history[symbol][0] if self.price_history[symbol][0] != 0 else 0.0
        
        # Update ATR history if provided
        if atr is not None:
            self.atr_history[symbol].append(atr)
            if len(self.atr_history[symbol]) > self.lookback_period:
                self.atr_history[symbol].pop(0)
        
        # Multi-factor regime classification
        regime = "UNKNOWN"
        
        # Use both price distance and volatility for classification
        if price_distance < 0.1 and volatility < 0.001:
            regime = "RANGING"
        elif price_distance > 0.3 and abs(momentum) > 0.002:
            regime = "TRENDING"
        elif volatility > 0.003 or (atr is not None and atr > 1.5 * np.mean(self.atr_history[symbol]) if self.atr_history[symbol] else 0):
            regime = "VOLATILE"
        elif abs(momentum) > 0.003:
            regime = "MOMENTUM"
        else:
            regime = "NEUTRAL"
            
        # Update regime and volatility history
        self.regime_history[symbol].append(regime)
        self.volatility_history[symbol].append(volatility)
        
        if len(self.regime_history[symbol]) > self.lookback_period:
            self.regime_history[symbol].pop(0)
            self.volatility_history[symbol].pop(0)
            
        return {
            "regime": regime,
            "volatility": volatility,
            "momentum": momentum,
            "price_distance": price_distance,
            "is_high_volatility": volatility > 0.002,
            "atr": atr
        }
        
    async def get_regime_history(self, symbol: str) -> Dict[str, List[Any]]:
        """Get historical regime data for a symbol"""
        return {
            "regimes": self.regime_history.get(symbol, []),
            "volatility": self.volatility_history.get(symbol, []),
            "atr": self.atr_history.get(symbol, []),
            "dominant_regime": self.get_dominant_regime(symbol)
        }
    
    def get_dominant_regime(self, symbol: str) -> str:
        """Get the dominant regime over recent history (last 5 periods)"""
        if symbol not in self.regime_history or len(self.regime_history[symbol]) < 3:
            return "UNKNOWN"
            
        recent_regimes = self.regime_history[symbol][-5:]
        regime_counts = {}
        
        for regime in recent_regimes:
            regime_counts[regime] = regime_counts.get(regime, 0) + 1
            
        # Find most common regime
        dominant_regime = max(regime_counts.items(), key=lambda x: x[1])
        # Only consider it dominant if it appears more than 60% of the time
        if dominant_regime[1] / len(recent_regimes) >= 0.6:
            return dominant_regime[0]
        else:
            return "MIXED"
        
    async def should_adjust_exits(self, symbol: str, current_regime: str = None) -> Tuple[bool, Dict[str, float]]:
        """Determine if exit levels should be adjusted based on regime stability and type"""
        # Get current regime if not provided
        if current_regime is None:
            if symbol not in self.regime_history or not self.regime_history[symbol]:
                return False, {"stop_loss": 1.0, "take_profit": 1.0, "trailing_stop": 1.0}
            current_regime = self.regime_history[symbol][-1]
        
        # Check regime stability (from the second implementation)
        recent_regimes = self.regime_history.get(symbol, [])[-3:]
        is_stable = len(recent_regimes) >= 3 and len(set(recent_regimes)) == 1  # All 3 regimes are the same
        
        # Set specific adjustments based on regime (from the first implementation)
        adjustments = {
            "stop_loss": 1.0,
            "take_profit": 1.0,
            "trailing_stop": 1.0
        }
        
        if is_stable:
            if current_regime == "VOLATILE":
                adjustments["stop_loss"] = 1.5      # Wider stop loss in volatile markets
                adjustments["take_profit"] = 2.0    # More ambitious take profit
                adjustments["trailing_stop"] = 1.25  # Wider trailing stop
            elif current_regime == "TRENDING":
                adjustments["stop_loss"] = 1.25     # Slightly wider stop
                adjustments["take_profit"] = 1.5    # More room to run
                adjustments["trailing_stop"] = 1.1   # Slightly wider trailing stop
            elif current_regime == "RANGING":
                adjustments["stop_loss"] = 0.8      # Tighter stop loss
                adjustments["take_profit"] = 0.8    # Tighter take profit
                adjustments["trailing_stop"] = 0.9   # Tighter trailing stop
            elif current_regime == "MOMENTUM":
                adjustments["stop_loss"] = 1.2      # Slightly wider stop
                adjustments["take_profit"] = 1.7    # More ambitious take profit
                adjustments["trailing_stop"] = 1.3   # Wider trailing to catch momentum
        
        should_adjust = is_stable and any(v != 1.0 for v in adjustments.values())
        return should_adjust, adjustments
    
    async def clear_history(self, symbol: str):
        """Clear historical data for a symbol"""
        if symbol in self.price_history:
            del self.price_history[symbol]
        if symbol in self.regime_history:
            del self.regime_history[symbol]
        if symbol in self.volatility_history:
            del self.volatility_history[symbol]
        if symbol in self.atr_history:
            del self.atr_history[symbol]

class DynamicExitManager:
    def __init__(self):
        self.ldc = LorentzianDistanceClassifier()
        self.exit_levels = {}
        self.initial_stops = {}
        
    async def initialize_exits(self, symbol: str, entry_price: float, position_type: str, 
                             initial_stop: float, initial_tp: float):
        """Initialize exit levels for a position"""
        self.exit_levels[symbol] = {
            "entry_price": entry_price,
            "position_type": position_type,
            "initial_stop": initial_stop,
            "initial_tp": initial_tp,
            "current_stop": initial_stop,
            "current_tp": initial_tp,
            "trailing_stop": None,
            "exit_levels_hit": []
        }
        
    async def update_exits(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """Update exit levels based on market regime and price action"""
        if symbol not in self.exit_levels:
            return {}
            
        position_data = self.exit_levels[symbol]
        
        # Get current market regime
        regime_data = await self.ldc.classify_market_regime(symbol, current_price)
        should_adjust, adjustments = await self.ldc.should_adjust_exits(symbol, regime_data["regime"])
        
        if not should_adjust:
            return {}
            
        # Calculate new exit levels
        new_levels = {}
        if position_data["position_type"] == "LONG":
            new_stop = current_price - (abs(current_price - position_data["initial_stop"]) * adjustments["stop_loss"])
            new_tp = current_price + (abs(position_data["initial_tp"] - current_price) * adjustments["take_profit"])
            
            if position_data["trailing_stop"] is None:
                new_trailing = current_price - (abs(current_price - position_data["initial_stop"]) * adjustments["trailing_stop"])
            else:
                new_trailing = max(
                    position_data["trailing_stop"],
                    current_price - (abs(current_price - position_data["initial_stop"]) * adjustments["trailing_stop"])
                )
        else:  # SHORT
            new_stop = current_price + (abs(position_data["initial_stop"] - current_price) * adjustments["stop_loss"])
            new_tp = current_price - (abs(current_price - position_data["initial_tp"]) * adjustments["take_profit"])
            
            if position_data["trailing_stop"] is None:
                new_trailing = current_price + (abs(position_data["initial_stop"] - current_price) * adjustments["trailing_stop"])
            else:
                new_trailing = min(
                    position_data["trailing_stop"],
                    current_price + (abs(position_data["initial_stop"] - current_price) * adjustments["trailing_stop"])
                )
                
        # Update levels
        position_data["current_stop"] = new_stop
        position_data["current_tp"] = new_tp
        position_data["trailing_stop"] = new_trailing
        
        return {
            "stop_loss": new_stop,
            "take_profit": new_tp,
            "trailing_stop": new_trailing,
            "regime": regime_data["regime"],
            "volatility": regime_data["volatility"]
        }
        
    async def check_exits(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """Check if any exit conditions are met"""
        if symbol not in self.exit_levels:
            return {}
            
        position_data = self.exit_levels[symbol]
        actions = {}
        
        # Check stop loss
        if position_data["position_type"] == "LONG":
            if current_price <= position_data["current_stop"]:
                actions["stop_loss"] = True
        else:  # SHORT
            if current_price >= position_data["current_stop"]:
                actions["stop_loss"] = True
                
        # Check take profit
        if position_data["position_type"] == "LONG":
            if current_price >= position_data["current_tp"]:
                actions["take_profit"] = True
        else:  # SHORT
            if current_price <= position_data["current_tp"]:
                actions["take_profit"] = True
                
        # Check trailing stop
        if position_data["trailing_stop"] is not None:
            if position_data["position_type"] == "LONG":
                if current_price <= position_data["trailing_stop"]:
                    actions["trailing_stop"] = True
            else:  # SHORT
                if current_price >= position_data["trailing_stop"]:
                    actions["trailing_stop"] = True
                    
        return actions
        
    async def clear_exits(self, symbol: str):
        """Clear exit levels for a symbol"""
        if symbol in self.exit_levels:
            del self.exit_levels[symbol]

class AdvancedLossManager:
    def __init__(self):
        self.positions = {}
        self.daily_pnl = 0.0
        self.max_daily_loss = 0.50  # 20% max daily loss
        self.max_drawdown = 0.20    # 15% max drawdown
        self.peak_balance = 0.0
        self.current_balance = 0.0
        self.position_limits = {}
        self.correlation_matrix = {}
        
    async def initialize_position(self, symbol: str, entry_price: float, position_type: str, 
                                units: float, account_balance: float):
        """Initialize position tracking with loss limits"""
        self.positions[symbol] = {
            "entry_price": entry_price,
            "position_type": position_type,
            "units": units,
            "current_units": units,
            "entry_time": datetime.now(timezone('Asia/Bangkok')),
            "max_loss": self._calculate_position_max_loss(entry_price, units, account_balance),
            "current_loss": 0.0,
            "correlation_factor": 1.0
        }
        
        # Update peak balance if needed
        if account_balance > self.peak_balance:
            self.peak_balance = account_balance
            
        self.current_balance = account_balance
        
    def _calculate_position_max_loss(self, entry_price: float, units: float, account_balance: float) -> float:
        """Calculate maximum loss for a position based on risk parameters"""
        position_value = abs(entry_price * units)
        risk_percentage = min(0.02, position_value / account_balance)  # Max 2% risk per position
        return position_value * risk_percentage
        
    async def update_position_loss(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """Update position loss and check limits"""
        if symbol not in self.positions:
            return {}
            
        position = self.positions[symbol]
        entry_price = position["entry_price"]
        units = position["current_units"]
        
        # Calculate current loss
        if position["position_type"] == "LONG":
            current_loss = (entry_price - current_price) * units
        else:  # SHORT
            current_loss = (current_price - entry_price) * units
            
        position["current_loss"] = current_loss
        
        # Check various loss limits
        actions = {}
        
        # Check position-specific loss limit
        if abs(current_loss) > position["max_loss"]:
            actions["position_limit"] = True
            
        # Check daily loss limit
        daily_loss_percentage = abs(self.daily_pnl) / self.peak_balance
        if daily_loss_percentage > self.max_daily_loss:
            actions["daily_limit"] = True
            
        # Check drawdown limit
        drawdown = (self.peak_balance - self.current_balance) / self.peak_balance
        if drawdown > self.max_drawdown:
            actions["drawdown_limit"] = True
            
        return actions
        
    async def update_correlation_matrix(self, symbol: str, other_positions: Dict[str, Dict[str, Any]]):
        """Update correlation matrix for position sizing"""
        if symbol not in self.correlation_matrix:
            self.correlation_matrix[symbol] = {}
            
        for other_symbol, other_pos in other_positions.items():
            if other_symbol == symbol:
                continue
                
            # Calculate correlation based on position types and currencies
            correlation = self._calculate_correlation(
                symbol, other_symbol, 
                self.positions[symbol]["position_type"],
                other_pos["position_type"]
            )
            
            self.correlation_matrix[symbol][other_symbol] = correlation
            
    def _calculate_correlation(self, symbol1: str, symbol2: str, 
                             type1: str, type2: str) -> float:
        """Calculate correlation between two positions"""
        # Extract base and quote currencies
        base1, quote1 = symbol1.split('_')
        base2, quote2 = symbol2.split('_')
        
        # Check for currency overlap
        if base1 == base2 or quote1 == quote2:
            # Same base or quote currency indicates correlation
            if type1 == type2:
                return 0.8  # Strong positive correlation
            else:
                return -0.8  # Strong negative correlation
                
        # Check for cross-currency correlation
        if base1 == quote2 or quote1 == base2:
            if type1 == type2:
                return 0.6  # Moderate positive correlation
            else:
                return -0.6  # Moderate negative correlation
                
        return 0.0  # No significant correlation
        
    async def get_position_correlation_factor(self, symbol: str) -> float:
        """Get correlation factor for position sizing"""
        if symbol not in self.correlation_matrix:
            return 1.0
            
        # Calculate average correlation with other positions
        correlations = list(self.correlation_matrix[symbol].values())
        if not correlations:
            return 1.0
            
        avg_correlation = sum(abs(c) for c in correlations) / len(correlations)
        return max(0.5, 1.0 - avg_correlation)  # Minimum factor of 0.5
        
    async def update_daily_pnl(self, pnl: float):
        """Update daily P&L and check limits"""
        self.daily_pnl += pnl
        self.current_balance += pnl
        
        # Update peak balance if needed
        if self.current_balance > self.peak_balance:
            self.peak_balance = self.current_balance
            
    async def should_reduce_risk(self) -> Tuple[bool, float]:
        """Determine if risk should be reduced based on current conditions"""
        daily_loss_percentage = abs(self.daily_pnl) / self.peak_balance
        drawdown = (self.peak_balance - self.current_balance) / self.peak_balance
        
        if daily_loss_percentage > self.max_daily_loss * 0.75:  # At 75% of max daily loss
            return True, 0.75  # Reduce risk by 25%
        elif drawdown > self.max_drawdown * 0.75:  # At 75% of max drawdown
            return True, 0.75  # Reduce risk by 25%
            
        return False, 1.0
        
    async def clear_position(self, symbol: str):
        """Clear position from loss management"""
        if symbol in self.positions:
            del self.positions[symbol]
        if symbol in self.correlation_matrix:
            del self.correlation_matrix[symbol]
            
    async def get_position_risk_metrics(self, symbol: str) -> Dict[str, Any]:
        """Get comprehensive risk metrics for a position"""
        if symbol not in self.positions:
            return {}
            
        position = self.positions[symbol]
        correlation_factor = await self.get_position_correlation_factor(symbol)
        
        return {
            "current_loss": position["current_loss"],
            "max_loss": position["max_loss"],
            "correlation_factor": correlation_factor,
            "daily_pnl": self.daily_pnl,
            "drawdown": (self.peak_balance - self.current_balance) / self.peak_balance
         }   

class RiskAnalytics:
    def __init__(self):
        self.positions = {}
        self.price_history = {}
        self.returns_history = {}
        self.var_history = {}
        self.es_history = {}
        self.portfolio_metrics = {}
        
    async def initialize_position(self, symbol: str, entry_price: float, units: float):
        """Initialize position tracking for risk analytics"""
        self.positions[symbol] = {
            "entry_price": entry_price,
            "units": units,
            "current_price": entry_price,
            "entry_time": datetime.now(timezone('Asia/Bangkok'))
        }
        
        if symbol not in self.price_history:
            self.price_history[symbol] = []
            self.returns_history[symbol] = []
            
    async def update_position(self, symbol: str, current_price: float):
        """Update position data and calculate metrics"""
        if symbol not in self.positions:
            return
            
        position = self.positions[symbol]
        position["current_price"] = current_price
        
        # Update price history
        self.price_history[symbol].append(current_price)
        if len(self.price_history[symbol]) > 100:  # Keep last 100 prices
            self.price_history[symbol].pop(0)
            
        # Calculate returns
        if len(self.price_history[symbol]) > 1:
            returns = (current_price - self.price_history[symbol][-2]) / self.price_history[symbol][-2]
            self.returns_history[symbol].append(returns)
            if len(self.returns_history[symbol]) > 100:
                self.returns_history[symbol].pop(0)
                
        # Calculate risk metrics
        await self._calculate_risk_metrics(symbol)
        
    async def _calculate_risk_metrics(self, symbol: str):
        """Calculate Value at Risk and Expected Shortfall"""
        if symbol not in self.returns_history or len(self.returns_history[symbol]) < 20:
            return
            
        returns = self.returns_history[symbol]
        position = self.positions[symbol]
        
        # Calculate VaR (95% confidence level)
        var_95 = np.percentile(returns, 5)  # 5th percentile for 95% VaR
        
        # Calculate Expected Shortfall (95% confidence level)
        es_95 = np.mean([r for r in returns if r <= var_95])
        
        # Store metrics
        self.var_history[symbol] = {
            "var_95": var_95,
            "timestamp": datetime.now(timezone('Asia/Bangkok'))
        }
        
        self.es_history[symbol] = {
            "es_95": es_95,
            "timestamp": datetime.now(timezone('Asia/Bangkok'))
        }
        
        # Calculate position-specific metrics
        position_value = position["units"] * position["current_price"]
        position_var = position_value * abs(var_95)
        position_es = position_value * abs(es_95)
        
        # Update portfolio metrics
        if symbol not in self.portfolio_metrics:
            self.portfolio_metrics[symbol] = {}
            
        self.portfolio_metrics[symbol].update({
            "position_value": position_value,
            "var_95": position_var,
            "es_95": position_es,
            "volatility": statistics.stdev(returns) if len(returns) > 1 else 0.0,
            "sharpe_ratio": self._calculate_sharpe_ratio(returns),
            "sortino_ratio": self._calculate_sortino_ratio(returns)
        })
        
    def _calculate_sharpe_ratio(self, returns: List[float]) -> float:
        """Calculate Sharpe ratio for returns"""
        if not returns:
            return 0.0
            
        risk_free_rate = 0.02  # 2% annual risk-free rate
        daily_rf = (1 + risk_free_rate) ** (1/252) - 1  # Convert to daily
        
        excess_returns = [r - daily_rf for r in returns]
        if not excess_returns:
            return 0.0
            
        avg_excess_return = statistics.mean(excess_returns)
        std_dev = statistics.stdev(excess_returns) if len(excess_returns) > 1 else 0.0
        
        if std_dev == 0:
            return 0.0
            
        return (avg_excess_return * 252) / (std_dev * np.sqrt(252))
        
    def _calculate_sortino_ratio(self, returns: List[float]) -> float:
        """Calculate Sortino ratio for returns"""
        if not returns:
            return 0.0
            
        risk_free_rate = 0.02  # 2% annual risk-free rate
        daily_rf = (1 + risk_free_rate) ** (1/252) - 1  # Convert to daily
        
        excess_returns = [r - daily_rf for r in returns]
        if not excess_returns:
            return 0.0
            
        avg_excess_return = statistics.mean(excess_returns)
        downside_returns = [r for r in excess_returns if r < 0]
        
        if not downside_returns:
            return float('inf')
            
        downside_std = statistics.stdev(downside_returns) if len(downside_returns) > 1 else 0.0
        
        if downside_std == 0:
            return float('inf')
            
        return (avg_excess_return * 252) / (downside_std * np.sqrt(252))
        
    async def get_position_risk_metrics(self, symbol: str) -> Dict[str, Any]:
        """Get comprehensive risk metrics for a position"""
        if symbol not in self.portfolio_metrics:
            return {}
            
        return self.portfolio_metrics[symbol]
        
    async def get_portfolio_risk_metrics(self) -> Dict[str, Any]:
        """Get portfolio-level risk metrics"""
        if not self.portfolio_metrics:
            return {}
            
        total_value = sum(m["position_value"] for m in self.portfolio_metrics.values())
        total_var = sum(m["var_95"] for m in self.portfolio_metrics.values())
        total_es = sum(m["es_95"] for m in self.portfolio_metrics.values())
        
        # Calculate portfolio volatility
        portfolio_returns = []
        for symbol in self.returns_history:
            portfolio_returns.extend(self.returns_history[symbol])
            
        portfolio_volatility = statistics.stdev(portfolio_returns) if portfolio_returns else 0.0
        
        return {
            "total_value": total_value,
            "total_var_95": total_var,
            "total_es_95": total_es,
            "portfolio_volatility": portfolio_volatility,
            "position_count": len(self.positions),
            "timestamp": datetime.now(timezone('Asia/Bangkok')).isoformat()
        }
        
    async def clear_position(self, symbol: str):
        """Clear position from risk analytics"""
        if symbol in self.positions:
            del self.positions[symbol]
        if symbol in self.price_history:
            del self.price_history[symbol]
        if symbol in self.returns_history:
            del self.returns_history[symbol]
        if symbol in self.var_history:
            del self.var_history[symbol]
        if symbol in self.es_history:
            del self.es_history[symbol]
        if symbol in self.portfolio_metrics:
            del self.portfolio_metrics[symbol]
            
class MarketStructureAnalyzer:
    def __init__(self):
        self.support_levels = {}
        self.resistance_levels = {}
        self.swing_points = {}
        
    async def analyze_market_structure(self, symbol: str, timeframe: str, 
                                     high: float, low: float, close: float) -> Dict[str, Any]:
        """Analyze market structure for better stop loss placement"""
        if symbol not in self.support_levels:
            self.support_levels[symbol] = []
        if symbol not in self.resistance_levels:
            self.resistance_levels[symbol] = []
        if symbol not in self.swing_points:
            self.swing_points[symbol] = []
            
        # Update swing points
        self._update_swing_points(symbol, high, low)
        
        # Identify support and resistance levels
        self._identify_levels(symbol)
        
        # Get nearest levels for stop loss calculation
        nearest_support = self._get_nearest_support(symbol, close)
        nearest_resistance = self._get_nearest_resistance(symbol, close)
        
        return {
            'nearest_support': nearest_support,
            'nearest_resistance': nearest_resistance,
            'swing_points': self.swing_points[symbol][-5:] if len(self.swing_points[symbol]) >= 5 else self.swing_points[symbol],
            'support_levels': self.support_levels[symbol],
            'resistance_levels': self.resistance_levels[symbol]
        }
        
    def _update_swing_points(self, symbol: str, high: float, low: float):
        """Update swing high and low points"""
        if symbol not in self.swing_points:
            self.swing_points[symbol] = []
            
        if len(self.swing_points[symbol]) < 2:
            self.swing_points[symbol].append({'high': high, 'low': low})
            return
            
        last_point = self.swing_points[symbol][-1]
        if high > last_point['high']:
            self.swing_points[symbol].append({'high': high, 'low': low})
        elif low < last_point['low']:
            self.swing_points[symbol].append({'high': high, 'low': low})
            
    def _identify_levels(self, symbol: str):
        """Identify support and resistance levels from swing points"""
        points = self.swing_points.get(symbol, [])
        if len(points) < 3:
            return
            
        # Identify support levels (local minima)
        for i in range(1, len(points)-1):
            if points[i]['low'] < points[i-1]['low'] and points[i]['low'] < points[i+1]['low']:
                if points[i]['low'] not in self.support_levels[symbol]:
                    self.support_levels[symbol].append(points[i]['low'])
                    
        # Identify resistance levels (local maxima)
        for i in range(1, len(points)-1):
            if points[i]['high'] > points[i-1]['high'] and points[i]['high'] > points[i+1]['high']:
                if points[i]['high'] not in self.resistance_levels[symbol]:
                    self.resistance_levels[symbol].append(points[i]['high'])
                    
    def _get_nearest_support(self, symbol: str, current_price: float) -> Optional[float]:
        """Get nearest support level below current price"""
        supports = sorted([s for s in self.support_levels.get(symbol, []) if s < current_price])
        return supports[-1] if supports else None
        
    def _get_nearest_resistance(self, symbol: str, current_price: float) -> Optional[float]:
        """Get nearest resistance level above current price"""
        resistances = sorted([r for r in self.resistance_levels.get(symbol, []) if r > current_price])
        return resistances[0] if resistances else None

class PositionSizingManager:
    def __init__(self):
        self.portfolio_heat = 0.0        # Track portfolio heat
        
    async def calculate_position_size(self, 
                                    account_balance: float,
                                    entry_price: float,
                                    stop_loss: float,
                                    atr: float,
                                    timeframe: str,
                                    market_condition: Dict[str, Any],
                                    correlation_factor: float = 1.0) -> float:
        """Calculate position size based on 20% of account balance with improved crypto handling"""
        # Calculate risk amount (20% of account balance)
        risk_amount = account_balance * 0.20
        
        # Determine if this is a crypto instrument
        is_crypto = False
        normalized_symbol = standardize_symbol(str(entry_price))
        if any(crypto in normalized_symbol for crypto in ["BTC", "ETH", "XRP", "LTC"]):
            is_crypto = True
        
        # Adjust risk based on market condition
        volatility_adjustment = market_condition.get('volatility_ratio', 1.0)
        if market_condition.get('volatility_state') == 'high':
            risk_amount *= 0.75  # Reduce risk by 25% in high volatility
        elif market_condition.get('volatility_state') == 'low':
            risk_amount *= 1.25  # Increase risk by 25% in low volatility
            
        # Adjust for correlation
        risk_amount *= correlation_factor
        
        # Additional adjustment for crypto due to higher volatility
        if is_crypto:
            risk_amount *= 0.8  # Additional 20% reduction for crypto
        
        # Calculate position size based on risk
        risk_per_unit = abs(entry_price - stop_loss)
        if risk_per_unit == 0 or risk_per_unit < 0.00001:  # Prevent division by zero or very small values
            risk_per_unit = atr  # Use ATR as a fallback
            
        position_size = risk_amount / risk_per_unit
            
        # Round to appropriate precision
        if timeframe in ["15M", "1H"]:
            position_size = round(position_size, 2)
        else:
            position_size = round(position_size, 1)
            
        return position_size
        
    async def update_portfolio_heat(self, new_position_size: float):
        """Update portfolio heat with new position"""
        self.portfolio_heat += new_position_size
        
    async def get_correlation_factor(self, symbol: str, existing_positions: List[str]) -> float:
        """Calculate correlation factor based on existing positions"""
        if not existing_positions:
            return 1.0
            
        # Implement correlation calculation logic here
        # This is a simplified version
        normalized_symbol = standardize_symbol(symbol)
        
        # Find similar pairs (same base or quote currency)
        similar_pairs = 0
        for pos in existing_positions:
            pos_normalized = standardize_symbol(pos)
            # Check if they share the same base or quote currency
            if (normalized_symbol.split('_')[0] == pos_normalized.split('_')[0] or 
                normalized_symbol.split('_')[1] == pos_normalized.split('_')[1]):
                similar_pairs += 1
        
        # Reduce correlation factor based on number of similar pairs
        if similar_pairs > 0:
            return max(0.5, 1.0 - (similar_pairs * 0.1))  # Minimum correlation factor of 0.5
        return 1.0

class EnhancedRiskManager:
    def __init__(self):
        self.positions = {}
        self.atr_period = 14
        self.take_profit_levels = TIMEFRAME_TAKE_PROFIT_LEVELS
        self.trailing_settings = TIMEFRAME_TRAILING_SETTINGS
        self.time_stops = TIMEFRAME_TIME_STOPS
        
        # ATR multipliers based on timeframe and instrument type
        self.atr_multipliers = {
            "FOREX": {
                "15M": 1.5,
                "1H": 1.75,
                "4H": 2.0,
                "1D": 2.25
            },
            "CRYPTO": {
                "15M": 2.0,
                "1H": 2.25,
                "4H": 2.5,
                "1D": 2.75
            },
            "XAU_USD": {
                "15M": 1.75,
                "1H": 2.0,
                "4H": 2.25,
                "1D": 2.5
            }
        }

    async def initialize_position(self, symbol: str, entry_price: float, position_type: str, 
                                timeframe: str, units: float, atr: float):
        """Initialize position with ATR-based stops and tiered take-profits"""
        # Determine instrument type
        instrument_type = self._get_instrument_type(symbol)
        
        # Get ATR multiplier based on timeframe and instrument
        atr_multiplier = self.atr_multipliers[instrument_type].get(
            timeframe, self.atr_multipliers[instrument_type]["1H"]
        )
        
        # Calculate initial stop loss
        if position_type == "LONG":
            stop_loss = entry_price - (atr * atr_multiplier)
            take_profits = [
                entry_price + (atr * atr_multiplier),  # 1:1
                entry_price + (atr * atr_multiplier * 2),  # 2:1
                entry_price + (atr * atr_multiplier * 3)  # 3:1
            ]
        else:  # SHORT
            stop_loss = entry_price + (atr * atr_multiplier)
            take_profits = [
                entry_price - (atr * atr_multiplier),  # 1:1
                entry_price - (atr * atr_multiplier * 2),  # 2:1
                entry_price - (atr * atr_multiplier * 3)  # 3:1
            ]
        
        # Get take-profit levels for this timeframe
        tp_levels = self.take_profit_levels.get(timeframe, self.take_profit_levels["1H"])
        
        # Initialize position tracking
        self.positions[symbol] = {
            'entry_price': entry_price,
            'position_type': position_type,
            'timeframe': timeframe,
            'units': units,
            'current_units': units,
            'stop_loss': stop_loss,
            'take_profits': take_profits,
            'tp_levels': tp_levels,
            'entry_time': datetime.now(timezone('Asia/Bangkok')),
            'exit_levels_hit': [],
            'trailing_stop': None,
            'atr': atr,
            'atr_multiplier': atr_multiplier,
            'instrument_type': instrument_type,
            'symbol': symbol
        }
        
        logger.info(f"Initialized position for {symbol}: Stop Loss: {stop_loss}, Take Profits: {take_profits}")

    def get_instrument_type(instrument: str) -> str:
        """Determine the instrument type for sizing and ATR calculations"""
        if "_" in instrument:  # Stock CFDs typically have underscores
            return "STOCK"
        elif "JPY" in instrument or any(x in instrument for x in ["USD", "EUR", "GBP", "AUD", "NZD", "CAD", "CHF"]):
            return "FOREX"
        else:
            return "COMMODITY"

    def ensure_proper_timeframe(timeframe: str) -> str:
        """Ensures timeframe is in the proper format (e.g., converts '15' to '15M')"""
        # Handle special cases first
        if timeframe.upper() in ['D', 'DAY', 'DAILY', '1D']:
            return 'D'
        if timeframe.upper() in ['W', 'WEEK', 'WEEKLY', '1W']:
            return 'W'
        if timeframe.upper() in ['M', 'MONTH', 'MONTHLY', '1M']:
            return 'M'
            
        # Strip any non-alphanumeric characters
        timeframe = re.sub(r'[^a-zA-Z0-9]', '', timeframe)
        
        # If it's just a number, append 'M' for minutes
        if timeframe.isdigit():
            return f"{timeframe}M"
        
        # If it already has a suffix (like 15M or 4H), return as is
        return timeframe

    async def update_position(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """Update position status and return any necessary actions"""
        if symbol not in self.positions:
            return {}
            
        position = self.positions[symbol]
        actions = {}
        
        # Check for stop loss hit
        if self._check_stop_loss_hit(position, current_price):
            actions['stop_loss'] = True
            return actions
            
        # Check for take-profit levels
        tp_actions = self._check_take_profits(position, current_price)
        if tp_actions:
            actions['take_profits'] = tp_actions
            
        # Update trailing stop if applicable
        trailing_action = self._update_trailing_stop(position, current_price)
        if trailing_action:
            actions['trailing_stop'] = trailing_action
            
        # Check time-based adjustments
        time_action = self._check_time_adjustments(position)
        if time_action:
            actions['time_adjustment'] = time_action
            
        return actions

    def _check_stop_loss_hit(self, position: Dict[str, Any], current_price: float) -> bool:
        """Check if stop loss has been hit"""
        if position['position_type'] == "LONG":
            return current_price <= position['stop_loss']
        else:
            return current_price >= position['stop_loss']

    def _check_take_profits(self, position: Dict[str, Any], current_price: float) -> Optional[Dict[str, Any]]:
        """Check if any take-profit levels have been hit"""
        actions = {}
        
        for i, tp in enumerate(position['take_profits']):
            if i not in position['exit_levels_hit']:
                if position['position_type'] == "LONG":
                    if current_price >= tp:
                        position['exit_levels_hit'].append(i)
                        tp_key = "first_exit" if i == 0 else "second_exit" if i == 1 else "runner"
                        actions[i] = {
                            'price': tp,
                            'units': position['current_units'] * position['tp_levels'][tp_key]
                        }
                else:  # SHORT
                    if current_price <= tp:
                        position['exit_levels_hit'].append(i)
                        tp_key = "first_exit" if i == 0 else "second_exit" if i == 1 else "runner"
                        actions[i] = {
                            'price': tp,
                            'units': position['current_units'] * position['tp_levels'][tp_key]
                        }
        
        return actions if actions else None

    def _update_trailing_stop(self, position: Dict[str, Any], current_price: float) -> Optional[Dict[str, Any]]:
        """Update trailing stop based on profit levels"""
        if not position['exit_levels_hit']:  # Only trail after first take-profit hit
            return None
            
        settings = self.trailing_settings.get(position['timeframe'], self.trailing_settings["1H"])
        current_multiplier = settings['initial_multiplier']
        
        # Adjust multiplier based on profit levels
        for level in settings['profit_levels']:
            if self._get_current_rr_ratio(position, current_price) >= level['threshold']:
                current_multiplier = level['multiplier']
                
        # Calculate new trailing stop
        if position['position_type'] == "LONG":
            new_stop = current_price - (position['atr'] * current_multiplier)
            if position['trailing_stop'] is None or new_stop > position['trailing_stop']:
                position['trailing_stop'] = new_stop
                return {'new_stop': new_stop}
        else:  # SHORT
            new_stop = current_price + (position['atr'] * current_multiplier)
            if position['trailing_stop'] is None or new_stop < position['trailing_stop']:
                position['trailing_stop'] = new_stop
                return {'new_stop': new_stop}
                
        return None

    def _get_current_rr_ratio(self, position: Dict[str, Any], current_price: float) -> float:
        """Calculate current risk-reward ratio"""
        risk = abs(position['entry_price'] - position['stop_loss'])
        if risk == 0:  # Prevent division by zero
            risk = position['atr']  # Use ATR as fallback
            
        if position['position_type'] == "LONG":
            reward = current_price - position['entry_price']
        else:
            reward = position['entry_price'] - current_price
        return reward / risk

    def _check_time_adjustments(self, position: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Check and apply time-based adjustments"""
        settings = self.time_stops.get(position['timeframe'], self.time_stops["1H"])
        current_duration = (datetime.now(timezone('Asia/Bangkok')) - position['entry_time']).total_seconds() / 3600
        
        if current_duration > settings['max_duration']:
            return {
                'action': 'tighten_stop',
                'multiplier': settings['stop_adjustment']
            }
        return None

    async def clear_position(self, symbol: str):
        """Clear position from risk management"""
        if symbol in self.positions:
            del self.positions[symbol]

class TradingConfig:
    def __init__(self):
        self.atr_multipliers = {
            "FOREX": {
                "15M": 1.5,
                "1H": 1.75,
                "4H": 2.0,
                "1D": 2.25
            },
            "CRYPTO": {
                "15M": 2.0,
                "1H": 2.25,
                "4H": 2.5,
                "1D": 2.75
            },
            "XAU_USD": {
                "15M": 1.75,
                "1H": 2.0,
                "4H": 2.25,
                "1D": 2.5
            }
        }
        
        self.take_profit_levels = {
            "15M": {
                "first_exit": 0.5,
                "second_exit": 0.25,
                "runner": 0.25
            },
            "1H": {
                "first_exit": 0.4,
                "second_exit": 0.3,
                "runner": 0.3
            },
            "4H": {
                "first_exit": 0.33,
                "second_exit": 0.33,
                "runner": 0.34
            },
            "1D": {
                "first_exit": 0.33,
                "second_exit": 0.33,
                "runner": 0.34
            }
        }
        
        self.market_conditions = {
            "volatility_adjustments": {
                "high": 0.75,    # Reduce risk by 25% in high volatility
                "low": 1.25,     # Increase risk by 25% in low volatility
                "normal": 1.0
            }
        }
                    
    def update_atr_multipliers(self, instrument: str, timeframe: str, new_multiplier: float):
        """Update ATR multiplier for specific instrument and timeframe"""
        instrument_type = "FOREX"
        if any(crypto in instrument for crypto in ["BTC", "ETH", "XRP", "LTC"]):
            instrument_type = "CRYPTO"
        elif "XAU" in instrument:
            instrument_type = "XAU_USD"
            
        if instrument_type in self.atr_multipliers and timeframe in self.atr_multipliers[instrument_type]:
            if 1.0 <= new_multiplier <= 4.0:  # Reasonable range for ATR multipliers
                self.atr_multipliers[instrument_type][timeframe] = new_multiplier
            else:
                logger.warning(f"Invalid ATR multiplier: {new_multiplier}. Must be between 1.0 and 4.0.")
                
    def update_take_profit_levels(self, timeframe: str, new_levels: Dict[str, float]):
        """Update take-profit levels for specific timeframe"""
        if timeframe in self.take_profit_levels:
            total = sum(new_levels.values())
            if abs(total - 1.0) < 0.01:  # Allow small rounding errors
                self.take_profit_levels[timeframe] = new_levels
            else:
                logger.warning(f"Invalid take-profit levels for {timeframe}. Sum must be 1.0.")

##############################################################################
# Trade Execution
##############################################################################

async def get_account_balance(account_id: str) -> float:
    """Get the current account balance"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Getting account balance for {account_id}")
    
    try:
        # Get session and API URL
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{account_id}"
        
        # Make the API request with retries
        retries = 0
        while retries < config.max_retries:
            try:
                async with session.get(url, timeout=HTTP_REQUEST_TIMEOUT) as response:
                    if response.status == 200:
                        response_data = await response.json()
                        account = response_data.get("account", {})
                        balance = float(account.get("balance", 0))
                        
                        logger.info(f"[{request_id}] Retrieved account balance: {balance}")
                        return balance
                    
                    # Handle error responses
                    response_text = await response.text()
                    logger.error(f"[{request_id}] Error getting account balance: {response.status}, Response: {response_text}")
                    
                    if "RATE_LIMIT" in response_text:
                        await asyncio.sleep(60)  # Longer wait for rate limits
                    else:
                        delay = config.base_delay * (2 ** retries)
                        await asyncio.sleep(delay)
                    
                    retries += 1
                    
            except aiohttp.ClientError as e:
                logger.error(f"[{request_id}] Network error getting account balance: {str(e)}")
                if retries < config.max_retries - 1:
                    await asyncio.sleep(config.base_delay * (2 ** retries))
                    retries += 1
                    continue
                raise
        
        # If we've exhausted all retries without success
        raise ValueError(f"Failed to get account balance after {config.max_retries} attempts")
        
    except Exception as e:
        logger.error(f"[{request_id}] Error getting account balance: {str(e)}", exc_info=True)
        # Return a placeholder value in case of error
        return 10000.0  # Placeholder value

async def get_account_summary() -> Tuple[bool, Dict[str, Any]]:
    """Get account summary with improved error handling"""
    try:
        session = await get_session()
        async with session.get(f"{config.oanda_api_url}/accounts/{config.oanda_account}/summary") as resp:
            if resp.status != 200:
                error_text = await resp.text()
                logger.error(f"Account summary fetch failed: {error_text}")
                return False, {"error": error_text}
            data = await resp.json()
            return True, data.get('account', {})
    except Exception as e:
        logger.error(f"Error fetching account summary: {str(e)}")
        return False, {"error": str(e)}

async def send_notification(title: str, message: str, level: str = "info") -> None:
    """Send a notification to the user"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Sending notification: {title} - {message}")
    
    try:
        # Choose the appropriate logging level
        log_level = getattr(logging, level.upper(), logging.INFO)
        
        # Log the notification
        logger.log(log_level, f"NOTIFICATION: {title} - {message}")
        
        # You could implement different notification methods here:
        # 1. Email notifications
        # 2. SMS notifications
        # 3. Push notifications
        # 4. Slack/Discord webhooks
        
        # Example for email notification (placeholder)
        if level.upper() in ["WARNING", "ERROR", "CRITICAL"]:
            # In a real implementation, you would send an actual email here
            logger.info(f"[{request_id}] Would send urgent email for {level} notification: {title}")
        
        # Example for Slack notification (placeholder)
        notification_data = {
            "text": f"*{title}*\n{message}",
            "color": {
                "info": "good",
                "warning": "warning",
                "error": "danger",
                "critical": "danger"
            }.get(level.lower(), "good")
        }
        
        # In a real implementation, you would send to a webhook here
        logger.debug(f"[{request_id}] Would send Slack notification: {json.dumps(notification_data)}")
        
    except Exception as e:
        logger.error(f"[{request_id}] Error sending notification: {str(e)}", exc_info=True)

async def calculate_trade_size(instrument: str, percentage: float, balance: float) -> Tuple[float, int]:
    """Calculate the size of the trade based on percentage of balance"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Calculating trade size for {instrument} with {percentage}% of ${balance}")
    
    try:
        # Get current price
        current_price = await get_current_price(instrument, "BUY")
        
        # Determine the instrument type
        instrument_type = get_instrument_type(instrument)
        
        # Set precision and calculate size based on instrument type
        if instrument_type == "FOREX":
            # For forex, size is in base currency units
            # The precision is typically 1 for most brokers
            precision = 0
            
            # Calculate the position size in base currency
            # percentage is the risk percentage (e.g., 2% of account)
            risk_amount = balance * (percentage / 100)
            
            # For forex, convert to units (micro/mini/standard lots)
            if "JPY" in instrument:
                # JPY pairs have different pricing
                units = int(risk_amount * 1000)  # Approximate for JPY pairs
            else:
                units = int(risk_amount * 10000)  # Standard conversion for most forex pairs
            
            # Ensure minimum size
            units = max(1, units)
            
            logger.info(f"[{request_id}] Calculated forex position size: {units} units")
            return units, precision
            
        elif instrument_type == "STOCK":
            # For stocks, size is in number of shares
            precision = 0  # Whole shares
            
            # Calculate the position size in shares
            risk_amount = balance * (percentage / 100)
            shares = int(risk_amount / current_price)
            
            # Ensure minimum size
            shares = max(1, shares)
            
            logger.info(f"[{request_id}] Calculated stock position size: {shares} shares")
            return shares, precision
            
        else:  # COMMODITY
            # For commodities, it depends on the specific instrument
            if instrument in ["XAU_USD", "GOLD"]:
                precision = 2  # Gold can be traded in decimals with some brokers
                
                # Calculate position size in ounces
                risk_amount = balance * (percentage / 100)
                ounces = round(risk_amount / current_price, precision)
                
                # Ensure minimum size
                ounces = max(0.01, ounces)
                
                logger.info(f"[{request_id}] Calculated gold position size: {ounces} ounces")
                return ounces, precision
                
            else:
                # Default for other commodities
                precision = 0
                units = int(balance * (percentage / 100) / current_price)
                units = max(1, units)
                
                logger.info(f"[{request_id}] Calculated commodity position size: {units} units")
                return units, precision
                
    except Exception as e:
        logger.error(f"[{request_id}] Error calculating trade size: {str(e)}", exc_info=True)
        # Return placeholder values in case of error
        return 100.0, 0  # Default to 100 units with 0 decimal precision

@handle_async_errors
async def execute_trade(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Execute trade with improved retry logic and error handling"""
    request_id = str(uuid.uuid4())
    instrument = standardize_symbol(alert_data['symbol'])
    
    # Add detailed logging at the beginning
    logger.info(f"[{request_id}] Executing trade for {instrument} - Action: {alert_data['action']}")
    
    try:
        # Calculate size and get current price
        balance = await get_account_balance(alert_data.get('account', config.oanda_account))
        units, precision = await calculate_trade_size(instrument, alert_data['percentage'], balance)
        if alert_data['action'].upper() == 'SELL':
            units = -abs(units)
            
        # Get current price for stop loss and take profit calculations
        current_price = await get_current_price(instrument, alert_data['action'])
        
        # Calculate stop loss and take profit levels
        atr = await get_atr(instrument, alert_data['timeframe'])
        instrument_type = get_instrument_type(instrument)
        
        # Get ATR multiplier based on timeframe and instrument
        atr_multiplier = get_atr_multiplier(instrument_type, alert_data['timeframe'])
        
        # Set price precision based on instrument
        # Most forex pairs use 5 decimal places, except JPY pairs which use 3
        price_precision = 3 if "JPY" in instrument else 5
        
        # Calculate stop loss and take profit levels with proper rounding
        if alert_data['action'].upper() == 'BUY':
            stop_loss = round(current_price - (atr * atr_multiplier), price_precision)
            take_profits = [
                round(current_price + (atr * atr_multiplier), price_precision),  # 1:1
                round(current_price + (atr * atr_multiplier * 2), price_precision),  # 2:1
                round(current_price + (atr * atr_multiplier * 3), price_precision)  # 3:1
            ]
        else:  # SELL
            stop_loss = round(current_price + (atr * atr_multiplier), price_precision)
            take_profits = [
                round(current_price - (atr * atr_multiplier), price_precision),  # 1:1
                round(current_price - (atr * atr_multiplier * 2), price_precision),  # 2:1
                round(current_price - (atr * atr_multiplier * 3), price_precision)  # 3:1
            ]
        
        # Create order data with stop loss and take profit using rounded values
        order_data = {
            "order": {
                "type": alert_data['orderType'],
                "instrument": instrument,
                "units": str(units),
                "timeInForce": alert_data['timeInForce'],
                "positionFill": "DEFAULT",
                "stopLossOnFill": {
                    "price": str(stop_loss),
                    "timeInForce": "GTC",
                    "triggerMode": "TOP_OF_BOOK"
                },
                "takeProfitOnFill": {
                    "price": str(take_profits[0]),  # First take profit level
                    "timeInForce": "GTC",
                    "triggerMode": "TOP_OF_BOOK"
                }
            }
        }
        
        # Add trailing stop if configured, also with proper rounding
        if alert_data.get('use_trailing_stop', True):
            trailing_distance = round(atr * atr_multiplier, price_precision)
            order_data["order"]["trailingStopLossOnFill"] = {
                "distance": str(trailing_distance),
                "timeInForce": "GTC",
                "triggerMode": "TOP_OF_BOOK"
            }
        
        # Log the order details for debugging
        logger.info(f"[{request_id}] Order data: {json.dumps(order_data)}")
        
        # Get session and API URL
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{alert_data.get('account', config.oanda_account)}/orders"
        
        # Execute trade with retries
        retries = 0
        while retries < config.max_retries:
            try:
                async with session.post(url, json=order_data, timeout=HTTP_REQUEST_TIMEOUT) as response:
                    response_text = await response.text()
                    logger.info(f"[{request_id}] Response status: {response.status}, Response: {response_text}")
                    
                    if response.status == 201:
                        result = json.loads(response_text)
                        logger.info(f"[{request_id}] Trade executed successfully with stops: {result}")
                        return True, result
                    
                    # Extract and log the specific error for better debugging
                    try:
                        error_data = json.loads(response_text)
                        error_code = error_data.get("errorCode", "UNKNOWN_ERROR")
                        error_message = error_data.get("errorMessage", "Unknown error")
                        logger.error(f"[{request_id}] OANDA error: {error_code} - {error_message}")
                    except:
                        pass
                    
                    # Handle error responses
                    if "RATE_LIMIT" in response_text:
                        await asyncio.sleep(60)  # Longer wait for rate limits
                    elif "MARKET_HALTED" in response_text:
                        return False, {"error": "Market is halted"}
                    else:
                        delay = config.base_delay * (2 ** retries)
                        await asyncio.sleep(delay)
                    
                    logger.warning(f"[{request_id}] Retry {retries + 1}/{config.max_retries}")
                    retries += 1
                    
            except aiohttp.ClientError as e:
                logger.error(f"[{request_id}] Network error: {str(e)}")
                if retries < config.max_retries - 1:
                    await asyncio.sleep(config.base_delay * (2 ** retries))
                    retries += 1
                    continue
                return False, {"error": f"Network error: {str(e)}"}
        
        return False, {"error": "Maximum retries exceeded"}
        
    except Exception as e:
        logger.error(f"[{request_id}] Error executing trade: {str(e)}")
        return False, {"error": str(e)}

@handle_async_errors
async def close_position(alert_data: Dict[str, Any], position_tracker=None) -> Tuple[bool, Dict[str, Any]]:
    """Close an open position with improved error handling and validation"""
    request_id = str(uuid.uuid4())
    try:
        instrument = standardize_symbol(alert_data['symbol'])
        account_id = alert_data.get('account', config.oanda_account)
        
        # Fetch current position details
        success, position_data = await get_open_positions(account_id)
        if not success:
            return False, position_data
            
        # Find the position to close
        position = next(
            (p for p in position_data.get('positions', [])
             if p['instrument'] == instrument),
            None
        )
        
        if not position:
            logger.warning(f"[{request_id}] No position found for {instrument}")
            return False, {"error": f"No open position for {instrument}"}
            
        # Determine units to close based on position type
        long_units = float(position['long'].get('units', '0'))
        short_units = float(position['short'].get('units', '0'))
        
        close_data = {
            "longUnits": "ALL" if long_units > 0 else "NONE",
            "shortUnits": "ALL" if short_units < 0 else "NONE"
        }
        
        # Execute the close
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{account_id}/positions/{instrument}/close"
        
        async with session.put(url, json=close_data, timeout=HTTP_REQUEST_TIMEOUT) as response:
            result = await response.json()
            
            if response.status == 200:
                logger.info(f"[{request_id}] Position closed successfully: {result}")
                
                # Calculate and log P&L if available
                pnl = 0.0
                try:
                    # Extract P&L from transaction details
                    if 'longOrderFillTransaction' in result and result['longOrderFillTransaction']:
                        pnl += float(result['longOrderFillTransaction'].get('pl', 0))
                    
                    if 'shortOrderFillTransaction' in result and result['shortOrderFillTransaction']:
                        pnl += float(result['shortOrderFillTransaction'].get('pl', 0))
                    
                    logger.info(f"[{request_id}] Position P&L: {pnl}")
                    
                    # Record P&L if tracker is provided
                    if position_tracker and pnl != 0:
                        await position_tracker.record_trade_pnl(pnl)
                except Exception as e:
                    logger.error(f"[{request_id}] Error calculating P&L: {str(e)}")
                
                return True, result
            else:
                logger.error(f"[{request_id}] Failed to close position: {result}")
                return False, result
                
    except Exception as e:
        logger.error(f"[{request_id}] Error closing position: {str(e)}")
        return False, {"error": str(e)}

@handle_async_errors
async def close_partial_position(alert_data: Dict[str, Any], percentage: float, position_tracker=None) -> Tuple[bool, Dict[str, Any]]:
    """Close a partial position with percentage specification"""
    request_id = str(uuid.uuid4())
    try:
        instrument = standardize_symbol(alert_data['symbol'])
        account_id = alert_data.get('account', config.oanda_account)
        # Fetch current position details
        success, position_data = await get_open_positions(account_id)
        if not success:
            return False, position_data
            
        # Find the position to partially close
        position = next(
            (p for p in position_data.get('positions', [])
             if p['instrument'] == instrument),
            None
        )
        
        if not position:
            logger.warning(f"[{request_id}] No position found for {instrument}")
            return False, {"error": f"No open position for {instrument}"}
            
        # Determine units to close based on position type and percentage
        long_units = float(position['long'].get('units', '0'))
        short_units = float(position['short'].get('units', '0'))
        
        # Calculate units to close
        if long_units > 0:
            units_to_close = int(long_units * percentage / 100)
            close_data = {"longUnits": str(units_to_close)}
        elif short_units < 0:
            units_to_close = int(abs(short_units) * percentage / 100)
            close_data = {"shortUnits": str(units_to_close)}
        else:
            return False, {"error": "No units to close"}
        
        # Execute the partial close
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{account_id}/positions/{instrument}/close"
        
        async with session.put(url, json=close_data, timeout=HTTP_REQUEST_TIMEOUT) as response:
            result = await response.json()
            
            if response.status == 200:
                logger.info(f"[{request_id}] Position partially closed successfully: {result}")
                
                # Calculate and log P&L if available
                pnl = 0.0
                try:
                    # Extract P&L from transaction details
                    if 'longOrderFillTransaction' in result and result['longOrderFillTransaction']:
                        pnl += float(result['longOrderFillTransaction'].get('pl', 0))
                    
                    if 'shortOrderFillTransaction' in result and result['shortOrderFillTransaction']:
                        pnl += float(result['shortOrderFillTransaction'].get('pl', 0))
                    
                    logger.info(f"[{request_id}] Partial position P&L: {pnl}")
                    
                    # Record P&L if tracker is provided
                    if position_tracker and pnl != 0:
                        await position_tracker.record_trade_pnl(pnl)
                except Exception as e:
                    logger.error(f"[{request_id}] Error calculating P&L: {str(e)}")
                
                return True, result
            else:
                logger.error(f"[{request_id}] Failed to close partial position: {result}")
                return False, result
                
    except Exception as e:
        logger.error(f"[{request_id}] Error closing partial position: {str(e)}")
        return False, {"error": str(e)}

##############################################################################
# Position Tracking
##############################################################################

class PositionTracker:
    def __init__(self):
        self.positions = {}
        self.bar_times = {}
        self._lock = asyncio.Lock()
        self._running = False
        self._initialized = False
        self.daily_pnl = 0.0
        self.pnl_reset_date = datetime.now().date()
        self._price_monitor_task = None

    @handle_async_errors
    async def reconcile_positions(self):
        """Reconcile positions with improved error handling and timeout"""
        while self._running:
            try:
                # Wait between reconciliation attempts
                await asyncio.sleep(900)  # Every 15 minutes
                
                logger.info("Starting position reconciliation")
                async with self._lock:
                    async with asyncio.timeout(60):  # Increased timeout to 60 seconds
                        success, positions_data = await get_open_positions()
                    
                        if not success:
                            logger.error("Failed to fetch positions for reconciliation")
                            continue
                    
                        # Convert Oanda positions to a set for efficient lookup
                        oanda_positions = {
                            p['instrument'] for p in positions_data.get('positions', [])
                        }
                    
                        # Check each tracked position
                        for symbol in list(self.positions.keys()):
                            try:
                                if symbol not in oanda_positions:
                                    # Position closed externally
                                    old_data = self.positions.pop(symbol, None)
                                    self.bar_times.pop(symbol, None)
                                    logger.warning(
                                        f"Removing stale position for {symbol}. "
                                        f"Old data: {old_data}"
                                    )
                            except Exception as e:
                                logger.error(
                                    f"Error reconciling position for {symbol}: {str(e)}"
                                )
                        
                        logger.info(
                            f"Reconciliation complete. Active positions: "
                            f"{list(self.positions.keys())}"
                        )
                        
            except asyncio.TimeoutError:
                logger.error("Position reconciliation timed out, will retry in next cycle")
                continue  # Continue to next iteration instead of sleeping
            except asyncio.CancelledError:
                logger.info("Position reconciliation task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in reconciliation loop: {str(e)}")
                await asyncio.sleep(60)  # Wait before retrying on unexpected errors
        
    async def start(self):
        """Initialize and start the position tracker"""
        if not self._initialized:
            async with self._lock:
                if not self._initialized:  # Double-check pattern
                    self._running = True
                    self.reconciliation_task = asyncio.create_task(self.reconcile_positions())
                    self._initialized = True
                    logger.info("Position tracker started")
        
    async def stop(self):
        """Gracefully stop the position tracker"""
        self._running = False
        if hasattr(self, 'reconciliation_task'):
            self.reconciliation_task.cancel()
            try:
                await self.reconciliation_task
            except asyncio.CancelledError:
                logger.info("Position reconciliation task cancelled")
            except Exception as e:
                logger.error(f"Error stopping reconciliation task: {str(e)}")
        logger.info("Position tracker stopped")
    
    @handle_async_errors
    async def record_position(self, symbol: str, action: str, timeframe: str, entry_price: float) -> bool:
        """Record a new position with improved error handling"""
        try:
            async with self._lock:
                current_time = datetime.now(timezone('Asia/Bangkok'))
                
                position_data = {
                    'entry_time': current_time,
                    'position_type': 'LONG' if action.upper() == 'BUY' else 'SHORT',
                    'bars_held': 0,
                    'timeframe': timeframe,
                    'last_update': current_time,
                    'entry_price': entry_price
                }
                
                self.positions[symbol] = position_data
                self.bar_times.setdefault(symbol, []).append(current_time)
                
                logger.info(f"Recorded position for {symbol}: {position_data}")
                return True
                
        except Exception as e:
            logger.error(f"Error recording position for {symbol}: {str(e)}")
            return False
    
    @handle_async_errors
    async def clear_position(self, symbol: str) -> bool:
        """Clear a position with improved error handling"""
        try:
            async with self._lock:
                if symbol in self.positions:
                    position_data = self.positions.pop(symbol)
                    self.bar_times.pop(symbol, None)
                    logger.info(f"Cleared position for {symbol}: {position_data}")
                    return True
                return False
        except Exception as e:
            logger.error(f"Error clearing position for {symbol}: {str(e)}")
            return False
    
    async def get_position_info(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get current position information for a symbol"""
        async with self._lock:
            return self.positions.get(symbol)
    
    async def get_all_positions(self) -> Dict[str, Dict[str, Any]]:
        """Get all current positions"""
        async with self._lock:
            return self.positions.copy()

    @handle_async_errors
    async def record_trade_pnl(self, pnl: float) -> None:
        """Record P&L from a trade and reset daily if needed"""
        async with self._lock:
            current_date = datetime.now().date()
            
            # Reset daily P&L if it's a new day
            if current_date != self.pnl_reset_date:
                logger.info(f"Resetting daily P&L (was {self.daily_pnl}) for new day: {current_date}")
                self.daily_pnl = 0.0
                self.pnl_reset_date = current_date
            
            # Add the P&L to today's total
            self.daily_pnl += pnl
            logger.info(f"Updated daily P&L: {self.daily_pnl}")
    
    async def get_daily_pnl(self) -> float:
        """Get current daily P&L"""
        async with self._lock:
            # Reset if it's a new day
            current_date = datetime.now().date()
            if current_date != self.pnl_reset_date:
                self.daily_pnl = 0.0
                self.pnl_reset_date = current_date
            
            return self.daily_pnl
            
    async def check_max_daily_loss(self, account_balance: float) -> Tuple[bool, float]:
        """Check daily loss percentage - for monitoring only"""
        daily_pnl = await self.get_daily_pnl()
        loss_percentage = abs(min(0, daily_pnl)) / account_balance
        
        # Log the information without enforcing limits
        if loss_percentage > MAX_DAILY_LOSS * 0.5:  # Warn at 50% of the reference limit
            logger.warning(f"Daily loss at {loss_percentage:.2%} of account (reference limit: {MAX_DAILY_LOSS:.2%})")
            
        return True, loss_percentage  # Always return True since we're not enforcing limits
    
    async def update_position_exits(self, symbol: str, current_price: float) -> bool:
        """Update and check dynamic exit conditions"""
        try:
            async with self._lock:
                if symbol not in self.positions:
                    return False
                    
                position = self.positions[symbol]
                
                # This would call the risk manager's update function
                # Currently a placeholder - implement with your risk manager
                return False
                
        except Exception as e:
            logger.error(f"Error updating position exits for {symbol}: {str(e)}")
            return False

    async def get_position_entry_price(self, symbol: str) -> Optional[float]:
        """Get the entry price for a position"""
        async with self._lock:
            position = self.positions.get(symbol)
            return position.get('entry_price') if position else None

    async def get_position_type(self, symbol: str) -> Optional[str]:
        """Get the position type (LONG/SHORT) for a symbol"""
        async with self._lock:
            position = self.positions.get(symbol)
            return position.get('position_type') if position else None

    async def get_position_timeframe(self, symbol: str) -> Optional[str]:
        """Get the timeframe for a position"""
        async with self._lock:
            position = self.positions.get(symbol)
            return position.get('timeframe') if position else None

    async def get_position_stats(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get comprehensive position statistics including P&L"""
        async with self._lock:
            position = self.positions.get(symbol)
            if not position:
                return None
                
            return {
                'entry_price': position.get('entry_price'),
                'position_type': position.get('position_type'),
                'timeframe': position.get('timeframe'),
                'entry_time': position.get('entry_time'),
                'bars_held': position.get('bars_held', 0),
                'last_update': position.get('last_update'),
                'daily_pnl': self.daily_pnl
            }

##############################################################################
# Alert Handler
##############################################################################

class AlertHandler:
    def __init__(self):
        self.position_tracker = PositionTracker()
        self.risk_manager = EnhancedRiskManager()
        self.volatility_monitor = VolatilityMonitor()
        self.market_structure = MarketStructureAnalyzer()
        self.position_sizing = PositionSizingManager()
        self.config = TradingConfig()
        self.dynamic_exit_manager = DynamicExitManager()
        self.loss_manager = AdvancedLossManager()
        self.risk_analytics = RiskAnalytics()
        self._lock = asyncio.Lock()
        self._initialized = False
        self._price_monitor_task = None
        self._running = False
        self.error_recovery = error_recovery
    
    async def start(self):
        """Initialize the handler and start price monitoring"""
        if not self._initialized:
            async with self._lock:
                if not self._initialized:  # Double-check pattern
                    await self.position_tracker.start()
                    self._initialized = True
                    self._running = True
                    self._price_monitor_task = asyncio.create_task(self._monitor_positions())
                    logger.info("Alert handler initialized with price monitoring")
    
    async def stop(self):
        """Stop the alert handler and cleanup resources"""
        try:
            self._running = False
            if self._price_monitor_task:
                self._price_monitor_task.cancel()
                try:
                    await self._price_monitor_task
                except asyncio.CancelledError:
                    logger.info("Price monitoring task cancelled")
                except Exception as e:
                    logger.error(f"Error cancelling position monitoring: {str(e)}")
            await self.position_tracker.stop()
            logger.info("Alert handler stopped")
        except Exception as e:
            logger.error(f"Error stopping alert handler: {str(e)}")

    async def _monitor_positions(self):
        """Monitor positions for dynamic exit conditions with improved cancellation handling"""
        while self._running:
            try:
                positions = await self.position_tracker.get_all_positions()
                for symbol, position in positions.items():
                    try:
                        # Get current price
                        current_price = await get_current_price(symbol, position['position_type'])
                        
                        # Update position in risk manager
                        actions = await self.risk_manager.update_position(symbol, current_price)
                        
                        # Update dynamic exits
                        exit_actions = await self.dynamic_exit_manager.update_exits(symbol, current_price)
                        if exit_actions:
                            actions.update(exit_actions)
                            
                        # Update loss management
                        loss_actions = await self.loss_manager.update_position_loss(symbol, current_price)
                        if loss_actions:
                            actions.update(loss_actions)
                            
                        # Update risk analytics
                        await self.risk_analytics.update_position(symbol, current_price)
                        
                        # Process any actions
                        if actions:
                            await self._handle_position_actions(symbol, actions, current_price)
                            
                    except asyncio.CancelledError:
                        raise  # Re-raise to be caught by outer handler
                    except Exception as e:
                        logger.error(f"Error monitoring position {symbol}: {str(e)}")
                        continue
                
                # Sleep for appropriate interval
                await asyncio.sleep(15)  # Check every 15 seconds
                
            except asyncio.CancelledError:
                logger.info("Position monitoring cancelled")
                break  # Explicitly break the loop
            except Exception as e:
                logger.error(f"Error in position monitoring: {str(e)}")
                await asyncio.sleep(60)  # Wait before retrying on error

    
                
    async def _handle_position_actions(self, symbol: str, actions: Dict[str, Any], current_price: float):
        """Handle position actions with circuit breaker and error recovery"""
        request_id = str(uuid.uuid4())
        try:
            # Your updated _handle_position_actions code here
            # Handle stop loss hit
            if actions.get('stop_loss') or actions.get('position_limit') or actions.get('daily_limit') or actions.get('drawdown_limit'):
                logger.info(f"Stop loss or risk limit hit for {symbol} at {current_price}")
                await self._close_position(symbol)
                
            # Handle take profits
            if 'take_profits' in actions:
                tp_actions = actions['take_profits']
                for level, tp_data in tp_actions.items():
                    logger.info(f"Take profit {level} hit for {symbol} at {tp_data['price']}")
                    
                    # For partial take profits
                    if level == 0:  # First take profit is partial (50%)
                        await self._close_partial_position(symbol, 50)  # Close 50%
                    elif level == 1:  # Second take profit is partial (50% of remainder = 25% of original)
                        await self._close_partial_position(symbol, 50)  # Close 50% of what's left
                    else:  # Final take profit is full close
                        await self._close_position(symbol)
                        
            # Handle trailing stop updates
            if 'trailing_stop' in actions and isinstance(actions['trailing_stop'], dict):
                logger.info(f"Updated trailing stop for {symbol} to {actions['trailing_stop'].get('new_stop')}")
                
            # Handle time-based adjustments
            if 'time_adjustment' in actions:
                logger.info(f"Time-based adjustment for {symbol}: {actions['time_adjustment'].get('action')}")
                
        except Exception as e:
            logger.error(f"Error handling position actions for {symbol}: {str(e)}")
            # Record error and attempt recovery - Add this code
            error_context = {
                "symbol": symbol, 
                "current_price": current_price, 
                "handler": self
            }
            await self.error_recovery.handle_error(
                request_id, 
                "_handle_position_actions", 
                e, 
                error_context
            )
            
    async def close_position(alert_data: Dict[str, Any], position_tracker) -> Tuple[bool, Dict[str, Any]]:
    """Close an existing position"""
    request_id = str(uuid.uuid4())
    symbol = standardize_symbol(alert_data['symbol'])
    logger.info(f"[{request_id}] Closing position for {symbol}")
    
    try:
        # Get current position details
        position_info = await position_tracker.get_position(symbol)
        if not position_info:
            logger.warning(f"[{request_id}] No active position found for {symbol}")
            return False, {"error": "No active position found"}
        
        position_type = position_info.get('type', 'UNKNOWN')
        logger.info(f"[{request_id}] Found {position_type} position for {symbol}")
        
        # Determine the appropriate action to close the position
        close_action = "SELL" if position_type == "LONG" else "BUY"
        
        # Create the close order data
        close_data = {
            "order": {
                "type": "MARKET",
                "instrument": symbol,
                "units": str(-position_info.get('units', 100)),  # Negative units to close
                "timeInForce": "FOK",
                "positionFill": "REDUCE_ONLY"
            }
        }
        
        # Get session and API URL
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{alert_data.get('account', config.oanda_account)}/orders"
        
        # Execute close order with retries
        retries = 0
        while retries < config.max_retries:
            try:
                async with session.post(url, json=close_data, timeout=HTTP_REQUEST_TIMEOUT) as response:
                    response_text = await response.text()
                    logger.info(f"[{request_id}] Close position response: {response.status}, Response: {response_text}")
                    
                    if response.status == 201:
                        result = json.loads(response_text)
                        logger.info(f"[{request_id}] Position closed successfully: {result}")
                        
                        # Calculate profit/loss
                        try:
                            fill_info = result.get('orderFillTransaction', {})
                            entry_price = position_info.get('entry_price', 0)
                            exit_price = float(fill_info.get('price', 0))
                            units = float(fill_info.get('units', 0))
                            pl = fill_info.get('pl', '0')
                            
                            # Record trade results for analytics
                            await risk_analytics.record_trade_result(
                                symbol, 
                                position_type,
                                entry_price,
                                exit_price,
                                abs(units),
                                float(pl)
                            )
                            
                            logger.info(f"[{request_id}] Trade result recorded: {position_type}, entry: {entry_price}, exit: {exit_price}, PL: {pl}")
                        except Exception as e:
                            logger.error(f"[{request_id}] Error recording trade result: {str(e)}")
                        
                        return True, result
                    
                    # Handle error responses
                    try:
                        error_data = json.loads(response_text)
                        error_code = error_data.get("errorCode", "UNKNOWN_ERROR")
                        error_message = error_data.get("errorMessage", "Unknown error")
                        logger.error(f"[{request_id}] OANDA error: {error_code} - {error_message}")
                    except:
                        pass
                    
                    # Handle specific errors
                    if "RATE_LIMIT" in response_text:
                        await asyncio.sleep(60)  # Longer wait for rate limits
                    elif "POSITION_NOT_CLOSEABLE" in response_text:
                        return False, {"error": "Position not closeable"}
                    else:
                        delay = config.base_delay * (2 ** retries)
                        await asyncio.sleep(delay)
                    
                    logger.warning(f"[{request_id}] Close position retry {retries + 1}/{config.max_retries}")
                    retries += 1
                    
            except aiohttp.ClientError as e:
                logger.error(f"[{request_id}] Network error closing position: {str(e)}")
                if retries < config.max_retries - 1:
                    await asyncio.sleep(config.base_delay * (2 ** retries))
                    retries += 1
                    continue
                return False, {"error": f"Network error: {str(e)}"}
        
        return False, {"error": "Maximum retries exceeded while closing position"}
        
    except Exception as e:
        logger.error(f"[{request_id}] Error closing position: {str(e)}", exc_info=True)
        return False, {"error": str(e)}    



##############################################################################
# Block 5: API and Application
##############################################################################

##############################################################################
# FastAPI Setup & Lifespan
##############################################################################

# Initialize global variables
alert_handler: Optional[AlertHandler] = None  # Add this at the top level

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager with proper initialization and cleanup"""
    logger.info("Initializing application...")
    global _session, alert_handler
    
    try:
        await get_session(force_new=True)
        alert_handler = AlertHandler()  # Initialize the handler
        await alert_handler.start()
        logger.info("Services initialized successfully")
        handle_shutdown_signals()
        yield
    finally:
        logger.info("Shutting down services...")
        await cleanup_resources()
        logger.info("Shutdown complete")

async def cleanup_resources():
    """Clean up application resources"""
    tasks = []
    if alert_handler is not None:
        tasks.append(alert_handler.stop())
    if _session is not None and not _session.closed:
        tasks.append(_session.close())
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

def handle_shutdown_signals():
    """Set up signal handlers for graceful shutdown"""
    async def shutdown(sig: signal.Signals):
        logger.info(f"Received exit signal {sig.name}")
        await cleanup_resources()
        
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_event_loop().add_signal_handler(sig,
            lambda s=sig: asyncio.create_task(shutdown(s))
        )

# Add this function to your code (near your API endpoints)
def create_error_response(status_code: int, message: str, request_id: str) -> JSONResponse:
    """Helper to create consistent error responses"""
    return JSONResponse(
        status_code=status_code,
        content={"error": message, "request_id": request_id}
    )

# Create FastAPI app with proper configuration
app = FastAPI(
    title="OANDA Trading Bot",
    description="Advanced async trading bot using FastAPI and aiohttp",
    version="1.2.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.allowed_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def inject_dependencies(request: Request, call_next):
    """Inject dependencies into request state"""
    request.state.alert_handler = alert_handler
    request.state.session = await get_session()
    return await call_next(request)

##############################################################################
# Middleware
##############################################################################

@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log requests with improved error handling"""
    request_id = str(uuid.uuid4())
    try:
        logger.info(f"[{request_id}] {request.method} {request.url} started")
        
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        
        logger.info(
            f"[{request_id}] {request.method} {request.url} completed "
            f"with status {response.status_code} in {process_time:.4f}s"
        )
        
        # Add request ID to response headers
        response.headers["X-Request-ID"] = request_id
        return response
    except Exception as e:
        logger.error(f"[{request_id}] Error processing request: {str(e)}", exc_info=True)
        return create_error_response(
            status_code=500,
            message="Internal server error",
            request_id=request_id
        )

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Rate limiting middleware with configurable limits"""
    # Only apply rate limiting to trading routes
    path = request.url.path
    
    if path in ["/api/trade", "/api/close", "/api/alerts"]:
        client_ip = request.client.host
        
        # Create rate limiters if not already done
        if not hasattr(app, "rate_limiters"):
            app.rate_limiters = {}
            
        # Get or create rate limiter for this IP
        if client_ip not in app.rate_limiters:
            app.rate_limiters[client_ip] = {
                "count": 0,
                "reset_time": time.time() + 60  # Reset after 60 seconds
            }
            
        # Check if limit exceeded
        rate_limiter = app.rate_limiters[client_ip]
        current_time = time.time()
        
        # Reset if needed
        if current_time > rate_limiter["reset_time"]:
            rate_limiter["count"] = 0
            rate_limiter["reset_time"] = current_time + 60
            
        # Increment and check
        rate_limiter["count"] += 1
        
        if rate_limiter["count"] > config.max_requests_per_minute:
            logger.warning(f"Rate limit exceeded for {client_ip}")
            return JSONResponse(
                status_code=429,
                content={"error": "Too many requests", "retry_after": int(rate_limiter["reset_time"] - current_time)}
            )
            
    return await call_next(request)

##############################################################################
# API Endpoints
##############################################################################

# 6. Update health check endpoint to include circuit breaker status
@app.get("/api/health")
async def health_check():
    """Health check endpoint with service status and circuit breaker information"""
    try:
        # Check session health
        session_status = "healthy" if _session and not _session.closed else "unavailable"
        
        # Check account connection health
        account_status = "unknown"
        if session_status == "healthy":
            try:
                async with asyncio.timeout(5):
                    success, _ = await get_account_summary()
                    account_status = "connected" if success else "disconnected"
            except asyncio.TimeoutError:
                account_status = "timeout"
            except Exception:
                account_status = "error"
                
        # Check position tracker health
        tracker_status = "healthy" if alert_handler and alert_handler._initialized else "unavailable"
        
        # Add circuit breaker status
        circuit_breaker_status = None
        if alert_handler and hasattr(alert_handler, "error_recovery"):
            circuit_breaker_status = await alert_handler.error_recovery.get_circuit_breaker_status()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {
                "session": session_status,
                "account": account_status,
                "position_tracker": tracker_status
            },
            "circuit_breaker": circuit_breaker_status,
            "version": "1.2.0"
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"status": "unhealthy", "error": str(e)}
        )

# 5. Add circuit breaker status and reset endpoints
@app.get("/api/circuit-breaker/status")
async def circuit_breaker_status():
    """Get the current status of the circuit breaker"""
    if not alert_handler or not hasattr(alert_handler, "error_recovery"):
        return JSONResponse(
            status_code=503,
            content={"error": "Service unavailable"}
        )
        
    status = await alert_handler.error_recovery.get_circuit_breaker_status()
    
    return {
        "circuit_breaker": status,
        "timestamp": datetime.now().isoformat()
    }

@app.post("/api/circuit-breaker/reset")
async def reset_circuit_breaker():
    """Manually reset the circuit breaker"""
    if not alert_handler or not hasattr(alert_handler, "error_recovery"):
        return JSONResponse(
            status_code=503,
            content={"error": "Service unavailable"}
        )
        
    success = await alert_handler.error_recovery.reset_circuit_breaker()
    
    return {
        "success": success,
        "message": "Circuit breaker reset successfully" if success else "Failed to reset circuit breaker",
        "circuit_breaker": await alert_handler.error_recovery.get_circuit_breaker_status(),
        "timestamp": datetime.now().isoformat()
    }
        
@app.get("/api/account")
async def get_account_info():
    """Get account information with comprehensive summary"""
    try:
        success, account_info = await get_account_summary()
        
        if not success:
            return JSONResponse(
                status_code=400,
                content={"error": "Failed to get account information"}
            )
            
        # Extract key metrics
        margin_rate = float(account_info.get("marginRate", "0"))
        margin_available = float(account_info.get("marginAvailable", "0"))
        margin_used = float(account_info.get("marginUsed", "0"))
        balance = float(account_info.get("balance", "0"))
        
        # Calculate margin utilization
        margin_utilization = (margin_used / balance) * 100 if balance > 0 else 0
        
        # Additional information for risk context
        daily_pnl = 0
        if alert_handler:
            daily_pnl = await alert_handler.position_tracker.get_daily_pnl()
            
        return {
            "account_id": account_info.get("id"),
            "balance": balance,
            "currency": account_info.get("currency"),
            "margin_available": margin_available,
            "margin_used": margin_used,
            "margin_rate": margin_rate,
            "margin_utilization": round(margin_utilization, 2),
            "open_position_count": len(account_info.get("positions", [])),
            "daily_pnl": daily_pnl,
            "last_updated": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting account info: {str(e)}")
        return JSONResponse(
            status_code=500, 
            content={"error": f"Internal server error: {str(e)}"}
        )

@app.get("/api/positions")
async def get_positions_info():
    """Get all tracked positions with additional information"""
    try:
        # Get positions from OANDA
        success, oanda_positions = await get_open_positions()
        
        # Get tracked positions
        tracked_positions = {}
        if alert_handler and alert_handler.position_tracker:
            tracked_positions = await alert_handler.position_tracker.get_all_positions()
            
        positions_data = {}
        
        # Process OANDA positions
        if success:
            for pos in oanda_positions.get("positions", []):
                symbol = pos["instrument"]
                
                # Determine position direction
                long_units = float(pos.get("long", {}).get("units", 0))
                short_units = float(pos.get("short", {}).get("units", 0))
                
                direction = "LONG" if long_units > 0 else "SHORT"
                units = long_units if direction == "LONG" else abs(short_units)
                
                # Get current price
                current_price = await get_current_price(symbol, direction)
                
                # Get tracked data if available
                tracked_data = tracked_positions.get(symbol, {})
                
                # Get risk management data if available
                risk_data = {}
                if alert_handler and symbol in alert_handler.risk_manager.positions:
                    position = alert_handler.risk_manager.positions[symbol]
                    risk_data = {
                        "stop_loss": position.get("stop_loss"),
                        "take_profits": position.get("take_profits", {}),
                        "trailing_stop": position.get("trailing_stop")
                    }
                
                # Calculate P&L
                unrealized_pl = float(pos.get("long" if direction == "LONG" else "short", {}).get("unrealizedPL", 0))
                entry_price = tracked_data.get("entry_price") or float(pos.get("long" if direction == "LONG" else "short", {}).get("averagePrice", 0))
                
                # Get trade duration
                entry_time = tracked_data.get("entry_time")
                duration = None
                if entry_time:
                    now = datetime.now(timezone('Asia/Bangkok'))
                    duration = (now - entry_time).total_seconds() / 3600  # Hours
                
                positions_data[symbol] = {
                    "symbol": symbol,
                    "direction": direction,
                    "units": units,
                    "entry_price": entry_price,
                    "current_price": current_price,
                    "unrealized_pl": unrealized_pl,
                    "timeframe": tracked_data.get("timeframe", "Unknown"),
                    "entry_time": entry_time.isoformat() if entry_time else None,
                    "duration_hours": round(duration, 2) if duration else None,
                    "risk_data": risk_data
                }
        
        return {
            "positions": list(positions_data.values()),
            "count": len(positions_data),
            "tracking_available": alert_handler is not None and alert_handler._initialized,
            "last_updated": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting positions info: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}"}
        )

@app.get("/api/market/{instrument}")
async def get_market_info(instrument: str, timeframe: str = "H1"):
    """Get market information for an instrument"""
    try:
        instrument = standardize_symbol(instrument)
        
        # Check if instrument is tradeable
        tradeable, reason = is_instrument_tradeable(instrument)
        
        # Get current price
        buy_price = await get_current_price(instrument, "BUY")
        sell_price = await get_current_price(instrument, "SELL")
        
        # Get market condition if volatility monitor is available
        market_condition = "NORMAL"
        if alert_handler and alert_handler.volatility_monitor:
            await alert_handler.volatility_monitor.update_volatility(
                instrument, 0.001, timeframe
            )
            market_condition = await alert_handler.volatility_monitor.get_market_condition(instrument)
        
        # Get market structure if available
        structure_data = {}
        if alert_handler and alert_handler.market_structure:
            try:
                structure = await alert_handler.market_structure.analyze_market_structure(
                    instrument, timeframe, buy_price, buy_price, buy_price
                )
                structure_data = {
                    "nearest_support": structure.get("nearest_support"),
                    "nearest_resistance": structure.get("nearest_resistance"),
                    "support_levels": structure.get("support_levels", []),
                    "resistance_levels": structure.get("resistance_levels", [])
                }
            except Exception as e:
                logger.warning(f"Error getting market structure: {str(e)}")
        
        # Market session information
        current_time = datetime.now(timezone('Asia/Bangkok'))
        
        # Create response
        return {
            "instrument": instrument,
            "timestamp": current_time.isoformat(),
            "tradeable": tradeable,
            "reason": reason if not tradeable else None,
            "prices": {
                "buy": buy_price,
                "sell": sell_price,
                "spread": round(abs(buy_price - sell_price), 5)
            },
            "market_condition": market_condition,
            "market_structure": structure_data,
            "current_session": get_current_market_session(current_time)
        }
    except Exception as e:
        logger.error(f"Error getting market info: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}"}
        )

@app.post("/api/alerts")
async def handle_alert(
    alert_data: AlertData,
    background_tasks: BackgroundTasks,
    request: Request
):
    """Process trading alerts with improved error handling and non-blocking execution"""
    request_id = str(uuid.uuid4())
    
    try:
        # Convert to dict for logging and processing
        alert_dict = alert_data.dict()
        logger.info(f"[{request_id}] Received alert: {json.dumps(alert_dict, indent=2)}")
        
        # Check for missing alert handler
        if not alert_handler:
            logger.error(f"[{request_id}] Alert handler not initialized")
            return JSONResponse(
                status_code=503,
                content={"error": "Service unavailable", "request_id": request_id}
            )
        
        # Process alert in the background
        background_tasks.add_task(
            alert_handler.process_alert,
            alert_dict
        )
        
        return {
            "message": "Alert received and processing started",
            "request_id": request_id,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"[{request_id}] Error processing alert: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}", "request_id": request_id}
        )

@handle_async_errors
async def execute_trade(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Execute trade with improved retry logic and error handling"""
    request_id = str(uuid.uuid4())
    instrument = standardize_symbol(alert_data['symbol'])
    
    # Add detailed logging at the beginning
    logger.info(f"[{request_id}] Executing trade for {instrument} - Action: {alert_data['action']}")
    
    try:
        # Normalize timeframe format - ensure it exists and is properly formatted
        if 'timeframe' not in alert_data:
            logger.warning(f"[{request_id}] No timeframe provided in alert data, using default")
            alert_data['timeframe'] = "15M"  # Default timeframe
        else:
            original_tf = alert_data['timeframe']
            alert_data['timeframe'] = ensure_proper_timeframe(alert_data['timeframe'])
            logger.info(f"[{request_id}] Normalized timeframe from {original_tf} to {alert_data['timeframe']}")
        
        # Calculate size and get current price
        balance = await get_account_balance(alert_data.get('account', config.oanda_account))
        units, precision = await calculate_trade_size(instrument, alert_data['percentage'], balance)
        if alert_data['action'].upper() == 'SELL':
            units = -abs(units)
            
        # Get current price for stop loss and take profit calculations
        current_price = await get_current_price(instrument, alert_data['action'])
        
        # Calculate stop loss and take profit levels
        atr = await get_atr(instrument, alert_data['timeframe'])
        instrument_type = get_instrument_type(instrument)
        
        # Get ATR multiplier based on timeframe and instrument
        atr_multiplier = get_atr_multiplier(instrument_type, alert_data['timeframe'])
        
        # Set price precision based on instrument
        # Most forex pairs use 5 decimal places, except JPY pairs which use 3
        price_precision = 3 if "JPY" in instrument else 5
        
        # Calculate stop loss and take profit levels with proper rounding
        if alert_data['action'].upper() == 'BUY':
            stop_loss = round(current_price - (atr * atr_multiplier), price_precision)
            take_profits = [
                round(current_price + (atr * atr_multiplier), price_precision),  # 1:1
                round(current_price + (atr * atr_multiplier * 2), price_precision),  # 2:1
                round(current_price + (atr * atr_multiplier * 3), price_precision)  # 3:1
            ]
        else:  # SELL
            stop_loss = round(current_price + (atr * atr_multiplier), price_precision)
            take_profits = [
                round(current_price - (atr * atr_multiplier), price_precision),  # 1:1
                round(current_price - (atr * atr_multiplier * 2), price_precision),  # 2:1
                round(current_price - (atr * atr_multiplier * 3), price_precision)  # 3:1
            ]
        
        # Create order data with stop loss and take profit using rounded values
        order_data = {
            "order": {
                "type": alert_data['orderType'],
                "instrument": instrument,
                "units": str(units),
                "timeInForce": alert_data['timeInForce'],
                "positionFill": "DEFAULT",
                "stopLossOnFill": {
                    "price": str(stop_loss),
                    "timeInForce": "GTC",
                    "triggerMode": "TOP_OF_BOOK"
                },
                "takeProfitOnFill": {
                    "price": str(take_profits[0]),  # First take profit level
                    "timeInForce": "GTC",
                    "triggerMode": "TOP_OF_BOOK"
                }
            }
        }
        
        # Add trailing stop if configured, also with proper rounding
        if alert_data.get('use_trailing_stop', True):
            trailing_distance = round(atr * atr_multiplier, price_precision)
            order_data["order"]["trailingStopLossOnFill"] = {
                "distance": str(trailing_distance),
                "timeInForce": "GTC",
                "triggerMode": "TOP_OF_BOOK"
            }
        
        # Log the order details for debugging
        logger.info(f"[{request_id}] Order data: {json.dumps(order_data)}")
        
        # Get session and API URL
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{alert_data.get('account', config.oanda_account)}/orders"
        
        # Execute trade with retries
        retries = 0
        while retries < config.max_retries:
            try:
                async with session.post(url, json=order_data, timeout=HTTP_REQUEST_TIMEOUT) as response:
                    response_text = await response.text()
                    logger.info(f"[{request_id}] Response status: {response.status}, Response: {response_text}")
                    
                    if response.status == 201:
                        result = json.loads(response_text)
                        logger.info(f"[{request_id}] Trade executed successfully with stops: {result}")
                        return True, result
                    
                    # Extract and log the specific error for better debugging
                    try:
                        error_data = json.loads(response_text)
                        error_code = error_data.get("errorCode", "UNKNOWN_ERROR")
                        error_message = error_data.get("errorMessage", "Unknown error")
                        logger.error(f"[{request_id}] OANDA error: {error_code} - {error_message}")
                    except:
                        pass
                    
                    # Handle error responses
                    if "RATE_LIMIT" in response_text:
                        await asyncio.sleep(60)  # Longer wait for rate limits
                    elif "MARKET_HALTED" in response_text:
                        return False, {"error": "Market is halted"}
                    else:
                        delay = config.base_delay * (2 ** retries)
                        await asyncio.sleep(delay)
                    
                    logger.warning(f"[{request_id}] Retry {retries + 1}/{config.max_retries}")
                    retries += 1
                    
            except aiohttp.ClientError as e:
                logger.error(f"[{request_id}] Network error: {str(e)}")
                if retries < config.max_retries - 1:
                    await asyncio.sleep(config.base_delay * (2 ** retries))
                    retries += 1
                    continue
                return False, {"error": f"Network error: {str(e)}"}
        
        return False, {"error": "Maximum retries exceeded"}
        
    except Exception as e:
        logger.error(f"[{request_id}] Error executing trade: {str(e)}")
        return False, {"error": str(e)} 

async def get_atr(instrument: str, timeframe: str) -> float:
    """Get ATR value with timeframe normalization"""
    # Normalize the timeframe format
    normalized_timeframe = ensure_proper_timeframe(timeframe)
    logger.debug(f"ATR calculation: Normalized timeframe from {timeframe} to {normalized_timeframe}")
    
    instrument_type = get_instrument_type(instrument)
    
    # Default ATR values by timeframe and instrument type
    default_atr_values = {
        "FOREX": {
            "15M": 0.0010,  # 10 pips
            "1H": 0.0025,   # 25 pips
            "4H": 0.0050,   # 50 pips
            "D": 0.0100     # 100 pips
        },
        "STOCK": {
            "15M": 0.01,    # 1% for stocks
            "1H": 0.02,     # 2% for stocks
            "4H": 0.03,     # 3% for stocks
            "D": 0.05       # 5% for stocks
        },
        "COMMODITY": {
            "15M": 0.05,    # 0.05% for commodities
            "1H": 0.10,     # 0.1% for commodities
            "4H": 0.20,     # 0.2% for commodities
            "D": 0.50       # 0.5% for commodities
        }
    }
    
    # Get the ATR value for this instrument and timeframe
    return default_atr_values[instrument_type].get(normalized_timeframe, default_atr_values[instrument_type]["1H"]) 

async def get_atr(instrument: str, timeframe: str) -> float:
    """Get ATR value with timeframe normalization"""
    # Normalize the timeframe format
    normalized_timeframe = ensure_proper_timeframe(timeframe)
    logger.debug(f"ATR calculation: Normalized timeframe from {timeframe} to {normalized_timeframe}")
    
    instrument_type = get_instrument_type(instrument)
    
    # Default ATR values by timeframe and instrument type
    default_atr_values = {
        "FOREX": {
            "15M": 0.0010,  # 10 pips
            "1H": 0.0025,   # 25 pips
            "4H": 0.0050,   # 50 pips
            "D": 0.0100     # 100 pips
        },
        "STOCK": {
            "15M": 0.01,    # 1% for stocks
            "1H": 0.02,     # 2% for stocks
            "4H": 0.03,     # 3% for stocks
            "D": 0.05       # 5% for stocks
        },
        "COMMODITY": {
            "15M": 0.05,    # 0.05% for commodities
            "1H": 0.10,     # 0.1% for commodities
            "4H": 0.20,     # 0.2% for commodities
            "D": 0.50       # 0.5% for commodities
        }
    }
    
    # Get the ATR value for this instrument and timeframe
    return default_atr_values[instrument_type].get(normalized_timeframe, default_atr_values[instrument_type]["1H"])

@app.post("/api/close")
async def close_position_endpoint(close_data: Dict[str, Any], request: Request):
    """Close a position with detailed result reporting"""
    request_id = str(uuid.uuid4())
    
    try:
        logger.info(f"[{request_id}] Close position request: {json.dumps(close_data, indent=2)}")
        
        success, result = await close_position(close_data)
        
        if success:
            # If using alert handler, clear the position
            if alert_handler and alert_handler.position_tracker:
                await alert_handler.position_tracker.clear_position(close_data['symbol'])
                if alert_handler.risk_manager:
                    await alert_handler.risk_manager.clear_position(close_data['symbol'])
                    
            return {
                "success": True,
                "message": "Position closed successfully",
                "transaction_id": result.get('longOrderFillTransaction', {}).get('id') or
                               result.get('shortOrderFillTransaction', {}).get('id'),
                "request_id": request_id
            }
        else:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "message": "Failed to close position",
                    "request_id": request_id,
                    "error": result.get('error', 'Unknown error')
                }
            )
    except Exception as e:
        logger.error(f"[{request_id}] Error closing position: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}", "request_id": request_id}
        )

@app.post("/api/config")
async def update_config_endpoint(config_data: Dict[str, Any], request: Request):
    """Update trading configuration"""
    try:
        if not alert_handler:
            return JSONResponse(
                status_code=503,
                content={"error": "Service unavailable"}
            )
            
        success = await alert_handler.update_config(config_data)
        
        if success:
            return {
                "success": True,
                "message": "Configuration updated successfully"
            }
        else:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "message": "Failed to update configuration"
                }
            )
    except Exception as e:
        logger.error(f"Error updating configuration: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}"}
        )

@app.post("/tradingview")
async def tradingview_webhook(
    request: Request,
    background_tasks: BackgroundTasks
):
    """Process TradingView webhook alerts"""
    request_id = str(uuid.uuid4())
    
    try:
        # Get the raw JSON payload
        payload = await request.json()
        logger.info(f"[{request_id}] Received TradingView webhook: {json.dumps(payload, indent=2)}")
        
        # Map TradingView fields to your AlertData model
        alert_data = {
            "symbol": payload.get("symbol", ""),
            "action": payload.get("action", ""),
            "timeframe": payload.get("timeframe", "15M"),
            "orderType": payload.get("orderType", "MARKET"),
            "timeInForce": payload.get("timeInForce", "FOK"),
            "percentage": float(payload.get("percentage", 15.0)),
            "account": payload.get("account", config.oanda_account),
            "comment": payload.get("comment", "")
        }
        
        # Process alert in the background
        if alert_handler:
            background_tasks.add_task(
                alert_handler.process_alert,
                alert_data
            )
            
            return {
                "message": "TradingView alert received and processing started",
                "request_id": request_id,
                "timestamp": datetime.now().isoformat()
            }
        else:
            return JSONResponse(
                status_code=503,
                content={"error": "Service unavailable", "request_id": request_id}
            )
    except Exception as e:
        logger.error(f"[{request_id}] Error processing TradingView webhook: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}", "request_id": request_id}
        )

##############################################################################
# Main Application Entry Point
##############################################################################

def start():
    """Start the application using uvicorn"""
    import uvicorn
    setup_logging()
    logger.info(f"Starting application in {config.environment} mode")
    
    host = config.host
    port = config.port
    
    logger.info(f"Server starting at {host}:{port}")
    uvicorn.run(
        "app:app",
        host=host,
        port=port,
        reload=config.environment == "development"
    )

if __name__ == "__main__":
    start()
