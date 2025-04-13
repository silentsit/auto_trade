"""
FX Trading Bridge - Enhanced Trading System

This is a comprehensive trading system with dynamic configuration management,
robust position tracking, and sophisticated risk management.

It combines the best features from original.py and main.py.
"""

import os
import sys
import json
import signal
import logging
import asyncio
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any, Callable, TypeVar, Union, ParamSpec
from decimal import Decimal
from functools import wraps
from contextlib import asynccontextmanager
import time
from enum import Enum, auto
import re
import uuid
import random
import statistics
import math
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from collections import defaultdict, deque

# Third-party imports
import aiohttp
from fastapi import FastAPI, Request, Response, BackgroundTasks, HTTPException, Depends, Body, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator, ValidationError, root_validator
import uvicorn
try:
    from sklearn.cluster import KMeans
except ImportError:
    # For environments without scikit-learn
    KMeans = None
try:
    from prometheus_client import Counter, Histogram
except ImportError:
    # For environments without prometheus
    Counter = Histogram = None

# Import Configuration API - new addition for dynamic configuration
try:
    # Try importing from fx.config first
    from fx.config import (
        integrate_with_app,
        get_config_value,
        update_config_value,
        get_config,
        import_legacy_settings
    )
except ImportError:
    # Fallback implementation if fx.config isn't available
    import os
    import json
    import logging
    from typing import Any, Dict, Optional
    
    logger = logging.getLogger("config_fallback")
    
    # Config storage
    _config_values = {}
    _config_file = "config.json"
    
    def get_config_value(key: str, default: Any = None) -> Any:
        """Get a configuration value by key."""
        # First check if it's in environment variables
        env_val = os.environ.get(key)
        if env_val is not None:
            return env_val
            
        # Then check in-memory storage
        if key in _config_values:
            return _config_values[key]
            
        # Finally check config file
        try:
            if os.path.exists(_config_file):
                with open(_config_file, 'r') as f:
                    config = json.load(f)
                    if key in config:
                        return config[key]
        except Exception as e:
            logger.error(f"Error reading config file: {str(e)}")
        
        return default
    
    def update_config_value(key: str, value: Any) -> bool:
        """Update a configuration value."""
        _config_values[key] = value
        return True
        
    def get_config():
        """Get the current configuration."""
        return _config_values.copy()
        
    def import_legacy_settings():
        """Import settings from legacy format."""
        return {}
        
    def integrate_with_app(app) -> None:
        """Integrate configuration with the FastAPI app."""
        @app.get("/api/config")
        async def get_config_endpoint(section: Optional[str] = None):
            """API endpoint to retrieve configuration"""
            # For security, only return non-sensitive config items
            safe_config = {
                "app": {
                    "version": get_config_value("VERSION", "1.0.0"),
                    "environment": get_config_value("ENVIRONMENT", "production")
                },
                "features": {
                    "enable_advanced_loss_management": get_config_value("ENABLE_ADVANCED_LOSS_MANAGEMENT", True),
                    "enable_multi_stage_tp": get_config_value("ENABLE_MULTI_STAGE_TP", True),
                    "enable_market_structure_analysis": get_config_value("ENABLE_MARKET_STRUCTURE_ANALYSIS", True)
                },
                "risk": {
                    "max_daily_loss": get_config_value("MAX_DAILY_LOSS", 0.20),
                    "max_risk_percentage": get_config_value("MAX_RISK_PERCENTAGE", 2.0)
                }
            }
            
            if section and section in safe_config:
                return {"status": "success", "data": safe_config[section]}
            
            return {"status": "success", "data": safe_config}
            
    logger.info("Using fallback configuration implementation")

# Import custom components
from multi_stage_tp_manager import MultiStageTakeProfitManager
from dynamic_exit_manager import DynamicExitManager
from trading_config import TradingConfig
from market_structure_analyzer import MarketStructureAnalyzer
from advanced_loss_management import AdvancedLossManagement
from exchange_adapter import ExchangeAdapterFactory, OandaAdapter, BinanceAdapter
# Import backtest engine
from backtest_engine import BacktestEngine, BacktestConfig, BacktestExchangeAdapter
# Import data downloader for backtesting
from data_downloader import DataDownloader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("trading_bot.log")
    ]
)
logger = logging.getLogger("fx-trading-bridge")

# Type variables for function annotations
P = ParamSpec('P')
T = TypeVar('T')

# Error handling decorators
def handle_async_errors(func: Callable[P, T]) -> Callable[P, T]:
    """Decorator for handling errors in async functions"""
    @wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            logger.error(traceback.format_exc())
            # Use error recovery system if available and this is not an internal error handler
            if 'error_recovery_system' in globals() and func.__name__ != 'handle_error':
                request_id = str(uuid.uuid4())
                context = {'args': args, 'kwargs': kwargs, 'function': func.__name__}
                await error_recovery_system.handle_error(request_id, func.__name__, e, context)
            raise
    return wrapper

def handle_sync_errors(func: Callable[P, T]) -> Callable[P, T]:
    """Decorator for handling errors in synchronous functions"""
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    return wrapper

# Exception classes
class TradingError(Exception):
    """Base class for trading related errors"""

class MarketError(TradingError):
    """Error related to market conditions"""

class OrderError(TradingError):
    """Error related to order execution"""

class CustomValidationError(TradingError):
    """Error related to data validation"""

# Error Recovery System
class ErrorRecoverySystem:
    """System to handle errors gracefully and implement resilience strategies"""
    
    def __init__(self):
        """Initialize the error recovery system"""
        self.errors = {}
        self.recovery_strategies = {
            "connection": self._handle_connection_error,
            "validation": self._handle_validation_error,
            "market": self._handle_market_error,
            "order": self._handle_order_error,
            "unknown": self._handle_unknown_error
        }
        self.max_retries = get_config_value("error_recovery", "max_retries", 3)
        self.backoff_factor = get_config_value("error_recovery", "backoff_factor", 2.0)
        self.grace_period = get_config_value("error_recovery", "grace_period", 300)  # 5 minutes
        self.running = False
        self.cleanup_task = None
        logger.info("Error Recovery System initialized")
    
    @handle_async_errors
    async def start(self):
        """Start the error recovery system"""
        if self.running:
            return
        
        self.running = True
        self.cleanup_task = asyncio.create_task(self._cleanup_old_errors())
        logger.info("Error Recovery System started")
    
    @handle_async_errors
    async def stop(self):
        """Stop the error recovery system"""
        if not self.running:
            return
        
        self.running = False
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
        logger.info("Error Recovery System stopped")
    
    @handle_async_errors
    async def handle_error(
        self, 
        request_id: str, 
        operation: str, 
        error: Exception, 
        context: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Handle an error and attempt recovery
        
        Args:
            request_id: Unique identifier for this error/request
            operation: The operation that failed
            error: The exception that was raised
            context: Additional context about the operation
            
        Returns:
            Dictionary with recovery information
        """
        # Record error
        error_type = self._categorize_error(error)
        timestamp = datetime.now(timezone.utc)
        
        error_record = {
            "id": request_id,
            "operation": operation,
            "type": error_type,
            "message": str(error),
            "timestamp": timestamp,
            "context": context or {},
            "recovery_attempts": 0,
            "resolved": False
        }
        
        # Store error
        self.errors[request_id] = error_record
        
        # Log error
        logger.error(f"Error in {operation}: {str(error)} (ID: {request_id})")
        
        # Execute recovery strategy
        recovery_strategy = self.recovery_strategies.get(error_type, self.recovery_strategies["unknown"])
        recovery_result = await recovery_strategy(request_id, error, context)
        
        # Update error record
        error_record["recovery_attempts"] += 1
        error_record["last_recovery_attempt"] = datetime.now(timezone.utc)
        error_record["recovery_result"] = recovery_result
        
        if recovery_result.get("success", False):
            error_record["resolved"] = True
        
        return recovery_result
    
    def _categorize_error(self, error: Exception) -> str:
        """Categorize the error type"""
        error_class = error.__class__.__name__
        error_msg = str(error).lower()
        
        # Connection errors
        if any(x in error_class for x in ["ConnectionError", "Timeout", "ConnectionTimeout"]):
            return "connection"
        elif "connection" in error_msg or "timeout" in error_msg:
            return "connection"
        
        # Validation errors
        if error_class in ["ValidationError", "CustomValidationError"]:
            return "validation"
        elif "invalid" in error_msg or "required" in error_msg:
            return "validation"
        
        # Market errors
        if error_class == "MarketError":
            return "market"
        elif "market" in error_msg or "liquidity" in error_msg or "spread" in error_msg:
            return "market"
        
        # Order errors
        if error_class == "OrderError":
            return "order"
        elif "order" in error_msg or "position" in error_msg or "trade" in error_msg:
            return "order"
        
        # Default: unknown
        return "unknown"
    
    @handle_async_errors
    async def _handle_connection_error(
        self, 
        request_id: str, 
        error: Exception, 
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Handle connection errors with retry logic"""
        error_record = self.errors[request_id]
        attempt = error_record["recovery_attempts"] + 1
        
        # Check if we should retry
        if attempt <= self.max_retries:
            # Calculate backoff delay
            delay = self.backoff_factor ** (attempt - 1)
            
            logger.info(f"Connection error recovery: Will retry in {delay}s (attempt {attempt}/{self.max_retries})")
            
            # Wait for backoff period
            await asyncio.sleep(delay)
            
            # Attempt to reconnect
            try:
                # Force new session
                await get_session(force_new=True)
                
                return {
                    "success": True,
                    "action": "reconnect",
                    "message": f"Successfully reconnected after {attempt} attempts",
                    "retry": False
                }
            except Exception as e:
                logger.error(f"Reconnection attempt failed: {str(e)}")
                
                return {
                    "success": False,
                    "action": "reconnect",
                    "message": f"Reconnection failed: {str(e)}",
                    "retry": attempt < self.max_retries
                }
        else:
            logger.warning(f"Connection error recovery: Max retries ({self.max_retries}) reached for {request_id}")
            
            return {
                "success": False,
                "action": "abort",
                "message": f"Max retries reached ({self.max_retries})",
                "retry": False
            }
    
    @handle_async_errors
    async def _handle_validation_error(
        self, 
        request_id: str, 
        error: Exception, 
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Handle validation errors"""
        # Validation errors typically can't be automatically recovered
        error_msg = str(error)
        error_details = {}
        
        # Try to extract field-specific errors
        if hasattr(error, "errors") and isinstance(error.errors, list):
            for err in error.errors():
                loc = ".".join(str(l) for l in err.get("loc", []))
                error_details[loc] = err.get("msg", "Unknown validation error")
        
        # Log detailed validation error
        logger.error(f"Validation error details: {error_details}")
        
        return {
            "success": False,
            "action": "report",
            "message": f"Validation error: {error_msg}",
            "details": error_details,
            "retry": False
        }
    
    @handle_async_errors
    async def _handle_market_error(
        self, 
        request_id: str, 
        error: Exception, 
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Handle market-related errors"""
        error_msg = str(error).lower()
        
        # Check for specific market conditions
        if "spread" in error_msg:
            # Spread too wide - wait and retry
            return {
                "success": False,
                "action": "wait",
                "message": "Market spread too wide, will retry later",
                "retry": True,
                "retry_after": 300  # 5 minutes
            }
        elif "liquidity" in error_msg:
            # Low liquidity - wait for market hours
            return {
                "success": False,
                "action": "wait",
                "message": "Low market liquidity, will retry during market hours",
                "retry": True,
                "retry_after": 3600  # 1 hour
            }
        else:
            # General market error
            return {
                "success": False,
                "action": "report",
                "message": f"Market error: {error_msg}",
                "retry": False
            }
    
    @handle_async_errors
    async def _handle_order_error(
        self, 
        request_id: str, 
        error: Exception, 
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Handle order-related errors"""
        error_msg = str(error).lower()
        
        # Determine if the error might be temporary
        is_temporary = any(x in error_msg for x in [
            "rate limit", "busy", "unavailable", "temporary", "try again"
        ])
        
        if is_temporary:
            error_record = self.errors[request_id]
            attempt = error_record["recovery_attempts"] + 1
            
            if attempt <= self.max_retries:
                delay = self.backoff_factor ** (attempt - 1)
                
                return {
                    "success": False,
                    "action": "retry",
                    "message": f"Temporary order error, will retry in {delay}s",
                    "retry": True,
                    "retry_after": delay
                }
        
        # For non-temporary or max retries reached
        return {
            "success": False,
            "action": "report",
            "message": f"Order error: {error_msg}",
            "retry": False
        }
    
    @handle_async_errors
    async def _handle_unknown_error(
        self, 
        request_id: str, 
        error: Exception, 
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Handle unknown errors"""
        # Unknown errors are generally not retried automatically
        return {
            "success": False,
            "action": "report",
            "message": f"Unknown error: {str(error)}",
            "retry": False
        }
    
    @handle_async_errors
    async def _cleanup_old_errors(self):
        """Background task to clean up old resolved errors"""
        try:
            while self.running:
                # Sleep for the grace period
                await asyncio.sleep(self.grace_period)
                
                # Remove old resolved errors
                now = datetime.now(timezone.utc)
                to_remove = []
                
                for error_id, error in self.errors.items():
                    # If error is resolved and older than grace period
                    if error["resolved"]:
                        error_time = error["timestamp"]
                        if isinstance(error_time, str):
                            error_time = datetime.fromisoformat(error_time)
                        
                        age = (now - error_time).total_seconds()
                        if age > self.grace_period:
                            to_remove.append(error_id)
                
                # Remove old errors
                for error_id in to_remove:
                    del self.errors[error_id]
                
                if to_remove:
                    logger.info(f"Cleaned up {len(to_remove)} resolved errors")
        except asyncio.CancelledError:
            logger.info("Error cleanup task cancelled")
        except Exception as e:
            logger.error(f"Error in error cleanup task: {str(e)}")
            logger.error(traceback.format_exc())
    
    @handle_async_errors
    async def get_active_errors(self) -> List[Dict[str, Any]]:
        """Get list of active (unresolved) errors"""
        return [
            error for error in self.errors.values()
            if not error["resolved"]
        ]
    
    @handle_async_errors
    async def get_error_stats(self) -> Dict[str, Any]:
        """Get error statistics"""
        total = len(self.errors)
        resolved = sum(1 for e in self.errors.values() if e["resolved"])
        unresolved = total - resolved
        
        by_type = {}
        for error in self.errors.values():
            error_type = error["type"]
            if error_type not in by_type:
                by_type[error_type] = {
                    "total": 0,
                    "resolved": 0,
                    "unresolved": 0
                }
            
            by_type[error_type]["total"] += 1
            if error["resolved"]:
                by_type[error_type]["resolved"] += 1
            else:
                by_type[error_type]["unresolved"] += 1
        
        return {
            "total": total,
            "resolved": resolved,
            "unresolved": unresolved,
            "by_type": by_type
        }

# Circuit Breaker Pattern
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
            logger.info(f"Circuit breaker auto-reset after {self.cooldown_seconds} seconds cooldown")
            
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

# Settings class for environment variables and configuration
class Settings(BaseModel):
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
    binance_api_key: Optional[str] = Field(
        default="",
        alias='BINANCE_API_KEY'
    )
    binance_api_secret: Optional[str] = Field(
        default="",
        alias='BINANCE_API_SECRET'
    )
    binance_testnet: bool = Field(
        default=True,
        alias='BINANCE_TESTNET'
    )
    allowed_origins: str = "*"
    connect_timeout: int = 10
    read_timeout: int = 30
    total_timeout: int = 45
    max_simultaneous_connections: int = 100
    spread_threshold_forex: float = 0.001
    spread_threshold_crypto: float = 0.008
    max_retries: int = 3
    base_delay: float = 1.0
    base_position: int = 5000
    max_daily_loss: float = 0.20  # 20% max daily loss
    host: str = "0.0.0.0"
    port: int = 10000
    environment: str = "production"
    max_requests_per_minute: int = 100
    trade_24_7: bool = True  # For crypto and 24/7 markets
    default_risk_percentage: float = 2.0
    max_risk_percentage: float = 5.0
    max_portfolio_heat: float = 15.0

    class Config:
        env_file = '.env'
        case_sensitive = True

# Global instances
risk_manager = None  # Will be initialized during startup
market_analysis = None  # Will be initialized during startup
position_tracker = None
error_recovery_system = None
client_session = None
settings = None

# Advanced components
circuit_breaker = None
volatility_monitor = None
advanced_position_sizer = None
lorentzian_classifier = None
time_exit_manager = None
multi_stage_tp_manager = None  # New component for managing multiple take profit targets
alert_handler = None
correlation_analyzer = None  # Correlation analysis component
market_structure_analyzer = None  # Advanced market structure analysis
dynamic_exit_manager = None  # Dynamic exit management system
advanced_loss_management = None  # Advanced loss management system
exchange_adapter = None  # Exchange-specific adapter
backtest_engine = None  # New global variable for backtest engine

app = FastAPI(title="FX Trading Bridge", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# JSON Formatter for structured logging
class JSONFormatter(logging.Formatter):
    """Format log records as JSON for structured logging"""
    def format(self, record):
        log_data = {
            "timestamp": datetime.now().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "path": record.pathname,
            "line": record.lineno
        }
        
        # Add exception info if available
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": traceback.format_exception(*record.exc_info)
            }
            
        # Add extra fields if available
        if hasattr(record, 'props'):
            log_data.update(record.props)
            
        return json.dumps(log_data)

def setup_logging():
    """Setup structured logging"""
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setFormatter(JSONFormatter())
    
    # Set log level based on environment
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    root_logger.setLevel(getattr(logging, log_level))
    
    logger.info("Logging initialized", extra={"props": {"environment": os.environ.get("ENVIRONMENT", "production")}})

# OANDA session management
_session = None
_session_created = None
_session_lock = asyncio.Lock()

@handle_async_errors
async def get_session(force_new: bool = False) -> aiohttp.ClientSession:
    """Get or create an aiohttp ClientSession"""
    global _session, _session_created
    
    # Lock to prevent multiple sessions from being created simultaneously
    async with _session_lock:
        # Check if session exists and is valid
        if _session is None or _session.closed or force_new:
            # Close existing session if needed
            if _session is not None and not _session.closed:
                await _session.close()
            
            # Connection timeout - from config
            connect_timeout = get_config_value("connection", "connect_timeout", 10)
            read_timeout = get_config_value("connection", "read_timeout", 30)
            total_timeout = get_config_value("connection", "total_timeout", 45)
            
            # Create a new session with timeouts
            timeout = aiohttp.ClientTimeout(
                connect=connect_timeout,
                sock_read=read_timeout,
                total=total_timeout
            )
            
            _session = aiohttp.ClientSession(timeout=timeout)
            _session_created = datetime.now()
            logger.info("Created new aiohttp ClientSession")
        
        return _session

# Session cleanup
@handle_async_errors
async def cleanup_stale_sessions():
    """Close and recreate sessions that have been open too long"""
    global _session, _session_created
    
    if _session is not None and _session_created is not None:
        # Get session age in hours
        session_age = (datetime.now() - _session_created).total_seconds() / 3600
        
        # Recreate session if it's older than 6 hours
        if session_age > 6:
            logger.info(f"Session is {session_age:.1f} hours old, recreating")
            await get_session(force_new=True)

# Position and trade model classes
class Position:
    """
    Represents an open trading position.
    
    This is the consolidated position class that serves as the single source of truth
    for all position data across the application.
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
        metadata: Optional[dict] = None,
        broker_data: Optional[dict] = None,
        timeframe: Optional[str] = "1h"
    ):
        self.symbol = symbol
        self.action = action.upper()  # Normalize to uppercase
        self.size = float(size)
        self.entry_price = float(entry_price)
        self.current_price = float(entry_price)  # Initialize with entry price
        self.take_profit_price = float(take_profit_price) if take_profit_price is not None else None
        self.stop_loss_price = float(stop_loss_price) if stop_loss_price is not None else None
        self.position_id = position_id or str(uuid.uuid4())
        self.open_time = datetime.now(timezone.utc)
        self.updated_time = self.open_time
        self.closed = False
        self.close_time = None
        self.close_price = None
        self.close_reason = None
        self.pnl = 0.0
        self.pnl_percentage = 0.0
        self.initial_risk = self._calculate_initial_risk()
        self.metadata = metadata or {}
        self.broker_data = broker_data or {}
        self.timeframe = timeframe
        self.r_multiple = 0.0
        
        # Add tracking for partial closes
        self.initial_size = float(size)
        self.closed_size = 0.0
        self.partial_closes = []
        
        # Initialize exit plan
        self.exit_plan = {
            "tp_levels": [],
            "trailing_stop": None,
            "time_exits": []
        }
        
        # Track volatility at entry
        self.entry_atr = self.metadata.get("atr", None)
        
        # Record trade in position tracker if available
        if 'position_tracker' in globals() and position_tracker is not None:
            asyncio.create_task(position_tracker.record_position(
                self.position_id, self.symbol, self.action, self.timeframe, 
                self.entry_price, self.size, self.metadata
            ))
    
    def _calculate_initial_risk(self) -> float:
        """Calculate the initial risk for this position"""
        if self.stop_loss_price is None:
            return 0.0
            
        # Calculate risk based on direction
        if self.action == "BUY":
            risk_amount = (self.entry_price - self.stop_loss_price) / self.entry_price
        else:  # SELL
            risk_amount = (self.stop_loss_price - self.entry_price) / self.entry_price
            
        return abs(risk_amount)
    
    @handle_sync_errors
    def update_pnl(self, current_price: float) -> Tuple[float, float]:
        """
        Update position P&L based on current price
        
        Args:
            current_price: The current market price
            
        Returns:
            Tuple of (pnl, pnl_percentage)
        """
        self.current_price = float(current_price)
        self.updated_time = datetime.now(timezone.utc)
        
        # Calculate P&L based on direction
        if self.action == "BUY":
            price_diff = self.current_price - self.entry_price
        else:  # SELL
            price_diff = self.entry_price - self.current_price
            
        # Calculate absolute and percentage P&L
        self.pnl = price_diff * self.size
        self.pnl_percentage = (price_diff / self.entry_price) * 100
        
        # Calculate R multiple if we have a stop loss
        if self.stop_loss_price is not None and self.initial_risk > 0:
            if self.action == "BUY":
                self.r_multiple = (self.current_price - self.entry_price) / (self.entry_price - self.stop_loss_price)
            else:
                self.r_multiple = (self.entry_price - self.current_price) / (self.stop_loss_price - self.entry_price)
        
        return self.pnl, self.pnl_percentage
    
    @handle_sync_errors
    def modify_take_profit(self, new_price: float) -> bool:
        """
        Modify the take profit price
        
        Args:
            new_price: New take profit price
            
        Returns:
            True if successful, False otherwise
        """
        if self.closed:
            logger.warning(f"Cannot modify take profit for closed position {self.position_id}")
            return False
            
        # Validate take profit price based on direction
        if self.action == "BUY" and new_price <= self.current_price:
            logger.warning(f"Invalid take profit price {new_price} for BUY position (current: {self.current_price})")
            return False
        
        if self.action == "SELL" and new_price >= self.current_price:
            logger.warning(f"Invalid take profit price {new_price} for SELL position (current: {self.current_price})")
            return False
            
        # Update take profit price
        self.take_profit_price = float(new_price)
        self.updated_time = datetime.now(timezone.utc)
        logger.info(f"Modified take profit for position {self.position_id} to {self.take_profit_price}")
        
        return True
    
    @handle_sync_errors
    def modify_stop_loss(self, new_price: float) -> bool:
        """
        Modify the stop loss price
        
        Args:
            new_price: New stop loss price
            
        Returns:
            True if successful, False otherwise
        """
        if self.closed:
            logger.warning(f"Cannot modify stop loss for closed position {self.position_id}")
            return False
            
        # Validate stop loss price based on direction
        if self.action == "BUY" and new_price >= self.current_price:
            logger.warning(f"Invalid stop loss price {new_price} for BUY position (current: {self.current_price})")
            return False
        
        if self.action == "SELL" and new_price <= self.current_price:
            logger.warning(f"Invalid stop loss price {new_price} for SELL position (current: {self.current_price})")
            return False
            
        # Update stop loss price
        old_stop = self.stop_loss_price
        self.stop_loss_price = float(new_price)
        self.updated_time = datetime.now(timezone.utc)
        logger.info(f"Modified stop loss for position {self.position_id} from {old_stop} to {self.stop_loss_price}")
        
        return True
    
    @handle_sync_errors
    def close(self, exit_price: float, reason: str = "manual") -> float:
        """
        Close the position
        
        Args:
            exit_price: The exit price
            reason: Reason for closing (e.g., "tp", "sl", "manual")
            
        Returns:
            PnL from the closed position
        """
        if self.closed:
            logger.warning(f"Position {self.position_id} is already closed")
            return self.pnl
            
        # Update position state
        self.closed = True
        self.close_time = datetime.now(timezone.utc)
        self.close_price = float(exit_price)
        self.close_reason = reason
        self.updated_time = self.close_time
        
        # Calculate final P&L
        if self.action == "BUY":
            price_diff = self.close_price - self.entry_price
        else:  # SELL
            price_diff = self.entry_price - self.close_price
            
        self.pnl = price_diff * self.size
        self.pnl_percentage = (price_diff / self.entry_price) * 100
        
        # Calculate final R multiple
        if self.stop_loss_price is not None and self.initial_risk > 0:
            if self.action == "BUY":
                self.r_multiple = (self.close_price - self.entry_price) / (self.entry_price - self.stop_loss_price)
            else:
                self.r_multiple = (self.entry_price - self.close_price) / (self.stop_loss_price - self.entry_price)
        
        # Record trade in position tracker
        if 'position_tracker' in globals() and position_tracker is not None:
            asyncio.create_task(position_tracker.clear_position(
                self.position_id, self.pnl, self.r_multiple, self.close_reason
            ))
        
        logger.info(f"Closed position {self.position_id} at {self.close_price} with P&L: {self.pnl:.2f} ({self.pnl_percentage:.2f}%)")
        return self.pnl
    
    @handle_sync_errors
    def close_partial(self, exit_price: float, percentage: float, reason: str = "partial") -> float:
        """
        Close a portion of the position
        
        Args:
            exit_price: The exit price
            percentage: Percentage to close (0-100)
            reason: Reason for closing
            
        Returns:
            PnL from the closed portion
        """
        if self.closed:
            logger.warning(f"Position {self.position_id} is already closed")
            return 0.0
            
        # Validate percentage
        percentage = float(percentage)
        if percentage <= 0 or percentage > 100:
            logger.warning(f"Invalid percentage for partial close: {percentage}")
            return 0.0
            
        # Calculate size to close
        close_size = (percentage / 100.0) * self.size
        
        # Update position size
        old_size = self.size
        self.size -= close_size
        self.closed_size += close_size
        
        # Calculate P&L for closed portion
        if self.action == "BUY":
            price_diff = exit_price - self.entry_price
        else:  # SELL
            price_diff = self.entry_price - exit_price
            
        closed_pnl = price_diff * close_size
        
        # Record partial close
        self.partial_closes.append({
            "time": datetime.now(timezone.utc),
            "price": exit_price,
            "size": close_size,
            "pnl": closed_pnl,
            "reason": reason
        })
        
        # Update position state
        self.updated_time = datetime.now(timezone.utc)
        
        # If position is now fully closed
        if self.size <= 0:
            self.close(exit_price, reason=reason)
        
        logger.info(f"Partially closed position {self.position_id}: {percentage}% at {exit_price} with P&L: {closed_pnl:.2f}")
        return closed_pnl
    
    @handle_sync_errors
    def to_dict(self) -> dict:
        """
        Convert position to dictionary representation
        
        Returns:
            Dictionary with position data
        """
        return {
            "position_id": self.position_id,
            "symbol": self.symbol,
            "action": self.action,
            "size": self.size,
            "initial_size": self.initial_size,
            "closed_size": self.closed_size,
            "entry_price": self.entry_price,
            "current_price": self.current_price,
            "take_profit_price": self.take_profit_price,
            "stop_loss_price": self.stop_loss_price,
            "open_time": self.open_time.isoformat(),
            "updated_time": self.updated_time.isoformat(),
            "closed": self.closed,
            "close_time": self.close_time.isoformat() if self.close_time else None,
            "close_price": self.close_price,
            "close_reason": self.close_reason,
            "pnl": self.pnl,
            "pnl_percentage": self.pnl_percentage,
            "initial_risk": self.initial_risk,
            "r_multiple": self.r_multiple,
            "metadata": self.metadata,
            "broker_data": self.broker_data,
            "timeframe": self.timeframe,
            "entry_atr": self.entry_atr,
            "partial_closes": self.partial_closes,
            "exit_plan": self.exit_plan
        }
    
    @classmethod
    @handle_sync_errors
    def from_dict(cls, data: dict) -> 'Position':
        """
        Create a Position object from dictionary data
        
        Args:
            data: Dictionary with position data
            
        Returns:
            Position object
        """
        # Create position with required fields
        position = cls(
            symbol=data["symbol"],
            action=data["action"],
            size=data["size"],
            entry_price=data["entry_price"],
            take_profit_price=data.get("take_profit_price"),
            stop_loss_price=data.get("stop_loss_price"),
            position_id=data.get("position_id"),
            metadata=data.get("metadata", {}),
            broker_data=data.get("broker_data", {}),
            timeframe=data.get("timeframe", "1h")
        )
        
        # Update additional fields
        position.current_price = data.get("current_price", position.entry_price)
        position.open_time = datetime.fromisoformat(data["open_time"]) if "open_time" in data else position.open_time
        position.updated_time = datetime.fromisoformat(data["updated_time"]) if "updated_time" in data else position.updated_time
        position.closed = data.get("closed", False)
        
        if "close_time" in data and data["close_time"]:
            position.close_time = datetime.fromisoformat(data["close_time"])
        
        position.close_price = data.get("close_price")
        position.close_reason = data.get("close_reason")
        position.pnl = data.get("pnl", 0.0)
        position.pnl_percentage = data.get("pnl_percentage", 0.0)
        position.initial_risk = data.get("initial_risk", position.initial_risk)
        position.r_multiple = data.get("r_multiple", 0.0)
        position.initial_size = data.get("initial_size", position.size)
        position.closed_size = data.get("closed_size", 0.0)
        position.partial_closes = data.get("partial_closes", [])
        position.entry_atr = data.get("entry_atr")
        position.exit_plan = data.get("exit_plan", {
            "tp_levels": [],
            "trailing_stop": None,
            "time_exits": []
        })
        
        return position

class PositionTracker:
    """
    Centralized position tracking system.
    
    This class serves as the single source of truth for all positions,
    ensuring consistency across the application. It handles persistence,
    reconciliation with broker, and maintains trade statistics.
    """
    def __init__(self):
        self.positions = {}  # position_id -> Position object
        self.symbols = {}    # symbol -> list of position_ids
        self.daily_stats = {
            "date": datetime.now().date().isoformat(),
            "trades_total": 0,
            "trades_win": 0,
            "trades_loss": 0,
            "pnl_total": 0.0,
            "pnl_max": 0.0,
            "pnl_min": 0.0,
            "win_rate": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "largest_win": 0.0,
            "largest_loss": 0.0,
            "profit_factor": 0.0
        }
        self.data_file = os.path.join(os.getcwd(), "data", "positions.json")
        self.stats_file = os.path.join(os.getcwd(), "data", "trade_stats.json")
        self.initialized = False
        
        # Create data directory if it doesn't exist
        os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
        
        # Background task for position updates
        self.update_task = None
        self.running = False
    
    @handle_async_errors
    async def reconcile_positions(self):
        """Reconcile positions with broker to ensure consistency"""
        try:
            # Get open positions from broker
            success, result = await get_open_positions()
            
            if not success:
                logger.error(f"Failed to get open positions from broker: {result}")
                return
                
            broker_positions = result.get("positions", [])
            broker_position_ids = set()
            
            # Process positions from broker
            for pos in broker_positions:
                broker_id = pos.get("id")
                symbol = pos.get("instrument")
                broker_position_ids.add(broker_id)
                
                # Check if position exists locally
                if broker_id in self.positions:
                    # Update local position with broker data
                    local_pos = self.positions[broker_id]
                    local_pos.broker_data = pos
                    local_pos.current_price = pos.get("current_price", local_pos.current_price)
                    local_pos.update_pnl(local_pos.current_price)
                else:
                    # Create new position from broker data
                    logger.info(f"Found new position from broker: {symbol} ({broker_id})")
                    new_pos = Position(
                        symbol=symbol,
                        action=pos.get("direction", "BUY"),
                        size=pos.get("units", 0),
                        entry_price=pos.get("avg_price", 0),
                        take_profit_price=pos.get("take_profit"),
                        stop_loss_price=pos.get("stop_loss"),
                        position_id=broker_id,
                        broker_data=pos
                    )
                    self.positions[broker_id] = new_pos
                    
                    # Update symbol mapping
                    if symbol not in self.symbols:
                        self.symbols[symbol] = []
                    self.symbols[symbol].append(broker_id)
            
            # Check for positions that exist locally but not in broker
            for pos_id, pos in list(self.positions.items()):
                if not pos.closed and pos_id not in broker_position_ids:
                    logger.warning(f"Position {pos_id} exists locally but not in broker. Marking as closed.")
                    pos.close(pos.current_price, reason="reconciliation")
            
            logger.info(f"Position reconciliation complete: {len(self.positions)} positions tracked")
            
        except Exception as e:
            logger.error(f"Error during position reconciliation: {str(e)}")
            logger.error(traceback.format_exc())
    
    async def start(self):
        """Start position tracking"""
        if self.running:
            return
            
        self.running = True
        
        # Load saved positions
        await self._load_positions()
        await self._load_stats()
        
        # Start background update task
        self.update_task = asyncio.create_task(self._background_updates())
        logger.info("Position tracker started")
        
        # Mark as initialized
        self.initialized = True
    
    async def stop(self):
        """Stop position tracking"""
        if not self.running:
            return
            
        self.running = False
        
        # Cancel background task
        if self.update_task:
            self.update_task.cancel()
            try:
                await self.update_task
            except asyncio.CancelledError:
                pass
        
        # Save positions and stats
        await self._save_positions()
        await self._save_stats()
        
        logger.info("Position tracker stopped")
    
    @handle_async_errors
    async def record_position(
        self, 
        position_id: str, 
        symbol: str, 
        action: str, 
        timeframe: str,
        entry_price: float,
        size: float,
        metadata: dict = None
    ) -> bool:
        """
        Record a new position
        
        This method is usually called by the Position constructor but can be used
        directly if needed.
        
        Args:
            position_id: Unique ID for the position
            symbol: Trading symbol
            action: BUY or SELL
            timeframe: Trading timeframe
            entry_price: Entry price
            size: Position size
            metadata: Additional metadata
            
        Returns:
            True if successful
        """
        # If the position already exists, update it
        if position_id in self.positions:
            logger.warning(f"Position {position_id} already exists, updating metadata")
            position = self.positions[position_id]
            if metadata:
                position.metadata.update(metadata)
            return True
        
        # If called with just an ID (for an existing Position object)
        # Try to find the position in self.positions
        elif position_id and not symbol:
            for pos_id, pos in self.positions.items():
                if pos_id == position_id:
                    return True
            return False
        
        # Otherwise we need the position object to be added separately
        
        # Update symbol tracking
        if symbol not in self.symbols:
            self.symbols[symbol] = []
        self.symbols[symbol].append(position_id)
        
        # Save positions after update
        await self._save_positions()
        
        logger.info(f"Position recorded: {symbol} {action} {size} @ {entry_price}")
        return True
    
    @handle_async_errors
    async def clear_position(
        self, 
        position_id: str, 
        pnl: float = 0.0, 
        r_multiple: float = 0.0,
        reason: str = "manual"
    ) -> bool:
        """
        Clear a position from tracking
        
        The position is not deleted but marked as closed and included
        in trade statistics.
        
        Args:
            position_id: Position ID to clear
            pnl: Profit/loss amount
            r_multiple: R multiple of the trade
            reason: Reason for closing
            
        Returns:
            True if successful
        """
        # Check if position exists
        if position_id not in self.positions:
            logger.warning(f"Position {position_id} not found")
            return False
        
        # Mark position as closed if not already
        position = self.positions[position_id]
        if not position.closed:
            position.closed = True
            position.close_time = datetime.now(timezone.utc)
            position.close_reason = reason
            position.pnl = pnl
            
            # Update metrics
            if position.entry_price > 0:
                position.pnl_percentage = (pnl / (position.entry_price * position.initial_size)) * 100
            position.r_multiple = r_multiple
        
        # Update trade statistics
        await self.record_trade_pnl(pnl)
        
        # Save positions after update
        await self._save_positions()
        
        logger.info(f"Position cleared: {position_id} with P&L: {pnl:.2f}")
        return True
    
    async def get_position_info(self, symbol: str) -> Optional[dict]:
        """Get information about open positions for a symbol"""
        if not self.initialized:
            logger.warning("Position tracker not initialized")
            return None
            
        if symbol not in self.symbols:
            return None
        
        positions = []
        for pos_id in self.symbols[symbol]:
            if pos_id in self.positions:
                pos = self.positions[pos_id]
                if not pos.closed:
                    positions.append(pos.to_dict())
        
        if not positions:
            return None
            
        return {
            "symbol": symbol,
            "positions": positions
        }
    
    async def get_all_positions(self) -> dict:
        """Get all tracked positions"""
        if not self.initialized:
            logger.warning("Position tracker not initialized")
            return {}
            
        # Group positions by symbol
        result = {}
        for symbol, pos_ids in self.symbols.items():
            open_positions = []
            for pos_id in pos_ids:
                if pos_id in self.positions:
                    pos = self.positions[pos_id]
                    if not pos.closed:
                        open_positions.append(pos.to_dict())
            
            if open_positions:
                result[symbol] = open_positions
        
        return result
    
    @handle_async_errors
    async def record_trade_pnl(self, pnl: float) -> None:
        """
        Record trade profit/loss for statistics
        
        Args:
            pnl: Profit/loss amount
        """
        # Check if daily stats need to be reset
        today = datetime.now().date().isoformat()
        if self.daily_stats["date"] != today:
            # Save previous day's stats before resetting
            await self._save_stats()
            
            # Reset stats for new day
            self.daily_stats = {
                "date": today,
                "trades_total": 0,
                "trades_win": 0,
                "trades_loss": 0,
                "pnl_total": 0.0,
                "pnl_max": 0.0,
                "pnl_min": 0.0,
                "win_rate": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "largest_win": 0.0,
                "largest_loss": 0.0,
                "profit_factor": 0.0
            }
        
        # Update statistics
        self.daily_stats["trades_total"] += 1
        self.daily_stats["pnl_total"] += pnl
        
        if pnl > 0:
            self.daily_stats["trades_win"] += 1
            self.daily_stats["avg_win"] = ((self.daily_stats["avg_win"] * (self.daily_stats["trades_win"] - 1)) + pnl) / self.daily_stats["trades_win"]
            if pnl > self.daily_stats["largest_win"]:
                self.daily_stats["largest_win"] = pnl
        elif pnl < 0:
            self.daily_stats["trades_loss"] += 1
            self.daily_stats["avg_loss"] = ((self.daily_stats["avg_loss"] * (self.daily_stats["trades_loss"] - 1)) + pnl) / self.daily_stats["trades_loss"]
            if pnl < self.daily_stats["largest_loss"]:
                self.daily_stats["largest_loss"] = pnl
        
        # Update win rate
        if self.daily_stats["trades_total"] > 0:
            self.daily_stats["win_rate"] = (self.daily_stats["trades_win"] / self.daily_stats["trades_total"]) * 100
        
        # Update profit factor
        total_profit = sum(max(0, self.positions[p].pnl) for p in self.positions if self.positions[p].closed)
        total_loss = sum(abs(min(0, self.positions[p].pnl)) for p in self.positions if self.positions[p].closed)
        if total_loss > 0:
            self.daily_stats["profit_factor"] = total_profit / total_loss
        
        # Update min/max PnL
        if pnl > self.daily_stats["pnl_max"]:
            self.daily_stats["pnl_max"] = pnl
        if pnl < self.daily_stats["pnl_min"]:
            self.daily_stats["pnl_min"] = pnl
        
        # Save stats
        await self._save_stats()
    
    async def get_daily_pnl(self) -> float:
        """Get the current daily PnL"""
        return self.daily_stats["pnl_total"]
    
    async def check_max_daily_loss(self, account_balance: float) -> Tuple[bool, float]:
        """
        Check if maximum daily loss has been reached
        
        Args:
            account_balance: Current account balance
            
        Returns:
            Tuple of (max_loss_reached, current_loss_percentage)
        """
        daily_pnl = self.daily_stats["pnl_total"]
        
        # If PnL is positive, no max loss concern
        if daily_pnl >= 0:
            return False, 0.0
        
        # Calculate loss as percentage of account
        loss_percentage = abs(daily_pnl) / account_balance * 100
        
        # Get max daily loss from config
        max_daily_loss = get_config_value("risk_management", "max_daily_loss", 20.0)
        
        # Check if max loss reached
        max_loss_reached = loss_percentage >= max_daily_loss
        
        if max_loss_reached:
            logger.warning(f"Maximum daily loss reached: {loss_percentage:.2f}% (limit: {max_daily_loss:.2f}%)")
        
        return max_loss_reached, loss_percentage
    
    async def update_position_exits(self, symbol: str, current_price: float) -> bool:
        """
        Update exit prices for a position based on current price
        
        Args:
            symbol: Symbol to update
            current_price: Current market price
            
        Returns:
            True if position updated
        """
        if symbol not in self.symbols:
            return False
            
        updated = False
        for pos_id in self.symbols[symbol]:
            if pos_id in self.positions:
                position = self.positions[pos_id]
                if not position.closed:
                    # Update position PnL
                    position.update_pnl(current_price)
                    
                    # Check for update to trailing stop
                    if position.action == "BUY" and position.r_multiple >= 1.0:
                        # Implement trailing stop once in profit
                        if position.stop_loss_price < position.entry_price:
                            position.modify_stop_loss(position.entry_price)
                            updated = True
                            logger.info(f"Updated stop loss to break-even for {position.symbol} (ID: {position.position_id})")
                    
                    # Similar logic for SELL positions
                    elif position.action == "SELL" and position.r_multiple >= 1.0:
                        if position.stop_loss_price > position.entry_price:
                            position.modify_stop_loss(position.entry_price)
                            updated = True
                            logger.info(f"Updated stop loss to break-even for {position.symbol} (ID: {position.position_id})")
        
        if updated:
            await self._save_positions()
            
        return updated
    
    async def get_position_entry_price(self, symbol: str) -> Optional[float]:
        """Get entry price for a symbol's position"""
        if symbol not in self.symbols:
            return None
            
        for pos_id in self.symbols[symbol]:
            if pos_id in self.positions:
                position = self.positions[pos_id]
                if not position.closed:
                    return position.entry_price
        
        return None
    
    async def get_position_type(self, symbol: str) -> Optional[str]:
        """Get position type (BUY/SELL) for a symbol"""
        if symbol not in self.symbols:
            return None
            
        for pos_id in self.symbols[symbol]:
            if pos_id in self.positions:
                position = self.positions[pos_id]
                if not position.closed:
                    return position.action
        
        return None
    
    async def get_position_timeframe(self, symbol: str) -> Optional[str]:
        """Get timeframe for a symbol's position"""
        if symbol not in self.symbols:
            return None
            
        for pos_id in self.symbols[symbol]:
            if pos_id in self.positions:
                position = self.positions[pos_id]
                if not position.closed:
                    return position.timeframe
        
        return None
    
    async def get_position_stats(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive statistics for a symbol's position
        
        This provides a single source of truth for all position metrics,
        ensuring consistency across the application.
        """
        if symbol not in self.symbols:
            return None
            
        open_positions = []
        closed_positions = []
        
        for pos_id in self.symbols[symbol]:
            if pos_id in self.positions:
                position = self.positions[pos_id]
                if position.closed:
                    closed_positions.append(position)
                else:
                    open_positions.append(position)
        
        if not open_positions and not closed_positions:
            return None
            
        # Compile statistics
        stats = {
            "symbol": symbol,
            "open_positions": len(open_positions),
            "closed_positions": len(closed_positions),
            "total_positions": len(open_positions) + len(closed_positions),
            "current_exposure": sum(p.size for p in open_positions),
            "current_pnl": sum(p.pnl for p in open_positions),
            "realized_pnl": sum(p.pnl for p in closed_positions),
            "win_rate": 0.0,
            "avg_r_multiple": 0.0
        }
        
        # Calculate win rate from closed positions
        if closed_positions:
            winning_trades = [p for p in closed_positions if p.pnl > 0]
            stats["win_rate"] = (len(winning_trades) / len(closed_positions)) * 100
            stats["avg_r_multiple"] = sum(p.r_multiple for p in closed_positions) / len(closed_positions)
        
        # Add active position data if available
        if open_positions:
            active = open_positions[0]  # Take the first open position
            stats["active_position"] = {
                "id": active.position_id,
                "action": active.action,
                "entry_price": active.entry_price,
                "current_price": active.current_price,
                "size": active.size,
                "pnl": active.pnl,
                "pnl_percentage": active.pnl_percentage,
                "r_multiple": active.r_multiple,
                "take_profit": active.take_profit_price,
                "stop_loss": active.stop_loss_price,
                "open_time": active.open_time.isoformat(),
                "duration": (datetime.now(timezone.utc) - active.open_time).total_seconds() / 3600  # Hours
            }
        
        return stats
    
    async def _background_updates(self):
        """Background task for periodic position updates"""
        try:
            while self.running:
                # Reconcile positions every hour
                await self.reconcile_positions()
                
                # Sleep for an hour
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            logger.info("Position tracker background task cancelled")
        except Exception as e:
            logger.error(f"Error in position tracker background task: {str(e)}")
            logger.error(traceback.format_exc())
    
    async def _load_positions(self):
        """Load positions from file"""
        try:
            if os.path.exists(self.data_file):
                with open(self.data_file, 'r') as f:
                    data = json.load(f)
                    
                    # Process positions
                    for pos_data in data.get("positions", []):
                        try:
                            position = Position.from_dict(pos_data)
                            self.positions[position.position_id] = position
                            
                            # Update symbol mapping
                            if position.symbol not in self.symbols:
                                self.symbols[position.symbol] = []
                            self.symbols[position.symbol].append(position.position_id)
                        except Exception as e:
                            logger.error(f"Error loading position: {str(e)}")
                    
                    logger.info(f"Loaded {len(self.positions)} positions from file")
            else:
                logger.info("No positions file found, starting fresh")
        except Exception as e:
            logger.error(f"Error loading positions: {str(e)}")
            logger.error(traceback.format_exc())
    
    async def _save_positions(self):
        """Save positions to file"""
        try:
            # Create data directory if it doesn't exist
            os.makedirs(os.path.dirname(self.data_file), exist_ok=True)
            
            # Prepare data for saving
            data = {
                "positions": [self.positions[pos_id].to_dict() for pos_id in self.positions],
                "last_updated": datetime.now(timezone.utc).isoformat()
            }
            
            # Write to file
            with open(self.data_file, 'w') as f:
                json.dump(data, f, indent=2)
                
            logger.debug(f"Saved {len(self.positions)} positions to file")
        except Exception as e:
            logger.error(f"Error saving positions: {str(e)}")
            logger.error(traceback.format_exc())
    
    async def _load_stats(self):
        """Load trade statistics from file"""
        try:
            if os.path.exists(self.stats_file):
                with open(self.stats_file, 'r') as f:
                    stats = json.load(f)
                    
                    # Check if stats are for today
                    today = datetime.now().date().isoformat()
                    if stats.get("date") == today:
                        self.daily_stats = stats
                        logger.info(f"Loaded trade statistics for {today}")
                    else:
                        logger.info(f"Trade statistics from {stats.get('date')} ignored, using fresh stats for {today}")
            else:
                logger.info("No trade statistics file found, starting fresh")
        except Exception as e:
            logger.error(f"Error loading trade statistics: {str(e)}")
            logger.error(traceback.format_exc())
    
    async def _save_stats(self):
        """Save trade statistics to file"""
        try:
            # Create data directory if it doesn't exist
            os.makedirs(os.path.dirname(self.stats_file), exist_ok=True)
            
            # Write to file
            with open(self.stats_file, 'w') as f:
                json.dump(self.daily_stats, f, indent=2)
                
            logger.debug("Saved trade statistics to file")
        except Exception as e:
            logger.error(f"Error saving trade statistics: {str(e)}")
            logger.error(traceback.format_exc())

# Create OANDA API functions
@handle_async_errors
async def get_open_positions(account_id: str = None) -> Tuple[bool, Dict[str, Any]]:
    """Get open positions from OANDA"""
    try:
        # Get account ID from settings if not provided
        account_id = account_id or get_config_value("oanda", "oanda_account_id", "")
        
        if not account_id:
            raise ValueError("No OANDA account ID provided or configured")
        
        # Get OANDA API token and URL from config
        api_token = get_config_value("oanda", "oanda_api_token", "")
        api_url = get_config_value("oanda", "oanda_api_url", "https://api-fxtrade.oanda.com/v3")
        
        if not api_token:
            raise ValueError("No OANDA API token configured")
        
        # Create URL
        url = f"{api_url}/accounts/{account_id}/positions"
        
        # Get HTTP session
        session = await get_session()
        
        # Create headers
        headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }
        
        # Send request
        async with session.get(url, headers=headers) as response:
            # Check response status
            if response.status != 200:
                error_msg = await response.text()
                logger.error(f"Error getting positions: {error_msg}")
                return False, {"error": f"OANDA API error: {response.status} - {error_msg}"}
            
            # Parse response
            data = await response.json()
            
            # Transform data into a more convenient format
            positions = []
            for pos in data.get("positions", []):
                # Extract position data
                instrument = pos.get("instrument")
                
                # Get long and short positions
                long_units = float(pos.get("long", {}).get("units", "0"))
                short_units = float(pos.get("short", {}).get("units", "0"))
                
                # Determine direction and units
                direction = "BUY" if long_units > 0 else "SELL"
                units = long_units if direction == "BUY" else abs(short_units)
                
                # Skip positions with zero units
                if units == 0:
                    continue
                
                # Get position details based on direction
                details = pos.get("long" if direction == "BUY" else "short", {})
                
                # Get average price
                avg_price = float(details.get("averagePrice", "0"))
                
                # Create position object
                position = {
                    "id": pos.get("id", str(uuid.uuid4())),
                    "instrument": instrument,
                    "direction": direction,
                    "units": units,
                    "avg_price": avg_price,
                    "unrealized_pl": float(details.get("unrealizedPL", "0")),
                    "realized_pl": float(details.get("realizedPL", "0")),
                    "pl": float(details.get("pl", "0")),
                    "resettable_pl": float(details.get("resettablePL", "0")),
                    "financing": float(details.get("financing", "0")),
                    "take_profit": None,
                    "stop_loss": None
                }
                
                # Add to positions list
                positions.append(position)
            
            return True, {"positions": positions}
    except Exception as e:
        logger.error(f"Error getting open positions: {str(e)}")
        logger.error(traceback.format_exc())
        return False, {"error": f"Error getting positions: {str(e)}"}

# Middleware for error handling
@app.middleware("http")
async def error_handling_middleware(request: Request, call_next):
    """Middleware for handling errors"""
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Create error response
        error_msg = str(e)
        if isinstance(e, ValidationError):
            status_code = 400
            error_msg = "Validation error"
        elif isinstance(e, HTTPException):
            status_code = e.status_code
            error_msg = e.detail
        else:
            status_code = 500
            error_msg = "Internal server error"
        
        return JSONResponse(
            status_code=status_code,
            content={
                "status": "error",
                "message": error_msg,
                "request_id": request_id
            }
        )

# API endpoints
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "version": "1.0.0",
        "time": datetime.now().isoformat(),
        "environment": os.environ.get("ENVIRONMENT", "production")
    }

@app.get("/api/account")
async def get_account_info():
    """Get account information"""
    try:
        # Get account ID from settings
        account_id = get_config_value("oanda", "oanda_account_id", "")
        
        if not account_id:
            raise ValueError("No OANDA account ID configured")
        
        # Get OANDA API token and URL from config
        api_token = get_config_value("oanda", "oanda_api_token", "")
        api_url = get_config_value("oanda", "oanda_api_url", "https://api-fxtrade.oanda.com/v3")
        
        if not api_token:
            raise ValueError("No OANDA API token configured")
        
        # Create URL
        url = f"{api_url}/accounts/{account_id}/summary"
        
        # Get HTTP session
        session = await get_session()
        
        # Create headers
        headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }
        
        # Send request
        async with session.get(url, headers=headers) as response:
            # Check response status
            if response.status != 200:
                error_msg = await response.text()
                logger.error(f"Error getting account info: {error_msg}")
                raise HTTPException(status_code=response.status, detail=f"OANDA API error: {error_msg}")
            
            # Parse response
            data = await response.json()
            
            # Get account data
            account = data.get("account", {})
            
            # Add trade statistics
            if position_tracker and position_tracker.initialized:
                account["trade_stats"] = position_tracker.daily_stats
                
                # Check if max daily loss reached
                max_loss_reached, loss_percentage = await position_tracker.check_max_daily_loss(
                    float(account.get("balance", "0"))
                )
                
                account["max_daily_loss_reached"] = max_loss_reached
                account["daily_loss_percentage"] = loss_percentage
            
            return {
                "status": "success",
                "account": account
            }
    except Exception as e:
        logger.error(f"Error getting account info: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error getting account info: {str(e)}")

@app.get("/api/positions")
async def get_positions_info():
    """Get positions information"""
    try:
        # Verify position tracker is initialized
        if not position_tracker or not position_tracker.initialized:
            raise HTTPException(status_code=500, detail="Position tracker not initialized")
        
        # Get positions from position tracker
        positions = await position_tracker.get_all_positions()
        
        # Get open positions from broker
        success, broker_positions = await get_open_positions()
        
        # Combine data
        result = {
            "status": "success",
            "tracked_positions": positions,
            "broker_positions": broker_positions.get("positions", []) if success else []
        }
        
        return result
    except Exception as e:
        logger.error(f"Error getting positions info: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error getting positions info: {str(e)}")

@app.post("/api/trade")
async def execute_trade_endpoint(request: Request):
    """Execute a trade"""
    try:
        # Parse request body
        payload = await request.json()
        
        # Validate payload
        if "symbol" not in payload:
            raise HTTPException(status_code=400, detail="Missing 'symbol' field")
        if "action" not in payload:
            raise HTTPException(status_code=400, detail="Missing 'action' field")
        
        # Get parameters
        symbol = payload.get("symbol")
        action = payload.get("action")
        size = float(payload.get("size", 0))
        take_profit = payload.get("take_profit")
        stop_loss = payload.get("stop_loss")
        
        # Log request
        logger.info(f"Trade request: {symbol} {action} {size}")
        
        # Placeholder for actual trade execution
        # This would typically call a broker API
        
        # For now, just create a position in the position tracker
        if position_tracker and position_tracker.initialized:
            # Create a position
            position = Position(
                symbol=symbol,
                action=action,
                size=size,
                entry_price=100.0,  # Placeholder
                take_profit_price=take_profit,
                stop_loss_price=stop_loss,
                metadata=payload.get("metadata", {})
            )
            
            return {
                "status": "success",
                "message": f"Trade executed: {symbol} {action} {size}",
                "position_id": position.position_id
            }
        else:
            raise HTTPException(status_code=500, detail="Position tracker not initialized")
    except Exception as e:
        logger.error(f"Error executing trade: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error executing trade: {str(e)}")

@app.post("/api/close")
async def close_position_endpoint(request: Request):
    """Close a position"""
    try:
        # Parse request body
        payload = await request.json()
        
        # Validate payload
        if "position_id" not in payload and "symbol" not in payload:
            raise HTTPException(status_code=400, detail="Missing 'position_id' or 'symbol' field")
        
        # Get parameters
        position_id = payload.get("position_id")
        symbol = payload.get("symbol")
        reason = payload.get("reason", "manual")
        
        # Log request
        logger.info(f"Close position request: {position_id or symbol}")
        
        # Verify position tracker is initialized
        if not position_tracker or not position_tracker.initialized:
            raise HTTPException(status_code=500, detail="Position tracker not initialized")
        
        # Find position
        if position_id:
            # Check if position exists
            if position_id not in position_tracker.positions:
                raise HTTPException(status_code=404, detail=f"Position {position_id} not found")
            
            # Get position
            position = position_tracker.positions[position_id]
            
            # Check if already closed
            if position.closed:
                return {
                    "status": "warning",
                    "message": f"Position {position_id} is already closed"
                }
            
            # Close position
            position.close(position.current_price, reason=reason)
            
            return {
                "status": "success",
                "message": f"Position {position_id} closed",
                "pnl": position.pnl
            }
        else:
            # Close by symbol
            positions_closed = 0
            total_pnl = 0.0
            
            # Check if symbol has positions
            if symbol not in position_tracker.symbols:
                raise HTTPException(status_code=404, detail=f"No positions found for {symbol}")
            
            # Close all open positions for symbol
            for pos_id in position_tracker.symbols[symbol]:
                if pos_id in position_tracker.positions:
                    position = position_tracker.positions[pos_id]
                    if not position.closed:
                        position.close(position.current_price, reason=reason)
                        positions_closed += 1
                        total_pnl += position.pnl
            
            if positions_closed == 0:
                return {
                    "status": "warning",
                    "message": f"No open positions found for {symbol}"
                }
            
            return {
                "status": "success",
                "message": f"Closed {positions_closed} positions for {symbol}",
                "positions_closed": positions_closed,
                "total_pnl": total_pnl
            }
    except Exception as e:
        logger.error(f"Error closing position: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error closing position: {str(e)}")

@app.get("/api/market_info/{symbol}")
async def get_market_info(symbol: str, timeframe: str = "1h"):
    """Get market information for a symbol"""
    try:
        # Verify market analysis is initialized
        if not market_analysis:
            raise HTTPException(status_code=500, detail="Market analysis not initialized")
        
        # Get trend data
        trend_direction, trend_strength = await market_analysis.identify_trend(symbol, [timeframe])
        
        # Get key levels
        price = 100.0  # Placeholder - would be actual market price
        key_levels = await market_analysis.analyze_key_levels(symbol, price)
        
        # Get market session data
        session_data = await market_analysis.get_current_market_session()
        
        # Compile market information
        market_info = {
        "status": "success",
        "symbol": symbol,
        "timeframe": timeframe,
        "timestamp": datetime.now().isoformat(),
        "data": {
                "price": price,
                "spread": 0.001,  # Placeholder
                "volume": 1000.0,  # Placeholder
                "change_24h": 0.5,  # Placeholder
                "high_24h": 101.0,  # Placeholder
                "low_24h": 99.0,   # Placeholder
                "atr": 0.5         # Placeholder
            },
            "analysis": {
                "trend": {
                    "direction": trend_direction,
                    "strength": trend_strength
                },
                "key_levels": key_levels,
                "session": session_data
            }
        }
        
        return market_info
    except Exception as e:
        logger.error(f"Error getting market info for {symbol}: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error getting market info: {str(e)}")

@app.post("/api/risk/position_size")
async def calculate_position_size(request: Request):
    """Calculate recommended position size based on risk parameters"""
    try:
        # Parse request body
        payload = await request.json()
        
        # Validate required fields
        required_fields = ["symbol", "entry_price", "stop_loss", "account_balance"]
        for field in required_fields:
            if field not in payload:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        # Get parameters
        symbol = payload["symbol"]
        entry_price = float(payload["entry_price"])
        stop_loss = float(payload["stop_loss"])
        account_balance = float(payload["account_balance"])
        risk_pct = float(payload.get("risk_percentage", 0.0))  # Optional
        
        # Verify risk manager is initialized
        if not risk_manager:
            raise HTTPException(status_code=500, detail="Risk manager not initialized")
        
        # Calculate position size
        position_size = await risk_manager.calculate_position_size(
            symbol=symbol,
            entry_price=entry_price,
            stop_loss=stop_loss,
            account_balance=account_balance,
            risk_pct=risk_pct if risk_pct > 0 else None
        )
        
        # Calculate pip value and risk details
        pip_risk = abs(entry_price - stop_loss)
        dollar_risk = position_size * pip_risk
        risk_percentage = (dollar_risk / account_balance) * 100
        
        return {
            "status": "success",
            "symbol": symbol,
            "position_size": position_size,
            "risk_details": {
                "dollar_risk": dollar_risk,
                "risk_percentage": risk_percentage,
                "pip_risk": pip_risk,
                "account_balance": account_balance
            }
        }
    except Exception as e:
        logger.error(f"Error calculating position size: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error calculating position size: {str(e)}")

@app.get("/api/risk/portfolio_heat")
async def get_portfolio_heat():
    """Get current portfolio heat (risk exposure)"""
    try:
        # Verify dependencies are initialized
        if not risk_manager:
            raise HTTPException(status_code=500, detail="Risk manager not initialized")
        
        if not position_tracker or not position_tracker.initialized:
            raise HTTPException(status_code=500, detail="Position tracker not initialized")
        
        # Get all positions
        positions_dict = await position_tracker.get_all_positions()
        
        # Flatten positions dictionary into a single list
        positions = {}
        for symbol_positions in positions_dict.values():
            for pos_id, position in symbol_positions.items():
                positions[pos_id] = position
        
        # Check portfolio heat
        is_acceptable, heat_percentage = await risk_manager.check_portfolio_heat(positions)
        
        # Get active symbols with positions
        active_symbols = list(positions_dict.keys())
        
        # Get position counts
        open_count = sum(1 for p in positions.values() if not p.closed)
        closed_count = sum(1 for p in positions.values() if p.closed)
        
        return {
            "status": "success",
            "portfolio_heat": heat_percentage,
            "max_heat_allowed": risk_manager.max_portfolio_heat,
            "is_acceptable": is_acceptable,
            "active_symbols": active_symbols,
            "position_counts": {
                "open": open_count,
                "closed": closed_count,
                "total": len(positions)
            }
        }
    except Exception as e:
        logger.error(f"Error getting portfolio heat: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error getting portfolio heat: {str(e)}")

@app.post("/api/analysis/trade_timing")
async def evaluate_trade_timing(request: Request):
    """Evaluate if current market conditions are favorable for execution"""
    try:
        # Parse request body
        payload = await request.json()
        
        # Validate required fields
        required_fields = ["symbol", "action", "timeframe"]
        for field in required_fields:
            if field not in payload:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        # Get parameters
        symbol = payload["symbol"]
        action = payload["action"].lower()
        timeframe = payload["timeframe"]
        
        # Verify action is valid
        if action not in ["buy", "sell"]:
            raise HTTPException(status_code=400, detail="Action must be 'buy' or 'sell'")
        
        # Verify market analysis is initialized
        if not market_analysis:
            raise HTTPException(status_code=500, detail="Market analysis not initialized")
        
        # Evaluate timing
        timing_evaluation = await market_analysis.evaluate_execution_timing(
            symbol=symbol,
            action=action,
            timeframe=timeframe
        )
        
        # Get additional market context
        trend_direction, trend_strength = await market_analysis.identify_trend(symbol, [timeframe])
        session_data = await market_analysis.get_current_market_session()
        
        return {
            "status": "success",
            "symbol": symbol,
            "action": action,
            "timeframe": timeframe,
            "timing_evaluation": timing_evaluation,
            "market_context": {
                "trend": {
                    "direction": trend_direction,
                    "strength": trend_strength
                },
                "session": session_data
            }
        }
    except Exception as e:
        logger.error(f"Error evaluating trade timing: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error evaluating trade timing: {str(e)}")

@app.post("/api/analysis/key_level")
async def add_key_level(request: Request):
    """Add or update a key price level"""
    try:
        # Parse request body
        payload = await request.json()
        
        # Validate required fields
        required_fields = ["symbol", "level_type", "price"]
        for field in required_fields:
            if field not in payload:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        # Get parameters
        symbol = payload["symbol"]
        level_type = payload["level_type"]
        price = float(payload["price"])
        strength = float(payload.get("strength", 0.5))
        metadata = payload.get("metadata", {})
        
        # Verify level type is valid
        if level_type not in ["support", "resistance"]:
            raise HTTPException(status_code=400, detail="Level type must be 'support' or 'resistance'")
        
        # Verify market analysis is initialized
        if not market_analysis:
            raise HTTPException(status_code=500, detail="Market analysis not initialized")
        
        # Add or update key level
        level = await market_analysis.update_key_level(
            symbol=symbol,
            level_type=level_type,
            price=price,
            strength=strength,
            metadata=metadata
        )
        
        return {
            "status": "success",
            "message": f"Added {level_type} level at {price} for {symbol}",
            "level": level
        }
    except Exception as e:
        logger.error(f"Error adding key level: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error adding key level: {str(e)}")

@app.get("/api/errors")
async def get_error_status():
    """Get information about system errors and recovery status"""
    try:
        # Verify error recovery system is initialized
        if not error_recovery_system:
            raise HTTPException(status_code=500, detail="Error recovery system not initialized")
        
        # Get error statistics
        stats = await error_recovery_system.get_error_stats()
        
        # Get active errors (limit to 10 most recent)
        active_errors = await error_recovery_system.get_active_errors()
        active_errors.sort(key=lambda e: e["timestamp"], reverse=True)
        active_errors = active_errors[:10]
        
        return {
            "status": "success",
            "stats": stats,
            "active_errors": active_errors
        }
    except Exception as e:
        logger.error(f"Error getting error status: {str(e)}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Error getting error status: {str(e)}")

# Risk Management System
class RiskManager:
    """Risk Management system to control trade sizing and portfolio exposure"""
    
    def __init__(self):
        """Initialize the risk manager with default values"""
        self.max_daily_loss_pct = get_config_value("risk", "max_daily_loss", 0.2)
        self.default_risk_per_trade = get_config_value("risk", "default_risk_percentage", 2.0)
        self.max_risk_per_trade = get_config_value("risk", "max_risk_percentage", 5.0)
        self.max_portfolio_heat = get_config_value("risk", "max_portfolio_heat", 15.0)
        self.max_correlated_exposure = get_config_value("risk", "max_correlated_exposure", 10.0)
        self.symbol_correlations = {}
        self.volatility_data = {}
        self.risk_multipliers = {}
        self.load_risk_data()
        logger.info("Risk Manager initialized")
    
    @handle_async_errors
    async def load_risk_data(self):
        """Load risk-related data from disk if available"""
        try:
            if os.path.exists("risk_data.json"):
                with open("risk_data.json", "r") as f:
                    risk_data = json.load(f)
                    self.symbol_correlations = risk_data.get("correlations", {})
                    self.volatility_data = risk_data.get("volatility", {})
                    self.risk_multipliers = risk_data.get("multipliers", {})
                    logger.info("Loaded risk data from disk")
        except Exception as e:
            logger.error(f"Error loading risk data: {str(e)}")
            # Initialize with empty data
            self.symbol_correlations = {}
            self.volatility_data = {}
            self.risk_multipliers = {}
    
    @handle_async_errors
    async def save_risk_data(self):
        """Save risk-related data to disk"""
        try:
            risk_data = {
                "correlations": self.symbol_correlations,
                "volatility": self.volatility_data,
                "multipliers": self.risk_multipliers
            }
            with open("risk_data.json", "w") as f:
                json.dump(risk_data, f, indent=2)
            logger.info("Saved risk data to disk")
        except Exception as e:
            logger.error(f"Error saving risk data: {str(e)}")
    
    @handle_async_errors
    async def calculate_position_size(
        self, 
        symbol: str, 
        entry_price: float, 
        stop_loss: float, 
        account_balance: float,
        risk_pct: Optional[float] = None
    ) -> float:
        """Calculate appropriate position size based on risk parameters"""
        # Use provided risk percentage or default
        risk_percentage = risk_pct if risk_pct is not None else self.default_risk_per_trade
        
        # Apply risk multiplier for this symbol if available
        risk_percentage *= self.risk_multipliers.get(symbol, 1.0)
        
        # Cap at maximum risk per trade
        risk_percentage = min(risk_percentage, self.max_risk_per_trade)
        
        # Calculate dollar risk amount
        risk_amount = account_balance * (risk_percentage / 100)
        
        # Calculate pip/point risk
        if entry_price > stop_loss:  # Long position
            pip_risk = entry_price - stop_loss
        else:  # Short position
            pip_risk = stop_loss - entry_price
        
        # Avoid division by zero
        if pip_risk <= 0:
            logger.warning(f"Invalid pip risk for {symbol}: {pip_risk}")
            return 0
        
        # Calculate position size based on risk per pip
        position_size = risk_amount / pip_risk
        
        # Round to appropriate precision based on market requirements
        if "JPY" in symbol:
            position_size = round(position_size / 1000) * 1000
        else:
            position_size = round(position_size / 100) * 100
        
        # Ensure minimum position size
        min_position = get_config_value("trading", "min_position_size", 1000)
        if position_size < min_position:
            position_size = min_position
        
        logger.info(f"Calculated position size for {symbol}: {position_size} based on {risk_percentage}% risk")
        return position_size
    
    @handle_async_errors
    async def check_portfolio_heat(self, positions: Dict[str, Position]) -> Tuple[bool, float]:
        """
        Check if total portfolio heat exceeds maximum allowed level
        Returns (is_acceptable, current_heat_percentage)
        """
        if not positions:
            return True, 0.0
        
        total_risk = sum(position.risk_percentage for position in positions.values())
        is_acceptable = total_risk <= self.max_portfolio_heat
        
        if not is_acceptable:
            logger.warning(f"Portfolio heat {total_risk}% exceeds maximum {self.max_portfolio_heat}%")
        
        return is_acceptable, total_risk
    
    @handle_async_errors
    async def evaluate_correlated_risk(
        self,
        new_symbol: str,
        positions: Dict[str, Position]
    ) -> Tuple[bool, float, List[str]]:
        """
        Evaluate if adding a position in new_symbol would create too much
        correlated risk based on existing positions.
        Returns (is_acceptable, correlation_score, correlated_symbols)
        """
        if not positions or new_symbol not in self.symbol_correlations:
            return True, 0.0, []
        
        # Find correlations for the new symbol
        symbol_corrs = self.symbol_correlations.get(new_symbol, {})
        if not symbol_corrs:
            return True, 0.0, []
        
        # Calculate total correlated exposure
        correlated_exposure = 0.0
        correlated_symbols = []
        
        for symbol, position in positions.items():
            if symbol in symbol_corrs:
                correlation = abs(symbol_corrs[symbol])
                if correlation > 0.6:  # Significant correlation threshold
                    correlated_exposure += position.risk_percentage * correlation
                    correlated_symbols.append(symbol)
        
        is_acceptable = correlated_exposure <= self.max_correlated_exposure
        
        if not is_acceptable:
            logger.warning(
                f"Correlated exposure for {new_symbol} is {correlated_exposure}%, "
                f"exceeds maximum {self.max_correlated_exposure}%"
            )
        
        return is_acceptable, correlated_exposure, correlated_symbols
    
    @handle_async_errors
    async def update_volatility_data(self, symbol: str, volatility: float):
        """Update volatility data for a symbol"""
        self.volatility_data[symbol] = volatility
        await self.save_risk_data()
    
    @handle_async_errors
    async def get_volatility_adjustment(self, symbol: str) -> float:
        """Get volatility-based position size adjustment factor"""
        # Default to 1.0 (no adjustment) if no volatility data
        if symbol not in self.volatility_data:
            return 1.0
        
        volatility = self.volatility_data[symbol]
        baseline = get_config_value("risk", "baseline_volatility", 0.8)
        
        # Higher volatility = smaller position size
        if volatility > baseline:
            return baseline / volatility
        # Lower volatility = can allow slightly larger position size (capped at 1.25x)
        else:
            return min(1.25, baseline / volatility)

# Market Analysis System
class MarketAnalysis:
    """Market Analysis System for trend detection and execution timing"""
    
    def __init__(self):
        """Initialize the market analysis system"""
        self.trend_data = {}
        self.key_levels = {}
        self.volatility_windows = {}
        self.market_conditions = {}
        self.market_session_data = self._initialize_market_sessions()
        self.timeframe_weights = {
            "1m": 0.05,
            "5m": 0.1,
            "15m": 0.15,
            "30m": 0.2,
            "1h": 0.25,
            "4h": 0.5,
            "1d": 1.0
        }
        self.load_market_data()
        logger.info("Market Analysis System initialized")
    
    def _initialize_market_sessions(self) -> Dict[str, Dict[str, Any]]:
        """Initialize market session data for forex"""
        utc_now = datetime.now(timezone.utc)
        
        sessions = {
            "sydney": {
                "start": 22,  # 10 PM UTC
                "end": 7,     # 7 AM UTC
                "volatility": "medium",
                "liquidity": "low"
            },
            "tokyo": {
                "start": 0,   # 12 AM UTC
                "end": 9,     # 9 AM UTC
                "volatility": "medium",
                "liquidity": "medium"
            },
            "london": {
                "start": 8,   # 8 AM UTC
                "end": 17,    # 5 PM UTC
                "volatility": "high",
                "liquidity": "high"
            },
            "new_york": {
                "start": 13,  # 1 PM UTC
                "end": 22,    # 10 PM UTC
                "volatility": "high",
                "liquidity": "high"
            }
        }
        
        # Add current session status
        current_hour = utc_now.hour
        for session, data in sessions.items():
            start, end = data["start"], data["end"]
            if start < end:
                data["active"] = start <= current_hour < end
            else:  # Session spans midnight
                data["active"] = current_hour >= start or current_hour < end
        
        return sessions
    
    @handle_async_errors
    async def load_market_data(self):
        """Load market data from disk if available"""
        try:
            if os.path.exists("market_data.json"):
                with open("market_data.json", "r") as f:
                    data = json.load(f)
                    self.trend_data = data.get("trends", {})
                    self.key_levels = data.get("levels", {})
                    self.market_conditions = data.get("conditions", {})
                    logger.info("Loaded market data from disk")
        except Exception as e:
            logger.error(f"Error loading market data: {str(e)}")
    
    @handle_async_errors
    async def save_market_data(self):
        """Save market data to disk"""
        try:
            data = {
                "trends": self.trend_data,
                "levels": self.key_levels,
                "conditions": self.market_conditions
            }
            with open("market_data.json", "w") as f:
                json.dump(data, f, indent=2)
            logger.info("Saved market data to disk")
        except Exception as e:
            logger.error(f"Error saving market data: {str(e)}")
    
    @handle_async_errors
    async def identify_trend(
        self, 
        symbol: str, 
        timeframes: List[str] = None
    ) -> Tuple[str, float]:
        """
        Identify the trend direction and strength for a symbol across timeframes
        Returns (trend_direction, strength) where:
          - trend_direction is "up", "down", or "sideways"
          - strength is a value between 0.0 (weak) and 1.0 (strong)
        """
        if not timeframes:
            timeframes = ["1h", "4h", "1d"]
        
        # Get trend data for this symbol
        symbol_trends = self.trend_data.get(symbol, {})
        if not symbol_trends:
            return "sideways", 0.0
        
        # Calculate weighted trend direction and strength
        total_weight = 0
        weighted_direction = 0  # -1 for down, 0 for sideways, 1 for up
        total_strength = 0
        
        for tf in timeframes:
            if tf in symbol_trends:
                tf_data = symbol_trends[tf]
                tf_weight = self.timeframe_weights.get(tf, 0.2)
                
                direction = tf_data.get("direction", "sideways")
                strength = tf_data.get("strength", 0.0)
                
                direction_value = 1 if direction == "up" else (-1 if direction == "down" else 0)
                
                weighted_direction += direction_value * tf_weight * strength
                total_strength += strength * tf_weight
                total_weight += tf_weight
        
        if total_weight == 0:
            return "sideways", 0.0
            
        # Normalize the results
        normalized_direction = weighted_direction / total_weight
        normalized_strength = total_strength / total_weight
        
        # Convert numerical direction to string
        if normalized_direction > 0.3:
            trend_direction = "up"
        elif normalized_direction < -0.3:
            trend_direction = "down"
        else:
            trend_direction = "sideways"
        
        logger.info(f"Trend analysis for {symbol}: {trend_direction} with strength {normalized_strength:.2f}")
        return trend_direction, normalized_strength
    
    @handle_async_errors
    async def update_trend_data(
        self, 
        symbol: str, 
        timeframe: str, 
        direction: str, 
        strength: float
    ):
        """Update trend data for a symbol/timeframe"""
        if symbol not in self.trend_data:
            self.trend_data[symbol] = {}
        
        self.trend_data[symbol][timeframe] = {
            "direction": direction,
            "strength": strength,
            "updated_at": datetime.now().isoformat()
        }
        
        await self.save_market_data()
    
    @handle_async_errors
    async def analyze_key_levels(
        self, 
        symbol: str, 
        price: float
    ) -> Dict[str, Any]:
        """
        Analyze price in relation to key support/resistance levels
        Returns information about nearby levels and price position
        """
        if symbol not in self.key_levels:
            return {"status": "no_levels", "levels": []}
        
        levels = self.key_levels[symbol]
        result = {
            "status": "between_levels",
            "nearest_support": None,
            "nearest_resistance": None,
            "distance_to_support": float('inf'),
            "distance_to_resistance": float('inf'),
            "levels": []
        }
        
        supports = [level for level in levels if level["type"] == "support"]
        resistances = [level for level in levels if level["type"] == "resistance"]
        
        # Find nearest support
        for level in supports:
            level_price = level["price"]
            if level_price < price:
                distance = price - level_price
                if distance < result["distance_to_support"]:
                    result["distance_to_support"] = distance
                    result["nearest_support"] = level
        
        # Find nearest resistance
        for level in resistances:
            level_price = level["price"]
            if level_price > price:
                distance = level_price - price
                if distance < result["distance_to_resistance"]:
                    result["distance_to_resistance"] = distance
                    result["nearest_resistance"] = level
        
        # Determine position relative to levels
        if result["distance_to_support"] < 0.001 * price:
            result["status"] = "at_support"
        elif result["distance_to_resistance"] < 0.001 * price:
            result["status"] = "at_resistance"
        
        # Add all nearby levels
        nearby_threshold = 0.01 * price  # 1% distance
        result["levels"] = [
            level for level in levels 
            if abs(level["price"] - price) < nearby_threshold
        ]
        
        return result
    
    @handle_async_errors
    async def update_key_level(
        self, 
        symbol: str, 
        level_type: str,  # "support" or "resistance" 
        price: float,
        strength: float = 0.5,
        metadata: Optional[Dict] = None
    ):
        """Add or update a key price level"""
        if symbol not in self.key_levels:
            self.key_levels[symbol] = []
        
        # Check if this level already exists (within 0.1% tolerance)
        tolerance = price * 0.001
        for level in self.key_levels[symbol]:
            if level["type"] == level_type and abs(level["price"] - price) < tolerance:
                # Update existing level
                level["price"] = price  # Update slightly
                level["strength"] = max(level["strength"], strength)  # Increase strength
                level["touches"] = level.get("touches", 0) + 1
                level["last_touch"] = datetime.now().isoformat()
                
                if metadata:
                    if "metadata" not in level:
                        level["metadata"] = {}
                    level["metadata"].update(metadata)
                
                await self.save_market_data()
                return level
        
        # Add new level
        new_level = {
            "type": level_type,
            "price": price,
            "strength": strength,
            "touches": 1,
            "created_at": datetime.now().isoformat(),
            "last_touch": datetime.now().isoformat()
        }
        
        if metadata:
            new_level["metadata"] = metadata
            
        self.key_levels[symbol].append(new_level)
        await self.save_market_data()
        return new_level
    
    @handle_async_errors
    async def get_current_market_session(self) -> Dict[str, Any]:
        """Get information about current forex market session"""
        # Refresh market session data
        self.market_session_data = self._initialize_market_sessions()
        
        active_sessions = {
            session: data for session, data in self.market_session_data.items()
            if data["active"]
        }
        
        result = {
            "active_sessions": active_sessions,
            "session_overlap": len(active_sessions) > 1,
            "high_liquidity": any(data["liquidity"] == "high" for data in active_sessions.values()),
            "high_volatility": any(data["volatility"] == "high" for data in active_sessions.values()),
        }
        
        return result
    
    @handle_async_errors
    async def evaluate_execution_timing(
        self,
        symbol: str,
        action: str,  # "buy" or "sell"
        timeframe: str
    ) -> Dict[str, Any]:
        """
        Evaluate if current market conditions are favorable for execution
        Returns a dict with timing evaluation and confidence score
        """
        result = {
            "suitable": False,
            "confidence": 0.0,
            "factors": []
        }
        
        # Factor 1: Check trend alignment
        trend_direction, trend_strength = await self.identify_trend(symbol, [timeframe])
        trend_aligned = (
            (action == "buy" and trend_direction == "up") or
            (action == "sell" and trend_direction == "down")
        )
        
        if trend_aligned:
            result["factors"].append({
                "name": "trend_alignment",
                "positive": True,
                "description": f"Trade direction aligned with {timeframe} trend",
                "weight": 0.3
            })
            result["confidence"] += 0.3 * trend_strength
        else:
            result["factors"].append({
                "name": "trend_alignment",
                "positive": False,
                "description": f"Trade direction against {timeframe} trend",
                "weight": 0.3
            })
        
        # Factor 2: Check market session
        session_data = await self.get_current_market_session()
        
        market_is_active = session_data["high_liquidity"] or session_data["session_overlap"]
        if market_is_active:
            result["factors"].append({
                "name": "market_session",
                "positive": True,
                "description": "Market is in active session with good liquidity",
                "weight": 0.2
            })
            result["confidence"] += 0.2
        else:
            result["factors"].append({
                "name": "market_session",
                "positive": False,
                "description": "Market session has low liquidity",
                "weight": 0.2
            })
        
        # Factor 3: Check key levels
        key_level_analysis = await self.analyze_key_levels(symbol, 0.0)  # Price will be filled by caller
        
        level_favorable = False
        level_description = "No key levels nearby"
        
        if key_level_analysis["status"] == "at_support" and action == "buy":
            level_favorable = True
            level_description = "Price at support level, favorable for buying"
        elif key_level_analysis["status"] == "at_resistance" and action == "sell":
            level_favorable = True
            level_description = "Price at resistance level, favorable for selling"
        
        result["factors"].append({
            "name": "key_levels",
            "positive": level_favorable,
            "description": level_description,
            "weight": 0.25
        })
        
        if level_favorable:
            result["confidence"] += 0.25
        
        # Final evaluation
        result["suitable"] = result["confidence"] >= 0.5
        
        return result

# Lorentzian Classifier for Market Regime Analysis
class LorentzianClassifier:
    """
    Advanced market regime classifier using Lorentzian distance metrics
    to identify trending, ranging, and volatile market conditions.
    """
    def __init__(self, lookback_period=20):
        """Initialize the classifier with configurable lookback period"""
        self.lookback_period = lookback_period
        self.prices = []
        self.distances = []
        self.regime = "unknown"
        self.confidence = 0.0
        self.fractal_dimension = 0.0
        self.volatility = 0.0
        
    def add_price(self, price: float) -> None:
        """Add a new price point and update the classification"""
        self.prices.append(price)
        
        # Keep only the lookback window
        if len(self.prices) > self.lookback_period:
            self.prices.pop(0)
            
        # Only classify after we have enough data
        if len(self.prices) >= self.lookback_period:
            self._calculate_lorentzian_distance(self.prices)
            self._calculate_fractal_dimension(self.prices)
            self._classify_regime()
            
    def _calculate_lorentzian_distance(self, prices: List[float]) -> float:
        """
        Calculate the Lorentzian distance metric for the price series
        This captures non-linear relationships in the price data
        """
        if len(prices) < 2:
            return 0.0
            
        distances = []
        for i in range(1, len(prices)):
            # Time component is fixed at 1 (each step is 1 time unit)
            time_diff = 1
            # Price component is the absolute difference
            price_diff = abs(prices[i] - prices[i-1])
            
            # Lorentzian distance formula: ln(1 + |price_diff|)
            # This weights large price changes more than small ones
            # but not as dramatically as squared differences
            distance = math.log(1 + price_diff)
            distances.append(distance)
            
        self.distances = distances
        avg_distance = sum(distances) / len(distances) if distances else 0
        self.volatility = avg_distance
        return avg_distance
        
    def _calculate_fractal_dimension(self, prices: List[float]) -> float:
        """
        Calculate approximate fractal dimension of the price series
        Higher values indicate more complexity/randomness
        """
        if len(prices) < 4:
            return 1.0  # Default to 1.0 (smooth line)
            
        # Calculate length of the price curve
        curve_length = 0
        for i in range(1, len(prices)):
            curve_length += abs(prices[i] - prices[i-1])
            
        # Calculate end-to-end distance
        end_to_end = abs(prices[-1] - prices[0])
        
        # Avoid division by zero
        if end_to_end == 0:
            end_to_end = 0.0001
            
        # Fractal dimension approximation
        # 1.0 = smooth line, 2.0 = plane-filling curve
        dimension = 1 + (math.log(curve_length) - math.log(end_to_end)) / math.log(len(prices))
        
        # Constrain to reasonable values
        dimension = max(1.0, min(2.0, dimension))
        
        self.fractal_dimension = dimension
        return dimension
        
    def _classify_regime(self) -> None:
        """
        Classify the current market regime based on Lorentzian distance
        and fractal dimension metrics
        """
        volatility = self.volatility
        dimension = self.fractal_dimension
        
        # Base classification on two metrics
        if dimension < 1.3:
            # Lower fractal dimension = smoother, more directed movement
            if volatility > 0.01:  # High volatility
                self.regime = "trending_volatile"
                self.confidence = min(1.0, volatility * 20) * (1.3 - dimension) / 0.3
            else:  # Lower volatility
                self.regime = "trending_smooth"
                self.confidence = (1.3 - dimension) / 0.3
        elif dimension < 1.6:
            # Middle fractal dimension = mixed signals
            if volatility > 0.015:  # Higher volatility
                self.regime = "volatile"
                self.confidence = min(1.0, volatility * 15) * (1.6 - dimension) / 0.3
            else:  # Lower volatility
                self.regime = "mixed"
                self.confidence = 0.5
        else:
            # Higher fractal dimension = more random/choppy
            if volatility > 0.02:  # High volatility
                self.regime = "choppy_volatile"
                self.confidence = min(1.0, volatility * 10) * (dimension - 1.6) / 0.4
            else:  # Lower volatility
                self.regime = "ranging"
                self.confidence = (dimension - 1.6) / 0.4
                
        # Ensure confidence is in [0, 1]
        self.confidence = max(0.0, min(1.0, self.confidence))
                
    def get_regime(self) -> Tuple[str, float]:
        """Get the current market regime and confidence"""
        return self.regime, self.confidence
        
    def get_risk_adjustment(self) -> float:
        """
        Get recommended risk adjustment factor based on market regime
        Returns a multiplier for position sizing (0.0 - 1.0)
        """
        regime, confidence = self.get_regime()
        
        # Base adjustments by regime
        adjustments = {
            "trending_smooth": 1.0,     # Full size for smooth trends
            "trending_volatile": 0.8,   # Slightly reduced for volatile trends
            "mixed": 0.6,               # More reduced for mixed signals
            "ranging": 0.5,             # Half size for ranges (mean reversion)
            "volatile": 0.4,            # Low size for volatile conditions
            "choppy_volatile": 0.3,     # Very low for choppy volatile
            "unknown": 0.5              # Default to moderate
        }
        
        # Get base adjustment and apply confidence scaling
        base_adjustment = adjustments.get(regime, 0.5)
        confidence_factor = 0.5 + (self.confidence * 0.5)  # Scale from 0.5-1.0
        
        return base_adjustment * confidence_factor

# Advanced Lorentzian Distance Classifier with feature extraction
class LorentzianDistanceClassifier:
    """
    Advanced market regime classifier using multi-dimensional Lorentzian distances
    to identify complex market conditions and generate trading signals.
    """
    def __init__(self, lookback_window=50, signal_threshold=0.75, feature_count=5):
        """Initialize with configurable lookback period and signal threshold"""
        self.lookback_window = lookback_window
        self.signal_threshold = signal_threshold
        self.feature_count = min(5, max(3, feature_count))  # Between 3-5 features
        
        # Price data storage
        self.prices = []
        self.returns = []
        self.volumes = []
        self.additional_data = []
        
        # Feature storage
        self.features = []
        self.feature_history = []
        
        # Classification results
        self.distance_matrix = None
        self.cluster_assignments = None
        self.current_regime = "unknown"
        self.regime_history = []
        self.confidence = 0.0
        
        # Signal generation
        self.current_signal = "neutral"
        self.signal_history = []
        self.signal_strength = 0.0
        
    def add_data_point(self, price: float, volume: Optional[float] = None, 
                      additional_metrics: Optional[Dict[str, float]] = None) -> None:
        """
        Add a new data point and update the classification
        
        Args:
            price: Latest price
            volume: Optional volume data
            additional_metrics: Optional dict of additional features
        """
        # Store price and calculate returns
        self.prices.append(price)
        if len(self.prices) > 1:
            ret = (price / self.prices[-2]) - 1.0
            self.returns.append(ret)
        else:
            self.returns.append(0.0)
            
        # Store volume
        if volume is not None:
            self.volumes.append(volume)
        elif len(self.volumes) > 0:
            # Reuse previous volume if not provided
            self.volumes.append(self.volumes[-1])
        else:
            self.volumes.append(1.0)  # Default volume
            
        # Store additional metrics
        self.additional_data.append(additional_metrics or {})
        
        # Keep only the lookback window
        if len(self.prices) > self.lookback_window:
            self.prices.pop(0)
            self.returns.pop(0)
            self.volumes.pop(0)
            self.additional_data.pop(0)
            
        # Only classify after we have enough data
        if len(self.prices) >= self.lookback_window:
            self._extract_features(additional_metrics)
            
            # Keep feature history for signal generation
            self.feature_history.append(self.features.copy())
            if len(self.feature_history) > self.lookback_window:
                self.feature_history.pop(0)
                
            if len(self.feature_history) >= 5:  # Need at least 5 points for classification
                self._calculate_distance_matrix()
                self._classify_market_condition()
                self._generate_signal()
    
    def _extract_features(self, additional_metrics: Optional[Dict[str, float]] = None) -> None:
        """Extract key features from price, returns, and volume data"""
        if len(self.prices) < self.lookback_window:
            self.features = [0.0] * self.feature_count
            return
            
        # Get recent data subsets
        recent_prices = self.prices[-self.lookback_window:]
        recent_returns = self.returns[-self.lookback_window:]
        recent_volumes = self.volumes[-self.lookback_window:] if len(self.volumes) >= self.lookback_window else None
        
        features = []
        
        # Feature 1: Price momentum (rate of change over different periods)
        short_period = max(1, self.lookback_window // 10)
        medium_period = max(3, self.lookback_window // 5)
        long_period = max(7, self.lookback_window // 2)
        
        price_roc_short = (recent_prices[-1] / recent_prices[-short_period]) - 1 if len(recent_prices) >= short_period else 0
        price_roc_medium = (recent_prices[-1] / recent_prices[-medium_period]) - 1 if len(recent_prices) >= medium_period else 0
        price_roc_long = (recent_prices[-1] / recent_prices[-long_period]) - 1 if len(recent_prices) >= long_period else 0
        
        # Normalized momentum feature
        momentum = 0.5 * price_roc_short + 0.3 * price_roc_medium + 0.2 * price_roc_long
        features.append(momentum)
        
        # Feature 2: Volatility (normalized)
        if len(recent_returns) >= 5:
            volatility = statistics.stdev(recent_returns) if len(recent_returns) > 1 else 0
            # Normalize to typical range (0-1)
            norm_volatility = min(1.0, volatility * 100)
            features.append(norm_volatility)
        else:
            features.append(0.0)
            
        # Feature 3: Trend consistency (directional strength)
        if len(recent_returns) >= 5:
            positive_returns = sum(1 for r in recent_returns if r > 0)
            negative_returns = sum(1 for r in recent_returns if r < 0)
            total_returns = len(recent_returns)
            
            # Scale from -1 (all negative) to +1 (all positive)
            consistency = (positive_returns - negative_returns) / total_returns
            features.append(consistency)
        else:
            features.append(0.0)
            
        # Feature 4: Volume profile (if available)
        if recent_volumes and len(recent_volumes) >= 5:
            avg_volume = sum(recent_volumes) / len(recent_volumes)
            recent_vol_avg = sum(recent_volumes[-5:]) / 5
            
            # Volume surge/decline detection - normalized to -1 to +1
            # +1 means recent volume is much higher than average
            vol_profile = min(1.0, max(-1.0, (recent_vol_avg / avg_volume) - 1.0))
            features.append(vol_profile)
        else:
            features.append(0.0)
            
        # Feature 5: Mean reversion potential
        # Measure distance from moving averages
        if len(recent_prices) >= 20:
            sma20 = sum(recent_prices[-20:]) / 20
            distance_from_sma = (recent_prices[-1] / sma20) - 1.0
            # Normalize to typical range
            mean_reversion = min(1.0, max(-1.0, distance_from_sma * 10))
            features.append(mean_reversion)
        else:
            features.append(0.0)
            
        # Additional features from external metrics
        if additional_metrics and self.feature_count > 5:
            for i, (key, value) in enumerate(additional_metrics.items()):
                if i + 5 < self.feature_count:  # Only add up to feature_count
                    # Normalize based on key type - this requires domain knowledge
                    # Here we just take the raw value, but in practice you'd normalize
                    features.append(value)
        
        # Ensure we have exactly feature_count features
        while len(features) < self.feature_count:
            features.append(0.0)
        features = features[:self.feature_count]
        
        self.features = features
            
    def _calculate_distance_matrix(self) -> np.ndarray:
        """
        Calculate the Lorentzian distance matrix between feature vectors
        in the feature history
        """
        if not self.feature_history or len(self.feature_history) < 2:
            return np.zeros((1, 1))
            
        n_samples = len(self.feature_history)
        distance_matrix = np.zeros((n_samples, n_samples))
        
        for i in range(n_samples):
            for j in range(i, n_samples):
                # Get feature vectors
                features_i = self.feature_history[i]
                features_j = self.feature_history[j]
                
                # Calculate Lorentzian distance
                distance = 0.0
                for f_i, f_j in zip(features_i, features_j):
                    # Lorentzian distance: ln(1 + |f_i - f_j|)
                    distance += math.log(1.0 + abs(f_i - f_j))
                
                # Store in matrix (symmetric)
                distance_matrix[i, j] = distance
                distance_matrix[j, i] = distance
                
        self.distance_matrix = distance_matrix
        return distance_matrix
        
    def _classify_market_condition(self) -> None:
        """
        Classify market conditions based on clustering of feature vectors
        and their Lorentzian distances
        """
        if self.distance_matrix is None or len(self.feature_history) < 5:
            self.current_regime = "unknown"
            self.confidence = 0.0
            return
            
        # Define regime characteristics
        regimes = {
            "trending_up": {"momentum": 0.7, "volatility": 0.3, "consistency": 0.7},
            "trending_down": {"momentum": -0.7, "volatility": 0.3, "consistency": -0.7},
            "ranging": {"momentum": 0.0, "volatility": 0.2, "consistency": 0.0},
            "volatile": {"momentum": 0.0, "volatility": 0.8, "consistency": 0.0},
            "breakout": {"momentum": 0.5, "volatility": 0.7, "consistency": 0.5}
        }
        
        # Get latest feature vector and calculate similarities to regime prototypes
        latest_features = self.feature_history[-1]
        
        # We need at least 3 features: momentum, volatility, consistency
        if len(latest_features) < 3:
            self.current_regime = "unknown"
            self.confidence = 0.0
            return
            
        momentum = latest_features[0]
        volatility = latest_features[1]
        consistency = latest_features[2]
        
        # Calculate similarity scores to each regime
        similarity_scores = {}
        for regime, characteristics in regimes.items():
            # For trending_down, we want negative momentum and consistency
            if regime == "trending_down":
                momentum_diff = abs(momentum - characteristics["momentum"])
                consistency_diff = abs(consistency - characteristics["consistency"])
            else:
                momentum_diff = abs(momentum - characteristics["momentum"])
                consistency_diff = abs(consistency - characteristics["consistency"])
                
            volatility_diff = abs(volatility - characteristics["volatility"])
            
            # Weighted sum of differences (lower is more similar)
            similarity = 1.0 - (0.4 * momentum_diff + 0.3 * volatility_diff + 0.3 * consistency_diff)
            similarity_scores[regime] = max(0.0, similarity)
            
        # Find best matching regime
        best_regime = max(similarity_scores.items(), key=lambda x: x[1])
        self.current_regime = best_regime[0]
        self.confidence = best_regime[1]
        
        # Store in history
        self.regime_history.append((self.current_regime, self.confidence))
        if len(self.regime_history) > self.lookback_window:
            self.regime_history.pop(0)
            
    def _generate_signal(self) -> None:
        """
        Generate trading signals based on market regime and feature patterns
        """
        if not self.feature_history or len(self.regime_history) < 5:
            self.current_signal = "neutral"
            self.signal_strength = 0.0
            return
            
        # Get recent regimes
        recent_regimes = [r[0] for r in self.regime_history[-10:]]
        regime_confidences = [r[1] for r in self.regime_history[-10:]]
        
        # Current regime and confidence
        current_regime = self.current_regime
        confidence = self.confidence
        
        # If confidence is too low, stay neutral
        if confidence < self.signal_threshold:
            self.current_signal = "neutral"
            self.signal_strength = 0.0
            return
        
        # Default signal is neutral
        signal = "neutral"
        strength = 0.0
        
        # Trading rules based on regime
        if current_regime == "trending_up":
            # Strong uptrend
            signal = "buy"
            strength = confidence
            
            # Check for consistency in recent regimes
            trend_consistency = recent_regimes.count("trending_up") / len(recent_regimes)
            strength *= (0.5 + 0.5 * trend_consistency)
            
        elif current_regime == "trending_down":
            # Strong downtrend
            signal = "sell"
            strength = confidence
            
            # Check for consistency in recent regimes
            trend_consistency = recent_regimes.count("trending_down") / len(recent_regimes)
            strength *= (0.5 + 0.5 * trend_consistency)
            
        elif current_regime == "ranging":
            # Ranging market - potential mean reversion
            if len(self.feature_history) >= 10:
                # Get recent mean reversion potential (feature 4 or 2 depending on count)
                mean_reversion_idx = min(4, len(self.features) - 1)
                mean_reversion = self.features[mean_reversion_idx]
                
                if mean_reversion > 0.5:  # Far above average - potential sell
                    signal = "sell"
                    strength = confidence * (mean_reversion - 0.5) * 2.0  # Scale 0.5-1.0 to 0.0-1.0
                    
                elif mean_reversion < -0.5:  # Far below average - potential buy
                    signal = "buy"
                    strength = confidence * (abs(mean_reversion) - 0.5) * 2.0  # Scale 0.5-1.0 to 0.0-1.0
                    
        elif current_regime == "breakout":
            # Breakout regime - follow momentum
            momentum = self.features[0]
            if momentum > 0.3:
                signal = "buy"
                strength = confidence * min(1.0, momentum)
            elif momentum < -0.3:
                signal = "sell"
                strength = confidence * min(1.0, abs(momentum))
                
        # Additional risk factor for volatile regimes
        if current_regime == "volatile":
            strength *= 0.5  # Reduce signal strength in volatile markets
            
        # Ensure strength is within bounds
        strength = max(0.0, min(1.0, strength))
        
        # Store results
        self.current_signal = signal
        self.signal_strength = strength
        
        # Update signal history
        self.signal_history.append((signal, strength))
        if len(self.signal_history) > self.lookback_window:
            self.signal_history.pop(0)
            
    def get_current_classification(self) -> Dict[str, Any]:
        """Get the current market classification and details"""
        return {
            "regime": self.current_regime,
            "confidence": self.confidence,
            "features": self.features[:5],  # Return at most 5 features
            "updated_at": datetime.now().isoformat()
        }
        
    def get_latest_signal(self) -> Dict[str, Any]:
        """Get the latest generated trading signal"""
        return {
            "signal": self.current_signal,
            "strength": self.signal_strength,
            "regime": self.current_regime,
            "updated_at": datetime.now().isoformat()
        }
        
    def get_signal_distribution(self, lookback: int = 10) -> Dict[str, float]:
        """
        Get the distribution of signals over recent history
        Returns the percentage of buy/sell/neutral signals
        """
        if not self.signal_history:
            return {"buy": 0.0, "sell": 0.0, "neutral": 1.0}
            
        # Get subset of history
        history = self.signal_history[-min(lookback, len(self.signal_history)):]
        total = len(history)
        
        # Count signals
        buy_count = sum(1 for s in history if s[0] == "buy")
        sell_count = sum(1 for s in history if s[0] == "sell")
        neutral_count = total - buy_count - sell_count
        
        # Calculate percentages
        return {
            "buy": buy_count / total if total > 0 else 0.0,
            "sell": sell_count / total if total > 0 else 0.0,
            "neutral": neutral_count / total if total > 0 else 0.0
        }
        
    def get_trend_strength(self) -> float:
        """
        Get the overall trend strength
        Returns a value from -1.0 (strong downtrend) to 1.0 (strong uptrend)
        """
        if not self.signal_history:
            return 0.0
            
        # Get recent signals with their strengths
        recent_signals = self.signal_history[-min(20, len(self.signal_history)):]
        
        if not recent_signals:
            return 0.0
            
        # Calculate weighted trend strength
        trend_strength = 0.0
        total_weight = 0.0
        
        # More recent signals have higher weight
        for i, (signal, strength) in enumerate(recent_signals):
            weight = (i + 1) / len(recent_signals)  # 0.05 to 1.0
            
            if signal == "buy":
                trend_strength += weight * strength
            elif signal == "sell":
                trend_strength -= weight * strength
                
            total_weight += weight
            
        # Normalize
        if total_weight > 0:
            trend_strength /= total_weight
            
        return trend_strength

# Advanced Volatility Monitoring
class VolatilityMonitor:
    """
    Advanced volatility monitoring system to track market volatility
    and provide adaptive risk management adjustments.
    """
    def __init__(self, lookback_period=14, atr_multiplier=1.5):
        """Initialize with lookback period and ATR multiplier"""
        self.lookback_period = lookback_period
        self.atr_multiplier = atr_multiplier
        
        # Data storage
        self.highs = {}
        self.lows = {}
        self.closes = {}
        
        # Volatility metrics
        self.atr = {}
        self.atr_percent = {}
        self.historical_volatility = {}
        self.volatility_percentile = {}
        self.atr_history = {}
        
    def add_candle(self, symbol: str, high: float, low: float, close: float) -> None:
        """
        Add a new candle and update volatility metrics
        
        Args:
            symbol: Trading symbol
            high: Candle high price
            low: Candle low price
            close: Candle close price
        """
        # Initialize data structures for this symbol if not exist
        if symbol not in self.highs:
            self.highs[symbol] = []
            self.lows[symbol] = []
            self.closes[symbol] = []
            self.atr[symbol] = 0.0
            self.atr_percent[symbol] = 0.0
            self.historical_volatility[symbol] = 0.0
            self.volatility_percentile[symbol] = 0.5
            self.atr_history[symbol] = []
        
        self.highs[symbol].append(high)
        self.lows[symbol].append(low)
        self.closes[symbol].append(close)
        
        # Keep only the lookback window
        if len(self.closes[symbol]) > self.lookback_period:
            self.highs[symbol].pop(0)
            self.lows[symbol].pop(0)
            self.closes[symbol].pop(0)
            
        # Only calculate after we have enough data
        if len(self.closes[symbol]) >= 2:
            self._calculate_atr(symbol)
            
        if len(self.closes[symbol]) >= self.lookback_period:
            self._calculate_historical_volatility(symbol)
            self._update_volatility_percentile(symbol)
            
    def _calculate_atr(self, symbol: str) -> None:
        """Calculate Average True Range for a symbol"""
        if symbol not in self.closes or len(self.closes[symbol]) < 2:
            self.atr[symbol] = 0.0
            self.atr_percent[symbol] = 0.0
            return
            
        true_ranges = []
        
        for i in range(1, len(self.closes[symbol])):
            # Current high-low
            hl = self.highs[symbol][i] - self.lows[symbol][i]
            # Previous close to current high
            ch = abs(self.closes[symbol][i-1] - self.highs[symbol][i])
            # Previous close to current low
            cl = abs(self.closes[symbol][i-1] - self.lows[symbol][i])
            
            # True range is max of these three
            tr = max(hl, ch, cl)
            true_ranges.append(tr)
            
        # Calculate ATR
        if not true_ranges:
            self.atr[symbol] = 0.0
        else:
            self.atr[symbol] = sum(true_ranges) / len(true_ranges)
            
        # Calculate ATR as percentage
        if self.closes[symbol] and self.closes[symbol][-1] > 0:
            self.atr_percent[symbol] = (self.atr[symbol] / self.closes[symbol][-1]) * 100
        else:
            self.atr_percent[symbol] = 0.0
            
        # Store in history
        self.atr_history[symbol].append(self.atr[symbol])
        if len(self.atr_history[symbol]) > 100:  # Keep up to 100 values for percentile
            self.atr_history[symbol].pop(0)
            
    def _calculate_historical_volatility(self, symbol: str) -> None:
        """Calculate historical volatility (standard deviation of returns)"""
        if symbol not in self.closes or len(self.closes[symbol]) < self.lookback_period:
            self.historical_volatility[symbol] = 0.0
            return
            
        # Calculate log returns
        returns = []
        for i in range(1, len(self.closes[symbol])):
            if self.closes[symbol][i-1] > 0:
                ret = math.log(self.closes[symbol][i] / self.closes[symbol][i-1])
                returns.append(ret)
                
        # Calculate standard deviation of returns
        if len(returns) >= 2:
            self.historical_volatility[symbol] = statistics.stdev(returns) * math.sqrt(252)  # Annualized
        else:
            self.historical_volatility[symbol] = 0.0
            
    def _update_volatility_percentile(self, symbol: str) -> None:
        """Update the current volatility percentile based on historical ATR"""
        if symbol not in self.atr_history or not self.atr_history[symbol] or self.atr[symbol] == 0:
            self.volatility_percentile[symbol] = 0.5  # Default to mid-range
            return
            
        # Calculate percentile
        below_current = sum(1 for a in self.atr_history[symbol] if a <= self.atr[symbol])
        self.volatility_percentile[symbol] = below_current / len(self.atr_history[symbol])
        
    def get_stop_distance(self, symbol: str, price: float) -> float:
        """
        Get recommended stop loss distance based on current volatility
        
        Args:
            symbol: Trading symbol
            price: Current price to base stop distance on
            
        Returns:
            Recommended stop distance in price units
        """
        if symbol not in self.atr or self.atr[symbol] == 0 or price == 0:
            return 0.0
            
        # Base distance on ATR
        base_distance = self.atr[symbol] * self.atr_multiplier
        
        # Adjust for volatility percentile
        # Higher volatility = wider stops
        if self.volatility_percentile[symbol] > 0.8:  # Extremely high volatility
            base_distance *= 1.5
        elif self.volatility_percentile[symbol] > 0.6:  # Above average volatility
            base_distance *= 1.2
        elif self.volatility_percentile[symbol] < 0.2:  # Very low volatility
            base_distance *= 0.8
            
        return base_distance
        
    def get_risk_adjustment(self, symbol: str) -> float:
        """
        Get recommended position size adjustment based on current volatility
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Multiplier for position size (0.0 - 1.0)
        """
        if symbol not in self.volatility_percentile:
            return 1.0  # Default no adjustment
            
        # Conservative approach: higher volatility = smaller position
        if self.volatility_percentile[symbol] > 0.9:  # Extremely high volatility
            return 0.25  # 25% of normal size
        elif self.volatility_percentile[symbol] > 0.75:  # High volatility
            return 0.5  # 50% of normal size
        elif self.volatility_percentile[symbol] > 0.6:  # Above average volatility
            return 0.75  # 75% of normal size
        elif self.volatility_percentile[symbol] < 0.2:  # Very low volatility
            return 1.25  # 125% of normal size (can take slightly larger position)
        else:
            return 1.0  # Normal position size
            
    def get_volatility_state(self, symbol: str) -> Dict[str, Any]:
        """Get current volatility state and metrics for a symbol"""
        if symbol not in self.volatility_percentile:
            return {
                "state": "unknown",
                "atr": 0.0,
                "atr_percent": 0.0,
                "historical_volatility": 0.0,
                "percentile": 0.5,
                "suggested_adjustment": 1.0
            }
            
        state = "normal"
        
        if self.volatility_percentile[symbol] > 0.9:
            state = "extremely_high"
        elif self.volatility_percentile[symbol] > 0.75:
            state = "high"
        elif self.volatility_percentile[symbol] > 0.6:
            state = "above_average"
        elif self.volatility_percentile[symbol] < 0.2:
            state = "low"
        elif self.volatility_percentile[symbol] < 0.1:
            state = "extremely_low"
            
        return {
            "state": state,
            "atr": self.atr.get(symbol, 0.0),
            "atr_percent": self.atr_percent.get(symbol, 0.0),
            "historical_volatility": self.historical_volatility.get(symbol, 0.0),
            "percentile": self.volatility_percentile.get(symbol, 0.5),
            "suggested_adjustment": self.get_risk_adjustment(symbol)
        }

# Advanced Position Sizing
class AdvancedPositionSizer:
    """
    Advanced position sizing that incorporates Kelly criterion,
    market volatility, win rate, and other risk factors.
    """
    def __init__(self, 
               max_risk_per_trade=0.02,     # Default 2% per trade
               max_portfolio_risk=0.06,     # Default 6% total portfolio risk
               apply_kelly_criterion=True,  # Whether to apply Kelly criterion
               kelly_fraction=0.5,          # Conservative Kelly (half-Kelly)
               use_atr_for_stops=True,      # Use ATR for stop loss calculation
               position_size_rounding=True, # Round position sizes to standard lots
               limit_risk_per_instrument=True, # Limit risk per instrument category
               adaptive_sizing=True         # Adjust size based on win rate and expectancy
               ):
        """Initialize with configurable risk parameters"""
        self.max_risk_per_trade = max_risk_per_trade
        self.max_portfolio_risk = max_portfolio_risk
        self.apply_kelly_criterion = apply_kelly_criterion
        self.kelly_fraction = kelly_fraction
        self.use_atr_for_stops = use_atr_for_stops
        self.position_size_rounding = position_size_rounding
        self.limit_risk_per_instrument = limit_risk_per_instrument
        self.adaptive_sizing = adaptive_sizing
        
        # Performance tracking for Kelly criterion
        self.win_count = 0
        self.loss_count = 0
        self.win_amounts = []
        self.loss_amounts = []
        self.current_streak = 0  # +ve for wins, -ve for losses
        
        # Portfolio risk tracking
        self.current_portfolio_risk = 0.0
        self.open_positions_count = 0
    
    def calculate_position_size(self, 
                               account_balance: float, 
                               risk_percentage: float, 
                               entry_price: float, 
                               stop_loss: float,
                               volatility_state: Dict[str, Any] = None,
                               market_regime: Tuple[str, float] = None,
                               risk_manager: Any = None,
                               timeframe: str = "H1",
                               instrument: str = "",
                               instrument_type: str = "FOREX") -> Dict[str, Any]:
        """
        Calculate optimal position size based on multiple risk factors
        
        Args:
            account_balance: Current account balance
            risk_percentage: Base risk percentage for this trade
            entry_price: Entry price
            stop_loss: Stop loss price
            volatility_state: Optional dict with volatility information
            market_regime: Optional tuple of (regime_name, confidence)
            risk_manager: Optional risk manager instance for correlation checks
            timeframe: Timeframe for the trade
            instrument: Instrument symbol
            instrument_type: Type of instrument (FOREX, CRYPTO, etc.)
            
        Returns:
            Dict with position_size and risk details
        """
        # Enforce max risk per trade
        risk_percentage = min(risk_percentage, self.max_risk_per_trade * 100)
        
        # Calculate stop distance
        if entry_price <= 0 or stop_loss <= 0:
            return {"position_size": 0, "risk_amount": 0, "error": "Invalid price inputs"}
            
        stop_distance = abs(entry_price - stop_loss)
        if stop_distance <= 0:
            return {"position_size": 0, "risk_amount": 0, "error": "Invalid stop distance"}
        
        # Calculate pip/point value based on instrument type
        point_size = self._convert_to_pips(stop_distance, instrument_type)
        
        # Baseline risk amount
        risk_amount = account_balance * (risk_percentage / 100)
        
        # Apply Kelly criterion if enabled
        kelly_factor = 1.0
        if self.apply_kelly_criterion and (self.win_count + self.loss_count) >= 10:
            kelly_factor = self._calculate_kelly_fraction()
            risk_amount *= kelly_factor
            
        # Apply winning/losing streak adjustment
        if self.adaptive_sizing and abs(self.current_streak) >= 3:
            streak_factor = self._calculate_streak_multiplier()
            risk_amount *= streak_factor
            
        # Apply volatility adjustment if provided
        volatility_factor = 1.0
        if volatility_state is not None:
            volatility_factor = volatility_state.get("suggested_adjustment", 1.0)
            risk_amount *= volatility_factor
            
        # Apply market regime adjustment if provided
        regime_factor = 1.0
        if market_regime is not None:
            regime, confidence = market_regime
            # Apply conservative sizing for uncertain regimes
            if regime in ["ranging", "volatile", "choppy_volatile"]:
                regime_factor = 0.7  # 70% of normal size
            elif regime in ["mixed"]:
                regime_factor = 0.8  # 80% of normal size
            elif regime in ["trending_volatile"]:
                regime_factor = 0.9  # 90% of normal size
                
            # Scale by confidence
            regime_factor = 0.5 + (regime_factor * confidence * 0.5)
            risk_amount *= regime_factor
            
        # Apply correlation adjustment if provided
        correlation_factor = 1.0
        if risk_manager is not None and hasattr(risk_manager, "get_correlation_factor"):
            correlation_factor = risk_manager.get_correlation_factor(instrument)
            risk_amount *= correlation_factor
            
        # Apply portfolio risk cap
        portfolio_factor = 1.0
        if self.current_portfolio_risk + (risk_percentage / 100) > self.max_portfolio_risk:
            # Scale down to fit within max portfolio risk
            available_risk = max(0, self.max_portfolio_risk - self.current_portfolio_risk)
            if risk_percentage / 100 > 0:
                portfolio_factor = available_risk / (risk_percentage / 100)
                risk_amount *= portfolio_factor
                
        # Calculate position size based on adjusted risk
        if point_size > 0:
            # Units = Risk amount / (pip risk * pip value)
            position_size = risk_amount / (point_size)
        else:
            return {"position_size": 0, "risk_amount": 0, "error": "Invalid point size"}
            
        # Round position size if enabled
        if self.position_size_rounding:
            standard_lot = self._get_standard_lot_size(instrument_type)
            position_size = self._round_to_standard_size(position_size, standard_lot)
            
        # Recalculate actual risk amount after rounding
        actual_risk_amount = position_size * point_size
        actual_risk_percentage = (actual_risk_amount / account_balance) * 100
            
        # Return detailed information
        return {
            "position_size": position_size,
            "risk_amount": actual_risk_amount,
            "risk_percentage": actual_risk_percentage,
            "stop_distance": stop_distance,
            "point_size": point_size,
            "adjustments": {
                "kelly": kelly_factor,
                "volatility": volatility_factor,
                "regime": regime_factor,
                "correlation": correlation_factor,
                "portfolio": portfolio_factor,
                "streak": self._calculate_streak_multiplier() if self.adaptive_sizing else 1.0
            }
        }
        
    def _convert_to_pips(self, price_distance: float, instrument_type: str) -> float:
        """Convert price distance to pips based on instrument type"""
        if instrument_type == "FOREX_JPY":
            return price_distance * 100  # 2 decimal places
        elif instrument_type == "FOREX":
            return price_distance * 10000  # 4 decimal places
        elif instrument_type in ["CRYPTO", "INDEX", "METAL"]:
            return price_distance  # Direct price difference
        else:
            return price_distance  # Default direct price difference
            
    def _get_standard_lot_size(self, instrument_type: str) -> float:
        """Get standard lot size for rounding"""
        if instrument_type == "FOREX":
            return 1000  # Mini lot
        elif instrument_type == "CRYPTO":
            return 0.01  # Typical crypto increment
        else:
            return 1.0  # Default
        
    def _round_to_standard_size(self, position_size: float, standard_lot: float) -> float:
        """Round position size to standard lot increments"""
        if standard_lot <= 0:
            return position_size
            
        # Round down to nearest lot
        return math.floor(position_size / standard_lot) * standard_lot
        
    def _calculate_kelly_fraction(self) -> float:
        """Calculate Kelly Criterion fraction based on win rate and win/loss ratio"""
        total_trades = self.win_count + self.loss_count
        if total_trades < 10:
            return 1.0  # Not enough data
            
        win_rate = self.win_count / total_trades
        
        # Calculate average win and loss
        avg_win = sum(self.win_amounts) / len(self.win_amounts) if self.win_amounts else 0
        avg_loss = sum(self.loss_amounts) / len(self.loss_amounts) if self.loss_amounts else 0
        
        if avg_loss == 0:
            return 1.0  # Avoid division by zero
            
        win_loss_ratio = avg_win / avg_loss
        
        # Kelly formula: f* = p - (1-p)/r
        # where p is win probability, r is win/loss ratio
        kelly = win_rate - ((1 - win_rate) / win_loss_ratio)
        
        # Apply fractional Kelly for conservatism
        kelly = max(0, kelly) * self.kelly_fraction
        
        # Cap at 1.0 (100% normal position size)
        return min(1.0, kelly)
        
    def _calculate_streak_multiplier(self) -> float:
        """Calculate position size multiplier based on winning/losing streak"""
        # No streak adjustment for short streaks
        if abs(self.current_streak) < 3:
            return 1.0
            
        if self.current_streak > 0:
            # Winning streak: gentle increase
            # +3 streak = 1.1x, +4 streak = 1.15x, +5 streak = 1.2x, max 1.3x at +10
            multiplier = min(1.3, 1.0 + (self.current_streak - 2) * 0.05)
        else:
            # Losing streak: more aggressive decrease
            # -3 streak = 0.8x, -4 streak = 0.7x, -5 streak = 0.6x, min 0.3x at -10
            multiplier = max(0.3, 1.0 - (abs(self.current_streak) - 2) * 0.1)
            
        return multiplier
        
    def update_performance_stats(self, 
                                win: bool, 
                                profit_loss_percentage: float, 
                                risk_percentage: float) -> None:
        """
        Update performance statistics for Kelly calculation
        
        Args:
            win: Whether the trade was a win
            profit_loss_percentage: P/L as percentage of account
            risk_percentage: Initial risk percentage for the trade
        """
        if win:
            self.win_count += 1
            self.win_amounts.append(profit_loss_percentage)
            # Update streak
            self.current_streak = max(0, self.current_streak) + 1
        else:
            self.loss_count += 1
            self.loss_amounts.append(profit_loss_percentage)
            # Update streak
            self.current_streak = min(0, self.current_streak) - 1
            
        # Keep only the last 100 trades for win/loss amounts
        if len(self.win_amounts) > 100:
            self.win_amounts.pop(0)
        if len(self.loss_amounts) > 100:
            self.loss_amounts.pop(0)
            
    def register_open_position(self, position_risk: float) -> None:
        """Register a new open position for portfolio risk tracking"""
        self.current_portfolio_risk += position_risk
        self.open_positions_count += 1
        
    def remove_closed_position(self, position_risk: float) -> None:
        """Remove a closed position from portfolio risk tracking"""
        self.current_portfolio_risk = max(0, self.current_portfolio_risk - position_risk)
        self.open_positions_count = max(0, self.open_positions_count - 1)
        
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get current performance statistics"""
        total_trades = self.win_count + self.loss_count
        win_rate = self.win_count / total_trades if total_trades > 0 else 0
        
        avg_win = sum(self.win_amounts) / len(self.win_amounts) if self.win_amounts else 0
        avg_loss = sum(self.loss_amounts) / len(self.loss_amounts) if self.loss_amounts else 0
        
        return {
            "total_trades": total_trades,
            "win_rate": win_rate,
            "avg_win_pct": avg_win,
            "avg_loss_pct": avg_loss,
            "current_streak": self.current_streak,
            "kelly_factor": self._calculate_kelly_fraction(),
            "portfolio_risk": self.current_portfolio_risk,
            "open_positions": self.open_positions_count
        }
        
    def suggest_stop_loss(self, 
                         entry_price: float, 
                         direction: str, 
                         atr_value: float = None,
                         instrument_type: str = "FOREX",
                         timeframe: str = "H1") -> float:
        """
        Suggest stop loss level based on ATR or fixed percentage
        
        Args:
            entry_price: Entry price
            direction: Trade direction ('BUY' or 'SELL')
            atr_value: ATR value if available
            instrument_type: Type of instrument
            timeframe: Timeframe for the trade
            
        Returns:
            Suggested stop loss price
        """
        if entry_price <= 0:
            return 0
            
        # Default settings for different timeframes (% of price)
        timeframe_stops = {
            "M1": 0.001,  # 0.1%
            "M5": 0.002,  # 0.2%
            "M15": 0.003, # 0.3%
            "M30": 0.004, # 0.4%
            "H1": 0.005,  # 0.5%
            "H4": 0.01,   # 1%
            "D1": 0.02    # 2%
        }
        
        # Default multipliers for ATR-based stops
        timeframe_multipliers = {
            "M1": 1.0,
            "M5": 1.5,
            "M15": 1.5,
            "M30": 1.5,
            "H1": 2.0,
            "H4": 2.5,
            "D1": 3.0
        }
        
        if self.use_atr_for_stops and atr_value is not None and atr_value > 0:
            # Use ATR for stop loss calculation
            multiplier = timeframe_multipliers.get(timeframe, 2.0)
            stop_distance = atr_value * multiplier
        else:
            # Use fixed percentage based on timeframe and instrument
            base_percent = timeframe_stops.get(timeframe, 0.005)
            
            # Adjust for instrument (crypto is more volatile)
            if instrument_type == "CRYPTO":
                base_percent *= 3  # 3x wider for crypto
            elif instrument_type == "FOREX_JPY":
                base_percent *= 0.8  # 0.8x for JPY pairs (narrower)
                
            stop_distance = entry_price * base_percent
            
        # Calculate stop price based on direction
        if direction.upper() == "BUY":
            stop_loss = entry_price - stop_distance
        else:  # SELL
            stop_loss = entry_price + stop_distance
            
        return stop_loss
        
    def suggest_take_profit_levels(self, 
                                  entry_price: float, 
                                  stop_loss: float, 
                                  direction: str,
                                  timeframe: str = "H1") -> Dict[str, float]:
        """
        Suggest multi-level take profit targets based on R-multiples
        
        Args:
            entry_price: Entry price
            stop_loss: Stop loss price
            direction: Trade direction ('BUY' or 'SELL')
            timeframe: Timeframe for the trade
            
        Returns:
            Dictionary with take profit levels
        """
        if entry_price <= 0 or stop_loss <= 0:
            return {"tp1": 0, "tp2": 0, "tp3": 0}
            
        # Calculate risk distance (1R)
        risk_distance = abs(entry_price - stop_loss)
        if risk_distance <= 0:
            return {"tp1": 0, "tp2": 0, "tp3": 0}
            
        # R-multiples based on timeframe
        timeframe_r_multiples = {
            "M1": {"tp1": 1.0, "tp2": 1.5, "tp3": 2.0},
            "M5": {"tp1": 1.0, "tp2": 1.5, "tp3": 2.5},
            "M15": {"tp1": 1.5, "tp2": 2.0, "tp3": 3.0},
            "M30": {"tp1": 1.5, "tp2": 2.5, "tp3": 3.5},
            "H1": {"tp1": 1.5, "tp2": 3.0, "tp3": 5.0},
            "H4": {"tp1": 2.0, "tp2": 3.5, "tp3": 6.0},
            "D1": {"tp1": 2.5, "tp2": 5.0, "tp3": 8.0}
        }
        
        # Get R-multiples for this timeframe
        r_multiples = timeframe_r_multiples.get(timeframe, {"tp1": 1.5, "tp2": 3.0, "tp3": 5.0})
        
        # Calculate take profit levels
        if direction.upper() == "BUY":
            tp1 = entry_price + (risk_distance * r_multiples["tp1"])
            tp2 = entry_price + (risk_distance * r_multiples["tp2"])
            tp3 = entry_price + (risk_distance * r_multiples["tp3"])
        else:  # SELL
            tp1 = entry_price - (risk_distance * r_multiples["tp1"])
            tp2 = entry_price - (risk_distance * r_multiples["tp2"])
            tp3 = entry_price - (risk_distance * r_multiples["tp3"])
            
        return {
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "r_multiples": r_multiples
        }

# Time-Based Exit Manager
class TimeBasedExitManager:
    """
    Manages position exits based on time in market and time-based conditions
    such as market sessions, weekend risk, and time decay.
    """
    def __init__(self, max_duration_hours=48, session_adjustments=True):
        """Initialize with configurable parameters"""
        self.max_duration_hours = max_duration_hours
        self.session_adjustments = session_adjustments
        
        # Position tracking
        self.positions = {}  # position_id -> position data
        
        # Market sessions
        self.sessions = {
            "sydney": {"start_hour": 22, "end_hour": 7},  # UTC hours
            "tokyo": {"start_hour": 0, "end_hour": 9},
            "london": {"start_hour": 8, "end_hour": 17},
            "new_york": {"start_hour": 13, "end_hour": 22}
        }
        
        # Weekend timing
        self.weekend_start = (4, 22)  # Friday 22:00 UTC
        self.weekend_end = (0, 22)    # Sunday 22:00 UTC
        
        # Success metrics
        self.total_exits = 0
        self.successful_exits = 0  # Exits that saved money
        
    def register_position(self, 
                         position_id: str,
                         symbol: str,
                         direction: str,
                         entry_time: datetime,
                         timeframe: str,
                         max_duration_override: Optional[int] = None) -> None:
        """
        Register a new position for time-based monitoring
        
        Args:
            position_id: Unique position identifier
            symbol: Trading symbol
            direction: Position direction ('BUY' or 'SELL')
            entry_time: Entry timestamp
            timeframe: Trading timeframe
            max_duration_override: Optional override for max duration in hours
        """
        # Determine base duration based on timeframe
        if max_duration_override is not None:
            base_duration = max_duration_override
        else:
            # Default durations by timeframe
            durations = {
                "M1": 2,      # 2 hours
                "M5": 5,      # 5 hours
                "M15": 10,    # 10 hours
                "M30": 24,    # 24 hours
                "H1": 48,     # 48 hours
                "H4": 96,     # 96 hours
                "D1": 240     # 10 days
            }
            base_duration = durations.get(timeframe, self.max_duration_hours)
        
        # Store position data
        self.positions[position_id] = {
            "symbol": symbol,
            "direction": direction,
            "entry_time": entry_time,
            "timeframe": timeframe,
            "max_duration": base_duration,
            "notified": False,
            "weekend_adjusted": False,
            "session_adjusted": False,
            "exit_reason": None,
            "exit_time": None
        }
        
    def remove_position(self, position_id: str) -> None:
        """Remove a position from monitoring"""
        if position_id in self.positions:
            del self.positions[position_id]
            
    def get_current_session(self, current_time: Optional[datetime] = None) -> str:
        """Get the current market session"""
        if current_time is None:
            current_time = datetime.now(timezone.utc)
            
        hour = current_time.hour
        weekday = current_time.weekday()  # 0 = Monday, 6 = Sunday
        
        # Check if weekend
        if (weekday == 4 and hour >= self.weekend_start[1]) or \
           weekday == 5 or \
           (weekday == 6 and hour < self.weekend_end[1]):
            return "weekend"
            
        # Check each session
        active_sessions = []
        for session, hours in self.sessions.items():
            start = hours["start_hour"]
            end = hours["end_hour"]
            
            if start < end:  # Same day session
                if start <= hour < end:
                    active_sessions.append(session)
            else:  # Overnight session
                if hour >= start or hour < end:
                    active_sessions.append(session)
                    
        if not active_sessions:
            return "quiet"
        elif len(active_sessions) == 1:
            return active_sessions[0]
        else:
            # Overlapping sessions, determine primary one
            if "london" in active_sessions and "new_york" in active_sessions:
                return "london_new_york"  # High volatility overlap
            elif "tokyo" in active_sessions and "london" in active_sessions:
                return "tokyo_london"     # Asian/European overlap
            else:
                return active_sessions[0]
                
    def is_weekend(self, current_time: Optional[datetime] = None) -> bool:
        """Check if current time is during weekend market closure"""
        if current_time is None:
            current_time = datetime.now(timezone.utc)
            
        hour = current_time.hour
        weekday = current_time.weekday()  # 0 = Monday, 6 = Sunday
        
        return (weekday == 4 and hour >= self.weekend_start[1]) or \
               weekday == 5 or \
               (weekday == 6 and hour < self.weekend_end[1])
               
    def _adjust_max_duration(self, position: Dict[str, Any], current_time: datetime) -> None:
        """Adjust maximum duration based on market conditions"""
        # Skip if already adjusted
        if position["session_adjusted"]:
            return
            
        current_session = self.get_current_session(current_time)
        symbol = position["symbol"]
        direction = position["direction"]
        instrument_type = "FOREX"  # Default, would be determined by symbol prefix in real system
        
        # Detect if symbol is crypto (for demo purposes)
        if symbol.startswith("BTC") or symbol.startswith("ETH") or "USD" in symbol:
            instrument_type = "CRYPTO"
            
        # Weekend adjustment for non-crypto
        if self.is_weekend(current_time) and instrument_type != "CRYPTO":
            # Weekend approaching/ongoing - reduce max duration
            time_to_sunday = (6 - current_time.weekday()) % 7  # Days until Sunday
            if time_to_sunday <= 1:  # Friday or Saturday
                position["max_duration"] = min(position["max_duration"], 8)  # Max 8 hours
                position["weekend_adjusted"] = True
        
        # Session-based adjustments
        if self.session_adjustments:
            # Adjust based on current session
            if current_session == "london_new_york" and instrument_type == "FOREX":
                # High volatility overlap - reduce duration for mean reversion strategies
                if "MEAN_REVERSION" in position.get("strategy_type", ""):
                    position["max_duration"] = min(position["max_duration"], 6)  # Max 6 hours
                    
            elif current_session == "quiet":
                # Low volatility period - reduce duration for all positions
                position["max_duration"] = max(4, position["max_duration"] * 0.7)
                
            # Align with upcoming session
            if direction.upper() == "BUY" and current_time.hour >= 20:
                # Buy position late in day - extend to catch Asian session
                position["max_duration"] = max(position["max_duration"], 12)
                
        position["session_adjusted"] = True
        
    def check_time_exits(self, 
                       current_time: Optional[datetime] = None,
                       market_prices: Optional[Dict[str, float]] = None) -> List[Dict[str, Any]]:
        """
        Check for time-based exits across all monitored positions
        
        Args:
            current_time: Optional current time for testing
            market_prices: Optional dict of current market prices
            
        Returns:
            List of positions that should be exited with reasons
        """
        if current_time is None:
            current_time = datetime.now(timezone.utc)
            
        exits = []
        
        for position_id, position in list(self.positions.items()):
            # Apply duration adjustments
            self._adjust_max_duration(position, current_time)
            
            # Calculate time in market
            entry_time = position["entry_time"]
            time_in_market = (current_time - entry_time).total_seconds() / 3600  # Hours
            
            exit_reason = None
            
            # Check maximum duration
            if time_in_market >= position["max_duration"]:
                exit_reason = "max_duration"
                
            # Check weekend for non-crypto
            elif self.is_weekend(current_time) and not position["symbol"].startswith("BTC"):
                # Only exit if approaching weekend (Friday)
                if current_time.weekday() == 4 and current_time.hour >= 20:
                    exit_reason = "weekend_approaching"
                    
            # Check if position has been open for >80% of max duration but no notification yet
            elif time_in_market >= (position["max_duration"] * 0.8) and not position["notified"]:
                position["notified"] = True
                # Don't exit but mark for notification
                continue
                
            if exit_reason:
                # Record the exit
                position["exit_reason"] = exit_reason
                position["exit_time"] = current_time
                
                # Add to list of positions to exit
                exits.append({
                    "position_id": position_id,
                    "symbol": position["symbol"],
                    "reason": exit_reason,
                    "time_in_market": time_in_market,
                    "exit_time": current_time
                })
                
                # Remove from monitoring
                self.total_exits += 1
                
        return exits
        
    def check_single_position(self, 
                           position_id: str,
                           current_time: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        """
        Check a single position for time-based exit conditions
        
        Args:
            position_id: Position ID to check
            current_time: Optional current time
            
        Returns:
            Exit details if position should be exited, None otherwise
        """
        if position_id not in self.positions:
            return None
            
        if current_time is None:
            current_time = datetime.now(timezone.utc)
            
        position = self.positions[position_id]
        
        # Apply duration adjustments
        self._adjust_max_duration(position, current_time)
        
        # Calculate time in market
        entry_time = position["entry_time"]
        time_in_market = (current_time - entry_time).total_seconds() / 3600  # Hours
        
        exit_reason = None
        
        # Check maximum duration
        if time_in_market >= position["max_duration"]:
            exit_reason = "max_duration"
            
        # Check weekend for non-crypto
        elif self.is_weekend(current_time) and not position["symbol"].startswith("BTC"):
            # Only exit if approaching weekend (Friday)
            if current_time.weekday() == 4 and current_time.hour >= 20:
                exit_reason = "weekend_approaching"
                
        if exit_reason:
            # Record the exit
            position["exit_reason"] = exit_reason
            position["exit_time"] = current_time
            
            # Remove from monitoring
            del self.positions[position_id]
            self.total_exits += 1
            
            return {
                "position_id": position_id,
                "symbol": position["symbol"],
                "reason": exit_reason,
                "time_in_market": time_in_market,
                "exit_time": current_time
            }
            
        return None
        
    def record_exit_outcome(self, position_id: str, saved_money: bool) -> None:
        """
        Record whether a time-based exit saved money
        
        Args:
            position_id: Position ID
            saved_money: Whether the exit saved money compared to later price action
        """
        if saved_money:
            self.successful_exits += 1
            
    def get_optimal_hold_time(self, 
                            symbol: str, 
                            timeframe: str,
                            direction: str) -> int:
        """
        Get optimal hold time based on historical performance
        
        Args:
            symbol: Trading symbol
            timeframe: Trading timeframe
            direction: Trade direction
            
        Returns:
            Suggested hold time in hours
        """
        # In a real system, this would analyze past trades
        # Here we provide reasonable defaults based on timeframe
        
        base_durations = {
            "M1": 2,     # 2 hours
            "M5": 4,     # 4 hours
            "M15": 6,    # 6 hours
            "M30": 12,   # 12 hours
            "H1": 24,    # 24 hours
            "H4": 48,    # 48 hours
            "D1": 120    # 5 days
        }
        
        # Get base duration from timeframe
        base = base_durations.get(timeframe, 24)
        
        # Apply symbol-specific adjustment
        if "JPY" in symbol:
            base *= 0.8  # JPY pairs typically have shorter optimal durations
        elif "USD" in symbol:
            base *= 1.0  # Standard duration for USD pairs
        elif "BTC" in symbol or "ETH" in symbol:
            base *= 0.7  # Crypto often has shorter optimal durations
            
        return int(base)
        
    def get_success_rate(self) -> float:
        """Get success rate of time-based exits"""
        if self.total_exits == 0:
            return 0.0
        return self.successful_exits / self.total_exits
        
    def get_time_until_exit(self, position_id: str) -> Optional[float]:
        """Get estimated time until exit for a position in hours"""
        if position_id not in self.positions:
            return None
            
        position = self.positions[position_id]
        current_time = datetime.now(timezone.utc)
        
        # Apply duration adjustments if not already done
        if not position["session_adjusted"]:
            self._adjust_max_duration(position, current_time)
            
        # Calculate time in market
        entry_time = position["entry_time"]
        time_in_market = (current_time - entry_time).total_seconds() / 3600  # Hours
        
        # Calculate time remaining
        time_remaining = position["max_duration"] - time_in_market
        
        return max(0, time_remaining)

# Global instances 
risk_manager = None  # Will be initialized during startup
market_analysis = None  # Will be initialized during startup
position_tracker = None
error_recovery_system = None
client_session = None
settings = None

# Advanced components
circuit_breaker = None
volatility_monitor = None
advanced_position_sizer = None
lorentzian_classifier = None
time_exit_manager = None
multi_stage_tp_manager = None  # New component for managing multiple take profit targets
alert_handler = None
correlation_analyzer = None  # Correlation analysis component
market_structure_analyzer = None  # Advanced market structure analysis
dynamic_exit_manager = None  # Dynamic exit management system
advanced_loss_management = None  # Advanced loss management system
exchange_adapter = None  # Exchange-specific adapter
backtest_engine = None  # New global variable for backtest engine

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan events for application startup and shutdown"""
    # Startup
    await setup_initial_dependencies()
    
    # Global instances setup
    global settings, position_tracker, error_recovery_system, risk_manager, market_analysis
    global circuit_breaker, volatility_monitor, advanced_position_sizer, lorentzian_classifier, time_exit_manager, multi_stage_tp_manager
    global market_structure_analyzer, dynamic_exit_manager, advanced_loss_management
    global exchange_adapter, backtest_engine
    
    # Create background tasks
    position_monitor_task = asyncio.create_task(position_monitor())
    
    yield  # Application runs here
    
    # Shutdown
    await cleanup_resources()
    
    # Cancel background tasks
    position_monitor_task.cancel()
    try:
        await position_monitor_task
    except asyncio.CancelledError:
        pass

# Register the lifespan with the application
app.router.lifespan_context = lifespan

async def setup_initial_dependencies():
    """Setup initial dependencies and global instances"""
    global settings, position_tracker, error_recovery_system, risk_manager, market_analysis, client_session
    global circuit_breaker, volatility_monitor, advanced_position_sizer, lorentzian_classifier, time_exit_manager, multi_stage_tp_manager
    global alert_handler, correlation_analyzer, dynamic_exit_manager, trading_config, market_structure_analyzer, advanced_loss_management
    global exchange_adapter, backtest_engine
    
    # Initialize trading configuration
    trading_config = TradingConfig()
    
    # Initialize settings
    settings = Settings()
    
    # Initialize position tracker
    position_tracker = PositionTracker()
    await position_tracker.start()
    
    # Initialize error recovery system
    error_recovery_system = ErrorRecoverySystem()
    await error_recovery_system.start()
    
    # Initialize circuit breaker for additional safety
    circuit_breaker = CircuitBreaker(
        error_threshold=int(trading_config.get_value("circuit_breaker", "error_threshold", 5)),
        window_seconds=int(trading_config.get_value("circuit_breaker", "window_seconds", 300)),
        cooldown_seconds=int(trading_config.get_value("circuit_breaker", "cooldown_seconds", 600))
    )
    
    # Initialize risk manager
    risk_manager = RiskManager()
    
    # Initialize market analysis system
    market_analysis = MarketAnalysis()
    
    # Initialize HTTP session
    client_session = await get_session()
    
    # Initialize volatility monitor
    volatility_monitor = VolatilityMonitor()
    
    # Initialize advanced position sizer
    advanced_position_sizer = AdvancedPositionSizer()
    
    # Initialize Lorentzian classifier for market regimes
    lorentzian_classifier = LorentzianClassifier()
    
    # Initialize time-based exit manager
    time_exit_manager = TimeBasedExitManager()
    
    # Initialize multi-stage take profit manager
    multi_stage_tp_manager = MultiStageTakeProfitManager(position_tracker=position_tracker)
    multi_stage_tp_manager.start()
    
    # Initialize market structure analyzer
    market_structure_analyzer = MarketStructureAnalyzer()
    market_structure_analyzer.start()
    
    # Initialize dynamic exit manager
    dynamic_exit_manager = DynamicExitManager(position_tracker, volatility_monitor)
    await dynamic_exit_manager.start()
    
    # Initialize advanced loss management
    advanced_loss_management = AdvancedLossManagement(position_tracker)
    await advanced_loss_management.start()
    
    # Initialize exchange adapter
    exchange_adapter = await ExchangeAdapterFactory.create_adapter(
        "oanda", 
        {
            "account_id": settings.oanda_account,
            "api_token": settings.oanda_token,
            "api_url": settings.oanda_api_url,
            "timeout": settings.total_timeout
        }
    )
    
    # Initialize alert handler
    alert_handler = AlertHandler()
    await alert_handler.start()
    
    # Initialize correlation analyzer
    correlation_analyzer = CorrelationAnalyzer()
    
    # Initialize logging
    setup_logging()
    
    # Initialize backtest engine with default config (will be changed via API)
    default_backtest_config = BacktestConfig(
        start_date=datetime.now() - timedelta(days=30),
        end_date=datetime.now(),
        initial_capital=100000.0,
        symbols=["EUR_USD", "GBP_USD", "USD_JPY"],
        data_frequency="1h"
    )
    backtest_engine = BacktestEngine(default_backtest_config)
    await backtest_engine.initialize()
    
    # Initialize data downloader
    global data_downloader
    data_downloader = DataDownloader()
    await data_downloader.initialize()
    
    logger.info("All dependencies initialized successfully")

async def position_monitor():
    """Background task to monitor positions"""
    try:
        while True:
            # Verify position tracker is initialized
            if position_tracker and position_tracker.initialized:
                # Get all open positions
                positions = await position_tracker.get_all_positions()
                
                # Update positions with current prices
                for symbol, symbol_positions in positions.items():
                    try:
                        # Get current price for symbol
                        current_price = 100.0  # Placeholder - in real implementation, get actual price from market data
                        
                        # Update regular stop loss/take profit
                        await position_tracker.update_position_exits(symbol, current_price)
                        
                        # Check for exit conditions with dynamic exit manager
                        for position in symbol_positions:
                            try:
                                position_id = position.get('position_id')
                                if not position_id:
                                    continue
                                
                                # Check if any exit conditions are triggered
                                exit_result = await dynamic_exit_manager.execute_exit_if_triggered(
                                    position_id=position_id,
                                    current_price=current_price
                                )
                                
                                if exit_result.get("executed", False):
                                    logger.info(f"Position {position_id} exited: {exit_result.get('reason')} at price {exit_result.get('price')}")
                                
                            except Exception as e:
                                logger.error(f"Error checking exits for position {position_id}: {str(e)}")
                                logger.error(traceback.format_exc())
                    except Exception as e:
                        logger.error(f"Error updating positions for {symbol}: {str(e)}")
                        logger.error(traceback.format_exc())
            
            # Sleep for 60 seconds
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        logger.info("Position monitor task cancelled")
    except Exception as e:
        logger.error(f"Error in position monitor task: {str(e)}")
        logger.error(traceback.format_exc())

async def cleanup_resources():
    """Cleanup resources when shutting down"""
    global client_session, error_recovery_system, position_tracker, multi_stage_tp_manager
    global market_structure_analyzer, dynamic_exit_manager, advanced_loss_management, alert_handler
    global exchange_adapter, backtest_engine
    
    # Close HTTP session
    if client_session:
        await client_session.close()
    
    # Stop error recovery system
    if error_recovery_system:
        await error_recovery_system.stop()
    
    # Stop position tracker
    if position_tracker:
        await position_tracker.stop()
    
    # Stop multi-stage take profit manager
    if multi_stage_tp_manager:
        multi_stage_tp_manager.stop()
    
    # Stop market structure analyzer
    if market_structure_analyzer:
        market_structure_analyzer.stop()
    
    # Stop dynamic exit manager
    if dynamic_exit_manager:
        await dynamic_exit_manager.stop()
    
    # Stop advanced loss management
    if advanced_loss_management:
        await advanced_loss_management.stop()
    
    # Shutdown exchange adapter
    if exchange_adapter:
        await exchange_adapter.shutdown()
    
    # Stop alert handler
    if alert_handler:
        await alert_handler.stop()
    
    # Cancel any remaining tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    
    await asyncio.gather(*tasks, return_exceptions=True)
    
    # Close data downloader session
    global data_downloader
    if data_downloader:
        await data_downloader.shutdown()
    
    logger.info("All resources cleaned up successfully")

def start():
    """Start the application"""
    setup_logging()
    
    # Load configuration - use try/except to handle missing function
    try:
        from fx.config import load_config_from_file
        load_config_from_file()
    except ImportError:
        logger.info("Using default configuration (load_config_from_file not available)")
    
    # Integrate Configuration API with the main app
    integrate_with_app(app)
    
    # Get configuration
    host = get_config_value("system", "host", "0.0.0.0")
    port = int(get_config_value("system", "port", 10000))
    
    # Add CORS middleware
    allowed_origins = get_config_value("system", "allowed_origins", "*").split(",")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Start server
    logger.info(f"Starting server on {host}:{port}")
    uvicorn.run(app, host=host, port=port)

@app.post("/api/take_profits")
async def set_take_profit_levels(request: Request):
    """Set take profit levels for a position"""
    try:
        data = await request.json()
        
        # Validate required fields
        required_fields = ["position_id", "entry_price", "position_direction", "position_size", "symbol"]
        for field in required_fields:
            if field not in data:
                return JSONResponse(
                    status_code=400,
                    content={"error": f"Missing required field: {field}"}
                )
        
        position_id = data["position_id"]
        entry_price = float(data["entry_price"])
        position_direction = data["position_direction"]
        position_size = float(data["position_size"])
        symbol = data["symbol"]
        
        # Optional fields with defaults
        stop_loss = data.get("stop_loss")
        if stop_loss:
            stop_loss = float(stop_loss)
        
        timeframe = data.get("timeframe", "H1")
        atr_value = data.get("atr_value")
        if atr_value:
            atr_value = float(atr_value)
        
        initial_take_profit = data.get("initial_take_profit")
        if initial_take_profit:
            initial_take_profit = float(initial_take_profit)
        
        volatility_multiplier = data.get("volatility_multiplier", 1.0)
        
        # Set take profit levels
        result = await multi_stage_tp_manager.set_take_profit_levels(
            position_id=position_id,
            entry_price=entry_price,
            stop_loss=stop_loss,
            position_direction=position_direction,
            position_size=position_size,
            symbol=symbol,
            timeframe=timeframe,
            atr_value=atr_value,
            initial_take_profit=initial_take_profit,
            volatility_multiplier=volatility_multiplier
        )
        
        # Enable/disable trailing take profit if specified
        if "trailing_enabled" in data:
            await multi_stage_tp_manager.enable_trailing_take_profit(
                position_id=position_id,
                enabled=data["trailing_enabled"]
            )
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": f"Take profit levels set for position {position_id}",
                "take_profit_levels": result
            }
        )
        
    except Exception as e:
        logger.error(f"Error setting take profit levels: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"error": f"Error setting take profit levels: {str(e)}"}
        )

@app.get("/api/take_profits/{position_id}")
async def get_take_profit_status(position_id: str):
    """Get the status of take profit levels for a position"""
    try:
        if not position_id:
            return JSONResponse(
                status_code=400,
                content={"error": "Position ID is required"}
            )
        
        # Get take profit status for the position
        tp_status = await multi_stage_tp_manager.get_position_take_profit_status(position_id)
        
        if "error" in tp_status:
            return JSONResponse(
                status_code=404,
                content={"error": f"Position not found: {position_id}"}
            )
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "position_id": position_id,
                "take_profit_status": tp_status
            }
        )
        
    except Exception as e:
        logger.error(f"Error getting take profit status: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"error": f"Error getting take profit status: {str(e)}"}
        )

@app.get("/api/take_profits/performance")
async def get_take_profit_performance(timeframe: Optional[str] = None):
    """Get performance metrics for take profit strategies"""
    try:
        # Get take profit performance statistics
        performance = await multi_stage_tp_manager.get_take_profit_performance(timeframe)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "performance_metrics": performance
            }
        )
        
    except Exception as e:
        logger.error(f"Error getting take profit performance: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"error": f"Error getting take profit performance: {str(e)}"}
        )

class AlertHandler:
    """
    Handles trading alerts from various sources including TradingView,
    processes them according to defined rules, and executes appropriate actions.
    """
    def __init__(self):
        """Initialize the alert handler"""
        self.active = False
        self.position_monitor_task = None
        self.alert_queue = asyncio.Queue()
        self.processing_lock = asyncio.Lock()
        self.alert_history = deque(maxlen=100)  # Keep last 100 alerts
        self.tradingview_templates = {
            "entry": {
                "required_fields": ["symbol", "action", "entry_price"],
                "optional_fields": ["timeframe", "stop_loss", "take_profit", "risk_percentage", "comment"]
            },
            "exit": {
                "required_fields": ["symbol", "action"],
                "optional_fields": ["exit_price", "timeframe", "percentage", "comment"]
            },
            "modify": {
                "required_fields": ["symbol", "modify_type"],  # modify_type: "sl", "tp", "both"
                "optional_fields": ["new_sl", "new_tp", "comment"]
            }
        }
        
    async def start(self):
        """Start the alert handler"""
        if self.active:
            return
            
        self.active = True
        self.position_monitor_task = asyncio.create_task(self._monitor_positions())
        logger.info("Alert handler started")
        
    async def stop(self):
        """Stop the alert handler"""
        if not self.active:
            return
            
        self.active = False
        if self.position_monitor_task:
            self.position_monitor_task.cancel()
            try:
                await self.position_monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("Alert handler stopped")
        
    async def _monitor_positions(self):
        """Background task to monitor positions and execute actions"""
        try:
            while self.active:
                try:
                    # Check all open positions
                    if position_tracker and position_tracker.initialized:
                        positions = await position_tracker.get_all_positions()
                        
                        # Process each position
                        for symbol, symbol_positions in positions.items():
                            # Get current price
                            try:
                                # This would be replaced with actual price fetching logic
                                current_price = 100.0  # Placeholder
                                
                                for position_id, position_data in symbol_positions.items():
                                    try:
                                        # Update position metrics
                                        position_type = position_data.get('direction', 'buy')
                                        timeframe = position_data.get('timeframe', '1h')
                                        
                                        # Calculate time in market
                                        entry_time = position_data.get('entry_time')
                                        if entry_time:
                                            time_in_market = (datetime.now(timezone.utc) - entry_time).total_seconds() / 3600  # Hours
                                            
                                            # Check if time-based exit is needed via time_exit_manager
                                            if time_exit_manager:
                                                exit_check = await time_exit_manager.check_single_position(
                                                    position_id=position_id, 
                                                    current_time=datetime.now(timezone.utc)
                                                )
                                                
                                                if exit_check:
                                                    # Time-based exit triggered
                                                    logger.info(f"Time-based exit triggered for {symbol} ({position_id})")
                                                    await self._close_position(position_id=position_id, reason="time_exit")
                                    except Exception as e:
                                        logger.error(f"Error processing position {position_id}: {str(e)}")
                            except Exception as e:
                                logger.error(f"Error fetching price for {symbol}: {str(e)}")
                                
                    # Sleep to avoid high CPU usage
                    await asyncio.sleep(60)  # Check every minute
                except Exception as e:
                    logger.error(f"Error in position monitor: {str(e)}")
                    await asyncio.sleep(60)  # Continue monitoring despite errors
        except asyncio.CancelledError:
            logger.info("Position monitor task cancelled")
        except Exception as e:
            logger.error(f"Fatal error in position monitor: {str(e)}")
            logger.error(traceback.format_exc())
            
    async def _handle_position_actions(self, position_id: str, actions: Dict[str, Any], current_price: float):
        """Execute actions on a position based on analysis"""
        if 'close' in actions and actions['close']:
            await self._close_position(position_id, reason=actions.get('reason', 'signal'))
        elif 'partial_close' in actions and actions['partial_close'] > 0:
            percentage = actions['partial_close']
            await self._close_partial_position(position_id, percentage)
        elif 'modify_sl' in actions and actions['modify_sl'] > 0:
            new_sl = actions['modify_sl']
            # Implementation for modifying stop loss
            position = await position_tracker.get_position_info(position_id)
            if position and 'position_obj' in position:
                position['position_obj'].modify_stop_loss(new_sl)
        elif 'modify_tp' in actions and actions['modify_tp'] > 0:
            new_tp = actions['modify_tp']
            # Implementation for modifying take profit
            position = await position_tracker.get_position_info(position_id)
            if position and 'position_obj' in position:
                position['position_obj'].modify_take_profit(new_tp)
                
    async def _close_position(self, position_id: str, reason: str = "signal"):
        """Close a position completely"""
        try:
            # Get position info
            position = await position_tracker.get_position_info(position_id)
            if not position:
                logger.warning(f"Position {position_id} not found for closing")
                return False
                
            # Calculate P&L
            symbol = position.get('symbol')
            entry_price = position.get('entry_price', 0)
            position_type = position.get('direction', 'buy')
            
            # Get current price (placeholder)
            # Would be replaced with actual price fetching
            current_price = 100.0  
            
            # Close the position
            success = await position_tracker.clear_position(
                position_id=position_id,
                reason=reason
            )
            
            if success:
                logger.info(f"Closed position {position_id} ({symbol}) with reason: {reason}")
                
                # If using time-based exit manager, remove from tracking
                if time_exit_manager:
                    time_exit_manager.remove_position(position_id)
                    
                # If using multi-stage take profit manager, remove from tracking
                if multi_stage_tp_manager:
                    await multi_stage_tp_manager.remove_position(position_id)
                    
                return True
            else:
                logger.error(f"Failed to close position {position_id}")
                return False
        except Exception as e:
            logger.error(f"Error closing position {position_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return False
            
    async def _close_partial_position(self, position_id: str, percentage: float, reason: str = "partial"):
        """Close a percentage of a position"""
        try:
            if percentage <= 0 or percentage >= 100:
                logger.error(f"Invalid partial close percentage: {percentage}%")
                return False
                
            # Get position info
            position = await position_tracker.get_position_info(position_id)
            if not position:
                logger.warning(f"Position {position_id} not found for partial closing")
                return False
                
            # Calculate P&L
            symbol = position.get('symbol')
            entry_price = position.get('entry_price', 0)
            position_type = position.get('direction', 'buy')
            
            # Get current price (placeholder)
            # Would be replaced with actual price fetching
            current_price = 100.0
            
            # Calculate partial close
            position_obj = position.get('position_obj')
            if position_obj:
                # Close partial position
                position_obj.close_partial(current_price, percentage / 100.0, reason)
                logger.info(f"Closed {percentage}% of position {position_id} ({symbol})")
                return True
            else:
                logger.error(f"Position object not found for {position_id}")
                return False
        except Exception as e:
            logger.error(f"Error in partial close of position {position_id}: {str(e)}")
            logger.error(traceback.format_exc())
            return False
            
    async def process_alert(self, alert_data: Dict[str, Any]) -> bool:
        """
        Process an incoming alert and execute the appropriate action
        
        Args:
            alert_data: The alert data including command, symbol, etc.
            
        Returns:
            Boolean indicating success
        """
        # Add to history
        alert_data['timestamp'] = datetime.now(timezone.utc).isoformat()
        self.alert_history.append(alert_data.copy())
        
        try:
            # Check alert type
            alert_type = alert_data.get('type', 'entry').lower()
            
            if alert_type == 'entry':
                return await self._process_entry_alert(alert_data)
            elif alert_type in ['exit', 'close']:
                return await self._process_exit_alert(alert_data)
            elif alert_type == 'modify':
                return await self._process_modify_alert(alert_data)
            else:
                logger.warning(f"Unknown alert type: {alert_type}")
                return False
        except Exception as e:
            logger.error(f"Error processing alert: {str(e)}")
            logger.error(traceback.format_exc())
            return False
            
    async def _process_entry_alert(self, alert_data: Dict[str, Any]) -> bool:
        """Process an entry alert"""
        # Extract key information
        symbol = alert_data.get('symbol')
        action = alert_data.get('action', '').lower()
        
        if not symbol or not action:
            logger.error("Missing required fields in entry alert")
            return False
            
        # Validate action
        if action not in ['buy', 'sell', 'long', 'short']:
            logger.error(f"Invalid action in entry alert: {action}")
            return False
            
        # Standardize action
        if action in ['long', 'buy']:
            action = 'buy'
        else:
            action = 'sell'
            
        # Get other parameters
        timeframe = alert_data.get('timeframe', '1h')
        risk_percentage = float(alert_data.get('risk_percentage', 1.0))
        
        # Entry price - in real system would fetch from market if not provided
        entry_price = float(alert_data.get('entry_price', 100.0))
        
        # Optional stop loss and take profit
        stop_loss = alert_data.get('stop_loss')
        take_profit = alert_data.get('take_profit')
        
        if stop_loss:
            stop_loss = float(stop_loss)
        if take_profit:
            take_profit = float(take_profit)
            
        # Create position ID
        position_id = f"{symbol}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
        
        # Record position
        success = await position_tracker.record_position(
            position_id=position_id,
            symbol=symbol,
            action=action,
            timeframe=timeframe,
            entry_price=entry_price,
            size=1000.0,  # Placeholder, would calculate based on risk
            metadata={
                'source': 'alert',
                'comment': alert_data.get('comment', ''),
                'risk_percentage': risk_percentage
            }
        )
        
        if success:
            logger.info(f"New position {position_id} ({symbol}) created from alert")
            
            # Register with time-based exit manager if available
            if time_exit_manager:
                time_exit_manager.register_position(
                    position_id=position_id,
                    symbol=symbol,
                    direction=action,
                    entry_time=datetime.now(timezone.utc),
                    timeframe=timeframe
                )
                
            # Set up multi-stage take profits if available
            if multi_stage_tp_manager and take_profit:
                await multi_stage_tp_manager.set_take_profit_levels(
                    position_id=position_id,
                    entry_price=entry_price,
                    direction=action,
                    stop_loss=stop_loss
                )
                
            return True
        else:
            logger.error(f"Failed to create position from alert for {symbol}")
            return False
            
    async def _process_exit_alert(self, alert_data: Dict[str, Any]) -> bool:
        """Process an exit alert"""
        # Extract key information
        symbol = alert_data.get('symbol')
        
        if not symbol:
            logger.error("Missing symbol in exit alert")
            return False
            
        # Check if this is a partial close
        percentage = float(alert_data.get('percentage', 100.0))
        
        # Find position by symbol
        position_info = await position_tracker.get_position_info(symbol)
        if not position_info:
            logger.warning(f"No position found for {symbol} to exit")
            return False
            
        position_id = position_info.get('position_id')
        
        # Handle complete or partial close
        if percentage >= 100:
            return await self._close_position(position_id, reason="alert_signal")
        else:
            return await self._close_partial_position(position_id, percentage, reason="alert_signal")
            
    async def _process_modify_alert(self, alert_data: Dict[str, Any]) -> bool:
        """Process a modify alert"""
        # Extract key information
        symbol = alert_data.get('symbol')
        modify_type = alert_data.get('modify_type', '').lower()
        
        if not symbol or not modify_type:
            logger.error("Missing required fields in modify alert")
            return False
            
        # Find position by symbol
        position_info = await position_tracker.get_position_info(symbol)
        if not position_info:
            logger.warning(f"No position found for {symbol} to modify")
            return False
            
        position_id = position_info.get('position_id')
        position_obj = position_info.get('position_obj')
        
        if not position_obj:
            logger.error(f"Position object not found for {position_id}")
            return False
            
        # Process different modification types
        if modify_type in ['sl', 'stop', 'stoploss', 'stop_loss']:
            new_sl = alert_data.get('new_sl')
            if new_sl:
                return position_obj.modify_stop_loss(float(new_sl))
        elif modify_type in ['tp', 'target', 'takeprofit', 'take_profit']:
            new_tp = alert_data.get('new_tp')
            if new_tp:
                return position_obj.modify_take_profit(float(new_tp))
        elif modify_type in ['both', 'all']:
            new_sl = alert_data.get('new_sl')
            new_tp = alert_data.get('new_tp')
            
            sl_success = True
            tp_success = True
            
            if new_sl:
                sl_success = position_obj.modify_stop_loss(float(new_sl))
            if new_tp:
                tp_success = position_obj.modify_take_profit(float(new_tp))
                
            return sl_success and tp_success
        else:
            logger.warning(f"Unknown modify type: {modify_type}")
            return False
            
    async def get_alert_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent alert history"""
        history = list(self.alert_history)
        history.reverse()  # Most recent first
        return history[:limit]
        
    async def get_stats(self) -> Dict[str, Any]:
        """Get alert handler statistics"""
        total_alerts = len(self.alert_history)
        
        # Count by type
        type_counts = defaultdict(int)
        for alert in self.alert_history:
            alert_type = alert.get('type', 'unknown')
            type_counts[alert_type] += 1
            
        # Count by symbol
        symbol_counts = defaultdict(int)
        for alert in self.alert_history:
            symbol = alert.get('symbol', 'unknown')
            symbol_counts[symbol] += 1
            
        return {
            "total_alerts": total_alerts,
            "by_type": dict(type_counts),
            "by_symbol": dict(symbol_counts)
        }

# Global instance of the alert handler
alert_handler = None

@app.post("/tradingview")
async def tradingview_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Handle webhooks from TradingView alerts
    
    This endpoint is designed to receive alerts from TradingView's alert webhook feature
    and process them according to the alert content (entry, exit, etc.)
    """
    try:
        # Get client IP and headers for security logging
        client_ip = request.client.host if request.client else "unknown"
        
        # Get request body (alert payload)
        alert_data = await request.json()
        
        # Basic validation
        if not isinstance(alert_data, dict):
            return JSONResponse(
                status_code=400,
                content={"error": "Invalid alert format, expected JSON object"}
            )
            
        # Process the alert in the background to avoid blocking
        background_tasks.add_task(alert_handler.process_alert, alert_data)
        
        # Log the incoming alert
        logger.info(f"Received TradingView alert from {client_ip}: {json.dumps(alert_data)[:200]}...")
        
        return JSONResponse(
            status_code=202,
            content={
                "success": True,
                "message": "Alert received and queued for processing"
            }
        )
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON received from {client_ip}")
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid JSON data"}
        )
    except Exception as e:
        logger.error(f"Error processing TradingView webhook: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"error": f"Server error: {str(e)}"}
        )

@app.get("/api/circuit-breaker/status")
async def get_circuit_breaker_status():
    """Get current status of the circuit breaker"""
    try:
        if not circuit_breaker:
            return JSONResponse(
                status_code=500,
                content={"error": "Circuit breaker not initialized"}
            )
            
        status = circuit_breaker.get_status()
        
        return JSONResponse(
            status_code=200,
            content=status
        )
    except Exception as e:
        logger.error(f"Error getting circuit breaker status: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Error getting circuit breaker status: {str(e)}"}
        )

@app.post("/api/circuit-breaker/reset")
async def reset_circuit_breaker():
    """Manually reset the circuit breaker"""
    try:
        if not circuit_breaker:
            return JSONResponse(
                status_code=500,
                content={"error": "Circuit breaker not initialized"}
            )
            
        await circuit_breaker.reset()
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Circuit breaker reset successfully",
                "status": circuit_breaker.get_status()
            }
        )
    except Exception as e:
        logger.error(f"Error resetting circuit breaker: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Error resetting circuit breaker: {str(e)}"}
        )

@app.get("/api/alerts/history")
async def get_alert_history(limit: int = 10):
    """Get recent alert history"""
    try:
        if not alert_handler:
            return JSONResponse(
                status_code=500,
                content={"error": "Alert handler not initialized"}
            )
            
        history = await alert_handler.get_alert_history(limit)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "count": len(history),
                "alerts": history
            }
        )
    except Exception as e:
        logger.error(f"Error retrieving alert history: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Error retrieving alert history: {str(e)}"}
        )

@app.get("/api/alerts/stats")
async def get_alert_stats():
    """Get alert processing statistics"""
    try:
        if not alert_handler:
            return JSONResponse(
                status_code=500,
                content={"error": "Alert handler not initialized"}
            )
            
        stats = await alert_handler.get_stats()
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "stats": stats
            }
        )
    except Exception as e:
        logger.error(f"Error retrieving alert statistics: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Error retrieving alert statistics: {str(e)}"}
        )

class CorrelationAnalyzer:
    """
    Analyzes correlation between trading instruments to identify and manage
    portfolio-level risk arising from correlated positions.
    """
    def __init__(self, correlation_threshold=0.7, max_data_points=100, update_frequency_hours=24):
        """Initialize the correlation analyzer"""
        self.correlation_threshold = correlation_threshold
        self.max_data_points = max_data_points
        self.update_frequency_hours = update_frequency_hours
        
        # Store price history for correlation calculation
        self.price_data = {}
        self.correlation_matrix = {}
        self.last_update_time = {}
        self.correlated_pairs = []
        
        # Lock for thread safety
        self._lock = asyncio.Lock()
        
    async def update_price_data(self, symbol: str, price: float) -> None:
        """
        Update price data for a symbol
        
        Args:
            symbol: Trading symbol
            price: Current price
        """
        async with self._lock:
            if symbol not in self.price_data:
                self.price_data[symbol] = []
                self.last_update_time[symbol] = datetime.now(timezone.utc)
                
            # Add new price data
            self.price_data[symbol].append(price)
            
            # Limit the number of data points
            if len(self.price_data[symbol]) > self.max_data_points:
                self.price_data[symbol].pop(0)
                
            # Check if we need to update correlation
            time_since_update = (datetime.now(timezone.utc) - self.last_update_time[symbol]).total_seconds() / 3600
            if time_since_update >= self.update_frequency_hours:
                self.last_update_time[symbol] = datetime.now(timezone.utc)
                await self.update_correlation_matrix()
                
    async def calculate_correlation(self, symbol1: str, symbol2: str) -> float:
        """
        Calculate correlation between two symbols
        
        Args:
            symbol1: First symbol
            symbol2: Second symbol
            
        Returns:
            Correlation coefficient (-1 to 1)
        """
        async with self._lock:
            # Check if we have both symbols
            if symbol1 not in self.price_data or symbol2 not in self.price_data:
                return 0.0
                
            # Get price data
            prices1 = self.price_data[symbol1]
            prices2 = self.price_data[symbol2]
            
            # Ensure we have enough data
            min_required = 10
            if len(prices1) < min_required or len(prices2) < min_required:
                return 0.0
                
            # Match lengths - use the most recent data points
            if len(prices1) > len(prices2):
                prices1 = prices1[-len(prices2):]
            elif len(prices2) > len(prices1):
                prices2 = prices2[-len(prices1):]
                
            try:
                # Calculate correlation using numpy
                correlation = float(np.corrcoef(prices1, prices2)[0, 1])
                
                # Handle NaN values
                if np.isnan(correlation):
                    return 0.0
                    
                return correlation
            except Exception as e:
                logger.error(f"Error calculating correlation between {symbol1} and {symbol2}: {str(e)}")
                return 0.0
                
    async def update_correlation_matrix(self) -> None:
        """Update the entire correlation matrix"""
        async with self._lock:
            symbols = list(self.price_data.keys())
            
            # Skip if we have fewer than 2 symbols
            if len(symbols) < 2:
                return
                
            # Initialize new matrix
            new_matrix = {}
            new_correlated_pairs = []
            
            # Calculate correlation for all pairs
            for i, symbol1 in enumerate(symbols):
                if symbol1 not in new_matrix:
                    new_matrix[symbol1] = {}
                    
                # Self correlation is always 1.0
                new_matrix[symbol1][symbol1] = 1.0
                
                for j in range(i + 1, len(symbols)):
                    symbol2 = symbols[j]
                    
                    if symbol2 not in new_matrix:
                        new_matrix[symbol2] = {}
                        
                    # Calculate correlation
                    correlation = await self.calculate_correlation(symbol1, symbol2)
                    
                    # Store in matrix (symmetric)
                    new_matrix[symbol1][symbol2] = correlation
                    new_matrix[symbol2][symbol1] = correlation
                    
                    # Check if highly correlated
                    if abs(correlation) >= self.correlation_threshold:
                        new_correlated_pairs.append((symbol1, symbol2, correlation))
                        
            # Update matrices
            self.correlation_matrix = new_matrix
            self.correlated_pairs = new_correlated_pairs
            
            # Log update
            logger.info(f"Updated correlation matrix for {len(symbols)} symbols, found {len(new_correlated_pairs)} correlated pairs")
            
    async def get_correlated_symbols(self, target_symbol: str, threshold: Optional[float] = None) -> List[Tuple[str, float]]:
        """
        Get symbols correlated with the target symbol
        
        Args:
            target_symbol: Symbol to find correlations for
            threshold: Optional threshold override
            
        Returns:
            List of tuples (symbol, correlation)
        """
        if threshold is None:
            threshold = self.correlation_threshold
            
        if target_symbol not in self.correlation_matrix:
            return []
            
        correlations = []
        
        for symbol, correlation in self.correlation_matrix[target_symbol].items():
            if symbol != target_symbol and abs(correlation) >= threshold:
                correlations.append((symbol, correlation))
                
        # Sort by absolute correlation (highest first)
        correlations.sort(key=lambda x: abs(x[1]), reverse=True)
        
        return correlations
        
    async def get_portfolio_correlation_risk(self, positions: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate portfolio-level risk from correlated positions
        
        Args:
            positions: Dictionary of positions
            
        Returns:
            Risk assessment dictionary
        """
        symbols = [pos_data.get('symbol') for pos_data in positions.values() 
                  if pos_data.get('symbol')]
                  
        if not symbols:
            return {
                "correlation_risk": "none",
                "risk_score": 0.0,
                "correlated_groups": []
            }
            
        # Identify correlated groups within the portfolio
        correlated_groups = []
        processed_symbols = set()
        
        for symbol in symbols:
            if symbol in processed_symbols:
                continue
                
            # Get correlated symbols in portfolio
            correlated = await self.get_correlated_symbols(symbol)
            correlated_in_portfolio = [(sym, corr) for sym, corr in correlated 
                                      if sym in symbols and sym not in processed_symbols]
                                      
            if correlated_in_portfolio:
                # Add the current symbol to the group
                group = [(symbol, 1.0)] + correlated_in_portfolio
                correlated_groups.append(group)
                
                # Mark as processed
                processed_symbols.add(symbol)
                processed_symbols.update([sym for sym, _ in correlated_in_portfolio])
            else:
                processed_symbols.add(symbol)
                
        # Calculate overall risk score
        risk_score = 0.0
        
        # More correlated groups = higher risk
        risk_score += len(correlated_groups) * 0.1
        
        # Larger groups = higher risk
        for group in correlated_groups:
            risk_score += (len(group) - 1) * 0.15
            
            # Higher correlations = higher risk
            avg_correlation = sum(abs(corr) for _, corr in group) / len(group)
            risk_score += avg_correlation * 0.2
            
        # Cap the risk score at 1.0
        risk_score = min(1.0, risk_score)
        
        # Determine risk level
        risk_level = "none"
        if risk_score >= 0.8:
            risk_level = "critical"
        elif risk_score >= 0.6:
            risk_level = "high"
        elif risk_score >= 0.4:
            risk_level = "medium"
        elif risk_score >= 0.2:
            risk_level = "low"
            
        return {
            "correlation_risk": risk_level,
            "risk_score": risk_score,
            "correlated_groups": [
                {
                    "symbols": [sym for sym, _ in group],
                    "correlations": {sym: corr for sym, corr in group},
                    "count": len(group)
                }
                for group in correlated_groups
            ]
        }
        
    def get_position_weight_adjustment(self, symbol: str, direction: str, 
                                     existing_positions: Dict[str, Dict[str, Any]]) -> float:
        """
        Calculate weight adjustment factor for a new position based on correlation
        with existing positions
        
        Args:
            symbol: Symbol of the new position
            direction: Direction of the new position (buy/sell)
            existing_positions: Dictionary of existing positions
            
        Returns:
            Adjustment factor (0.0 to 1.0, where lower means reduce position size)
        """
        if not existing_positions or symbol not in self.correlation_matrix:
            return 1.0  # No adjustment needed
            
        # Default adjustment factor
        adjustment = 1.0
        
        # Get correlations for this symbol
        correlated_positions = []
        
        for pos_id, pos_data in existing_positions.items():
            exist_symbol = pos_data.get('symbol')
            exist_direction = pos_data.get('direction', '').lower()
            
            if exist_symbol and exist_symbol != symbol and exist_symbol in self.correlation_matrix[symbol]:
                correlation = self.correlation_matrix[symbol][exist_symbol]
                
                # Check direction for positive/negative correlation impact
                direction_multiplier = 1.0
                if (direction == 'buy' and exist_direction == 'buy') or (direction == 'sell' and exist_direction == 'sell'):
                    # Same direction - positive correlation increases risk, negative reduces it
                    direction_multiplier = 1.0
                else:
                    # Opposite direction - positive correlation reduces risk (hedging), negative increases it
                    direction_multiplier = -1.0
                    
                # Apply correlation impact
                adjusted_correlation = correlation * direction_multiplier
                
                # Only consider significant correlations
                if abs(adjusted_correlation) >= self.correlation_threshold * 0.8:
                    correlated_positions.append((exist_symbol, adjusted_correlation))
                    
        if not correlated_positions:
            return adjustment
            
        # Calculate adjustment based on correlation strength and number of correlated positions
        correlation_adjustment = 0.0
        
        for _, correlation in correlated_positions:
            # Negative adjusted correlation (after direction multiplier) means higher risk
            if correlation < 0:
                correlation_adjustment += abs(correlation) * 0.1
            # Positive adjusted correlation means lower risk (diversification benefit)
            else:
                correlation_adjustment -= correlation * 0.05
                
        # Cap the adjustment
        correlation_adjustment = max(-0.5, min(0.5, correlation_adjustment))
        
        # Apply the adjustment
        adjustment = max(0.5, min(1.0, 1.0 - correlation_adjustment))
        
        return adjustment

@app.get("/api/correlation/matrix")
async def get_correlation_matrix():
    """Get the correlation matrix for all tracked symbols"""
    try:
        if not correlation_analyzer:
            return JSONResponse(
                status_code=500,
                content={"error": "Correlation analyzer not initialized"}
            )
            
        # Get current correlation matrix
        matrix = correlation_analyzer.correlation_matrix
        
        # Format for API response
        formatted_matrix = {}
        
        for symbol1, correlations in matrix.items():
            formatted_matrix[symbol1] = {}
            for symbol2, correlation in correlations.items():
                # Round to 2 decimal places for readability
                formatted_matrix[symbol1][symbol2] = round(correlation, 2)
                
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "correlation_matrix": formatted_matrix,
                "symbols_count": len(matrix),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        )
    except Exception as e:
        logger.error(f"Error getting correlation matrix: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Error getting correlation matrix: {str(e)}"}
        )

@app.get("/api/correlation/symbol/{symbol}")
async def get_symbol_correlations(symbol: str, threshold: float = None):
    """
    Get correlations for a specific symbol
    
    Args:
        symbol: The trading symbol to get correlations for
        threshold: Optional threshold override (default is from settings)
    """
    try:
        if not correlation_analyzer:
            return JSONResponse(
                status_code=500,
                content={"error": "Correlation analyzer not initialized"}
            )
            
        # Get correlated symbols
        correlated_symbols = await correlation_analyzer.get_correlated_symbols(
            symbol, threshold=threshold
        )
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "symbol": symbol,
                "correlations": [
                    {"symbol": sym, "correlation": round(corr, 2)}
                    for sym, corr in correlated_symbols
                ],
                "total_count": len(correlated_symbols),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        )
    except Exception as e:
        logger.error(f"Error getting correlations for {symbol}: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Error getting correlations for {symbol}: {str(e)}"}
        )

@app.get("/api/correlation/portfolio")
async def get_portfolio_correlation_risk():
    """Get portfolio-level correlation risk assessment"""
    try:
        if not correlation_analyzer or not position_tracker:
            return JSONResponse(
                status_code=500,
                content={"error": "Required components not initialized"}
            )
            
        # Get current positions
        positions = await position_tracker.get_all_positions()
        
        # Flatten positions dictionary
        flat_positions = {}
        for symbol_positions in positions.values():
            flat_positions.update(symbol_positions)
            
        # Get correlation risk assessment
        risk_assessment = await correlation_analyzer.get_portfolio_correlation_risk(flat_positions)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "correlation_risk_assessment": risk_assessment,
                "position_count": len(flat_positions),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        )
    except Exception as e:
        logger.error(f"Error getting portfolio correlation risk: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Error getting portfolio correlation risk: {str(e)}"}
        )

# Add new API endpoints for Dynamic Exit Manager
@app.post("/api/exits")
async def initialize_exits(request: Request):
    """Initialize exit strategies for a position"""
    try:
        data = await request.json()
        
        # Validate required fields
        required_fields = ["position_id", "symbol", "entry_price", "stop_loss", "position_direction", "position_size"]
        for field in required_fields:
            if field not in data:
                return JSONResponse(
                    status_code=400,
                    content={"error": f"Missing required field: {field}"}
                )
        
        # Optional fields
        timeframe = data.get("timeframe", "H1")
        strategy_type = data.get("strategy_type", "standard")
        custom_params = data.get("custom_params")
        
        # Initialize exits
        result = await dynamic_exit_manager.initialize_exits(
            position_id=data["position_id"],
            symbol=data["symbol"],
            entry_price=float(data["entry_price"]),
            stop_loss=float(data["stop_loss"]),
            position_direction=data["position_direction"],
            position_size=float(data["position_size"]),
            timeframe=timeframe,
            strategy_type=strategy_type,
            custom_params=custom_params
        )
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": f"Exit strategies initialized for position {data['position_id']}",
                "exit_strategies": result
            }
        )
    except Exception as e:
        logger.error(f"Error initializing exit strategies: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"error": f"Error initializing exit strategies: {str(e)}"}
        )

@app.get("/api/exits/{position_id}")
async def get_exit_status(position_id: str):
    """Get exit status for a position"""
    try:
        result = await dynamic_exit_manager.get_exit_status(position_id)
        
        if "error" in result:
            return JSONResponse(
                status_code=404,
                content={"error": result["error"]}
            )
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "exit_status": result
            }
        )
    except Exception as e:
        logger.error(f"Error getting exit status: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"error": f"Error getting exit status: {str(e)}"}
        )

@app.post("/api/exits/{position_id}/check")
async def check_exit_conditions(position_id: str, request: Request):
    """Check exit conditions for a position"""
    try:
        data = await request.json()
        current_price = float(data.get("current_price", 0))
        
        if current_price <= 0:
            return JSONResponse(
                status_code=400,
                content={"error": "Invalid current price"}
            )
        
        result = await dynamic_exit_manager.execute_exit_if_triggered(
            position_id=position_id,
            current_price=current_price
        )
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "exit_check_result": result
            }
        )
    except Exception as e:
        logger.error(f"Error checking exit conditions: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"error": f"Error checking exit conditions: {str(e)}"}
        )

@app.get("/api/exits/performance")
async def get_exit_performance(strategy_type: Optional[str] = None):
    """Get performance metrics for exit strategies"""
    try:
        metrics = await dynamic_exit_manager.get_performance_metrics(strategy_type)
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "performance_metrics": metrics
            }
        )
    except Exception as e:
        logger.error(f"Error getting exit performance metrics: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"error": f"Error getting exit performance metrics: {str(e)}"}
        )

@app.get("/api/exits/history")
async def get_exit_history(limit: int = 20, symbol: Optional[str] = None, strategy_type: Optional[str] = None):
    """Get exit history"""
    try:
        history = await dynamic_exit_manager.get_exit_history(
            limit=limit,
            symbol=symbol,
            strategy_type=strategy_type
        )
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "history": history,
                "count": len(history)
            }
        )
    except Exception as e:
        logger.error(f"Error getting exit history: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"error": f"Error getting exit history: {str(e)}"}
        )

# Add API endpoints for Trading Configuration
@app.get("/api/config")
async def get_trading_config(section: Optional[str] = None):
    """Get trading configuration"""
    try:
        if section:
            config_data = trading_config.get_section(section)
        else:
            config_data = trading_config.get_config_summary()
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "config": config_data
            }
        )
    except Exception as e:
        logger.error(f"Error getting trading configuration: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"error": f"Error getting trading configuration: {str(e)}"}
        )

@app.post("/api/config/{section}")
async def update_config_section(section: str, request: Request):
    """Update a configuration section"""
    try:
        data = await request.json()
        
        success = trading_config.set_section(
            section=section,
            data=data,
            user_override=True
        )
        
        if not success:
            return JSONResponse(
                status_code=400,
                content={"error": f"Failed to update {section} configuration"}
            )
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": f"Updated {section} configuration",
                "updated_config": trading_config.get_section(section)
            }
        )
    except Exception as e:
        logger.error(f"Error updating configuration section: {str(e)}")
        logger.error(traceback.format_exc())
        return JSONResponse(
            status_code=500,
            content={"error": f"Error updating configuration section: {str(e)}"}
        )

@app.get("/api/exchange/change/{exchange_id}")
async def change_exchange(exchange_id: str):
    """Change the active exchange adapter"""
    global exchange_adapter
    
    try:
        if exchange_id.lower() not in ["oanda", "binance"]:
            return JSONResponse(
                status_code=400,
                content={"error": f"Unsupported exchange: {exchange_id}"}
            )
        
        # Shutdown existing adapter if any
        if exchange_adapter:
            await exchange_adapter.shutdown()
        
        if exchange_id.lower() == "oanda":
            # Initialize Oanda adapter
            exchange_adapter = await ExchangeAdapterFactory.create_adapter(
                "oanda", 
                {
                    "account_id": settings.oanda_account,
                    "api_token": settings.oanda_token,
                    "api_url": settings.oanda_api_url,
                    "timeout": settings.total_timeout
                }
            )
        elif exchange_id.lower() == "binance":
            # Check if Binance credentials are available
            if not hasattr(settings, "binance_api_key") or not hasattr(settings, "binance_api_secret"):
                return JSONResponse(
                    status_code=400,
                    content={"error": "Binance credentials not configured. Please add binance_api_key and binance_api_secret to settings."}
                )
            
            # Initialize Binance adapter
            exchange_adapter = await ExchangeAdapterFactory.create_adapter(
                "binance", 
                {
                    "api_key": settings.binance_api_key,
                    "api_secret": settings.binance_api_secret,
                    "timeout": settings.total_timeout
                }
            )
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": f"Exchange changed to {exchange_id}"
            }
        )
    except Exception as e:
        logger.error(f"Error changing exchange: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Error changing exchange: {str(e)}"}
        )

@app.get("/api/exchange/current")
async def get_current_exchange():
    """Get the current active exchange adapter"""
    try:
        if not exchange_adapter:
            return JSONResponse(
                status_code=404,
                content={"error": "No exchange adapter initialized"}
            )
        
        exchange_name = "unknown"
        if isinstance(exchange_adapter, OandaAdapter):
            exchange_name = "oanda"
        elif isinstance(exchange_adapter, BinanceAdapter):
            exchange_name = "binance"
        
        return JSONResponse(
            status_code=200,
            content={
                "exchange": exchange_name
            }
        )
    except Exception as e:
        logger.error(f"Error getting current exchange: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Error getting current exchange: {str(e)}"}
        )

@app.get("/api/exchange/account")
async def get_exchange_account_info():
    """Get exchange account information"""
    try:
        if not exchange_adapter:
            return JSONResponse(
                status_code=500,
                content={"error": "Exchange adapter not initialized"}
            )
            
        # Call the exchange adapter to get account info
        account_info = await exchange_adapter.get_account_info()
        
        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "account_info": account_info
            }
        )
    except Exception as e:
        logger.error(f"Error getting account info: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Error getting account info: {str(e)}"}
        )

# Add API routes for backtesting
@app.post("/api/backtest/configure")
async def configure_backtest(
    start_date: str, 
    end_date: str, 
    initial_capital: float = 100000.0,
    symbols: List[str] = None,
    data_frequency: str = "1h"
):
    """Configure the backtesting parameters"""
    global backtest_engine
    
    try:
        # Validate inputs
        try:
            start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        except ValueError:
            return {"error": "Invalid date format. Use ISO format (YYYY-MM-DD)"}
        
        if not symbols:
            symbols = ["EUR_USD", "GBP_USD", "USD_JPY"]
        
        # Create new config
        config = BacktestConfig(
            start_date=start,
            end_date=end,
            initial_capital=initial_capital,
            symbols=symbols,
            data_frequency=data_frequency
        )
        
        # Initialize new backtest engine
        backtest_engine = BacktestEngine(config)
        await backtest_engine.initialize()
        
        return {"status": "success", "message": "Backtest configuration updated"}
    
    except Exception as e:
        logger.error(f"Error configuring backtest: {str(e)}")
        return {"error": f"Failed to configure backtest: {str(e)}"}

@app.post("/api/backtest/run")
async def run_backtest(strategy_name: str = "sma_crossover"):
    """Run a backtest with the selected strategy"""
    global backtest_engine
    
    try:
        if not backtest_engine:
            return {"error": "Backtest engine not initialized"}
        
        # Get strategy function based on name
        strategy_func = get_strategy_by_name(strategy_name)
        if not strategy_func:
            return {"error": f"Strategy '{strategy_name}' not found"}
        
        # Run the backtest
        await backtest_engine.run(strategy_func)
        
        # Return summary metrics
        return {
            "status": "success",
            "metrics": backtest_engine.metrics,
            "equity_final": backtest_engine.equity_curve[-1]["equity"] if backtest_engine.equity_curve else None,
            "total_trades": len(backtest_engine.exchange.trades)
        }
    
    except Exception as e:
        logger.error(f"Error running backtest: {str(e)}")
        return {"error": f"Failed to run backtest: {str(e)}"}

@app.get("/api/backtest/results")
async def get_backtest_results():
    """Get the results of the most recent backtest"""
    global backtest_engine
    
    try:
        if not backtest_engine:
            return {"error": "Backtest engine not initialized"}
        
        if not backtest_engine.metrics:
            return {"error": "No backtest results available"}
        
        # Generate equity curve image
        backtest_engine.plot_equity_curve("static/equity_curve.png")
        
        return {
            "metrics": backtest_engine.metrics,
            "equity_curve": backtest_engine.equity_curve,
            "trades": backtest_engine.exchange.trades,
            "equity_curve_image": "/static/equity_curve.png"
        }
    
    except Exception as e:
        logger.error(f"Error retrieving backtest results: {str(e)}")
        return {"error": f"Failed to get backtest results: {str(e)}"}

@app.get("/api/backtest/strategies")
async def list_strategies():
    """List available backtest strategies"""
    try:
        strategies = [
            {"id": "sma_crossover", "name": "SMA Crossover", "description": "Simple moving average crossover strategy"},
            {"id": "bollinger_bands", "name": "Bollinger Bands", "description": "Trading using Bollinger Bands mean reversion"},
            {"id": "macd", "name": "MACD", "description": "Moving Average Convergence Divergence strategy"},
            {"id": "rsi", "name": "RSI", "description": "Relative Strength Index overbought/oversold strategy"}
        ]
        
        return strategies
    
    except Exception as e:
        logger.error(f"Error listing strategies: {str(e)}")
        return {"error": f"Failed to list strategies: {str(e)}"}

# Helper function to get strategy by name
def get_strategy_by_name(name: str):
    """Get strategy function by name"""
    from backtest_engine import simple_sma_crossover_strategy
    
    strategies = {
        "sma_crossover": simple_sma_crossover_strategy,
        # Add more strategies here
    }
    
    return strategies.get(name)

# Example of a custom strategy
async def bollinger_bands_strategy(exchange: BacktestExchangeAdapter, current_time: datetime):
    """
    Bollinger Bands strategy for backtesting.
    
    Args:
        exchange: Backtesting exchange adapter
        current_time: Current simulation time
    """
    symbol = "EUR_USD"
    
    # Get historical data
    candles = await exchange.get_candles(symbol, "1h", limit=50)
    
    if len(candles) < 20:
        return  # Not enough data
    
    # Calculate Bollinger Bands (20-period SMA with 2 standard deviations)
    closes = np.array([candle["close"] for candle in candles])
    
    sma = np.mean(closes[-20:])
    std = np.std(closes[-20:])
    
    upper_band = sma + (2 * std)
    lower_band = sma - (2 * std)
    
    # Get current price
    ticker = await exchange.get_ticker(symbol)
    if "error" in ticker:
        return
    
    current_price = ticker["price"]
    
    # Get current positions
    positions = await exchange.get_positions()
    has_position = any(p["symbol"] == symbol for p in positions)
    
    # Trading logic - mean reversion strategy
    if current_price <= lower_band and not has_position:
        # Price at lower band - buy
        from exchange_adapter import Order, OrderType, OrderSide
        order = Order(
            symbol=symbol,
            order_type=OrderType.MARKET,
            side=OrderSide.BUY,
            quantity=10000
        )
        await exchange.place_order(order)
    
    elif current_price >= upper_band and has_position:
        # Price at upper band - sell
        await exchange.close_position(symbol)

# Add API routes for historical data management
@app.get("/api/backtest/data")
async def list_historical_data():
    """List all available historical data for backtesting"""
    global data_downloader
    
    try:
        if not data_downloader:
            return {"error": "Data downloader not initialized"}
        
        data_files = data_downloader.list_available_data()
        return {
            "status": "success",
            "data_files": data_files,
            "symbols": data_downloader.get_symbols_with_data()
        }
    
    except Exception as e:
        logger.error(f"Error listing historical data: {str(e)}")
        return {"error": f"Failed to list historical data: {str(e)}"}

@app.post("/api/backtest/data/download")
async def download_historical_data(request: Request):
    """Download historical data from a specified source"""
    global data_downloader
    
    try:
        if not data_downloader:
            return {"error": "Data downloader not initialized"}
        
        data = await request.json()
        
        source = data.get("source", "fxcm").lower()
        symbol = data.get("symbol", "EUR_USD")
        timeframe = data.get("timeframe", "1h")
        start_date_str = data.get("start_date")
        end_date_str = data.get("end_date")
        api_key = data.get("api_key")
        
        # Parse dates if provided
        start_date = None
        end_date = None
        
        if start_date_str:
            try:
                start_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
            except ValueError:
                return {"error": "Invalid start date format. Use ISO format (YYYY-MM-DD)"}
        
        if end_date_str:
            try:
                end_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
            except ValueError:
                return {"error": "Invalid end date format. Use ISO format (YYYY-MM-DD)"}
        
        # Download data based on source
        success = False
        
        if source == "alphavantage":
            if not api_key:
                return {"error": "Alpha Vantage API key is required"}
            
            success = await data_downloader.download_alpha_vantage_forex(
                symbol=symbol,
                api_key=api_key,
                interval=timeframe,
                output_size="full"
            )
        elif source == "fxcm":
            success = await data_downloader.download_fxcm_historical(
                symbol=symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date
            )
        else:
            return {"error": f"Unsupported data source: {source}"}
        
        if success:
            return {
                "status": "success",
                "message": f"Downloaded {symbol} {timeframe} data from {source}"
            }
        else:
            return {"error": f"Failed to download data"}
    
    except Exception as e:
        logger.error(f"Error downloading historical data: {str(e)}")
        return {"error": f"Failed to download historical data: {str(e)}"}

@app.post("/api/backtest/data/import")
async def import_csv_data(request: Request):
    """Import historical data from a provided CSV file"""
    global data_downloader
    
    try:
        if not data_downloader:
            return {"error": "Data downloader not initialized"}
        
        # This needs to be handled with file upload, but for now just accept a path to a local file
        data = await request.json()
        
        file_path = data.get("file_path")
        symbol = data.get("symbol")
        timeframe = data.get("timeframe", "1h")
        time_format = data.get("time_format", "%Y-%m-%d %H:%M:%S")
        has_header = data.get("has_header", True)
        
        if not file_path or not symbol:
            return {"error": "file_path and symbol are required parameters"}
        
        column_map = data.get("column_map")
        
        success = data_downloader.import_csv_data(
            file_path=file_path,
            symbol=symbol,
            timeframe=timeframe,
            time_format=time_format,
            has_header=has_header,
            column_map=column_map
        )
        
        if success:
            return {
                "status": "success",
                "message": f"Imported {symbol} {timeframe} data from {file_path}"
            }
        else:
            return {"error": f"Failed to import data"}
    
    except Exception as e:
        logger.error(f"Error importing CSV data: {str(e)}")
        return {"error": f"Failed to import CSV data: {str(e)}"}

@app.delete("/api/backtest/data/{filename}")
async def delete_historical_data(filename: str):
    """Delete a historical data file"""
    global data_downloader
    
    try:
        if not data_downloader:
            return {"error": "Data downloader not initialized"}
        
        # Validate filename format to prevent directory traversal
        if not filename.endswith(".csv") or "/" in filename or "\\" in filename:
            return {"error": "Invalid filename"}
        
        file_path = os.path.join(data_downloader.data_dir, filename)
        
        if not os.path.exists(file_path):
            return {"error": f"File not found: {filename}"}
        
        # Delete the file
        os.remove(file_path)
        
        return {
            "status": "success",
            "message": f"Deleted historical data file: {filename}"
        }
    
    except Exception as e:
        logger.error(f"Error deleting historical data: {str(e)}")
        return {"error": f"Failed to delete historical data: {str(e)}"}

@app.get("/api/backtest/data/symbols")
async def get_available_symbols():
    """Get a list of symbols that have historical data available"""
    global data_downloader
    
    try:
        if not data_downloader:
            return {"error": "Data downloader not initialized"}
        
        symbols = data_downloader.get_symbols_with_data()
        
        # Get available timeframes for each symbol
        symbols_with_timeframes = {}
        for symbol in symbols:
            symbols_with_timeframes[symbol] = data_downloader.get_available_timeframes(symbol)
        
        return {
            "status": "success",
            "symbols": symbols,
            "symbols_with_timeframes": symbols_with_timeframes
        }
    
    except Exception as e:
        logger.error(f"Error getting available symbols: {str(e)}")
        return {"error": f"Failed to get available symbols: {str(e)}"}

@app.post("/api/backtest/strategy-comparison")
async def run_strategy_comparison(
    request: Request,
    strategy_names: List[str] = Body(..., description="List of strategy names to compare"),
    symbols: Optional[List[str]] = Body(None, description="List of symbols to use (default: first configured symbol)"),
    timeframes: Optional[Dict[str, str]] = Body(None, description="Map of symbol to timeframe"),
    use_historical_data: bool = Body(True, description="Whether to use downloaded historical data")
):
    """
    Run a comparison of multiple strategies on the same historical data
    """
    if not backtest_engine:
        return {"error": "Backtest engine not initialized"}
        
    try:
        # Get the strategy functions from names
        strategies = []
        for name in strategy_names:
            strategy_func = get_strategy_by_name(name)
            if not strategy_func:
                return {"error": f"Strategy not found: {name}"}
            strategies.append(strategy_func)
            
        # Run the comparison
        results = await backtest_engine.run_strategy_comparison(
            strategies=strategies,
            symbols=symbols,
            timeframes=timeframes
        )
        
        return {
            "status": "success",
            "comparison_results": results
        }
        
    except Exception as e:
        logger.error(f"Error running strategy comparison: {str(e)}")
        return {"error": f"Failed to run comparison: {str(e)}"}

@app.post("/api/backtest/optimization")
async def optimize_strategy_parameters(
    request: Request,
    strategy_name: str = Body(..., description="Strategy name to optimize"),
    parameter_ranges: Dict[str, List[Any]] = Body(..., description="Parameter ranges to test (parameter: [values])"),
    optimization_metric: str = Body("sharpe_ratio", description="Metric to optimize (sharpe_ratio, total_return, etc.)"),
    symbols: Optional[List[str]] = Body(None, description="Symbols to use for optimization"),
    max_combinations: int = Body(20, description="Maximum parameter combinations to test")
):
    """
    Optimize strategy parameters using grid search
    """
    if not backtest_engine:
        return {"error": "Backtest engine not initialized"}
        
    try:
        # Get the base strategy function
        base_strategy = get_strategy_by_name(strategy_name)
        if not base_strategy:
            return {"error": f"Strategy not found: {strategy_name}"}
            
        # Generate parameter combinations
        import itertools
        param_names = list(parameter_ranges.keys())
        param_values = list(parameter_ranges.values())
        
        # Limit combinations to prevent excessive processing
        combinations = list(itertools.product(*param_values))
        if len(combinations) > max_combinations:
            logger.warning(f"Too many parameter combinations ({len(combinations)}). Limiting to {max_combinations}.")
            import random
            random.shuffle(combinations)
            combinations = combinations[:max_combinations]
            
        logger.info(f"Running optimization with {len(combinations)} parameter combinations")
        
        # Function to create parameterized strategies
        def create_parameterized_strategy(params_dict, base_func):
            param_str = "_".join([f"{k}_{v}" for k, v in params_dict.items()])
            
            async def parameterized_strategy(exchange, current_time):
                # Add parameters to the exchange object for the strategy to access
                for param_name, param_value in params_dict.items():
                    setattr(exchange, f"param_{param_name}", param_value)
                return await base_func(exchange, current_time)
                
            # Set a name for the strategy
            parameterized_strategy.__name__ = f"{base_func.__name__}_{param_str}"
            return parameterized_strategy
        
        # Create strategy variants
        strategies = []
        params_list = []
        
        for combo in combinations:
            params_dict = {param_names[i]: combo[i] for i in range(len(param_names))}
            strategy = create_parameterized_strategy(params_dict, base_strategy)
            strategies.append(strategy)
            params_list.append(params_dict)
            
        # Run the comparison
        results = await backtest_engine.run_strategy_comparison(
            strategies=strategies,
            symbols=symbols
        )
        
        # Find the best parameters based on the selected metric
        best_strategy = None
        best_value = float('-inf')
        
        # For max drawdown, lower is better
        if optimization_metric == "max_drawdown":
            best_value = float('inf')
            for strategy_name, result in results["individual_results"].items():
                metric_value = result["metrics"][optimization_metric]
                if metric_value < best_value:
                    best_value = metric_value
                    best_strategy = strategy_name
        else:
            # For other metrics, higher is better
            for strategy_name, result in results["individual_results"].items():
                metric_value = result["metrics"][optimization_metric]
                if metric_value > best_value:
                    best_value = metric_value
                    best_strategy = strategy_name
                    
        # Extract the parameters from the best strategy
        best_params = None
        if best_strategy:
            # The index of the best strategy in the strategies list
            best_index = next((i for i, s in enumerate(strategies) if s.__name__ == best_strategy), -1)
            if best_index >= 0:
                best_params = params_list[best_index]
        
        return {
            "status": "success",
            "best_parameters": best_params,
            "best_value": best_value,
            "metric": optimization_metric,
            "all_results": results
        }
        
    except Exception as e:
        logger.error(f"Error optimizing strategy: {str(e)}")
        return {"error": f"Failed to optimize: {str(e)}"}

@app.get("/api/backtest/analysis/{backtest_id}")
async def get_backtest_analysis(
    backtest_id: str,
    metrics: Optional[List[str]] = Query(None, description="Specific metrics to include")
):
    """
    Get detailed analysis of a specific backtest
    """
    if not backtest_engine:
        return {"error": "Backtest engine not initialized"}
        
    try:
        # In a real implementation, we would retrieve a stored backtest result by ID
        # For now, we'll use the most recent backtest
        
        if not hasattr(backtest_engine, "last_backtest_id") or backtest_engine.last_backtest_id != backtest_id:
            return {"error": f"Backtest not found: {backtest_id}"}
            
        # Generate comprehensive analysis report
        analysis = {
            "backtest_id": backtest_id,
            "summary": backtest_engine.get_summary_metrics(),
            "monthly_returns": backtest_engine.get_monthly_returns(),
            "drawdown_periods": backtest_engine.get_drawdown_periods(),
            "trade_statistics": backtest_engine.get_trade_statistics()
        }
        
        # If specific metrics were requested, filter the response
        if metrics:
            filtered_analysis = {"backtest_id": backtest_id}
            for metric in metrics:
                if metric in analysis:
                    filtered_analysis[metric] = analysis[metric]
            return filtered_analysis
            
        return analysis
        
    except Exception as e:
        logger.error(f"Error getting backtest analysis: {str(e)}")
        return {"error": f"Failed to get analysis: {str(e)}"}

# Import the backtest visualization module
from backtest_visualization import BacktestVisualizer
import threading

# Add a new API endpoint for the dashboard
@app.get("/dashboard", include_in_schema=True)
async def launch_backtest_dashboard():
    """
    Launch the backtest visualization dashboard.
    The dashboard will be available at http://localhost:8050
    """
    try:
        # Dashboard port from env or default
        dashboard_port = int(os.environ.get("DASHBOARD_PORT", 8050))
        
        def run_dashboard():
            visualizer = BacktestVisualizer()
            try:
                # Load most recent results or create sample if none exist
                try:
                    visualizer.load_results()
                except FileNotFoundError:
                    # If no results found, create sample data
                    from backtest_visualization import create_sample_results
                    sample_results = create_sample_results()
                    visualizer.load_results(result_data=sample_results)
                    visualizer.save_results_to_file("sample_backtest")
                
                # Create and run dashboard
                host = "0.0.0.0"  # Allow external access
                visualizer.create_dashboard(host=host, port=dashboard_port, debug=False)
            except Exception as e:
                logger.error(f"Error running dashboard: {str(e)}")
        
        # Check if dashboard is already running
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('127.0.0.1', dashboard_port))
        sock.close()
        
        if result == 0:
            # Port is open, dashboard is already running
            return {
                "status": "success", 
                "message": f"Dashboard is already running at http://localhost:{dashboard_port}"
            }
        else:
            # Start dashboard in a background thread
            dashboard_thread = threading.Thread(target=run_dashboard)
            dashboard_thread.daemon = True
            dashboard_thread.start()
            
            return {
                "status": "success",
                "message": f"Dashboard launched at http://localhost:{dashboard_port}",
                "note": "It may take a few seconds to start up."
            }
    except Exception as e:
        logger.error(f"Failed to launch dashboard: {str(e)}")
        return {"status": "error", "message": f"Failed to launch dashboard: {str(e)}"}

if __name__ == "__main__":
    start() 