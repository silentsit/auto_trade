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
import statistics
import numpy as np
from datetime import datetime, timedelta
from pytz import timezone
from fastapi import FastAPI, Request, HTTPException, status, BackgroundTasks, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any, Union, List, Tuple, Callable, TypeVar, ParamSpec
from contextlib import asynccontextmanager
from pydantic import BaseModel, validator, ValidationError, Field, root_validator, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import wraps
from redis.asyncio import Redis
import httpx
import copy
import sys
import ast
import inspect
import traceback
from enum import Enum, auto
from collections import defaultdict, deque
import hashlib
import hmac
import random
import math
import pandas as pd
from dataclasses import dataclass, field
from sklearn.cluster import KMeans
import oandapyV20
import oandapyV20.endpoints.accounts as accounts
import oandapyV20.endpoints.orders as orders
import oandapyV20.endpoints.trades as trades
import oandapyV20.endpoints.positions as positions

# Type variables for type hints
P = ParamSpec('P')
T = TypeVar('T')

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
        
        return v

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
# Custom Exception Classes & Error Handling
##############################################################################

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

def handle_async_errors(func: Callable[P, T]) -> Callable[P, T]:
    """
    Decorator for handling errors in async functions.
    Logs errors and maintains proper error propagation.
    """
    @wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return await func(*args, **kwargs)
        except TradingError as e:
            logging.error(f"Trading error in {func.__name__}: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            logging.error(f"Unexpected error in {func.__name__}: {str(e)}", exc_info=True)
            raise TradingError(f"Internal error in {func.__name__}: {str(e)}") from e
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
            logging.error(f"Trading error in {func.__name__}: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            logging.error(f"Unexpected error in {func.__name__}: {str(e)}", exc_info=True)
            raise TradingError(f"Internal error in {func.__name__}: {str(e)}") from e
    return wrapper

class ErrorRecoverySystem:
    """
    System to recover from common errors and retry operations
    with exponential backoff and state recovery
    """
    def __init__(self):
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
        
        logging.error(f"Error in {operation} (request {request_id}): {error_str}")
        
        # Get recovery strategy based on error type and operation
        recovery_strategy = self._get_recovery_strategy(operation, error_type, error_str)
        
        if not recovery_strategy:
            logging.warning(f"No recovery strategy for {error_type} in {operation}")
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
                logging.error(f"Maximum recovery attempts reached for {request_id} ({operation})")
                
                # Remove from tracking
                del self.recovery_attempts[request_id]
                return False
                
        # Log recovery attempt
        logging.info(f"Attempting recovery for {operation} (attempt {attempt_info['count']})")
        
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
            logging.error(f"Error during recovery attempt: {str(recovery_error)}")
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
                
        # Generic retry for other errors
        return "retry"
        
    async def _retry_operation(self, operation: str, context: Dict[str, Any], attempt: int) -> bool:
        """Retry the operation with exponential backoff"""
        # Implement retry logic here with specific operations
        # For now, just return True as a placeholder
        await asyncio.sleep(min(30, 2 ** attempt))  # Exponential backoff
        return True
        
    async def _reconnect_and_retry(self, operation: str, context: Dict[str, Any], attempt: int) -> bool:
        """Reconnect to APIs and retry the operation"""
        # Implement reconnection logic here
        # For now, just return True as a placeholder
        await asyncio.sleep(min(30, 2 ** attempt))  # Exponential backoff
        return True
        
    async def _reset_session_and_retry(self, operation: str, context: Dict[str, Any], attempt: int) -> bool:
        """Reset the HTTP session and retry"""
        # Implement session reset logic here
        # For now, just return True as a placeholder
        await asyncio.sleep(min(30, 2 ** attempt))  # Exponential backoff
        return True
        
    async def _sync_positions(self, context: Dict[str, Any]) -> bool:
        """Synchronize position data with broker"""
        # Implementation would depend on handler/broker APIs
        # For now, just return True as a placeholder
        return True
        
    def _record_recovery_outcome(self, operation: str, error_type: str, strategy: str, success: bool):
        """Record recovery outcome for analysis"""
        key = f"{operation}:{error_type}:{strategy}"
        
        if key not in self.recovery_history:
            self.recovery_history[key] = {
                "attempts": 0,
                "successes": 0,
                "failures": 0,
                "last_attempt": time.time()
            }
            
        history = self.recovery_history[key]
        history["attempts"] += 1
        if success:
            history["successes"] += 1
            else:
            history["failures"] += 1
        history["last_attempt"] = time.time()
        history["success_rate"] = history["successes"] / history["attempts"]
        
        # Log outcome
        logging.info(
            f"Recovery {key}: {history['successes']}/{history['attempts']} "
            f"({history['success_rate']:.1%}) successful"
        )
        
    async def get_recovery_stats(self) -> Dict[str, Any]:
        """Get recovery statistics for analysis"""
        stats = {
            "total_recoveries_attempted": sum(h["attempts"] for h in self.recovery_history.values()),
            "total_recoveries_succeeded": sum(h["successes"] for h in self.recovery_history.values()),
            "strategies": {}
        }
        
        for key, history in self.recovery_history.items():
            operation, error_type, strategy = key.split(":")
            if strategy not in stats["strategies"]:
                stats["strategies"][strategy] = {
                    "attempts": 0,
                    "successes": 0
                }
                
            stats["strategies"][strategy]["attempts"] += history["attempts"]
            stats["strategies"][strategy]["successes"] += history["successes"]
            
        # Calculate success rates
        for strategy, data in stats["strategies"].items():
            data["success_rate"] = data["successes"] / data["attempts"] if data["attempts"] > 0 else 0
            
        return stats
        
    async def schedule_stale_position_check(self):
        """Schedule a check for stale positions"""
        async def _delayed_check():
            await asyncio.sleep(60)  # Wait 1 minute before checking
            await self._check_for_stale_positions()
            
        asyncio.create_task(_delayed_check())
        
    async def _check_for_stale_positions(self):
        """Check for and correct any stale position data"""
        logging.info("Checking for stale position data")
        
        try:
            # Get alert handler (which should be initialized by now)
            context = {"handler": alert_handler}
            await self._sync_positions(context)
        except Exception as e:
            logging.error(f"Error checking for stale positions: {str(e)}")

##############################################################################
# Configuration
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
    trade_24_7: bool = True  # Set to True for crypto exchanges trading 24/7
    base_position: int = 5000  # Base position size in account currency
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

# Initialize settings
config = Settings()

# Session Configuration
HTTP_REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=config.total_timeout,
    connect=config.connect_timeout,
    sock_read=config.read_timeout
)

##############################################################################
# Logging Setup
##############################################################################

class JSONFormatter(logging.Formatter):
    """Format logs as JSON for better parsing"""
    def format(self, record):
        log_data = {
            'timestamp': datetime.now().isoformat(),
            'level': record.levelname,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName
        }
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)
        return json.dumps(log_data)

def setup_logging():
    """Set up structured logging with rotation"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Console handler with JSON formatting in production
    console_handler = logging.StreamHandler()
    if config.environment == "production":
        console_handler.setFormatter(JSONFormatter())
    else:
        console_handler.setFormatter(
            logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        )
    logger.addHandler(console_handler)
    
    # File handler with rotation
    try:
        file_handler = logging.handlers.RotatingFileHandler(
            'trading_bot.log', 
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5
        )
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    except Exception as e:
        logger.warning(f"Could not set up file logging: {str(e)}")
        
    return logger

# Initialize logger
logger = setup_logging()

##############################################################################
# HTTP Session Management
##############################################################################

_session: Optional[aiohttp.ClientSession] = None

async def get_session(force_new: bool = False) -> aiohttp.ClientSession:
    """Get or create a session with improved error handling"""
    global _session
    try:
        if _session is None or _session.closed or force_new:
            if _session and not _session.closed:
                await _session.close()
            
            _session = aiohttp.ClientSession(
                timeout=HTTP_REQUEST_TIMEOUT,
                headers={
                    "Authorization": f"Bearer {config.oanda_token}",
                    "Content-Type": "application/json",
                    "Accept-Datetime-Format": "RFC3339"
                }
            )
        return _session
    except Exception as e:
        logger.error(f"Session creation error: {str(e)}")
        raise

async def cleanup_stale_sessions():
    """Cleanup stale sessions"""
    try:
        if _session and not _session.closed:
            await _session.close()
    except Exception as e:
        logger.error(f"Error cleaning up sessions: {str(e)}")

##############################################################################
# Utility Functions
##############################################################################

def standardize_symbol(symbol: str) -> str:
    """Standardize symbol format with better error handling"""
    if not symbol:
        return symbol
    
    try:
        # Convert to uppercase 
        symbol_upper = symbol.upper().replace('-', '_').replace('/', '_')
        
        # Direct crypto mapping
        if symbol_upper in ["BTCUSD", "BTCUSD:OANDA", "BTC/USD"]:
            return "BTC_USD"
        elif symbol_upper in ["ETHUSD", "ETHUSD:OANDA", "ETH/USD"]:
            return "ETH_USD"
        elif symbol_upper in ["XRPUSD", "XRPUSD:OANDA", "XRP/USD"]:
            return "XRP_USD"
        elif symbol_upper in ["LTCUSD", "LTCUSD:OANDA", "LTC/USD"]:
            return "LTC_USD"
        
        # If already contains underscore, return as is
        if "_" in symbol_upper:
            return symbol_upper
        
        # For 6-character symbols (like EURUSD), split into base/quote
        if len(symbol_upper) == 6:
            return f"{symbol_upper[:3]}_{symbol_upper[3:]}"
                
        # For crypto detection 
        for crypto in ["BTC", "ETH", "LTC", "XRP"]:
            if crypto in symbol_upper:
                # Check if followed by USD or USDT
                if "USD" in symbol_upper:
                    parts = symbol_upper.split("USD")
                    if parts[0].endswith(crypto):
                        return f"{crypto}_USD"
        
        # Return the original symbol in standardized format if no specific rule matches
        return symbol_upper
    except Exception as e:
        logging.error(f"Error standardizing symbol {symbol}: {str(e)}")
        return symbol.upper()  # Fallback to simple uppercase

##############################################################################
# Market Analysis Functions
##############################################################################

@handle_sync_errors
def check_market_hours(session_config: dict) -> bool:
    """
    Check if current time is within market hours based on configuration
    """
    # 24/7 markets like crypto are always open
    if config.trade_24_7:
        return True
        
    now = datetime.now()
    current_day = now.strftime("%A").upper()
    
    # Check if trading is allowed on the current day
    day_setting = session_config.get(current_day, {})
    if not day_setting.get("active", False):
        logging.info(f"Trading not active on {current_day}")
        return False
        
    # Check if current time is within trading hours for the day
    sessions = day_setting.get("sessions", [])
    current_time_minutes = now.hour * 60 + now.minute
    
    for session in sessions:
        start_minutes = session.get("start_hour", 0) * 60 + session.get("start_minute", 0)
        end_minutes = session.get("end_hour", 23) * 60 + session.get("end_minute", 59)
        
        if start_minutes <= current_time_minutes <= end_minutes:
            return True
            
    logging.info(f"Current time {now.strftime('%H:%M')} is outside trading hours")
    return False

def is_instrument_tradeable(instrument: str) -> Tuple[bool, str]:
    """
    Check if an instrument can be traded based on symbol and current market conditions
    """
    if not instrument:
        return False, "Empty instrument name"
        
    instrument = standardize_symbol(instrument)
    
    # Determine instrument type
    instrument_type = get_instrument_type(instrument)
    
    # Check if this is a forex pair that should only be traded during specific hours
    if instrument_type == "FOREX" and not config.trade_24_7:
        # Define standard forex trading sessions (UTC)
        forex_sessions = {
            "MONDAY": {"active": True, "sessions": [
                {"start_hour": 0, "start_minute": 0, "end_hour": 23, "end_minute": 59}
            ]},
            "TUESDAY": {"active": True, "sessions": [
                {"start_hour": 0, "start_minute": 0, "end_hour": 23, "end_minute": 59}
            ]},
            "WEDNESDAY": {"active": True, "sessions": [
                {"start_hour": 0, "start_minute": 0, "end_hour": 23, "end_minute": 59}
            ]},
            "THURSDAY": {"active": True, "sessions": [
                {"start_hour": 0, "start_minute": 0, "end_hour": 23, "end_minute": 59}
            ]},
            "FRIDAY": {"active": True, "sessions": [
                {"start_hour": 0, "start_minute": 0, "end_hour": 21, "end_minute": 0}
            ]},
            "SATURDAY": {"active": False, "sessions": []},
            "SUNDAY": {"active": True, "sessions": [
                {"start_hour": 21, "start_minute": 0, "end_hour": 23, "end_minute": 59}
            ]}
        }
        
        if not check_market_hours(forex_sessions):
            return False, "Outside forex trading hours"
            
    return True, "Instrument is tradeable"

async def get_atr(instrument: str, timeframe: str) -> float:
    """
    Calculate Average True Range (ATR) for an instrument
    """
    try:
        # Placeholder for actual API call to get price data
        # In a real implementation, you would:
        # 1. Fetch historical candle data for the instrument and timeframe
        # 2. Calculate ATR based on that data
        
        # For simplicity, return a simulated value based on instrument type
        instrument_type = get_instrument_type(instrument)
        
        # Baseline ATR values (to be replaced with actual calculations)
        base_atr = {
            "FOREX": 0.001,  # 10 pips for major pairs
            "CRYPTO": 200.0,  # For BTC or ETH
            "COMMODITY": 0.5,  # For gold, oil, etc.
            "INDEX": 25.0,    # For major indices
            "STOCK": 1.0      # For average stocks
        }
        
        # Adjust based on timeframe
        timeframe_multiplier = {
            "1": 0.2,     # 1 minute
            "5": 0.4,     # 5 minutes
            "15": 0.7,    # 15 minutes
            "30": 1.0,    # 30 minutes
            "60": 1.5,    # 1 hour
            "240": 2.5,   # 4 hours
            "1440": 4.0   # 1 day
        }
        
        # Get base ATR for instrument type
        atr_value = base_atr.get(instrument_type, 0.001)
        
        # Apply timeframe multiplier
        tf_mult = timeframe_multiplier.get(timeframe, 1.0)
        
        # Return simulated ATR
        return atr_value * tf_mult
        
    except Exception as e:
        logging.error(f"Error calculating ATR for {instrument}: {str(e)}")
        # Return a default value as fallback
        return 0.001

def get_instrument_type(symbol: str) -> str:
    """Determine instrument type based on symbol"""
    symbol = symbol.upper()
    
    # Check for cryptocurrencies
    if any(crypto in symbol for crypto in ["BTC", "ETH", "XRP", "LTC", "USDT", "USDC"]):
        return "CRYPTO"
        
    # Check for forex pairs
    forex_majors = ["EUR", "USD", "JPY", "GBP", "AUD", "CAD", "CHF", "NZD"]
    if len([major for major in forex_majors if major in symbol]) >= 2:
        return "FOREX"
        
    # Check for indices
    indices = ["SPX", "NDX", "DJI", "FTSE", "DAX", "NKY", "HSI"]
    if any(index in symbol for index in indices):
        return "INDEX"
        
    # Check for commodities
    commodities = ["GOLD", "XAU", "SILVER", "XAG", "OIL", "BRENT", "WTI"]
    if any(comm in symbol for comm in commodities):
        return "COMMODITY"
        
    # Default to stock
    return "STOCK"

def get_atr_multiplier(instrument_type: str, timeframe: str) -> float:
    """
    Get ATR multiplier for stop loss calculation based on instrument type and timeframe
    """
    # Default multipliers by instrument type
    base_multiplier = {
        "FOREX": 1.5,
        "CRYPTO": 2.0,
        "COMMODITY": 1.0,
        "INDEX": 1.2,
        "STOCK": 1.0
    }
    
    # Timeframe adjustments
    # Lower timeframes typically need larger multipliers to avoid premature stop-outs
    timeframe_adjustments = {
        "1": 1.5,      # 1 minute
        "5": 1.3,      # 5 minutes
        "15": 1.2,     # 15 minutes
        "30": 1.1,     # 30 minutes
        "60": 1.0,     # 1 hour
        "240": 0.9,    # 4 hours
        "1440": 0.8    # 1 day
    }
    
    # Get base multiplier for instrument type
    multiplier = base_multiplier.get(instrument_type, 1.0)
    
    # Apply timeframe adjustment
    adjustment = timeframe_adjustments.get(timeframe, 1.0)
    
    return multiplier * adjustment

@handle_async_errors
async def get_current_price(instrument: str, action: str) -> float:
    """
    Get current price for an instrument based on action (BUY/SELL)
    """
    try:
        session = await get_session()
        
        # In a real implementation, you would fetch the actual price from a broker API
        # For demonstration purposes, this is a placeholder
        
        # For a real implementation with OANDA:
        # endpoint = f"{config.oanda_api_url}/instruments/{instrument}/price"
        # async with session.get(endpoint) as response:
        #     if response.status != 200:
        #         raise Exception(f"Error fetching price: HTTP {response.status}")
        #     data = await response.json()
        #     prices = data.get("prices", [{}])[0]
        #     
        #     if action.upper() == "BUY":
        #         return float(prices.get("ask", 0))
        #     else:
        #         return float(prices.get("bid", 0))
        
        # For now, return a simulated price
        # This should be replaced with actual API calls
        base_price = {
            "BTC_USD": 50000.0,
            "ETH_USD": 3000.0,
            "XRP_USD": 0.5,
            "EUR_USD": 1.1,
            "USD_JPY": 110.0,
            "GBP_USD": 1.3,
            "USD_CAD": 1.25,
            "AUD_USD": 0.75,
            "NZD_USD": 0.7,
        }.get(instrument, 100.0)
        
        # Add a small spread
        spread = base_price * 0.0005  # 0.05% spread
        
        if action.upper() in ["BUY", "LONG"]:
            return base_price + spread
        else:
            return base_price - spread
            
    except Exception as e:
        logging.error(f"Error getting price for {instrument}: {str(e)}")
        raise

def get_current_market_session(current_time: datetime) -> str:
    """
    Determine the current market session based on time
    Returns one of: ASIAN, EUROPEAN, US, OVERNIGHT
    """
    # Convert to UTC for consistent comparison
    utc_time = current_time.astimezone(timezone('UTC'))
    utc_hour = utc_time.hour
    
    # Define sessions in UTC
    if 0 <= utc_hour < 7:
        return "ASIAN"
    elif 7 <= utc_hour < 12:
        return "EUROPEAN"
    elif 12 <= utc_hour < 20:
        return "US"
    else:
        return "OVERNIGHT"

##############################################################################
# Market Analysis Classes
##############################################################################

class VolatilityMonitor:
    """Monitors and classifies market volatility"""
    def __init__(self):
        self.volatility_data = {}
        self.market_conditions = {}
        
    async def initialize_market_condition(self, symbol: str, timeframe: str):
        """Initialize market condition monitoring for a symbol"""
        if symbol not in self.volatility_data:
            atr = await get_atr(symbol, timeframe)
            self.volatility_data[symbol] = {
                "atr_history": [atr],
                "atr_current": atr,
                "timeframe": timeframe,
                "last_update": datetime.now()
            }
            self.market_conditions[symbol] = "NORMAL"
            
    async def update_volatility(self, symbol: str, current_atr: float, timeframe: str):
        """Update volatility metrics for a symbol"""
        if symbol not in self.volatility_data:
            await self.initialize_market_condition(symbol, timeframe)
            return
            
        # Get volatility data for this symbol
        vol_data = self.volatility_data[symbol]
        
        # Update ATR history (keep last 20 values)
        vol_data["atr_history"].append(current_atr)
        if len(vol_data["atr_history"]) > 20:
            vol_data["atr_history"].pop(0)
            
        # Update current ATR
        vol_data["atr_current"] = current_atr
        vol_data["last_update"] = datetime.now()
        
        # Calculate average ATR from history
        avg_atr = sum(vol_data["atr_history"]) / len(vol_data["atr_history"])
        
        # Classify market condition based on current ATR vs average
        if current_atr > avg_atr * 1.5:
            self.market_conditions[symbol] = "VOLATILE"
        elif current_atr < avg_atr * 0.5:
            self.market_conditions[symbol] = "RANGING"
        else:
            self.market_conditions[symbol] = "NORMAL"
            
    async def get_market_condition(self, symbol: str) -> Dict[str, Any]:
        """Get current market condition and volatility data for a symbol"""
        if symbol not in self.market_conditions:
            return {"condition": "UNKNOWN", "atr": 0}
            
        return {
            "condition": self.market_conditions.get(symbol, "NORMAL"),
            "atr": self.volatility_data.get(symbol, {}).get("atr_current", 0)
        }
        
    async def should_adjust_risk(self, symbol: str, timeframe: str) -> Tuple[bool, float]:
        """Determine if risk should be adjusted based on volatility"""
        condition = await self.get_market_condition(symbol)
        
        if condition["condition"] == "VOLATILE":
            return True, 0.7  # Reduce risk in volatile markets
        elif condition["condition"] == "RANGING":
            return True, 1.2  # Increase risk in ranging markets
            
        return False, 1.0

class LorentzianDistanceClassifier:
    """
    Classifies market regimes using Lorentzian distance metrics
    """
    def __init__(self, lookback_period: int = 20):
        self.lookback_period = lookback_period
        self.price_history = {}
        self.regime_history = {}
        
    async def calculate_lorentzian_distance(self, price: float, history: List[float]) -> float:
        """
        Calculate Lorentzian distance of current price from historical data
        Uses a non-Euclidean distance measure that's more robust to outliers
        """
        if not history:
            return 0
            
        distances = [math.log(1 + abs(price - h)) for h in history]
        return sum(distances) / len(distances)
        
    async def classify_market_regime(self, symbol: str, current_price: float, atr: float = None) -> Dict[str, Any]:
        """
        Classify the current market regime for a symbol
        Returns regime type and confidence score
        """
        # Initialize symbol data if needed
        if symbol not in self.price_history:
            self.price_history[symbol] = []
            self.regime_history[symbol] = []
            
        # Add current price to history
        self.price_history[symbol].append(current_price)
        
        # Limit history length
        if len(self.price_history[symbol]) > self.lookback_period * 2:
            self.price_history[symbol].pop(0)
            
        # Need at least 10 data points for meaningful classification
        if len(self.price_history[symbol]) < 10:
            regime = {
                "type": "UNKNOWN",
                "confidence": 0,
                "distance": 0
            }
            self.regime_history[symbol].append(regime)
            return regime
            
        # Get price history
        history = self.price_history[symbol]
        
        # Calculate trend metrics
        price_change = current_price - history[0]
        price_volatility = statistics.stdev(history) if len(history) > 1 else 0
        
        # Use ATR if provided, otherwise calculate from price history
        if atr is None and len(history) > 1:
            # Simplified ATR calculation
            ranges = [abs(history[i] - history[i-1]) for i in range(1, len(history))]
            atr = sum(ranges) / len(ranges)
        elif atr is None:
            atr = 0.001  # Default small value
            
        # Calculate Lorentzian distance - a non-Euclidean distance measure
        distance = await self.calculate_lorentzian_distance(current_price, history[:-1])
        
        # Classify regime based on price change and volatility
        if abs(price_change) < atr and price_volatility < atr * 2:
            regime_type = "RANGING"
            confidence = min(0.9, max(0.5, 1.0 - (price_volatility / (atr * 2))))
        elif price_change > 0 and price_volatility > atr:
            regime_type = "TRENDING_UP"
            confidence = min(0.9, max(0.5, price_change / (atr * 5)))
        elif price_change < 0 and price_volatility > atr:
            regime_type = "TRENDING_DOWN"
            confidence = min(0.9, max(0.5, abs(price_change) / (atr * 5)))
        elif distance > atr * 3:
            regime_type = "VOLATILE"
            confidence = min(0.9, max(0.5, distance / (atr * 5)))
        else:
            regime_type = "NORMAL"
            confidence = 0.6
            
        # Create regime object
        regime = {
            "type": regime_type,
            "confidence": confidence,
            "distance": distance
        }
        
        # Add to history and limit length
        self.regime_history[symbol].append(regime)
        if len(self.regime_history[symbol]) > self.lookback_period:
            self.regime_history[symbol].pop(0)
            
        return regime
        
    async def get_regime_history(self, symbol: str) -> Dict[str, List[Any]]:
        """Get regime history for a symbol"""
        if symbol not in self.regime_history:
            return {"regimes": [], "confidence": []}
            
        regimes = self.regime_history[symbol]
        return {
            "regimes": [r["type"] for r in regimes],
            "confidence": [r["confidence"] for r in regimes]
        }
        
    def get_dominant_regime(self, symbol: str) -> str:
        """
        Get the dominant market regime over the recent history
        Uses a weighted vote based on recency and confidence
        """
        if symbol not in self.regime_history:
            return "UNKNOWN"
            
        regimes = self.regime_history[symbol]
        if not regimes:
            return "UNKNOWN"
            
        # Count regime types with recency and confidence weighting
        regime_votes = {"TRENDING_UP": 0, "TRENDING_DOWN": 0, "RANGING": 0, "VOLATILE": 0, "NORMAL": 0}
        
        for i, regime in enumerate(regimes):
            # More recent regimes get higher weight
            recency_weight = (i + 1) / len(regimes)
            # Higher confidence gets higher weight
            confidence_weight = regime["confidence"]
            
            # Add weighted vote
            regime_votes[regime["type"]] += recency_weight * confidence_weight
            
        # Get regime with highest vote
        dominant_regime = max(regime_votes.items(), key=lambda x: x[1])
        return dominant_regime[0]
        
    async def should_adjust_exits(self, symbol: str, current_regime: str = None) -> Tuple[bool, Dict[str, float]]:
        """
        Determine if trade exits should be adjusted based on market regime
        Returns (should_adjust, adjustment_factors)
        """
        if current_regime is None:
            current_regime = self.get_dominant_regime(symbol)
            
        # Default exit adjustments
        adjustments = {
            "tp_factor": 1.0,
            "sl_factor": 1.0,
            "trail_factor": 1.0
        }
        
        # Adjust based on regime
        if current_regime == "TRENDING_UP" or current_regime == "TRENDING_DOWN":
            # In trends, extend take profits and use tighter trailing stops
            adjustments["tp_factor"] = 1.5  # 150% of normal TP
            adjustments["trail_factor"] = 0.8  # 80% of normal trailing stop
            return True, adjustments
        elif current_regime == "RANGING":
            # In ranging markets, tighten take profits and loosen stops
            adjustments["tp_factor"] = 0.7  # 70% of normal TP
            adjustments["sl_factor"] = 1.2  # 120% of normal SL
            return True, adjustments
        elif current_regime == "VOLATILE":
            # In volatile markets, widen everything to avoid noise
            adjustments["tp_factor"] = 1.3  # 130% of normal TP
            adjustments["sl_factor"] = 1.3  # 130% of normal SL
            adjustments["trail_factor"] = 1.2  # 120% of normal trailing stop
            return True, adjustments
            
        # No adjustment needed
        return False, adjustments
        
    async def clear_history(self, symbol: str):
        """Clear history for a symbol"""
        if symbol in self.price_history:
            del self.price_history[symbol]
        if symbol in self.regime_history:
            del self.regime_history[symbol]

class MarketStructureAnalyzer:
    """Analyzes market structure to identify key levels and patterns"""
    def __init__(self):
        self.market_data = {}
        
    async def analyze_market_structure(self, symbol: str, timeframe: str, 
                                     high: float, low: float, close: float) -> Dict[str, Any]:
        """Analyze market structure and identify key levels"""
        # Initialize if needed
        if symbol not in self.market_data:
            self.market_data[symbol] = {
                "swing_highs": [],
                "swing_lows": [],
                "supports": [],
                "resistances": [],
                "highs": [],
                "lows": [],
                "closes": [],
                "timeframe": timeframe,
                "last_update": datetime.now()
            }
            
        # Update data arrays (limit to 100 points)
        data = self.market_data[symbol]
        data["highs"].append(high)
        data["lows"].append(low)
        data["closes"].append(close)
        data["last_update"] = datetime.now()
        
        # Limit array sizes
        for key in ["highs", "lows", "closes"]:
            if len(data[key]) > 100:
                data[key].pop(0)
                
        # Update swing points
        self._update_swing_points(symbol, high, low)
        
        # Identify support and resistance levels
        self._identify_levels(symbol)
        
        # Return current levels relative to price
        return {
            "current_price": close,
            "nearest_support": self._get_nearest_support(symbol, close),
            "nearest_resistance": self._get_nearest_resistance(symbol, close),
            "support_levels": data["supports"][-5:] if data["supports"] else [],
            "resistance_levels": data["resistances"][-5:] if data["resistances"] else []
        }
        
    def _update_swing_points(self, symbol: str, high: float, low: float):
        """Update swing high and low points"""
        data = self.market_data[symbol]
        highs = data["highs"]
        lows = data["lows"]
        
        # Need at least 5 points to identify swing points
        if len(highs) < 5 or len(lows) < 5:
            return
            
        # Check for swing highs - price peak with 2 lower highs on each side
        if highs[-3] > highs[-5] and highs[-3] > highs[-4] and highs[-3] > highs[-2] and highs[-3] > highs[-1]:
            data["swing_highs"].append(highs[-3])
            
        # Check for swing lows - price trough with 2 higher lows on each side
        if lows[-3] < lows[-5] and lows[-3] < lows[-4] and lows[-3] < lows[-2] and lows[-3] < lows[-1]:
            data["swing_lows"].append(lows[-3])
            
        # Limit swing points to 20 most recent
        for key in ["swing_highs", "swing_lows"]:
            if len(data[key]) > 20:
                data[key].pop(0)
                
    def _identify_levels(self, symbol: str):
        """Identify support and resistance levels based on swing points"""
        data = self.market_data[symbol]
        
        # Use swing points to identify levels
        if data["swing_highs"]:
            # Group similar levels (within 0.5% of each other)
            resistance_levels = []
            for level in data["swing_highs"]:
                # Check if this level is close to an existing one
                found = False
                for i, existing in enumerate(resistance_levels):
                    if abs(level - existing["level"]) / existing["level"] < 0.005:
                        # Update existing level (weighted average)
                        existing["count"] += 1
                        existing["level"] = (existing["level"] * (existing["count"] - 1) + level) / existing["count"]
                        found = True
                        break
                        
                if not found:
                    resistance_levels.append({"level": level, "count": 1})
                    
            # Sort by strength (count) and convert to list of levels
            data["resistances"] = [r["level"] for r in sorted(resistance_levels, key=lambda x: x["count"], reverse=True)]
            
        if data["swing_lows"]:
            # Group similar levels (within 0.5% of each other)
            support_levels = []
            for level in data["swing_lows"]:
                # Check if this level is close to an existing one
                found = False
                for i, existing in enumerate(support_levels):
                    if abs(level - existing["level"]) / existing["level"] < 0.005:
                        # Update existing level (weighted average)
                        existing["count"] += 1
                        existing["level"] = (existing["level"] * (existing["count"] - 1) + level) / existing["count"]
                        found = True
                        break
                        
                if not found:
                    support_levels.append({"level": level, "count": 1})
                    
            # Sort by strength (count) and convert to list of levels
            data["supports"] = [s["level"] for s in sorted(support_levels, key=lambda x: x["count"], reverse=True)]
        
    def _get_nearest_support(self, symbol: str, current_price: float) -> Optional[float]:
        """Get nearest support level below current price"""
        supports = self.market_data.get(symbol, {}).get("supports", [])
        supports_below = [s for s in supports if s < current_price]
        return max(supports_below) if supports_below else None
        
    def _get_nearest_resistance(self, symbol: str, current_price: float) -> Optional[float]:
        """Get nearest resistance level above current price"""
        resistances = self.market_data.get(symbol, {}).get("resistances", [])
        resistances_above = [r for r in resistances if r > current_price]
        return min(resistances_above) if resistances_above else None

##############################################################################
# Trading Management Classes
##############################################################################

class DynamicExitManager:
    """Manages dynamic exit points for trades based on market conditions"""
    def __init__(self):
        self.exits = {}
        
    async def initialize_exits(self, symbol: str, entry_price: float, position_type: str, 
                             initial_stop: float, initial_tp: float):
        """Initialize exit points for a new position"""
        self.exits[symbol] = {
            "entry_price": entry_price,
            "position_type": position_type.upper(),
            "initial_stop": initial_stop,
            "current_stop": initial_stop,
            "initial_tp": initial_tp,
            "current_tp": initial_tp,
            "trailing_active": False,
            "trailing_distance": 0,
            "exit_updated": datetime.now(),
            "max_favorable_excursion": entry_price
        }
        
    async def update_exits(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """Update exit points based on price movement and time in trade"""
        if symbol not in self.exits:
            return {"success": False, "error": "Position not found"}
            
        position = self.exits[symbol]
        is_long = position["position_type"] == "BUY"
        entry_price = position["entry_price"]
        
        # Calculate how far price has moved in our favor (if at all)
        if is_long:
            favorable_excursion = current_price - entry_price
            # Update max favorable excursion
            if current_price > position.get("max_favorable_excursion", entry_price):
                position["max_favorable_excursion"] = current_price
        else:
            favorable_excursion = entry_price - current_price
            # Update max favorable excursion
            if current_price < position.get("max_favorable_excursion", entry_price):
                position["max_favorable_excursion"] = current_price
                
        # Only update exits if price has moved in our favor
        if favorable_excursion <= 0:
            return {
                "success": True,
                "stop_loss": position["current_stop"],
                "take_profit": position["current_tp"],
                "trailing_active": position["trailing_active"]
            }
            
        # Calculate risk-to-reward ratio
        initial_risk = abs(entry_price - position["initial_stop"])
        if initial_risk == 0:  # Avoid division by zero
            initial_risk = abs(entry_price * 0.001)  # Use 0.1% as minimum risk
            
        # Calculate how many "R" we've captured (favorable excursion in terms of initial risk)
        r_multiple = favorable_excursion / initial_risk
        
        # Trailing stop logic - start trailing once we've captured more than 1R
        if r_multiple >= 1.0 and not position["trailing_active"]:
            # Activate trailing stop
            position["trailing_active"] = True
            
            # Set trailing distance based on the R multiple
            # The more we're in profit, the wider we can set the trailing stop
            if r_multiple < 2.0:
                trailing_r = 0.5  # Trail at 0.5R if we've captured 1R-2R
            elif r_multiple < 3.0:
                trailing_r = 0.7  # Trail at 0.7R if we've captured 2R-3R
            else:
                trailing_r = 0.8  # Trail at 0.8R if we've captured more than 3R
                
            position["trailing_distance"] = initial_risk * trailing_r
            
            # Calculate new stop loss level
            if is_long:
                new_stop = current_price - position["trailing_distance"]
                # Only move stop up, never down
                if new_stop > position["current_stop"]:
                    position["current_stop"] = new_stop
            else:
                new_stop = current_price + position["trailing_distance"]
                # Only move stop down, never up
                if new_stop < position["current_stop"]:
                    position["current_stop"] = new_stop
                    
        # If trailing stop is active, update the stop loss based on current price
        elif position["trailing_active"]:
            if is_long:
                new_stop = current_price - position["trailing_distance"]
                # Only move stop up, never down
                if new_stop > position["current_stop"]:
                    position["current_stop"] = new_stop
            else:
                new_stop = current_price + position["trailing_distance"]
                # Only move stop down, never up
                if new_stop < position["current_stop"]:
                    position["current_stop"] = new_stop
                    
        # Update the take profit level based on R-multiple
        # As we capture more R, we can extend the take profit
        if r_multiple >= 2.0:
            # Calculate new take profit level
            if is_long:
                new_tp = entry_price + (initial_risk * (r_multiple + 1))
                # Only extend take profit, never reduce it
                if new_tp > position["current_tp"]:
                    position["current_tp"] = new_tp
            else:
                new_tp = entry_price - (initial_risk * (r_multiple + 1))
                # Only extend take profit, never reduce it
                if new_tp < position["current_tp"]:
                    position["current_tp"] = new_tp
                    
        # Update timestamp
        position["exit_updated"] = datetime.now()
        
        return {
            "success": True,
            "stop_loss": position["current_stop"],
            "take_profit": position["current_tp"],
            "trailing_active": position["trailing_active"],
            "r_multiple": r_multiple
        }
        
    async def check_exits(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """Check if current price has hit any exit points"""
        if symbol not in self.exits:
            return {"action": "NONE", "reason": "Position not found"}
            
        position = self.exits[symbol]
        is_long = position["position_type"] == "BUY"
        
        # Check stop loss
        if is_long and current_price <= position["current_stop"]:
            return {
                "action": "CLOSE",
                "reason": "STOP_LOSS",
                "price": current_price
            }
        elif not is_long and current_price >= position["current_stop"]:
            return {
                "action": "CLOSE",
                "reason": "STOP_LOSS",
                "price": current_price
            }
            
        # Check take profit
        if is_long and current_price >= position["current_tp"]:
            return {
                "action": "CLOSE",
                "reason": "TAKE_PROFIT",
                "price": current_price
            }
        elif not is_long and current_price <= position["current_tp"]:
            return {
                "action": "CLOSE",
                "reason": "TAKE_PROFIT",
                "price": current_price
            }
            
        # No exit triggered
        return {
            "action": "NONE",
            "reason": "MONITORING",
            "stop_loss": position["current_stop"],
            "take_profit": position["current_tp"]
        }
        
    async def clear_exits(self, symbol: str):
        """Clear exit points for a symbol"""
        if symbol in self.exits:
            del self.exits[symbol]

class AdvancedLossManager:
    """Advanced loss management with portfolio correlation and drawdown protection"""
    def __init__(self):
        self.positions = {}
        self.correlations = {}
        self.daily_pnl = 0
        self.max_daily_loss = config.max_daily_loss  # From settings
        
    async def initialize_position(self, symbol: str, entry_price: float, position_type: str, 
                                units: float, account_balance: float):
        """Initialize position tracking for loss management"""
        self.positions[symbol] = {
            "entry_price": entry_price,
            "position_type": position_type.upper(),
            "units": units,
            "max_loss": self._calculate_position_max_loss(entry_price, units, account_balance),
            "current_loss": 0,
            "percentage_loss": 0,
            "correlation_factor": 1.0,  # Default until correlations calculated
            "entry_time": datetime.now(),
            "last_update": datetime.now()
        }
        
    def _calculate_position_max_loss(self, entry_price: float, units: float, account_balance: float) -> float:
        """Calculate maximum allowable loss for this position"""
        position_value = entry_price * units
        return position_value * 0.02  # 2% of position value as max loss
        
    async def update_position_loss(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """Update current loss calculations for a position"""
        if symbol not in self.positions:
            return {"success": False, "error": "Position not found"}
            
        position = self.positions[symbol]
        is_long = position["position_type"] == "BUY"
        entry_price = position["entry_price"]
        units = position["units"]
        
        # Calculate current loss/profit
        if is_long:
            price_change = current_price - entry_price
        else:
            price_change = entry_price - current_price
            
        current_pnl = price_change * units
        percentage_pnl = (price_change / entry_price) * 100
        
        # Update position data
        position["current_loss" if current_pnl < 0 else "current_profit"] = abs(current_pnl)
        position["percentage_loss" if percentage_pnl < 0 else "percentage_profit"] = abs(percentage_pnl)
        position["last_update"] = datetime.now()
        
        # Check if we've exceeded max loss
        if current_pnl < 0 and abs(current_pnl) > position["max_loss"]:
            return {
                "success": True,
                "action": "CLOSE",
                "reason": "MAX_POSITION_LOSS_EXCEEDED",
                "loss": abs(current_pnl),
                "percentage": abs(percentage_pnl)
            }
            
        # Check time-based exit criteria
        time_in_trade = datetime.now() - position["entry_time"]
        hours_in_trade = time_in_trade.total_seconds() / 3600
        
        # If in losing trade for more than 24 hours, suggest closing
        if current_pnl < 0 and hours_in_trade > 24:
            return {
                "success": True,
                "action": "EVALUATE",
                "reason": "EXTENDED_LOSING_TRADE",
                "loss": abs(current_pnl),
                "percentage": abs(percentage_pnl),
                "hours": hours_in_trade
            }
            
        return {
            "success": True,
            "action": "MAINTAIN",
            "pnl": current_pnl,
            "percentage": percentage_pnl
        }
        
    async def update_correlation_matrix(self, symbol: str, other_positions: Dict[str, Dict[str, Any]]):
        """Update correlation factors based on position types"""
        if symbol not in self.positions:
            return
            
        position = self.positions[symbol]
        position_type = position["position_type"]
        
        # Reset correlation factor
        position["correlation_factor"] = 1.0
        
        # Check correlations with other positions
        for other_symbol, other_pos in other_positions.items():
            if other_symbol == symbol:
                continue
                
            other_type = other_pos.get("position_type")
            correlation = self._calculate_correlation(
                symbol, other_symbol, position_type, other_type
            )
            
            # Store the correlation
            if symbol not in self.correlations:
                self.correlations[symbol] = {}
            self.correlations[symbol][other_symbol] = correlation
            
            # Increase correlation factor for correlated positions
            if abs(correlation) > 0.7:
                position["correlation_factor"] += 0.2
                
        # Cap correlation factor
        position["correlation_factor"] = min(position["correlation_factor"], 2.0)
        
    def _calculate_correlation(self, symbol1: str, symbol2: str, 
                             type1: str, type2: str) -> float:
        """
        Calculate correlation between two positions
        This is a simplified version that uses hardcoded correlations for common pairs
        In a real system, you would calculate this from price data
        """
        # Some hardcoded correlations for common forex pairs
        known_correlations = {
            ("EUR_USD", "GBP_USD"): 0.85,
            ("EUR_USD", "USD_CHF"): -0.9,
            ("EUR_USD", "AUD_USD"): 0.65,
            ("GBP_USD", "USD_CHF"): -0.8,
            ("USD_JPY", "USD_CHF"): 0.7,
            ("AUD_USD", "NZD_USD"): 0.85,
            ("EUR_USD", "USD_CAD"): -0.8,
            ("USD_CAD", "AUD_USD"): -0.65
        }
        
        # Look for the pair in known correlations
        pair = (symbol1, symbol2)
        if pair in known_correlations:
            correlation = known_correlations[pair]
        elif (symbol2, symbol1) in known_correlations:
            correlation = known_correlations[(symbol2, symbol1)]
        else:
            # Default moderate correlation for unknown pairs
            correlation = 0.3
            
        # Adjust correlation based on position types
        # If both positions are the same type, they are potentially more correlated in terms of risk
        if type1 == type2:
            # Same position types amplify correlation
            return correlation
        else:
            # Opposite position types reduce correlation
            return -correlation
            
    async def get_position_correlation_factor(self, symbol: str) -> float:
        """Get the correlation factor for a symbol"""
        if symbol not in self.positions:
            return 1.0
            
        return self.positions[symbol].get("correlation_factor", 1.0)
        
    async def update_daily_pnl(self, pnl: float):
        """Update daily P&L tracking"""
        self.daily_pnl += pnl
        
    async def should_reduce_risk(self) -> Tuple[bool, float]:
        """
        Determine if risk should be reduced based on daily P&L
        Returns (should_reduce, risk_factor)
        """
        if self.daily_pnl < 0:
            # Calculate how close we are to max daily loss
            loss_percentage = abs(self.daily_pnl) / self.max_daily_loss
            
            if loss_percentage > 0.7:
                # Significant drawdown, reduce risk substantially
                return True, 0.5
            elif loss_percentage > 0.5:
                # Moderate drawdown, reduce risk somewhat
                return True, 0.7
                
        return False, 1.0
        
    async def clear_position(self, symbol: str):
        """Clear position tracking for a symbol"""
        if symbol in self.positions:
            del self.positions[symbol]
        if symbol in self.correlations:
            del self.correlations[symbol]
            
    async def get_position_risk_metrics(self, symbol: str) -> Dict[str, Any]:
        """Get risk metrics for a position"""
        if symbol not in self.positions:
            return {"error": "Position not found"}
            
        position = self.positions[symbol]
        return {
            "max_loss": position.get("max_loss", 0),
            "current_loss": position.get("current_loss", 0),
            "percentage_loss": position.get("percentage_loss", 0),
            "correlation_factor": position.get("correlation_factor", 1.0),
            "time_in_trade": (datetime.now() - position.get("entry_time", datetime.now())).total_seconds() / 3600
        }

class RiskAnalytics:
    """Advanced risk analytics and metrics calculation"""
    def __init__(self):
        self.positions = {}
        self.portfolio_metrics = {
            "positions_count": 0,
            "total_exposure": 0,
            "portfolio_heat": 0,
            "sharpe_ratio": 0,
            "sortino_ratio": 0,
            "profit_factor": 0,
            "win_rate": 0
        }
        
    async def initialize_position(self, symbol: str, entry_price: float, units: float):
        """Initialize position tracking for risk analytics"""
        self.positions[symbol] = {
            "entry_price": entry_price,
            "units": units,
            "value": entry_price * units,
            "returns": [],
            "max_drawdown": 0,
            "peak_value": entry_price * units,
            "current_value": entry_price * units,
            "entry_time": datetime.now(),
            "last_update": datetime.now()
        }
        
        # Update portfolio metrics
        await self._calculate_risk_metrics(symbol)
        
    async def update_position(self, symbol: str, current_price: float):
        """Update position metrics based on current price"""
        if symbol not in self.positions:
            return
            
        position = self.positions[symbol]
        
        # Calculate current value
        current_value = current_price * position["units"]
        
        # Calculate return since last update
        prev_value = position["current_value"]
        if prev_value > 0:
            period_return = (current_value - prev_value) / prev_value
            position["returns"].append(period_return)
            
            # Limit returns history to last 100 periods
            if len(position["returns"]) > 100:
                position["returns"].pop(0)
                
        # Update current value
        position["current_value"] = current_value
        
        # Update peak value if current value is higher
        if current_value > position["peak_value"]:
            position["peak_value"] = current_value
            
        # Calculate drawdown
        if position["peak_value"] > 0:
            drawdown = (position["peak_value"] - current_value) / position["peak_value"]
            position["current_drawdown"] = max(0, drawdown)
            
            # Update max drawdown if current drawdown is higher
            if drawdown > position.get("max_drawdown", 0):
                position["max_drawdown"] = drawdown
                
        # Update last update timestamp
        position["last_update"] = datetime.now()
        
        # Calculate risk metrics
        await self._calculate_risk_metrics(symbol)
        
    async def _calculate_risk_metrics(self, symbol: str):
        """Calculate risk metrics for a position and update portfolio metrics"""
        position = self.positions[symbol]
        
        # Calculate position-specific metrics
        if position["returns"]:
            # Sharpe ratio
            position["sharpe_ratio"] = self._calculate_sharpe_ratio(position["returns"])
            
            # Sortino ratio
            position["sortino_ratio"] = self._calculate_sortino_ratio(position["returns"])
            
            # Win rate
            position["win_rate"] = len([r for r in position["returns"] if r > 0]) / len(position["returns"])
            
            # Profit factor
            gains = sum([r for r in position["returns"] if r > 0])
            losses = sum([abs(r) for r in position["returns"] if r < 0])
            position["profit_factor"] = gains / losses if losses > 0 else float('inf')
            
        # Update portfolio metrics
        total_value = sum(p["current_value"] for p in self.positions.values())
        total_exposure = sum(p["value"] for p in self.positions.values())
        
        # Portfolio heat (exposure relative to portfolio value)
        self.portfolio_metrics["total_exposure"] = total_exposure
        self.portfolio_metrics["positions_count"] = len(self.positions)
        
        # Portfolio heat calculation
        if total_value > 0:
            self.portfolio_metrics["portfolio_heat"] = (total_exposure / total_value) * 100
            
        # Aggregate position metrics for portfolio
        all_returns = []
        sharpe_ratios = []
        sortino_ratios = []
        profit_factors = []
        win_rates = []
        
        for pos in self.positions.values():
            if pos.get("returns"):
                all_returns.extend(pos.get("returns", []))
                sharpe_ratios.append(pos.get("sharpe_ratio", 0))
                sortino_ratios.append(pos.get("sortino_ratio", 0))
                profit_factors.append(pos.get("profit_factor", 1))
                win_rates.append(pos.get("win_rate", 0))
                
        if all_returns:
            self.portfolio_metrics["sharpe_ratio"] = self._calculate_sharpe_ratio(all_returns)
            self.portfolio_metrics["sortino_ratio"] = self._calculate_sortino_ratio(all_returns)
            
        if sharpe_ratios:
            self.portfolio_metrics["avg_sharpe_ratio"] = sum(sharpe_ratios) / len(sharpe_ratios)
            
        if sortino_ratios:
            self.portfolio_metrics["avg_sortino_ratio"] = sum(sortino_ratios) / len(sortino_ratios)
            
        if profit_factors:
            self.portfolio_metrics["avg_profit_factor"] = sum(profit_factors) / len(profit_factors)
            
        if win_rates:
            self.portfolio_metrics["avg_win_rate"] = sum(win_rates) / len(win_rates)
            
    def _calculate_sharpe_ratio(self, returns: List[float]) -> float:
        """Calculate Sharpe ratio from returns"""
        if not returns or len(returns) < 2:
            return 0
            
        # Calculate mean return
        mean_return = sum(returns) / len(returns)
        
        # Calculate standard deviation of returns
        variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)
        std_dev = math.sqrt(variance) if variance > 0 else 0.0001  # Avoid division by zero
        
        # Calculate annualized Sharpe ratio (assuming daily returns and 252 trading days)
        # Multiply by sqrt(252) to annualize
        risk_free_rate = 0.01 / 252  # Assuming 1% annual risk-free rate
        sharpe = (mean_return - risk_free_rate) / std_dev * math.sqrt(252) if std_dev > 0 else 0
        
        return sharpe
        
    def _calculate_sortino_ratio(self, returns: List[float]) -> float:
        """Calculate Sortino ratio from returns (focusing on downside risk)"""
        if not returns or len(returns) < 2:
            return 0
            
        # Calculate mean return
        mean_return = sum(returns) / len(returns)
        
        # Calculate downside deviation (standard deviation of negative returns only)
        negative_returns = [r for r in returns if r < 0]
        
        if not negative_returns:
            return float('inf')  # No negative returns
            
        downside_variance = sum((r - 0) ** 2 for r in negative_returns) / len(negative_returns)
        downside_dev = math.sqrt(downside_variance) if downside_variance > 0 else 0.0001
        
        # Calculate annualized Sortino ratio (assuming daily returns and 252 trading days)
        risk_free_rate = 0.01 / 252  # Assuming 1% annual risk-free rate
        sortino = (mean_return - risk_free_rate) / downside_dev * math.sqrt(252) if downside_dev > 0 else 0
        
        return sortino
        
    async def get_position_risk_metrics(self, symbol: str) -> Dict[str, Any]:
        """Get risk metrics for a position"""
        if symbol not in self.positions:
            return {"error": "Position not found"}
            
        position = self.positions[symbol]
        return {
            "value": position.get("current_value", 0),
            "max_drawdown": position.get("max_drawdown", 0) * 100,  # Convert to percentage
            "sharpe_ratio": position.get("sharpe_ratio", 0),
            "sortino_ratio": position.get("sortino_ratio", 0),
            "win_rate": position.get("win_rate", 0) * 100,  # Convert to percentage
            "profit_factor": position.get("profit_factor", 0)
        }
        
    async def get_portfolio_risk_metrics(self) -> Dict[str, Any]:
        """Get portfolio-level risk metrics"""
        diversity_score = 0
        
        # Calculate portfolio diversity based on position correlations
        if len(self.positions) > 1:
            # More sophisticated diversity calculation would use correlation matrix
            # This is a simplified version based on instrument types
            instrument_types = []
            for symbol in self.positions:
                instrument_type = get_instrument_type(symbol)
                instrument_types.append(instrument_type)
                
            # Count unique instrument types
            unique_types = set(instrument_types)
            type_count = len(unique_types)
            
            # Diversity score: 0 (all same type) to 1 (all different types)
            diversity_score = (type_count - 1) / (len(instrument_types) - 1) if len(instrument_types) > 1 else 0
            
        return {
            "positions_count": self.portfolio_metrics["positions_count"],
            "total_exposure": self.portfolio_metrics["total_exposure"],
            "portfolio_heat": self.portfolio_metrics["portfolio_heat"],
            "sharpe_ratio": self.portfolio_metrics.get("sharpe_ratio", 0),
            "sortino_ratio": self.portfolio_metrics.get("sortino_ratio", 0),
            "avg_win_rate": self.portfolio_metrics.get("avg_win_rate", 0) * 100,  # Convert to percentage
            "avg_profit_factor": self.portfolio_metrics.get("avg_profit_factor", 0),
            "diversity_score": diversity_score * 100  # Convert to percentage
        }
        
    async def clear_position(self, symbol: str):
        """Clear position tracking for a symbol"""
        if symbol in self.positions:
            del self.positions[symbol]

class PositionSizingManager:
    """Manages position sizing based on risk parameters and market conditions"""
    def __init__(self):
        self.portfolio_heat = 0  # Current portfolio heat (risk exposure)
        
    async def calculate_position_size(self, 
                                    account_balance: float,
                                    entry_price: float,
                                    stop_loss: float,
                                    atr: float,
                                    timeframe: str,
                                    market_condition: Dict[str, Any],
                                    correlation_factor: float = 1.0) -> float:
        """
        Calculate optimal position size based on risk parameters
        Returns position size in units
        """
        # Default risk percentage from config
        risk_percentage = config.default_risk_percentage
        
        # Adjust risk based on market condition
        market_volatility = market_condition.get("condition", "NORMAL")
        if market_volatility == "VOLATILE":
            risk_percentage *= 0.7  # Reduce risk in volatile markets
        elif market_volatility == "RANGING":
            risk_percentage *= 1.2  # Slightly increase risk in ranging markets
            
        # Adjust risk based on timeframe
        # Lower timeframes have higher noise, so reduce risk
        if timeframe in ["1", "5", "15"]:
            risk_percentage *= 0.8
        elif timeframe in ["240", "1440"]:  # 4H and Daily
            risk_percentage *= 1.2
            
        # Adjust for correlation with existing positions
        risk_percentage /= correlation_factor
        
        # Cap at maximum risk percentage from config
        risk_percentage = min(risk_percentage, config.max_risk_percentage)
        
        # Calculate dollar risk amount
        risk_amount = account_balance * (risk_percentage / 100)
        
        # Calculate position size based on stop loss distance
        stop_distance = abs(entry_price - stop_loss)
        if stop_distance == 0:  # Avoid division by zero
            stop_distance = atr * 0.5  # Use half of ATR as minimum stop distance
            
        # Calculate position size in currency units
        position_size = risk_amount / stop_distance
        
        # Adjust position size if portfolio heat is already high
        max_heat = config.max_portfolio_heat
        if self.portfolio_heat > max_heat * 0.7:
            # Reduce position size as we approach max heat
            heat_factor = 1 - ((self.portfolio_heat - (max_heat * 0.7)) / (max_heat * 0.3))
            heat_factor = max(0.2, heat_factor)  # Don't reduce below 20%
            position_size *= heat_factor
            
        # Calculate units based on position size and entry price
        units = position_size / entry_price
        
        # Round units to appropriate precision based on instrument type
        instrument_type = get_instrument_type(standardize_symbol(entry_price))
        if instrument_type == "FOREX":
            units = round(units, 0)  # Round to whole units for forex
        elif instrument_type == "CRYPTO":
            units = round(units, 4)  # Round to 4 decimal places for crypto
        else:
            units = round(units, 2)  # Round to 2 decimal places for stocks/indices
            
        return units
        
    async def update_portfolio_heat(self, new_position_size: float):
        """Update portfolio heat with new position"""
        self.portfolio_heat += new_position_size * 0.01  # Simplified calculation
        
    async def get_correlation_factor(self, symbol: str, existing_positions: List[str]) -> float:
        """
        Calculate correlation factor for a new position
        Higher factor means higher correlation with existing positions
        """
        if not existing_positions:
            return 1.0  # No existing positions, no correlation
            
        # Simplified correlation calculation
        # In a real system, this would use actual price correlation calculations
        
        correlation_sum = 0
        count = 0
        
        for existing in existing_positions:
            # Calculate correlation between symbol and existing position
            # For now, use a simplified approach
            
            # Check if they're the same instrument type
            symbol_type = get_instrument_type(symbol)
            existing_type = get_instrument_type(existing)
            
            if symbol_type == existing_type:
                # Same instrument type has higher correlation
                correlation = 0.7
            else:
                # Different instrument types have lower correlation
                correlation = 0.2
                
            correlation_sum += correlation
            count += 1
            
        # Average correlation with all existing positions
        avg_correlation = correlation_sum / count if count > 0 else 0
        
        # Calculate correlation factor
        # Higher correlation means higher factor (reducing position size)
        correlation_factor = 1 + (avg_correlation * 0.5)  # Range: 1.0 - 1.5
        
        return correlation_factor

class EnhancedRiskManager:
    """
    Comprehensive risk management with dynamic adjustments
    and multi-timeframe analysis
    """
    def __init__(self):
        self.positions = {}
        self.tp_levels = {
            # Default take profit levels as multiples of risk (R)
            "1": 1.5,   # 1 minute: 1.5R
            "5": 1.5,   # 5 minutes: 1.5R
            "15": 1.8,  # 15 minutes: 1.8R
            "30": 2.0,  # 30 minutes: 2.0R
            "60": 2.2,  # 1 hour: 2.2R
            "240": 2.5, # 4 hours: 2.5R
            "1440": 3.0 # Daily: 3.0R
        }
        self.atr_multipliers = {
            # Default ATR multipliers for stop loss by instrument type and timeframe
            "FOREX": {
                "1": 1.5,   # 1 minute: 1.5x ATR
                "5": 1.3,   # 5 minutes: 1.3x ATR
                "15": 1.2,  # 15 minutes: 1.2x ATR
                "30": 1.1,  # 30 minutes: 1.1x ATR
                "60": 1.0,  # 1 hour: 1.0x ATR
                "240": 0.9, # 4 hours: 0.9x ATR
                "1440": 0.8 # Daily: 0.8x ATR
            },
            "CRYPTO": {
                "1": 2.0,   # 1 minute: 2.0x ATR
                "5": 1.8,   # 5 minutes: 1.8x ATR
                "15": 1.6,  # 15 minutes: 1.6x ATR
                "30": 1.5,  # 30 minutes: 1.5x ATR
                "60": 1.3,  # 1 hour: 1.3x ATR
                "240": 1.2, # 4 hours: 1.2x ATR
                "1440": 1.0 # Daily: 1.0x ATR
            },
            "DEFAULT": {
                "1": 1.7,   # 1 minute: 1.7x ATR
                "5": 1.5,   # 5 minutes: 1.5x ATR
                "15": 1.3,  # 15 minutes: 1.3x ATR
                "30": 1.2,  # 30 minutes: 1.2x ATR
                "60": 1.1,  # 1 hour: 1.1x ATR
                "240": 1.0, # 4 hours: 1.0x ATR
                "1440": 0.9 # Daily: 0.9x ATR
            }
        }
        
    async def initialize_position(self, symbol: str, entry_price: float, position_type: str, 
                                timeframe: str, units: float, atr: float):
        """Initialize risk management for a new position"""
        # Determine instrument type
        instrument_type = self._get_instrument_type(symbol)
        
        # Get ATR multiplier based on instrument type and timeframe
        atr_multiplier = self.atr_multipliers.get(
            instrument_type, 
            self.atr_multipliers["DEFAULT"]
        ).get(timeframe, 1.0)
        
        # Calculate initial stop loss
        sl_distance = atr * atr_multiplier
        if position_type.upper() == "BUY":
            stop_loss = entry_price - sl_distance
        else:
            stop_loss = entry_price + sl_distance
            
        # Calculate take profit levels
        tp_factor = self.tp_levels.get(timeframe, 2.0)
        tp_distance = sl_distance * tp_factor
        
        if position_type.upper() == "BUY":
            take_profit = entry_price + tp_distance
        else:
            take_profit = entry_price - tp_distance
            
        # Initialize trailing stop parameters
        trailing_activation = 1.0  # Activate at 1R profit
        trailing_distance = sl_distance * 0.6  # 60% of initial stop distance
        
        # Store position data
        self.positions[symbol] = {
            "entry_price": entry_price,
            "position_type": position_type.upper(),
            "timeframe": timeframe,
            "units": units,
            "atr": atr,
            "initial_stop_loss": stop_loss,
            "current_stop_loss": stop_loss,
            "initial_take_profit": take_profit,
            "current_take_profit": take_profit,
            "trailing_activation": trailing_activation,
            "trailing_distance": trailing_distance,
            "trailing_active": False,
            "entry_time": datetime.now(),
            "last_update": datetime.now(),
            "time_stops": {
                "check_after_hours": 6,  # Check position after 6 hours
                "max_holding_time": 48  # Maximum holding time in hours
            }
        }
        
    def _get_instrument_type(self, symbol: str) -> str:
        """Get instrument type for ATR multiplier selection"""
        symbol = symbol.upper()
        
        if any(crypto in symbol for crypto in ["BTC", "ETH", "XRP", "LTC", "USDT"]):
            return "CRYPTO"
            
        forex_majors = ["EUR", "USD", "JPY", "GBP", "AUD", "CAD", "CHF", "NZD"]
        if len([major for major in forex_majors if major in symbol]) >= 2:
            return "FOREX"
            
        return "DEFAULT"
        
    async def update_position(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """
        Update risk management parameters based on price movement
        Returns action to take (if any)
        """
        if symbol not in self.positions:
            return {"action": "NONE", "reason": "Position not found"}
            
        position = self.positions[symbol]
        is_long = position["position_type"] == "BUY"
        
        # Check if stop loss or take profit hit
        if self._check_stop_loss_hit(position, current_price):
            return {
                "action": "CLOSE",
                "reason": "STOP_LOSS_HIT",
                "price": current_price
            }
            
        tp_result = self._check_take_profits(position, current_price)
        if tp_result:
            return tp_result
            
        # Update trailing stop if needed
        trail_result = self._update_trailing_stop(position, current_price)
        if trail_result:
            return trail_result
            
        # Check time-based adjustments
        time_result = self._check_time_adjustments(position)
        if time_result:
            return time_result
            
        # No action needed
        return {
            "action": "NONE",
            "reason": "MONITORING",
            "current_stop": position["current_stop_loss"],
            "current_tp": position["current_take_profit"],
            "r_ratio": self._get_current_rr_ratio(position, current_price)
        }
        
    def _check_stop_loss_hit(self, position: Dict[str, Any], current_price: float) -> bool:
        """Check if current price has hit stop loss"""
        is_long = position["position_type"] == "BUY"
        stop_loss = position["current_stop_loss"]
        
        if is_long and current_price <= stop_loss:
            return True
        elif not is_long and current_price >= stop_loss:
            return True
            
        return False
        
    def _check_take_profits(self, position: Dict[str, Any], current_price: float) -> Optional[Dict[str, Any]]:
        """Check if take profit has been hit"""
        is_long = position["position_type"] == "BUY"
        take_profit = position["current_take_profit"]
        
        if is_long and current_price >= take_profit:
            return {
                "action": "CLOSE",
                "reason": "TAKE_PROFIT_HIT",
                "price": current_price
            }
        elif not is_long and current_price <= take_profit:
            return {
                "action": "CLOSE",
                "reason": "TAKE_PROFIT_HIT",
                "price": current_price
            }
            
        # Calculate current R-ratio (profit in terms of initial risk)
        r_ratio = self._get_current_rr_ratio(position, current_price)
        
        # If profit is more than 2R, consider partial close
        if r_ratio >= 2.0:
            return {
                "action": "PARTIAL_CLOSE",
                "reason": "PROFIT_TARGET_REACHED",
                "price": current_price,
                "percentage": 50,  # Close 50% of position
                "r_ratio": r_ratio
            }
            
        return None
        
    def _update_trailing_stop(self, position: Dict[str, Any], current_price: float) -> Optional[Dict[str, Any]]:
        """Update trailing stop if conditions are met"""
        is_long = position["position_type"] == "BUY"
        entry_price = position["entry_price"]
        initial_stop = position["initial_stop_loss"]
        trailing_activation = position["trailing_activation"]
        trailing_distance = position["trailing_distance"]
        
        # Calculate current profit/loss in terms of initial risk (R)
        initial_risk = abs(entry_price - initial_stop)
        if initial_risk == 0:  # Avoid division by zero
            initial_risk = position["atr"] * 0.5
            
        if is_long:
            current_profit = current_price - entry_price
        else:
            current_profit = entry_price - current_price
            
        r_ratio = current_profit / initial_risk
        
        # Check if trailing stop should be activated
        if r_ratio >= trailing_activation and not position["trailing_active"]:
            position["trailing_active"] = True
            
            # Calculate new trailing stop
            if is_long:
                new_stop = current_price - trailing_distance
                # Only move stop up if it's better than current stop
                if new_stop > position["current_stop_loss"]:
                    position["current_stop_loss"] = new_stop
            else:
                new_stop = current_price + trailing_distance
                # Only move stop down if it's better than current stop
                if new_stop < position["current_stop_loss"]:
                    position["current_stop_loss"] = new_stop
                    
            return {
                "action": "UPDATE_STOP",
                "reason": "TRAILING_STOP_ACTIVATED",
                "new_stop": position["current_stop_loss"]
            }
            
        # If trailing stop is already active, update it as price moves
        elif position["trailing_active"]:
            if is_long:
                new_stop = current_price - trailing_distance
                # Only move stop up, never down
                if new_stop > position["current_stop_loss"]:
                    position["current_stop_loss"] = new_stop
                    return {
                        "action": "UPDATE_STOP",
                        "reason": "TRAILING_STOP_UPDATED",
                        "new_stop": position["current_stop_loss"]
                    }
            else:
                new_stop = current_price + trailing_distance
                # Only move stop down, never up
                if new_stop < position["current_stop_loss"]:
                    position["current_stop_loss"] = new_stop
                    return {
                        "action": "UPDATE_STOP",
                        "reason": "TRAILING_STOP_UPDATED",
                        "new_stop": position["current_stop_loss"]
                    }
                    
        return None
        
    def _get_current_rr_ratio(self, position: Dict[str, Any], current_price: float) -> float:
        """Calculate current risk-reward ratio"""
        is_long = position["position_type"] == "BUY"
        entry_price = position["entry_price"]
        initial_stop = position["initial_stop_loss"]
        
        # Calculate initial risk
        initial_risk = abs(entry_price - initial_stop)
        if initial_risk == 0:  # Avoid division by zero
            initial_risk = position["atr"] * 0.5
            
        # Calculate current profit/loss
        if is_long:
            current_profit = current_price - entry_price
        else:
            current_profit = entry_price - current_price
            
        # Return R-multiple
        return current_profit / initial_risk
        
    def _check_time_adjustments(self, position: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Check if time-based adjustments are needed"""
        # Calculate time in trade
        time_in_trade = datetime.now() - position["entry_time"]
        hours_in_trade = time_in_trade.total_seconds() / 3600
        
        # Check if trade has been open for too long
        if hours_in_trade >= position["time_stops"]["max_holding_time"]:
            return {
                "action": "CLOSE",
                "reason": "MAX_HOLDING_TIME_REACHED",
                "hours": hours_in_trade
            }
            
        # Check if trade should be evaluated after specified hours
        if hours_in_trade >= position["time_stops"]["check_after_hours"]:
            # This is just a notification, not an actual close action
            return {
                "action": "EVALUATE",
                "reason": "TIME_CHECK_DUE",
                "hours": hours_in_trade
            }
            
        return None
        
    async def clear_position(self, symbol: str):
        """Clear position tracking for a symbol"""
        if symbol in self.positions:
            del self.positions[symbol]

##############################################################################
# FastAPI App Setup
##############################################################################

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Setup and teardown for the FastAPI application"""
    # Setup code: Initialize resources on startup
    logger.info("Starting application server")
    
    try:
        # Initialize any clients or resources needed
        # For example, Redis if available
        if config.redis_url:
            app.state.redis = Redis.from_url(config.redis_url)
            logger.info("Redis connection established")
        
        # Initialize any other startup resources
        # ...
        
    except Exception as e:
        logger.error(f"Error during application startup: {str(e)}")
    
    # Yield control to the application
    yield
    
    # Teardown code after the application is shutting down
    logger.info("Shutting down application server")
    
    # Call cleanup tasks
    await cleanup_resources()

    logger.info("Application shutdown complete")
    
async def cleanup_resources():
    """Clean up resources during shutdown"""
    # List to track cleanup tasks
    tasks = []
    
    # Close HTTP session
    if _session and not _session.closed:
        tasks.append(asyncio.create_task(_session.close()))
    
    # Close Redis connection
    try:
        if hasattr(app.state, 'redis'):
            await app.state.redis.close()
    except Exception as e:
        logger.error(f"Error closing Redis connection: {str(e)}")
    
    # Wait for all cleanup tasks
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

##############################################################################
# API Endpoints
##############################################################################

@app.get("/api/health")
async def health_check():
    """Health check endpoint for monitoring"""
    return {"status": "ok", "timestamp": datetime.now(timezone('Asia/Bangkok')).isoformat()}

@app.get("/api/positions")
async def get_positions_endpoint(position_manager: PositionManager = Depends(get_position_manager)):
    """Get all open positions"""
    try:
        positions = await position_manager.get_positions()
        return {"status": "success", "positions": positions}
    except Exception as e:
        logger.error(f"Error fetching positions: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching positions: {str(e)}"
        )

@app.post("/api/alerts")
async def process_alert_endpoint(
    alert_data: AlertData,
    alert_handler: AlertHandler = Depends(get_alert_handler)
):
    """Process an alert from TradingView or other sources"""
    try:
        result = await alert_handler.process_alert(alert_data)
        return result
    except ValidationError as e:
        logger.error(f"Validation error processing alert: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid alert data: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error processing alert: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing alert: {str(e)}"
        )

@app.post("/tradingview")
async def tradingview_webhook(
    request: Request,
    alert_handler: AlertHandler = Depends(get_alert_handler)
):
    """Webhook for TradingView alerts"""
    try:
        # Parse incoming JSON
        payload = await request.json()
        logger.info(f"Received TradingView alert: {payload}")
        
        # Extract alert data
        alert_data = AlertData(
            symbol=payload.get("symbol"),
            timeframe=payload.get("timeframe", "1H"),
            action=payload.get("action"),
            orderType=payload.get("orderType", "MARKET"),
            timeInForce=payload.get("timeInForce", "FOK"),
            percentage=float(payload.get("percentage", 15.0)),
            account=payload.get("account"),
            comment=payload.get("comment")
        )
        
        # Process the alert
        result = await alert_handler.process_alert(alert_data)
        return result
    except ValidationError as e:
        logger.error(f"Validation error from TradingView: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"status": "error", "detail": f"Invalid alert data: {str(e)}"}
        )
    except Exception as e:
        logger.error(f"Error processing TradingView alert: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "detail": f"Error processing alert: {str(e)}"}
        )

@app.get("/api/account")
async def get_account_summary_endpoint():
    """Get account summary from OANDA"""
    try:
        success, data = await get_account_summary()
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=data.get("error", "Unknown error getting account summary")
            )
        return data
    except Exception as e:
        logger.error(f"Error fetching account summary: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching account summary: {str(e)}"
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
        app,  # Use the app instance directly
        host=host,
        port=port,
        reload=config.environment == "development"
    )

if __name__ == "__main__":
    start() 