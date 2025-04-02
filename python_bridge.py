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
from position_manager import position_manager  # Import the new consolidated position manager

# Type variables for type hints
P = ParamSpec('P')
T = TypeVar('T')

# Prometheus metrics
TRADE_REQUESTS = Counter('trade_requests', 'Total trade requests')
TRADE_LATENCY = Histogram('trade_latency', 'Trade processing latency')

# Redis for shared state
redis = Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379'))

# Global thread safety lock
_lock = asyncio.Lock()

##############################################################################
# Error Handling Infrastructure
##############################################################################

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
        
        logger.error(f"Error in {operation} (request {request_id}): {error_str}")
            
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
            if not handler:
                logger.error("Cannot sync positions: missing handler")
                return False
                
            # Get current positions from broker
            success, positions_data = await get_open_positions()
            if not success:
                logger.error("Failed to get positions from broker")
                return False
                
            # Get tracked positions
            tracked_positions = await position_manager.get_all_positions()
            
            # Extract broker positions
            broker_positions = {
                p["instrument"]: p for p in positions_data.get("positions", [])
            }
            
            # Sync any missing positions
            for symbol in tracked_positions:
                if symbol not in broker_positions:
                    logger.warning(f"Position {symbol} exists in tracker but not with broker - removing")
                    await position_manager.clear_position(symbol)
                    
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
                "operation_stats": self.recovery_history
            }
            
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
            logger.error(f"Trading error in {func.__name__}: {str(e)}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {str(e)}", exc_info=True)
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
    sock_connect=config.connect_timeout,
    sock_read=config.read_timeout
)

# Configure logger
logger = logging.getLogger("python_bridge")
if not logger.handlers:
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Global lock for thread safety
_lock = asyncio.Lock()

# AlertHandler will be initialized in startup
alert_handler = None

##############################################################################
# Main Alert Processing Logic
##############################################################################

async def process_alert(alert_data: Dict[str, Any]) -> bool:
    """Process trading alerts with comprehensive risk management"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Processing alert: {json.dumps(alert_data, indent=2)}")

    try:
        if not alert_data:
            logger.error(f"[{request_id}] Empty alert data received")
            return False
    
        async with _lock:
            action = alert_data['action'].upper()
            symbol = alert_data['symbol']
            instrument = standardize_symbol(symbol)
            
            # Ensure timeframe is properly formatted
            if 'timeframe' not in alert_data:
                logger.warning(f"[{request_id}] No timeframe provided in alert data, using default")
                alert_data['timeframe'] = "15M"  # Default timeframe
            else:
                original_tf = alert_data['timeframe']
                alert_data['timeframe'] = ensure_proper_timeframe(alert_data['timeframe'])
                logger.info(f"[{request_id}] Normalized timeframe from {original_tf} to {alert_data['timeframe']}")
                
            timeframe = alert_data['timeframe']
            logger.info(f"[{request_id}] Standardized instrument: {instrument}, Action: {action}, Timeframe: {timeframe}")
            
            # Position closure logic
            if action in ['CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']:
                logger.info(f"[{request_id}] Processing close request")
                success, result = await close_position(alert_data)
                logger.info(f"[{request_id}] Close position result: success={success}, result={json.dumps(result)}")
                if success:
                    await position_manager.clear_position(symbol)
                return success
            
            # Market condition check with detailed logging
            tradeable, reason = is_instrument_tradeable(instrument)
            logger.info(f"[{request_id}] Instrument {instrument} tradeable check: {tradeable}, Reason: {reason}")
            
            if not tradeable:
                logger.warning(f"[{request_id}] Market check failed: {reason}")
                return False
            
            # Get market data
            current_price = await get_current_price(instrument, action)
            logger.info(f"[{request_id}] Got current price: {current_price}")
            
            atr = await get_atr(instrument, timeframe)
            logger.info(f"[{request_id}] Got ATR value: {atr}")
            
            # Analyze market structure
            market_structure = await market_structure_analyzer.analyze_market_structure(
                symbol, timeframe, current_price, current_price, current_price
            )
            logger.info(f"[{request_id}] Market structure analysis complete")
            
            # Update volatility monitoring
            await volatility_monitor.update_volatility(symbol, atr, timeframe)
            market_condition = await volatility_monitor.get_market_condition(symbol)
            logger.info(f"[{request_id}] Market condition: {market_condition}")
            
            # Get existing positions for correlation
            existing_positions = await position_manager.get_all_positions()
            correlation_factor = await position_manager.get_correlation_factor(
                symbol, list(existing_positions.keys())
            )
            
            # Use nearest support/resistance for stop loss if available
            stop_price = None
            if action == 'BUY' and market_structure['nearest_support']:
                stop_price = market_structure['nearest_support']
            elif action == 'SELL' and market_structure['nearest_resistance']:
                stop_price = market_structure['nearest_resistance']
            
            # Otherwise use ATR-based stop
            if not stop_price:
                # This will be handled internally by TrackedPosition
                if action == 'BUY':
                    stop_price = current_price - (atr * 1.5)  # Using a default multiplier
                else:
                    stop_price = current_price + (atr * 1.5)  # Using a default multiplier
            
            # Calculate position size
            account_balance = await get_account_balance(alert_data.get('account', config.oanda_account))
            position_size = await position_manager.calculate_position_size(
                account_balance,
                current_price,
                stop_price,
                atr,
                timeframe,
                market_condition,
                correlation_factor
            )
            
            # Log the original calculated size
            logger.info(f"[{request_id}] Calculated position size: {position_size}")
            
            # Ensure position size is within valid range (1-100)
            position_size = max(1.0, min(100.0, position_size))
            logger.info(f"[{request_id}] Final adjusted position size: {position_size}")
            
            # Update alert data with calculated position size
            alert_data['percentage'] = position_size
            
            # Execute trade
            success, result = await execute_trade(alert_data)
            if success:
                # Extract entry price and units from result
                entry_price = float(result.get('orderFillTransaction', {}).get('price', current_price))
                units = float(result.get('orderFillTransaction', {}).get('units', position_size))
                
                # Initialize position tracking in the consolidated position manager
                await position_manager.initialize_position(
                    symbol,
                    action,
                    entry_price,
                    units,
                    timeframe,
                    atr,
                    account_balance
                )
                
                logger.info(f"[{request_id}] Trade executed successfully with comprehensive risk management")
            else:
                logger.warning(f"[{request_id}] Trade execution failed: {result}")
                
            return success
                
    except Exception as e:
        logger.error(f"[{request_id}] Critical error: {str(e)}", exc_info=True)
        # Record error for recovery
        if error_recovery:
            error_context = {"func": process_alert, "args": [alert_data], "handler": None}
            await error_recovery.handle_error(request_id, "process_alert", e, error_context)
        return False

##############################################################################
# Market Condition Helpers
##############################################################################

def is_instrument_tradeable(instrument: str) -> Tuple[bool, str]:
    """Check if an instrument is tradeable based on market hours and conditions"""
    try:
        # Get current time in UTC
        current_time = datetime.utcnow()
        current_day = current_time.weekday()  # Monday is 0, Sunday is 6
        current_hour = current_time.hour
        
        # Check if it's weekend (forex markets closed)
        if current_day >= 5 and not config.trade_24_7:  # Saturday or Sunday
            return False, "Weekend - forex markets are closed"
            
        # Handle forex specific rules
        if "JPY" in instrument or any(x in instrument for x in ["USD", "EUR", "GBP", "AUD", "NZD", "CAD", "CHF"]):
            # If we're trading 24/7, skip the forex market hours check
            if config.trade_24_7:
                return True, "Trading enabled 24/7 for all instruments"
                
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
            # If we're trading 24/7, skip the stock market hours check
            if config.trade_24_7:
                return True, "Trading enabled 24/7 for all instruments"
                
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

# Now we can import trading_api after config is defined
from trading_api import (  # Import the API functions from the trading_api module
    get_session, 
    cleanup_stale_sessions, 
    standardize_symbol, 
    ensure_proper_timeframe,
    close_position, 
    execute_trade, 
    get_open_positions, 
    get_account_balance, 
    get_atr,
    get_instrument_type
)
from market_analytics import market_structure_analyzer, volatility_monitor, resolve_imports

# Update trading_api's config with our config
import trading_api
trading_api.config = config
trading_api.HTTP_REQUEST_TIMEOUT = HTTP_REQUEST_TIMEOUT

# Make sure market_analytics has its imports resolved
resolve_imports()

# Initialize the error recovery system
error_recovery = ErrorRecoverySystem()
