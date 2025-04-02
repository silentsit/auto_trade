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
from typing import Dict, Any, Tuple, Optional, List, Callable, TypeVar, ParamSpec, Union
from datetime import datetime, timedelta
from pytz import timezone
from fastapi import FastAPI, Request, HTTPException, status, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
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
logger = logging.getLogger("python_bridge")
if not logger.handlers:
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def log_error(operation: str, request_id: str, error_str: str, status_code: Optional[int] = None):
    """Log API errors properly with request information"""
    if status_code:
        logger.error(f"Error in {operation} (request {request_id}): {error_str} (Status: {status_code})")
    else:
        logger.error(f"Error in {operation} (request {request_id}): {error_str}")

def handle_api_response(response_data: Dict[str, Any], operation: str, request_id: str) -> Dict[str, Any]:
    """Handle API response with proper error logging"""
    if response_data.get("success") is False:
        error_str = response_data.get("error", "Unknown error")
        status_code = response_data.get("status")
        log_error(operation, request_id, error_str, status_code)
    
    return response_data

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
HTTP_REQUEST_TIMEOUT = 10  # This is later overwritten by aiohttp.ClientTimeout

##############################################################################
# Global Utility Functions
##############################################################################

def ensure_proper_timeframe(timeframe: str) -> str:
    """Ensures timeframe is in the proper format (e.g., converts '15' to '15M')"""
    if timeframe.upper() in ['D', 'DAY', 'DAILY', '1D']:
        return 'D'
    if timeframe.upper() in ['W', 'WEEK', 'WEEKLY', '1W']:
        return 'W'
    if timeframe.upper() in ['M', 'MONTH', 'MONTHLY', '1M']:
        return 'M'
    timeframe = re.sub(r'[^a-zA-Z0-9]', '', timeframe)
    if timeframe.isdigit():
        return f"{timeframe}M"
    return timeframe

def standardize_symbol(symbol: str) -> str:
    """Standardize trading symbols to a consistent format"""
    symbol = symbol.strip().upper()
    if '_' not in symbol and '/' in symbol:
        symbol = symbol.replace('/', '_')
    if symbol == 'XAUUSD':
        return 'XAU_USD'
    elif symbol == 'XAGUSD':
        return 'XAG_USD'
    elif symbol == 'BTCUSD':
        return 'BTC_USD'
    forex_currencies = ['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'NZD', 'CAD', 'CHF']
    if '_' not in symbol and len(symbol) == 6:
        for i in range(3, 6):
            base = symbol[:i]
            quote = symbol[i:]
            if base in forex_currencies and quote in forex_currencies:
                return f"{base}_{quote}"
    return symbol

def get_instrument_type(instrument: str) -> str:
    """Determine the instrument type for sizing and ATR calculations"""
    instrument = standardize_symbol(instrument)
    # Check for crypto
    if any(crypto in instrument for crypto in ["BTC", "ETH", "XRP", "LTC"]):
        return "CRYPTO"
    # Check for gold or silver
    if instrument in ["XAU_USD", "XAG_USD"]:
        return instrument
    # If it matches a forex pattern like "XXX_YYY", assume FOREX
    if re.match(r'^[A-Z]{3}_[A-Z]{3}$', instrument):
        return "FOREX"
    return "STOCK"

# Dummy implementation of get_open_positions (replace with your actual code)
async def get_open_positions(account_id: str = None) -> Tuple[bool, Dict[str, Any]]:
    return True, {"positions": []}

##############################################################################
# Error Handling Infrastructure
##############################################################################
##############################################################################
# Circuit Breaker Pattern (Without Notifications)
##############################################################################

class CircuitBreaker:
    def __init__(self, failure_threshold=5, cooldown_seconds=300):
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self._lock = asyncio.Lock()
        
    async def record_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = datetime.utcnow()
            if self.state == "CLOSED" and self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                logger.warning(f"Circuit breaker tripped after {self.failure_count} failures")
                
    async def record_success(self):
        async with self._lock:
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
                logger.info("Circuit breaker reset after successful operation")
                
    async def is_open(self):
        async with self._lock:
            if self.state == "OPEN" and self.last_failure_time:
                elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                if elapsed >= self.cooldown_seconds:
                    self.state = "HALF_OPEN"
                    logger.info(f"Circuit breaker auto-reset after {self.cooldown_seconds} seconds cooldown")
            return self.state == "OPEN"

    async def get_status(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                "state": self.state,
                "failure_count": self.failure_count,
                "last_failure_time": self.last_failure_time.isoformat() if self.last_failure_time else None
            }

    async def reset(self):
        async with self._lock:
            self.failure_count = 0
            self.state = "CLOSED"
            self.last_failure_time = None
            logger.info("Circuit breaker manually reset")

class ErrorRecovery:
    def __init__(self):
        self.circuit_breaker = CircuitBreaker()
        self.recovery_attempts = {}
        self.recovery_history = {}

    async def handle_error(self, request_id: str, operation: str, 
                          error: Exception, context: Dict[str, Any] = None) -> bool:
        error_str = str(error)
        error_type = type(error).__name__
        logger.error(f"Error in {operation} (request {request_id}): {error_str}")
        if await self.circuit_breaker.is_open():
            logger.warning(f"Circuit breaker open, skipping recovery for {operation}")
            return False
        await self.circuit_breaker.record_failure()
        recovery_strategy = self._get_recovery_strategy(operation, error_type, error_str)
        if not recovery_strategy:
            logger.warning(f"No recovery strategy for {error_type} in {operation}")
            return False
        async with self._lock:
            if request_id not in self.recovery_attempts:
                self.recovery_attempts[request_id] = {
                    "count": 0,
                    "last_attempt": time.time(),
                    "operation": operation
                }
            attempt_info = self.recovery_attempts[request_id]
            attempt_info["count"] += 1
            attempt_info["last_attempt"] = time.time()
            if attempt_info["count"] > 3:
                logger.error(f"Maximum recovery attempts reached for {request_id} ({operation})")
                del self.recovery_attempts[request_id]
                return False
        logger.info(f"Attempting recovery for {operation} (attempt {attempt_info['count']})")
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
            self._record_recovery_outcome(operation, error_type, recovery_strategy, recovery_success)
            if recovery_success:
                async with self._lock:
                    if request_id in self.recovery_attempts:
                        del self.recovery_attempts[request_id]
            return recovery_success
        except Exception as recovery_error:
            logger.error(f"Error during recovery attempt: {str(recovery_error)}")
            return False

    def handle_async_errors(func):
        """Decorator to handle errors in asynchronous functions.
        
        If an exception occurs, it logs the error (including traceback) and returns
        a tuple (False, {"error": error message}).
        """
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                # Log the error with traceback
                logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
                # Return a standard error tuple
                return False, {"error": str(e)}
        return wrapper


    def _get_recovery_strategy(self, operation: str, error_type: str, error_message: str) -> Optional[str]:
        if any(term in error_type for term in ["Timeout", "Connection", "ClientError"]):
            return "reconnect"
        if "session" in error_message.lower() or "closed" in error_message.lower():
            return "session_reset"
        if operation in ["close_position", "_handle_position_actions", "update_position"]:
            if "not defined" in error_message or "not found" in error_message:
                return "position_sync"
        if error_type in ["TypeError", "AttributeError", "KeyError"]:
            if "subscriptable" in error_message or "not defined" in error_message:
                return "position_sync"
        if operation in ["execute_trade", "close_position", "get_account_balance", "get_current_price"]:
            return "retry"
        return None

    async def _retry_operation(self, operation: str, context: Dict[str, Any], attempt: int) -> bool:
        if not context or not context.get("func"):
            logger.error(f"Cannot retry operation {operation}: missing context or function")
            return False
        delay = min(30, config.base_delay * (2 ** (attempt - 1)))
        logger.info(f"Retrying {operation} after {delay}s delay (attempt {attempt})")
        await asyncio.sleep(delay)
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
        logger.info(f"Reconnecting session before retrying {operation}")
        try:
            await get_session(force_new=True)
            return await self._retry_operation(operation, context, attempt)
        except Exception as e:
            logger.error(f"Reconnection failed: {str(e)}")
            return False
            
    async def _reset_session_and_retry(self, operation: str, context: Dict[str, Any], attempt: int) -> bool:
        logger.info(f"Resetting session for {operation}")
        try:
            await cleanup_stale_sessions()
            await get_session(force_new=True)
            await asyncio.sleep(5)
            return await self._retry_operation(operation, context, attempt)
        except Exception as e:
            logger.error(f"Session reset failed: {str(e)}")
            return False
            
    async def _sync_positions(self, context: Dict[str, Any]) -> bool:
        logger.info("Synchronizing positions with broker")
        try:
            handler = context.get("handler")
            if not handler or not hasattr(handler, "position_tracker"):
                logger.error("Cannot sync positions: missing handler or tracker")
                return False
            success, positions_data = await get_open_positions()
            if not success:
                logger.error("Failed to get positions from broker")
                return False
            tracked_positions = await handler.position_tracker.get_all_positions()
            broker_positions = {
                p["instrument"]: p for p in positions_data.get("positions", [])
            }
            for symbol in tracked_positions:
                if symbol not in broker_positions:
                    logger.warning(f"Position {symbol} exists in tracker but not with broker - removing")
                    await handler.position_tracker.clear_position(symbol)
                    await handler.risk_manager.clear_position(symbol)
            logger.info(f"Position sync complete. Removed {len(tracked_positions) - len(broker_positions)} stale positions")
            return True
        except Exception as e:
            logger.error(f"Position sync failed: {str(e)}")
            return False
            
    def _record_recovery_outcome(self, operation: str, error_type: str, strategy: str, success: bool):
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
        return await self.circuit_breaker.get_status()
        
    async def reset_circuit_breaker(self) -> bool:
        await self.circuit_breaker.reset()
        return True

# Initialize the error recovery system
error_recovery = ErrorRecovery()

##############################################################################
# Session Management & HTTP Client
##############################################################################

_session: Optional[aiohttp.ClientSession] = None

async def get_session(force_new: bool = False) -> aiohttp.ClientSession:
    global _session
    if force_new or _session is None or _session.closed:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer YOUR_OANDA_API_KEY"  # Replace with your actual API key
        }
        _session = aiohttp.ClientSession(headers=headers, timeout=HTTP_REQUEST_TIMEOUT)
    return _session

async def cleanup_stale_sessions():
    try:
        if _session and not _session.closed:
            await _session.close()
    except Exception as e:
        logger.error(f"Error cleaning up sessions: {str(e)}")

##############################################################################
# Configuration & Constants
##############################################################################

class Settings(BaseSettings):
    oanda_account: str = Field(alias='OANDA_ACCOUNT_ID')
    oanda_token: str = Field(alias='OANDA_API_TOKEN')
    oanda_api_url: str = Field(default="https://api-fxtrade.oanda.com/v3", alias='OANDA_API_URL')
    oanda_environment: str = Field(default="practice", alias='OANDA_ENVIRONMENT')
    allowed_origins: str = "http://localhost"
    connect_timeout: int = 10
    read_timeout: int = 30
    total_timeout: int = 45
    max_simultaneous_connections: int = 100
    spread_threshold_forex: float = 0.001
    spread_threshold_crypto: float = 0.008
    max_retries: int = 3
    base_delay: float = 1.0
    base_position: int = 5000
    max_daily_loss: float = 0.20
    host: str = "0.0.0.0"
    port: int = 8000
    environment: str = "production"
    max_requests_per_minute: int = 100
    trade_24_7: bool = True

    class Config:
        env_file = '.env'
        case_sensitive = True
        
config = Settings()

MAX_DAILY_LOSS = config.max_daily_loss

HTTP_REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=config.total_timeout,
    connect=config.connect_timeout,
    sock_read=config.read_timeout
)

# (Other constants and mappings remain unchanged below...)
# ...

##############################################################################
# Models, Logging, and Session Management
##############################################################################

# (JSONFormatter, setup_logging, AlertData model, etc. remain unchanged)
# ...

##############################################################################
# Global Market Utilities
##############################################################################

# (Functions like check_market_hours, is_instrument_tradeable, get_current_price, etc.)
# ...

##############################################################################
# Risk Management Classes
##############################################################################

# (Classes VolatilityMonitor, LorentzianDistanceClassifier, DynamicExitManager, AdvancedLossManager,
#  RiskAnalytics, MarketStructureAnalyzer, PositionSizingManager, EnhancedRiskManager, TradingConfig)
# ... (unchanged for brevity)

##############################################################################
# Trade Execution & Close Position Functions
##############################################################################

async def get_account_balance(account_id: str) -> float:
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Getting account balance for {account_id}")
    try:
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{account_id}"
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
                    response_text = await response.text()
                    logger.error(f"[{request_id}] Error getting account balance: {response.status}, Response: {response_text}")
                    if "RATE_LIMIT" in response_text:
                        await asyncio.sleep(60)
                    else:
                        delay = config.base_delay * (2 ** retries)
                        await asyncio.sleep(delay)
                    retries += 1
            except aiohttp.ClientError as e:
                logger.error(f"[{request_id}] Network error: {str(e)}")
                if retries < config.max_retries - 1:
                    await asyncio.sleep(config.base_delay * (2 ** retries))
                    retries += 1
                    continue
                raise
        raise ValueError(f"Failed to get account balance after {config.max_retries} attempts")
    except Exception as e:
        logger.error(f"[{request_id}] Error: {str(e)}", exc_info=True)
        return 10000.0

async def close_position(alert_data: Dict[str, Any], position_tracker=None) -> Tuple[bool, Dict[str, Any]]:
    request_id = str(uuid.uuid4())
    try:
        instrument = standardize_symbol(alert_data['symbol'])
        account_id = alert_data.get('account', config.oanda_account)
        success, position_data = await get_open_positions(account_id)
        if not success:
            return False, position_data
        position = next(
            (p for p in position_data.get('positions', [])
             if p['instrument'] == instrument),
            None
        )
        if not position:
            logger.warning(f"[{request_id}] No position found for {instrument}")
            return False, {"error": f"No open position for {instrument}"}
        long_units = float(position['long'].get('units', '0'))
        short_units = float(position['short'].get('units', '0'))
        close_data = {
            "longUnits": "ALL" if long_units > 0 else "NONE",
            "shortUnits": "ALL" if short_units < 0 else "NONE"
        }
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{account_id}/positions/{instrument}/close"
        async with session.put(url, json=close_data, timeout=HTTP_REQUEST_TIMEOUT) as response:
            result = await response.json()
            if response.status == 200:
                logger.info(f"[{request_id}] Position closed: {result}")
                pnl = 0.0
                try:
                    if 'longOrderFillTransaction' in result and result['longOrderFillTransaction']:
                        pnl += float(result['longOrderFillTransaction'].get('pl', 0))
                    if 'shortOrderFillTransaction' in result and result['shortOrderFillTransaction']:
                        pnl += float(result['shortOrderFillTransaction'].get('pl', 0))
                    logger.info(f"[{request_id}] P&L: {pnl}")
                    if position_tracker and pnl != 0 and alert_handler and alert_handler.risk_analytics:
                        await alert_handler.risk_analytics.record_trade_result(
                            instrument, position.get('direction', 'UNKNOWN'),
                            float(position.get('entry_price', 0)),
                            0, abs(long_units or short_units), pnl
                        )
                except Exception as e:
                    logger.error(f"[{request_id}] Error recording P&L: {str(e)}")
                return True, result
            else:
                logger.error(f"[{request_id}] Failed to close position: {result}")
                return False, result
    except Exception as e:
        logger.error(f"[{request_id}] Error closing position: {str(e)}")
        return False, {"error": str(e)}

@handle_async_errors
async def execute_trade(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    request_id = str(uuid.uuid4())
    instrument = standardize_symbol(alert_data['symbol'])
    logger.info(f"[{request_id}] Executing trade for {instrument} - Action: {alert_data['action']}")
    try:
        if 'timeframe' not in alert_data:
            logger.warning(f"[{request_id}] No timeframe provided; defaulting to 15M")
            alert_data['timeframe'] = "15M"
        else:
            original_tf = alert_data['timeframe']
            alert_data['timeframe'] = ensure_proper_timeframe(alert_data['timeframe'])
            logger.info(f"[{request_id}] Normalized timeframe from {original_tf} to {alert_data['timeframe']}")
        balance = await get_account_balance(alert_data.get('account', config.oanda_account))
        # Calculate trade size (using your calculate_trade_size function)
        units, precision = await calculate_trade_size(instrument, alert_data['percentage'], balance)
        if alert_data['action'].upper() == 'SELL':
            units = -abs(units)
        current_price = await get_current_price(instrument, alert_data['action'])
        atr = await get_atr(instrument, alert_data['timeframe'])
        instrument_type = get_instrument_type(instrument)
        atr_multiplier = get_atr_multiplier(instrument_type, alert_data['timeframe'])
        price_precision = 3 if "JPY" in instrument else 5
        if alert_data['action'].upper() == 'BUY':
            stop_loss = round(current_price - (atr * atr_multiplier), price_precision)
            take_profits = [
                round(current_price + (atr * atr_multiplier), price_precision),
                round(current_price + (atr * atr_multiplier * 2), price_precision),
                round(current_price + (atr * atr_multiplier * 3), price_precision)
            ]
        else:
            stop_loss = round(current_price + (atr * atr_multiplier), price_precision)
            take_profits = [
                round(current_price - (atr * atr_multiplier), price_precision),
                round(current_price - (atr * atr_multiplier * 2), price_precision),
                round(current_price - (atr * atr_multiplier * 3), price_precision)
            ]
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
                    "price": str(take_profits[0]),
                    "timeInForce": "GTC",
                    "triggerMode": "TOP_OF_BOOK"
                }
            }
        }
        if alert_data.get('use_trailing_stop', True):
            trailing_distance = round(atr * atr_multiplier, price_precision)
            order_data["order"]["trailingStopLossOnFill"] = {
                "distance": str(trailing_distance),
                "timeInForce": "GTC",
                "triggerMode": "TOP_OF_BOOK"
            }
        logger.info(f"[{request_id}] Order data: {json.dumps(order_data)}")
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{alert_data.get('account', config.oanda_account)}/orders"
        retries = 0
        while retries < config.max_retries:
            try:
                async with session.post(url, json=order_data, timeout=HTTP_REQUEST_TIMEOUT) as response:
                    response_text = await response.text()
                    logger.info(f"[{request_id}] Response: {response.status}, {response_text}")
                    if response.status == 201:
                        result = json.loads(response_text)
                        logger.info(f"[{request_id}] Trade executed successfully: {result}")
                        return True, result
                    try:
                        error_data = json.loads(response_text)
                        error_code = error_data.get("errorCode", "UNKNOWN_ERROR")
                        error_message = error_data.get("errorMessage", "Unknown error")
                        logger.error(f"[{request_id}] OANDA error: {error_code} - {error_message}")
                    except:
                        pass
                    if "RATE_LIMIT" in response_text:
                        await asyncio.sleep(60)
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
    normalized_timeframe = ensure_proper_timeframe(timeframe)
    logger.debug(f"ATR calculation: Normalized timeframe from {timeframe} to {normalized_timeframe}")
    instrument_type = get_instrument_type(instrument)
    default_atr_values = {
        "FOREX": {"15M": 0.0010, "1H": 0.0025, "4H": 0.0050, "D": 0.0100},
        "STOCK": {"15M": 0.01, "1H": 0.02, "4H": 0.03, "D": 0.05},
        "COMMODITY": {"15M": 0.05, "1H": 0.10, "4H": 0.20, "D": 0.50},
        "CRYPTO": {"15M": 0.02, "1H": 0.03, "4H": 0.04, "D": 0.05}  # Example values
    }
    return default_atr_values.get(instrument_type, default_atr_values["FOREX"]).get(normalized_timeframe, default_atr_values["FOREX"]["1H"])

##############################################################################
# Position Tracking, Alert Handling, and API Setup
##############################################################################

# (PositionTracker, AlertHandler, and the remaining API endpoints remain mostly unchanged.)
# Added missing methods to AlertHandler:
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
        if not self._initialized:
            async with self._lock:
                if not self._initialized:
                    await self.position_tracker.start()
                    self._initialized = True
                    self._running = True
                    self._price_monitor_task = asyncio.create_task(self._monitor_positions())
                    logger.info("Alert handler initialized with price monitoring")
    
    async def stop(self):
        try:
            self._running = False
            if self._price_monitor_task:
                self._price_monitor_task.cancel()
                try:
                    await self._price_monitor_task
                except asyncio.CancelledError:
                    logger.info("Price monitoring task cancelled")
            await self.position_tracker.stop()
            logger.info("Alert handler stopped")
        except Exception as e:
            logger.error(f"Error stopping alert handler: {str(e)}")
    
    async def _monitor_positions(self):
        while self._running:
            try:
                positions = await self.position_tracker.get_all_positions()
                for symbol, position in positions.items():
                    try:
                        current_price = await get_current_price(symbol, position['position_type'])
                        actions = await self.risk_manager.update_position(symbol, current_price)
                        exit_actions = await self.dynamic_exit_manager.update_exits(symbol, current_price)
                        if exit_actions:
                            actions.update(exit_actions)
                        loss_actions = await self.loss_manager.update_position_loss(symbol, current_price)
                        if loss_actions:
                            actions.update(loss_actions)
                        await self.risk_analytics.update_position(symbol, current_price)
                        if actions:
                            await self._handle_position_actions(symbol, actions, current_price)
                    except Exception as e:
                        logger.error(f"Error monitoring position {symbol}: {str(e)}")
                        continue
                await asyncio.sleep(15)
            except Exception as e:
                logger.error(f"Error in position monitoring: {str(e)}")
                await asyncio.sleep(60)
    
    async def _handle_position_actions(self, symbol: str, actions: Dict[str, Any], current_price: float):
        request_id = str(uuid.uuid4())
        try:
            if actions.get('stop_loss') or actions.get('position_limit') or actions.get('daily_limit') or actions.get('drawdown_limit'):
                logger.info(f"Stop loss or risk limit hit for {symbol} at {current_price}")
                await close_position({"symbol": symbol, "account": config.oanda_account})
            if 'take_profits' in actions:
                tp_actions = actions['take_profits']
                for level, tp_data in tp_actions.items():
                    logger.info(f"Take profit {level} hit for {symbol} at {tp_data['price']}")
                    if level == 0:
                        await close_partial_position({"symbol": symbol, "account": config.oanda_account}, 50)
                    elif level == 1:
                        await close_partial_position({"symbol": symbol, "account": config.oanda_account}, 50)
                    else:
                        await close_position({"symbol": symbol, "account": config.oanda_account})
            if 'trailing_stop' in actions and isinstance(actions['trailing_stop'], dict):
                logger.info(f"Updated trailing stop for {symbol} to {actions['trailing_stop'].get('new_stop')}")
            if 'time_adjustment' in actions:
                logger.info(f"Time-based adjustment for {symbol}: {actions['time_adjustment'].get('action')}")
        except Exception as e:
            logger.error(f"Error handling actions for {symbol}: {str(e)}")
            error_context = {"symbol": symbol, "current_price": current_price, "handler": self}
            await self.error_recovery.handle_error(request_id, "_handle_position_actions", e, error_context)

    async def process_alert(self, alert_data: Dict[str, Any]):
        logger.info(f"Processing alert: {alert_data}")
        success, result = await execute_trade(alert_data)
        if success:
            logger.info("Trade executed successfully")
        else:
            logger.error("Trade execution failed")
    
    async def update_config(self, config_data: Dict[str, Any]) -> bool:
        logger.info(f"Updating configuration with: {config_data}")
        # Stub implementation
        return True

# (Other endpoints remain unchanged.)

##############################################################################
# FastAPI Setup & Lifespan
##############################################################################

alert_handler: Optional[AlertHandler] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing application...")
    global _session, alert_handler
    try:
        await get_session(force_new=True)
        alert_handler = AlertHandler()
        await alert_handler.start()
        logger.info("Services initialized successfully")
        handle_shutdown_signals()
        yield
    finally:
        logger.info("Shutting down services...")
        await cleanup_resources()
        logger.info("Shutdown complete")

async def cleanup_resources():
    tasks = []
    if alert_handler is not None:
        tasks.append(alert_handler.stop())
    if _session is not None and not _session.closed:
        tasks.append(_session.close())
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

def handle_shutdown_signals():
    async def shutdown(sig: signal.Signals):
        logger.info(f"Received exit signal {sig.name}")
        await cleanup_resources()
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_event_loop().add_signal_handler(sig,
            lambda s=sig: asyncio.create_task(shutdown(s))
        )

def create_error_response(status_code: int, message: str, request_id: str) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": message, "request_id": request_id})

app = FastAPI(
    title="OANDA Trading Bot",
    description="Advanced async trading bot using FastAPI and aiohttp",
    version="1.2.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.allowed_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def inject_dependencies(request: Request, call_next):
    request.state.alert_handler = alert_handler
    request.state.session = await get_session()
    return await call_next(request)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    request_id = str(uuid.uuid4())
    try:
        logger.info(f"[{request_id}] {request.method} {request.url} started")
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        logger.info(f"[{request_id}] Completed with status {response.status_code} in {process_time:.4f}s")
        response.headers["X-Request-ID"] = request_id
        return response
    except Exception as e:
        logger.error(f"[{request_id}] Error processing request: {str(e)}", exc_info=True)
        return create_error_response(500, "Internal server error", request_id)

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    path = request.url.path
    if path in ["/api/trade", "/api/close", "/api/alerts"]:
        client_ip = request.client.host
        if not hasattr(app, "rate_limiters"):
            app.rate_limiters = {}
        if client_ip not in app.rate_limiters:
            app.rate_limiters[client_ip] = {"count": 0, "reset_time": time.time() + 60}
        rate_limiter = app.rate_limiters[client_ip]
        current_time = time.time()
        if current_time > rate_limiter["reset_time"]:
            rate_limiter["count"] = 0
            rate_limiter["reset_time"] = current_time + 60
        rate_limiter["count"] += 1
        if rate_limiter["count"] > config.max_requests_per_minute:
            logger.warning(f"Rate limit exceeded for {client_ip}")
            return JSONResponse(
                status_code=429,
                content={"error": "Too many requests", "retry_after": int(rate_limiter["reset_time"] - current_time)}
            )
    return await call_next(request)

# (API endpoints such as /api/health, /api/circuit-breaker/status, /api/account, etc.)
# remain unchanged.

def start():
    import uvicorn
    setup_logging()
    logger.info(f"Starting application in {config.environment} mode")
    host = config.host
    port = config.port
    logger.info(f"Server starting at {host}:{port}")
    uvicorn.run("app:app", host=host, port=port, reload=(config.environment == "development"))

if __name__ == "__main__":
    start()
