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
from datetime import datetime, timedelta
from pytz import timezone
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any, Union, List, Tuple, Callable, TypeVar, ParamSpec
from contextlib import asynccontextmanager
from pydantic import BaseModel, validator, ValidationError
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

##############################################################################
# Error Handling Infrastructure
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

from pydantic import Field
from pydantic_settings import BaseSettings

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
    risk_percent: float = 0.02
    max_daily_loss: float = 0.05
    connect_timeout: int = 10
    read_timeout: int = 30
    total_timeout: int = 45
    max_simultaneous_connections: int = 100
    spread_threshold_forex: float = 0.001
    spread_threshold_crypto: float = 0.008
    max_retries: int = 3
    base_delay: float = 1.0
    base_position: int = 300000  # Updated from 100000 to 300000

    trade_24_7: bool = False  # Set to True for exchanges trading 24/7

    class Config:
        env_file = '.env'
        case_sensitive = True
        
config = Settings()

# Session Configuration
HTTP_REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=config.total_timeout,
    connect=config.connect_timeout,
    sock_read=config.read_timeout
)

# Trading Constants
RISK_PERCENTAGE = config.risk_percent
MAX_DAILY_LOSS = config.max_daily_loss

# Market Session Configuration
MARKET_SESSIONS = {
    "FOREX": {
        "hours": "24/5",
        "timezone": "America/New_York",
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

# Instrument Configurations
INSTRUMENT_LEVERAGES = {
    "USD_CHF": 20, "EUR_USD": 20, "GBP_USD": 20,
    "USD_JPY": 20, "AUD_USD": 20, "USD_THB": 20,
    "CAD_CHF": 20, "NZD_USD": 20, "AUD_CAD": 20,
    "BTC_USD": 2, "ETH_USD": 2, "XRP_USD": 2, "LTC_USD": 2,
    "XAU_USD": 1
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
    percentage: Optional[float] = 1.0
    account: Optional[str] = None
    id: Optional[str] = None
    comment: Optional[str] = None

    @validator('timeframe', pre=True, always=True)
    def validate_timeframe(cls, v):
        """Validate timeframe with improved error handling and None checking"""
        if v is None:
            return "1M"  # Default value if timeframe is None

        if not isinstance(v, str):
            v = str(v)

        # Special handling for TradingView interval values
        if v.isdigit():
            # Map common TradingView numeric intervals to proper format
            # Updated to handle '1' as 15M for your 15-minute chart
            mapping = {
                '1': "15M",  # TradingView sends '1' for 15-minute charts
                '4': "4H", 
                '12': "12H", 
                '5': "5M", 
                '15': "15M", 
                '30': "30M",
                '60': "1H",
                '240': "4H"
            }
            v = mapping.get(v, f"{v}M")

        # Now validate the format
        pattern = re.compile(r'^(\d+)([mMhH])$')
        match = pattern.match(v)
        if not match:
            # For simpler values like '1', convert to minutes format
            if v.isdigit():
                return f"{v}M"
            raise ValueError(f"Invalid timeframe format: {v}. Use '15M' or '1H' format")
        
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
        if not v or len(v) < 6:
            raise ValueError("Symbol must be at least 6 characters")
        
        v = v.upper().replace('/', '_')
        if v in ['XAUUSD', 'XAUSD']:
            instrument = 'XAU_USD'
        else:
            instrument = f"{v[:3]}_{v[3:]}"
        
        if instrument not in INSTRUMENT_LEVERAGES:
            raise ValueError(f"Invalid instrument: {instrument}")
        
        return v

    @validator('percentage')
    def validate_percentage(cls, v):
        """Validate percentage with proper bounds checking"""
        if v is None:
            return 1.0
        if not 0 < v <= 100:
            raise ValueError("Percentage must be between 0 and 100")
        return float(v)

    @validator('symbol')
    def validate_symbol(cls, v):
        """Validate symbol with improved checks"""
        if not v or len(v) < 6:
            raise ValueError("Symbol must be at least 6 characters")
        
        v = v.upper().replace('/', '_')
        if v in ['XAUUSD', 'XAUSD']:
            instrument = 'XAU_USD'
        else:
            instrument = f"{v[:3]}_{v[3:]}"
        
        if instrument not in INSTRUMENT_LEVERAGES:
            raise ValueError(f"Invalid instrument: {instrument}")
        
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
# Session Management
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
# Market Utilities
##############################################################################

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
    """Check if instrument is tradeable with improved error handling"""
    try:
        if any(c in instrument for c in ["BTC","ETH","XRP","LTC"]):
            session_type = "CRYPTO"
        elif "XAU" in instrument:
            session_type = "XAU_USD"
        else:
            session_type = "FOREX"
        
        if session_type not in MARKET_SESSIONS:
            return False, f"Unknown session type for instrument {instrument}"
            
        if check_market_hours(MARKET_SESSIONS[session_type]):
            return True, "Market open"
        return False, f"Instrument {instrument} outside market hours"
    except Exception as e:
        logger.error(f"Error checking instrument tradeable status: {str(e)}")
        return False, f"Error checking trading status: {str(e)}"

@handle_async_errors
async def get_current_price(instrument: str, action: str) -> float:
    """Get current price with improved error handling and timeout"""
    try:
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{config.oanda_account}/pricing"
        params = {"instruments": instrument}
        
        async with session.get(url, params=params, timeout=HTTP_REQUEST_TIMEOUT) as response:
            if response.status != 200:
                error_text = await response.text()
                raise ValueError(f"Price fetch failed: {error_text}")
            data = await response.json()
            if not data.get('prices'):
                raise ValueError("No price data received")
            bid = float(data['prices'][0]['bids'][0]['price'])
            ask = float(data['prices'][0]['asks'][0]['price'])
            return ask if action == 'BUY' else bid
    except asyncio.TimeoutError:
        logger.error(f"Timeout getting price for {instrument}")
        raise
    except Exception as e:
        logger.error(f"Error getting price for {instrument}: {str(e)}")
        raise

##############################################################################
# Trade Execution
##############################################################################

async def get_account_balance(account_id: str) -> float:
    """Fetch account balance for dynamic position sizing"""
    try:
        session = await get_session()
        async with session.get(f"{config.oanda_api_url}/accounts/{account_id}/summary") as resp:
            data = await resp.json()
            return float(data['account']['balance'])
    except Exception as e:
        logger.error(f"Error fetching account balance: {str(e)}")
        raise

async def calculate_trade_size(instrument: str, risk_percentage: float, balance: float) -> Tuple[float, int]:
    """Calculate trade size with improved validation and error handling.
    
    risk_percentage should be between 0 and 100.
    """
    if risk_percentage <= 0 or risk_percentage > 100:
        raise ValueError("Invalid risk_percentage value")
        
    try:
        # Convert risk percentage to risk amount based on balance
        risk_amount = balance * (risk_percentage / 100)
        
        # Determine instrument type and calculate trade size accordingly
        if 'XAU' in instrument:
            precision = 2
            min_size = 0.01  # Adjusted for ounces
            max_size = 5.0   # Adjusted for ounces
            leverage = INSTRUMENT_LEVERAGES.get(instrument, 1)
            
            # Get current XAU price asynchronously
            price = await get_current_price(instrument, 'BUY')
            trade_size = (risk_amount * leverage) / price
            
        elif 'BTC' in instrument:
            precision = 8
            min_size = 0.001  # Adjusted for BTC
            max_size = 1.0    # Adjusted for BTC
            leverage = INSTRUMENT_LEVERAGES.get(instrument, 1)
            
            # Get current BTC price asynchronously
            price = await get_current_price(instrument, 'BUY')
            trade_size = (risk_amount * leverage) / price
            
        else:  # Standard forex pairs
            precision = 0
            min_size = 1000
            max_size = config.base_position
            leverage = INSTRUMENT_LEVERAGES.get(instrument, 20)
            trade_size = risk_amount * leverage
        
        # Apply min and max constraints
        trade_size = max(min_size, min(trade_size, max_size))
        
        # Round the trade size according to instrument precision
        if precision > 0:
            trade_size = round(trade_size, precision)
        else:
            trade_size = int(round(trade_size))
        
        return trade_size, precision
        
    except Exception as e:
        logger.error(f"Error calculating trade size: {str(e)}")
        raise


@handle_async_errors
async def execute_trade(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Execute trade with improved retry logic and error handling"""
    request_id = str(uuid.uuid4())
    instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}".upper()
    
    try:
        # Calculate size and get current price
        balance = await get_account_balance(alert_data.get('account', config.oanda_account))
        units, precision = await calculate_trade_size(instrument, alert_data['percentage'], balance)
        if alert_data['action'].upper() == 'SELL':
            units = -abs(units)
            
        order_data = {
            "order": {
                "type": alert_data['orderType'],
                "instrument": instrument,
                "units": str(units),
                "timeInForce": alert_data['timeInForce'],
                "positionFill": "DEFAULT"
            }
        }
        
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{alert_data.get('account', config.oanda_account)}/orders"
        
        retries = 0
        while retries < config.max_retries:
            try:
                async with session.post(url, json=order_data, timeout=HTTP_REQUEST_TIMEOUT) as response:
                    if response.status == 201:
                        result = await response.json()
                        logger.info(f"[{request_id}] Trade executed successfully: {result}")
                        return True, result
                    
                    error_content = await response.text()
                    if "RATE_LIMIT" in error_content:
                        await asyncio.sleep(60)  # Longer wait for rate limits
                    elif "MARKET_HALTED" in error_content:
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
async def get_open_positions(account_id: Optional[str] = None) -> Tuple[bool, Dict[str, Any]]:
    """Fetch all open positions with improved error handling"""
    try:
        account_id = account_id or config.oanda_account
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{account_id}/openPositions"
        
        async with session.get(url, timeout=HTTP_REQUEST_TIMEOUT) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"Failed to fetch positions: {error_text}")
                return False, {"error": f"Position fetch failed: {error_text}"}
                
            positions_data = await response.json()
            return True, positions_data
            
    except asyncio.TimeoutError:
        logger.error("Timeout fetching positions")
        return False, {"error": "Request timeout"}
    except Exception as e:
        logger.error(f"Error fetching positions: {str(e)}")
        return False, {"error": str(e)}

@handle_async_errors
async def close_position(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Close an open position with improved error handling and validation"""
    request_id = str(uuid.uuid4())
    try:
        instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}".upper()
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
                return True, result
            else:
                logger.error(f"[{request_id}] Failed to close position: {result}")
                return False, result
                
    except Exception as e:
        logger.error(f"[{request_id}] Error closing position: {str(e)}")
        return False, {"error": str(e)}

##############################################################################
# Block 4: Position Tracking and Alert Management
##############################################################################

##############################################################################
# Position Tracking
##############################################################################

class PositionTracker:
    def __init__(self):
        self.positions: Dict[str, Dict[str, Any]] = {}
        self.bar_times: Dict[str, List[datetime]] = {}
        self._lock = asyncio.Lock()
        self._running = False
        self._initialized = False

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
    async def record_position(self, symbol: str, action: str, timeframe: str) -> bool:
        """Record a new position with improved error handling"""
        try:
            async with self._lock:
                current_time = datetime.now(timezone('Asia/Bangkok'))
                
                position_data = {
                    'entry_time': current_time,
                    'position_type': 'LONG' if action.upper() == 'BUY' else 'SHORT',
                    'bars_held': 0,
                    'timeframe': timeframe,
                    'last_update': current_time
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

##############################################################################
# Alert Handler
##############################################################################

class AlertHandler:
    def __init__(self):
        self.position_tracker = PositionTracker()
        self._lock = asyncio.Lock()
        self._initialized = False
    
    async def start(self):
        """Initialize the handler only once"""
        if not self._initialized:
            async with self._lock:
                if not self._initialized:  # Double-check pattern
                    await self.position_tracker.start()
                    self._initialized = True
                    logger.info("Alert handler initialized")
    
    async def stop(self):
        """Stop the alert handler and cleanup resources"""
        try:
            await self.position_tracker.stop()
            logger.info("Alert handler stopped")
        except Exception as e:
            logger.error(f"Error stopping alert handler: {str(e)}")
    
    async def process_alert(self, alert_data: Dict[str, Any]) -> bool:
        """Process trading alerts with improved error handling and validation"""
        request_id = str(uuid.uuid4())
        logger.info(f"[{request_id}] Processing alert: {json.dumps(alert_data, indent=2)}")

        try:
            if not alert_data:
                logger.error(f"[{request_id}] Empty alert data received")
                return False

            async with self._lock:
                action = alert_data['action'].upper()
                symbol = alert_data['symbol']
                instrument = f"{symbol[:3]}_{symbol[3:]}".upper()
                    
                # Market condition check
                tradeable, reason = is_instrument_tradeable(instrument)
                if not tradeable:
                    logger.warning(f"[{request_id}] Market check failed: {reason}")
                    return False

                # Fetch current positions
                success, positions_data = await get_open_positions(
                    alert_data.get('account', config.oanda_account)
                )
                if not success:
                    logger.error(f"[{request_id}] Position check failed: {positions_data}")
                    return False

                # Position closure logic
                if action in ['CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']:
                    logger.info(f"[{request_id}] Processing close request")
                    success, result = await close_position(alert_data)
                    if success:
                        await self.position_tracker.clear_position(symbol)
                    return success

                # Find existing position
                position = next(
                    (p for p in positions_data.get('positions', [])
                     if p['instrument'] == instrument),
                    None
                )

                # Close opposite positions if needed
                if position:
                    has_long = float(position['long'].get('units', '0')) > 0
                    has_short = float(position['short'].get('units', '0')) < 0
                    
                    if (action == 'BUY' and has_short) or (action == 'SELL' and has_long):
                        logger.info(f"[{request_id}] Closing opposite position")
                        close_data = {**alert_data, 'action': 'CLOSE'}
                        success, result = await close_position(close_data)
                        if not success:
                            logger.error(f"[{request_id}] Failed to close opposite position")
                            return False
                        await self.position_tracker.clear_position(symbol)

                # Execute new trade
                logger.info(f"[{request_id}] Executing new trade")
                success, result = await execute_trade(alert_data)
                if success:
                    await self.position_tracker.record_position(
                        symbol,
                        action,
                        alert_data['timeframe']
                    )
                    logger.info(f"[{request_id}] Trade executed successfully")
                return success
                
        except Exception as e:
            logger.error(f"[{request_id}] Critical error: {str(e)}", exc_info=True)
            return False

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
        asyncio.get_event_loop().add_signal_handler(
            sig,
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
        logger.info(f"[{request_id}] {request.method} {request.url}")
        if request.method != "HEAD":
            body = await request.body()
            logger.debug(f"[{request_id}] Body: {body.decode()}")
            async def receive():
                return {"type": "http.request", "body": body}
            request._receive = receive
        
        start_time = time.time()
        response = await call_next(request)
        duration = time.time() - start_time
        
        logger.info(f"[{request_id}] Completed in {duration:.2f}s - Status: {response.status_code}")
        return response
        
    except Exception as e:
        logger.error(f"[{request_id}] Request processing failed: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "message": "Internal server error",
                "request_id": request_id
            }
        )

##############################################################################
# API Endpoints
##############################################################################

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "time": datetime.utcnow().isoformat(),
        "version": "1.2.0"
    }

@app.post("/alerts")
async def handle_alert_endpoint(alert: AlertData):
    """Handle direct trading alerts"""
    request_id = str(uuid.uuid4())
    try:
        return await process_incoming_alert(alert.dict(), source="direct")
    except CustomValidationError as e:
        return create_error_response(422, str(e), request_id)
    except TradingError as e:
        return create_error_response(400, str(e), request_id)
    except Exception as e:
        logger.error(f"Unexpected error in alert endpoint: {str(e)}", exc_info=True)
        return create_error_response(500, "Internal server error", request_id)

# Add this improved logging to your translate_tradingview_signal function:

def translate_tradingview_signal(data: Dict[str, Any]) -> Dict[str, Any]:
    """Translate TradingView webhook data with improved validation and debugging"""
    logger.info(f"Incoming TradingView data: {json.dumps(data, indent=2)}")
    
    translated = {}
    missing_fields = []
    
    for k, v in TV_FIELD_MAP.items():
        value = data.get(v)
        if value is not None:
            translated[k] = value
        elif k in ['symbol', 'action']:  # These are required fields
            missing_fields.append(f"{k} (mapped from '{v}')")
    
    # Log the translation process
    logger.info(f"Translated data: {json.dumps(translated, indent=2)}")
    
    if missing_fields:
        error_msg = f"Missing required fields: {', '.join(missing_fields)}"
        logger.error(error_msg)
        raise ValueError(error_msg)
            
    # Ensure required fields have defaults
    if 'symbol' not in translated:
        raise ValueError("Symbol field is required")
    
    # Apply defaults for optional fields
    translated.setdefault('timeframe', "15M")
    translated.setdefault('orderType', "MARKET")
    translated.setdefault('timeInForce', "FOK")
    translated.setdefault('percentage', 15)
    
    return translated


@app.post("/tradingview")
async def handle_tradingview_webhook(request: Request):
    """Handle TradingView webhook alerts"""
    request_id = str(uuid.uuid4())
    try:
        body = await request.json()
        logger.info(f"Received TradingView webhook: {json.dumps(body, indent=2)}")
        cleaned_data = translate_tradingview_signal(body)
        return await process_incoming_alert(cleaned_data, source="tradingview")
    except CustomValidationError as e:
        return create_error_response(422, str(e), request_id)
    except TradingError as e:
        return create_error_response(400, str(e), request_id)
    except json.JSONDecodeError as e:
        return create_error_response(400, f"Invalid JSON format: {str(e)}", request_id)
    except Exception as e:
        logger.error(f"Unexpected error in webhook: {str(e)}", exc_info=True)
        return create_error_response(500, "Internal server error", request_id)

async def process_incoming_alert(data: Dict[str, Any], source: str) -> JSONResponse:
    """Process incoming alerts with improved validation and error handling"""
    request_id = str(uuid.uuid4())
    try:
        data['account'] = data.get('account', config.oanda_account)
        validated_data = AlertData(**data)
        success = await alert_handler.process_alert(validated_data.dict())
        
        if success:
            return JSONResponse(
                status_code=200,
                content={"message": "Alert processed", "request_id": request_id}
            )
        return JSONResponse(
            status_code=400,
            content={"error": "Processing failed", "request_id": request_id}
        )
    except ValidationError as e:
        logger.error(f"[{request_id}] Validation error: {e.errors()}")
        return JSONResponse(
            status_code=422,
            content={"errors": e.errors(), "request_id": request_id}
        )
    except Exception as e:
        logger.error(f"[{request_id}] Unexpected error: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error", "request_id": request_id}
        )

@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    """Root endpoint with version info"""
    return {
        "status": "active",
        "version": "1.2.0",
        "timestamp": datetime.utcnow().isoformat(),
        "endpoints": ["/alerts", "/tradingview", "/health"]
    }

##############################################################################
# Main Entry Point
##############################################################################

if __name__ == "__main__":
    import uvicorn
    
    # Get port from environment with fallback
    port = int(os.getenv("PORT", 8000))
    
    # Configure uvicorn with improved settings
    uvicorn_config = uvicorn.Config(
        "main:app",
        host="0.0.0.0",
        port=port,
        log_config=None,
        timeout_keep_alive=65,
        reload=False,
        workers=1
    )
    
    server = uvicorn.Server(uvicorn_config)
    
    # Add signal handlers
    for sig in (signal.SIGTERM, signal.SIGINT):
        server.install_signal_handlers()
    
    # Run the server
    server.run()
