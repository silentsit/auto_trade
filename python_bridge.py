##############################################################################
# Enhanced Trading Bot with Professional Risk Management
# Extends the original trading bot with institutional-grade risk controls
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

class RiskManagementError(TradingError):
    """Errors related to risk management violations"""
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
    
    # Enhanced Risk Management Settings
    use_stop_loss: bool = True
    stop_loss_atr_multiplier: float = 2.0
    atr_period: int = 14
    use_take_profit: bool = True
    tp1_rr_ratio: float = 1.0   # Take 1/3 at 1R
    tp2_rr_ratio: float = 2.0   # Take 1/3 at 2R
    enable_trailing_stop: bool = True
    initial_trail_multiplier: float = 3.0
    tight_trail_multiplier: float = 1.5
    rr3_threshold: float = 3.0  # Tighten trail at 3R
    rr5_threshold: float = 5.0  # Further tighten trail at 5R
    use_time_decay: bool = True
    time_decay_bars: int = 5
    reduce_position_at_half: bool = True  # Reduce position by half when loss reaches 50% of stop distance
    max_drawdown_per_trade: float = 0.03  # 3% max drawdown per trade
    max_risk_per_trade: float = 0.01      # 1% account risk per trade
    max_correlated_exposure: float = 0.05 # 5% max exposure to correlated assets
    
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

# Add this after config = Settings()
MAX_DAILY_LOSS = config.max_daily_loss

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

# Instrument leverages based on Singapore MAS regulations
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
}

# Define correlation groups for instruments
CORRELATION_GROUPS = {
    "USD_PAIRS": ["USD_CHF", "EUR_USD", "GBP_USD", "USD_JPY", "AUD_USD", "USD_CAD", "NZD_USD", "USD_SGD", "USD_THB"],
    "JPY_PAIRS": ["USD_JPY", "EUR_JPY", "GBP_JPY", "AUD_JPY", "NZD_JPY"],
    "CRYPTO": ["BTC_USD", "ETH_USD", "XRP_USD", "LTC_USD", "BTCUSD"],
    "GOLD": ["XAU_USD"]
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

# Enhanced risk management field mappings
TV_RISK_FIELD_MAP = {
    'stopLossATR': 'stopLossATR',  # ATR multiplier for stop loss
    'takeProfitRR1': 'takeProfitRR1',  # First take profit in R multiple
    'takeProfitRR2': 'takeProfitRR2',  # Second take profit in R multiple
    'trailingAtr': 'trailingAtr',  # ATR multiplier for trailing
    'useTrailing': 'useTrailing',  # Whether to use trailing stop
    'usePartialTP': 'usePartialTP'  # Whether to use partial take profits
}

# Error Mapping
ERROR_MAP = {
    "INSUFFICIENT_MARGIN": (True, "Insufficient margin", 400),
    "ACCOUNT_NOT_TRADEABLE": (True, "Account restricted", 403),
    "MARKET_HALTED": (False, "Market is halted", 503),
    "RATE_LIMIT": (True, "Rate limit exceeded", 429)
}

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
# Enhanced Models with Risk Management
##############################################################################

class EnhancedAlertData(BaseModel):
    """Enhanced Alert data model with improved validation and risk parameters"""
    symbol: str
    action: str
    timeframe: Optional[str] = "1M"
    orderType: Optional[str] = "MARKET"
    timeInForce: Optional[str] = "FOK"
    percentage: Optional[float] = 15.0
    account: Optional[str] = None
    id: Optional[str] = None
    comment: Optional[str] = None
    # Risk management parameters
    stopLossATR: Optional[float] = config.stop_loss_atr_multiplier
    takeProfitRR1: Optional[float] = config.tp1_rr_ratio
    takeProfitRR2: Optional[float] = config.tp2_rr_ratio
    trailingAtr: Optional[float] = config.initial_trail_multiplier
    useTrailing: Optional[bool] = config.enable_trailing_stop
    usePartialTP: Optional[bool] = config.use_take_profit

    @validator('timeframe', pre=True, always=True)
    def validate_timeframe(cls, v):
        """Validate timeframe with improved error handling and None checking"""
        if v is None:
            return "15M"  # Default value if timeframe is None

        if not isinstance(v, str):
            v = str(v)

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
    
    @validator('stopLossATR')
    def validate_stop_loss_atr(cls, v):
        """Validate stop loss ATR multiplier"""
        if v is None:
            return config.stop_loss_atr_multiplier
        if v < 0.5 or v > 10:
            raise ValueError("Stop loss ATR multiplier must be between 0.5 and 10")
        return float(v)
    
    @validator('takeProfitRR1', 'takeProfitRR2')
    def validate_take_profit_rr(cls, v):
        """Validate take profit risk-reward ratio"""
        if v is None:
            return None
        if v < 0.1:
            raise ValueError("Take profit risk-reward ratio must be at least 0.1")
        return float(v)
        
    @validator('trailingAtr')
    def validate_trailing_atr(cls, v):
        """Validate trailing stop ATR multiplier"""
        if v is None:
            return config.initial_trail_multiplier
        if v < 0.5:
            raise ValueError("Trailing stop ATR multiplier must be at least 0.5")
        return float(v)

    class Config:
        str_strip_whitespace = True
        validate_assignment = True
        extra = "forbid"

# For backward compatibility
AlertData = EnhancedAlertData

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

@handle_async_errors
async def get_instrument_atr(instrument: str, timeframe: str = "H1", period: int = 14) -> float:
    """Fetch ATR for an instrument"""
    try:
        # Convert timeframe to Oanda format
        if timeframe.upper().endswith('M'):
            granularity = f"M{timeframe[:-1]}"
        elif timeframe.upper().endswith('H'):
            granularity = f"H{timeframe[:-1]}"
        else:
            granularity = "H1"  # Default
        
        # Get historical candles
        session = await get_session()
        url = f"{config.oanda_api_url}/instruments/{instrument}/candles"
        params = {
            "count": period + 1,  # Need period + 1 to calculate period ATRs
            "granularity": granularity,
            "price": "M"  # Midpoint
        }
        
        async with session.get(url, params=params, timeout=HTTP_REQUEST_TIMEOUT) as response:
            if response.status != 200:
                error_text = await response.text()
                raise ValueError(f"ATR fetch failed: {error_text}")
            
            data = await response.json()
            if not data.get('candles') or len(data['candles']) < period + 1:
                raise ValueError(f"Insufficient candle data received for ATR calculation: {len(data.get('candles', []))}")
            
            # Calculate ATR
            highs = [float(candle['mid']['h']) for candle in data['candles']]
            lows = [float(candle['mid']['l']) for candle in data['candles']]
            closes = [float(candle['mid']['c']) for candle in data['candles']]
            
            # Calculate True Range values
            tr_values = []
            for i in range(1, len(highs)):
                high_low = highs[i] - lows[i]
                high_prev_close = abs(highs[i] - closes[i-1])
                low_prev_close = abs(lows[i] - closes[i-1])
                tr = max(high_low, high_prev_close, low_prev_close)
                tr_values.append(tr)
            
            # Calculate simple average of TR values for ATR
            atr = sum(tr_values) / len(tr_values)
            logger.info(f"Calculated ATR for {instrument} ({granularity}): {atr}")
            return atr
            
    except Exception as e:
        logger.error(f"Error calculating ATR for {instrument}: {str(e)}")
        # Provide a fallback ATR value based on price level to avoid failures
        try:
            price = await get_current_price(instrument, "BUY")
            if "JPY" in instrument:
                return price * 0.001  # Approx 10 pips for JPY pairs
            elif any(crypto in instrument for crypto in ["BTC", "ETH", "XRP", "LTC"]):
                return price * 0.02  # 2% for crypto
            elif "XAU" in instrument:
                return 1.5  # $1.5 for gold
            else:
                return price * 0.0005  # Approx 5 pips for other FX
        except:
            return 0.0001  # Final fallback

##############################################################################
# Enhanced Risk Management Functions
##############################################################################

@handle_async_errors
async def calculate_risk_parameters(
    instrument: str, 
    action: str,
    price: float,
    atr: float,
    stop_loss_atr: float,
    balance: float
) -> Dict[str, Any]:
    """Calculate professional risk management parameters"""
    try:
        # Calculate stop distance in pips/points
        stop_distance = atr * stop_loss_atr
        
        # Determine direction and stop price
        if action.upper() == 'BUY':
            stop_price = price - stop_distance
        else:  # SELL
            stop_price = price + stop_distance
        
        # Calculate risk amount (balance * max risk per trade)
        risk_amount = balance * config.max_risk_per_trade
        
        # Calculate position size based on risk amount and stop distance
        position_value = risk_amount / (stop_distance / price)
        
        # Get leverage for the instrument
        leverage = INSTRUMENT_LEVERAGES.get(standardize_symbol(instrument), 20)
        
        # Calculate actual position size
        position_size = position_value * leverage
        
        # Risk in monetary terms
        risk_in_currency = balance * config.max_risk_per_trade
        
        # Maximum drawdown per trade
        max_drawdown = balance * config.max_drawdown_per_trade
        
        return {
            "stop_price": stop_price,
            "stop_distance": stop_distance,
            "position_size": position_size,
            "risk_amount": risk_amount,
            "risk_percentage": config.max_risk_per_trade * 100,
            "max_drawdown": max_drawdown,
            "atr": atr,
            "entry_price": price
        }
    except Exception as e:
        logger.error(f"Error calculating risk parameters: {str(e)}")
        raise

@handle_async_errors
async def calculate_take_profit_levels(
    entry_price: float,
    stop_price: float,
    action: str,
    tp1_rr: float,
    tp2_rr: float
) -> Tuple[float, float]:
    """Calculate take profit levels based on risk-reward ratios"""
    try:
        # Calculate risk (R) in price terms
        risk = abs(entry_price - stop_price)
        
        # Calculate take profit levels based on direction
        if action.upper() == 'BUY':
            tp1 = entry_price + (risk * tp1_rr)
            tp2 = entry_price + (risk * tp2_rr)
        else:  # SELL
            tp1 = entry_price - (risk * tp1_rr)
            tp2 = entry_price - (risk * tp2_rr)
            
        return tp1, tp2
    except Exception as e:
        logger.error(f"Error calculating take profit levels: {str(e)}")
        raise

@handle_async_errors
async def check_correlation_exposure(
    account_id: str,
    instrument: str,
    position_size: float
) -> Tuple[bool, float, str]:
    """Check if adding the position would exceed correlation limits"""
    try:
        # Get all open positions
        success, positions_data = await get_open_positions(account_id)
        if not success:
            logger.error(f"Failed to fetch positions for correlation check: {positions_data}")
            return True, 0, "Failed to check correlation exposure"
            
        # Get account balance
        balance = await get_account_balance(account_id)
        
        # Find which correlation group the instrument belongs to
        instrument_group = None
        std_instrument = standardize_symbol(instrument)
        
        for group_name, instruments in CORRELATION_GROUPS.items():
            if std_instrument in instruments:
                instrument_group = group_name
                break
                
        if not instrument_group:
            # If no correlation group found, allow the trade
            return True, 0, "No correlation group found"
            
        # Calculate current exposure to the correlation group
        group_exposure = 0
        for position in positions_data.get('positions', []):
            pos_instrument = position['instrument']
            
            # Check if this position belongs to the same correlation group
            if any(pos_instrument in instruments for _, instruments in CORRELATION_GROUPS.items() if instrument_group == _):
                # Add up the position values (long and short)
                long_units = float(position['long'].get('units', '0'))
                short_units = float(position['short'].get('units', '0'))
                
                # Get absolute position size
                position_units = abs(long_units) + abs(short_units)
                
                # Get current price to calculate position value
                price = await get_current_price(pos_instrument, 'BUY')
                
                # Add to group exposure (as percentage of account)
                group_exposure += (position_units * price) / balance
        
        # Calculate what new exposure would be
        price = await get_current_price(std_instrument, 'BUY')
        new_exposure = group_exposure + ((position_size * price) / balance)
        
        # Check if new exposure exceeds limit
        if new_exposure > config.max_correlated_exposure:
            logger.warning(f"Correlation exposure limit exceeded: {new_exposure:.2%} > {config.max_correlated_exposure:.2%}")
            return False, new_exposure, f"Correlation exposure limit exceeded for {instrument_group}"
            
        return True, new_exposure, "Correlation exposure within limits"
