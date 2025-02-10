import os
import uuid
import asyncio
import aiohttp
import logging
import logging.handlers
import re
import time
import json
from datetime import datetime, timedelta
from pytz import timezone
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any, Union, List, Tuple
from contextlib import asynccontextmanager
from pydantic import BaseModel, validator, ValidationError
from functools import wraps
from typing import Dict, Any
from fastapi import Request
from fastapi.responses import JSONResponse
import json
import uuid

##############################################################################
# 1. Enhanced Error Handling Decorator
##############################################################################
def handle_async_errors(func):
    """
    Decorator for async functions to provide robust error logging
    and standardized error responses.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except aiohttp.ClientError as e:
            logging.error(f"Network error in {func.__name__}: {str(e)}", exc_info=True)
            return False, {"error": f"Network error: {str(e)}"}
        except asyncio.TimeoutError as e:
            logging.error(f"Timeout in {func.__name__}: {str(e)}", exc_info=True)
            return False, {"error": "Request timed out"}
        except Exception as e:
            logging.error(f"Unexpected error in {func.__name__}: {str(e)}", exc_info=True)
            return False, {"error": f"Unexpected error: {str(e)}"}
    return wrapper

##############################################################################
# 2. Environment and Logging Setup
##############################################################################
def get_env_or_raise(key: str, default: Optional[str] = None) -> str:
    value = os.getenv(key, default)
    if value is None:
        raise ValueError(f"Required environment variable {key} is not set")
    return value

# Load environment variables or raise if missing
OANDA_API_TOKEN = get_env_or_raise('OANDA_API_TOKEN')
OANDA_ACCOUNT_ID = get_env_or_raise('OANDA_ACCOUNT_ID')
OANDA_API_URL = get_env_or_raise('OANDA_API_URL', 'https://api-fxtrade.oanda.com/v3')

# Allow multiple domains (comma-separated)
ALLOWED_ORIGINS = get_env_or_raise(
    "ALLOWED_ORIGINS", 
    "https://your-tradingview-domain.com"
).split(",")

def setup_logging():
    """
    Sets up rotating file logging plus console logging.
    """
    try:
        log_dir = '/opt/render/project/src/logs'
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'trading_bot.log')
    except Exception as e:
        print(f"Failed to create log directory: {e}")
        log_file = 'trading_bot.log'

    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - [%(levelname)s] - %(message)s'
    )

    # File handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(log_format)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)

    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Additional logger for "trading_bot" if needed
    logger_ = logging.getLogger('trading_bot')
    logger_.setLevel(logging.INFO)
    
    return logger_

logger = setup_logging()

##############################################################################
# 3. HTTP Session Management
##############################################################################
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 30
TOTAL_TIMEOUT = 45
HTTP_REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=TOTAL_TIMEOUT,
    connect=CONNECT_TIMEOUT,
    sock_read=READ_TIMEOUT
)
MAX_SIMULTANEOUS_CONNECTIONS = 100

session: Optional[aiohttp.ClientSession] = None

@handle_async_errors
async def get_session(force_new: bool = False) -> aiohttp.ClientSession:
    """
    Returns a global aiohttp ClientSession.
    Creates one if it doesn't exist or force_new is True.
    """
    global session
    if session is None or session.closed or force_new:
        if session and not session.closed:
            await session.close()
        
        connector = aiohttp.TCPConnector(
            limit=MAX_SIMULTANEOUS_CONNECTIONS,
            enable_cleanup_closed=True,
            force_close=False,
            keepalive_timeout=65
        )
        session = aiohttp.ClientSession(
            connector=connector,
            timeout=HTTP_REQUEST_TIMEOUT,
            headers={
                "Authorization": f"Bearer {OANDA_API_TOKEN}",
                "Content-Type": "application/json"
            }
        )
    return session

async def ensure_session() -> Tuple[bool, Optional[str]]:
    """
    Ensures that the global session is available and not closed.
    Returns (True, None) if the session is ready, otherwise (False, errorMessage).
    """
    try:
        if session is None or session.closed:
            await get_session(force_new=True)
            logger.info("Created new HTTP session")
        return True, None
    except Exception as e:
        error_msg = f"Failed to create HTTP session: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

##############################################################################
# 4. FastAPI App Creation (with Lifespan and CORS)
##############################################################################
app = FastAPI(
    title="OANDA Trading Bot",
    description="Advanced async trading bot using FastAPI and aiohttp",
    version="1.1.0"
)

# Use an async lifespan to manage startup/shutdown of global services
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application, initializing services...")
    await get_session(force_new=True)
    yield
    logger.info("Shutting down application...")
    if session and not session.closed:
        await session.close()
        logger.info("Closed HTTP session")

app.router.lifespan_context = lifespan

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

##############################################################################
# 5. Logging Middleware
##############################################################################
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """
    Logs each incoming request and the matching response status.
    """
    body = b""
    try:
        body = await request.body()
        logger.info(f"Request to {request.url.path}")
        logger.info(f"Request body: {body.decode()}")

        # Reconstruct request stream for downstream
        async def receive():
            return {"type": "http.request", "body": body}
        request._receive = receive

        response = await call_next(request)
        logger.info(f"Response status: {response.status_code}")
        return response
    except Exception as e:
        logger.error(f"Request failed: {str(e)}")
        logger.error(f"Request body was: {body.decode() if body else 'Empty'}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}"}
        )

##############################################################################
# 1. General Constants and Instrument Configurations
##############################################################################
SPREAD_THRESHOLD_FOREX = 0.001
SPREAD_THRESHOLD_CRYPTO = 0.008
MAX_RETRIES = 3
BASE_DELAY = 1.0
BASE_POSITION = 100000

# Default precision for instruments
DEFAULT_FOREX_PRECISION = 5
DEFAULT_CRYPTO_PRECISION = 2
DEFAULT_MIN_ORDER_SIZE = 1000

# Instrument leverages
INSTRUMENT_LEVERAGES = {
    "USD_CHF": 20,
    "EUR_USD": 20,
    "GBP_USD": 20,
    "USD_JPY": 20,
    "AUD_USD": 20,
    "USD_THB": 20,
    "CAD_CHF": 20,
    "NZD_USD": 20,
    "BTC_USD": 2,
    "ETH_USD": 2,
    "XRP_USD": 2,
    "LTC_USD": 2
}

# Instrument precision
INSTRUMENT_PRECISION = {
    "EUR_USD": 5,
    "GBP_USD": 5,       # Adjust if needed
    "USD_JPY": 3,
    "AUD_USD": 5,
    "USD_THB": 5,
    "CAD_CHF": 5,
    "NZD_USD": 5,
    "BTC_USD": 2,
    "ETH_USD": 2,
    "XRP_USD": 4,
    "LTC_USD": 2
}

# Instrument minimum order sizes
MIN_ORDER_SIZES = {
    "EUR_USD": 1000,
    "GBP_USD": 1000,
    "AUD_USD": 1000,
    "USD_THB": 1000,
    "CAD_CHF": 1000,
    "NZD_USD": 1000,
    "BTC_USD": 0.25,
    "ETH_USD": 4,
    "XRP_USD": 200,
    "LTC_USD": 1
}

INSTRUMENT_LEVERAGES["XAU_USD"] = 1
MIN_ORDER_SIZES["XAU_USD"] = 1         
MAX_ORDER_SIZES = {
    "XAU_USD": 10000,
}

TIMEFRAME_PATTERN = re.compile(r'^(\d+)([mMhH])$')

##############################################################################
# 2. Pydantic Model for Alerts
##############################################################################
class AlertData(BaseModel):
    symbol: str
    action: str
    timeframe: Optional[str] = "1M"
    orderType: Optional[str] = "MARKET"
    timeInForce: Optional[str] = "FOK"
    percentage: Optional[float] = 1.0
    account: Optional[str] = None
    id: Optional[str] = None
    comment: Optional[str] = None

    @validator('timeframe')
    def validate_timeframe(cls, v):
        """
        Validates timeframe in either '15M' / '1H' format or numeric minute
        format. Maps certain numeric inputs (like 5 -> '5M', 12 -> '12H', etc.).
        """
        if v.isdigit():
            mapping = {1: "1H", 4: "4H", 12: "12H", 5: "5M", 15: "15M", 30: "30M"}
            try:
                num = int(v)
                v = mapping.get(num, f"{v}M")
            except ValueError as e:
                raise ValueError("Invalid timeframe value") from e
        
        match = TIMEFRAME_PATTERN.match(v)
        if not match:
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
        raise ValueError("Invalid timeframe unit. Use M or H")

    @validator('action')
    def validate_action(cls, v):
        """
        Ensures action is one of the recognized trade directives.
        """
        valid_actions = ['BUY', 'SELL', 'CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']
        v = v.upper()
        if v not in valid_actions:
            raise ValueError(f"Action must be one of {valid_actions}")
        return v

    @validator('percentage')
    def validate_percentage(cls, v):
        """
        Percentage must be between 0 and 1 (exclusive of 0, inclusive of 1).
        """
        if not isinstance(v, (int, float)):
            raise ValueError("Percentage must be a number")
        if not 0 < v <= 1:
            raise ValueError("Percentage must be between 0 and 1")
        return float(v)

    @validator('symbol')
    def validate_symbol(cls, v):
        """
        Symbol must map to a valid OANDA instrument in the format XXX_YYY.
        e.g. 'GBPUSD' -> 'GBP_USD'
        """
        if len(v) < 6:
            raise ValueError("Symbol must be at least 6 characters")
        instrument = f"{v[:3]}_{v[3:]}"
        if instrument not in INSTRUMENT_LEVERAGES:
            raise ValueError(f"Invalid trading instrument: {instrument}")
        return v.upper()

    @validator('timeInForce')
    def validate_time_in_force(cls, v):
        valid_values = ['FOK', 'IOC', 'GTC', 'GFD']
        v = v.upper() if v else 'FOK'
        if v not in valid_values:
            raise ValueError(f"timeInForce must be one of {valid_values}")
        return v

    @validator('orderType')
    def validate_order_type(cls, v):
        valid_types = ['MARKET', 'LIMIT', 'STOP', 'MARKET_IF_TOUCHED']
        v = v.upper() if v else 'MARKET'
        if v not in valid_types:
            raise ValueError(f"orderType must be one of {valid_types}")
        return v

    class Config:
        anystr_strip_whitespace = True

##############################################################################
# 1. PositionTracker for Holding and Closing Logic
##############################################################################
class PositionTracker:
    """
    Tracks currently open positions by symbol, including how many bars they
    have been held according to the timeframe.
    """
    def __init__(self):
        self.positions: Dict[str, Dict[str, Any]] = {}
        self.bar_times: Dict[str, List[datetime]] = {}
        self._lock = asyncio.Lock()
        
    async def record_position(self, symbol: str, action: str, timeframe: str):
        """
        Records a new position in the tracker when a trade is opened.
        """
        async with self._lock:
            current_time = datetime.now(timezone('Asia/Bangkok'))
            self.positions[symbol] = {
                'entry_time': current_time,
                'position_type': 'LONG' if action.upper() == 'BUY' else 'SHORT',
                'bars_held': 0,
                'timeframe': timeframe,
                'last_update': current_time
            }
            self.bar_times.setdefault(symbol, []).append(current_time)
            logger.info(f"Recorded position for {symbol}: {self.positions[symbol]}")
        
    async def update_bars_held(self, symbol: str) -> int:
        """
        Recalculates how many timeframe bars the position has been held
        based on the original entry time and current time.
        """
        async with self._lock:
            if symbol not in self.positions:
                return 0
            position = self.positions[symbol]
            entry_time = position['entry_time']
            timeframe = int(position['timeframe'])  # timeframe in minutes
            current_time = datetime.now(timezone('Asia/Bangkok'))

            # Number of full bars passed
            bars = (current_time - entry_time).seconds // (timeframe * 60)
            position['bars_held'] = bars
            position['last_update'] = current_time
            return bars

    async def should_close_position(self, symbol: str, new_signal: str = None) -> bool:
        """
        Determines if the existing position should be closed due to:
          - Reaching 4 bars held
          - Opposing new signal when bars_held < 4
        """
        async with self._lock:
            if symbol not in self.positions:
                return False
            
            bars_held = await self.update_bars_held(symbol)
            position = self.positions[symbol]
            
            # If we've held for >= 4 bars, close
            if bars_held >= 4:
                return True

            # If < 4 bars but we get an opposing signal, close
            if 0 < bars_held < 4 and new_signal:
                is_opposing = (
                    (position['position_type'] == 'LONG' and new_signal.upper() == 'SELL') or
                    (position['position_type'] == 'SHORT' and new_signal.upper() == 'BUY')
                )
                if is_opposing:
                    return True
            return False

    async def get_close_action(self, symbol: str) -> str:
        """
        Returns either CLOSE_LONG or CLOSE_SHORT depending on the position type.
        """
        async with self._lock:
            if symbol not in self.positions:
                return 'CLOSE'  # fallback
            pos_type = self.positions[symbol]['position_type']
            if pos_type == 'LONG':
                return 'CLOSE_LONG'
            else:
                return 'CLOSE_SHORT'

    async def clear_position(self, symbol: str):
        """
        Removes a position from the tracker.
        """
        async with self._lock:
            self.positions.pop(symbol, None)
            self.bar_times.pop(symbol, None)
            logger.info(f"Cleared position tracking for {symbol}")

##############################################################################
# 2. Market Time Helpers
##############################################################################
def is_market_open() -> Tuple[bool, str]:
    """
    Basic check for weekend closure (Forex).
    Adjust if you trade cryptos 24/7 or need more advanced checks.
    """
    current_time = datetime.now(timezone('Asia/Bangkok'))
    wday = current_time.weekday()
    hour = current_time.hour

    # Market often closes late Friday and reopens early Monday (UTC-based)
    # Example: OANDA stops trading around Friday 5pm NY time,
    # reopens Sunday. Adjust to local times as needed.
    if (wday == 5 and hour >= 5) or (wday == 6) or (wday == 0 and hour < 5):
        return False, "Weekend market closure"
    return True, "Market open"

def calculate_next_market_open() -> datetime:
    """
    Returns the next approximate market open time. 
    This is just an example placeholder.
    """
    current = datetime.now(timezone('Asia/Bangkok'))
    if current.weekday() == 5:
        # Saturday
        days_to_add = 1
    elif current.weekday() == 6 and current.hour < 4:
        # Sunday morning
        days_to_add = 0
    else:
        # Next Monday
        days_to_add = 7 - current.weekday()
    next_open = current + timedelta(days=days_to_add)
    return next_open.replace(hour=4, minute=0, second=0, microsecond=0)

async def check_market_status(instrument: str, account_id: str) -> Tuple[bool, str]:
    """
    For now, this simply calls is_market_open().
    You could expand with per-instrument checks, holiday closures, etc.
    """
    market_open, msg = is_market_open()
    return market_open, msg

##############################################################################
# 3. Price and Position Fetchers
##############################################################################
@handle_async_errors
async def get_instrument_price(instrument: str, account_id: str) -> Tuple[bool, Dict[str, Any]]:
    """
    Fetches the current bid/ask price for the given instrument.
    """
    session_ok, error = await ensure_session()
    if not session_ok:
        return False, {"error": error}

    url = f"{OANDA_API_URL.rstrip('/')}/accounts/{account_id}/pricing?instruments={instrument}"
    logger.info(f"Fetching price from OANDA: {url}")

    for attempt in range(MAX_RETRIES):
        async with session.get(url) as response:
            if response.status != 200:
                error_msg = f"OANDA API error (price fetch): {response.status}"
                error_content = await response.text()
                logger.error(f"{error_msg} - Response: {error_content}")
                if attempt < MAX_RETRIES - 1:
                    delay = BASE_DELAY * (2 ** attempt)
                    logger.warning(f"Retrying in {delay}s (attempt {attempt+1}/{MAX_RETRIES})")
                    await asyncio.sleep(delay)
                    await get_session(force_new=True)
                    continue
                return False, {"error": error_msg, "response": error_content}
            
            pricing_data = await response.json()
            if not pricing_data.get('prices'):
                err_msg = f"No pricing data returned for {instrument}"
                logger.warning(err_msg)
                return False, {"error": err_msg}
            return True, pricing_data
    
    return False, {"error": "Failed to fetch price after retries"}

@handle_async_errors
async def get_open_positions(account_id: str) -> Tuple[bool, Dict[str, Any]]:
    """
    Fetches currently open positions from OANDA for the given account.
    """
    session_ok, error = await ensure_session()
    if not session_ok:
        return False, {"error": error}

    url = f"{OANDA_API_URL.rstrip('/')}/accounts/{account_id}/openPositions"
    async with session.get(url) as response:
        if response.status != 200:
            error_msg = f"Failed to fetch open positions: {response.status}"
            error_content = await response.text()
            logger.error(f"{error_msg} - Response: {error_content}")
            return False, {"error": error_msg}
        positions = await response.json()
        return True, positions

##############################################################################
# 4. Spread Check and Leverage Utility
##############################################################################
def check_spread_warning(pricing_data: Dict[str, Any], instrument: str) -> Tuple[bool, float]:
    """
    Checks if the spread is too large (above configured thresholds).
    """
    if not pricing_data.get('prices'):
        logger.warning(f"No pricing data for spread check: {instrument}")
        return False, 0
    
    price = pricing_data['prices'][0]
    try:
        bid = float(price['bids'][0]['price'])
        ask = float(price['asks'][0]['price'])
    except (KeyError, IndexError, ValueError) as e:
        logger.error(f"Error parsing bid/ask for {instrument}: {str(e)}")
        return False, 0
    
    spread = ask - bid
    spread_percentage = spread / bid
    is_crypto = any(crypto in instrument for crypto in ['BTC', 'ETH', 'XRP', 'LTC'])
    threshold = SPREAD_THRESHOLD_CRYPTO if is_crypto else SPREAD_THRESHOLD_FOREX

    if spread_percentage > threshold:
        logger.warning(f"Wide spread detected for {instrument}: {spread:.5f} ({spread_percentage*100:.2f}%)")
        return True, spread

    return False, 0

def get_instrument_leverage(instrument: str) -> int:
    """
    Retrieves the configured leverage for a given instrument, defaults to 1 if not found.
    """
    return INSTRUMENT_LEVERAGES.get(instrument, 1)

##############################################################################
# 5. Helper to Convert TradingView -> AlertData
##############################################################################
def translate_tradingview_signal(alert_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Translates common TradingView fields to the structure expected by our bot.
    For example, if the input uses 'ticker' instead of 'symbol', or 'interval'
    instead of 'timeframe', etc.
    """
    if 'ticker' in alert_data and not alert_data.get('symbol'):
        alert_data['symbol'] = alert_data.pop('ticker')
    if 'interval' in alert_data and not alert_data.get('timeframe'):
        alert_data['timeframe'] = alert_data.pop('interval')
    alert_data.pop('exchange', None)
    alert_data.pop('strategy', None)
    return alert_data

##############################################################################
# 1. Validation of Trade Direction
##############################################################################
@handle_async_errors
async def validate_trade_direction(alert_data: Dict[str, Any]) -> Tuple[bool, Optional[str], bool]:
    """
    Checks for any conflict with existing positions. For example, if a LONG
    position already exists but we get another BUY signal, disallow it.
    Return:
      (True/False, error_message, is_closing_trade)
    """
    action = alert_data['action']
    if action in ['CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']:
        # It's already a close action
        return True, None, True

    success, positions_data = await get_open_positions(alert_data.get('account', OANDA_ACCOUNT_ID))
    if not success:
        return False, "Failed to fetch positions", False

    instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
    for position in positions_data.get('positions', []):
        if position['instrument'] == instrument:
            long_units = float(position['long'].get('units', '0') or '0')
            short_units = float(position['short'].get('units', '0') or '0')
            # If we have a long position but are trying to BUY again
            if long_units > 0 and action == 'BUY':
                return False, f"Existing LONG position for {instrument}", False
            # If we have a short position but are trying to SELL again
            if short_units < 0 and action == 'SELL':
                return False, f"Existing SHORT position for {instrument}", False
    return True, None, False

##############################################################################
# 2. Trade Execution
##############################################################################
@handle_async_errors
async def execute_trade(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """
    Executes a new trade (BUY/SELL) based on the alert data.
    """
    required_fields = ['symbol', 'action', 'orderType', 'timeInForce', 'percentage']
    missing_fields = [f for f in required_fields if f not in alert_data]
    if missing_fields:
        error_msg = f"Missing required fields: {missing_fields}"
        logger.error(error_msg)
        return False, {"error": error_msg}

    instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
    leverage = get_instrument_leverage(instrument)
    is_crypto = any(crypto in instrument for crypto in ['BTC', 'ETH', 'XRP', 'LTC'])
    precision = INSTRUMENT_PRECISION.get(instrument, DEFAULT_CRYPTO_PRECISION if is_crypto else DEFAULT_FOREX_PRECISION)
    min_size = MIN_ORDER_SIZES.get(instrument, DEFAULT_MIN_ORDER_SIZE)

    try:
        percentage = float(alert_data['percentage'])
        if not 0 < percentage <= 1:
            error_msg = f"Percentage must be 0 < p <= 1. Received: {percentage}"
            logger.error(error_msg)
            return False, {"error": error_msg}
        
        trade_size = BASE_POSITION * percentage * leverage
        if trade_size <= 0:
            error_msg = f"Calculated trade size <= 0: {trade_size}"
            logger.error(error_msg)
            return False, {"error": error_msg}

        raw_units = trade_size
    except ValueError as e:
        error_msg = f"Invalid percentage value: {str(e)}"
        logger.error(error_msg)
        return False, {"error": error_msg}

    session_ok, error = await ensure_session()
    if not session_ok:
        return False, {"error": error}
    
    # Get price to determine buy vs sell side
    price_success, price_data = await get_instrument_price(instrument, alert_data.get('account', OANDA_ACCOUNT_ID))
    if not price_success:
        return False, price_data
    
    is_sell = alert_data['action'] == 'SELL'
    try:
        if is_sell:
            price = float(price_data['prices'][0]['bids'][0]['price'])
        else:
            price = float(price_data['prices'][0]['asks'][0]['price'])
    except (KeyError, IndexError, ValueError) as e:
        error_msg = f"Error parsing price data: {str(e)}"
        logger.error(error_msg)
        return False, {"error": error_msg}

    # Round or cast units according to instrument type
    if is_crypto:
        units = round(raw_units, precision)
    else:
        units = int(round(raw_units))

    # Adjust for min size, handle SELL negativity
    if abs(units) < min_size:
        logger.warning(f"Order size {abs(units)} below minimum {min_size} for {instrument}")
        units = min_size if not is_sell else -min_size
    elif is_sell:
        units = -abs(units)

    # *** Maximum order size check: adjust if units exceed the maximum allowed ***
    max_units = MAX_ORDER_SIZES.get(instrument, None)
    if max_units and abs(units) > max_units:
        logger.warning(f"Calculated order size {abs(units)} exceeds max allowed {max_units} for {instrument}. Adjusting order size.")
        units = max_units if not is_sell else -max_units

    units_str = str(units)

    order_data = {
        "order": {
            "type": alert_data['orderType'],
            "instrument": instrument,
            "units": units_str,
            "timeInForce": alert_data['timeInForce'],
            "positionFill": "DEFAULT"
        }
    }

    logger.info(f"Trade details for {instrument}: "
                f"{'SELL' if is_sell else 'BUY'}, Price={price:.5f}, "
                f"Units={units_str}, Size=${trade_size:.2f}")

    url = f"{OANDA_API_URL.rstrip('/')}/accounts/{alert_data['account']}/orders"

    for attempt in range(MAX_RETRIES):
        async with session.post(url, json=order_data) as response:
            if response.status != 201:
                error_msg = f"OANDA API error (trade execution): {response.status}"
                error_content = await response.text()
                logger.error(f"{error_msg} - Response: {error_content}")
                
                # If margin is insufficient, log clearly
                if "INSUFFICIENT_MARGIN" in error_content:
                    logger.error("Trade failed due to insufficient margin.")
                
                if attempt < MAX_RETRIES - 1:
                    delay = BASE_DELAY * (2 ** attempt)
                    logger.warning(f"Retrying trade in {delay}s (attempt {attempt+1}/{MAX_RETRIES})")
                    await asyncio.sleep(delay)
                    await get_session(force_new=True)
                    continue
                
                return False, {
                    "error": error_msg,
                    "response": error_content,
                    "order_data": order_data
                }
            
            order_response = await response.json()
            logger.info(f"Trade executed successfully: {order_response}")
            return True, order_response

    # If we somehow exit the loop without returning:
    return False, {"error": "Failed to execute trade after maximum retries"}

##############################################################################
# 3. Position Closing Logic
##############################################################################
@handle_async_errors
async def close_position(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """
    Closes a position for the given instrument, either LONG, SHORT, or any.
    """
    account_id = alert_data.get('account', OANDA_ACCOUNT_ID)
    instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
    
    success, positions_data = await get_open_positions(account_id)
    if not success:
        return False, positions_data  # error info
    
    position = next(
        (p for p in positions_data.get('positions', []) if p['instrument'] == instrument),
        None
    )
    if not position:
        # If there's no position to close, consider it "closed" successfully.
        msg = f"No existing position for {instrument}. Nothing to close."
        logger.info(msg)
        return True, {"message": msg}

    # Determine whether to close LONG, SHORT, or both
    long_units = float(position.get('long', {}).get('units', '0') or '0')
    short_units = float(position.get('short', {}).get('units', '0') or '0')
    action = alert_data['action'].upper()

    close_body = {}
    if action == 'CLOSE_LONG' and long_units > 0:
        close_body = {"longUnits": "ALL"}
    elif action == 'CLOSE_SHORT' and short_units < 0:
        close_body = {"shortUnits": "ALL"}
    elif action == 'CLOSE':
        # Close whichever side is open
        if long_units > 0:
            close_body["longUnits"] = "ALL"
        if short_units < 0:
            close_body["shortUnits"] = "ALL"

    if not close_body:
        msg = f"Position exists for {instrument}, but no matching side to close for action={action}"
        logger.info(msg)
        return True, {"message": msg}  # Return True to not block subsequent trades

    url = f"{OANDA_API_URL.rstrip('/')}/accounts/{account_id}/positions/{instrument}/close"
    logger.info(f"Attempting to close {instrument} with body: {close_body}")

    async with session.put(url, json=close_body) as response:
        if response.status != 200:
            error_content = await response.text()
            logger.error(f"Failed to close position: {error_content}")
            return False, {"error": f"Close position failed: {error_content}"}
        
        close_response = await response.json()
        logger.info(f"Position closed successfully: {close_response}")
        return True, close_response

##############################################################################
# 4. AlertHandler for Orchestrating Trades
##############################################################################
class AlertHandler:
    def __init__(self, max_retries: int = MAX_RETRIES, base_delay: float = BASE_DELAY):
        self.logger = logging.getLogger('alert_handler')
        self.max_retries = max_retries
        self.base_delay = base_delay
        self._trade_lock = asyncio.Lock()
        self.position_tracker = PositionTracker()

    async def process_alert(self, alert_data: Dict[str, Any]) -> bool:
        """
        Process incoming alerts:
          1. Possibly close existing position if signaled or if we want to
             open a new one that conflicts.
          2. Open new trade if valid and the market is open.
        """
        if not alert_data:
            self.logger.error("No alert data provided")
            return False

        alert_id = alert_data.get('id', str(uuid.uuid4()))
        action = alert_data.get('action', '').upper()
        symbol = alert_data['symbol']

        self.logger.info(f"Processing alert {alert_id} with action={action} for symbol={symbol}")

        async with self._trade_lock:
            # Check if an existing position should be closed first
            if await self.position_tracker.should_close_position(symbol, new_signal=action):
                close_action = await self.position_tracker.get_close_action(symbol)
                close_alert = {**alert_data, 'action': close_action}
                closed_ok, _ = await close_position(close_alert)
                if closed_ok:
                    await self.position_tracker.clear_position(symbol)
                else:
                    # If we failed to close, do not proceed with opening a new trade
                    return False

            # If the alert is purely to CLOSE, skip opening a new trade
            if action in ['CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']:
                # Already handled above, so we are done
                self.logger.info(f"Alert {alert_id} was a close-only signal. Done.")
                return True

            # Market hours check
            instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
            is_tradeable, status_msg = await check_market_status(instrument, alert_data.get('account', OANDA_ACCOUNT_ID))
            if not is_tradeable:
                self.logger.warning(f"Market closed for {instrument}: {status_msg}")
                return False

            # Validate trade direction (no duplicative long/short)
            is_valid, err_msg, is_closing_trade = await validate_trade_direction(alert_data)
            if not is_valid:
                self.logger.warning(f"Trade validation failed for {alert_id}: {err_msg}")
                return False

            # Since we are opening a new trade, attempt to execute
            for attempt in range(self.max_retries):
                trade_ok, trade_result = await execute_trade(alert_data)
                if trade_ok:
                    # Record the new position in tracker
                    await self.position_tracker.record_position(symbol, action, alert_data['timeframe'])
                    self.logger.info(f"Alert {alert_id} processed successfully.")
                    return True
                else:
                    if attempt < self.max_retries - 1:
                        delay = self.base_delay * (2 ** attempt)
                        err_message = trade_result.get('error', 'Unknown error')
                        self.logger.warning(
                            f"Alert {alert_id} trade failed; retrying in {delay}s: {err_message}"
                        )
                        await asyncio.sleep(delay)
                    else:
                        self.logger.error(f"Alert {alert_id} final attempt failed: {trade_result}")
                        return False

            self.logger.info(f"Alert {alert_id} discarded after {self.max_retries} attempts.")
            return False

# Instantiate a global alert handler
alert_handler = AlertHandler()

##############################################################################
# 1. Alert and TradingView Endpoints
##############################################################################

async def process_incoming_alert(data: Dict[str, Any], source: str = "direct") -> JSONResponse:
    """
    Shared helper function to process incoming alerts from any endpoint.
    Args:
        data: The alert data dictionary
        source: Source of the alert ("direct" or "tradingview")
    """
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Processing {source} alert: {json.dumps(data, indent=2)}")

    try:
        # Ensure account is set
        if not data.get('account'):
            data['account'] = OANDA_ACCOUNT_ID

        # If from TradingView, normalize the data
        if source == "tradingview":
            data = translate_tradingview_signal(data)
            logger.info(f"[{request_id}] Normalized TradingView data: {json.dumps(data, indent=2)}")

        # Validate using AlertData model
        try:
            validated_data = AlertData(**data)
            logger.info(f"[{request_id}] Data validation successful")
        except ValidationError as e:
            logger.error(f"[{request_id}] Validation error: {e.errors()}")
            return JSONResponse(
                status_code=400,
                content={
                    "error": f"Validation error: {e.errors()}",
                    "request_id": request_id
                }
            )

        # Process the alert
        success = await alert_handler.process_alert(validated_data.dict())
        if success:
            logger.info(f"[{request_id}] Alert processed successfully")
            return JSONResponse(
                status_code=200,
                content={
                    "message": "Alert processed successfully",
                    "request_id": request_id
                }
            )
        else:
            logger.error(f"[{request_id}] Alert processing failed")
            return JSONResponse(
                status_code=400,
                content={
                    "error": "Failed to process alert",
                    "request_id": request_id
                }
            )

    except Exception as e:
        logger.exception(f"[{request_id}] Unexpected error: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "error": f"Unexpected error: {str(e)}",
                "request_id": request_id
            }
        )

@app.post("/alerts")
async def handle_alert_endpoint(alert_data: AlertData):
    """
    Endpoint for direct AlertData submissions
    """
    return await process_incoming_alert(alert_data.dict(), source="direct")

@app.post("/tradingview")
async def handle_tradingview_webhook(request: Request):
    """
    Endpoint for TradingView webhooks that may send data in a slightly different format
    """
    try:
        # Get the raw JSON data
        body = await request.json()
        logger.info(f"Received TradingView webhook: {json.dumps(body, indent=2)}")
        
        # Ensure all required fields are present
        required_fields = ['symbol', 'action']
        missing_fields = [field for field in required_fields if not body.get(field)]
        if missing_fields:
            return JSONResponse(
                status_code=400,
                content={"error": f"Missing required fields: {missing_fields}"}
            )

        # Process using shared helper
        return await process_incoming_alert(body, source="tradingview")

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in TradingView webhook: {str(e)}")
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid JSON format"}
        )
    except Exception as e:
        logger.exception(f"Unexpected error in TradingView webhook: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Unexpected error: {str(e)}"}
        )

##############################################################################
# 2. Health Check Endpoint
##############################################################################
@app.api_route("/", methods=["GET", "HEAD"])
async def health_check():
    return {"status": "active", "timestamp": datetime.utcnow().isoformat()}


##############################################################################
# 3. Exception Handlers & Shutdown
##############################################################################
@app.exception_handler(404)
async def not_found_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=404,
        content={"message": f"Endpoint {request.url.path} not found"}
    )

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutdown initiated.")
    if session and not session.closed:
        await session.close()
        logger.info("HTTP session closed.")

##############################################################################
# 4. uvicorn Main for Local Launch
##############################################################################
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))  # Fallback port 8000
    uvicorn.run(
        "main:app",   # <-- Replace "main" with your filename if needed
        host="0.0.0.0",
        port=port,
        reload=False,
        workers=1,
        timeout_keep_alive=65,
        log_config=None
    )
