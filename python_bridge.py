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

OANDA_API_TOKEN = get_env_or_raise('OANDA_API_TOKEN')
OANDA_ACCOUNT_ID = get_env_or_raise('OANDA_ACCOUNT_ID')
OANDA_API_URL = get_env_or_raise('OANDA_API_URL', 'https://api-fxtrade.oanda.com/v3')

ALLOWED_ORIGINS = get_env_or_raise("ALLOWED_ORIGINS", "https://your-tradingview-domain.com").split(",")

def setup_logging():
    try:
        log_dir = '/opt/render/project/src/logs'
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'trading_bot.log')
    except Exception as e:
        print(f"Failed to create log directory: {e}")
        log_file = 'trading_bot.log'

    log_format = logging.Formatter('%(asctime)s - %(name)s - [%(levelname)s] - %(message)s')
    file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    file_handler.setFormatter(log_format)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
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
HTTP_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=TOTAL_TIMEOUT, connect=CONNECT_TIMEOUT, sock_read=READ_TIMEOUT)
MAX_SIMULTANEOUS_CONNECTIONS = 100

session: Optional[aiohttp.ClientSession] = None

@handle_async_errors
async def get_session(force_new: bool = False) -> aiohttp.ClientSession:
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
app = FastAPI(title="OANDA Trading Bot", description="Advanced async trading bot", version="1.1.0")

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

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    body = b""
    try:
        body = await request.body()
        logger.info(f"Request to {request.url.path}")
        logger.info(f"Request body: {body.decode()}")
        async def receive(): return {"type": "http.request", "body": body}
        request._receive = receive
        response = await call_next(request)
        logger.info(f"Response status: {response.status_code}")
        return response
    except Exception as e:
        logger.error(f"Request failed: {str(e)}")
        logger.error(f"Request body was: {body.decode() if body else 'Empty'}")
        return JSONResponse(status_code=500, content={"error": f"Internal server error: {str(e)}"})

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

INSTRUMENT_LEVERAGES = {
    "USD_CHF": 20, "EUR_USD": 20, "GBP_USD": 20, "USD_JPY": 20,
    "AUD_USD": 20, "USD_THB": 20, "CAD_CHF": 20, "NZD_USD": 20,
    "BTC_USD": 2, "ETH_USD": 2, "XRP_USD": 2, "LTC_USD": 2,
    "XAU_USD": 1
}

INSTRUMENT_PRECISION = {
    "EUR_USD": 5, "GBP_USD": 5, "USD_JPY": 3, "AUD_USD": 5,
    "USD_THB": 5, "CAD_CHF": 5, "NZD_USD": 5, "BTC_USD": 2,
    "ETH_USD": 2, "XRP_USD": 4, "LTC_USD": 2, "XAU_USD": 2
}

MIN_ORDER_SIZES = {
    "EUR_USD": 1000, "GBP_USD": 1000, "AUD_USD": 1000, "USD_THB": 1000,
    "CAD_CHF": 1000, "NZD_USD": 1000, "BTC_USD": 0.25, "ETH_USD": 4,
    "XRP_USD": 200, "LTC_USD": 1, "XAU_USD": 1
}

MAX_ORDER_SIZES = {
    "XAU_USD": 10000,
    "BTC_USD": 1000,  # Verified broker limit
    "ETH_USD": 500,
    "GBP_USD": 500000,
    "EUR_USD": 500000,
    "AUD_USD": 500000
}

# Base position sizes
FOREX_BASE_POSITION = 100000  # Standard forex lot
CRYPTO_BASE_POSITION = 1000   # Smaller base for crypto

TIMEFRAME_PATTERN = re.compile(r'^(\d+)([mMhH])$')

##############################################################################
# 2. Session Management
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
# Pydantic Model for Alerts
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
        valid_actions = ['BUY', 'SELL', 'CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']
        v = v.upper()
        if v not in valid_actions:
            raise ValueError(f"Action must be one of {valid_actions}")
        return v

    @validator('percentage')
    def validate_percentage(cls, v):
        if not isinstance(v, (int, float)):
            raise ValueError("Percentage must be a number")
        if not 0 < v <= 1:
            raise ValueError("Percentage must be between 0 and 1")
        return float(v)

    @validator('symbol')
    def validate_symbol(cls, v):
        if len(v) < 6:
            raise ValueError("Symbol must be at least 6 characters")
        instrument = f"{v[:3]}_{v[3:]}".upper()
        if instrument not in INSTRUMENT_LEVERAGES:
            raise ValueError(f"Invalid instrument: {instrument}")
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
# Position Tracker
##############################################################################
class PositionTracker:
    def __init__(self):
        self.positions: Dict[str, Dict[str, Any]] = {}
        self.bar_times: Dict[str, List[datetime]] = {}
        self._lock = asyncio.Lock()
        
    async def record_position(self, symbol: str, action: str, timeframe: str):
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
        async with self._lock:
            if symbol not in self.positions:
                return 0
            position = self.positions[symbol]
            entry_time = position['entry_time']
            timeframe = int(position['timeframe'])
            current_time = datetime.now(timezone('Asia/Bangkok'))
            bars = (current_time - entry_time).seconds // (timeframe * 60)
            position['bars_held'] = bars
            position['last_update'] = current_time
            return bars

    async def should_close_position(self, symbol: str, new_signal: str = None) -> bool:
        async with self._lock:
            if symbol not in self.positions:
                return False
            
            bars_held = await self.update_bars_held(symbol)
            position = self.positions[symbol]
            
            if bars_held >= 4:
                return True

            if 0 < bars_held < 4 and new_signal:
                is_opposing = (
                    (position['position_type'] == 'LONG' and new_signal.upper() == 'SELL') or
                    (position['position_type'] == 'SHORT' and new_signal.upper() == 'BUY')
                )
                if is_opposing:
                    return True
            return False

    async def get_close_action(self, symbol: str) -> str:
        async with self._lock:
            if symbol not in self.positions:
                return 'CLOSE'
            pos_type = self.positions[symbol]['position_type']
            return 'CLOSE_LONG' if pos_type == 'LONG' else 'CLOSE_SHORT'

    async def clear_position(self, symbol: str):
        async with self._lock:
            self.positions.pop(symbol, None)
            self.bar_times.pop(symbol, None)
            logger.info(f"Cleared position tracking for {symbol}")

##############################################################################
# Market Time Helpers
##############################################################################
def is_market_open() -> Tuple[bool, str]:
    current_time = datetime.now(timezone('Asia/Bangkok'))
    wday = current_time.weekday()
    hour = current_time.hour

    if (wday == 5 and hour >= 5) or (wday == 6) or (wday == 0 and hour < 5):
        return False, "Weekend market closure"
    return True, "Market open"

def calculate_next_market_open() -> datetime:
    current = datetime.now(timezone('Asia/Bangkok'))
    if current.weekday() == 5:
        days_to_add = 1
    elif current.weekday() == 6 and current.hour < 4:
        days_to_add = 0
    else:
        days_to_add = 7 - current.weekday()
    next_open = current + timedelta(days=days_to_add)
    return next_open.replace(hour=4, minute=0, second=0, microsecond=0)

async def check_market_status(instrument: str, account_id: str) -> Tuple[bool, str]:
    market_open, msg = is_market_open()
    return market_open, msg

##############################################################################
# 3. Price and Position Fetchers
##############################################################################
@handle_async_errors
async def get_instrument_price(instrument: str, account_id: str) -> Tuple[bool, Dict[str, Any]]:
    session_ok, error = await ensure_session()
    if not session_ok: return False, {"error": error}
    url = f"{OANDA_API_URL.rstrip('/')}/accounts/{account_id}/pricing?instruments={instrument}"
    logger.info(f"Fetching price from OANDA: {url}")
    for attempt in range(MAX_RETRIES):
        async with session.get(url) as response:
            if response.status != 200:
                error_msg = f"OANDA API error (price fetch): {response.status}"
                error_content = await response.text()
                logger.error(f"{error_msg} - Response: {error_content}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(BASE_DELAY * (2 ** attempt))
                    await get_session(force_new=True)
                    continue
                return False, {"error": error_msg}
            pricing_data = await response.json()
            if not pricing_data.get('prices'):
                return False, {"error": f"No pricing data for {instrument}"}
            return True, pricing_data
    return False, {"error": "Failed to fetch price after retries"}

@handle_async_errors
async def get_open_positions(account_id: str) -> Tuple[bool, Dict[str, Any]]:
    session_ok, error = await ensure_session()
    if not session_ok: return False, {"error": error}
    url = f"{OANDA_API_URL.rstrip('/')}/accounts/{account_id}/openPositions"
    async with session.get(url) as response:
        if response.status != 200:
            error_msg = f"Failed to fetch positions: {response.status}"
            logger.error(f"{error_msg} - {await response.text()}")
            return False, {"error": error_msg}
        return True, await response.json()

##############################################################################
# 4. Spread Check and Leverage Utility
##############################################################################
def check_spread_warning(pricing_data: Dict[str, Any], instrument: str) -> Tuple[bool, float]:
    if not pricing_data.get('prices'):
        logger.warning(f"No pricing data for spread check: {instrument}")
        return False, 0
    try:
        bid = float(pricing_data['prices'][0]['bids'][0]['price'])
        ask = float(pricing_data['prices'][0]['asks'][0]['price'])
    except (KeyError, IndexError, ValueError) as e:
        logger.error(f"Error parsing bid/ask for {instrument}: {str(e)}")
        return False, 0
    spread = ask - bid
    spread_percentage = spread / bid
    is_crypto = any(x in instrument for x in ['BTC', 'ETH', 'XRP', 'LTC'])
    threshold = SPREAD_THRESHOLD_CRYPTO if is_crypto else SPREAD_THRESHOLD_FOREX
    if spread_percentage > threshold:
        logger.warning(f"Spread {spread_percentage*100:.2f}% for {instrument} exceeds threshold")
        return True, spread
    return False, spread

def get_instrument_leverage(instrument: str) -> int:
    return INSTRUMENT_LEVERAGES.get(instrument, 1)

##############################################################################
# 5. Helper to Convert TradingView -> AlertData
##############################################################################
def translate_tradingview_signal(alert_data: Dict[str, Any]) -> Dict[str, Any]:
    if 'ticker' in alert_data and not alert_data.get('symbol'):
        alert_data['symbol'] = alert_data.pop('ticker')
    if 'interval' in alert_data and not alert_data.get('timeframe'):
        alert_data['timeframe'] = alert_data.pop('interval')
    alert_data.pop('exchange', None)
    alert_data.pop('strategy', None)
    return alert_data

##############################################################################
# 6. Validation of Trade Direction
##############################################################################
@handle_async_errors
async def validate_trade_direction(alert_data: Dict[str, Any]) -> Tuple[bool, Optional[str], bool]:
    request_id = str(uuid.uuid4())
    action = alert_data['action'].upper()
    if action in ['CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']:
        # For close actions, ensure there's a position to close
        success, positions_data = await get_open_positions(alert_data.get('account', OANDA_ACCOUNT_ID))
        if not success:
            return False, "Failed to fetch positions", False
        instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
        position_exists = any(p['instrument'] == instrument for p in positions_data.get('positions', []))
        if not position_exists:
            return False, f"No position to close for {instrument}", False
        return True, None, True
    # For BUY/SELL actions:
    success, positions_data = await get_open_positions(alert_data.get('account', OANDA_ACCOUNT_ID))
    if not success:
        return False, "Failed to fetch positions", False
    instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
    for p in positions_data.get('positions', []):
        if p['instrument'] == instrument:
            long_units = float(p.get('long', {}).get('units', 0) or 0)
            short_units = float(p.get('short', {}).get('units', 0) or 0)
            if long_units > 0 and action == 'BUY':
                return False, f"Existing LONG position for {instrument}", False
            if short_units < 0 and action == 'SELL':
                return False, f"Existing SHORT position for {instrument}", False
    return True, None, False

##############################################################################
# 1. Trade Validation and Direction
##############################################################################
@handle_async_errors
async def validate_trade_direction(alert_data: Dict[str, Any]) -> Tuple[bool, Optional[str], bool]:
    request_id = str(uuid.uuid4())
    action = alert_data['action'].upper()
    if action in ['CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']:
        # For close actions, ensure there's a position to close
        success, positions_data = await get_open_positions(alert_data.get('account', OANDA_ACCOUNT_ID))
        if not success:
            logger.error(f"[{request_id}] Failed to fetch positions")
            return False, "Failed to fetch positions", False
        
        instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
        position_exists = any(p['instrument'] == instrument for p in positions_data.get('positions', []))
        
        if not position_exists:
            logger.warning(f"[{request_id}] No position to close for {instrument}")
            return False, f"No position to close for {instrument}", False
        return True, None, True

    instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
    logger.info(f"[{request_id}] Checking positions for {instrument}")
    
    success, positions_data = await get_open_positions(alert_data.get('account', OANDA_ACCOUNT_ID))
    if not success:
        return False, "Failed to fetch positions", False
        
    logger.info(f"[{request_id}] Current positions: {json.dumps(positions_data, indent=2)}")

    for position in positions_data.get('positions', []):
        if position['instrument'] == instrument:
            long_units = float(position.get('long', {}).get('units', '0') or '0')
            short_units = float(position.get('short', {}).get('units', '0') or '0')
            logger.info(f"[{request_id}] Found position - Long units: {long_units}, Short units: {short_units}")
            
            if long_units > 0 and action == 'BUY':
                logger.warning(f"[{request_id}] Attempted BUY with existing LONG position")
                return False, f"Existing LONG position for {instrument}", False
            if short_units < 0 and action == 'SELL':
                logger.warning(f"[{request_id}] Attempted SELL with existing SHORT position")
                return False, f"Existing SHORT position for {instrument}", False

    logger.info(f"[{request_id}] Trade direction validation passed")
    return True, None, False

##############################################################################
# 2. Execute Trade Function with Enhanced Position Sizing
##############################################################################
@handle_async_errors
async def execute_trade(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
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
        
        # Use different base position sizes for crypto and forex
        if is_crypto:
            trade_size = CRYPTO_BASE_POSITION * percentage * leverage
            logger.info(f"Using crypto base position size for {instrument}")
        else:
            trade_size = FOREX_BASE_POSITION * percentage * leverage
            logger.info(f"Using forex base position size for {instrument}")
            
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

    wide_spread, spread_size = check_spread_warning(price_data, instrument)
    if wide_spread:
        logger.warning(f"Wide spread detected ({spread_size}), proceeding with caution")

    # Enhanced position sizing limits
    max_units = MAX_ORDER_SIZES.get(instrument)
    if max_units is not None:
        max_units = float(max_units)
        proposed_units = abs(raw_units)
        if proposed_units > max_units:
            logger.warning(f"Clamping units from {proposed_units} to {max_units} for {instrument}")
            units = min(max_units, proposed_units) * (-1 if is_sell else 1)
            units = round(units, 4)  # Extra precision control
    else:
        units = raw_units

    # Apply instrument-specific rounding
    if is_crypto:
        units = int(round(units))  # Whole units for crypto
    else:
        units = int(round(units))  # Standard forex rounding

    # Adjust for min size, handle SELL negativity
    if abs(units) < min_size:
        logger.warning(f"Order size {abs(units)} below minimum {min_size} for {instrument}")
        units = min_size if not is_sell else -min_size
    elif is_sell:
        units = -abs(units)

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

    url = f"{OANDA_API_URL.rstrip('/')}/accounts/{alert_data.get('account', OANDA_ACCOUNT_ID)}/orders"

    # Execute trade with enhanced error handling and retry logic
    for attempt in range(MAX_RETRIES):
        async with session.post(url, json=order_data) as response:
            if response.status != 201:
                error_content = await response.text()
                error_msg = f"OANDA API error (trade execution): {response.status}"
                logger.error(f"{error_msg} - Response: {error_content}")
                
                try:
                    error_data = json.loads(error_content)
                    error_code = error_data.get('errorCode')
                    
                    if error_code == 'UNITS_LIMIT_EXCEEDED':
                        logger.error(f"Fundamental position sizing error for {instrument}")
                        return False, {
                            "error": "Position size exceeds broker limits",
                            "max_allowed": max_units,
                            "attempted": abs(units)
                        }
                    elif error_code == 'INSUFFICIENT_MARGIN':
                        logger.error("Trade failed due to insufficient margin")
                        return False, {"error": "Insufficient margin for trade"}
                    elif error_code == 'PRICE_DISTANCE_EXCEEDED':
                        logger.error("Price moved too far from requested price")
                        return False, {"error": "Price moved too far from requested level"}
                    elif error_code == 'INSTRUMENT_NOT_TRADEABLE':
                        logger.error(f"Instrument {instrument} not tradeable")
                        return False, {"error": "Instrument not tradeable at this time"}
                    
                except json.JSONDecodeError:
                    logger.error(f"Could not parse error response: {error_content}")
                
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

    return False, {"error": "Failed to execute trade after maximum retries"}
    
##############################################################################
# 8. Position Closing Logic
##############################################################################
@handle_async_errors
async def close_position(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    account_id = alert_data.get('account', OANDA_ACCOUNT_ID)
    symbol = alert_data['symbol']
    instrument = f"{symbol[:3]}_{symbol[3:]}".upper()  # Ensure correct format
    success, positions_data = await get_open_positions(account_id)
    if not success:
        return False, positions_data
    position = next((p for p in positions_data.get('positions', []) if p['instrument'] == instrument), None)
    if not position:
        return True, {"message": f"No open position for {instrument}"}
    
    action = alert_data['action'].upper()
    long_units = float(position.get('long', {}).get('units', 0) or 0)
    short_units = float(position.get('short', {}).get('units', 0) or 0)
    close_body = {}
    if action == 'CLOSE_LONG' and long_units > 0:
        close_body["longUnits"] = "ALL"
    elif action == 'CLOSE_SHORT' and short_units < 0:
        close_body["shortUnits"] = "ALL"
    elif action == 'CLOSE':
        if long_units > 0:
            close_body["longUnits"] = "ALL"
        if short_units < 0:
            close_body["shortUnits"] = "ALL"
    
    if not close_body:
        return True, {"message": "No matching position side to close"}
    
    url = f"{OANDA_API_URL.rstrip('/')}/accounts/{account_id}/positions/{instrument}/close"
    logger.info(f"Closing position for {instrument} with payload: {close_body}")
    async with session.put(url, json=close_body) as response:
        logger.info(f"Close position response status: {response.status}")
        if response.status != 200:
            error_content = await response.text()
            logger.error(f"Close position failed: {error_content}")
            return False, {"error": error_content}
        close_response = await response.json()
        logger.info(f"Position closed successfully: {close_response}")
        return True, close_response

##############################################################################
# 9. AlertHandler for Orchestrating Trades
##############################################################################
class AlertHandler:
    def __init__(self, max_retries: int = MAX_RETRIES, base_delay: float = BASE_DELAY):
        self.logger = logging.getLogger('alert_handler')
        self.max_retries = max_retries
        self.base_delay = base_delay
        self._trade_lock = asyncio.Lock()
        self.position_tracker = PositionTracker()

    async def process_alert(self, alert_data: Dict[str, Any]) -> bool:
        request_id = str(uuid.uuid4())
        if not alert_data:
            self.logger.error("No alert data provided")
            return False

        alert_id = alert_data.get('id', str(uuid.uuid4()))
        action = alert_data.get('action', '').upper()
        symbol = alert_data['symbol']

        self.logger.info(f"[{request_id}] Starting to process alert {alert_id}")
        self.logger.info(f"[{request_id}] Alert details - Action: {action}, Symbol: {symbol}")
        self.logger.info(f"[{request_id}] Full alert data: {json.dumps(alert_data, indent=2)}")

        async with self._trade_lock:
            # Market hours check
            instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
            self.logger.info(f"[{request_id}] Checking market status for {instrument}")
            is_tradeable, status_msg = await check_market_status(instrument, alert_data.get('account', OANDA_ACCOUNT_ID))
            if not is_tradeable:
                self.logger.warning(f"[{request_id}] Market closed for {instrument}: {status_msg}")
                return False

            # Determine if the action is to close a position
            action = alert_data['action'].upper()
            is_closing_action = action in ['CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']

            if is_closing_action:
                self.logger.info(f"[{request_id}] Closing position for {instrument}")
                close_ok, close_result = await close_position(alert_data)
                if close_ok:
                    await self.position_tracker.clear_position(symbol)
                    self.logger.info(f"[{request_id}] Position closed successfully")
                    return True
                else:
                    self.logger.error(f"[{request_id}] Failed to close position: {close_result}")
                    return False
            else:
                self.logger.info(f"[{request_id}] Validating trade direction")
                is_valid, err_msg, is_closing_trade = await validate_trade_direction(alert_data)
                if not is_valid:
                    self.logger.warning(f"[{request_id}] Trade validation failed: {err_msg}")
                    return False
                self.logger.info(f"[{request_id}] Trade validation passed")
                for attempt in range(self.max_retries):
                    trade_ok, trade_result = await execute_trade(alert_data)
                    self.logger.info(f"[{request_id}] Trade execution attempt {attempt+1} result: {json.dumps(trade_result, indent=2)}")
                    if trade_ok:
                        await self.position_tracker.record_position(symbol, action, alert_data['timeframe'])
                        self.logger.info(f"[{request_id}] Alert processed successfully")
                        return True
                    else:
                        error_msg = trade_result.get('error', 'Unknown error')
                        self.logger.error(f"[{request_id}] Trade execution error: {error_msg}")
                        if attempt < self.max_retries - 1:
                            delay = self.base_delay * (2 ** attempt)
                            self.logger.warning(f"[{request_id}] Retrying trade in {delay}s")
                            await asyncio.sleep(delay)
                        else:
                            self.logger.error(f"[{request_id}] Final trade attempt failed")
                            return False
                return False

# Instantiate a global alert handler
alert_handler = AlertHandler()

##############################################################################
# 10. Alert and TradingView Endpoints
##############################################################################
async def process_incoming_alert(data: Dict[str, Any], source: str = "direct") -> JSONResponse:
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Processing {source} alert: {json.dumps(data, indent=2)}")
    try:
        if not data.get('account'):
            data['account'] = OANDA_ACCOUNT_ID

        if source == "tradingview":
            data = translate_tradingview_signal(data)
            logger.info(f"[{request_id}] Normalized TradingView data: {json.dumps(data, indent=2)}")

        try:
            validated_data = AlertData(**data)
            logger.info(f"[{request_id}] Data validation successful")
        except ValidationError as e:
            logger.error(f"[{request_id}] Validation error: {e.errors()}")
            return JSONResponse(status_code=400, content={"error": f"Validation error: {e.errors()}", "request_id": request_id})

        success = await alert_handler.process_alert(validated_data.dict())
        if success:
            return JSONResponse(status_code=200, content={"message": "Alert processed successfully", "request_id": request_id})
        else:
            return JSONResponse(status_code=400, content={"error": "Failed to process alert", "request_id": request_id})
    except Exception as e:
        logger.exception(f"[{request_id}] Unexpected error: {str(e)}")
        return JSONResponse(status_code=500, content={"error": f"Unexpected error: {str(e)}", "request_id": request_id})

@app.post("/alerts")
async def handle_alert_endpoint(alert_data: AlertData):
    return await process_incoming_alert(alert_data.dict(), source="direct")

@app.post("/tradingview")
async def handle_tradingview_webhook(request: Request):
    try:
        body = await request.json()
        logger.info(f"Received TradingView webhook: {json.dumps(body, indent=2)}")
        required_fields = ['symbol', 'action']
        missing_fields = [field for field in required_fields if not body.get(field)]
        if missing_fields:
            return JSONResponse(status_code=400, content={"error": f"Missing required fields: {missing_fields}"})
        return await process_incoming_alert(body, source="tradingview")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in TradingView webhook: {str(e)}")
        return JSONResponse(status_code=400, content={"error": "Invalid JSON format"})
    except Exception as e:
        logger.exception(f"Unexpected error in TradingView webhook: {str(e)}")
        return JSONResponse(status_code=500, content={"error": f"Unexpected error: {str(e)}"})

@app.get("/check-config")
async def check_configuration():
    try:
        config = {
            "api_url": OANDA_API_URL,
            "account_id": OANDA_ACCOUNT_ID,
            "api_token_set": bool(OANDA_API_TOKEN),
        }
        session_ok, session_error = await ensure_session()
        if not session_ok:
            return JSONResponse(status_code=500, content={"error": "Session creation failed", "details": session_error, "config": config})
        account_url = f"{OANDA_API_URL.rstrip('/')}/accounts/{OANDA_ACCOUNT_ID}"
        async with session.get(account_url) as response:
            if response.status != 200:
                error_content = await response.text()
                return JSONResponse(status_code=response.status, content={"error": "Failed to access OANDA account", "details": error_content, "config": config})
            account_info = await response.json()
            return {"status": "ok", "config": config, "account_info": account_info}
    except Exception as e:
        logger.exception("Configuration check failed")
        return JSONResponse(status_code=500, content={"error": str(e), "config": config})

@app.api_route("/", methods=["GET", "HEAD"])
async def health_check():
    return {"status": "active", "timestamp": datetime.utcnow().isoformat()}

@app.exception_handler(404)
async def not_found_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=404, content={"message": f"Endpoint {request.url.path} not found"})

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutdown initiated.")
    if session and not session.closed:
        await session.close()
        logger.info("HTTP session closed.")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False, workers=1, timeout_keep_alive=65, log_config=None)
