# =========================
# Block 1: Imports, Environment, Logging, and Session Management
# =========================
import os
import uuid
import asyncio
import aiohttp
import logging
import re
import math
import time
from datetime import datetime, timedelta
from pytz import timezone
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any, Union
from contextlib import asynccontextmanager
from pydantic import BaseModel, validator, ValidationError
import json  # For JSONDecodeError if needed

# Environment variables
OANDA_API_TOKEN = os.getenv('OANDA_API_TOKEN')
OANDA_ACCOUNT_ID = os.getenv('OANDA_ACCOUNT_ID')
OANDA_API_URL = os.getenv('OANDA_API_URL', 'https://api-fxtrade.oanda.com/v3')
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "https://your-tradingview-domain.com").split(",")

# Logging configuration
try:
    os.makedirs('/opt/render/project/src/logs', exist_ok=True)
    log_file = os.path.join('/opt/render/project/src/logs', 'trading_bot.log')
except Exception as e:
    print(f"Failed to create log directory: {e}")
    log_file = 'trading_bot.log'

max_bytes = 10 * 1024 * 1024  # 10MB
backup_count = 1
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
    ]
)
logger = logging.getLogger(__name__)

# HTTP session settings
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 30
HTTP_REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=READ_TIMEOUT, connect=CONNECT_TIMEOUT, sock_read=READ_TIMEOUT)
SESSION_RETRY_ATTEMPTS = 3
SESSION_RETRY_DELAY = 1.0

# Global session variable
session: Optional[aiohttp.ClientSession] = None

async def get_session(force_new: bool = False) -> aiohttp.ClientSession:
    global session
    if session is None or session.closed or force_new:
        if session and not session.closed:
            await session.close()
        connector = aiohttp.TCPConnector(
            limit=100,
            enable_cleanup_closed=True,
            force_close=False,
            keepalive_timeout=65
        )
        session = aiohttp.ClientSession(
            connector=connector,
            timeout=HTTP_REQUEST_TIMEOUT,
            headers={"Authorization": f"Bearer {OANDA_API_TOKEN}", "Content-Type": "application/json"}
        )
    return session

async def ensure_session() -> tuple[bool, Optional[str]]:
    global session
    try:
        if session is None or session.closed:
            session = await get_session(force_new=True)
            logger.info("Created new HTTP session")
        return True, None
    except Exception as e:
        error_msg = f"Failed to create HTTP session: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing global HTTP session")
    await get_session(force_new=True)
    yield
    if session and not session.closed:
        logger.info("Closing global HTTP session")
        await session.close()

# Create the FastAPI instance (must be before any @app decorators)
app = FastAPI(
    title="OANDA Trading Bot",
    description="An async trading bot using FastAPI and aiohttp",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# Block 2: Middleware and Health Check Endpoint
# =========================
@app.middleware("http")
async def log_requests(request: Request, call_next):
    body = b""
    try:
        body = await request.body()
        logger.info(f"Request to {request.url.path}")
        logger.info(f"Request body: {body.decode()}")
        
        # Rebuild the request stream for downstream handlers
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

@app.get("/")
async def health_check():
    return {"status": "active", "timestamp": datetime.utcnow().isoformat()}

# =========================
# Block 3: Configuration Constants, Instrument Configurations, and PositionTracker Class
# =========================
SPREAD_THRESHOLD_FOREX = 0.001
SPREAD_THRESHOLD_CRYPTO = 0.008
MAX_RETRIES = 3
BASE_DELAY = 1.0
BASE_POSITION = 100000
DEFAULT_FOREX_PRECISION = 5
DEFAULT_CRYPTO_PRECISION = 2
DEFAULT_MIN_ORDER_SIZE = 1000

# Instrument configurations (example values)
INSTRUMENT_LEVERAGES = {
    "USD_CHF": 20,
    "EUR_USD": 20,
    "GBP_USD": 20,
    "USD_JPY": 20,
    "AUD_USD": 20,    # Added: AUD/USD
    "USD_THB": 20,    # Added: USD/THB
    "CAD_CHF": 20,    # Added: CAD/CHF
    "NZD_USD": 20,    # Added: NZD/USD
    "BTC_USD": 2,
    "ETH_USD": 2,
    "XRP_USD": 2,
    "LTC_USD": 2
}

INSTRUMENT_PRECISION = {
    "EUR_USD": 5,
    "GBP_USD": 0,     # Assuming no decimals as per your original config; adjust if needed
    "USD_JPY": 3,
    "AUD_USD": 5,     # Added: AUD/USD (example: 5 decimals)
    "USD_THB": 5,     # Added: USD/THB (example: 5 decimals)
    "CAD_CHF": 5,     # Added: CAD/CHF (example: 5 decimals)
    "NZD_USD": 5,     # Added: NZD/USD (example: 5 decimals)
    "BTC_USD": 2,
    "ETH_USD": 2,
    "XRP_USD": 4
}

MIN_ORDER_SIZES = {
    "EUR_USD": 1000,
    "GBP_USD": 1000,
    "AUD_USD": 1000,  # Added: AUD/USD
    "USD_THB": 1000,  # Added: USD/THB
    "CAD_CHF": 1000,  # Added: CAD/CHF
    "NZD_USD": 1000,  # Added: NZD/USD
    "BTC_USD": 0.25,
    "ETH_USD": 4,
    "XRP_USD": 200
}

class PositionTracker:
    def __init__(self):
        self.positions: Dict[str, Dict[str, Any]] = {}
        self.bar_times: Dict[str, list] = {}
        
    async def record_position(self, symbol: str, action: str, timeframe: str):
        current_time = datetime.now(timezone('Asia/Bangkok'))
        self.positions[symbol] = {
            'entry_time': current_time,
            'position_type': 'LONG' if action.upper() == 'BUY' else 'SHORT',
            'bars_held': 0,
            'timeframe': timeframe
        }
        self.bar_times.setdefault(symbol, []).append(current_time)
        
    async def update_bars_held(self, symbol: str) -> int:
        if symbol not in self.positions:
            return 0
        position = self.positions[symbol]
        entry_time = position['entry_time']
        timeframe = int(position['timeframe'])
        current_time = datetime.now(timezone('Asia/Bangkok'))
        bars = (current_time - entry_time).seconds // (timeframe * 60)
        position['bars_held'] = bars
        return bars

    async def should_close_position(self, symbol: str, new_signal: str = None) -> bool:
        if symbol not in self.positions:
            return False
        bars_held = await self.update_bars_held(symbol)
        position = self.positions[symbol]
        if bars_held >= 4:
            return True
        if 0 < bars_held < 4 and new_signal:
            is_opposing = ((position['position_type'] == 'LONG' and new_signal == 'SELL') or
                           (position['position_type'] == 'SHORT' and new_signal == 'BUY'))
            if is_opposing:
                return True
        return False

    async def get_close_action(self, symbol: str) -> str:
        if symbol not in self.positions:
            return 'CLOSE'
        return f"CLOSE_{self.positions[symbol]['position_type']}"

    async def clear_position(self, symbol: str):
        self.positions.pop(symbol, None)
        self.bar_times.pop(symbol, None)

# =========================
# Block 4: Pydantic Model and Market Functions
# =========================
TIMEFRAME_PATTERN = re.compile(r'^(\d+)([mMhH])$')

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
        # If the input is just digits, interpret according to the mapping:
        if v.isdigit():
            mapping = {
                1: "1H",
                4: "4H",
                12: "12H",
                5: "5M",
                15: "15M"
            }
            try:
                num = int(v)
            except Exception as e:
                raise ValueError("Timeframe must be a number or in proper format.") from e
            v = mapping.get(num, f"{v}M")
        
        match = TIMEFRAME_PATTERN.match(v)
        if not match:
            raise ValueError("Invalid timeframe format. Use like '15M' or '1H'")
        
        value_str, unit = match.groups()
        value = int(value_str)
        
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
        if v.upper() not in valid_actions:
            raise ValueError(f"Action must be one of {valid_actions}")
        return v.upper()

    @validator('percentage')
    def validate_percentage(cls, v):
        if not 0 < v <= 1:
            raise ValueError("Percentage must be between 0 and 1")
        return v

    @validator('symbol')
    def validate_symbol(cls, v):
        if len(v) < 6:
            raise ValueError("Symbol must be at least 6 characters")
        instrument = f"{v[:3]}_{v[3:]}"
        if instrument not in INSTRUMENT_LEVERAGES:
            raise ValueError(f"Invalid trading instrument: {instrument}")
        return v.upper()

    @validator('timeInForce')
    def validate_time_in_force(cls, v):
        valid_values = ['FOK', 'IOC', 'GTC', 'GFD']
        if v and v.upper() not in valid_values:
            raise ValueError(f"timeInForce must be one of {valid_values}")
        return v.upper() if v else 'FOK'

    @validator('orderType')
    def validate_order_type(cls, v):
        valid_types = ['MARKET', 'LIMIT', 'STOP', 'MARKET_IF_TOUCHED']
        if v and v.upper() not in valid_types:
            raise ValueError(f"orderType must be one of {valid_types}")
        return v.upper() if v else 'MARKET'

def is_market_open() -> tuple[bool, str]:
    current_time = datetime.now(timezone('Asia/Bangkok'))
    wday = current_time.weekday()
    hour = current_time.hour
    if (wday == 5 and hour >= 5) or (wday == 6) or (wday == 0 and hour < 5):
        return False, "Weekend market closure"
    return True, "Regular trading hours"

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

def check_spread_warning(pricing_data: Dict[str, Any], instrument: str) -> tuple[bool, float]:
    if not pricing_data.get('prices'):
        logger.warning(f"No pricing data available for spread check: {instrument}")
        return False, 0
    price = pricing_data['prices'][0]
    try:
        bid = float(price['bids'][0]['price'])
        ask = float(price['asks'][0]['price'])
    except (KeyError, IndexError, ValueError) as e:
        logger.error(f"Error parsing price data for {instrument}: {e}")
        return False, 0
    spread = ask - bid
    spread_percentage = spread / bid
    is_crypto = any(crypto in instrument for crypto in ['BTC', 'ETH', 'XRP', 'LTC'])
    threshold = SPREAD_THRESHOLD_CRYPTO if is_crypto else SPREAD_THRESHOLD_FOREX
    if spread_percentage > threshold:
        logger.warning(f"Wide spread detected for {instrument}: {spread:.5f} ({spread_percentage*100:.2f}%)")
        return True, spread
    return False, 0

async def get_instrument_price(instrument: str, account_id: str) -> tuple[bool, Dict[str, Any]]:
    if not all([OANDA_API_TOKEN, OANDA_API_URL]):
        error_msg = "Missing required OANDA configuration"
        logger.error(error_msg)
        return False, {"error": error_msg}
    session_ok, error = await ensure_session()
    if not session_ok:
        return False, {"error": error}
    url = f"{OANDA_API_URL.rstrip('/')}/accounts/{account_id}/pricing?instruments={instrument}"
    logger.info(f"Fetching instrument price from OANDA: {url}")
    try:
        for attempt in range(MAX_RETRIES):
            try:
                async with session.get(url) as response:
                    if response.status != 200:
                        error_msg = f"OANDA API error: {response.status}"
                        error_content = await response.text()
                        logger.error(f"{error_msg} - Response: {error_content}")
                        if attempt < MAX_RETRIES - 1:
                            delay = BASE_DELAY * (2 ** attempt)
                            logger.warning(f"Retrying in {delay}s (attempt {attempt+1}/{MAX_RETRIES})")
                            await asyncio.sleep(delay)
                            await ensure_session()
                            continue
                        return False, {"error": error_msg, "response": error_content}
                    pricing_data = await response.json()
                    if not pricing_data.get('prices'):
                        error_msg = f"No pricing data returned for {instrument}"
                        logger.warning(error_msg)
                        return False, {"error": error_msg}
                    return True, pricing_data
            except aiohttp.ClientError as e:
                if attempt < MAX_RETRIES - 1:
                    delay = BASE_DELAY * (2 ** attempt)
                    logger.warning(f"Network error, retrying in {delay}s: {str(e)}")
                    await asyncio.sleep(delay)
                    await get_session(force_new=True)
                    continue
                error_msg = f"Network error after {MAX_RETRIES} attempts: {str(e)}"
                logger.error(error_msg)
                return False, {"error": error_msg}
    except Exception as e:
        error_msg = f"Unexpected error fetching price: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, {"error": error_msg}

async def get_open_positions(account_id: str) -> tuple[bool, Dict[str, Any]]:
    try:
        session_ok, error = await ensure_session()
        if not session_ok:
            return False, {"error": error}
        url = f"{OANDA_API_URL}/accounts/{account_id}/openPositions"
        async with session.get(url) as response:
            if response.status != 200:
                error_msg = f"Failed to fetch positions: {response.status}"
                error_content = await response.text()
                logger.error(f"{error_msg} - Response: {error_content}")
                return False, {"error": error_msg}
            positions = await response.json()
            return True, positions
    except Exception as e:
        error_msg = f"Error fetching positions: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, {"error": error_msg}

@validator('timeframe')
def validate_timeframe(cls, v):
    # If the input is digits only, use our custom mapping:
    if v.isdigit():
        # Example mapping:
        # 1 -> 1H, 4 -> 4H, 12 -> 12H, 5 -> 5M, 15 -> 15M
        mapping = {1: "1H", 4: "4H", 12: "12H", 5: "5M", 15: "15M"}
        num = int(v)
        v = mapping.get(num, f"{v}M")  # default to minutes if not one of our keys
    match = TIMEFRAME_PATTERN.match(v)
    if not match:
        raise ValueError("Invalid timeframe format. Use like '15M' or '1H'")
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


# =========================
# Block 5: Trade Execution Functions and Utility Functions for Trading Signals
# =========================
async def validate_trade_direction(alert_data: Dict[str, Any]) -> tuple[bool, Optional[str], bool]:
    try:
        if alert_data['action'] in ['CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']:
            return True, None, True
        success, positions = await get_open_positions(alert_data.get('account', OANDA_ACCOUNT_ID))
        if not success:
            return False, "Failed to fetch positions", False
        instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
        for position in positions.get('positions', []):
            if position['instrument'] == instrument:
                long_units = float(position['long'].get('units', '0') or '0')
                short_units = float(position['short'].get('units', '0') or '0')
                if long_units > 0 and alert_data['action'] == 'BUY':
                    return False, f"Existing LONG position for {instrument}", False
                if short_units < 0 and alert_data['action'] == 'SELL':
                    return False, f"Existing SHORT position for {instrument}", False
        return True, None, False
    except Exception as e:
        logger.error(f"Error in trade validation: {str(e)}", exc_info=True)
        return False, f"Trade validation failed: {str(e)}", False

async def execute_trade(alert_data: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
    try:
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
                error_msg = f"Percentage must be between 0 and 1: {percentage}"
                logger.error(error_msg)
                return False, {"error": error_msg}
            trade_size = BASE_POSITION * percentage * leverage
            if trade_size <= 0:
                error_msg = f"Invalid trade size calculated: {trade_size}"
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
        price_success, price_data = await get_instrument_price(instrument, alert_data['account'])
        if not price_success:
            return False, price_data
        price_info = price_data['prices'][0]
        is_sell = alert_data['action'].upper() == 'SELL'
        try:
            price = float(price_info['bids'][0]['price']) if is_sell else float(price_info['asks'][0]['price'])
        except (KeyError, IndexError, ValueError) as e:
            error_msg = f"Error parsing price data: {str(e)}"
            logger.error(error_msg)
            return False, {"error": error_msg}
        if is_crypto:
            units = round(raw_units, precision)
        else:
            units = int(round(raw_units))
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
        logger.info(f"Trade details: {instrument}, {'SELL' if is_sell else 'BUY'}, Price={price}, Units={units_str}, Size=${trade_size}")
        url = f"{OANDA_API_URL}/accounts/{alert_data['account']}/orders"
        for attempt in range(MAX_RETRIES):
            try:
                async with session.post(url, json=order_data) as response:
                    if response.status != 201:
                        error_msg = f"OANDA API error: {response.status}"
                        error_content = await response.text()
                        logger.error(f"{error_msg} - Response: {error_content}")
                        if attempt < MAX_RETRIES - 1:
                            delay = BASE_DELAY * (2 ** attempt)
                            logger.warning(f"Retrying trade in {delay}s (attempt {attempt+1}/{MAX_RETRIES})")
                            await asyncio.sleep(delay)
                            await ensure_session()
                            continue
                        return False, {"error": error_msg, "response": error_content, "order_data": order_data}
                    order_response = await response.json()
                    logger.info(f"Trade executed successfully: {order_response}")
                    return True, order_response
            except aiohttp.ClientError as e:
                if attempt < MAX_RETRIES - 1:
                    delay = BASE_DELAY * (2 ** attempt)
                    logger.warning(f"Network error, retrying trade in {delay}s: {str(e)}")
                    await asyncio.sleep(delay)
                    await get_session(force_new=True)
                    continue
                error_msg = f"Network error after {MAX_RETRIES} attempts: {str(e)}"
                logger.error(error_msg)
                return False, {"error": error_msg}
    except Exception as e:
        error_msg = f"Unexpected error executing trade: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False, {"error": error_msg}

def translate_tradingview_signal(alert_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Translates a TradingView signal into the format expected by the trading bot.
    For example, if the signal contains 'ticker' or 'interval', convert them to 'symbol' and 'timeframe'.
    Also remove any extraneous fields.
    """
    if 'ticker' in alert_data and not alert_data.get('symbol'):
        alert_data['symbol'] = alert_data.pop('ticker')
    if 'interval' in alert_data and not alert_data.get('timeframe'):
        alert_data['timeframe'] = alert_data.pop('interval')
    # Remove extra fields that are not needed
    alert_data.pop('exchange', None)
    alert_data.pop('strategy', None)
    return alert_data

def get_instrument_leverage(instrument: str) -> int:
    """
    Returns the leverage for the given instrument.
    Defaults to 1 if the instrument is not found.
    """
    return INSTRUMENT_LEVERAGES.get(instrument, 1)

async def check_market_status(instrument: str, account_id: str) -> tuple[bool, str]:
    """
    Checks whether the market is open.
    This implementation uses is_market_open for simplicity.
    """
    market_open, msg = is_market_open()
    return market_open, msg

# =========================
# Block 6: AlertHandler Class, Endpoints, Shutdown Event, Exception Handler, and Main Block
# =========================
class AlertHandler:
    def __init__(self, max_retries: int = MAX_RETRIES, base_delay: float = BASE_DELAY):
        self.logger = logging.getLogger('alert_handler')
        self.max_retries = max_retries
        self.base_delay = base_delay
        self._trade_lock = asyncio.Lock()
        self.position_tracker = PositionTracker()

    async def close_position(self, alert_data: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
        try:
            account_id = alert_data.get('account', OANDA_ACCOUNT_ID)
            instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
            success, positions = await get_open_positions(account_id)
            if not success:
                return False, positions
            position = next((p for p in positions.get('positions', []) if p['instrument'] == instrument), None)
            if not position:
                error_msg = f"No position found for {instrument}"
                self.logger.error(error_msg)
                return False, {"error": error_msg}
            long_units = float(position.get('long', {}).get('units', '0') or '0')
            short_units = float(position.get('short', {}).get('units', '0') or '0')
            action = alert_data['action'].upper()
            close_body = {}
            if action == 'CLOSE_LONG' and long_units > 0:
                close_body = {"longUnits": "ALL"}
            elif action == 'CLOSE_SHORT' and short_units < 0:
                close_body = {"shortUnits": "ALL"}
            elif action == 'CLOSE':
                if long_units > 0:
                    close_body = {"longUnits": "ALL"}
                elif short_units < 0:
                    close_body = {"shortUnits": "ALL"}
            if not close_body:
                error_msg = f"No matching position to close for {instrument}"
                self.logger.error(error_msg)
                return False, {"error": error_msg}
            url = f"{OANDA_API_URL}/accounts/{account_id}/positions/{instrument}/close"
            self.logger.info(f"Closing {instrument} position with data: {close_body}")
            async with session.put(url, json=close_body) as response:
                if response.status != 200:
                    error_content = await response.text()
                    self.logger.error(f"Failed to close position: {error_content}")
                    return False, {"error": f"Close position failed: {error_content}"}
                close_response = await response.json()
                self.logger.info(f"Position closed successfully: {close_response}")
                return True, close_response
        except Exception as e:
            error_msg = f"Error closing position: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return False, {"error": error_msg}

    async def process_alert(self, alert_data: Dict[str, Any]) -> bool:
        if not alert_data:
            self.logger.error("No alert data provided")
            return False
        alert_id = alert_data.get('id', str(uuid.uuid4()))
        action = alert_data.get('action', '').upper()
        self.logger.info(f"Processing alert {alert_id} with action {action} for symbol {alert_data['symbol']}")
        async with self._trade_lock:
            # Check if an existing position should be closed
            symbol = alert_data['symbol']
            if await self.position_tracker.should_close_position(symbol, new_signal=action):
                close_action = await self.position_tracker.get_close_action(symbol)
                close_alert = {**alert_data, 'action': close_action}
                success, _ = await self.close_position(close_alert)
                if success:
                    await self.position_tracker.clear_position(symbol)
                    return True
            # Market hours check before trading
            instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
            is_tradeable, status_msg = await check_market_status(instrument, alert_data.get('account', OANDA_ACCOUNT_ID))
            if not is_tradeable:
                self.logger.warning(f"Market closed for {instrument}: {status_msg}")
                return False
            # Execute trade with retries
            for attempt in range(self.max_retries):
                try:
                    is_valid, error_message, is_closing_trade = await validate_trade_direction(alert_data)
                    if not is_valid:
                        self.logger.warning(f"Trade validation failed for alert {alert_id}: {error_message}")
                        return False
                    alert_data['is_closing_trade'] = is_closing_trade
                    is_tradeable, status_msg = await check_market_status(instrument, alert_data.get('account', OANDA_ACCOUNT_ID))
                    if not is_tradeable:
                        self.logger.warning(f"Market closed for {instrument}: {status_msg}")
                        return False
                    success, trade_result = await execute_trade(alert_data)
                    if success:
                        await self.position_tracker.record_position(alert_data['symbol'], alert_data['action'], alert_data['timeframe'])
                        self.logger.info(f"Alert {alert_id} processed successfully")
                        return True
                    if attempt < self.max_retries - 1:
                        delay = self.base_delay * (2 ** attempt)
                        self.logger.warning(f"Alert {alert_id} failed, retrying in {delay}s: {trade_result.get('error', 'Unknown error')}")
                        await asyncio.sleep(delay)
                    else:
                        self.logger.error(f"Alert {alert_id} failed on final attempt: {trade_result}")
                        return False
                except Exception as e:
                    self.logger.error(f"Error processing alert {alert_id}: {str(e)}", exc_info=True)
                    if attempt < self.max_retries - 1:
                        delay = self.base_delay * (2 ** attempt)
                        await asyncio.sleep(delay)
                    else:
                        return False
            self.logger.info(f"Alert {alert_id} discarded after {self.max_retries} failed attempts")
            return False

alert_handler = AlertHandler()

@app.post("/alerts")
async def handle_alert_endpoint(alert_data: AlertData):
    try:
        alert_data_dict = alert_data.dict()
        if not alert_data_dict.get('account'):
            alert_data_dict['account'] = OANDA_ACCOUNT_ID
        alert_data_dict = translate_tradingview_signal(alert_data_dict)
        alert_id = alert_data_dict.get('id', str(uuid.uuid4()))
        logger.info(f"Received alert {alert_id}: {alert_data_dict}")
        success = await alert_handler.process_alert(alert_data_dict)
        if success:
            return JSONResponse(status_code=200, content={"message": f"Alert {alert_id} processed successfully"})
        else:
            return JSONResponse(status_code=400, content={"error": f"Failed to process alert {alert_id}"})
    except ValidationError as e:
        error_msg = f"Invalid alert data: {str(e)}"
        logger.error(f"Validation error details: {e.errors()}")  # Log detailed validation errors
        return JSONResponse(status_code=400, content={"error": error_msg})
    except Exception as e:
        error_msg = f"Unexpected error processing alert: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return JSONResponse(status_code=500, content={"error": error_msg})

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutdown initiated")
    if session and not session.closed:
        await session.close()
        logger.info("HTTP session closed")

@app.exception_handler(404)
async def not_found_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=404,
        content={"message": f"Endpoint {request.url} not found"}
    )

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))  # Default to 8000 if PORT is not set
    uvicorn.run(
        "python_bridge:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        workers=1,
        timeout_keep_alive=65,
        log_config=None
    )
