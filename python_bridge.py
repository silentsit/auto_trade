# Original imports section
import os
import uuid
import asyncio
import aiohttp
import logging
import logging.handlers
import re
import time
import json
import holidays
from datetime import datetime, timedelta
from pytz import timezone
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any, Union, List, Tuple, Callable
from contextlib import asynccontextmanager
from pydantic import BaseModel, validator, ValidationError
from functools import wraps

# ADD THE NEW ERROR HANDLING CODE RIGHT HERE, after imports and before any other code

# Type variables for type hints
from typing import TypeVar, ParamSpec

##############################################################################
# Error Handling Infrastructure
##############################################################################

P = ParamSpec('P')
T = TypeVar('T')

# Custom exception hierarchy
class TradingError(Exception):
    """Base exception for trading-related errors"""
    pass

class MarketError(TradingError):
    """Errors related to market conditions"""
    pass

class OrderError(TradingError):
    """Errors related to order execution"""
    pass

class ValidationError(TradingError):
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

def get_env_or_raise(key: str, default: Optional[str] = None) -> str:
    value = os.getenv(key, default)
    if value is None:
        raise ValueError(f"Required environment variable {key} is not set")
    return value

# Core Environment Variables
OANDA_API_TOKEN = get_env_or_raise('OANDA_API_TOKEN')
OANDA_ACCOUNT_ID = get_env_or_raise('OANDA_ACCOUNT_ID')
OANDA_API_URL = get_env_or_raise('OANDA_API_URL', 'https://api-fxtrade.oanda.com/v3')
ALLOWED_ORIGINS = get_env_or_raise("ALLOWED_ORIGINS", "http://localhost").split(",")

# Session Configuration
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 30
TOTAL_TIMEOUT = 45
HTTP_REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=TOTAL_TIMEOUT,
    connect=CONNECT_TIMEOUT,
    sock_read=READ_TIMEOUT
)
MAX_SIMULTANEOUS_CONNECTIONS = 100

# Session management
_session = None

async def get_session(force_new: bool = False) -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed or force_new:
        if _session and not _session.closed:
            await _session.close()
        _session = aiohttp.ClientSession(
            timeout=HTTP_REQUEST_TIMEOUT,
            headers={"Authorization": f"Bearer {OANDA_API_TOKEN}"}
        )
    return _session

# Trading Constants
RISK_PERCENTAGE = 0.02  # 2% risk per trade
SPREAD_THRESHOLD_FOREX = 0.001
SPREAD_THRESHOLD_CRYPTO = 0.008
MAX_RETRIES = 3
BASE_DELAY = 1.0
BASE_POSITION = 100000

# Market Session Configuration
MARKET_SESSIONS = {
    "FOREX": {
        "hours": "24/5",
        "timezone": "America/New_York",
        "holidays": "US"
    },
    "XAU_USD": {
        "hours": "23:00-21:59|UTC",
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

##############################################################################
# Logging Setup
##############################################################################

class JSONFormatter(logging.Formatter):
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
    try:
        log_dir = '/opt/render/project/src/logs'
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'trading_bot.log')
    except Exception as e:
        log_file = 'trading_bot.log'

    formatter = JSONFormatter()
    
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5
    )
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return logging.getLogger('trading_bot')

logger = setup_logging()

##############################################################################
# Models
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
        
        pattern = re.compile(r'^(\d+)([mMhH])$')
        match = pattern.match(v)
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

    @validator('symbol')
    def validate_symbol(cls, v):
        if len(v) < 6:
            raise ValueError("Symbol must be at least 6 characters")
        
        v = v.upper().replace('/', '_')
        if v in ['XAUUSD', 'XAUSD']:
            instrument = 'XAU_USD'
        else:
            instrument = f"{v[:3]}_{v[3:]}"
        
        if instrument not in INSTRUMENT_LEVERAGES:
            raise ValueError(f"Invalid instrument: {instrument}")
        
        return v

    class Config:
        str_strip_whitespace = True
        validate_assignment = True
        extra = "forbid"

##############################################################################
# Position Tracking
##############################################################################

class PositionTracker:
    def __init__(self):
        self.positions: Dict[str, Dict[str, Any]] = {}
        self.bar_times: Dict[str, List[datetime]] = {}
        self._lock = asyncio.Lock()
        self.reconciliation_task = asyncio.create_task(self.reconcile_positions())
        
    @handle_async_errors
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

    @handle_async_errors
    async def clear_position(self, symbol: str):
        async with self._lock:
            self.positions.pop(symbol, None)
            self.bar_times.pop(symbol, None)
            logger.info(f"Cleared position tracking for {symbol}")

    @handle_async_errors
    async def reconcile_positions(self):
        while True:
            await asyncio.sleep(300)  # Every 5 minutes
            async with self._lock:
                try:
                    success, positions_data = await get_open_positions()
                    if not success:
                        continue
                    
                    for symbol in list(self.positions.keys()):
                        oanda_pos = next(
                            (p for p in positions_data.get('positions', [])
                            if p['instrument'] == symbol), None)
                        
                        if not oanda_pos:
                            self.positions.pop(symbol, None)
                            self.bar_times.pop(symbol, None)
                except Exception as e:
                    logger.error(f"Reconciliation failed: {str(e)}")

##############################################################################
# Market Utilities
##############################################################################

@handle_sync_errors
def check_market_hours(session_config: dict) -> bool:
    tz = timezone(session_config['timezone'])
    now = datetime.now(tz)
    
    # Check holidays
    if session_config['holidays']:
        holiday_cal = getattr(holidays, session_config['holidays'])()
        if now.date() in holiday_cal:
            return False
    
    # Check daily hours
    if "24/7" in session_config['hours']:
        return True
    if "24/5" in session_config['hours']:
        return now.weekday() < 5
    
    time_ranges = session_config['hours'].split('|')
    for time_range in time_ranges:
        start_str, end_str = time_range.split('-')
        start = datetime.strptime(start_str, "%H:%M").time()
        end = datetime.strptime(end_str, "%H:%M").time()
        if start <= now.time() <= end:
            return True
    return False

def is_instrument_tradeable(instrument: str) -> Tuple[bool, str]:
    if any(c in instrument for c in ["BTC","ETH","XRP","LTC"]):
        session_type = "CRYPTO"
    elif "XAU" in instrument:
        session_type = "XAU_USD"
    else:
        session_type = "FOREX"
    
    if check_market_hours(MARKET_SESSIONS[session_type]):
        return True, "Market open"
    return False, f"Instrument {instrument} outside market hours"

@handle_async_errors
async def get_current_price(instrument: str) -> float:
    session = await get_session()
    url = f"{OANDA_API_URL}/accounts/{OANDA_ACCOUNT_ID}/pricing?instruments={instrument}"
    async with session.get(url) as response:
        if response.status != 200:
            raise ValueError(f"Price fetch failed: {await response.text()}")
        data = await response.json()
        return float(data['prices'][0]['bids'][0]['price'])

##############################################################################
# Trade Execution
##############################################################################

def calculate_trade_size(instrument: str, percentage: float) -> Tuple[float, int]:
    if 'XAU' in instrument:
        precision = 2
        min_size = 1
        max_size = 500
        base_size = 10
    elif 'BTC' in instrument:
        precision = 8
        min_size = 0.01
        max_size = 100
        base_size = 0.5
    elif 'ETH' in instrument:
        precision = 8
        min_size = 0.1
        max_size = 1000
        base_size = 5
    else:
        precision = 0
        min_size = 1000
        max_size = BASE_POSITION
        base_size = BASE_POSITION
    
    leverage = INSTRUMENT_LEVERAGES.get(instrument, 1)
    trade_size = base_size * percentage * leverage
    trade_size = max(min_size, min(trade_size, max_size))
    
    if any(asset in instrument for asset in ['BTC', 'ETH', 'XAU']):
        trade_size = round(trade_size, precision)
    else:
        trade_size = int(round(trade_size))
    
    return trade_size, precision
    
@handle_async_errors
async def close_position(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    account_id = alert_data.get('account', OANDA_ACCOUNT_ID)
    symbol = alert_data['symbol']
    instrument = f"{symbol[:3]}_{symbol[3:]}".upper()
    request_id = str(uuid.uuid4())
    
    logger.info(f"[{request_id}] Attempting to close position for {instrument}")
    
    success, positions_data = await get_open_positions(account_id)
    if not success:
        logger.error(f"[{request_id}] Failed to fetch positions: {positions_data}")
        return False, positions_data
    
    position = next(
        (p for p in positions_data.get('positions', [])
         if p['instrument'] == instrument), None
    )
    
    if not position:
        logger.info(f"[{request_id}] No open position found for {instrument}")
        return True, {"message": f"No open position for {instrument}"}
    
    close_body = {}
    long_units = float(position['long'].get('units', '0'))
    short_units = float(position['short'].get('units', '0'))
    
    if alert_data['action'].upper() in ['CLOSE', 'CLOSE_LONG'] and long_units > 0:
        close_body["longUnits"] = "ALL"
    if alert_data['action'].upper() in ['CLOSE', 'CLOSE_SHORT'] and short_units < 0:
        close_body["shortUnits"] = "ALL"
        
    if not close_body:
        logger.info(f"[{request_id}] No matching position side to close")
        return True, {"message": "No matching position side to close"}
    
    url = f"{OANDA_API_URL}/accounts/{account_id}/positions/{instrument}/close"
    session = await get_session()
    
    for attempt in range(MAX_RETRIES):
        try:
            async with session.put(url, json=close_body) as response:
                if response.status == 200:
                    result = await response.json()
                    logger.info(f"[{request_id}] Position closed successfully: {result}")
                    return True, result
                
                error_content = await response.text()
                logger.error(f"[{request_id}] Close failed (attempt {attempt + 1}): {error_content}")
                
                if attempt < MAX_RETRIES - 1:
                    delay = BASE_DELAY * (2 ** attempt)
                    logger.warning(f"[{request_id}] Retrying close in {delay}s")
                    await asyncio.sleep(delay)
                    await get_session(force_new=True)
                    continue
                
                return False, {"error": error_content}
                
        except Exception as e:
            logger.error(f"[{request_id}] Error closing position (attempt {attempt + 1}): {str(e)}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(BASE_DELAY * (2 ** attempt))
                continue
            return False, {"error": str(e)}
    
    return False, {"error": "Failed to close position after maximum retries"}

@handle_async_errors
async def execute_trade(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
    units, precision = calculate_trade_size(instrument, alert_data['percentage'])
    
    try:
        entry_price = await get_current_price(instrument)
        if alert_data['action'].upper() == 'BUY':
            stop_price = entry_price * (1 - RISK_PERCENTAGE)
        else:
            units = -abs(units)
            stop_price = entry_price * (1 + RISK_PERCENTAGE)
    except Exception as e:
        logger.error(f"Failed to get price for stop loss: {str(e)}")
        return False, {"error": "Price check failed"}

    order_data = {
        "order": {
            "type": alert_data['orderType'],
            "instrument": instrument,
            "units": str(units),
            "timeInForce": alert_data['timeInForce'],
            "positionFill": "DEFAULT",
            "stopLossOnFill": {
                "timeInForce": "GTC",
                "price": f"{stop_price:.5f}"
            }
        }
    }
    
    session = await get_session()
    url = f"{OANDA_API_URL}/accounts/{alert_data.get('account', OANDA_ACCOUNT_ID)}/orders"
    
    for attempt in range(MAX_RETRIES):
        async with session.post(url, json=order_data) as response:
            if response.status == 201:
                result = await response.json()
                logger.info(f"Trade executed successfully: {result}")
                return True, result
            
            error_content = await response.text()
            if attempt < MAX_RETRIES - 1:
                delay = BASE_DELAY * (2 ** attempt)
                logger.warning(f"Retrying trade in {delay}s (attempt {attempt+1}/{MAX_RETRIES})")
                await asyncio.sleep(delay)
                await get_session(force_new=True)
                continue
            
            logger.error(f"Trade execution failed: {error_content}")
            return False, {
                "error": f"Trade execution failed: {response.status}",
                "response": error_content,
                "order_data": order_data
            }

@handle_async_errors
async def get_open_positions(account_id: str = OANDA_ACCOUNT_ID) -> Tuple[bool, Dict[str, Any]]:
    session = await get_session()
    url = f"{OANDA_API_URL}/accounts/{account_id}/openPositions"
    async with session.get(url) as response:
        if response.status == 200:
            return True, await response.json()
        return False, {"error": await response.text()}

##############################################################################
# Alert Handler
##############################################################################

class AlertHandler:
    def __init__(self):
        self.position_tracker = PositionTracker()
        self._lock = asyncio.Lock()
    
    async def process_alert(self, alert_data: Dict[str, Any]) -> bool:
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

                # Position closure logic
                if action in ['CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']:
                    logger.info(f"[{request_id}] Processing close request")
                    position = next(
                        (p for p in positions_data.get('positions', [])
                        if p['instrument'] == instrument), 
                        None
                    )

                # Existing position check
                success, positions_data = await get_open_positions(alert_data.get('account', OANDA_ACCOUNT_ID))
                if not success:
                    logger.error(f"[{request_id}] Position check failed: {positions_data}")
                    return False

                position = next(
                    (p for p in positions_data.get('positions', [])
                     if p['instrument'] == instrument), 
                    None
                )

                # Close opposite positions
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

alert_handler = AlertHandler()

##############################################################################
# FastAPI Setup
##############################################################################

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

##############################################################################
# API Endpoints
##############################################################################

@app.middleware("http")
async def log_requests(request: Request, call_next):
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

@app.post("/alerts")
async def handle_alert_endpoint(alert: AlertData):
    try:
        return await process_incoming_alert(alert.dict(), source="direct")
    except TradingError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in alert endpoint: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

def translate_tradingview_signal(data: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'symbol': data.get('ticker', data.get('symbol', '')),
        'action': data.get('action', 'CLOSE'),
        'timeframe': data.get('interval', '1M'),
        'orderType': data.get('orderType', 'MARKET'),
        'timeInForce': data.get('timeInForce', 'FOK'),
        'percentage': float(data.get('percentage', 1.0)),
        'account': data.get('account'),
        'id': data.get('id'),
        'comment': data.get('comment')
    }

@app.post("/tradingview")
async def handle_tradingview_webhook(request: Request):
    try:
        body = await request.json()
        logger.info(f"Received TradingView webhook: {json.dumps(body, indent=2)}")
        cleaned_data = translate_tradingview_signal(body)
        return await process_incoming_alert(cleaned_data, source="tradingview")
    except TradingError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON format: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in webhook: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

async def process_incoming_alert(data: Dict[str, Any], source: str) -> JSONResponse:
    request_id = str(uuid.uuid4())
    try:
        data['account'] = data.get('account', OANDA_ACCOUNT_ID)
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
    return {
        "status": "active",
        "version": "2.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "endpoints": ["/alerts", "/tradingview"]
    }

##############################################################################
# Lifespan Management
##############################################################################

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing application...")
    global _session
    session = None
    tracker = PositionTracker()
    
    try:
        await get_session(force_new=True)
        logger.info("Services initialized successfully")
        yield
    finally:
        logger.info("Shutting down services...")
        if session and not session.closed:
            await session.close()
        tracker.reconciliation_task.cancel()
        try:
            await tracker.reconciliation_task
        except asyncio.CancelledError:
            pass
        logger.info("Shutdown complete")

##############################################################################
# Main Entry Point
##############################################################################

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        log_config=None,
        timeout_keep_alive=65,
        reload=False
    )
