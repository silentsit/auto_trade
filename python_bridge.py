# main.py
"""
Consolidated trading bot application with improved position closing and crypto sizing
"""

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

# HTTP Session Configuration
CONNECT_TIMEOUT = 10
READ_TIMEOUT = 30
TOTAL_TIMEOUT = 45
HTTP_REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=TOTAL_TIMEOUT,
    connect=CONNECT_TIMEOUT,
    sock_read=READ_TIMEOUT
)
MAX_SIMULTANEOUS_CONNECTIONS = 100

# Global session
session: Optional[aiohttp.ClientSession] = None

##############################################################################
# FastAPI App Creation 
##############################################################################

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application, initializing services...")
    await get_session(force_new=True)
    yield
    logger.info("Shutting down application...")
    if session and not session.closed:
        await session.close()

app = FastAPI(
    title="OANDA Trading Bot",
    description="Advanced async trading bot using FastAPI and aiohttp",
    version="1.2.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Trading Constants
SPREAD_THRESHOLD_FOREX = 0.001
SPREAD_THRESHOLD_CRYPTO = 0.008
MAX_RETRIES = 3
BASE_DELAY = 1.0
BASE_POSITION = 100000

# Instrument Configurations
INSTRUMENT_LEVERAGES = {
    "USD_CHF": 20, "EUR_USD": 20, "GBP_USD": 20,
    "USD_JPY": 20, "AUD_USD": 20, "USD_THB": 20,
    "CAD_CHF": 20, "NZD_USD": 20, "BTC_USD": 2,
    "ETH_USD": 2, "XRP_USD": 2, "LTC_USD": 2,
    "XAU_USD": 1
}

##############################################################################
# Logging Setup (Original Implementation)
##############################################################################

def setup_logging():
    try:
        log_dir = '/opt/render/project/src/logs'
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'trading_bot.log')
    except Exception as e:
        log_file = 'trading_bot.log'

    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - [%(levelname)s] - %(message)s'
    )
    
    file_handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10*1024*1024, backupCount=5
    )
    file_handler.setFormatter(log_format)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return logging.getLogger('trading_bot')

logger = setup_logging()

##############################################################################
# Error Handling & Session Management (Hybrid)
##############################################################################

def handle_async_errors(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except aiohttp.ClientError as e:
            logger.error(f"Network error in {func.__name__}: {str(e)}", exc_info=True)
            return False, {"error": f"Network error: {str(e)}"}
        except asyncio.TimeoutError as e:
            logger.error(f"Timeout in {func.__name__}: {str(e)}", exc_info=True)
            return False, {"error": "Request timed out"}
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {str(e)}", exc_info=True)
            return False, {"error": f"Unexpected error: {str(e)}"}
    return wrapper

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
        
        # Hybrid headers: RFC3339 from Claude + original auth
        session = aiohttp.ClientSession(
            connector=connector,
            timeout=HTTP_REQUEST_TIMEOUT,
            headers={
                "Authorization": f"Bearer {OANDA_API_TOKEN}",
                "Content-Type": "application/json",
                "Accept-Datetime-Format": "RFC3339"  # From Claude
            }
        )
    return session

##############################################################################
# Models (Hybrid Validation)
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
        
        # Hybrid symbol handling: Claude's normalization + user's metal detection
        v = v.upper().replace('/', '_')  # From Claude
        if v in ['XAUUSD', 'XAUSD']:     # From user
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
# Position Tracking (Original Implementation)
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

    async def clear_position(self, symbol: str):
        async with self._lock:
            self.positions.pop(symbol, None)
            self.bar_times.pop(symbol, None)
            logger.info(f"Cleared position tracking for {symbol}")

##############################################################################
# Market Utilities (Original + Hybrid Spread Checks)
##############################################################################

def is_market_open() -> Tuple[bool, str]:
    current_time = datetime.now(timezone('Asia/Bangkok'))
    wday = current_time.weekday()
    if (wday == 5 and current_time.hour >= 5) or (wday == 6) or (wday == 0 and current_time.hour < 5):
        return False, "Weekend closure"
    return True, "Market open"

@handle_async_errors
async def get_instrument_price(instrument: str, account_id: str) -> Tuple[bool, Dict[str, Any]]:
    session = await get_session()
    url = f"{OANDA_API_URL}/accounts/{account_id}/pricing?instruments={instrument}"
    async with session.get(url) as response:
        if response.status != 200:
            return False, {"error": await response.text()}
        return True, await response.json()

@handle_async_errors
async def get_open_positions(account_id: str) -> Tuple[bool, Dict[str, Any]]:
    session = await get_session()
    url = f"{OANDA_API_URL}/accounts/{account_id}/openPositions"
    async with session.get(url) as response:
        if response.status != 200:
            return False, {"error": await response.text()}
        return True, await response.json()

##############################################################################
# Trade Execution (Hybrid Calculation Engine)
##############################################################################

def calculate_trade_size(instrument: str, percentage: float) -> Tuple[float, int]:
    """Hybrid calculation with user's metal sizes + Claude's crypto precision"""
    if 'XAU' in instrument:
        # User's metal parameters with realistic sizing
        precision = 2
        min_size = 1       # Minimum gold order size (1 ounce)
        max_size = 500     # User's preferred maximum
        base_size = 10     # Base position size
    elif 'BTC' in instrument:
        # Claude's crypto precision with user's leverage
        precision = 8      # Bitcoin needs 8 decimals
        min_size = 0.01    # Minimum BTC order size
        max_size = 100     # Maximum BTC order size
        base_size = 0.5    # Base position size
    elif 'ETH' in instrument:
        precision = 8      # Ethereum precision
        min_size = 0.1     # Minimum ETH order size
        max_size = 1000    # Maximum ETH order size
        base_size = 5      # Base position size
    else:  # Forex
        precision = 0      # Integer units for forex
        min_size = 1000
        max_size = BASE_POSITION
        base_size = BASE_POSITION
    
    # Leverage handling from user's explicit definitions
    leverage = INSTRUMENT_LEVERAGES.get(instrument, 1)
    trade_size = base_size * percentage * leverage
    
    # Apply constraints
    trade_size = max(min_size, min(trade_size, max_size))
    
    # Rounding logic combining both approaches
    if any(asset in instrument for asset in ['BTC', 'ETH', 'XAU']):
        trade_size = round(trade_size, precision)
    else:
        trade_size = int(round(trade_size))
    
    return trade_size, precision

@handle_async_errors
async def close_position(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Improved closing from Claude with user's position tracking"""
    account_id = alert_data.get('account', OANDA_ACCOUNT_ID)
    symbol = alert_data['symbol']
    instrument = f"{symbol[:3]}_{symbol[3:]}".upper()
    request_id = str(uuid.uuid4())
    
    logger.info(f"[{request_id}] Attempting to close position for {instrument}")
    
    # Fetch current positions
    success, positions_data = await get_open_positions(account_id)
    if not success:
        logger.error(f"[{request_id}] Failed to fetch positions: {positions_data}")
        return False, positions_data
    
    # Find matching position
    position = next(
        (p for p in positions_data.get('positions', []) 
         if p['instrument'] == instrument),
        None
    )
    
    if not position:
        logger.info(f"[{request_id}] No open position found for {instrument}")
        return True, {"message": f"No open position for {instrument}"}
    
    # Prepare close request
    action = alert_data['action'].upper()
    long_units = float(position['long'].get('units', '0'))
    short_units = float(position['short'].get('units', '0'))
    
    close_body = {}
    
    # Always close entire position regardless of size
    if action in ['CLOSE', 'CLOSE_LONG'] and long_units > 0:
        close_body["longUnits"] = "ALL"
    if action in ['CLOSE', 'CLOSE_SHORT'] and short_units < 0:
        close_body["shortUnits"] = "ALL"
        
    if not close_body:
        logger.info(f"[{request_id}] No matching position side to close")
        return True, {"message": "No matching position side to close"}
    
    # Execute close request with retries
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
                logger.error(f"[{request_id}] Close position failed (attempt {attempt + 1}): {error_content}")
                
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
    """Hybrid execution with both solutions' best practices"""
    instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
    
    # Calculate size using hybrid calculator
    units, precision = calculate_trade_size(
        instrument, 
        alert_data['percentage']
    )
    
    # Adjust for sell orders
    if alert_data['action'].upper() == 'SELL':
        units = -abs(units)
    
    session = await get_session()
    url = f"{OANDA_API_URL}/accounts/{alert_data.get('account', OANDA_ACCOUNT_ID)}/orders"
    
    # Position fill from Claude's implementation
    order_data = {
        "order": {
            "type": alert_data['orderType'],
            "instrument": instrument,
            "units": str(units),
            "timeInForce": alert_data['timeInForce'],
            "positionFill": "DEFAULT"
        }
    }
    
    logger.info(f"Executing trade for {instrument}: {order_data}")
    
    # Retry logic from original solution
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
    
    return False, {"error": "Failed to execute trade after maximum retries"}

##############################################################################
# Alert Processing (Hybrid Implementation)
##############################################################################

class AlertHandler:
    """Combined handler with both solutions' logic"""
    def __init__(self):
        self.position_tracker = PositionTracker()
        self._lock = asyncio.Lock()
    
    async def process_alert(self, alert_data: Dict[str, Any]) -> bool:
        request_id = str(uuid.uuid4())
        if not alert_data:
            logger.error(f"[{request_id}] No alert data provided")
            return False

        async with self._lock:
            action = alert_data['action'].upper()
            
            # Priority close handling from Claude
            if action in ['CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']:
                logger.info(f"[{request_id}] Processing close position request")
                success, result = await close_position(alert_data)
                if success:
                    await self.position_tracker.clear_position(alert_data['symbol'])
                return success
            
            # Market check from original
            instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
            is_tradeable, status_msg = is_market_open()
            if not is_tradeable:
                logger.warning(f"[{request_id}] Market not tradeable: {status_msg}")
                return False

            # Execute trade with hybrid engine
            success, result = await execute_trade(alert_data)
            if success:
                await self.position_tracker.record_position(
                    alert_data['symbol'],
                    action,
                    alert_data['timeframe']
                )
            return success

# Create global alert handler
alert_handler = AlertHandler()

##############################################################################
# API Endpoints (Hybrid with Enhanced Error Handling)
##############################################################################

@app.middleware("http")
async def log_requests(request: Request, call_next):
    request_id = str(uuid.uuid4())
    
    try:
        if request.method != "HEAD":
            body = await request.body()
            logger.debug(f"[{request_id}] Body: {body.decode()}")
            async def receive():
                return {"type": "http.request", "body": body}
            request._receive = receive
        
        logger.info(f"[{request_id}] {request.method} {request.url}")
        logger.debug(f"[{request_id}] Headers: {dict(request.headers)}")
        
        response = await call_next(request)
        logger.info(f"[{request_id}] Response: {response.status_code}")
        return response
        
    except Exception as e:
        logger.error(f"[{request_id}] Error processing request: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "message": "Internal server error",
                "request_id": request_id
            }
        )

@app.post("/alerts")
async def handle_alert_endpoint(alert: AlertData):
    return await process_incoming_alert(alert.dict(), source="direct")

def translate_tradingview_signal(data: Dict[str, Any]) -> Dict[str, Any]:
    allowed_fields = {
        'symbol', 'action', 'timeframe', 'orderType', 'timeInForce',
        'percentage', 'account', 'id', 'comment'
    }
    
    cleaned_data = {k: v for k, v in data.items() if k in allowed_fields}
    
    if 'symbol' not in cleaned_data:
        cleaned_data['symbol'] = data.get('ticker', '')
    if 'timeframe' not in cleaned_data:
        cleaned_data['timeframe'] = data.get('interval', '1M')
    
    if 'orderType' not in cleaned_data:
        cleaned_data['orderType'] = 'MARKET'
    if 'timeInForce' not in cleaned_data:
        cleaned_data['timeInForce'] = 'FOK'
    if 'percentage' not in cleaned_data:
        cleaned_data['percentage'] = 1.0
    
    return cleaned_data

@app.post("/tradingview")
async def handle_tradingview_webhook(request: Request):
    try:
        body = await request.json()
        logger.info(f"Received TradingView webhook: {json.dumps(body, indent=2)}")
        cleaned_data = translate_tradingview_signal(body)
        logger.info(f"Cleaned webhook data: {json.dumps(cleaned_data, indent=2)}")
        return await process_incoming_alert(cleaned_data, source="tradingview")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in TradingView webhook: {str(e)}")
        return JSONResponse(
            status_code=400,
            content={
                "error": "Invalid JSON format",
                "details": str(e)
            }
        )

async def process_incoming_alert(data: Dict[str, Any], source: str = "direct") -> JSONResponse:
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Processing {source} alert: {json.dumps(data, indent=2)}")

    try:
        if not data.get('account'):
            data['account'] = OANDA_ACCOUNT_ID

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

@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {
        "status": "active",
        "version": "1.2.0",
        "timestamp": datetime.utcnow().isoformat()
    }

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
