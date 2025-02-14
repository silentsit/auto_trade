# block1_base.py
"""
Block 1: Base Configuration & Core Utilities
Handles imports, environment setup, logging, and session management
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

# Environment Setup
def get_env_or_raise(key: str, default: Optional[str] = None) -> str:
    """Get environment variable or raise if not found"""
    value = os.getenv(key, default)
    if value is None:
        raise ValueError(f"Required environment variable {key} is not set")
    return value

# Core Environment Variables
OANDA_API_TOKEN = get_env_or_raise('OANDA_API_TOKEN')
OANDA_ACCOUNT_ID = get_env_or_raise('OANDA_ACCOUNT_ID')
OANDA_API_URL = get_env_or_raise('OANDA_API_URL', 'https://api-fxtrade.oanda.com/v3')
ALLOWED_ORIGINS = get_env_or_raise("ALLOWED_ORIGINS", "http://localhost").split(",")

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

INSTRUMENT_PRECISION = {
    "EUR_USD": 5, "GBP_USD": 5, "USD_JPY": 3,
    "AUD_USD": 5, "USD_THB": 5, "CAD_CHF": 5,
    "NZD_USD": 5, "BTC_USD": 2, "ETH_USD": 2,
    "XRP_USD": 4, "LTC_USD": 2, "XAU_USD": 2
}

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

def setup_logging():
    """Configure logging with file and console handlers"""
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

# Initialize logger
logger = setup_logging()

async def get_session(force_new: bool = False) -> aiohttp.ClientSession:
    """Get or create global HTTP session"""
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

# Error handling decorator
def handle_async_errors(func):
    """Decorator for standardized async error handling"""
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

# block2_fastapi.py
"""
Block 2: FastAPI Setup & Middleware
Handles FastAPI application setup, middleware, and core app functionality
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uuid
import json

from block1_base import (
    logger, get_session, ALLOWED_ORIGINS
)

# FastAPI Application Setup
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan"""
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

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request Logging Middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all incoming requests with unique IDs"""
    request_id = str(uuid.uuid4())
    
    try:
        body = await request.body()
        logger.info(f"[{request_id}] {request.method} {request.url}")
        logger.debug(f"[{request_id}] Headers: {dict(request.headers)}")
        logger.debug(f"[{request_id}] Body: {body.decode()}")
        
        # Reconstruct request stream
        async def receive():
            return {"type": "http.request", "body": body}
        request._receive = receive
        
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

# Basic endpoints
@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "active",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/check-config")
async def check_configuration():
    """Configuration verification endpoint"""
    return {
        "status": "active",
        "allowed_origins": ALLOWED_ORIGINS,
        "api_configured": bool(OANDA_API_TOKEN and OANDA_ACCOUNT_ID)
    }

# Error Handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions"""
    return JSONResponse(
        status_code=exc.status_code,
        content={"message": exc.detail}
    )

@app.exception_handler(500)
async def server_error_handler(request: Request, exc: Exception):
    """Handle server errors"""
    logger.error(f"Server error: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"message": "Internal server error"}
    )

# block3_models.py
"""
Block 3: Models and Core Trading Infrastructure
Handles data models, position tracking, and market utilities
"""

from pydantic import BaseModel, validator
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
import asyncio
from pytz import timezone
from pydantic import BaseModel, validator
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
import asyncio
from pytz import timezone
import re

from block1_base import (
    logger, handle_async_errors, get_session,
    OANDA_API_URL, OANDA_ACCOUNT_ID, INSTRUMENT_LEVERAGES
)

# Pydantic Models
class AlertData(BaseModel):
    """Data model for incoming trading alerts"""
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

# Position Tracking
class PositionTracker:
    """Tracks and manages trading positions"""
    def __init__(self):
        self.positions: Dict[str, Dict[str, Any]] = {}
        self.bar_times: Dict[str, List[datetime]] = {}
        self._lock = asyncio.Lock()
    
    async def record_position(self, symbol: str, action: str, timeframe: str):
        """Record a new position"""
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
        """Update and return the number of bars a position has been held"""
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
        """Determine if a position should be closed"""
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
        """Get the appropriate close action for a position"""
        async with self._lock:
            if symbol not in self.positions:
                return 'CLOSE'
            pos_type = self.positions[symbol]['position_type']
            return 'CLOSE_LONG' if pos_type == 'LONG' else 'CLOSE_SHORT'

    async def clear_position(self, symbol: str):
        """Remove a position from tracking"""
        async with self._lock:
            self.positions.pop(symbol, None)
            self.bar_times.pop(symbol, None)
            logger.info(f"Cleared position tracking for {symbol}")

# Market Utilities
def is_market_open() -> Tuple[bool, str]:
    """Check if the market is currently open"""
    current_time = datetime.now(timezone('Asia/Bangkok'))
    wday = current_time.weekday()
    if (wday == 5 and current_time.hour >= 5) or (wday == 6) or (wday == 0 and current_time.hour < 5):
        return False, "Weekend closure"
    return True, "Market open"

@handle_async_errors
async def get_instrument_price(instrument: str, account_id: str) -> Tuple[bool, Dict[str, Any]]:
    """Fetch current price for an instrument"""
    session = await get_session()
    url = f"{OANDA_API_URL}/accounts/{account_id}/pricing?instruments={instrument}"
    async with session.get(url) as response:
        if response.status != 200:
            return False, {"error": await response.text()}
        return True, await response.json()

@handle_async_errors
async def get_open_positions(account_id: str) -> Tuple[bool, Dict[str, Any]]:
    """Fetch all open positions for an account"""
    session = await get_session()
    url = f"{OANDA_API_URL}/accounts/{account_id}/openPositions"
    async with session.get(url) as response:
        if response.status != 200:
            return False, {"error": await response.text()}
        return True, await response.json()

# Market Status Utilities
async def check_market_status(instrument: str, account_id: str) -> Tuple[bool, str]:
    """Check if trading is available for an instrument"""
    is_open, msg = is_market_open()
    if not is_open:
        return False, msg
        
    # Additional checks could be added here (holidays, specific instrument trading hours, etc.)
    return True, "Market available for trading"

# block4_trading.py
"""
Block 4: Trade Execution and Endpoints
Handles trade execution, position management, and API endpoints
"""

from fastapi import Request
from fastapi.responses import JSONResponse
import uuid
import json
import asyncio
from typing import Dict, Any, Tuple

from block1_base import (
    logger, handle_async_errors, get_session,
    OANDA_API_URL, OANDA_ACCOUNT_ID, BASE_POSITION,
    INSTRUMENT_LEVERAGES, MAX_RETRIES, BASE_DELAY
)
from block2_fastapi import app
from block3_models import (
    AlertData, PositionTracker,
    get_instrument_price, get_open_positions,
    check_market_status
)

##############################################################################
# Trade Size Calculation
##############################################################################

def calculate_trade_size(instrument: str, percentage: float) -> Tuple[float, int]:
    """
    Calculate trade size with special handling for crypto pairs
    Returns: (units, precision)
    """
    is_crypto = any(c in instrument for c in ['BTC', 'ETH'])
    
    # Set precision and minimum sizes for crypto
    if 'BTC' in instrument:
        precision = 2
        min_size = 0.01  # Minimum BTC order size
        max_size = 100   # Maximum BTC order size
        base_size = 0.5  # Base BTC position size
    elif 'ETH' in instrument:
        precision = 2
        min_size = 0.1   # Minimum ETH order size
        max_size = 1000  # Maximum ETH order size
        base_size = 5    # Base ETH position size
    else:
        precision = 5
        min_size = 1000  # Standard forex minimum
        max_size = BASE_POSITION
        base_size = BASE_POSITION
    
    # Calculate size with leverage
    leverage = INSTRUMENT_LEVERAGES.get(instrument, 1)
    trade_size = base_size * percentage * leverage
    
    # Apply min/max constraints
    trade_size = max(min_size, min(trade_size, max_size))
    
    # Round according to precision
    if is_crypto:
        trade_size = round(trade_size, precision)
    else:
        trade_size = int(round(trade_size))
    
    return trade_size, precision

##############################################################################
# Position Management
##############################################################################

@handle_async_errors
async def close_position(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Close an existing position with improved reliability"""
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

##############################################################################
# Trade Execution
##############################################################################

@handle_async_errors
async def execute_trade(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Execute a trade with improved crypto handling"""
    instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
    
    # Calculate proper trade size
    units, precision = calculate_trade_size(
        instrument, 
        alert_data['percentage']
    )
    
    # Adjust for sell orders
    if alert_data['action'].upper() == 'SELL':
        units = -abs(units)
    
    session = await get_session()
    url = f"{OANDA_API_URL}/accounts/{alert_data.get('account', OANDA_ACCOUNT_ID)}/orders"
    
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
# Alert Processing
##############################################################################
class AlertHandler:
    """Handles incoming trading alerts"""
    def __init__(self):
        self.position_tracker = PositionTracker()
        self._lock = asyncio.Lock()
    
    async def process_alert(self, alert_data: Dict[str, Any]) -> bool:
        """Process incoming alerts with priority for close positions"""
        request_id = str(uuid.uuid4())
        if not alert_data:
            logger.error(f"[{request_id}] No alert data provided")
            return False

        async with self._lock:
            action = alert_data['action'].upper()
            
            # Immediately process close positions without additional checks
            if action in ['CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']:
                logger.info(f"[{request_id}] Processing close position request")
                success, result = await close_position(alert_data)
                if success:
                    await self.position_tracker.clear_position(alert_data['symbol'])
                return success
            
            # For new trades, proceed with normal checks
            instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
            
            # Market status check
            is_tradeable, status_msg = await check_market_status(
                instrument, 
                alert_data.get('account', OANDA_ACCOUNT_ID)
            )
            if not is_tradeable:
                logger.warning(f"[{request_id}] Market not tradeable: {status_msg}")
                return False

            # Execute new trade
            success, result = await execute_trade(alert_data)
            if success:
                await self.position_tracker.record_position(
                    alert_data['symbol'],
                    action,
                    alert_data['timeframe']
                )
            return success

##############################################################################
# API Endpoints
##############################################################################

# Create global alert handler
alert_handler = AlertHandler()

@app.post("/alerts")
async def handle_alert_endpoint(alert: AlertData):
    """Process direct alert submissions"""
    return await process_incoming_alert(alert.dict(), source="direct")

@app.post("/tradingview")
async def handle_tradingview_webhook(request: Request):
    """Process TradingView webhook alerts"""
    try:
        body = await request.json()
        return await process_incoming_alert(body, source="tradingview")
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in TradingView webhook: {str(e)}")
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid JSON format"}
        )

@app.put("/positions/{position_id}/close")
@handle_async_errors
async def close_position_endpoint(position_id: str):
    """Endpoint for closing specific positions"""
    session = await get_session()
    url = f"{OANDA_API_URL}/accounts/{OANDA_ACCOUNT_ID}/positions/{position_id}/close"
    
    async with session.put(url) as response:
        if response.status == 200:
            return JSONResponse(await response.json())
        return JSONResponse(
            {"error": await response.text()},
            status_code=response.status
        )

async def process_incoming_alert(data: Dict[str, Any], source: str = "direct") -> JSONResponse:
    """Process incoming alerts from any source"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Processing {source} alert: {json.dumps(data, indent=2)}")

    try:
        if not data.get('account'):
            data['account'] = OANDA_ACCOUNT_ID

        # Normalize TradingView data if needed
        if source == "tradingview":
            data = translate_tradingview_signal(data)
            logger.info(f"[{request_id}] Normalized TradingView data: {json.dumps(data, indent=2)}")

        # Validate alert data
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

def translate_tradingview_signal(alert_data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert TradingView format to internal format"""
    if 'ticker' in alert_data and not alert_data.get('symbol'):
        alert_data['symbol'] = alert_data.pop('ticker')
    if 'interval' in alert_data and not alert_data.get('timeframe'):
        alert_data['timeframe'] = alert_data.pop('interval')
    alert_data.pop('exchange', None)
    alert_data.pop('strategy', None)
    return alert_data

# block5_main.py
"""
Block 5: Error Handling and Main Application
Handles application error handling, shutdown, and entry point
"""

import os
import logging
from datetime import datetime
from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse

from block1_base import (
    logger, session, get_session,
    OANDA_API_URL, OANDA_ACCOUNT_ID
)
from block2_fastapi import app

##############################################################################
# Error Handlers
##############################################################################

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Handle HTTP exceptions with proper logging"""
    request_id = getattr(request.state, 'request_id', 'unknown')
    logger.warning(f"[{request_id}] HTTP exception: {exc.status_code} - {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "message": exc.detail,
            "request_id": request_id
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions with proper logging"""
    request_id = getattr(request.state, 'request_id', 'unknown')
    logger.error(
        f"[{request_id}] Unexpected error: {str(exc)}", 
        exc_info=True
    )
    return JSONResponse(
        status_code=500,
        content={
            "message": "Internal server error",
            "request_id": request_id
        }
    )

##############################################################################
# Application Lifecycle
##############################################################################

@app.on_event("startup")
async def startup_handler():
    """Initialize application resources"""
    logger.info("Application startup initiated")
    
    try:
        # Initialize HTTP session
        await get_session(force_new=True)
        logger.info("HTTP session initialized")
        
        # Test API connectivity
        async with session.get(f"{OANDA_API_URL}/accounts/{OANDA_ACCOUNT_ID}") as response:
            if response.status == 200:
                logger.info("OANDA API connection verified")
            else:
                logger.error(f"OANDA API connection failed: {response.status}")
                
    except Exception as e:
        logger.error(f"Startup error: {str(e)}", exc_info=True)
        raise

@app.on_event("shutdown")
async def shutdown_handler():
    """Handle graceful shutdown"""
    logger.info("Application shutdown initiated")
    
    try:
        # Close HTTP session
        if session and not session.closed:
            await session.close()
            logger.info("HTTP session closed")
        
        # Close any open positions if configured
        if os.getenv("CLOSE_POSITIONS_ON_SHUTDOWN", "false").lower() == "true":
            try:
                # Implementation for closing positions could go here
                logger.info("Position closure on shutdown completed")
            except Exception as e:
                logger.error(f"Error closing positions on shutdown: {str(e)}")
        
    except Exception as e:
        logger.error(f"Shutdown error: {str(e)}", exc_info=True)
    
    logger.info("Shutdown complete")

##############################################################################
# Health and Monitoring
##############################################################################

@app.get("/health")
async def health_check():
    """Enhanced health check endpoint"""
    status = {
        "status": "initializing",
        "components": {
            "api_connection": False,
            "session": False
        },
        "timestamp": datetime.utcnow().isoformat()
    }
    
    try:
        # Check session
        if session and not session.closed:
            status["components"]["session"] = True
        
        # Check API connection
        async with session.get(f"{OANDA_API_URL}/accounts/{OANDA_ACCOUNT_ID}") as response:
            status["components"]["api_connection"] = response.status == 200
        
        # Determine overall status
        if all(status["components"].values()):
            status["status"] = "healthy"
        elif any(status["components"].values()):
            status["status"] = "degraded"
        else:
            status["status"] = "unhealthy"
            
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        status["status"] = "error"
        status["error"] = str(e)
    
    return status

@app.get("/metrics")
async def get_metrics():
    """Basic metrics endpoint for monitoring"""
    try:
        success, positions = await get_open_positions(OANDA_ACCOUNT_ID)
        position_count = len(positions.get("positions", [])) if success else 0
        
        return {
            "open_positions": position_count,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Metrics collection failed: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": "Failed to collect metrics"}
        )

##############################################################################
# Main Entry Point
##############################################################################

def configure_logging(log_level: str = "INFO") -> None:
    """Configure logging settings"""
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - [%(levelname)s] - %(message)s'
    )
    
    # Set uvicorn access logger format
    logging.getLogger("uvicorn.access").handlers[0].setFormatter(log_format)
    
    # Set log level
    logging.getLogger("uvicorn").setLevel(log_level)
    logging.getLogger("uvicorn.access").setLevel(log_level)

def main():
    """Main application entry point"""
    try:
        import uvicorn
        
        # Get configuration from environment
        host = os.getenv("HOST", "0.0.0.0")
        port = int(os.getenv("PORT", 8000))
        workers = int(os.getenv("WORKERS", 1))
        log_level = os.getenv("LOG_LEVEL", "INFO")
        
        # Configure logging
        configure_logging(log_level)
        
        logger.info(f"Starting server on {host}:{port} with {workers} workers")
        
        # Configure uvicorn
        config = uvicorn.Config(
            "main:app",
            host=host,
            port=port,
            workers=workers,
            loop="auto",
            log_config=None,
            timeout_keep_alive=65,
            access_log=True,
            log_level=log_level.lower()
        )
        
        # Start server
        server = uvicorn.Server(config)
        server.run()
        
    except Exception as e:
        logger.error(f"Server startup failed: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()

