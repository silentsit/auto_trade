import os
import uuid
import asyncio
import aiohttp
import logging
import logging.handlers
import re
import math
import json
from datetime import datetime, timedelta
from pytz import timezone
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any, Tuple
from contextlib import asynccontextmanager
from pydantic import BaseModel, validator
from functools import wraps

##############################################################################
# 1. Error Handling Decorator
##############################################################################
def handle_async_errors(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try: 
            return await func(*args, **kwargs)
        except aiohttp.ClientError as e:
            logging.error(f"Network error: {str(e)}", exc_info=True)
            return False, {"error": f"Network error: {str(e)}"}
        except Exception as e:
            logging.error(f"Unexpected error: {str(e)}", exc_info=True)
            return False, {"error": f"Unexpected error: {str(e)}"}
    return wrapper

##############################################################################
# 2. Environment and Logging Setup
##############################################################################
def get_env_or_raise(key: str, default: Optional[str] = None) -> str:
    value = os.getenv(key, default)
    if not value: 
        raise ValueError(f"Missing env var: {key}")
    return value

OANDA_API_TOKEN = get_env_or_raise('OANDA_API_TOKEN')
OANDA_ACCOUNT_ID = get_env_or_raise('OANDA_ACCOUNT_ID')
OANDA_API_URL = get_env_or_raise('OANDA_API_URL', 'https://api-fxtrade.oanda.com/v3')
ALLOWED_ORIGINS = get_env_or_raise("ALLOWED_ORIGINS", "https://your-tradingview-domain.com").split(",")

def setup_logging():
    logger = logging.getLogger('trading_bot')
    logger.setLevel(logging.INFO)
    handler = logging.handlers.RotatingFileHandler(
        'trading.log', maxBytes=10*1024*1024, backupCount=5
    )
    handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    logger.addHandler(handler)
    return logger

logger = setup_logging()

##############################################################################
# 3. Session Management
##############################################################################
session: Optional[aiohttp.ClientSession] = None

async def get_session(force_new: bool = False) -> aiohttp.ClientSession:
    global session
    if not session or session.closed or force_new:
        if session and not session.closed:
            await session.close()
        session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=100),
            headers={"Authorization": f"Bearer {OANDA_API_TOKEN}"},
            timeout=aiohttp.ClientTimeout(total=30)
        )
    return session

async def ensure_session() -> Tuple[bool, Optional[str]]:
    try:
        await get_session()
        return True, None
    except Exception as e:
        return False, str(e)

##############################################################################
# 4. FastAPI App Setup
##############################################################################
app = FastAPI(
    title="OANDA Trading Bot",
    description="Advanced async trading bot",
    version="1.0.0"
)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting application...")
    await get_session(force_new=True)
    yield
    logger.info("Shutting down application...")
    if session and not session.closed:
        await session.close()
        logger.info("Closed HTTP session")

app.router.lifespan_context = lifespan

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
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}"}
        )

# Error Handlers
@app.exception_handler(404)
async def not_found_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=404,
        content={"message": f"Endpoint {request.url.path} not found"}
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"message": "Internal server error"}
    )

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutdown initiated")
    if session and not session.closed:
        await session.close()
        logger.info("Closed HTTP session")

##############################################################################
# 5. Trading Constants and Configurations
##############################################################################
SPREAD_THRESHOLD_FOREX = 0.001
SPREAD_THRESHOLD_CRYPTO = 0.008
MAX_RETRIES = 3
BASE_DELAY = 1.0
FOREX_BASE_POSITION = 100000
CRYPTO_BASE_POSITION = 1  # Smaller base for crypto
DEFAULT_FOREX_PRECISION = 5
DEFAULT_CRYPTO_PRECISION = 8  # Higher precision for crypto
DEFAULT_MIN_ORDER_SIZE = 0.001

INSTRUMENT_LEVERAGES = {
    "USD_CHF": 20, "EUR_USD": 20, "GBP_USD": 20, "USD_JPY": 20,
    "AUD_USD": 20, "USD_THB": 20, "CAD_CHF": 20, "NZD_USD": 20,
    "BTC_USD": 2, "ETH_USD": 2, "XRP_USD": 2, "LTC_USD": 2, "XAU_USD": 1
}

INSTRUMENT_PRECISION = {
    "EUR_USD": 5, "GBP_USD": 5, "USD_JPY": 3, "AUD_USD": 5,
    "USD_THB": 5, "CAD_CHF": 5, "NZD_USD": 5, "BTC_USD": 8,
    "ETH_USD": 8, "XRP_USD": 8, "LTC_USD": 8, "XAU_USD": 2
}

MIN_ORDER_SIZES = {
    "EUR_USD": 1000, "GBP_USD": 1000, "AUD_USD": 1000, 
    "USD_THB": 1000, "CAD_CHF": 1000, "NZD_USD": 1000,
    "BTC_USD": 0.001, "ETH_USD": 0.01, "XRP_USD": 1.0, 
    "LTC_USD": 0.1, "XAU_USD": 1
}

MAX_ORDER_SIZES = {
    "XAU_USD": 10000, "BTC_USD": 1000, "ETH_USD": 500,
    "GBP_USD": 500000, "EUR_USD": 500000, "AUD_USD": 500000
}

##############################################################################
# 6. Data Models and Validators
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
        match = re.match(r'^(\d+)([mMhH])$', v)
        if not match: 
            raise ValueError("Invalid timeframe format")
        val, unit = match.groups()
        val = int(val)
        if unit.upper() == 'H' and val > 24: 
            raise ValueError("Max 24H timeframe")
        if unit.upper() == 'M' and val > 1440: 
            raise ValueError("Max 1440M timeframe")
        return f"{val*60 if unit.upper() == 'H' else val}"

    @validator('action')
    def validate_action(cls, v):
        if v.upper() not in ['BUY', 'SELL', 'CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']:
            raise ValueError("Invalid action")
        return v.upper()

    @validator('percentage')
    def validate_percentage(cls, v):
        if not 0 < v <= 1: 
            raise ValueError("Percentage 0-1 required")
        return v

    @validator('symbol')
    def validate_symbol(cls, v):
        instrument = f"{v[:3]}_{v[3:]}".upper()
        if instrument not in INSTRUMENT_LEVERAGES:
            raise ValueError(f"Invalid instrument {instrument}")
        return v.upper()

    @validator('timeInForce')
    def validate_time_in_force(cls, v):
        valid_values = ['FOK', 'IOC', 'GTC', 'GFD']
        if v.upper() not in valid_values:
            raise ValueError(f"Invalid timeInForce. Must be one of {valid_values}")
        return v.upper()

    @validator('orderType')
    def validate_order_type(cls, v):
        valid_types = ['MARKET', 'LIMIT', 'STOP', 'MARKET_IF_TOUCHED']
        if v.upper() not in valid_types:
            raise ValueError(f"Invalid orderType. Must be one of {valid_types}")
        return v.upper()

##############################################################################
# 7. Helper Functions
##############################################################################
def translate_tradingview_signal(data: Dict) -> Dict:
    """Convert TradingView webhook format to internal format"""
    if 'ticker' in data: 
        data['symbol'] = data.pop('ticker')
    if 'interval' in data: 
        data['timeframe'] = data.pop('interval')
    data.pop('exchange', None)
    data.pop('strategy', None)
    return data

##############################################################################
# 8. Core Trading Functions
##############################################################################
@handle_async_errors
async def execute_trade(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    required_fields = ['symbol', 'action', 'orderType', 'timeInForce', 'percentage']
    if missing := [f for f in required_fields if f not in alert_data]:
        return False, {"error": f"Missing fields: {missing}"}

    instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
    leverage = INSTRUMENT_LEVERAGES.get(instrument, 1)
    is_crypto = any(c in instrument for c in ['BTC', 'ETH', 'XRP', 'LTC'])
    precision = INSTRUMENT_PRECISION.get(instrument, DEFAULT_CRYPTO_PRECISION if is_crypto else DEFAULT_FOREX_PRECISION)
    
    try:
        percentage = float(alert_data['percentage'])
        base = CRYPTO_BASE_POSITION if is_crypto else FOREX_BASE_POSITION
        trade_size = base * percentage * leverage
        raw_units = trade_size
    except ValueError as e:
        return False, {"error": f"Calculation error: {str(e)}"}

    session_ok, error = await ensure_session()
    if not session_ok: 
        return False, {"error": error}

    price_success, price_data = await get_instrument_price(
        instrument, 
        alert_data.get('account', OANDA_ACCOUNT_ID)
    )
    if not price_success: 
        return False, price_data

    try:
        is_sell = alert_data['action'] == 'SELL'
        price_key = 'bids' if is_sell else 'asks'
        price = float(price_data['prices'][0][price_key][0]['price'])
        if price <= 0: 
            raise ValueError("Invalid price")
    except (KeyError, ValueError):
        return False, {"error": "Invalid price data"}

    # Check spread
    wide_spread, spread_size = check_spread_warning(price_data, instrument)
    if wide_spread:
        logger.warning(f"Wide spread detected ({spread_size}) for {instrument}")

    # Position size limits
    max_units = MAX_ORDER_SIZES.get(instrument)
    if max_units and abs(raw_units) > max_units:
        raw_units = math.copysign(max_units, raw_units)

    # Apply precision and rounding
    units = round(raw_units, precision) if is_crypto else int(round(raw_units))
    min_size = MIN_ORDER_SIZES.get(instrument, DEFAULT_MIN_ORDER_SIZE)
    
    if abs(units) < min_size:
        units = math.copysign(max(min_size, abs(units)), -1 if is_sell else 1)
        if max_units and abs(units) > max_units:
            units = math.copysign(max_units, units)

    if units == 0: 
        return False, {"error": "Zero units calculated"}

    units_str = f"{units:.{precision}f}" if is_crypto else str(int(units))
    
    order_data = {
        "order": {
            "type": alert_data['orderType'],
            "instrument": instrument,
            "units": units_str,
            "timeInForce": alert_data['timeInForce'],
            "positionFill": "DEFAULT"
        }
    }

    # Execute with retries
    for attempt in range(MAX_RETRIES):
        async with session.post(
            f"{OANDA_API_URL}/accounts/{alert_data.get('account', OANDA_ACCOUNT_ID)}/orders",
            json=order_data
        ) as response:
            if response.status == 201:
                order_response = await response.json()
                logger.info(f"Trade executed successfully: {order_response}")
                return True, order_response

            error_content = await response.text()
            try:
                error_data = json.loads(error_content)
                error_code = error_data.get('errorCode')
                
                if error_code == 'UNITS_LIMIT_EXCEEDED':
                    logger.error(f"Position size exceeds limits for {instrument}")
                    return False, {
                        "error": "Position size exceeds broker limits",
                        "max_allowed": max_units,
                        "attempted": abs(units)
                    }
                elif error_code in ['INSUFFICIENT_MARGIN', 'PRICE_DISTANCE_EXCEEDED', 'INSTRUMENT_NOT_TRADEABLE']:
                    logger.error(f"Trade rejected: {error_code}")
                    return False, {"error": error_code}
            except json.JSONDecodeError:
                logger.error(f"Could not parse error response: {error_content}")

            if attempt < MAX_RETRIES - 1:
                delay = BASE_DELAY * (2 ** attempt)
                logger.warning(f"Retrying trade in {delay}s")
                await asyncio.sleep(delay)
                await get_session(force_new=True)
            else:
                logger.error("All retry attempts failed")
                return False, {"error": error_content}

    return False, {"error": "Failed to execute trade after retries"}

@handle_async_errors
async def close_position(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    account_id = alert_data.get('account', OANDA_ACCOUNT_ID)
    instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
    
    success, positions = await get_open_positions(account_id)
    if not success: 
        return False, positions
    
    position = next((p for p in positions.get('positions', []) 
                    if p['instrument'] == instrument), None)
    if not position: 
        return False, {"error": "No position found"}
    
    close_payload = {}
    action = alert_data['action'].upper()
    long_units = float(position.get('long', {}).get('units', 0))
    short_units = float(position.get('short', {}).get('units', 0))
    
    if action == 'CLOSE_LONG' and long_units > 0:
        close_payload["longUnits"] = "ALL"
    elif action == 'CLOSE_SHORT' and short_units < 0:
        close_payload["shortUnits"] = "ALL"
    elif action == 'CLOSE':
        if long_units > 0: 
            close_payload["longUnits"] = "ALL"
        if short_units < 0: 
            close_payload["shortUnits"] = "ALL"
    
    async with session.put(
        f"{OANDA_API_URL}/accounts/{account_id}/positions/{instrument}/close",
        json=close_payload
    ) as response:
        if response.status == 200:
            result = await response.json()
            logger.info(f"Position closed successfully: {result}")
            return True, result
        error = await response.text()
        logger.error(f"Failed to close position: {error}")
        return False, {"error": error}

@handle_async_errors
async def validate_trade_direction(alert_data: Dict[str, Any]) -> Tuple[bool, str, bool]:
    action = alert_data['action'].upper()
    if action in ['CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']: 
        return True, None, True
    
    instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
    success, positions = await get_open_positions(
        alert_data.get('account', OANDA_ACCOUNT_ID)
    )
    if not success: 
        return False, "Position check failed", False
    
    for position in positions.get('positions', []):
        if position['instrument'] == instrument:
            long = float(position.get('long', {}).get('units', 0))
            short = float(position.get('short', {}).get('units', 0))
            if (action == 'BUY' and long > 0) or (action == 'SELL' and short < 0):
                return False, "Existing position conflict", False
    return True, None, False

##############################################################################
# 9. Position Management
##############################################################################
class PositionTracker:
    def __init__(self):
        self.positions: Dict[str, Dict] = {}
        self.bar_times: Dict[str, List] = {}
        self._lock = asyncio.Lock()
        
    async def record_position(self, symbol: str, action: str, timeframe: str):
        async with self._lock:
            current_time = datetime.now(timezone('Asia/Bangkok'))
            self.positions[symbol] = {
                'entry_time': current_time,
                'position_type': 'LONG' if action.upper() == 'BUY' else 'SHORT',
                'timeframe': timeframe,
                'last_update': current_time
            }
            logger.info(f"Recorded position for {symbol}: {self.positions[symbol]}")
    
    async def update_bars_held(self, symbol: str) -> int:
        async with self._lock:
            if symbol not in self.positions: 
                return 0
            pos = self.positions[symbol]
            elapsed = (datetime.now(timezone('Asia/Bangkok')) - pos['entry_time']).seconds
            bars_held = elapsed // (int(pos['timeframe']) * 60)
            return bars_held
    
    async def should_close_position(self, symbol: str, new_signal: str = None) -> bool:
        async with self._lock:
            if symbol not in self.positions: 
                return False
            bars = await self.update_bars_held(symbol)
            pos_type = self.positions[symbol]['position_type']
            
            # Close after 4 bars
            if bars >= 4: 
                return True
            
            # Close on opposing signal before 4 bars
            if new_signal and 0 < bars < 4:
                is_opposing = (
                    (pos_type == 'LONG' and new_signal == 'SELL') or
                    (pos_type == 'SHORT' and new_signal == 'BUY')
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
            if symbol in self.positions:
                self.positions.pop(symbol)
                logger.info(f"Cleared position for {symbol}")

##############################################################################
# 10. Market Operations
##############################################################################
@handle_async_errors
async def get_instrument_price(instrument: str, account_id: str) -> Tuple[bool, Dict[str, Any]]:
    """Fetch current price for an instrument"""
    async with session.get(
        f"{OANDA_API_URL}/accounts/{account_id}/pricing?instruments={instrument}"
    ) as response:
        if response.status != 200:
            error = await response.text()
            logger.error(f"Price fetch failed: {error}")
            return False, {"error": error}
        return True, await response.json()

@handle_async_errors
async def get_open_positions(account_id: str) -> Tuple[bool, Dict[str, Any]]:
    """Fetch all open positions for an account"""
    async with session.get(
        f"{OANDA_API_URL}/accounts/{account_id}/openPositions"
    ) as response:
        if response.status != 200:
            error = await response.text()
            logger.error(f"Position fetch failed: {error}")
            return False, {"error": error}
        return True, await response.json()

def check_spread_warning(pricing_data: Dict[str, Any], instrument: str) -> Tuple[bool, float]:
    """Check if spread exceeds threshold for instrument type"""
    try:
        bid = float(pricing_data['prices'][0]['bids'][0]['price'])
        ask = float(pricing_data['prices'][0]['asks'][0]['price'])
        spread = ask - bid
        threshold = SPREAD_THRESHOLD_CRYPTO if any(c in instrument for c in ['BTC','ETH']) else SPREAD_THRESHOLD_FOREX
        spread_percentage = spread/bid
        return (spread_percentage > threshold, spread)
    except (KeyError, ValueError) as e:
        logger.error(f"Spread check failed: {str(e)}")
        return False, 0.0

async def check_market_status(instrument: str) -> bool:
    """Check if market is open for trading"""
    now = datetime.now(timezone('Asia/Bangkok'))
    
    # Weekend check
    if now.weekday() >= 5:
        logger.info(f"Market closed (weekend) for {instrument}")
        return False
    
    # Crypto trades 24/7
    if any(c in instrument for c in ['BTC','ETH','XRP','LTC']):
        return True
    
    # Forex market hours (simplified)
    is_trading_hours = 5 <= now.hour < 21
    if not is_trading_hours:
        logger.info(f"Outside trading hours for {instrument}")
        return False
        
    return True

##############################################################################
# 11. Alert Handler
##############################################################################
class AlertHandler:
    def __init__(self):
        self.tracker = PositionTracker()
        self.lock = asyncio.Lock()

    async def process_alert(self, alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        request_id = str(uuid.uuid4())[:8]
        logger.info(f"[{request_id}] Processing alert: {alert_data}")

        async with self.lock:
            # Market status check
            instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
            market_open = await check_market_status(instrument)
            if not market_open:
                return False, {"error": "Market closed for instrument"}

            # Handle closing actions
            action = alert_data['action'].upper()
            if action in ['CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']:
                success, result = await close_position(alert_data)
                if success:
                    await self.tracker.clear_position(alert_data['symbol'])
                    logger.info(f"[{request_id}] Position closed successfully")
                return success, result

            # Check for automatic position closing
            should_close = await self.tracker.should_close_position(
                alert_data['symbol'], 
                action
            )
            if should_close:
                close_action = await self.tracker.get_close_action(alert_data['symbol'])
                close_data = alert_data.copy()
                close_data['action'] = close_action
                success, result = await close_position(close_data)
                if success:
                    await self.tracker.clear_position(alert_data['symbol'])
                    logger.info(f"[{request_id}] Auto-closed position")

            # Validate new trade direction
            valid, msg, _ = await validate_trade_direction(alert_data)
            if not valid:
                logger.warning(f"[{request_id}] Invalid trade direction: {msg}")
                return False, {"error": msg}

            # Execute trade with retries
            for attempt in range(MAX_RETRIES):
                trade_ok, trade_result = await execute_trade(alert_data)
                if trade_ok:
                    await self.tracker.record_position(
                        alert_data['symbol'],
                        alert_data['action'],
                        alert_data['timeframe']
                    )
                    logger.info(f"[{request_id}] Trade executed successfully")
                    return True, trade_result
                
                logger.warning(f"[{request_id}] Attempt {attempt+1} failed: {trade_result}")
                if attempt < MAX_RETRIES - 1:
                    delay = BASE_DELAY * (2 ** attempt)
                    await asyncio.sleep(delay)
            
            logger.error(f"[{request_id}] All trade attempts failed")
            return False, {"error": "All trade attempts failed"}

# Global alert handler instance
alert_handler = AlertHandler()

##############################################################################
# 12. API Endpoints
##############################################################################
@app.post("/tradingview")
async def tradingview_webhook(request: Request):
    """Handle TradingView webhook alerts"""
    try:
        data = await request.json()
        logger.info(f"Received TradingView webhook: {json.dumps(data, indent=2)}")
        
        # Validate required fields
        required_fields = ['symbol', 'action']
        missing_fields = [field for field in required_fields if not data.get(field)]
        if missing_fields:
            return JSONResponse(
                status_code=400,
                content={"error": f"Missing required fields: {missing_fields}"}
            )

        # Process alert
        translated_data = translate_tradingview_signal(data)
        success, result = await alert_handler.process_alert(translated_data)
        return JSONResponse(
            status_code=200 if success else 400,
            content=result
        )
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in webhook: {str(e)}")
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid JSON format"}
        )
    except Exception as e:
        logger.exception("Webhook processing failed")
        return JSONResponse(
            status_code=500,
            content={"error": f"Unexpected error: {str(e)}"}
        )

@app.post("/alerts")
async def handle_alert(alert_data: AlertData):
    """Handle direct alert submissions"""
    success, result = await alert_handler.process_alert(alert_data.dict())
    return JSONResponse(
        status_code=200 if success else 400,
        content=result
    )

@app.get("/check-config")
async def check_configuration():
    """Verify OANDA configuration and connectivity"""
    try:
        async with session.get(
            f"{OANDA_API_URL}/accounts/{OANDA_ACCOUNT_ID}"
        ) as response:
            account_info = await response.json() if response.status == 200 else None
            
        return {
            "status": "active",
            "account": OANDA_ACCOUNT_ID,
            "api_url": OANDA_API_URL,
            "account_info": account_info
        }
    except Exception as e:
        logger.exception("Configuration check failed")
        return JSONResponse(
            status_code=500,
            content={"error": str(e)}
        )

@app.get("/health")
async def health_check():
    """Basic health check endpoint"""
    return {
        "status": "active",
        "timestamp": datetime.utcnow().isoformat()
    }

##############################################################################
# 13. Application Entry Point
##############################################################################
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        log_config=None
    )
