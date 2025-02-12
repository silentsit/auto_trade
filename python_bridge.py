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
    """Sets up rotating file logging plus console logging."""
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
# 1. Constants and Configurations
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
}

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
# 2. Position Management
##############################################################################
@handle_async_errors
async def close_position(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    account_id = alert_data.get('account', OANDA_ACCOUNT_ID)
    symbol = alert_data['symbol']
    instrument = f"{symbol[:3]}_{symbol[3:]}".upper()
    
    # Fetch open positions
    success, positions_data = await get_open_positions(account_id)
    if not success:
        return False, positions_data
    
    # Find the position for the instrument
    position = next(
        (p for p in positions_data.get('positions', []) if p['instrument'] == instrument),
        None
    )
    if not position:
        return True, {"message": f"No open position for {instrument}"}
    
    # Determine units to close
    action = alert_data['action'].upper()
    long_units = float(position.get('long', {}).get('units', 0))
    short_units = float(position.get('short', {}).get('units', 0))
    
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
    
    # Send request to OANDA
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
# Alert Handler Class
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
        Process incoming alerts with enhanced logging
        """
        request_id = str(uuid.uuid4())
        if not alert_data:
            self.logger.error(f"[{request_id}] No alert data provided")
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

            # Validate trade direction
            self.logger.info(f"[{request_id}] Validating trade direction")
            is_valid, err_msg, is_closing_trade = await validate_trade_direction(alert_data)
            if not is_valid:
                self.logger.warning(f"[{request_id}] Trade validation failed: {err_msg}")
                return False
            self.logger.info(f"[{request_id}] Trade validation passed")

            # Determine if the action is to close a position
            action = alert_data['action'].upper()
            is_closing_action = action in ['CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']

            if is_closing_action:
                # Handle closing the position
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
                # Execute trade for opening positions
                self.logger.info(f"[{request_id}] Starting trade execution")
                for attempt in range(self.max_retries):
                    trade_ok, trade_result = await execute_trade(alert_data)
                    self.logger.info(f"[{request_id}] Trade execution attempt {attempt + 1} result: {json.dumps(trade_result, indent=2)}")
                    
                    if trade_ok:
                        await self.position_tracker.record_position(symbol, action, alert_data['timeframe'])
                        self.logger.info(f"[{request_id}] Alert processed successfully")
                        return True
                    else:
                        error_msg = trade_result.get('error', 'Unknown error')
                        if 'response' in trade_result:
                            self.logger.error(f"[{request_id}] OANDA response: {trade_result['response']}")
                        
                        if attempt < self.max_retries - 1:
                            delay = self.base_delay * (2 ** attempt)
                            self.logger.warning(f"[{request_id}] Trade failed; retrying in {delay}s: {error_msg}")
                            await asyncio.sleep(delay)
                        else:
                            self.logger.error(f"[{request_id}] Final attempt failed: {error_msg}")
                            return False

                self.logger.info(f"[{request_id}] Alert {alert_id} discarded after {self.max_retries} attempts")
                return False

# Instantiate a global alert handler
alert_handler = AlertHandler()

##############################################################################
# Alert Processing Functions
##############################################################################
async def process_incoming_alert(data: Dict[str, Any], source: str = "direct") -> JSONResponse:
    """
    Shared helper function to process incoming alerts from any endpoint.
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

##############################################################################
# FastAPI App Configuration
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
# Endpoints
##############################################################################
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

@app.get("/check-config")
async def check_configuration():
    """
    Endpoint to verify OANDA configuration and connectivity
    """
    try:
        # Check environment variables
        config = {
            "api_url": OANDA_API_URL,
            "account_id": OANDA_ACCOUNT_ID,
            "api_token_set": bool(OANDA_API_TOKEN),
        }
        
        # Test session creation
        session_ok, session_error = await ensure_session()
        if not session_ok:
            return JSONResponse(
                status_code=500,
                content={
                    "error": "Session creation failed",
                    "details": session_error,
                    "config": config
                }
            )
        
        # Test account access
        account_url = f"{OANDA_API_URL.rstrip('/')}/accounts/{OANDA_ACCOUNT_ID}"
        async with session.get(account_url) as response:
            if response.status != 200:
                error_content = await response.text()
                return JSONResponse(
                    status_code=response.status,
                    content={
                        "error": "Failed to access OANDA account",
                        "details": error_content,
                        "config": config
                    }
                )
            
            account_info = await response.json()
            return {
                "status": "ok",
                "config": config,
                "account_info": account_info
            }
            
    except Exception as e:
        logger.exception("Configuration check failed")
        return JSONResponse(
            status_code=500,
            content={
                "error": str(e),
                "config": config
            }
        )

@app.api_route("/", methods=["GET", "HEAD"])
async def health_check():
    return {"status": "active", "timestamp": datetime.utcnow().isoformat()}

##############################################################################
# Exception Handlers & Shutdown
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
# Main Entry Point
##############################################################################
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=False,
        workers=1,
        timeout_keep_alive=65,
        log_config=None
    )
