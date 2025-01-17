###
# Imports and Dependencies
###
import os
import uuid
import asyncio
import aiohttp
import logging
from logging.handlers import RotatingFileHandler
import math
import time
from datetime import datetime, timedelta
from pytz import timezone
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any, Union
from contextlib import asynccontextmanager
from pydantic import BaseModel, validator

###
# Configuration Constants
###
# Trading thresholds and limits
SPREAD_THRESHOLD_FOREX = 0.001  # 0.1% for forex
SPREAD_THRESHOLD_CRYPTO = 0.008  # 0.8% for crypto
MAX_RETRIES = 3
BASE_DELAY = 1.0  # Base delay in seconds
BASE_POSITION = 100000  # Base position size for trades

# HTTP settings
CONNECT_TIMEOUT = 10  # Connection timeout in seconds
READ_TIMEOUT = 30    # Read timeout in seconds
HTTP_REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=READ_TIMEOUT,
    connect=CONNECT_TIMEOUT,
    sock_read=READ_TIMEOUT
)

# Session retry settings
SESSION_RETRY_ATTEMPTS = 3
SESSION_RETRY_DELAY = 1.0

# Default trading settings
DEFAULT_FOREX_PRECISION = 5
DEFAULT_CRYPTO_PRECISION = 2
DEFAULT_MIN_ORDER_SIZE = 1000

# Environment variables
OANDA_API_TOKEN = os.getenv('OANDA_API_TOKEN')
OANDA_API_URL = os.getenv('OANDA_API_URL', 'https://api-fxtrade.oanda.com/v3')
OANDA_ACCOUNT_ID = os.getenv('OANDA_ACCOUNT_ID')
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'

###
# Logging Configuration
###
try:
    os.makedirs('/opt/render/project/src/logs', exist_ok=True)
    log_file = os.path.join('/opt/render/project/src/logs', 'trading_bot.log')
except Exception as e:
    print(f"Failed to create log directory: {e}")
    log_file = 'trading_bot.log'

max_bytes = 10 * 1024 * 1024  # 10MB
backup_count = 1  # Only keep 1 backup

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
    ]
)
logger = logging.getLogger(__name__)

###
# Session Management
###
session: Optional[aiohttp.ClientSession] = None

async def get_session(force_new: bool = False) -> aiohttp.ClientSession:
    """
    Get or create global aiohttp session with optimized settings.
    
    Args:
        force_new: Force creation of new session even if one exists
    Returns:
        aiohttp.ClientSession: Active session
    """
    global session
    if session is None or session.closed or force_new:
        if session and not session.closed:
            await session.close()
            
        connector = aiohttp.TCPConnector(
            limit=100,  # Connection pool limit
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

async def ensure_session() -> tuple[bool, Optional[str]]:
    """
    Ensure a valid session exists, attempting to recreate if necessary.
    
    Returns:
        tuple[bool, Optional[str]]: (success, error_message)
    """
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
    """Manages the lifecycle of the global session."""
    global session
    logger.info("Initializing global HTTP session")
    session = await get_session(force_new=True)
    yield
    if session and not session.closed:
        logger.info("Closing global HTTP session")
        await session.close()

# Initialize FastAPI app
app = FastAPI(
    title="OANDA Trading Bot",
    description="An async trading bot using FastAPI and aiohttp",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Validation Models
class AlertData(BaseModel):
    """Pydantic model for validating alert data."""
    symbol: str
    action: str
    orderType: Optional[str] = "MARKET"
    timeInForce: Optional[str] = "FOK"
    percentage: Optional[float] = 1.0
    account: Optional[str] = None
    id: Optional[str] = None
    comment: Optional[str] = None

    @validator('action')
    def validate_action(cls, v):
        """Validate action includes CLOSE operations."""
        valid_actions = ['BUY', 'SELL', 'CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']
        if v.upper() not in valid_actions:
            raise ValueError(f'Action must be one of {valid_actions}')
        return v.upper()

    @validator('percentage')
    def validate_percentage(cls, v):
        """Validate percentage is between 0 and 1."""
        if not 0 < v <= 1:
            raise ValueError('Percentage must be between 0 and 1')
        return v

    @validator('symbol')
    def validate_symbol(cls, v):
        """Validate trading instrument exists in configuration."""
        if len(v) < 6:
            raise ValueError('Symbol must be at least 6 characters')
        instrument = f"{v[:3]}_{v[3:]}"
        if instrument not in INSTRUMENT_LEVERAGES:
            raise ValueError(f'Invalid trading instrument: {instrument}')
        return v.upper()

    @validator('timeInForce')
    def validate_time_in_force(cls, v):
        """Validate order time in force parameter."""
        valid_values = ['FOK', 'IOC', 'GTC', 'GFD']
        if v and v.upper() not in valid_values:
            raise ValueError(f'timeInForce must be one of {valid_values}')
        return v.upper() if v else 'FOK'

    @validator('orderType')
    def validate_order_type(cls, v):
        """Validate order type."""
        valid_types = ['MARKET', 'LIMIT', 'STOP', 'MARKET_IF_TOUCHED']
        if v and v.upper() not in valid_types:
            raise ValueError(f'orderType must be one of {valid_types}')
        return v.upper() if v else 'MARKET'

###
# Core Trading Utilities
###
def get_instrument_leverage(instrument: str) -> float:
    """Return the leverage for a given instrument."""
    return INSTRUMENT_LEVERAGES.get(instrument, 20)

def is_market_open() -> tuple[bool, str]:
    """
    Check if market is open based on Bangkok time.
    
    Returns:
        tuple[bool, str]: (is_open, reason)
            - is_open: True if market is open, False otherwise
            - reason: Explanation of market status
    """
    current_time = datetime.now(timezone('Asia/Bangkok'))
    wday = current_time.weekday()
    hour = current_time.hour
    minute = current_time.minute

    # Saturday (wday=5) after 5am or Sunday (wday=6) => closed
    # Monday (wday=0) before 5am => still closed
    if (wday == 5 and hour >= 5) or (wday == 6) or (wday == 0 and hour < 5):
        return False, "Weekend market closure"
    return True, "Regular trading hours"

def calculate_next_market_open() -> datetime:
    """Calculate next market open time in Bangkok timezone."""
    current = datetime.now(timezone('Asia/Bangkok'))
    if current.weekday() == 5:  # Saturday
        days_to_add = 1
    elif current.weekday() == 6 and current.hour < 4:  # Early Sunday
        days_to_add = 0
    else:
        days_to_add = 7 - current.weekday()

    next_open = current + timedelta(days=days_to_add)
    next_open = next_open.replace(hour=4, minute=0, second=0, microsecond=0)
    return next_open

def check_spread_warning(pricing_data: Dict[str, Any], instrument: str) -> tuple[bool, float]:
    """
    Check spreads with different thresholds for forex and crypto.
    
    Args:
        pricing_data: Price data from OANDA
        instrument: Trading instrument code
        
    Returns:
        tuple[bool, float]: (has_wide_spread, spread)
    """
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
    spread_percentage = (spread / bid)

    is_crypto = any(crypto in instrument for crypto in ['BTC', 'ETH', 'XRP', 'LTC'])
    threshold = SPREAD_THRESHOLD_CRYPTO if is_crypto else SPREAD_THRESHOLD_FOREX

    if spread_percentage > threshold:
        logger.warning(
            f"Wide spread detected for {instrument}: {spread:.5f} "
            f"({spread_percentage*100:.2f}%)"
        )
        return True, spread

    return False, 0

async def check_market_status(instrument: str, account_id: str) -> tuple[bool, Dict[str, Any]]:
    """
    Check market status with trading hours and spread monitoring.
    
    Args:
        instrument: Trading instrument code
        account_id: OANDA account ID
        
    Returns:
        tuple[bool, Dict[str, Any]]: (is_tradeable, status_message)
    """
    current_time = datetime.now(timezone('Asia/Bangkok'))
    logger.info(
        f"Checking market status at {current_time.strftime('%Y-%m-%d %H:%M:%S')} Bangkok time"
    )

    # Check market hours
    is_open_flag, reason = is_market_open()
    if not is_open_flag:
        next_open = calculate_next_market_open()
        msg = f"Market closed: {reason}. Opens {next_open.strftime('%Y-%m-%d %H:%M:%S')} Bangkok time"
        logger.info(msg)
        return False, {
            "error": "Market closed",
            "reason": reason,
            "next_open": next_open.strftime('%Y-%m-%d %H:%M:%S'),
            "current_time": current_time.strftime('%Y-%m-%d %H:%M:%S')
        }

    # Check price and spread
    price_success, pricing_data = await get_instrument_price(instrument, account_id)
    if not price_success:
        return False, pricing_data

    has_wide_spread, spread = check_spread_warning(pricing_data, instrument)
    if has_wide_spread:
        return False, {
            "error": "Wide spread",
            "spread": spread,
            "instrument": instrument,
            "time": current_time.strftime('%Y-%m-%d %H:%M:%S')
        }

    return True, pricing_data

def translate_tradingview_signal(alert_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Translate TradingView signal format to internal format.
    
    Args:
        alert_data: Raw alert data from TradingView
        
    Returns:
        Dict[str, Any]: Translated alert data
    """
    # Extract core data
    action = alert_data.get('action', '').upper()
    comment = alert_data.get('comment', '').upper()
    
    # Handle close signals from comment field
    if 'CLOSE' in comment:
        if 'LONG' in comment:
            alert_data['action'] = 'CLOSE_LONG'
        elif 'SHORT' in comment:
            alert_data['action'] = 'CLOSE_SHORT'
    
    # Handle position opening
    elif action in ['BUY', 'SELL']:
        # Keep original action
        pass
    
    return alert_data

###
# Async Network Operations
###
async def get_instrument_price(instrument: str, account_id: str) -> tuple[bool, Dict[str, Any]]:
    """
    Fetch current pricing data from OANDA using global session.
    
    Args:
        instrument: Trading instrument code
        account_id: OANDA account ID
    
    Returns:
        tuple[bool, Dict[str, Any]]: (success, result)
            - success: True if price fetch was successful
            - result: Price data or error message
    """
    if not all([OANDA_API_TOKEN, OANDA_API_URL]):
        error_msg = "Missing required OANDA configuration"
        logger.error(error_msg)
        return False, {"error": error_msg}

    # Ensure valid session
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
                            logger.warning(f"Retrying in {delay}s (attempt {attempt + 1}/{MAX_RETRIES})")
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
    """
    Fetch current open positions from OANDA.
    
    Args:
        account_id: OANDA account ID
        
    Returns:
        tuple[bool, Dict[str, Any]]: (success, positions)
    """
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

async def validate_trade_direction(alert_data: Dict[str, Any]) -> tuple[bool, Optional[str], bool]:
    """
    Validate trade direction and check for existing positions.
    
    Args:
        alert_data: Validated alert data
        
    Returns:
        tuple[bool, Optional[str], bool]: (is_valid, error_message, is_closing_trade)
    """
    try:
        success, positions = await get_open_positions(alert_data.get('account', OANDA_ACCOUNT_ID))
        if not success:
            return True, None, False  # Continue with trade on position fetch failure
            
        instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
        for position in positions.get('positions', []):
            if position['instrument'] == instrument:
                logger.info(f"Found existing position for {instrument}: {position}")
                
        return True, None, False
        
    except Exception as e:
        logger.error(f"Error in trade validation: {str(e)}", exc_info=True)
        return True, None, False  # Continue with trade on validation error

###
# Trade Execution Functions
###
async def execute_trade(alert_data: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
    """
    Execute trade with OANDA with proper precision handling.
    
    Args:
        alert_data: Validated alert data containing trade details
    
    Returns:
        tuple[bool, Dict[str, Any]]: (success, result)
    """
    try:
        # Validate required fields
        required_fields = ['symbol', 'action', 'orderType', 'timeInForce', 'percentage']
        missing_fields = [field for field in required_fields if field not in alert_data]
        if missing_fields:
            error_msg = f"Missing required fields: {missing_fields}"
            logger.error(error_msg)
            return False, {"error": error_msg}

        instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
        
        # Get trading parameters
        leverage = get_instrument_leverage(instrument)
        is_crypto = any(crypto in instrument for crypto in ['BTC', 'ETH', 'XRP', 'LTC'])
        
        # Get and validate precision
        precision = INSTRUMENT_PRECISION.get(
            instrument,
            DEFAULT_CRYPTO_PRECISION if is_crypto else DEFAULT_FOREX_PRECISION
        )
        
        if not isinstance(precision, (int, float)) or precision < 0:
            error_msg = f"Invalid precision value for {instrument}: {precision}"
            logger.error(error_msg)
            return False, {"error": error_msg}

        # Get and validate minimum order size
        min_size = MIN_ORDER_SIZES.get(instrument, DEFAULT_MIN_ORDER_SIZE)
        if not isinstance(min_size, (int, float)) or min_size <= 0:
            error_msg = f"Invalid minimum order size for {instrument}: {min_size}"
            logger.error(error_msg)
            return False, {"error": error_msg}
        
        # Calculate trade size with validation
        try:
            percentage = float(alert_data['percentage'])
            if not 0 < percentage <= 1:
                error_msg = f"Percentage must be between 0 and 1: {percentage}"
                logger.error(error_msg)
                return False, {"error": error_msg}
                
            if not isinstance(leverage, (int, float)) or leverage <= 0:
                error_msg = f"Invalid leverage value: {leverage}"
                logger.error(error_msg)
                return False, {"error": error_msg}
                
            trade_size = BASE_POSITION * percentage * leverage
            if trade_size <= 0 or math.isnan(trade_size) or math.isinf(trade_size):
                error_msg = f"Invalid trade size calculated: {trade_size}"
                logger.error(error_msg)
                return False, {"error": error_msg}
        except ValueError as e:
            error_msg = f"Invalid percentage value: {str(e)}"
            logger.error(error_msg)
            return False, {"error": error_msg}
        
        # Ensure valid session before price fetch
        session_ok, error = await ensure_session()
        if not session_ok:
            return False, {"error": error}
        
        # Get current price
        price_success, price_data = await get_instrument_price(instrument, alert_data['account'])
        if not price_success:
            return False, price_data
        
        # Calculate units
        price_info = price_data['prices'][0]
        is_sell = alert_data['action'].upper() == 'SELL'
        try:
            price = float(price_info['bids'][0]['price']) if is_sell else float(price_info['asks'][0]['price'])
        except (KeyError, IndexError, ValueError) as e:
            error_msg = f"Error parsing price data: {str(e)}"
            logger.error(error_msg)
            return False, {"error": error_msg}
        
        # Calculate units with proper precision handling
        raw_units = trade_size / price
        
        # Round to whole numbers for all instruments
        units = int(round(raw_units))
        
        # Validate units
        if units is None or math.isnan(units) or math.isinf(units):
            error_msg = f"Invalid units calculated: {units}"
            logger.error(error_msg)
            return False, {"error": error_msg}
        
        # Enforce minimum order size
        if abs(units) < min_size:
            logger.warning(f"Order size {abs(units)} below minimum {min_size} for {instrument}")
            units = min_size if not is_sell else -min_size
        elif is_sell:
            units = -abs(units)
        
        # Convert units to integer string
        units_str = str(int(units))
        
        # Prepare order
        order_data = {
            "order": {
                "type": alert_data['orderType'],
                "instrument": instrument,
                "units": units_str,
                "timeInForce": alert_data['timeInForce'],
                "positionFill": "DEFAULT"
            }
        }

        logger.info(
            f"Trade details: {instrument}, {'SELL' if is_sell else 'BUY'}, "
            f"Price={price}, Units={units_str}, Size=${trade_size}"
        )
                    
        url = f"{OANDA_API_URL}/accounts/{alert_data['account']}/orders"
        
        # Execute trade with retry logic
        for attempt in range(MAX_RETRIES):
            try:
                async with session.post(url, json=order_data) as response:
                    if response.status != 201:
                        error_msg = f"OANDA API error: {response.status}"
                        error_content = await response.text()
                        logger.error(f"{error_msg} - Response: {error_content}")
                        
                        if attempt < MAX_RETRIES - 1:
                            delay = BASE_DELAY * (2 ** attempt)
                            logger.warning(f"Retrying trade in {delay}s (attempt {attempt + 1}/{MAX_RETRIES})")
                            await asyncio.sleep(delay)
                            await ensure_session()
                            continue
                        
                        return False, {
                            "error": error_msg,
                            "response": error_content,
                            "order_data": order_data
                        }

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

###
# Alert Handler Class
###
class AlertHandler:
    def __init__(self, max_retries: int = MAX_RETRIES, base_delay: float = BASE_DELAY):
        self.logger = logging.getLogger('alert_handler')
        self.max_retries = max_retries
        self.base_delay = base_delay
        self._trade_lock = asyncio.Lock()

    async def close_position(self, alert_data: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
        """
        Close an existing position.
        
        Args:
            alert_data: Validated alert data containing position details
            
        Returns:
            tuple[bool, Dict[str, Any]]: (success, result)
        """
        try:
            account_id = alert_data.get('account')
            instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
            
            # Ensure valid session
            session_ok, error = await ensure_session()
            if not session_ok:
                return False, {"error": error}

            url = f"{OANDA_API_URL}/accounts/{account_id}/positions/{instrument}/close"
            
            # Determine long/short closing based on action
            close_body = {}
            if alert_data['action'] == 'CLOSE_LONG':
                close_body = {"longUnits": "ALL"}
            elif alert_data['action'] == 'CLOSE_SHORT':
                close_body = {"shortUnits": "ALL"}
            else:
                error_msg = f"Invalid close action: {alert_data['action']}"
                self.logger.error(error_msg)
                return False, {"error": error_msg}
            
            self.logger.info(f"Closing {instrument} position with data: {close_body}")
            
            # Execute close with retry logic
            for attempt in range(self.max_retries):
                try:
                    async with session.put(url, json=close_body) as response:
                        if response.status != 200:
                            error_content = await response.text()
                            self.logger.error(f"Failed to close position: {error_content}")
                            
                            if attempt < self.max_retries - 1:
                                delay = self.base_delay * (2 ** attempt)
                                self.logger.warning(f"Retrying close in {delay}s (attempt {attempt + 1}/{self.max_retries})")
                                await asyncio.sleep(delay)
                                await ensure_session()
                                continue
                                
                            return False, {"error": f"Close position failed: {error_content}"}
                        
                        close_response = await response.json()
                        self.logger.info(f"Position closed successfully: {close_response}")
                        return True, close_response

                except aiohttp.ClientError as e:
                    if attempt < self.max_retries - 1:
                        delay = self.base_delay * (2 ** attempt)
                        await asyncio.sleep(delay)
                        await get_session(force_new=True)
                        continue
                    error_msg = f"Network error closing position: {str(e)}"
                    self.logger.error(error_msg)
                    return False, {"error": error_msg}

        except Exception as e:
            error_msg = f"Error closing position: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return False, {"error": error_msg}

    async def process_alert(self, alert_data: Dict[str, Any]) -> bool:
        """
        Process an alert with position validation and locking.
        
        Args:
            alert_data: Validated alert data
            
        Returns:
            bool: Success status
        """
        if not alert_data:
            self.logger.error("No alert data provided")
            return False

        alert_id = alert_data.get('id', str(uuid.uuid4()))
        
        async with self._trade_lock:
            # Handle close actions differently
            if alert_data['action'] in ['CLOSE_LONG', 'CLOSE_SHORT']:
                success, result = await self.close_position(alert_data)
                if success:
                    self.logger.info(f"Successfully closed position for alert {alert_id}")
                    return True
                else:
                    self.logger.error(f"Failed to close position for alert {alert_id}: {result}")
                    return False

            # Handle regular trades
            for attempt in range(self.max_retries):
                try:
                    # Validate trade direction first
                    is_valid, error_message, is_closing_trade = await validate_trade_direction(alert_data)
                    if not is_valid:
                        self.logger.warning(f"Trade validation failed for alert {alert_id}: {error_message}")
                        return False

                    # Add closing trade information to alert_data
                    alert_data['is_closing_trade'] = is_closing_trade

                    instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
                    is_tradeable, status_message = await check_market_status(
                        instrument,
                        alert_data.get('account', OANDA_ACCOUNT_ID)
                    )

                    if not is_tradeable:
                        if attempt < self.max_retries - 1:
                            delay = self.base_delay * (2 ** attempt)
                            self.logger.warning(
                                f"Market not tradeable for alert {alert_id}, retrying in {delay}s "
                                f"(attempt {attempt + 1}/{self.max_retries}): {status_message}"
                            )
                            await asyncio.sleep(delay)
                            continue
                        
                        self.logger.error(f"Market remained untradeable for alert {alert_id}: {status_message}")
                        return False

                    success, trade_result = await execute_trade(alert_data)
                    if success:
                        if attempt > 0:
                            self.logger.info(f"Alert {alert_id} succeeded on attempt {attempt + 1}")
                        return True
                    
                    if attempt < self.max_retries - 1:
                        delay = self.base_delay * (2 ** attempt)
                        self.logger.warning(
                            f"Alert {alert_id} failed, retrying in {delay}s "
                            f"(attempt {attempt + 1}/{self.max_retries}): "
                            f"{trade_result.get('error', 'Unknown error')}"
                        )
                        await asyncio.sleep(delay)
                    else:
                        self.logger.error(f"Alert {alert_id} failed on final attempt: {trade_result}")
                
                except Exception as e:
                    self.logger.error(f"Error processing alert {alert_id}: {str(e)}", exc_info=True)
                    if attempt < self.max_retries - 1:
                        delay = self.base_delay * (2 ** attempt)
                        await asyncio.sleep(delay)
                    else:
                        self.logger.error(f"Alert {alert_id} failed permanently: {str(e)}")
                        return False
            
            self.logger.info(f"Alert {alert_id} discarded after {self.max_retries} failed attempts")
            return False

async def get_open_positions(account_id: str) -> tuple[bool, Dict[str, Any]]:
    """
    Fetch current open positions from OANDA.
    
    Args:
        account_id: OANDA account ID
        
    Returns:
        tuple[bool, Dict[str, Any]]: (success, positions)
    """
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

async def validate_trade_direction(alert_data: Dict[str, Any]) -> tuple[bool, Optional[str], bool]:
    """
    Validate trade direction and check for existing positions.
    
    Args:
        alert_data: Validated alert data
        
    Returns:
        tuple[bool, Optional[str], bool]: (is_valid, error_message, is_closing_trade)
    """
    try:
        account_id = alert_data.get('account', OANDA_ACCOUNT_ID)
        success, positions = await get_open_positions(account_id)
        if not success:
            return False, "Failed to fetch positions", False  
            
        instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
        for position in positions.get('positions', []):
            if position['instrument'] == instrument:
                logger.info(f"Found existing position for {instrument}: {position}")
                
                current_direction = 'SELL' if float(position['long']['units']) > 0 else 'BUY'
                alert_direction = alert_data['action'].upper()
                
                if current_direction == alert_direction:
                    logger.warning(f"Ignoring {alert_direction} alert for {instrument} due to existing {current_direction} position")
                    return False, f"Existing {current_direction} position for {instrument}", False
                
                return True, None, True
        
        return True, None, False
        
    except Exception as e:
        logger.error(f"Error in trade validation: {str(e)}", exc_info=True)
        return False, f"Trade validation failed: {str(e)}", False

###
# Alert Handler Class
###
class AlertHandler:
    def __init__(self, max_retries: int = MAX_RETRIES, base_delay: float = BASE_DELAY):
        self.logger = logging.getLogger('alert_handler')
        self.max_retries = max_retries
        self.base_delay = base_delay
        self._trade_lock = asyncio.Lock()

    async def close_position(self, alert_data: Dict[str, Any]) -> tuple[bool, Dict[str, Any]]:
        """
        Close an existing position.
        
        Args:
            alert_data: Validated alert data containing position details
            
        Returns:
            tuple[bool, Dict[str, Any]]: (success, result)
        """
        try:
            account_id = alert_data.get('account', OANDA_ACCOUNT_ID)
            instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
            
            # Ensure valid session
            session_ok, error = await ensure_session()
            if not session_ok:
                return False, {"error": error}

            url = f"{OANDA_API_URL}/accounts/{account_id}/positions/{instrument}/close"
            
            # Determine long/short closing based on position units
            success, positions = await get_open_positions(account_id)
            if not success:
                return False, positions
            
            position = next((p for p in positions.get('positions', []) if p['instrument'] == instrument), None)
            if not position:
                error_msg = f"No position found for {instrument}"
                return False, {"error": error_msg}
            
            long_units = int(position.get('long', {}).get('units', '0'))
            short_units = int(position.get('short', {}).get('units', '0'))
            
            close_body = {}
            if long_units > 0:
                close_body = {"longUnits": "ALL"}
            elif short_units > 0: 
                close_body = {"shortUnits": "ALL"}
            else:
                error_msg = f"Ambiguous position for {instrument}: long={long_units}, short={short_units}"
                return False, {"error": error_msg}
            
            self.logger.info(f"Closing {instrument} position with data: {close_body}")
            
            # Execute close with retry logic
            for attempt in range(self.max_retries):
                try:
                    async with session.put(url, json=close_body) as response:
                        if response.status != 200:
                            error_content = await response.text()
                            self.logger.error(f"Failed to close position: {error_content}")
                            
                            if attempt < self.max_retries - 1:
                                delay = self.base_delay * (2 ** attempt)
                                self.logger.warning(f"Retrying close in {delay}s (attempt {attempt + 1}/{self.max_retries})")
                                await asyncio.sleep(delay)
                                await ensure_session()
                                continue
                                
                            return False, {"error": f"Close position failed: {error_content}"}
                        
                        close_response = await response.json()
                        self.logger.info(f"Position closed successfully: {close_response}")
                        return True, close_response

                except aiohttp.ClientError as e:
                    if attempt < self.max_retries - 1:
                        delay = self.base_delay * (2 ** attempt)
                        await asyncio.sleep(delay)
                        await get_session(force_new=True)
                        continue
                    error_msg = f"Network error closing position: {str(e)}"
                    self.logger.error(error_msg)
                    return False, {"error": error_msg}

        except Exception as e:
            error_msg = f"Error closing position: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            return False, {"error": error_msg}

    async def process_alert(self, alert_data: Dict[str, Any]) -> bool:
        """
        Process an alert with position validation and locking.
        
        Args:
            alert_data: Validated alert data
            
        Returns:
            bool: Success status
        """
        if not alert_data:
            self.logger.error("No alert data provided")
            return False

        alert_id = alert_data.get('id', str(uuid.uuid4()))
        
        async with self._trade_lock:
            # Handle close actions differently
            if alert_data['action'] in ['CLOSE_LONG', 'CLOSE_SHORT']:
                success, result = await self.close_position(alert_data)
                if success:
                    self.logger.info(f"Successfully closed position for alert {alert_id}")
                    return True
                else:
                    self.logger.error(f"Failed to close position for alert {alert_id}: {result}")
                    return False

            # Handle regular trades
            for attempt in range(self.max_retries):
                try:
                    # Validate trade direction first 
                    is_valid, error_message, is_closing_trade = await validate_trade_direction(alert_data)
                    if not is_valid:
                        self.logger.warning(f"Trade validation failed for alert {alert_id}: {error_message}")
                        return False

                    # Add closing trade information to alert_data
                    alert_data['is_closing_trade'] = is_closing_trade

                    instrument = f"{alert_data['symbol'][:3]}_{alert_data['symbol'][3:]}"
                    is_tradeable, status_message = await check_market_status(
                        instrument,
                        alert_data.get('account', OANDA_ACCOUNT_ID) 
                    )

                    if not is_tradeable:
                        if attempt < self.max_retries - 1:
                            delay = self.base_delay * (2 ** attempt)
                            self.logger.warning(
                                f"Market not tradeable for alert {alert_id}, retrying in {delay}s "  
                                f"(attempt {attempt + 1}/{self.max_retries}): {status_message}"
                            )
                            await asyncio.sleep(delay)
                            continue
                        
                        self.logger.error(f"Market remained untradeable for alert {alert_id}: {status_message}")
                        return False

                    success, trade_result = await execute_trade(alert_data)
                    if success:
                        if attempt > 0:
                            self.logger.info(f"Alert {alert_id} succeeded on attempt {attempt + 1}")
                        return True
                    
                    if attempt < self.max_retries - 1:
                        delay = self.base_delay * (2 ** attempt)
                        self.logger.warning(
                            f"Alert {alert_id} failed, retrying in {delay}s "
                            f"(attempt {attempt + 1}/{self.max_retries}): "  
                            f"{trade_result.get('error', 'Unknown error')}"
                        )
                        await asyncio.sleep(delay)
                    else:
                        self.logger.error(f"Alert {alert_id} failed on final attempt: {trade_result}")
                
                except Exception as e:
                    self.logger.error(f"Error processing alert {alert_id}: {str(e)}", exc_info=True)
                    if attempt < self.max_retries - 1:
                        delay = self.base_delay * (2 ** attempt)  
                        await asyncio.sleep(delay)
                    else:
                        self.logger.error(f"Alert {alert_id} failed permanently: {str(e)}")
                        return False
            
            self.logger.info(f"Alert {alert_id} discarded after {self.max_retries} failed attempts")
            return False

# Initialize AlertHandler instance
alert_handler = AlertHandler()

###
# API Endpoint 
###
@app.post("/alerts")
async def handle_alert(alert_data: AlertData):
    """Process incoming alert data."""
    try:
        alert_data_dict = alert_data.dict()
        if not alert_data_dict.get('account'):
            alert_data_dict['account'] = OANDA_ACCOUNT_ID 
        
        alert_data_dict = translate_tradingview_signal(alert_data_dict)
        
        alert_id = alert_data_dict.get('id', str(uuid.uuid4()))
        logger.info(f"Received alert {alert_id}: {alert_data_dict}")

        success = await alert_handler.process_alert(alert_data_dict)
        if success:
            return JSONResponse(
                status_code=200,
                content={"message": f"Alert {alert_id} processed successfully"}
            )
        else:
            return JSONResponse(
                status_code=400, 
                content={"error": f"Failed to process alert {alert_id}"}
            )
            
    except ValidationError as e:
        error_msg = f"Invalid alert data: {str(e)}"
        logger.error(error_msg)
        return JSONResponse(status_code=400, content={"error": error_msg})
    
    except Exception as e:
        error_msg = f"Unexpected error processing alert: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return JSONResponse(status_code=500, content={"error": error_msg})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

