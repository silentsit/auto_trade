"""
Trading API Module

This module handles all interaction with the broker API, providing a clean interface
for the rest of the application to use.
"""

import os
import uuid
import asyncio
import aiohttp
import logging
import json
from typing import Dict, Any, Tuple, Optional, List
from datetime import datetime
from functools import wraps

# Configure logger
logger = logging.getLogger("trading_api")

# Configuration settings - moved from python_bridge import to avoid circular dependency
HTTP_REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=45,
    connect=10,
    sock_connect=10,
    sock_read=30
)

# Default config values - these will be overridden by the actual config from python_bridge
# when it's imported there, but needed here to avoid circular dependencies
class DefaultConfig:
    oanda_token = os.getenv("OANDA_API_TOKEN", "")
    oanda_account = os.getenv("OANDA_ACCOUNT_ID", "")
    oanda_api_url = os.getenv("OANDA_API_URL", "https://api-fxtrade.oanda.com/v3")
    max_simultaneous_connections = 100
    max_retries = 3
    base_delay = 1.0

config = DefaultConfig()

# Track the session globally
_session = None
_session_lock = asyncio.Lock()


async def get_session(force_new=False):
    """Get or create a shared aiohttp session"""
    global _session
    
    async with _session_lock:
        if _session is None or force_new:
            if _session is not None:
                await _session.close()
                
            conn = aiohttp.TCPConnector(limit=config.max_simultaneous_connections)
            headers = {
                "Authorization": f"Bearer {config.oanda_token}",
                "Content-Type": "application/json"
            }
            
            _session = aiohttp.ClientSession(
                connector=conn,
                headers=headers,
                timeout=HTTP_REQUEST_TIMEOUT
            )
            logger.info("Created new API session")
            
    return _session

async def cleanup_stale_sessions():
    """Close any stale sessions"""
    global _session
    
    async with _session_lock:
        if _session is not None:
            await _session.close()
            _session = None
            logger.info("Closed stale API session")


def standardize_symbol(symbol: str) -> str:
    """Standardize instrument symbols to a consistent format"""
    # Remove any whitespace
    symbol = symbol.strip()
    
    # Handle common Forex pairs
    forex_pairs = ["EUR", "USD", "JPY", "GBP", "AUD", "NZD", "CAD", "CHF"]
    
    # Check if this might be a forex pair
    if any(pair in symbol.upper() for pair in forex_pairs):
        # Split and standardize
        for pair in forex_pairs:
            if pair in symbol.upper():
                # Extract the components
                components = []
                remaining = symbol.upper()
                
                for check_pair in forex_pairs:
                    if check_pair in remaining:
                        components.append(check_pair)
                        remaining = remaining.replace(check_pair, '')
                
                # If we found at least 2 components, format as XXX_YYY
                if len(components) >= 2:
                    return f"{components[0]}_{components[1]}"
    
    # Handle special cases like gold and silver
    if "GOLD" in symbol.upper() or "XAU" in symbol.upper():
        return "XAU_USD"
    
    if "SILVER" in symbol.upper() or "XAG" in symbol.upper():
        return "XAG_USD"
    
    # If no special handling needed, return as is
    return symbol


def ensure_proper_timeframe(timeframe: str) -> str:
    """Normalize timeframe string to standard format"""
    # Convert to uppercase
    tf = timeframe.upper()
    
    # Common TradingView timeframes
    if tf in ["1", "1M"]:
        return "1M"
    elif tf in ["5", "5M"]:
        return "5M"
    elif tf in ["15", "15M"]:
        return "15M"
    elif tf in ["30", "30M"]:
        return "30M"
    elif tf in ["60", "1H", "H1", "H", "HOUR"]:
        return "1H"
    elif tf in ["240", "4H", "H4"]:
        return "4H"
    elif tf in ["D", "1D", "D1", "DAY", "DAILY"]:
        return "1D"
    elif tf in ["W", "1W", "W1", "WEEK", "WEEKLY"]:
        return "1W"
    elif tf in ["MN", "1MN", "MONTH", "MONTHLY"]:
        return "1MN"
    
    # Default to 1H if unrecognized
    logger.warning(f"Unrecognized timeframe: {timeframe}, defaulting to 1H")
    return "1H"


def convert_timeframe_to_granularity(timeframe: str) -> str:
    """Convert timeframe to Oanda granularity format"""
    # First standardize the timeframe
    tf = ensure_proper_timeframe(timeframe)
    
    # Convert to Oanda format
    if tf == "1M":
        return "M1"
    elif tf == "5M":
        return "M5"
    elif tf == "15M":
        return "M15"
    elif tf == "30M":
        return "M30"
    elif tf == "1H":
        return "H1"
    elif tf == "4H":
        return "H4"
    elif tf == "1D":
        return "D"
    elif tf == "1W":
        return "W"
    elif tf == "1MN":
        return "M"
    
    # Default to H1 if unrecognized
    logger.warning(f"Unrecognized timeframe for conversion: {timeframe}, defaulting to H1")
    return "H1"


async def close_position(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Close an existing position"""
    request_id = str(uuid.uuid4())
    symbol = standardize_symbol(alert_data['symbol'])
    logger.info(f"[{request_id}] Closing position for {symbol}")
    
    try:
        # Create the close order data
        close_data = {
            "order": {
                "type": "MARKET",
                "instrument": symbol,
                "units": "ALL",  # Close all units
                "timeInForce": "FOK",
                "positionFill": "REDUCE_ONLY"
            }
        }
        
        # Get session and API URL
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{alert_data.get('account', config.oanda_account)}/orders"
        
        # Execute close order with retries
        retries = 0
        while retries < config.max_retries:
            try:
                async with session.post(url, json=close_data, timeout=HTTP_REQUEST_TIMEOUT) as response:
                    response_text = await response.text()
                    logger.info(f"[{request_id}] Close position response: {response.status}, Response: {response_text}")
                    
                    if response.status == 201:
                        result = json.loads(response_text)
                        logger.info(f"[{request_id}] Position closed successfully: {result}")
                        return True, result
                    
                    # Handle error responses
                    try:
                        error_data = json.loads(response_text)
                        error_code = error_data.get("errorCode", "UNKNOWN_ERROR")
                        error_message = error_data.get("errorMessage", "Unknown error")
                        logger.error(f"[{request_id}] OANDA error: {error_code} - {error_message}")
                    except:
                        pass
                    
                    # Handle specific errors
                    if "RATE_LIMIT" in response_text:
                        await asyncio.sleep(60)  # Longer wait for rate limits
                    elif "POSITION_NOT_CLOSEABLE" in response_text:
                        return False, {"error": "Position not closeable"}
                    else:
                        delay = config.base_delay * (2 ** retries)
                        await asyncio.sleep(delay)
                    
                    logger.warning(f"[{request_id}] Close position retry {retries + 1}/{config.max_retries}")
                    retries += 1
                    
            except aiohttp.ClientError as e:
                logger.error(f"[{request_id}] Network error closing position: {str(e)}")
                if retries < config.max_retries - 1:
                    await asyncio.sleep(config.base_delay * (2 ** retries))
                    retries += 1
                    continue
                return False, {"error": f"Network error: {str(e)}"}
        
        return False, {"error": "Maximum retries exceeded while closing position"}
        
    except Exception as e:
        logger.error(f"[{request_id}] Error closing position: {str(e)}", exc_info=True)
        return False, {"error": str(e)}


async def execute_trade(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Execute a trade based on alert data"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Executing trade: {json.dumps(alert_data, indent=2)}")
    
    try:
        # Extract data from alert
        symbol = standardize_symbol(alert_data['symbol'])
        action = alert_data['action'].upper()
        percentage = alert_data.get('percentage', 100)  # Default to 100% if not specified
        account_id = alert_data.get('account', config.oanda_account)
        
        # Calculate units based on percentage
        units = config.base_position * (percentage / 100)
        
        # Adjust units based on action
        if action == "SELL":
            units = -units
            
        # Create the order
        order_data = {
            "order": {
                "type": "MARKET",
                "instrument": symbol,
                "units": str(int(units)),
                "timeInForce": "FOK",
                "positionFill": "OPEN_ONLY"
            }
        }
        
        # Get session and API URL
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{account_id}/orders"
        
        # Execute the trade with retries
        retries = 0
        while retries < config.max_retries:
            try:
                async with session.post(url, json=order_data, timeout=HTTP_REQUEST_TIMEOUT) as response:
                    response_text = await response.text()
                    logger.info(f"[{request_id}] Trade execution response: {response.status}, Response: {response_text}")
                    
                    if response.status == 201:
                        result = json.loads(response_text)
                        logger.info(f"[{request_id}] Trade executed successfully: {result}")
                        return True, result
                    
                    # Handle error responses
                    try:
                        error_data = json.loads(response_text)
                        error_code = error_data.get("errorCode", "UNKNOWN_ERROR")
                        error_message = error_data.get("errorMessage", "Unknown error")
                        logger.error(f"[{request_id}] OANDA error: {error_code} - {error_message}")
                    except:
                        pass
                    
                    # Handle specific errors
                    if "RATE_LIMIT" in response_text:
                        await asyncio.sleep(60)  # Longer wait for rate limits
                    else:
                        delay = config.base_delay * (2 ** retries)
                        await asyncio.sleep(delay)
                    
                    logger.warning(f"[{request_id}] Trade execution retry {retries + 1}/{config.max_retries}")
                    retries += 1
                    
            except aiohttp.ClientError as e:
                logger.error(f"[{request_id}] Network error executing trade: {str(e)}")
                if retries < config.max_retries - 1:
                    await asyncio.sleep(config.base_delay * (2 ** retries))
                    retries += 1
                    continue
                return False, {"error": f"Network error: {str(e)}"}
        
        return False, {"error": "Maximum retries exceeded while executing trade"}
        
    except Exception as e:
        logger.error(f"[{request_id}] Error executing trade: {str(e)}", exc_info=True)
        return False, {"error": str(e)}


async def get_open_positions() -> Tuple[bool, Dict[str, Any]]:
    """Get all open positions"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Getting open positions")
    
    try:
        # Get session and API URL
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{config.oanda_account}/openPositions"
        
        # Make the request with retries
        retries = 0
        while retries < config.max_retries:
            try:
                async with session.get(url, timeout=HTTP_REQUEST_TIMEOUT) as response:
                    if response.status == 200:
                        result = await response.json()
                        logger.info(f"[{request_id}] Got open positions: {len(result.get('positions', []))} positions found")
                        return True, result
                    
                    # Handle error responses
                    response_text = await response.text()
                    logger.error(f"[{request_id}] Error getting positions: {response.status}, Response: {response_text}")
                    
                    # Handle rate limiting
                    if "RATE_LIMIT" in response_text:
                        await asyncio.sleep(60)
                    else:
                        delay = config.base_delay * (2 ** retries)
                        await asyncio.sleep(delay)
                    
                    logger.warning(f"[{request_id}] Get positions retry {retries + 1}/{config.max_retries}")
                    retries += 1
                    
            except aiohttp.ClientError as e:
                logger.error(f"[{request_id}] Network error getting positions: {str(e)}")
                if retries < config.max_retries - 1:
                    await asyncio.sleep(config.base_delay * (2 ** retries))
                    retries += 1
                    continue
                return False, {"error": f"Network error: {str(e)}"}
        
        return False, {"error": "Maximum retries exceeded while getting positions"}
        
    except Exception as e:
        logger.error(f"[{request_id}] Error getting positions: {str(e)}", exc_info=True)
        return False, {"error": str(e)}


async def get_account_balance(account_id: str) -> float:
    """Get the account balance"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Getting account balance for {account_id}")
    
    try:
        # Get session and API URL
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{account_id}"
        
        # Make the request with retries
        retries = 0
        while retries < config.max_retries:
            try:
                async with session.get(url, timeout=HTTP_REQUEST_TIMEOUT) as response:
                    if response.status == 200:
                        result = await response.json()
                        account = result.get("account", {})
                        balance = float(account.get("balance", 0))
                        logger.info(f"[{request_id}] Got account balance: {balance}")
                        return balance
                    
                    # Handle error responses
                    response_text = await response.text()
                    logger.error(f"[{request_id}] Error getting account info: {response.status}, Response: {response_text}")
                    
                    # Handle rate limiting
                    if "RATE_LIMIT" in response_text:
                        await asyncio.sleep(60)
                    else:
                        delay = config.base_delay * (2 ** retries)
                        await asyncio.sleep(delay)
                    
                    logger.warning(f"[{request_id}] Get account info retry {retries + 1}/{config.max_retries}")
                    retries += 1
                    
            except aiohttp.ClientError as e:
                logger.error(f"[{request_id}] Network error getting account info: {str(e)}")
                if retries < config.max_retries - 1:
                    await asyncio.sleep(config.base_delay * (2 ** retries))
                    retries += 1
                    continue
                return 10000  # Default balance as fallback
        
        logger.error(f"[{request_id}] Maximum retries exceeded while getting account info, using default balance")
        return 10000  # Default balance as fallback
        
    except Exception as e:
        logger.error(f"[{request_id}] Error getting account info: {str(e)}", exc_info=True)
        return 10000  # Default balance as fallback


async def get_candles(instrument: str, timeframe: str, count: int = 100) -> Tuple[bool, List[Dict[str, Any]]]:
    """Get historical candles for an instrument"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Getting {count} candles for {instrument} on {timeframe} timeframe")
    
    try:
        # Convert timeframe to granularity
        granularity = convert_timeframe_to_granularity(timeframe)
        
        # Get session and API URL
        session = await get_session()
        url = f"{config.oanda_api_url}/instruments/{instrument}/candles"
        
        # Set up the query parameters
        params = {
            "granularity": granularity,
            "count": str(count),
            "price": "M"  # Midpoint candles
        }
        
        # Make the request with retries
        retries = 0
        while retries < config.max_retries:
            try:
                async with session.get(url, params=params, timeout=HTTP_REQUEST_TIMEOUT) as response:
                    if response.status == 200:
                        result = await response.json()
                        candles = result.get("candles", [])
                        logger.info(f"[{request_id}] Got {len(candles)} candles for {instrument}")
                        return True, candles
                    
                    # Handle error responses
                    response_text = await response.text()
                    logger.error(f"[{request_id}] Error getting candles: {response.status}, Response: {response_text}")
                    
                    # Handle rate limiting
                    if "RATE_LIMIT" in response_text:
                        await asyncio.sleep(60)
                    else:
                        delay = config.base_delay * (2 ** retries)
                        await asyncio.sleep(delay)
                    
                    logger.warning(f"[{request_id}] Get candles retry {retries + 1}/{config.max_retries}")
                    retries += 1
                    
            except aiohttp.ClientError as e:
                logger.error(f"[{request_id}] Network error getting candles: {str(e)}")
                if retries < config.max_retries - 1:
                    await asyncio.sleep(config.base_delay * (2 ** retries))
                    retries += 1
                    continue
                return False, []
        
        return False, []
        
    except Exception as e:
        logger.error(f"[{request_id}] Error getting candles: {str(e)}", exc_info=True)
        return False, []


async def get_atr(instrument: str, timeframe: str, period: int = 14) -> float:
    """Calculate the Average True Range for an instrument"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Calculating ATR for {instrument} on {timeframe} timeframe (period: {period})")
    
    try:
        # Get candles
        success, candles = await get_candles(instrument, timeframe, count=period+10)
        
        if not success or len(candles) < period:
            logger.warning(f"[{request_id}] Not enough candles to calculate ATR for {instrument}")
            return 0.001  # Default value as fallback
        
        # Calculate ATR
        true_ranges = []
        
        for i in range(1, len(candles)):
            prev_candle = candles[i-1]
            curr_candle = candles[i]
            
            prev_close = float(prev_candle.get("mid", {}).get("c", 0))
            curr_high = float(curr_candle.get("mid", {}).get("h", 0))
            curr_low = float(curr_candle.get("mid", {}).get("l", 0))
            
            # Calculate true range
            true_range = max(
                curr_high - curr_low,
                abs(curr_high - prev_close),
                abs(curr_low - prev_close)
            )
            
            true_ranges.append(true_range)
        
        # Use the last 'period' true ranges
        if len(true_ranges) > period:
            true_ranges = true_ranges[-period:]
        
        # Calculate average
        atr = sum(true_ranges) / len(true_ranges)
        
        logger.info(f"[{request_id}] Calculated ATR for {instrument}: {atr}")
        return atr
        
    except Exception as e:
        logger.error(f"[{request_id}] Error calculating ATR: {str(e)}", exc_info=True)
        return 0.001  # Default value as fallback


def get_instrument_type(instrument: str) -> str:
    """Determine instrument type for appropriate parameters"""
    normalized_symbol = instrument
    if any(crypto in normalized_symbol for crypto in ["BTC", "ETH", "XRP", "LTC"]):
        return "CRYPTO"
    elif "XAU" in normalized_symbol:
        return "XAU_USD"
    else:
        return "FOREX" 