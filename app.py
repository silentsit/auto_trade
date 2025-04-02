import json
import uuid
from typing import Dict, Any, Tuple
import asyncio
import aiohttp
import re
import logging
import threading
from datetime import datetime, timedelta

# Configure logger
logger = logging.getLogger("trading_bot")
if not logger.handlers:
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Global lock for thread safety
_lock = asyncio.Lock()

# These would normally be initialized elsewhere but for now let's declare them
error_recovery = None
position_tracker = None
risk_manager = None
dynamic_exit_manager = None
loss_manager = None
risk_analytics = None
market_structure_analyzer = None
volatility_monitor = None
position_sizing = None

# Import configuration (assuming it exists elsewhere)
class config:
    oanda_account = "your_oanda_account"
    oanda_api_url = "https://api-fxpractice.oanda.com/v3"
    max_retries = 3
    base_delay = 1

# Constants
HTTP_REQUEST_TIMEOUT = 10  # seconds

# Circuit Breaker implementation
class CircuitBreaker:
    def __init__(self, failure_threshold=5, cooldown_seconds=300):
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self._lock = asyncio.Lock()
        
    async def record_failure(self):
        """Record a failure and potentially open the circuit"""
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = datetime.utcnow()
            
            if self.state == "CLOSED" and self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                logger.warning(f"Circuit breaker tripped after {self.failure_count} failures")
                
    async def record_success(self):
        """Record a success and potentially reset the circuit"""
        async with self._lock:
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
                logger.info("Circuit breaker reset after successful operation")
                
    async def is_open(self):
        """Check if circuit breaker is open and should block operations"""
        async with self._lock:
            # If circuit is open, check if cooldown period has elapsed
            if self.state == "OPEN" and self.last_failure_time:
                elapsed = (datetime.utcnow() - self.last_failure_time).total_seconds()
                
                # If cooldown period elapsed, transition to half-open
                if elapsed >= self.cooldown_seconds:
                    self.state = "HALF_OPEN"
                    logger.info(f"Circuit breaker auto-reset after {self.cooldown_seconds} seconds cooldown")
                    
            return self.state == "OPEN"

# ErrorRecovery system that includes a circuit breaker
class ErrorRecovery:
    def __init__(self):
        self.circuit_breaker = CircuitBreaker()
        
    async def handle_error(self, request_id, context, error, error_data=None):
        """Handle an error and perform recovery actions"""
        logger.error(f"[{request_id}] Error in {context}: {str(error)}")
        
        # Record failure in circuit breaker
        await self.circuit_breaker.record_failure()
        
        # Additional error handling logic would go here
        return False

# Initialize error recovery system
error_recovery = ErrorRecovery()

def get_instrument_type(instrument: str) -> str:
    """Determine the instrument type for sizing and ATR calculations"""
    if "_" in instrument:  # Stock CFDs typically have underscores
        return "STOCK"
    elif "JPY" in instrument or any(x in instrument for x in ["USD", "EUR", "GBP", "AUD", "NZD", "CAD", "CHF"]):
        return "FOREX"
    else:
        return "COMMODITY"

def ensure_proper_timeframe(timeframe: str) -> str:
    """Ensures timeframe is in the proper format (e.g., converts '15' to '15M')"""
    # Handle special cases first
    if timeframe.upper() in ['D', 'DAY', 'DAILY', '1D']:
        return 'D'
    if timeframe.upper() in ['W', 'WEEK', 'WEEKLY', '1W']:
        return 'W'
    if timeframe.upper() in ['M', 'MONTH', 'MONTHLY', '1M']:
        return 'M'
        
    # Strip any non-alphanumeric characters
    timeframe = re.sub(r'[^a-zA-Z0-9]', '', timeframe)
    
    # If it's just a number, append 'M' for minutes
    if timeframe.isdigit():
        return f"{timeframe}M"
    
    # If it already has a suffix (like 15M or 4H), return as is
    return timeframe

def standardize_symbol(symbol: str) -> str:
    """Standardize trading symbols to a consistent format"""
    # Remove any whitespace and convert to uppercase
    symbol = symbol.strip().upper()
    
    # Convert TradingView style symbols to OANDA format
    if '_' not in symbol and '/' in symbol:
        # Convert "EUR/USD" format to "EUR_USD"
        symbol = symbol.replace('/', '_')
    
    # Handle some common symbol variations
    if symbol == 'XAUUSD':
        return 'XAU_USD'
    elif symbol == 'XAGUSD':
        return 'XAG_USD'
    elif symbol == 'BTCUSD':
        return 'BTC_USD'
    
    # Check if it's a forex pair without proper formatting
    forex_currencies = ['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'NZD', 'CAD', 'CHF']
    if '_' not in symbol and len(symbol) == 6:
        for i in range(3, 6):
            base = symbol[:i]
            quote = symbol[i:]
            if base in forex_currencies and quote in forex_currencies:
                return f"{base}_{quote}"
    
    return symbol

async def process_alert(alert_data: Dict[str, Any]) -> bool:
    """Process trading alerts with comprehensive risk management and circuit breaker"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Processing alert: {json.dumps(alert_data, indent=2)}")

    try:
        if not alert_data:
            logger.error(f"[{request_id}] Empty alert data received")
            return False

        # CHECK CIRCUIT BREAKER FIRST
        if error_recovery and await error_recovery.circuit_breaker.is_open():
            logger.warning(f"[{request_id}] Circuit breaker is open, rejecting alert")
            # Log that this request was stopped by circuit breaker
            await send_notification(
                "Alert Rejected: Circuit Breaker Open",
                f"Trading alert for {alert_data.get('symbol', 'unknown')} was rejected because the circuit breaker is open.",
                "warning"
            )
            return False
    
        async with _lock:
            action = alert_data['action'].upper()
            symbol = alert_data['symbol']
            instrument = standardize_symbol(symbol)
            
            # Ensure timeframe is properly formatted
            if 'timeframe' not in alert_data:
                logger.warning(f"[{request_id}] No timeframe provided in alert data, using default")
                alert_data['timeframe'] = "15M"  # Default timeframe
            else:
                original_tf = alert_data['timeframe']
                alert_data['timeframe'] = ensure_proper_timeframe(alert_data['timeframe'])
                logger.info(f"[{request_id}] Normalized timeframe from {original_tf} to {alert_data['timeframe']}")
                
            timeframe = alert_data['timeframe']
            logger.info(f"[{request_id}] Standardized instrument: {instrument}, Action: {action}, Timeframe: {timeframe}")
            
            # Position closure logic
            if action in ['CLOSE', 'CLOSE_LONG', 'CLOSE_SHORT']:
                logger.info(f"[{request_id}] Processing close request")
                success, result = await close_position(alert_data, position_tracker)
                logger.info(f"[{request_id}] Close position result: success={success}, result={json.dumps(result)}")
                if success:
                    await position_tracker.clear_position(symbol)
                    await risk_manager.clear_position(symbol)
                    await dynamic_exit_manager.clear_exits(symbol)
                    await loss_manager.clear_position(symbol)
                    await risk_analytics.clear_position(symbol)
                return success
            
            # Market condition check with detailed logging
            tradeable, reason = is_instrument_tradeable(instrument)
            logger.info(f"[{request_id}] Instrument {instrument} tradeable check: {tradeable}, Reason: {reason}")
            
            if not tradeable:
                logger.warning(f"[{request_id}] Market check failed: {reason}")
                return False
            
            # Get market data
            current_price = await get_current_price(instrument, action)
            logger.info(f"[{request_id}] Got current price: {current_price}")
            
            atr = await get_atr(instrument, timeframe)
            logger.info(f"[{request_id}] Got ATR value: {atr}")
            
            # Analyze market structure
            market_structure = await market_structure_analyzer.analyze_market_structure(
                symbol, timeframe, current_price, current_price, current_price
            )
            logger.info(f"[{request_id}] Market structure analysis complete")
            
            # Update volatility monitoring
            await volatility_monitor.update_volatility(symbol, atr, timeframe)
            market_condition = await volatility_monitor.get_market_condition(symbol)
            logger.info(f"[{request_id}] Market condition: {market_condition}")
            
            # Get existing positions for correlation
            existing_positions = await position_tracker.get_all_positions()
            correlation_factor = await position_sizing.get_correlation_factor(
                symbol, list(existing_positions.keys())
            )
            logger.info(f"[{request_id}] Correlation factor: {correlation_factor}")
            
            # Use nearest support/resistance for stop loss if available
            stop_price = None
            if action == 'BUY' and market_structure['nearest_support']:
                stop_price = market_structure['nearest_support']
            elif action == 'SELL' and market_structure['nearest_resistance']:
                stop_price = market_structure['nearest_resistance']
            
            # Otherwise use ATR-based stop
            if not stop_price:
                instrument_type = get_instrument_type(instrument)
                tf_multiplier = risk_manager.atr_multipliers[instrument_type].get(
                    timeframe, risk_manager.atr_multipliers[instrument_type]["1H"]
                )
                
                if action == 'BUY':
                    stop_price = current_price - (atr * tf_multiplier)
                else:
                    stop_price = current_price + (atr * tf_multiplier)
            
            logger.info(f"[{request_id}] Calculated stop price: {stop_price}")
            
            # Calculate position size
            account_balance = await get_account_balance(alert_data.get('account', config.oanda_account))
            logger.info(f"[{request_id}] Account balance: {account_balance}")
            
            position_size = await position_sizing.calculate_position_size(
                account_balance,
                current_price,
                stop_price,
                atr,
                timeframe,
                market_condition,
                correlation_factor
            )
            
            # Log the original calculated size
            logger.info(f"[{request_id}] Calculated position size: {position_size}")
            
            # Ensure position size is within valid range (1-100)
            position_size = max(1.0, min(100.0, position_size))
            logger.info(f"[{request_id}] Final adjusted position size: {position_size}")
            
            # Update alert data with calculated position size
            alert_data['percentage'] = position_size

            # About to execute trade
            logger.info(f"[{request_id}] About to execute trade with data: {json.dumps(alert_data)}")
            
            # Execute trade
            success, result = await execute_trade(alert_data)
            logger.info(f"[{request_id}] Trade execution result: success={success}, result={json.dumps(result) if isinstance(result, dict) else str(result)}")
            
            if success:
                # Extract entry price and units from result
                entry_price = float(result.get('orderFillTransaction', {}).get('price', current_price))
                units = float(result.get('orderFillTransaction', {}).get('units', position_size))
                logger.info(f"[{request_id}] Trade executed with entry price: {entry_price}, units: {units}")
                
                # Initialize position tracking in all managers
                await risk_manager.initialize_position(
                    symbol,
                    entry_price,
                    'LONG' if action == 'BUY' else 'SHORT',
                    timeframe,
                    units,
                    atr
                )
                
                await dynamic_exit_manager.initialize_exits(
                    symbol,
                    entry_price,
                    'LONG' if action == 'BUY' else 'SHORT',
                    stop_price,
                    entry_price + (abs(entry_price - stop_price) * 2)  # 2:1 initial take profit
                )
                
                await loss_manager.initialize_position(
                    symbol,
                    entry_price,
                    'LONG' if action == 'BUY' else 'SHORT',
                    units,
                    account_balance
                )
                
                await risk_analytics.initialize_position(
                    symbol,
                    entry_price,
                    units
                )
                
                # Update portfolio heat
                await position_sizing.update_portfolio_heat(position_size)
                
                # Record position
                await position_tracker.record_position(
                    symbol,
                    action,
                    timeframe,
                    entry_price
                )
                
                logger.info(f"[{request_id}] Trade executed successfully with comprehensive risk management")
            else:
                logger.warning(f"[{request_id}] Trade execution failed: {result}")
                
            return success
                
    except Exception as e:
        logger.error(f"[{request_id}] Critical error: {str(e)}", exc_info=True)
        # Record error in circuit breaker and recovery system
        if error_recovery:
            error_context = {"func": process_alert, "args": [alert_data], "handler": None}
            await error_recovery.handle_error(request_id, "process_alert", e, error_context)
        return False

@handle_async_errors
async def execute_trade(alert_data: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    """Execute trade with improved retry logic and error handling"""
    request_id = str(uuid.uuid4())
    instrument = standardize_symbol(alert_data['symbol'])
    
    # Add detailed logging at the beginning
    logger.info(f"[{request_id}] Executing trade for {instrument} - Action: {alert_data['action']}")
    
    try:
        # Normalize timeframe format - ensure it exists and is properly formatted
        if 'timeframe' not in alert_data:
            logger.warning(f"[{request_id}] No timeframe provided in alert data, using default")
            alert_data['timeframe'] = "15M"  # Default timeframe
        else:
            original_tf = alert_data['timeframe']
            alert_data['timeframe'] = ensure_proper_timeframe(alert_data['timeframe'])
            logger.info(f"[{request_id}] Normalized timeframe from {original_tf} to {alert_data['timeframe']}")
        
        # Calculate size and get current price
        balance = await get_account_balance(alert_data.get('account', config.oanda_account))
        units, precision = await calculate_trade_size(instrument, alert_data['percentage'], balance)
        if alert_data['action'].upper() == 'SELL':
            units = -abs(units)
            
        # Get current price for stop loss and take profit calculations
        current_price = await get_current_price(instrument, alert_data['action'])
        
        # Calculate stop loss and take profit levels
        atr = await get_atr(instrument, alert_data['timeframe'])
        instrument_type = get_instrument_type(instrument)
        
        # Get ATR multiplier based on timeframe and instrument
        atr_multiplier = get_atr_multiplier(instrument_type, alert_data['timeframe'])
        
        # Set price precision based on instrument
        # Most forex pairs use 5 decimal places, except JPY pairs which use 3
        price_precision = 3 if "JPY" in instrument else 5
        
        # Calculate stop loss and take profit levels with proper rounding
        if alert_data['action'].upper() == 'BUY':
            stop_loss = round(current_price - (atr * atr_multiplier), price_precision)
            take_profits = [
                round(current_price + (atr * atr_multiplier), price_precision),  # 1:1
                round(current_price + (atr * atr_multiplier * 2), price_precision),  # 2:1
                round(current_price + (atr * atr_multiplier * 3), price_precision)  # 3:1
            ]
        else:  # SELL
            stop_loss = round(current_price + (atr * atr_multiplier), price_precision)
            take_profits = [
                round(current_price - (atr * atr_multiplier), price_precision),  # 1:1
                round(current_price - (atr * atr_multiplier * 2), price_precision),  # 2:1
                round(current_price - (atr * atr_multiplier * 3), price_precision)  # 3:1
            ]
        
        # Create order data with stop loss and take profit using rounded values
        order_data = {
            "order": {
                "type": alert_data['orderType'],
                "instrument": instrument,
                "units": str(units),
                "timeInForce": alert_data['timeInForce'],
                "positionFill": "DEFAULT",
                "stopLossOnFill": {
                    "price": str(stop_loss),
                    "timeInForce": "GTC",
                    "triggerMode": "TOP_OF_BOOK"
                },
                "takeProfitOnFill": {
                    "price": str(take_profits[0]),  # First take profit level
                    "timeInForce": "GTC",
                    "triggerMode": "TOP_OF_BOOK"
                }
            }
        }
        
        # Add trailing stop if configured, also with proper rounding
        if alert_data.get('use_trailing_stop', True):
            trailing_distance = round(atr * atr_multiplier, price_precision)
            order_data["order"]["trailingStopLossOnFill"] = {
                "distance": str(trailing_distance),
                "timeInForce": "GTC",
                "triggerMode": "TOP_OF_BOOK"
            }
        
        # Log the order details for debugging
        logger.info(f"[{request_id}] Order data: {json.dumps(order_data)}")
        
        # Get session and API URL
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{alert_data.get('account', config.oanda_account)}/orders"
        
        # Execute trade with retries
        retries = 0
        while retries < config.max_retries:
            try:
                async with session.post(url, json=order_data, timeout=HTTP_REQUEST_TIMEOUT) as response:
                    response_text = await response.text()
                    logger.info(f"[{request_id}] Response status: {response.status}, Response: {response_text}")
                    
                    if response.status == 201:
                        result = json.loads(response_text)
                        logger.info(f"[{request_id}] Trade executed successfully with stops: {result}")
                        return True, result
                    
                    # Extract and log the specific error for better debugging
                    try:
                        error_data = json.loads(response_text)
                        error_code = error_data.get("errorCode", "UNKNOWN_ERROR")
                        error_message = error_data.get("errorMessage", "Unknown error")
                        logger.error(f"[{request_id}] OANDA error: {error_code} - {error_message}")
                    except:
                        pass
                    
                    # Handle error responses
                    if "RATE_LIMIT" in response_text:
                        await asyncio.sleep(60)  # Longer wait for rate limits
                    elif "MARKET_HALTED" in response_text:
                        return False, {"error": "Market is halted"}
                    else:
                        delay = config.base_delay * (2 ** retries)
                        await asyncio.sleep(delay)
                    
                    logger.warning(f"[{request_id}] Retry {retries + 1}/{config.max_retries}")
                    retries += 1
                    
            except aiohttp.ClientError as e:
                logger.error(f"[{request_id}] Network error: {str(e)}")
                if retries < config.max_retries - 1:
                    await asyncio.sleep(config.base_delay * (2 ** retries))
                    retries += 1
                    continue
                return False, {"error": f"Network error: {str(e)}"}
        
        return False, {"error": "Maximum retries exceeded"}
        
    except Exception as e:
        logger.error(f"[{request_id}] Error executing trade: {str(e)}")
        return False, {"error": str(e)}

async def get_atr(instrument: str, timeframe: str) -> float:
    """Get ATR value with timeframe normalization"""
    # Normalize the timeframe format
    normalized_timeframe = ensure_proper_timeframe(timeframe)
    logger.debug(f"ATR calculation: Normalized timeframe from {timeframe} to {normalized_timeframe}")
    
    instrument_type = get_instrument_type(instrument)
    
    # Default ATR values by timeframe and instrument type
    default_atr_values = {
        "FOREX": {
            "15M": 0.0010,  # 10 pips
            "1H": 0.0025,   # 25 pips
            "4H": 0.0050,   # 50 pips
            "D": 0.0100     # 100 pips
        },
        "STOCK": {
            "15M": 0.01,    # 1% for stocks
            "1H": 0.02,     # 2% for stocks
            "4H": 0.03,     # 3% for stocks
            "D": 0.05       # 5% for stocks
        },
        "COMMODITY": {
            "15M": 0.05,    # 0.05% for commodities
            "1H": 0.10,     # 0.1% for commodities
            "4H": 0.20,     # 0.2% for commodities
            "D": 0.50       # 0.5% for commodities
        }
    }
    
    # Get the ATR value for this instrument and timeframe
    return default_atr_values[instrument_type].get(normalized_timeframe, default_atr_values[instrument_type]["1H"])

def get_atr_multiplier(instrument_type: str, timeframe: str) -> float:
    """Get the appropriate ATR multiplier based on instrument type and timeframe"""
    # Normalize the timeframe format
    normalized_timeframe = ensure_proper_timeframe(timeframe)
    logger.debug(f"ATR multiplier: Normalized timeframe from {timeframe} to {normalized_timeframe}")
    
    # Default multipliers by instrument type and timeframe
    default_multipliers = {
        "FOREX": {
            "15M": 1.5
        },
        "STOCK": {
            "15M": 2.0,
            "1H": 2.5,
            "4H": 3.0,
            "D": 3.5
        },
        "COMMODITY": {
            "15M": 1.8,
            "1H": 2.2,
            "4H": 2.7,
            "D": 3.2
        }
    }
    
    # Return the appropriate multiplier or default to 1H if timeframe not found
    return default_multipliers[instrument_type].get(normalized_timeframe, default_multipliers[instrument_type]["1H"]) 

def handle_async_errors(func):
    """Decorator to handle async errors"""
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
            return False, {"error": str(e)}
    return wrapper

# Placeholder implementations for functions used in process_alert
async def close_position(alert_data: Dict[str, Any], position_tracker) -> Tuple[bool, Dict[str, Any]]:
    """Close an existing position"""
    request_id = str(uuid.uuid4())
    symbol = standardize_symbol(alert_data['symbol'])
    logger.info(f"[{request_id}] Closing position for {symbol}")
    
    try:
        # Get current position details
        position_info = await position_tracker.get_position(symbol)
        if not position_info:
            logger.warning(f"[{request_id}] No active position found for {symbol}")
            return False, {"error": "No active position found"}
        
        position_type = position_info.get('type', 'UNKNOWN')
        logger.info(f"[{request_id}] Found {position_type} position for {symbol}")
        
        # Determine the appropriate action to close the position
        close_action = "SELL" if position_type == "LONG" else "BUY"
        
        # Create the close order data
        close_data = {
            "order": {
                "type": "MARKET",
                "instrument": symbol,
                "units": str(-position_info.get('units', 100)),  # Negative units to close
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
                        
                        # Calculate profit/loss
                        try:
                            fill_info = result.get('orderFillTransaction', {})
                            entry_price = position_info.get('entry_price', 0)
                            exit_price = float(fill_info.get('price', 0))
                            units = float(fill_info.get('units', 0))
                            pl = fill_info.get('pl', '0')
                            
                            # Record trade results for analytics
                            await risk_analytics.record_trade_result(
                                symbol, 
                                position_type,
                                entry_price,
                                exit_price,
                                abs(units),
                                float(pl)
                            )
                            
                            logger.info(f"[{request_id}] Trade result recorded: {position_type}, entry: {entry_price}, exit: {exit_price}, PL: {pl}")
                        except Exception as e:
                            logger.error(f"[{request_id}] Error recording trade result: {str(e)}")
                        
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

def is_instrument_tradeable(instrument: str) -> Tuple[bool, str]:
    """Check if an instrument is tradeable based on market hours and conditions"""
    try:
        # Get current time in UTC
        current_time = datetime.utcnow()
        current_day = current_time.weekday()  # Monday is 0, Sunday is 6
        current_hour = current_time.hour
        
        # Check if it's weekend (forex markets closed)
        if current_day >= 5:  # Saturday or Sunday
            return False, "Weekend - forex markets are closed"
            
        # Handle forex specific rules
        if "JPY" in instrument or any(x in instrument for x in ["USD", "EUR", "GBP", "AUD", "NZD", "CAD", "CHF"]):
            # Check for forex market hours
            # Forex markets are closed from Friday 22:00 UTC to Sunday 22:00 UTC
            if current_day == 4 and current_hour >= 22:  # Friday after 22:00
                return False, "Forex markets closed for the weekend"
            if current_day == 6 and current_hour < 22:  # Sunday before 22:00
                return False, "Forex markets closed for the weekend"
                
            # Check for low liquidity periods
            if 22 <= current_hour or current_hour <= 1:
                return True, "Forex market open (low liquidity period)"
                
            return True, "Forex market open"
            
        # Handle stock market hours (assuming US stocks)
        if "_" in instrument and any(x in instrument for x in ["US", "NYSE", "NASDAQ"]):
            # US stock markets are open 9:30 AM to 4:00 PM Eastern Time
            # Convert to UTC (Eastern Time is UTC-5 or UTC-4 during daylight saving)
            # For simplicity, we'll use a rough approximation
            if current_day >= 0 and current_day <= 4:  # Monday to Friday
                if 14 <= current_hour < 21:  # 9:30 AM to 4:00 PM ET is roughly 14:30 to 21:00 UTC
                    return True, "Stock market open"
            return False, "Stock market closed"
            
        # For other instruments (commodities, etc.), check specific rules
        # For simplicity, we'll just return true
        return True, "Market is open"
        
    except Exception as e:
        logger.error(f"Error checking if instrument is tradeable: {str(e)}")
        # Default to closed when there's an error, to be safe
        return False, f"Error checking market hours: {str(e)}"

async def get_current_price(instrument: str, action: str) -> float:
    """Get the current price for an instrument"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Getting current price for {instrument}, action: {action}")
    
    try:
        # Get session and API URL
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{config.oanda_account}/pricing"
        
        # Set up the query parameters
        params = {
            "instruments": instrument,
            "includeUnitsAvailable": "true"
        }
        
        # Make the API request with retries
        retries = 0
        while retries < config.max_retries:
            try:
                async with session.get(url, params=params, timeout=HTTP_REQUEST_TIMEOUT) as response:
                    if response.status == 200:
                        response_data = await response.json()
                        prices = response_data.get("prices", [])
                        
                        if not prices:
                            logger.warning(f"[{request_id}] No pricing data found for {instrument}")
                            raise ValueError(f"No pricing data found for {instrument}")
                            
                        price_data = prices[0]
                        
                        # Get the appropriate price based on the action
                        if action.upper() == "BUY":
                            # For buying, use the ask price
                            current_price = float(price_data.get("asks", [{}])[0].get("price", 0))
                        else:
                            # For selling, use the bid price
                            current_price = float(price_data.get("bids", [{}])[0].get("price", 0))
                            
                        logger.info(f"[{request_id}] Retrieved {action} price for {instrument}: {current_price}")
                        return current_price
                    
                    # Handle error responses
                    response_text = await response.text()
                    logger.error(f"[{request_id}] Error getting price: {response.status}, Response: {response_text}")
                    
                    if "RATE_LIMIT" in response_text:
                        await asyncio.sleep(60)  # Longer wait for rate limits
                    else:
                        delay = config.base_delay * (2 ** retries)
                        await asyncio.sleep(delay)
                    
                    retries += 1
                    
            except aiohttp.ClientError as e:
                logger.error(f"[{request_id}] Network error getting price: {str(e)}")
                if retries < config.max_retries - 1:
                    await asyncio.sleep(config.base_delay * (2 ** retries))
                    retries += 1
                    continue
                raise
        
        # If we've exhausted all retries without success
        raise ValueError(f"Failed to get price for {instrument} after {config.max_retries} attempts")
        
    except Exception as e:
        logger.error(f"[{request_id}] Error getting current price: {str(e)}", exc_info=True)
        # Return a placeholder value in case of error
        return 1.1234  # Placeholder value

async def get_account_balance(account_id: str) -> float:
    """Get the current account balance"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Getting account balance for {account_id}")
    
    try:
        # Get session and API URL
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{account_id}"
        
        # Make the API request with retries
        retries = 0
        while retries < config.max_retries:
            try:
                async with session.get(url, timeout=HTTP_REQUEST_TIMEOUT) as response:
                    if response.status == 200:
                        response_data = await response.json()
                        account = response_data.get("account", {})
                        balance = float(account.get("balance", 0))
                        
                        logger.info(f"[{request_id}] Retrieved account balance: {balance}")
                        return balance
                    
                    # Handle error responses
                    response_text = await response.text()
                    logger.error(f"[{request_id}] Error getting account balance: {response.status}, Response: {response_text}")
                    
                    if "RATE_LIMIT" in response_text:
                        await asyncio.sleep(60)  # Longer wait for rate limits
                    else:
                        delay = config.base_delay * (2 ** retries)
                        await asyncio.sleep(delay)
                    
                    retries += 1
                    
            except aiohttp.ClientError as e:
                logger.error(f"[{request_id}] Network error getting account balance: {str(e)}")
                if retries < config.max_retries - 1:
                    await asyncio.sleep(config.base_delay * (2 ** retries))
                    retries += 1
                    continue
                raise
        
        # If we've exhausted all retries without success
        raise ValueError(f"Failed to get account balance after {config.max_retries} attempts")
        
    except Exception as e:
        logger.error(f"[{request_id}] Error getting account balance: {str(e)}", exc_info=True)
        # Return a placeholder value in case of error
        return 10000.0  # Placeholder value

async def send_notification(title: str, message: str, level: str = "info") -> None:
    """Send a notification to the user"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Sending notification: {title} - {message}")
    
    try:
        # Choose the appropriate logging level
        log_level = getattr(logging, level.upper(), logging.INFO)
        
        # Log the notification
        logger.log(log_level, f"NOTIFICATION: {title} - {message}")
        
        # You could implement different notification methods here:
        # 1. Email notifications
        # 2. SMS notifications
        # 3. Push notifications
        # 4. Slack/Discord webhooks
        
        # Example for email notification (placeholder)
        if level.upper() in ["WARNING", "ERROR", "CRITICAL"]:
            # In a real implementation, you would send an actual email here
            logger.info(f"[{request_id}] Would send urgent email for {level} notification: {title}")
        
        # Example for Slack notification (placeholder)
        notification_data = {
            "text": f"*{title}*\n{message}",
            "color": {
                "info": "good",
                "warning": "warning",
                "error": "danger",
                "critical": "danger"
            }.get(level.lower(), "good")
        }
        
        # In a real implementation, you would send to a webhook here
        logger.debug(f"[{request_id}] Would send Slack notification: {json.dumps(notification_data)}")
        
    except Exception as e:
        logger.error(f"[{request_id}] Error sending notification: {str(e)}", exc_info=True)

async def calculate_trade_size(instrument: str, percentage: float, balance: float) -> Tuple[float, int]:
    """Calculate the size of the trade based on percentage of balance"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Calculating trade size for {instrument} with {percentage}% of ${balance}")
    
    try:
        # Get current price
        current_price = await get_current_price(instrument, "BUY")
        
        # Determine the instrument type
        instrument_type = get_instrument_type(instrument)
        
        # Set precision and calculate size based on instrument type
        if instrument_type == "FOREX":
            # For forex, size is in base currency units
            # The precision is typically 1 for most brokers
            precision = 0
            
            # Calculate the position size in base currency
            # percentage is the risk percentage (e.g., 2% of account)
            risk_amount = balance * (percentage / 100)
            
            # For forex, convert to units (micro/mini/standard lots)
            if "JPY" in instrument:
                # JPY pairs have different pricing
                units = int(risk_amount * 1000)  # Approximate for JPY pairs
            else:
                units = int(risk_amount * 10000)  # Standard conversion for most forex pairs
            
            # Ensure minimum size
            units = max(1, units)
            
            logger.info(f"[{request_id}] Calculated forex position size: {units} units")
            return units, precision
            
        elif instrument_type == "STOCK":
            # For stocks, size is in number of shares
            precision = 0  # Whole shares
            
            # Calculate the position size in shares
            risk_amount = balance * (percentage / 100)
            shares = int(risk_amount / current_price)
            
            # Ensure minimum size
            shares = max(1, shares)
            
            logger.info(f"[{request_id}] Calculated stock position size: {shares} shares")
            return shares, precision
            
        else:  # COMMODITY
            # For commodities, it depends on the specific instrument
            if instrument in ["XAU_USD", "GOLD"]:
                precision = 2  # Gold can be traded in decimals with some brokers
                
                # Calculate position size in ounces
                risk_amount = balance * (percentage / 100)
                ounces = round(risk_amount / current_price, precision)
                
                # Ensure minimum size
                ounces = max(0.01, ounces)
                
                logger.info(f"[{request_id}] Calculated gold position size: {ounces} ounces")
                return ounces, precision
                
            else:
                # Default for other commodities
                precision = 0
                units = int(balance * (percentage / 100) / current_price)
                units = max(1, units)
                
                logger.info(f"[{request_id}] Calculated commodity position size: {units} units")
                return units, precision
                
    except Exception as e:
        logger.error(f"[{request_id}] Error calculating trade size: {str(e)}", exc_info=True)
        # Return placeholder values in case of error
        return 100.0, 0  # Default to 100 units with 0 decimal precision

async def get_session() -> aiohttp.ClientSession:
    """Get or create an HTTP session"""
    # In a real implementation, this would maintain a singleton session
    # But for simplicity in this example, we'll create a new one each time
    
    # Set up authentication headers
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer YOUR_OANDA_API_KEY"  # Replace with actual API key mechanism
    }
    
    # Create a ClientSession with the headers
    session = aiohttp.ClientSession(headers=headers)
    
    return session  

# Add this function to handle API errors with proper logging
async def log_api_error(operation: str, request_id: str, error_str: str, status_code: int = None):
    """Log API errors properly with request information"""
    if status_code:
        logger.error(f"Error in {operation} (request {request_id}): {error_str} (Status: {status_code})")
    else:
        logger.error(f"Error in {operation} (request {request_id}): {error_str}")
    
    # Could also add error tracking/monitoring here
    if error_recovery:
        await error_recovery.circuit_breaker.record_failure()