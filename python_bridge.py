##############################################################################
# Core Setup - Block 1: Imports, Error Handling, Configuration
##############################################################################

import os
import uuid
import asyncio
import aiohttp
import logging
import logging.handlers
import re
import time
import json
import signal
import holidays
import statistics
import numpy as np
from datetime import datetime, timedelta
from pytz import timezone
from fastapi import FastAPI, Request, HTTPException, status, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any, Union, List, Tuple, Callable, TypeVar, ParamSpec
from contextlib import asynccontextmanager
from pydantic import BaseModel, validator, ValidationError, Field, root_validator
from functools import wraps
from redis.asyncio import Redis
from prometheus_client import Counter, Histogram
from pydantic_settings import BaseSettings

# Type variables for type hints
P = ParamSpec('P')
T = TypeVar('T')

# Prometheus metrics
TRADE_REQUESTS = Counter('trade_requests', 'Total trade requests')
TRADE_LATENCY = Histogram('trade_latency', 'Trade processing latency')

# Redis for shared state
redis = Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379'))

##############################################################################
# Primary Models - Define these first to avoid reference errors
##############################################################################

class AlertData(BaseModel):
    """Alert data model with improved validation"""
    symbol: str
    timeframe: str 
    action: str
    price: float
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    risk_percentage: Optional[float] = None
    message: Optional[str] = None
    
    class Config:
        extra = "forbid"
        
    @root_validator(pre=True)
    def validate_data(cls, values):
        """Validate incoming alert data"""
        try:
            # Check for required fields
            required_fields = ['symbol', 'timeframe', 'action']
            for field in required_fields:
                if field not in values:
                    raise ValueError(f"Missing required field: {field}")
                    
            # Standardize symbol format
            values['symbol'] = standardize_symbol(values['symbol'])
            
            # Validate timeframe
            valid_timeframes = ['15M', '1H', '4H', '1D']
            if values['timeframe'] not in valid_timeframes:
                logger.warning(f"Invalid timeframe: {values['timeframe']}. Defaulting to 1H")
                values['timeframe'] = '1H'
                
            # Validate action
            values['action'] = values['action'].upper()
            if values['action'] not in ['BUY', 'SELL']:
                raise ValueError(f"Invalid action: {values['action']}. Must be BUY or SELL")
                
            # Handle missing or invalid price - will be set during processing
            if 'price' not in values or values['price'] is None or values['price'] <= 0:
                logger.info(f"Missing or invalid price, will fetch current price during processing")
                values['price'] = 0.0  # Will be replaced with current price later
                
            # Set default risk percentage if not provided
            if 'risk_percentage' not in values or not values['risk_percentage']:
                values['risk_percentage'] = 2.0  # Default 2% risk per trade
                
            return values
        except Exception as e:
            logger.error(f"Error validating alert data: {str(e)}")
            raise ValueError(f"Error validating alert data: {str(e)}")

##############################################################################
# Error Handling Infrastructure
##############################################################################

class TradingError(Exception):
    """Base exception for trading-related errors"""
    pass

class MarketError(TradingError):
    """Errors related to market conditions"""
    pass

class OrderError(TradingError):
    """Errors related to order execution"""
    pass

class CustomValidationError(TradingError):
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

class Settings(BaseSettings):
    """Centralized configuration management"""
    oanda_account: str = Field(alias='OANDA_ACCOUNT_ID')
    oanda_token: str = Field(alias='OANDA_API_TOKEN')
    oanda_api_url: str = Field(
        default="https://api-fxtrade.oanda.com/v3",
        alias='OANDA_API_URL'
    )
    oanda_environment: str = Field(
        default="practice",
        alias='OANDA_ENVIRONMENT'
    )
    allowed_origins: str = "http://localhost"
    connect_timeout: int = 10
    read_timeout: int = 30
    total_timeout: int = 45
    max_simultaneous_connections: int = 100
    spread_threshold_forex: float = 0.001
    spread_threshold_crypto: float = 0.008
    max_retries: int = 3
    base_delay: float = 1.0
    base_position: int = 5000  # Updated from 300000 to 3000
    max_daily_loss: float = 0.20  # 20% max daily loss
    host: str = "0.0.0.0"
    port: int = 8000
    environment: str = "production"
    max_requests_per_minute: int = 100  # Added missing config parameter

    trade_24_7: bool = True  # Set to True for exchanges trading 24/7

    class Config:
        env_file = '.env'
        case_sensitive = True
        
config = Settings()

# Add this back for monitoring purposes
MAX_DAILY_LOSS = config.max_daily_loss

# Session Configuration
HTTP_REQUEST_TIMEOUT = aiohttp.ClientTimeout(
    total=config.total_timeout,
    connect=config.connect_timeout,
    sock_read=config.read_timeout
)

# Market Session Configuration
MARKET_SESSIONS = {
    "FOREX": {
        "hours": "24/5",
        "timezone": "Asia/Bangkok",
        "holidays": "US"
    },
    "XAU_USD": {
        "hours": "23:00-21:59",
        "timezone": "UTC",
        "holidays": []
    },
    "CRYPTO": {
        "hours": "24/7",
        "timezone": "UTC",
        "holidays": []
    }
}

# 1. Update INSTRUMENT_LEVERAGES based on Singapore MAS regulations and your full pair list
INSTRUMENT_LEVERAGES = {
    # Forex - major pairs
    "USD_CHF": 33.3, "EUR_USD": 50, "GBP_USD": 20,
    "USD_JPY": 20, "AUD_USD": 33.3, "USD_THB": 20,
    "CAD_CHF": 33.3, "NZD_USD": 33.3, "AUD_CAD": 33.3,
    # Additional forex pairs
    "AUD_JPY": 20, "USD_SGD": 20, "EUR_JPY": 20,
    "GBP_JPY": 20, "USD_CAD": 50, "NZD_JPY": 20,
    # Crypto - 2:1 leverage
    "BTC_USD": 2, "ETH_USD": 2, "XRP_USD": 2, "LTC_USD": 2, "BTCUSD": 2,
    # Gold - 10:1 leverage
    "XAU_USD": 10
    # Add more pairs from your forex list as needed
}

# Rest of the constants remain the same...

##############################################################################
# Block 2: Models, Logging, and Session Management
##############################################################################

##############################################################################
# Logging Setup
##############################################################################

class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging"""
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
    """Setup logging with improved error handling and rotation"""
    try:
        log_dir = '/opt/render/project/src/logs'
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'trading_bot.log')
    except Exception as e:
        log_file = 'trading_bot.log'
        logging.warning(f"Using default log file due to error: {str(e)}")

    formatter = JSONFormatter()
    
    # Configure file handler with proper encoding and rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Clear existing handlers
    root_logger = logging.getLogger()
    for hdlr in root_logger.handlers[:]:
        root_logger.removeHandler(hdlr)
    
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return logging.getLogger('trading_bot')

logger = setup_logging()

##############################################################################
# Session Management
##############################################################################

_session: Optional[aiohttp.ClientSession] = None

async def get_session(force_new: bool = False) -> aiohttp.ClientSession:
    """Get or create a session with improved error handling"""
    global _session
    try:
        if _session is None or _session.closed or force_new:
            if _session and not _session.closed:
                await _session.close()
            
            _session = aiohttp.ClientSession(
                timeout=HTTP_REQUEST_TIMEOUT,
                headers={
                    "Authorization": f"Bearer {config.oanda_token}",
                    "Content-Type": "application/json",
                    "Accept-Datetime-Format": "RFC3339"
                }
            )
        return _session
    except Exception as e:
        logger.error(f"Session creation error: {str(e)}")
        raise

async def cleanup_stale_sessions():
    """Cleanup stale sessions"""
    try:
        if _session and not _session.closed:
            await _session.close()
    except Exception as e:
        logger.error(f"Error cleaning up sessions: {str(e)}")

##############################################################################
# Position Tracking
##############################################################################

@handle_async_errors
async def get_open_positions(account_id: Optional[str] = None) -> Tuple[bool, Dict[str, Any]]:
    """Fetch open positions for an account with improved error handling"""
    try:
        session = await get_session()
        account = account_id or config.oanda_account
        url = f"{config.oanda_api_url}/accounts/{account}/openPositions"
        
        async with session.get(url, timeout=HTTP_REQUEST_TIMEOUT) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"Failed to fetch positions: {error_text}")
                return False, {"error": error_text}
            
            data = await response.json()
            return True, data
    except asyncio.TimeoutError:
        logger.error(f"Timeout fetching positions for account {account}")
        return False, {"error": "Request timed out"}
    except Exception as e:
        logger.error(f"Error fetching positions: {str(e)}")
        return False, {"error": str(e)}

class PositionTracker:
    def __init__(self):
        self.positions = {}
        self.bar_times = {}
        self._lock = asyncio.Lock()
        self._running = False
        self._initialized = False
        self.daily_pnl = 0.0
        self.pnl_reset_date = datetime.now().date()
        self._price_monitor_task = None

    @handle_async_errors
    async def reconcile_positions(self):
        """Reconcile positions with improved error handling and timeout"""
        while self._running:
            try:
                # Wait between reconciliation attempts
                await asyncio.sleep(900)  # Every 15 minutes
                
                logger.info("Starting position reconciliation")
                async with self._lock:
                    async with asyncio.timeout(60):  # Increased timeout to 60 seconds
                        success, positions_data = await get_open_positions()
                    
                        if not success:
                            logger.error("Failed to fetch positions for reconciliation")
                            continue
                    
                        # Convert Oanda positions to a set for efficient lookup
                        oanda_positions = {
                            p['instrument'] for p in positions_data.get('positions', [])
                        }
                    
                        # Check each tracked position
                        for symbol in list(self.positions.keys()):
                            try:
                                if symbol not in oanda_positions:
                                    # Position closed externally
                                    old_data = self.positions.pop(symbol, None)
                                    self.bar_times.pop(symbol, None)
                                    logger.warning(
                                        f"Removing stale position for {symbol}. "
                                        f"Old data: {old_data}"
                                    )
                            except Exception as e:
                                logger.error(
                                    f"Error reconciling position for {symbol}: {str(e)}"
                                )
                        
                        logger.info(
                            f"Reconciliation complete. Active positions: "
                            f"{list(self.positions.keys())}"
                        )
                        
            except asyncio.TimeoutError:
                logger.error("Position reconciliation timed out, will retry in next cycle")
                continue  # Continue to next iteration instead of sleeping
            except asyncio.CancelledError:
                logger.info("Position reconciliation task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in reconciliation loop: {str(e)}")
                await asyncio.sleep(60)  # Wait before retrying on unexpected errors
    
    @handle_async_errors
    async def record_position(self, symbol: str, action: str, timeframe: str, price: float):
        """Record a new position being opened"""
        async with self._lock:
            position_type = "LONG" if action.upper() == "BUY" else "SHORT"
            entry_time = datetime.now(timezone('Asia/Bangkok'))
            
            self.positions[symbol] = {
                "entry_price": price,
                "position_type": position_type,
                "entry_time": entry_time,
                "timeframe": timeframe,
                "bars_since_entry": 0,
                "highest_since_entry": price,
                "lowest_since_entry": price,
                "last_price": price,
                "last_update": entry_time
            }
            
            # Reset bar time tracking
            self.bar_times[symbol] = {}
            
            logger.info(f"Recorded new {position_type} position for {symbol} at {price}")
            return True
    
    @handle_async_errors        
    async def clear_position(self, symbol: str):
        """Clear a position that has been closed"""
        async with self._lock:
            if symbol in self.positions:
                position_data = self.positions.pop(symbol, None)
                self.bar_times.pop(symbol, None)
                logger.info(f"Cleared position tracking for {symbol}: {position_data}")
                return True
            return False
            
    @handle_async_errors
    async def update_price(self, symbol: str, current_price: float):
        """Update price tracking for a position"""
        if symbol not in self.positions:
            return False
            
        async with self._lock:
            position = self.positions[symbol]
            position["last_price"] = current_price
            position["last_update"] = datetime.now(timezone('Asia/Bangkok'))
            
            # Update highest/lowest since entry
            if current_price > position["highest_since_entry"]:
                position["highest_since_entry"] = current_price
                
            if current_price < position["lowest_since_entry"]:
                position["lowest_since_entry"] = current_price
                
            return True
                
    async def start(self):
        """Start the position tracker"""
        if self._running:
            return
            
        self._running = True
        logger.info("Position tracker started")
        
        # Start reconciliation task
        asyncio.create_task(self.reconcile_positions())
    
    async def stop(self):
        """Stop the position tracker"""
        if not self._running:
            return
            
        self._running = False
        logger.info("Position tracker stopped")

##############################################################################
# Class: AlertHandler 
##############################################################################

class AlertHandler:
    """Handles processing of trading alerts with full risk management"""
    
    def __init__(self):
        self.risk_manager = EnhancedRiskManager()
        self.position_sizing = PositionSizingManager()
        self.market_structure = MarketStructureAnalyzer()
        self.volatility_monitor = VolatilityMonitor()
        self.loss_manager = AdvancedLossManager()
        self.exit_manager = DynamicExitManager()
        self.config = TradingConfig()
        self.position_tracker = PositionTracker()
        self._running = False
        self._monitoring_task = None

    async def start(self):
        """Start background tasks for the alert handler"""
        if self._running:
            return
            
        self._running = True
        logger.info("Starting alert handler and monitoring tasks")
        
        # Start position tracker
        await self.position_tracker.start()
        
        # Start background monitoring tasks
        self._monitoring_task = asyncio.create_task(self._monitor_positions())
        
        logger.info("Alert handler initialized with price monitoring")
        
    async def stop(self):
        """Stop background tasks for the alert handler"""
        if not self._running:
            return
            
        self._running = False
        logger.info("Stopping alert handler")
        
        # Stop position tracker
        await self.position_tracker.stop()
        
        # Cancel monitoring task
        if self._monitoring_task and not self._monitoring_task.done():
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
                
        logger.info("Alert handler stopped")
        
    async def _monitor_positions(self):
        """Background task to monitor positions and manage exits"""
        try:
            while self._running:
                # Implement position monitoring logic here if needed
                await asyncio.sleep(60)  # Check every minute
        except asyncio.CancelledError:
            logger.info("Position monitoring cancelled")
        except Exception as e:
            logger.error(f"Error in position monitoring: {str(e)}", exc_info=True)

    async def _get_current_spread(self, symbol: str) -> float:
        """Get current spread for the instrument"""
        # Implement actual spread checking logic
        instrument_type = get_instrument_type(symbol)
        return 0.0001 if instrument_type == "FOREX" else 0.5

    async def _pre_trade_risk_checks(self, alert_data: AlertData, current_price: float,
                                   spread: float, request_id: str) -> Dict[str, Any]:
        """Perform all pre-trade risk checks"""
        checks = {
            "valid": True,
            "reasons": [],
            "adjusted_risk": 1.0
        }

        # 1. Price validation check
        price_diff = abs(alert_data.price - current_price)
        if price_diff > (spread * 3):
            checks["valid"] = False
            checks["reasons"].append(
                f"Price difference too large: {price_diff} vs current {current_price}"
            )

        # 2. Volatility check
        await self.volatility_monitor.initialize_market_condition(
            alert_data.symbol, alert_data.timeframe
        )
        adjust_risk, risk_factor = await self.volatility_monitor.should_adjust_risk(
            alert_data.symbol, alert_data.timeframe
        )
        if adjust_risk:
            checks["adjusted_risk"] = risk_factor
            logger.info(f"Adjusted risk factor to {risk_factor}", 
                       extra={"request_id": request_id})

        # 3. Correlation check
        success, existing_positions = await get_open_positions()
        if success:
            correlation_factor = await self.position_sizing.get_correlation_factor(
                alert_data.symbol, [p['instrument'] for p in existing_positions.get('positions', [])]
            )
            checks["adjusted_risk"] *= correlation_factor

        # 4. Daily loss check
        should_reduce, reduction = await self.loss_manager.should_reduce_risk()
        if should_reduce:
            checks["adjusted_risk"] *= reduction
            logger.warning(f"Reducing risk by {reduction} due to daily loss limits",
                          extra={"request_id": request_id})

        # 5. Market structure analysis 
        market_structure = await self.market_structure.analyze_market_structure(
            alert_data.symbol, alert_data.timeframe, current_price, current_price, current_price
        )
        
        # Calculate stop loss if not provided
        if not alert_data.stop_loss:
            instrument_type = get_instrument_type(alert_data.symbol)
            atr = get_atr(alert_data.symbol, alert_data.timeframe)
            multiplier = get_atr_multiplier(instrument_type, alert_data.timeframe)
            
            if alert_data.action == 'BUY':
                alert_data.stop_loss = current_price - (atr * multiplier)
            else:  # SELL
                alert_data.stop_loss = current_price + (atr * multiplier)
                
            logger.info(f"Auto-calculated stop loss for {alert_data.symbol}: {alert_data.stop_loss}")
            
        # Calculate take profit if not provided
        if not alert_data.take_profit:
            risk = abs(current_price - alert_data.stop_loss)
            
            if alert_data.action == 'BUY':
                alert_data.take_profit = current_price + (risk * 2)  # 1:2 risk-reward
            else:  # SELL
                alert_data.take_profit = current_price - (risk * 2)  # 1:2 risk-reward
                
            logger.info(f"Auto-calculated take profit for {alert_data.symbol}: {alert_data.take_profit}")

        if (alert_data.action == "BUY" and market_structure['nearest_support'] and
            alert_data.stop_loss < market_structure['nearest_support']):
            checks["valid"] = False
            checks["reasons"].append("Stop loss below nearest support level")
            
        if (alert_data.action == "SELL" and market_structure['nearest_resistance'] and
            alert_data.stop_loss > market_structure['nearest_resistance']):
            checks["valid"] = False
            checks["reasons"].append("Stop loss above nearest resistance level")

        if not checks["valid"]:
            checks["status"] = "rejected"
            checks["message"] = ", ".join(checks["reasons"])
            
        return checks

    async def _calculate_position_size(self, alert_data: AlertData, current_price: float,
                                     risk_factor: float, request_id: str) -> float:
        """Calculate position size with risk adjustments"""
        account_data = await get_account_summary()
        balance = account_data.get('balance', 10000)  # Fallback to $10k

        market_condition = await self.volatility_monitor.get_market_condition(alert_data.symbol)
        position_size = await self.position_sizing.calculate_position_size(
            account_balance=balance,
            entry_price=current_price,
            stop_loss=alert_data.stop_loss,
            atr=get_atr(alert_data.symbol, alert_data.timeframe),
            timeframe=alert_data.timeframe,
            market_condition=market_condition,
            correlation_factor=risk_factor
        )

        logger.info(f"Calculated position size: {position_size}",
                   extra={"request_id": request_id})
        return position_size

    async def _execute_trade(self, alert_data: AlertData, position_size: float,
                           current_price: float, request_id: str) -> Dict[str, Any]:
        """Execute the trade with proper error handling"""
        # Implement actual trade execution logic here
        logger.info(f"Executing {alert_data.action} order for {alert_data.symbol} "
                   f"Size: {position_size} @ {current_price}",
                   extra={"request_id": request_id})
        
        # Return simulated trade execution result
        return {
            "status": "success",
            "order_id": str(uuid.uuid4()),
            "symbol": alert_data.symbol,
            "units": position_size,
            "price": current_price,
            "stop_loss": alert_data.stop_loss,
            "take_profit": alert_data.take_profit
        }

    async def _initialize_risk_management(self, alert_data: AlertData, trade_result: Dict[str, Any],
                                        request_id: str):
        """Initialize all risk management systems for the new position"""
        position_type = "LONG" if alert_data.action == "BUY" else "SHORT"
        
        await self.risk_manager.initialize_position(
            symbol=alert_data.symbol,
            entry_price=trade_result['price'],
            position_type=position_type,
            timeframe=alert_data.timeframe,
            units=trade_result['units'],
            atr=get_atr(alert_data.symbol, alert_data.timeframe)
        )
        
        await self.loss_manager.initialize_position(
            symbol=alert_data.symbol,
            entry_price=trade_result['price'],
            position_type=position_type,
            units=trade_result['units'],
            account_balance=trade_result.get('account_balance', 10000)
        )
        
        await self.exit_manager.initialize_exits(
            symbol=alert_data.symbol,
            entry_price=trade_result['price'],
            position_type=position_type,
            initial_stop=alert_data.stop_loss,
            initial_tp=alert_data.take_profit
        )
        
        # Record the position in the tracker
        await self.position_tracker.record_position(
            alert_data.symbol, 
            alert_data.action, 
            alert_data.timeframe, 
            trade_result['price']
        )
        
        logger.info("Risk management initialized for position",
                   extra={"request_id": request_id})

    @handle_async_errors
    async def process_alert(self, alert_data: AlertData) -> Dict[str, Any]:
        """Process a trading alert with full risk checks"""
        start_time = time.time()
        request_id = str(uuid.uuid4())
        logger.info(f"Processing alert {request_id}", extra={"request_id": request_id})

        try:
            # Get current price if not provided or zero
            if alert_data.price <= 0:
                alert_data.price = await get_current_price(alert_data.symbol, alert_data.action)
                logger.info(f"Fetched current price for {alert_data.symbol}: {alert_data.price}")
                if alert_data.price <= 0:
                    return {"status": "error", "message": f"Could not get valid price for {alert_data.symbol}"}
            
            # 1. Market Condition Check
            tradeable, reason = await is_instrument_tradeable(alert_data.symbol)
            if not tradeable:
                logger.warning(f"Not tradeable: {reason}", extra={"request_id": request_id})
                return {"status": "rejected", "reason": reason}

            # 2. Get current price and spread
            current_price = alert_data.price
            spread = await self._get_current_spread(alert_data.symbol)
            
            # 3. Risk Management Checks
            risk_check = await self._pre_trade_risk_checks(
                alert_data, current_price, spread, request_id
            )
            if not risk_check["valid"]:
                return risk_check

            # 4. Calculate position size
            position_size = await self._calculate_position_size(
                alert_data, current_price, risk_check["adjusted_risk"], request_id
            )

            # 5. Execute trade
            trade_result = await self._execute_trade(
                alert_data, position_size, current_price, request_id
            )

            # 6. Initialize risk management
            await self._initialize_risk_management(alert_data, trade_result, request_id)

            TRADE_LATENCY.observe(time.time() - start_time)
            return trade_result

        except Exception as e:
            logger.error(f"Alert processing failed: {str(e)}", exc_info=True,
                       extra={"request_id": request_id})
            TRADE_REQUESTS.inc()
            return {"status": "error", "message": str(e)}

class VolatilityMonitor:
    def __init__(self):
        self.volatility = {}
        self.volatility_thresholds = {
            "15M": {"std_dev": 2.0, "lookback": 20},
            "1H": {"std_dev": 2.5, "lookback": 24},
            "4H": {"std_dev": 3.0, "lookback": 30},
            "1D": {"std_dev": 3.5, "lookback": 20}
        }
        self.market_conditions = {}
        
    async def initialize_market_condition(self, symbol: str, timeframe: str):
        """Initialize market condition tracking for a symbol"""
        if symbol not in self.market_conditions:
            self.market_conditions[symbol] = {
                'timeframe': timeframe,
                'volatility_state': 'normal',
                'last_update': datetime.now(timezone('Asia/Bangkok')),
                'volatility_ratio': 1.0
            }
            
    async def update_volatility(self, symbol: str, current_atr: float, timeframe: str):
        """Update volatility history and calculate current state"""
        if symbol not in self.volatility:
            self.volatility[symbol] = []
            
        settings = self.volatility_thresholds.get(timeframe, self.volatility_thresholds["1H"])
        self.volatility[symbol].append(current_atr)
        
        # Maintain lookback period
        if len(self.volatility[symbol]) > settings['lookback']:
            self.volatility[symbol].pop(0)
            
        # Calculate volatility metrics
        if len(self.volatility[symbol]) >= settings['lookback']:
            mean_atr = sum(self.volatility[symbol]) / len(self.volatility[symbol])
            std_dev = statistics.stdev(self.volatility[symbol])
            current_ratio = current_atr / mean_atr
            
            # Update market condition
            self.market_conditions[symbol] = self.market_conditions.get(symbol, {})
            self.market_conditions[symbol]['volatility_ratio'] = current_ratio
            self.market_conditions[symbol]['last_update'] = datetime.now(timezone('Asia/Bangkok'))
            
            if current_atr > (mean_atr + settings['std_dev'] * std_dev):
                self.market_conditions[symbol]['volatility_state'] = 'high'
            elif current_atr < (mean_atr - settings['std_dev'] * std_dev):
                self.market_conditions[symbol]['volatility_state'] = 'low'
            else:
                self.market_conditions[symbol]['volatility_state'] = 'normal'
                
    async def get_market_condition(self, symbol: str) -> Dict[str, Any]:
        """Get current market condition for a symbol"""
        return self.market_conditions.get(symbol, {
            'volatility_state': 'unknown',
            'volatility_ratio': 1.0
        })
        
    async def should_adjust_risk(self, symbol: str, timeframe: str) -> Tuple[bool, float]:
        """Determine if risk parameters should be adjusted based on volatility"""
        condition = await self.get_market_condition(symbol)
        
        if condition['volatility_state'] == 'high':
            return True, 0.75  # Reduce risk by 25%
        elif condition['volatility_state'] == 'low':
            return True, 1.25  # Increase risk by 25%
        return False, 1.0

class MarketStructureAnalyzer:
    def __init__(self):
        self.support_levels = {}
        self.resistance_levels = {}
        self.swing_points = {}
        
    async def analyze_market_structure(self, symbol: str, timeframe: str, 
                                     high: float, low: float, close: float) -> Dict[str, Any]:
        """Analyze market structure for better stop loss placement"""
        if symbol not in self.support_levels:
            self.support_levels[symbol] = []
        if symbol not in self.resistance_levels:
            self.resistance_levels[symbol] = []
        if symbol not in self.swing_points:
            self.swing_points[symbol] = []
            
        # Update swing points
        self._update_swing_points(symbol, high, low)
        
        # Identify support and resistance levels
        self._identify_levels(symbol)
        
        # Get nearest levels for stop loss calculation
        nearest_support = self._get_nearest_support(symbol, close)
        nearest_resistance = self._get_nearest_resistance(symbol, close)
        
        return {
            'nearest_support': nearest_support,
            'nearest_resistance': nearest_resistance,
            'swing_points': self.swing_points[symbol][-5:] if len(self.swing_points[symbol]) >= 5 else self.swing_points[symbol],
            'support_levels': self.support_levels[symbol],
            'resistance_levels': self.resistance_levels[symbol]
        }
        
    def _update_swing_points(self, symbol: str, high: float, low: float):
        """Update swing high and low points"""
        if symbol not in self.swing_points:
            self.swing_points[symbol] = []
            
        if len(self.swing_points[symbol]) < 2:
            self.swing_points[symbol].append({'high': high, 'low': low})
            return
            
        last_point = self.swing_points[symbol][-1]
        if high > last_point['high']:
            self.swing_points[symbol].append({'high': high, 'low': low})
        elif low < last_point['low']:
            self.swing_points[symbol].append({'high': high, 'low': low})
            
    def _identify_levels(self, symbol: str):
        """Identify support and resistance levels from swing points"""
        points = self.swing_points.get(symbol, [])
        if len(points) < 3:
            return
            
        # Identify support levels (local minima)
        for i in range(1, len(points)-1):
            if points[i]['low'] < points[i-1]['low'] and points[i]['low'] < points[i+1]['low']:
                if points[i]['low'] not in self.support_levels[symbol]:
                    self.support_levels[symbol].append(points[i]['low'])
                    
        # Identify resistance levels (local maxima)
        for i in range(1, len(points)-1):
            if points[i]['high'] > points[i-1]['high'] and points[i]['high'] > points[i+1]['high']:
                if points[i]['high'] not in self.resistance_levels[symbol]:
                    self.resistance_levels[symbol].append(points[i]['high'])
                    
    def _get_nearest_support(self, symbol: str, current_price: float) -> Optional[float]:
        """Get nearest support level below current price"""
        supports = sorted([s for s in self.support_levels.get(symbol, []) if s < current_price])
        return supports[-1] if supports else None
        
    def _get_nearest_resistance(self, symbol: str, current_price: float) -> Optional[float]:
        """Get nearest resistance level above current price"""
        resistances = sorted([r for r in self.resistance_levels.get(symbol, []) if r > current_price])
        return resistances[0] if resistances else None

class DynamicExitManager:
    def __init__(self):
        self.exit_levels = {}
        
    async def initialize_exits(self, symbol: str, entry_price: float, position_type: str, 
                             initial_stop: float, initial_tp: float):
        """Initialize exit levels for a position"""
        self.exit_levels[symbol] = {
            "entry_price": entry_price,
            "position_type": position_type,
            "initial_stop": initial_stop,
            "initial_tp": initial_tp,
            "current_stop": initial_stop,
            "current_tp": initial_tp,
            "trailing_stop": None,
            "exit_levels_hit": []
        }
        
    async def update_exits(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """Update exit levels based on market regime and price action"""
        if symbol not in self.exit_levels:
            return {}
            
        # Simplified implementation - just return empty dict
        return {}
            
    async def check_exits(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """Check if any exit conditions are met"""
        if symbol not in self.exit_levels:
            return {}
            
        position_data = self.exit_levels[symbol]
        actions = {}
        
        # Check stop loss
        if position_data["position_type"] == "LONG":
            if current_price <= position_data["current_stop"]:
                actions["stop_loss"] = True
        else:  # SHORT
            if current_price >= position_data["current_stop"]:
                actions["stop_loss"] = True
                
        # Check take profit
        if position_data["position_type"] == "LONG":
            if current_price >= position_data["current_tp"]:
                actions["take_profit"] = True
        else:  # SHORT
            if current_price <= position_data["current_tp"]:
                actions["take_profit"] = True
                
        # Check trailing stop
        if position_data["trailing_stop"] is not None:
            if position_data["position_type"] == "LONG":
                if current_price <= position_data["trailing_stop"]:
                    actions["trailing_stop"] = True
            else:  # SHORT
                if current_price >= position_data["trailing_stop"]:
                    actions["trailing_stop"] = True
                    
        return actions
        
    async def clear_exits(self, symbol: str):
        """Clear exit levels for a symbol"""
        if symbol in self.exit_levels:
            del self.exit_levels[symbol]

class PositionSizingManager:
    def __init__(self):
        self.portfolio_heat = 0.0        # Track portfolio heat
        
    async def calculate_position_size(self, 
                                    account_balance: float,
                                    entry_price: float,
                                    stop_loss: float,
                                    atr: float,
                                    timeframe: str,
                                    market_condition: Dict[str, Any],
                                    correlation_factor: float = 1.0) -> float:
        """Calculate position size based on risk parameters"""
        # Calculate risk amount (2% of account balance by default)
        risk_amount = account_balance * 0.02
        
        # Adjust risk based on market condition
        volatility_adjustment = market_condition.get('volatility_ratio', 1.0)
        if market_condition.get('volatility_state') == 'high':
            risk_amount *= 0.75  # Reduce risk by 25% in high volatility
        elif market_condition.get('volatility_state') == 'low':
            risk_amount *= 1.25  # Increase risk by 25% in low volatility
            
        # Adjust for correlation
        risk_amount *= correlation_factor
        
        # Calculate position size based on risk
        risk_per_unit = abs(entry_price - stop_loss)
        if risk_per_unit == 0 or risk_per_unit < 0.00001:  # Prevent division by zero or very small values
            risk_per_unit = atr  # Use ATR as a fallback
            
        position_size = risk_amount / risk_per_unit
            
        # Round to appropriate precision
        if timeframe in ["15M", "1H"]:
            position_size = round(position_size, 2)
        else:
            position_size = round(position_size, 1)
            
        return position_size
        
    async def update_portfolio_heat(self, new_position_size: float):
        """Update portfolio heat with new position"""
        self.portfolio_heat += new_position_size
        
    async def get_correlation_factor(self, symbol: str, existing_positions: List[str]) -> float:
        """Calculate correlation factor based on existing positions"""
        if not existing_positions:
            return 1.0
            
        # Implement correlation calculation logic here
        # This is a simplified version
        normalized_symbol = standardize_symbol(symbol)
        
        # Find similar pairs (same base or quote currency)
        similar_pairs = 0
        for pos in existing_positions:
            pos_normalized = standardize_symbol(pos)
            # Check if they share the same base or quote currency
            if (normalized_symbol.split('_')[0] == pos_normalized.split('_')[0] or 
                normalized_symbol.split('_')[1] == pos_normalized.split('_')[1]):
                similar_pairs += 1
        
        # Reduce correlation factor based on number of similar pairs
        if similar_pairs > 0:
            return max(0.5, 1.0 - (similar_pairs * 0.1))  # Minimum correlation factor of 0.5
        return 1.0

class AdvancedLossManager:
    def __init__(self):
        self.positions = {}
        self.daily_pnl = 0.0
        self.max_daily_loss = 0.20    # 20% max daily loss
        self.max_drawdown = 0.15    # 15% max drawdown
        self.peak_balance = 0.0
        self.current_balance = 0.0
        
    async def initialize_position(self, symbol: str, entry_price: float, position_type: str, 
                                units: float, account_balance: float):
        """Initialize position tracking with loss limits"""
        self.positions[symbol] = {
            "entry_price": entry_price,
            "position_type": position_type,
            "units": units,
            "current_units": units,
            "entry_time": datetime.now(timezone('Asia/Bangkok')),
            "max_loss": self._calculate_position_max_loss(entry_price, units, account_balance),
            "current_loss": 0.0,
            "correlation_factor": 1.0
        }
        
        # Update peak balance if needed
        if account_balance > self.peak_balance:
            self.peak_balance = account_balance
            
        self.current_balance = account_balance
        
    def _calculate_position_max_loss(self, entry_price: float, units: float, account_balance: float) -> float:
        """Calculate maximum loss for a position based on risk parameters"""
        position_value = abs(entry_price * units)
        risk_percentage = min(0.02, position_value / account_balance)  # Max 2% risk per position
        return position_value * risk_percentage
        
    async def update_position_loss(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """Update position loss and check limits"""
        if symbol not in self.positions:
            return {}
            
        position = self.positions[symbol]
        entry_price = position["entry_price"]
        units = position["current_units"]
        
        # Calculate current loss
        if position["position_type"] == "LONG":
            current_loss = (entry_price - current_price) * units
        else:  # SHORT
            current_loss = (current_price - entry_price) * units
            
        position["current_loss"] = current_loss
        
        # Check various loss limits
        actions = {}
        
        # Check position-specific loss limit
        if abs(current_loss) > position["max_loss"]:
            actions["position_limit"] = True
            
        # Check daily loss limit
        daily_loss_percentage = abs(self.daily_pnl) / self.peak_balance
        if daily_loss_percentage > self.max_daily_loss:
            actions["daily_limit"] = True
            
        # Check drawdown limit
        drawdown = (self.peak_balance - self.current_balance) / self.peak_balance
        if drawdown > self.max_drawdown:
            actions["drawdown_limit"] = True
            
        return actions
        
    async def update_daily_pnl(self, pnl: float):
        """Update daily P&L and check limits"""
        self.daily_pnl += pnl
        self.current_balance += pnl
        
        # Update peak balance if needed
        if self.current_balance > self.peak_balance:
            self.peak_balance = self.current_balance
            
    async def should_reduce_risk(self) -> Tuple[bool, float]:
        """Determine if risk should be reduced based on current conditions"""
        daily_loss_percentage = abs(self.daily_pnl) / self.peak_balance
        drawdown = (self.peak_balance - self.current_balance) / self.peak_balance
        
        if daily_loss_percentage > self.max_daily_loss * 0.75:  # At 75% of max daily loss
            return True, 0.75  # Reduce risk by 25%
        elif drawdown > self.max_drawdown * 0.75:  # At 75% of max drawdown
            return True, 0.75  # Reduce risk by 25%
            
        return False, 1.0
        
    async def clear_position(self, symbol: str):
        """Clear position from loss management"""
        if symbol in self.positions:
            del self.positions[symbol]

class RiskAnalytics:
    def __init__(self):
        self.positions = {}
        
    async def initialize_position(self, symbol: str, entry_price: float, units: float):
        """Initialize position tracking for risk analytics"""
        self.positions[symbol] = {
            "entry_price": entry_price,
            "units": units,
            "current_price": entry_price,
            "entry_time": datetime.now(timezone('Asia/Bangkok'))
        }
        
    async def update_position(self, symbol: str, current_price: float):
        """Update position data"""
        if symbol in self.positions:
            self.positions[symbol]["current_price"] = current_price
        
    async def clear_position(self, symbol: str):
        """Clear position from risk analytics"""
        if symbol in self.positions:
            del self.positions[symbol]

class EnhancedRiskManager:
    def __init__(self):
        self.positions = {}
        self.atr_period = 14
        self.take_profit_levels = {
            "15M": {
                "first_exit": 0.5,  # 50% at 1:1
                "second_exit": 0.25,  # 25% at 2:1
                "runner": 0.25  # 25% with trailing
            },
            "1H": {
                "first_exit": 0.4,  # 40% at 1:1
                "second_exit": 0.3,  # 30% at 2:1
                "runner": 0.3  # 30% with trailing
            },
            "4H": {
                "first_exit": 0.33,  # 33% at 1:1
                "second_exit": 0.33,  # 33% at 2:1
                "runner": 0.34  # 34% with trailing
            },
            "1D": {
                "first_exit": 0.33,  # 33% at 1:1
                "second_exit": 0.33,  # 33% at 2:1
                "runner": 0.34  # 34% with trailing
            }
        }
        
        # ATR multipliers based on timeframe and instrument type
        self.atr_multipliers = {
            "FOREX": {
                "15M": 1.5,
                "1H": 1.75,
                "4H": 2.0,
                "1D": 2.25
            },
            "CRYPTO": {
                "15M": 2.0,
                "1H": 2.25,
                "4H": 2.5,
                "1D": 2.75
            },
            "XAU_USD": {
                "15M": 1.75,
                "1H": 2.0,
                "4H": 2.25,
                "1D": 2.5
            }
        }

    async def initialize_position(self, symbol: str, entry_price: float, position_type: str, 
                                timeframe: str, units: float, atr: float):
        """Initialize position with ATR-based stops and tiered take-profits"""
        # Determine instrument type
        instrument_type = self._get_instrument_type(symbol)
        
        # Get ATR multiplier based on timeframe and instrument
        atr_multiplier = self.atr_multipliers[instrument_type].get(
            timeframe, self.atr_multipliers[instrument_type]["1H"]
        )
        
        # Calculate initial stop loss
        if position_type == "LONG":
            stop_loss = entry_price - (atr * atr_multiplier)
            take_profits = [
                entry_price + (atr * atr_multiplier),  # 1:1
                entry_price + (atr * atr_multiplier * 2),  # 2:1
                entry_price + (atr * atr_multiplier * 3)  # 3:1
            ]
        else:  # SHORT
            stop_loss = entry_price + (atr * atr_multiplier)
            take_profits = [
                entry_price - (atr * atr_multiplier),  # 1:1
                entry_price - (atr * atr_multiplier * 2),  # 2:1
                entry_price - (atr * atr_multiplier * 3)  # 3:1
            ]
        
        # Get take-profit levels for this timeframe
        tp_levels = self.take_profit_levels.get(timeframe, self.take_profit_levels["1H"])
        
        # Initialize position tracking
        self.positions[symbol] = {
            'entry_price': entry_price,
            'position_type': position_type,
            'timeframe': timeframe,
            'units': units,
            'current_units': units,
            'stop_loss': stop_loss,
            'take_profits': take_profits,
            'tp_levels': tp_levels,
            'entry_time': datetime.now(timezone('Asia/Bangkok')),
            'exit_levels_hit': [],
            'trailing_stop': None,
            'atr': atr,
            'atr_multiplier': atr_multiplier,
            'instrument_type': instrument_type,
            'symbol': symbol
        }
        
        logger.info(f"Initialized position for {symbol}: Stop Loss: {stop_loss}, Take Profits: {take_profits}")

    def _get_instrument_type(self, symbol: str) -> str:
        """Determine instrument type for appropriate ATR multiplier"""
        normalized_symbol = standardize_symbol(symbol)
        if any(crypto in normalized_symbol for crypto in ["BTC", "ETH", "XRP", "LTC"]):
            return "CRYPTO"
        elif "XAU" in normalized_symbol:
            return "XAU_USD"
        else:
            return "FOREX"

    async def update_position(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """Update position status and return any necessary actions"""
        if symbol not in self.positions:
            return {}
            
        position = self.positions[symbol]
        actions = {}
        
        # Check for stop loss hit
        if self._check_stop_loss_hit(position, current_price):
            actions['stop_loss'] = True
            return actions
            
        # Check for take-profit levels
        tp_actions = self._check_take_profits(position, current_price)
        if tp_actions:
            actions['take_profits'] = tp_actions
            
        return actions

    def _check_stop_loss_hit(self, position: Dict[str, Any], current_price: float) -> bool:
        """Check if stop loss has been hit"""
        if position['position_type'] == "LONG":
            return current_price <= position['stop_loss']
        else:
            return current_price >= position['stop_loss']

    def _check_take_profits(self, position: Dict[str, Any], current_price: float) -> Optional[Dict[str, Any]]:
        """Check if any take-profit levels have been hit"""
        actions = {}
        
        for i, tp in enumerate(position['take_profits']):
            if i not in position['exit_levels_hit']:
                if position['position_type'] == "LONG":
                    if current_price >= tp:
                        position['exit_levels_hit'].append(i)
                        tp_key = "first_exit" if i == 0 else "second_exit" if i == 1 else "runner"
                        actions[i] = {
                            'price': tp,
                            'units': position['current_units'] * position['tp_levels'][tp_key]
                        }
                else:  # SHORT
                    if current_price <= tp:
                        position['exit_levels_hit'].append(i)
                        tp_key = "first_exit" if i == 0 else "second_exit" if i == 1 else "runner"
                        actions[i] = {
                            'price': tp,
                            'units': position['current_units'] * position['tp_levels'][tp_key]
                        }
        
        return actions if actions else None

    async def clear_position(self, symbol: str):
        """Clear position from risk management"""
        if symbol in self.positions:
            del self.positions[symbol]

class TradingConfig:
    def __init__(self):
        self.atr_multipliers = {
            "FOREX": {
                "15M": 1.5,
                "1H": 1.75,
                "4H": 2.0,
                "1D": 2.25
            },
            "CRYPTO": {
                "15M": 2.0,
                "1H": 2.25,
                "4H": 2.5,
                "1D": 2.75
            },
            "XAU_USD": {
                "15M": 1.75,
                "1H": 2.0,
                "4H": 2.25,
                "1D": 2.5
            }
        }
        
        self.take_profit_levels = {
            "15M": {
                "first_exit": 0.5,
                "second_exit": 0.25,
                "runner": 0.25
            },
            "1H": {
                "first_exit": 0.4,
                "second_exit": 0.3,
                "runner": 0.3
            },
            "4H": {
                "first_exit": 0.33,
                "second_exit": 0.33,
                "runner": 0.34
            },
            "1D": {
                "first_exit": 0.33,
                "second_exit": 0.33,
                "runner": 0.34
            }
        }

##############################################################################
# Market Utility Functions
##############################################################################

def standardize_symbol(symbol: str) -> str:
    """Standardize trading symbol format with improved error handling"""
    try:
        # Convert to uppercase
        symbol = symbol.upper()
        
        # Replace common separators with underscore
        symbol = symbol.replace('/', '_').replace('-', '_')
        
        # Special case for cryptocurrencies
        crypto_mappings = {
            "BTCUSD": "BTC_USD",
            "ETHUSD": "ETH_USD",
            "XRPUSD": "XRP_USD",
            "LTCUSD": "LTC_USD"
        }
        
        if symbol in crypto_mappings:
            return crypto_mappings[symbol]
            
        # If no underscore in forex pair, add it between currency pairs
        if '_' not in symbol and len(symbol) == 6:
            return f"{symbol[:3]}_{symbol[3:]}"
            
        return symbol
    except Exception as e:
        logger.error(f"Error standardizing symbol {symbol}: {str(e)}")
        return symbol  # Return original if any error

def check_market_hours(session_config: dict) -> bool:
    """Check if market is open based on session config"""
    try:
        current_time = datetime.now(timezone('Asia/Bangkok'))
        weekday = current_time.weekday()  # 0 is Monday, 6 is Sunday
        
        # Check for holidays first
        holidays = session_config.get('holidays', [])
        current_date = current_time.strftime('%Y-%m-%d')
        if current_date in holidays:
            logger.info(f"Market closed due to holiday on {current_date}")
            return False
            
        # Check for weekend (Saturday and Sunday typically closed)
        if weekday >= 5 and not session_config.get('weekend_trading', False):
            logger.info(f"Market closed for weekend (day {weekday})")
            return False
            
        # Check trading hours
        trading_hours = session_config.get('trading_hours', {}).get(str(weekday), None)
        if not trading_hours:
            logger.info(f"No trading hours defined for day {weekday}")
            return False
            
        # Convert current time to seconds since midnight for comparison
        current_seconds = current_time.hour * 3600 + current_time.minute * 60 + current_time.second
        
        # Check if current time falls within any trading sessions
        for session in trading_hours:
            start_time = session.get('start', '00:00:00')
            end_time = session.get('end', '23:59:59')
            
            # Convert times to seconds
            start_h, start_m, start_s = map(int, start_time.split(':'))
            end_h, end_m, end_s = map(int, end_time.split(':'))
            
            start_seconds = start_h * 3600 + start_m * 60 + start_s
            end_seconds = end_h * 3600 + end_m * 60 + end_s
            
            if start_seconds <= current_seconds <= end_seconds:
                return True
                
        logger.info(f"Market closed at current time {current_time.strftime('%H:%M:%S')}")
        return False
    except Exception as e:
        logger.error(f"Error checking market hours: {str(e)}")
        return False  # Default to closed if any error

async def is_instrument_tradeable(instrument: str) -> Tuple[bool, str]:
    """Check if an instrument is tradeable - determine session and check market hours"""
    try:
        logger.info(f"Checking if {instrument} is tradeable")
        # Standardize instrument name
        normalized = standardize_symbol(instrument)
        
        # Determine instrument type
        instrument_type = get_instrument_type(normalized)
        logger.info(f"Instrument {normalized} determined to be {instrument_type}")
        
        # Define session configurations for different types
        session_configs = {
            "FOREX": {
                "trading_hours": {
                    "0": [{"start": "00:00:00", "end": "23:59:59"}],  # Monday
                    "1": [{"start": "00:00:00", "end": "23:59:59"}],  # Tuesday
                    "2": [{"start": "00:00:00", "end": "23:59:59"}],  # Wednesday
                    "3": [{"start": "00:00:00", "end": "23:59:59"}],  # Thursday
                    "4": [{"start": "00:00:00", "end": "23:59:59"}],  # Friday
                },
                "weekend_trading": False
            },
            "CRYPTO": {
                "trading_hours": {
                    "0": [{"start": "00:00:00", "end": "23:59:59"}],  # Monday
                    "1": [{"start": "00:00:00", "end": "23:59:59"}],  # Tuesday
                    "2": [{"start": "00:00:00", "end": "23:59:59"}],  # Wednesday
                    "3": [{"start": "00:00:00", "end": "23:59:59"}],  # Thursday
                    "4": [{"start": "00:00:00", "end": "23:59:59"}],  # Friday
                    "5": [{"start": "00:00:00", "end": "23:59:59"}],  # Saturday
                    "6": [{"start": "00:00:00", "end": "23:59:59"}],  # Sunday
                },
                "weekend_trading": True
            },
            "XAU_USD": {
                "trading_hours": {
                    "0": [{"start": "00:00:00", "end": "23:59:59"}],  # Monday
                    "1": [{"start": "00:00:00", "end": "23:59:59"}],  # Tuesday
                    "2": [{"start": "00:00:00", "end": "23:59:59"}],  # Wednesday
                    "3": [{"start": "00:00:00", "end": "23:59:59"}],  # Thursday
                    "4": [{"start": "00:00:00", "end": "23:59:59"}],  # Friday
                },
                "weekend_trading": False
            }
        }
        
        # Get appropriate session config
        session_config = session_configs.get(instrument_type, session_configs["FOREX"])
        
        # Check market hours
        is_open = check_market_hours(session_config)
        
        if is_open:
            logger.info(f"Instrument {normalized} is tradeable now")
            return True, "Market open"
        else:
            logger.info(f"Instrument {normalized} is not tradeable now - market closed")
            return False, "Market closed"
    except Exception as e:
        logger.error(f"Error checking if instrument {instrument} is tradeable: {str(e)}")
        return False, f"Error: {str(e)}"

def get_atr(instrument: str, timeframe: str) -> float:
    """Get the ATR value for risk management"""
    try:
        # Default ATR values for different instrument types
        default_atrs = {
            "FOREX": {
                "15M": 0.0005,  # 5 pips for major pairs
                "1H": 0.0010,   # 10 pips
                "4H": 0.0020,   # 20 pips
                "1D": 0.0050    # 50 pips
            },
            "CRYPTO": {
                "15M": 50.0,
                "1H": 100.0,
                "4H": 250.0,
                "1D": 500.0
            },
            "XAU_USD": {
                "15M": 0.5,
                "1H": 1.0,
                "4H": 2.0,
                "1D": 5.0
            }
        }
        
        instrument_type = get_instrument_type(instrument)
        
        # Return default ATR for this instrument type and timeframe
        return default_atrs.get(instrument_type, default_atrs["FOREX"]).get(timeframe, default_atrs[instrument_type]["1H"])
    except Exception as e:
        logger.error(f"Error getting ATR for {instrument} on {timeframe}: {str(e)}")
        return 0.0010  # Default fallback to 10 pips

def get_instrument_type(symbol: str) -> str:
    """Determine instrument type from standardized symbol"""
    symbol = standardize_symbol(symbol)
    
    # Crypto check
    if any(crypto in symbol for crypto in ["BTC", "ETH", "XRP", "LTC"]):
        return "CRYPTO"
    
    # Gold check
    if "XAU" in symbol:
        return "XAU_USD"
    
    # Default to forex
    return "FOREX"

def get_atr_multiplier(instrument_type: str, timeframe: str) -> float:
    """Get ATR multiplier based on instrument type and timeframe"""
    multipliers = {
        "FOREX": {
            "15M": 1.5,
            "1H": 1.75,
            "4H": 2.0,
            "1D": 2.25
        },
        "CRYPTO": {
            "15M": 2.0,
            "1H": 2.25,
            "4H": 2.5,
            "1D": 2.75
        },
        "XAU_USD": {
            "15M": 1.75,
            "1H": 2.0,
            "4H": 2.25,
            "1D": 2.5
        }
    }
    
    return multipliers.get(instrument_type, multipliers["FOREX"]).get(timeframe, multipliers[instrument_type]["1H"])

async def get_current_price(instrument: str, action: str) -> float:
    """Get current price of instrument with error handling and timeout management"""
    try:
        # Default prices for testing
        default_prices = {
            "EUR_USD": 1.0700,
            "GBP_USD": 1.2600,
            "USD_JPY": 155.50,
            "AUD_USD": 0.6500,
            "USD_CAD": 1.3700,
            "XAU_USD": 2050.00,
            "BTC_USD": 40000.00,
            "ETH_USD": 2200.00
        }
        
        # Standardize instrument name
        normalized = standardize_symbol(instrument)
        
        # Simulate price difference for bid/ask
        base_price = default_prices.get(normalized, 1.0000)
        if action.upper() == "BUY":
            return base_price * 1.0001  # Ask price slightly higher
        else:
            return base_price * 0.9999  # Bid price slightly lower
    except Exception as e:
        logger.error(f"Error getting current price for {instrument}: {str(e)}")
        return 0.0  # Return zero to indicate error

def get_current_market_session(current_time: datetime) -> str:
    """Determine the current market session based on time"""
    # Define market sessions based on hour (UTC)
    weekday = current_time.weekday()
    hour = current_time.hour
    
    # Weekend check
    if weekday >= 5:  # Saturday and Sunday
        return "WEEKEND"
        
    # Asian session: 00:00-08:00 UTC
    if 0 <= hour < 8:
        return "ASIAN"
        
    # London session: 08:00-16:00 UTC
    elif 8 <= hour < 16:
        return "LONDON"
        
    # New York session: 13:00-21:00 UTC (overlap with London for 3 hours)
    elif 13 <= hour < 21:
        if 13 <= hour < 16:
            return "LONDON_NY_OVERLAP"
        else:
            return "NEWYORK"
            
    # Sydney session: 21:00-00:00 UTC
    else:
        return "SYDNEY"

##############################################################################
# Main Application Entry Point
##############################################################################

def start():
    """Start the application using uvicorn"""
    import uvicorn
    setup_logging()
    logger.info(f"Starting application in {config.environment} mode")
    
    host = config.host
    port = config.port
    
    logger.info(f"Server starting at {host}:{port}")
    uvicorn.run(
        "python_bridge:app",  # Updated module name to python_bridge
        host=host,
        port=port,
        reload=config.environment == "development"
    )

if __name__ == "__main__":
    start() 
