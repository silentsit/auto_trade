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
import statistics
from datetime import datetime, timedelta
from pytz import timezone
from fastapi import FastAPI, Request, HTTPException, status, BackgroundTasks, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Dict, Any, Union, List, Tuple, Callable, TypeVar, ParamSpec
from contextlib import asynccontextmanager
from pydantic import BaseModel, validator, ValidationError, Field, root_validator, field_validator, model_validator, BaseSettings, SettingsConfigDict
from functools import wraps
from redis.asyncio import Redis
import httpx
import copy

# Type variables for type hints
P = ParamSpec('P')
T = TypeVar('T')

##############################################################################
# Data Models
##############################################################################

def standardize_symbol(symbol: str) -> str:
    """Helper function to standardize symbols to avoid circular import"""
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
        try:
            # Add logger when available
            logger.error(f"Error standardizing symbol {symbol}: {str(e)}")
        except:
            pass
        return symbol  # Return original if any error

class AlertData(BaseModel):
    """Model for handling trading alerts from TradingView or other sources"""
    symbol: str
    action: str  # BUY, SELL, CLOSE, etc.
    timeframe: Optional[str] = "1H"
    price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    risk_percentage: Optional[float] = 2.0
    size: Optional[float] = None
    message: Optional[str] = None
    source: Optional[str] = "tradingview"
    id: Optional[str] = None
    
    @field_validator('symbol')
    @classmethod
    def validate_symbol(cls, v):
        """Ensure symbol is standardized"""
        if not v:
            raise ValueError("Symbol cannot be empty")
        try:
            return standardize_symbol(v)
        except:
            # Handling circular import issue
            return v.upper()
        
    @field_validator('action')
    @classmethod
    def validate_action(cls, v):
        """Check that action is valid"""
        valid_actions = ["BUY", "SELL", "CLOSE", "CLOSE_ALL", "MODIFY", "UPDATE"]
        if v.upper() not in valid_actions:
            raise ValueError(f"Invalid action: {v}. Must be one of {valid_actions}")
        return v.upper()
        
    @field_validator('price', 'stop_loss', 'take_profit', 'risk_percentage', 'size')
    @classmethod
    def validate_numeric(cls, v, info):
        """Validate numeric fields are positive or None"""
        if v is not None and v <= 0:
            if info.field_name == 'price' and info.data.get('action') in ['CLOSE', 'CLOSE_ALL']:
                return v  # Allow zero price for close actions
            raise ValueError(f"{info.field_name.replace('_', ' ').title()} must be positive")
        return v
        
    @model_validator(mode='after')
    def check_trade_parameters(self):
        """Ensure trade parameters are consistent"""
        action = self.action
        price = self.price
        stop_loss = self.stop_loss
        take_profit = self.take_profit
        
        # For buy/sell actions, either price or sl/tp should be provided
        if action in ['BUY', 'SELL']:
            # If price is missing, will be fetched from market
            if stop_loss is None and take_profit is None:
                # No SL/TP provided, will use defaults based on ATR
                pass
                
            # Check stop loss is below entry for buys and above for sells
            if price and stop_loss:
                if action == 'BUY' and stop_loss >= price:
                    raise ValueError("Stop loss must be below entry price for BUY orders")
                elif action == 'SELL' and stop_loss <= price:
                    raise ValueError("Stop loss must be above entry price for SELL orders")
                    
            # Check take profit is above entry for buys and below for sells
            if price and take_profit:
                if action == 'BUY' and take_profit <= price:
                    raise ValueError("Take profit must be above entry price for BUY orders")
                elif action == 'SELL' and take_profit >= price:
                    raise ValueError("Take profit must be below entry price for SELL orders")
        
        # For close actions, symbol is required
        if action in ['CLOSE', 'CLOSE_ALL'] and not self.symbol:
            raise ValueError("Symbol is required for CLOSE actions")
            
        # For modify actions, symbol and at least one of SL/TP is required
        if action in ['MODIFY', 'UPDATE']:
            if not self.symbol:
                raise ValueError("Symbol is required for MODIFY actions")
            if stop_loss is None and take_profit is None:
                raise ValueError("Either stop_loss or take_profit must be provided for MODIFY actions")
                
        return self
            
    model_config = {"extra": "ignore"}  # Allow extra fields in the data

##############################################################################
# Configuration & Constants
##############################################################################

class Settings(BaseSettings):
    """Application settings with defaults and validation"""
    # OANDA API settings
    oanda_account_id: str = Field(default="dummy_account")
    oanda_api_token: str = Field(default="dummy_token")
    oanda_api_url: str = Field(default="https://api-fxtrade.oanda.com/v3")
    oanda_environment: str = Field(default="practice")
    
    # Network settings
    allowed_origins: str = Field(default="http://localhost")
    connect_timeout: int = Field(default=10)
    read_timeout: int = Field(default=30)
    total_timeout: int = Field(default=45)
    max_simultaneous_connections: int = Field(default=100)
    
    # Trading thresholds
    spread_threshold_forex: float = Field(default=0.001)
    spread_threshold_crypto: float = Field(default=0.008)
    max_retries: int = Field(default=3)
    base_delay: float = Field(default=1.0)
    base_position: int = Field(default=5000)
    max_daily_loss: float = Field(default=0.20)
    
    # Server settings
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000)
    environment: str = Field(default="development")
    max_requests_per_minute: int = Field(default=100)
    
    # Trading settings
    trade_24_7: bool = Field(default=True)
    redis_url: str = Field(default="redis://localhost:6379/0")
    log_level: str = Field(default="INFO")
    risk_per_trade: float = Field(default=2.0)
    use_multiple_take_profits: bool = Field(default=True)
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        env_prefix="",
        extra="ignore"
    )
    
# Initialize settings once
config = Settings()

# Set required variables for testing if using dummy values
if not config.oanda_account_id:
    config.oanda_account_id = os.getenv("OANDA_ACCOUNT_ID", "dummy_account")
    
if not config.oanda_api_token:
    config.oanda_api_token = os.getenv("OANDA_API_TOKEN", "dummy_token")

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

# Instrument leverages based on Singapore MAS regulations
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
}

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
                    "Authorization": f"Bearer {config.oanda_api_token}",
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
        account = account_id or config.oanda_account_id
        url = f"{config.oanda_api_url}/accounts/{account}/openPositions"
        
        async with session.get(url, timeout=HTTP_REQUEST_TIMEOUT) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"Failed to fetch positions: {error_text}")
                return False, {"error": error_text}
            
            data = await response.json()
            return True, data
    except asyncio.TimeoutError:
        logger.error(f"Timeout fetching positions for account {config.oanda_account_id}")
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

    # Rest of PositionTracker implementation...

##############################################################################
# Unified Position Management
##############################################################################

# Initialize Redis connection
redis_client = None

class Position:
    """Represents a trading position with all relevant details"""
    def __init__(self, id: str, symbol: str, direction: str, entry_price: float, 
                 size: float, stop_loss: Optional[float] = None, 
                 take_profit: Optional[float] = None, timestamp: Optional[str] = None):
        self.id = id
        self.symbol = symbol
        self.direction = direction
        self.entry_price = entry_price
        self.size = size
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.timestamp = timestamp or datetime.now(timezone('Asia/Bangkok')).isoformat()
        self.pnl = 0.0
        self.pnl_percentage = 0.0
        self.status = "open"

class PositionManager:
    """Manages all trading positions and interacts with broker API"""
    def __init__(self, api_client: Any = None):
        self.positions = {}  # Dictionary of position ID to Position objects
        self.api_client = api_client
        self._initialized = False
        self._position_lock = asyncio.Lock()
        
        # Performance tracking
        self._daily_trades = []
        self._total_trades = 0
        self._winning_trades = 0
        self._losing_trades = 0
        self._daily_pnl = 0.0
        self._account_balance = 0.0
        self._account_equity = 0.0
        self._max_drawdown = 0.0
        
        # Cache for account stats
        self._account_stats = {}
        self._last_stats_update = datetime.min
    
    async def initialize(self):
        """Initialize the position manager"""
        if self._initialized:
            return
            
        try:
            # Load existing positions from broker
            positions = await self.get_open_positions()
            for position in positions:
                self.positions[position.id] = position
                
            # Get account info
            await self.update_account_stats()
            
            self._initialized = True
            logger.info(f"Position manager initialized with {len(self.positions)} existing positions")
        except Exception as e:
            logger.error(f"Failed to initialize position manager: {str(e)}")
            raise
    
    async def get_open_positions(self) -> List[Position]:
        """Get all open positions from the broker"""
        try:
            success, data = await get_open_positions()
            
            if not success:
                logger.error(f"Failed to get open positions: {data}")
                return []
                
            positions = []
            for pos_data in data.get('positions', []):
                try:
                    # Extract essential position data
                    symbol = pos_data.get('instrument')
                    if not symbol:
                        continue
                        
                    position_id = pos_data.get('id', str(uuid.uuid4()))
                    direction = pos_data.get('direction', 'long').lower()
                    entry_price = float(pos_data.get('price', 0))
                    size = float(pos_data.get('units', 0))
                    stop_loss = float(pos_data.get('stopLoss', 0)) or None
                    take_profit = float(pos_data.get('takeProfit', 0)) or None
                    
                    # Create Position object
                    position = Position(
                        id=position_id,
                        symbol=symbol,
                        direction=direction,
                        entry_price=entry_price,
                        size=size,
                        stop_loss=stop_loss,
                        take_profit=take_profit
                    )
                    
                    positions.append(position)
                except Exception as e:
                    logger.error(f"Error processing position data: {str(e)}")
            
            return positions
        except Exception as e:
            logger.error(f"Error getting open positions: {str(e)}")
            return []
    
    def get_all_positions(self) -> Dict[str, Position]:
        """Get all locally tracked positions"""
        return self.positions
    
    async def get_position(self, symbol: str) -> Optional[Position]:
        """Get position for a specific symbol"""
        try:
            # Find positions for this symbol
            matching_positions = [p for p in self.positions.values() if p.symbol == symbol]
            
            if not matching_positions:
                return None
                
            # If multiple positions for same symbol, return the most recent one
            return sorted(matching_positions, key=lambda p: p.timestamp, reverse=True)[0]
        except Exception as e:
            logger.error(f"Error getting position for {symbol}: {str(e)}")
            return None
    
    async def create_position(self, alert_data: AlertData) -> Dict[str, Any]:
        """Create a new trading position"""
        try:
            # Generate position ID
            position_id = str(uuid.uuid4())
            
            # Calculate risk-appropriate position size
            account_stats = await self.get_account_stats()
            account_balance = account_stats.get('balance', 0)
            
            # If size not specified, calculate based on risk percentage
            position_size = alert_data.size
            if not position_size and alert_data.risk_percentage:
                # Get price and ATR for risk calculation
                entry_price = await get_current_price(alert_data.symbol, alert_data.action)
                if entry_price <= 0:
                    return {"success": False, "message": "Could not get valid entry price"}
                    
                atr = get_atr(alert_data.symbol, alert_data.timeframe or "D1")
                
                # Calculate stop loss if not provided
                stop_loss = alert_data.stop_loss
                if not stop_loss:
                    if alert_data.action.lower() == "buy":
                        stop_loss = entry_price - (atr * 2)
                    else:
                        stop_loss = entry_price + (atr * 2)
                
                # Calculate risk amount
                risk_amount = account_balance * (alert_data.risk_percentage / 100)
                
                # Calculate position size based on risk
                price_diff = abs(entry_price - stop_loss)
                if price_diff > 0:
                    position_size = risk_amount / price_diff
                else:
                    position_size = 0
            
            # Validate position size
            if position_size <= 0:
                return {"success": False, "message": "Invalid position size calculated"}
            
            # Ensure we have entry price
            entry_price = alert_data.price or await get_current_price(alert_data.symbol, alert_data.action)
            if entry_price <= 0:
                return {"success": False, "message": "Could not get valid entry price"}
            
            # Create order with broker
            try:
                # Order execution would happen here
                # For now, just log the intended order
                logger.info(f"Placing {alert_data.action.upper()} order for {position_size} units of {alert_data.symbol} at {entry_price}")
                
                # Simulate successful order execution
                order_successful = True
                order_result = {
                    "id": position_id,
                    "symbol": alert_data.symbol,
                    "direction": alert_data.action.lower(),
                    "price": entry_price,
                    "size": position_size,
                    "status": "filled"
                }
                
                if not order_successful:
                    return {"success": False, "message": "Order execution failed with broker"}
                
                # Create position object
                position = Position(
                    id=position_id,
                    symbol=alert_data.symbol,
                    direction=alert_data.action.lower(),
                    entry_price=entry_price,
                    size=position_size,
                    stop_loss=alert_data.stop_loss,
                    take_profit=alert_data.take_profit
                )
                
                # Store position
                async with self._position_lock:
                    self.positions[position_id] = position
                
                # Return success with position details
                return {
                    "success": True,
                    "message": "Position created successfully",
                    "data": {
                        "position_id": position_id,
                        "symbol": alert_data.symbol,
                        "direction": alert_data.action.lower(),
                        "entry_price": entry_price,
                        "size": position_size,
                        "stop_loss": alert_data.stop_loss,
                        "take_profit": alert_data.take_profit
                    }
                }
            except Exception as e:
                logger.error(f"Failed to execute order: {str(e)}")
                return {"success": False, "message": f"Order execution error: {str(e)}"}
        except Exception as e:
            logger.error(f"Error creating position: {str(e)}")
            return {"success": False, "message": f"Position creation error: {str(e)}"}
    
    async def close_position(self, symbol: str, position_id: Optional[str] = None) -> Dict[str, Any]:
        """Close a specific position"""
        try:
            # Find the position to close
            if position_id and position_id in self.positions:
                position = self.positions[position_id]
            else:
                # Find by symbol if no ID provided
                position = await self.get_position(symbol)
                
            if not position:
                return {"success": False, "message": f"No open position found for {symbol}"}
            
            # Execute close order with broker
            try:
                # Broker API call would go here
                logger.info(f"Closing position for {position.symbol} (ID: {position.id})")
                
                # Simulate successful order execution
                close_successful = True
                
                if not close_successful:
                    return {"success": False, "message": "Failed to close position with broker"}
                
                # Get current price for P&L calculation
                close_price = await get_current_price(position.symbol, "close")
                
                # Calculate P&L
                if position.direction.lower() == "buy":
                    pnl = (close_price - position.entry_price) * position.size
                else:
                    pnl = (position.entry_price - close_price) * position.size
                
                # Track trade statistics
                self._total_trades += 1
                self._daily_pnl += pnl
                
                if pnl > 0:
                    self._winning_trades += 1
                else:
                    self._losing_trades += 1
                
                # Update account statistics
                position_data = {
                    "symbol": position.symbol,
                    "direction": position.direction,
                    "entry_price": position.entry_price,
                    "exit_price": close_price,
                    "size": position.size,
                    "pnl": pnl,
                    "closed_at": datetime.now(timezone('Asia/Bangkok')).isoformat()
                }
                
                self._daily_trades.append(position_data)
                
                # Remove position from tracking
                async with self._position_lock:
                    if position.id in self.positions:
                        del self.positions[position.id]
                
                # Update account stats after position change
                await self.update_account_stats()
                
                return {
                    "success": True,
                    "message": f"Position closed successfully",
                    "data": {
                        "position_id": position.id,
                        "symbol": position.symbol,
                        "pnl": pnl
                    }
                }
            except Exception as e:
                logger.error(f"Error executing close order: {str(e)}")
                return {"success": False, "message": f"Close order execution error: {str(e)}"}
        except Exception as e:
            logger.error(f"Error closing position: {str(e)}")
            return {"success": False, "message": f"Position closing error: {str(e)}"}
    
    async def close_all_positions(self) -> Dict[str, Any]:
        """Close all open positions"""
        try:
            if not self.positions:
                return {"success": True, "message": "No open positions to close"}
            
            results = []
            success_count = 0
            failure_count = 0
            
            # Copy keys to avoid modification during iteration
            position_ids = list(self.positions.keys())
            
            for pos_id in position_ids:
                try:
                    position = self.positions.get(pos_id)
                    if not position:
                        continue
                        
                    result = await self.close_position(position.symbol, pos_id)
                    results.append(result)
                    
                    if result["success"]:
                        success_count += 1
                    else:
                        failure_count += 1
                except Exception as e:
                    logger.error(f"Error closing position {pos_id}: {str(e)}")
                    failure_count += 1
            
            return {
                "success": failure_count == 0,
                "message": f"Closed {success_count} positions successfully, {failure_count} failures",
                "results": results
            }
        except Exception as e:
            logger.error(f"Error closing all positions: {str(e)}")
            return {"success": False, "message": f"Error closing all positions: {str(e)}"}
    
    async def update_position(self, position_id: str, symbol: str, 
                             stop_loss: Optional[float] = None, 
                             take_profit: Optional[float] = None) -> Dict[str, Any]:
        """Update position parameters (stop loss, take profit)"""
        try:
            # Find the position to update
            if position_id and position_id in self.positions:
                position = self.positions[position_id]
            else:
                # Find by symbol if ID doesn't match
                position = await self.get_position(symbol)
                
            if not position:
                return {"success": False, "message": f"No open position found for {symbol}"}
            
            # Track what's being updated
            updates = {}
            
            if stop_loss is not None and stop_loss > 0:
                updates["stop_loss"] = stop_loss
                position.stop_loss = stop_loss
                
            if take_profit is not None and take_profit > 0:
                updates["take_profit"] = take_profit
                position.take_profit = take_profit
                
            if not updates:
                return {"success": True, "message": "No updates provided"}
            
            # Execute position update with broker
            try:
                # Broker API call would go here
                logger.info(f"Updating position for {position.symbol} with {updates}")
                
                # Simulate successful update
                update_successful = True
                
                if not update_successful:
                    return {"success": False, "message": "Failed to update position with broker"}
                
                return {
                    "success": True,
                    "message": "Position updated successfully",
                    "data": {
                        "position_id": position.id,
                        "symbol": position.symbol,
                        "updates": updates
                    }
                }
            except Exception as e:
                logger.error(f"Error executing position update: {str(e)}")
                return {"success": False, "message": f"Position update error: {str(e)}"}
        except Exception as e:
            logger.error(f"Error updating position: {str(e)}")
            return {"success": False, "message": f"Position update error: {str(e)}"}
    
    async def add_position(self, position: Position) -> None:
        """Add a position to tracking"""
        try:
            async with self._position_lock:
                self.positions[position.id] = position
        except Exception as e:
            logger.error(f"Error adding position: {str(e)}")
    
    async def remove_position(self, position_id: str) -> None:
        """Remove a position from tracking"""
        try:
            async with self._position_lock:
                if position_id in self.positions:
                    del self.positions[position_id]
        except Exception as e:
            logger.error(f"Error removing position: {str(e)}")
    
    async def update_account_stats(self) -> None:
        """Update account statistics from broker"""
        try:
            # Call broker API to get account details
            success, account_data = await get_account_summary()
            
            if not success:
                logger.error("Failed to get account data for statistics")
                return
                
            # Extract account information
            self._account_balance = float(account_data.get('balance', 0))
            self._account_equity = float(account_data.get('equity', self._account_balance))
            
            # Calculate drawdown
            if self._account_balance > 0:
                current_drawdown = (self._account_balance - self._account_equity) / self._account_balance
                self._max_drawdown = max(self._max_drawdown, current_drawdown)
            
            # Calculate win rate
            win_rate = 0
            if self._total_trades > 0:
                win_rate = self._winning_trades / self._total_trades
                
            # Update stats cache
            self._account_stats = {
                "balance": self._account_balance,
                "equity": self._account_equity,
                "drawdown": current_drawdown if 'current_drawdown' in locals() else 0,
                "max_drawdown": self._max_drawdown,
                "daily_pnl": self._daily_pnl,
                "daily_loss": max(0, -self._daily_pnl / self._account_balance if self._account_balance > 0 else 0),
                "total_trades": self._total_trades,
                "win_rate": win_rate,
                "open_positions": len(self.positions)
            }
            
            self._last_stats_update = datetime.now()
        except Exception as e:
            logger.error(f"Error updating account stats: {str(e)}")
    
    async def get_account_stats(self) -> Dict[str, Any]:
        """Get account statistics"""
        try:
            # If stats are fresh, return cached version
            if (datetime.now() - self._last_stats_update).total_seconds() < 60:
                return self._account_stats
                
            # Otherwise update and return
            await self.update_account_stats()
            return self._account_stats
        except Exception as e:
            logger.error(f"Error getting account stats: {str(e)}")
            return {}
    
    async def reset_daily_stats(self) -> None:
        """Reset daily statistics (called at start of trading day)"""
        try:
            self._daily_trades = []
            self._daily_pnl = 0.0
            
            # Update stats after reset
            await self.update_account_stats()
            logger.info("Daily trading statistics reset")
        except Exception as e:
            logger.error(f"Error resetting daily stats: {str(e)}")

async def get_position_manager() -> PositionManager:
    """Get or create the PositionManager singleton"""
    try:
        position_manager = PositionManager()
        await position_manager.initialize()
        return position_manager
    except Exception as e:
        logger.error(f"Failed to create PositionManager: {str(e)}")
        raise

##############################################################################
# Alert Handler Class
##############################################################################

class AlertHandler:
    """Handler for trading alerts with comprehensive risk management"""
    def __init__(self, position_manager: PositionManager):
        self.position_manager = position_manager
        
        # Initialize risk management components
        self._risk_level = "moderate"  # Default risk level
        self._max_drawdown = 0.05  # 5% max drawdown
        self._max_daily_loss = 0.03  # 3% max daily loss
        self._max_positions = 10  # Maximum concurrent positions
        
        # State tracking
        self._initialized = False
        self._running = False
        self._position_queue = asyncio.Queue()
        self._reconciliation_task = None
        
    async def initialize(self):
        """Initialize the alert handler and its components"""
        if self._initialized:
            return
            
        # Start background tasks
        self._running = True
        self._reconciliation_task = asyncio.create_task(self._reconcile_positions())
        
        # Mark as initialized
        self._initialized = True
        logger.info("Alert handler initialized successfully")
        
    async def _reconcile_positions(self):
        """Background task to reconcile positions with broker"""
        try:
            while self._running:
                try:
                    # Get positions from broker and local storage
                    broker_positions = await self.position_manager.get_open_positions()
                    local_positions = self.position_manager.get_all_positions()
                    
                    # Find discrepancies
                    broker_ids = set(pos.id for pos in broker_positions)
                    local_ids = set(local_positions.keys())
                    
                    # Positions in local but not in broker (need to be removed)
                    for pos_id in local_ids - broker_ids:
                        logger.warning(f"Position {pos_id} exists locally but not with broker - removing")
                        await self.position_manager.remove_position(pos_id)
                    
                    # Positions in broker but not local (need to be added)
                    for pos in broker_positions:
                        if pos.id not in local_ids:
                            logger.warning(f"Position {pos.id} exists with broker but not locally - adding")
                            self.position_manager.add_position(pos)
                    
                    await asyncio.sleep(60)  # Check every minute
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error reconciling positions: {str(e)}")
                    await asyncio.sleep(300)  # Longer delay on error
        except asyncio.CancelledError:
            logger.info("Position reconciliation task cancelled")
        except Exception as e:
            logger.error(f"Fatal error in position reconciliation: {str(e)}")
    
    async def process_alert(self, alert_data: AlertData) -> Dict[str, Any]:
        """Process an incoming trading alert"""
        try:
            logger.info(f"Processing alert for {alert_data.symbol} - {alert_data.action}")
            
            # Validate alert against risk management rules
            valid, reason = await self._validate_alert(alert_data)
            if not valid:
                return {"success": False, "message": f"Alert validation failed: {reason}"}
            
            # Check if market is open for the instrument
            tradeable, reason = await is_instrument_tradeable(alert_data.symbol)
            if not tradeable:
                return {"success": False, "message": f"Market closed: {reason}"}
            
            # Process based on action type
            if alert_data.action.lower() in ["buy", "sell"]:
                # Create new position
                position_result = await self.position_manager.create_position(alert_data)
                if position_result["success"]:
                    return {"success": True, "message": f"Position opened successfully", "data": position_result["data"]}
                else:
                    return {"success": False, "message": f"Failed to open position: {position_result['message']}"}
            
            elif alert_data.action.lower() in ["close", "close_all"]:
                # Close positions
                if alert_data.action.lower() == "close_all":
                    close_result = await self.position_manager.close_all_positions()
                else:
                    close_result = await self.position_manager.close_position(alert_data.symbol, alert_data.id)
                
                return {"success": close_result["success"], "message": close_result["message"]}
            
            elif alert_data.action.lower() in ["modify", "update"]:
                # Modify existing position
                update_result = await self.position_manager.update_position(
                    alert_data.id,
                    alert_data.symbol,
                    stop_loss=alert_data.stop_loss,
                    take_profit=alert_data.take_profit
                )
                return {"success": update_result["success"], "message": update_result["message"]}
            
            else:
                return {"success": False, "message": f"Unsupported action: {alert_data.action}"}
                
        except Exception as e:
            logger.error(f"Error processing alert: {str(e)}", exc_info=True)
            return {"success": False, "message": f"Internal error: {str(e)}"}
    
    async def _validate_alert(self, alert_data: AlertData) -> Tuple[bool, str]:
        """Validate alert against risk management rules"""
        try:
            # Check if we're at max positions capacity
            if len(self.position_manager.get_all_positions()) >= self._max_positions:
                return False, "Maximum number of concurrent positions reached"
            
            # Get account stats
            account_stats = await self.position_manager.get_account_stats()
            
            # Check for max drawdown breach
            if account_stats.get("drawdown", 0) > self._max_drawdown:
                return False, f"Maximum drawdown reached ({account_stats['drawdown']*100:.2f}%)"
            
            # Check for max daily loss breach
            if account_stats.get("daily_loss", 0) > self._max_daily_loss:
                return False, f"Maximum daily loss reached ({account_stats['daily_loss']*100:.2f}%)"
            
            # For new positions, check risk per trade
            if alert_data.action.lower() in ["buy", "sell"]:
                # Calculate potential risk
                account_balance = account_stats.get("balance", 0)
                if account_balance <= 0:
                    return False, "Invalid account balance"
                
                # Get price data for risk calculation
                entry = await get_current_price(alert_data.symbol, alert_data.action)
                if entry <= 0:
                    return False, "Invalid entry price"
                
                # Calculate potential risk
                stop_loss = alert_data.stop_loss
                if not stop_loss:
                    # Calculate default stop loss if none provided
                    atr = get_atr(alert_data.symbol, alert_data.timeframe or "D1")
                    if alert_data.action.lower() == "buy":
                        stop_loss = entry - (atr * 1.5)
                    else:
                        stop_loss = entry + (atr * 1.5)
                
                # Calculate risk amount
                risk_per_trade = abs(entry - stop_loss) / entry
                risk_amount = account_balance * risk_per_trade * alert_data.size
                
                # Check if risk is acceptable
                max_risk_per_trade = 0.02  # 2% per trade
                if risk_amount / account_balance > max_risk_per_trade:
                    return False, f"Risk per trade exceeds maximum ({risk_amount/account_balance*100:.2f}%)"
            
            return True, ""
            
        except Exception as e:
            logger.error(f"Error validating alert: {str(e)}")
            return False, f"Validation error: {str(e)}"
    
    async def process_positions(self):
        """Process all open positions (check for SL/TP)"""
        try:
            positions = self.position_manager.get_all_positions()
            if not positions:
                return
                
            for pos_id, position in positions.items():
                try:
                    # Check current price
                    current_price = await get_current_price(position.symbol, "close")
                    
                    # Skip if price couldn't be retrieved
                    if current_price <= 0:
                        logger.warning(f"Could not retrieve price for {position.symbol} - skipping")
                        continue
                    
                    # Check stop loss
                    if position.stop_loss and (
                        (position.direction.lower() == "buy" and current_price <= position.stop_loss) or
                        (position.direction.lower() == "sell" and current_price >= position.stop_loss)
                    ):
                        logger.info(f"Stop loss hit for {position.symbol} at {current_price}")
                        await self.position_manager.close_position(position.symbol, position.id)
                    
                    # Check take profit
                    elif position.take_profit and (
                        (position.direction.lower() == "buy" and current_price >= position.take_profit) or
                        (position.direction.lower() == "sell" and current_price <= position.take_profit)
                    ):
                        logger.info(f"Take profit hit for {position.symbol} at {current_price}")
                        await self.position_manager.close_position(position.symbol, position.id)
                        
                except Exception as e:
                    logger.error(f"Error processing position {pos_id}: {str(e)}")
                
        except Exception as e:
            logger.error(f"Error in position processing: {str(e)}")

    async def _close_position(self, symbol: str, reason: str = "system"):
        """Close a position with proper cleanup"""
        try:
            # Get position data
            position = await self.position_manager.get_position(symbol)
            if not position:
                logger.warning(f"Cannot close position for {symbol}: not found")
                return False
            
            # Close the position with the broker
            close_result = await self.position_manager.close_position(symbol)
            
            if not close_result.get("success", False):
                logger.error(f"Failed to close position for {symbol}: {close_result.get('message', 'Unknown error')}")
                return False
                
            logger.info(f"Successfully closed position for {symbol} due to {reason}")
            return True
        except Exception as e:
            logger.error(f"Error in _close_position for {symbol}: {str(e)}")
            return False

##############################################################################
# Application State Management
##############################################################################

# Global instances for application state
position_manager = None
alert_handler = None

async def get_position_manager() -> PositionManager:
    """Get or create the global position manager instance"""
    global position_manager
    if position_manager is None:
        position_manager = PositionManager()
        await position_manager.initialize()
    return position_manager

async def get_alert_handler() -> AlertHandler:
    """Get or create the AlertHandler singleton"""
    try:
        position_manager = await get_position_manager()
        
        alert_handler = AlertHandler(position_manager)
        await alert_handler.initialize()
        return alert_handler
    except Exception as e:
        logger.error(f"Failed to create AlertHandler: {str(e)}")
        raise

##############################################################################
# FastAPI Setup & Lifespan
##############################################################################

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager with proper initialization and cleanup"""
    logger.info("Initializing application...")
    
    try:
        # Initialize position manager
        pm = await get_position_manager()
        logger.info("Position manager initialized")
        
        # Initialize alert handler with position manager
        ah = await get_alert_handler()
        logger.info("Alert handler initialized")
        
        # Set up shutdown signal handling
        handle_shutdown_signals()
        
        logger.info("Services initialized successfully")
        yield
    finally:
        logger.info("Shutting down services...")
        await cleanup_resources()
        logger.info("Shutdown complete")

async def cleanup_resources():
    """Clean up application resources"""
    global alert_handler, position_manager, redis_client
    
    tasks = []
    
    # Stop alert handler if initialized
    if alert_handler is not None:
        tasks.append(alert_handler.stop())
    
    # Close any open sessions
    if hasattr(get_session, 'session') and not get_session.session.closed:
        tasks.append(get_session.session.close())
    
    # Close Redis connection
    if redis_client is not None:
        try:
            await redis_client.close()
            redis_client = None
        except Exception as e:
            logger.error(f"Error closing Redis connection: {str(e)}")
    
    # Wait for all cleanup tasks
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

def handle_shutdown_signals():
    """Set up signal handlers for graceful shutdown"""
    async def shutdown(sig: signal.Signals):
        logger.info(f"Received exit signal {sig.name}")
        await cleanup_resources()
        
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_event_loop().add_signal_handler(sig,
            lambda s=sig: asyncio.create_task(shutdown(s))
        )

# Create FastAPI app with proper configuration
app = FastAPI(
    title="OANDA Trading Bot",
    description="Advanced async trading bot using FastAPI and aiohttp",
    version="1.2.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.allowed_origins.split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

##############################################################################
# API Endpoints
##############################################################################

@app.get("/api/health")
async def health_check():
    """Health check endpoint for monitoring"""
    return {"status": "ok", "timestamp": datetime.now(timezone('Asia/Bangkok')).isoformat()}

@app.get("/api/positions")
async def get_positions_endpoint(position_manager: PositionManager = Depends(get_position_manager)):
    """Get all open positions"""
    try:
        positions = await position_manager.get_positions()
        return {"status": "success", "positions": positions}
    except Exception as e:
        logger.error(f"Error fetching positions: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching positions: {str(e)}"
        )

@app.post("/api/alerts")
async def process_alert_endpoint(
    alert_data: AlertData,
    alert_handler: AlertHandler = Depends(get_alert_handler)
):
    """Process an alert from TradingView or other sources"""
    try:
        result = await alert_handler.process_alert(alert_data)
        return result
    except ValidationError as e:
        logger.error(f"Validation error processing alert: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid alert data: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Error processing alert: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing alert: {str(e)}"
        )

@app.post("/tradingview")
async def tradingview_webhook(
    request: Request,
    alert_handler: AlertHandler = Depends(get_alert_handler)
):
    """Webhook for TradingView alerts"""
    try:
        # Parse incoming JSON
        payload = await request.json()
        logger.info(f"Received TradingView alert: {payload}")
        
        # Extract alert data
        alert_data = AlertData(
            symbol=payload.get("symbol"),
            timeframe=payload.get("timeframe", "1H"),
            action=payload.get("action"),
            price=float(payload.get("price", 0)),
            stop_loss=float(payload.get("stop_loss", 0)) if payload.get("stop_loss") else None,
            take_profit=float(payload.get("take_profit", 0)) if payload.get("take_profit") else None,
            risk_percentage=float(payload.get("risk_percentage", 2.0)),
            message=payload.get("message")
        )
        
        # Process the alert
        result = await alert_handler.process_alert(alert_data)
        return result
    except ValidationError as e:
        logger.error(f"Validation error from TradingView: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"status": "error", "detail": f"Invalid alert data: {str(e)}"}
        )
    except Exception as e:
        logger.error(f"Error processing TradingView alert: {str(e)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"status": "error", "detail": f"Error processing alert: {str(e)}"}
        )

@app.get("/api/account")
async def get_account_summary_endpoint():
    """Get account summary from OANDA"""
    try:
        success, data = await get_account_summary()
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=data.get("error", "Unknown error getting account summary")
            )
        return data
    except Exception as e:
        logger.error(f"Error fetching account summary: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching account summary: {str(e)}"
        )

@app.post("/api/close")
async def close_position_endpoint(
    close_data: Dict[str, Any],
    position_manager: PositionManager = Depends(get_position_manager),
    alert_handler: AlertHandler = Depends(get_alert_handler)
):
    """Close a specific position or all positions"""
    try:
        # Check if closing all positions
        if close_data.get("all", False):
            positions = await position_manager.get_positions()
            results = {}
            
            for position in positions.values():
                if 'symbol' in position:
                    # Close in alert handler (which will handle risk managers)
                    await alert_handler._close_position(
                        position['symbol'], 
                        "manual_close"
                    )
                    results[position['symbol']] = {
                        "status": "closed"
                    }
            
            return {"status": "success", "closed": results}
        
        # Close a specific position
        elif "symbol" in close_data:
            symbol = standardize_symbol(close_data["symbol"])
            position = await position_manager.get_position(symbol)
            
            if not position:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No open position found for {symbol}"
                )
                
            # Close in alert handler (which will handle risk managers)
            await alert_handler._close_position(
                symbol, 
                "manual_close"
            )
            
            return {
                "status": "success",
                "closed": {
                    "symbol": symbol
                }
            }
        
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Must specify 'all' or 'symbol' to close positions"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error closing position: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error closing position: {str(e)}"
        )

##############################################################################
# Market Utility Functions
##############################################################################

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
    # This is a duplicate implementation and should be removed in favor of the async version
    pass  # This function will be replaced by references to the async version

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

@handle_async_errors
async def get_current_price(instrument: str, action: str = "BUY") -> float:
    """Get current price of instrument with error handling and timeout"""
    try:
        # Get OANDA pricing
        session = await get_session()
        normalized = standardize_symbol(instrument)
        
        url = f"{config.oanda_api_url}/accounts/{config.oanda_account_id}/pricing"
        params = {
            "instruments": normalized,
            "includeHomeConversions": True
        }
        
        async with session.get(url, params=params, timeout=HTTP_REQUEST_TIMEOUT) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"OANDA price fetch error for {normalized}: {error_text}")
                return 0.0
                
            data = await response.json()
            
            if "prices" not in data or not data["prices"]:
                logger.error(f"No price data returned for {normalized}")
                return 0.0
                
            price_data = data["prices"][0]
            
            if action.upper() == "BUY":
                # Use ask price for buying
                ask_details = price_data.get("asks", [{}])[0]
                return float(ask_details.get("price", 0))
            else:
                # Use bid price for selling
                bid_details = price_data.get("bids", [{}])[0]
                return float(bid_details.get("price", 0))
                
    except asyncio.TimeoutError:
        logger.error(f"Timeout getting price for {instrument}")
        return 0.0
    except Exception as e:
        logger.error(f"Error getting price for {instrument}: {str(e)}")
        return 0.0

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

@handle_async_errors
async def get_account_summary() -> Tuple[bool, Dict[str, Any]]:
    """Get account summary from OANDA API"""
    try:
        session = await get_session()
        url = f"{config.oanda_api_url}/accounts/{config.oanda_account_id}/summary"
        
        async with session.get(url, timeout=HTTP_REQUEST_TIMEOUT) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"Failed to fetch account summary: {error_text}")
                return False, {"error": error_text}
                
            data = await response.json()
            
            # Extract key account metrics
            account = data.get("account", {})
            
            # Format and return the account summary
            return True, {
                "id": account.get("id"),
                "name": account.get("alias"),
                "balance": float(account.get("balance", 0)),
                "currency": account.get("currency"),
                "margin_available": float(account.get("marginAvailable", 0)),
                "margin_used": float(account.get("marginUsed", 0)),
                "margin_closeout_percent": float(account.get("marginCloseoutPercent", 0)),
                "open_trade_count": account.get("openTradeCount", 0),
                "open_position_count": account.get("openPositionCount", 0),
                "unrealized_pl": float(account.get("unrealizedPL", 0)),
                "nav": float(account.get("NAV", 0)),
                "timestamp": datetime.now(timezone('Asia/Bangkok')).isoformat()
            }
    except asyncio.TimeoutError:
        logger.error(f"Timeout fetching account summary")
        return False, {"error": "Request timed out"}
    except Exception as e:
        logger.error(f"Error fetching account summary: {str(e)}")
        return False, {"error": str(e)}

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
        app,  # Use the app instance directly
        host=host,
        port=port,
        reload=config.environment == "development"
    )

if __name__ == "__main__":
    start() 
