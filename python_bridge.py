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

    # Rest of PositionTracker implementation...

##############################################################################
# Class: AlertHandler 
##############################################################################

class AlertHandler:
    def __init__(self, position_manager: PositionManager):
        self.position_manager = position_manager
        self.volatility_monitor = VolatilityMonitor()
        self.market_structure = MarketStructureAnalyzer()
        self.risk_manager = EnhancedRiskManager()
        self.exit_manager = DynamicExitManager()
        self.loss_manager = AdvancedLossManager()
        self.position_sizing = PositionSizingManager()
        self.risk_analytics = RiskAnalytics()
        
    async def process_alert(self, alert_data: AlertData) -> Dict[str, Any]:
        """Process a trading alert with advanced risk management"""
        try:
            logger.info(f"Processing alert for {alert_data.symbol} - {alert_data.action} at {alert_data.price}")
            
            # Check if market is open for this instrument
            is_tradeable, reason = await is_instrument_tradeable(alert_data.symbol)
            if not is_tradeable:
                logger.warning(f"Cannot process alert for {alert_data.symbol}: {reason}")
                return {
                    "status": "rejected",
                    "reason": reason,
                    "symbol": alert_data.symbol,
                    "action": alert_data.action
                }
                
            # Get account summary for position sizing
            account_data = await get_account_summary()
            if not account_data or 'balance' not in account_data:
                logger.error("Failed to get account data for position sizing")
                return {
                    "status": "error",
                    "reason": "Failed to get account data",
                    "symbol": alert_data.symbol
                }
            
            account_balance = account_data['balance']
            
            # Get current positions
            positions = await get_open_positions()
            existing_symbols = [pos['instrument'] for pos in positions]
            
            # Initialize volatility monitor
            await self.volatility_monitor.initialize_market_condition(
                alert_data.symbol, alert_data.timeframe
            )
            
            # Update volatility with current ATR
            atr = get_atr(alert_data.symbol, alert_data.timeframe)
            await self.volatility_monitor.update_volatility(
                alert_data.symbol, atr, alert_data.timeframe
            )
            
            # Get market condition
            market_condition = await self.volatility_monitor.get_market_condition(alert_data.symbol)
            
            # Check if we should adjust risk based on volatility
            should_adjust, adjustment_factor = await self.volatility_monitor.should_adjust_risk(
                alert_data.symbol, alert_data.timeframe
            )
            
            # Check if we should reduce risk based on loss limits
            loss_adjust, loss_factor = await self.loss_manager.should_reduce_risk()
            if loss_adjust:
                logger.warning(f"Reducing risk due to loss limits - factor: {loss_factor}")
                adjustment_factor = min(adjustment_factor, loss_factor)
                
            # Get correlation factor based on existing positions
            correlation_factor = await self.position_sizing.get_correlation_factor(
                alert_data.symbol, existing_symbols
            )
            
            # Calculate position size with all factors
            position_size = await self.position_sizing.calculate_position_size(
                account_balance=account_balance,
                entry_price=alert_data.price,
                stop_loss=alert_data.stop_loss,
                atr=atr,
                timeframe=alert_data.timeframe,
                market_condition=market_condition,
                correlation_factor=correlation_factor
            )
            
            # Adjust for volatility and loss factors
            if should_adjust or loss_adjust:
                position_size *= adjustment_factor
                position_size = round(position_size, 2)
                
            # Initialize risk management
            await self.risk_manager.initialize_position(
                symbol=alert_data.symbol,
                entry_price=alert_data.price,
                position_type=alert_data.action,
                timeframe=alert_data.timeframe,
                units=position_size,
                atr=atr
            )
            
            # Get the dynamically calculated stop loss and take profit levels
            position_data = self.risk_manager.positions.get(alert_data.symbol, {})
            if position_data:
                calculated_stop = position_data.get('stop_loss')
                calculated_tps = position_data.get('take_profits', [])
                
                # Prepare execution details
                result = {
                    "status": "accepted",
                    "symbol": alert_data.symbol,
                    "action": alert_data.action,
                    "entry_price": alert_data.price,
                    "stop_loss": calculated_stop if calculated_stop else alert_data.stop_loss,
                    "take_profits": calculated_tps if calculated_tps else [alert_data.take_profit],
                    "position_size": position_size,
                    "timeframe": alert_data.timeframe,
                    "market_condition": market_condition,
                    "risk_percentage": alert_data.risk_percentage,
                    "timestamp": datetime.now(timezone('Asia/Bangkok')).isoformat()
                }
                
                # Execute the position via Oanda API if required
                try:
                    # This is where the actual order execution would occur
                    # For now, we'll just log that an order would be placed
                    logger.info(f"Would place {alert_data.action} order for {position_size} units of {alert_data.symbol} at {alert_data.price}")
                    
                    # Initialize loss manager
                    await self.loss_manager.initialize_position(
                        symbol=alert_data.symbol,
                        entry_price=alert_data.price,
                        position_type=alert_data.action,
                        units=position_size,
                        account_balance=account_balance
                    )
                    
                    # Initialize exit manager
                    await self.exit_manager.initialize_exits(
                        symbol=alert_data.symbol,
                        entry_price=alert_data.price,
                        position_type=alert_data.action,
                        initial_stop=calculated_stop if calculated_stop else alert_data.stop_loss,
                        initial_tp=calculated_tps[0] if calculated_tps else alert_data.take_profit
                    )
                    
                    # Initialize risk analytics
                    await self.risk_analytics.initialize_position(
                        symbol=alert_data.symbol,
                        entry_price=alert_data.price,
                        units=position_size
                    )
                    
                    # Store position in central repository
                    position_data = {
                        'symbol': alert_data.symbol,
                        'action': alert_data.action,
                        'entry_price': alert_data.price,
                        'stop_loss': calculated_stop,
                        'take_profits': calculated_tps,
                        'position_size': position_size,
                        'timeframe': alert_data.timeframe,
                        # Include all other relevant data
                    }
                    
                    position_id = await self.position_manager.add_position(position_data)
                    
                    return result
                    
                except Exception as e:
                    logger.error(f"Error executing position for {alert_data.symbol}: {str(e)}")
                    return {
                        "status": "error",
                        "reason": f"Position execution failed: {str(e)}",
                        "symbol": alert_data.symbol
                    }
            else:
                logger.error(f"Risk management initialization failed for {alert_data.symbol}")
                return {
                    "status": "error",
                    "reason": "Risk management initialization failed",
                    "symbol": alert_data.symbol
                }
                
        except Exception as e:
            logger.error(f"Error processing alert: {str(e)}")
            return {
                "status": "error",
                "reason": str(e),
                "symbol": alert_data.symbol if hasattr(alert_data, 'symbol') else "unknown"
            }

##############################################################################
# FastAPI Setup & Lifespan
##############################################################################

# Initialize global variables
alert_handler: Optional[AlertHandler] = None  # Add this at the top level

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager with proper initialization and cleanup"""
    logger.info("Initializing application...")
    global _session, alert_handler
    
    try:
        await get_session(force_new=True)
        alert_handler = AlertHandler()  # Initialize the handler
        await alert_handler.start()
        logger.info("Services initialized successfully")
        handle_shutdown_signals()
        yield
    finally:
        logger.info("Shutting down services...")
        await cleanup_resources()
        logger.info("Shutdown complete")

async def cleanup_resources():
    """Clean up application resources"""
    tasks = []
    if alert_handler is not None:
        tasks.append(alert_handler.stop())
    if _session is not None and not _session.closed:
        tasks.append(_session.close())
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

# Add this function to your code (near your API endpoints)
def create_error_response(status_code: int, message: str, request_id: str) -> JSONResponse:
    """Helper to create consistent error responses"""
    return JSONResponse(
        status_code=status_code,
        content={"error": message, "request_id": request_id}
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

@app.middleware("http")
async def inject_dependencies(request: Request, call_next):
    """Inject dependencies into request state"""
    request.state.alert_handler = alert_handler
    request.state.session = await get_session()
    return await call_next(request)

##############################################################################
# Middleware
##############################################################################

@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log requests with improved error handling"""
    request_id = str(uuid.uuid4())
    try:
        logger.info(f"[{request_id}] {request.method} {request.url} started")
        
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        
        logger.info(
            f"[{request_id}] {request.method} {request.url} completed "
            f"with status {response.status_code} in {process_time:.4f}s"
        )
        
        # Add request ID to response headers
        response.headers["X-Request-ID"] = request_id
        return response
    except Exception as e:
        logger.error(f"[{request_id}] Error processing request: {str(e)}", exc_info=True)
        return create_error_response(
            status_code=500,
            message="Internal server error",
            request_id=request_id
        )

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Rate limiting middleware with configurable limits"""
    # Only apply rate limiting to trading routes
    path = request.url.path
    
    if path in ["/api/trade", "/api/close", "/api/alerts"]:
        client_ip = request.client.host
        
        # Create rate limiters if not already done
        if not hasattr(app, "rate_limiters"):
            app.rate_limiters = {}
            
        # Get or create rate limiter for this IP
        if client_ip not in app.rate_limiters:
            app.rate_limiters[client_ip] = {
                "count": 0,
                "reset_time": time.time() + 60  # Reset after 60 seconds
            }
            
        # Check if limit exceeded
        rate_limiter = app.rate_limiters[client_ip]
        current_time = time.time()
        
        # Reset if needed
        if current_time > rate_limiter["reset_time"]:
            rate_limiter["count"] = 0
            rate_limiter["reset_time"] = current_time + 60
            
        # Increment and check
        rate_limiter["count"] += 1
        
        if rate_limiter["count"] > config.max_requests_per_minute:
            logger.warning(f"Rate limit exceeded for {client_ip}")
            return JSONResponse(
                status_code=429,
                content={"error": "Too many requests", "retry_after": int(rate_limiter["reset_time"] - current_time)}
            )
            
    return await call_next(request)

##############################################################################
# API Endpoints
##############################################################################

@app.get("/api/health")
async def health_check():
    """Health check endpoint with service status information"""
    try:
        # Check session health
        session_status = "healthy" if _session and not _session.closed else "unavailable"
        
        # Check account connection health
        account_status = "unknown"
        if session_status == "healthy":
            try:
                async with asyncio.timeout(5):
                    success, _ = await get_account_summary()
                    account_status = "connected" if success else "disconnected"
            except asyncio.TimeoutError:
                account_status = "timeout"
            except Exception:
                account_status = "error"
                
        # Check position tracker health
        tracker_status = "healthy" if alert_handler and alert_handler._initialized else "unavailable"
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {
                "session": session_status,
                "account": account_status,
                "position_tracker": tracker_status
            },
            "version": "1.2.0"
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"status": "unhealthy", "error": str(e)}
        )

@app.get("/api/account")
async def get_account_info():
    """Get account information with comprehensive summary"""
    try:
        success, account_info = await get_account_summary()
        
        if not success:
            return JSONResponse(
                status_code=400,
                content={"error": "Failed to get account information"}
            )
            
        # Extract key metrics
        margin_rate = float(account_info.get("marginRate", "0"))
        margin_available = float(account_info.get("marginAvailable", "0"))
        margin_used = float(account_info.get("marginUsed", "0"))
        balance = float(account_info.get("balance", "0"))
        
        # Calculate margin utilization
        margin_utilization = (margin_used / balance) * 100 if balance > 0 else 0
        
        # Additional information for risk context
        daily_pnl = 0
        if alert_handler:
            daily_pnl = await alert_handler.position_tracker.get_daily_pnl()
            
        return {
            "account_id": account_info.get("id"),
            "balance": balance,
            "currency": account_info.get("currency"),
            "margin_available": margin_available,
            "margin_used": margin_used,
            "margin_rate": margin_rate,
            "margin_utilization": round(margin_utilization, 2),
            "open_position_count": len(account_info.get("positions", [])),
            "daily_pnl": daily_pnl,
            "last_updated": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting account info: {str(e)}")
        return JSONResponse(
            status_code=500, 
            content={"error": f"Internal server error: {str(e)}"}
        )

@app.get("/api/positions")
async def get_positions_info():
    """Get all tracked positions with additional information"""
    try:
        # Get positions from OANDA
        success, oanda_positions = await get_open_positions()
        
        # Get tracked positions
        tracked_positions = {}
        if alert_handler and alert_handler.position_tracker:
            tracked_positions = await alert_handler.position_tracker.get_all_positions()
            
        positions_data = {}
        
        # Process OANDA positions
        if success:
            for pos in oanda_positions.get("positions", []):
                symbol = pos["instrument"]
                
                # Determine position direction
                long_units = float(pos.get("long", {}).get("units", 0))
                short_units = float(pos.get("short", {}).get("units", 0))
                
                direction = "LONG" if long_units > 0 else "SHORT"
                units = long_units if direction == "LONG" else abs(short_units)
                
                # Get current price
                current_price = await get_current_price(symbol, direction)
                
                # Get tracked data if available
                tracked_data = tracked_positions.get(symbol, {})
                
                # Get risk management data if available
                risk_data = {}
                if alert_handler and symbol in alert_handler.risk_manager.positions:
                    position = alert_handler.risk_manager.positions[symbol]
                    risk_data = {
                        "stop_loss": position.get("stop_loss"),
                        "take_profits": position.get("take_profits", {}),
                        "trailing_stop": position.get("trailing_stop")
                    }
                
                # Calculate P&L
                unrealized_pl = float(pos.get("long" if direction == "LONG" else "short", {}).get("unrealizedPL", 0))
                entry_price = tracked_data.get("entry_price") or float(pos.get("long" if direction == "LONG" else "short", {}).get("averagePrice", 0))
                
                # Get trade duration
                entry_time = tracked_data.get("entry_time")
                duration = None
                if entry_time:
                    now = datetime.now(timezone('Asia/Bangkok'))
                    duration = (now - entry_time).total_seconds() / 3600  # Hours
                
                positions_data[symbol] = {
                    "symbol": symbol,
                    "direction": direction,
                    "units": units,
                    "entry_price": entry_price,
                    "current_price": current_price,
                    "unrealized_pl": unrealized_pl,
                    "timeframe": tracked_data.get("timeframe", "Unknown"),
                    "entry_time": entry_time.isoformat() if entry_time else None,
                    "duration_hours": round(duration, 2) if duration else None,
                    "risk_data": risk_data
                }
        
        return {
            "positions": list(positions_data.values()),
            "count": len(positions_data),
            "tracking_available": alert_handler is not None and alert_handler._initialized,
            "last_updated": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting positions info: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}"}
        )

@app.get("/api/market/{instrument}")
async def get_market_info(instrument: str, timeframe: str = "H1"):
    """Get market information for an instrument"""
    try:
        instrument = standardize_symbol(instrument)
        
        # Check if instrument is tradeable
        tradeable, reason = is_instrument_tradeable(instrument)
        
        # Get current price
        buy_price = await get_current_price(instrument, "BUY")
        sell_price = await get_current_price(instrument, "SELL")
        
        # Get market condition if volatility monitor is available
        market_condition = "NORMAL"
        if alert_handler and alert_handler.volatility_monitor:
            await alert_handler.volatility_monitor.update_volatility(
                instrument, 0.001, timeframe
            )
            market_condition = await alert_handler.volatility_monitor.get_market_condition(instrument)
        
        # Get market structure if available
        structure_data = {}
        if alert_handler and alert_handler.market_structure:
            try:
                structure = await alert_handler.market_structure.analyze_market_structure(
                    instrument, timeframe, buy_price, buy_price, buy_price
                )
                structure_data = {
                    "nearest_support": structure.get("nearest_support"),
                    "nearest_resistance": structure.get("nearest_resistance"),
                    "support_levels": structure.get("support_levels", []),
                    "resistance_levels": structure.get("resistance_levels", [])
                }
            except Exception as e:
                logger.warning(f"Error getting market structure: {str(e)}")
        
        # Market session information
        current_time = datetime.now(timezone('Asia/Bangkok'))
        
        # Create response
        return {
            "instrument": instrument,
            "timestamp": current_time.isoformat(),
            "tradeable": tradeable,
            "reason": reason if not tradeable else None,
            "prices": {
                "buy": buy_price,
                "sell": sell_price,
                "spread": round(abs(buy_price - sell_price), 5)
            },
            "market_condition": market_condition,
            "market_structure": structure_data,
            "current_session": get_current_market_session(current_time)
        }
    except Exception as e:
        logger.error(f"Error getting market info: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}"}
        )

@app.post("/api/alerts")
async def handle_alert(
    alert_data: AlertData,
    background_tasks: BackgroundTasks,
    request: Request
):
    """Process trading alerts with improved error handling and non-blocking execution"""
    request_id = str(uuid.uuid4())
    
    try:
        # Convert to dict for logging and processing
        alert_dict = alert_data.dict()
        logger.info(f"[{request_id}] Received alert: {json.dumps(alert_dict, indent=2)}")
        
        # Check for missing alert handler
        if not alert_handler:
            logger.error(f"[{request_id}] Alert handler not initialized")
            return JSONResponse(
                status_code=503,
                content={"error": "Service unavailable", "request_id": request_id}
            )
        
        # Process alert in the background
        background_tasks.add_task(
            alert_handler.process_alert,
            alert_dict
        )
        
        return {
            "message": "Alert received and processing started",
            "request_id": request_id,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"[{request_id}] Error processing alert: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}", "request_id": request_id}
        )

@app.post("/api/trade")
async def execute_trade_endpoint(
    alert_data: AlertData,
    background_tasks: BackgroundTasks,
    request: Request
):
    """Execute a trade with specified parameters"""
    request_id = str(uuid.uuid4())
    
    try:
        # Convert to dict
        alert_dict = alert_data.dict()
        logger.info(f"[{request_id}] Trade request: {json.dumps(alert_dict, indent=2)}")
        
        # Execute the trade directly
        success, result = await execute_trade(alert_dict)
        
        if success:
            # If using alert handler, record the position
            if alert_handler and alert_handler.position_tracker:
                background_tasks.add_task(
                    alert_handler.position_tracker.record_position,
                    alert_dict['symbol'],
                    alert_dict['action'],
                    alert_dict['timeframe'],
                    float(result.get('orderFillTransaction', {}).get('price', 0))
                )
                
            return {
                "success": True,
                "message": "Trade executed successfully",
                "transaction_id": result.get('orderFillTransaction', {}).get('id'),
                "request_id": request_id,
                "details": result
            }
        else:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "message": "Trade execution failed",
                    "request_id": request_id,
                    "error": result.get('error', 'Unknown error')
                }
            )
    except Exception as e:
        logger.error(f"[{request_id}] Error executing trade: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}", "request_id": request_id}
        )

@app.post("/api/close")
async def close_position_endpoint(close_data: Dict[str, Any], request: Request):
    """Close a position with detailed result reporting"""
    request_id = str(uuid.uuid4())
    
    try:
        logger.info(f"[{request_id}] Close position request: {json.dumps(close_data, indent=2)}")
        
        success, result = await close_position(close_data)
        
        if success:
            # If using alert handler, clear the position
            if alert_handler and alert_handler.position_tracker:
                await alert_handler.position_tracker.clear_position(close_data['symbol'])
                if alert_handler.risk_manager:
                    await alert_handler.risk_manager.clear_position(close_data['symbol'])
                    
            return {
                "success": True,
                "message": "Position closed successfully",
                "transaction_id": result.get('longOrderFillTransaction', {}).get('id') or
                               result.get('shortOrderFillTransaction', {}).get('id'),
                "request_id": request_id
            }
        else:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "message": "Failed to close position",
                    "request_id": request_id,
                    "error": result.get('error', 'Unknown error')
                }
            )
    except Exception as e:
        logger.error(f"[{request_id}] Error closing position: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}", "request_id": request_id}
        )

@app.post("/api/config")
async def update_config_endpoint(config_data: Dict[str, Any], request: Request):
    """Update trading configuration"""
    try:
        if not alert_handler:
            return JSONResponse(
                status_code=503,
                content={"error": "Service unavailable"}
            )
            
        success = await alert_handler.update_config(config_data)
        
        if success:
            return {
                "success": True,
                "message": "Configuration updated successfully"
            }
        else:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "message": "Failed to update configuration"
                }
            )
    except Exception as e:
        logger.error(f"Error updating configuration: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}"}
        )

@app.post("/tradingview")
async def tradingview_webhook(
    request: Request,
    background_tasks: BackgroundTasks
):
    """Process TradingView webhook alerts"""
    request_id = str(uuid.uuid4())
    
    try:
        # Get the raw JSON payload
        payload = await request.json()
        logger.info(f"[{request_id}] Received TradingView webhook: {json.dumps(payload, indent=2)}")
        
        # Map TradingView fields to your AlertData model
        alert_data = {
            "symbol": payload.get("symbol", ""),
            "action": payload.get("action", ""),
            "timeframe": payload.get("timeframe", "15M"),
            "orderType": payload.get("orderType", "MARKET"),
            "timeInForce": payload.get("timeInForce", "FOK"),
            "percentage": float(payload.get("percentage", 15.0)),
            "account": payload.get("account", config.oanda_account),
            "comment": payload.get("comment", "")
        }
        
        # Process alert in the background
        if alert_handler:
            background_tasks.add_task(
                alert_handler.process_alert,
                alert_data
            )
            
            return {
                "message": "TradingView alert received and processing started",
                "request_id": request_id,
                "timestamp": datetime.now().isoformat()
            }
        else:
            return JSONResponse(
                status_code=503,
                content={"error": "Service unavailable", "request_id": request_id}
            )
    except Exception as e:
        logger.error(f"[{request_id}] Error processing TradingView webhook: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": f"Internal server error: {str(e)}", "request_id": request_id}
        )

##############################################################################
# Trade Execution
##############################################################################

async def get_account_summary() -> Tuple[bool, Dict[str, Any]]:
    """Get account summary with improved error handling"""
    try:
        session = await get_session()
        async with session.get(f"{config.oanda_api_url}/accounts/{config.oanda_account}/summary") as resp:
            if resp.status != 200:
                error_text = await resp.text()
                logger.error(f"Account summary fetch failed: {error_text}")
                return False, {"error": error_text}
            data = await resp.json()
            return True, data.get('account', {})
    except Exception as e:
        logger.error(f"Error fetching account summary: {str(e)}")
        return False, {"error": str(e)}

async def get_account_balance(account_id: str) -> float:
    """Fetch account balance for dynamic position sizing"""
    try:
        session = await get_session()
        async with session.get(f"{config.oanda_api_url}/accounts/{account_id}/summary") as resp:
            data = await resp.json()
            return float(data['account']['balance'])
    except Exception as e:
        logger.error(f"Error fetching account balance: {str(e)}")
        raise

##############################################################################
# Models
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
            required_fields = ['symbol', 'timeframe', 'action', 'price']
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
                
            # Validate price is positive
            if not isinstance(values['price'], (int, float)) or values['price'] <= 0:
                raise ValueError(f"Invalid price: {values['price']}. Must be a positive number")
                
            # Calculate stop loss if not provided
            if 'stop_loss' not in values or not values['stop_loss']:
                instrument_type = get_instrument_type(values['symbol']) 
                atr = get_atr(values['symbol'], values['timeframe'])
                multiplier = get_atr_multiplier(instrument_type, values['timeframe'])
                
                if values['action'] == 'BUY':
                    values['stop_loss'] = values['price'] - (atr * multiplier)
                else:  # SELL
                    values['stop_loss'] = values['price'] + (atr * multiplier)
                    
                logger.info(f"Auto-calculated stop loss for {values['symbol']}: {values['stop_loss']}")
                
            # Calculate take profit if not provided
            if 'take_profit' not in values or not values['take_profit']:
                risk = abs(values['price'] - values['stop_loss'])
                
                if values['action'] == 'BUY':
                    values['take_profit'] = values['price'] + (risk * 2)  # 1:2 risk-reward
                else:  # SELL
                    values['take_profit'] = values['price'] - (risk * 2)  # 1:2 risk-reward
                    
                logger.info(f"Auto-calculated take profit for {values['symbol']}: {values['take_profit']}")
                
            # Set default risk percentage if not provided
            if 'risk_percentage' not in values or not values['risk_percentage']:
                values['risk_percentage'] = 2.0  # Default 2% risk per trade
                
            return values
        except Exception as e:
            logger.error(f"Error validating alert data: {str(e)}")
            raise ValueError(f"Error validating alert data: {str(e)}")

##############################################################################
# Risk Management Classes
##############################################################################

class VolatilityMonitor:
    def __init__(self):
        self.volatility_history = {}
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
        if symbol not in self.volatility_history:
            self.volatility_history[symbol] = []
            
        settings = self.volatility_thresholds.get(timeframe, self.volatility_thresholds["1H"])
        self.volatility_history[symbol].append(current_atr)
        
        # Maintain lookback period
        if len(self.volatility_history[symbol]) > settings['lookback']:
            self.volatility_history[symbol].pop(0)
            
        # Calculate volatility metrics
        if len(self.volatility_history[symbol]) >= settings['lookback']:
            mean_atr = sum(self.volatility_history[symbol]) / len(self.volatility_history[symbol])
            std_dev = statistics.stdev(self.volatility_history[symbol])
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