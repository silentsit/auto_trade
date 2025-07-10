import os
import sys
import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone
import oandapyV20
from oandapyV20.endpoints.orders import OrderCreate
from oandapyV20.endpoints.positions import PositionClose, OpenPositions
from oandapyV20.endpoints.accounts import AccountDetails
from oandapyV20.endpoints.pricing import PricingInfo
import aiohttp
import time
from urllib3.exceptions import ProtocolError
from http.client import RemoteDisconnected
from requests.exceptions import ConnectionError, Timeout
import socket

# Modular imports
from config import config
from database import PostgresDatabaseManager
from backup import BackupManager
from error_recovery import ErrorRecoverySystem, BrokerConnectionError
from api import router as api_router
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from utils import MarketDataUnavailableError, calculate_simple_position_size, get_position_size_limits, validate_trade_inputs, check_market_impact, analyze_transaction_costs, round_position_size

app = FastAPI(
    title="Enhanced Trading System API",
    description="Institutional-grade trading system with advanced risk management",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Mount static files after app is defined
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    favicon_path = os.path.join("static", "favicon.ico")
    return FileResponse(favicon_path)

# Globals for components
alert_handler = None
error_recovery = None
db_manager = None
backup_manager = None
oanda = None
session = None

# Logging setup
def setup_production_logging():
    """Setup logging optimized for cloud deployment (Render)"""
    # Clear any existing handlers to avoid conflicts
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create a console handler for stdout (required for Render)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)
    
    # Set specific logger levels to reduce noise
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.INFO)
    
    # Force flush to ensure logs appear
    console_handler.flush()
    
    print("✅ Production logging configured for Render", flush=True)

def setup_local_logging():
    """Setup logging for local development with both console and file"""
    # Clear any existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create handlers
    console_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler("trading_system.log")
    
    # Set levels
    console_handler.setLevel(logging.INFO)
    file_handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
    print("✅ Local logging configured", flush=True)

# Ensure we have sys imported
import sys

# Use environment-appropriate logging
environment = os.getenv("ENVIRONMENT", "development")
print(f"🔧 Configuring logging for environment: {environment}", flush=True)

if environment == "production":
    setup_production_logging()
else:
    setup_local_logging()

# Force immediate log output
sys.stdout.flush()
sys.stderr.flush()

logger = logging.getLogger("trading_system")

# Test logging immediately
print("🧪 Testing logging system...", flush=True)
logger.info("LOGGING TEST: This should appear in Render logs")
logger.warning("LOGGING TEST: Warning level test")
logger.error("LOGGING TEST: Error level test")
sys.stdout.flush()

# Log startup information
logger.info("="*50)
logger.info("🚀 TRADING BOT STARTING UP")
logger.info(f"Environment: {os.getenv('ENVIRONMENT', 'development')}")
logger.info(f"Python version: {sys.version}")
logger.info(f"Working directory: {os.getcwd()}")
logger.info(f"OANDA Environment: {config.oanda_environment}")
logger.info("="*50)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

# OANDA connection health tracking
class OANDAConnectionManager:
    def __init__(self):
        self.last_successful_request = time.time()
        self.consecutive_failures = 0
        self.connection_healthy = True
        self.health_check_interval = 60  # seconds
        self.max_consecutive_failures = 3
        
    def record_success(self):
        """Record a successful request"""
        self.last_successful_request = time.time()
        self.consecutive_failures = 0
        self.connection_healthy = True
        
    def record_failure(self):
        """Record a failed request"""
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.max_consecutive_failures:
            self.connection_healthy = False
            
    def should_reinitialize(self):
        """Check if we should reinitialize the OANDA client"""
        return (
            not self.connection_healthy or 
            time.time() - self.last_successful_request > 300  # 5 minutes
        )

connection_manager = OANDAConnectionManager()

async def get_session():
    """Get HTTP session with proper timeout and connection settings"""
    global session
    if session is None or session.closed:
        timeout = aiohttp.ClientTimeout(
            total=30,      # Total timeout
            connect=10,    # Connection timeout
            sock_read=20   # Socket read timeout
        )
        
        connector = aiohttp.TCPConnector(
            limit=100,                    # Total connection pool size
            limit_per_host=30,           # Per-host connection limit
            ttl_dns_cache=300,           # DNS cache TTL
            use_dns_cache=True,
            keepalive_timeout=60,        # Keep-alive timeout
            enable_cleanup_closed=True,  # Clean up closed connections
            force_close=False,           # Allow connection reuse
            ssl=False                    # For practice environment
        )
        
        session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector
        )
    return session

def initialize_oanda_client():
    """Initialize OANDA API client with enhanced error handling"""
    global oanda
    try:
        access_token = config.oanda_access_token
        if isinstance(access_token, object) and hasattr(access_token, 'get_secret_value'):
            access_token = access_token.get_secret_value()
        
        # Enhanced OANDA client initialization with custom settings
        oanda = oandapyV20.API(
            access_token=access_token,
            environment=config.oanda_environment,
            headers={
                "Connection": "keep-alive",
                "User-Agent": "institutional-trading-bot/1.0"
            }
        )
        
        # Set custom timeout for requests (if supported by oandapyV20)
        if hasattr(oanda, 'client') and hasattr(oanda.client, 'timeout'):
            oanda.client.timeout = 30
            
        logger.info(f"OANDA client initialized for {config.oanda_environment} environment")
        connection_manager.record_success()
        return True
    except Exception as e:
        logger.error(f"Failed to initialize OANDA client: {e}")
        connection_manager.record_failure()
        return False

def is_connection_error(exception):
    """Check if the exception is a connection-related error"""
    connection_errors = (
        ConnectionError,
        RemoteDisconnected,
        ProtocolError,
        Timeout,
        socket.timeout,
        socket.error,
        OSError,
        BrokerConnectionError
    )
    
    if isinstance(exception, connection_errors):
        return True
        
    # Check for specific error messages
    error_str = str(exception).lower()
    connection_indicators = [
        'connection aborted',
        'remote end closed connection',
        'connection reset',
        'timeout',
        'network is unreachable',
        'connection refused',
        'broken pipe',
        'connection timed out'
    ]
    
    return any(indicator in error_str for indicator in connection_indicators)

async def robust_oanda_request(request, max_retries: int = 5, initial_delay: float = 3.0):
    """Enhanced OANDA API request with sophisticated retry logic"""
    global oanda
    
    # Check if we should reinitialize the client
    if connection_manager.should_reinitialize():
        logger.info("Reinitializing OANDA client due to connection health issues")
        if not initialize_oanda_client():
            raise BrokerConnectionError("Failed to reinitialize OANDA client")
    
    if not oanda:
        if not initialize_oanda_client():
            raise BrokerConnectionError("OANDA client not initialized")
    
    for attempt in range(max_retries):
        try:
            logger.debug(f"OANDA request attempt {attempt + 1}/{max_retries}")
            response = oanda.request(request)
            
            # Record successful request
            connection_manager.record_success()
            return response
            
        except Exception as e:
            connection_manager.record_failure()
            
            is_conn_error = is_connection_error(e)
            is_final_attempt = attempt == max_retries - 1
            
            if is_final_attempt:
                logger.error(f"OANDA request failed after {max_retries} attempts: {e}")
                raise BrokerConnectionError(f"OANDA request failed after {max_retries} attempts: {e}")
            
            # Calculate delay with jitter for connection errors
            if is_conn_error:
                # Longer delays for connection errors
                delay = initial_delay * (2 ** attempt) + (attempt * 0.5)  # Add jitter
                logger.warning(f"OANDA connection error attempt {attempt + 1}/{max_retries}, retrying in {delay:.1f}s: {e}")
                
                # Reinitialize client for connection errors after 2nd attempt
                if attempt >= 1:
                    logger.info("Reinitializing OANDA client after connection error")
                    initialize_oanda_client()
            else:
                # Shorter delays for other errors
                delay = initial_delay * (1.5 ** attempt)
                logger.warning(f"OANDA request error attempt {attempt + 1}/{max_retries}, retrying in {delay:.1f}s: {e}")
            
            await asyncio.sleep(delay)

async def _close_position(symbol: str, account_id: str = None) -> dict:
    """Close any open position for a given symbol on OANDA for a specific account."""
    try:
        # Use provided account_id or fall back to default
        target_account_id = account_id or config.oanda_account_id
        
        # First, check what positions actually exist for this symbol
        from oandapyV20.endpoints.positions import OpenPositions
        positions_request = OpenPositions(accountID=target_account_id)
        
        try:
            positions_response = await robust_oanda_request(positions_request)
        except Exception as positions_error:
            # If we can't even check positions due to connectivity, fallback to old behavior
            logger.warning(f"[CLOSE] Could not check existing positions for {symbol} on account {target_account_id} due to error: {positions_error}")
            logger.info(f"[CLOSE] Falling back to attempting direct close for {symbol} on account {target_account_id}")
            
            # Try the original approach as fallback
            from oandapyV20.endpoints.positions import PositionClose
            request = PositionClose(
                accountID=target_account_id,
                instrument=symbol,
                data={"longUnits": "ALL", "shortUnits": "ALL"}
            )
            
            try:
                response = await robust_oanda_request(request)
                logger.info(f"[CLOSE] Fallback close successful for {symbol} on account {target_account_id}")
                return {
                    "status": "success", 
                    "message": f"Position closed successfully for {symbol} on account {target_account_id} (fallback method)",
                    "response": response,
                    "account_id": target_account_id
                }
            except Exception as close_error:
                close_error_msg = str(close_error)
                if "CLOSEOUT_POSITION_DOESNT_EXIST" in close_error_msg:
                    logger.info(f"[CLOSE] Position doesn't exist for {symbol} on account {target_account_id} (fallback confirmed)")
                    return {"status": "success", "message": f"Position for {symbol} doesn't exist on account {target_account_id}", "account_id": target_account_id}
                else:
                    raise close_error
        
        # Find positions for this specific symbol
        target_position = None
        if 'positions' in positions_response:
            for pos in positions_response['positions']:
                if pos['instrument'] == symbol:
                    target_position = pos
                    break
        
        if not target_position:
            logger.warning(f"[CLOSE] No open position found for {symbol} on account {target_account_id}")
            return {"status": "success", "message": f"No open position found for {symbol} on account {target_account_id}", "positions_closed": [], "account_id": target_account_id}
        
        # Check which side has an actual position
        long_units = float(target_position['long']['units'])
        short_units = float(target_position['short']['units'])
        
        logger.info(f"[CLOSE] Found position for {symbol} on account {target_account_id}: long_units={long_units}, short_units={short_units}")
        
        if long_units == 0 and short_units == 0:
            logger.info(f"[CLOSE] No active positions to close for {symbol} on account {target_account_id}")
            return {"status": "success", "message": f"No active positions to close for {symbol} on account {target_account_id}", "positions_closed": [], "account_id": target_account_id}
        
        # Build close data only for positions that actually exist
        close_data = {}
        if long_units != 0:
            close_data["longUnits"] = "ALL"
        if short_units != 0:
            close_data["shortUnits"] = "ALL"
        
        if not close_data:
            logger.info(f"[CLOSE] No non-zero positions to close for {symbol} on account {target_account_id}")
            return {"status": "success", "message": f"No non-zero positions to close for {symbol} on account {target_account_id}", "positions_closed": [], "account_id": target_account_id}
        
        # Execute the position close with only the positions that exist
        from oandapyV20.endpoints.positions import PositionClose
        request = PositionClose(
            accountID=target_account_id,
            instrument=symbol,
            data=close_data
        )
        
        response = await robust_oanda_request(request)
        
        # Log successful closure
        closed_positions = []
        if long_units != 0:
            closed_positions.append(f"long ({long_units} units)")
        if short_units != 0:
            closed_positions.append(f"short ({abs(short_units)} units)")
        
        logger.info(f"[CLOSE] Successfully closed position for {symbol} on account {target_account_id}: {', '.join(closed_positions)}")
        
        return {
            "status": "success", 
            "message": f"Position closed successfully for {symbol} on account {target_account_id}",
            "positions_closed": closed_positions,
            "response": response,
            "account_id": target_account_id
        }
        
    except Exception as e:
        error_msg = str(e)
        
        # Handle specific OANDA error cases more gracefully
        if "CLOSEOUT_POSITION_DOESNT_EXIST" in error_msg:
            logger.warning(f"[CLOSE] Position doesn't exist for {symbol} on account {target_account_id} (may have been closed already)")
            return {"status": "success", "message": f"Position for {symbol} doesn't exist on account {target_account_id} (may have been closed already)", "account_id": target_account_id}
        elif "CLOSEOUT_POSITION_REJECT" in error_msg:
            logger.error(f"[CLOSE] Position close rejected for {symbol} on account {target_account_id}: {error_msg}")
            return {"status": "error", "message": f"Position close rejected: {error_msg}", "account_id": target_account_id}
        else:
            logger.error(f"Error closing position for {symbol} on account {target_account_id}: {str(e)}", exc_info=True)
            return {"status": "error", "message": str(e), "account_id": target_account_id}

async def execute_trade(payload: dict) -> tuple[bool, dict]:
    """Execute trade with OANDA"""
    try:
        from utils import get_current_price, get_account_balance, get_atr
        from config import config  # Move this import to the top
        
        symbol = payload.get("symbol")
        action = payload.get("action")
        risk_percent = payload.get("risk_percent", 1.0)
        
        # Pre-trade checks
        if not symbol or not action:
            logger.error(f"Trade execution aborted: Missing symbol or action in payload: {payload}")
            return False, {"error": "Missing symbol or action in trade payload"}
            
        # Get account balance
        account_balance = await get_account_balance()
        
        # Get current price
        try:
            current_price = await get_current_price(symbol, action)
        except MarketDataUnavailableError as e:
            logger.error(f"Trade execution aborted: {e}")
            return False, {"error": str(e)}
            
        # Get stop loss (assume it's provided in payload or calculate using ATR if not)
        stop_loss = payload.get("stop_loss")
        if stop_loss is None:
            try:
                atr = await get_atr(symbol, payload.get("timeframe", "H1"))
            except MarketDataUnavailableError as e:
                logger.error(f"Trade execution aborted: {e}")
                return False, {"error": str(e)}
            stop_loss = current_price - (atr * config.atr_stop_loss_multiplier) if action.upper() == "BUY" else current_price + (atr * config.atr_stop_loss_multiplier)
        else:
            atr = None  # Will be set below if needed
            
        stop_distance = abs(current_price - stop_loss)
        if stop_distance <= 0:
            logger.error(f"Trade execution aborted: Invalid stop loss distance: {stop_distance}")
            return False, {"error": "Invalid stop loss distance"}
            
        # Use enhanced risk-based position sizing with margin checking
        position_size = calculate_simple_position_size(
            account_balance=account_balance,
            risk_percent=risk_percent,
            entry_price=current_price,
            stop_loss=stop_loss,
            symbol=symbol
        )
        
        if position_size <= 0:
            logger.error(f"Trade execution aborted: Calculated position size is zero or negative")
            return False, {"error": "Calculated position size is zero or negative"}
        
        # Round position size to OANDA requirements
        from utils import round_position_size
        raw_position_size = position_size
        position_size = round_position_size(symbol, position_size)
        
        logger.info(f"[RISK-BASED SIZING] {symbol}: Raw={raw_position_size:.2f}, Rounded={position_size}, Risk%={risk_percent:.2f}")
        
        if position_size <= 0:
            logger.error(f"Trade execution aborted: Rounded position size is zero")
            return False, {"error": "Rounded position size is zero"}
        
        min_units, max_units = get_position_size_limits(symbol)
        
        # Market impact estimation (warn at 1%, cap at 5% by default)
        is_allowed, impact_warning, impact_percent, impact_volume, impact_threshold = await check_market_impact(
            symbol, position_size, timeframe=payload.get("timeframe", "D1"), warn_threshold=1.0, cap_threshold=5.0
        )
        
        if not is_allowed:
            logger.error(f"Trade execution aborted: {impact_warning}")
            return False, {"error": impact_warning, "impact_percent": impact_percent, "impact_volume": impact_volume, "impact_threshold": impact_threshold}
            
        if impact_warning:
            logger.warning(f"Market impact warning: {impact_warning}")
            
        # Transaction cost analysis
        transaction_costs = await analyze_transaction_costs(symbol, position_size, action=action)
        logger.info(f"Transaction cost summary for {symbol}: {transaction_costs.get('summary', '')}")
        
        # Get ATR for validation (reuse if already calculated above)
        if atr is None:
            try:
                atr = await get_atr(symbol, payload.get("timeframe", "H1"))
            except MarketDataUnavailableError as e:
                logger.error(f"Trade execution aborted: {e}")
                return False, {"error": str(e)}
                
        is_valid, validation_reason = validate_trade_inputs(
            units=position_size,
            risk_percent=risk_percent,
            atr=atr,
            stop_loss_distance=stop_distance,
            min_units=min_units,
            max_units=max_units
        )
        
        if not is_valid:
            logger.error(f"Trade validation failed for {symbol}: {validation_reason}")
            return False, {"error": f"Trade validation failed: {validation_reason}", "transaction_costs": transaction_costs}
            
        logger.info(f"Trade validation passed for {symbol}: {validation_reason}")
        
        # Create OANDA order
        order_data = {
            "order": {
                "type": "MARKET",
                "instrument": symbol,
                "units": str(position_size) if action.upper() == "BUY" else str(-position_size),
                "timeInForce": "FOK"
            }
        }
        
        order_request = OrderCreate(
            accountID=config.oanda_account_id,
            data=order_data
        )
        
        response = await robust_oanda_request(order_request)
        
        if 'orderFillTransaction' in response:
            fill_info = response['orderFillTransaction']
            logger.info(
                f"Trade execution for {symbol}: "
                f"Account Balance=${account_balance:.2f}, "
                f"Risk%={risk_percent:.2f}, "
                f"Entry={current_price}, Stop={stop_loss}, "
                f"Position Size={position_size}"
            )
            return True, {
                "success": True,
                "fill_price": float(fill_info.get('price', current_price)),
                "units": abs(float(fill_info.get('units', position_size))),
                "transaction_id": fill_info.get('id'),
                "symbol": symbol,
                "action": action,
                "transaction_costs": transaction_costs
            }
        else:
            logger.error(f"Order not filled for {symbol}: {response}")
            return False, {"error": "Order not filled", "response": response, "transaction_costs": transaction_costs}
            
    except Exception as e:
        logger.error(f"Error executing trade: {e}")
        return False, {"error": str(e)}
        
def set_api_components():
    """Set component references in api module"""
    try:
        import api
        api.alert_handler = alert_handler
        api.tracker = alert_handler.position_tracker if alert_handler else None
        api.risk_manager = alert_handler.risk_manager if alert_handler else None
        api.vol_monitor = alert_handler.volatility_monitor if alert_handler else None
        api.regime_classifier = alert_handler.regime_classifier if alert_handler else None
        api.db_manager = db_manager
        api.backup_manager = backup_manager
        api.error_recovery = error_recovery
        api.notification_system = alert_handler.notification_system if alert_handler else None
        api.system_monitor = alert_handler.system_monitor if alert_handler else None
        logger.info("API components updated successfully")
    except Exception as e:
        logger.error(f"Error setting API components: {e}")

# Lifespan context for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    global alert_handler, error_recovery, db_manager, backup_manager, session
    
    startup_success = True
    startup_errors = []
    
    try:
        logger.info("🚀 Starting application initialization...")
        print("🚀 Starting application initialization...", flush=True)
        
        # Test critical environment variables
        critical_vars = ["OANDA_ACCOUNT", "OANDA_TOKEN", "DATABASE_URL"]
        missing_vars = [var for var in critical_vars if not os.getenv(var)]
        
        if missing_vars:
            error_msg = f"❌ Missing critical environment variables: {', '.join(missing_vars)}"
            logger.error(error_msg)
            print(error_msg, flush=True)
            startup_errors.append(error_msg)
            # Continue startup but log the issue
        
        # Initialize HTTP session first
        logger.info("🔗 Initializing HTTP session...")
        print("🔗 Initializing HTTP session...", flush=True)
        try:
            session = await get_session()
            logger.info("✅ HTTP session initialized")
            print("✅ HTTP session initialized", flush=True)
        except Exception as e:
            error_msg = f"❌ HTTP session initialization failed: {e}"
            logger.error(error_msg)
            print(error_msg, flush=True)
            startup_errors.append(error_msg)
            startup_success = False
        
        # Initialize OANDA client
        logger.info("🏦 Initializing OANDA client...")
        print("🏦 Initializing OANDA client...", flush=True)
        try:
            if not initialize_oanda_client():
                logger.warning("⚠️ OANDA client initialization failed - will retry on first request")
                print("⚠️ OANDA client initialization failed - will retry on first request", flush=True)
            else:
                logger.info("✅ OANDA client initialized")
                print("✅ OANDA client initialized", flush=True)
        except Exception as e:
            error_msg = f"❌ OANDA client error: {e}"
            logger.error(error_msg)
            print(error_msg, flush=True)
            startup_errors.append(error_msg)
        
        # Initialize database manager
        logger.info("🗄️ Initializing database manager...")
        print("🗄️ Initializing database manager...", flush=True)
        try:
            db_manager = PostgresDatabaseManager()
            await db_manager.initialize()
            logger.info("✅ Database manager initialized")
            print("✅ Database manager initialized", flush=True)
        except Exception as e:
            error_msg = f"❌ Database initialization failed: {e}"
            logger.error(error_msg)
            print(error_msg, flush=True)
            logger.warning("⚠️ Continuing without database persistence")
            print("⚠️ Continuing without database persistence", flush=True)
            db_manager = None
            startup_errors.append(error_msg)

        # *** INSERT HERE: Initialize 100k Bot Database ***
        logger.info("🗄️ Initializing 100k Bot database...")
        print("🗄️ Initializing 100k Bot database...", flush=True)
        bot_100k_db = None
        try:
            from database_100k import Bot100kDatabaseManager
            bot_100k_db = Bot100kDatabaseManager()
            await bot_100k_db.initialize()
            logger.info("✅ 100k Bot database manager initialized")
            print("✅ 100k Bot database manager initialized", flush=True)
        except Exception as e:
            error_msg = f"❌ 100k Bot database initialization failed: {e}"
            logger.error(error_msg)
            print(error_msg, flush=True)
            logger.warning("⚠️ Continuing without 100k bot database persistence")
            print("⚠️ Continuing without 100k bot database persistence", flush=True)
            bot_100k_db = None
            startup_errors.append(error_msg)

        # Initialize backup manager
        logger.info("💾 Initializing backup manager...")
        print("💾 Initializing backup manager...", flush=True)
        try:
            backup_manager = BackupManager()
            logger.info("✅ Backup manager initialized")
            print("✅ Backup manager initialized", flush=True)
        except Exception as e:
            error_msg = f"❌ Backup manager initialization failed: {e}"
            logger.error(error_msg)
            print(error_msg, flush=True)
            startup_errors.append(error_msg)

        # Initialize error recovery system
        logger.info("🛡️ Initializing error recovery system...")
        print("🛡️ Initializing error recovery system...", flush=True)
        try:
            error_recovery = ErrorRecoverySystem()
            logger.info("✅ Error recovery system initialized.")
            print("✅ Error recovery system initialized.", flush=True)
        except Exception as e:
            error_msg = f"❌ Error recovery system failed: {e}"
            logger.error(error_msg)
            print(error_msg, flush=True)
            startup_errors.append(error_msg)

                 # Initialize alert handler and trading components
        logger.info("📡 Initializing alert handler and trading components...")
        print("📡 Initializing alert handler and trading components...", flush=True)
        try:
            from alert_handler import AlertHandler
            alert_handler = AlertHandler(
                db_manager=db_manager,
                backup_manager=backup_manager,
                error_recovery=error_recovery,
                bot_100k_db=bot_100k_db
            )
            result = await alert_handler.initialize()
            if result:
                logger.info("✅ Alert handler and all subcomponents initialized successfully.")
                print("✅ Alert handler and all subcomponents initialized successfully.", flush=True)
            else:
                logger.warning("⚠️ Alert handler started with some issues.")
                print("⚠️ Alert handler started with some issues.", flush=True)
        except Exception as e:
            error_msg = f"❌ Alert handler initialization failed: {e}"
            logger.error(error_msg)
            print(error_msg, flush=True)
            startup_errors.append(error_msg)
            startup_success = False

        # Setup API component references
        logger.info("🔧 Setting up API component references...")
        print("🔧 Setting up API component references...", flush=True)
        setup_api_components()

        # Final startup status
        if startup_success and not startup_errors:
            logger.info("🎉 Application startup completed successfully!")
            print("🎉 Application startup completed successfully!", flush=True)
        elif startup_errors:
            logger.warning(f"⚠️ Application started with {len(startup_errors)} issues:")
            print(f"⚠️ Application started with {len(startup_errors)} issues:", flush=True)
            for error in startup_errors:
                logger.warning(f"  - {error}")
                print(f"  - {error}", flush=True)
        
        logger.info("📊 Bot is now ready to process trading signals")
        print("📊 Bot is now ready to process trading signals", flush=True)
        
        # Force flush all logs
        sys.stdout.flush()
        sys.stderr.flush()
        
        yield
        
    except Exception as e:
        error_msg = f"💥 CRITICAL STARTUP ERROR: {e}"
        logger.error(error_msg, exc_info=True)
        print(error_msg, flush=True)
        # Re-raise to prevent app from starting in bad state
        raise
    
    finally:
        # Shutdown code
        logger.info("🛑 Shutting down application...")
        print("🛑 Shutting down application...", flush=True)
        
        try:
            if session:
                await session.close()
                logger.info("✅ HTTP session closed")
                print("✅ HTTP session closed", flush=True)
        except Exception as e:
            logger.error(f"❌ Error closing HTTP session: {e}")
            print(f"❌ Error closing HTTP session: {e}", flush=True)
        
        try:
            if alert_handler:
                await alert_handler.cleanup()
                logger.info("✅ Alert handler stopped")
                print("✅ Alert handler stopped", flush=True)
        except Exception as e:
            logger.error(f"❌ Error stopping alert handler: {e}")
            print(f"❌ Error stopping alert handler: {e}", flush=True)
        
        try:
            if backup_manager:
                await backup_manager.create_backup()
                logger.info("✅ Final backup created successfully")
                print("✅ Final backup created successfully", flush=True)
        except Exception as e:
            logger.error(f"❌ Error creating final backup: {e}")
            print(f"❌ Error creating final backup: {e}", flush=True)
        
        try:
            if db_manager:
                await db_manager.close()
                logger.info("✅ Database connection closed")
                print("✅ Database connection closed", flush=True)
        except Exception as e:
            logger.error(f"❌ Error closing database connection: {e}")
            print(f"❌ Error closing database connection: {e}", flush=True)
        
        logger.info("🏁 Application shutdown complete.")
        print("🏁 Application shutdown complete.", flush=True)
        
        # Final flush
        sys.stdout.flush()
        sys.stderr.flush()

app.router.lifespan_context = lifespan

# Add a basic health check endpoint
@app.get("/health")
async def health_check():
    """Basic health check endpoint"""
    logger.info("Health check endpoint called")
    print("Health check endpoint called", flush=True)
    
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "environment": os.getenv("ENVIRONMENT", "development"),
        "message": "Trading bot is running and logging properly"
    }

@app.get("/")
async def root():
    """Root endpoint"""
    logger.info("Root endpoint called")
    print("Root endpoint called", flush=True)
    
    return {
        "message": "Trading Bot API",
        "version": "1.0.0",
        "status": "running",
        "environment": os.getenv("ENVIRONMENT", "development"),
        "endpoints": {
            "health": "/health",
            "api_status": "/api/status",
            "trading": "/tradingview"
        }
    }

def main():
    import uvicorn
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"Starting Uvicorn on {host}:{port}")
    uvicorn.run("main:app", host=host, port=port, reload=False)

if __name__ == "__main__":
    main()
