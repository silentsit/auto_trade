import os
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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("trading_system.log"),
    ],
)
logger = logging.getLogger("trading_system")

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

async def _close_position(symbol: str) -> dict:
    """Close any open position for a given symbol on OANDA."""
    try:
        # First, check what positions actually exist for this symbol
        from oandapyV20.endpoints.positions import OpenPositions
        positions_request = OpenPositions(accountID=config.oanda_account_id)
        
        try:
            positions_response = await robust_oanda_request(positions_request)
        except Exception as positions_error:
            # If we can't even check positions due to connectivity, fallback to old behavior
            logger.warning(f"[CLOSE] Could not check existing positions for {symbol} due to error: {positions_error}")
            logger.info(f"[CLOSE] Falling back to attempting direct close for {symbol}")
            
            # Try the original approach as fallback
            from oandapyV20.endpoints.positions import PositionClose
            request = PositionClose(
                accountID=config.oanda_account_id,
                instrument=symbol,
                data={"longUnits": "ALL", "shortUnits": "ALL"}
            )
            
            try:
                response = await robust_oanda_request(request)
                logger.info(f"[CLOSE] Fallback close successful for {symbol}")
                return {
                    "status": "success", 
                    "message": f"Position closed successfully for {symbol} (fallback method)",
                    "response": response
                }
            except Exception as close_error:
                close_error_msg = str(close_error)
                if "CLOSEOUT_POSITION_DOESNT_EXIST" in close_error_msg:
                    logger.info(f"[CLOSE] Position doesn't exist for {symbol} (fallback confirmed)")
                    return {"status": "success", "message": f"Position for {symbol} doesn't exist"}
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
            logger.warning(f"[CLOSE] No open position found for {symbol}")
            return {"status": "success", "message": f"No open position found for {symbol}", "positions_closed": []}
        
        # Check which side has an actual position
        long_units = float(target_position['long']['units'])
        short_units = float(target_position['short']['units'])
        
        logger.info(f"[CLOSE] Found position for {symbol}: long_units={long_units}, short_units={short_units}")
        
        if long_units == 0 and short_units == 0:
            logger.info(f"[CLOSE] No active positions to close for {symbol}")
            return {"status": "success", "message": f"No active positions to close for {symbol}", "positions_closed": []}
        
        # Build close data only for positions that actually exist
        close_data = {}
        if long_units != 0:
            close_data["longUnits"] = "ALL"
        if short_units != 0:
            close_data["shortUnits"] = "ALL"
        
        if not close_data:
            logger.info(f"[CLOSE] No non-zero positions to close for {symbol}")
            return {"status": "success", "message": f"No non-zero positions to close for {symbol}", "positions_closed": []}
        
        # Execute the position close with only the positions that exist
        from oandapyV20.endpoints.positions import PositionClose
        request = PositionClose(
            accountID=config.oanda_account_id,
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
        
        logger.info(f"[CLOSE] Successfully closed position for {symbol}: {', '.join(closed_positions)}")
        
        return {
            "status": "success", 
            "message": f"Position closed successfully for {symbol}",
            "positions_closed": closed_positions,
            "response": response
        }
        
    except Exception as e:
        error_msg = str(e)
        
        # Handle specific OANDA error cases more gracefully
        if "CLOSEOUT_POSITION_DOESNT_EXIST" in error_msg:
            logger.warning(f"[CLOSE] Position doesn't exist for {symbol} (may have been closed already)")
            return {"status": "success", "message": f"Position for {symbol} doesn't exist (may have been closed already)"}
        elif "CLOSEOUT_POSITION_REJECT" in error_msg:
            logger.error(f"[CLOSE] Position close rejected for {symbol}: {error_msg}")
            return {"status": "error", "message": f"Position close rejected: {error_msg}"}
        else:
            logger.error(f"Error closing position for {symbol}: {str(e)}", exc_info=True)
            return {"status": "error", "message": str(e)}

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
    logger.info("Starting application...")
    try:
        # Initialize HTTP session first
        session = await get_session()
        logger.info("HTTP session initialized")
        
        # Initialize OANDA client
        if not initialize_oanda_client():
            logger.warning("OANDA client initialization failed - will retry on first request")
        
        # Initialize database manager
        try:
            db_manager = PostgresDatabaseManager()
            await db_manager.initialize()
            logger.info("Database manager initialized")
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            logger.warning("Continuing without database persistence")
            db_manager = None

        # Initialize backup manager
        try:
            backup_manager = BackupManager(db_manager=db_manager)
            logger.info("Backup manager initialized")
        except Exception as e:
            logger.error(f"Backup manager initialization failed: {e}")
            backup_manager = None

        # Initialize error recovery system
        error_recovery = ErrorRecoverySystem()
        logger.info("Error recovery system initialized.")

        # Import and initialize alert handler after other components
        from alert_handler import EnhancedAlertHandler
        alert_handler = EnhancedAlertHandler(db_manager=db_manager)
        startup_success = await alert_handler.start()
        if startup_success:
            logger.info("Alert handler and all subcomponents initialized successfully.")
        else:
            logger.warning("Alert handler started with some issues.")

        # Set API component references
        set_api_components()

        yield
    finally:
        logger.info("Shutting down application...")
        
        # Close HTTP session
        if session:
            await session.close()
            
        # Shutdown alert handler and subcomponents
        if alert_handler:
            await alert_handler.stop()
            
        # Final backup before shutdown
        if backup_manager:
            try:
                await backup_manager.create_backup(include_market_data=True, compress=True)
                logger.info("Final backup created successfully")
            except Exception as e:
                logger.error(f"Error creating final backup: {e}")
                
        # Close database connection
        if db_manager:
            try:
                await db_manager.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")
            
        logger.info("Application shutdown complete.")

app.router.lifespan_context = lifespan

@app.get("/", tags=["system"])
async def root():
    return {
        "message": "Enhanced Trading System API",
        "version": "1.0.0",
        "status": "running",
        "docs": "/api/docs",
        "connection_health": connection_manager.connection_healthy,
        "last_successful_request": connection_manager.last_successful_request
    }

@app.head("/", include_in_schema=False)
async def root_head():
    return {}

@app.get("/api/health", tags=["system"])
async def health_check():
    health_status = "ok"
    issues = []
    
    if not alert_handler:
        health_status = "degraded"
        issues.append("Alert handler not initialized")
    
    if not db_manager:
        health_status = "degraded"
        issues.append("Database not available")
    
    if not oanda:
        health_status = "degraded" 
        issues.append("OANDA client not connected")
    
    return {
        "status": health_status,
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "oanda_connected": oanda is not None,
        "database_connected": db_manager is not None,
        "connection_health": connection_manager.connection_healthy,
        "last_successful_request": connection_manager.last_successful_request,
        "issues": issues if issues else None
    }

def main():
    import uvicorn
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"Starting Uvicorn on {host}:{port}")
    uvicorn.run("main:app", host=host, port=port, reload=False)

if __name__ == "__main__":
    main()
