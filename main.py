import os
import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone
import oandapyV20
from oandapyV20.endpoints.orders import OrderCreate
from oandapyV20.endpoints.positions import PositionClose
from oandapyV20.endpoints.accounts import AccountDetails
from oandapyV20.endpoints.pricing import PricingInfo

# Modular imports
from config import config
from database import PostgresDatabaseManager
from backup import BackupManager
from error_recovery import ErrorRecoverySystem, BrokerConnectionError
from api import router as api_router
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from utils import MarketDataUnavailableError, calculate_simple_position_size, get_position_size_limits, validate_trade_inputs, check_market_impact, analyze_transaction_costs

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

async def get_session():
    """Get or create aiohttp session"""
    global session
    if session is None:
        import aiohttp
        session = aiohttp.ClientSession()
    return session

def initialize_oanda_client():
    """Initialize OANDA API client"""
    global oanda
    try:
        access_token = config.oanda_access_token
        if isinstance(access_token, object) and hasattr(access_token, 'get_secret_value'):
            access_token = access_token.get_secret_value()
        
        oanda = oandapyV20.API(
            access_token=access_token,
            environment=config.oanda_environment
        )
        logger.info(f"OANDA client initialized for {config.oanda_environment} environment")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize OANDA client: {e}")
        return False

async def robust_oanda_request(request, max_retries: int = 3, initial_delay: float = 1.0):
    """Make robust OANDA API request with retries"""
    global oanda
    if not oanda:
        if not initialize_oanda_client():
            raise Exception("OANDA client not initialized")
    
    for attempt in range(max_retries):
        try:
            response = oanda.request(request)
            return response
        except Exception as e:
            if attempt == max_retries - 1:
                raise BrokerConnectionError(f"OANDA request failed after {max_retries} attempts: {e}")
            await asyncio.sleep(initial_delay * (2 ** attempt))
            logger.warning(f"OANDA request attempt {attempt + 1} failed, retrying: {e}")

async def _close_position(symbol: str) -> dict:
    """Close any open position for a given symbol on OANDA."""
    try:
        request = PositionClose(
            accountID=config.oanda_account_id,
            instrument=symbol,
            data={"longUnits": "ALL", "shortUnits": "ALL"}
        )
        
        response = await robust_oanda_request(request)
        logger.info(f"[CLOSE] Closed position for {symbol}: {response}")
        return response
    except Exception as e:
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
            
        # Use universal position sizing
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
        
        # CRITICAL: Round position size to OANDA requirements (whole numbers)
        from utils import round_position_size
        raw_position_size = position_size
        position_size = round_position_size(symbol, position_size)
        
        logger.info(f"Position sizing for {symbol}: Raw={raw_position_size:.8f}, Rounded={position_size}")
        
        if position_size <= 0:
            logger.error(f"Trade execution aborted: Rounded position size is zero")
            return False, {"error": "Rounded position size is zero"}
        
        # Round position size to appropriate precision for OANDA
        from utils import round_position_size  # Add this import
        position_size = round_position_size(symbol, position_size)
        
        logger.info(f"Position size after rounding: {position_size} units for {symbol}")
        
        min_units, max_units = get_position_size_limits(symbol)
            
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

        logger.info(f"Position sizing for {symbol}: Balance=${account_balance:.2f}, Risk={risk_percent}%, "
        f"Entry={current_price}, Stop={stop_loss}, Raw Size={position_size:.8f}")
        
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
        # Initialize OANDA client first
        if not initialize_oanda_client():
            logger.error("Failed to initialize OANDA client")

        # Initialize database manager
        db_manager = PostgresDatabaseManager()
        await db_manager.initialize()
        logger.info("Database manager initialized.")

        # Initialize backup manager
        backup_manager = BackupManager(db_manager=db_manager)
        logger.info("Backup manager initialized.")

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
            except Exception as e:
                logger.error(f"Error creating final backup: {e}")
                
        # Close database connection
        if db_manager:
            await db_manager.close()
            
        logger.info("Application shutdown complete.")

app.router.lifespan_context = lifespan

@app.get("/", tags=["system"])
async def root():
    return {
        "message": "Enhanced Trading System API",
        "version": "1.0.0",
        "status": "running",
        "docs": "/api/docs"
    }

@app.head("/", include_in_schema=False)
async def root_head():
    return {}

@app.get("/api/health", tags=["system"])
async def health_check():
    if not db_manager:
        return {"status": "error", "message": "DB not initialized"}
    if not alert_handler:
        return {"status": "error", "message": "Alert handler not initialized"}
    return {
        "status": "ok",
        "version": "1.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "oanda_connected": oanda is not None
    }

def main():
    import uvicorn
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"Starting Uvicorn on {host}:{port}")
    uvicorn.run("main:app", host=host, port=port, reload=False)

if __name__ == "__main__":
    main()
