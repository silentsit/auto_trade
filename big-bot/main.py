import os
import logging
import asyncio
import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone
import oandapyV20
from oandapyV20.endpoints.orders import OrderCreate
from oandapyV20.endpoints.positions import PositionClose
from oandapyV20.endpoints.accounts import AccountDetails
from oandapyV20.endpoints.pricing import PricingInfo

# Enhanced logging setup for Render
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),  # Force stdout
    ],
    force=True  # Override existing loggers
)

# Ensure uvicorn logs to stdout too
uvicorn_logger = logging.getLogger("uvicorn")
uvicorn_logger.setLevel(logging.INFO)

logger = logging.getLogger("trading_system")
logger.info("Logging configured for Render deployment")

# Debug environment variables before importing config
logger.info("=== PRE-CONFIG ENVIRONMENT DEBUG ===")
critical_env_vars = ['DATABASE_URL', 'OANDA_ACCOUNT_ID', 'OANDA_ACCESS_TOKEN']
for var in critical_env_vars:
    value = os.getenv(var)
    if value:
        if 'URL' in var or 'TOKEN' in var:
            logger.info(f"{var}: SET (length: {len(value)})")
            if '<' in value or '>' in value:
                logger.error(f"{var} contains placeholder values!")
        else:
            logger.info(f"{var}: {value}")
    else:
        logger.error(f"{var}: NOT SET")
logger.info("=" * 35)

# Now import config after environment debug
from config import config
from database import PostgresDatabaseManager
from backup import BackupManager
from error_recovery import ErrorRecoverySystem, BrokerConnectionError
from api import router as api_router

# Globals for components
alert_handler = None
error_recovery = None
db_manager = None
backup_manager = None
oanda = None
session = None

# FastAPI app setup
app = FastAPI(
    title="Enhanced Trading System API",
    description="Institutional-grade trading system with advanced risk management",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

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
        
        if not access_token:
            logger.error("OANDA access token is empty")
            return False
        
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
        from utils import get_current_price, get_account_balance
        
        symbol = payload.get("symbol")
        action = payload.get("action")
        risk_percent = payload.get("risk_percent", 1.0)
        
        # Get account balance
        account_balance = await get_account_balance()
        
        # Get current price
        current_price = await get_current_price(symbol, action)
        
        # Calculate position size based on risk
        risk_amount = account_balance * (risk_percent / 100.0)
        position_size = int(risk_amount / current_price)
        
        if position_size <= 0:
            return False, {"error": "Calculated position size is zero or negative"}
        
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
            return True, {
                "success": True,
                "fill_price": float(fill_info.get('price', current_price)),
                "units": abs(int(fill_info.get('units', position_size))),
                "transaction_id": fill_info.get('id'),
                "symbol": symbol,
                "action": action
            }
        else:
            return False, {"error": "Order not filled", "response": response}
            
    except Exception as e:
        logger.error(f"Error executing trade: {e}")
        return False, {"error": str(e)}

# Lifespan context for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    global alert_handler, error_recovery, db_manager, backup_manager, session
    logger.info("Starting application...")
    
    try:
        # Log final config state
        logger.info("=== CONFIG LOADED STATE ===")
        logger.info(f"Database URL set: {'Yes' if config.database_url else 'No'}")
        logger.info(f"OANDA Account ID: {config.oanda_account_id}")
        logger.info(f"OANDA Environment: {config.oanda_environment}")
        logger.info("=" * 30)
        
        # Initialize OANDA client first
        if not initialize_oanda_client():
            logger.error("Failed to initialize OANDA client - continuing anyway")

        # Initialize database manager with explicit error handling
        try:
            logger.info("Initializing database manager...")
            db_manager = PostgresDatabaseManager()
            await db_manager.initialize()
            logger.info("Database manager initialized successfully.")
        except Exception as db_error:
            logger.error(f"Database initialization failed: {db_error}")
            logger.error("Application cannot continue without database")
            raise

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

        yield
        
    except Exception as startup_error:
        logger.error(f"CRITICAL: Application startup failed: {startup_error}", exc_info=True)
        raise
        
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
