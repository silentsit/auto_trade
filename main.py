import os
import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone
import oandapyV20

# Modular imports
from config import config
from database import PostgresDatabaseManager
from alert_handler import EnhancedAlertHandler
from backup import BackupManager
from error_recovery import ErrorRecoverySystem
from api import router as api_router

# Globals for components
alert_handler = None
error_recovery = None
db_manager = None
backup_manager = None
oanda = None  # Add global OANDA client

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

# Add session management for HTTP requests
session = None

async def get_session():
    """Get or create aiohttp session"""
    global session
    if session is None:
        import aiohttp
        session = aiohttp.ClientSession()
    return session

# Initialize OANDA client
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

# Add the robust_oanda_request function here
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
                from error_recovery import BrokerConnectionError
                raise BrokerConnectionError(f"OANDA request failed after {max_retries} attempts: {e}")
            await asyncio.sleep(initial_delay * (2 ** attempt))
            logger.warning(f"OANDA request attempt {attempt + 1} failed, retrying: {e}")

# Lifespan context for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    global alert_handler, error_recovery, db_manager, backup_manager, session
    logger.info("Starting application...")
    try:
        # Initialize OANDA client first
        if not initialize_oanda_client():
            logger.error("Failed to initialize OANDA client")
            # Don't fail startup, but log the error
        
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

        # Initialize alert handler (wires up all subcomponents)
        alert_handler = EnhancedAlertHandler(db_manager=db_manager)
        startup_success = await alert_handler.start()
        if startup_success:
            logger.info("Alert handler and all subcomponents initialized successfully.")
        else:
            logger.warning("Alert handler started with some issues.")

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

# --- Orchestration-level Endpoints Only ---
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

# --- Main entrypoint for Uvicorn ---
def main():
    import uvicorn
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"Starting Uvicorn on {host}:{port}")
    uvicorn.run("main:app", host=host, port=port, reload=False)

if __name__ == "__main__":
    main()
