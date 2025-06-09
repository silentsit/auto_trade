import os
import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timezone

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

# Lifespan context for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    global alert_handler, error_recovery, db_manager, backup_manager
    logger.info("Starting application...")
    try:
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
        alert_handler = EnhancedAlertHandler()
        await alert_handler.start()
        logger.info("Alert handler and all subcomponents initialized.")

        yield
    finally:
        logger.info("Shutting down application...")
        # Shutdown alert handler and subcomponents
        if alert_handler:
            await alert_handler.stop()
        # Final backup before shutdown
        if backup_manager:
            await backup_manager.create_backup(include_market_data=True, compress=True)
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
        "timestamp": datetime.now(timezone.utc).isoformat()
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
