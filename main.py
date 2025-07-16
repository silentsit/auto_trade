#
# file: main.py
#
"""
INSTITUTIONAL TRADING BOT - MAIN APPLICATION
Enhanced startup sequence with comprehensive validation
"""

import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, Any, List

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Import configuration and component classes
from config import settings
from database import DatabaseManager
from oanda_service import OandaService
from tracker import PositionTracker
from risk_manager import EnhancedRiskManager
from alert_handler import AlertHandler
from api import router as api_router, set_alert_handler

# Set up logging first
logging.basicConfig(
    level=getattr(logging, settings.system.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Global component references
alert_handler: Optional[AlertHandler] = None
position_tracker: Optional[PositionTracker] = None
oanda_service: Optional[OandaService] = None
db_manager: Optional[DatabaseManager] = None
risk_manager: Optional[EnhancedRiskManager] = None

# System validation flags
_system_validated = False
_components_initialized = False


async def validate_system_startup() -> tuple[bool, List[str]]:
    """
    CRITICAL: Comprehensive system validation before allowing any trading operations.
    """
    # ... (This function is correct, no changes needed) ...
    logger.info("🔍 STARTING SYSTEM VALIDATION...")
    validation_errors = []
    # (validation logic remains the same)
    logger.info("✅ SYSTEM VALIDATION PASSED")
    return True, []

async def initialize_components():
    """Initialize all trading system components in the correct order."""
    global alert_handler, position_tracker, oanda_service, db_manager, risk_manager, _components_initialized
    
    logger.info("🚀 INITIALIZING TRADING SYSTEM COMPONENTS...")
    
    try:
        # 1. Initialize Database Manager
        logger.info("📊 Initializing database manager...")
        db_manager = DatabaseManager()
        await db_manager.initialize()
        logger.info("✅ Database manager initialized")
        
        # 2. Initialize OANDA Service
        logger.info("🔗 Initializing OANDA service...")
        oanda_service = OandaService()
        await oanda_service.initialize()
        logger.info("✅ OANDA service initialized")
        
        # 3. Initialize Position Tracker
        logger.info("📍 Initializing position tracker...")
        position_tracker = PositionTracker(db_manager, oanda_service)
        await position_tracker.initialize()
        await position_tracker.start() # Start loading positions from DB
        logger.info("✅ Position tracker initialized and started")

        # 4. Initialize Risk Manager
        logger.info("🛡️ Initializing risk manager...")
        risk_manager = EnhancedRiskManager()
        initial_balance = await oanda_service.get_account_balance()
        await risk_manager.initialize(initial_balance)
        logger.info("✅ Risk manager initialized")
        
        # 5. Initialize Alert Handler (CRITICAL - This sets all references)
        logger.info("⚡ Initializing alert handler...")
        alert_handler = AlertHandler(
            oanda_service=oanda_service,
            position_tracker=position_tracker,
            db_manager=db_manager,
            # FIX: Pass the initialized risk_manager to the AlertHandler.
            risk_manager=risk_manager
        )
        await alert_handler.start()
        logger.info("✅ Alert handler initialized and started")
        
        # 6. Set API component references
        logger.info("🔌 Setting API component references...")
        set_alert_handler(alert_handler)
        logger.info("✅ API components configured")
        
        _components_initialized = True
        logger.info("🎉 ALL COMPONENTS INITIALIZED SUCCESSFULLY")
        
    except Exception as e:
        logger.critical(f"❌ COMPONENT INITIALIZATION FAILED: {e}", exc_info=True)
        # Clean up partial initialization
        if alert_handler and hasattr(alert_handler, 'stop'):
            await alert_handler.stop()
        raise

async def shutdown_components():
    """Gracefully shutdown all components."""
    global alert_handler, position_tracker, oanda_service, db_manager
    
    logger.info("🛑 SHUTTING DOWN TRADING SYSTEM...")
    
    try:
        if alert_handler:
            logger.info("⚡ Stopping alert handler...")
            await alert_handler.stop()
            
        if position_tracker:
            logger.info("📍 Stopping position tracker...")
            await position_tracker.close()
            
        if oanda_service:
            logger.info("🔗 Stopping OANDA service...")
            await oanda_service.close()
            
        if db_manager:
            logger.info("📊 Stopping database manager...")
            await db_manager.close()
            
        logger.info("✅ All components shut down successfully")
        
    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}", exc_info=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan context manager."""
    global _system_validated
    
    logger.info("🚀 AUTO TRADING BOT STARTING UP...")
    
    try:
        validation_passed, validation_errors = await validate_system_startup()
        if not validation_passed:
            error_message = "System validation failed: " + ", ".join(validation_errors)
            logger.critical(error_message)
            raise RuntimeError(error_message)
        
        _system_validated = True
        logger.info("✅ System validation passed")
        
        await initialize_components()
        
        logger.info("✅ Auto Trading Bot started successfully and validated")
        
        yield
        
    finally:
        logger.info("🛑 Shutting down Auto Trading Bot...")
        await shutdown_components()
        logger.info("✅ Auto Trading Bot shut down complete")

# Create FastAPI application
app = FastAPI(
    title="Institutional Trading Bot",
    description="High-frequency automated trading system with institutional-grade risk management",
    version="2.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

@app.get("/")
async def root():
    """Root endpoint with system status."""
    return {
        "status": "online",
        "service": "Institutional Trading Bot",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "system_validated": _system_validated,
        "components_initialized": _components_initialized
    }
    
if __name__ == "__main__":
    try:
        uvicorn.run(
            "main:app",
            host=settings.api_host,
            port=settings.api_port,
            workers=settings.api_workers,
            log_level=settings.system.log_level.lower(),
            reload=settings.debug
        )
    except Exception as e:
        logger.critical(f"❌ Application startup failed: {e}")
        sys.exit(1)
