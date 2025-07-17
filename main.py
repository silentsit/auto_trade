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
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Import configuration
from config import settings, get_oanda_config, get_trading_config

# Set up logging first
logging.basicConfig(
    level=getattr(logging, settings.system.log_level),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Global component references
alert_handler: Optional['AlertHandler'] = None
position_tracker: Optional['PositionTracker'] = None
oanda_service: Optional['OandaService'] = None
db_manager: Optional['DatabaseManager'] = None
risk_manager: Optional['EnhancedRiskManager'] = None

# System validation flags
_system_validated = False
_components_initialized = False

async def validate_system_startup() -> tuple[bool, List[str]]:
    """
    CRITICAL: Comprehensive system validation before allowing any trading operations.
    This prevents the production errors we've been seeing.
    """
    logger.info("🔍 STARTING SYSTEM VALIDATION...")
    
    validation_errors = []
    validation_warnings = []
    
    # 1. Environment Variable Validation
    logger.info("📋 Validating environment variables...")
    required_env_vars = {
        'OANDA_ACCESS_TOKEN': ['OANDA_ACCESS_TOKEN', 'OANDA_TOKEN', 'OANDA_API_TOKEN', 'ACCESS_TOKEN'],
        'OANDA_ACCOUNT_ID': ['OANDA_ACCOUNT_ID', 'OANDA_ACCOUNT'],
        'DATABASE_URL': ['DATABASE_URL']
    }
    
    for required_var, possible_names in required_env_vars.items():
        found_var = None
        for var_name in possible_names:
            if os.getenv(var_name):
                found_var = var_name
                break
                
        if not found_var:
            validation_errors.append(f"❌ Missing environment variable: {required_var} (tried: {', '.join(possible_names)})")
        else:
            logger.info(f"✅ Found {required_var} via {found_var}")
    
    # 2. OANDA Configuration Validation
    logger.info("🔧 Validating OANDA configuration...")
    oanda_config = get_oanda_config()
    
    if not oanda_config.access_token:
        validation_errors.append("❌ OANDA access token not configured")
    else:
        token_preview = f"{oanda_config.access_token[:8]}***{oanda_config.access_token[-4:]}"
        logger.info(f"✅ OANDA token configured: {token_preview}")
        
    if not oanda_config.account_id:
        validation_errors.append("❌ OANDA account ID not configured")
    else:
        logger.info(f"✅ OANDA account ID: {oanda_config.account_id}")
        
    if oanda_config.environment not in ["practice", "live"]:
        validation_errors.append(f"❌ Invalid OANDA environment: {oanda_config.environment}")
    else:
        logger.info(f"✅ OANDA environment: {oanda_config.environment}")
    
    # 3. Trading Configuration Validation
    logger.info("📊 Validating trading configuration...")
    trading_config = get_trading_config()
    
    if trading_config.max_risk_per_trade <= 0 or trading_config.max_risk_per_trade > 20:
        validation_warnings.append(f"⚠️ High risk per trade: {trading_config.max_risk_per_trade}%")
    
    # 4. Database Configuration
    logger.info("🗄️ Validating database configuration...")
    if not settings.database.url:
        validation_errors.append("❌ Database URL not configured")
    else:
        # Mask password in URL for logging
        db_url_safe = settings.database.url
        if "@" in db_url_safe:
            parts = db_url_safe.split("://")
            if len(parts) == 2:
                protocol = parts[0]
                rest = parts[1]
                if "@" in rest:
                    creds, host_db = rest.split("@", 1)
                    if ":" in creds:
                        user, _ = creds.split(":", 1)
                        db_url_safe = f"{protocol}://{user}:***@{host_db}"
                    
        logger.info(f"✅ Database URL configured: {db_url_safe}")
    
    # 5. System Resources Validation
    logger.info("💾 Validating system resources...")
    try:
        import psutil
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        if memory.percent > 90:
            validation_warnings.append(f"⚠️ High memory usage: {memory.percent}%")
        if disk.percent > 90:
            validation_warnings.append(f"⚠️ High disk usage: {disk.percent}%")
            
        logger.info(f"✅ Memory: {memory.percent}%, Disk: {disk.percent}%")
    except ImportError:
        logger.warning("⚠️ psutil not available - skipping resource checks")
    
    # 6. Network Connectivity Test
    logger.info("🌐 Testing network connectivity...")
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            # Test OANDA API connectivity
            oanda_url = settings.get_oanda_base_url()
            headers = {"Authorization": f"Bearer {oanda_config.access_token}"}
            
            async with session.get(f"{oanda_url}/v3/accounts", headers=headers, timeout=10) as response:
                if response.status == 200:
                    logger.info("✅ OANDA API connectivity successful")
                elif response.status == 401:
                    validation_errors.append("❌ OANDA authentication failed - check access token")
                else:
                    validation_warnings.append(f"⚠️ OANDA API returned status {response.status}")
                    
    except Exception as e:
        validation_warnings.append(f"⚠️ Network connectivity test failed: {str(e)}")
    
    # Report validation results
    if validation_errors:
        logger.error("❌ SYSTEM VALIDATION FAILED")
        for error in validation_errors:
            logger.error(f"   {error}")
    else:
        logger.info("✅ SYSTEM VALIDATION PASSED")
        
    if validation_warnings:
        logger.warning("⚠️ SYSTEM WARNINGS:")
        for warning in validation_warnings:
            logger.warning(f"   {warning}")
    
    validation_passed = len(validation_errors) == 0
    
    return validation_passed, validation_errors

async def initialize_components():
    """Initialize all trading system components in the correct order"""
    global alert_handler, position_tracker, oanda_service, db_manager, _components_initialized
    
    logger.info("🚀 INITIALIZING TRADING SYSTEM COMPONENTS...")
    
    try:
        # 1. Initialize Database Manager
        logger.info("📊 Initializing database manager...")
        from database import DatabaseManager
        db_manager = DatabaseManager()
        await db_manager.initialize()
        logger.info("✅ Database manager initialized")
        
        # 2. Initialize OANDA Service
        logger.info("🔗 Initializing OANDA service...")
        from oanda_service import OandaService
        oanda_service = OandaService()
        await oanda_service.initialize()
        logger.info("✅ OANDA service initialized")
        
        # 3. Initialize Position Tracker
        logger.info("📍 Initializing position tracker...")
        from tracker import PositionTracker
        position_tracker = PositionTracker(db_manager, oanda_service)
        await position_tracker.initialize()
        logger.info("✅ Position tracker initialized")
        
        # 4. Initialize Risk Manager
        logger.info("🛡️ Initializing risk manager...")
        from risk_manager import EnhancedRiskManager
        risk_manager = EnhancedRiskManager()
        
        # Get account balance from OANDA service
        account_balance = await oanda_service.get_account_balance()
        await risk_manager.initialize(account_balance)
        logger.info("✅ Risk manager initialized")
        
        # 5. Initialize Alert Handler (CRITICAL - This sets position_tracker reference)
        logger.info("⚡ Initializing alert handler...")
        from alert_handler import AlertHandler
        alert_handler = AlertHandler(
            oanda_service=oanda_service,
            position_tracker=position_tracker,
            db_manager=db_manager,
            risk_manager=risk_manager
        )
        
        # CRITICAL: Ensure position_tracker is properly set
        if not alert_handler.position_tracker:
            logger.error("❌ CRITICAL: Alert handler position_tracker is None after initialization!")
            raise RuntimeError("Position tracker not properly set in alert handler")
            
        logger.info("✅ Alert handler initialized with position_tracker")
        
        # 6. Start the alert handler
        logger.info("🎯 Starting alert handler...")
        await alert_handler.start()
        
        # VALIDATION: Ensure alert handler is started and components are ready
        if not alert_handler._started:
            raise RuntimeError("Alert handler failed to start properly")
            
        if not alert_handler.position_tracker:
            raise RuntimeError("Alert handler position_tracker became None after start")
            
        logger.info("✅ Alert handler started successfully")
        
        # 7. Set API component references
        logger.info("🔌 Setting API component references...")
        from api import set_alert_handler
        set_alert_handler(alert_handler)
        logger.info("✅ API components configured")
        
        _components_initialized = True
        logger.info("🎉 ALL COMPONENTS INITIALIZED SUCCESSFULLY")
        
    except Exception as e:
        logger.error(f"❌ COMPONENT INITIALIZATION FAILED: {e}")
        # Clean up partial initialization
        if alert_handler:
            try:
                await alert_handler.stop()
            except:
                pass
        raise

async def shutdown_components():
    """Gracefully shutdown all components"""
    global alert_handler, position_tracker, oanda_service, db_manager
    
    logger.info("🛑 SHUTTING DOWN TRADING SYSTEM...")
    
    try:
        if alert_handler:
            logger.info("⚡ Stopping alert handler...")
            await alert_handler.stop()
            alert_handler = None
            
        if position_tracker:
            logger.info("📍 Stopping position tracker...")
            await position_tracker.close()
            position_tracker = None
            
        if oanda_service:
            logger.info("🔗 Stopping OANDA service...")
            await oanda_service.close()
            oanda_service = None
            
        if db_manager:
            logger.info("📊 Stopping database manager...")
            await db_manager.close()
            db_manager = None
            
        logger.info("✅ All components shut down successfully")
        
    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan context manager"""
    global _system_validated
    
    logger.info("🚀 AUTO TRADING BOT STARTING UP...")
    
    try:
        # CRITICAL SYSTEM VALIDATION BEFORE ALLOWING TRADING
        logger.info("🔍 Running comprehensive system validation...")
        validation_passed, validation_errors = await validate_system_startup()
        
        if not validation_passed:
            logger.error("❌ SYSTEM VALIDATION FAILED - TRADING DISABLED")
            for error in validation_errors:
                logger.error(f"   {error}")
            raise RuntimeError("System validation failed - fix errors before starting")
        
        _system_validated = True
        logger.info("✅ System validation passed")
        
        # Initialize all components
        await initialize_components()
        
        logger.info("✅ Auto Trading Bot started successfully and validated")
        
        yield
        
    except Exception as e:
        logger.error(f"❌ STARTUP FAILED: {e}")
        raise
    finally:
        # Shutdown
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

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
from api import router
app.include_router(router)

# Health check endpoint (always available)
@app.get("/")
async def root():
    """Root endpoint with system status"""
    return {
        "status": "online",
        "service": "Institutional Trading Bot",
        "version": "2.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "system_validated": _system_validated,
        "components_initialized": _components_initialized
    }

@app.get("/startup-status")
async def startup_status():
    """Detailed startup status endpoint"""
    return {
        "system_validated": _system_validated,
        "components_initialized": _components_initialized,
        "alert_handler_ready": alert_handler is not None and getattr(alert_handler, '_started', False),
        "position_tracker_ready": position_tracker is not None,
        "oanda_service_ready": oanda_service is not None,
        "db_manager_ready": db_manager is not None,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# Error handlers
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions gracefully"""
    logger.warning(f"HTTP {exc.status_code}: {exc.detail}")
    return {
        "status": "error",
        "code": exc.status_code,
        "message": exc.detail,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle unexpected exceptions"""
    logger.error(f"Unexpected error: {exc}", exc_info=True)
    return {
        "status": "error",
        "code": 500,
        "message": "Internal server error",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

def set_api_components():
    """Set API component references - called after initialization"""
    global alert_handler
    if alert_handler:
        from api import set_alert_handler
        set_alert_handler(alert_handler)
        logger.info("✅ API components configured")

if __name__ == "__main__":
    # Development server
    try:
        uvicorn.run(
            "main:app",
            host=settings.api_host,
            port=settings.api_port,
            workers=settings.api_workers,
            log_level=settings.system.log_level.lower(),
            reload=settings.debug
        )
    except KeyboardInterrupt:
        logger.info("🛑 Application stopped by user")
    except Exception as e:
        logger.error(f"❌ Application startup failed: {e}")
        sys.exit(1)
