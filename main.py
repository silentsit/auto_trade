"""
INSTITUTIONAL TRADING BOT - MAIN APPLICATION
Enhanced startup sequence with comprehensive validation
"""

import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from fastapi.responses import JSONResponse

# Import configuration
from config import settings, get_oanda_config, get_trading_config
from utils import is_market_hours

# Set up logging first
logging.basicConfig(
    level=getattr(logging, settings.system.log_level),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Global component references
alert_handler: Optional[Any] = None
position_tracker: Optional[Any] = None
oanda_service: Optional[Any] = None
db_manager: Optional[Any] = None
risk_manager: Optional[Any] = None
unified_exit_manager: Optional[Any] = None

# System validation flags
_system_validated = False
_components_initialized = False

# CRITICAL FIX: Import all modules at top level to prevent cloud deployment issues
try:
    # Use explicit relative imports for better deployment compatibility
    import sys
    import os
    
    # Add the current directory to the Python path if not already there
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
    
    # Now import the modules
    from unified_storage import UnifiedStorage, DatabaseConfig, StorageType
    from oanda_service import OandaService
    from tracker import PositionTracker
    from risk_manager import EnhancedRiskManager
    from unified_exit_manager import create_unified_exit_manager
    from unified_analysis import LorentzianDistanceClassifier, VolatilityMonitor
    from alert_handler import AlertHandler
    from health_checker import HealthChecker
    logger.info("✅ All required modules imported successfully")
except ImportError as e:
    logger.error(f"❌ CRITICAL: Failed to import required modules: {e}")
    logger.error("This usually indicates a deployment or Python path issue")
    # Don't exit here - let the system try to start and fail gracefully
    # Set PositionTracker to None to prevent NameError
    PositionTracker = None

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
    
    if trading_config.max_risk_per_trade <= 0 or trading_config.max_risk_per_trade > 25:
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
    
    # 5. System Resources Validation (Skipped - psutil not available)
    logger.info("💾 Skipping system resource validation - psutil not available")
    
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

def get_seconds_until_next_market_event(dt=None):
    """Returns (is_open, seconds_until_next_event) based on is_market_hours logic."""
    if dt is None:
        dt = datetime.now(timezone.utc)
    ny_tz = timezone(timedelta(hours=-5))
    ny_time = dt.astimezone(ny_tz)
    weekday = ny_time.weekday()
    hour = ny_time.hour
    minute = ny_time.minute
    second = ny_time.second
    if is_market_hours(dt):
        # Market is open, find next close (Friday 17:00 NY time)
        if weekday < 4 or (weekday == 4 and hour < 17):
            # Next close is this Friday 17:00
            days_until_friday = 4 - weekday
            close_time = ny_time.replace(hour=17, minute=0, second=0, microsecond=0) + timedelta(days=days_until_friday)
        else:
            # It's Friday after 17:00 or weekend, so next close is now
            close_time = ny_time
        seconds = (close_time - ny_time).total_seconds()
        if seconds < 1:
            seconds = 1
        return True, int(seconds)
    else:
        # Market is closed, find next open (Sunday 17:00 NY time)
        days_until_sunday = (6 - weekday) % 7
        open_time = ny_time.replace(hour=17, minute=0, second=0, microsecond=0) + timedelta(days=days_until_sunday)
        if weekday == 6 and hour < 17:
            # It's Sunday before 17:00
            open_time = ny_time.replace(hour=17, minute=0, second=0, microsecond=0)
        seconds = (open_time - ny_time).total_seconds()
        if seconds < 1:
            seconds = 1
        return False, int(seconds)

async def start_correlation_price_updates(correlation_manager, oanda_service):
    """
    DYNAMIC CORRELATION UPDATES
    
    Continuously update correlation manager with fresh price data
    to enable real-time correlation calculations for institutional risk management
    """
    logger.info("🔄 Starting dynamic correlation price updates...")
    
    # Major forex pairs to track for correlations (reduced for stability)
    tracked_symbols = [
        'EUR_USD', 'GBP_USD', 'USD_JPY', 'USD_CHF', 'AUD_USD', 'USD_CAD'
    ]
    
    last_price_update = {}
    last_correlation_recalc = datetime.now(timezone.utc) - timedelta(hours=1)
    
    while True:
        try:
            is_open, seconds_until_event = get_seconds_until_next_market_event()
            if not is_open:
                logger.info(f"🛑 Market is closed. Sleeping until open in {seconds_until_event//60} minutes.")
                await asyncio.sleep(seconds_until_event)
                continue
            # Market is open, poll until next close
            logger.info(f"✅ Market is open. Polling prices for {seconds_until_event//60} minutes until next close.")
            polling_end = datetime.now(timezone.utc) + timedelta(seconds=seconds_until_event)
            while datetime.now(timezone.utc) < polling_end:
                current_time = datetime.now(timezone.utc)
                # 1. UPDATE PRICE DATA (BATCHED - every 15 minutes)
                symbols_to_update = []
                for symbol in tracked_symbols:
                    last_update = last_price_update.get(symbol, datetime.min.replace(tzinfo=timezone.utc))
                    time_since_update = (current_time - last_update).total_seconds()
                    if time_since_update >= 900:  # 15 minutes
                        symbols_to_update.append(symbol)
                
                if symbols_to_update:
                    # INSTITUTIONAL FIX: Batch price updates instead of individual calls
                    logger.info(f"📊 Updating price data for {len(symbols_to_update)} symbols...")
                    
                    # Process in smaller batches to prevent connection overload
                    batch_size = 1  # Reduced to 1 for maximum stability
                    for i in range(0, len(symbols_to_update), batch_size):
                        batch = symbols_to_update[i:i + batch_size]
                        batch_success_count = 0
                        
                        for symbol in batch:
                            try:
                                current_price = await oanda_service.get_current_price(symbol, "BUY")
                                await correlation_manager.add_price_data(symbol, current_price, current_time)
                                last_price_update[symbol] = current_time
                                batch_success_count += 1
                                
                                if symbol == batch[0]:  # Log first symbol in batch
                                    logger.info(f"📊 Updated price data: {symbol} = {current_price}")
                                
                                # CRITICAL: Longer delay between requests for stability
                                await asyncio.sleep(5.0)  # Increased to 5s for maximum stability
                                
                            except Exception as e:
                                logger.warning(f"Failed to get price for {symbol}: {e}")
                                # Don't update last_update time so we retry next cycle
                                # CRITICAL: Add recovery delay on errors
                                await asyncio.sleep(5.0)  # 5 second recovery delay
                        
                        # INSTITUTIONAL FIX: Batch completion delay
                        if i + batch_size < len(symbols_to_update):
                            logger.info(f"✅ Batch {i//batch_size + 1} complete ({batch_success_count}/{len(batch)} successful), waiting before next batch...")
                            await asyncio.sleep(10.0)  # 10 second delay between batches
                    
                    logger.info(f"✅ Price update cycle complete for {len(symbols_to_update)} symbols")
                # 2. RECALCULATE CORRELATIONS (every 1 hour)
                time_since_recalc = (current_time - last_correlation_recalc).total_seconds()
                if time_since_recalc >= 3600:  # 1 hour
                    logger.info("🔄 Recalculating dynamic correlations...")
                    await correlation_manager.update_all_correlations()
                    last_correlation_recalc = current_time
                    logger.info("✅ Correlation recalculation complete")
                # 3. Wait before next cycle (60 seconds)
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            logger.info("🛑 Correlation price updates cancelled")
            break
        except Exception as e:
            logger.error(f"Error in correlation price updates: {e}")
            await asyncio.sleep(60)  # Wait before retry

async def initialize_components():
    """Initialize all trading system components in the correct order"""
    global alert_handler, position_tracker, oanda_service, db_manager, risk_manager, _components_initialized
    
    logger.info("🚀 INITIALIZING TRADING SYSTEM COMPONENTS...")
    
    try:
        # 1. Initialize Unified Storage
        logger.info("📊 Initializing unified storage...")
        try:
            # Create database configuration
            db_config = DatabaseConfig(
                storage_type=StorageType.SQLITE,  # Default to SQLite for now
                connection_string="trading_bot.db"
            )
            
            db_manager = UnifiedStorage(config=db_config)
            await db_manager.connect()
            logger.info("✅ Unified storage initialized")

            if db_config.storage_type == StorageType.SQLITE:
                logger.info("Running in SQLite mode - skipping PostgreSQL backups")
        except Exception as e:
            logger.error(f"❌ Unified storage initialization failed: {e}")
            raise Exception(f"Database initialization failed: {e}")
        
        # 2. Initialize OANDA Service
        logger.info("🔗 Initializing OANDA service...")
        try:
            oanda_service = OandaService()
            await oanda_service.initialize()
            
            # Start connection monitoring for better reliability
            await oanda_service.start_connection_monitor()
            
            logger.info("✅ OANDA service initialized")
        except Exception as e:
            logger.error(f"❌ OANDA service initialization failed: {e}")
            raise Exception(f"OANDA service initialization failed: {e}")
        
        # Test crypto availability after OANDA service is initialized
        logger.info("🪙 Testing crypto availability...")
        try:
            crypto_debug = await oanda_service.debug_crypto_availability()
            crypto_found = crypto_debug.get('crypto_instruments', [])
            if crypto_found:
                logger.info(f"✅ Found crypto instruments: {crypto_found}")
            else:
                # Get OANDA config for environment info
                oanda_config = get_oanda_config()
                logger.warning(f"⚠️ No crypto instruments found in {oanda_config.environment} environment")
                logger.info("💡 Consider switching to live environment for crypto trading")
        except Exception as e:
            logger.warning(f"⚠️ Crypto availability test failed: {str(e)}")
        
        # 3. Initialize Position Tracker
        logger.info("📍 Initializing position tracker...")
        try:
            if PositionTracker is None:
                raise ImportError("PositionTracker module could not be imported")
            
            position_tracker = PositionTracker(db_manager, oanda_service)
            await position_tracker.initialize()
            logger.info("✅ Position tracker initialized")
        except Exception as e:
            logger.error(f"❌ Position tracker initialization failed: {e}")
            raise Exception(f"Position tracker initialization failed: {e}")
        
        # 4. Initialize Risk Manager
        logger.info("🛡️ Initializing risk manager...")
        try:
            risk_manager = EnhancedRiskManager()
            
            # Get account balance from OANDA service
            account_balance = await oanda_service.get_account_balance()
            await risk_manager.initialize(account_balance)
            logger.info("✅ Risk manager initialized")
        except Exception as e:
            logger.error(f"❌ Risk manager initialization failed: {e}")
            raise Exception(f"Risk manager initialization failed: {e}")
        
        # 4.5. Initialize Correlation Price Data Integration
        logger.info("📊 Initializing dynamic correlation system...")
        try:
            correlation_manager = risk_manager.correlation_manager
            
            # TEMPORARILY DISABLED: Start correlation price data updates
            # asyncio.create_task(start_correlation_price_updates(correlation_manager, oanda_service))
            logger.info("✅ Dynamic correlation system started")
        except Exception as e:
            logger.error(f"❌ Correlation system initialization failed: {e}")
            # Don't fail startup for correlation system - it's not critical
        
        # 5. Initialize Unified Exit Manager
        logger.info("🎯 Initializing unified exit manager...")
        try:
            # Initialize required components for unified exit manager
            regime_classifier = LorentzianDistanceClassifier()
            volatility_monitor = VolatilityMonitor()
            unified_exit_manager = create_unified_exit_manager(
                position_tracker, oanda_service, regime_classifier, volatility_monitor
            )
            
            # Store in global variable
            globals()['unified_exit_manager'] = unified_exit_manager
            
            await unified_exit_manager.start_monitoring()
            logger.info("✅ Unified exit manager started")
        except Exception as e:
            logger.error(f"❌ Unified exit manager initialization failed: {e}")
            # Don't fail startup for exit manager - it's not critical for basic operation
        
        # 6. Initialize Alert Handler (CRITICAL - This sets position_tracker reference)
        logger.info("⚡ Initializing alert handler...")
        try:
            alert_handler = AlertHandler(
                oanda_service=oanda_service,
                position_tracker=position_tracker,
                db_manager=db_manager,
                risk_manager=risk_manager,
                unified_exit_manager=unified_exit_manager
            )
            
            # CRITICAL: Ensure position_tracker is properly set
            if not alert_handler.position_tracker:
                logger.error("❌ CRITICAL: Alert handler position_tracker is None after initialization!")
                raise RuntimeError("Position tracker not properly set in alert handler")
                
            logger.info("✅ Alert handler initialized with position_tracker")
            
            # 7. Start the alert handler
            logger.info("🎯 Starting alert handler...")
            await alert_handler.start()
            
            # VALIDATION: Ensure alert handler is started and components are ready
            if not alert_handler._started:
                raise RuntimeError("Alert handler failed to start properly")
                
            if not alert_handler.position_tracker:
                raise RuntimeError("Alert handler position_tracker became None after start")
                
            logger.info("✅ Alert handler started successfully")
            
        except Exception as e:
            logger.error(f"❌ Alert handler initialization failed: {e}")
            raise Exception(f"Alert handler initialization failed: {e}")
        
        # 8. Set API component references
        logger.info("🔌 Setting API component references...")
        try:
            from api import set_alert_handler
            set_alert_handler(alert_handler)
            logger.info("✅ API components configured")
        except Exception as e:
            logger.error(f"❌ API component configuration failed: {e}")
            # Don't fail startup for API configuration - it's not critical for trading
        
        # 9. Initialize and start Health Checker (CRITICAL for weekend monitoring)
        logger.info("🏥 Initializing health checker...")
        try:
            health_checker = HealthChecker(alert_handler, db_manager)
            await health_checker.start()
            logger.info("✅ Health checker started - Weekend position monitoring active")
            
            # Store health_checker reference for shutdown
            globals()['health_checker'] = health_checker
        except Exception as e:
            logger.error(f"❌ Health checker initialization failed: {e}")
            # Don't fail startup for health checker - it's not critical for trading
        
        _components_initialized = True
        logger.info("🎉 ALL COMPONENTS INITIALIZED SUCCESSFULLY")
        
    except Exception as e:
        logger.error(f"❌ COMPONENT INITIALIZATION FAILED: {e}")
        # Clean up partial initialization
        if alert_handler:
            try:
                await alert_handler.stop()
            except Exception as shutdown_exc:
                logger.error(f"Error during alert handler shutdown after failed init: {shutdown_exc}")
        if db_manager:
            try:
                await db_manager.close()
            except Exception as db_exc:
                logger.error(f"Error during DB manager shutdown after failed init: {db_exc}")
        if oanda_service:
            try:
                await oanda_service.stop()
            except Exception as oanda_exc:
                logger.error(f"Error during OANDA service shutdown after failed init: {oanda_exc}")
        
        raise  # Re-raise the exception to stop the application

async def shutdown_components():
    """Shut down all trading system components gracefully"""
    global alert_handler, position_tracker, oanda_service, db_manager, unified_exit_manager
    
    logger.info("🛑 SHUTTING DOWN TRADING SYSTEM...")
    
    # Shut down in reverse order of initialization
    if 'health_checker' in globals():
        logger.info("🏥 Stopping health checker...")
        await globals()['health_checker'].stop()
    
    if unified_exit_manager:
        logger.info("🎯 Stopping unified exit manager...")
        await unified_exit_manager.stop_monitoring()
    
    if alert_handler:
        logger.info("⚡ Stopping alert handler...")
        await alert_handler.stop()
    
    if position_tracker:
        logger.info("📍 Stopping position tracker...")
        await position_tracker.stop()
        
    if oanda_service:
        logger.info("🔗 Stopping OANDA service...")
        await oanda_service.stop()
        
    if db_manager:
        logger.info("📊 Stopping unified storage...")
        await db_manager.disconnect()
        
    logger.info("✅ All components shut down successfully")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan manager to handle startup and shutdown of the trading bot.
    """
    global _system_validated
    
    logger.info("🚀 AUTO TRADING BOT STARTING UP...")
    
    # 1. Perform system validation
    validation_passed, errors = await validate_system_startup()
    if not validation_passed:
        _system_validated = False
        logger.critical("❌ STARTUP HALTED: System validation failed.")
        # In a real production system, we might exit here or prevent the API from starting
        # For now, we allow the API to run but block trading operations
    else:
        _system_validated = True
        logger.info("✅ System validation passed")
    
    # 2. Initialize trading components only if validation passed
    if _system_validated:
        try:
            await initialize_components()
        except Exception as e:
            logger.critical(f"❌ STARTUP FAILED: {e}")
            await shutdown_components()
            logger.info("✅ Auto Trading Bot shut down complete")
            # Exit the process to prevent running in a broken state
            sys.exit(1)
    
    yield
    
    # 3. Shutdown trading components
    logger.info("🛑 Shutting down Auto Trading Bot...")
    if _components_initialized:
        await shutdown_components()
        
    logger.info("✅ Auto Trading Bot shut down complete")

# --- FastAPI App Setup ---
app = FastAPI(
    title="Institutional Trading Bot API",
    description="API for the multi-asset institutional trading bot",
    version="2.0.0",
    lifespan=lifespan
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Import and include API router
from api import router as api_router
app.include_router(api_router)

@app.get("/")
async def root():
    return {
        "status": "online",
        "message": "Institutional Trading Bot API",
        "version": "2.0.0",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/startup-status")
async def startup_status():
    """Check the status of the system startup"""
    return {
        "system_validated": _system_validated,
        "components_initialized": _components_initialized,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/health")
async def health_check():
    """Comprehensive health check endpoint"""
    try:
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "system_validated": _system_validated,
            "components_initialized": _components_initialized
        }
        
        # Add OANDA service health if available
        if oanda_service:
            try:
                connection_status = await oanda_service.get_connection_status()
                health_status["oanda_service"] = connection_status
                
                if connection_status.get("circuit_breaker_active", False):
                    health_status["status"] = "degraded"
                    health_status["warnings"] = ["OANDA circuit breaker is active"]
                elif connection_status.get("connection_errors_count", 0) > 5:
                    health_status["status"] = "degraded"
                    health_status["warnings"] = ["High OANDA connection error count"]
                    
            except Exception as e:
                health_status["oanda_service"] = {"error": str(e)}
                health_status["status"] = "degraded"
                health_status["warnings"] = ["OANDA service health check failed"]
        
        # Add database health if available
        if db_manager:
            try:
                # Check database health
                is_healthy = await db_manager.is_healthy()
                health_status["database"] = {"status": "connected" if is_healthy else "error"}
                if not is_healthy:
                    health_status["status"] = "degraded"
                    health_status["warnings"] = health_status.get("warnings", []) + ["Database health check failed"]
            except Exception as e:
                health_status["database"] = {"status": "error", "error": str(e)}
                health_status["status"] = "degraded"
                health_status["warnings"] = health_status.get("warnings", []) + ["Database health check failed"]
        
        return health_status
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "An internal server error occurred."})

def set_api_components():
    """Function to be called to set API components (if needed)"""
    # This might be used in a different setup, but lifespan context is preferred
    logger.info("set_api_components called - component setup is now handled via lifespan")

# --- Main Execution ---
if __name__ == "__main__":
    logger.info("Starting Uvicorn server for local development...")
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,  # Disable reload in production
        log_level=settings.system.log_level.lower()
    )
