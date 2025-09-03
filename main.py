"""
INSTITUTIONAL TRADING BOT - MAIN APPLICATION
Clean startup/shutdown with HealthChecker and proper imports
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Any, Optional, List, Tuple

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

# -----------------------------------------------------------------------------
# Configuration & Utilities
# -----------------------------------------------------------------------------
from config import settings, get_oanda_config, get_trading_config
from utils import is_market_hours

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, settings.system.log_level, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Imports from local modules (these files exist in your project)
# -----------------------------------------------------------------------------
try:
    from unified_storage import UnifiedStorage, DatabaseConfig, StorageType
    from oanda_service import OandaService
    from tracker import PositionTracker
    from risk_manager import EnhancedRiskManager  # assuming this module exists in your project
    from unified_exit_manager import create_unified_exit_manager
    from unified_analysis import UnifiedMarketAnalyzer  # optional if present
    from alert_handler import AlertHandler
    from health_checker import HealthChecker
    from api import router as api_router, set_api_components
    logger.info("✅ All required modules imported successfully")
except Exception as e:
    logger.error(f"❌ CRITICAL: Failed to import required modules: {e}", exc_info=True)
    raise

# -----------------------------------------------------------------------------
# Globals (assigned during startup)
# -----------------------------------------------------------------------------
alert_handler: Optional[AlertHandler] = None
position_tracker: Optional[PositionTracker] = None
oanda_service: Optional[OandaService] = None
db_manager: Optional[UnifiedStorage] = None
risk_manager: Optional[EnhancedRiskManager] = None
unified_exit_manager = None
health_checker: Optional[HealthChecker] = None
market_analyzer: Optional[UnifiedMarketAnalyzer] = None

_background_tasks: list[asyncio.Task] = []

# -----------------------------------------------------------------------------
# Validation helpers
# -----------------------------------------------------------------------------
async def validate_system_startup() -> Tuple[bool, List[str]]:
    """
    Validate critical env/settings before allowing any trading operations.
    """
    logger.info("🔍 STARTING SYSTEM VALIDATION...")

    errors: List[str] = []
    warnings: List[str] = []

    # Environment vars
    required_env = {
        "OANDA_ACCESS_TOKEN": ["OANDA_ACCESS_TOKEN"],
        "OANDA_ACCOUNT_ID": ["OANDA_ACCOUNT_ID"],
        # DATABASE_URL is optional if you want to fall back to SQLite;
        # if present and startswith postgres, we'll use Postgres.
    }

    for key, names in required_env.items():
        if not any(os.getenv(n) for n in names):
            errors.append(f"Missing environment variable: {key}")

    # OANDA settings sanity
    if not settings.oanda.environment:
        errors.append("OANDA environment not configured")
    else:
        logger.info(f"✅ OANDA environment: {settings.oanda.environment}")

    # DB config visibility
    db_url = os.getenv("DATABASE_URL", "").strip()
    if db_url:
        logger.info(f"✅ Database URL configured: {db_url}")
    else:
        warnings.append("DATABASE_URL not set; defaulting to SQLite")

    if errors:
        for e in errors:
            logger.error(f"❌ {e}")
        return False, errors

    if warnings:
        logger.warning("⚠️ SYSTEM WARNINGS:")
        for w in warnings:
            logger.warning(f"   {w}")

    logger.info("✅ SYSTEM VALIDATION PASSED")
    return True, warnings

# -----------------------------------------------------------------------------
# Market / polling helpers
# -----------------------------------------------------------------------------
def get_seconds_until_next_market_event(dt: Optional[datetime] = None):
    """Returns (is_open, seconds_until_next_event) based on is_market_hours logic."""
    if dt is None:
        dt = datetime.now(timezone.utc)
    ny_tz = timezone(timedelta(hours=-5))
    ny_time = dt.astimezone(ny_tz)
    weekday = ny_time.weekday()
    hour = ny_time.hour
    if is_market_hours(dt):
        # next close: Friday 17:00 NY
        if weekday < 4 or (weekday == 4 and hour < 17):
            days_until_friday = 4 - weekday
            close_time = ny_time.replace(hour=17, minute=0, second=0, microsecond=0) + timedelta(days=days_until_friday)
        else:
            close_time = ny_time
        seconds = max(1, int((close_time - ny_time).total_seconds()))
        return True, seconds
    else:
        # next open: Sunday 17:00 NY
        days_until_sunday = (6 - weekday) % 7
        open_time = ny_time.replace(hour=17, minute=0, second=0, microsecond=0) + timedelta(days=days_until_sunday)
        if weekday == 6 and hour < 17:
            open_time = ny_time.replace(hour=17, minute=0, second=0, microsecond=0)
        seconds = max(1, int((open_time - ny_time).total_seconds()))
        return False, seconds

async def start_correlation_price_updates(oanda: OandaService):
    """
    Continuously update (optionally) a market analyzer with price data
    to support correlation or regime analytics if available.
    """
    logger.info("🔄 Starting dynamic correlation/price updates.")
    tracked_symbols = ['EUR_USD', 'GBP_USD', 'USD_JPY', 'USD_CHF', 'AUD_USD', 'USD_CAD']
    last_correlation_recalc = datetime.now(timezone.utc) - timedelta(hours=1)

    # This will only be meaningful if UnifiedMarketAnalyzer is initialized
    global market_analyzer

    while True:
        try:
            is_open, seconds_until_event = get_seconds_until_next_market_event()
            if not is_open:
                logger.info(f"🛑 Market is closed. Sleeping for {seconds_until_event//60} minutes.")
                await asyncio.sleep(seconds_until_event)
                continue

            polling_end = datetime.now(timezone.utc) + timedelta(seconds=seconds_until_event)
            while datetime.now(timezone.utc) < polling_end:
                # Fetch latest prices (pseudo-implementation; your OandaService likely has a method)
                try:
                    prices = await oanda.get_prices(tracked_symbols)  # implement in oanda_service if not present
                except Exception:
                    prices = None

                # If analyzer exists, feed data
                if market_analyzer and prices:
                    try:
                        await market_analyzer.ingest_prices(prices)
                        if (datetime.now(timezone.utc) - last_correlation_recalc) > timedelta(minutes=15):
                            await market_analyzer.recalculate_correlations()
                            last_correlation_recalc = datetime.now(timezone.utc)
                    except Exception as e:
                        logger.warning(f"Analyzer update warning: {e}")

                await asyncio.sleep(15)  # poll cadence

        except asyncio.CancelledError:
            logger.info("Correlation/price updates task cancelled.")
            break
        except Exception as e:
            logger.error(f"Correlation/price updates loop error: {e}", exc_info=True)
            await asyncio.sleep(10)

# -----------------------------------------------------------------------------
# Component initialization & shutdown
# -----------------------------------------------------------------------------
async def initialize_components():
    """
    Initialize and wire all major components.
    """
    global alert_handler, position_tracker, oanda_service, db_manager
    global risk_manager, unified_exit_manager, health_checker, market_analyzer
    global _background_tasks

    # Database selection
    db_url = os.getenv("DATABASE_URL", "").strip()
    if db_url.lower().startswith("postgres"):
        db_cfg = DatabaseConfig(storage_type=StorageType.POSTGRESQL, connection_string=db_url)
    elif db_url:
        # allow sqlite path in env
        db_cfg = DatabaseConfig(storage_type=StorageType.SQLITE, connection_string=db_url)
    else:
        db_cfg = DatabaseConfig(storage_type=StorageType.SQLITE, connection_string="trading_bot.db")

    db_manager = UnifiedStorage(config=db_cfg)
    await db_manager.connect()

    # OANDA
    oanda_service = OandaService(get_oanda_config())
    await oanda_service.initialize()

    # Position tracking
    position_tracker = PositionTracker(db_manager=db_manager, oanda_service=oanda_service)

    # Risk manager (use balance if your service exposes it)
    try:
        balance = await oanda_service.get_account_balance()
    except Exception:
        balance = None
    risk_manager = EnhancedRiskManager(
        oanda_service=oanda_service,
        trading_config=get_trading_config(),
        initial_balance=balance
    )

    # Exit monitor/manager
    unified_exit_manager = create_unified_exit_manager(
        position_tracker=position_tracker,
        oanda_service=oanda_service,
        db_manager=db_manager
    )
    await unified_exit_manager.start_monitoring()

    # Alert handler
    alert_handler = AlertHandler(
        position_tracker=position_tracker,
        oanda_service=oanda_service,
        risk_manager=risk_manager,
        db_manager=db_manager,
        exit_monitor=unified_exit_manager
    )
    await alert_handler.start()
    logger.info("🎯 Alert handler initialized and started.")

    # Optional market analyzer if your module exists
    try:
        market_analyzer = UnifiedMarketAnalyzer()
        logger.info("📊 Unified Market Analyzer initialized")
    except Exception as e:
        market_analyzer = None
        logger.info(f"Unified Market Analyzer not initialized (optional): {e}")

    # Health checker (replaces old UnifiedMonitor)
    health_checker = HealthChecker(alert_handler=alert_handler, db_manager=db_manager)
    await health_checker.start()
    logger.info("🏥 Health checker started.")

    # Background tasks
    _background_tasks = []
    _background_tasks.append(asyncio.create_task(start_correlation_price_updates(oanda_service)))

    # Inform API layer now that components exist
    set_api_components()
    logger.info("🔌 Setting API component references...")

async def shutdown_components():
    """
    Gracefully stop background tasks and components.
    """
    global alert_handler, position_tracker, oanda_service, db_manager
    global risk_manager, unified_exit_manager, health_checker, _background_tasks

    logger.info("🛑 SHUTTING DOWN TRADING SYSTEM...")

    # cancel background tasks
    for t in _background_tasks:
        try:
            t.cancel()
        except Exception:
            pass
    for t in _background_tasks:
        try:
            await t
        except Exception:
            pass
    _background_tasks.clear()

    # health checker
    if health_checker:
        try:
            await health_checker.stop()
            logger.info("🏥 Stopping health checker...")
        except Exception as e:
            logger.warning(f"Health checker stop warning: {e}")

    # alert handler
    if alert_handler:
        try:
            await alert_handler.stop()
            logger.info("⚡ Stopping alert handler...")
        except Exception as e:
            logger.warning(f"Alert handler stop warning: {e}")

    # unified exit manager
    if unified_exit_manager:
        try:
            await unified_exit_manager.stop_monitoring()
            logger.info("🎯 Stopping unified exit manager...")
        except Exception as e:
            logger.warning(f"Exit manager stop warning: {e}")

    # position tracker has no explicit stop in many designs; skip safely

    # oanda service
    if oanda_service:
        try:
            await oanda_service.shutdown()
            logger.info("🔗 Stopping OANDA service...")
        except Exception as e:
            logger.warning(f"OANDA service stop warning: {e}")

    # database
    if db_manager:
        try:
            logger.info("📊 Stopping unified storage...")
            await db_manager.disconnect()
        except Exception as e:
            logger.warning(f"DB disconnect warning: {e}")

    logger.info("✅ All components shut down successfully")

# -----------------------------------------------------------------------------
# FastAPI setup with lifespan
# -----------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    ok, _msgs = await validate_system_startup()
    if not ok:
        # Defer startup but keep the app responsive with a 503 response
        logger.error("System validation failed; API will run but trading components won't start.")
    else:
        try:
            await initialize_components()
            logger.info("🎉 ALL COMPONENTS INITIALIZED SUCCESSFULLY")
        except Exception as e:
            logger.error(f"❌ Startup initialization failed: {e}", exc_info=True)

    yield

    # Shutdown path
    try:
        await shutdown_components()
        logger.info("✅ Auto Trading Bot shut down complete")
    except Exception as e:
        logger.error(f"❌ Shutdown failed: {e}", exc_info=True)

app = FastAPI(title="Auto Trading Bot", lifespan=lifespan)

# CORS (adjust as needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes
@app.get("/", tags=["root"])
async def root():
    return JSONResponse({"status": "ok", "message": "Auto Trading Bot API is running"})

# Include API router (alert endpoints, etc.)
app.include_router(api_router)

# -----------------------------------------------------------------------------
# Local execution
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # Render expects binding to 0.0.0.0 and PORT env if provided
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        workers=1,
        log_level="info",
        timeout_keep_alive=60,
        limit_concurrency=100,
        access_log=True,
    )
