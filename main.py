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
import traceback
import signal
from enum import Enum

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

# Import configuration
from config import settings, get_oanda_config, get_trading_config
from utils import is_market_hours
# Import database with fallback
try:
    from database import DatabaseManager
    print("✅ Database module imported successfully")
except ImportError as e:
    print(f"⚠️ Database module not available: {e}")
    # Fallback for deployment environments where database.py might not be available
    class DatabaseManager:
        def __init__(self, *args, **kwargs):
            self.connected = False
            print("Using fallback DatabaseManager - database functionality limited")
        
        async def connect(self):
            self.connected = True
            print("Fallback DatabaseManager connected (no actual database)")
        
        async def disconnect(self):
            self.connected = False
        
        async def execute_query(self, query, *args, **kwargs):
            print(f"Fallback DatabaseManager: execute_query called with {query}")
            return []
        
        async def fetch_one(self, query, *args, **kwargs):
            print(f"Fallback DatabaseManager: fetch_one called with {query}")
            return None
        
        async def fetch_all(self, query, *args, **kwargs):
            print(f"Fallback DatabaseManager: fetch_all called with {query}")
            return []
        
        async def create_tables(self):
            print("Fallback DatabaseManager: create_tables called (no-op)")
        
        async def backup_database(self, *args, **kwargs):
            print("Fallback DatabaseManager: backup_database called (no-op)")
            return {"status": "fallback", "message": "No actual database backup performed"}
        
        async def initialize(self):
            print("Fallback DatabaseManager: initialize called (no-op)")
        
        async def close(self):
            print("Fallback DatabaseManager: close called (no-op)")

# Import core trading components
from oanda_service import OandaService
from tracker import PositionTracker
from risk_manager import EnhancedRiskManager
from trailing_stop_monitor import TrailingStopMonitor
from profit_ride_override import ProfitRideOverride
from regime_classifier import LorentzianDistanceClassifier
from volatility_monitor import VolatilityMonitor
from alert_handler import AlertHandler
from health_checker import HealthChecker
from api import router as api_router, set_alert_handler
from advanced_logging import metrics_collector, advanced_logger, LogLevel, LogCategory

# Import new performance and risk systems
from performance_optimization import (
    redis_cache_manager, message_queue_manager, 
    database_replica_manager, PerformanceOptimizer
)
from realtime_pnl_attribution import (
    pnl_attribution_engine, pnl_manager
)
from ml_integration import (
    ml_model_manager, ml_signal_enhancer, ml_exit_override_optimizer, 
    ml_position_sizer, enhance_tradingview_signal, should_override_exit, 
    calculate_ml_position_size
)
from advanced_risk_metrics import (
    risk_calculator, portfolio_risk_manager, 
    calculate_portfolio_risk, calculate_var, run_stress_tests
)

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
trailing_stop_monitor: Optional[Any] = None

# New performance and risk system components
performance_optimizer: Optional[Any] = None
pnl_manager: Optional[Any] = None
ml_models: Optional[Any] = None
risk_metrics: Optional[Any] = None

# System validation flags
_system_validated = False
_components_initialized = False

# Error handling and fallback system
class SystemState(Enum):
    STARTING = "starting"
    RUNNING = "running"
    DEGRADED = "degraded"
    ERROR = "error"
    MAINTENANCE = "maintenance"

class ErrorSeverity(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class SystemHealth:
    def __init__(self):
        self.state = SystemState.STARTING
        self.last_error = None
        self.error_count = 0
        self.component_status = {}
        self.fallback_active = False
        self.last_health_check = datetime.now(timezone.utc)
        
    def update_component_status(self, component: str, status: bool, error: str = None):
        """Update component health status"""
        self.component_status[component] = {
            "healthy": status,
            "last_check": datetime.now(timezone.utc),
            "error": error
        }
        
        # Update overall system state
        healthy_components = sum(1 for comp in self.component_status.values() if comp["healthy"])
        total_components = len(self.component_status)
        
        if healthy_components == total_components:
            self.state = SystemState.RUNNING
        elif healthy_components >= total_components * 0.7:  # 70% healthy
            self.state = SystemState.DEGRADED
        else:
            self.state = SystemState.ERROR
            
    def get_health_summary(self) -> Dict[str, Any]:
        """Get comprehensive health summary"""
        return {
            "state": self.state.value,
            "last_health_check": self.last_health_check.isoformat(),
            "error_count": self.error_count,
            "fallback_active": self.fallback_active,
            "components": self.component_status,
            "uptime_seconds": (datetime.now(timezone.utc) - self.last_health_check).total_seconds()
        }

# Global system health tracker
system_health = SystemHealth()

# Error tracking and recovery
class ErrorRecoveryManager:
    def __init__(self):
        self.error_history = []
        self.recovery_attempts = {}
        self.max_recovery_attempts = 3
        self.recovery_cooldown = 300  # 5 minutes
        
    def log_error(self, component: str, error: Exception, severity: ErrorSeverity = ErrorSeverity.MEDIUM):
        """Log error and determine recovery action"""
        error_entry = {
            "timestamp": datetime.now(timezone.utc),
            "component": component,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "severity": severity.value,
            "traceback": traceback.format_exc()
        }
        
        self.error_history.append(error_entry)
        system_health.error_count += 1
        system_health.last_error = error_entry
        
        # Keep only last 100 errors
        if len(self.error_history) > 100:
            self.error_history = self.error_history[-100:]
            
        logger.error(f"🚨 ERROR in {component}: {error} (Severity: {severity.value})")
        
        # Determine recovery action
        return self._determine_recovery_action(component, error, severity)
        
    def _determine_recovery_action(self, component: str, error: Exception, severity: ErrorSeverity) -> str:
        """Determine appropriate recovery action based on error"""
        if severity == ErrorSeverity.CRITICAL:
            return "restart_component"
        elif severity == ErrorSeverity.HIGH:
            return "fallback_mode"
        elif severity == ErrorSeverity.MEDIUM:
            return "retry_with_backoff"
        else:
            return "log_and_continue"
            
    def can_attempt_recovery(self, component: str) -> bool:
        """Check if recovery can be attempted for component"""
        attempts = self.recovery_attempts.get(component, 0)
        if attempts >= self.max_recovery_attempts:
            last_attempt = self.recovery_attempts.get(f"{component}_last_attempt")
            if last_attempt:
                time_since_attempt = (datetime.now(timezone.utc) - last_attempt).total_seconds()
                if time_since_attempt < self.recovery_cooldown:
                    return False
            # Reset attempts after cooldown
            self.recovery_attempts[component] = 0
            
        return True
        
    def record_recovery_attempt(self, component: str):
        """Record a recovery attempt"""
        self.recovery_attempts[component] = self.recovery_attempts.get(component, 0) + 1
        self.recovery_attempts[f"{component}_last_attempt"] = datetime.now(timezone.utc)

# Global error recovery manager
error_recovery = ErrorRecoveryManager()

# Fallback mechanisms
class FallbackManager:
    def __init__(self):
        self.active_fallbacks = set()
        self.fallback_configs = {
            "database": {
                "primary": "postgresql",
                "fallback": "sqlite",
                "enabled": True
            },
            "oanda_service": {
                "retry_attempts": 3,
                "retry_delay": 5,
                "circuit_breaker_threshold": 5,
                "enabled": True
            },
            "position_tracking": {
                "memory_fallback": True,
                "file_fallback": True,
                "enabled": True
            }
        }
        
    def activate_fallback(self, component: str, reason: str):
        """Activate fallback for component"""
        self.active_fallbacks.add(component)
        system_health.fallback_active = True
        logger.warning(f"🔄 FALLBACK ACTIVATED for {component}: {reason}")
        
    def deactivate_fallback(self, component: str):
        """Deactivate fallback for component"""
        self.active_fallbacks.discard(component)
        if not self.active_fallbacks:
            system_health.fallback_active = False
        logger.info(f"✅ FALLBACK DEACTIVATED for {component}")
        
    def is_fallback_active(self, component: str) -> bool:
        """Check if fallback is active for component"""
        return component in self.active_fallbacks

# Global fallback manager
fallback_manager = FallbackManager()

# Signal handlers for graceful shutdown
def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown"""
    def signal_handler(signum, frame):
        logger.info(f"🛑 Received signal {signum}, initiating graceful shutdown...")
        system_health.state = SystemState.MAINTENANCE
        # Set shutdown flag for graceful cleanup
        global _shutdown_requested
        _shutdown_requested = True
        
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

# Global shutdown flag
_shutdown_requested = False

# Comprehensive error handling decorator
def with_error_handling(component: str, fallback_enabled: bool = True):
    """Decorator for comprehensive error handling with fallback support"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                # Log error with severity assessment
                severity = _assess_error_severity(e, component)
                recovery_action = error_recovery.log_error(component, e, severity)
                
                # Update component health
                system_health.update_component_status(component, False, str(e))
                
                # Handle recovery action
                if recovery_action == "restart_component" and error_recovery.can_attempt_recovery(component):
                    logger.info(f"🔄 Attempting to restart {component}...")
                    error_recovery.record_recovery_attempt(component)
                    # Restart logic would go here
                    
                elif recovery_action == "fallback_mode" and fallback_enabled:
                    fallback_manager.activate_fallback(component, str(e))
                    
                elif recovery_action == "retry_with_backoff":
                    await asyncio.sleep(5)  # Backoff delay
                    return await func(*args, **kwargs)  # Retry once
                
                # Re-raise critical errors
                if severity == ErrorSeverity.CRITICAL:
                    raise
                    
                # Return fallback response for non-critical errors
                return _get_fallback_response(component, e)
                
        return wrapper
    return decorator

def _assess_error_severity(error: Exception, component: str) -> ErrorSeverity:
    """Assess error severity based on error type and component"""
    error_type = type(error).__name__
    
    # Critical errors that require immediate attention
    critical_errors = [
        "DatabaseConnectionError",
        "AuthenticationError", 
        "PermissionError",
        "MemoryError"
    ]
    
    # High severity errors that affect core functionality
    high_errors = [
        "ConnectionError",
        "TimeoutError",
        "HTTPException"
    ]
    
    if error_type in critical_errors:
        return ErrorSeverity.CRITICAL
    elif error_type in high_errors:
        return ErrorSeverity.HIGH
    elif "timeout" in str(error).lower() or "connection" in str(error).lower():
        return ErrorSeverity.MEDIUM
    else:
        return ErrorSeverity.LOW

def _get_fallback_response(component: str, error: Exception) -> Dict[str, Any]:
    """Get fallback response when component fails"""
    return {
        "status": "degraded",
        "component": component,
        "error": str(error),
        "fallback_active": True,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# Health check and monitoring endpoints
async def health_check() -> Dict[str, Any]:
    """Comprehensive health check endpoint"""
    try:
        health_summary = system_health.get_health_summary()
        
        # Add component-specific health checks
        component_checks = {}
        
        # Check database
        if db_manager:
            try:
                await db_manager.test_connection()
                component_checks["database"] = {"status": "healthy", "type": db_manager.db_type}
            except Exception as e:
                component_checks["database"] = {"status": "unhealthy", "error": str(e)}
        
        # Check OANDA service
        if oanda_service:
            try:
                await oanda_service.get_account_balance()
                component_checks["oanda"] = {"status": "healthy"}
            except Exception as e:
                component_checks["oanda"] = {"status": "unhealthy", "error": str(e)}
        
        # Check position tracker
        if position_tracker:
            try:
                positions = await position_tracker.get_all_positions()
                component_checks["position_tracker"] = {
                    "status": "healthy", 
                    "active_positions": len(positions)
                }
            except Exception as e:
                component_checks["position_tracker"] = {"status": "unhealthy", "error": str(e)}
        
        health_summary["component_checks"] = component_checks
        health_summary["system_uptime"] = (datetime.now(timezone.utc) - system_health.last_health_check).total_seconds()
        
        return health_summary
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

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
    
    # Convert to NY time - handle DST properly
    # DST in 2025: March 9 - November 2 (approximate)
    # For production, should use pytz, but this is a reasonable approximation
    month = dt.month
    day = dt.day
    
    # Daylight Saving Time (EDT = UTC-4) runs roughly March-November
    if 3 <= month <= 10:  # March through October
        is_dst = True
    elif month == 2 or month == 11:  # February or November  
        is_dst = False
    else:  # December, January
        is_dst = False
    
    # More precise DST calculation for March and November edge cases
    if month == 3:  # March - DST starts second Sunday
        # Simplified: assume DST starts March 9th (approximate)
        is_dst = day >= 9
    elif month == 11:  # November - DST ends first Sunday  
        # Simplified: assume DST ends November 2nd (approximate)
        is_dst = day < 2
    
    ny_offset = -4 if is_dst else -5  # EDT or EST
    ny_tz = timezone(timedelta(hours=ny_offset))
    ny_time = dt.astimezone(ny_tz)
    weekday = ny_time.weekday()  # 0=Monday, 6=Sunday
    hour = ny_time.hour
    minute = ny_time.minute
    second = ny_time.second
    
    if is_market_hours(dt):
        # Market is open, find next close (Friday 17:00 NY time)
        if weekday < 4:  # Monday-Thursday
            # Next close is this Friday 17:00
            days_until_friday = 4 - weekday
            close_time = ny_time.replace(hour=17, minute=0, second=0, microsecond=0) + timedelta(days=days_until_friday)
        elif weekday == 4 and hour < 17:  # Friday before 17:00
            # Next close is today at 17:00
            close_time = ny_time.replace(hour=17, minute=0, second=0, microsecond=0)
        else:
            # Should not happen if is_market_hours is correct, but safety fallback
            close_time = ny_time + timedelta(minutes=1)
        
        seconds = (close_time - ny_time).total_seconds()
        if seconds < 60:  # Minimum 1 minute to prevent tight loops
            seconds = 60
        return True, int(seconds)
    else:
        # Market is closed, find next open (Sunday 17:00 NY time)
        if weekday == 6:  # Sunday
            if hour < 17:
                # It's Sunday before 17:00 - market opens today at 17:00
                open_time = ny_time.replace(hour=17, minute=0, second=0, microsecond=0)
            else:
                # It's Sunday after 17:00 - market opens next Sunday at 17:00
                open_time = ny_time.replace(hour=17, minute=0, second=0, microsecond=0) + timedelta(days=7)
        elif weekday == 5:  # Saturday
            # Market opens next day (Sunday) at 17:00
            open_time = ny_time.replace(hour=17, minute=0, second=0, microsecond=0) + timedelta(days=1)
        elif weekday == 4 and hour >= 17:  # Friday after 17:00
            # Market opens in 2 days (Sunday) at 17:00
            open_time = ny_time.replace(hour=17, minute=0, second=0, microsecond=0) + timedelta(days=2)
        else:
            # Should not happen if is_market_hours is correct, but safety fallback
            # Default to next Sunday 17:00
            days_until_sunday = (6 - weekday) % 7
            if days_until_sunday == 0:  # Today is Sunday
                days_until_sunday = 7  # Next Sunday
            open_time = ny_time.replace(hour=17, minute=0, second=0, microsecond=0) + timedelta(days=days_until_sunday)
        
        seconds = (open_time - ny_time).total_seconds()
        
        if seconds < 300:  # Minimum 5 minutes to prevent tight loops during market closed periods
            seconds = 300
        return False, int(seconds)

async def retry_queued_alerts_task(alert_handler):
    """
    INSTITUTIONAL FIX: Retry queued alerts when OANDA connectivity recovers.
    
    Runs every 30 seconds to check for queued alerts and retry them
    if OANDA connection is now healthy.
    """
    logger.info("🔄 Starting queued alert retry task...")
    
    while True:
        try:
            await asyncio.sleep(30)  # Check every 30 seconds
            
            # Check if there are queued alerts
            status = alert_handler.get_status()
            queued_count = status.get("queued_alerts", 0)
            
            if queued_count > 0:
                logger.info(f"🔍 Found {queued_count} queued alerts, attempting retry...")
                result = await alert_handler.retry_queued_alerts()
                
                if result.get("processed", 0) > 0:
                    logger.info(f"✅ Successfully processed {result['processed']} queued alerts")
                elif result.get("status") == "waiting":
                    logger.debug(f"⏳ {queued_count} alerts still queued (OANDA degraded)")
        except Exception as e:
            logger.error(f"Error in queued alert retry task: {e}")
            await asyncio.sleep(10)  # Brief pause before continuing

async def start_correlation_price_updates(correlation_manager, oanda_service):
    """
    DYNAMIC CORRELATION UPDATES
    
    Continuously update correlation manager with fresh price data
    to enable real-time correlation calculations for institutional risk management
    """
    logger.info("🔄 Starting dynamic correlation price updates...")
    
    # Major forex pairs to track for correlations
    tracked_symbols = [
        'EUR_USD', 'GBP_USD', 'USD_JPY', 'USD_CHF', 'AUD_USD', 'USD_CAD',
        'EUR_GBP', 'EUR_JPY', 'EUR_CHF', 'GBP_JPY', 'GBP_CHF', 'AUD_JPY',
        'NZD_USD', 'CHF_JPY', 'CAD_JPY'
    ]
    
    last_price_update = {}
    last_correlation_recalc = datetime.now(timezone.utc) - timedelta(hours=1)
    
    while True:
        try:
            is_open, seconds_until_event = get_seconds_until_next_market_event()
            if not is_open:
                # Add debugging info to understand the issue
                current_utc = datetime.now(timezone.utc)
                logger.info(f"🛑 Market is closed. Current UTC: {current_utc}, Sleeping until open in {seconds_until_event//60} minutes ({seconds_until_event} seconds).")
                if seconds_until_event < 60:
                    logger.warning(f"⚠️ DEBUGGING: Sleep time suspiciously short ({seconds_until_event}s). This may indicate a calculation error.")
                
                # RENDER DEPLOYMENT FIX: Break up long sleeps to prevent deployment timeouts
                # Render may restart the service if it sleeps too long
                max_sleep_chunk = 300  # 5 minutes max per sleep
                remaining_sleep = seconds_until_event
                
                while remaining_sleep > 0:
                    chunk_sleep = min(remaining_sleep, max_sleep_chunk)
                    logger.info(f"💤 Sleeping for {chunk_sleep} seconds ({chunk_sleep//60} minutes)...")
                    await asyncio.sleep(chunk_sleep)
                    remaining_sleep -= chunk_sleep
                    
                    # Re-check market status after each sleep chunk in case of time changes
                    if remaining_sleep > 0:
                        is_still_closed, _ = get_seconds_until_next_market_event()
                        if is_still_closed:
                            logger.info(f"💤 Continuing sleep - {remaining_sleep} seconds remaining...")
                        else:
                            logger.info(f"⏰ Market opened during sleep - breaking out early")
                            break
                continue
            # Market is open, poll until next close
            logger.info(f"✅ Market is open. Polling prices for {seconds_until_event//60} minutes until next close.")
            polling_end = datetime.now(timezone.utc) + timedelta(seconds=seconds_until_event)
            while datetime.now(timezone.utc) < polling_end:
                current_time = datetime.now(timezone.utc)
                # 1. UPDATE PRICE DATA (every 15 minutes)
                symbols_to_update = []
                for symbol in tracked_symbols:
                    last_update = last_price_update.get(symbol, datetime.min.replace(tzinfo=timezone.utc))
                    time_since_update = (current_time - last_update).total_seconds()
                    if time_since_update >= 900:  # 15 minutes
                        symbols_to_update.append(symbol)
                if symbols_to_update:
                    try:
                        # INSTITUTIONAL FIX: Chunk large batch requests to avoid overwhelming OANDA practice API
                        # Split into groups of 20 symbols with 200ms stagger between chunks
                        chunk_size = 20
                        all_batched_prices = {}
                        for i in range(0, len(symbols_to_update), chunk_size):
                            chunk = symbols_to_update[i:i+chunk_size]
                            chunk_prices = await oanda_service.get_current_prices(chunk)
                            all_batched_prices.update(chunk_prices)
                            # Stagger chunks to reduce TCP pressure on OANDA
                            if i + chunk_size < len(symbols_to_update):
                                await asyncio.sleep(0.2)  # 200ms between chunks
                        
                        batched_prices = all_batched_prices
                        for idx, symbol in enumerate(symbols_to_update):
                            px = batched_prices.get(symbol)
                            if not px:
                                logger.warning(f"Failed to get price for {symbol}: No pricing returned")
                                continue
                            # Use mid price for correlation stability
                            mid_price = (float(px.get('bid', 0.0)) + float(px.get('ask', 0.0))) / 2.0
                            if mid_price <= 0:
                                logger.warning(f"Failed to compute mid price for {symbol} from {px}")
                                continue
                            await correlation_manager.add_price_data(symbol, mid_price, current_time)
                            last_price_update[symbol] = current_time
                            if idx == 0:
                                logger.info(f"📊 Updated price data: {symbol} = {mid_price}")
                            else:
                                logger.debug(f"Updated price data: {symbol}")
                    except Exception as e:
                        logger.warning(f"Batch price fetch failed: {e}")
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
        # 1. Initialize Database Manager
        logger.info("📊 Initializing database manager...")
        db_manager = DatabaseManager()
        await db_manager.initialize()
        logger.info("✅ Database manager initialized")

        if db_manager.db_type == "sqlite":
            logger.info("Running in SQLite mode - skipping PostgreSQL backups")
        
        # 2. Initialize OANDA Service
        logger.info("🔗 Initializing OANDA service...")
        oanda_service = OandaService()
        await oanda_service.initialize()
        logger.info("✅ OANDA service initialized")
        
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
        position_tracker = PositionTracker(db_manager)
        await position_tracker.start()
        logger.info("✅ Position tracker initialized")
        
        # 3.1 Reconcile OANDA open positions into tracker if missing
        try:
            open_positions_oanda = await oanda_service.get_open_positions_from_oanda()
            if open_positions_oanda:
                existing = await position_tracker.get_all_positions()
                existing_ids = set(existing.keys())
                imported = 0
                for pos in open_positions_oanda:
                    symbol = pos.get('instrument')
                    units = float(pos.get('units', 0) or 0)
                    if units == 0 or not symbol:
                        continue
                    action = 'BUY' if units > 0 else 'SELL'
                    # Create a synthetic position_id if not already present
                    position_id = f"OANDA_{symbol}_{action}"
                    if position_id in existing_ids:
                        continue
                    # Fetch a current price for entry placeholder
                    try:
                        entry_price = await oanda_service.get_current_price(symbol, 'BUY' if units > 0 else 'SELL')
                    except Exception:
                        entry_price = 0.0
                    await position_tracker.record_position(
                        position_id=position_id,
                        symbol=symbol,
                        action=action,
                        timeframe='H1',
                        entry_price=entry_price,
                        size=abs(units),
                        stop_loss=None,
                        take_profit=None,
                        metadata={'source': 'oanda_reconcile'}
                    )
                    imported += 1
                if imported:
                    logger.info(f"🔄 Reconciled {imported} open positions from OANDA into tracker")
        except Exception as e:
            logger.warning(f"OANDA reconciliation skipped due to error: {e}")
        
        # 4. Initialize Risk Manager
        logger.info("🛡️ Initializing risk manager...")
        risk_manager = EnhancedRiskManager()
        
        # Get account balance from OANDA service
        account_balance = await oanda_service.get_account_balance()
        await risk_manager.initialize(account_balance)
        logger.info("✅ Risk manager initialized")
        
        # 4.1 Bootstrap risk manager from tracker open positions
        try:
            open_positions_nested = await position_tracker.get_open_positions()
            for symbol, positions in open_positions_nested.items():
                for position_id, position_data in positions.items():
                    entry_price = float(position_data.get('entry_price', 0) or 0)
                    size = float(position_data.get('size', 0) or 0)
                    stop_loss = position_data.get('stop_loss')
                    timeframe = position_data.get('timeframe', 'H1')
                    action = position_data.get('action', 'BUY')
                    if entry_price > 0 and size > 0:
                        if stop_loss is None:
                            # Conservative assumed risk: 1% of price if SL unknown
                            est_risk = 0.01
                        else:
                            est_risk = abs(entry_price - float(stop_loss)) / entry_price
                        est_risk = max(0.0001, min(est_risk, 0.10))  # clamp 1bp to 10%
                        await risk_manager.register_position(
                            position_id=position_id,
                            symbol=symbol,
                            action=action,
                            size=size,
                            entry_price=entry_price,
                            account_risk=est_risk,
                            stop_loss=stop_loss,
                            timeframe=str(timeframe)
                        )
            logger.info("🔐 Risk manager bootstrapped from tracker open positions")
        except Exception as e:
            logger.warning(f"Risk bootstrap skipped due to error: {e}")
        
        # 4.5. Initialize Correlation Price Data Integration
        logger.info("📊 Initializing dynamic correlation system...")
        correlation_manager = risk_manager.correlation_manager
        
        # Start correlation price data updates
        asyncio.create_task(start_correlation_price_updates(correlation_manager, oanda_service))
        logger.info("✅ Dynamic correlation system started")
        
        # 5. Initialize Tiered TP Monitor
        logger.info("🎯 Initializing tiered TP monitor...")
        
        # Initialize required components for override manager
        regime_classifier = LorentzianDistanceClassifier()
        volatility_monitor = VolatilityMonitor()
        override_manager = ProfitRideOverride(regime_classifier, volatility_monitor)
        
        trailing_stop_monitor = TrailingStopMonitor(oanda_service, position_tracker, override_manager)
        await trailing_stop_monitor.start_monitoring()
        globals()['trailing_stop_monitor'] = trailing_stop_monitor
        logger.info("✅ Trailing stop monitor started")
        
        # 6. Initialize Alert Handler (CRITICAL - This sets position_tracker reference)
        logger.info("⚡ Initializing alert handler...")
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
        
        # 7. Start the alert handler
        logger.info("🎯 Starting alert handler...")
        await alert_handler.start()
        
        # VALIDATION: Ensure alert handler is started and components are ready
        if not alert_handler._started:
            raise RuntimeError("Alert handler failed to start properly")
            
        if not alert_handler.position_tracker:
            raise RuntimeError("Alert handler position_tracker became None after start")
            
        logger.info("✅ Alert handler started successfully")
        
        # 7.5 Start queued alert retry task
        logger.info("🔄 Starting queued alert retry task...")
        asyncio.create_task(retry_queued_alerts_task(alert_handler))
        logger.info("✅ Queued alert retry task started")
        
        # 8. Set API component references
        logger.info("🔌 Setting API component references...")
        set_alert_handler(alert_handler)
        logger.info("✅ API components configured")
        
        # 9. Initialize and start Health Checker (CRITICAL for weekend monitoring)
        logger.info("🏥 Initializing health checker...")
        health_checker = HealthChecker(alert_handler, db_manager)
        await health_checker.start()
        logger.info("✅ Health checker started - Weekend position monitoring active")
        
        # Store health_checker reference for shutdown
        globals()['health_checker'] = health_checker
        
        # 10. Initialize Performance Optimization Systems
        logger.info("⚡ Initializing performance optimization systems...")
        global performance_optimizer, pnl_manager, ml_models, risk_metrics
        
        # Initialize performance optimizer
        performance_optimizer = PerformanceOptimizer()
        await performance_optimizer.initialize()
        logger.info("✅ Performance optimizer initialized")
        
        # Initialize P&L attribution engine
        from realtime_pnl_attribution import RealTimePnLAttribution
        pnl_manager = RealTimePnLAttribution()
        if hasattr(pnl_manager, 'initialize') and callable(getattr(pnl_manager, 'initialize')):
            await pnl_manager.initialize()
        logger.info("✅ Real-time P&L manager initialized")
        
        # Initialize ML model manager and components
        ml_models = ml_model_manager
        # Note: MLModelManager doesn't have an initialize method - it's ready to use
        logger.info("✅ ML model manager initialized")
        
        # Initialize ML signal execution components
        global ml_signal_enhancer, ml_exit_override_optimizer, ml_position_sizer
        ml_signal_enhancer = ml_signal_enhancer
        ml_exit_override_optimizer = ml_exit_override_optimizer
        ml_position_sizer = ml_position_sizer
        logger.info("✅ ML signal execution components initialized")
        
        # Initialize advanced risk metrics
        risk_metrics = portfolio_risk_manager
        logger.info("✅ Advanced risk metrics initialized")
        
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
    global alert_handler, position_tracker, oanda_service, db_manager, trailing_stop_monitor
    
    logger.info("🛑 SHUTTING DOWN TRADING SYSTEM...")
    
    # Shut down in reverse order of initialization
    if 'health_checker' in globals():
        logger.info("🏥 Stopping health checker...")
        await globals()['health_checker'].stop()
    
    if trailing_stop_monitor:
        logger.info("🎯 Stopping trailing stop monitor...")
        await trailing_stop_monitor.stop_monitoring()
    
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
        logger.info("📊 Stopping database manager...")
        await db_manager.close()
        
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

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler with comprehensive error logging"""
    error_id = f"ERR_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Log error with full context
    logger.error(f"🚨 GLOBAL EXCEPTION [{error_id}]: {type(exc).__name__}: {str(exc)}")
    logger.error(f"Request: {request.method} {request.url}")
    logger.error(f"Traceback: {traceback.format_exc()}")
    
    # Update system health
    system_health.update_component_status("api", False, str(exc))
    
    # Return appropriate response based on error type
    if isinstance(exc, HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": exc.detail,
                "error_id": error_id,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        )
    else:
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal server error",
                "error_id": error_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "fallback_active": system_health.fallback_active
            }
        )

# Request logging middleware
@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    """Log all requests with timing and error tracking"""
    start_time = datetime.now(timezone.utc)
    request_id = f"REQ_{start_time.strftime('%Y%m%d_%H%M%S_%f')}"
    
    logger.info(f"📥 {request.method} {request.url} [{request_id}]")
    
    try:
        response = await call_next(request)
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        
        logger.info(f"📤 {request.method} {request.url} [{request_id}] - {response.status_code} ({duration:.3f}s)")
        
        # Add request ID to response headers
        response.headers["X-Request-ID"] = request_id
        return response
        
    except Exception as e:
        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.error(f"❌ {request.method} {request.url} [{request_id}] - ERROR ({duration:.3f}s): {e}")
        raise

# Import and include API router
app.include_router(api_router)

@app.get("/")
@app.head("/")
async def root():
    """Health check endpoint - keeps deployment alive during market closed periods"""
    try:
        # Check if market is currently open
        is_open, seconds_until_event = get_seconds_until_next_market_event()
        market_status = "OPEN" if is_open else "CLOSED"
        
        return {
            "status": "online",
            "message": "Institutional Trading Bot API",
            "version": "2.0.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "market_status": market_status,
            "next_event_minutes": seconds_until_event // 60,
            "deployment_type": "render_cloud"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "error",
            "message": "Institutional Trading Bot API - Health Check Failed",
            "version": "2.0.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(e)
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
async def health_endpoint():
    """Comprehensive health check endpoint"""
    return await health_check()

@app.get("/health/detailed")
async def detailed_health():
    """Detailed health check with component status"""
    health_data = await health_check()
    
    # Add additional detailed information
    health_data.update({
        "error_history": error_recovery.error_history[-10:],  # Last 10 errors
        "active_fallbacks": list(fallback_manager.active_fallbacks),
        "recovery_attempts": error_recovery.recovery_attempts,
        "system_metrics": {
            "memory_usage": "N/A",  # Would need psutil
            "cpu_usage": "N/A",     # Would need psutil
            "disk_usage": "N/A"     # Would need psutil
        }
    })
    
    return health_data

@app.get("/errors")
async def get_errors(limit: int = 50):
    """Get recent error history"""
    return {
        "errors": error_recovery.error_history[-limit:],
        "total_errors": len(error_recovery.error_history),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.post("/system/maintenance")
async def enter_maintenance_mode():
    """Enter maintenance mode (stop trading)"""
    system_health.state = SystemState.MAINTENANCE
    logger.warning("🔧 SYSTEM ENTERED MAINTENANCE MODE")
    return {
        "status": "maintenance_mode_activated",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.post("/system/resume")
async def resume_normal_operation():
    """Resume normal operation from maintenance mode"""
    system_health.state = SystemState.RUNNING
    logger.info("✅ SYSTEM RESUMED NORMAL OPERATION")
    return {
        "status": "normal_operation_resumed",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.post("/fallback/{component}/activate")
async def activate_fallback(component: str, reason: str = "Manual activation"):
    """Manually activate fallback for a component"""
    fallback_manager.activate_fallback(component, reason)
    return {
        "status": f"fallback_activated_for_{component}",
        "reason": reason,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.post("/fallback/{component}/deactivate")
async def deactivate_fallback(component: str):
    """Manually deactivate fallback for a component"""
    fallback_manager.deactivate_fallback(component)
    return {
        "status": f"fallback_deactivated_for_{component}",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

@app.get("/environment")
async def get_environment_info():
    """Get current environment configuration"""
    return settings.get_environment_info()

@app.get("/environment/switch")
async def list_available_environments():
    """List available environments"""
    return {
        "current_environment": settings.environment,
        "available_environments": list(settings.environments.keys()),
        "environment_configs": {
            env: {
                "name": config.name,
                "debug": config.debug,
                "trading_enabled": config.trading_enabled,
                "risk_multiplier": config.risk_multiplier,
                "max_positions": config.max_positions
            }
            for env, config in settings.environments.items()
        }
    }

@app.post("/environment/validate")
async def validate_environment_config():
    """Validate current environment configuration"""
    validation_results = {
        "environment": settings.environment,
        "validation_passed": True,
        "warnings": [],
        "errors": [],
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    
    # Validate database connection
    try:
        if db_manager:
            await db_manager.test_connection()
            validation_results["database"] = "healthy"
        else:
            validation_results["warnings"].append("Database manager not initialized")
    except Exception as e:
        validation_results["errors"].append(f"Database validation failed: {str(e)}")
        validation_results["validation_passed"] = False
    
    # Validate OANDA configuration
    try:
        if oanda_service:
            await oanda_service.get_account_balance()
            validation_results["oanda"] = "healthy"
        else:
            validation_results["warnings"].append("OANDA service not initialized")
    except Exception as e:
        validation_results["errors"].append(f"OANDA validation failed: {str(e)}")
        validation_results["validation_passed"] = False
    
    # Validate trading configuration
    if not settings.trading.trading_enabled:
        validation_results["warnings"].append("Trading is disabled in current environment")
    
    # Validate risk settings
    if settings.trading.max_risk_per_trade > 15.0:
        validation_results["warnings"].append(f"High risk per trade: {settings.trading.max_risk_per_trade}%")
    
    return validation_results

@app.get("/monitoring/metrics")
async def get_metrics():
    """Get system metrics and performance data"""
    try:
        return {
            "metrics": metrics_collector.get_metrics(),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@app.get("/monitoring/logs")
async def get_logs(category: str = "all", limit: int = 100):
    """Get recent logs"""
    try:
        log_files = {
            "trading": "logs/trading.log",
            "risk": "logs/risk.log",
            "execution": "logs/execution.log",
            "system": "logs/system.log",
            "performance": "logs/performance.log",
            "error": "logs/error.log",
            "audit": "logs/audit.log"
        }
        
        logs = []
        
        if category == "all":
            for cat, file_path in log_files.items():
                if os.path.exists(file_path):
                    with open(file_path, "r") as f:
                        lines = f.readlines()[-limit:]
                        for line in lines:
                            try:
                                log_entry = json.loads(line.strip())
                                logs.append(log_entry)
                            except:
                                continue
        else:
            file_path = log_files.get(category)
            if file_path and os.path.exists(file_path):
                with open(file_path, "r") as f:
                    lines = f.readlines()[-limit:]
                    for line in lines:
                        try:
                            log_entry = json.loads(line.strip())
                            logs.append(log_entry)
                        except:
                            continue
        
        # Sort by timestamp
        logs.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        
        return {
            "logs": logs[:limit],
            "total_count": len(logs),
            "category": category,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        return {
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@app.get("/monitoring/performance")
async def get_performance_analytics():
    """Get comprehensive performance analytics"""
    try:
        # Get position analytics if available
        position_analytics = {}
        if position_tracker and hasattr(position_tracker, 'get_performance_analytics'):
            position_analytics = await position_tracker.get_performance_analytics()
        
        # Get system metrics
        system_metrics = metrics_collector.get_metrics()
        
        return {
            "position_analytics": position_analytics,
            "system_metrics": system_metrics,
            "system_health": system_health.get_health_summary(),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        return {
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

@app.post("/monitoring/log")
async def log_custom_event(
    level: str,
    category: str,
    component: str,
    message: str,
    data: Optional[Dict[str, Any]] = None
):
    """Log custom event"""
    try:
        log_level = LogLevel(level.upper())
        log_category = LogCategory(category.lower())
        
        await advanced_logger.log(
            log_level,
            log_category,
            component,
            message,
            data
        )
        
        return {
            "status": "logged",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        return {
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

# Performance Optimization Endpoints
@app.get("/performance/cache/status")
async def get_cache_status():
    """Get Redis cache status and statistics"""
    try:
        if not performance_optimizer:
            return {"error": "Performance optimizer not initialized"}
        
        cache_stats = await performance_optimizer.get_cache_statistics()
        return {
            "cache_status": cache_stats,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/performance/cache/clear")
async def clear_cache():
    """Clear Redis cache"""
    try:
        if not performance_optimizer:
            return {"error": "Performance optimizer not initialized"}
        
        await performance_optimizer.clear_cache()
        return {
            "success": True,
            "message": "Cache cleared successfully",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/performance/queue/status")
async def get_queue_status():
    """Get message queue status"""
    try:
        if not performance_optimizer:
            return {"error": "Performance optimizer not initialized"}
        
        queue_stats = await performance_optimizer.get_queue_statistics()
        return {
            "queue_status": queue_stats,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {"error": str(e)}

# Real-time P&L Attribution Endpoints
@app.get("/pnl/attribution")
async def get_pnl_attribution():
    """Get real-time P&L attribution breakdown"""
    try:
        if not pnl_manager:
            return {"error": "P&L manager not initialized"}
        
        attribution = await pnl_manager.get_attribution_breakdown()
        return {
            "attribution": attribution,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/pnl/performance")
async def get_pnl_performance():
    """Get P&L performance metrics"""
    try:
        if not pnl_manager:
            return {"error": "P&L manager not initialized"}
        
        performance = await pnl_manager.get_performance_metrics()
        return {
            "performance": performance,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {"error": str(e)}

# Machine Learning Endpoints for Signal Execution Bot
@app.get("/ml/models")
async def get_ml_models():
    """Get available ML models"""
    try:
        if not ml_models:
            return {"error": "ML model manager not initialized"}
        
        models = {}
        for model_id, model in ml_models.models.items():
            models[model_id] = {
                "name": model.name,
                "type": model.model_type.value,
                "status": model.status.value,
                "version": model.version,
                "performance": model.performance_metrics
            }
        
        return {
            "models": models,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/ml/enhance-signal")
async def enhance_tradingview_signal_endpoint(signal: Dict[str, Any]):
    """Enhance TradingView signal with ML validation and scoring"""
    try:
        if not ml_signal_enhancer:
            return {"error": "ML signal enhancer not initialized"}
        
        enhanced_signal = await enhance_tradingview_signal(signal)
        return {
            "enhanced_signal": enhanced_signal,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/ml/exit-override")
async def ml_exit_override_endpoint(
    position: Dict[str, Any], 
    exit_signal: Dict[str, Any]
):
    """Get ML recommendation for exit override decision"""
    try:
        if not ml_exit_override_optimizer:
            return {"error": "ML exit override optimizer not initialized"}
        
        override_decision = await should_override_exit(position, exit_signal)
        return {
            "override_decision": override_decision,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/ml/position-sizing")
async def ml_position_sizing_endpoint(
    signal: Dict[str, Any], 
    base_size: float
):
    """Get ML-optimized position size for TradingView signal"""
    try:
        if not ml_position_sizer:
            return {"error": "ML position sizer not initialized"}
        
        sizing_result = await calculate_ml_position_size(signal, base_size)
        return {
            "sizing_result": sizing_result,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {"error": str(e)}

# Advanced Risk Metrics Endpoints
@app.get("/risk/portfolio")
async def get_portfolio_risk():
    """Get comprehensive portfolio risk profile"""
    try:
        if not risk_metrics:
            return {"error": "Risk metrics not initialized"}
        
        # Get portfolio data from position tracker
        portfolio_data = {}
        if position_tracker:
            portfolio_data = await position_tracker.get_portfolio_data()
        
        risk_profile = await risk_metrics.calculate_portfolio_risk_profile(portfolio_data)
        if not risk_profile:
            return {"error": "Risk profile calculation failed"}
        
        return {
            "risk_profile": asdict(risk_profile),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/risk/stress-tests")
async def run_stress_tests():
    """Run portfolio stress tests"""
    try:
        if not risk_metrics:
            return {"error": "Risk metrics not initialized"}
        
        # Get portfolio data
        portfolio_data = {}
        if position_tracker:
            portfolio_data = await position_tracker.get_portfolio_data()
        
        stress_results = await risk_metrics.run_stress_tests(portfolio_data)
        return {
            "stress_tests": stress_results,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/risk/var")
async def calculate_var(
    confidence_level: float = 0.95,
    time_horizon: str = "1d"
):
    """Calculate Value at Risk"""
    try:
        if not risk_metrics:
            return {"error": "Risk metrics not initialized"}
        
        # Get returns data
        returns = []
        if position_tracker:
            portfolio_data = await position_tracker.get_portfolio_data()
            returns = portfolio_data.get("returns", [])
        
        if not returns:
            return {"error": "No returns data available"}
        
        var_metric = await risk_metrics.calculate_var(returns, confidence_level)
        if not var_metric:
            return {"error": "VaR calculation failed"}
        
        return {
            "var": asdict(var_metric),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {"error": str(e)}

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

@app.get("/oanda/account")
async def get_oanda_account_info():
    """Get OANDA account info for connectivity debugging."""
    try:
        if oanda_service:
            info = await oanda_service.get_account_info()
            return {"status": "success", "account_info": info}
        else:
            return {"status": "error", "message": "OANDA service not initialized"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/oanda/instruments")
async def get_oanda_instruments():
    """Get available OANDA instruments for connectivity debugging."""
    try:
        if oanda_service:
            instruments = await oanda_service.debug_crypto_availability()
            return {"status": "success", "instruments": instruments}
        else:
            return {"status": "error", "message": "OANDA service not initialized"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# --- Main Execution ---
if __name__ == "__main__":
    # Setup signal handlers for graceful shutdown
    setup_signal_handlers()
    
    logger.info("🚀 Starting Institutional Trading Bot...")
    logger.info(f"Environment: {os.getenv('ENVIRONMENT', 'development')}")
    logger.info(f"Log Level: {settings.system.log_level}")
    logger.info(f"Server: {settings.server.host}:{settings.server.port}")
    
    try:
        uvicorn.run(
            "main:app",
            host=settings.server.host,
            port=settings.server.port,
            reload=settings.server.reload,
            log_level=settings.system.log_level.lower(),
            access_log=True
        )
    except KeyboardInterrupt:
        logger.info("🛑 Received keyboard interrupt, shutting down gracefully...")
        system_health.state = SystemState.MAINTENANCE
    except Exception as e:
        logger.error(f"🚨 Fatal error in main: {e}")
        error_recovery.log_error("main", e, ErrorSeverity.CRITICAL)
        sys.exit(1)
    finally:
        logger.info("✅ Institutional Trading Bot shutdown complete")
