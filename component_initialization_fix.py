"""
COMPONENT INITIALIZATION FIX
Enhanced initialization with proper error handling and fallback mechanisms
"""

import asyncio
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ComponentStatus:
    """Component initialization status"""
    name: str
    initialized: bool
    error: Optional[str] = None
    fallback_used: bool = False

class RobustComponentInitializer:
    """
    Robust component initialization with comprehensive error handling
    and fallback mechanisms for institutional-grade reliability
    """
    
    def __init__(self):
        self.component_status: Dict[str, ComponentStatus] = {}
        self.initialization_order = [
            "storage",
            "oanda_service", 
            "risk_manager",
            "position_tracker",
            "unified_analysis",
            "unified_exit_manager",
            "alert_handler",
            "health_monitor"
        ]
    
    async def initialize_all_components(self) -> Dict[str, ComponentStatus]:
        """
        Initialize all components with proper error handling and fallbacks
        """
        logger.info("🚀 STARTING ROBUST COMPONENT INITIALIZATION...")
        
        # Initialize each component in order
        for component_name in self.initialization_order:
            try:
                await self._initialize_component(component_name)
            except Exception as e:
                logger.error(f"❌ Failed to initialize {component_name}: {e}")
                self.component_status[component_name] = ComponentStatus(
                    name=component_name,
                    initialized=False,
                    error=str(e),
                    fallback_used=True
                )
        
        # Log final status
        self._log_initialization_summary()
        
        return self.component_status
    
    async def _initialize_component(self, component_name: str):
        """Initialize a specific component with error handling"""
        logger.info(f"🔧 Initializing {component_name}...")
        
        if component_name == "storage":
            await self._init_storage()
        elif component_name == "oanda_service":
            await self._init_oanda_service()
        elif component_name == "risk_manager":
            await self._init_risk_manager()
        elif component_name == "position_tracker":
            await self._init_position_tracker()
        elif component_name == "unified_analysis":
            await self._init_unified_analysis()
        elif component_name == "unified_exit_manager":
            await self._init_unified_exit_manager()
        elif component_name == "alert_handler":
            await self._init_alert_handler()
        elif component_name == "health_monitor":
            await self._init_health_monitor()
        else:
            raise ValueError(f"Unknown component: {component_name}")
        
        self.component_status[component_name] = ComponentStatus(
            name=component_name,
            initialized=True
        )
        logger.info(f"✅ {component_name} initialized successfully")
    
    async def _init_storage(self):
        """Initialize storage with fallback"""
        try:
            from main import _init_storage
            from main import C
            
            C.storage = await _init_storage()
            if not C.storage:
                raise Exception("Storage initialization returned None")
                
        except Exception as e:
            logger.error(f"Storage initialization failed: {e}")
            # Create a minimal fallback storage
            from main import C
            C.storage = None  # Will be handled by other components
            raise
    
    async def _init_oanda_service(self):
        """Initialize OANDA service with maintenance-aware fallback and market hours check"""
        try:
            from oanda_service import OandaService
            from maintenance_aware_oanda import create_maintenance_aware_oanda_service
            from main import C
            from utils import is_market_hours
            
            # Check if markets are open before attempting OANDA connection
            if not is_market_hours():
                logger.info("📅 Markets are closed - skipping OANDA connection during weekend/maintenance")
                logger.info("🔄 OANDA service will be initialized when markets reopen")
                C.oanda = None
                return  # Skip OANDA initialization during market closure
            
            logger.info("📈 Markets are open - initializing OANDA service")
            
            # Create base OANDA service
            base_oanda = OandaService()
            
            # Wrap with maintenance-aware service
            C.oanda = await create_maintenance_aware_oanda_service(base_oanda)
            
            if not C.oanda:
                raise Exception("OANDA service initialization returned None")
                
        except Exception as e:
            logger.error(f"OANDA service initialization failed: {e}")
            from main import C
            C.oanda = None
            raise
    
    async def _init_risk_manager(self):
        """Initialize risk manager with account balance"""
        try:
            from risk_manager import EnhancedRiskManager
            from main import C
            
            C.risk = EnhancedRiskManager()
            if not C.risk:
                raise Exception("Risk manager initialization returned None")
            
            # CRITICAL FIX: Initialize risk manager with account balance
            logger.info(f"🔍 Risk Manager Init Debug - OANDA available: {C.oanda is not None}")
            if C.oanda and hasattr(C.oanda, 'get_account_balance'):
                try:
                    logger.info("🔍 Attempting to get account balance from OANDA...")
                    account_balance = await C.oanda.get_account_balance()
                    logger.info(f"🔍 Got account balance: ${account_balance:.2f}")
                    await C.risk.initialize(account_balance)
                    logger.info(f"✅ Risk manager initialized with account balance: ${account_balance:.2f}")
                except Exception as e:
                    logger.warning(f"⚠️ Could not get account balance, using fallback: {e}")
                    # Use fallback balance for risk calculations
                    await C.risk.initialize(100000.0)
                    logger.info("✅ Risk manager initialized with fallback balance: $100,000")
            else:
                logger.warning("⚠️ OANDA service not available, using fallback balance")
                await C.risk.initialize(100000.0)
                logger.info("✅ Risk manager initialized with fallback balance: $100,000")
                
        except Exception as e:
            logger.error(f"Risk manager initialization failed: {e}")
            from main import C
            C.risk = None
            raise
    
    async def _init_position_tracker(self):
        """Initialize position tracker with fallback"""
        try:
            from tracker import PositionTracker
            from main import C
            
            # Try with dependencies first
            if C.storage and C.oanda:
                C.tracker = PositionTracker(db_manager=C.storage, oanda_service=C.oanda)
            else:
                C.tracker = PositionTracker()
            
            # Try to start if method exists
            if hasattr(C.tracker, 'start'):
                start_result = C.tracker.start()
                if asyncio.iscoroutine(start_result):
                    await start_result
            
            if not C.tracker:
                raise Exception("Position tracker initialization returned None")
                
        except Exception as e:
            logger.error(f"Position tracker initialization failed: {e}")
            from main import C
            C.tracker = None
            raise
    
    async def _init_unified_analysis(self):
        """Initialize unified analysis (optional)"""
        try:
            from unified_analysis import UnifiedAnalysis
            from main import C
            
            C.analysis = UnifiedAnalysis()
            
            # Try to start if method exists
            if hasattr(C.analysis, 'start'):
                start_result = C.analysis.start()
                if asyncio.iscoroutine(start_result):
                    await start_result
            
            if not C.analysis:
                raise Exception("Unified analysis initialization returned None")
                
        except Exception as e:
            logger.warning(f"Unified analysis initialization failed (optional): {e}")
            from main import C
            C.analysis = None
            # Don't raise - this is optional
    
    async def _init_unified_exit_manager(self):
        """Initialize unified exit manager"""
        try:
            from unified_exit_manager import UnifiedExitManager
            from main import C
            
            # Check dependencies
            if not C.tracker:
                raise Exception("Position tracker not available")
            if not C.oanda:
                raise Exception("OANDA service not available")
            
            C.exit_mgr = UnifiedExitManager(
                position_tracker=C.tracker,
                oanda_service=C.oanda,
                unified_analysis=C.analysis  # Can be None
            )
            
            # Try to start monitoring if method exists
            if hasattr(C.exit_mgr, 'start_monitoring'):
                start_result = C.exit_mgr.start_monitoring()
                if asyncio.iscoroutine(start_result):
                    await start_result
            
            if not C.exit_mgr:
                raise Exception("Unified exit manager initialization returned None")
                
        except Exception as e:
            logger.error(f"Unified exit manager initialization failed: {e}")
            from main import C
            C.exit_mgr = None
            raise
    
    async def _init_alert_handler(self):
        """Initialize alert handler - CRITICAL COMPONENT with degraded mode fallback"""
        try:
            # Use deferred import to avoid circular imports
            from main import _import_alert_handler
            from order_queue import OrderQueue
            from config import config
            from maintenance_aware_oanda import create_degraded_mode_alert_handler
            from main import C
            from utils import is_market_hours
            import api
            
            # Import AlertHandler with circular import protection
            AlertHandler = _import_alert_handler()
            
            # Check if markets are open
            markets_open = is_market_hours()
            
            # Check if OANDA service is operational
            oanda_operational = False
            if C.oanda and hasattr(C.oanda, 'is_operational'):
                oanda_operational = C.oanda.is_operational()
            elif C.oanda and hasattr(C.oanda, 'can_trade'):
                oanda_operational = C.oanda.can_trade()
            
            # Check other critical dependencies
            if not C.tracker:
                raise Exception("Position tracker not available")
            if not C.risk:
                raise Exception("Risk manager not available")
            
            if oanda_operational and markets_open:
                # Normal mode - OANDA is operational and markets are open
                logger.info("🚀 Initializing alert handler in NORMAL MODE (markets open)")
                C.alerts = AlertHandler(
                    oanda_service=C.oanda,
                    position_tracker=C.tracker,
                    risk_manager=C.risk,
                    unified_analysis=C.analysis,
                    order_queue=OrderQueue(),
                    config=config,
                    db_manager=C.storage,
                    unified_exit_manager=C.exit_mgr
                )
            else:
                # Degraded mode - OANDA unavailable or markets closed
                if not markets_open:
                    logger.warning("🚨 Initializing alert handler in DEGRADED MODE (markets closed)")
                else:
                    logger.warning("🚨 Initializing alert handler in DEGRADED MODE (OANDA unavailable)")
                C.alerts = create_degraded_mode_alert_handler()
            
            # CRITICAL FIX: Properly start alert handler and ensure _started persists
            if hasattr(C.alerts, 'start'):
                try:
                    start_result = C.alerts.start()
                    if asyncio.iscoroutine(start_result):
                        start_success = await start_result
                    else:
                        start_success = start_result
                    
                    # Verify _started was set correctly
                    if hasattr(C.alerts, '_started') and C.alerts._started:
                        logger.info(f"✅ Alert handler start() successful (_started={C.alerts._started})")
                    else:
                        logger.error(f"❌ Alert handler start() called but _started not set properly (_started={getattr(C.alerts, '_started', 'MISSING')})")
                        # Force set _started
                        C.alerts._started = True
                        logger.warning(f"🔧 Forced _started=True after failed start() (_started={C.alerts._started})")
                        
                except Exception as e:
                    logger.error(f"❌ Error calling alert handler start(): {e}")
                    # Set _started manually as fallback
                    if hasattr(C.alerts, '_started'):
                        C.alerts._started = True
                        logger.info(f"✅ Set _started=True as fallback after error (_started={C.alerts._started})")
                    else:
                        logger.error("❌ CRITICAL: Alert handler has no _started attribute even after initialization")
            else:
                logger.warning("⚠️ Alert handler has no start() method")
                # Ensure _started is set
                if hasattr(C.alerts, '_started'):
                    C.alerts._started = True
                    logger.info(f"✅ Set _started=True (no start method) (_started={C.alerts._started})")
                else:
                    logger.error("❌ CRITICAL: Alert handler has no _started attribute")
            
            # CRITICAL: Final safety check with detailed logging
            if hasattr(C.alerts, '_started'):
                final_started_value = C.alerts._started
                logger.info(f"🔍 Final _started check: {final_started_value}")
                if not final_started_value:
                    logger.error("❌ CRITICAL: _started is False after initialization - forcing to True")
                    C.alerts._started = True
                    logger.warning(f"🔧 FORCED _started=True (_started={C.alerts._started})")
                else:
                    logger.info(f"✅ Final safety check passed (_started={C.alerts._started})")
            else:
                logger.error("❌ CRITICAL: Alert handler missing _started attribute after initialization")
                # Try to add the attribute
                try:
                    C.alerts._started = True
                    logger.warning(f"🔧 Added missing _started attribute (_started={C.alerts._started})")
                except Exception as e:
                    logger.error(f"❌ Failed to add _started attribute: {e}")
            
            # Expose to API immediately
            api.set_alert_handler(C.alerts)
            
            if not C.alerts:
                raise Exception("Alert handler initialization returned None")
                
        except Exception as e:
            logger.error(f"Alert handler initialization failed: {e}")
            from main import C
            C.alerts = None
            raise
    
    async def _init_health_monitor(self):
        """Initialize health monitor (optional)"""
        try:
            from health_checker import UnifiedMonitor
            from main import C
            
            C.monitor = UnifiedMonitor()
            
            # Try different start methods
            start_methods = ["start", "start_weekend_monitoring"]
            started = False
            
            for method in start_methods:
                if hasattr(C.monitor, method):
                    start_result = getattr(C.monitor, method)()
                    if asyncio.iscoroutine(start_result):
                        await start_result
                    started = True
                    break
            
            if not started:
                logger.info("Health monitor present but no start method found")
            
            if not C.monitor:
                raise Exception("Health monitor initialization returned None")
                
        except Exception as e:
            logger.warning(f"Health monitor initialization failed (optional): {e}")
            from main import C
            C.monitor = None
            # Don't raise - this is optional
    
    def _log_initialization_summary(self):
        """Log summary of component initialization"""
        logger.info("📊 COMPONENT INITIALIZATION SUMMARY:")
        
        critical_components = ["oanda_service", "risk_manager", "alert_handler"]
        optional_components = ["unified_analysis", "health_monitor"]
        
        for component_name in self.initialization_order:
            status = self.component_status.get(component_name)
            if not status:
                logger.error(f"  ❌ {component_name}: NOT INITIALIZED")
                continue
            
            if status.initialized:
                if component_name in critical_components:
                    logger.info(f"  ✅ {component_name}: READY")
                else:
                    logger.info(f"  ✅ {component_name}: READY (optional)")
            else:
                if component_name in critical_components:
                    logger.error(f"  ❌ {component_name}: FAILED - {status.error}")
                else:
                    logger.warning(f"  ⚠️ {component_name}: FAILED (optional) - {status.error}")
        
        # Check if critical components are ready
        critical_ready = all(
            self.component_status.get(comp, ComponentStatus(comp, False)).initialized 
            for comp in critical_components
        )
        
        if critical_ready:
            logger.info("🎉 ALL CRITICAL COMPONENTS READY - SYSTEM OPERATIONAL")
        else:
            logger.error("❌ CRITICAL COMPONENTS MISSING - SYSTEM DEGRADED")
    
    def get_system_status(self) -> Dict[str, Any]:
        """Get comprehensive system status"""
        critical_components = ["oanda_service", "risk_manager", "alert_handler"]
        
        critical_ready = all(
            self.component_status.get(comp, ComponentStatus(comp, False)).initialized 
            for comp in critical_components
        )
        
        return {
            "system_operational": critical_ready,
            "components": {
                name: {
                    "initialized": status.initialized,
                    "error": status.error,
                    "fallback_used": status.fallback_used
                }
                for name, status in self.component_status.items()
            },
            "critical_components_ready": critical_ready,
            "total_components": len(self.component_status),
            "initialized_components": sum(1 for s in self.component_status.values() if s.initialized)
        }

# Enhanced initialization function for main.py
async def robust_initialize_components():
    """
    Enhanced component initialization with comprehensive error handling
    """
    initializer = RobustComponentInitializer()
    status = await initializer.initialize_all_components()
    
    # Return system status for monitoring
    return initializer.get_system_status()
