"""
MARKET HOURS FIX
Fixes the degraded mode issue by properly handling OANDA reconnection when markets reopen
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from utils import is_market_hours

logger = logging.getLogger(__name__)

class MarketHoursManager:
    """
    Manages OANDA service initialization based on market hours
    and handles reconnection when markets reopen
    """
    
    def __init__(self):
        self.last_market_check = None
        self.market_status = None
        self.oanda_service = None
        self.alert_handler = None
        self.check_interval = 300  # Check every 5 minutes
        
    async def initialize_oanda_service(self):
        """Initialize OANDA service if markets are open"""
        try:
            from oanda_service import OandaService
            from maintenance_aware_oanda import create_maintenance_aware_oanda_service
            from main import C
            
            logger.info("🔧 Attempting to initialize OANDA service...")
            
            # Create base OANDA service
            base_oanda = OandaService()
            
            # Wrap with maintenance-aware service
            self.oanda_service = await create_maintenance_aware_oanda_service(base_oanda)
            
            if self.oanda_service and hasattr(self.oanda_service, 'is_operational'):
                if self.oanda_service.is_operational():
                    logger.info("✅ OANDA service initialized and operational")
                    C.oanda = self.oanda_service
                    return True
                else:
                    logger.warning("⚠️ OANDA service initialized but not operational")
                    C.oanda = self.oanda_service
                    return False
            else:
                logger.error("❌ Failed to create OANDA service")
                return False
                
        except Exception as e:
            logger.error(f"❌ OANDA service initialization failed: {e}")
            return False
    
    async def check_and_reconnect(self):
        """Check market status and reconnect OANDA if needed"""
        try:
            current_market_status = is_market_hours()
            self.last_market_check = datetime.now()
            
            # If markets just opened and we don't have OANDA service
            if current_market_status and not self.market_status and not self.oanda_service:
                logger.info("📈 Markets just opened - initializing OANDA service")
                success = await self.initialize_oanda_service()
                if success:
                    await self._upgrade_alert_handler()
                return success
            
            # If markets are open and we have OANDA service, check if it's operational
            elif current_market_status and self.oanda_service:
                if hasattr(self.oanda_service, 'is_operational'):
                    if not self.oanda_service.is_operational():
                        logger.info("🔄 OANDA service not operational - attempting reconnection")
                        success = await self.initialize_oanda_service()
                        if success:
                            await self._upgrade_alert_handler()
                        return success
                    else:
                        logger.debug("✅ OANDA service operational")
                        return True
            
            # If markets are closed, we can't trade anyway
            elif not current_market_status:
                logger.debug("📅 Markets are closed - no action needed")
                return False
            
            self.market_status = current_market_status
            return current_market_status
            
        except Exception as e:
            logger.error(f"Error in market hours check: {e}")
            return False
    
    async def _upgrade_alert_handler(self):
        """Upgrade alert handler from degraded mode to normal mode"""
        try:
            from main import C
            from alert_handler import AlertHandler
            from order_queue import OrderQueue
            from config import config
            import api
            
            if not C.tracker or not C.risk:
                logger.error("Cannot upgrade alert handler - missing dependencies")
                return False
            
            logger.info("🚀 Upgrading alert handler from degraded mode to normal mode")
            
            # Create new alert handler with OANDA service
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
            
            # Start the new alert handler
            if hasattr(C.alerts, 'start'):
                start_result = C.alerts.start()
                if asyncio.iscoroutine(start_result):
                    await start_result
            
            # Update API reference
            api.set_alert_handler(C.alerts)
            
            logger.info("✅ Alert handler upgraded to normal mode")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upgrade alert handler: {e}")
            return False
    
    async def start_market_monitor(self):
        """Start background market hours monitoring"""
        logger.info("Starting market hours monitor...")
        asyncio.create_task(self._market_monitor_loop())
    
    async def _market_monitor_loop(self):
        """Background loop to monitor market hours and OANDA status"""
        while True:
            try:
                await self.check_and_reconnect()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in market monitor loop: {e}")
                await asyncio.sleep(60)  # Wait 1 minute on error

# Global instance
market_manager = MarketHoursManager()

async def force_oanda_reconnection():
    """Force OANDA reconnection - can be called via API"""
    try:
        logger.info("🔄 Forcing OANDA reconnection...")
        success = await market_manager.initialize_oanda_service()
        if success:
            await market_manager._upgrade_alert_handler()
            logger.info("✅ Forced reconnection successful")
        else:
            logger.error("❌ Forced reconnection failed")
        return success
    except Exception as e:
        logger.error(f"Error in forced reconnection: {e}")
        return False

async def get_market_status():
    """Get current market and OANDA status"""
    try:
        from main import C
        
        market_open = is_market_hours()
        oanda_operational = False
        
        if C.oanda and hasattr(C.oanda, 'is_operational'):
            oanda_operational = C.oanda.is_operational()
        elif C.oanda and hasattr(C.oanda, 'can_trade'):
            oanda_operational = C.oanda.can_trade()
        
        alert_handler_mode = "degraded"
        if C.alerts and hasattr(C.alerts, 'degraded_mode'):
            alert_handler_mode = "degraded" if C.alerts.degraded_mode else "normal"
        elif C.alerts and hasattr(C.alerts, '_started'):
            alert_handler_mode = "normal" if C.alerts._started else "stopped"
        
        return {
            "market_open": market_open,
            "oanda_operational": oanda_operational,
            "alert_handler_mode": alert_handler_mode,
            "can_trade": market_open and oanda_operational,
            "last_check": market_manager.last_market_check.isoformat() if market_manager.last_market_check else None,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting market status: {e}")
        return {
            "market_open": False,
            "oanda_operational": False,
            "alert_handler_mode": "error",
            "can_trade": False,
            "error": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
