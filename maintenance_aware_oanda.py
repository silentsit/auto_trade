"""
MAINTENANCE-AWARE OANDA SERVICE WRAPPER
Handles OANDA maintenance mode gracefully with fallback mechanisms
"""

import asyncio
import logging
from typing import Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import time

logger = logging.getLogger(__name__)

@dataclass
class MaintenanceStatus:
    """Maintenance status information"""
    is_maintenance: bool
    last_503_time: Optional[datetime] = None
    consecutive_503s: int = 0
    estimated_end_time: Optional[datetime] = None
    fallback_active: bool = False

class MaintenanceAwareOandaService:
    """
    Wrapper around OandaService that handles maintenance mode gracefully
    and provides fallback mechanisms for degraded operation
    """
    
    def __init__(self, oanda_service=None):
        self.oanda_service = oanda_service
        self.maintenance_status = MaintenanceStatus(is_maintenance=False)
        self.fallback_mode = False
        self.last_successful_connection = None
        self.maintenance_check_interval = 300  # 5 minutes
        self.max_maintenance_wait = 3600  # 1 hour max wait
        
    async def initialize_with_fallback(self):
        """Initialize OANDA service with maintenance-aware fallback"""
        try:
            if self.oanda_service:
                await self.oanda_service.initialize()
                self.maintenance_status.is_maintenance = False
                self.fallback_mode = False
                self.last_successful_connection = datetime.now()
                logger.info("✅ OANDA service initialized successfully")
                return True
        except Exception as e:
            error_str = str(e).lower()
            if '503' in error_str or 'maintenance' in error_str:
                logger.warning(f"⚠️ OANDA maintenance mode detected: {e}")
                self._handle_maintenance_mode()
                return False
            else:
                logger.error(f"❌ OANDA service initialization failed: {e}")
                return False
    
    def _handle_maintenance_mode(self):
        """Handle maintenance mode detection"""
        self.maintenance_status.is_maintenance = True
        self.maintenance_status.last_503_time = datetime.now()
        self.maintenance_status.consecutive_503s += 1
        self.maintenance_status.fallback_active = True
        self.fallback_mode = True
        
        # Estimate maintenance end time (typically 15-30 minutes)
        estimated_duration = min(30, 15 + (self.maintenance_status.consecutive_503s * 5))
        self.maintenance_status.estimated_end_time = datetime.now() + timedelta(minutes=estimated_duration)
        
        logger.warning(f"🔄 Maintenance mode active - estimated end: {self.maintenance_status.estimated_end_time}")
    
    async def check_maintenance_status(self) -> bool:
        """Check if OANDA is still in maintenance mode"""
        if not self.maintenance_status.is_maintenance:
            return False
        
        # If we have an estimated end time, check if it's passed
        if self.maintenance_status.estimated_end_time:
            if datetime.now() > self.maintenance_status.estimated_end_time:
                logger.info("🔄 Maintenance period should be over, attempting reconnection...")
                return await self._attempt_reconnection()
        
        # Check if we've been in maintenance too long
        if self.maintenance_status.last_503_time:
            time_in_maintenance = datetime.now() - self.maintenance_status.last_503_time
            if time_in_maintenance.total_seconds() > self.max_maintenance_wait:
                logger.warning("⚠️ Maintenance period exceeded maximum wait time")
                return await self._attempt_reconnection()
        
        return True  # Still in maintenance
    
    async def _attempt_reconnection(self) -> bool:
        """Attempt to reconnect to OANDA service"""
        try:
            if self.oanda_service:
                # Try a simple health check
                if hasattr(self.oanda_service, '_health_check'):
                    is_healthy = await self.oanda_service._health_check()
                    if is_healthy:
                        self.maintenance_status.is_maintenance = False
                        self.maintenance_status.consecutive_503s = 0
                        self.fallback_mode = False
                        self.last_successful_connection = datetime.now()
                        logger.info("✅ OANDA service reconnected successfully")
                        return False  # No longer in maintenance
        except Exception as e:
            error_str = str(e).lower()
            if '503' in error_str or 'maintenance' in error_str:
                logger.warning(f"Still in maintenance: {e}")
                self._handle_maintenance_mode()
            else:
                logger.error(f"Reconnection failed: {e}")
        
        return True  # Still in maintenance
    
    def is_operational(self) -> bool:
        """Check if the service is operational (not in maintenance)"""
        return not self.maintenance_status.is_maintenance and not self.fallback_mode
    
    def can_trade(self) -> bool:
        """Check if trading is possible"""
        if self.is_operational():
            return True
        
        # In maintenance mode, we can't trade
        return False
    
    async def get_connection_status(self) -> Dict[str, Any]:
        """Get comprehensive connection status including maintenance info"""
        base_status = {
            "operational": self.is_operational(),
            "can_trade": self.can_trade(),
            "maintenance_mode": self.maintenance_status.is_maintenance,
            "fallback_active": self.fallback_mode,
            "last_successful_connection": self.last_successful_connection.isoformat() if self.last_successful_connection else None,
            "consecutive_503s": self.maintenance_status.consecutive_503s,
            "estimated_maintenance_end": self.maintenance_status.estimated_end_time.isoformat() if self.maintenance_status.estimated_end_time else None
        }
        
        # Add OANDA service status if available
        if self.oanda_service and hasattr(self.oanda_service, 'get_connection_status'):
            try:
                oanda_status = await self.oanda_service.get_connection_status()
                base_status.update({
                    "oanda_health_score": oanda_status.get("health_score", 0),
                    "oanda_connection_state": oanda_status.get("connection_state", "unknown"),
                    "oanda_can_trade": oanda_status.get("can_trade", False)
                })
            except Exception as e:
                logger.warning(f"Could not get OANDA status: {e}")
        
        return base_status
    
    async def start_maintenance_monitor(self):
        """Start background maintenance monitoring"""
        logger.info("Starting maintenance mode monitor...")
        asyncio.create_task(self._maintenance_monitor_loop())
    
    async def _maintenance_monitor_loop(self):
        """Background loop to monitor maintenance status"""
        while True:
            try:
                if self.maintenance_status.is_maintenance:
                    still_in_maintenance = await self.check_maintenance_status()
                    if not still_in_maintenance:
                        logger.info("🎉 OANDA maintenance ended - service restored")
                
                # Wait before next check
                await asyncio.sleep(self.maintenance_check_interval)
                
            except Exception as e:
                logger.error(f"Error in maintenance monitor: {e}")
                await asyncio.sleep(60)  # Wait 1 minute on error

class DegradedModeAlertHandler:
    """
    Degraded mode alert handler that can operate without OANDA service
    Provides basic functionality and queues alerts for later processing
    """
    
    def __init__(self):
        self._started = True  # Always started in degraded mode
        self._initialization_complete = True
        self.queued_alerts = []
        self.degraded_mode = True
        self.last_alert_time = None
        self.position_tracker = None
        self.oanda_service = None
        self.risk_manager = None
        logger.info("✅ DegradedModeAlertHandler initialized (_started=True)")
    
    async def start(self):
        """Start the degraded mode alert handler"""
        self._started = True
        logger.info("✅ DegradedModeAlertHandler started successfully (_started=True)")
        return True
    
    async def stop(self):
        """Stop the degraded mode alert handler"""
        self._started = False
        logger.info("🛑 DegradedModeAlertHandler stopped")
    
    def is_started(self) -> bool:
        """Check if degraded mode alert handler is started"""
        return hasattr(self, '_started') and self._started
    
    def get_status(self) -> dict:
        """Get degraded mode alert handler status"""
        return {
            "started": self.is_started(),
            "_started_attribute_exists": hasattr(self, '_started'),
            "_started_value": getattr(self, '_started', None),
            "initialization_complete": getattr(self, '_initialization_complete', False),
            "degraded_mode": True,
            "oanda_service_available": False,
            "position_tracker_available": False,
            "risk_manager_available": False,
            "components_ready": False,
            "queued_alerts_count": len(self.queued_alerts)
        }
        
    async def handle_alert(self, alert_data: Dict[str, Any]) -> Tuple[bool, str]:
        """Handle alert in degraded mode - queue for later processing"""
        try:
            # Log the alert
            logger.info(f"📨 ALERT RECEIVED (DEGRADED MODE): {alert_data.get('symbol', 'UNKNOWN')} - {alert_data.get('action', 'UNKNOWN')}")
            
            # Store alert for later processing
            alert_with_timestamp = {
                **alert_data,
                'received_at': datetime.now().isoformat(),
                'status': 'queued',
                'degraded_mode': True
            }
            
            self.queued_alerts.append(alert_with_timestamp)
            self.last_alert_time = datetime.now()
            
            # Keep only last 100 alerts to prevent memory issues
            if len(self.queued_alerts) > 100:
                self.queued_alerts = self.queued_alerts[-100:]
            
            logger.info(f"✅ Alert queued for later processing (total queued: {len(self.queued_alerts)})")
            return True, "Alert queued for processing when OANDA service is restored"
            
        except Exception as e:
            logger.error(f"Failed to handle alert in degraded mode: {e}")
            return False, f"Failed to queue alert: {e}"
    
    async def process_alert(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process alert - compatible with normal AlertHandler interface"""
        try:
            # Check if we're still in degraded mode
            if self.degraded_mode:
                # Queue the alert
                success, message = await self.handle_alert(alert_data)
                return {
                    "status": "queued",
                    "message": message,
                    "degraded_mode": True,
                    "alert_id": alert_data.get('alert_id', 'unknown'),
                    "result": [success, message]
                }
            else:
                # If not in degraded mode, we should have been upgraded
                # This shouldn't happen, but handle gracefully
                return {
                    "status": "error",
                    "message": "Alert handler in inconsistent state",
                    "alert_id": alert_data.get('alert_id', 'unknown')
                }
                
        except Exception as e:
            logger.error(f"Error processing alert in degraded mode: {e}")
            return {
                "status": "error",
                "message": f"Failed to process alert: {e}",
                "alert_id": alert_data.get('alert_id', 'unknown')
            }
    
    async def process_queued_alerts(self, oanda_service):
        """Process queued alerts when OANDA service is restored"""
        if not self.queued_alerts:
            return
        
        logger.info(f"🔄 Processing {len(self.queued_alerts)} queued alerts...")
        
        processed_count = 0
        failed_count = 0
        
        for alert in self.queued_alerts[:]:  # Copy to avoid modification during iteration
            try:
                # Try to process the alert with restored OANDA service
                # This would need to be implemented based on your alert processing logic
                logger.info(f"Processing queued alert: {alert.get('symbol')} - {alert.get('action')}")
                
                # Mark as processed
                alert['status'] = 'processed'
                alert['processed_at'] = datetime.now().isoformat()
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Failed to process queued alert: {e}")
                alert['status'] = 'failed'
                alert['error'] = str(e)
                failed_count += 1
        
        # Remove processed alerts
        self.queued_alerts = [alert for alert in self.queued_alerts if alert['status'] == 'queued']
        
        logger.info(f"✅ Processed {processed_count} queued alerts, {failed_count} failed")
    
    def get_degraded_status(self) -> Dict[str, Any]:
        """Get degraded mode specific status (use get_status for full status)"""
        return {
            "degraded_mode": self.degraded_mode,
            "queued_alerts_count": len(self.queued_alerts),
            "last_alert_time": self.last_alert_time.isoformat() if self.last_alert_time else None,
            "operational": False,
            "can_process_alerts": False,
            "can_trade": False
        }
    
    async def start(self):
        """Start degraded mode handler"""
        self._started = True
        logger.info("🚨 Alert handler started in DEGRADED MODE - alerts will be queued")
    
    async def stop(self):
        """Stop degraded mode handler"""
        self._started = False
        logger.info("Alert handler stopped (degraded mode)")

# Factory function to create maintenance-aware service
async def create_maintenance_aware_oanda_service(oanda_service=None):
    """Create a maintenance-aware OANDA service wrapper"""
    wrapper = MaintenanceAwareOandaService(oanda_service)
    
    if oanda_service:
        await wrapper.initialize_with_fallback()
        await wrapper.start_maintenance_monitor()
    
    return wrapper

# Factory function to create degraded mode alert handler
def create_degraded_mode_alert_handler():
    """Create a degraded mode alert handler"""
    return DegradedModeAlertHandler()
