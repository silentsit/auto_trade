import asyncio
from datetime import datetime, timezone, timedelta
import logging
from config import config

logger = logging.getLogger("health_checker")

class HealthChecker:
    def __init__(self, alert_handler, db_manager):
        self.alert_handler = alert_handler
        self.db_manager = db_manager
        self.last_heartbeat = datetime.now(timezone.utc)
        self.oanda_connection_failures = 0
        self.max_connection_failures = 3
        
    async def start_monitoring(self):
        """Monitor system health every 30 seconds"""
        while True:
            try:
                await self.check_system_health()
                await asyncio.sleep(30)
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                await asyncio.sleep(60)
    
    async def check_system_health(self):
        """Check all critical components"""
        issues = []
        
        # Check database connection
        try:
            if not await self.db_manager.ensure_connection():
                issues.append("Database connection failed")
        except Exception:
            issues.append("Database unreachable")
        
        # Enhanced OANDA connection check
        try:
            await self._check_oanda_connection()
            self.oanda_connection_failures = 0  # Reset on success
        except Exception as e:
            self.oanda_connection_failures += 1
            issues.append(f"OANDA connection failed ({self.oanda_connection_failures}/{self.max_connection_failures}): {str(e)}")
            
            # Attempt recovery if we've had too many failures
            if self.oanda_connection_failures >= self.max_connection_failures:
                logger.warning(f"OANDA connection failed {self.oanda_connection_failures} times, attempting recovery")
                await self._recover_oanda_connection()
        
        # Check for stale positions
        if hasattr(self.alert_handler, 'position_tracker'):
            stale_positions = await self._check_stale_positions()
            if stale_positions:
                issues.append(f"{len(stale_positions)} stale positions detected")
        
        if issues:
            await self._handle_health_issues(issues)
        
        self.last_heartbeat = datetime.now(timezone.utc)
        
    async def _check_oanda_connection(self):
        """Enhanced OANDA connection check"""
        try:
            from main import robust_oanda_request
            from oandapyV20.endpoints.accounts import AccountDetails
            
            # Use a lightweight request to check connection
            request = AccountDetails(accountID=config.oanda_account_id)
            
            # Set a shorter timeout for health checks
            start_time = datetime.now(timezone.utc)
            response = await robust_oanda_request(request, max_retries=2, initial_delay=1.0)
            end_time = datetime.now(timezone.utc)
            
            response_time = (end_time - start_time).total_seconds()
            
            if response_time > 10.0:  # If response takes more than 10 seconds
                logger.warning(f"OANDA response time is slow: {response_time:.2f}s")
                
            logger.debug(f"OANDA health check passed in {response_time:.2f}s")
            
        except Exception as e:
            logger.error(f"OANDA health check failed: {e}")
            raise
    
    async def _recover_oanda_connection(self):
        """Attempt to recover OANDA connection"""
        try:
            logger.info("Attempting OANDA connection recovery...")
            
            # Try to reinitialize the OANDA client
            from main import initialize_oanda_client
            if initialize_oanda_client():
                logger.info("OANDA connection recovery successful")
                self.oanda_connection_failures = 0
            else:
                logger.error("OANDA connection recovery failed")
                
        except Exception as e:
            logger.error(f"Error during OANDA connection recovery: {e}")
    
    async def _check_stale_positions(self):
        """Check for positions that haven't been updated recently"""
        try:
            if not self.alert_handler.position_tracker:
                return []
                
            open_positions = await self.alert_handler.position_tracker.get_open_positions()
            stale_positions = []
            current_time = datetime.now(timezone.utc)
            
            for symbol, positions in open_positions.items():
                for position_id, position_data in positions.items():
                    last_update_str = position_data.get('last_update')
                    if last_update_str:
                        try:
                            from utils import parse_iso_datetime
                            last_update = parse_iso_datetime(last_update_str)
                            time_diff = (current_time - last_update).total_seconds()
                            
                            if time_diff > 900:  # 15 minutes
                                stale_positions.append({
                                    'position_id': position_id,
                                    'symbol': symbol,
                                    'last_update': last_update_str,
                                    'age_seconds': time_diff
                                })
                        except Exception:
                            continue
            
            return stale_positions
            
        except Exception as e:
            logger.error(f"Error checking stale positions: {e}")
            return []
    
    async def _handle_health_issues(self, issues):
        """Handle detected health issues"""
        for issue in issues:
            logger.warning(f"Health issue detected: {issue}")
            
            # You can add notification logic here if needed
            # For example, send alerts to Slack/Telegram
            
        # Record issues for monitoring
        if hasattr(self.alert_handler, 'system_monitor'):
            await self.alert_handler.system_monitor.update_component_status(
                "health_checker", 
                "warning" if len(issues) < 3 else "critical",
                f"Detected {len(issues)} health issues"
            ) 