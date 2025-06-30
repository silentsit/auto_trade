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
        self.weekend_position_check_task = None
        self._running = False
        
    async def start(self):
        """Start the health checker"""
        if self._running:
            return
        self._running = True
        
        # Start weekend position monitoring if enabled
        if config.enable_weekend_position_limits:
            self.weekend_position_check_task = asyncio.create_task(
                self._monitor_weekend_positions()
            )
            logger.info("Weekend position monitoring started")
    
    async def stop(self):
        """Stop the health checker"""
        self._running = False
        
        # Stop weekend position monitoring
        if self.weekend_position_check_task:
            self.weekend_position_check_task.cancel()
            try:
                await self.weekend_position_check_task
            except asyncio.CancelledError:
                pass
            logger.info("Weekend position monitoring stopped")
    
    async def _monitor_weekend_positions(self):
        """Continuously monitor weekend positions and close those exceeding age limits"""
        while self._running:
            try:
                await self._check_weekend_position_age_limits()
                await asyncio.sleep(config.weekend_position_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in weekend position monitoring: {e}")
                await asyncio.sleep(60)  # Wait before retrying
    
    async def _check_weekend_position_age_limits(self):
        """Check for weekend positions that exceed age limits and close them"""
        try:
            if not self.alert_handler.position_tracker:
                return
                
            open_positions = await self.alert_handler.position_tracker.get_open_positions()
            positions_to_close = []
            current_time = datetime.now(timezone.utc)
            
            for symbol, positions in open_positions.items():
                for position_id, position_data in positions.items():
                    # Get the actual Position object to check weekend status
                    position_obj = await self.alert_handler.position_tracker.get_position_object(position_id)
                    
                    if position_obj and position_obj.is_weekend_position():
                        # Update weekend status
                        position_obj.update_weekend_status()
                        weekend_age = position_obj.get_weekend_age_hours()
                        
                        # Check if position exceeds weekend age limit
                        if weekend_age >= config.weekend_position_max_age_hours:
                            positions_to_close.append({
                                'position_id': position_id,
                                'symbol': symbol,
                                'action': position_data.get('action', 'Unknown'),
                                'weekend_age_hours': weekend_age,
                                'position_obj': position_obj
                            })
                            logger.warning(
                                f"Weekend position {position_id} ({symbol}) exceeded age limit: "
                                f"{weekend_age:.1f}h >= {config.weekend_position_max_age_hours}h"
                            )
                        elif weekend_age >= (config.weekend_position_max_age_hours - config.weekend_auto_close_buffer_hours):
                            # Position is approaching limit - log warning
                            remaining_hours = config.weekend_position_max_age_hours - weekend_age
                            logger.info(
                                f"Weekend position {position_id} ({symbol}) approaching age limit: "
                                f"{weekend_age:.1f}h, {remaining_hours:.1f}h remaining"
                            )
            
            # Close positions that exceeded limits
            for pos_info in positions_to_close:
                await self._close_weekend_position(pos_info)
                
        except Exception as e:
            logger.error(f"Error checking weekend position age limits: {e}")
    
    async def _close_weekend_position(self, position_info: dict):
        """Close a weekend position that exceeded age limits"""
        try:
            position_id = position_info['position_id']
            symbol = position_info['symbol']
            action = position_info['action']
            weekend_age = position_info['weekend_age_hours']
            
            # Get current price for closing
            current_price = await self.alert_handler.get_current_price(symbol, action)
            
            # Close the position
            result = await self.alert_handler.position_tracker.close_position(
                position_id, 
                current_price, 
                reason=f"weekend_age_limit_{weekend_age:.1f}h"
            )
            
            if result and result.success:
                logger.info(
                    f"Successfully closed weekend position {position_id} ({symbol}) "
                    f"after {weekend_age:.1f} hours - Age limit: {config.weekend_position_max_age_hours}h"
                )
                
                # Send notification if available
                if hasattr(self.alert_handler, 'notification_system') and self.alert_handler.notification_system:
                    await self._send_weekend_closure_notification(position_info, result)
            else:
                logger.error(f"Failed to close weekend position {position_id}: {result.error if result else 'Unknown error'}")
                
        except Exception as e:
            logger.error(f"Error closing weekend position {position_info['position_id']}: {e}")
    
    async def _send_weekend_closure_notification(self, position_info: dict, close_result):
        """Send notification about weekend position closure"""
        try:
            message = (
                f"🕒 WEEKEND AGE LIMIT CLOSURE\n\n"
                f"Position: {position_info['position_id']}\n"
                f"Symbol: {position_info['symbol']}\n"
                f"Action: {position_info['action']}\n"
                f"Weekend Age: {position_info['weekend_age_hours']:.1f} hours\n"
                f"Age Limit: {config.weekend_position_max_age_hours} hours\n"
                f"Reason: Position exceeded weekend age limit\n"
                f"Status: Successfully closed"
            )
            
            await self.alert_handler.notification_system.send_message(
                message, 
                notification_type="weekend_closure"
            )
        except Exception as e:
            logger.error(f"Error sending weekend closure notification: {e}")
    
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
        
        # Check weekend position age limits
        if config.enable_weekend_position_limits:
            weekend_issues = await self._check_weekend_position_status()
            if weekend_issues:
                issues.extend(weekend_issues)
        
        if issues:
            await self._handle_health_issues(issues)
        
        self.last_heartbeat = datetime.now(timezone.utc)
        return {"status": "healthy" if not issues else "issues", "issues": issues, "timestamp": self.last_heartbeat}
    
    async def _check_weekend_position_status(self) -> list:
        """Check weekend position status for health reporting"""
        issues = []
        try:
            if not self.alert_handler.position_tracker:
                return issues
                
            open_positions = await self.alert_handler.position_tracker.get_open_positions()
            weekend_positions_count = 0
            approaching_limit_count = 0
            
            for symbol, positions in open_positions.items():
                for position_id, position_data in positions.items():
                    position_obj = await self.alert_handler.position_tracker.get_position_object(position_id)
                    
                    if position_obj and position_obj.is_weekend_position():
                        weekend_positions_count += 1
                        position_obj.update_weekend_status()
                        weekend_age = position_obj.get_weekend_age_hours()
                        
                        # Check if approaching limit
                        remaining_hours = config.weekend_position_max_age_hours - weekend_age
                        if remaining_hours <= config.weekend_auto_close_buffer_hours:
                            approaching_limit_count += 1
            
            if weekend_positions_count > 0:
                issues.append(f"{weekend_positions_count} weekend positions active")
                
            if approaching_limit_count > 0:
                issues.append(f"{approaching_limit_count} weekend positions approaching age limit")
                
        except Exception as e:
            logger.error(f"Error checking weekend position status: {e}")
            issues.append("Weekend position status check failed")
            
        return issues
        
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