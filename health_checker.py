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
        
        # Check OANDA connection
        try:
            from main import robust_oanda_request
            from oandapyV20.endpoints.accounts import AccountDetails
            request = AccountDetails(accountID=config.oanda_account_id)
            await robust_oanda_request(request)
        except Exception:
            issues.append("OANDA connection failed")
        
        # Check for stale positions
        if hasattr(self.alert_handler, 'position_tracker'):
            stale_positions = await self._check_stale_positions()
            if stale_positions:
                issues.append(f"{len(stale_positions)} stale positions detected")
        
        if issues:
            await self._handle_health_issues(issues)
        
        self.last_heartbeat = datetime.now(timezone.utc)

    async def _check_stale_positions(self):
        # Placeholder: implement logic to detect stale positions
        return []

    async def _handle_health_issues(self, issues):
        # Placeholder: implement logic to handle health issues (e.g., alert, restart, escalate)
        logger.error(f"Health issues detected: {issues}") 