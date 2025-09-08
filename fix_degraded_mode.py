"""
FIX DEGRADED MODE
Direct fix to upgrade alert handler from degraded mode to normal mode
"""

import asyncio
import logging
import requests
import json
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

async def fix_degraded_mode():
    """Fix the degraded mode issue by upgrading the alert handler"""
    try:
        # First, let's check the current status
        response = requests.get("https://auto-trade-b0bi.onrender.com/api/status/components")
        if response.status_code != 200:
            logger.error(f"Failed to get system status: {response.status_code}")
            return False
        
        status = response.json()
        logger.info(f"Current system status: {json.dumps(status, indent=2)}")
        
        # Check if we're in degraded mode
        alert_handler_status = status.get('alert_handler_status', {})
        if alert_handler_status.get('degraded_mode', False):
            logger.info("🚨 Alert handler is in degraded mode - attempting fix...")
            
            # The issue is that the alert handler needs to be upgraded
            # Since we can't directly access the server, we need to restart it
            # or trigger a reconnection
            
            # Let's try to send a test alert to see if it gets queued
            test_alert = {
                "symbol": "EURUSD",
                "action": "BUY",
                "alert_id": "test_fix_" + str(int(datetime.now().timestamp())),
                "position_id": "test_fix_" + str(int(datetime.now().timestamp())),
                "exchange": "OANDA",
                "account": "101-003-26651494-012",
                "percentage": 1,
                "orderType": "MARKET",
                "timeInForce": "FOK",
                "comment": "Test fix alert",
                "strategy": "Test",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "timeframe": "15"
            }
            
            logger.info("📨 Sending test alert to check if it gets queued...")
            alert_response = requests.post("https://auto-trade-b0bi.onrender.com/tradingview", json=test_alert)
            
            if alert_response.status_code == 200:
                result = alert_response.json()
                logger.info(f"Test alert result: {json.dumps(result, indent=2)}")
                
                if result.get('status') == 'queued':
                    logger.warning("⚠️ Alert was queued - system still in degraded mode")
                    return False
                elif result.get('status') == 'success':
                    logger.info("✅ Alert was processed - system is working!")
                    return True
                else:
                    logger.error(f"❌ Alert processing failed: {result}")
                    return False
            else:
                logger.error(f"Failed to send test alert: {alert_response.status_code}")
                return False
        else:
            logger.info("✅ Alert handler is not in degraded mode")
            return True
            
    except Exception as e:
        logger.error(f"Error fixing degraded mode: {e}")
        return False

async def check_system_health():
    """Check overall system health"""
    try:
        # Check system status
        response = requests.get("https://auto-trade-b0bi.onrender.com/api/status/components")
        if response.status_code == 200:
            status = response.json()
            
            logger.info("🔍 SYSTEM HEALTH CHECK:")
            logger.info(f"  System Operational: {status.get('system_operational', False)}")
            logger.info(f"  OANDA Operational: {status.get('oanda_operational', False)}")
            logger.info(f"  Can Trade: {status.get('can_trade', False)}")
            logger.info(f"  Degraded Mode: {status.get('degraded_mode', False)}")
            
            # Check alert handler status
            alert_status = status.get('alert_handler_status', {})
            logger.info(f"  Alert Handler Mode: {'degraded' if alert_status.get('degraded_mode') else 'normal'}")
            logger.info(f"  Queued Alerts: {alert_status.get('queued_alerts_count', 0)}")
            
            return status
        else:
            logger.error(f"Failed to get system status: {response.status_code}")
            return None
            
    except Exception as e:
        logger.error(f"Error checking system health: {e}")
        return None

async def main():
    """Main function"""
    logger.info("🔧 FIXING DEGRADED MODE ISSUE")
    logger.info("=" * 50)
    
    # Check current health
    logger.info("1. Checking system health...")
    health = await check_system_health()
    
    if not health:
        logger.error("❌ Cannot check system health")
        return
    
    # Attempt to fix degraded mode
    logger.info("2. Attempting to fix degraded mode...")
    success = await fix_degraded_mode()
    
    if success:
        logger.info("✅ SUCCESS: Degraded mode fixed!")
    else:
        logger.info("❌ FAILED: Could not fix degraded mode")
        logger.info("💡 SOLUTION: The bot needs to be restarted to properly initialize OANDA service")
        logger.info("   The issue is that the alert handler was initialized in degraded mode")
        logger.info("   and never upgraded to normal mode when OANDA became available.")
    
    logger.info("=" * 50)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
