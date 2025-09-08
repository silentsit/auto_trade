"""
TEST ALERT HANDLER FIX
Test script to verify the alert handler _started attribute fix
"""

import asyncio
import logging
import requests
import json
from datetime import datetime, timezone

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_alert_processing():
    """Test alert processing with the fixed alert handler"""
    try:
        # Create a test alert
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
            "comment": "Test alert handler fix",
            "strategy": "Test",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeframe": "15"
        }
        
        logger.info("📨 Testing alert processing with fixed alert handler...")
        logger.info(f"Test alert: {json.dumps(test_alert, indent=2)}")
        
        # Send the alert
        response = requests.post("https://auto-trade-b0bi.onrender.com/tradingview", json=test_alert)
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ Alert processing result: {json.dumps(result, indent=2)}")
            
            # Check if the error is fixed
            if "AlertHandler' object has no attribute '_started'" in str(result):
                logger.error("❌ ERROR STILL PRESENT: _started attribute issue not fixed")
                return False
            else:
                logger.info("✅ SUCCESS: No _started attribute error detected")
                return True
        else:
            logger.error(f"❌ Failed to send alert: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error testing alert processing: {e}")
        return False

async def test_system_status():
    """Test system status to see current state"""
    try:
        response = requests.get("https://auto-trade-b0bi.onrender.com/api/status/components")
        if response.status_code == 200:
            status = response.json()
            logger.info(f"System Status: {json.dumps(status, indent=2)}")
            return status
        else:
            logger.error(f"Failed to get system status: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        return None

async def main():
    """Main test function"""
    logger.info("🧪 TESTING ALERT HANDLER FIX")
    logger.info("=" * 50)
    
    # Test 1: Check system status
    logger.info("1. Checking system status...")
    status = await test_system_status()
    
    # Test 2: Test alert processing
    logger.info("2. Testing alert processing...")
    success = await test_alert_processing()
    
    logger.info("=" * 50)
    if success:
        logger.info("✅ SUCCESS: Alert handler fix working!")
    else:
        logger.info("❌ ISSUE: Alert handler still has problems")
    
    logger.info("🎯 TEST COMPLETE")

if __name__ == "__main__":
    asyncio.run(main())
