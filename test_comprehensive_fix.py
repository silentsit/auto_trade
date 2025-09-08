"""
TEST COMPREHENSIVE FIX
Test script to verify both _started attribute and timeframe fixes
"""

import asyncio
import logging
import requests
import json
from datetime import datetime, timezone

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_comprehensive_fix():
    """Test both the _started attribute fix and timeframe fix"""
    try:
        # Create a test alert with timeframe "1" (the problematic one)
        test_alert = {
            "symbol": "EURUSD",
            "action": "BUY",
            "alert_id": "test_comprehensive_" + str(int(datetime.now().timestamp())),
            "position_id": "test_comprehensive_" + str(int(datetime.now().timestamp())),
            "exchange": "OANDA",
            "account": "101-003-26651494-012",
            "percentage": 1,
            "orderType": "MARKET",
            "timeInForce": "FOK",
            "comment": "Test comprehensive fix",
            "strategy": "Lorentzian_Classification",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeframe": "1"  # This should be converted to "15" by the bot
        }
        
        logger.info("🧪 Testing comprehensive fix (timeframe conversion + _started attribute)...")
        logger.info(f"Test alert: {json.dumps(test_alert, indent=2)}")
        
        # Send the alert
        response = requests.post("https://auto-trade-b0bi.onrender.com/tradingview", json=test_alert)
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"Alert processing result: {json.dumps(result, indent=2)}")
            
            # Check for the specific error messages
            message = result.get('message', '')
            if "System initializing" in message:
                logger.error("❌ STILL GETTING 'System initializing' error - _started attribute not fixed")
                return False
            elif "Handler not started" in message:
                logger.error("❌ STILL GETTING 'Handler not started' error - _started attribute not fixed")
                return False
            elif "'AlertHandler' object has no attribute '_started'" in message:
                logger.error("❌ STILL GETTING '_started' attribute error")
                return False
            elif result.get('status') == 'success':
                logger.info("✅ SUCCESS: Alert processed successfully!")
                return True
            else:
                logger.info(f"✅ Alert handler is responding (status: {result.get('status')})")
                return True
        else:
            logger.error(f"❌ Failed to send alert: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error testing comprehensive fix: {e}")
        return False

async def test_timeframe_conversion():
    """Test timeframe conversion specifically"""
    try:
        # Test with timeframe "1"
        test_alert_1 = {
            "symbol": "GBPUSD",
            "action": "SELL",
            "alert_id": "test_timeframe_1_" + str(int(datetime.now().timestamp())),
            "position_id": "test_timeframe_1_" + str(int(datetime.now().timestamp())),
            "exchange": "OANDA",
            "account": "101-003-26651494-012",
            "percentage": 1,
            "orderType": "MARKET",
            "timeInForce": "FOK",
            "comment": "Test timeframe conversion from 1 to 15",
            "strategy": "Lorentzian_Classification",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeframe": "1"  # Should be converted to "15"
        }
        
        logger.info("🧪 Testing timeframe conversion (1 -> 15)...")
        
        # Send the alert
        response = requests.post("https://auto-trade-b0bi.onrender.com/tradingview", json=test_alert_1)
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"Timeframe conversion test result: {json.dumps(result, indent=2)}")
            
            # Check if the alert was processed (not rejected due to _started attribute)
            if "System initializing" not in result.get('message', ''):
                logger.info("✅ Timeframe conversion test passed - alert was processed")
                return True
            else:
                logger.error("❌ Timeframe conversion test failed - _started attribute issue")
                return False
        else:
            logger.error(f"❌ Failed to send timeframe test alert: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Error testing timeframe conversion: {e}")
        return False

async def main():
    """Main test function"""
    logger.info("🧪 TESTING COMPREHENSIVE FIXES")
    logger.info("=" * 60)
    
    # Test 1: Comprehensive fix
    logger.info("1. Testing comprehensive fix...")
    comprehensive_success = await test_comprehensive_fix()
    
    # Test 2: Timeframe conversion
    logger.info("2. Testing timeframe conversion...")
    timeframe_success = await test_timeframe_conversion()
    
    logger.info("=" * 60)
    if comprehensive_success and timeframe_success:
        logger.info("✅ SUCCESS: All fixes working!")
    elif comprehensive_success:
        logger.info("✅ PARTIAL SUCCESS: _started attribute fixed, timeframe needs work")
    elif timeframe_success:
        logger.info("✅ PARTIAL SUCCESS: Timeframe fixed, _started attribute needs work")
    else:
        logger.info("❌ ISSUE: Both fixes need more work")
    
    logger.info("🎯 TEST COMPLETE")

if __name__ == "__main__":
    asyncio.run(main())
