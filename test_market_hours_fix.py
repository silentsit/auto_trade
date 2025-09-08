"""
TEST MARKET HOURS FIX
Test script to verify the market hours fix and force OANDA reconnection
"""

import asyncio
import logging
import requests
import json
from datetime import datetime, timezone

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_market_status():
    """Test market status endpoint"""
    try:
        response = requests.get("https://auto-trade-b0bi.onrender.com/api/market-status")
        if response.status_code == 200:
            status = response.json()
            logger.info(f"Market Status: {json.dumps(status, indent=2)}")
            return status
        else:
            logger.error(f"Failed to get market status: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error getting market status: {e}")
        return None

async def test_force_reconnect():
    """Test force reconnection endpoint"""
    try:
        logger.info("🔄 Testing force reconnection...")
        response = requests.post("https://auto-trade-b0bi.onrender.com/api/force-reconnect")
        if response.status_code == 200:
            result = response.json()
            logger.info(f"Force Reconnect Result: {json.dumps(result, indent=2)}")
            return result
        else:
            logger.error(f"Failed to force reconnect: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error forcing reconnection: {e}")
        return None

async def test_process_queued_alerts():
    """Test processing queued alerts"""
    try:
        logger.info("📨 Testing queued alerts processing...")
        response = requests.post("https://auto-trade-b0bi.onrender.com/api/process-queued-alerts")
        if response.status_code == 200:
            result = response.json()
            logger.info(f"Queued Alerts Result: {json.dumps(result, indent=2)}")
            return result
        else:
            logger.error(f"Failed to process queued alerts: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error processing queued alerts: {e}")
        return None

async def test_system_status():
    """Test system status endpoint"""
    try:
        response = requests.get("https://auto-trade-b0bi.onrender.com/api/status/components")
        if response.status_code == 200:
            status = response.json()
            logger.info(f"System Components Status: {json.dumps(status, indent=2)}")
            return status
        else:
            logger.error(f"Failed to get system status: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        return None

async def main():
    """Main test function"""
    logger.info("🧪 TESTING MARKET HOURS FIX")
    logger.info("=" * 50)
    
    # Test 1: Check current market status
    logger.info("1. Checking current market status...")
    market_status = await test_market_status()
    
    # Test 2: Check system components
    logger.info("2. Checking system components...")
    system_status = await test_system_status()
    
    # Test 3: Force OANDA reconnection
    logger.info("3. Forcing OANDA reconnection...")
    reconnect_result = await test_force_reconnect()
    
    # Test 4: Process queued alerts
    logger.info("4. Processing queued alerts...")
    queued_result = await test_process_queued_alerts()
    
    # Test 5: Check status again after reconnection
    logger.info("5. Checking status after reconnection...")
    await asyncio.sleep(5)  # Wait a bit for reconnection
    final_status = await test_market_status()
    
    logger.info("=" * 50)
    logger.info("🎯 TEST COMPLETE")
    
    # Summary
    if market_status and market_status.get('can_trade'):
        logger.info("✅ SUCCESS: System can now trade!")
    else:
        logger.info("❌ ISSUE: System still cannot trade")
    
    if queued_result and queued_result.get('processed_count', 0) > 0:
        logger.info(f"✅ SUCCESS: Processed {queued_result['processed_count']} queued alerts")
    else:
        logger.info("ℹ️ INFO: No queued alerts to process")

if __name__ == "__main__":
    asyncio.run(main())
