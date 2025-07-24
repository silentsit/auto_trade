#!/usr/bin/env python3
"""
Test script to verify close signal processing fix
Run this to test various TradingView close signal formats
"""

import asyncio
import json
import aiohttp
import time
from datetime import datetime
import sys
import os
import logging
from typing import Dict, Any

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import _close_position
from alert_handler import EnhancedAlertHandler
from config import config
from database import PostgresDatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Test different close signal formats that TradingView might send
TEST_SIGNALS = [
    {
        "name": "Pine Script Message Format",
        "payload": {"message": "CLOSE_POSITION", "symbol": "EUR_USD"}
    },
    {
        "name": "Direct Action Format",
        "payload": {"action": "CLOSE", "symbol": "EUR_USD"}
    },
    {
        "name": "Direction Field Format",
        "payload": {"direction": "CLOSE", "symbol": "EUR_USD"}
    },
    {
        "name": "Side Field Format",
        "payload": {"side": "EXIT", "symbol": "EUR_USD"}
    },
    {
        "name": "Alert Condition Format",
        "payload": {"alertcondition": "Close Position Signal", "symbol": "EUR_USD"}
    },
    {
        "name": "Mixed Format",
        "payload": {"message": "EXIT", "action": "STOP", "symbol": "EUR_USD"}
    },
    {
        "name": "JSON in Message Format",
        "payload": {"message": '{"action": "CLOSE", "symbol": "EUR_USD"}'}
    }
]

# Replace None with a real db_manager for testing
DATABASE_URL = "postgresql://silentsit:NURBHBK75ByrQVwMLtXvjVe2Nk9nurOa@dpg-d02aav6uk2gs73e9414g-a/auto_trade"

async def get_test_handler():
    db_manager = PostgresDatabaseManager(db_url=DATABASE_URL)
    try:
        await db_manager.initialize()
    except Exception as e:
        raise RuntimeError(f"Failed to initialize test db_manager: {e}")
    handler = EnhancedAlertHandler(db_manager=db_manager)
    await handler.start()
    return handler

async def test_close_signals(base_url="http://localhost:8000"):
    """Test various close signal formats"""
    
    print("🔧 Testing Close Signal Processing Fix")
    print("=" * 50)
    
    async with aiohttp.ClientSession() as session:
        for i, test_case in enumerate(TEST_SIGNALS, 1):
            print(f"\n📝 Test {i}: {test_case['name']}")
            print(f"   Payload: {test_case['payload']}")
            
            try:
                async with session.post(
                    f"{base_url}/debug/test-close-signal",
                    json=test_case['payload'],
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    result = await response.json()
                    
                    if response.status == 200:
                        if result.get('status') == 'ok':
                            alert_result = result.get('result', {})
                            if alert_result.get('status') == 'success':
                                print(f"   ✅ SUCCESS: Close signal detected and processed")
                                print(f"      Result: {alert_result.get('message', 'N/A')}")
                            elif alert_result.get('status') == 'error':
                                if 'No matching position found' in alert_result.get('message', ''):
                                    print(f"   ✅ SUCCESS: Close signal detected (no position to close)")
                                else:
                                    print(f"   ❌ ERROR: {alert_result.get('message', 'Unknown error')}")
                            else:
                                print(f"   ⚠️  UNKNOWN: {alert_result}")
                        else:
                            print(f"   ❌ FAILED: {result.get('error', 'Unknown error')}")
                    else:
                        print(f"   ❌ HTTP ERROR: {response.status}")
                        
            except asyncio.TimeoutError:
                print(f"   ⏱️  TIMEOUT: Request took too long")
            except Exception as e:
                print(f"   ❌ EXCEPTION: {str(e)}")
            
            await asyncio.sleep(0.5)  # Small delay between tests
    
    print("\n" + "=" * 50)
    print("🏁 Test completed!")
    print("\n📋 What to check:")
    print("   - All test cases should show 'Close signal detected'")
    print("   - Check the bot logs for '[CLOSE DETECTION]' messages")
    print("   - Verify TradingView webhooks now trigger position closes")

async def test_webhook_endpoint(base_url="http://localhost:8000"):
    """Test the actual webhook endpoint with a close signal"""
    
    print("\n🌐 Testing TradingView Webhook Endpoint")
    print("=" * 50)
    
    webhook_payload = {
        "message": "CLOSE_POSITION",
        "symbol": "EUR_USD",
        "timestamp": datetime.now().isoformat()
    }
    
    print(f"📤 Sending webhook: {webhook_payload}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{base_url}/tradingview",
                json=webhook_payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                result = await response.json()
                
                print(f"📥 Response ({response.status}): {result}")
                
                if response.status == 200 and result.get('status') == 'ok':
                    alert_result = result.get('result', {})
                    if 'CLOSE' in str(alert_result):
                        print("✅ Webhook successfully processed close signal!")
                    else:
                        print("⚠️  Webhook processed but may not have detected close signal")
                else:
                    print("❌ Webhook failed to process properly")
                    
    except Exception as e:
        print(f"❌ Webhook test failed: {str(e)}")

async def test_close_position_error_handling():
    """Test the improved _close_position function error handling"""
    print("\n=== Testing Position Close Error Handling ===")
    
    # Test with a symbol that likely doesn't have an open position
    test_symbols = ["EUR_USD", "GBP_JPY", "USD_JPY"]
    
    for symbol in test_symbols:
        try:
            print(f"\nTesting close for {symbol}...")
            result = await _close_position(symbol)
            print(f"Result: {json.dumps(result, indent=2)}")
            
            # Verify the result structure
            assert "status" in result, "Result must contain 'status' field"
            assert result["status"] in ["success", "error"], "Status must be 'success' or 'error'"
            assert "message" in result, "Result must contain 'message' field"
            
            if result["status"] == "success":
                print(f"✅ Close operation handled gracefully for {symbol}")
            else:
                print(f"❌ Close operation failed for {symbol}: {result['message']}")
                
        except Exception as e:
            print(f"❌ Unexpected error testing {symbol}: {str(e)}")

async def test_close_signal_processing():
    """Test close signal processing through alert handler"""
    print("\n=== Testing Close Signal Processing ===")
    
    handler = EnhancedAlertHandler(db_manager=None)  # TODO: Replace None with actual db_manager if available
    
    # Sample close signal that was causing issues
    close_alert = {
        "symbol": "GBPJPY",
        "action": "CLOSE", 
        "alert_id": "test_close_signal",
        "position_id": "GBPJPY_60_test",
        "exchange": "OANDA",
        "account": config.oanda_account_id,
        "orderType": "MARKET",
        "timeInForce": "FOK",
        "comment": "Close Long Signal",
        "strategy": "Lorentzian_Classification",
        "timestamp": "2025-07-03T11:00:00Z",
        "timeframe": "60",
        "direction": "CLOSE"
    }
    
    try:
        print("Processing close signal...")
        result = await handler.process_alert(close_alert)
        print(f"Result: {json.dumps(result, indent=2)}")
        
        # Check result structure
        assert "status" in result, "Result must contain 'status' field"
        assert "alert_id" in result, "Result must contain 'alert_id' field"
        
        if result["status"] == "success":
            print("✅ Close signal processed successfully")
        elif result["status"] == "error" and "doesn't exist" in result.get("message", "").lower():
            print("✅ Close signal handled gracefully (position doesn't exist)")
        else:
            print(f"⚠️  Close signal result: {result['status']} - {result.get('message', 'No message')}")
            
    except Exception as e:
        print(f"❌ Error processing close signal: {str(e)}")

async def main():
    """Run all tests"""
    print("🚀 Starting Position Close Tests")
    print("="*50)
    
    try:
        await test_close_position_error_handling()
        await test_close_signal_processing()
        
        print("\n" + "="*50)
        print("✅ All tests completed!")
        
    except Exception as e:
        print(f"\n❌ Test suite failed: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 