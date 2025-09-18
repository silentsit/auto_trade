#!/usr/bin/env python3
"""
Test TradingView format exactly as provided by user
"""

import asyncio
import aiohttp
import json
from datetime import datetime, timezone

async def test_tradingview_buy_signal():
    webhook_url = "https://auto-trade-b0bi.onrender.com/tradingview"
    
    # Test with the exact format provided, but with real values substituted
    test_payload = {
        "symbol": "BTC_USD",  # Test BTC_USD as requested
        "action": "BUY",
        "alert_id": f"BTC_USD_60_{int(datetime.now().timestamp())}",
        "position_id": f"BTC_USD_60_{int(datetime.now().timestamp())}",
        "exchange": "OANDA", 
        "account": "101-003-26651494-012",
        "percentage": 10,
        "orderType": "MARKET",
        "timeInForce": "FOK",
        "comment": "Open Long Signal",
        "strategy": "Lorentzian_Classification",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "timeframe": "60"
    }
    
    print("🧪 Testing BUY signal with your exact TradingView format...")
    print(f"📤 Payload: {json.dumps(test_payload, indent=2)}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                webhook_url,
                json=test_payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                response_text = await response.text()
                
                print(f"📥 Status: {response.status}")
                print(f"📥 Response: {response_text}")
                
                if response.status == 200:
                    try:
                        resp_json = json.loads(response_text)
                        
                        if "Failed to calculate ATR" in resp_json.get("message", ""):
                            print("❌ ATR calculation still failing")
                            print("🔧 Bot needs redeployment for ATR fixes to take effect")
                            return "atr_failed"
                        elif resp_json.get("status") == "success":
                            print("✅ BUY SIGNAL EXECUTED SUCCESSFULLY!")
                            print("🎉 Trade should now be open in your OANDA account")
                            print("📊 Check your OANDA Practice account for the new EUR_USD position")
                            return "success"
                        elif resp_json.get("status") == "error":
                            error_msg = resp_json.get("message", "Unknown error")
                            print(f"⚠️ Trade failed: {error_msg}")
                            
                            # Analyze the error type
                            if "ATR" in error_msg:
                                print("🔧 ATR issue - bot needs redeployment")
                                return "atr_failed"
                            elif "risk" in error_msg.lower():
                                print("🛡️ Risk management rejection - normal behavior")
                                return "risk_rejected"
                            elif "balance" in error_msg.lower():
                                print("💰 Account balance issue")
                                return "balance_issue"
                            elif "price" in error_msg.lower():
                                print("📈 Price data issue")
                                return "price_issue"
                            else:
                                print("🔍 Other issue")
                                return "other_error"
                        else:
                            print(f"📊 Unexpected response format: {resp_json}")
                            return "unexpected"
                    except json.JSONDecodeError:
                        print(f"❌ Could not parse JSON response: {response_text}")
                        return "json_error"
                else:
                    print(f"❌ HTTP error: {response.status}")
                    return "http_error"
                    
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return "request_failed"

async def main():
    print("🚀 Testing TradingView BUY Signal Format")
    print("=" * 60)
    
    result = await test_tradingview_buy_signal()
    
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS")
    print("=" * 60)
    
    if result == "success":
        print("✅ SUCCESS: Bot is working correctly!")
        print("🎯 Your TradingView signals should now execute trades")
        print("📋 Next steps:")
        print("   1. Check OANDA Practice account for the EUR_USD position")
        print("   2. Test with real TradingView alert")
        print("   3. Monitor trade execution")
        
    elif result == "atr_failed":
        print("🔧 ATR CALCULATION FAILING")
        print("💡 The fixes are implemented but need redeployment")
        print("📋 Solution:")
        print("   1. Bot needs to be redeployed on Render")
        print("   2. Changes are ready but not yet active")
        print("   3. After redeploy, signals should work")
        
    elif result == "risk_rejected":
        print("🛡️ RISK MANAGEMENT REJECTION")
        print("💡 Bot is working but rejecting trade for safety")
        print("📋 Check:")
        print("   1. Account balance sufficient")
        print("   2. Risk percentage settings")
        print("   3. Position size limits")
        
    else:
        print(f"⚠️ ISSUE DETECTED: {result}")
        print("💡 Bot is receiving signals but has other issues")
        print("📋 Investigation needed for specific error type")

if __name__ == "__main__":
    asyncio.run(main())
