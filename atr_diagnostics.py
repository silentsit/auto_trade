#!/usr/bin/env python3
"""
ATR DIAGNOSTICS TOOL
Test ATR calculation and OANDA historical data fetching
"""

import asyncio
import aiohttp
import json
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_atr_calculation():
    """Test ATR calculation with bot's webhook endpoint"""
    webhook_url = "https://auto-trade-b0bi.onrender.com/tradingview"
    
    # Create a simple test payload that should trigger ATR calculation
    test_payload = {
        "symbol": "EUR_USD",  # Use underscore format that OANDA expects
        "action": "BUY",
        "alert_id": f"ATR_TEST_{int(datetime.now().timestamp())}",
        "position_id": f"EUR_USD_ATR_TEST_{int(datetime.now().timestamp())}",
        "exchange": "OANDA",
        "account": "101-003-26651494-012",
        "orderType": "MARKET",
        "timeInForce": "FOK",
        "comment": "ATR Diagnostic Test",
        "strategy": "ATR_Diagnostics",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "timeframe": "H1"  # This should work with OANDA
    }
    
    logger.info("🧪 Testing ATR calculation with EUR_USD...")
    logger.info(f"📤 Payload: {json.dumps(test_payload, indent=2)}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                webhook_url,
                json=test_payload,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                response_text = await response.text()
                
                logger.info(f"📥 Status: {response.status}")
                logger.info(f"📥 Response: {response_text}")
                
                if response.status == 200:
                    try:
                        response_json = json.loads(response_text)
                        
                        if "Failed to calculate ATR" in response_json.get("message", ""):
                            logger.error("❌ ATR calculation failed!")
                            logger.error("🔍 This indicates OANDA historical data is not available")
                            
                            # Suggest solutions
                            logger.info("\n💡 POTENTIAL SOLUTIONS:")
                            logger.info("1. OANDA Practice environment may have limited historical data")
                            logger.info("2. Symbol format might be incorrect (EUR_USD vs EURUSD)")
                            logger.info("3. Granularity H1 might not be available")
                            logger.info("4. OANDA connection issues")
                            
                            return "atr_failed"
                        else:
                            logger.info("✅ ATR calculation appears to be working")
                            return "atr_success"
                            
                    except json.JSONDecodeError:
                        logger.error(f"❌ Could not parse JSON response: {response_text}")
                        return "json_error"
                else:
                    logger.error(f"❌ HTTP error: {response.status}")
                    return "http_error"
                    
    except Exception as e:
        logger.error(f"❌ Request failed: {e}")
        return "request_failed"

async def test_different_symbols():
    """Test ATR with different symbol formats"""
    webhook_url = "https://auto-trade-b0bi.onrender.com/tradingview"
    
    # Test different symbol formats
    symbols_to_test = [
        "EUR_USD",    # Standard OANDA format
        "EURUSD",     # TradingView format
        "GBP_USD",    # Another major pair
        "USD_JPY"     # JPY pair (different decimal places)
    ]
    
    results = {}
    
    for symbol in symbols_to_test:
        logger.info(f"\n🧪 Testing symbol: {symbol}")
        
        test_payload = {
            "symbol": symbol,
            "action": "BUY",
            "alert_id": f"SYMBOL_TEST_{symbol}_{int(datetime.now().timestamp())}",
            "position_id": f"{symbol}_TEST_{int(datetime.now().timestamp())}",
            "exchange": "OANDA",
            "account": "101-003-26651494-012",
            "orderType": "MARKET",
            "timeInForce": "FOK",
            "timeframe": "H1"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    webhook_url,
                    json=test_payload,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    response_text = await response.text()
                    
                    if "Failed to calculate ATR" in response_text:
                        results[symbol] = "❌ ATR Failed"
                        logger.info(f"  {symbol}: ❌ ATR calculation failed")
                    elif "status" in response_text and "error" in response_text:
                        results[symbol] = "⚠️ Other Error"
                        logger.info(f"  {symbol}: ⚠️ Other error (not ATR)")
                    else:
                        results[symbol] = "✅ Success"
                        logger.info(f"  {symbol}: ✅ ATR calculation worked")
                        
        except Exception as e:
            results[symbol] = f"❌ Exception: {str(e)}"
            logger.error(f"  {symbol}: ❌ Exception: {e}")
        
        await asyncio.sleep(1)  # Small delay between tests
    
    return results

async def check_oanda_connection():
    """Check OANDA connection status via bot health endpoint"""
    health_url = "https://auto-trade-b0bi.onrender.com/"
    
    logger.info("🏥 Checking bot health and OANDA connection...")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(health_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                response_text = await response.text()
                
                if response.status == 200:
                    try:
                        data = json.loads(response_text)
                        logger.info(f"✅ Bot Status: {data.get('status', 'unknown')}")
                        logger.info(f"📊 Market Status: {data.get('market_status', 'unknown')}")
                        logger.info(f"🕐 Timestamp: {data.get('timestamp', 'unknown')}")
                        
                        return data
                    except json.JSONDecodeError:
                        logger.error(f"❌ Could not parse health response: {response_text}")
                        return None
                else:
                    logger.error(f"❌ Health check failed: {response.status}")
                    return None
                    
    except Exception as e:
        logger.error(f"❌ Health check exception: {e}")
        return None

async def main():
    """Run comprehensive ATR diagnostics"""
    logger.info("🚀 Starting ATR Diagnostics")
    logger.info("=" * 60)
    
    # 1. Check bot health
    health_data = await check_oanda_connection()
    
    if not health_data:
        logger.error("❌ Bot health check failed - cannot proceed with ATR tests")
        return
    
    # 2. Test basic ATR calculation
    atr_result = await test_atr_calculation()
    
    # 3. Test different symbols
    logger.info("\n" + "=" * 60)
    logger.info("🧪 Testing ATR with different symbols...")
    symbol_results = await test_different_symbols()
    
    # 4. Summary
    logger.info("\n" + "=" * 60)
    logger.info("📊 DIAGNOSTIC SUMMARY")
    logger.info("=" * 60)
    
    logger.info(f"Bot Health: {'✅ OK' if health_data else '❌ Failed'}")
    logger.info(f"ATR Test: {atr_result}")
    
    logger.info("\nSymbol Test Results:")
    for symbol, result in symbol_results.items():
        logger.info(f"  {symbol}: {result}")
    
    # Determine root cause
    failed_symbols = [s for s, r in symbol_results.items() if "ATR Failed" in r]
    
    if len(failed_symbols) == len(symbol_results):
        logger.info("\n🔍 ROOT CAUSE: ALL symbols failing ATR calculation")
        logger.info("💡 LIKELY ISSUE: OANDA historical data connection problem")
        logger.info("🛠️  SOLUTION: Check OANDA Practice environment and API connectivity")
    elif len(failed_symbols) > 0:
        logger.info(f"\n🔍 ROOT CAUSE: Some symbols failing ({failed_symbols})")
        logger.info("💡 LIKELY ISSUE: Symbol format or specific instrument issues")
        logger.info("🛠️  SOLUTION: Fix symbol formatting in TradingView alerts")
    else:
        logger.info("\n✅ ATR calculation is working correctly!")
        logger.info("💡 If trades still fail, check other components")

if __name__ == "__main__":
    asyncio.run(main())
