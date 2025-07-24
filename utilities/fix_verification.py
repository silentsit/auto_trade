#!/usr/bin/env python3
"""
Fix Verification Script for Trading Bot Issues
Run this after deploying to verify all fixes are working
"""

import asyncio
import aiohttp
import json
from datetime import datetime

class TradingBotTester:
    def __init__(self, base_url="https://auto-trade-b0bi.onrender.com"):
        self.base_url = base_url.rstrip('/')
        
    async def test_json_fix(self):
        """Test that the JSON import fix resolved the UnboundLocalError"""
        print("🔧 Testing JSON Fix...")
        
        # Test various close signal formats that previously caused JSON errors
        test_signals = [
            {"message": "CLOSE_POSITION", "symbol": "EUR_USD"},
            {"action": "CLOSE", "symbol": "GBP_USD"},
            {"direction": "CLOSE", "symbol": "USD_JPY"},
            {"alertcondition": "Close Position ▲▼", "symbol": "AUD_USD"},
            {"side": "EXIT", "symbol": "NZD_USD"}
        ]
        
        async with aiohttp.ClientSession() as session:
            for i, signal in enumerate(test_signals, 1):
                try:
                    async with session.post(
                        f"{self.base_url}/debug/test-close-signal",
                        json=signal,
                        timeout=10
                    ) as response:
                        result = await response.json()
                        
                        if "json" in str(result).lower() and "error" in str(result).lower():
                            print(f"   ❌ Test {i}: Still has JSON error - {result}")
                        else:
                            print(f"   ✅ Test {i}: JSON processing working - {signal['symbol']}")
                            
                except Exception as e:
                    print(f"   ⚠️  Test {i}: Connection error - {e}")
        
        print()

    async def test_stale_position_cleanup(self):
        """Test cleanup of stale positions"""
        print("🧹 Testing Stale Position Cleanup...")
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    f"{self.base_url}/debug/cleanup-stale-positions",
                    timeout=15
                ) as response:
                    result = await response.json()
                    
                    if result.get("status") == "success":
                        found = result.get("stale_positions_found", 0)
                        cleaned = result.get("positions_cleaned", 0)
                        print(f"   ✅ Found {found} stale positions, cleaned {cleaned}")
                        
                        if found > 0:
                            print("   📋 Stale position details:")
                            for pos in result.get("details", []):
                                print(f"      - {pos['symbol']} (ID: {pos['position_id'][:20]}..., Age: {pos['age_days']} days)")
                    else:
                        print(f"   ❌ Cleanup failed: {result.get('error', 'Unknown error')}")
                        
            except Exception as e:
                print(f"   ⚠️  Cleanup test failed: {e}")
        
        print()

    async def test_webhook_endpoint(self):
        """Test the main webhook endpoint with a realistic TradingView signal"""
        print("📡 Testing Main Webhook Endpoint...")
        
        # Simulate a TradingView signal similar to what caused the original error
        test_signal = {
            "symbol": "GBPUSD",
            "action": "SELL", 
            "alert_id": f"TEST_SIGNAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "position_id": f"GBPUSD_15_{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}Z",
            "exchange": "OANDA",
            "account": "101-003-26651494-011",
            "percentage": 10,
            "risk_percent": 10,
            "orderType": "MARKET",
            "timeInForce": "FOK",
            "comment": "Test Signal - Fix Verification",
            "strategy": "Lorentzian_Classification",
            "timestamp": datetime.now().strftime('%Y-%m-%dT%H:%M:%S') + "Z",
            "timeframe": "15"
        }
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    f"{self.base_url}/tradingview",
                    json=test_signal,
                    timeout=15
                ) as response:
                    result = await response.json()
                    
                    if "json" in str(result).lower() and "error" in str(result).lower():
                        print(f"   ❌ Webhook still has JSON error: {result}")
                    elif result.get("status") == "error":
                        print(f"   ⚠️  Webhook processed but returned error: {result.get('message', 'Unknown')}")
                    else:
                        print(f"   ✅ Webhook processing successful: {result.get('status', 'Unknown status')}")
                        
            except Exception as e:
                print(f"   ⚠️  Webhook test failed: {e}")
        
        print()

    async def check_system_status(self):
        """Check overall system health"""
        print("🏥 Checking System Health...")
        
        async with aiohttp.ClientSession() as session:
            try:
                # Test root endpoint
                async with session.get(f"{self.base_url}/", timeout=10) as response:
                    if response.status == 200:
                        print("   ✅ Main service online")
                    else:
                        print(f"   ❌ Service returned {response.status}")
                
                # Test health endpoint if it exists
                try:
                    async with session.get(f"{self.base_url}/health", timeout=5) as response:
                        if response.status == 200:
                            result = await response.json()
                            print(f"   ✅ Health check: {result}")
                        else:
                            print("   ⚠️  No health endpoint available")
                except:
                    print("   ⚠️  No health endpoint available")
                    
            except Exception as e:
                print(f"   ❌ System check failed: {e}")
        
        print()

    async def test_crypto_signal_handling(self):
        """Test that crypto signals are properly detected and rejected"""
        print("🔍 Testing Crypto Signal Handling...")
        
        # Test crypto signal rejection
        crypto_test_signals = [
            {"symbol": "BTC_USD", "action": "BUY", "message": "test"},
            {"symbol": "ETHUSD", "direction": "SELL", "message": "test"},
            {"symbol": "BTCUSD", "direction": "CLOSE", "message": "close position"}
        ]
        
        async with aiohttp.ClientSession() as session:
            for signal in crypto_test_signals:
                try:
                    async with session.post(
                        f"{self.base_url}/tradingview",
                        json=signal,
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:
                        result = await response.json()
                        
                        if result.get("status") == "rejected" and result.get("signal_type") == "crypto_unsupported":
                            print(f"   ✅ Crypto signal {signal['symbol']} properly rejected: {result.get('reason', 'No reason provided')[:60]}...")
                        else:
                            print(f"   ⚠️  Crypto signal {signal['symbol']} not properly handled: {result}")
                        
                except Exception as e:
                    print(f"   ❌ Error testing crypto signal {signal['symbol']}: {e}")
            
            # Check crypto signal stats
            try:
                async with session.get(
                    f"{self.base_url}/debug/crypto-signals",
                    timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status == 200:
                        stats = await response.json()
                        total_signals = stats.get('stats', {}).get('total_crypto_signals', 0)
                        print(f"   📊 Crypto Signal Stats: {total_signals} signals logged")
                    else:
                        print(f"   ⚠️  Could not fetch crypto stats: {response.status}")
            except Exception as e:
                print(f"   ⚠️  Error fetching crypto stats: {e}")
        
        print()

    async def test_direction_aware_close(self):
        """Test the enhanced direction-aware close signal matching"""
        print("🎯 Testing Direction-Aware Close Signal Matching...")
        
        # Test direction-aware close for different scenarios
        test_scenarios = [
            {"symbol": "EUR_USD", "target_direction": "BUY", "description": "Close BUY position"},
            {"symbol": "GBP_USD", "target_direction": "SELL", "description": "Close SELL position"},
            {"symbol": "USD_JPY", "target_direction": "BUY", "description": "Close BUY position (fallback test)"}
        ]
        
        async with aiohttp.ClientSession() as session:
            for scenario in test_scenarios:
                try:
                    test_payload = {
                        "symbol": scenario["symbol"],
                        "target_direction": scenario["target_direction"]
                    }
                    
                    async with session.post(
                        f"{self.base_url}/debug/test-direction-close",
                        json=test_payload,
                        timeout=aiohttp.ClientTimeout(total=15)
                    ) as response:
                        result = await response.json()
                        
                        if result.get("status") == "success":
                            test_result = result.get("result", {})
                            close_method = test_result.get("close_method", "unknown")
                            
                            if "direction_aware_fallback" in close_method:
                                print(f"   ✅ {scenario['description']}: Direction-aware matching worked ({close_method})")
                            elif test_result.get("status") == "error" and "No matching position found" in test_result.get("message", ""):
                                print(f"   ⚠️  {scenario['description']}: No {scenario['target_direction']} positions to close (expected if no positions)")
                            else:
                                print(f"   ✅ {scenario['description']}: Close processed ({test_result.get('status')})")
                        else:
                            print(f"   ❌ {scenario['description']}: Test failed - {result.get('error', 'Unknown error')}")
                            
                except Exception as e:
                    print(f"   ❌ Error testing {scenario['description']}: {e}")
            
            # Test comment-based direction detection (user's actual format)
            try:
                user_format_signals = [
                    {
                        "symbol": "EUR_USD",
                        "action": "CLOSE",
                        "alert_id": f"EURUSD_15_{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}Z",
                        "position_id": f"EURUSD_15_{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}Z",
                        "exchange": "OANDA",
                        "account": "101-003-26651494-011",
                        "orderType": "MARKET",
                        "timeInForce": "FOK",
                        "comment": "Close Long Signal",  # Should detect BUY direction
                        "strategy": "Lorentzian_Classification",
                        "timestamp": datetime.now().strftime('%Y-%m-%dT%H:%M:%S') + "Z",
                        "timeframe": "15"
                    },
                    {
                        "symbol": "GBP_USD",
                        "action": "CLOSE",
                        "alert_id": f"GBPUSD_15_{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}Z",
                        "position_id": f"GBPUSD_15_{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}Z",
                        "exchange": "OANDA",
                        "account": "101-003-26651494-011",
                        "orderType": "MARKET",
                        "timeInForce": "FOK",
                        "comment": "Close Short Signal",  # Should detect SELL direction
                        "strategy": "Lorentzian_Classification",
                        "timestamp": datetime.now().strftime('%Y-%m-%dT%H:%M:%S') + "Z",
                        "timeframe": "15"
                    }
                ]
                
                for i, signal in enumerate(user_format_signals, 1):
                    direction_type = "Long" if "Long" in signal["comment"] else "Short"
                    
                    async with session.post(
                        f"{self.base_url}/debug/test-close-signal",
                        json=signal,
                        timeout=aiohttp.ClientTimeout(total=15)
                    ) as response:
                        result = await response.json()
                        
                        if result.get("status") == "success":
                            print(f"   ✅ Comment-based detection {i} ({direction_type}): Working correctly")
                        else:
                            print(f"   ⚠️  Comment-based detection {i} ({direction_type}): {result.get('status', 'Unknown status')}")
                        
            except Exception as e:
                print(f"   ❌ Error testing comment-based direction detection: {e}")
        
        print()

    async def run_all_tests(self):
        """Run complete test suite"""
        print("=" * 60)
        print("🚀 TRADING BOT FIX VERIFICATION")
        print("=" * 60)
        print(f"Testing: {self.base_url}")
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        await self.check_system_status()
        await self.test_json_fix()
        await self.test_stale_position_cleanup()
        await self.test_webhook_endpoint()
        await self.test_crypto_signal_handling()
        await self.test_direction_aware_close()
        
        print("=" * 60)
        print("✅ Fix verification complete!")
        print("=" * 60)
        print()
        print("📋 Next Steps:")
        print("1. Deploy the fixes to GitHub/Render if not already done")
        print("2. Monitor logs for the JSON error disappearing")
        print("3. Test with real TradingView close signals")
        print("4. Check that old stale positions are properly cleaned up")
        print("5. For crypto trading, consider switching to OANDA live account or integrating crypto exchanges")
        print("6. Test direction-aware close signals to ensure proper position matching")
        print()
        print("🔍 Check crypto signal logs at: /debug/crypto-signals")
        print("🎯 Test direction-aware closes at: /debug/test-direction-close")
        print("💡 Crypto solutions available at the debug endpoint")
        print()

async def main():
    import sys
    
    # Allow custom URL
    url = sys.argv[1] if len(sys.argv) > 1 else "https://auto-trade-b0bi.onrender.com"
    
    print(f"Starting verification for: {url}")
    print("Press Ctrl+C to cancel\n")
    
    await asyncio.sleep(2)  # Give user time to read
    
    tester = TradingBotTester(url)
    await tester.run_all_tests()

if __name__ == "__main__":
    asyncio.run(main()) 