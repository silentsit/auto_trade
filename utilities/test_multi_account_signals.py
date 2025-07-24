#!/usr/bin/env python3
"""
Multi-Account Signal Testing Script
Tests the multi-account trading functionality with TradingView signals
"""

import asyncio
import aiohttp
import json
from datetime import datetime
from typing import Dict, Any

class MultiAccountSignalTester:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        
    def create_signal_template(self, signal_type: str = "accounts_array") -> Dict[str, Any]:
        """Create different types of multi-account signals"""
        
        base_signal = {
            "symbol": "EUR_USD",
            "action": "BUY",
            "alert_id": f"TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "position_id": f"EUR_USD_H1_{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}Z",
            "exchange": "OANDA",
            "percentage": 10,
            "risk_percent": 10,
            "orderType": "MARKET",
            "timeInForce": "FOK",
            "comment": "Multi-Account Test Signal",
            "strategy": "Lorentzian_Classification",
            "timestamp": datetime.now().strftime('%Y-%m-%dT%H:%M:%S') + "Z",
            "timeframe": "H1"
        }
        
        if signal_type == "accounts_array":
            # Option 1: Multiple accounts in array
            base_signal["accounts"] = [
                "101-003-26651494-006",
                "101-003-26651494-011"
            ]
            
        elif signal_type == "broadcast":
            # Option 2: Broadcast to all configured accounts
            base_signal["account"] = "BROADCAST"
            
        elif signal_type == "single_account":
            # Option 3: Single account (for comparison)
            base_signal["account"] = "101-003-26651494-011"
            
        return base_signal
    
    def create_close_signal(self, signal_type: str = "accounts_array") -> Dict[str, Any]:
        """Create multi-account close signals"""
        base_signal = {
            "symbol": "EUR_USD",
            "action": "CLOSE",
            "alert_id": f"CLOSE_TEST_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "exchange": "OANDA",
            "comment": "Multi-Account Close Test Signal",
            "strategy": "Lorentzian_Classification",
            "timestamp": datetime.now().strftime('%Y-%m-%dT%H:%M:%S') + "Z",
            "timeframe": "H1"
        }
        
        if signal_type == "accounts_array":
            base_signal["accounts"] = [
                "101-003-26651494-006",
                "101-003-26651494-011"
            ]
        elif signal_type == "broadcast":
            base_signal["account"] = "BROADCAST"
        elif signal_type == "single_account":
            base_signal["account"] = "101-003-26651494-011"
            
        return base_signal
    
    async def test_signal(self, signal: Dict[str, Any], test_name: str) -> Dict[str, Any]:
        """Test a signal against the webhook endpoint"""
        print(f"\n🧪 Testing: {test_name}")
        print("=" * 60)
        print(f"📤 Signal: {json.dumps(signal, indent=2)}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/tradingview",
                    json=signal,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    result = await response.json()
                    
                    print(f"📥 Response ({response.status}): {json.dumps(result, indent=2)}")
                    
                    if response.status == 200 and result.get('status') == 'ok':
                        print("✅ Signal processed successfully!")
                        
                        # Check for multi-account specific results
                        signal_result = result.get('result', {})
                        if signal_result.get('multi_account_execution'):
                            print(f"🎯 Multi-account execution detected!")
                            print(f"   - Total accounts: {signal_result.get('total_accounts', 0)}")
                            print(f"   - Successful accounts: {signal_result.get('successful_accounts', 0)}")
                            print(f"   - Success rate: {signal_result.get('success_rate', 0):.2%}")
                        
                        return result
                    else:
                        print(f"❌ Signal failed to process: {result}")
                        return result
                        
        except Exception as e:
            print(f"❌ Test failed: {str(e)}")
            return {"error": str(e)}
    
    async def run_comprehensive_tests(self):
        """Run comprehensive multi-account tests"""
        print("🚀 Multi-Account Signal Testing Suite")
        print("=" * 60)
        
        test_results = {}
        
        # Test 1: Accounts Array
        print("\n1️⃣ Testing Accounts Array Method")
        signal1 = self.create_signal_template("accounts_array")
        test_results["accounts_array"] = await self.test_signal(signal1, "Accounts Array - Multiple Accounts")
        
        # Test 2: Broadcast Method
        print("\n2️⃣ Testing Broadcast Method")
        signal2 = self.create_signal_template("broadcast")
        test_results["broadcast"] = await self.test_signal(signal2, "Broadcast - All Configured Accounts")
        
        # Test 3: Single Account (Control)
        print("\n3️⃣ Testing Single Account Method (Control)")
        signal3 = self.create_signal_template("single_account")
        test_results["single_account"] = await self.test_signal(signal3, "Single Account - Control Test")
        
        # Test 4: Multi-Account Close Signal
        print("\n4️⃣ Testing Multi-Account Close Signal")
        close_signal = self.create_close_signal("accounts_array")
        test_results["close_multi_account"] = await self.test_signal(close_signal, "Multi-Account Close Signal")
        
        # Test Summary
        print("\n📊 TEST SUMMARY")
        print("=" * 60)
        successful_tests = 0
        total_tests = len(test_results)
        
        for test_name, result in test_results.items():
            if result.get('status') == 'ok':
                print(f"✅ {test_name}: PASSED")
                successful_tests += 1
            else:
                print(f"❌ {test_name}: FAILED")
        
        print(f"\n🏆 Overall Results: {successful_tests}/{total_tests} tests passed")
        
        return test_results
    
    async def test_your_signal_format(self):
        """Test your specific signal format"""
        print("\n🎯 Testing Your Specific Signal Format")
        print("=" * 60)
        
        # Your original signal format with multiple accounts
        your_signal = {
            "symbol": "{{ticker}}",
            "action": "BUY",
            "alert_id": "{{ticker}}_{{interval}}_{{time}}",
            "position_id": "{{ticker}}_{{interval}}_{{time}}",
            "exchange": "OANDA",
            "accounts": [  # This is the key change - use "accounts" array
                "101-003-26651494-006",
                "101-003-26651494-011"
            ],
            "percentage": 10,
            "risk_percent": 10,
            "orderType": "MARKET",
            "timeInForce": "FOK",
            "comment": "Open Long Signal",
            "strategy": "Lorentzian_Classification",
            "timestamp": "{{time}}",
            "timeframe": "{{interval}}"
        }
        
        print("📝 Your Signal Format (with template variables resolved):")
        resolved_signal = your_signal.copy()
        resolved_signal["symbol"] = "EUR_USD"
        resolved_signal["alert_id"] = f"EUR_USD_H1_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        resolved_signal["position_id"] = f"EUR_USD_H1_{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}Z"
        resolved_signal["timestamp"] = datetime.now().strftime('%Y-%m-%dT%H:%M:%S') + "Z"
        resolved_signal["timeframe"] = "H1"
        
        return await self.test_signal(resolved_signal, "Your Signal Format - Multi-Account")

async def main():
    """Main test function"""
    tester = MultiAccountSignalTester()
    
    # Run comprehensive tests
    await tester.run_comprehensive_tests()
    
    # Test your specific format
    await tester.test_your_signal_format()
    
    print("\n🎉 Multi-Account Testing Complete!")
    print("\nℹ️  You can now use any of these formats in TradingView:")
    print("   1. \"accounts\": [\"account1\", \"account2\"] - for specific accounts")
    print("   2. \"account\": \"BROADCAST\" - for all configured accounts")
    print("   3. \"account\": \"specific_account\" - for single account")

if __name__ == "__main__":
    asyncio.run(main()) 