#!/usr/bin/env python3
"""
Trading Bot Diagnostic Tool
Comprehensive system check to identify why trades aren't being executed
"""

import asyncio
import json
import sys
import aiohttp
from datetime import datetime, timezone
import logging
import pytz
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class TradingBotDiagnostic:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.issues_found = []
        self.status = {
            "system_online": False,
            "market_open": False,
            "oanda_connected": False,
            "webhook_working": False,
            "configuration_valid": False
        }

    async def diagnose_all(self):
        """Run complete diagnostic suite"""
        print("🔍 TRADING BOT COMPREHENSIVE DIAGNOSTIC")
        print("=" * 60)
        print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"Target: {self.base_url}")
        print()

        # Run all diagnostic checks
        await self.check_system_status()
        await self.check_market_hours()
        await self.check_oanda_connection()
        await self.check_webhook_endpoint()
        await self.check_configuration()
        await self.test_trade_simulation()
        
        # Summary
        self.print_summary()
        self.print_recommendations()

    async def check_system_status(self):
        """Check if the trading system is online and responsive"""
        print("🏥 SYSTEM STATUS CHECK")
        print("-" * 40)
        
        try:
            async with aiohttp.ClientSession() as session:
                # Test main endpoint
                async with session.get(f"{self.base_url}/", timeout=10) as response:
                    if response.status == 200:
                        print("   ✅ Main service: ONLINE")
                        self.status["system_online"] = True
                    else:
                        print(f"   ❌ Main service: ERROR ({response.status})")
                        self.issues_found.append(f"Service returned HTTP {response.status}")

                # Test health endpoint
                try:
                    async with session.get(f"{self.base_url}/api/health", timeout=5) as response:
                        if response.status == 200:
                            health_data = await response.json()
                            print(f"   ✅ Health check: {health_data.get('status', 'OK')}")
                        else:
                            print("   ⚠️  Health endpoint unavailable")
                except Exception as e:
                    print(f"   ⚠️  Health endpoint error: {str(e)}")

        except Exception as e:
            print(f"   ❌ System offline: {str(e)}")
            self.issues_found.append(f"System offline: {str(e)}")
        
        print()

    async def check_market_hours(self):
        """Check if markets are currently open for trading"""
        print("🕐 MARKET HOURS CHECK")
        print("-" * 40)
        
        try:
            # Import the market hours checking function
            sys.path.append('.')
            from utils import is_instrument_tradeable, get_current_market_session
            
            # Test major forex pairs
            test_symbols = ["EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF"]
            market_session = get_current_market_session()
            
            print(f"   Current session: {market_session.upper()}")
            
            all_tradeable = True
            for symbol in test_symbols:
                tradeable, reason = is_instrument_tradeable(symbol)
                status = "✅ OPEN" if tradeable else "❌ CLOSED"
                print(f"   {symbol}: {status}")
                if not tradeable:
                    print(f"      Reason: {reason}")
                    all_tradeable = False
            
            if all_tradeable:
                self.status["market_open"] = True
                print("   🟢 Markets are OPEN for trading")
            else:
                print("   🔴 Markets are CLOSED")
                self.issues_found.append("Markets are currently closed for trading")
                
        except Exception as e:
            print(f"   ❌ Market hours check failed: {str(e)}")
            self.issues_found.append(f"Market hours check error: {str(e)}")
        
        print()

    async def check_oanda_connection(self):
        """Test OANDA API connection and account access"""
        print("🔌 OANDA CONNECTION CHECK")
        print("-" * 40)
        
        try:
            sys.path.append('.')
            from config import config
            from oanda_service import OandaService
            
            print(f"   Environment: {config.oanda_environment}")
            print(f"   Account ID: {config.oanda_account_id}")
            
            # Test OANDA service
            oanda_service = OandaService()
            
            # Test account access
            try:
                account_balance = await oanda_service.get_account_balance()
                print(f"   ✅ Account balance: ${account_balance:,.2f}")
                self.status["oanda_connected"] = True
            except Exception as e:
                print(f"   ❌ Account access failed: {str(e)}")
                self.issues_found.append(f"OANDA account access error: {str(e)}")
            
            # Test pricing data
            try:
                current_price = await oanda_service.get_current_price("EUR_USD", "BUY")
                print(f"   ✅ EUR_USD price: {current_price}")
            except Exception as e:
                print(f"   ❌ Price data failed: {str(e)}")
                self.issues_found.append(f"OANDA pricing error: {str(e)}")
                
        except Exception as e:
            print(f"   ❌ OANDA connection test failed: {str(e)}")
            self.issues_found.append(f"OANDA connection error: {str(e)}")
        
        print()

    async def check_webhook_endpoint(self):
        """Test webhook endpoint for TradingView signals"""
        print("📡 WEBHOOK ENDPOINT CHECK")
        print("-" * 40)
        
        try:
            async with aiohttp.ClientSession() as session:
                # Test webhook endpoint with sample data
                test_signal = {
                    "symbol": "EUR_USD",
                    "action": "BUY", 
                    "direction": "BUY",
                    "risk_percent": 1.0,
                    "test": True
                }
                
                async with session.post(
                    f"{self.base_url}/tradingview",
                    json=test_signal,
                    timeout=30
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        print("   ✅ Webhook endpoint: WORKING")
                        print(f"   Response: {result.get('status', 'ok')}")
                        self.status["webhook_working"] = True
                    else:
                        print(f"   ❌ Webhook failed: HTTP {response.status}")
                        self.issues_found.append(f"Webhook endpoint returned {response.status}")
                        
        except Exception as e:
            print(f"   ❌ Webhook test failed: {str(e)}")
            self.issues_found.append(f"Webhook error: {str(e)}")
        
        print()

    async def check_configuration(self):
        """Validate critical configuration settings"""
        print("⚙️  CONFIGURATION CHECK")
        print("-" * 40)
        
        try:
            sys.path.append('.')
            from config import config
            
            config_issues = []
            
            # Check OANDA credentials
            if not config.oanda_account_id:
                config_issues.append("OANDA account ID not set")
            else:
                print("   ✅ OANDA account ID: Configured")
                
            if not config.oanda_access_token or str(config.oanda_access_token) == "":
                config_issues.append("OANDA access token not set") 
            else:
                print("   ✅ OANDA access token: Configured")
            
            # Check risk settings
            print(f"   📊 Max risk: {config.max_risk_percentage}%")
            print(f"   📊 Default risk: {config.default_risk_percentage}%")
            print(f"   📊 Allocation: {config.allocation_percent}%")
            
            # Check account list
            if config.enable_multi_account_trading:
                print(f"   👥 Multi-account: ENABLED ({len(config.multi_accounts)} accounts)")
                for account in config.multi_accounts:
                    print(f"      - {account}")
            else:
                print("   👤 Single account mode")
            
            if config_issues:
                for issue in config_issues:
                    print(f"   ❌ {issue}")
                    self.issues_found.append(f"Configuration: {issue}")
            else:
                self.status["configuration_valid"] = True
                print("   ✅ Configuration: VALID")
                
        except Exception as e:
            print(f"   ❌ Configuration check failed: {str(e)}")
            self.issues_found.append(f"Configuration error: {str(e)}")
        
        print()

    async def test_trade_simulation(self):
        """Simulate a trade execution to test the full pipeline"""
        print("🧪 TRADE SIMULATION TEST")
        print("-" * 40)
        
        if not self.status["market_open"]:
            print("   ⏸️  Skipped (markets closed)")
            print()
            return
            
        try:
            async with aiohttp.ClientSession() as session:
                # Send a realistic test signal
                test_signal = {
                    "symbol": "EUR_USD",
                    "action": "BUY",
                    "direction": "BUY", 
                    "risk_percent": 1.0,
                    "timeframe": "H1",
                    "comment": "Diagnostic Test Signal",
                    "test_mode": True  # Add this flag to prevent actual execution
                }
                
                print("   📤 Sending test signal...")
                async with session.post(
                    f"{self.base_url}/tradingview",
                    json=test_signal,
                    timeout=60
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        print(f"   ✅ Signal processed: {result.get('status', 'ok')}")
                        
                        # Check if trade would have been executed
                        if 'result' in result:
                            trade_result = result['result']
                            if trade_result.get('success'):
                                print("   ✅ Trade simulation: PASSED")
                            else:
                                error_msg = trade_result.get('message', 'Unknown error')
                                print(f"   ❌ Trade failed: {error_msg}")
                                self.issues_found.append(f"Trade simulation failed: {error_msg}")
                    else:
                        print(f"   ❌ Signal rejected: HTTP {response.status}")
                        
        except Exception as e:
            print(f"   ❌ Trade simulation failed: {str(e)}")
            self.issues_found.append(f"Trade simulation error: {str(e)}")
        
        print()

    def print_summary(self):
        """Print diagnostic summary"""
        print("📋 DIAGNOSTIC SUMMARY")
        print("=" * 60)
        
        # Status overview
        total_checks = len(self.status)
        passed_checks = sum(1 for status in self.status.values() if status)
        
        print(f"System Health: {passed_checks}/{total_checks} checks passed")
        print()
        
        # Detailed status
        status_icons = {"system_online": "🌐", "market_open": "🕐", "oanda_connected": "🔌", 
                       "webhook_working": "📡", "configuration_valid": "⚙️"}
        
        for check, status in self.status.items():
            icon = status_icons.get(check, "📋")
            status_text = "PASS" if status else "FAIL"
            color = "🟢" if status else "🔴"
            check_name = check.replace("_", " ").title()
            print(f"{icon} {check_name}: {color} {status_text}")
        
        print()

    def print_recommendations(self):
        """Print recommendations based on findings"""
        print("💡 RECOMMENDATIONS")
        print("=" * 60)
        
        if not self.issues_found:
            print("🎉 No issues found! Your trading bot should be working properly.")
            print()
            print("If you're still not seeing trades:")
            print("1. Check that TradingView is sending signals to your webhook URL")
            print("2. Verify your Pine Script alert configuration")
            print("3. Ensure your strategy is generating signals")
            print("4. Check the logs for any recent activity")
            return
        
        print(f"Found {len(self.issues_found)} issues:")
        print()
        
        for i, issue in enumerate(self.issues_found, 1):
            print(f"{i}. {issue}")
        
        print()
        print("🔧 SUGGESTED FIXES:")
        
        # Provide specific solutions
        if any("offline" in issue.lower() for issue in self.issues_found):
            print("• Start the trading bot: python main.py")
        
        if any("closed" in issue.lower() for issue in self.issues_found):
            print("• Wait for markets to open (Sun 17:05 EST - Fri 17:00 EST)")
        
        if any("oanda" in issue.lower() for issue in self.issues_found):
            print("• Check OANDA credentials in .env file")
            print("• Verify OANDA account status and permissions")
        
        if any("configuration" in issue.lower() for issue in self.issues_found):
            print("• Update missing configuration values in .env")
            print("• Check config.py for required settings")
        
        if any("webhook" in issue.lower() for issue in self.issues_found):
            print("• Verify webhook endpoint is accessible")
            print("• Check firewall and port settings")

async def main():
    """Run the diagnostic tool"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Trading Bot Diagnostic Tool")
    parser.add_argument("--url", default="http://localhost:8000", 
                       help="Base URL of the trading system (default: http://localhost:8000)")
    
    args = parser.parse_args()
    
    diagnostic = TradingBotDiagnostic(args.url)
    await diagnostic.diagnose_all()

if __name__ == "__main__":
    asyncio.run(main()) 