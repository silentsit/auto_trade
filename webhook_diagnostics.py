#!/usr/bin/env python3
"""
WEBHOOK DIAGNOSTICS TOOL
Comprehensive tool to test and diagnose TradingView webhook integration
"""

import asyncio
import json
import logging
import aiohttp
from datetime import datetime, timezone
from typing import Dict, Any, List
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WebhookDiagnostics:
    """Comprehensive webhook testing and diagnostics"""
    
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.test_results = []
        
    async def send_test_webhook(self, payload: Dict[str, Any], test_name: str) -> Dict[str, Any]:
        """Send a test webhook and capture the response"""
        try:
            logger.info(f"🧪 Testing: {test_name}")
            logger.info(f"📤 Payload: {json.dumps(payload, indent=2)}")
            
            async with aiohttp.ClientSession() as session:
                start_time = datetime.now()
                async with session.post(
                    self.webhook_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    end_time = datetime.now()
                    response_time = (end_time - start_time).total_seconds()
                    
                    response_text = await response.text()
                    
                    result = {
                        "test_name": test_name,
                        "status_code": response.status,
                        "response_time_ms": int(response_time * 1000),
                        "response_text": response_text,
                        "success": 200 <= response.status < 300,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }
                    
                    if result["success"]:
                        logger.info(f"✅ {test_name}: {response.status} ({response_time:.3f}s)")
                        try:
                            response_json = json.loads(response_text)
                            result["response_json"] = response_json
                            logger.info(f"📥 Response: {json.dumps(response_json, indent=2)}")
                        except json.JSONDecodeError:
                            logger.info(f"📥 Response (text): {response_text}")
                    else:
                        logger.error(f"❌ {test_name}: {response.status} - {response_text}")
                    
                    self.test_results.append(result)
                    return result
                    
        except Exception as e:
            logger.error(f"❌ {test_name} FAILED: {str(e)}")
            result = {
                "test_name": test_name,
                "error": str(e),
                "success": False,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            self.test_results.append(result)
            return result

    async def test_buy_signal(self) -> Dict[str, Any]:
        """Test BUY signal webhook"""
        payload = {
            "symbol": "EURUSD",
            "action": "BUY",
            "alert_id": f"TEST_BUY_{int(datetime.now().timestamp())}",
            "position_id": f"EURUSD_TEST_{int(datetime.now().timestamp())}",
            "exchange": "OANDA",
            "account": "101-003-26651494-012",
            "orderType": "MARKET",
            "timeInForce": "FOK",
            "comment": "Diagnostic Test - BUY Signal",
            "strategy": "Webhook_Diagnostics",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeframe": "60"
        }
        return await self.send_test_webhook(payload, "BUY Signal Test")

    async def test_sell_signal(self) -> Dict[str, Any]:
        """Test SELL signal webhook"""
        payload = {
            "symbol": "GBPUSD",
            "action": "SELL",
            "alert_id": f"TEST_SELL_{int(datetime.now().timestamp())}",
            "position_id": f"GBPUSD_TEST_{int(datetime.now().timestamp())}",
            "exchange": "OANDA",
            "account": "101-003-26651494-012",
            "orderType": "MARKET",
            "timeInForce": "FOK",
            "comment": "Diagnostic Test - SELL Signal",
            "strategy": "Webhook_Diagnostics",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeframe": "60"
        }
        return await self.send_test_webhook(payload, "SELL Signal Test")

    async def test_close_signal(self) -> Dict[str, Any]:
        """Test CLOSE signal webhook"""
        payload = {
            "symbol": "EURUSD",
            "action": "CLOSE",
            "alert_id": f"TEST_CLOSE_{int(datetime.now().timestamp())}",
            "position_id": f"EURUSD_TEST_{int(datetime.now().timestamp())}",
            "exchange": "OANDA",
            "account": "101-003-26651494-012",
            "orderType": "MARKET",
            "timeInForce": "FOK",
            "comment": "Diagnostic Test - CLOSE Signal",
            "strategy": "Webhook_Diagnostics",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeframe": "60"
        }
        return await self.send_test_webhook(payload, "CLOSE Signal Test")

    async def test_malformed_signal(self) -> Dict[str, Any]:
        """Test malformed webhook to verify error handling"""
        payload = {
            "symbol": "INVALID",
            "action": "INVALID_ACTION",
            "missing_required_fields": True
        }
        return await self.send_test_webhook(payload, "Malformed Signal Test")

    async def test_tradingview_format(self) -> Dict[str, Any]:
        """Test typical TradingView alert format"""
        payload = {
            "symbol": "{{ticker}}",
            "action": "BUY", 
            "alert_id": "{{ticker}}_{{interval}}_{{time}}",
            "position_id": "{{ticker}}_{{interval}}_{{time}}",
            "exchange": "OANDA",
            "account": "101-003-26651494-012",
            "orderType": "MARKET",
            "timeInForce": "FOK",
            "comment": "Real TradingView Format Test",
            "strategy": "Lorentzian_Classification",
            "timestamp": "{{time}}",
            "timeframe": "{{interval}}"
        }
        return await self.send_test_webhook(payload, "TradingView Format Test")

    async def test_health_endpoint(self) -> Dict[str, Any]:
        """Test the health check endpoint"""
        try:
            health_url = self.webhook_url.replace('/tradingview', '/')
            logger.info(f"🏥 Testing health endpoint: {health_url}")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(health_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    response_text = await response.text()
                    
                    result = {
                        "test_name": "Health Check",
                        "status_code": response.status,
                        "response_text": response_text,
                        "success": response.status == 200,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }
                    
                    if result["success"]:
                        logger.info(f"✅ Health Check: {response.status}")
                        try:
                            response_json = json.loads(response_text)
                            result["response_json"] = response_json
                            logger.info(f"📊 Bot Status: {response_json.get('market_status', 'unknown')}")
                        except json.JSONDecodeError:
                            pass
                    else:
                        logger.error(f"❌ Health Check: {response.status}")
                    
                    return result
                    
        except Exception as e:
            logger.error(f"❌ Health Check FAILED: {str(e)}")
            return {
                "test_name": "Health Check",
                "error": str(e),
                "success": False,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

    async def run_comprehensive_test(self) -> Dict[str, Any]:
        """Run all diagnostic tests"""
        logger.info("🚀 Starting Comprehensive Webhook Diagnostics")
        logger.info(f"🎯 Target URL: {self.webhook_url}")
        logger.info("=" * 80)
        
        # Test health endpoint first
        await self.test_health_endpoint()
        
        # Test all signal types
        await self.test_buy_signal()
        await asyncio.sleep(1)  # Small delay between tests
        
        await self.test_sell_signal()
        await asyncio.sleep(1)
        
        await self.test_close_signal()
        await asyncio.sleep(1)
        
        await self.test_malformed_signal()
        await asyncio.sleep(1)
        
        await self.test_tradingview_format()
        
        # Generate summary report
        return await self.generate_report()

    async def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive diagnostic report"""
        logger.info("=" * 80)
        logger.info("📊 DIAGNOSTIC REPORT SUMMARY")
        logger.info("=" * 80)
        
        total_tests = len(self.test_results)
        successful_tests = len([r for r in self.test_results if r.get("success", False)])
        failed_tests = total_tests - successful_tests
        
        logger.info(f"Total Tests: {total_tests}")
        logger.info(f"✅ Successful: {successful_tests}")
        logger.info(f"❌ Failed: {failed_tests}")
        logger.info(f"Success Rate: {(successful_tests/total_tests)*100:.1f}%")
        
        # Analyze specific issues
        issues = []
        recommendations = []
        
        # Check for specific signal type failures
        buy_test = next((r for r in self.test_results if "BUY" in r.get("test_name", "")), None)
        sell_test = next((r for r in self.test_results if "SELL" in r.get("test_name", "")), None)
        close_test = next((r for r in self.test_results if "CLOSE" in r.get("test_name", "")), None)
        
        if buy_test and not buy_test.get("success", False):
            issues.append("BUY signals are not processing correctly")
            recommendations.append("Check alert_handler.py BUY signal processing logic")
            
        if sell_test and not sell_test.get("success", False):
            issues.append("SELL signals are not processing correctly")
            recommendations.append("Check alert_handler.py SELL signal processing logic")
            
        if close_test and not close_test.get("success", False):
            issues.append("CLOSE signals are failing (expected - no positions to close)")
            recommendations.append("This is normal if no positions exist to close")
        
        # Check webhook connectivity
        if failed_tests > 0:
            issues.append("Some webhook tests failed - check network connectivity")
            recommendations.append("Verify webhook URL and bot deployment status")
        
        report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "webhook_url": self.webhook_url,
            "total_tests": total_tests,
            "successful_tests": successful_tests,
            "failed_tests": failed_tests,
            "success_rate": (successful_tests/total_tests)*100,
            "issues": issues,
            "recommendations": recommendations,
            "test_details": self.test_results
        }
        
        logger.info("\n🔍 ISSUES IDENTIFIED:")
        for issue in issues:
            logger.info(f"  • {issue}")
            
        logger.info("\n💡 RECOMMENDATIONS:")
        for rec in recommendations:
            logger.info(f"  • {rec}")
        
        # Save report to file
        report_filename = f"webhook_diagnostics_report_{int(datetime.now().timestamp())}.json"
        with open(report_filename, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"\n📄 Full report saved to: {report_filename}")
        logger.info("=" * 80)
        
        return report

async def main():
    """Main function to run diagnostics"""
    if len(sys.argv) > 1:
        webhook_url = sys.argv[1]
    else:
        webhook_url = "https://auto-trade-b0bi.onrender.com/tradingview"
    
    diagnostics = WebhookDiagnostics(webhook_url)
    await diagnostics.run_comprehensive_test()

if __name__ == "__main__":
    asyncio.run(main())
