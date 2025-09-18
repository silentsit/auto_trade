#!/usr/bin/env python3
"""
COMPREHENSIVE WEBHOOK CONNECTIVITY DIAGNOSTIC
Tests webhook connectivity to your deployed Render bot
"""

import aiohttp
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WebhookConnectivityTester:
    def __init__(self, base_url: str):
        """
        Initialize with your Render bot URL
        Replace 'your-app-name' with your actual Render app name
        """
        self.base_url = base_url.rstrip('/')
        self.webhook_url = f"{self.base_url}/tradingview"
        self.health_url = f"{self.base_url}/"
        
    async def test_health_endpoint(self) -> Dict[str, Any]:
        """Test if the bot is responding at all"""
        logger.info("🏥 Testing health endpoint...")
        
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.health_url) as response:
                    status = response.status
                    text = await response.text()
                    
                    logger.info(f"✅ Health check: Status {status}")
                    logger.info(f"Response: {text[:200]}{'...' if len(text) > 200 else ''}")
                    
                    return {
                        "success": status == 200,
                        "status_code": status,
                        "response_text": text,
                        "url": self.health_url
                    }
                    
        except asyncio.TimeoutError:
            logger.error("❌ Health check timed out (30s)")
            return {"success": False, "error": "timeout", "url": self.health_url}
        except Exception as e:
            logger.error(f"❌ Health check failed: {e}")
            return {"success": False, "error": str(e), "url": self.health_url}
    
    async def test_webhook_endpoint(self, test_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Test the webhook endpoint with a test payload"""
        logger.info("🎯 Testing webhook endpoint...")
        
        try:
            timeout = aiohttp.ClientTimeout(total=60)  # Longer timeout for webhook
            headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'TradingViewWebhookTester/1.0'
            }
            
            logger.info(f"📤 Sending to: {self.webhook_url}")
            logger.info(f"📦 Payload: {json.dumps(test_payload, indent=2)}")
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    self.webhook_url, 
                    json=test_payload,
                    headers=headers
                ) as response:
                    status = response.status
                    text = await response.text()
                    
                    logger.info(f"📥 Webhook response: Status {status}")
                    logger.info(f"📥 Response body: {text}")
                    
                    # Try to parse response as JSON
                    try:
                        response_json = json.loads(text)
                    except:
                        response_json = {"raw_text": text}
                    
                    return {
                        "success": status in [200, 201],
                        "status_code": status,
                        "response": response_json,
                        "raw_response": text,
                        "url": self.webhook_url
                    }
                    
        except asyncio.TimeoutError:
            logger.error("❌ Webhook test timed out (60s)")
            return {"success": False, "error": "timeout", "url": self.webhook_url}
        except Exception as e:
            logger.error(f"❌ Webhook test failed: {e}")
            return {"success": False, "error": str(e), "url": self.webhook_url}
    
    async def run_comprehensive_test(self):
        """Run all connectivity tests"""
        logger.info("🚀 COMPREHENSIVE WEBHOOK CONNECTIVITY TEST")
        logger.info("=" * 60)
        
        # Test 1: Health check
        health_result = await self.test_health_endpoint()
        
        # Test 2: Simple BUY signal
        buy_payload = {
            "symbol": "EUR_USD",
            "action": "BUY", 
            "alert_id": f"TEST_BUY_{int(datetime.now().timestamp())}",
            "position_id": f"TEST_BUY_{int(datetime.now().timestamp())}",
            "exchange": "OANDA",
            "account": "101-003-26651494-012",
            "percentage": 10,
            "orderType": "MARKET",
            "timeInForce": "FOK",
            "comment": "Connectivity Test - BUY Signal",
            "strategy": "Connectivity_Test",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "timeframe": "60"
        }
        
        webhook_result = await self.test_webhook_endpoint(buy_payload)
        
        # Test 3: Check if API routes are available
        routes_to_test = [
            "/health",
            "/system/health", 
            "/system/status"
        ]
        
        route_results = {}
        for route in routes_to_test:
            try:
                url = f"{self.base_url}{route}"
                timeout = aiohttp.ClientTimeout(total=15)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url) as response:
                        route_results[route] = {
                            "status": response.status,
                            "accessible": response.status in [200, 404]  # 404 means route exists but not allowed
                        }
            except:
                route_results[route] = {"status": "error", "accessible": False}
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("📊 CONNECTIVITY TEST RESULTS")
        logger.info("=" * 60)
        
        logger.info(f"🏥 Health Endpoint: {'✅ PASS' if health_result['success'] else '❌ FAIL'}")
        if not health_result['success']:
            logger.info(f"   Error: {health_result.get('error', 'Unknown')}")
        
        logger.info(f"🎯 Webhook Endpoint: {'✅ PASS' if webhook_result['success'] else '❌ FAIL'}")
        if not webhook_result['success']:
            logger.info(f"   Error: {webhook_result.get('error', 'Unknown')}")
        elif webhook_result['success']:
            logger.info(f"   Response: {webhook_result.get('response', {})}")
        
        logger.info("🔍 Route Accessibility:")
        for route, result in route_results.items():
            status = "✅ ACCESSIBLE" if result['accessible'] else "❌ NOT ACCESSIBLE"
            logger.info(f"   {route}: {status} (Status: {result['status']})")
        
        # Diagnostic recommendations
        logger.info("\n" + "=" * 60) 
        logger.info("🩺 DIAGNOSTIC RECOMMENDATIONS")
        logger.info("=" * 60)
        
        if not health_result['success']:
            logger.info("❌ Bot is not responding - check Render deployment status")
            logger.info("   • Check Render logs for startup errors")
            logger.info("   • Verify environment variables are set")
            logger.info("   • Check if service is sleeping/crashed")
            
        elif not webhook_result['success']:
            logger.info("❌ Webhook endpoint not working:")
            if webhook_result.get('status_code') == 404:
                logger.info("   • Webhook route not found - check api.py inclusion")
            elif webhook_result.get('status_code') == 500:
                logger.info("   • Internal server error - check processing logic")
                logger.info("   • Review Render logs for error details")
            elif webhook_result.get('error') == 'timeout':
                logger.info("   • Webhook processing is too slow or hanging")
                logger.info("   • Check for infinite loops or blocking operations")
            else:
                logger.info(f"   • HTTP {webhook_result.get('status_code', 'unknown')}")
                
        else:
            logger.info("✅ All connectivity tests passed!")
            logger.info("   • Bot is responding to health checks")
            logger.info("   • Webhook endpoint is accessible and processing")
            logger.info("   • Issue may be with TradingView webhook configuration")
        
        return {
            "health": health_result,
            "webhook": webhook_result,
            "routes": route_results
        }

async def main():
    """Main test function"""
    
    # IMPORTANT: Replace this with your actual Render URL
    render_url = input("Enter your Render bot URL (e.g., https://your-app-name.onrender.com): ").strip()
    
    if not render_url:
        logger.error("❌ No URL provided!")
        sys.exit(1)
    
    if not render_url.startswith('http'):
        render_url = f"https://{render_url}"
    
    logger.info(f"🎯 Testing connectivity to: {render_url}")
    
    tester = WebhookConnectivityTester(render_url)
    results = await tester.run_comprehensive_test()
    
    return results

if __name__ == "__main__":
    # Run the connectivity test
    results = asyncio.run(main())
    
    print("\n" + "=" * 60)
    print("🏁 TEST COMPLETED")
    print("=" * 60)
    print("Check the output above for detailed results and recommendations.")
    print("If webhook connectivity passes but bot still doesn't execute trades,")
    print("the issue is likely with TradingView webhook URL configuration.")
