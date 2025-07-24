#!/usr/bin/env python3
"""
ENHANCED OANDA CONNECTION HEALTH MONITOR
Real-time monitoring with proactive diagnostics and alerting
"""

import asyncio
import time
import logging
import json
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from config import config
from oanda_service import OandaService
from oandapyV20.endpoints.accounts import AccountDetails
from oandapyV20.endpoints.pricing import PricingInfo

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("connection_monitor")

@dataclass
class ConnectionMetrics:
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    avg_response_time: float = 0.0
    min_response_time: float = float('inf')
    max_response_time: float = 0.0
    consecutive_failures: int = 0
    last_success_time: Optional[datetime] = None
    last_failure_time: Optional[datetime] = None
    connection_uptime_percent: float = 0.0
    start_time: datetime = None

    def __post_init__(self):
        if self.start_time is None:
            self.start_time = datetime.now()

class EnhancedConnectionMonitor:
    def __init__(self, oanda_service: Optional[OandaService] = None):
        self.oanda_service = oanda_service or OandaService()
        self.metrics = ConnectionMetrics()
        self.response_times: List[float] = []
        self.failure_log: List[Tuple[datetime, str]] = []
        self.running = False
        self.monitoring_interval = 30  # seconds
        self.alert_threshold_failures = 3
        self.alert_threshold_response_time = 5.0  # seconds
        
    def record_success(self, response_time: float):
        """Record a successful request with detailed metrics"""
        self.metrics.total_requests += 1
        self.metrics.successful_requests += 1
        self.metrics.consecutive_failures = 0
        self.metrics.last_success_time = datetime.now()
        
        # Update response time metrics
        self.response_times.append(response_time)
        if len(self.response_times) > 100:  # Keep only last 100
            self.response_times = self.response_times[-100:]
            
        self.metrics.avg_response_time = sum(self.response_times) / len(self.response_times)
        self.metrics.min_response_time = min(self.metrics.min_response_time, response_time)
        self.metrics.max_response_time = max(self.metrics.max_response_time, response_time)
        
        # Calculate uptime percentage
        if self.metrics.total_requests > 0:
            self.metrics.connection_uptime_percent = (
                self.metrics.successful_requests / self.metrics.total_requests * 100
            )
    
    def record_failure(self, error_message: str):
        """Record a failed request with error details"""
        self.metrics.total_requests += 1
        self.metrics.failed_requests += 1
        self.metrics.consecutive_failures += 1
        self.metrics.last_failure_time = datetime.now()
        
        # Log failure details
        self.failure_log.append((datetime.now(), error_message))
        if len(self.failure_log) > 50:  # Keep only last 50 failures
            self.failure_log = self.failure_log[-50:]
        
        # Calculate uptime percentage
        if self.metrics.total_requests > 0:
            self.metrics.connection_uptime_percent = (
                self.metrics.successful_requests / self.metrics.total_requests * 100
            )
    
    async def test_account_connection(self) -> Tuple[bool, float, str]:
        """Test basic account connection with detailed diagnostics"""
        try:
            start = time.time()
            account_request = AccountDetails(accountID=self.oanda_service.config.oanda_account_id)
            response = await self.oanda_service.robust_oanda_request(account_request, max_retries=2)
            end = time.time()
            
            response_time = end - start
            self.record_success(response_time)
            
            balance = float(response['account']['balance'])
            currency = response['account']['currency']
            
            status_msg = f"Balance: {balance:.2f} {currency}"
            logger.info(f"✅ Account check passed - {status_msg} ({response_time:.3f}s)")
            return True, response_time, status_msg
            
        except Exception as e:
            self.record_failure(str(e))
            error_msg = f"Account check failed: {e}"
            logger.error(f"❌ {error_msg}")
            return False, 0, error_msg
    
    async def test_pricing_connection(self, instruments=None) -> Tuple[bool, float, str]:
        """Test pricing data connection with market status"""
        if instruments is None:
            instruments = ["EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF"]
            
        try:
            start = time.time()
            request = PricingInfo(
                accountID=self.oanda_service.config.oanda_account_id,
                params={"instruments": ",".join(instruments)}
            )
            response = await self.oanda_service.robust_oanda_request(request, max_retries=2)
            end = time.time()
            
            response_time = end - start
            self.record_success(response_time)
            
            prices = response.get('prices', [])
            tradeable_count = sum(1 for p in prices if p.get('tradeable', False))
            
            status_msg = f"{tradeable_count}/{len(prices)} instruments tradeable"
            logger.info(f"✅ Pricing check passed - {status_msg} ({response_time:.3f}s)")
            
            # Log sample prices for key instruments
            for price in prices[:3]:
                instrument = price['instrument']
                bid = price.get('bid', {}).get('price', 'N/A')
                ask = price.get('ask', {}).get('price', 'N/A')
                tradeable = "✓" if price.get('tradeable') else "✗"
                logger.debug(f"   {instrument}: {bid}/{ask} {tradeable}")
            
            return True, response_time, status_msg
            
        except Exception as e:
            self.record_failure(str(e))
            error_msg = f"Pricing check failed: {e}"
            logger.error(f"❌ {error_msg}")
            return False, 0, error_msg

    async def test_network_latency(self) -> Tuple[bool, float, str]:
        """Test network latency with ping-like functionality"""
        try:
            # Use a lightweight account status request as ping
            start = time.time()
            account_request = AccountDetails(accountID=self.oanda_service.config.oanda_account_id)
            
            # Use raw oanda client for faster test
            response = self.oanda_service.oanda.request(account_request)
            end = time.time()
            
            latency = end - start
            
            if latency > self.alert_threshold_response_time:
                status_msg = f"High latency: {latency:.3f}s"
                logger.warning(f"⚠️ {status_msg}")
            else:
                status_msg = f"Latency: {latency:.3f}s"
                logger.debug(f"🏃 Network latency test - {status_msg}")
            
            return True, latency, status_msg
            
        except Exception as e:
            error_msg = f"Latency test failed: {e}"
            logger.error(f"❌ {error_msg}")
            return False, 0, error_msg

    def get_health_summary(self) -> Dict:
        """Get comprehensive health summary"""
        uptime = datetime.now() - self.metrics.start_time
        
        # Determine overall health status
        if self.metrics.consecutive_failures >= self.alert_threshold_failures:
            health_status = "CRITICAL"
        elif self.metrics.connection_uptime_percent < 95:
            health_status = "WARNING"
        elif self.metrics.avg_response_time > self.alert_threshold_response_time:
            health_status = "SLOW"
        else:
            health_status = "HEALTHY"
        
        # Recent failures summary
        recent_failures = [
            f"{failure_time.strftime('%H:%M:%S')}: {error}" 
            for failure_time, error in self.failure_log[-5:]
        ]
        
        return {
            "status": health_status,
            "uptime": str(uptime).split('.')[0],  # Remove microseconds
            "metrics": asdict(self.metrics),
            "recent_failures": recent_failures,
            "performance": {
                "avg_response_time": f"{self.metrics.avg_response_time:.3f}s",
                "min_response_time": f"{self.metrics.min_response_time:.3f}s" if self.metrics.min_response_time != float('inf') else "N/A",
                "max_response_time": f"{self.metrics.max_response_time:.3f}s",
                "success_rate": f"{self.metrics.connection_uptime_percent:.1f}%"
            }
        }

    def print_status_report(self):
        """Print a formatted status report"""
        summary = self.get_health_summary()
        
        print("\n" + "="*60)
        print("🔍 OANDA CONNECTION HEALTH REPORT")
        print("="*60)
        print(f"Overall Status: {summary['status']}")
        print(f"Uptime: {summary['uptime']}")
        print(f"Success Rate: {summary['performance']['success_rate']}")
        print(f"Avg Response Time: {summary['performance']['avg_response_time']}")
        print(f"Total Requests: {summary['metrics']['total_requests']}")
        print(f"Consecutive Failures: {summary['metrics']['consecutive_failures']}")
        
        if summary['recent_failures']:
            print("\n🚨 Recent Failures:")
            for failure in summary['recent_failures']:
                print(f"  • {failure}")
        
        print("="*60)

    async def run_diagnostic_suite(self) -> Dict:
        """Run complete diagnostic suite"""
        logger.info("🔍 Starting comprehensive OANDA connection diagnostics...")
        
        results = {
            "timestamp": datetime.now().isoformat(),
            "tests": {},
            "summary": {}
        }
        
        # Test 1: Account Connection
        logger.info("📋 Testing account connection...")
        account_success, account_time, account_msg = await self.test_account_connection()
        results["tests"]["account"] = {
            "success": account_success,
            "response_time": account_time,
            "message": account_msg
        }
        
        # Test 2: Pricing Connection
        logger.info("💹 Testing pricing data connection...")
        pricing_success, pricing_time, pricing_msg = await self.test_pricing_connection()
        results["tests"]["pricing"] = {
            "success": pricing_success,
            "response_time": pricing_time,
            "message": pricing_msg
        }
        
        # Test 3: Network Latency
        logger.info("🌐 Testing network latency...")
        latency_success, latency_time, latency_msg = await self.test_network_latency()
        results["tests"]["latency"] = {
            "success": latency_success,
            "response_time": latency_time,
            "message": latency_msg
        }
        
        # Generate summary
        all_tests_passed = all(test["success"] for test in results["tests"].values())
        avg_response_time = sum(test["response_time"] for test in results["tests"].values() if test["success"]) / len([t for t in results["tests"].values() if t["success"]])
        
        results["summary"] = {
            "all_tests_passed": all_tests_passed,
            "avg_response_time": avg_response_time,
            "health_status": "HEALTHY" if all_tests_passed and avg_response_time < 2.0 else "WARNING"
        }
        
        logger.info(f"✅ Diagnostic suite completed - Status: {results['summary']['health_status']}")
        return results

    async def start_monitoring(self, duration_minutes: int = None):
        """Start continuous monitoring"""
        self.running = True
        logger.info(f"🚀 Starting continuous OANDA connection monitoring (interval: {self.monitoring_interval}s)")
        
        if duration_minutes:
            end_time = datetime.now() + timedelta(minutes=duration_minutes)
            logger.info(f"⏰ Monitoring will run for {duration_minutes} minutes")
        else:
            end_time = None
            logger.info("⏰ Monitoring will run indefinitely (Ctrl+C to stop)")
        
        try:
            while self.running:
                if end_time and datetime.now() >= end_time:
                    logger.info("⏰ Monitoring duration completed")
                    break
                
                # Run diagnostic tests
                await self.run_diagnostic_suite()
                
                # Print status report every 5 cycles
                if self.metrics.total_requests % 15 == 0:  # 5 cycles * 3 tests each
                    self.print_status_report()
                
                # Check for alerts
                if self.metrics.consecutive_failures >= self.alert_threshold_failures:
                    logger.error(f"🚨 ALERT: {self.metrics.consecutive_failures} consecutive failures detected!")
                
                if self.metrics.avg_response_time > self.alert_threshold_response_time:
                    logger.warning(f"🐌 ALERT: Slow response time detected: {self.metrics.avg_response_time:.3f}s")
                
                # Wait for next cycle
                await asyncio.sleep(self.monitoring_interval)
                
        except KeyboardInterrupt:
            logger.info("🛑 Monitoring stopped by user")
        finally:
            self.running = False
            self.print_status_report()

    def stop_monitoring(self):
        """Stop continuous monitoring"""
        self.running = False

async def main():
    """Main function for standalone execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description="OANDA Connection Health Monitor")
    parser.add_argument("--duration", type=int, help="Monitoring duration in minutes")
    parser.add_argument("--interval", type=int, default=30, help="Monitoring interval in seconds")
    parser.add_argument("--diagnostic-only", action="store_true", help="Run single diagnostic suite only")
    
    args = parser.parse_args()
    
    monitor = EnhancedConnectionMonitor()
    monitor.monitoring_interval = args.interval
    
    if args.diagnostic_only:
        logger.info("Running single diagnostic suite...")
        results = await monitor.run_diagnostic_suite()
        print(json.dumps(results, indent=2))
    else:
        await monitor.start_monitoring(args.duration)

if __name__ == "__main__":
    asyncio.run(main()) 