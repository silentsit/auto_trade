#!/usr/bin/env python3
"""
OANDA Connection Health Monitor
Run this script to monitor OANDA connection health in real-time
"""

import asyncio
import time
import logging
from datetime import datetime, timezone
from config import config
from main import initialize_oanda_client, robust_oanda_request
from oandapyV20.endpoints.accounts import AccountDetails
from oandapyV20.endpoints.pricing import PricingInfo

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("connection_monitor")

class ConnectionMonitor:
    def __init__(self):
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.avg_response_time = 0.0
        self.response_times = []
        self.start_time = time.time()
        
    async def test_account_connection(self):
        """Test basic account connection"""
        try:
            start = time.time()
            request = AccountDetails(accountID=config.oanda_account_id)
            response = await robust_oanda_request(request, max_retries=2)
            end = time.time()
            
            response_time = end - start
            self.record_success(response_time)
            
            balance = float(response['account']['balance'])
            currency = response['account']['currency']
            
            logger.info(f"✅ Account check passed - Balance: {balance:.2f} {currency} ({response_time:.3f}s)")
            return True, response_time
            
        except Exception as e:
            self.record_failure()
            logger.error(f"❌ Account check failed: {e}")
            return False, 0
    
    async def test_pricing_connection(self, instruments=["EUR_USD", "GBP_USD", "USD_JPY"]):
        """Test pricing data connection"""
        try:
            start = time.time()
            request = PricingInfo(
                accountID=config.oanda_account_id,
                params={"instruments": ",".join(instruments)}
            )
            response = await robust_oanda_request(request, max_retries=2)
            end = time.time()
            
            response_time = end - start
            self.record_success(response_time)
            
            prices_count = len(response.get('prices', []))
            logger.info(f"✅ Pricing check passed - {prices_count} instruments ({response_time:.3f}s)")
            
            # Log some sample prices
            for price in response.get('prices', [])[:3]:
                instrument = price['instrument']
                bid = price.get('bid', {}).get('price', 'N/A')
                ask = price.get('ask', {}).get('price', 'N/A')
                logger.info(f"   {instrument}: {bid}/{ask}")
            
            return True, response_time
            
        except Exception as e:
            self.record_failure()
            logger.error(f"❌ Pricing check failed: {e}")
            return False, 0
    
    def record_success(self, response_time):
        """Record a successful request"""
        self.total_requests += 1
        self.successful_requests += 1
        self.response_times.append(response_time)
        
        # Keep only last 100 response times for average calculation
        if len(self.response_times) > 100:
            self.response_times = self.response_times[-100:]
            
        self.avg_response_time = sum(self.response_times) / len(self.response_times)
    
    def record_failure(self):
        """Record a failed request"""
        self.total_requests += 1
        self.failed_requests += 1
    
    def get_stats(self):
        """Get connection statistics"""
        uptime = time.time() - self.start_time
        success_rate = (self.successful_requests / self.total_requests * 100) if self.total_requests > 0 else 0
        
        return {
            "uptime_seconds": uptime,
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "success_rate": success_rate,
            "avg_response_time": self.avg_response_time,
            "last_response_times": self.response_times[-5:] if self.response_times else []
        }
    
    def print_stats(self):
        """Print current statistics"""
        stats = self.get_stats()
        uptime_minutes = stats["uptime_seconds"] / 60
        
        print(f"\n📊 Connection Statistics (Uptime: {uptime_minutes:.1f}m)")
        print(f"   Total Requests: {stats['total_requests']}")
        print(f"   Successful: {stats['successful_requests']} ({stats['success_rate']:.1f}%)")
        print(f"   Failed: {stats['failed_requests']}")
        print(f"   Avg Response Time: {stats['avg_response_time']:.3f}s")
        
        if stats['last_response_times']:
            recent_times = [f"{t:.3f}s" for t in stats['last_response_times']]
            print(f"   Recent Response Times: {', '.join(recent_times)}")

async def run_continuous_monitor(interval_seconds=30):
    """Run continuous connection monitoring"""
    monitor = ConnectionMonitor()
    
    # Initialize OANDA client
    logger.info("🚀 Starting OANDA Connection Monitor")
    logger.info(f"   Environment: {config.oanda_environment}")
    logger.info(f"   Account ID: {config.oanda_account_id}")
    logger.info(f"   Check Interval: {interval_seconds}s")
    
    if not initialize_oanda_client():
        logger.error("❌ Failed to initialize OANDA client")
        return
    
    logger.info("✅ OANDA client initialized successfully")
    
    try:
        while True:
            logger.info(f"\n🔍 Running connection health check...")
            
            # Test account connection
            account_success, account_time = await monitor.test_account_connection()
            
            # Test pricing connection
            pricing_success, pricing_time = await monitor.test_pricing_connection()
            
            # Print statistics
            monitor.print_stats()
            
            # Health summary
            if account_success and pricing_success:
                health_status = "🟢 HEALTHY"
            elif account_success or pricing_success:
                health_status = "🟡 DEGRADED"
            else:
                health_status = "🔴 UNHEALTHY"
            
            logger.info(f"\n{health_status} - Connection Status")
            
            # Wait for next check
            logger.info(f"⏱️  Next check in {interval_seconds}s...")
            await asyncio.sleep(interval_seconds)
            
    except KeyboardInterrupt:
        logger.info("\n⏹️  Monitoring stopped by user")
        monitor.print_stats()

async def run_single_test():
    """Run a single connection test"""
    monitor = ConnectionMonitor()
    
    logger.info("🔍 Running single OANDA connection test...")
    
    if not initialize_oanda_client():
        logger.error("❌ Failed to initialize OANDA client")
        return
    
    # Test account connection
    account_success, account_time = await monitor.test_account_connection()
    
    # Test pricing connection  
    pricing_success, pricing_time = await monitor.test_pricing_connection()
    
    # Print results
    monitor.print_stats()
    
    if account_success and pricing_success:
        logger.info("🟢 Overall Status: HEALTHY")
    elif account_success or pricing_success:
        logger.info("🟡 Overall Status: DEGRADED")
    else:
        logger.info("🔴 Overall Status: UNHEALTHY")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "single":
        # Run single test
        asyncio.run(run_single_test())
    else:
        # Run continuous monitoring
        interval = 30
        if len(sys.argv) > 1:
            try:
                interval = int(sys.argv[1])
            except ValueError:
                logger.warning(f"Invalid interval '{sys.argv[1]}', using default 30s")
        
        asyncio.run(run_continuous_monitor(interval)) 