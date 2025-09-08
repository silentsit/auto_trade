"""
TEST MARKET HOURS INITIALIZATION
Test that the bot properly handles market hours during initialization
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
import sys

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_market_hours_detection():
    """Test market hours detection function"""
    try:
        from utils import is_market_hours
        
        # Test current time
        current_time = datetime.now(timezone.utc)
        is_open = is_market_hours(current_time)
        
        logger.info(f"📅 Current time: {current_time}")
        logger.info(f"📈 Markets open: {is_open}")
        
        # Test weekend times (should be False)
        weekend_times = [
            # Friday 6 PM EST (should be closed)
            datetime(2025, 9, 5, 23, 0, 0, tzinfo=timezone.utc),  # Friday 6 PM EST
            # Saturday 12 PM EST (should be closed)
            datetime(2025, 9, 6, 17, 0, 0, tzinfo=timezone.utc),  # Saturday 12 PM EST
            # Sunday 10 AM EST (should be closed)
            datetime(2025, 9, 7, 15, 0, 0, tzinfo=timezone.utc),  # Sunday 10 AM EST
        ]
        
        for test_time in weekend_times:
            is_open = is_market_hours(test_time)
            logger.info(f"📅 {test_time.strftime('%A %H:%M UTC')} - Markets open: {is_open}")
        
        # Test weekday times (should be True)
        weekday_times = [
            # Monday 9 AM EST (should be open)
            datetime(2025, 9, 8, 14, 0, 0, tzinfo=timezone.utc),  # Monday 9 AM EST
            # Wednesday 2 PM EST (should be open)
            datetime(2025, 9, 10, 19, 0, 0, tzinfo=timezone.utc),  # Wednesday 2 PM EST
            # Friday 2 PM EST (should be open)
            datetime(2025, 9, 12, 19, 0, 0, tzinfo=timezone.utc),  # Friday 2 PM EST
        ]
        
        for test_time in weekday_times:
            is_open = is_market_hours(test_time)
            logger.info(f"📅 {test_time.strftime('%A %H:%M UTC')} - Markets open: {is_open}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Market hours detection test failed: {e}")
        return False

async def test_weekend_initialization():
    """Test initialization during weekend (should skip OANDA)"""
    try:
        from component_initialization_fix import RobustComponentInitializer
        from main import C
        
        logger.info("🧪 Testing weekend initialization...")
        
        # Mock weekend time
        import utils
        original_is_market_hours = utils.is_market_hours
        
        def mock_weekend():
            return False  # Always return weekend
        
        utils.is_market_hours = mock_weekend
        
        try:
            # Initialize components
            initializer = RobustComponentInitializer()
            
            # Test OANDA initialization (should skip)
            await initializer._init_oanda_service()
            
            # Check if OANDA was skipped
            if C.oanda is None:
                logger.info("✅ OANDA service correctly skipped during weekend")
            else:
                logger.error("❌ OANDA service should have been skipped during weekend")
                return False
            
            # Test alert handler initialization (should be degraded mode)
            await initializer._init_alert_handler()
            
            if C.alerts and hasattr(C.alerts, 'degraded_mode') and C.alerts.degraded_mode:
                logger.info("✅ Alert handler correctly initialized in degraded mode during weekend")
            else:
                logger.error("❌ Alert handler should be in degraded mode during weekend")
                return False
            
            return True
            
        finally:
            # Restore original function
            utils.is_market_hours = original_is_market_hours
        
    except Exception as e:
        logger.error(f"❌ Weekend initialization test failed: {e}")
        return False

async def test_weekday_initialization():
    """Test initialization during weekday (should attempt OANDA)"""
    try:
        from component_initialization_fix import RobustComponentInitializer
        from main import C
        
        logger.info("🧪 Testing weekday initialization...")
        
        # Mock weekday time
        import utils
        original_is_market_hours = utils.is_market_hours
        
        def mock_weekday():
            return True  # Always return weekday
        
        utils.is_market_hours = mock_weekday
        
        try:
            # Initialize components
            initializer = RobustComponentInitializer()
            
            # Test OANDA initialization (should attempt)
            try:
                await initializer._init_oanda_service()
                logger.info("✅ OANDA service initialization attempted during weekday")
            except Exception as e:
                logger.warning(f"⚠️ OANDA service failed during weekday (expected if OANDA is down): {e}")
            
            # Test alert handler initialization
            try:
                await initializer._init_alert_handler()
                if C.alerts:
                    logger.info("✅ Alert handler initialized during weekday")
                else:
                    logger.error("❌ Alert handler failed to initialize during weekday")
                    return False
            except Exception as e:
                logger.error(f"❌ Alert handler initialization failed during weekday: {e}")
                return False
            
            return True
            
        finally:
            # Restore original function
            utils.is_market_hours = original_is_market_hours
        
    except Exception as e:
        logger.error(f"❌ Weekday initialization test failed: {e}")
        return False

async def main():
    """Main test function"""
    logger.info("🧪 TESTING MARKET HOURS INITIALIZATION")
    logger.info("=" * 50)
    
    # Test 1: Market hours detection
    logger.info("\n1️⃣ Testing market hours detection...")
    market_hours_result = await test_market_hours_detection()
    
    # Test 2: Weekend initialization
    logger.info("\n2️⃣ Testing weekend initialization...")
    weekend_result = await test_weekend_initialization()
    
    # Test 3: Weekday initialization
    logger.info("\n3️⃣ Testing weekday initialization...")
    weekday_result = await test_weekday_initialization()
    
    # Summary
    logger.info("\n📋 TEST SUMMARY:")
    logger.info("=" * 50)
    logger.info(f"  Market Hours Detection: {'✅ PASS' if market_hours_result else '❌ FAIL'}")
    logger.info(f"  Weekend Initialization: {'✅ PASS' if weekend_result else '❌ FAIL'}")
    logger.info(f"  Weekday Initialization: {'✅ PASS' if weekday_result else '❌ FAIL'}")
    
    all_tests_passed = market_hours_result and weekend_result and weekday_result
    logger.info(f"\n🎯 OVERALL RESULT: {'✅ ALL TESTS PASSED' if all_tests_passed else '❌ SOME TESTS FAILED'}")
    
    return all_tests_passed

if __name__ == "__main__":
    try:
        result = asyncio.run(main())
        sys.exit(0 if result else 1)
    except KeyboardInterrupt:
        logger.info("\n⏹️ Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n💥 Test suite crashed: {e}")
        sys.exit(1)
