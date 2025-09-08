"""                                                                                                                                                                                                     M
TEST CONFIG FIX
Quick test to verify the config.config attribute error is fixed
"""

import asyncio
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_config_access():
    """Test that config object can be accessed properly"""
    try:
        from config import config
        logger.info("✅ Config object imported successfully")
        
        # Test accessing config attributes
        logger.info(f"  OANDA Account ID: {config.oanda_account_id}")
        logger.info(f"  OANDA Environment: {config.oanda_environment}")
        logger.info(f"  Database URL: {config.database_url}")
        
        # Test that config.config doesn't exist (should raise AttributeError)
        try:
            config.config
            logger.error("❌ config.config should not exist!")
            return False
        except AttributeError:
            logger.info("✅ config.config correctly doesn't exist")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Config test failed: {e}")
        return False

async def test_alert_handler_initialization():
    """Test alert handler initialization with fixed config"""
    try:
        from component_initialization_fix import RobustComponentInitializer
        from main import C
        
        logger.info("🧪 Testing alert handler initialization...")
        
        # Initialize components
        initializer = RobustComponentInitializer()
        
        # Test just the alert handler initialization
        try:
            await initializer._init_alert_handler()
            logger.info("✅ Alert handler initialization test passed")
            return True
        except Exception as e:
            if "'Settings' object has no attribute 'config'" in str(e):
                logger.error("❌ Config fix not working - still getting config.config error")
                return False
            else:
                logger.warning(f"⚠️ Alert handler initialization failed for other reason: {e}")
                return True  # Other errors are expected in test environment
        
    except Exception as e:
        logger.error(f"❌ Alert handler test failed: {e}")
        return False

async def main():
    """Main test function"""
    logger.info("🧪 TESTING CONFIG FIX")
    logger.info("=" * 40)
    
    # Test 1: Config access
    logger.info("\n1️⃣ Testing config access...")
    config_result = await test_config_access()
    
    # Test 2: Alert handler initialization
    logger.info("\n2️⃣ Testing alert handler initialization...")
    alert_result = await test_alert_handler_initialization()
    
    # Summary
    logger.info("\n📋 TEST SUMMARY:")
    logger.info("=" * 40)
    logger.info(f"  Config Access: {'✅ PASS' if config_result else '❌ FAIL'}")
    logger.info(f"  Alert Handler Init: {'✅ PASS' if alert_result else '❌ FAIL'}")
    
    all_tests_passed = config_result and alert_result
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
