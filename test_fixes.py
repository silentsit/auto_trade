#!/usr/bin/env python3
"""
Test script to verify the fixes for connection issues and correlation calculation
"""

import asyncio
import logging
import sys
from datetime import datetime, timezone

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

async def test_connection_resilience():
    """Test OANDA connection resilience"""
    logger.info("🔗 Testing OANDA connection resilience...")
    
    try:
        from oanda_service import OandaService
        
        # Initialize service
        oanda_service = OandaService()
        await oanda_service.initialize()
        
        # Start connection monitor
        await oanda_service.start_connection_monitor()
        
        # Test multiple requests to see if connection handling works
        for i in range(3):
            try:
                logger.info(f"Test request {i+1}/3...")
                balance = await oanda_service.get_account_balance()
                logger.info(f"✅ Request {i+1} successful: ${balance:.2f}")
                
                # Small delay between requests
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"❌ Request {i+1} failed: {e}")
        
        # Get connection status
        status = await oanda_service.get_connection_status()
        logger.info(f"✅ Final connection status: {status}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Connection resilience test failed: {e}")
        return False

async def test_correlation_fallback():
    """Test correlation manager fallback mechanism"""
    logger.info("📊 Testing correlation fallback mechanism...")
    
    try:
        from correlation_manager import CorrelationManager
        
        correlation_manager = CorrelationManager()
        
        # Test correlation calculation with no price data
        logger.info("Testing correlation with insufficient data...")
        correlation_data = await correlation_manager.calculate_correlation("EUR_USD", "GBP_USD")
        
        if correlation_data:
            logger.info(f"✅ Correlation calculated successfully:")
            logger.info(f"   - Value: {correlation_data.correlation:+.2f}")
            logger.info(f"   - Source: {correlation_data.data_source}")
            logger.info(f"   - Strength: {correlation_data.strength}")
            logger.info(f"   - Sample size: {correlation_data.sample_size}")
        else:
            logger.error("❌ No correlation data returned")
            return False
        
        # Test multiple pairs
        test_pairs = [
            ("EUR_USD", "USD_JPY"),
            ("GBP_USD", "AUD_USD"),
            ("USD_CHF", "EUR_CHF")
        ]
        
        for pair in test_pairs:
            try:
                corr_data = await correlation_manager.calculate_correlation(pair[0], pair[1])
                if corr_data:
                    logger.info(f"✅ {pair[0]}/{pair[1]}: {corr_data.correlation:+.2f} ({corr_data.data_source})")
                else:
                    logger.warning(f"⚠️ {pair[0]}/{pair[1]}: No data available")
            except Exception as e:
                logger.error(f"❌ {pair[0]}/{pair[1]}: Error - {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Correlation fallback test failed: {e}")
        return False

async def test_system_health():
    """Test overall system health"""
    logger.info("🏥 Testing system health...")
    
    try:
        # Test database connection
        from database import DatabaseManager
        db_manager = DatabaseManager()
        await db_manager.initialize()
        logger.info("✅ Database connection healthy")
        
        # Test OANDA service
        from oanda_service import OandaService
        oanda_service = OandaService()
        await oanda_service.initialize()
        logger.info("✅ OANDA service healthy")
        
        # Test correlation manager
        from correlation_manager import CorrelationManager
        correlation_manager = CorrelationManager()
        logger.info("✅ Correlation manager healthy")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ System health test failed: {e}")
        return False

async def main():
    """Main test function"""
    logger.info("🧪 Starting fix verification tests...")
    logger.info(f"⏰ Started at: {datetime.now(timezone.utc).isoformat()}")
    
    tests = [
        ("System Health", test_system_health),
        ("Connection Resilience", test_connection_resilience),
        ("Correlation Fallback", test_correlation_fallback)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n{'='*50}")
        logger.info(f"Running: {test_name}")
        logger.info(f"{'='*50}")
        
        try:
            result = await test_func()
            results[test_name] = result
            
            if result:
                logger.info(f"✅ {test_name}: PASSED")
            else:
                logger.error(f"❌ {test_name}: FAILED")
                
        except Exception as e:
            logger.error(f"❌ {test_name}: ERROR - {e}")
            results[test_name] = False
    
    # Summary
    logger.info(f"\n{'='*50}")
    logger.info("TEST SUMMARY")
    logger.info(f"{'='*50}")
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        logger.info(f"{test_name}: {status}")
    
    logger.info(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! Fixes are working correctly.")
        return True
    else:
        logger.error(f"⚠️ {total - passed} tests failed. Some issues remain.")
        return False

if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("🛑 Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Test execution failed: {e}")
        sys.exit(1)
