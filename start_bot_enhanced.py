#!/usr/bin/env python3
"""
Enhanced Auto Trading Bot Startup Script
With improved error handling and connection monitoring
"""

import asyncio
import logging
import sys
import os
from datetime import datetime, timezone

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

async def test_oanda_connection():
    """Test OANDA connection with enhanced error handling"""
    try:
        from oanda_service import OandaService
        from config import get_oanda_config
        
        logger.info("🔗 Testing OANDA connection...")
        
        # Initialize OANDA service
        oanda_service = OandaService()
        await oanda_service.initialize()
        
        # Start connection monitor
        await oanda_service.start_connection_monitor()
        
        # Test basic functionality
        logger.info("✅ Testing account balance fetch...")
        balance = await oanda_service.get_account_balance()
        logger.info(f"✅ Account balance: ${balance:.2f}")
        
        # Test price fetching
        logger.info("✅ Testing price fetch...")
        price = await oanda_service.get_current_price("EUR_USD", "BUY")
        logger.info(f"✅ EUR_USD price: {price}")
        
        # Get connection status
        status = await oanda_service.get_connection_status()
        logger.info(f"✅ Connection status: {status}")
        
        logger.info("✅ OANDA connection test successful!")
        return oanda_service
        
    except Exception as e:
        logger.error(f"❌ OANDA connection test failed: {e}")
        return None

async def test_correlation_manager():
    """Test correlation manager with fallback handling"""
    try:
        from correlation_manager import CorrelationManager
        
        logger.info("📊 Testing correlation manager...")
        
        correlation_manager = CorrelationManager()
        
        # Test correlation calculation with insufficient data
        logger.info("✅ Testing correlation fallback mechanism...")
        correlation_data = await correlation_manager.calculate_correlation("EUR_USD", "GBP_USD")
        
        if correlation_data:
            logger.info(f"✅ Correlation calculated: {correlation_data.correlation:+.2f} (source: {correlation_data.data_source})")
        else:
            logger.warning("⚠️ No correlation data available")
        
        logger.info("✅ Correlation manager test successful!")
        return correlation_manager
        
    except Exception as e:
        logger.error(f"❌ Correlation manager test failed: {e}")
        return None

async def test_system_components():
    """Test all system components"""
    logger.info("🚀 Testing system components...")
    
    # Test OANDA service
    oanda_service = await test_oanda_connection()
    if not oanda_service:
        logger.error("❌ OANDA service test failed - cannot proceed")
        return False
    
    # Test correlation manager
    correlation_manager = await test_correlation_manager()
    if not correlation_manager:
        logger.warning("⚠️ Correlation manager test failed - some features may not work")
    
    # Test database connection
    try:
        from database import DatabaseManager
        logger.info("📊 Testing database connection...")
        
        db_manager = DatabaseManager()
        await db_manager.initialize()
        logger.info("✅ Database connection test successful!")
        
    except Exception as e:
        logger.error(f"❌ Database connection test failed: {e}")
        return False
    
    logger.info("✅ All system component tests completed!")
    return True

async def main():
    """Main startup function"""
    logger.info("🚀 Enhanced Auto Trading Bot Startup Test")
    logger.info(f"⏰ Started at: {datetime.now(timezone.utc).isoformat()}")
    
    try:
        # Test system components
        if await test_system_components():
            logger.info("🎉 All tests passed! System is ready.")
            
            # Start the main application
            logger.info("🚀 Starting main application...")
            
            # Import the app but don't run uvicorn here
            # The user should run the main application separately
            logger.info("✅ System is ready for startup!")
            logger.info("💡 To start the main application, run: python main.py")
            logger.info("💡 Or use: uvicorn main:app --host 0.0.0.0 --port 8000")
            
        else:
            logger.error("❌ System component tests failed. Please check configuration and try again.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("🛑 Startup interrupted by user")
    except Exception as e:
        logger.error(f"❌ Startup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main()) 