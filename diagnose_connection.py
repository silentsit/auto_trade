"""
OANDA Connection Diagnostic Tool
Tests connection stability and identifies issues with the trading bot's OANDA integration.

Usage: python utilities/diagnose_connection.py
"""

import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import config
from oanda_service import OandaService

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_connection_stability():
    """Test OANDA connection stability with multiple sequential requests"""
    logger.info("=" * 80)
    logger.info("OANDA CONNECTION DIAGNOSTIC TEST")
    logger.info("=" * 80)
    
    # Initialize OANDA service
    logger.info("\n1. Initializing OANDA service...")
    oanda_service = OandaService(config)
    
    try:
        await oanda_service.initialize()
        logger.info("✅ OANDA service initialized successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize OANDA service: {e}")
        return False
    
    # Test 1: Single account info request
    logger.info("\n2. Testing single account info request...")
    try:
        account_info = await oanda_service.get_account_info()
        balance = account_info.get('balance', 'N/A')
        logger.info(f"✅ Account info retrieved: Balance = {balance}")
    except Exception as e:
        logger.error(f"❌ Account info request failed: {e}")
        return False
    
    # Test 2: Multiple sequential pricing requests
    logger.info("\n3. Testing sequential pricing requests (simulating real usage)...")
    test_symbols = ['EUR_USD', 'GBP_USD', 'USD_JPY', 'AUD_USD', 'USD_CAD']
    failures = 0
    
    for i in range(5):  # 5 rounds of requests
        try:
            logger.info(f"  Round {i+1}/5...")
            prices = await oanda_service.get_current_prices(test_symbols)
            success_count = len(prices)
            if success_count == len(test_symbols):
                logger.info(f"  ✅ All {success_count} prices retrieved successfully")
            else:
                logger.warning(f"  ⚠️ Only {success_count}/{len(test_symbols)} prices retrieved")
                failures += 1
            
            # Small delay between rounds
            await asyncio.sleep(2)
        except Exception as e:
            logger.error(f"  ❌ Pricing request failed: {e}")
            failures += 1
    
    if failures == 0:
        logger.info("✅ All sequential pricing requests succeeded")
    else:
        logger.warning(f"⚠️ {failures}/5 pricing rounds had issues")
    
    # Test 3: Batch pricing with many symbols
    logger.info("\n4. Testing batch pricing with 20 symbols...")
    large_symbol_list = [
        'EUR_USD', 'GBP_USD', 'USD_JPY', 'AUD_USD', 'USD_CAD',
        'NZD_USD', 'EUR_GBP', 'EUR_JPY', 'GBP_JPY', 'AUD_JPY',
        'USD_CHF', 'EUR_CHF', 'GBP_CHF', 'CHF_JPY', 'CAD_JPY',
        'AUD_NZD', 'EUR_AUD', 'GBP_AUD', 'EUR_CAD', 'GBP_CAD'
    ]
    
    try:
        batch_prices = await oanda_service.get_current_prices(large_symbol_list)
        success_rate = (len(batch_prices) / len(large_symbol_list)) * 100
        logger.info(f"✅ Batch pricing: {len(batch_prices)}/{len(large_symbol_list)} symbols ({success_rate:.1f}% success)")
        
        if success_rate < 80:
            logger.warning("⚠️ Low batch pricing success rate - connection may be unstable")
    except Exception as e:
        logger.error(f"❌ Batch pricing failed: {e}")
        return False
    
    # Test 4: Connection health status
    logger.info("\n5. Checking connection health metrics...")
    try:
        status = await oanda_service.get_connection_status()
        logger.info(f"  Health Score: {status.get('health_score')}/100")
        logger.info(f"  Health Status: {status.get('health_status')}")
        logger.info(f"  Circuit Breaker Active: {status.get('circuit_breaker_active')}")
        logger.info(f"  Can Trade: {status.get('can_trade')}")
        logger.info(f"  Connection State: {status.get('connection_state')}")
        logger.info(f"  Connection Errors: {status.get('connection_errors_count')}")
        
        if status.get('health_score', 0) < 50:
            logger.warning("⚠️ Connection health is below 50 - may need intervention")
    except Exception as e:
        logger.error(f"❌ Failed to get connection status: {e}")
    
    # Test 5: Historical data retrieval
    logger.info("\n6. Testing historical data retrieval...")
    try:
        historical_data = await oanda_service.get_historical_data('EUR_USD', count=100, granularity='H1')
        if historical_data is not None and len(historical_data) > 0:
            logger.info(f"✅ Historical data retrieved: {len(historical_data)} candles")
        else:
            logger.warning("⚠️ Historical data returned empty or None")
    except Exception as e:
        logger.error(f"❌ Historical data retrieval failed: {e}")
    
    # Final summary
    logger.info("\n" + "=" * 80)
    logger.info("DIAGNOSTIC SUMMARY")
    logger.info("=" * 80)
    
    final_status = await oanda_service.get_connection_status()
    health_score = final_status.get('health_score', 0)
    
    if health_score >= 90:
        logger.info("✅ CONNECTION EXCELLENT - Bot should trade normally")
        return True
    elif health_score >= 70:
        logger.info("✅ CONNECTION GOOD - Bot should operate with minor issues")
        return True
    elif health_score >= 50:
        logger.warning("⚠️ CONNECTION DEGRADED - Bot may experience intermittent failures")
        return False
    else:
        logger.error("❌ CONNECTION POOR - Bot will likely fail to execute trades")
        return False


async def test_circuit_breaker():
    """Test circuit breaker functionality"""
    logger.info("\n7. Testing circuit breaker recovery...")
    oanda_service = OandaService(config)
    await oanda_service.initialize()
    
    # Simulate circuit breaker activation
    original_failures = oanda_service.circuit_breaker_failures
    logger.info(f"  Current circuit breaker failures: {original_failures}")
    logger.info(f"  Circuit breaker threshold: {oanda_service.circuit_breaker_threshold}")
    
    # Check if circuit breaker would activate
    if original_failures >= oanda_service.circuit_breaker_threshold:
        logger.warning("  ⚠️ Circuit breaker is currently ACTIVE or near activation")
        logger.info("  Waiting for cooldown period...")
        await asyncio.sleep(5)
    else:
        logger.info("  ✅ Circuit breaker is not active")


async def main():
    """Main diagnostic routine"""
    try:
        # Run connection stability tests
        connection_ok = await test_connection_stability()
        
        # Run circuit breaker test
        await test_circuit_breaker()
        
        logger.info("\n" + "=" * 80)
        if connection_ok:
            logger.info("✅ DIAGNOSIS COMPLETE: System is ready for trading")
            logger.info("Recommendation: Restart the bot to apply connection improvements")
        else:
            logger.warning("⚠️ DIAGNOSIS COMPLETE: System has connection issues")
            logger.warning("Recommendation: Check OANDA API credentials and network connectivity")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"❌ Diagnostic script failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

