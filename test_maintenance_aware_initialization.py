"""
TEST SCRIPT FOR MAINTENANCE-AWARE INITIALIZATION
Tests the robust component initialization with maintenance mode handling
"""

import asyncio
import logging
import sys
from datetime import datetime
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

async def test_component_initialization():
    """Test the robust component initialization system"""
    logger.info("🧪 TESTING MAINTENANCE-AWARE COMPONENT INITIALIZATION")
    logger.info("=" * 60)
    
    try:
        # Import the robust initialization system
        from component_initialization_fix import robust_initialize_components
        
        # Run the initialization
        logger.info("🚀 Starting robust component initialization...")
        system_status = await robust_initialize_components()
        
        # Display results
        logger.info("📊 INITIALIZATION RESULTS:")
        logger.info(f"  System Operational: {system_status.get('system_operational', False)}")
        logger.info(f"  Critical Components Ready: {system_status.get('critical_components_ready', False)}")
        logger.info(f"  Total Components: {system_status.get('total_components', 0)}")
        logger.info(f"  Initialized Components: {system_status.get('initialized_components', 0)}")
        
        # Check component details
        components = system_status.get('components', {})
        logger.info("\n🔍 COMPONENT STATUS:")
        for name, status in components.items():
            status_icon = "✅" if status.get('initialized', False) else "❌"
            component_type = status.get('type', 'unknown')
            logger.info(f"  {status_icon} {name}: {component_type}")
        
        # Test API endpoints
        await test_api_endpoints()
        
        return system_status
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        return None

async def test_api_endpoints():
    """Test the API endpoints for maintenance awareness"""
    logger.info("\n🌐 TESTING API ENDPOINTS:")
    
    try:
        # Test component status endpoint
        from main import C
        
        # Simulate API call to component status
        components = {
            "storage": {
                "initialized": C.storage is not None,
                "type": "critical" if C.storage else "missing"
            },
            "oanda_service": {
                "initialized": C.oanda is not None,
                "type": "critical" if C.oanda else "missing"
            },
            "risk_manager": {
                "initialized": C.risk is not None,
                "type": "critical" if C.risk else "missing"
            },
            "position_tracker": {
                "initialized": C.tracker is not None,
                "type": "critical" if C.tracker else "missing"
            },
            "alert_handler": {
                "initialized": C.alerts is not None,
                "type": "critical" if C.alerts else "missing"
            }
        }
        
        # Check OANDA operational status
        oanda_operational = True
        if C.oanda and hasattr(C.oanda, 'is_operational'):
            oanda_operational = C.oanda.is_operational()
        elif C.oanda and hasattr(C.oanda, 'can_trade'):
            oanda_operational = C.oanda.can_trade()
        
        # Check alert handler status
        alert_handler_status = {}
        if C.alerts and hasattr(C.alerts, 'get_status'):
            try:
                alert_handler_status = C.alerts.get_status()
            except Exception as e:
                alert_handler_status = {"error": str(e)}
        
        logger.info(f"  📡 OANDA Operational: {oanda_operational}")
        logger.info(f"  🚨 Degraded Mode: {not oanda_operational}")
        logger.info(f"  💰 Can Trade: {oanda_operational}")
        
        if alert_handler_status:
            logger.info(f"  📊 Alert Handler Status: {json.dumps(alert_handler_status, indent=2)}")
        
        return {
            "oanda_operational": oanda_operational,
            "degraded_mode": not oanda_operational,
            "can_trade": oanda_operational,
            "alert_handler_status": alert_handler_status
        }
        
    except Exception as e:
        logger.error(f"❌ API test failed: {e}")
        return None

async def test_degraded_mode_alert_handling():
    """Test alert handling in degraded mode"""
    logger.info("\n🚨 TESTING DEGRADED MODE ALERT HANDLING:")
    
    try:
        from main import C
        
        if not C.alerts:
            logger.warning("  ⚠️ Alert handler not available")
            return False
        
        # Test alert in degraded mode
        test_alert = {
            "symbol": "EUR_USD",
            "action": "BUY",
            "timeframe": "15",
            "comment": "Test alert in degraded mode",
            "alert_id": "test_degraded_001"
        }
        
        logger.info(f"  📨 Sending test alert: {test_alert['symbol']} {test_alert['action']}")
        
        # Check if handler has degraded mode capability
        if hasattr(C.alerts, 'degraded_mode') and C.alerts.degraded_mode:
            logger.info("  🚨 Alert handler is in DEGRADED MODE")
            
            # Test degraded mode alert handling
            if hasattr(C.alerts, 'handle_alert'):
                result = await C.alerts.handle_alert(test_alert)
                logger.info(f"  ✅ Degraded mode alert result: {result}")
                return True
            else:
                logger.warning("  ⚠️ Alert handler doesn't support degraded mode alert handling")
                return False
        else:
            logger.info("  ✅ Alert handler is in NORMAL MODE")
            
            # Test normal alert processing
            if hasattr(C.alerts, 'process_alert'):
                result = await C.alerts.process_alert(test_alert)
                logger.info(f"  ✅ Normal mode alert result: {result}")
                return True
            else:
                logger.warning("  ⚠️ Alert handler doesn't support normal alert processing")
                return False
        
    except Exception as e:
        logger.error(f"❌ Degraded mode test failed: {e}")
        return False

async def test_maintenance_aware_oanda():
    """Test the maintenance-aware OANDA wrapper"""
    logger.info("\n🔧 TESTING MAINTENANCE-AWARE OANDA WRAPPER:")
    
    try:
        from maintenance_aware_oanda import create_maintenance_aware_oanda_service, create_degraded_mode_alert_handler
        
        # Test degraded mode alert handler
        degraded_handler = create_degraded_mode_alert_handler()
        logger.info(f"  ✅ Degraded mode alert handler created: {type(degraded_handler).__name__}")
        
        # Test alert handling in degraded mode
        test_alert = {
            "symbol": "GBP_USD",
            "action": "SELL",
            "timeframe": "1H",
            "comment": "Test degraded mode alert"
        }
        
        result = await degraded_handler.handle_alert(test_alert)
        logger.info(f"  📨 Degraded mode alert result: {result}")
        
        # Test status
        status = degraded_handler.get_status()
        logger.info(f"  📊 Degraded handler status: {json.dumps(status, indent=2)}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Maintenance-aware OANDA test failed: {e}")
        return False

async def main():
    """Main test function"""
    logger.info("🚀 STARTING MAINTENANCE-AWARE INITIALIZATION TESTS")
    logger.info("=" * 60)
    
    # Test 1: Component initialization
    logger.info("\n1️⃣ TESTING COMPONENT INITIALIZATION")
    init_result = await test_component_initialization()
    
    # Test 2: API endpoints
    logger.info("\n2️⃣ TESTING API ENDPOINTS")
    api_result = await test_api_endpoints()
    
    # Test 3: Degraded mode alert handling
    logger.info("\n3️⃣ TESTING DEGRADED MODE ALERT HANDLING")
    degraded_result = await test_degraded_mode_alert_handling()
    
    # Test 4: Maintenance-aware OANDA wrapper
    logger.info("\n4️⃣ TESTING MAINTENANCE-AWARE OANDA WRAPPER")
    maintenance_result = await test_maintenance_aware_oanda()
    
    # Summary
    logger.info("\n📋 TEST SUMMARY:")
    logger.info("=" * 60)
    logger.info(f"  Component Initialization: {'✅ PASS' if init_result else '❌ FAIL'}")
    logger.info(f"  API Endpoints: {'✅ PASS' if api_result else '❌ FAIL'}")
    logger.info(f"  Degraded Mode Alert Handling: {'✅ PASS' if degraded_result else '❌ FAIL'}")
    logger.info(f"  Maintenance-Aware OANDA: {'✅ PASS' if maintenance_result else '❌ FAIL'}")
    
    # Overall result
    all_tests_passed = all([init_result, api_result, degraded_result, maintenance_result])
    logger.info(f"\n🎯 OVERALL RESULT: {'✅ ALL TESTS PASSED' if all_tests_passed else '❌ SOME TESTS FAILED'}")
    
    if all_tests_passed:
        logger.info("🎉 System is ready for maintenance-aware operation!")
    else:
        logger.warning("⚠️ Some issues detected - system may not be fully operational")
    
    return all_tests_passed

if __name__ == "__main__":
    # Run the tests
    try:
        result = asyncio.run(main())
        sys.exit(0 if result else 1)
    except KeyboardInterrupt:
        logger.info("\n⏹️ Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n💥 Test suite crashed: {e}")
        sys.exit(1)
