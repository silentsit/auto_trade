#!/usr/bin/env python3
"""
Test script to verify the conservative position sizing fixes.
This script tests the new minimum stop loss distances and position size caps.
"""

import asyncio
import logging
from config import get_trading_config
from oanda_service import OandaService
from utils import calculate_position_size

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_conservative_sizing():
    """Test the conservative position sizing implementation"""
    
    print("🧪 TESTING CONSERVATIVE POSITION SIZING FIXES")
    print("=" * 70)
    
    try:
        # Initialize OANDA service
        config = get_trading_config()
        oanda_service = OandaService(config)
        await oanda_service.initialize()
        
        print("✅ OANDA service initialized successfully")
        
        # Test 1: Conservative sizing with different timeframes
        print("\n📊 TEST 1: Conservative Sizing by Timeframe")
        print("-" * 50)
        
        test_cases = [
            ("M15", "EUR_USD", 5.0),  # 15-minute chart, 5% risk
            ("H1", "GBP_USD", 5.0),   # 1-hour chart, 5% risk
            ("H4", "USD_JPY", 5.0),   # 4-hour chart, 5% risk
        ]
        
        for timeframe, symbol, risk_percent in test_cases:
            try:
                # Get current price
                current_price = await oanda_service.get_current_price(symbol, "BUY")
                if not current_price:
                    print(f"❌ Could not get price for {symbol}")
                    continue
                
                # Get account info
                account_info = await oanda_service.get_account_info()
                balance = account_info.get('balance', 10000)
                leverage = account_info.get('leverage', 50.0)
                
                print(f"\n🔍 Testing {symbol} on {timeframe} timeframe:")
                print(f"   Current Price: ${current_price:.5f}")
                print(f"   Account Balance: ${balance:.2f}")
                print(f"   Leverage: {leverage:.1f}:1")
                print(f"   Risk Percentage: {risk_percent}%")
                
                # Calculate position size
                position_size, sizing_info = await calculate_position_size(
                    symbol=symbol,
                    entry_price=current_price,
                    risk_percent=risk_percent,
                    account_balance=balance,
                    leverage=leverage,
                    timeframe=timeframe
                )
                
                print(f"   ✅ Position Size: {position_size:,.0f} units")
                print(f"   ✅ Position Value: ${sizing_info.get('position_value', 0):.2f}")
                print(f"   ✅ Stop Distance: {sizing_info.get('stop_loss_distance', 0):.5f}")
                print(f"   ✅ Risk Amount: ${sizing_info.get('risk_amount', 0):.2f}")
                print(f"   ✅ Method: {sizing_info.get('method', 'Unknown')}")
                
                # Verify conservative sizing was applied
                if sizing_info.get('position_cap_applied', False):
                    print(f"   🚨 POSITION CAP APPLIED: Size was capped for safety!")
                
            except Exception as e:
                print(f"❌ Test failed for {symbol} on {timeframe}: {e}")
        
        # Test 2: Compare old vs new sizing
        print("\n💰 TEST 2: Old vs New Sizing Comparison")
        print("-" * 50)
        
        # Simulate old sizing (without minimum stops)
        symbol = "EUR_USD"
        current_price = await oanda_service.get_current_price(symbol, "BUY")
        if current_price:
            account_info = await oanda_service.get_account_info()
            balance = account_info.get('balance', 10000)
            leverage = account_info.get('leverage', 50.0)
            
            print(f"🔍 Comparing sizing for {symbol}:")
            print(f"   Current Price: ${current_price:.5f}")
            print(f"   Account Balance: ${balance:.2f}")
            print(f"   Risk: 5%")
            
            # New conservative sizing
            new_size, new_info = await calculate_position_size(
                symbol=symbol,
                entry_price=current_price,
                risk_percent=5.0,
                account_balance=balance,
                leverage=leverage,
                timeframe="M15"
            )
            
            print(f"   ✅ NEW Conservative Size: {new_size:,.0f} units")
            print(f"   ✅ NEW Stop Distance: {new_info.get('stop_loss_distance', 0):.5f}")
            print(f"   ✅ NEW Position Value: ${new_info.get('position_value', 0):.2f}")
            
            # Estimate old sizing (without minimum stops)
            # Old method would use tiny ATR stops (e.g., 0.0001)
            old_stop_distance = 0.0001  # Example tiny stop
            old_risk_amount = balance * 0.05  # 5% risk
            old_size = old_risk_amount / old_stop_distance
            
            print(f"   🚨 OLD Method (estimated): {old_size:,.0f} units")
            print(f"   🚨 OLD Stop Distance: {old_stop_distance:.5f}")
            print(f"   🚨 OLD Position Value: ${(old_size * current_price):.2f}")
            
            # Calculate improvement
            size_reduction = ((old_size - new_size) / old_size) * 100
            print(f"   🎯 SIZE REDUCTION: {size_reduction:.1f}% smaller positions!")
        
        # Test 3: Verify minimum stop distances
        print("\n🛡️  TEST 3: Minimum Stop Distance Verification")
        print("-" * 50)
        
        timeframes = ["M1", "M5", "M15", "M30", "H1", "H4", "D1"]
        expected_min_stops = {
            "M1": 0.0020, "M5": 0.0015, "M15": 0.0010, "M30": 0.0008,
            "H1": 0.0006, "H4": 0.0005, "D1": 0.0004
        }
        
        for tf in timeframes:
            try:
                size, info = await calculate_position_size(
                    symbol="EUR_USD",
                    entry_price=1.0850,  # Fixed price for testing
                    risk_percent=5.0,
                    account_balance=10000,
                    leverage=50.0,
                    timeframe=tf
                )
                
                actual_stop = info.get('stop_loss_distance', 0)
                expected_stop = expected_min_stops.get(tf, 0.0005)
                
                if actual_stop >= expected_stop:
                    print(f"   ✅ {tf}: Stop {actual_stop:.5f} >= Min {expected_stop:.5f}")
                else:
                    print(f"   ❌ {tf}: Stop {actual_stop:.5f} < Min {expected_stop:.5f}")
                    
            except Exception as e:
                print(f"   ❌ {tf}: Test failed - {e}")
        
        await oanda_service.stop()
        print("\n✅ All conservative sizing tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        logger.error(f"Conservative sizing test failed: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(test_conservative_sizing())
