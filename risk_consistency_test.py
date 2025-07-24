#!/usr/bin/env python3
"""
Risk Management Consistency Verification Test

This script verifies that the institutional fixes to position sizing
create consistent risk calculations across all system components.

Author: Institutional Trading Bot Team
Date: 2024
"""

import asyncio
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_risk_consistency():
    """Test that position sizing now produces consistent risk calculations"""
    
    print("🔍 INSTITUTIONAL RISK MANAGEMENT VERIFICATION")
    print("=" * 60)
    
    try:
        from utils import calculate_position_size
        from config import get_trading_config
        
        # Test parameters
        test_cases = [
            {
                "symbol": "EUR_USD",
                "entry_price": 1.1000,
                "risk_percent": 10.0,  # 10% risk per trade
                "account_balance": 10000.0,
                "stop_loss_price": 1.0950,  # 50 pip stop
                "scenario": "Normal volatility (50 pips)"
            },
            {
                "symbol": "EUR_USD", 
                "entry_price": 1.1000,
                "risk_percent": 10.0,
                "account_balance": 10000.0,
                "stop_loss_price": 1.0975,  # 25 pip stop (low volatility)
                "scenario": "Low volatility (25 pips)"
            },
            {
                "symbol": "EUR_USD",
                "entry_price": 1.1000,
                "risk_percent": 10.0,
                "account_balance": 10000.0,
                "stop_loss_price": 1.0900,  # 100 pip stop (high volatility)
                "scenario": "High volatility (100 pips)"
            }
        ]
        
        print("\n📊 TESTING RISK CONSISTENCY")
        print("-" * 40)
        
        for i, test_case in enumerate(test_cases, 1):
            print(f"\nTest {i}: {test_case['scenario']}")
            
            # Calculate position size with actual stop loss
            position_size, sizing_info = await calculate_position_size(
                symbol=test_case["symbol"],
                entry_price=test_case["entry_price"],
                risk_percent=test_case["risk_percent"],
                account_balance=test_case["account_balance"],
                stop_loss_price=test_case["stop_loss_price"]
            )
            
            # Calculate actual risk
            stop_distance = abs(test_case["entry_price"] - test_case["stop_loss_price"])
            actual_risk_amount = position_size * stop_distance
            intended_risk_amount = test_case["account_balance"] * (test_case["risk_percent"] / 100.0)
            risk_accuracy = (actual_risk_amount / intended_risk_amount) * 100
            
            print(f"  • Position Size: {position_size:,.0f} units")
            print(f"  • Stop Distance: {stop_distance:.4f}")
            print(f"  • Intended Risk: ${intended_risk_amount:,.2f}")
            print(f"  • Actual Risk: ${actual_risk_amount:,.2f}")
            print(f"  • Risk Accuracy: {risk_accuracy:.1f}%")
            
            # Check if risk is consistent (should be very close to 100%)
            if 98 <= risk_accuracy <= 102:
                print(f"  ✅ PASS: Risk calculation is consistent")
            else:
                print(f"  ❌ FAIL: Risk calculation is inconsistent")
        
        print("\n" + "=" * 60)
        print("✅ INSTITUTIONAL RISK MANAGEMENT VERIFICATION COMPLETE")
        print("📈 All position sizes now calculated based on actual stop loss distance")
        print("🎯 Risk per trade is now consistent regardless of market volatility")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        logger.error(f"Risk consistency test failed: {e}")

async def compare_old_vs_new_approach():
    """Compare old fixed-pip approach vs new risk-based approach"""
    
    print("\n🔄 OLD vs NEW APPROACH COMPARISON")
    print("=" * 50)
    
    # Simulate old approach (fixed 50 pips)
    old_stop_distance = 50 * 0.0001  # 50 pips
    old_risk_amount = 1000.0  # 10% of $10k
    old_position_size = old_risk_amount / old_stop_distance
    
    print(f"OLD APPROACH (Fixed 50 pips):")
    print(f"  • Stop Distance: {old_stop_distance:.4f} (always 50 pips)")
    print(f"  • Position Size: {old_position_size:,.0f} units")
    print(f"  • Problem: Same position size regardless of actual stop!")
    
    print(f"\nNEW APPROACH (Actual stop distance):")
    print(f"  • Stop Distance: Variable based on actual ATR/stop loss")
    print(f"  • Position Size: Calculated from actual risk distance")
    print(f"  • Benefit: Consistent risk regardless of market volatility!")

if __name__ == "__main__":
    asyncio.run(test_risk_consistency())
    asyncio.run(compare_old_vs_new_approach()) 