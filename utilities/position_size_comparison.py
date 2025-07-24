#!/usr/bin/env python3
"""
Position Size Comparison: 15% vs 20% allocation
"""

def compare_allocations():
    """Compare position sizes before and after the change"""
    
    print("📊 POSITION SIZE COMPARISON: 15% vs 20% ALLOCATION")
    print("=" * 60)
    
    # Your current positions
    positions = {
        "USD_THB": {"price": 32.613},
        "GBP_JPY": {"price": 188.564}
    }
    
    # Estimated account balance based on your current positions
    account_balance = 8000  # Approximate based on diagnostic
    leverage = 30
    
    print(f"Account Balance: ${account_balance:,}")
    print(f"Leverage: {leverage}x")
    print()
    
    for symbol, data in positions.items():
        price = data["price"]
        
        print(f"🔍 {symbol} (Price: {price})")
        print("-" * 40)
        
        # Calculate for 15% allocation
        allocation_15 = 15.0
        notional_15 = account_balance * (allocation_15 / 100.0) * leverage
        units_15 = int(notional_15 / price)
        
        # Calculate for 20% allocation
        allocation_20 = 20.0
        notional_20 = account_balance * (allocation_20 / 100.0) * leverage
        units_20 = int(notional_20 / price)
        
        # Calculate increase
        increase_units = units_20 - units_15
        increase_percent = ((units_20 - units_15) / units_15) * 100
        
        print(f"   15% Allocation: {units_15:,} units (${notional_15:,.0f} notional)")
        print(f"   20% Allocation: {units_20:,} units (${notional_20:,.0f} notional)")
        print(f"   Increase: +{increase_units:,} units (+{increase_percent:.1f}%)")
        print()
    
    print("=" * 60)
    print("📈 SUMMARY OF CHANGES:")
    print("=" * 60)
    print("✅ Changed allocation from 15% to 20% in both files:")
    print("   - alert_handler.py (line 366)")
    print("   - core/alert_handler.py (line 381)")
    print()
    print("🎯 EXPECTED RESULTS:")
    print("   - 33% larger position sizes")
    print("   - More aggressive position sizing")
    print("   - Higher exposure per trade")
    print()
    print("⚠️  RISK CONSIDERATIONS:")
    print("   - Higher allocation = higher risk")
    print("   - Monitor your account balance")
    print("   - Consider reducing if drawdown increases")
    print()
    print("🔧 ALTERNATIVE ADJUSTMENTS:")
    print("   - 25% allocation = 67% larger positions")
    print("   - 30% allocation = 100% larger positions")
    print("   - Max recommended: 25% for most accounts")

if __name__ == "__main__":
    compare_allocations() 