#!/usr/bin/env python3
"""
Test script to show margin utilization improvement
"""

def test_margin_improvement():
    """Test the margin utilization improvement"""
    
    print("🚀 MARGIN UTILIZATION IMPROVEMENT TEST")
    print("=" * 50)
    
    # Test parameters
    account_balance = 10000  # $10k account
    entry_price = 1.0850     # EUR/USD
    atr = 0.0050            # 50 pips ATR
    risk_percent = 20.0     # 20% risk
    leverage = 50.0         # 50:1 leverage
    
    print(f"📊 Test Parameters:")
    print(f"   Account Balance: ${account_balance:,.2f}")
    print(f"   Entry Price: {entry_price}")
    print(f"   ATR: {atr} ({atr * 10000:.1f} pips)")
    print(f"   Risk Percentage: {risk_percent}%")
    print(f"   Leverage: {leverage}:1")
    print()
    
    # OLD APPROACH (Risk-based only)
    print("📉 OLD APPROACH: Risk-Based Only")
    print("-" * 35)
    
    # Old ATR multiplier (2x)
    old_atr_mult = 2.0
    stop_loss_distance = atr * old_atr_mult
    risk_amount = account_balance * (risk_percent / 100.0)
    
    old_size = risk_amount / stop_loss_distance
    old_margin = (old_size * entry_price) / leverage
    old_margin_pct = (old_margin / account_balance) * 100
    
    print(f"   ATR Multiplier: {old_atr_mult}x")
    print(f"   Stop Distance: {stop_loss_distance * 10000:.1f} pips")
    print(f"   Position Size: {old_size:,.0f} units")
    print(f"   Margin Used: {old_margin_pct:.2f}%")
    print()
    
    # NEW APPROACH (Hybrid + Reduced ATR)
    print("📈 NEW APPROACH: Hybrid Sizing + Reduced ATR")
    print("-" * 45)
    
    # New ATR multiplier (1.5x)
    new_atr_mult = 1.5
    stop_loss_distance = atr * new_atr_mult
    risk_amount = account_balance * (risk_percent / 100.0)
    
    # Risk-based size
    risk_based_size = risk_amount / stop_loss_distance
    
    # Percentage-based size
    target_position_value = account_balance * (risk_percent / 100.0) * leverage
    percentage_based_size = target_position_value / entry_price
    
    # Use the larger of the two (hybrid approach)
    new_size = max(risk_based_size, percentage_based_size)
    new_margin = (new_size * entry_price) / leverage
    new_margin_pct = (new_margin / account_balance) * 100
    
    print(f"   ATR Multiplier: {new_atr_mult}x")
    print(f"   Stop Distance: {stop_loss_distance * 10000:.1f} pips")
    print(f"   Risk-Based Size: {risk_based_size:,.0f} units")
    print(f"   Percentage-Based Size: {percentage_based_size:,.0f} units")
    print(f"   Hybrid Size (Larger): {new_size:,.0f} units")
    print(f"   Margin Used: {new_margin_pct:.2f}%")
    print()
    
    # IMPROVEMENT CALCULATION
    print("🎯 IMPROVEMENT SUMMARY")
    print("-" * 25)
    
    size_increase = ((new_size - old_size) / old_size) * 100
    margin_increase = new_margin_pct - old_margin_pct
    
    print(f"   Position Size Increase: {size_increase:+.1f}%")
    print(f"   Margin Utilization Increase: {margin_increase:+.2f}%")
    print(f"   New Position Size: {new_size:,.0f} units")
    print(f"   New Margin Usage: {new_margin_pct:.2f}%")
    print()
    
    # COMPARISON WITH YOUR 2.48%
    print("🔍 COMPARISON WITH YOUR 2.48%")
    print("-" * 35)
    
    your_margin_pct = 2.48
    your_margin = account_balance * (your_margin_pct / 100.0)
    your_position_value = your_margin * leverage
    your_position_size = your_position_value / entry_price
    
    print(f"   Your Current Margin: {your_margin_pct}%")
    print(f"   Your Position Size: {your_position_size:,.0f} units")
    print(f"   New Margin: {new_margin_pct:.2f}%")
    print(f"   New Position Size: {new_size:,.0f} units")
    print()
    
    improvement_vs_yours = ((new_size - your_position_size) / your_position_size) * 100
    print(f"   🚀 IMPROVEMENT: {improvement_vs_yours:+.1f}% larger positions!")
    print(f"   📊 MARGIN UTILIZATION: {new_margin_pct:.2f}% vs {your_margin_pct}%")
    
    if new_margin_pct > your_margin_pct:
        print(f"   ✅ This will significantly increase your position sizes!")
    else:
        print(f"   ⚠️ Your current setup might be using different parameters")

if __name__ == "__main__":
    test_margin_improvement() 