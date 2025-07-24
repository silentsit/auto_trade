#!/usr/bin/env python3
"""
Test script to debug BTCUSD execution issues
"""
import asyncio
import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oanda_service import OandaService
from config import config
from utils import format_symbol_for_oanda, calculate_position_size

async def test_crypto_availability():
    """Test crypto availability and BTCUSD execution"""
    print("🔍 Testing Crypto Availability...")
    
    try:
        # Initialize OANDA service
        service = OandaService(config)
        await service.initialize()
        
        # Check crypto availability
        crypto_availability = await service.check_crypto_availability()
        print(f"📊 Crypto availability: {crypto_availability}")
        
        # Test BTC_USD specifically
        btc_supported = await service.is_crypto_supported('BTC_USD')
        print(f"🔸 BTC_USD supported: {btc_supported}")
        
        # Test symbol formatting
        test_symbols = ['BTCUSD', 'btcusd', 'BTC_USD']
        for symbol in test_symbols:
            formatted = format_symbol_for_oanda(symbol)
            print(f"🔸 {symbol} -> {formatted}")
        
        # Test position sizing for BTC
        if btc_supported:
            print("\n💰 Testing BTC position sizing...")
            account_balance = await service.get_account_balance()
            print(f"Account balance: ${account_balance:,.2f}")
            
            current_price = await service.get_current_price('BTC_USD', 'BUY')
            print(f"Current BTC price: ${current_price:,.2f}")
            
            # Test position size calculation
            size, details = await calculate_position_size(
                symbol='BTC_USD',
                entry_price=current_price,
                risk_percent=5.0,
                account_balance=account_balance,
                leverage=2.0,  # Crypto leverage
                timeframe='H1'
            )
            print(f"Calculated position size: {size:,} units")
            print(f"Position details: {details}")
            
            # Test trade execution payload
            payload = {
                "symbol": "BTC_USD",
                "action": "BUY",
                "risk_percent": 5.0,
                "signal_price": current_price,
                "timeframe": "H1"
            }
            
            print(f"\n🚀 Testing trade execution with payload: {payload}")
            success, result = await service.execute_trade(payload)
            print(f"Trade execution result: {success}")
            if not success:
                print(f"Error: {result}")
            else:
                print(f"Success: {result}")
        
        else:
            print("❌ BTC_USD not supported - cannot test execution")
            
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_crypto_availability()) 