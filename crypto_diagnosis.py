#!/usr/bin/env python3
"""
OANDA Crypto Trading Diagnosis Tool
Checks if cryptocurrency instruments are available and tradeable in your OANDA environment
"""

import asyncio
import json
import sys
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

async def comprehensive_trading_diagnosis():
    """Comprehensive diagnosis of your trading environment"""
    try:
        # Import the required modules
        from config import config
        from utils import is_instrument_tradeable, get_instrument_type, standardize_symbol, get_instrument_leverage
        from oanda_service import OandaService
        import oandapyV20
        from oandapyV20.endpoints.instruments import InstrumentsCandles
        from oandapyV20.endpoints.accounts import AccountInstruments
        
        print("🔍 COMPREHENSIVE TRADING DIAGNOSIS")
        print("=" * 60)
        print(f"Environment: {config.oanda_environment}")
        print(f"Account ID: {config.oanda_account_id}")
        print(f"Base URL: {config.oanda_api_url}")
        print()
        
        # Initialize OANDA client
        oanda_service = OandaService()
        await oanda_service.initialize()
        
        # Test symbols
        test_symbols = [
            # Forex pairs
            "EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF", "AUD_USD", "USD_CAD",
            # Crypto pairs
            "BTC_USD", "ETH_USD", "LTC_USD", "XRP_USD", "BCH_USD",
            # Alternative formats
            "BTCUSD", "ETHUSD", "BTC/USD", "ETH/USD"
        ]
        
        print("📊 INSTRUMENT AVAILABILITY CHECK")
        print("-" * 40)
        
        # Check available instruments
        try:
            request = AccountInstruments(accountID=config.oanda_account_id)
            response = await oanda_service.robust_oanda_request(request)
            
            available_instruments = []
            if 'instruments' in response:
                available_instruments = [inst['name'] for inst in response['instruments']]
                print(f"✅ Found {len(available_instruments)} available instruments")
            else:
                print("❌ No instruments found in response")
                return
                
        except Exception as e:
            print(f"❌ Failed to get available instruments: {e}")
            return
        
        print("\n🔍 TESTING SPECIFIC SYMBOLS")
        print("-" * 40)
        
        results = {}
        
        for symbol in test_symbols:
            print(f"\nTesting: {symbol}")
            
            # Standardize symbol
            std_symbol = standardize_symbol(symbol)
            print(f"  Standardized: {std_symbol}")
            
            # Check if available
            is_available = std_symbol in available_instruments
            print(f"  Available: {'✅' if is_available else '❌'}")
            
            # Get leverage
            leverage = get_instrument_leverage(std_symbol)
            print(f"  Leverage: {leverage}:1")
            
            # Check if tradeable
            if is_available:
                try:
                    tradeable, reason = is_instrument_tradeable(std_symbol)
                    print(f"  Tradeable: {'✅' if tradeable else '❌'} ({reason})")
                except Exception as e:
                    print(f"  Tradeable: ❌ (Error: {e})")
                    tradeable = False
            else:
                tradeable = False
                print(f"  Tradeable: ❌ (Not available)")
            
            # Test price fetch
            if is_available:
                try:
                    price = await oanda_service.get_current_price(std_symbol, "BUY")
                    print(f"  Current Price: ${price:.5f}")
                    price_available = True
                except Exception as e:
                    print(f"  Current Price: ❌ (Error: {e})")
                    price_available = False
            else:
                price_available = False
            
            results[symbol] = {
                "standardized": std_symbol,
                "available": is_available,
                "tradeable": tradeable,
                "leverage": leverage,
                "price_available": price_available
            }
        
        print("\n📋 SUMMARY REPORT")
        print("-" * 40)
        
        # Forex summary
        forex_symbols = [s for s in test_symbols if any(fx in s for fx in ['EUR', 'GBP', 'USD', 'JPY', 'CHF', 'AUD', 'CAD'])]
        forex_available = sum(1 for s in forex_symbols if results[s]["available"])
        print(f"Forex Pairs: {forex_available}/{len(forex_symbols)} available")
        
        # Crypto summary
        crypto_symbols = [s for s in test_symbols if any(crypto in s for crypto in ['BTC', 'ETH', 'LTC', 'XRP', 'BCH'])]
        crypto_available = sum(1 for s in crypto_symbols if results[s]["available"])
        print(f"Crypto Pairs: {crypto_available}/{len(crypto_symbols)} available")
        
        print(f"\nTotal Available: {sum(1 for r in results.values() if r['available'])}/{len(results)}")
        
        # Recommendations
        print("\n💡 RECOMMENDATIONS")
        print("-" * 40)
        
        if crypto_available == 0:
            print("❌ No crypto pairs available")
            print("   → Crypto trading may not be enabled in your OANDA environment")
            print("   → Consider switching to live environment for crypto access")
            print("   → Focus on forex pairs for now")
        else:
            print(f"✅ {crypto_available} crypto pairs available")
            print("   → Crypto trading is enabled")
        
        if forex_available < len(forex_symbols):
            print(f"⚠️  Only {forex_available}/{len(forex_symbols)} forex pairs available")
            print("   → Some forex pairs may be restricted")
        
        # Position sizing test
        print("\n💰 POSITION SIZING TEST")
        print("-" * 40)
        
        try:
            # Get comprehensive account info including leverage
            account_info = await oanda_service.get_account_info()
            balance = float(account_info.get('balance', 0))
            actual_leverage = float(account_info.get('leverage', 50.0))
            
            print(f"Account Balance: ${balance:.2f}")
            print(f"Account Leverage: {actual_leverage:.1f}:1")
            
            # Test position sizing for a forex pair
            test_symbol = "EUR_USD"
            if results[test_symbol]["available"]:
                from utils import calculate_position_size
                
                test_price = await oanda_service.get_current_price(test_symbol, "BUY")
                position_size, sizing_info = await calculate_position_size(
                    symbol=test_symbol,
                    entry_price=test_price,
                    risk_percent=10.0,
                    account_balance=balance,
                    leverage=actual_leverage,  # ✅ Use actual account leverage
                    timeframe="H1"
                )
                
                print(f"Test Position Size for {test_symbol}:")
                print(f"  Entry Price: ${test_price:.5f}")
                print(f"  Position Size: {position_size:,} units")
                print(f"  Position Value: ${sizing_info.get('position_value', 0):.2f}")
                print(f"  Required Margin: ${sizing_info.get('required_margin', 0):.2f}")
                
                if position_size < 1000:
                    print("  ⚠️  Position size seems small - check your risk settings")
                else:
                    print("  ✅ Position size looks reasonable")
            
        except Exception as e:
            print(f"❌ Position sizing test failed: {e}")
        
        # Save results
        with open("trading_diagnosis_results.json", "w") as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "environment": config.oanda_environment,
                "account_id": config.oanda_account_id,
                "results": results
            }, f, indent=2)
        
        print(f"\n📄 Results saved to: trading_diagnosis_results.json")
            
    except Exception as e:
        print(f"❌ Diagnosis failed: {e}")
        logger.error(f"Diagnosis error: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(comprehensive_trading_diagnosis()) 