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

# Use existing logging configuration instead of creating a new one
# logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

async def check_oanda_crypto_availability():
    """Check if crypto instruments are available in OANDA environment"""
    try:
        # Import the required modules
        from config import config
        from utils import is_instrument_tradeable, get_instrument_type, standardize_symbol
        from oanda_service import OandaService
        import oandapyV20
        from oandapyV20.endpoints.instruments import InstrumentsCandles
        from oandapyV20.endpoints.accounts import AccountInstruments
        
        print("🔍 OANDA Crypto Trading Diagnosis")
        print("=" * 50)
        print(f"Environment: {config.oanda_environment}")
        print(f"Account ID: {config.oanda_account_id}")
        print()
        
        # Initialize OANDA client
        oanda_service = OandaService()
        
        # Test crypto symbols we want to trade
        crypto_symbols = [
            "BTC_USD", "BTCUSD", "BTC/USD",
            "ETH_USD", "ETHUSD", "ETH/USD",
            "LTC_USD", "XRP_USD", "BCH_USD"
        ]
        
        print("📊 Testing Crypto Symbol Availability")
        print("-" * 40)
        
        results = {}
        
        for symbol in crypto_symbols:
            print(f"\n🔸 Testing: {symbol}")
            
            # Standardize symbol
            std_symbol = standardize_symbol(symbol)
            print(f"   Standardized: {std_symbol}")
            
            # Check instrument type
            instrument_type = get_instrument_type(std_symbol)
            print(f"   Type: {instrument_type}")
            
            # Check if tradeable
            is_tradeable, reason = is_instrument_tradeable(std_symbol)
            print(f"   Tradeable: {is_tradeable} ({reason})")
            
            # Try to get current price
            try:
                price = await oanda_service.get_current_price(std_symbol, "BUY")
                print(f"   ✅ Price Available: {price}")
                results[symbol] = {"available": True, "price": price, "standardized": std_symbol}
            except Exception as e:
                print(f"   ❌ Price Error: {str(e)}")
                results[symbol] = {"available": False, "error": str(e), "standardized": std_symbol}
        
        print("\n" + "=" * 50)
        print("📋 SUMMARY")
        print("=" * 50)
        
        available_cryptos = [k for k, v in results.items() if v.get("available")]
        unavailable_cryptos = [k for k, v in results.items() if not v.get("available")]
        
        print(f"✅ Available Cryptos ({len(available_cryptos)}): {', '.join(available_cryptos) if available_cryptos else 'None'}")
        print(f"❌ Unavailable Cryptos ({len(unavailable_cryptos)}): {', '.join(unavailable_cryptos) if unavailable_cryptos else 'None'}")
        
        if not available_cryptos:
            print("\n🚨 DIAGNOSIS: No cryptocurrency instruments available in your OANDA environment!")
            print("\n💡 SOLUTIONS:")
            print("1. Switch to OANDA live environment (if you have crypto access)")
            print("2. Contact OANDA support to enable crypto trading")
            print("3. Use a different broker that supports crypto in practice mode")
            print("4. Use crypto CFDs instead of spot crypto (if available)")
        
        return results
        
    except Exception as e:
        logger.error(f"Diagnosis failed: {e}")
        return None

async def check_oanda_instrument_list():
    """Get complete list of available instruments from OANDA"""
    try:
        from config import config
        import oandapyV20
        from oandapyV20.endpoints.accounts import AccountInstruments
        
        print("\n🔍 Fetching Available OANDA Instruments")
        print("-" * 40)
        
        # Initialize OANDA client
        access_token = config.oanda_access_token
        if hasattr(access_token, 'get_secret_value'):
            access_token = access_token.get_secret_value()
        
        oanda = oandapyV20.API(
            access_token=access_token,
            environment=config.oanda_environment
        )
        
        # Get account instruments
        instruments_request = AccountInstruments(accountID=config.oanda_account_id)
        response = oanda.request(instruments_request)
        
        if 'instruments' in response:
            instruments = response['instruments']
            print(f"📊 Total Available Instruments: {len(instruments)}")
            
            # Categorize instruments
            forex = []
            crypto = []
            commodities = []
            indices = []
            other = []
            
            for instrument in instruments:
                name = instrument['name']
                inst_type = instrument.get('type', 'UNKNOWN')
                
                if any(crypto_word in name.upper() for crypto_word in ['BTC', 'ETH', 'LTC', 'XRP', 'BCH', 'DOT', 'ADA', 'SOL']):
                    crypto.append(name)
                elif 'CURRENCY' in inst_type or len(name) == 7 and '_' in name:
                    forex.append(name)
                elif any(commodity in name.upper() for commodity in ['XAU', 'XAG', 'OIL', 'GOLD', 'SILVER']):
                    commodities.append(name)
                elif any(index_word in name.upper() for index_word in ['SPX', 'NAS', 'DJ', 'FTSE', 'DAX']):
                    indices.append(name)
                else:
                    other.append(name)
            
            print(f"\n📈 Instrument Categories:")
            print(f"   🪙 Forex: {len(forex)}")
            print(f"   ₿ Crypto: {len(crypto)}")
            print(f"   🥇 Commodities: {len(commodities)}")
            print(f"   📊 Indices: {len(indices)}")
            print(f"   ❓ Other: {len(other)}")
            
            if crypto:
                print(f"\n🎉 CRYPTO INSTRUMENTS FOUND:")
                for c in crypto:
                    print(f"   ₿ {c}")
            else:
                print(f"\n❌ NO CRYPTO INSTRUMENTS FOUND")
                print("   Your OANDA practice environment doesn't support cryptocurrency trading.")
            
            return {
                'total': len(instruments),
                'forex': forex,
                'crypto': crypto,
                'commodities': commodities,
                'indices': indices,
                'other': other
            }
        else:
            print("❌ Failed to fetch instruments")
            return None
            
    except Exception as e:
        logger.error(f"Failed to fetch instrument list: {e}")
        return None

async def test_crypto_signal_processing():
    """Test how crypto signals would be processed"""
    try:
        print("\n🧪 Testing Crypto Signal Processing")
        print("-" * 40)
        
        test_signals = [
            {"symbol": "BTCUSD", "action": "BUY", "message": "Test BTC signal"},
            {"symbol": "BTC_USD", "action": "BUY", "message": "Test BTC signal"},
            {"symbol": "ETHUSD", "action": "BUY", "message": "Test ETH signal"},
            {"symbol": "ETH_USD", "action": "BUY", "message": "Test ETH signal"},
        ]
        
        for signal in test_signals:
            print(f"\n🔸 Testing Signal: {signal['symbol']}")
            
            from utils import standardize_symbol, is_instrument_tradeable, get_instrument_type
            
            std_symbol = standardize_symbol(signal['symbol'])
            instrument_type = get_instrument_type(std_symbol)
            is_tradeable, reason = is_instrument_tradeable(std_symbol)
            
            print(f"   Original: {signal['symbol']}")
            print(f"   Standardized: {std_symbol}")
            print(f"   Type: {instrument_type}")
            print(f"   Tradeable: {is_tradeable} - {reason}")
            
            if not is_tradeable:
                print(f"   ❌ Signal would be REJECTED: {reason}")
            else:
                print(f"   ✅ Signal would be ACCEPTED")
        
    except Exception as e:
        logger.error(f"Signal processing test failed: {e}")

async def main():
    """Run comprehensive crypto diagnosis"""
    print("🚀 Starting OANDA Crypto Trading Diagnosis")
    print("=" * 60)
    
    try:
        # Check crypto availability
        crypto_results = await check_oanda_crypto_availability()
        
        # Get instrument list
        instrument_data = await check_oanda_instrument_list()
        
        # Test signal processing
        await test_crypto_signal_processing()
        
        print("\n" + "=" * 60)
        print("🎯 FINAL RECOMMENDATIONS")
        print("=" * 60)
        
        if instrument_data and instrument_data.get('crypto'):
            print("✅ Your OANDA environment supports crypto trading!")
            print("   Issue might be in signal processing or instrument mapping.")
        else:
            print("❌ OANDA Practice Environment does NOT support crypto trading.")
            print("\n💡 IMMEDIATE SOLUTIONS:")
            print("1. 🔄 Switch to live OANDA environment (with proper crypto access)")
            print("2. 📞 Contact OANDA support about crypto access")
            print("3. 🏦 Consider using a crypto-friendly broker for crypto signals")
            print("4. 🔧 Modify your Pine Script to send signals only for supported instruments")
            
        print(f"\n📊 Analysis saved to: crypto_diagnosis_results.json")
        
        # Save results
        results = {
            "timestamp": datetime.now().isoformat(),
            "environment": "practice",  # Based on your config
            "crypto_availability": crypto_results,
            "instrument_data": instrument_data,
            "conclusion": "No crypto support" if not (instrument_data and instrument_data.get('crypto')) else "Crypto supported"
        }
        
        with open("crypto_diagnosis_results.json", "w") as f:
            json.dump(results, f, indent=2, default=str)
            
    except Exception as e:
        logger.error(f"Diagnosis failed: {e}")
        print(f"\n❌ Diagnosis Error: {e}")

if __name__ == "__main__":
    asyncio.run(main()) 