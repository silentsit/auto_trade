#!/usr/bin/env python3
"""
Test script to debug crypto availability in OANDA
"""

import asyncio
import os
import sys
from oanda_service import OandaService
from config import config
from utils import logger

async def test_crypto_availability():
    """Test the crypto availability check"""
    try:
        print("🔍 Testing OANDA Crypto Availability...")
        print(f"Environment: {config.oanda_environment}")
        print(f"Account ID: {config.oanda_account_id}")
        print(f"Token: {config.oanda_access_token[:10]}...")
        
        # Initialize OANDA service
        oanda_service = OandaService()
        await oanda_service.initialize()
        
        print("\n✅ OANDA service initialized successfully")
        
        # Test crypto availability
        print("\n🔍 Checking crypto availability...")
        crypto_availability = await oanda_service.check_crypto_availability()
        
        print(f"\n📊 Crypto Availability Results:")
        for symbol, available in crypto_availability.items():
            status = "✅ AVAILABLE" if available else "❌ NOT AVAILABLE"
            print(f"  {symbol}: {status}")
        
        # Test specific crypto symbols
        test_symbols = ['BTC_USD', 'ETH_USD', 'LTC_USD', 'XRP_USD', 'BCH_USD']
        print(f"\n🔍 Testing individual crypto support:")
        for symbol in test_symbols:
            is_supported = await oanda_service.is_crypto_supported(symbol)
            status = "✅ SUPPORTED" if is_supported else "❌ NOT SUPPORTED"
            print(f"  {symbol}: {status}")
        
        # Test account instruments directly
        print(f"\n🔍 Testing direct API call to get instruments...")
        try:
            from oandapyV20.endpoints.accounts import AccountInstruments
            
            request = AccountInstruments(accountID=config.oanda_account_id)
            response = await oanda_service.robust_oanda_request(request)
            
            print(f"✅ API call successful")
            print(f"📊 Total instruments available: {len(response.get('instruments', []))}")
            
            # Look for crypto instruments
            crypto_instruments = []
            for inst in response.get('instruments', []):
                if any(crypto in inst['name'] for crypto in ['BTC', 'ETH', 'LTC', 'XRP', 'BCH']):
                    crypto_instruments.append(inst['name'])
            
            print(f"🔍 Found crypto instruments: {crypto_instruments}")
            
            # Show first 10 instruments for debugging
            all_instruments = [inst['name'] for inst in response.get('instruments', [])]
            print(f"📋 First 10 instruments: {all_instruments[:10]}")
            
        except Exception as e:
            print(f"❌ Direct API call failed: {e}")
            logger.error(f"Direct API call failed: {e}", exc_info=True)
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        logger.error(f"Test failed: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(test_crypto_availability()) 