#!/usr/bin/env python3
"""
Simple test for crypto availability
"""

import asyncio
import os

# Set environment variables for testing
os.environ["OANDA_ACCESS_TOKEN"] = "ad615bb9***2ca9"  # Your actual token
os.environ["OANDA_ACCOUNT_ID"] = "101-003-26651494-012"
os.environ["OANDA_ENVIRONMENT"] = "practice"
os.environ["DATABASE_URL"] = "sqlite:///test.db"

async def test_crypto():
    try:
        print("🔍 Testing crypto availability...")
        
        # Import after setting environment
        from oanda_service import OandaService
        
        oanda = OandaService()
        await oanda.initialize()
        
        print("✅ OANDA service initialized")
        
        # Test crypto availability
        crypto_availability = await oanda.check_crypto_availability()
        print(f"📊 Crypto availability: {crypto_availability}")
        
        # Test specific symbol
        is_btc_supported = await oanda.is_crypto_supported('BTC_USD')
        print(f"🔍 BTC_USD supported: {is_btc_supported}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_crypto()) 