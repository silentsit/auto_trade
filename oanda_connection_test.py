#!/usr/bin/env python3
"""
OANDA Connection Diagnostic Tool
Tests OANDA practice environment connectivity and identifies issues
"""

import asyncio
import logging
import oandapyV20
import oandapyV20.endpoints.accounts as accounts
import oandapyV20.endpoints.pricing as pricing
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# OANDA credentials from your env
ACCESS_TOKEN = "ad615bb907e0eefba1af9fba0ea59472-a46f4bed248535ac8fc8031cbdf92ca9"
ACCOUNT_ID = "101-003-26651494-012"
ENVIRONMENT = "practice"

async def test_oanda_connection():
    """Comprehensive OANDA connection test"""
    logger.info("🔧 Starting OANDA Connection Diagnostic...")
    
    try:
        # 1. Initialize client
        logger.info("1️⃣ Initializing OANDA client...")
        client = oandapyV20.API(
            access_token=ACCESS_TOKEN,
            environment=ENVIRONMENT
        )
        logger.info("✅ Client initialized successfully")
        
        # 2. Test account access
        logger.info("2️⃣ Testing account access...")
        account_request = accounts.AccountDetails(accountID=ACCOUNT_ID)
        response = client.request(account_request)
        logger.info(f"✅ Account access successful: {response['account']['currency']} account")
        
        # 3. Test pricing for problematic symbols
        logger.info("3️⃣ Testing pricing for failing symbols...")
        test_symbols = ["EUR_JPY", "EUR_GBP", "USD_CAD", "AUD_USD", "USD_CHF"]
        
        for symbol in test_symbols:
            try:
                pricing_request = pricing.PricingInfo(
                    accountID=ACCOUNT_ID,
                    params={"instruments": symbol}
                )
                response = client.request(pricing_request)
                price_data = response['prices'][0]
                logger.info(f"✅ {symbol}: Bid={price_data['bids'][0]['price']}, Ask={price_data['asks'][0]['price']}")
                
            except Exception as e:
                logger.error(f"❌ {symbol}: Failed - {str(e)}")
        
        # 4. Test major pairs
        logger.info("4️⃣ Testing major currency pairs...")
        major_pairs = ["EUR_USD", "GBP_USD", "USD_JPY"]
        
        for symbol in major_pairs:
            try:
                pricing_request = pricing.PricingInfo(
                    accountID=ACCOUNT_ID,
                    params={"instruments": symbol}
                )
                response = client.request(pricing_request)
                price_data = response['prices'][0]
                logger.info(f"✅ {symbol}: Bid={price_data['bids'][0]['price']}, Ask={price_data['asks'][0]['price']}")
                
            except Exception as e:
                logger.error(f"❌ {symbol}: Failed - {str(e)}")
        
        logger.info("🎯 Connection diagnostic complete!")
        
    except Exception as e:
        logger.error(f"❌ CRITICAL: Connection diagnostic failed: {e}")
        logger.error("Possible issues:")
        logger.error("- Token expired or invalid")
        logger.error("- Account ID incorrect")
        logger.error("- Network connectivity issues")
        logger.error("- OANDA API service disruption")

if __name__ == "__main__":
    asyncio.run(test_oanda_connection())
