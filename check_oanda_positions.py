#!/usr/bin/env python3
"""
Simple script to check for open positions on OANDA
"""
import asyncio
import os
from oanda_service import OandaService
from oandapyV20.endpoints.trades import OpenTrades

async def check_positions():
    print("🔍 Checking OANDA positions...")
    
    # Set environment variables
    os.environ['OANDA_ACCOUNT_ID'] = '101-003-26651494-012'
    os.environ['OANDA_ACCESS_TOKEN'] = 'ad615bb907e0eefba1af9fba0ea59472-a46f4bed248535ac8fc8031cbdf92ca9'
    os.environ['OANDA_ENVIRONMENT'] = 'practice'
    
    try:
        oanda = OandaService()
        await oanda.initialize()
        
        # Get open trades
        trades = await oanda.robust_oanda_request(OpenTrades(accountID='101-003-26651494-012'))
        
        open_trades = trades.get('trades', [])
        print(f"📊 Found {len(open_trades)} open trades")
        
        if open_trades:
            for trade in open_trades:
                print(f"  📈 {trade['instrument']}: {trade['currentUnits']} units at {trade['price']}")
                print(f"     Trade ID: {trade['id']}")
                print(f"     State: {trade['state']}")
                if 'stopLossOrder' in trade:
                    print(f"     SL: {trade['stopLossOrder']['price']}")
                if 'takeProfitOrder' in trade:
                    print(f"     TP: {trade['takeProfitOrder']['price']}")
                print()
        else:
            print("  No open positions found")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(check_positions()) 