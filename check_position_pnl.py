#!/usr/bin/env python3
"""
Script to check P&L for each open position and determine which ones are profitable
"""
import asyncio
import os

# Set environment variables BEFORE importing modules
os.environ['OANDA_ACCOUNT_ID'] = '101-003-26651494-012'
os.environ['OANDA_ACCESS_TOKEN'] = 'ad615bb907e0eefba1af9fba0ea59472-a46f4bed248535ac8fc8031cbdf92ca9'
os.environ['OANDA_ENVIRONMENT'] = 'practice'

from oanda_service import OandaService
from oandapyV20.endpoints.trades import OpenTrades

async def check_position_pnl():
    print("💰 Checking P&L for open positions...")
    
    try:
        oanda = OandaService()
        await oanda.initialize()
        
        # Get open trades
        trades = await oanda.robust_oanda_request(OpenTrades(accountID='101-003-26651494-012'))
        
        open_trades = trades.get('trades', [])
        print(f"📊 Found {len(open_trades)} open trades")
        print()
        
        profitable_positions = []
        losing_positions = []
        
        for trade in open_trades:
            symbol = trade['instrument']
            units = int(trade['currentUnits'])  # Convert to int
            entry_price = float(trade['price'])
            trade_id = trade['id']
            
            # Determine action based on units
            action = "BUY" if units > 0 else "SELL"
            
            # Get current price
            current_price = await oanda.get_current_price(symbol, action)
            if not current_price:
                print(f"❌ Could not get current price for {symbol}")
                continue
            
            # Calculate P&L
            if action == "BUY":
                pnl = (current_price - entry_price) * units
            else:
                pnl = (entry_price - current_price) * abs(units)
            
            # Calculate percentage gain/loss
            pnl_percentage = (pnl / (entry_price * abs(units))) * 100
            
            position_info = {
                'symbol': symbol,
                'trade_id': trade_id,
                'action': action,
                'units': abs(units),
                'entry_price': entry_price,
                'current_price': current_price,
                'pnl': pnl,
                'pnl_percentage': pnl_percentage
            }
            
            if pnl > 0:
                profitable_positions.append(position_info)
                print(f"📈 {symbol} ({action}): +${pnl:.2f} (+{pnl_percentage:.2f}%)")
                print(f"   Entry: {entry_price:.5f}, Current: {current_price:.5f}")
                print(f"   Units: {abs(units)}, Trade ID: {trade_id}")
            else:
                losing_positions.append(position_info)
                print(f"📉 {symbol} ({action}): ${pnl:.2f} ({pnl_percentage:.2f}%)")
                print(f"   Entry: {entry_price:.5f}, Current: {current_price:.5f}")
                print(f"   Units: {abs(units)}, Trade ID: {trade_id}")
            print()
        
        print("=" * 50)
        print(f"📊 SUMMARY:")
        print(f"   Profitable positions: {len(profitable_positions)}")
        print(f"   Losing positions: {len(losing_positions)}")
        
        if profitable_positions:
            print(f"\n🎯 PROFITABLE POSITIONS (should get SL/TP):")
            for pos in profitable_positions:
                print(f"   {pos['symbol']} ({pos['action']}): +${pos['pnl']:.2f} (+{pos['pnl_percentage']:.2f}%)")
        
        if losing_positions:
            print(f"\n📉 LOSING POSITIONS (no SL/TP needed):")
            for pos in losing_positions:
                print(f"   {pos['symbol']} ({pos['action']}): ${pos['pnl']:.2f} ({pos['pnl_percentage']:.2f}%)")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(check_position_pnl()) 