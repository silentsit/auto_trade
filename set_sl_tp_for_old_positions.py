#!/usr/bin/env python3
"""
Script to set SL/TP for positions that have been open for more than 4 candles
"""
import asyncio
import os
from datetime import datetime, timezone, timedelta

# Set environment variables BEFORE importing modules
os.environ['OANDA_ACCOUNT_ID'] = '101-003-26651494-012'
os.environ['OANDA_ACCESS_TOKEN'] = 'ad615bb907e0eefba1af9fba0ea59472-a46f4bed248535ac8fc8031cbdf92ca9'
os.environ['OANDA_ENVIRONMENT'] = 'practice'

from oanda_service import OandaService
from oandapyV20.endpoints.trades import OpenTrades
from utils import get_atr

async def set_sl_tp_for_old_positions():
    print("⏰ Setting SL/TP for positions older than 4 candles...")
    
    try:
        oanda = OandaService()
        await oanda.initialize()
        
        # Get open trades
        trades = await oanda.robust_oanda_request(OpenTrades(accountID='101-003-26651494-012'))
        open_trades = trades.get('trades', [])
        
        print(f"📊 Found {len(open_trades)} open trades")
        print()
        
        positions_to_update = []
        
        for trade in open_trades:
            symbol = trade['instrument']
            units = int(trade['currentUnits'])
            entry_price = float(trade['price'])
            trade_id = trade['id']
            open_time_str = trade.get('openTime', '')
            
            # Parse open time
            try:
                # Remove 'Z' and parse ISO format
                open_time_str = open_time_str.replace('Z', '+00:00')
                open_time = datetime.fromisoformat(open_time_str)
            except Exception as e:
                print(f"❌ Could not parse open time for {symbol}: {e}")
                continue
            
            # Calculate duration
            now = datetime.now(timezone.utc)
            duration = now - open_time
            
            print(f"📈 {symbol} ({'BUY' if units > 0 else 'SELL'}):")
            print(f"   Trade ID: {trade_id}")
            print(f"   Open time: {open_time}")
            print(f"   Duration: {duration}")
            
            # Check if position is older than 4 candles (assuming 15M timeframe for now)
            # 4 candles = 1 hour
            four_candles_duration = timedelta(hours=1)
            
            if duration > four_candles_duration:
                print(f"   ✅ Position is older than 4 candles - will set SL/TP")
                
                # Get current price and calculate P&L
                action = "BUY" if units > 0 else "SELL"
                current_price = await oanda.get_current_price(symbol, action)
                
                if not current_price:
                    print(f"   ❌ Could not get current price")
                    continue
                
                # Calculate P&L
                if action == "BUY":
                    pnl = (current_price - entry_price) * units
                else:
                    pnl = (entry_price - current_price) * abs(units)
                
                print(f"   Current price: {current_price}")
                print(f"   P&L: ${pnl:.2f}")
                
                # Only set SL/TP if position is in profit
                if pnl > 0:
                    print(f"   📈 Position is profitable - setting SL/TP")
                    
                    # Get ATR (assuming 15M timeframe for old positions)
                    atr = await get_atr(symbol, "15M")
                    if not atr:
                        print(f"   ⚠️ Could not get ATR, using default")
                        atr = 0.001  # Default ATR
                    
                    print(f"   ATR: {atr}")
                    
                    # Use 1.5x ATR for 15M timeframe
                    sl_mult = 1.5
                    
                    # Calculate SL/TP
                    if action == "BUY":
                        stop_loss = current_price - (atr * sl_mult)
                        take_profit = current_price + (atr * 10.0)  # Far TP as backup
                    else:
                        stop_loss = current_price + (atr * sl_mult)
                        take_profit = current_price - (atr * 10.0)  # Far TP as backup
                    
                    print(f"   🎯 SL: {stop_loss:.5f}, TP: {take_profit:.5f}")
                    
                    positions_to_update.append({
                        'trade_id': trade_id,
                        'symbol': symbol,
                        'action': action,
                        'stop_loss': stop_loss,
                        'take_profit': take_profit,
                        'pnl': pnl,
                        'duration': duration
                    })
                else:
                    print(f"   📉 Position is not profitable - no SL/TP needed")
            else:
                print(f"   ⏭️ Position is newer than 4 candles - skipping")
            
            print()
        
        # Update positions on OANDA
        if positions_to_update:
            print("=" * 50)
            print(f"🎯 UPDATING {len(positions_to_update)} POSITIONS ON OANDA:")
            
            for pos in positions_to_update:
                try:
                    await oanda.modify_position(
                        pos['trade_id'], 
                        stop_loss=pos['stop_loss'], 
                        take_profit=pos['take_profit']
                    )
                    print(f"   ✅ {pos['symbol']} ({pos['action']}): SL={pos['stop_loss']:.5f}, TP={pos['take_profit']:.5f}")
                except Exception as e:
                    print(f"   ❌ {pos['symbol']}: Failed to update - {e}")
            
            print(f"\n✅ Successfully updated {len(positions_to_update)} positions")
        else:
            print("📝 No positions meet the criteria for SL/TP update")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(set_sl_tp_for_old_positions()) 