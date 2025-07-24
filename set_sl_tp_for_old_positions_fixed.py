#!/usr/bin/env python3
"""
Script to set SL/TP for positions that have been open for more than 4 candles
FIXED VERSION: Handles price precision and ATR calculation properly
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

def get_price_precision(symbol):
    """Get the correct number of decimal places for each instrument"""
    precision_map = {
        'EUR_USD': 5,
        'GBP_USD': 5,
        'USD_JPY': 3,
        'USD_CHF': 5,
        'AUD_USD': 5,
        'USD_CAD': 5,
        'EUR_JPY': 3,
        'GBP_JPY': 3,
        'EUR_GBP': 5,
        'AUD_JPY': 3,
        'EUR_AUD': 5,
        'GBP_CHF': 5,
        'EUR_CHF': 5,
        'AUD_CAD': 5,
        'CAD_JPY': 3,
        'NZD_USD': 5,
        'NZD_JPY': 3,
        'GBP_AUD': 5,
        'EUR_CAD': 5,
        'AUD_CHF': 5,
        'GBP_CAD': 5,
        'USD_SEK': 5,
        'EUR_SEK': 5,
        'USD_NOK': 5,
        'EUR_NOK': 5,
        'USD_DKK': 5,
        'EUR_DKK': 5,
        'USD_PLN': 5,
        'EUR_PLN': 5,
        'USD_CZK': 5,
        'EUR_CZK': 5,
        'USD_HUF': 5,
        'EUR_HUF': 5,
        'USD_ZAR': 5,
        'EUR_ZAR': 5,
        'USD_MXN': 5,
        'EUR_MXN': 5,
        'USD_SGD': 5,
        'EUR_SGD': 5,
        'USD_HKD': 5,
        'EUR_HKD': 5,
        'USD_TRY': 5,
        'EUR_TRY': 5,
        'USD_BRL': 5,
        'EUR_BRL': 5,
        'USD_RUB': 5,
        'EUR_RUB': 5,
        'USD_INR': 5,
        'EUR_INR': 5,
        'USD_CNY': 5,
        'EUR_CNY': 5,
        'USD_KRW': 5,
        'EUR_KRW': 5,
        'USD_THB': 5,
        'EUR_THB': 5,
        'USD_MYR': 5,
        'EUR_MYR': 5,
        'USD_IDR': 5,
        'EUR_IDR': 5,
        'USD_PHP': 5,
        'EUR_PHP': 5,
        'USD_VND': 5,
        'EUR_VND': 5,
        'USD_EGP': 5,
        'EUR_EGP': 5,
        'USD_NGN': 5,
        'EUR_NGN': 5,
        'USD_KES': 5,
        'EUR_KES': 5,
        'USD_GHS': 5,
        'EUR_GHS': 5,
        'USD_UGX': 5,
        'EUR_UGX': 5,
        'USD_TZS': 5,
        'EUR_TZS': 5,
        'USD_ZMW': 5,
        'EUR_ZMW': 5,
        'USD_BWP': 5,
        'EUR_BWP': 5,
        'USD_NAD': 5,
        'EUR_NAD': 5,
        'USD_MUR': 5,
        'EUR_MUR': 5,
        'USD_MAD': 5,
        'EUR_MAD': 5,
        'USD_TND': 5,
        'EUR_TND': 5,
        'USD_BDT': 5,
        'EUR_BDT': 5,
        'USD_LKR': 5,
        'EUR_LKR': 5,
        'USD_NPR': 5,
        'EUR_NPR': 5,
        'USD_PKR': 5,
        'EUR_PKR': 5,
        'USD_AFN': 5,
        'EUR_AFN': 5,
        'USD_IRR': 5,
        'EUR_IRR': 5,
        'USD_IQD': 5,
        'EUR_IQD': 5,
        'USD_SYP': 5,
        'EUR_SYP': 5,
        'USD_YER': 5,
        'EUR_YER': 5,
        'USD_OMR': 5,
        'EUR_OMR': 5,
        'USD_QAR': 5,
        'EUR_QAR': 5,
        'USD_AED': 5,
        'EUR_AED': 5,
        'USD_KWD': 5,
        'EUR_KWD': 5,
        'USD_BHD': 5,
        'EUR_BHD': 5,
        'USD_JOD': 5,
        'EUR_JOD': 5,
        'USD_LBP': 5,
        'EUR_LBP': 5,
        'USD_ILS': 5,
        'EUR_ILS': 5,
        'USD_PEN': 5,
        'EUR_PEN': 5,
        'USD_CLP': 5,
        'EUR_CLP': 5,
        'USD_COP': 5,
        'EUR_COP': 5,
        'USD_ARS': 5,
        'EUR_ARS': 5,
        'USD_UYU': 5,
        'EUR_UYU': 5,
        'USD_PYG': 5,
        'EUR_PYG': 5,
        'USD_BOB': 5,
        'EUR_BOB': 5,
        'USD_CRC': 5,
        'EUR_CRC': 5,
        'USD_GTQ': 5,
        'EUR_GTQ': 5,
        'USD_HNL': 5,
        'EUR_HNL': 5,
        'USD_NIO': 5,
        'EUR_NIO': 5,
        'USD_PAB': 5,
        'EUR_PAB': 5,
        'USD_SVC': 5,
        'EUR_SVC': 5,
        'USD_TTD': 5,
        'EUR_TTD': 5,
        'USD_BBD': 5,
        'EUR_BBD': 5,
        'USD_JMD': 5,
        'EUR_JMD': 5,
        'USD_XCD': 5,
        'EUR_XCD': 5,
        'USD_HTG': 5,
        'EUR_HTG': 5,
        'USD_DOP': 5,
        'EUR_DOP': 5,
        'USD_ANG': 5,
        'EUR_ANG': 5,
        'USD_AWG': 5,
        'EUR_AWG': 5,
        'USD_KYD': 5,
        'EUR_KYD': 5,
        'USD_BMD': 5,
        'EUR_BMD': 5,
        'USD_BZD': 5,
        'EUR_BZD': 5,
        'USD_FJD': 5,
        'EUR_FJD': 5,
        'USD_WST': 5,
        'EUR_WST': 5,
        'USD_TOP': 5,
        'EUR_TOP': 5,
        'USD_SBD': 5,
        'EUR_SBD': 5,
        'USD_VUV': 5,
        'EUR_VUV': 5,
        'USD_NZD': 5,
        'EUR_NZD': 5,
        'USD_PGK': 5,
        'EUR_PGK': 5,
        'USD_KPW': 5,
        'EUR_KPW': 5,
        'USD_LAK': 5,
        'EUR_LAK': 5,
        'USD_KHR': 5,
        'EUR_KHR': 5,
        'USD_MMK': 5,
        'EUR_MMK': 5,
        'USD_CDF': 5,
        'EUR_CDF': 5,
        'USD_BIF': 5,
        'EUR_BIF': 5,
        'USD_DJF': 5,
        'EUR_DJF': 5,
        'USD_ETB': 5,
        'EUR_ETB': 5,
        'USD_ERN': 5,
        'EUR_ERN': 5,
        'USD_GMD': 5,
        'EUR_GMD': 5,
        'USD_GNF': 5,
        'EUR_GNF': 5,
        'USD_LRD': 5,
        'EUR_LRD': 5,
        'USD_LSL': 5,
        'EUR_LSL': 5,
        'USD_MWK': 5,
        'EUR_MWK': 5,
        'USD_MZN': 5,
        'EUR_MZN': 5,
        'USD_RWF': 5,
        'EUR_RWF': 5,
        'USD_SLL': 5,
        'EUR_SLL': 5,
        'USD_SOS': 5,
        'EUR_SOS': 5,
        'USD_SZL': 5,
        'EUR_SZL': 5,
        'USD_XAF': 5,
        'EUR_XAF': 5,
        'USD_XOF': 5,
        'EUR_XOF': 5,
        'USD_XPF': 5,
        'EUR_XPF': 5,
        'USD_YER': 5,
        'EUR_YER': 5,
        'USD_ZWL': 5,
        'EUR_ZWL': 5,
        'BTC_USD': 2,
        'ETH_USD': 2,
        'LTC_USD': 2,
        'BCH_USD': 2,
        'XRP_USD': 5,
        'ADA_USD': 5,
        'DOT_USD': 3,
        'LINK_USD': 3,
        'UNI_USD': 3,
        'AAVE_USD': 2,
        'COMP_USD': 2,
        'MKR_USD': 2,
        'SNX_USD': 3,
        'YFI_USD': 2,
        'SUSHI_USD': 3,
        'CRV_USD': 4,
        'BAL_USD': 3,
        'REN_USD': 4,
        'ZRX_USD': 4,
        'UMA_USD': 3,
        'BAND_USD': 3,
        'NMR_USD': 2,
        'STORJ_USD': 4,
        'MANA_USD': 4,
        'SAND_USD': 4,
        'AXS_USD': 3,
        'FLOW_USD': 3,
        'AUDIO_USD': 4,
        'CHZ_USD': 5,
        'ENJ_USD': 4,
        'ALGO_USD': 4,
        'ATOM_USD': 3,
        'NEAR_USD': 4,
        'FTM_USD': 4,
        'AVAX_USD': 2,
        'SOL_USD': 2,
        'MATIC_USD': 4,
        'SHIB_USD': 8,
        'DOGE_USD': 6,
        'TRX_USD': 5,
        'EOS_USD': 3,
        'XLM_USD': 5,
        'VET_USD': 5,
        'FIL_USD': 3,
        'ICP_USD': 2,
        'APT_USD': 3,
        'SUI_USD': 4,
        'OP_USD': 3,
        'ARB_USD': 3,
        'INJ_USD': 2,
        'TIA_USD': 3,
        'SEI_USD': 4,
        'JUP_USD': 4,
        'PYTH_USD': 4,
        'WIF_USD': 5,
        'BONK_USD': 8,
        'PEPE_USD': 8,
        'FLOKI_USD': 6,
        'BOME_USD': 8,
        'WLD_USD': 3,
        'ORDI_USD': 2,
        'TIA_USD': 3,
        'JUP_USD': 4,
        'PYTH_USD': 4,
        'WIF_USD': 5,
        'BONK_USD': 8,
        'PEPE_USD': 8,
        'FLOKI_USD': 6,
        'BOME_USD': 8,
        'WLD_USD': 3,
        'ORDI_USD': 2,
    }
    return precision_map.get(symbol, 5)  # Default to 5 decimal places

def round_price(price, symbol):
    """Round price to the correct number of decimal places for the instrument"""
    precision = get_price_precision(symbol)
    return round(price, precision)

async def set_sl_tp_for_old_positions_fixed():
    print("⏰ Setting SL/TP for positions older than 4 candles (FIXED VERSION)...")
    
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
                    
                    # Get ATR with proper OANDA service
                    atr = await get_atr(symbol, "15M", oanda_service=oanda)
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
                    
                    # Round prices to correct precision
                    stop_loss = round_price(stop_loss, symbol)
                    take_profit = round_price(take_profit, symbol)
                    
                    print(f"   🎯 SL: {stop_loss} (rounded), TP: {take_profit} (rounded)")
                    
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
                    print(f"   ✅ {pos['symbol']} ({pos['action']}): SL={pos['stop_loss']}, TP={pos['take_profit']}")
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
    asyncio.run(set_sl_tp_for_old_positions_fixed()) 