#!/usr/bin/env python3
"""
Script to set SL/TP for existing positions that should trigger the override function
"""
import asyncio
import logging
import os
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def set_sl_tp_for_existing():
    logger.info("🎯 SETTING SL/TP FOR EXISTING POSITIONS...")
    try:
        from config import settings, get_oanda_config
        from oanda_service import OandaService
        from tracker import PositionTracker
        from database import DatabaseManager
        from utils import get_atr
        from profit_ride_override import ProfitRideOverride
        from regime_classifier import LorentzianDistanceClassifier
        from volatility_monitor import VolatilityMonitor
        
        db_manager = DatabaseManager()
        await db_manager.initialize()
        oanda_service = OandaService()
        await oanda_service.initialize()
        position_tracker = PositionTracker(db_manager, oanda_service)
        await position_tracker.initialize()
        
        # Initialize override components
        regime = LorentzianDistanceClassifier()
        vol = VolatilityMonitor()
        override_manager = ProfitRideOverride(regime, vol)
        
        open_positions = await position_tracker.get_open_positions()
        
        if not open_positions:
            logger.info("No open positions found")
            return
        
        logger.info(f"Found {len(open_positions)} symbols with positions")
        
        for symbol, positions in open_positions.items():
            for position_id, position_data in positions.items():
                try:
                    logger.info(f"\n🎯 Checking position {position_id} ({symbol})")
                    
                    # Skip if already has SL/TP
                    if position_data.get('stop_loss') or position_data.get('take_profit'):
                        logger.info(f"   ⏭️ Position already has SL/TP - skipping")
                        continue
                    
                    # Skip if override already fired
                    metadata = position_data.get('metadata', {})
                    if metadata.get('profit_ride_override_fired'):
                        logger.info(f"   ⏭️ Override already fired - skipping")
                        continue
                    
                    # Get current price
                    current_price = await oanda_service.get_current_price(symbol, position_data['action'])
                    if not current_price:
                        logger.warning(f"   ⚠️ Could not get current price")
                        continue
                    
                    # Calculate P&L
                    entry_price = position_data['entry_price']
                    size = position_data['size']
                    action = position_data['action']
                    
                    if action == "BUY":
                        pnl = (current_price - entry_price) * size
                    else:
                        pnl = (entry_price - current_price) * size
                    
                    logger.info(f"   Entry: {entry_price}, Current: {current_price}, P&L: {pnl:.2f}")
                    
                    # Check if position is in profit (should trigger override)
                    if pnl <= 0:
                        logger.info(f"   📉 Position not in profit - no SL/TP needed")
                        continue
                    
                    # Get ATR
                    timeframe = position_data.get('timeframe', 'H1')
                    atr = await get_atr(symbol, timeframe)
                    if not atr:
                        logger.warning(f"   ⚠️ Could not get ATR")
                        continue
                    
                    logger.info(f"   📈 Position in profit - calculating SL/TP with ATR: {atr}")
                    
                    # Determine ATR multiplier based on timeframe
                    tf = timeframe.upper()
                    if tf in ["15M", "15MIN", "15"]:
                        sl_mult = 1.5  # 15M uses 1.5x ATR
                        logger.info(f"   🕐 15M timeframe - using 1.5x ATR for SL")
                    else:
                        sl_mult = 2.0  # 1H/4H uses 2.0x ATR
                        logger.info(f"   🕐 {timeframe} timeframe - using 2.0x ATR for SL")
                    
                    # Calculate SL/TP
                    if action == "BUY":
                        stop_loss = current_price - (atr * sl_mult)
                        take_profit = current_price + (atr * 10.0)  # Far TP as backup
                    else:
                        stop_loss = current_price + (atr * sl_mult)
                        take_profit = current_price - (atr * 10.0)  # Far TP as backup
                    
                    logger.info(f"   🎯 SL: {stop_loss:.5f}, TP: {take_profit:.5f}")
                    
                    # Update position in database
                    await position_tracker.update_position(
                        position_id, 
                        stop_loss=stop_loss, 
                        take_profit=take_profit,
                        metadata={
                            **metadata,
                            'profit_ride_override_fired': True,
                            'override_trigger_data': {
                                'trigger_price': current_price,
                                'trigger_atr': atr,
                                'trigger_timestamp': datetime.now(timezone.utc).isoformat(),
                                'trigger_rr_ratio': 1.5 if tf in ["15M", "15MIN", "15"] else 2.0,
                                'trigger_timeframe': timeframe
                            }
                        }
                    )
                    
                    # Update OANDA if we have trade ID
                    trade_id = metadata.get('transaction_id') or metadata.get('oanda_trade_id')
                    if trade_id:
                        try:
                            await oanda_service.modify_position(trade_id, stop_loss=stop_loss, take_profit=take_profit)
                            logger.info(f"   ✅ Updated SL/TP on OANDA")
                        except Exception as e:
                            logger.warning(f"   ⚠️ Failed to update OANDA SL/TP: {e}")
                    else:
                        logger.warning(f"   ⚠️ No trade ID found - cannot update OANDA")
                    
                    logger.info(f"   ✅ SL/TP set successfully")
                    
                except Exception as e:
                    logger.error(f"Error processing position {position_id}: {e}")
        
        logger.info(f"\n✅ SL/TP setup complete!")
        
    except Exception as e:
        logger.error(f"SL/TP setup failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if not os.getenv('OANDA_ACCESS_TOKEN'):
        print("Please set OANDA_ACCESS_TOKEN environment variable")
        exit(1)
    if not os.getenv('OANDA_ACCOUNT_ID'):
        print("Please set OANDA_ACCOUNT_ID environment variable")
        exit(1)
    asyncio.run(set_sl_tp_for_existing()) 