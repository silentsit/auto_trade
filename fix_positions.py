#!/usr/bin/env python3
"""
Fix script to set SL/TP for current positions and check override status
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fix_current_positions():
    """Fix current positions by setting proper SL/TP"""
    
    logger.info("🔧 FIXING CURRENT POSITIONS...")
    
    try:
        # Import after setting up environment
        from config import settings, get_oanda_config
        from oanda_service import OandaService
        from tracker import PositionTracker
        from database import DatabaseManager
        from utils import get_atr
        
        # Initialize services
        db_manager = DatabaseManager()
        await db_manager.initialize()
        
        oanda_service = OandaService()
        await oanda_service.initialize()
        
        position_tracker = PositionTracker(db_manager, oanda_service)
        await position_tracker.initialize()
        
        # Get all open positions
        open_positions = await position_tracker.get_open_positions()
        
        logger.info(f"📊 Found {len(open_positions)} symbols with open positions")
        
        for symbol, positions in open_positions.items():
            for position_id, position_data in positions.items():
                logger.info(f"\n🔧 Fixing position {position_id} for {symbol}")
                
                # Get current price
                current_price = await oanda_service.get_current_price(symbol, position_data['action'])
                if not current_price:
                    logger.warning(f"Could not get current price for {symbol}")
                    continue
                
                # Get ATR
                atr = await get_atr(symbol, "15")  # Use 15M ATR
                if not atr:
                    logger.warning(f"Could not get ATR for {symbol}")
                    continue
                
                # Calculate new SL/TP based on current price and ATR
                action = position_data['action']
                if action == "BUY":
                    new_sl = current_price - (atr * 2.0)  # 2 ATR stop loss
                    new_tp = current_price + (atr * 4.0)  # 4 ATR take profit
                else:  # SELL
                    new_sl = current_price + (atr * 2.0)  # 2 ATR stop loss
                    new_tp = current_price - (atr * 4.0)  # 4 ATR take profit
                
                logger.info(f"   Current Price: {current_price}")
                logger.info(f"   ATR: {atr}")
                logger.info(f"   New SL: {new_sl}")
                logger.info(f"   New TP: {new_tp}")
                
                # Check if position has transaction_id
                metadata = position_data.get('metadata', {})
                trade_id = metadata.get('transaction_id')
                
                if trade_id:
                    # Update on OANDA
                    try:
                        await oanda_service.modify_position(trade_id, stop_loss=new_sl, take_profit=new_tp)
                        logger.info(f"   ✅ Updated SL/TP on OANDA for trade {trade_id}")
                    except Exception as e:
                        logger.error(f"   ❌ Failed to update OANDA: {e}")
                else:
                    logger.warning(f"   ⚠️ No transaction_id found - cannot update OANDA")
                
                # Update in tracker
                try:
                    await position_tracker.update_position(
                        position_id, 
                        stop_loss=new_sl, 
                        take_profit=new_tp, 
                        metadata=metadata
                    )
                    logger.info(f"   ✅ Updated position in tracker")
                except Exception as e:
                    logger.error(f"   ❌ Failed to update tracker: {e}")
                
                # Check if override should be triggered
                entry_price = position_data.get('entry_price', 0)
                if entry_price > 0:
                    pnl_pips = abs(current_price - entry_price) * 10000  # Convert to pips
                    logger.info(f"   📊 Current P&L: {pnl_pips:.1f} pips")
                    
                    # If position is in profit and no override has been fired
                    if (action == "BUY" and current_price > entry_price) or (action == "SELL" and current_price < entry_price):
                        if not metadata.get('profit_ride_override_fired', False):
                            logger.info(f"   🎯 Position in profit - override should be considered")
                        else:
                            logger.info(f"   ✅ Override already fired")
        
        logger.info(f"\n✅ Position fixing complete!")
        
    except Exception as e:
        logger.error(f"Position fixing failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Set environment variables if not already set
    if not os.getenv('OANDA_ACCESS_TOKEN'):
        print("Please set OANDA_ACCESS_TOKEN environment variable")
        exit(1)
    if not os.getenv('OANDA_ACCOUNT_ID'):
        print("Please set OANDA_ACCOUNT_ID environment variable")
        exit(1)
        
    asyncio.run(fix_current_positions()) 