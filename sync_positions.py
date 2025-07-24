#!/usr/bin/env python3
"""
Sync script to import current OANDA positions into the bot's database
"""

import asyncio
import logging
import os
from datetime import datetime, timezone

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def sync_oanda_positions():
    """Sync current OANDA positions with the bot's database"""
    
    logger.info("🔄 SYNCING OANDA POSITIONS...")
    
    try:
        # Import after setting up environment
        from config import settings, get_oanda_config
        from oanda_service import OandaService
        from tracker import PositionTracker
        from database import DatabaseManager
        from utils import get_atr, format_symbol_for_oanda
        
        # Initialize services
        db_manager = DatabaseManager()
        await db_manager.initialize()
        
        oanda_service = OandaService()
        await oanda_service.initialize()
        
        position_tracker = PositionTracker(db_manager, oanda_service)
        await position_tracker.initialize()
        
        # Get current positions from OANDA
        logger.info("📊 Fetching positions from OANDA...")
        
        # Use OANDA API directly to get open trades
        from oandapyV20.endpoints.trades import OpenTrades
        
        try:
            trades_request = OpenTrades(accountID=oanda_service.config.oanda_account_id)
            response = await oanda_service.robust_oanda_request(trades_request)
            oanda_positions = response.get('trades', [])
        except Exception as e:
            logger.error(f"Failed to get trades from OANDA: {e}")
            oanda_positions = []
        
        if not oanda_positions:
            logger.info("No open positions found in OANDA")
            return
        
        logger.info(f"📊 Found {len(oanda_positions)} positions in OANDA")
        
        synced_count = 0
        for trade in oanda_positions:
            try:
                # Extract trade data
                instrument = trade.get('instrument', '')
                units = float(trade.get('currentUnits', 0))
                
                if units == 0:
                    continue
                
                # Determine action and size
                if units > 0:
                    action = "BUY"
                    size = units
                else:
                    action = "SELL"
                    size = abs(units)
                
                entry_price = float(trade.get('price', 0))
                
                # Format symbol for bot
                symbol = format_symbol_for_oanda(instrument)
                
                logger.info(f"\n🔄 Syncing position for {symbol}")
                logger.info(f"   Action: {action}")
                logger.info(f"   Size: {size}")
                logger.info(f"   Entry Price: {entry_price}")
                
                # Get current price
                current_price = await oanda_service.get_current_price(symbol, action)
                if not current_price:
                    logger.warning(f"Could not get current price for {symbol}")
                    continue
                
                # Calculate P&L
                if action == "BUY":
                    pnl = (current_price - entry_price) * size
                else:
                    pnl = (entry_price - current_price) * size
                
                # Get ATR for SL/TP calculation
                atr = await get_atr(symbol, "15")
                if not atr:
                    logger.warning(f"Could not get ATR for {symbol}")
                    atr = 0.001  # Default small ATR
                
                # No initial SL/TP - let positions run naked initially
                # SL/TP will only be set when override function triggers
                stop_loss = None
                take_profit = None
                
                # Create position ID
                timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                position_id = f"{symbol}_{action}_{timestamp}"
                
                # Create metadata
                metadata = {
                    'transaction_id': trade.get('id'),
                    'oanda_trade_id': trade.get('id'),
                    'synced_at': datetime.now(timezone.utc).isoformat(),
                    'original_entry_price': entry_price,
                    'current_price': current_price,
                    'atr': atr
                }
                
                # Check if position already exists
                existing_positions = await position_tracker.get_open_positions()
                position_exists = False
                
                for symbol_positions in existing_positions.values():
                    for existing_id, existing_data in symbol_positions.items():
                        if existing_data.get('metadata', {}).get('oanda_trade_id') == trade.get('id'):
                            position_exists = True
                            logger.info(f"   ✅ Position already exists in database")
                            break
                    if position_exists:
                        break
                
                if not position_exists:
                    # Add position to database
                    await position_tracker.record_position(
                        position_id=position_id,
                        symbol=symbol,
                        action=action,
                        timeframe="H1",  # Default timeframe for synced positions
                        entry_price=entry_price,
                        size=size,
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                        metadata=metadata
                    )
                    
                    # No initial SL/TP on OANDA - positions run naked initially
                    # SL/TP will only be set when override function triggers
                    logger.info(f"   ⚠️ No SL/TP set initially - position runs naked")
                    
                    synced_count += 1
                    logger.info(f"   ✅ Position synced successfully")
                else:
                    logger.info(f"   ⏭️ Position already synced")
                
            except Exception as e:
                logger.error(f"Error syncing trade {trade.get('id', 'unknown')}: {e}")
        
        logger.info(f"\n📈 SYNC SUMMARY:")
        logger.info(f"   Total OANDA positions: {len(oanda_positions)}")
        logger.info(f"   Newly synced: {synced_count}")
        
        # Now check if tiered TP should be set up
        if synced_count > 0:
            logger.info(f"\n🎯 SETTING UP TIERED TP FOR SYNCED POSITIONS...")
            
            # Get all positions again
            all_positions = await position_tracker.get_open_positions()
            
            for symbol, positions in all_positions.items():
                for position_id, position_data in positions.items():
                    metadata = position_data.get('metadata', {})
                    
                    # Only process newly synced positions
                    if metadata.get('synced_at'):
                        logger.info(f"\n🎯 Setting up tiered TP for {symbol}")
                        
                        # Check if position is in profit
                        current_price = position_data.get('current_price', 0)
                        entry_price = position_data.get('entry_price', 0)
                        
                        if current_price > 0 and entry_price > 0:
                            if (position_data['action'] == "BUY" and current_price > entry_price) or \
                               (position_data['action'] == "SELL" and current_price < entry_price):
                                
                                logger.info(f"   📈 Position in profit - setting up tiered TP")
                                
                                # Set up tiered TP levels
                                atr = metadata.get('atr', 0.001)
                                tiered_tp_levels = []
                                
                                # Create tiered TP levels
                                tp_configs = [
                                    {"level": 1, "atr_mult": 1.5, "percentage": 0.25},
                                    {"level": 2, "atr_mult": 2.5, "percentage": 0.35},
                                    {"level": 3, "atr_mult": 4.0, "percentage": 0.40},
                                ]
                                
                                for config in tp_configs:
                                    atr_distance = atr * config["atr_mult"]
                                    
                                    if position_data['action'] == "BUY":
                                        tp_price = entry_price + atr_distance
                                    else:
                                        tp_price = entry_price - atr_distance
                                    
                                    tiered_tp_levels.append({
                                        'level': config["level"],
                                        'atr_multiple': config["atr_mult"],
                                        'percentage': config["percentage"],
                                        'price': tp_price,
                                        'units': int(size * config["percentage"]),
                                        'triggered': False,
                                        'closed_at': None
                                    })
                                
                                # Update metadata with tiered TP
                                metadata['tiered_tp_levels'] = tiered_tp_levels
                                metadata['profit_ride_override_fired'] = True
                                metadata['override_trigger_data'] = {
                                    'trigger_price': current_price,
                                    'trigger_atr': atr,
                                    'trigger_timestamp': datetime.now(timezone.utc).isoformat(),
                                    'trigger_rr_ratio': 2.0,
                                    'trigger_timeframe': '15'
                                }
                                
                                # Update position in database
                                await position_tracker.update_position(
                                    position_id,
                                    metadata=metadata
                                )
                                
                                logger.info(f"   ✅ Tiered TP levels set up")
                                for level in tiered_tp_levels:
                                    logger.info(f"      Level {level['level']}: {level['units']} units at {level['price']:.5f}")
                            else:
                                logger.info(f"   📉 Position not in profit - no tiered TP needed")
        
        logger.info(f"\n✅ Position sync complete!")
        
    except Exception as e:
        logger.error(f"Position sync failed: {e}")
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
        
    asyncio.run(sync_oanda_positions()) 