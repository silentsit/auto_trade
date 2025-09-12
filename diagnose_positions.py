#!/usr/bin/env python3
"""
Diagnostic script to check position states and tiered TP setup
"""

import asyncio
import logging
from config import settings, get_oanda_config
from oanda_service import OandaService
from tracker import PositionTracker
from database import DatabaseManager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def diagnose_positions():
    """Diagnose current positions and their tiered TP status"""
    
    logger.info("🔍 DIAGNOSING POSITIONS...")
    
    try:
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
        
        total_positions = 0
        positions_with_tiered_tp = 0
        positions_with_override = 0
        
        for symbol, positions in open_positions.items():
            logger.info(f"\n🔍 Symbol: {symbol}")
            logger.info(f"   Positions: {len(positions)}")
            
            for position_id, position_data in positions.items():
                total_positions += 1
                
                logger.info(f"\n   📋 Position ID: {position_id}")
                logger.info(f"      Action: {position_data.get('action')}")
                logger.info(f"      Size: {position_data.get('size')}")
                logger.info(f"      Entry Price: {position_data.get('entry_price')}")
                logger.info(f"      Current P&L: {position_data.get('pnl', 'N/A')}")
                
                # Check metadata
                metadata = position_data.get('metadata', {})
                
                # Check for override
                override_fired = metadata.get('profit_ride_override_fired', False)
                if override_fired:
                    positions_with_override += 1
                    logger.info(f"      ✅ Override FIRED")
                    
                    # Check trigger data
                    trigger_data = metadata.get('override_trigger_data', {})
                    if trigger_data:
                        logger.info(f"      📅 Triggered: {trigger_data.get('trigger_timestamp')}")
                        logger.info(f"      📊 Trigger ATR: {trigger_data.get('trigger_atr')}")
                        logger.info(f"      📈 Trigger Price: {trigger_data.get('trigger_price')}")
                else:
                    logger.info(f"      ❌ Override NOT fired")
                
                # Check for tiered TP levels
                tiered_tp_levels = metadata.get('tiered_tp_levels', [])
                if tiered_tp_levels:
                    positions_with_tiered_tp += 1
                    logger.info(f"      🎯 Tiered TP Levels: {len(tiered_tp_levels)}")
                    for i, level in enumerate(tiered_tp_levels):
                        logger.info(f"         Level {level.get('level')}: {level.get('percentage', 0)*100:.0f}% at {level.get('price', 'N/A')}")
                        logger.info(f"            Triggered: {level.get('triggered', False)}")
                        if level.get('closed_at'):
                            logger.info(f"            Closed: {level.get('closed_at')}")
                else:
                    logger.info(f"      ❌ No tiered TP levels")
                
                # Check SL/TP
                sl = position_data.get('stop_loss')
                tp = position_data.get('take_profit')
                logger.info(f"      🛑 Stop Loss: {sl}")
                logger.info(f"      🎯 Take Profit: {tp}")
                
                # Check OANDA trade ID
                trade_id = metadata.get('transaction_id')
                logger.info(f"      🆔 OANDA Trade ID: {trade_id}")
        
        logger.info(f"\n📈 SUMMARY:")
        logger.info(f"   Total Positions: {total_positions}")
        logger.info(f"   Positions with Override: {positions_with_override}")
        logger.info(f"   Positions with Tiered TP: {positions_with_tiered_tp}")
        
        # Check if tiered TP monitor is running
        logger.info(f"\n🔧 CHECKING TIERED TP MONITOR STATUS...")
        try:
            from tiered_tp_monitor import TieredTPMonitor
            from profit_ride_override import ProfitRideOverride
            from regime_classifier import LorentzianDistanceClassifier
            from volatility_monitor import VolatilityMonitor
            
            regime_classifier = LorentzianDistanceClassifier()
            volatility_monitor = VolatilityMonitor()
            override_manager = ProfitRideOverride(regime_classifier, volatility_monitor)
            
            tiered_tp_monitor = TieredTPMonitor(oanda_service, position_tracker, override_manager)
            
            # Check if monitor is running
            if hasattr(tiered_tp_monitor, 'monitoring'):
                logger.info(f"   Tiered TP Monitor Running: {tiered_tp_monitor.monitoring}")
            else:
                logger.info(f"   Tiered TP Monitor Status: Unknown")
                
        except Exception as e:
            logger.error(f"   Error checking tiered TP monitor: {e}")
        
    except Exception as e:
        logger.error(f"Diagnosis failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(diagnose_positions()) 