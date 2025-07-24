"""
Tiered Take Profit Monitor
Monitors positions with tiered TP levels and executes partial closures
"""

import asyncio
import logging
from typing import Dict, List, Optional
from datetime import datetime, timezone
from dataclasses import dataclass

from oanda_service import OandaService
from tracker import PositionTracker
from profit_ride_override import TieredTPLevel, ProfitRideOverride
from position_journal import position_journal

logger = logging.getLogger(__name__)

@dataclass
class TieredTPExecution:
    """Result of a tiered TP execution"""
    level: int
    units_closed: int
    price: float
    pnl: float
    timestamp: datetime

class TieredTPMonitor:
    def __init__(self, oanda_service: OandaService, position_tracker: PositionTracker, override_manager: ProfitRideOverride):
        self.oanda_service = oanda_service
        self.position_tracker = position_tracker
        self.override_manager = override_manager
        self.monitoring = False
        self.monitor_interval = 30  # Check every 30 seconds
        self._monitor_task = None
        
    async def start_monitoring(self):
        """Start the tiered TP monitoring loop"""
        if self.monitoring:
            logger.warning("Tiered TP monitor is already running")
            return
            
        self.monitoring = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("🎯 Tiered TP monitor started")
        
    async def stop_monitoring(self):
        """Stop the tiered TP monitoring loop"""
        self.monitoring = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 Tiered TP monitor stopped")
        
    async def _monitor_loop(self):
        """Main monitoring loop"""
        while self.monitoring:
            try:
                await self._check_all_positions()
                await asyncio.sleep(self.monitor_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in tiered TP monitor loop: {e}")
                await asyncio.sleep(self.monitor_interval)
                
    async def _check_all_positions(self):
        """Check all active positions for tiered TP triggers"""
        try:
            # Get all open positions (flattened)
            open_positions_nested = await self.position_tracker.get_open_positions()
            active_positions = {}
            for symbol, positions in open_positions_nested.items():
                for position_id, position_data in positions.items():
                    active_positions[position_id] = position_data
            
            for position_id, position_data in active_positions.items():
                # Check if position has tiered TP levels
                if position_data.get('metadata', {}).get('tiered_tp_levels'):
                    await self._check_position_tp_triggers(position_id, position_data)
                    
        except Exception as e:
            logger.error(f"Error checking positions for tiered TP: {e}")
            
    async def _check_position_tp_triggers(self, position_id: str, position_data: Dict):
        """Check if any TP levels have been triggered for a specific position"""
        try:
            symbol = position_data['symbol']
            current_price = await self.oanda_service.get_current_price(symbol, position_data['action'])
            
            if not current_price:
                logger.warning(f"Could not get current price for {symbol}")
                return
                
            # Create position object for override manager
            from dataclasses import dataclass
            @dataclass
            class PositionForOverride:
                symbol: str
                action: str
                entry_price: float
                size: float
                timeframe: str
                stop_loss: float = None
                metadata: dict = None
                
                def __post_init__(self):
                    if self.metadata is None:
                        self.metadata = {}
            
            position_obj = PositionForOverride(
                symbol=position_data['symbol'],
                action=position_data['action'],
                entry_price=position_data['entry_price'],
                size=position_data['size'],
                timeframe=position_data.get('timeframe', '15'),
                stop_loss=position_data.get('stop_loss'),
                metadata=position_data.get('metadata', {})
            )
            
            # Check for triggered TP levels
            triggered_levels = await self.override_manager.check_tiered_tp_triggers(position_obj, current_price)
            
            if triggered_levels:
                logger.info(f"🎯 TP triggers detected for {symbol}: {len(triggered_levels)} levels")
                await self._execute_tiered_tp_closures(position_id, position_data, triggered_levels, current_price)
                
        except Exception as e:
            logger.error(f"Error checking TP triggers for {position_id}: {e}")
            
    async def _execute_tiered_tp_closures(self, position_id: str, position_data: Dict, triggered_levels: List[TieredTPLevel], current_price: float):
        """Execute partial closures for triggered TP levels"""
        try:
            symbol = position_data['symbol']
            action = position_data['action']
            
            for level in triggered_levels:
                if level.units <= 0:
                    continue
                    
                logger.info(f"🎯 Executing TP Level {level.level} for {symbol}: {level.units} units at {current_price}")
                
                # Execute partial closure
                close_units = -level.units if action.upper() == "BUY" else level.units
                close_payload = {
                    "symbol": symbol,
                    "action": "CLOSE",
                    "units": close_units
                }
                
                success, result = await self.oanda_service.execute_trade(close_payload)
                
                if success:
                    # Calculate P&L for this partial closure
                    entry_price = position_data['entry_price']
                    if action.upper() == "BUY":
                        pnl = (current_price - entry_price) * level.units
                    else:
                        pnl = (entry_price - current_price) * level.units
                    
                    # Update position metadata
                    position_data['metadata']['tiered_tp_levels'] = self._update_tp_level_status(
                        position_data['metadata']['tiered_tp_levels'],
                        level.level,
                        True,
                        datetime.now(timezone.utc)
                    )
                    
                    # Update position size
                    new_size = position_data['size'] - level.units
                    position_data['size'] = new_size
                    
                    # Update tracker
                    await self.position_tracker.update_position(
                        position_id, 
                        size=new_size,
                        metadata=position_data['metadata']
                    )
                    
                    # Record partial exit
                    await position_journal.record_partial_exit(
                        position_id=position_id,
                        exit_price=current_price,
                        units_closed=level.units,
                        exit_reason=f"Tiered TP Level {level.level}",
                        pnl=pnl
                    )
                    
                    logger.info(f"✅ TP Level {level.level} executed: {level.units} units closed, P&L: ${pnl:.2f}")
                    
                    # Check if position is fully closed
                    if new_size <= 0:
                        logger.info(f"🎉 Position {position_id} fully closed via tiered TP")
                        await self.position_tracker.close_position(position_id, current_price, "Tiered TP Complete")
                        break
                        
                else:
                    logger.error(f"❌ Failed to execute TP Level {level.level}: {result.get('error')}")
                    
        except Exception as e:
            logger.error(f"Error executing tiered TP closures for {position_id}: {e}")
            
    def _update_tp_level_status(self, tp_levels_data: List[Dict], level: int, triggered: bool, closed_at: datetime) -> List[Dict]:
        """Update the status of a TP level in the metadata"""
        updated_levels = []
        for level_data in tp_levels_data:
            if level_data['level'] == level:
                level_data['triggered'] = triggered
                level_data['closed_at'] = closed_at.isoformat()
            updated_levels.append(level_data)
        return updated_levels
        
    async def get_tiered_tp_status(self, position_id: str) -> Optional[Dict]:
        """Get the current status of tiered TP levels for a position"""
        try:
            position_data = await self.position_tracker.get_position(position_id)
            if not position_data:
                return None
                
            tp_levels = position_data.get('metadata', {}).get('tiered_tp_levels', [])
            if not tp_levels:
                return None
                
            return {
                'position_id': position_id,
                'symbol': position_data['symbol'],
                'current_size': position_data['size'],
                'tp_levels': tp_levels,
                'levels_completed': sum(1 for level in tp_levels if level.get('triggered', False)),
                'total_levels': len(tp_levels)
            }
            
        except Exception as e:
            logger.error(f"Error getting tiered TP status for {position_id}: {e}")
            return None 