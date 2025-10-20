"""
ORPHANED TRADE MONITOR
Institutional-grade fail-safe system for positions that missed their close signals

Purpose:
- Detect positions that have been open longer than their timeframe without trailing stops
- Apply trailing stops to profitable orphaned positions
- Close unprofitable orphaned positions immediately
- Reconcile DB vs OANDA position mismatches

Author: Chief Quant Architect
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class OrphanedPosition:
    """Represents a position that's been abandoned by the strategy"""
    position_id: str
    symbol: str
    action: str
    entry_price: float
    current_price: float
    size: float
    timeframe: str
    open_time: datetime
    pnl: float
    pnl_percentage: float
    stop_loss: Optional[float]
    trailing_stop_active: bool
    age_minutes: float
    timeframe_minutes: int
    age_ratio: float  # age / timeframe (should be < 1.0 for healthy positions)


class OrphanedTradeMonitor:
    """
    Monitors for positions that have exceeded their expected lifetime
    without proper exit protocols in place.
    
    This is a critical risk management layer that prevents positions
    from being abandoned due to missed signals or webhook failures.
    """
    
    def __init__(self, position_tracker, oanda_service, override_manager, alert_handler):
        self.position_tracker = position_tracker
        self.oanda_service = oanda_service
        self.override_manager = override_manager
        self.alert_handler = alert_handler
        self.monitoring = False
        self._monitor_task = None
        
        # Configuration
        self.check_interval_seconds = 180  # Check every 3 minutes
        self.orphan_age_multiplier = 1.5  # Position is orphaned if age > timeframe * 1.5
        self.min_profit_for_trailing = 0.0  # Any profit qualifies for trailing stop
        
        # Metrics
        self.orphans_detected = 0
        self.trailing_stops_applied = 0
        self.emergency_closes = 0
        self.db_sync_fixes = 0
        
    async def start_monitoring(self):
        """Start the orphaned trade monitoring loop"""
        if self.monitoring:
            logger.warning("⚠️ Orphaned trade monitor already running")
            return
            
        self.monitoring = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("🛡️ Orphaned Trade Monitor started - fail-safe system active")
        
    async def stop_monitoring(self):
        """Stop monitoring"""
        self.monitoring = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 Orphaned Trade Monitor stopped")
        
    async def _monitor_loop(self):
        """Main monitoring loop"""
        while self.monitoring:
            try:
                await self._check_for_orphaned_trades()
                await self._reconcile_db_vs_oanda()
                await asyncio.sleep(self.check_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Error in orphaned trade monitor: {e}", exc_info=True)
                await asyncio.sleep(self.check_interval_seconds)
                
    async def _check_for_orphaned_trades(self):
        """
        Check all open positions for orphaned status
        
        A position is orphaned if:
        1. Age > timeframe * orphan_age_multiplier
        2. No trailing stop is active
        3. No take-profit is set
        """
        try:
            open_positions = await self.position_tracker.get_open_positions()
            
            orphaned_positions = []
            
            for symbol, positions in open_positions.items():
                for position_id, position_data in positions.items():
                    orphan_status = await self._check_if_orphaned(position_id, position_data)
                    
                    if orphan_status:
                        orphaned_positions.append(orphan_status)
                        
            if orphaned_positions:
                logger.warning(f"⚠️ ORPHANED POSITIONS DETECTED: {len(orphaned_positions)}")
                
                for orphan in orphaned_positions:
                    await self._handle_orphaned_position(orphan)
            else:
                logger.debug("✅ No orphaned positions detected")
                
        except Exception as e:
            logger.error(f"❌ Error checking for orphaned trades: {e}", exc_info=True)
            
    async def _check_if_orphaned(self, position_id: str, position_data: Dict) -> Optional[OrphanedPosition]:
        """
        Determine if a position is orphaned
        
        Returns OrphanedPosition object if orphaned, None otherwise
        """
        try:
            # Parse timeframe to minutes
            timeframe_str = position_data.get('timeframe', '15')
            timeframe_minutes = self._timeframe_to_minutes(timeframe_str)
            
            # Calculate position age
            open_time_str = position_data.get('open_time') or position_data.get('created_at')
            if not open_time_str:
                logger.warning(f"⚠️ Position {position_id} has no open_time - skipping")
                return None
                
            if isinstance(open_time_str, str):
                # Parse ISO format with timezone
                if '+' in open_time_str or open_time_str.endswith('Z'):
                    open_time = datetime.fromisoformat(open_time_str.replace('Z', '+00:00'))
                else:
                    open_time = datetime.fromisoformat(open_time_str).replace(tzinfo=timezone.utc)
            else:
                open_time = open_time_str
                
            now = datetime.now(timezone.utc)
            age_delta = now - open_time
            age_minutes = age_delta.total_seconds() / 60
            
            # Calculate age ratio
            age_ratio = age_minutes / timeframe_minutes if timeframe_minutes > 0 else 0
            
            # Check if position has trailing stop active
            metadata = position_data.get('metadata', {})
            if isinstance(metadata, str):
                import json
                metadata = json.loads(metadata) if metadata else {}
                
            trailing_stop_active = metadata.get('trailing_stop_active', False)
            take_profit = position_data.get('take_profit')
            
            # Position is orphaned if:
            # 1. Age exceeds timeframe * multiplier
            # 2. No trailing stop is active
            # 3. No take-profit is set
            is_orphaned = (
                age_ratio >= self.orphan_age_multiplier and
                not trailing_stop_active and
                take_profit is None
            )
            
            if not is_orphaned:
                return None
                
            # Get current price and calculate P&L
            symbol = position_data['symbol']
            action = position_data['action']
            current_price = await self.oanda_service.get_current_price(symbol, action)
            
            if not current_price:
                logger.warning(f"⚠️ Could not get current price for {symbol}")
                return None
                
            entry_price = position_data['entry_price']
            size = position_data['size']
            
            # Calculate P&L
            if action == 'BUY':
                pnl = (current_price - entry_price) * size
                pnl_percentage = ((current_price - entry_price) / entry_price) * 100
            else:  # SELL
                pnl = (entry_price - current_price) * size
                pnl_percentage = ((entry_price - current_price) / entry_price) * 100
                
            self.orphans_detected += 1
            
            return OrphanedPosition(
                position_id=position_id,
                symbol=symbol,
                action=action,
                entry_price=entry_price,
                current_price=current_price,
                size=size,
                timeframe=timeframe_str,
                open_time=open_time,
                pnl=pnl,
                pnl_percentage=pnl_percentage,
                stop_loss=position_data.get('stop_loss'),
                trailing_stop_active=trailing_stop_active,
                age_minutes=age_minutes,
                timeframe_minutes=timeframe_minutes,
                age_ratio=age_ratio
            )
            
        except Exception as e:
            logger.error(f"❌ Error checking if position {position_id} is orphaned: {e}", exc_info=True)
            return None
            
    async def _handle_orphaned_position(self, orphan: OrphanedPosition):
        """
        Handle an orphaned position based on P&L
        
        If profitable: Apply trailing stop
        If losing: Close immediately
        """
        try:
            logger.warning("=" * 80)
            logger.warning(f"🚨 ORPHANED POSITION DETECTED: {orphan.position_id}")
            logger.warning(f"   Symbol: {orphan.symbol} {orphan.action}")
            logger.warning(f"   Timeframe: {orphan.timeframe} ({orphan.timeframe_minutes}min)")
            logger.warning(f"   Age: {orphan.age_minutes:.1f}min ({orphan.age_ratio:.2f}x timeframe)")
            logger.warning(f"   Entry: {orphan.entry_price:.5f}")
            logger.warning(f"   Current: {orphan.current_price:.5f}")
            logger.warning(f"   P&L: ${orphan.pnl:.2f} ({orphan.pnl_percentage:+.2f}%)")
            logger.warning(f"   Stop Loss: {orphan.stop_loss}")
            logger.warning(f"   Trailing Stop Active: {orphan.trailing_stop_active}")
            logger.warning("=" * 80)
            
            if orphan.pnl > self.min_profit_for_trailing:
                # Position is profitable - apply trailing stop
                await self._apply_emergency_trailing_stop(orphan)
            else:
                # Position is losing - close immediately
                await self._emergency_close_position(orphan)
                
        except Exception as e:
            logger.error(f"❌ Error handling orphaned position {orphan.position_id}: {e}", exc_info=True)
            
    async def _apply_emergency_trailing_stop(self, orphan: OrphanedPosition):
        """Apply trailing stop to a profitable orphaned position"""
        try:
            logger.warning(f"🎯 EMERGENCY ACTION: Applying trailing stop to profitable orphan {orphan.position_id}")
            logger.warning(f"   Current profit: ${orphan.pnl:.2f} ({orphan.pnl_percentage:+.2f}%)")
            
            # Create position object for override manager
            from dataclasses import dataclass as dc
            @dc
            class Position:
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
                        
            position_obj = Position(
                symbol=orphan.symbol,
                action=orphan.action,
                entry_price=orphan.entry_price,
                size=orphan.size,
                timeframe=orphan.timeframe,
                stop_loss=orphan.stop_loss,
                metadata={}
            )
            
            # Activate trailing stop through the profit ride override manager
            trailing_config = await self.override_manager.activate_trailing_stop(
                position_obj, 
                orphan.current_price
            )
            
            if trailing_config:
                # Update position metadata
                await self.position_tracker.update_position_metadata(orphan.position_id, {
                    'trailing_stop_active': True,
                    'trailing_stop_price': trailing_config['trailing_stop_price'],
                    'profit_ride_override_fired': True,
                    'orphan_rescue_timestamp': datetime.now(timezone.utc).isoformat()
                })
                
                # Update OANDA with new stop loss
                success = await self.oanda_service.modify_position(
                    orphan.symbol,
                    orphan.action,
                    stop_loss=trailing_config['trailing_stop_price'],
                    take_profit=None  # Clear TP for trailing stop mode
                )
                
                if success:
                    self.trailing_stops_applied += 1
                    logger.warning(f"✅ EMERGENCY TRAILING STOP APPLIED: {orphan.symbol}")
                    logger.warning(f"   Trailing stop price: {trailing_config['trailing_stop_price']:.5f}")
                    logger.warning(f"   Protected profit: ${orphan.pnl:.2f}")
                else:
                    logger.error(f"❌ Failed to update OANDA with trailing stop for {orphan.symbol}")
                    
        except Exception as e:
            logger.error(f"❌ Error applying emergency trailing stop to {orphan.position_id}: {e}", exc_info=True)
            
    async def _emergency_close_position(self, orphan: OrphanedPosition):
        """Emergency close of a losing orphaned position"""
        try:
            logger.warning(f"❌ EMERGENCY ACTION: Closing losing orphan {orphan.position_id}")
            logger.warning(f"   Current loss: ${orphan.pnl:.2f} ({orphan.pnl_percentage:+.2f}%)")
            logger.warning(f"   Reason: Position orphaned with no exit protocol")
            
            # Close through alert handler to ensure proper tracking
            close_result = await self.alert_handler._execute_close_internal(
                symbol=orphan.symbol,
                position_id=orphan.position_id,
                size=orphan.size,
                reason="orphan_emergency_close"
            )
            
            if close_result.get('status') == 'success':
                self.emergency_closes += 1
                logger.warning(f"✅ EMERGENCY CLOSE EXECUTED: {orphan.symbol}")
                logger.warning(f"   Exit price: {close_result.get('exit_price', 'N/A')}")
                logger.warning(f"   Final P&L: ${close_result.get('pnl', orphan.pnl):.2f}")
            else:
                logger.error(f"❌ Emergency close failed for {orphan.position_id}: {close_result}")
                
        except Exception as e:
            logger.error(f"❌ Error emergency closing {orphan.position_id}: {e}", exc_info=True)
            
    async def _reconcile_db_vs_oanda(self):
        """
        Reconcile position database vs OANDA reality
        
        Fixes the BTCUSD issue where DB shows open position but OANDA doesn't have it
        """
        try:
            # Get positions from DB
            db_positions = await self.position_tracker.get_open_positions()
            
            # Get positions from OANDA
            from oandapyV20.endpoints.positions import OpenPositions
            positions_request = OpenPositions(accountID=self.oanda_service.config.oanda_account_id)
            oanda_response = await self.oanda_service.robust_oanda_request(positions_request)
            
            if not oanda_response or 'positions' not in oanda_response:
                logger.warning("⚠️ Could not fetch OANDA positions for reconciliation")
                return
                
            # Build set of OANDA instrument symbols that have open positions
            oanda_open_symbols = set()
            for pos in oanda_response['positions']:
                instrument = pos.get('instrument', '')
                long_units = float(pos.get('long', {}).get('units', 0))
                short_units = float(pos.get('short', {}).get('units', 0))
                
                if long_units != 0 or short_units != 0:
                    oanda_open_symbols.add(instrument)
                    
            # Check each DB position against OANDA
            for symbol, positions in db_positions.items():
                for position_id, position_data in positions.items():
                    db_symbol = position_data['symbol']
                    
                    # Check if position exists in OANDA
                    symbol_variants = [
                        db_symbol,
                        db_symbol.replace('_', ''),
                        db_symbol.replace('_', '/'),
                    ]
                    
                    exists_in_oanda = any(variant in oanda_open_symbols for variant in symbol_variants)
                    
                    if not exists_in_oanda:
                        # Position in DB but not in OANDA - likely already closed by SL/TP
                        logger.warning(f"🔄 DB/OANDA MISMATCH DETECTED:")
                        logger.warning(f"   Position ID: {position_id}")
                        logger.warning(f"   Symbol: {db_symbol}")
                        logger.warning(f"   Status: Open in DB, but not found in OANDA")
                        logger.warning(f"   Likely closed by SL/TP on OANDA side")
                        
                        # Sync DB by marking position as closed
                        await self._sync_closed_position(position_id, position_data)
                        
        except Exception as e:
            logger.error(f"❌ Error reconciling DB vs OANDA: {e}", exc_info=True)
            
    async def _sync_closed_position(self, position_id: str, position_data: Dict):
        """Sync a position that was closed on OANDA but still open in DB"""
        try:
            logger.warning(f"🔄 Syncing closed position {position_id} in database")
            
            # Get final price from OANDA
            symbol = position_data['symbol']
            action = position_data['action']
            current_price = await self.oanda_service.get_current_price(symbol, action)
            
            if not current_price:
                current_price = position_data.get('current_price', position_data['entry_price'])
                
            # Close in tracker
            await self.position_tracker.close_position(
                position_id=position_id,
                exit_price=current_price,
                reason="sync_oanda_sl_tp_close"
            )
            
            self.db_sync_fixes += 1
            logger.warning(f"✅ Position {position_id} synced as closed")
            logger.warning(f"   Probable reason: Stop-loss or take-profit hit on OANDA")
            
        except Exception as e:
            logger.error(f"❌ Error syncing closed position {position_id}: {e}", exc_info=True)
            
    def _timeframe_to_minutes(self, timeframe: str) -> int:
        """Convert timeframe string to minutes"""
        try:
            # Handle string timeframes like '15', '60', '120', '240', '1H', '2H', '4H', '1D'
            if isinstance(timeframe, int):
                return timeframe
                
            timeframe = str(timeframe).upper()
            
            if timeframe.endswith('D'):
                return int(timeframe[:-1]) * 1440  # days to minutes
            elif timeframe.endswith('H'):
                return int(timeframe[:-1]) * 60  # hours to minutes
            elif timeframe.endswith('M'):
                return int(timeframe[:-1])  # already in minutes
            else:
                # Assume it's just a number representing minutes
                return int(timeframe)
                
        except Exception as e:
            logger.error(f"❌ Error parsing timeframe '{timeframe}': {e}")
            return 15  # Default to 15 minutes
            
    async def get_metrics(self) -> Dict:
        """Get monitoring metrics"""
        return {
            "orphans_detected": self.orphans_detected,
            "trailing_stops_applied": self.trailing_stops_applied,
            "emergency_closes": self.emergency_closes,
            "db_sync_fixes": self.db_sync_fixes,
            "monitoring_active": self.monitoring
        }

