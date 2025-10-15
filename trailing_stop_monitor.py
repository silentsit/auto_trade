"""
Trailing Stop Monitor
Monitors positions for profit override activation and manages trailing stops
"""

import asyncio
import logging
from typing import Dict, List, Optional
from datetime import datetime, timezone
from dataclasses import dataclass

from oanda_service import OandaService
from tracker import PositionTracker
from profit_ride_override import ProfitRideOverride
from position_journal import position_journal
from config import config

logger = logging.getLogger(__name__)

# NOTE: TieredTPLevel and TieredTPExecution classes removed - bot now uses trailing stop system only

class TrailingStopMonitor:
    def __init__(self, oanda_service: OandaService, position_tracker: PositionTracker, override_manager: ProfitRideOverride):
        self.oanda_service = oanda_service
        self.position_tracker = position_tracker
        self.override_manager = override_manager
        self.monitoring = False
        self.monitor_interval = 300  # Check every 5 minutes - optimal for live trading
        self._monitor_task = None
        # Taper monitoring
        self._taper_events: List[Dict] = []
        self._max_taper_events: int = 500
        
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
        """Enhanced monitoring with priority-based system"""
        try:
            # Get all open positions (flattened)
            open_positions_nested = await self.position_tracker.get_open_positions()
            active_positions = {}
            for symbol, positions in open_positions_nested.items():
                for position_id, position_data in positions.items():
                    active_positions[position_id] = position_data
            
            # 1. PRIORITY: Check SL/TP triggers first
            await self._check_sl_tp_triggers(active_positions)
            
            # 2. Update trailing stops for profit override positions
            await self._update_trailing_stops(active_positions)
            
            # 3. Check other dynamic exit conditions
            await self._check_dynamic_exits(active_positions)
                    
        except Exception as e:
            logger.error(f"Error in enhanced position monitoring: {e}")
            
    async def _check_position_tp_triggers(self, position_id: str, position_data: Dict):
        """Check for profit override conditions and manage trailing stops"""
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
            
            # Check if trailing stop is already active
            if position_obj.metadata.get('trailing_stop_active', False):
                # Update trailing stop
                new_trailing_stop = await self.override_manager.update_trailing_stop(position_obj, current_price)
                
                # Check if trailing stop was hit
                if await self.override_manager.check_trailing_stop_hit(position_obj, current_price):
                    logger.info(f"🎯 Trailing stop hit for {symbol} at {current_price:.5f}")
                    await self._close_position_due_to_trailing_stop(position_id, position_data, current_price)
                    return
                
                # Update position data with new trailing stop
                if new_trailing_stop:
                    await self.position_tracker.update_position_metadata(position_id, {
                        'trailing_stop_price': new_trailing_stop,
                        'trailing_stop_active': True
                    })
            else:
                # Check if position should be overridden (profit override decision)
                override_decision = await self.override_manager.should_override(position_obj, current_price)
                
                if override_decision.ignore_close:
                    logger.info(f"🎯 Profit override activated for {symbol} - implementing trailing stop")
                    
                    # Activate trailing stop system
                    trailing_config = await self.override_manager.activate_trailing_stop(position_obj, current_price)
                    
                    # Update position metadata
                    await self.position_tracker.update_position_metadata(position_id, {
                        'profit_ride_override_fired': True,
                        'trailing_stop_active': True,
                        'trailing_stop_price': trailing_config.get('trailing_stop_price'),
                        'trailing_distance': trailing_config.get('trailing_distance'),
                        'atr_multiplier': trailing_config.get('atr_multiplier'),
                        'breakeven_enabled': False,
                        'override_decision': {
                            'confidence_score': override_decision.confidence_score,
                            'risk_reward_ratio': override_decision.risk_reward_ratio,
                            'market_regime': override_decision.market_regime,
                            'volatility_state': override_decision.volatility_state,
                            'momentum_score': override_decision.momentum_score
                        }
                    })
                
        except Exception as e:
            logger.error(f"Error checking profit override for {position_id}: {e}")
    
    async def _close_position_due_to_trailing_stop(self, position_id: str, position_data: Dict, current_price: float):
        """Close position due to trailing stop hit"""
        try:
            symbol = position_data['symbol']
            action = position_data['action']
            size = position_data['size']
            
            logger.info(f"🎯 Closing {symbol} due to trailing stop hit at {current_price:.5f}")
            
            # Close the position
            close_payload = {
                "symbol": symbol,
                "action": "SELL" if action == "BUY" else "BUY",
                "size": size,
                "reason": "trailing_stop_hit"
            }
            
            # Use the position tracker to close the position (expects: position_id, exit_price, reason)
            await self.position_tracker.close_position(position_id, current_price, "trailing_stop_hit")
            
            # Update metadata to mark as closed by trailing stop
            await self.position_tracker.update_position_metadata(position_id, {
                'trailing_stop_hit': True,
                'trailing_stop_exit_price': current_price,
                'closed_by': 'trailing_stop'
            })
            
        except Exception as e:
            logger.error(f"Error closing position due to trailing stop: {e}")
            
    # NOTE: Tiered TP functions removed - bot now uses trailing stop system only
    # The following functions were unused and have been removed:
    # - _execute_tiered_tp_closures
    # - _update_tp_level_status  
    # - get_tiered_tp_status

    async def _check_sl_tp_triggers(self, active_positions: Dict):
        """Check SL/TP triggers with priority handling"""
        try:
            # Build a unique symbol list for batch pricing
            symbols_to_fetch = set()
            for _, position_data in active_positions.items():
                metadata = position_data.get('metadata', {})
                if not metadata.get('profit_ride_override_fired', False):
                    symbols_to_fetch.add(position_data['symbol'])

            if not symbols_to_fetch:
                return

            prices_map = await self.oanda_service.get_current_prices(list(symbols_to_fetch))

            for position_id, position_data in active_positions.items():
                try:
                    metadata = position_data.get('metadata', {})
                    if metadata.get('profit_ride_override_fired', False):
                        continue

                    px = prices_map.get(position_data['symbol'])
                    if not px:
                        logger.warning(f"No price available for {position_data['symbol']} during SL/TP check")
                        continue

                    action = position_data['action']
                    current_price = float(px['ask']) if action == 'BUY' else float(px['bid'])
                    if not current_price:
                        continue

                    stop_loss = position_data.get('stop_loss')
                    take_profit = position_data.get('take_profit')

                    if stop_loss and current_price <= stop_loss:
                        logger.info(f"SL triggered for {position_id} at {current_price:.5f}")
                        await self._close_position_due_to_sl(position_id, position_data, current_price)
                    elif take_profit and current_price >= take_profit:
                        logger.info(f"TP triggered for {position_id} at {current_price:.5f}")
                        await self._close_position_due_to_tp(position_id, position_data, current_price)
                except Exception as e:
                    logger.error(f"Error checking SL/TP for {position_id}: {e}")
        except Exception as e:
            logger.error(f"Batch SL/TP check failed: {e}")

    async def _update_trailing_stops(self, active_positions: Dict):
        """Update trailing stops for profit override positions"""
        try:
            # Collect symbols that require trailing stop updates
            symbols_to_fetch = set()
            for _, position_data in active_positions.items():
                metadata = position_data.get('metadata', {})
                if metadata.get('trailing_stop_active', False):
                    symbols_to_fetch.add(position_data['symbol'])

            if not symbols_to_fetch:
                return

            prices_map = await self.oanda_service.get_current_prices(list(symbols_to_fetch))

            for position_id, position_data in active_positions.items():
                try:
                    metadata = position_data.get('metadata', {})
                    if not metadata.get('trailing_stop_active', False):
                        continue

                    px = prices_map.get(position_data['symbol'])
                    if not px:
                        logger.warning(f"No price available for {position_data['symbol']} during trailing stop update")
                        continue

                    action = position_data['action']
                    current_price = float(px['ask']) if action == 'BUY' else float(px['bid'])
                    if not current_price:
                        continue

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
                        metadata=metadata
                    )
                    
                    # Update trailing stop
                    new_trailing_stop = await self.override_manager.update_trailing_stop(
                        position_obj, current_price
                    )
                    
                    if new_trailing_stop:
                        position_data['metadata']['trailing_stop_price'] = new_trailing_stop
                    
                    # Check if trailing stop was hit
                    if await self.override_manager.check_trailing_stop_hit(position_obj, current_price):
                        await self._close_position_due_to_trailing_stop(position_id, position_data, current_price)
                        continue

                    # Phase 1: Deterministic taper with liquidity/slippage gate
                    try:
                        if config.trading.enable_profit_ride_taper:
                            regime_data = self.override_manager.regime.get_regime_data(position_obj.symbol) if self.override_manager and hasattr(self.override_manager, 'regime') else {"confidence": 1.0}
                            regime_conf = float(regime_data.get("confidence", 1.0))
                            taper_decision = await self.override_manager.should_taper(position_obj, current_price, regime_conf)
                            if taper_decision and taper_decision.get("fraction"):
                                fraction = float(taper_decision["fraction"])
                                # Enforce clip floor/ceiling
                                fraction = max(config.trading.taper_min_clip_fraction, min(0.9, fraction))
                                units_to_close = float(position_obj.size) * fraction
                                if units_to_close > 0:
                                    logger.info(f"✂️ Tapering {position_obj.symbol}: closing {fraction:.2%} due to {taper_decision.get('reason')} at {current_price:.5f}")
                                    await self.position_tracker.close_partial_position(
                                        position_id=position_id,
                                        exit_price=current_price,
                                        units_to_close=units_to_close,
                                        reason=f"taper:{taper_decision.get('reason')}"
                                    )
                                    # Record taper event
                                    self._record_taper_event({
                                        "timestamp": datetime.now(timezone.utc).isoformat(),
                                        "position_id": position_id,
                                        "symbol": position_obj.symbol,
                                        "fraction": fraction,
                                        "units": units_to_close,
                                        "price": current_price,
                                        "reason": taper_decision.get('reason'),
                                        "regime_confidence": regime_conf
                                    })
                                    # Reduce in-memory size for subsequent decisions
                                    position_obj.size = max(0.0, position_obj.size - units_to_close)
                    except Exception as e:
                        logger.error(f"Error during taper evaluation for {position_id}: {e}")

    # ---------- Taper monitoring utilities ----------
    def _record_taper_event(self, event: Dict):
        try:
            self._taper_events.append(event)
            if len(self._taper_events) > self._max_taper_events:
                self._taper_events = self._taper_events[-self._max_taper_events:]
        except Exception:
            pass

    def get_taper_events(self, limit: int = 100) -> List[Dict]:
        limit = max(1, min(limit, self._max_taper_events))
        return list(self._taper_events[-limit:])

    def get_taper_stats(self) -> Dict:
        stats: Dict[str, Any] = {"total": len(self._taper_events), "by_symbol": {}, "by_reason": {}}
        for ev in self._taper_events:
            sym = ev.get("symbol")
            rsn = ev.get("reason")
            stats["by_symbol"][sym] = stats["by_symbol"].get(sym, 0) + 1
            stats["by_reason"][rsn] = stats["by_reason"].get(rsn, 0) + 1
        return stats
                except Exception as e:
                    logger.error(f"Error updating trailing stop for {position_id}: {e}")
        except Exception as e:
            logger.error(f"Batch trailing stop update failed: {e}")

    async def _check_dynamic_exits(self, active_positions: Dict):
        """Check other dynamic exit conditions"""
        for position_id, position_data in active_positions.items():
            try:
                # Add any other dynamic exit conditions here
                # For example: time-based exits, volatility-based exits, etc.
                pass
                
            except Exception as e:
                logger.error(f"Error checking dynamic exits for {position_id}: {e}")

    async def _close_position_due_to_sl(self, position_id: str, position_data: Dict, current_price: float):
        """
        Close position due to stop loss hit.
        
        CRITICAL FIX: Actually close the position in OANDA and update database.
        Previously this was a stub that only logged, causing infinite retry loops.
        """
        try:
            symbol = position_data.get('symbol')
            units = position_data.get('units', 0)
            
            logger.info(f"🛑 Closing position {position_id} due to SL at {current_price:.5f}")
            
            # Close position in OANDA
            success, result = await self.oanda_service.close_position(symbol, units)
            
            if success:
                # Update database to mark position as closed
                await self.position_tracker.close_position(
                    position_id=position_id,
                    close_price=current_price,
                    close_reason="trailing_stop_loss"
                )
                
                # Log to position journal
                position_journal.log_exit(
                    position_id=position_id,
                    symbol=symbol,
                    exit_price=current_price,
                    exit_reason="Trailing SL Hit",
                    pnl=result.get('profit', 0) if isinstance(result, dict) else 0
                )
                
                logger.info(f"✅ Position {position_id} closed successfully via trailing SL")
            else:
                # Position not found in OANDA - likely already closed
                logger.warning(f"⚠️ Position {position_id} not found in OANDA, marking as closed in database")
                
                # Still update database to prevent infinite retries
                await self.position_tracker.close_position(
                    position_id=position_id,
                    close_price=current_price,
                    close_reason="trailing_stop_loss_not_found"
                )
                
        except Exception as e:
            logger.error(f"❌ Error closing position {position_id} due to SL: {e}", exc_info=True)

    async def _close_position_due_to_tp(self, position_id: str, position_data: Dict, current_price: float):
        """
        Close position due to take profit hit.
        
        CRITICAL FIX: Actually close the position in OANDA and update database.
        Previously this was a stub that only logged, causing infinite retry loops.
        """
        try:
            symbol = position_data.get('symbol')
            units = position_data.get('units', 0)
            
            logger.info(f"🎯 Closing position {position_id} due to TP at {current_price:.5f}")
            
            # Close position in OANDA
            success, result = await self.oanda_service.close_position(symbol, units)
            
            if success:
                # Update database to mark position as closed
                await self.position_tracker.close_position(
                    position_id=position_id,
                    close_price=current_price,
                    close_reason="take_profit"
                )
                
                # Log to position journal
                position_journal.log_exit(
                    position_id=position_id,
                    symbol=symbol,
                    exit_price=current_price,
                    exit_reason="Take Profit Hit",
                    pnl=result.get('profit', 0) if isinstance(result, dict) else 0
                )
                
                logger.info(f"✅ Position {position_id} closed successfully via TP")
            else:
                # Position not found in OANDA - likely already closed
                logger.warning(f"⚠️ Position {position_id} not found in OANDA, marking as closed in database")
                
                # Still update database to prevent infinite retries
                await self.position_tracker.close_position(
                    position_id=position_id,
                    close_price=current_price,
                    close_reason="take_profit_not_found"
                )
                
        except Exception as e:
            logger.error(f"❌ Error closing position {position_id} due to TP: {e}", exc_info=True) 