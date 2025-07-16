import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, List, NamedTuple
from utils import logger
from config import settings
from position_journal import Position

class ClosePositionResult(NamedTuple):
    success: bool
    position_data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

class PositionTracker:
    """
    Tracks all positions across different symbols and timeframes,
    providing a centralized registry for position management.
    With database persistence capability.
    """
    def __init__(self, db_manager=None, oanda_service=None):
        self.positions = {}
        self.open_positions_by_symbol = {}
        self.closed_positions = {}
        self.position_history = []
        self._lock = asyncio.Lock()
        self.max_history = 1000
        self._running = False
        self.db_manager = db_manager
        self.oanda_service = oanda_service
        self._price_update_lock = asyncio.Lock()

    async def initialize(self):
        """Initialize the position tracker"""
        logger.info("Initializing position tracker...")
        try:
            # Initialize any required connections or state
            if self.db_manager:
                logger.info("Position tracker connected to database")
            if self.oanda_service:
                logger.info("Position tracker connected to OANDA service")
            logger.info("✅ Position tracker initialized successfully")
        except Exception as e:
            logger.error(f"❌ Position tracker initialization failed: {e}")
            raise

    async def start(self):
        if self._running:
            return
        self._running = True
        if self.db_manager:
            try:
                open_positions = await self.db_manager.get_open_positions()
                for position_data in open_positions:
                    await self.restore_position(position_data["position_id"], position_data)
                closed_positions = await self.db_manager.get_closed_positions(limit=1000)
                self.closed_positions = {p["position_id"]: p for p in closed_positions}
                self.position_history = []
                for position_data in open_positions:
                    self.position_history.append(position_data)
                for position_data in closed_positions:
                    self.position_history.append(position_data)
                self.position_history.sort(key=lambda x: x.get("open_time", ""), reverse=True)
                if len(self.position_history) > self.max_history:
                    self.position_history = self.position_history[:self.max_history]
                logger.info(f"Position tracker started with {len(open_positions)} open and {len(closed_positions)} closed positions loaded from database")
            except Exception as e:
                logger.error(f"Error loading positions from database: {str(e)}")
                logger.info("Position tracker started with empty position list")
        else:
            logger.info("Position tracker started (database persistence not available)")

    async def stop(self):
        if not self._running:
            return
        self._running = False
        logger.info("Position tracker stopped")
        
    # FIX: Add the missing close() method required by main.py for graceful shutdown.
    async def close(self):
        """Alias for stop() to conform to the application's lifecycle management."""
        await self.stop()

    async def get_position_by_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            if symbol not in self.open_positions_by_symbol:
                return None
            positions_for_symbol = self.open_positions_by_symbol[symbol]
            if not positions_for_symbol:
                return None
            first_position_id = next(iter(positions_for_symbol))
            position = positions_for_symbol[first_position_id]
            return self._position_to_dict(position)

    async def record_position(self, position_id: str, symbol: str, action: str, timeframe: str, entry_price: float, size: float, stop_loss: Optional[float] = None, take_profit: Optional[float] = None, metadata: Optional[Dict[str, Any]] = None) -> bool:
        max_pos = settings.trading.max_positions
        if len(self.positions) >= max_pos:
             logger.warning(f"Maximum total positions reached: {max_pos}")
             return False

        position = Position(position_id, symbol, action, timeframe, entry_price, size, stop_loss, take_profit, metadata)
        self.positions[position_id] = position
        if symbol not in self.open_positions_by_symbol:
            self.open_positions_by_symbol[symbol] = {}
        self.open_positions_by_symbol[symbol][position_id] = position
        position_dict = self._position_to_dict(position)
        self.position_history.insert(0, position_dict) # Add to the front
        if len(self.position_history) > self.max_history:
            self.position_history.pop()

        if self.db_manager:
            try:
                await self.db_manager.save_position(position_dict)
            except Exception as e:
                logger.error(f"Error saving position {position_id} to database: {str(e)}")
        logger.info(f"Recorded new position: {position_id} ({symbol} {action})")
        return True

    async def update_position(self, position_id: str, stop_loss: Optional[float] = None, take_profit: Optional[float] = None, metadata: Optional[Dict[str, Any]] = None) -> bool:
        async with self._lock:
            if position_id not in self.positions:
                logger.warning(f"Position {position_id} not found")
                return False
            position = self.positions[position_id]
            if stop_loss is not None:
                position.update_stop_loss(stop_loss)
            if take_profit is not None:
                position.update_take_profit(take_profit)
            if metadata is not None:
                position.update_metadata(metadata)
            position_dict = self._position_to_dict(position)
            for i, hist_pos in enumerate(self.position_history):
                if hist_pos.get("position_id") == position_id:
                    self.position_history[i] = position_dict
                    break
            if self.db_manager:
                try:
                    await self.db_manager.update_position(position_id, position_dict)
                except Exception as e:
                    logger.error(f"Error updating position {position_id} in database: {str(e)}")
            return True

    async def update_position_price(self, position_id: str, current_price: float) -> bool:
        async with self._price_update_lock:
            async with self._lock:
                if position_id not in self.positions:
                    return False
                position = self.positions[position_id]
                position.update_price(current_price)
                if self.db_manager:
                    try:
                        update_data = {
                            "current_price": position.current_price,
                            "pnl": position.pnl,
                            "pnl_percentage": position.pnl_percentage,
                            "last_update": position.last_update.isoformat()
                        }
                        await self.db_manager.update_position(position_id, update_data)
                    except Exception as e:
                        logger.error(f"Error updating position price for {position_id} in database: {str(e)}")
                return True

    async def get_position_info(self, position_id: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            if position_id in self.positions:
                return self._position_to_dict(self.positions[position_id])
            elif position_id in self.closed_positions:
                return self.closed_positions[position_id]
            if self.db_manager:
                try:
                    position_data = await self.db_manager.get_position(position_id)
                    if position_data:
                        if position_data.get("status") == "open":
                            await self.restore_position(position_id, position_data)
                        else:
                            self.closed_positions[position_id] = position_data
                        return position_data
                except Exception as e:
                    logger.error(f"Error getting position {position_id} from database: {str(e)}")
            return None

    async def get_open_positions(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        async with self._lock:
            result = {}
            for symbol, positions in self.open_positions_by_symbol.items():
                result[symbol] = {pos_id: self._position_to_dict(pos) for pos_id, pos in positions.items()}
            return result

    async def get_closed_positions(self, limit: int = 100) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            if self.db_manager:
                try:
                    closed_positions = await self.db_manager.get_closed_positions(limit=limit)
                    return {p["position_id"]: p for p in closed_positions}
                except Exception as e:
                    logger.error(f"Error getting closed positions from database: {str(e)}")
            
            sorted_positions = sorted(self.closed_positions.items(), key=lambda x: x[1].get("close_time", ""), reverse=True)
            return dict(sorted_positions[:limit])

    async def get_all_positions(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            result = {pos_id: self._position_to_dict(pos) for pos_id, pos in self.positions.items()}
            result.update(self.closed_positions)
            return result

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            open_count = len(self.positions)
            closed_count = len(self.closed_positions)
            open_pnl = sum(p.pnl for p in self.positions.values())
            closed_pnl = sum(p.get("pnl", 0) for p in self.closed_positions.values())
            
            winning_positions = [p for p in self.closed_positions.values() if p.get("pnl", 0) > 0]
            losing_positions = [p for p in self.closed_positions.values() if p.get("pnl", 0) < 0]
            
            win_count = len(winning_positions)
            loss_count = len(losing_positions)
            win_rate = win_count / closed_count * 100 if closed_count else 0
            
            return {
                "open_positions": open_count, "closed_positions": closed_count,
                "total_positions": open_count + closed_count, "open_pnl": open_pnl,
                "closed_pnl": closed_pnl, "total_pnl": open_pnl + closed_pnl,
                "win_count": win_count, "loss_count": loss_count, "win_rate": win_rate,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

    def _position_to_dict(self, position: Position) -> Dict[str, Any]:
        return {
            "position_id": position.position_id, "symbol": position.symbol, "action": position.action,
            "timeframe": position.timeframe, "entry_price": position.entry_price, "size": position.size,
            "stop_loss": position.stop_loss, "take_profit": position.take_profit,
            "open_time": position.open_time.isoformat(),
            "close_time": position.close_time.isoformat() if position.close_time else None,
            "exit_price": position.exit_price, "current_price": position.current_price,
            "pnl": position.pnl, "pnl_percentage": position.pnl_percentage,
            "status": position.status, "last_update": position.last_update.isoformat(),
            "metadata": position.metadata, "exit_reason": position.exit_reason
        }

    async def restore_position(self, position_id: str, position_data: Dict[str, Any]) -> bool:
        async with self._lock:
            if position_id in self.positions: return True
            try:
                position = Position(
                    position_id=position_id,
                    symbol=position_data["symbol"], action=position_data["action"],
                    timeframe=position_data["timeframe"], entry_price=position_data["entry_price"],
                    size=position_data["size"], stop_loss=position_data.get("stop_loss"),
                    take_profit=position_data.get("take_profit"), metadata=position_data.get("metadata", {})
                )
                if position_data.get("status", "open") == "open":
                    self.positions[position_id] = position
                    if position.symbol not in self.open_positions_by_symbol:
                        self.open_positions_by_symbol[position.symbol] = {}
                    self.open_positions_by_symbol[position.symbol][position_id] = position
                else:
                    self.closed_positions[position_id] = position_data
                logger.info(f"Restored position: {position_id}")
                return True
            except Exception as e:
                logger.error(f"Error restoring position {position_id}: {str(e)}")
                return False

    async def close_position(self, position_id: str, exit_price: float, reason: str) -> ClosePositionResult:
        async with self._lock:
            if position_id not in self.positions:
                return ClosePositionResult(success=False, error=f"Position {position_id} not found")
            try:
                position = self.positions.pop(position_id)
                position.close(exit_price, reason)
                if position.symbol in self.open_positions_by_symbol:
                    self.open_positions_by_symbol[position.symbol].pop(position_id, None)
                    if not self.open_positions_by_symbol[position.symbol]:
                        del self.open_positions_by_symbol[position.symbol]
                
                position_dict = self._position_to_dict(position)
                self.closed_positions[position_id] = position_dict
                
                if self.db_manager:
                    await self.db_manager.update_position(position_id, position_dict)
                
                logger.info(f"Closed position: {position_id} at {exit_price} (reason: {reason})")
                return ClosePositionResult(success=True, position_data=position_dict)
            except Exception as e:
                logger.error(f"Error closing position {position_id}: {str(e)}")
                return ClosePositionResult(success=False, error=str(e))
