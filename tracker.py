import asyncio
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, List, NamedTuple
from utils import logger
from config import config
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
        if position_id in self.positions:
            logger.warning(f"Position {position_id} already exists")
            return False
        symbol_positions = self.open_positions_by_symbol.get(symbol, {})
        if len(symbol_positions) >= config.max_positions_per_symbol:
            logger.warning(f"Maximum positions for {symbol} reached: {config.max_positions_per_symbol}")
            return False
        position = Position(position_id, symbol, action, timeframe, entry_price, size, stop_loss, take_profit, metadata)
        self.positions[position_id] = position
        if symbol not in self.open_positions_by_symbol:
            self.open_positions_by_symbol[symbol] = {}
        self.open_positions_by_symbol[symbol][position_id] = position
        position_dict = self._position_to_dict(position)
        self.position_history.append(position_dict)
        if len(self.position_history) > self.max_history:
            self.position_history = self.position_history[-self.max_history:]
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
                    logger.warning(f"Position {position_id} not found")
                    return False
                position = self.positions[position_id]
                position.update_price(current_price)
                if self.db_manager:
                    try:
                        position_dict = self._position_to_dict(position)
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
                result[symbol] = {}
                for position_id, position in positions.items():
                    result[symbol][position_id] = self._position_to_dict(position)
            return result

    async def get_closed_positions(self, limit: int = 100) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            if self.db_manager:
                try:
                    closed_positions = await self.db_manager.get_closed_positions(limit=limit)
                    return {p["position_id"]: p for p in closed_positions}
                except Exception as e:
                    logger.error(f"Error getting closed positions from database: {str(e)}")
            sorted_positions = sorted(
                self.closed_positions.items(),
                key=lambda x: x[1].get("close_time", ""),
                reverse=True
            )
            limited_positions = sorted_positions[:limit]
            return dict(limited_positions)

    async def get_all_positions(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            result = {}
            for position_id, position in self.positions.items():
                result[position_id] = self._position_to_dict(position)
            result.update(self.closed_positions)
            return result

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            open_count = len(self.positions)
            closed_count = len(self.closed_positions)
            total_count = open_count + closed_count
            open_pnl = sum(p.pnl for p in self.positions.values())
            closed_pnl = sum(p.get("pnl", 0) for p in self.closed_positions.values())
            total_pnl = open_pnl + closed_pnl
            if self.closed_positions:
                winning_positions = [p for p in self.closed_positions.values() if p.get("pnl", 0) > 0]
                losing_positions = [p for p in self.closed_positions.values() if p.get("pnl", 0) < 0]
                win_count = len(winning_positions)
                loss_count = len(losing_positions)
                win_rate = win_count / len(self.closed_positions) * 100 if self.closed_positions else 0
                avg_win = sum(p.get("pnl", 0) for p in winning_positions) / win_count if win_count > 0 else 0
                avg_loss = sum(abs(p.get("pnl", 0)) for p in losing_positions) / loss_count if loss_count > 0 else 0
                profit_factor = sum(p.get("pnl", 0) for p in winning_positions) / abs(sum(p.get("pnl", 0) for p in losing_positions)) if sum(p.get("pnl", 0) for p in losing_positions) != 0 else float('inf')
            else:
                win_count = 0
                loss_count = 0
                win_rate = 0
                avg_win = 0
                avg_loss = 0
                profit_factor = 0
            symbol_counts = {}
            for position in self.positions.values():
                symbol = position.symbol
                if symbol not in symbol_counts:
                    symbol_counts[symbol] = 0
                symbol_counts[symbol] += 1
            timeframe_counts = {}
            for position in self.positions.values():
                timeframe = position.timeframe
                if timeframe not in timeframe_counts:
                    timeframe_counts[timeframe] = 0
                timeframe_counts[timeframe] += 1
            return {
                "open_positions": open_count,
                "closed_positions": closed_count,
                "total_positions": total_count,
                "open_pnl": open_pnl,
                "closed_pnl": closed_pnl,
                "total_pnl": total_pnl,
                "win_count": win_count,
                "loss_count": loss_count,
                "win_rate": win_rate,
                "avg_win": avg_win,
                "avg_loss": avg_loss,
                "profit_factor": profit_factor,
                "symbol_counts": symbol_counts,
                "timeframe_counts": timeframe_counts,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

    def _position_to_dict(self, position: Position) -> Dict[str, Any]:
        return {
            "position_id": position.position_id,
            "symbol": position.symbol,
            "action": position.action,
            "timeframe": position.timeframe,
            "entry_price": position.entry_price,
            "size": position.size,
            "stop_loss": None,
            "take_profit": position.take_profit,
            "open_time": position.open_time.isoformat(),
            "close_time": position.close_time.isoformat() if position.close_time else None,
            "exit_price": position.exit_price,
            "current_price": position.current_price,
            "pnl": position.pnl,
            "pnl_percentage": position.pnl_percentage,
            "status": position.status,
            "last_update": position.last_update.isoformat(),
            "metadata": position.metadata,
            "exit_reason": position.exit_reason
        }

    async def restore_position(self, position_id: str, position_data: Dict[str, Any]) -> bool:
        async with self._lock:
            if position_id in self.positions:
                return True
            try:
                symbol = position_data.get("symbol")
                action = position_data.get("action")
                timeframe = position_data.get("timeframe")
                entry_price = position_data.get("entry_price")
                size = position_data.get("size")
                if not all([symbol, action, timeframe, entry_price, size]):
                    logger.error(f"Cannot restore position {position_id}: Missing required fields")
                    return False
                position = Position(
                    position_id=position_id,
                    symbol=symbol,
                    action=action,
                    timeframe=timeframe,
                    entry_price=entry_price,
                    size=size,
                    stop_loss=position_data.get("stop_loss"),
                    take_profit=position_data.get("take_profit"),
                    metadata=position_data.get("metadata", {})
                )
                if "open_time" in position_data and position_data["open_time"]:
                    position.open_time = datetime.fromisoformat(position_data["open_time"].replace("Z", "+00:00"))
                if "current_price" in position_data:
                    position.current_price = position_data["current_price"]
                if "close_time" in position_data and position_data["close_time"]:
                    position.close_time = datetime.fromisoformat(position_data["close_time"].replace("Z", "+00:00"))
                if "exit_price" in position_data:
                    position.exit_price = position_data["exit_price"]
                if "pnl" in position_data:
                    position.pnl = position_data["pnl"]
                if "pnl_percentage" in position_data:
                    position.pnl_percentage = position_data["pnl_percentage"]
                if "status" in position_data:
                    position.status = position_data["status"]
                if "last_update" in position_data and position_data["last_update"]:
                    position.last_update = datetime.fromisoformat(position_data["last_update"].replace("Z", "+00:00"))
                if "exit_reason" in position_data:
                    position.exit_reason = position_data["exit_reason"]
                if position.status == "open":
                    self.positions[position_id] = position
                    if symbol not in self.open_positions_by_symbol:
                        self.open_positions_by_symbol[symbol] = {}
                    self.open_positions_by_symbol[symbol][position_id] = position
                else:
                    self.closed_positions[position_id] = self._position_to_dict(position)
                position_dict = self._position_to_dict(position)
                self.position_history.append(position_dict)
                logger.info(f"Restored position: {position_id} ({symbol} {action})")
                return True
            except Exception as e:
                logger.error(f"Error restoring position {position_id}: {str(e)}")
                return False

    async def get_positions_by_symbol(self, symbol: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        async with self._lock:
            result = []
            if self.db_manager:
                try:
                    db_positions = await self.db_manager.get_positions_by_symbol(symbol, status)
                    return db_positions
                except Exception as e:
                    logger.error(f"Error getting positions for symbol {symbol} from database: {str(e)}")
            if status == "open" or status is None:
                if symbol in self.open_positions_by_symbol:
                    for position in self.open_positions_by_symbol[symbol].values():
                        result.append(self._position_to_dict(position))
            if status == "closed" or status is None:
                for position_data in self.closed_positions.values():
                    if position_data.get("symbol") == symbol:
                        result.append(position_data)
            result.sort(key=lambda x: x.get("open_time", ""), reverse=True)
            return result

    async def close_position(self, position_id: str, exit_price: float, reason: str) -> ClosePositionResult:
        """Close a position"""
        async with self._lock:
            if position_id not in self.positions:
                return ClosePositionResult(success=False, error=f"Position {position_id} not found")
            
            try:
                position = self.positions[position_id]
                position.close(exit_price, reason)
                
                # Move from open to closed
                symbol = position.symbol
                if symbol in self.open_positions_by_symbol:
                    self.open_positions_by_symbol[symbol].pop(position_id, None)
                    if not self.open_positions_by_symbol[symbol]:
                        del self.open_positions_by_symbol[symbol]
                
                # Remove from active positions
                del self.positions[position_id]
                
                # Add to closed positions
                position_dict = self._position_to_dict(position)
                self.closed_positions[position_id] = position_dict
                
                # Update history
                for i, hist_pos in enumerate(self.position_history):
                    if hist_pos.get("position_id") == position_id:
                        self.position_history[i] = position_dict
                        break
                
                # Update database
                if self.db_manager:
                    try:
                        await self.db_manager.update_position(position_id, position_dict)
                    except Exception as e:
                        logger.error(f"Error updating closed position in database: {str(e)}")
                
                logger.info(f"Closed position: {position_id} at {exit_price} (reason: {reason})")
                return ClosePositionResult(success=True, position_data=position_dict)
                
            except Exception as e:
                logger.error(f"Error closing position {position_id}: {str(e)}")
                return ClosePositionResult(success=False, error=str(e))

    async def close_partial_position(self, position_id: str, exit_price: float, 
                                   units_to_close: float, reason: str) -> ClosePositionResult:
        """Close partial position"""
        async with self._lock:
            if position_id not in self.positions:
                return ClosePositionResult(success=False, error=f"Position {position_id} not found")
            
            try:
                position = self.positions[position_id]
                
                if units_to_close >= position.size:
                    # Close entire position
                    return await self.close_position(position_id, exit_price, reason)
                
                # Calculate partial close
                original_size = position.size
                remaining_size = original_size - units_to_close
                
                # Update position size
                position.size = remaining_size
                position.last_update = datetime.now(timezone.utc)
                
                # Calculate partial PnL
                if position.action == "BUY":
                    partial_pnl = (exit_price - position.entry_price) * units_to_close
                else:
                    partial_pnl = (position.entry_price - exit_price) * units_to_close
                
                # Update position data
                position_dict = self._position_to_dict(position)
                
                # Update database
                if self.db_manager:
                    try:
                        await self.db_manager.update_position(position_id, position_dict)
                    except Exception as e:
                        logger.error(f"Error updating partial close in database: {str(e)}")
                
                logger.info(f"Partial close: {position_id}, closed {units_to_close} units at {exit_price}")
                
                return ClosePositionResult(
                    success=True, 
                    position_data={
                        "position_id": position_id,
                        "units_closed": units_to_close,
                        "remaining_units": remaining_size,
                        "partial_pnl": partial_pnl,
                        "exit_price": exit_price,
                        "reason": reason
                    }
                )
                
            except Exception as e:
                logger.error(f"Error in partial close for {position_id}: {str(e)}")
                return ClosePositionResult(success=False, error=str(e))

    async def get_price_history(self, symbol: str, timeframe: str, count: int = 6) -> List[Dict[str, Any]]:
        """Get price history for symbol (placeholder implementation)"""
        try:
            # This is a placeholder - implement actual price history fetching
            # For now, return empty list to avoid breaking the system
            return []
        except Exception as e:
            logger.error(f"Error getting price history for {symbol}: {e}")
            return []

    async def purge_old_closed_positions(self, max_age_days: int = 30):
        """Purge old closed positions"""
        try:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=max_age_days)
            
            positions_to_remove = []
            for position_id, position_data in self.closed_positions.items():
                close_time_str = position_data.get("close_time")
                if close_time_str:
                    try:
                        close_time = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
                        if close_time < cutoff_date:
                            positions_to_remove.append(position_id)
                    except Exception:
                        continue
            
            for position_id in positions_to_remove:
                del self.closed_positions[position_id]
                
            logger.info(f"Purged {len(positions_to_remove)} old closed positions")
            
        except Exception as e:
            logger.error(f"Error purging old positions: {e}")

    async def sync_with_database(self):
        """Sync positions with database"""
        try:
            if not self.db_manager:
                return
                
            # Get all positions from database
            db_positions = await self.db_manager.get_open_positions()
            
            # Sync open positions
            for pos_data in db_positions:
                position_id = pos_data.get("position_id")
                if position_id and position_id not in self.positions:
                    await self.restore_position(position_id, pos_data)
                    
            logger.info("Database sync completed")
            
        except Exception as e:
            logger.error(f"Error syncing with database: {e}")

    async def clean_up_duplicate_positions(self):
        """Clean up any duplicate positions"""
        try:
            # Group positions by symbol and action
            symbol_groups = {}
            
            for position_id, position in self.positions.items():
                key = f"{position.symbol}_{position.action}"
                if key not in symbol_groups:
                    symbol_groups[key] = []
                symbol_groups[key].append((position_id, position))
            
            # Check for duplicates and keep the newest
            duplicates_removed = 0
            for key, positions in symbol_groups.items():
                if len(positions) > 1:
                    # Sort by open time, keep the newest
                    positions.sort(key=lambda x: x[1].open_time, reverse=True)
                    
                    # Remove older duplicates
                    for position_id, position in positions[1:]:
                        await self.close_position(
                            position_id, 
                            position.current_price, 
                            "duplicate_cleanup"
                        )
                        duplicates_removed += 1
            
            if duplicates_removed > 0:
                logger.info(f"Cleaned up {duplicates_removed} duplicate positions")
                
        except Exception as e:
            logger.error(f"Error cleaning up duplicates: {e}")
    
    async def clear_position(self, symbol: str) -> bool:
        """
        Remove all open positions for the given symbol from tracking,
        move them to closed_positions, and log the operation.
        Returns True if any positions were cleared, False if none found.
        """
        async with self._lock:
            if symbol not in self.open_positions_by_symbol or not self.open_positions_by_symbol[symbol]:
                logger.warning(f"[PositionTracker] No open positions to clear for symbol: {symbol}")
                return False
    
            cleared_any = False
            positions_to_clear = list(self.open_positions_by_symbol[symbol].values())
    
            for position in positions_to_clear:
                position_id = getattr(position, "position_id", None)
                if not position_id:
                    continue
    
                # Mark as closed, update time
                position.status = "closed"
                position.close_time = datetime.now(timezone.utc)
                # If your Position has an exit_reason, use it; otherwise use generic
                if hasattr(position, "exit_reason"):
                    position.exit_reason = "force_clear"
                # Remove from master dict of open positions
                if position_id in self.positions:
                    del self.positions[position_id]
                # Move to closed_positions
                self.closed_positions[position_id] = self._position_to_dict(position)
                cleared_any = True
    
                logger.info(f"[PositionTracker] Cleared position {position_id} for {symbol} (forced close).")
    
            # Finally, remove from open_positions_by_symbol
            self.open_positions_by_symbol[symbol] = {}
            return cleared_any

    async def get_position_object(self, position_id: str) -> Optional[Position]:
        """Get the actual Position object by position_id"""
        async with self._lock:
            return self.positions.get(position_id)
    
    async def update_weekend_status_for_all_positions(self):
        """Update weekend status for all open positions - call periodically"""
        async with self._lock:
            for position_id, position in self.positions.items():
                if position.status == "open":
                    position.update_weekend_status()
    
    async def get_weekend_positions_summary(self) -> Dict[str, Any]:
        """Get summary of weekend positions for monitoring"""
        weekend_positions = []
        total_weekend_age = 0.0
        
        async with self._lock:
            for position_id, position in self.positions.items():
                if position.status == "open" and position.is_weekend_position():
                    position.update_weekend_status()
                    weekend_age = position.get_weekend_age_hours()
                    weekend_positions.append({
                        'position_id': position_id,
                        'symbol': position.symbol,
                        'action': position.action,
                        'weekend_age_hours': weekend_age,
                        'weekend_start_time': position.weekend_start_time.isoformat() if position.weekend_start_time else None
                    })
                    total_weekend_age += weekend_age
        
        return {
            'weekend_positions_count': len(weekend_positions),
            'total_weekend_age_hours': total_weekend_age,
            'average_weekend_age_hours': total_weekend_age / len(weekend_positions) if weekend_positions else 0.0,
            'positions': weekend_positions
        }

