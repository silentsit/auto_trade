from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
import asyncio
import logging

logger = logging.getLogger("position_journal")

class Position:
    """Represents a trading position with full lifecycle management"""
    def __init__(self, 
                position_id: str,
                symbol: str, 
                action: str,
                timeframe: str,
                entry_price: float,
                size: float,
                stop_loss: Optional[float] = None,  
                take_profit: Optional[float] = None,
                metadata: Optional[Dict[str, any]] = None):
        """Initialize a position"""
        self.position_id = position_id
        self.symbol = symbol
        self.action = action.upper()
        self.timeframe = timeframe
        self.entry_price = float(entry_price)
        self.size = float(size)
        self.stop_loss = None
        self.take_profit = float(take_profit) if take_profit is not None else None
        self.open_time = datetime.now(timezone.utc)
        self.close_time = None
        self.exit_price = None
        self.pnl = 0.0
        self.pnl_percentage = 0.0
        self.status = "open"
        self.last_update = self.open_time
        self.current_price = self.entry_price
        self.metadata = metadata or {}
        self.exit_reason = None
    
    def update_price(self, current_price: float):
        """Update current price and calculate P&L"""
        self.current_price = float(current_price)
        self.last_update = datetime.now(timezone.utc)
        # Calculate unrealized P&L
        if self.action == "BUY":
            self.pnl = (self.current_price - self.entry_price) * self.size
        else:  # SELL
            self.pnl = (self.entry_price - self.current_price) * self.size
        # Calculate P&L percentage
        if self.entry_price > 0:
            if self.action == "BUY":
                self.pnl_percentage = (self.current_price / self.entry_price - 1) * 100
            else:  # SELL
                self.pnl_percentage = (1 - self.current_price / self.entry_price) * 100
    
    def close(self, exit_price: float, exit_reason: str = "manual"):
        """Close the position"""
        self.exit_price = float(exit_price)
        self.close_time = datetime.now(timezone.utc)
        self.status = "closed"
        self.exit_reason = exit_reason
        # Calculate realized P&L
        if self.action == "BUY":
            self.pnl = (self.exit_price - self.entry_price) * self.size
        else:  # SELL
            self.pnl = (self.entry_price - self.exit_price) * self.size
        # Calculate P&L percentage
        if self.entry_price > 0:
            if self.action == "BUY":
                self.pnl_percentage = (self.exit_price / self.entry_price - 1) * 100
            else:  # SELL
                self.pnl_percentage = (1 - self.exit_price / self.entry_price) * 100
        # Update last update time
        self.last_update = self.close_time
    
    def update_stop_loss(self, new_stop_loss: float):
        """Update stop loss level (disabled - does nothing)"""
        self.last_update = datetime.now(timezone.utc)
    
    def update_take_profit(self, new_take_profit: float):
        """Update take profit level"""
        self.take_profit = float(new_take_profit)
        self.last_update = datetime.now(timezone.utc)
    
    def update_metadata(self, metadata: Dict[str, any]):
        """Update position metadata"""
        self.metadata.update(metadata)
        self.last_update = datetime.now(timezone.utc)

class PositionJournal:
    """
    Keeps a detailed journal of all trading activity with performance metrics,
    annotations, and post-trade analysis.
    """
    def __init__(self):
        """Initialize position journal"""
        self.entries = {}  # position_id -> journal entries
        self.statistics = {
            "total_entries": 0,
            "total_exits": 0,
            "position_count": 0,
            "win_count": 0,
            "loss_count": 0
        }
        self._lock = asyncio.Lock()
    
    async def record_entry(self,
                         position_id: str,
                         symbol: str,
                         action: str,
                         timeframe: str,
                         entry_price: float,
                         size: float,
                         strategy: str,
                         execution_time: float = 0.0,
                         slippage: float = 0.0,  # REQUIRED: pass actual slippage from execution
                         stop_loss: Optional[float] = None,
                         take_profit: Optional[float] = None,
                         market_regime: str = "unknown",
                         volatility_state: str = "normal",
                         metadata: Optional[Dict[str, Any]] = None):
        """Record a position entry in the journal. Slippage is required for execution quality reporting."""
        async with self._lock:
            if position_id not in self.entries:
                self.entries[position_id] = {
                    "position_id": position_id,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "strategy": strategy,
                    "journal": [],
                    "position_status": "open",
                    "created_at": datetime.now(timezone.utc).isoformat()
                }
                self.statistics["position_count"] += 1
            entry_record = {
                "type": "entry",
                "action": action,
                "price": entry_price,
                "size": size,
                "stop_loss": None,
                "take_profit": take_profit,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "execution_time": execution_time,
                "slippage": slippage,
                "market_regime": market_regime,
                "volatility_state": volatility_state,
                "metadata": metadata or {}
            }
            self.entries[position_id]["journal"].append(entry_record)
            self.statistics["total_entries"] += 1
        logger.info(f"[Slippage] Entry for {symbol} {position_id}: slippage={slippage:.5f}")
        # Log execution quality after entry
        report = await self.get_execution_quality_report()
        logger.info(f"[Execution Quality] After entry: {report}")
    
    async def record_exit(self,
                        position_id: str,
                        exit_price: float,
                        exit_reason: str,
                        pnl: float,
                        execution_time: float = 0.0,
                        slippage: float = 0.0,
                        market_regime: str = "unknown",
                        volatility_state: str = "normal",
                        metadata: Optional[Dict[str, Any]] = None):
        """Record a position exit in the journal"""
        async with self._lock:
            if position_id not in self.entries:
                return
            exit_record = {
                "type": "exit",
                "price": exit_price,
                "reason": exit_reason,
                "pnl": pnl,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "execution_time": execution_time,
                "slippage": slippage,
                "market_regime": market_regime,
                "volatility_state": volatility_state,
                "metadata": metadata or {}
            }
            self.entries[position_id]["journal"].append(exit_record)
            self.entries[position_id]["position_status"] = "closed"
            self.statistics["total_exits"] += 1
            if pnl > 0:
                self.statistics["win_count"] += 1
            else:
                self.statistics["loss_count"] += 1
        # Log execution quality after exit
        report = await self.get_execution_quality_report()
        logger.info(f"[Execution Quality] After exit: {report}")
    
    async def add_note(self,
                     position_id: str,
                     note: str,
                     note_type: str = "general",
                     metadata: Optional[Dict[str, Any]] = None):
        """Add a note to a position journal"""
        async with self._lock:
            if position_id not in self.entries:
                return
            note_record = {
                "type": "note",
                "note_type": note_type,
                "text": note,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "metadata": metadata or {}
            }
            self.entries[position_id]["journal"].append(note_record)
    
    async def record_adjustment(self,
                              position_id: str,
                              adjustment_type: str,
                              old_value: Any,
                              new_value: Any,
                              reason: str,
                              metadata: Optional[Dict[str, Any]] = None):
        """Record a position adjustment (stop loss, take profit, etc.)"""
        async with self._lock:
            if position_id not in self.entries:
                return
            adjustment_record = {
                "type": "adjustment",
                "adjustment_type": adjustment_type,
                "old_value": old_value,
                "new_value": new_value,
                "reason": reason,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "metadata": metadata or {}
            }
            self.entries[position_id]["journal"].append(adjustment_record)
    
    async def get_position_journal(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get the journal for a position"""
        async with self._lock:
            if position_id not in self.entries:
                return None
            return self.entries[position_id]
    
    async def get_all_entries(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Get all journal entries, sorted by creation time"""
        async with self._lock:
            sorted_entries = sorted(
                self.entries.values(),
                key=lambda x: x.get("created_at", ""),
                reverse=True
            )
            return sorted_entries[offset:offset+limit]
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get journal statistics"""
        async with self._lock:
            win_rate = 0.0
            if self.statistics["win_count"] + self.statistics["loss_count"] > 0:
                win_rate = (self.statistics["win_count"] /
                          (self.statistics["win_count"] + self.statistics["loss_count"])) * 100
            return {
                "total_positions": self.statistics["position_count"],
                "open_positions": self.statistics["position_count"] - (self.statistics["win_count"] + self.statistics["loss_count"]),
                "closed_positions": self.statistics["win_count"] + self.statistics["loss_count"],
                "winning_positions": self.statistics["win_count"],
                "losing_positions": self.statistics["loss_count"],
                "win_rate": win_rate,
                "total_entries": self.statistics["total_entries"],
                "total_exits": self.statistics["total_exits"]
            }
    
    async def analyze_performance_by_factor(self, factor: str) -> Dict[str, Any]:
        """Analyze performance grouped by a specific factor (strategy, market_regime, etc.)"""
        async with self._lock:
            closed_positions = [p for p in self.entries.values() 
                              if p["position_status"] == "closed"]
            if not closed_positions:
                return {
                    "status": "no_data",
                    "message": "No closed positions to analyze"
                }
            factor_performance = {}
            for position in closed_positions:
                entry_record = next((r for r in position["journal"] if r["type"] == "entry"), None)
                exit_record = next((r for r in position["journal"] if r["type"] == "exit"), None)
                if not entry_record or not exit_record:
                    continue
                if factor == "strategy":
                    factor_value = position.get("strategy", "unknown")
                elif factor == "market_regime":
                    factor_value = entry_record.get("market_regime", "unknown")
                elif factor == "volatility_state":
                    factor_value = entry_record.get("volatility_state", "normal")
                elif factor == "exit_reason":
                    factor_value = exit_record.get("reason", "unknown")
                elif factor == "symbol":
                    factor_value = position.get("symbol", "unknown")
                elif factor == "timeframe":
                    factor_value = position.get("timeframe", "unknown")
                else:
                    factor_value = entry_record.get("metadata", {}).get(factor, "unknown")
                if factor_value not in factor_performance:
                    factor_performance[factor_value] = {
                        "count": 0,
                        "wins": 0,
                        "losses": 0,
                        "total_pnl": 0.0,
                        "avg_pnl": 0.0,
                        "win_rate": 0.0
                    }
                stats = factor_performance[factor_value]
                stats["count"] += 1
                pnl = exit_record.get("pnl", 0.0)
                stats["total_pnl"] += pnl
                if pnl > 0:
                    stats["wins"] += 1
                else:
                    stats["losses"] += 1
                stats["avg_pnl"] = stats["total_pnl"] / stats["count"]
                if stats["wins"] + stats["losses"] > 0:
                    stats["win_rate"] = (stats["wins"] / (stats["wins"] + stats["losses"])) * 100
            return {
                "status": "success",
                "factor": factor,
                "performance": factor_performance
            }

    async def get_execution_quality_report(self) -> dict:
        """
        Compute execution quality metrics:
        - Latency: average time between signal and fill (entry execution_time)
        - Slippage: average difference between expected and actual fill price (entry slippage)
        - Success rate: percentage of winning trades
        Returns a summary dictionary.
        """
        async with self._lock:
            total_entries = 0
            total_exits = 0
            total_latency = 0.0
            total_slippage = 0.0
            win_count = 0
            loss_count = 0
            for entry in self.entries.values():
                entry_records = entry.get("journal", [])
                entry_exec_times = [r["execution_time"] for r in entry_records if r["type"] == "entry" and "execution_time" in r]
                entry_slippages = [r["slippage"] for r in entry_records if r["type"] == "entry" and "slippage" in r]
                exit_records = [r for r in entry_records if r["type"] == "exit"]
                for exit_rec in exit_records:
                    pnl = exit_rec.get("pnl", 0.0)
                    if pnl > 0:
                        win_count += 1
                    else:
                        loss_count += 1
                if entry_exec_times:
                    total_latency += sum(entry_exec_times)
                    total_entries += len(entry_exec_times)
                if entry_slippages:
                    total_slippage += sum(entry_slippages)
            total_exits = win_count + loss_count
            avg_latency = total_latency / total_entries if total_entries else 0.0
            avg_slippage = total_slippage / total_entries if total_entries else 0.0
            success_rate = (win_count / total_exits * 100) if total_exits else 0.0
            return {
                "average_latency": avg_latency,
                "average_slippage": avg_slippage,
                "success_rate": success_rate,
                "total_trades": total_exits,
                "win_count": win_count,
                "loss_count": loss_count
            }

# Create a module-level instance for global use
position_journal = PositionJournal()
