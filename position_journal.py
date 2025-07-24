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
    
        # Weekend position tracking
        self.weekend_start_time = None  # When position first encountered weekend
        self.was_open_during_weekend = False  # Flag to track if position was open during weekend
        self.weekend_age_hours = 0.0  # Total hours the position has been open during weekends
    
    def _is_weekend(self, dt: datetime) -> bool:
        """Check if given datetime is during weekend (Friday 17:00 EST to Sunday 17:05 EST)"""
        from datetime import time
        import pytz
        
        # Convert to EST
        est = pytz.timezone('US/Eastern')
        dt_est = dt.astimezone(est)
        weekday = dt_est.weekday()  # Monday=0, Sunday=6
        time_est = dt_est.time()
        
        # Friday after 17:00 EST
        if weekday == 4 and time_est >= time(17, 0):  # Friday
            return True
            
        # Saturday all day
        if weekday == 5:  # Saturday
            return True
            
        # Sunday before 17:05 EST
        if weekday == 6 and time_est < time(17, 5):  # Sunday
            return True
            
        return False
    
    def update_weekend_status(self):
        """Update weekend tracking status - call this periodically"""
        current_time = datetime.now(timezone.utc)
        
        # Check if we're currently in weekend
        if self._is_weekend(current_time):
            # If this is the first time we've detected weekend for this position
            if not self.was_open_during_weekend:
                self.weekend_start_time = current_time
                self.was_open_during_weekend = True
                logger.info(f"Position {self.position_id} ({self.symbol}) now tracking weekend age - started at {current_time}")
            
            # Update weekend age if we have a start time
            if self.weekend_start_time:
                weekend_duration = (current_time - self.weekend_start_time).total_seconds() / 3600
                self.weekend_age_hours = weekend_duration
        else:
            # We're not in weekend, but if we were tracking weekend age, keep the total
            if self.was_open_during_weekend and self.weekend_start_time:
                # Position survived the weekend, keep the total weekend age but stop accumulating
                pass
    
    def get_weekend_age_hours(self) -> float:
        """Get the total hours this position has been open during weekends"""
        if self.was_open_during_weekend:
            self.update_weekend_status()  # Update before returning
            return self.weekend_age_hours
        return 0.0
    
    def is_weekend_position(self) -> bool:
        """Check if this position was left open during a weekend"""
        return self.was_open_during_weekend
    
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
    
    async def record_partial_exit(self,
                                position_id: str,
                                exit_price: float,
                                units_closed: int,
                                exit_reason: str,
                                pnl: float,
                                execution_time: float = 0.0,
                                slippage: float = 0.0,
                                market_regime: str = "unknown",
                                volatility_state: str = "normal",
                                metadata: Optional[Dict[str, Any]] = None):
        """
        Record a partial exit (for tiered TP levels)
        """
        try:
            timestamp = datetime.now(timezone.utc)
            
            # Create partial exit record
            partial_exit_record = {
                "timestamp": timestamp.isoformat(),
                "position_id": position_id,
                "exit_type": "partial",
                "exit_price": float(exit_price),
                "units_closed": int(units_closed),
                "exit_reason": str(exit_reason),
                "pnl": float(pnl),
                "execution_time": float(execution_time),
                "slippage": float(slippage),
                "market_regime": str(market_regime),
                "volatility_state": str(volatility_state),
                "metadata": metadata or {}
            }
            
            # Store in memory
            if position_id not in self.entries:
                self.entries[position_id] = {
                    "position_id": position_id,
                    "symbol": "unknown", # Placeholder, needs to be added to entry
                    "timeframe": "unknown", # Placeholder, needs to be added to entry
                    "strategy": "unknown", # Placeholder, needs to be added to entry
                    "journal": [],
                    "position_status": "open",
                    "created_at": datetime.now(timezone.utc).isoformat()
                }
                self.statistics["position_count"] += 1
            
            self.entries[position_id]["journal"].append(partial_exit_record)
            
            # Log the partial exit
            logger.info(f"📊 Partial exit recorded for {position_id}: {units_closed} units at {exit_price}, P&L: ${pnl:.2f}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error recording partial exit for {position_id}: {e}")
            return False

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
    
    async def add_note(self,
                     position_id: str,
                     note: str,
                     note_type: str = "general",
                     metadata: Optional[Dict[str, Any]] = None):
        """Add a note or observation to a position's journal."""
        async with self._lock:
            if position_id not in self.entries:
                return
            note_record = {
                "type": "note",
                "note_type": note_type,
                "content": note,
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
        """Record an adjustment to a position (e.g., stop loss move)."""
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
        """Retrieve the full journal for a single position"""
        async with self._lock:
            return self.entries.get(position_id)
            
    async def get_all_entries(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Retrieve all journal entries with pagination"""
        async with self._lock:
            all_journals = list(self.entries.values())
            return sorted(all_journals, key=lambda x: x['created_at'], reverse=True)[offset:offset+limit]
            
    async def get_statistics(self) -> Dict[str, Any]:
        """Get performance statistics from the journal"""
        async with self._lock:
            total_pnl = sum(
                entry['pnl']
                for position in self.entries.values()
                for entry in position['journal']
                if entry['type'] == 'exit'
            )
            total_wins = sum(1 for pos in self.entries.values() for entry in pos['journal'] if entry['type'] == 'exit' and entry['pnl'] > 0)
            total_losses = sum(1 for pos in self.entries.values() for entry in pos['journal'] if entry['type'] == 'exit' and entry['pnl'] <= 0)
            
            # Calculate win rate
            total_trades = total_wins + total_losses
            win_rate = (total_wins / total_trades) * 100 if total_trades > 0 else 0
            
            # Calculate average win and loss
            avg_win = sum(e['pnl'] for p in self.entries.values() for e in p['journal'] if e['type'] == 'exit' and e['pnl'] > 0) / total_wins if total_wins > 0 else 0
            avg_loss = sum(e['pnl'] for p in self.entries.values() for e in p['journal'] if e['type'] == 'exit' and e['pnl'] <= 0) / total_losses if total_losses > 0 else 0
            
            # Calculate profit factor and risk/reward
            total_profit = sum(e['pnl'] for p in self.entries.values() for e in p['journal'] if e['type'] == 'exit' and e['pnl'] > 0)
            total_loss = abs(sum(e['pnl'] for p in self.entries.values() for e in p['journal'] if e['type'] == 'exit' and e['pnl'] <= 0))
            profit_factor = total_profit / total_loss if total_loss > 0 else 0
            risk_reward_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0
            
            return {
                "total_trades": total_trades,
                "total_wins": total_wins,
                "total_losses": total_losses,
                "win_rate_percent": win_rate,
                "total_pnl": total_pnl,
                "profit_factor": profit_factor,
                "average_win": avg_win,
                "average_loss": avg_loss,
                "risk_reward_ratio": risk_reward_ratio
            }
            
    async def analyze_performance_by_factor(self, factor: str) -> Dict[str, Any]:
        """Analyze performance grouped by a given factor (e.g., 'strategy', 'timeframe', 'market_regime')"""
        async with self._lock:
            performance_by_factor = {}
            
            for position in self.entries.values():
                entry_data = next((e for e in position['journal'] if e['type'] == 'entry'), None)
                exit_data = next((e for e in position['journal'] if e['type'] == 'exit'), None)
                
                if not entry_data or not exit_data:
                    continue
                    
                factor_value = entry_data.get(factor)
                if not factor_value:
                    continue
                    
                if factor_value not in performance_by_factor:
                    performance_by_factor[factor_value] = {
                        "trades": 0, "wins": 0, "losses": 0, "total_pnl": 0.0,
                        "total_profit": 0.0, "total_loss": 0.0
                    }
                    
                pnl = exit_data['pnl']
                stats = performance_by_factor[factor_value]
                stats["trades"] += 1
                stats["total_pnl"] += pnl
                if pnl > 0:
                    stats["wins"] += 1
                    stats["total_profit"] += pnl
                else:
                    stats["losses"] += 1
                    stats["total_loss"] += abs(pnl)
                    
            # Calculate final metrics for each factor
            for factor_value, stats in performance_by_factor.items():
                stats["win_rate"] = (stats["wins"] / stats["trades"]) * 100 if stats["trades"] > 0 else 0
                stats["profit_factor"] = stats["total_profit"] / stats["total_loss"] if stats["total_loss"] > 0 else 0
            
            return performance_by_factor
            
    async def get_execution_quality_report(self) -> dict:
        """Generates a report on trade execution quality (slippage and timing)."""
        async with self._lock:
            all_slippages = []
            all_execution_times = []
            for position in self.entries.values():
                for record in position['journal']:
                    if record.get('slippage') is not None:
                        all_slippages.append(record['slippage'])
                    if record.get('execution_time') is not None:
                        all_execution_times.append(record['execution_time'])
            
            if not all_slippages:
                return {"average_slippage": 0, "average_execution_time_ms": 0, "total_trades_analyzed": 0}
            
            avg_slippage = sum(all_slippages) / len(all_slippages)
            avg_exec_time = sum(all_execution_times) / len(all_execution_times) * 1000  # in ms
            
            return {
                "average_slippage": avg_slippage,
                "average_execution_time_ms": avg_exec_time,
                "total_trades_analyzed": len(all_slippages)
            }

# Singleton instance
position_journal = PositionJournal()
