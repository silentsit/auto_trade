from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple
import asyncio
import logging
import json
import uuid
from dataclasses import dataclass, asdict
from enum import Enum
import numpy as np

logger = logging.getLogger("position_journal")

class PositionStatus(Enum):
    OPEN = "open"
    CLOSED = "closed"
    PARTIALLY_CLOSED = "partially_closed"
    PENDING = "pending"
    CANCELLED = "cancelled"

class ExitReason(Enum):
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"
    MANUAL = "manual"
    TIMEOUT = "timeout"
    RISK_LIMIT = "risk_limit"
    WEEKEND_CLOSE = "weekend_close"
    SYSTEM_ERROR = "system_error"
    PROFIT_OVERRIDE = "profit_override"

@dataclass
class PositionMetrics:
    """Advanced position metrics for institutional tracking"""
    max_favorable_excursion: float = 0.0
    max_adverse_excursion: float = 0.0
    time_in_trade: float = 0.0
    volatility_exposure: float = 0.0
    correlation_risk: float = 0.0
    drawdown_contribution: float = 0.0
    sharpe_ratio: float = 0.0
    calmar_ratio: float = 0.0
    win_probability: float = 0.0
    expected_value: float = 0.0

class Position:
    """Represents a trading position with full lifecycle management and institutional-grade tracking"""
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
        """Initialize a position with advanced tracking"""
        self.position_id = position_id
        self.symbol = symbol
        self.action = action.upper()
        self.timeframe = timeframe
        self.entry_price = float(entry_price)
        self.size = float(size)
        self.stop_loss = float(stop_loss) if stop_loss is not None else None
        self.take_profit = float(take_profit) if take_profit is not None else None
        self.open_time = datetime.now(timezone.utc)
        self.close_time = None
        self.exit_price = None
        self.pnl = 0.0
        self.pnl_percentage = 0.0
        self.status = PositionStatus.OPEN
        self.last_update = self.open_time
        self.current_price = self.entry_price
        self.metadata = metadata or {}
        self.exit_reason = None
        
        # Advanced tracking
        self.price_history = [entry_price]
        self.pnl_history = [0.0]
        self.timestamp_history = [self.open_time]
        self.max_favorable_excursion = 0.0
        self.max_adverse_excursion = 0.0
        self.volatility_exposure = 0.0
        self.correlation_risk = 0.0
        self.risk_metrics = PositionMetrics()
        
        # Performance tracking
        self.entry_slippage = 0.0
        self.exit_slippage = 0.0
        self.execution_latency = 0.0
        self.fill_quality_score = 0.0
        
        # Risk management
        self.initial_risk = self._calculate_initial_risk()
        self.current_risk = self.initial_risk
        self.risk_reward_ratio = 0.0
        self.var_95 = 0.0  # Value at Risk 95%
        self.expected_shortfall = 0.0
        
        # Weekend position tracking
        self.weekend_start_time = None  # When position first encountered weekend
        self.was_open_during_weekend = False  # Flag to track if position was open during weekend
        self.weekend_age_hours = 0.0  # Total hours the position has been open during weekends
        
    def _calculate_initial_risk(self) -> float:
        """Calculate initial risk amount"""
        if self.stop_loss:
            risk_distance = abs(self.entry_price - self.stop_loss)
            return risk_distance * self.size
        return 0.0
        
    def calculate_risk_metrics(self) -> Dict[str, float]:
        """Calculate comprehensive risk metrics"""
        if not self.price_history or len(self.price_history) < 2:
            return {}
            
        # Calculate MFE and MAE
        pnl_values = np.array(self.pnl_history)
        self.max_favorable_excursion = float(np.max(pnl_values)) if len(pnl_values) > 0 else 0.0
        self.max_adverse_excursion = float(np.min(pnl_values)) if len(pnl_values) > 0 else 0.0
        
        # Calculate time in trade
        if self.status == PositionStatus.OPEN:
            self.risk_metrics.time_in_trade = (datetime.now(timezone.utc) - self.open_time).total_seconds() / 3600
        else:
            self.risk_metrics.time_in_trade = (self.close_time - self.open_time).total_seconds() / 3600
            
        # Calculate volatility exposure (simplified)
        if len(self.price_history) > 1:
            price_changes = np.diff(self.price_history)
            self.volatility_exposure = float(np.std(price_changes)) if len(price_changes) > 0 else 0.0
            
        # Calculate risk-reward ratio
        if self.initial_risk > 0:
            self.risk_reward_ratio = abs(self.pnl) / self.initial_risk
            
        return {
            "max_favorable_excursion": self.max_favorable_excursion,
            "max_adverse_excursion": self.max_adverse_excursion,
            "time_in_trade_hours": self.risk_metrics.time_in_trade,
            "volatility_exposure": self.volatility_exposure,
            "risk_reward_ratio": self.risk_reward_ratio,
            "current_risk": self.current_risk,
            "initial_risk": self.initial_risk
        }
    
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
    
    def update_price(self, current_price: float, update_metrics: bool = True):
        """Update current price and calculate P&L with advanced tracking"""
        self.current_price = float(current_price)
        self.last_update = datetime.now(timezone.utc)
        
        # Add to price history
        self.price_history.append(current_price)
        self.timestamp_history.append(self.last_update)
        
        # Calculate unrealized P&L
        if self.action == "BUY":
            self.pnl = (self.current_price - self.entry_price) * self.size
        else:  # SELL
            self.pnl = (self.entry_price - self.current_price) * self.size
            
        # Add to PnL history
        self.pnl_history.append(self.pnl)
        
        # Calculate P&L percentage
        if self.entry_price > 0:
            if self.action == "BUY":
                self.pnl_percentage = (self.current_price / self.entry_price - 1) * 100
            else:  # SELL
                self.pnl_percentage = (1 - self.current_price / self.entry_price) * 100
                
        # Update risk metrics if requested
        if update_metrics:
            self.calculate_risk_metrics()
            
        # Update current risk
        if self.stop_loss:
            if self.action == "BUY":
                self.current_risk = max(0, (self.stop_loss - self.current_price) * self.size)
            else:  # SELL
                self.current_risk = max(0, (self.current_price - self.stop_loss) * self.size)
    
    def close(self, exit_price: float, exit_reason: str = "manual", exit_slippage: float = 0.0):
        """Close the position with comprehensive tracking"""
        self.exit_price = float(exit_price)
        self.close_time = datetime.now(timezone.utc)
        self.status = PositionStatus.CLOSED
        self.exit_reason = exit_reason
        self.exit_slippage = exit_slippage
        
        # Add final price to history
        self.price_history.append(exit_price)
        self.timestamp_history.append(self.close_time)
        
        # Calculate realized P&L
        if self.action == "BUY":
            self.pnl = (self.exit_price - self.entry_price) * self.size
        else:  # SELL
            self.pnl = (self.entry_price - self.exit_price) * self.size
            
        # Add final PnL to history
        self.pnl_history.append(self.pnl)
        
        # Calculate P&L percentage
        if self.entry_price > 0:
            if self.action == "BUY":
                self.pnl_percentage = (self.exit_price / self.entry_price - 1) * 100
            else:  # SELL
                self.pnl_percentage = (1 - self.exit_price / self.entry_price) * 100
                
        # Calculate final risk metrics
        self.calculate_risk_metrics()
        
        # Calculate fill quality score
        self.fill_quality_score = self._calculate_fill_quality()
        
        # Update last update time
        self.last_update = self.close_time
        
    def _calculate_fill_quality(self) -> float:
        """Calculate fill quality score based on slippage and execution"""
        # Base score
        score = 1.0
        
        # Penalty for slippage
        if self.entry_slippage > 0:
            slippage_penalty = min(0.5, self.entry_slippage / self.entry_price * 1000)  # 0.1% = 0.1 penalty
            score -= slippage_penalty
            
        if self.exit_slippage > 0:
            slippage_penalty = min(0.5, self.exit_slippage / self.exit_price * 1000)
            score -= slippage_penalty
            
        # Bonus for good risk-reward
        if self.risk_reward_ratio > 2.0:
            score += 0.1
        elif self.risk_reward_ratio > 1.5:
            score += 0.05
            
        return max(0.0, min(1.0, score))
        
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        return {
            "position_id": self.position_id,
            "symbol": self.symbol,
            "action": self.action,
            "timeframe": self.timeframe,
            "status": self.status.value,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "current_price": self.current_price,
            "size": self.size,
            "pnl": self.pnl,
            "pnl_percentage": self.pnl_percentage,
            "open_time": self.open_time.isoformat(),
            "close_time": self.close_time.isoformat() if self.close_time else None,
            "time_in_trade_hours": self.risk_metrics.time_in_trade,
            "exit_reason": self.exit_reason,
            "risk_metrics": self.calculate_risk_metrics(),
            "fill_quality_score": self.fill_quality_score,
            "entry_slippage": self.entry_slippage,
            "exit_slippage": self.exit_slippage,
            "execution_latency": self.execution_latency,
            "weekend_exposure": self.weekend_age_hours,
            "metadata": self.metadata
        }
    
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
    Institutional-grade position journal with comprehensive tracking,
    performance analytics, and risk management integration.
    """
    def __init__(self):
        """Initialize position journal with advanced tracking"""
        self.entries = {}  # position_id -> journal entries
        self.positions = {}  # position_id -> Position objects
        self.performance_metrics = {}
        self.risk_analytics = {}
        self.statistics = {
            "total_entries": 0,
            "total_exits": 0,
            "position_count": 0,
            "win_count": 0,
            "loss_count": 0,
            "total_pnl": 0.0,
            "total_volume": 0.0,
            "average_win": 0.0,
            "average_loss": 0.0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
            "consecutive_wins": 0,
            "consecutive_losses": 0,
            "current_streak": 0
        }
        self._lock = asyncio.Lock()
        self._last_metrics_update = datetime.now(timezone.utc)
        
    async def add_position(self, position: Position):
        """Add a position to the journal"""
        async with self._lock:
            self.positions[position.position_id] = position
            self.statistics["position_count"] += 1
            
    async def update_position(self, position_id: str, current_price: float):
        """Update position with current price"""
        async with self._lock:
            if position_id in self.positions:
                self.positions[position_id].update_price(current_price)
                
    async def close_position(self, position_id: str, exit_price: float, exit_reason: str = "manual", exit_slippage: float = 0.0):
        """Close a position and update statistics"""
        async with self._lock:
            if position_id in self.positions:
                position = self.positions[position_id]
                position.close(exit_price, exit_reason, exit_slippage)
                
                # Update statistics
                self.statistics["total_exits"] += 1
                self.statistics["total_pnl"] += position.pnl
                self.statistics["total_volume"] += position.size * position.entry_price
                
                if position.pnl > 0:
                    self.statistics["win_count"] += 1
                    self.statistics["consecutive_wins"] += 1
                    self.statistics["consecutive_losses"] = 0
                    self.statistics["current_streak"] = self.statistics["consecutive_wins"]
                else:
                    self.statistics["loss_count"] += 1
                    self.statistics["consecutive_losses"] += 1
                    self.statistics["consecutive_wins"] = 0
                    self.statistics["current_streak"] = -self.statistics["consecutive_losses"]
                
                # Update performance metrics
                await self._update_performance_metrics()
                
    async def _update_performance_metrics(self):
        """Update comprehensive performance metrics"""
        if self.statistics["position_count"] == 0:
            return
            
        # Calculate win rate
        total_trades = self.statistics["win_count"] + self.statistics["loss_count"]
        if total_trades > 0:
            self.statistics["win_rate"] = self.statistics["win_count"] / total_trades
            
        # Calculate average win/loss
        if self.statistics["win_count"] > 0:
            wins = [pos.pnl for pos in self.positions.values() if pos.pnl > 0 and pos.status == PositionStatus.CLOSED]
            self.statistics["average_win"] = sum(wins) / len(wins) if wins else 0.0
            
        if self.statistics["loss_count"] > 0:
            losses = [pos.pnl for pos in self.positions.values() if pos.pnl < 0 and pos.status == PositionStatus.CLOSED]
            self.statistics["average_loss"] = sum(losses) / len(losses) if losses else 0.0
            
        # Calculate profit factor
        if self.statistics["average_loss"] != 0:
            self.statistics["profit_factor"] = abs(self.statistics["average_win"] / self.statistics["average_loss"])
            
        # Calculate Sharpe ratio (simplified)
        if total_trades > 1:
            pnl_values = [pos.pnl for pos in self.positions.values() if pos.status == PositionStatus.CLOSED]
            if len(pnl_values) > 1:
                mean_pnl = np.mean(pnl_values)
                std_pnl = np.std(pnl_values)
                if std_pnl > 0:
                    self.statistics["sharpe_ratio"] = mean_pnl / std_pnl
                    
        # Calculate max drawdown
        self.statistics["max_drawdown"] = self._calculate_max_drawdown()
        
        self._last_metrics_update = datetime.now(timezone.utc)
        
    def _calculate_max_drawdown(self) -> float:
        """Calculate maximum drawdown"""
        if not self.positions:
            return 0.0
            
        # Get all closed positions sorted by close time
        closed_positions = [pos for pos in self.positions.values() if pos.status == PositionStatus.CLOSED]
        if not closed_positions:
            return 0.0
            
        closed_positions.sort(key=lambda x: x.close_time)
        
        # Calculate running PnL and find max drawdown
        running_pnl = 0.0
        peak_pnl = 0.0
        max_dd = 0.0
        
        for pos in closed_positions:
            running_pnl += pos.pnl
            peak_pnl = max(peak_pnl, running_pnl)
            drawdown = peak_pnl - running_pnl
            max_dd = max(max_dd, drawdown)
            
        return max_dd
        
    async def get_performance_analytics(self) -> Dict[str, Any]:
        """Get comprehensive performance analytics"""
        async with self._lock:
            await self._update_performance_metrics()
            
            # Get position-level analytics
            position_analytics = {}
            for pos_id, position in self.positions.items():
                position_analytics[pos_id] = position.get_performance_summary()
                
            # Get risk analytics
            risk_analytics = self._calculate_risk_analytics()
            
            return {
                "statistics": self.statistics,
                "position_analytics": position_analytics,
                "risk_analytics": risk_analytics,
                "last_updated": self._last_metrics_update.isoformat(),
                "active_positions": len([p for p in self.positions.values() if p.status == PositionStatus.OPEN]),
                "closed_positions": len([p for p in self.positions.values() if p.status == PositionStatus.CLOSED])
            }
            
    def _calculate_risk_analytics(self) -> Dict[str, Any]:
        """Calculate comprehensive risk analytics"""
        closed_positions = [pos for pos in self.positions.values() if pos.status == PositionStatus.CLOSED]
        
        if not closed_positions:
            return {}
            
        # Calculate risk metrics
        total_risk = sum(pos.initial_risk for pos in closed_positions)
        total_pnl = sum(pos.pnl for pos in closed_positions)
        
        # Calculate Value at Risk (VaR) 95%
        pnl_values = [pos.pnl for pos in closed_positions]
        var_95 = np.percentile(pnl_values, 5) if len(pnl_values) > 0 else 0.0
        
        # Calculate Expected Shortfall (ES)
        es_values = [pnl for pnl in pnl_values if pnl <= var_95]
        expected_shortfall = np.mean(es_values) if es_values else 0.0
        
        # Calculate correlation risk
        symbols = list(set(pos.symbol for pos in closed_positions))
        correlation_risk = len(symbols) / len(closed_positions) if closed_positions else 0.0
        
        return {
            "total_risk": total_risk,
            "risk_adjusted_return": total_pnl / total_risk if total_risk > 0 else 0.0,
            "var_95": var_95,
            "expected_shortfall": expected_shortfall,
            "correlation_risk": correlation_risk,
            "diversification_ratio": len(symbols) / len(closed_positions) if closed_positions else 0.0,
            "average_position_size": np.mean([pos.size for pos in closed_positions]) if closed_positions else 0.0,
            "average_time_in_trade": np.mean([pos.risk_metrics.time_in_trade for pos in closed_positions]) if closed_positions else 0.0
        }
    
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
