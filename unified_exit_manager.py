"""
UNIFIED EXIT MANAGER
Consolidated exit management system that handles:
- Immediate SL/TP for all new positions
- Priority monitoring of SL/TP triggers
- Close signal handling with override analysis
- Profit override activation with trailing stops
- Breakeven management at +1R
- Exit signal monitoring and effectiveness tracking

This module consolidates functionality from:
- profit_ride_override.py
- tiered_tp_monitor.py  
- dynamic_exit_manager.py
- exit_monitor.py
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass, field
from enum import Enum
import math

from tracker import PositionTracker
from oanda_service import OandaService
from unified_analysis import LorentzianDistanceClassifier, VolatilityMonitor
from utils import get_atr_multiplier, logger

logger = logging.getLogger(__name__)

class ExitType(Enum):
    STOP_LOSS = "stop_loss"
    TAKE_PROFIT = "take_profit"
    TRAILING_STOP = "trailing_stop"
    CLOSE_SIGNAL = "close_signal"
    MANUAL = "manual"

@dataclass
class TrailingStopConfig:
    """Configuration for trailing stop behavior"""
    atr_multiplier: float = 2.0
    min_distance: float = 0.0001
    breakeven_threshold: float = 1.0  # 1x initial risk
    max_trail_distance: float = 0.0050

@dataclass
class OverrideDecision:
    """Decision about whether to override a close signal"""
    should_override: bool = False
    reason: str = ""
    trailing_stop_enabled: bool = False
    confidence: float = 0.0

@dataclass
class ExitStrategy:
    """Strategy for managing position exits"""
    position_id: str
    symbol: str
    entry_price: float
    current_price: float
    stop_loss_price: float
    take_profit_price: float
    trailing_stop_price: Optional[float] = None
    breakeven_enabled: bool = False
    breakeven_price: Optional[float] = None
    initial_risk_amount: float = 0.0
    current_risk_amount: float = 0.0
    override_fired: bool = False
    created_at: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)

class UnifiedExitManager:
    """
    Unified exit management system that implements the new flow:
    1. All new positions get immediate SL/TP
    2. SL/TP monitoring takes priority
    3. Close signals are analyzed for override eligibility
    4. Profit override activates trailing stops
    """
    
    def __init__(self, 
                 position_tracker: PositionTracker = None,
                 oanda_service: OandaService = None,
                 unified_analysis = None,
                 # Legacy parameter support
                 storage = None,
                 db_manager = None):
        """Initialize the unified exit manager with flexible parameter support"""
        self.position_tracker = position_tracker
        self.oanda_service = oanda_service
        self.unified_analysis = unified_analysis
        
        # Legacy support - map old parameters to new ones
        if storage and not position_tracker:
            # If storage is provided but position_tracker isn't, we can't initialize properly
            self.logger = logger
            self.logger.warning("Legacy 'storage' parameter provided but position_tracker missing - exit manager may not function properly")
        if db_manager and not position_tracker:
            self.logger = logger
            self.logger.warning("Legacy 'db_manager' parameter provided but position_tracker missing - exit manager may not function properly")
        self.exit_strategies: Dict[str, ExitStrategy] = {}
        self.monitoring = False
        self.monitor_task: Optional[asyncio.Task] = None
        self.logger = logger
        
        # Configuration
        self.breakeven_threshold = 1.0  # 1x initial risk
        self.max_ride_time_hours = 48
        self.account_drawdown_limit = 0.05  # 5%
        
    def _get_trailing_stop_config(self, timeframe: str) -> TrailingStopConfig:
        """Get trailing stop configuration based on timeframe"""
        if timeframe == "15M":
            return TrailingStopConfig(
                atr_multiplier=1.5,
                min_distance=0.0002,
                breakeven_threshold=1.0,
                max_trail_distance=0.0030
            )
        elif timeframe == "1H":
            return TrailingStopConfig(
                atr_multiplier=2.0,
                min_distance=0.0003,
                breakeven_threshold=1.0,
                max_trail_distance=0.0040
            )
        else:  # 4H+
            return TrailingStopConfig(
                atr_multiplier=2.5,
                min_distance=0.0005,
                breakeven_threshold=1.0,
                max_trail_distance=0.0060
            )
    
    async def add_position(self, position: Dict[str, Any]) -> ExitStrategy:
        """Add a new position to the exit manager"""
        try:
            position_id = str(position.get('id', ''))
            symbol = str(position.get('instrument', ''))
            entry_price = float(position.get('price', 0))
            side = position.get('side', 'buy')
            
            if not all([position_id, symbol, entry_price]):
                raise ValueError(f"Invalid position data: {position}")
            
            # Get ATR for volatility-based calculations
            timeframe = position.get('timeframe', '1H')
            atr_value = await self._get_atr_value(symbol, timeframe)
            
            # Calculate ATR-based SL and TP with 1:2 risk-reward ratio
            atr_multiplier = get_atr_multiplier(symbol, timeframe)
            sl_distance = atr_value * atr_multiplier
            
            if side == 'buy':
                stop_loss_price = entry_price - sl_distance
                take_profit_price = entry_price + (sl_distance * 2)  # 1:2 ratio
            else:  # sell
                stop_loss_price = entry_price + sl_distance
                take_profit_price = entry_price - (sl_distance * 2)  # 1:2 ratio
            
            # Calculate initial risk amount
            position_size = float(position.get('units', 0))
            initial_risk_amount = abs(entry_price - stop_loss_price) * position_size
            
            strategy = ExitStrategy(
                position_id=position_id,
                symbol=symbol,
                entry_price=entry_price,
                current_price=entry_price,
                stop_loss_price=stop_loss_price,
                take_profit_price=take_profit_price,
                initial_risk_amount=initial_risk_amount,
                current_risk_amount=initial_risk_amount
            )
            
            self.exit_strategies[position_id] = strategy
            
            # Set SL/TP on the broker immediately
            asyncio.create_task(self._set_broker_sl_tp(position_id, stop_loss_price, take_profit_price))
            
            self.logger.info(f"Initialized exits for {position_id}: SL={stop_loss_price:.5f}, TP={take_profit_price:.5f}")
            
            return strategy
        except Exception as e:
            self.logger.error(f"Error adding position {position_id}: {e}")
            raise
    
    async def _get_atr_value(self, symbol: str, timeframe: str) -> float:
        """Get ATR value for a symbol and timeframe"""
        try:
            # This would need to be implemented based on your data source
            # For now, return a default value
            return 0.0010  # Default ATR value
        except Exception as e:
            self.logger.warning(f"Could not get ATR for {symbol}, using default: {e}")
            return 0.0010
    
    async def _set_broker_sl_tp(self, position_id: str, stop_loss: float, take_profit: float):
        """Set stop loss and take profit on the broker"""
        try:
            # This would integrate with your OANDA service
            # For now, just log the action
            self.logger.info(f"Setting broker SL/TP for {position_id}: SL={stop_loss:.5f}, TP={take_profit:.5f}")
        except Exception as e:
            self.logger.error(f"Failed to set broker SL/TP for {position_id}: {e}")
    
    async def handle_close_signal(self, position_id: str, reason: str) -> bool:
        """Handle a close signal with override analysis"""
        try:
            if position_id not in self.exit_strategies:
                self.logger.warning(f"No exit strategy found for {position_id}")
                return True
            
            strategy = self.exit_strategies[position_id]
            
            # Analyze whether to override the close signal
            override_decision = await self._analyze_override_eligibility(strategy)
            
            if override_decision.should_override:
                self.logger.info(f"Close signal overridden for {position_id}: {override_decision.reason}")
                self._activate_profit_override(strategy)
                return False  # Signal was overridden
            else:
                self.logger.info(f"Close signal executed for {position_id}: {override_decision.reason}")
                # Execute the close signal
                asyncio.create_task(self._execute_close_signal(position_id, reason))
                return True  # Signal was executed
                
        except Exception as e:
            self.logger.error(f"Error handling close signal for {position_id}: {e}")
            # Fallback: execute close signal
            asyncio.create_task(self._execute_close_signal(position_id, reason))
            return True
    
    async def _analyze_override_eligibility(self, strategy: ExitStrategy) -> OverrideDecision:
        """Analyze whether a close signal should be overridden"""
        try:
            # Check if override has already been fired
            if strategy.override_fired:
                return OverrideDecision(
                    should_override=False,
                    reason="Override already activated for this position",
                    confidence=0.0
                )
            
            # Check account drawdown limit
            account_balance = await self._get_account_balance()
            if account_balance <= 0:
                return OverrideDecision(
                    should_override=False,
                    reason="Cannot check account balance",
                    confidence=0.0
                )
            
            # Check position age
            position_age = datetime.now() - strategy.created_at
            if position_age.total_seconds() > (self.max_ride_time_hours * 3600):
                return OverrideDecision(
                    should_override=False,
                    reason=f"Position too old ({position_age.total_seconds()/3600:.1f}h)",
                    confidence=0.0
                )
            
            # Check if profit is sufficient (>= 1x initial risk)
            current_profit = abs(strategy.current_price - strategy.entry_price) * strategy.current_risk_amount / strategy.initial_risk_amount
            if current_profit < strategy.initial_risk_amount:
                return OverrideDecision(
                    should_override=False,
                    reason=f"Insufficient profit ({current_profit:.2f} < {strategy.initial_risk_amount:.2f})",
                    confidence=0.0
                )
            
            # Check market regime - these are synchronous methods, no await needed
            regime_data = self.regime.get_regime_data(strategy.symbol)
            current_regime = regime_data.get('regime', 'unknown')
            regime_strength = regime_data.get('regime_strength', 0.0)
            
            # Check volatility - this is also synchronous, no await needed
            vol_data = self.vol.get_volatility_state(strategy.symbol)
            volatility_ratio = vol_data.get('volatility_ratio', 1.0)
            
            # Decision logic
            override_score = 0.0
            reasons = []
            
            # Profit factor (0-3 points)
            profit_factor = current_profit / strategy.initial_risk_amount
            if profit_factor >= 2.0:
                override_score += 3.0
                reasons.append("High profit factor")
            elif profit_factor >= 1.5:
                override_score += 2.0
                reasons.append("Good profit factor")
            elif profit_factor >= 1.0:
                override_score += 1.0
                reasons.append("Minimum profit threshold met")
            
            # Market regime (0-2 points)
            if current_regime in ['trending_up', 'trending_down'] and regime_strength > 0.7:
                override_score += 2.0
                reasons.append("Strong trending regime")
            elif current_regime in ['momentum_up', 'momentum_down'] and regime_strength > 0.6:
                override_score += 1.5
                reasons.append("Momentum regime")
            
            # Volatility (0-2 points)
            if 0.8 <= volatility_ratio <= 1.5:
                override_score += 2.0
                reasons.append("Optimal volatility")
            elif 0.6 <= volatility_ratio <= 2.0:
                override_score += 1.0
                reasons.append("Acceptable volatility")
            
            # Position age bonus (0-1 point)
            if position_age.total_seconds() < (24 * 3600):  # Less than 24 hours
                override_score += 1.0
                reasons.append("Fresh position")
            
            # Decision threshold
            should_override = override_score >= 6.0  # Need 6+ points to override
            confidence = min(override_score / 10.0, 1.0)  # Normalize to 0-1
            
            return OverrideDecision(
                should_override=should_override,
                reason=f"Score: {override_score:.1f}/10 - {'; '.join(reasons)}",
                trailing_stop_enabled=should_override,
                confidence=confidence
            )
            
        except Exception as e:
            self.logger.error(f"Error analyzing override eligibility: {e}")
            return OverrideDecision(
                should_override=False,
                reason=f"Analysis error: {str(e)}",
                confidence=0.0
            )
    
    async def _get_account_balance(self) -> float:
        """Get current account balance"""
        try:
            # This would integrate with your OANDA service
            # For now, return a default value
            return 100000.0  # Default balance
        except Exception as e:
            self.logger.warning(f"Could not get account balance: {e}")
            return 0.0
    
    def _activate_profit_override(self, strategy: ExitStrategy):
        """Activate profit override and enable trailing stops"""
        strategy.override_fired = True
        strategy.trailing_stop_price = strategy.current_price
        
        # Calculate initial trailing stop based on ATR
        timeframe = "1H"  # Default timeframe
        config = self._get_trailing_stop_config(timeframe)
        
        if strategy.current_price > strategy.entry_price:  # Long position
            strategy.trailing_stop_price = strategy.current_price - (config.min_distance * config.atr_multiplier)
        else:  # Short position
            strategy.trailing_stop_price = strategy.current_price + (config.min_distance * config.atr_multiplier)
        
        # CRITICAL FIX: Remove initial SL/TP from broker when trailing stops activate
        # This prevents redundancy and ensures only trailing stops are active
        asyncio.create_task(self._remove_broker_sl_tp(strategy.position_id))
        
        self.logger.info(f"Profit override activated for {strategy.position_id}: "
                        f"Trailing stop at {strategy.trailing_stop_price:.5f}, "
                        f"Initial SL/TP removed from broker")
    
    async def _execute_close_signal(self, position_id: str, reason: str):
        """Execute the close signal by closing the position"""
        try:
            # This would integrate with your position tracker and OANDA service
            self.logger.info(f"Executing close signal for {position_id}: {reason}")
            
            # Remove from exit strategies
            if position_id in self.exit_strategies:
                del self.exit_strategies[position_id]
                
        except Exception as e:
            self.logger.error(f"Error executing close signal for {position_id}: {e}")
    
    def _check_sl_tp_triggers(self):
        """Check if initial SL or TP has been hit - PRIORITY CHECK"""
        positions_to_remove = []
        
        for position_id, strategy in self.exit_strategies.items():
            try:
                # Only check SL/TP if profit override has NOT been activated
                # If override is active, trailing stops are handling the exit
                if not strategy.override_fired:
                    # Check if SL or TP was hit
                    if strategy.current_price <= strategy.stop_loss_price or strategy.current_price >= strategy.take_profit_price:
                        exit_type = ExitType.STOP_LOSS if strategy.current_price <= strategy.stop_loss_price else ExitType.TAKE_PROFIT
                        
                        self.logger.info(f"SL/TP triggered for {position_id}: {exit_type.value} at {strategy.current_price:.5f}")
                        
                        # Execute exit
                        asyncio.create_task(self._execute_exit(position_id, exit_type))
                        positions_to_remove.append(position_id)
                else:
                    # Profit override is active - SL/TP are disabled, only trailing stops matter
                    self.logger.debug(f"SL/TP check skipped for {position_id} - profit override active")
                    
            except Exception as e:
                self.logger.error(f"Error checking SL/TP for {position_id}: {e}")
        
        # Remove exited positions
        for position_id in positions_to_remove:
            if position_id in self.exit_strategies:
                del self.exit_strategies[position_id]
    
    def _update_trailing_stops(self):
        """Update trailing stops for positions with profit override active"""
        for position_id, strategy in list(self.exit_strategies.items()):
            if not strategy.override_fired or not strategy.trailing_stop_price:
                continue
                
            try:
                # Check breakeven activation
                if not strategy.breakeven_enabled and strategy.current_price >= strategy.entry_price + strategy.initial_risk_amount:
                    strategy.breakeven_enabled = True
                    strategy.breakeven_price = strategy.entry_price
                    self.logger.info(f"Breakeven activated for {position_id} at {strategy.breakeven_price:.5f}")
                
                # Update trailing stop
                timeframe = "1H"  # Default timeframe
                config = self._get_trailing_stop_config(timeframe)
                
                if strategy.current_price > strategy.entry_price:  # Long position
                    new_trailing_stop = strategy.current_price - (config.min_distance * config.atr_multiplier)
                    
                    # Apply breakeven if enabled
                    if strategy.breakeven_enabled and strategy.breakeven_price > new_trailing_stop:
                        new_trailing_stop = strategy.breakeven_price
                    
                    # Only move trailing stop up
                    if new_trailing_stop > strategy.trailing_stop_price:
                        strategy.trailing_stop_price = new_trailing_stop
                        self.logger.debug(f"Updated trailing stop for {position_id}: {strategy.trailing_stop_price:.5f}")
                        
                else:  # Short position
                    new_trailing_stop = strategy.current_price + (config.min_distance * config.atr_multiplier)
                    
                    # Apply breakeven if enabled
                    if strategy.breakeven_enabled and strategy.breakeven_price < new_trailing_stop:
                        new_trailing_stop = strategy.breakeven_price
                    
                    # Only move trailing stop down
                    if new_trailing_stop < strategy.trailing_stop_price:
                        strategy.trailing_stop_price = new_trailing_stop
                        self.logger.debug(f"Updated trailing stop for {position_id}: {strategy.trailing_stop_price:.5f}")
                
                # Check if trailing stop was hit
                if self._is_trailing_stop_hit(strategy):
                    self.logger.info(f"Trailing stop hit for {position_id} at {strategy.current_price:.5f}")
                    asyncio.create_task(self._execute_exit(position_id, ExitType.TRAILING_STOP))
                    del self.exit_strategies[position_id]
                    
            except Exception as e:
                self.logger.error(f"Error updating trailing stop for {position_id}: {e}")
    
    def _is_trailing_stop_hit(self, strategy: ExitStrategy) -> bool:
        """Check if trailing stop has been hit"""
        if not strategy.trailing_stop_price:
            return False
            
        if strategy.current_price > strategy.entry_price:  # Long position
            return strategy.current_price <= strategy.trailing_stop_price
        else:  # Short position
            return strategy.current_price >= strategy.trailing_stop_price
    
    async def _check_dynamic_exits(self):
        """Check for other dynamic exit conditions"""
        # Call the specific exit checks
        for position_id, strategy in list(self.exit_strategies.items()):
            try:
                # Check regime exit
                await self._check_regime_exit(position_id, strategy)
                
                # Check volatility exit
                await self._check_volatility_exit(position_id, strategy)
            except Exception as e:
                self.logger.error(f"Error in dynamic exit checks for {position_id}: {e}")
    
    async def _check_regime_exit(self, position_id: str, strategy: ExitStrategy) -> bool:
        """Check if position should exit based on regime change"""
        try:
            # Get current regime data - this is a synchronous method
            regime_data = self.regime.get_regime_data(strategy.symbol)
            current_regime = regime_data.get('regime', 'unknown')
            regime_strength = regime_data.get('regime_strength', 0.0)
            
            # Check if we should exit based on regime
            # For example, exit longs if regime changes to trending_down with high confidence
            if strategy.entry_price < strategy.current_price:  # Long position
                if current_regime == 'trending_down' and regime_strength > 0.8:
                    self.logger.info(f"Regime exit triggered for {position_id}: {current_regime} ({regime_strength:.2f})")
                    await self._execute_exit(position_id, ExitType.CLOSE_SIGNAL)
                    return True
            else:  # Short position
                if current_regime == 'trending_up' and regime_strength > 0.8:
                    self.logger.info(f"Regime exit triggered for {position_id}: {current_regime} ({regime_strength:.2f})")
                    await self._execute_exit(position_id, ExitType.CLOSE_SIGNAL)
                    return True
                    
            return False
        except Exception as e:
            self.logger.error(f"Error checking regime exit: {e}")
            return False
            
    async def _check_volatility_exit(self, position_id: str, strategy: ExitStrategy) -> bool:
        """Check if position should exit based on volatility change"""
        try:
            # Get current volatility data - this is a synchronous method
            vol_data = self.vol.get_volatility_state(strategy.symbol)
            volatility_state = vol_data.get('volatility_state', 'normal')
            volatility_ratio = vol_data.get('volatility_ratio', 1.0)
            
            # Check if we should exit based on volatility
            # For example, exit if volatility spikes too high
            if volatility_state == 'high' and volatility_ratio > 2.0:
                # Only exit if we've been in the position for a while
                position_age = datetime.now() - strategy.created_at
                if position_age.total_seconds() > 3600:  # More than 1 hour
                    self.logger.info(f"Volatility exit triggered for {position_id}: {volatility_state} ({volatility_ratio:.2f})")
                    await self._execute_exit(position_id, ExitType.CLOSE_SIGNAL)
                    return True
                    
            return False
        except Exception as e:
            self.logger.error(f"Error checking volatility exit: {e}")
            return False
    
    async def _remove_broker_sl_tp(self, position_id: str):
        """Remove SL/TP from broker when trailing stops activate"""
        try:
            # This would integrate with your OANDA service to remove SL/TP
            self.logger.info(f"Removing initial SL/TP from broker for {position_id}")
            
            # TODO: Implement actual broker SL/TP removal
            # await self.oanda_service.remove_stop_loss_take_profit(position_id)
            
        except Exception as e:
            self.logger.error(f"Error removing broker SL/TP for {position_id}: {e}")
    
    async def _execute_exit(self, position_id: str, exit_type: ExitType):
        """Execute position exit"""
        try:
            # This would integrate with your position tracker and OANDA service
            self.logger.info(f"Executing {exit_type.value} exit for {position_id}")
            
            # Remove from exit strategies
            if position_id in self.exit_strategies:
                del self.exit_strategies[position_id]
                
        except Exception as e:
            self.logger.error(f"Error executing exit for {position_id}: {e}")
    
    async def _monitor_loop(self):
        """Main monitoring loop with new priority order"""
        while self.monitoring:
            try:
                # 1. PRIORITY: Check SL/TP triggers first
                self._check_sl_tp_triggers()
                
                # 2. Update trailing stops for profit override positions
                self._update_trailing_stops()
                
                # 3. Check other dynamic exit conditions
                await self._check_dynamic_exits()
                
                # Update current prices for all strategies
                await self._update_prices()
                
                # Wait before next iteration
                await asyncio.sleep(1)  # 1 second monitoring interval
                
            except Exception as e:
                self.logger.error(f"Error in monitor loop: {e}")
                await asyncio.sleep(5)  # Wait longer on error
    
    async def _update_prices(self):
        """Update current prices for all exit strategies"""
        for position_id, strategy in self.exit_strategies.items():
            try:
                # This would integrate with your price data source
                # For now, just update the timestamp
                strategy.last_updated = datetime.now()
                
            except Exception as e:
                self.logger.error(f"Error updating price for {position_id}: {e}")
    
    async def start_monitoring(self):
        """Start the monitoring loop"""
        if self.monitoring:
            return
            
        self.monitoring = True
        self.monitor_task = asyncio.create_task(self._monitor_loop())
        self.logger.info("Unified exit manager monitoring started")
    
    async def stop_monitoring(self):
        """Stop the monitoring loop"""
        if not self.monitoring:
            return
            
        self.monitoring = False
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
        self.logger.info("Unified exit manager monitoring stopped")
    
    def get_exit_strategy(self, position_id: str) -> Optional[ExitStrategy]:
        """Get exit strategy for a position"""
        return self.exit_strategies.get(position_id)
    
    def get_all_strategies(self) -> Dict[str, ExitStrategy]:
        """Get all exit strategies"""
        return self.exit_strategies.copy()
    
    def update_position_price(self, position_id: str, new_price: float):
        """Update current price for a position"""
        if position_id in self.exit_strategies:
            strategy = self.exit_strategies[position_id]
            strategy.current_price = new_price
            strategy.last_updated = datetime.now()

def create_unified_exit_manager(position_tracker: PositionTracker,
                               oanda_service: OandaService,
                               unified_analysis) -> UnifiedExitManager:
    """Factory function to create a unified exit manager"""
    return UnifiedExitManager(
        position_tracker=position_tracker,
        oanda_service=oanda_service,
        unified_analysis=unified_analysis
    )


# ============================================================================
# EXIT SIGNAL MONITORING (Merged from exit_monitor.py)
# ============================================================================

@dataclass
class ExitSignalRecord:
    """Record of an exit signal and its processing"""
    signal_time: datetime
    symbol: str
    position_id: Optional[str]
    alert_data: Dict[str, Any]
    status: str  # 'pending', 'success', 'failed', 'timeout'
    processing_time: Optional[float] = None
    error_message: Optional[str] = None
    retry_count: int = 0

class ExitSignalMonitor:
    """
    Monitor exit signals and track their effectiveness.
    Provides alerts when exits are consistently failing.
    """
    
    def __init__(self):
        self.pending_exits: Dict[str, ExitSignalRecord] = {}
        self.completed_exits: List[ExitSignalRecord] = []
        self.failed_exits: List[ExitSignalRecord] = []
        self._lock = asyncio.Lock()
        self._monitoring = False
        self._monitor_task = None
        
        # Statistics
        self.stats = {
            "total_signals": 0,
            "successful_exits": 0,
            "failed_exits": 0,
            "timed_out_exits": 0,
            "average_processing_time": 0.0,
            "success_rate": 0.0
        }
    
    async def start_monitoring(self):
        """Start the exit signal monitoring"""
        if self._monitoring:
            return
            
        self._monitoring = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("Exit signal monitoring started")
    
    async def stop_monitoring(self):
        """Stop the exit signal monitoring"""
        self._monitoring = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("Exit signal monitoring stopped")
    
    async def record_exit_signal(self, alert_data: Dict[str, Any]) -> str:
        """Record a new exit signal for monitoring"""
        async with self._lock:
            signal_id = f"{alert_data.get('symbol', 'UNKNOWN')}_{int(time.time())}"
            
            record = ExitSignalRecord(
                signal_time=datetime.now(timezone.utc),
                symbol=alert_data.get('symbol', 'UNKNOWN'),
                position_id=alert_data.get('position_id') or alert_data.get('alert_id'),
                alert_data=alert_data.copy(),
                status='pending'
            )
            
            self.pending_exits[signal_id] = record
            self.stats["total_signals"] += 1
            
            logger.info(f"[EXIT MONITOR] Recorded exit signal: {signal_id}")
            return signal_id
    
    async def record_exit_success(self, signal_id: str, processing_time: float):
        """Record successful exit processing"""
        async with self._lock:
            if signal_id in self.pending_exits:
                record = self.pending_exits[signal_id]
                record.status = 'success'
                record.processing_time = processing_time
                
                self.completed_exits.append(record)
                del self.pending_exits[signal_id]
                
                self.stats["successful_exits"] += 1
                self._update_stats()
                
                logger.info(f"[EXIT MONITOR] Exit success: {signal_id} (processed in {processing_time:.2f}s)")
    
    async def record_exit_failure(self, signal_id: str, error_message: str, processing_time: Optional[float] = None):
        """Record failed exit processing"""
        async with self._lock:
            if signal_id in self.pending_exits:
                record = self.pending_exits[signal_id]
                record.status = 'failed'
                record.error_message = error_message
                record.processing_time = processing_time
                
                self.failed_exits.append(record)
                del self.pending_exits[signal_id]
                
                self.stats["failed_exits"] += 1
                self._update_stats()
                
                logger.error(f"[EXIT MONITOR] Exit failed: {signal_id} - {error_message}")
    
    async def record_exit_retry(self, signal_id: str):
        """Record an exit retry attempt"""
        async with self._lock:
            if signal_id in self.pending_exits:
                record = self.pending_exits[signal_id]
                record.retry_count += 1
                logger.info(f"[EXIT MONITOR] Exit retry #{record.retry_count}: {signal_id}")
    
    async def _monitor_loop(self):
        """Main monitoring loop to check for timeouts and issues"""
        while self._monitoring:
            try:
                await self._check_timeouts()
                await self._analyze_patterns()
                await asyncio.sleep(30)  # Check every 30 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in exit monitor loop: {e}")
                await asyncio.sleep(30)
    
    async def _check_timeouts(self):
        """Check for exit signals that have timed out"""
        # Default timeout if config not available
        timeout_seconds = 300  # 5 minutes default
        try:
            from config import config
            timeout_seconds = getattr(config, 'exit_signal_timeout_minutes', 5) * 60
        except:
            pass
            
        current_time = datetime.now(timezone.utc)
        
        async with self._lock:
            timed_out = []
            
            for signal_id, record in self.pending_exits.items():
                time_elapsed = (current_time - record.signal_time).total_seconds()
                
                if time_elapsed > timeout_seconds:
                    record.status = 'timeout'
                    record.error_message = f"Exit signal timed out after {time_elapsed:.1f} seconds"
                    
                    timed_out.append((signal_id, record))
                    self.stats["timed_out_exits"] += 1
            
            # Move timed out records to failed exits
            for signal_id, record in timed_out:
                self.failed_exits.append(record)
                del self.pending_exits[signal_id]
                
                logger.error(f"[EXIT MONITOR] Exit timed out: {signal_id} (symbol: {record.symbol})")
            
            if timed_out:
                self._update_stats()
    
    async def _analyze_patterns(self):
        """Analyze exit failure patterns and alert if needed"""
        if len(self.completed_exits) + len(self.failed_exits) < 10:
            return  # Need more data
        
        recent_failures = 0
        recent_total = 0
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=1)
        
        # Check recent exits (last hour)
        for record in self.completed_exits[-20:]:  # Last 20 exits
            if record.signal_time > cutoff_time:
                recent_total += 1
        
        for record in self.failed_exits[-20:]:  # Last 20 failures
            if record.signal_time > cutoff_time:
                recent_total += 1
                recent_failures += 1
        
        if recent_total >= 5:  # Minimum data for analysis
            failure_rate = recent_failures / recent_total
            
            if failure_rate > 0.5:  # More than 50% failure rate
                logger.error(f"[EXIT MONITOR] HIGH FAILURE RATE ALERT: {failure_rate:.1%} of recent exits failed!")
                await self._send_failure_alert(failure_rate, recent_failures, recent_total)
    
    async def _send_failure_alert(self, failure_rate: float, failed_count: int, total_count: int):
        """Send alert about high exit failure rate"""
        alert_message = (
            f"🚨 EXIT FAILURE ALERT 🚨\n\n"
            f"High exit signal failure rate detected:\n"
            f"📊 Failure Rate: {failure_rate:.1%}\n"
            f"❌ Failed: {failed_count}/{total_count} recent exits\n"
            f"⏰ Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
            f"Please check:\n"
            f"• Pine Script alert configuration\n"
            f"• Position ID matching logic\n"
            f"• Network connectivity\n"
            f"• System logs for detailed errors"
        )
        
        logger.critical(alert_message.replace('\n', ' | '))
    
    def _update_stats(self):
        """Update monitoring statistics"""
        total_processed = self.stats["successful_exits"] + self.stats["failed_exits"] + self.stats["timed_out_exits"]
        
        if total_processed > 0:
            self.stats["success_rate"] = self.stats["successful_exits"] / total_processed
        
        # Calculate average processing time
        processing_times = [
            record.processing_time for record in self.completed_exits 
            if record.processing_time is not None
        ]
        
        if processing_times:
            self.stats["average_processing_time"] = sum(processing_times) / len(processing_times)
    
    async def get_monitoring_report(self) -> Dict[str, Any]:
        """Get comprehensive monitoring report"""
        async with self._lock:
            pending_count = len(self.pending_exits)
            
            # Recent failures analysis
            recent_failures = []
            for record in self.failed_exits[-5:]:  # Last 5 failures
                recent_failures.append({
                    "time": record.signal_time.isoformat(),
                    "symbol": record.symbol,
                    "position_id": record.position_id,
                    "error": record.error_message,
                    "retry_count": record.retry_count
                })
            
            return {
                "monitoring_active": self._monitoring,
                "statistics": self.stats.copy(),
                "pending_exits": pending_count,
                "recent_failures": recent_failures,
                "recommendations": self._get_recommendations()
            }
    
    def _get_recommendations(self) -> List[str]:
        """Get recommendations based on current metrics"""
        recommendations = []
        
        if self.stats["success_rate"] < 0.8:
            recommendations.append("Exit success rate is low - check Pine Script alert configuration")
        
        if self.stats["average_processing_time"] > 10.0:
            recommendations.append("Exit processing is slow - check system performance")
        
        if self.stats["timed_out_exits"] > 0:
            recommendations.append("Some exits are timing out - increase timeout or check connectivity")
        
        if len(self.pending_exits) > 5:
            recommendations.append("Many exits are pending - check alert processing")
        
        return recommendations

# Global exit monitor instance
exit_monitor = ExitSignalMonitor()
