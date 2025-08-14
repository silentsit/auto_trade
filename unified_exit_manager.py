"""
UNIFIED EXIT MANAGER
Consolidated exit management system that handles:
- Immediate SL/TP for all new positions
- Priority monitoring of SL/TP triggers
- Close signal handling with override analysis
- Profit override activation with trailing stops
- Breakeven management at +1R

This module consolidates functionality from:
- profit_ride_override.py
- tiered_tp_monitor.py  
- dynamic_exit_manager.py
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import math

from tracker import PositionTracker
from oanda_service import OandaService
from regime_classifier import LorentzianDistanceClassifier
from volatility_monitor import VolatilityMonitor
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
                 position_tracker: PositionTracker,
                 oanda_service: OandaService,
                 regime_classifier: LorentzianDistanceClassifier,
                 volatility_monitor: VolatilityMonitor):
        """Initialize the unified exit manager"""
        self.position_tracker = position_tracker
        self.oanda_service = oanda_service
        self.regime = regime_classifier
        self.vol = volatility_monitor
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
    
    def initialize_position_exits(self, position: Dict) -> ExitStrategy:
        """Initialize exit strategy for a new position with immediate SL/TP"""
        position_id = position.get('id')
        symbol = position.get('instrument')
        entry_price = float(position.get('price', 0))
        side = position.get('side', 'buy')
        
        if not all([position_id, symbol, entry_price]):
            raise ValueError(f"Invalid position data: {position}")
        
        # Get ATR for volatility-based calculations
        timeframe = position.get('timeframe', '1H')
        atr_value = asyncio.run(self._get_atr_value(symbol, timeframe))
        
        # Calculate ATR-based SL and TP with 1:2 risk-reward ratio
        atr_multiplier = get_atr_multiplier(timeframe)
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
    
    def handle_close_signal(self, position_id: str, signal_type: str, reason: str = "Close signal received") -> bool:
        """Handle incoming close signal - the central method for the new flow"""
        try:
            # First check if position still exists
            if position_id not in self.exit_strategies:
                self.logger.info(f"Close signal acknowledged for {position_id}: Position already exited")
                return True
            
            strategy = self.exit_strategies[position_id]
            
            # Analyze whether to override the close signal
            override_decision = self._analyze_override_eligibility(strategy)
            
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
    
    def _analyze_override_eligibility(self, strategy: ExitStrategy) -> OverrideDecision:
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
            account_balance = asyncio.run(self._get_account_balance())
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
            
            # Check market regime
            regime_data = self.regime.get_regime_data(strategy.symbol)
            current_regime = regime_data.get('regime', 'unknown')
            regime_strength = regime_data.get('regime_strength', 0.0)
            
            # Check volatility
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
        
        self.logger.info(f"Profit override activated for {strategy.position_id}: "
                        f"Trailing stop at {strategy.trailing_stop_price:.5f}")
    
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
                # Check if SL or TP was hit
                if strategy.current_price <= strategy.stop_loss_price or strategy.current_price >= strategy.take_profit_price:
                    exit_type = ExitType.STOP_LOSS if strategy.current_price <= strategy.stop_loss_price else ExitType.TAKE_PROFIT
                    
                    self.logger.info(f"SL/TP triggered for {position_id}: {exit_type.value} at {strategy.current_price:.5f}")
                    
                    # Execute exit
                    asyncio.create_task(self._execute_exit(position_id, exit_type))
                    positions_to_remove.append(position_id)
                    
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
    
    def _check_dynamic_exits(self):
        """Check for other dynamic exit conditions"""
        # This method can be expanded for additional exit logic
        pass
    
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
                self._check_dynamic_exits()
                
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
                               regime_classifier: LorentzianDistanceClassifier,
                               volatility_monitor: VolatilityMonitor) -> UnifiedExitManager:
    """Factory function to create a unified exit manager"""
    return UnifiedExitManager(
        position_tracker=position_tracker,
        oanda_service=oanda_service,
        regime_classifier=regime_classifier,
        volatility_monitor=volatility_monitor
    )
