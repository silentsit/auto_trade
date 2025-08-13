"""
Unified Exit Manager - Consolidated profit riding, dynamic exits, and volatility-adaptive trailing stops

This module consolidates the functionality from:
- profit_ride_override.py
- tiered_tp_monitor.py  
- dynamic_exit_manager.py

NEW FLOW:
1. All new positions get SL/TP set immediately
2. Monitor for SL/TP hits and exit if triggered
3. When close signal arrives, check if position still exists
4. If position exists, analyze for override decision
5. If override triggered, activate trailing stops
6. If no override, execute close immediately
"""

import asyncio
import logging
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum

from tracker import PositionTracker
from oanda_service import OandaService
from regime_classifier import LorentzianDistanceClassifier
from volatility_monitor import VolatilityMonitor
from utils import get_atr

logger = logging.getLogger(__name__)

class ExitReason(Enum):
    """Enumeration of exit reasons"""
    STOP_LOSS_HIT = "stop_loss_hit"
    TAKE_PROFIT_HIT = "take_profit_hit"
    CLOSE_SIGNAL_EXECUTED = "close_signal_executed"
    CLOSE_SIGNAL_OVERRIDE = "close_signal_override"
    TRAILING_STOP_HIT = "trailing_stop_hit"
    TIME_BASED_EXIT = "time_based_exit"
    VOLATILITY_EXIT = "volatility_exit"
    REGIME_EXIT = "regime_exit"

@dataclass
class TrailingStopConfig:
    """Configuration for trailing stop behavior"""
    initial_stop_atr_multiplier: float = 2.0
    take_profit_atr_multiplier: float = 4.0
    trailing_activation_atr_multiplier: float = 1.0
    trailing_stop_atr_multiplier: float = 1.5
    breakeven_activation_atr_multiplier: float = 1.0
    max_hold_time_hours: int = 8

@dataclass
class OverrideDecision:
    """Decision about whether to override a close signal"""
    ignore_close: bool
    sl_atr_multiplier: float
    tp_atr_multiplier: float
    reason: str
    trailing_stop_enabled: bool = False

@dataclass
class ExitStrategy:
    """Complete exit strategy for a position"""
    position_id: str
    symbol: str
    action: str
    entry_price: float
    entry_time: datetime
    atr_value: float
    timeframe: str
    
    # Initial SL/TP (set immediately for all positions)
    stop_loss_price: float
    stop_loss_atr_multiplier: float
    take_profit_price: float
    take_profit_atr_multiplier: float
    
    # Trailing stop (only enabled after override)
    trailing_stop_enabled: bool = False
    trailing_stop_price: Optional[float] = None
    trailing_stop_atr_multiplier: float = 1.5
    trailing_stop_activated: bool = False
    
    # Breakeven management
    breakeven_enabled: bool = False
    breakeven_price: Optional[float] = None
    breakeven_activated: bool = False
    
    # Risk tracking
    initial_risk_amount: float = 0.0
    current_risk_amount: float = 0.0
    
    # Override tracking
    override_fired: bool = False
    
    # Metadata
    last_update: datetime = None
    
    def __post_init__(self):
        if self.last_update is None:
            self.last_update = datetime.now(timezone.utc)

class UnifiedExitManager:
    """
    Unified exit manager implementing the cleaner flow:
    1. Immediate SL/TP for all positions
    2. SL/TP monitoring and execution
    3. Close signal handling with position status check
    4. Override logic only for existing positions
    """
    
    def __init__(self, 
                 position_tracker: PositionTracker,
                 oanda_service: OandaService,
                 regime_classifier: LorentzianDistanceClassifier,
                 volatility_monitor: VolatilityMonitor,
                 monitor_interval: int = 30):
        
        self.position_tracker = position_tracker
        self.oanda_service = oanda_service
        self.regime = regime_classifier
        self.vol = volatility_monitor
        self.monitor_interval = monitor_interval
        
        # State management
        self.monitoring = False
        self._monitor_task = None
        self.exit_strategies: Dict[str, ExitStrategy] = {}
        self._lock = asyncio.Lock()
        
        logger.info("🎯 Unified Exit Manager initialized with cleaner flow")
    
    # ============================================================================
    # === POSITION INITIALIZATION (IMMEDIATE SL/TP) ===
    # ============================================================================
    
    async def initialize_position_exits(self, position: Dict) -> ExitStrategy:
        """
        Initialize exit strategy for a new position
        IMMEDIATELY sets SL/TP - this is the key change
        """
        try:
            # Get ATR value for the position
            atr_value = await get_atr(position['symbol'], position['timeframe'])
            if not atr_value:
                logger.error(f"Could not get ATR for {position['symbol']}")
                return None
            
            # Get trailing stop configuration (for when override is triggered)
            config = self._get_trailing_stop_config(position['timeframe'])
            
            # Calculate stop loss and take profit prices (1:2 risk-reward)
            if position['action'].upper() == "BUY":
                stop_loss_price = position['entry_price'] - (atr_value * config.initial_stop_atr_multiplier)
                take_profit_price = position['entry_price'] + (atr_value * config.take_profit_atr_multiplier)
            else:
                stop_loss_price = position['entry_price'] + (atr_value * config.initial_stop_atr_multiplier)
                take_profit_price = position['entry_price'] - (atr_value * config.take_profit_atr_multiplier)
            
            # Calculate initial risk amount
            initial_risk_amount = abs((stop_loss_price - position['entry_price']) * position['units'])
            
            # Create exit strategy - SL/TP are ENABLED immediately
            strategy = ExitStrategy(
                position_id=position['position_id'],
                symbol=position['symbol'],
                action=position['action'],
                entry_price=position['entry_price'],
                entry_time=position['entry_time'],
                atr_value=atr_value,
                timeframe=position['timeframe'],
                stop_loss_price=stop_loss_price,
                stop_loss_atr_multiplier=config.initial_stop_atr_multiplier,
                take_profit_price=take_profit_price,
                take_profit_atr_multiplier=config.take_profit_atr_multiplier,
                trailing_stop_enabled=False,  # Only enabled after override
                trailing_stop_price=None,
                trailing_stop_atr_multiplier=config.trailing_stop_atr_multiplier,
                breakeven_enabled=False,
                breakeven_price=None,
                initial_risk_amount=initial_risk_amount,
                current_risk_amount=initial_risk_amount,
                last_update=datetime.now(timezone.utc)
            )
            
            # Store strategy
            async with self._lock:
                self.exit_strategies[position['position_id']] = strategy
            
            logger.info(f"🎯 Initialized exit strategy for {position['symbol']} with immediate SL/TP (1:2 risk-reward)")
            return strategy
            
        except Exception as e:
            logger.error(f"Error initializing exit strategy for {position['symbol']}: {e}")
            return None
    
    # ============================================================================
    # === SL/TP MONITORING (MAIN LOOP) ===
    # ============================================================================
    
    async def start_monitoring(self):
        """Start the unified exit monitoring loop"""
        if self.monitoring:
            logger.warning("Unified exit manager is already running")
            return
            
        self.monitoring = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("🎯 Unified exit manager monitoring started")
        
    async def stop_monitoring(self):
        """Stop the unified exit monitoring loop"""
        self.monitoring = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 Unified exit manager monitoring stopped")
        
    async def _monitor_loop(self):
        """Main monitoring loop - checks SL/TP hits FIRST"""
        while self.monitoring:
            try:
                await self._check_sl_tp_triggers()
                await self._check_trailing_stops()
                await self._check_dynamic_exits()
                await asyncio.sleep(self.monitor_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in unified exit monitor loop: {e}")
                await asyncio.sleep(self.monitor_interval)
    
    async def _check_sl_tp_triggers(self):
        """
        Check if SL or TP has been hit - this is PRIORITY #1
        If hit, exit position immediately
        """
        try:
            # Get all open positions
            open_positions_nested = await self.position_tracker.get_open_positions()
            active_positions = {}
            for symbol, positions in open_positions_nested.items():
                for position_id, position_data in positions.items():
                    active_positions[position_id] = position_data
            
            for position_id, position_data in active_positions.items():
                # Get exit strategy
                strategy = await self.get_exit_strategy(position_id)
                if not strategy:
                    continue
                
                # Get current price
                current_price = await self.oanda_service.get_current_price(strategy.symbol, strategy.action)
                if not current_price:
                    continue
                
                # Check if SL or TP hit
                should_exit = False
                exit_reason = None
                
                if strategy.action.upper() == "BUY":
                    # Long position
                    if current_price <= strategy.stop_loss_price:
                        should_exit = True
                        exit_reason = ExitReason.STOP_LOSS_HIT
                    elif current_price >= strategy.take_profit_price:
                        should_exit = True
                        exit_reason = ExitReason.TAKE_PROFIT_HIT
                else:
                    # Short position
                    if current_price >= strategy.stop_loss_price:
                        should_exit = True
                        exit_reason = ExitReason.STOP_LOSS_HIT
                    elif current_price <= strategy.take_profit_price:
                        should_exit = True
                        exit_reason = ExitReason.TAKE_PROFIT_HIT
                
                # Execute exit if SL/TP hit
                if should_exit:
                    await self._execute_exit(position_id, exit_reason, f"{exit_reason.value}: {strategy.symbol}")
                    
        except Exception as e:
            logger.error(f"Error checking SL/TP triggers: {e}")
    
    async def _check_trailing_stops(self):
        """
        Check trailing stop conditions for positions with override activated
        """
        try:
            for position_id, strategy in self.exit_strategies.items():
                if not strategy.trailing_stop_enabled:
                    continue
                
                # Check if position still exists
                position_exists = await self._position_still_exists(position_id)
                if not position_exists:
                    continue
                
                await self._update_trailing_stop(strategy)
                
        except Exception as e:
            logger.error(f"Error checking trailing stops: {e}")
    
    async def _check_dynamic_exits(self):
        """
        Check dynamic exit conditions (time-based, volatility-based, regime-based)
        """
        try:
            for position_id, strategy in self.exit_strategies.items():
                # Check if position still exists
                position_exists = await self._position_still_exists(position_id)
                if not position_exists:
                    continue
                
                # Time-based exit
                await self._check_time_based_exit(strategy)
                
                # Volatility-based exit
                await self._check_volatility_exit(strategy)
                
                # Regime-based exit
                await self._check_regime_exit(strategy)
                
        except Exception as e:
            logger.error(f"Error checking dynamic exits: {e}")
    
    # ============================================================================
    # === CLOSE SIGNAL HANDLING ===
    # ============================================================================
    
    async def handle_close_signal(self, position_id: str, signal_type: str, reason: str = "Close signal received") -> bool:
        """
        Handle close signal - this is the NEW flow
        1. Check if position still exists
        2. If not, acknowledge it already exited
        3. If exists, analyze for override decision
        4. Execute close or activate override
        """
        try:
            # Step 1: Check if position still exists
            position_exists = await self._position_still_exists(position_id)
            if not position_exists:
                logger.info(f"📝 Close signal acknowledged for {position_id}: Position already exited")
                return True
            
            # Step 2: Get exit strategy
            strategy = await self.get_exit_strategy(position_id)
            if not strategy:
                logger.error(f"No exit strategy found for {position_id}")
                return False
            
            # Step 3: Analyze for override decision
            override_decision = await self._analyze_override_eligibility(strategy)
            
            if override_decision.ignore_close:
                # Step 4a: Activate profit override
                await self._activate_profit_override(strategy)
                logger.info(f"🎯 Profit override activated for {strategy.symbol}: {override_decision.reason}")
                return True
            else:
                # Step 4b: Execute close signal immediately
                await self._execute_exit(position_id, ExitReason.CLOSE_SIGNAL_EXECUTED, reason)
                logger.info(f"🔄 Close signal executed for {strategy.symbol}: {reason}")
                return True
                
        except Exception as e:
            logger.error(f"Error handling close signal for {position_id}: {e}")
            return False
    
    async def _analyze_override_eligibility(self, strategy: ExitStrategy) -> OverrideDecision:
        """
        Analyze if position is eligible for profit override
        This is called ONLY when close signal arrives and position still exists
        """
        try:
            # --- Risk Control: Only fire once per position ---
            if strategy.override_fired:
                return OverrideDecision(ignore_close=False, reason="Override already fired")
            
            # --- Risk Control: Check account drawdown ---
            # Note: This would need to be passed in or retrieved
            drawdown = 0.0  # Placeholder - implement actual drawdown check
            
            if drawdown > 0.70:  # 70% drawdown limit
                return OverrideDecision(ignore_close=False, reason="Account drawdown limit exceeded")
            
            # --- Risk Control: Check position age ---
            position_age_hours = (datetime.now(timezone.utc) - strategy.entry_time).total_seconds() / 3600
            if position_age_hours > 8:  # 8 hour maximum hold time
                return OverrideDecision(ignore_close=False, reason="Maximum hold time exceeded")
            
            # --- Profit Threshold Check ---
            # Get current position data
            position_data = await self._get_position_data(strategy.position_id)
            if not position_data:
                return OverrideDecision(ignore_close=False, reason="Could not get position data")
            
            current_pnl = position_data.get('unrealized_pnl', 0.0)
            
            # Must be profitable enough to justify override
            if current_pnl < strategy.initial_risk_amount:
                return OverrideDecision(ignore_close=False, reason="Insufficient profit for override")
            
            # --- Market Condition Checks ---
            # Get current market regime and volatility
            try:
                # Use existing methods or provide defaults
                regime_data = await self.regime.get_regime_data(strategy.symbol)
                regime_info = {"regime": regime_data.get("current_regime", "unknown")}
                
                vol_state = await self.vol.get_volatility_state(strategy.symbol)
                vol_ratio = vol_state.get("volatility_ratio", 1.0)
            except Exception as e:
                logger.warning(f"Could not get market conditions: {e}")
                # Default to conservative override
                regime_info = {"regime": "unknown"}
                vol_ratio = 1.0
            
            # --- Override Decision Logic ---
            should_override = False
            override_reason = ""
            
            # Check if market is trending in our favor
            if strategy.action.upper() == "BUY" and regime_info.get("regime") == "trending_up":
                should_override = True
                override_reason = "Bullish trending market"
            elif strategy.action.upper() == "SELL" and regime_info.get("regime") == "trending_down":
                should_override = True
                override_reason = "Bearish trending market"
            
            # Check volatility conditions
            if vol_ratio < 1.5:  # Low volatility - good for holding
                if should_override:
                    override_reason += " + Low volatility"
                else:
                    should_override = True
                    override_reason = "Low volatility conditions"
            
            # Check momentum
            current_price = await self.oanda_service.get_current_price(strategy.symbol, strategy.action)
            if current_price:
                if strategy.action.upper() == "BUY":
                    momentum = (current_price - strategy.entry_price) / strategy.atr_value
                else:
                    momentum = (strategy.entry_price - current_price) / strategy.atr_value
                
                if momentum > 0.15:  # Strong momentum
                    if should_override:
                        override_reason += " + Strong momentum"
                    else:
                        should_override = True
                        override_reason = "Strong momentum"
            
            if should_override:
                return OverrideDecision(
                    ignore_close=True,
                    sl_atr_multiplier=strategy.stop_loss_atr_multiplier,
                    tp_atr_multiplier=strategy.take_profit_atr_multiplier,
                    reason=override_reason,
                    trailing_stop_enabled=True
                )
            else:
                return OverrideDecision(ignore_close=False, reason="Market conditions don't support override")
                
        except Exception as e:
            logger.error(f"Error analyzing override eligibility: {e}")
            return OverrideDecision(ignore_close=False, reason=f"Error in analysis: {e}")
    
    async def _activate_profit_override(self, strategy: ExitStrategy):
        """
        Activate profit override for a position
        This enables trailing stops and marks the position for override management
        """
        try:
            # Mark override as fired
            strategy.override_fired = True
            
            # Enable trailing stops
            strategy.trailing_stop_enabled = True
            
            # Get current price and ATR for initial trailing stop
            current_price = await self.oanda_service.get_current_price(strategy.symbol, strategy.action)
            current_atr = await get_atr(strategy.symbol, strategy.timeframe)
            
            if current_price and current_atr:
                # Set initial trailing stop price
                config = self._get_trailing_stop_config(strategy.timeframe)
                
                if strategy.action.upper() == "BUY":
                    strategy.trailing_stop_price = current_price - (current_atr * config.trailing_stop_atr_multiplier)
                else:
                    strategy.trailing_stop_price = current_price + (current_atr * config.trailing_stop_atr_multiplier)
                
                strategy.trailing_stop_activated = True
                strategy.last_update = datetime.now(timezone.utc)
                
                logger.info(f"🎯 Profit override trailing stops activated for {strategy.symbol}")
            
        except Exception as e:
            logger.error(f"Error activating profit override for {strategy.position_id}: {e}")
    
    # ============================================================================
    # === EXECUTION METHODS ===
    # ============================================================================
    
    async def _execute_exit(self, position_id: str, exit_reason: ExitReason, details: str):
        """
        Execute position exit
        """
        try:
            logger.info(f"🚪 Executing exit for {position_id}: {exit_reason.value} - {details}")
            
            # This would call the position tracker to close the position
            # await self.position_tracker.close_position(position_id, details)
            
            # Remove the exit strategy
            await self.remove_exit_strategy(position_id)
            
        except Exception as e:
            logger.error(f"Error executing exit for {position_id}: {e}")
    
    # ============================================================================
    # === UTILITY METHODS ===
    # ============================================================================
    
    def _get_trailing_stop_config(self, timeframe: str) -> TrailingStopConfig:
        """Get trailing stop configuration based on timeframe"""
        if timeframe.upper() in ["15M", "15", "15MIN"]:
            # 15M: Aggressive approach
            return TrailingStopConfig(
                initial_stop_atr_multiplier=1.5,      # Tighter initial stop
                take_profit_atr_multiplier=3.0,       # 1:2 risk-reward
                trailing_activation_atr_multiplier=1.0, # Start trailing at +1R
                trailing_stop_atr_multiplier=1.0,     # Tight trailing
                breakeven_activation_atr_multiplier=1.0, # Breakeven at +1R
                max_hold_time_hours=8
            )
        elif timeframe.upper() in ["1H", "1HR", "60"]:
            # 1H: Balanced approach
            return TrailingStopConfig(
                initial_stop_atr_multiplier=2.0,      # Standard initial stop
                take_profit_atr_multiplier=4.0,       # 1:2 risk-reward
                trailing_activation_atr_multiplier=1.0, # Start trailing at +1R
                trailing_stop_atr_multiplier=1.5,     # Moderate trailing
                breakeven_activation_atr_multiplier=1.0, # Breakeven at +1R
                max_hold_time_hours=24
            )
        else:  # 4H and higher
            # 4H+: Conservative approach
            return TrailingStopConfig(
                initial_stop_atr_multiplier=2.5,      # Wider initial stop
                take_profit_atr_multiplier=5.0,       # 1:2 risk-reward
                trailing_activation_atr_multiplier=1.0, # Start trailing at +1R
                trailing_stop_atr_multiplier=2.0,     # Loose trailing
                breakeven_activation_atr_multiplier=1.0, # Breakeven at +1R
                max_hold_time_hours=48
            )
    
    async def get_exit_strategy(self, position_id: str) -> Optional[ExitStrategy]:
        """Get exit strategy for a position"""
        async with self._lock:
            return self.exit_strategies.get(position_id)
    
    async def remove_exit_strategy(self, position_id: str):
        """Remove exit strategy for a position"""
        async with self._lock:
            if position_id in self.exit_strategies:
                del self.exit_strategies[position_id]
                logger.debug(f"Removed exit strategy for {position_id}")
    
    async def _position_still_exists(self, position_id: str) -> bool:
        """Check if position still exists in the market"""
        try:
            open_positions = await self.position_tracker.get_open_positions()
            for symbol, positions in open_positions.items():
                if position_id in positions:
                    return True
            return False
        except Exception as e:
            logger.error(f"Error checking if position exists: {e}")
            return False
    
    async def _get_position_data(self, position_id: str) -> Optional[Dict]:
        """Get current position data"""
        try:
            open_positions = await self.position_tracker.get_open_positions()
            for symbol, positions in open_positions.items():
                if position_id in positions:
                    return positions[position_id]
            return None
        except Exception as e:
            logger.error(f"Error getting position data: {e}")
            return None
    
    async def _update_trailing_stop(self, strategy: ExitStrategy):
        """Update trailing stop for a position"""
        try:
            # Get current price and ATR
            current_price = await self.oanda_service.get_current_price(strategy.symbol, strategy.action)
            current_atr = await get_atr(strategy.symbol, strategy.timeframe)
            
            if not current_price or not current_atr:
                return
            
            # Calculate current profit in ATR terms
            if strategy.action.upper() == "BUY":
                profit_atr = (current_price - strategy.entry_price) / current_atr
            else:
                profit_atr = (strategy.entry_price - current_price) / current_atr
            
            # Get trailing stop configuration
            config = self._get_trailing_stop_config(strategy.timeframe)
            
            # --- Breakeven Logic ---
            if not strategy.breakeven_activated and profit_atr >= config.breakeven_activation_atr_multiplier:
                strategy.breakeven_activated = True
                strategy.breakeven_enabled = True
                strategy.breakeven_price = strategy.entry_price
                strategy.current_risk_amount = 0.0
                logger.info(f"💰 Breakeven activated for {strategy.symbol} at +{profit_atr:.2f}R")
            
            # --- Trailing Stop Update Logic ---
            if strategy.trailing_stop_activated and strategy.trailing_stop_enabled:
                if strategy.action.upper() == "BUY":
                    new_trailing_stop = current_price - (current_atr * config.trailing_stop_atr_multiplier)
                    if new_trailing_stop > strategy.trailing_stop_price:
                        strategy.trailing_stop_price = new_trailing_stop
                else:
                    new_trailing_stop = current_price + (current_atr * config.trailing_stop_atr_multiplier)
                    if new_trailing_stop < strategy.trailing_stop_price:
                        strategy.trailing_stop_price = new_trailing_stop
            
            # --- Check if trailing stop hit ---
            should_close = False
            if strategy.trailing_stop_enabled and strategy.trailing_stop_price:
                if strategy.action.upper() == "BUY":
                    if current_price <= strategy.trailing_stop_price:
                        should_close = True
                else:
                    if current_price >= strategy.trailing_stop_price:
                        should_close = True
            
            if should_close:
                await self._execute_exit(strategy.position_id, ExitReason.TRAILING_STOP_HIT, "Trailing stop hit")
            
        except Exception as e:
            logger.error(f"Error updating trailing stop for {strategy.position_id}: {e}")
    
    async def _check_time_based_exit(self, strategy: ExitStrategy):
        """Check if position should exit based on time"""
        try:
            config = self._get_trailing_stop_config(strategy.timeframe)
            position_age_hours = (datetime.now(timezone.utc) - strategy.entry_time).total_seconds() / 3600
            
            if position_age_hours > config.max_hold_time_hours:
                await self._execute_exit(strategy.position_id, ExitReason.TIME_BASED_EXIT, f"Max hold time exceeded: {position_age_hours:.1f}h")
                
        except Exception as e:
            logger.error(f"Error checking time-based exit: {e}")
    
    async def _check_volatility_exit(self, strategy: ExitStrategy):
        """Check if position should exit based on volatility"""
        try:
            vol_state = await self.vol.get_volatility_state(strategy.symbol)
            vol_ratio = vol_state.get("volatility_ratio", 1.0)
            if vol_ratio > 3.0:  # Extreme volatility
                await self._execute_exit(strategy.position_id, ExitReason.VOLATILITY_EXIT, f"Extreme volatility: {vol_ratio:.2f}")
                
        except Exception as e:
            logger.error(f"Error checking volatility exit: {e}")
    
    async def _check_regime_exit(self, strategy: ExitStrategy):
        """Check if position should exit based on regime change"""
        try:
            regime_data = await self.regime.get_regime_data(strategy.symbol)
            current_regime = regime_data.get("current_regime", "unknown")
            
            # Exit if regime is unfavorable
            if strategy.action.upper() == "BUY" and current_regime == "trending_down":
                await self._execute_exit(strategy.position_id, ExitReason.REGIME_EXIT, f"Regime changed to {current_regime}")
            elif strategy.action.upper() == "SELL" and current_regime == "trending_up":
                await self._execute_exit(strategy.position_id, ExitReason.REGIME_EXIT, f"Regime changed to {current_regime}")
                
        except Exception as e:
            logger.error(f"Error checking regime exit: {e}")
    
    async def get_position_summary(self, position_id: str) -> Optional[Dict]:
        """Get summary of exit strategy for a position"""
        try:
            strategy = await self.get_exit_strategy(position_id)
            if not strategy:
                return None
            
            # Get current position data
            position_data = await self._get_position_data(position_id)
            current_pnl = position_data.get('unrealized_pnl', 0.0) if position_data else 0.0
            
            # Calculate profit in ATR terms
            current_price = await self.oanda_service.get_current_price(strategy.symbol, strategy.action)
            if current_price:
                if strategy.action.upper() == "BUY":
                    profit_atr = (current_price - strategy.entry_price) / strategy.atr_value
                else:
                    profit_atr = (strategy.entry_price - current_price) / strategy.atr_value
            else:
                profit_atr = 0.0
            
            return {
                "position_id": strategy.position_id,
                "symbol": strategy.symbol,
                "action": strategy.action,
                "entry_price": strategy.entry_price,
                "entry_time": strategy.entry_time.isoformat(),
                "current_pnl": current_pnl,
                "profit_atr": profit_atr,
                "stop_loss_price": strategy.stop_loss_price,
                "take_profit_price": strategy.take_profit_price,
                "trailing_stop_enabled": strategy.trailing_stop_enabled,
                "trailing_stop_price": strategy.trailing_stop_price,
                "breakeven_enabled": strategy.breakeven_enabled,
                "breakeven_price": strategy.breakeven_price,
                "override_fired": strategy.override_fired,
                "initial_risk_amount": strategy.initial_risk_amount,
                "current_risk_amount": strategy.current_risk_amount,
                "last_update": strategy.last_update.isoformat() if strategy.last_update else None
            }
            
        except Exception as e:
            logger.error(f"Error getting position summary for {position_id}: {e}")
            return None

# Factory function for creating unified exit manager
def create_unified_exit_manager(position_tracker, oanda_service, regime_classifier, volatility_monitor, monitor_interval=30):
    """Create and return a unified exit manager instance"""
    return UnifiedExitManager(
        position_tracker=position_tracker,
        oanda_service=oanda_service,
        regime_classifier=regime_classifier,
        volatility_monitor=volatility_monitor,
        monitor_interval=monitor_interval
    )
