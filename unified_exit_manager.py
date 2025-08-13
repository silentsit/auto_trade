"""
UNIFIED EXIT MANAGER
Consolidates profit riding, dynamic exit management, and volatility-adaptive trailing stops
into a single, efficient module for better performance and maintainability.
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta

from utils import get_atr, get_instrument_type
from regime_classifier import LorentzianDistanceClassifier
from volatility_monitor import VolatilityMonitor
from position_journal import Position
from oanda_service import OandaService
from tracker import PositionTracker

logger = logging.getLogger(__name__)

# ============================================================================
# === DATA STRUCTURES ===
# ============================================================================

@dataclass
class TrailingStopConfig:
    """Configuration for volatility-adaptive trailing stops"""
    # Initial stop loss (ATR-based)
    initial_stop_atr_multiplier: float = 2.0
    
    # Take profit target (1:2 risk-reward)
    take_profit_atr_multiplier: float = 4.0  # 2x the stop loss distance
    
    # Trailing stop activation
    trailing_activation_atr_multiplier: float = 1.0  # Start trailing at +1R
    
    # Trailing stop distance (ATR-based)
    trailing_stop_atr_multiplier: float = 1.5  # Tighter than initial stop
    
    # Breakeven activation
    breakeven_activation_atr_multiplier: float = 1.0  # Move to breakeven at +1R
    
    # Maximum hold time (timeframe-specific)
    max_hold_time_hours: int = 8

@dataclass
class OverrideDecision:
    """Decision about whether to override normal close signals"""
    ignore_close: bool
    sl_atr_multiplier: float = 2.0
    tp_atr_multiplier: float = 4.0
    reason: str = ""
    trailing_stop_enabled: bool = False

@dataclass
class ExitStrategy:
    """Complete exit strategy configuration for a position"""
    position_id: str
    symbol: str
    action: str
    entry_price: float
    entry_time: datetime
    atr_value: float
    timeframe: str
    
    # Stop Loss Configuration
    stop_loss_price: float
    stop_loss_atr_multiplier: float
    
    # Take Profit Configuration (1:2 risk-reward)
    take_profit_price: float
    take_profit_atr_multiplier: float
    
    # Trailing Stop Configuration
    trailing_stop_enabled: bool = False
    trailing_stop_price: Optional[float] = None
    trailing_stop_atr_multiplier: float = 1.5
    
    # Breakeven Configuration
    breakeven_enabled: bool = False
    breakeven_price: Optional[float] = None
    
    # Risk Management
    initial_risk_amount: float  # Risk in currency terms
    current_risk_amount: float  # Current risk (can be 0 if at breakeven)
    
    # Market Regime Configuration
    market_regime: str = "unknown"
    volatility_ratio: float = 1.0
    
    # Status Tracking
    override_fired: bool = False
    trailing_stop_activated: bool = False
    breakeven_activated: bool = False
    last_update: datetime = None

# ============================================================================
# === UNIFIED EXIT MANAGER ===
# ============================================================================

class UnifiedExitManager:
    """
    Unified exit manager that handles:
    1. Profit riding overrides
    2. Volatility-adaptive trailing stops
    3. Breakeven management at +1R
    4. Dynamic exit management based on market conditions
    5. Time-based exit rules
    """
    
    def __init__(self, 
                 regime_classifier: LorentzianDistanceClassifier,
                 volatility_monitor: VolatilityMonitor,
                 oanda_service: OandaService,
                 position_tracker: PositionTracker):
        
        self.regime = regime_classifier
        self.vol = volatility_monitor
        self.oanda_service = oanda_service
        self.position_tracker = position_tracker
        
        # Monitoring state
        self.monitoring = False
        self.monitor_interval = 30  # Check every 30 seconds
        self._monitor_task = None
        
        # Exit strategies cache
        self.exit_strategies: Dict[str, ExitStrategy] = {}
        self._lock = asyncio.Lock()
        
        logger.info("🚀 Unified Exit Manager initialized")

    # ============================================================================
    # === TRAILING STOP CONFIGURATION ===
    # ============================================================================
    
    def _get_trailing_stop_config(self, timeframe: str) -> TrailingStopConfig:
        """
        Get timeframe-specific trailing stop configuration
        Optimized for 15M, 1H, and 4H timeframes
        """
        if timeframe.upper() in ["15", "15M", "15MIN"]:
            # 15M: Aggressive trailing, quick breakeven
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

    # ============================================================================
    # === PROFIT RIDE OVERRIDE LOGIC ===
    # ============================================================================
    
    async def should_override_close(self, position: Position, current_price: float, drawdown: float = 0.0) -> OverrideDecision:
        """
        Determine if we should override a close signal to let profits ride
        Based on market conditions, volatility, and position profitability
        """
        # --- Risk Control: Only fire once per position ---
        if position.metadata.get('profit_ride_override_fired', False):
            return OverrideDecision(ignore_close=False, reason="Override already fired")
        
        # --- Risk Control: Check account drawdown ---
        if drawdown > 0.70:  # 70% drawdown limit
            return OverrideDecision(ignore_close=False, reason="Account drawdown limit exceeded")
        
        # --- Risk Control: Check position age ---
        position_age_hours = (datetime.now(timezone.utc) - position.entry_time).total_seconds() / 3600
        if position_age_hours > 8:  # 8 hour maximum hold time
            return OverrideDecision(ignore_close=False, reason="Maximum hold time exceeded")
        
        # --- Profit Threshold Check ---
        entry_price = position.entry_price
        current_pnl = position.unrealized_pnl
        
        # Calculate risk amount (stop loss distance * position size)
        atr_value = await get_atr(position.symbol, position.timeframe)
        if not atr_value:
            return OverrideDecision(ignore_close=False, reason="Could not get ATR value")
        
        # Get trailing stop configuration
        config = self._get_trailing_stop_config(position.timeframe)
        stop_distance = atr_value * config.initial_stop_atr_multiplier
        risk_amount = abs(stop_distance * position.units)
        
        # Must be profitable enough to justify override
        if current_pnl < risk_amount:
            return OverrideDecision(ignore_close=False, reason="Insufficient profit for override")
        
        # --- Market Condition Checks ---
        # Get current market regime and volatility
        try:
            regime_info = await self.regime.get_current_regime(position.symbol)
            vol_ratio = await self.vol.get_volatility_ratio(position.symbol)
        except Exception as e:
            logger.warning(f"Could not get market conditions: {e}")
            # Default to conservative override
            regime_info = {"regime": "unknown"}
            vol_ratio = 1.0
        
        # --- Override Decision Logic ---
        should_override = False
        override_reason = ""
        
        # Check if market is trending in our favor
        if position.action.upper() == "BUY" and regime_info.get("regime") == "trending_up":
            should_override = True
            override_reason = "Bullish trending market"
        elif position.action.upper() == "SELL" and regime_info.get("regime") == "trending_down":
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
        if position.action.upper() == "BUY":
            momentum = (current_price - entry_price) / atr_value
        else:
            momentum = (entry_price - current_price) / atr_value
        
        if momentum > 0.15:  # Strong momentum
            if should_override:
                override_reason += " + Strong momentum"
            else:
                should_override = True
                override_reason = "Strong momentum"
        
        if should_override:
            # Mark override as fired
            position.metadata['profit_ride_override_fired'] = True
            
            logger.info(f"🎯 Profit ride override FIRED for {position.symbol}: {override_reason}")
            
            return OverrideDecision(
                ignore_close=True,
                sl_atr_multiplier=config.initial_stop_atr_multiplier,
                tp_atr_multiplier=config.take_profit_atr_multiplier,
                reason=override_reason,
                trailing_stop_enabled=True
            )
        else:
            return OverrideDecision(ignore_close=False, reason="Market conditions don't support override")

    # ============================================================================
    # === TRAILING STOP MANAGEMENT ===
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
        """Main monitoring loop for all exit strategies"""
        while self.monitoring:
            try:
                await self._check_all_positions()
                await asyncio.sleep(self.monitor_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in unified exit monitor loop: {e}")
                await asyncio.sleep(self.monitor_interval)
                
    async def _check_all_positions(self):
        """Check all active positions for exit triggers"""
        try:
            # Get all open positions
            open_positions_nested = await self.position_tracker.get_open_positions()
            active_positions = {}
            for symbol, positions in open_positions_nested.items():
                for position_id, position_data in positions.items():
                    active_positions[position_id] = position_id
            
            for position_id, position_data in active_positions.items():
                # Check trailing stop and breakeven conditions
                await self._check_trailing_stop_conditions(position_id, position_data)
                
                # Check dynamic exit conditions
                await self._check_dynamic_exit_conditions(position_id, position_data)
                    
        except Exception as e:
            logger.error(f"Error checking positions for exits: {e}")
            
    async def _check_trailing_stop_conditions(self, position_id: str, position_data: Dict):
        """Check and update trailing stop conditions for a position"""
        try:
            # Get current exit strategy
            strategy = await self.get_exit_strategy(position_id)
            if not strategy:
                return
            
            # Get current price
            current_price = await self.oanda_service.get_current_price(strategy.symbol, strategy.action)
            if not current_price:
                return
            
            # Get current ATR for volatility adaptation
            current_atr = await get_atr(strategy.symbol, strategy.timeframe)
            if not current_atr:
                return
            
            # Calculate current profit in ATR terms
            if strategy.action.upper() == "BUY":
                profit_atr = (current_price - strategy.entry_price) / current_atr
            else:
                profit_atr = (strategy.entry_price - current_price) / current_atr
            
            # Get trailing stop configuration
            config = self._get_trailing_stop_config(strategy.timeframe)
            
            # --- Trailing Stop Activation Logic ---
            if not strategy.trailing_stop_activated and profit_atr >= config.trailing_activation_atr_multiplier:
                # Activate trailing stop at +1R
                strategy.trailing_stop_activated = True
                strategy.trailing_stop_enabled = True
                
                # Set initial trailing stop price
                if strategy.action.upper() == "BUY":
                    strategy.trailing_stop_price = current_price - (current_atr * config.trailing_stop_atr_multiplier)
                else:
                    strategy.trailing_stop_price = current_price + (current_atr * config.trailing_stop_atr_multiplier)
                
                logger.info(f"🎯 Trailing stop activated for {strategy.symbol} at +{profit_atr:.2f}R")
            
            # --- Breakeven Logic ---
            if not strategy.breakeven_activated and profit_atr >= config.breakeven_activation_atr_multiplier:
                # Move stop loss to breakeven at +1R
                strategy.breakeven_activated = True
                strategy.breakeven_enabled = True
                strategy.breakeven_price = strategy.entry_price
                
                # Update current risk amount
                strategy.current_risk_amount = 0.0
                
                logger.info(f"💰 Breakeven activated for {strategy.symbol} at +{profit_atr:.2f}R")
            
            # --- Trailing Stop Update Logic ---
            if strategy.trailing_stop_activated and strategy.trailing_stop_enabled:
                # Update trailing stop if price moves in our favor
                if strategy.action.upper() == "BUY":
                    # Long position - trail upward
                    new_trailing_stop = current_price - (current_atr * config.trailing_stop_atr_multiplier)
                    if new_trailing_stop > strategy.trailing_stop_price:
                        strategy.trailing_stop_price = new_trailing_stop
                        logger.debug(f"📈 Updated trailing stop for {strategy.symbol}: {new_trailing_stop:.5f}")
                else:
                    # Short position - trail downward
                    new_trailing_stop = current_price + (current_atr * config.trailing_stop_atr_multiplier)
                    if new_trailing_stop < strategy.trailing_stop_price:
                        strategy.trailing_stop_price = new_trailing_stop
                        logger.debug(f"📉 Updated trailing stop for {strategy.symbol}: {new_trailing_stop:.5f}")
            
            # --- Stop Loss Trigger Check ---
            should_close = False
            close_reason = ""
            
            # Check if price hit the trailing stop
            if strategy.trailing_stop_enabled and strategy.trailing_stop_price:
                if strategy.action.upper() == "BUY":
                    if current_price <= strategy.trailing_stop_price:
                        should_close = True
                        close_reason = "Trailing stop hit"
                else:
                    if current_price >= strategy.trailing_stop_price:
                        should_close = True
                        close_reason = "Trailing stop hit"
            
            # Check if price hit the take profit
            if current_price >= strategy.take_profit_price if strategy.action.upper() == "BUY" else current_price <= strategy.take_profit_price:
                should_close = True
                close_reason = "Take profit reached"
            
            # Execute close if needed
            if should_close:
                await self._close_position(position_id, close_reason)
                
        except Exception as e:
            logger.error(f"Error checking trailing stop conditions for {position_id}: {e}")

    # ============================================================================
    # === DYNAMIC EXIT MANAGEMENT ===
    # ============================================================================
    
    async def _check_dynamic_exit_conditions(self, position_id: str, position_data: Dict):
        """Check dynamic exit conditions for a position"""
        try:
            # Check time-based exits
            await self._check_time_based_exits(position_id, position_data)
            
            # Check volatility-based exits
            await self._check_volatility_based_exits(position_id, position_data)
            
            # Check regime-based exits
            await self._check_regime_based_exits(position_id, position_data)
            
        except Exception as e:
            logger.error(f"Error checking dynamic exit conditions for {position_id}: {e}")
    
    async def _check_time_based_exits(self, position_id: str, position_data: Dict):
        """Check if position should be closed based on time in trade"""
        try:
            entry_time = position_data.get('entry_time')
            if not entry_time:
                return
            
            # Convert to datetime if it's a string
            if isinstance(entry_time, str):
                entry_time = datetime.fromisoformat(entry_time.replace('Z', '+00:00'))
            
            position_age_hours = (datetime.now(timezone.utc) - entry_time).total_seconds() / 3600
            
            # Get timeframe-specific max hold time
            timeframe = position_data.get('timeframe', '1H')
            if timeframe.upper() in ["15", "15M", "15MIN"]:
                max_hold_time = 8  # 8 hours for 15M
            elif timeframe.upper() in ["1H", "1HR", "60"]:
                max_hold_time = 24  # 24 hours for 1H
            else:  # 4H+
                max_hold_time = 48  # 48 hours for 4H
            
            if position_age_hours > max_hold_time:
                logger.info(f"⏰ Time-based exit triggered for {position_id}: {position_age_hours:.1f}h > {max_hold_time}h")
                await self._close_position(position_id, "Time-based exit")
                
        except Exception as e:
            logger.error(f"Error checking time-based exits for {position_id}: {e}")
    
    async def _check_volatility_based_exits(self, position_id: str, position_data: Dict):
        """Check if position should be closed based on volatility changes"""
        try:
            symbol = position_data['symbol']
            current_vol_ratio = await self.vol.get_volatility_ratio(symbol)
            
            if current_vol_ratio > 2.0:  # High volatility
                # Check if position is profitable enough to justify holding
                current_pnl = position_data.get('unrealized_pnl', 0)
                if current_pnl < 0:  # Losing position in high volatility
                    logger.info(f"📊 Volatility-based exit triggered for {position_id}: vol_ratio={current_vol_ratio:.2f}")
                    await self._close_position(position_id, "High volatility exit")
                    
        except Exception as e:
            logger.error(f"Error checking volatility-based exits for {position_id}: {e}")
    
    async def _check_regime_based_exits(self, position_id: str, position_data: Dict):
        """Check if position should be closed based on market regime changes"""
        try:
            symbol = position_data['symbol']
            current_regime = await self.regime.get_current_regime(symbol)
            
            if not current_regime:
                return
            
            action = position_data['action']
            regime = current_regime.get('regime', 'unknown')
            
            # Close if market regime is unfavorable
            if action.upper() == "BUY" and regime == "trending_down":
                logger.info(f"📈 Regime-based exit triggered for {position_id}: regime={regime}")
                await self._close_position(position_id, "Bearish regime exit")
            elif action.upper() == "SELL" and regime == "trending_up":
                logger.info(f"📉 Regime-based exit triggered for {position_id}: regime={regime}")
                await self._close_position(position_id, "Bullish regime exit")
                
        except Exception as e:
            logger.error(f"Error checking regime-based exits for {position_id}: {e}")
    
    async def _close_position(self, position_id: str, reason: str):
        """Close a position with the specified reason"""
        try:
            # This would typically call the position tracker or OANDA service
            # For now, we'll just log the close action
            logger.info(f"🔄 Closing position {position_id}: {reason}")
            
            # You would implement the actual close logic here:
            # await self.position_tracker.close_position(position_id, reason)
            
        except Exception as e:
            logger.error(f"Error closing position {position_id}: {e}")

    # ============================================================================
    # === EXIT STRATEGY MANAGEMENT ===
    # ============================================================================
    
    async def create_exit_strategy(self, position: Position) -> ExitStrategy:
        """Create a complete exit strategy for a new position"""
        try:
            # Get ATR value for the position
            atr_value = await get_atr(position.symbol, position.timeframe)
            if not atr_value:
                logger.error(f"Could not get ATR for {position.symbol}")
                return None
            
            # Get trailing stop configuration
            config = self._get_trailing_stop_config(position.timeframe)
            
            # Calculate stop loss and take profit prices (1:2 risk-reward)
            if position.action.upper() == "BUY":
                stop_loss_price = position.entry_price - (atr_value * config.initial_stop_atr_multiplier)
                take_profit_price = position.entry_price + (atr_value * config.take_profit_atr_multiplier)
            else:
                stop_loss_price = position.entry_price + (atr_value * config.initial_stop_atr_multiplier)
                take_profit_price = position.entry_price - (atr_value * config.take_profit_atr_multiplier)
            
            # Calculate initial risk amount
            initial_risk_amount = abs((stop_loss_price - position.entry_price) * position.units)
            
            # Create exit strategy
            strategy = ExitStrategy(
                position_id=position.position_id,
                symbol=position.symbol,
                action=position.action,
                entry_price=position.entry_price,
                entry_time=position.entry_time,
                atr_value=atr_value,
                timeframe=position.timeframe,
                stop_loss_price=stop_loss_price,
                stop_loss_atr_multiplier=config.initial_stop_atr_multiplier,
                take_profit_price=take_profit_price,
                take_profit_atr_multiplier=config.take_profit_atr_multiplier,
                trailing_stop_enabled=False,
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
                self.exit_strategies[position.position_id] = strategy
            
            logger.info(f"Created exit strategy for {position.symbol} with 1:2 risk-reward ratio")
            return strategy
            
        except Exception as e:
            logger.error(f"Error creating exit strategy for {position.symbol}: {e}")
            return None
    
    async def update_exit_strategy(self, position_id: str, updates: Dict[str, Any]):
        """Update an existing exit strategy"""
        try:
            async with self._lock:
                if position_id in self.exit_strategies:
                    strategy = self.exit_strategies[position_id]
                    for key, value in updates.items():
                        if hasattr(strategy, key):
                            setattr(strategy, key, value)
                    strategy.last_update = datetime.now(timezone.utc)
                    
                    logger.info(f"Updated exit strategy for {position_id}")
                    
        except Exception as e:
            logger.error(f"Error updating exit strategy for {position_id}: {e}")
    
    async def get_exit_strategy(self, position_id: str) -> Optional[ExitStrategy]:
        """Get the exit strategy for a position"""
        async with self._lock:
            return self.exit_strategies.get(position_id)
    
    async def remove_exit_strategy(self, position_id: str):
        """Remove an exit strategy when position is closed"""
        try:
            async with self._lock:
                if position_id in self.exit_strategies:
                    del self.exit_strategies[position_id]
                    logger.info(f"Removed exit strategy for {position_id}")
                    
        except Exception as e:
            logger.error(f"Error removing exit strategy for {position_id}: {e}")

    # ============================================================================
    # === UTILITY METHODS ===
    # ============================================================================
    
    async def get_position_summary(self, position_id: str) -> Dict[str, Any]:
        """Get a summary of exit strategy and status for a position"""
        try:
            strategy = await self.get_exit_strategy(position_id)
            if not strategy:
                return {"error": "No exit strategy found"}
            
            # Get current position data
            position_data = await self.position_tracker.get_position(position_id)
            if not position_data:
                return {"error": "Position not found"}
            
            # Calculate current status
            current_price = await self.oanda_service.get_current_price(strategy.symbol, strategy.action)
            if not current_price:
                return {"error": "Could not get current price"}
            
            # Calculate P&L and drawdown
            if strategy.action.upper() == "BUY":
                pnl = (current_price - strategy.entry_price) * position_data['units']
                drawdown = (strategy.entry_price - current_price) / strategy.entry_price
            else:
                pnl = (strategy.entry_price - current_price) * position_data['units']
                drawdown = (current_price - strategy.entry_price) / strategy.entry_price
            
            # Calculate profit in ATR terms
            current_atr = await get_atr(strategy.symbol, strategy.timeframe)
            if current_atr:
                if strategy.action.upper() == "BUY":
                    profit_atr = (current_price - strategy.entry_price) / current_atr
                else:
                    profit_atr = (strategy.entry_price - current_price) / current_atr
            else:
                profit_atr = 0.0
            
            return {
                "position_id": position_id,
                "symbol": strategy.symbol,
                "action": strategy.action,
                "entry_price": strategy.entry_price,
                "current_price": current_price,
                "unrealized_pnl": pnl,
                "drawdown": drawdown,
                "profit_atr": profit_atr,
                "time_in_trade_hours": (datetime.now(timezone.utc) - strategy.entry_time).total_seconds() / 3600,
                "stop_loss_price": strategy.stop_loss_price,
                "take_profit_price": strategy.take_profit_price,
                "trailing_stop_enabled": strategy.trailing_stop_enabled,
                "trailing_stop_price": strategy.trailing_stop_price,
                "breakeven_enabled": strategy.breakeven_enabled,
                "breakeven_price": strategy.breakeven_price,
                "initial_risk_amount": strategy.initial_risk_amount,
                "current_risk_amount": strategy.current_risk_amount,
                "market_regime": strategy.market_regime,
                "volatility_ratio": strategy.volatility_ratio,
                "override_fired": strategy.override_fired,
                "last_update": strategy.last_update
            }
            
        except Exception as e:
            logger.error(f"Error getting position summary for {position_id}: {e}")
            return {"error": str(e)}
    
    async def get_all_strategies_summary(self) -> List[Dict[str, Any]]:
        """Get summary of all active exit strategies"""
        try:
            summaries = []
            async with self._lock:
                for position_id in self.exit_strategies:
                    summary = await self.get_position_summary(position_id)
                    if "error" not in summary:
                        summaries.append(summary)
            
            return summaries
            
        except Exception as e:
            logger.error(f"Error getting all strategies summary: {e}")
            return []

# ============================================================================
# === FACTORY FUNCTION ===
# ============================================================================

async def create_unified_exit_manager(regime_classifier: LorentzianDistanceClassifier,
                                    volatility_monitor: VolatilityMonitor,
                                    oanda_service: OandaService,
                                    position_tracker: PositionTracker) -> UnifiedExitManager:
    """Factory function to create and initialize a unified exit manager"""
    manager = UnifiedExitManager(regime_classifier, volatility_monitor, oanda_service, position_tracker)
    return manager
