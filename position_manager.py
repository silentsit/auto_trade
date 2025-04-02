"""
Consolidated Position Management System

This module provides a unified approach to position tracking and management,
consolidating multiple separate trackers into a single source of truth.
"""

import os
import uuid
import asyncio
import logging
import time
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Union, List, Tuple, Callable
from functools import wraps
from pytz import timezone
import statistics
import numpy as np

# Constants
TIMEFRAME_TAKE_PROFIT_LEVELS = {
    "15M": [0.3, 0.3, 0.4],  # 30%, 30%, 40% of position
    "1H": [0.25, 0.25, 0.5],  # 25%, 25%, 50% of position
    "4H": [0.2, 0.3, 0.5],    # 20%, 30%, 50% of position
    "1D": [0.2, 0.2, 0.6]     # 20%, 20%, 60% of position
}

TIMEFRAME_TRAILING_SETTINGS = {
    "15M": {"activation": 1.5, "distance": 1.0},  # Activate at 1.5R, trail at 1.0R
    "1H": {"activation": 1.5, "distance": 1.25},   # Activate at 1.5R, trail at 1.25R
    "4H": {"activation": 2.0, "distance": 1.5},    # Activate at 2.0R, trail at 1.5R
    "1D": {"activation": 2.0, "distance": 2.0}     # Activate at 2.0R, trail at 2.0R
}

TIMEFRAME_TIME_STOPS = {
    "15M": {"bars": 20, "breakeven": 10, "partial": 5},
    "1H": {"bars": 12, "breakeven": 6, "partial": 3},
    "4H": {"bars": 10, "breakeven": 5, "partial": 2},
    "1D": {"bars": 5, "breakeven": 3, "partial": 1}
}

MAX_DAILY_LOSS = 0.20  # 20% max daily loss

# Configure logger
logger = logging.getLogger("position_manager")

def handle_async_errors(func):
    """
    Decorator for handling errors in async functions.
    Logs errors and maintains proper error propagation.
    """
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}", exc_info=True)
            raise
    return wrapper

class TrackedPosition:
    """
    Comprehensive position tracking object that consolidates all position data
    and state in a single object.
    """
    def __init__(
        self,
        symbol: str,
        action: str,
        entry_price: float,
        units: float,
        timeframe: str,
        atr: float,
        account_balance: float,
        entry_time: datetime = None
    ):
        # Basic position info
        self.symbol = symbol
        self.position_type = 'LONG' if action.upper() == 'BUY' else 'SHORT'
        self.entry_price = entry_price
        self.initial_units = units
        self.current_units = units
        self.timeframe = timeframe
        
        # Time tracking
        self.entry_time = entry_time or datetime.now(timezone('Asia/Bangkok'))
        self.last_update = self.entry_time
        self.bars_held = 0
        self.bar_times = [self.entry_time]
        
        # Risk management
        self.atr = atr
        self.instrument_type = self._get_instrument_type()
        self.atr_multiplier = self._get_atr_multiplier()
        
        # Stop and target levels
        self.stop_loss = self._calculate_stop_loss()
        self.take_profits = self._calculate_take_profits()
        self.tp_levels = TIMEFRAME_TAKE_PROFIT_LEVELS.get(timeframe, TIMEFRAME_TAKE_PROFIT_LEVELS["1H"])
        self.exit_levels_hit = []
        self.trailing_stop = None
        
        # Analytics tracking
        self.initial_account_balance = account_balance
        self.risk_amount = abs(entry_price - self.stop_loss) * units
        self.risk_percentage = self.risk_amount / account_balance
        self.trade_result = None
        self.pnl = 0.0
        self.pnl_percentage = 0.0
        self.max_favorable_excursion = 0.0
        self.max_adverse_excursion = 0.0
        
        # Market analytics
        self.market_condition = {}
        self.correlation_factor = 1.0
        
    def _get_instrument_type(self) -> str:
        """Determine instrument type for appropriate ATR multiplier"""
        normalized_symbol = self.symbol
        if any(crypto in normalized_symbol for crypto in ["BTC", "ETH", "XRP", "LTC"]):
            return "CRYPTO"
        elif "XAU" in normalized_symbol:
            return "XAU_USD"
        else:
            return "FOREX"
            
    def _get_atr_multiplier(self) -> float:
        """Get the appropriate ATR multiplier based on instrument type and timeframe"""
        # ATR multipliers based on timeframe and instrument type
        atr_multipliers = {
            "FOREX": {
                "15M": 1.5,
                "1H": 1.75,
                "4H": 2.0,
                "1D": 2.25
            },
            "CRYPTO": {
                "15M": 2.0,
                "1H": 2.25,
                "4H": 2.5,
                "1D": 2.75
            },
            "XAU_USD": {
                "15M": 1.75,
                "1H": 2.0,
                "4H": 2.25,
                "1D": 2.5
            }
        }
        
        return atr_multipliers[self.instrument_type].get(
            self.timeframe, 
            atr_multipliers[self.instrument_type]["1H"]
        )
        
    def _calculate_stop_loss(self) -> float:
        """Calculate initial stop loss based on ATR"""
        if self.position_type == "LONG":
            return self.entry_price - (self.atr * self.atr_multiplier)
        else:  # SHORT
            return self.entry_price + (self.atr * self.atr_multiplier)
            
    def _calculate_take_profits(self) -> List[float]:
        """Calculate take profit levels based on ATR"""
        if self.position_type == "LONG":
            return [
                self.entry_price + (self.atr * self.atr_multiplier),  # 1:1
                self.entry_price + (self.atr * self.atr_multiplier * 2),  # 2:1
                self.entry_price + (self.atr * self.atr_multiplier * 3)  # 3:1
            ]
        else:  # SHORT
            return [
                self.entry_price - (self.atr * self.atr_multiplier),  # 1:1
                self.entry_price - (self.atr * self.atr_multiplier * 2),  # 2:1
                self.entry_price - (self.atr * self.atr_multiplier * 3)  # 3:1
            ]
            
    def update_with_current_price(self, current_price: float) -> Dict[str, Any]:
        """Update position with current price and return necessary actions"""
        actions = {}
        
        # Calculate current P&L
        self._update_pnl(current_price)
        
        # Check for stop loss hit
        if self._check_stop_loss_hit(current_price):
            actions['stop_loss'] = True
            return actions
            
        # Check for take-profit levels
        tp_actions = self._check_take_profits(current_price)
        if tp_actions:
            actions['take_profits'] = tp_actions
            
        # Update trailing stop if applicable
        trailing_action = self._update_trailing_stop(current_price)
        if trailing_action:
            actions['trailing_stop'] = trailing_action
            
        # Check time-based adjustments
        time_action = self._check_time_adjustments()
        if time_action:
            actions['time_adjustment'] = time_action
            
        return actions
        
    def _update_pnl(self, current_price: float) -> None:
        """Update P&L calculations"""
        # Calculate raw P&L
        if self.position_type == "LONG":
            self.pnl = (current_price - self.entry_price) * self.current_units
            # Update max favorable/adverse excursion
            self.max_favorable_excursion = max(self.max_favorable_excursion, current_price - self.entry_price)
            self.max_adverse_excursion = min(self.max_adverse_excursion, current_price - self.entry_price)
        else:  # SHORT
            self.pnl = (self.entry_price - current_price) * self.current_units
            # Update max favorable/adverse excursion
            self.max_favorable_excursion = max(self.max_favorable_excursion, self.entry_price - current_price)
            self.max_adverse_excursion = min(self.max_adverse_excursion, self.entry_price - current_price)
            
        # Calculate percentage P&L
        self.pnl_percentage = self.pnl / self.initial_account_balance if self.initial_account_balance else 0.0
        
    def _check_stop_loss_hit(self, current_price: float) -> bool:
        """Check if stop loss has been hit"""
        # If we have a trailing stop, use that instead of the initial stop
        stop_price = self.trailing_stop if self.trailing_stop else self.stop_loss
        
        if self.position_type == "LONG" and current_price <= stop_price:
            return True
        elif self.position_type == "SHORT" and current_price >= stop_price:
            return True
            
        return False
        
    def _check_take_profits(self, current_price: float) -> Dict[str, Any]:
        """Check if take profit levels have been hit"""
        result = {}
        
        for i, (tp_price, tp_level) in enumerate(zip(self.take_profits, self.tp_levels)):
            # Skip if this level has already been hit
            if i in self.exit_levels_hit:
                continue
                
            # Check if level has been hit
            hit = False
            if self.position_type == "LONG" and current_price >= tp_price:
                hit = True
            elif self.position_type == "SHORT" and current_price <= tp_price:
                hit = True
                
            if hit:
                result[f"tp{i+1}"] = {
                    "price": tp_price,
                    "percentage": tp_level,
                    "units": self.current_units * tp_level
                }
                self.exit_levels_hit.append(i)
                
        return result
        
    def _update_trailing_stop(self, current_price: float) -> Optional[Dict[str, Any]]:
        """Update trailing stop if applicable"""
        # Get trailing settings for this timeframe
        settings = TIMEFRAME_TRAILING_SETTINGS.get(self.timeframe, TIMEFRAME_TRAILING_SETTINGS["1H"])
        activation_distance = self.atr * self.atr_multiplier * settings["activation"]
        trail_distance = self.atr * self.atr_multiplier * settings["distance"]
        
        # Check if price has moved enough to activate trailing stop
        if self.position_type == "LONG":
            if current_price >= self.entry_price + activation_distance:
                new_stop = current_price - trail_distance
                
                # Only update if it would raise the stop
                if not self.trailing_stop or new_stop > self.trailing_stop:
                    old_stop = self.trailing_stop or self.stop_loss
                    self.trailing_stop = new_stop
                    return {
                        "old_stop": old_stop,
                        "new_stop": new_stop,
                        "price": current_price
                    }
        else:  # SHORT
            if current_price <= self.entry_price - activation_distance:
                new_stop = current_price + trail_distance
                
                # Only update if it would lower the stop
                if not self.trailing_stop or new_stop < self.trailing_stop:
                    old_stop = self.trailing_stop or self.stop_loss
                    self.trailing_stop = new_stop
                    return {
                        "old_stop": old_stop,
                        "new_stop": new_stop,
                        "price": current_price
                    }
                    
        return None
        
    def _check_time_adjustments(self) -> Optional[Dict[str, Any]]:
        """Check if any time-based adjustments should be made"""
        # Get time settings for this timeframe
        settings = TIMEFRAME_TIME_STOPS.get(self.timeframe, TIMEFRAME_TIME_STOPS["1H"])
        
        # Calculate position duration in bars
        duration = len(self.bar_times)
        
        # If we've held longer than breakeven threshold, move stop to breakeven
        if duration >= settings["breakeven"] and not self.trailing_stop:
            self.trailing_stop = self.entry_price
            return {
                "type": "breakeven",
                "at_bar": duration,
                "stop": self.entry_price
            }
            
        # If we've held too long, suggest closing the position
        if duration >= settings["bars"]:
            return {
                "type": "time_exit",
                "at_bar": duration,
                "max_bars": settings["bars"]
            }
            
        return None
        
    def record_bar_update(self) -> None:
        """Record a new bar update"""
        current_time = datetime.now(timezone('Asia/Bangkok'))
        self.bar_times.append(current_time)
        self.last_update = current_time
        self.bars_held = len(self.bar_times)
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert position to a dictionary for storage/display"""
        return {
            'symbol': self.symbol,
            'position_type': self.position_type,
            'entry_price': self.entry_price,
            'current_units': self.current_units,
            'initial_units': self.initial_units,
            'timeframe': self.timeframe,
            'entry_time': self.entry_time.isoformat(),
            'last_update': self.last_update.isoformat(),
            'bars_held': self.bars_held,
            'stop_loss': self.stop_loss,
            'trailing_stop': self.trailing_stop,
            'take_profits': self.take_profits,
            'exit_levels_hit': self.exit_levels_hit,
            'risk_amount': self.risk_amount,
            'risk_percentage': self.risk_percentage,
            'pnl': self.pnl,
            'pnl_percentage': self.pnl_percentage,
            'max_favorable_excursion': self.max_favorable_excursion,
            'max_adverse_excursion': self.max_adverse_excursion
        }


class PositionManager:
    """
    Unified position management system that consolidates tracking, risk management,
    and analysis into a single source of truth.
    """
    def __init__(self):
        self.positions = {}  # symbol -> TrackedPosition
        self._lock = asyncio.Lock()
        self._running = False
        self._initialized = False
        self.daily_pnl = 0.0
        self.pnl_reset_date = datetime.now().date()
        self.portfolio_heat = 0.0
        self.reconciliation_task = None
        
    @handle_async_errors
    async def start(self):
        """Initialize and start the position manager"""
        if not self._initialized:
            async with self._lock:
                if not self._initialized:  # Double-check pattern
                    self._running = True
                    self.reconciliation_task = asyncio.create_task(self.reconcile_positions())
                    self._initialized = True
                    logger.info("Position manager started")

    @handle_async_errors
    async def stop(self):
        """Gracefully stop the position manager"""
        self._running = False
        if self.reconciliation_task:
            self.reconciliation_task.cancel()
            try:
                await self.reconciliation_task
            except asyncio.CancelledError:
                logger.info("Position reconciliation task cancelled")
            except Exception as e:
                logger.error(f"Error stopping reconciliation task: {str(e)}")
        logger.info("Position manager stopped")

    @handle_async_errors
    async def reconcile_positions(self):
        """Reconcile positions with broker"""
        while self._running:
            try:
                # Wait between reconciliation attempts
                await asyncio.sleep(900)  # Every 15 minutes
                
                logger.info("Starting position reconciliation")
                async with self._lock:
                    try:
                        # This would call get_open_positions from your API module
                        # Here, I'm using a placeholder
                        success, positions_data = await get_open_positions()
                        
                        if not success:
                            logger.error("Failed to fetch positions for reconciliation")
                            continue
                        
                        # Extract broker positions
                        broker_positions = {
                            p["instrument"]: p for p in positions_data.get("positions", [])
                        }
                        
                        # Check each tracked position
                        for symbol in list(self.positions.keys()):
                            if symbol not in broker_positions:
                                # Position closed externally
                                old_position = self.positions.pop(symbol, None)
                                logger.warning(
                                    f"Removing stale position for {symbol}. "
                                    f"Position was: {old_position.to_dict() if old_position else 'Unknown'}"
                                )
                        
                        logger.info(
                            f"Reconciliation complete. Active positions: "
                            f"{list(self.positions.keys())}"
                        )
                    except asyncio.TimeoutError:
                        logger.error("Position reconciliation timed out")
                    except Exception as e:
                        logger.error(f"Error in reconciliation: {str(e)}")
                        
            except asyncio.CancelledError:
                logger.info("Position reconciliation task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in reconciliation loop: {str(e)}")
                await asyncio.sleep(60)  # Wait before retrying on unexpected errors

    @handle_async_errors
    async def initialize_position(
        self,
        symbol: str,
        action: str,
        entry_price: float,
        units: float,
        timeframe: str,
        atr: float,
        account_balance: float
    ) -> bool:
        """Initialize a new position with all necessary tracking"""
        try:
            async with self._lock:
                position = TrackedPosition(
                    symbol=symbol,
                    action=action,
                    entry_price=entry_price,
                    units=units,
                    timeframe=timeframe,
                    atr=atr,
                    account_balance=account_balance
                )
                
                self.positions[symbol] = position
                
                # Update portfolio heat
                self.portfolio_heat += units * entry_price
                
                logger.info(f"Initialized position for {symbol}: {position.to_dict()}")
                return True
                
        except Exception as e:
            logger.error(f"Error initializing position for {symbol}: {str(e)}")
            return False

    @handle_async_errors
    async def update_position(self, symbol: str, current_price: float) -> Dict[str, Any]:
        """Update position with current price and return necessary actions"""
        actions = {}
        
        async with self._lock:
            position = self.positions.get(symbol)
            if not position:
                return actions
                
            # Update position with current price
            position_actions = position.update_with_current_price(current_price)
            
            # Process any actions needed
            if position_actions:
                actions = position_actions
                actions['symbol'] = symbol
                actions['position'] = position.to_dict()
                
            return actions

    @handle_async_errors
    async def clear_position(self, symbol: str, final_price: float = None, pnl: float = None) -> bool:
        """Clear a position and record final results"""
        try:
            async with self._lock:
                position = self.positions.pop(symbol, None)
                if not position:
                    logger.warning(f"Attempted to clear non-existent position: {symbol}")
                    return False
                    
                # If PnL is provided, record it
                if pnl is not None:
                    await self.record_trade_pnl(pnl)
                    position.pnl = pnl
                    
                # If final price is provided, update position's PnL
                elif final_price is not None:
                    position._update_pnl(final_price)
                    await self.record_trade_pnl(position.pnl)
                
                # Update portfolio heat
                self.portfolio_heat -= position.current_units * position.entry_price
                
                logger.info(f"Cleared position for {symbol}: {position.to_dict()}")
                return True
                
        except Exception as e:
            logger.error(f"Error clearing position for {symbol}: {str(e)}")
            return False

    @handle_async_errors
    async def record_trade_pnl(self, pnl: float) -> None:
        """Record P&L from a trade and reset daily if needed"""
        async with self._lock:
            current_date = datetime.now().date()
            
            # Reset daily P&L if it's a new day
            if current_date != self.pnl_reset_date:
                logger.info(f"Resetting daily P&L (was {self.daily_pnl}) for new day: {current_date}")
                self.daily_pnl = 0.0
                self.pnl_reset_date = current_date
            
            # Add the P&L to today's total
            self.daily_pnl += pnl
            logger.info(f"Updated daily P&L: {self.daily_pnl}")

    async def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get position info for a symbol"""
        async with self._lock:
            position = self.positions.get(symbol)
            if position:
                return position.to_dict()
            return None

    async def get_all_positions(self) -> Dict[str, Dict[str, Any]]:
        """Get all positions"""
        async with self._lock:
            return {
                symbol: position.to_dict() 
                for symbol, position in self.positions.items()
            }

    async def get_daily_pnl(self) -> float:
        """Get current daily P&L"""
        async with self._lock:
            # Reset if it's a new day
            current_date = datetime.now().date()
            if current_date != self.pnl_reset_date:
                self.daily_pnl = 0.0
                self.pnl_reset_date = current_date
            
            return self.daily_pnl

    async def check_max_daily_loss(self, account_balance: float) -> Tuple[bool, float]:
        """Check daily loss percentage"""
        daily_pnl = await self.get_daily_pnl()
        loss_percentage = abs(min(0, daily_pnl)) / account_balance
        
        # Log the information
        if loss_percentage > MAX_DAILY_LOSS * 0.5:  # Warn at 50% of the limit
            logger.warning(f"Daily loss at {loss_percentage:.2%} of account (limit: {MAX_DAILY_LOSS:.2%})")
        
        # Return whether trading should continue and the current loss percentage
        return loss_percentage < MAX_DAILY_LOSS, loss_percentage

    async def calculate_position_size(
        self, 
        account_balance: float,
        entry_price: float,
        stop_loss: float,
        atr: float,
        timeframe: str,
        market_condition: Dict[str, Any] = None,
        correlation_factor: float = 1.0
    ) -> float:
        """Calculate position size based on risk parameters"""
        # Calculate risk amount (2% of account balance)
        risk_amount = account_balance * 0.02
        
        # Adjust risk based on market condition
        if market_condition:
            volatility_adjustment = market_condition.get('volatility_ratio', 1.0)
            if market_condition.get('volatility_state') == 'high':
                risk_amount *= 0.75  # Reduce risk by 25% in high volatility
            elif market_condition.get('volatility_state') == 'low':
                risk_amount *= 1.25  # Increase risk by 25% in low volatility
        
        # Adjust for correlation
        risk_amount *= correlation_factor
        
        # Determine if this is a crypto instrument
        is_crypto = any(crypto in str(entry_price) for crypto in ["BTC", "ETH", "XRP", "LTC"])
        
        # Additional adjustment for crypto due to higher volatility
        if is_crypto:
            risk_amount *= 0.8  # Additional 20% reduction for crypto
        
        # Calculate position size based on risk
        risk_per_unit = abs(entry_price - stop_loss)
        if risk_per_unit == 0 or risk_per_unit < 0.00001:  # Prevent division by zero or very small values
            risk_per_unit = atr  # Use ATR as a fallback
            
        position_size = risk_amount / risk_per_unit
        
        # Round to appropriate precision
        if timeframe in ["15M", "1H"]:
            position_size = round(position_size, 2)
        else:
            position_size = round(position_size, 1)
            
        return position_size

    async def get_correlation_factor(self, symbol: str, existing_positions: List[str]) -> float:
        """Calculate correlation factor based on existing positions"""
        if not existing_positions:
            return 1.0
            
        # Find similar pairs (same base or quote currency)
        similar_pairs = 0
        for pos in existing_positions:
            # Check if they share the same base or quote currency (assumes format like 'EUR_USD')
            symbol_parts = symbol.split('_')
            pos_parts = pos.split('_')
            
            if len(symbol_parts) > 1 and len(pos_parts) > 1:
                if symbol_parts[0] == pos_parts[0] or symbol_parts[1] == pos_parts[1]:
                    similar_pairs += 1
        
        # Reduce correlation factor based on number of similar pairs
        if similar_pairs > 0:
            return max(0.5, 1.0 - (similar_pairs * 0.1))  # Minimum correlation factor of 0.5
        return 1.0

    async def get_portfolio_heat(self) -> float:
        """Get current portfolio heat"""
        return self.portfolio_heat

    async def update_portfolio_heat(self, position_size_delta: float):
        """Update portfolio heat with position size change"""
        self.portfolio_heat += position_size_delta
        logger.info(f"Updated portfolio heat: {self.portfolio_heat}")

# Create a singleton instance
position_manager = PositionManager()

# The following functions are placeholders and should be implemented
# based on your actual API broker interaction module

async def get_open_positions():
    """
    Placeholder for actual implementation to get positions from broker
    Should return (success: bool, positions_data: Dict)
    """
    # This should be implemented according to your broker API
    return False, {"positions": []} 