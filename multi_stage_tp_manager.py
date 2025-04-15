"""
Multi-Stage Take Profit Manager for FX Trading

This module provides advanced take profit management capabilities including:
- Multiple profit targets per position with partial closure at each level
- Dynamic adjustment of take profit levels based on volatility
- Trailing take profits based on market conditions
- Proper handling of partial position closures
- Performance tracking and metrics
"""

import logging
import threading
import time
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
import statistics
import math


# Decorator for error handling
def error_handler(func):
    """Decorator to handle errors in methods."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Error in {func.__name__}: {str(e)}")
            # Re-raise if it's a critical error
            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                raise
            return None
    return wrapper


class MultiStageTakeProfitManager:
    """
    Advanced take profit manager for sophisticated profit-taking strategies.
    
    Features:
    - Multiple profit targets per position with partial closure at each level
    - Dynamic adjustment of take profit levels based on volatility and market conditions
    - Trailing take profits based on price movement
    - Integration with position tracker for partial closes
    - Performance metrics tracking
    """
    
    def __init__(self, position_tracker=None):
        """
        Initialize the MultiStageTakeProfitManager.
        
        Args:
            position_tracker: Component that tracks positions (optional)
        """
        self.position_tracker = position_tracker
        self.positions = {}  # Stores take profit data for each position
        self.lock = threading.RLock()  # For thread safety
        self.logger = logging.getLogger("MultiStageTakeProfitManager")
        self.running = False
        self.performance_metrics = {
            "targets_hit": 0,
            "targets_missed": 0,
            "partial_closes": 0,
            "average_profit_realized": 0.0
        }
    
    @error_handler
    async def start(self):
        """Start the take profit manager."""
        self.running = True
        self.logger.info("MultiStageTakeProfitManager started")
    
    @error_handler
    async def initialize(self):
        """Initialize the take profit manager."""
        self.logger.info("Initializing MultiStageTakeProfitManager")
        return True
    
    @error_handler
    async def stop(self):
        """Stop the take profit manager."""
        self.running = False
        self.logger.info("MultiStageTakeProfitManager stopped")
    
    @error_handler
    def register_position(self, position_id, entry_price, position_size, direction):
        """
        Register a new position with the take profit manager.
        
        Args:
            position_id: Unique identifier for the position
            entry_price: Entry price of the position
            position_size: Size of the position
            direction: Direction of the position ('buy' or 'sell')
        """
        with self.lock:
            self.positions[position_id] = {
                'entry_price': entry_price,
                'position_size': position_size,
                'direction': direction.lower(),
                'current_price': entry_price,
                'take_profit_levels': [],
                'triggered_levels': [],
                'trailing_enabled': False,
                'trailing_activation': None,
                'trailing_distance': None,
                'last_update_time': datetime.now()
            }
            self.logger.info(f"Position {position_id} registered with TP manager")
    
    @error_handler
    def set_take_profit_levels(self, position_id, levels):
        """
        Set multiple take profit levels for a position.
        
        Args:
            position_id: Position identifier
            levels: List of tuples (price, percentage) where percentage 
                   is the portion of position to close at that level
        """
        with self.lock:
            if position_id not in self.positions:
                self.logger.warning(f"Position {position_id} not registered")
                return False
            
            position = self.positions[position_id]
            
            # Sort levels by price (ascending for buys, descending for sells)
            if position['direction'] == 'buy':
                sorted_levels = sorted(levels, key=lambda x: x[0])
            else:
                sorted_levels = sorted(levels, key=lambda x: x[0], reverse=True)
            
            position['take_profit_levels'] = [
                {
                    'price': price,
                    'percentage': percentage,
                    'trailing_price': price,
                    'triggered': False,
                    'time_set': datetime.now()
                }
                for price, percentage in sorted_levels
            ]
            
            self.logger.info(f"Set {len(levels)} TP levels for position {position_id}")
            return True
    
    @error_handler
    def enable_trailing_take_profit(self, position_id, activation_price, trailing_distance):
        """
        Enable trailing take profit for a position.
        
        Args:
            position_id: Position identifier
            activation_price: Price at which trailing starts
            trailing_distance: Distance to maintain from highest/lowest price
        """
        with self.lock:
            if position_id not in self.positions:
                self.logger.warning(f"Position {position_id} not registered")
                return False
            
            position = self.positions[position_id]
            position['trailing_enabled'] = True
            position['trailing_activation'] = activation_price
            position['trailing_distance'] = trailing_distance
            
            self.logger.info(f"Enabled trailing TP for position {position_id}")
            return True
    
    @error_handler
    def disable_trailing_take_profit(self, position_id):
        """
        Disable trailing take profit for a position.
        
        Args:
            position_id: Position identifier
        """
        with self.lock:
            if position_id not in self.positions:
                return False
            
            position = self.positions[position_id]
            position['trailing_enabled'] = False
            
            return True
    
    @error_handler
    def adjust_take_profits_for_volatility(self, position_id, volatility_factor):
        """
        Adjust take profit levels based on current market volatility.
        
        Args:
            position_id: Position identifier
            volatility_factor: Factor to adjust TPs (>1 widens, <1 tightens)
        """
        with self.lock:
            if position_id not in self.positions:
                return False
            
            position = self.positions[position_id]
            entry_price = position['entry_price']
            
            # Only adjust non-triggered levels
            for level in position['take_profit_levels']:
                if not level['triggered']:
                    # Calculate the distance from entry
                    distance = abs(level['price'] - entry_price)
                    
                    # Apply volatility factor to distance
                    adjusted_distance = distance * volatility_factor
                    
                    # Calculate new take profit price
                    if position['direction'] == 'buy':
                        level['price'] = entry_price + adjusted_distance
                        level['trailing_price'] = level['price']
                    else:
                        level['price'] = entry_price - adjusted_distance
                        level['trailing_price'] = level['price']
            
            self.logger.info(f"Adjusted TP levels for position {position_id} with volatility factor {volatility_factor}")
            return True
    
    @error_handler
    def update_price(self, position_id, current_price, current_time=None):
        """
        Update the current price for a position and check for triggered take profits.
        
        Args:
            position_id: Position identifier
            current_price: Current market price
            current_time: Current time (optional, defaults to now)
            
        Returns:
            List of triggered take profit levels
        """
        if current_time is None:
            current_time = datetime.now()
        
        with self.lock:
            if position_id not in self.positions:
                return []
            
            position = self.positions[position_id]
            position['current_price'] = current_price
            position['last_update_time'] = current_time
            
            # Check for trailing take profit activation
            if position['trailing_enabled'] and position['trailing_activation']:
                self._update_trailing_take_profits(position_id, current_price)
            
            # Check for triggered take profit levels
            triggered = self._check_triggered_levels(position_id, current_price)
            return triggered
    
    @error_handler
    def _update_trailing_take_profits(self, position_id, current_price):
        """
        Update trailing take profit levels based on current price.
        
        Args:
            position_id: Position identifier
            current_price: Current market price
        """
        position = self.positions[position_id]
        
        # Check if trailing should be activated
        if position['direction'] == 'buy':
            trailing_activated = (current_price >= position['trailing_activation'])
        else:
            trailing_activated = (current_price <= position['trailing_activation'])
        
        # Update trailing prices if activated
        if trailing_activated:
            for level in position['take_profit_levels']:
                if not level['triggered']:
                    if position['direction'] == 'buy':
                        # For buy positions, trail price up
                        new_trailing_price = current_price - position['trailing_distance']
                        if new_trailing_price > level['trailing_price']:
                            level['trailing_price'] = new_trailing_price
                    else:
                        # For sell positions, trail price down
                        new_trailing_price = current_price + position['trailing_distance']
                        if new_trailing_price < level['trailing_price']:
                            level['trailing_price'] = new_trailing_price
    
    @error_handler
    def _check_triggered_levels(self, position_id, current_price):
        """
        Check if any take profit levels have been triggered.
        
        Args:
            position_id: Position identifier
            current_price: Current market price
            
        Returns:
            List of triggered level indices
        """
        position = self.positions[position_id]
        triggered_levels = []
        
        for i, level in enumerate(position['take_profit_levels']):
            if not level['triggered']:
                # Check if price has reached take profit level
                if position['direction'] == 'buy':
                    triggered = current_price >= level['trailing_price']
                else:
                    triggered = current_price <= level['trailing_price']
                
                if triggered:
                    level['triggered'] = True
                    level['trigger_time'] = datetime.now()
                    level['trigger_price'] = current_price
                    position['triggered_levels'].append(i)
                    triggered_levels.append(i)
                    
                    # Update performance metrics
                    self.performance_metrics["targets_hit"] += 1
                    self.performance_metrics["partial_closes"] += 1
                    
                    profit = abs(current_price - position['entry_price'])
                    self._update_average_profit(profit)
                    
                    self.logger.info(f"Take profit level {i} triggered for position {position_id} at price {current_price}")
        
        return triggered_levels
    
    @error_handler
    def _update_average_profit(self, profit):
        """
        Update the average profit realized metric.
        
        Args:
            profit: Profit amount to include in average
        """
        current_avg = self.performance_metrics["average_profit_realized"]
        total_closes = self.performance_metrics["partial_closes"]
        
        # Calculate new average
        if total_closes == 1:  # First close
            self.performance_metrics["average_profit_realized"] = profit
        else:
            # Weighted average calculation
            self.performance_metrics["average_profit_realized"] = (
                (current_avg * (total_closes - 1) + profit) / total_closes
            )
    
    @error_handler
    def execute_partial_close(self, position_id, level_index):
        """
        Execute a partial close for a triggered take profit level.
        
        Args:
            position_id: Position identifier
            level_index: Index of the triggered level
            
        Returns:
            Dict with execution details
        """
        with self.lock:
            if position_id not in self.positions:
                return None
            
            position = self.positions[position_id]
            
            if level_index >= len(position['take_profit_levels']):
                return None
            
            level = position['take_profit_levels'][level_index]
            
            if not level['triggered']:
                return None
            
            # Calculate position size to close
            close_size = position['position_size'] * level['percentage']
            
            # Execute partial close (if position tracker is available)
            if self.position_tracker and hasattr(self.position_tracker, 'partial_close'):
                try:
                    result = self.position_tracker.partial_close(
                        position_id,
                        close_size,
                        level['trigger_price'],
                        f"Take profit level {level_index}"
                    )
                    
                    # Update remaining position size
                    if result and result.get('success', False):
                        position['position_size'] -= close_size
                    
                    return result
                except Exception as e:
                    self.logger.error(f"Error executing partial close: {str(e)}")
                    return {'success': False, 'error': str(e)}
            
            # If no position tracker, just return success
            return {
                'success': True,
                'position_id': position_id,
                'level': level_index,
                'size_closed': close_size,
                'price': level['trigger_price'],
                'time': level.get('trigger_time', datetime.now())
            }
    
    @error_handler
    def remove_position(self, position_id):
        """
        Remove a position from the take profit manager.
        
        Args:
            position_id: Position identifier to remove
        """
        with self.lock:
            if position_id in self.positions:
                del self.positions[position_id]
                self.logger.info(f"Position {position_id} removed from TP manager")
                return True
            return False
    
    @error_handler
    def get_position_status(self, position_id):
        """
        Get the current take profit status for a position.
        
        Args:
            position_id: Position identifier
            
        Returns:
            Dict with take profit status
        """
        with self.lock:
            if position_id not in self.positions:
                return None
            
            position = self.positions[position_id].copy()
            
            # Add additional status information
            status = {
                'position_id': position_id,
                'entry_price': position['entry_price'],
                'current_price': position['current_price'],
                'direction': position['direction'],
                'position_size': position['position_size'],
                'levels_count': len(position['take_profit_levels']),
                'triggered_count': len(position['triggered_levels']),
                'trailing_enabled': position['trailing_enabled'],
                'last_update': position['last_update_time'],
                'levels': position['take_profit_levels']
            }
            
            return status
    
    @error_handler
    def get_performance_metrics(self):
        """
        Get performance metrics for the take profit manager.
        
        Returns:
            Dict with performance metrics
        """
        with self.lock:
            return self.performance_metrics.copy()


# Utility functions for take profit calculations

def fibonacci_take_profits(entry_price, stop_loss_price, direction, levels=[0.382, 0.618, 1.0, 1.618, 2.618]):
    """
    Calculate take profit levels using Fibonacci ratios.
    
    Args:
        entry_price: Entry price of the position
        stop_loss_price: Stop loss price
        direction: Trade direction ('buy' or 'sell')
        levels: Fibonacci levels to use
        
    Returns:
        List of take profit prices
    """
    # Calculate the risk (distance from entry to stop)
    risk = abs(entry_price - stop_loss_price)
    
    # Calculate take profit prices based on risk multiples
    take_profits = []
    
    for level in levels:
        if direction.lower() == 'buy':
            tp_price = entry_price + (risk * level)
        else:
            tp_price = entry_price - (risk * level)
        
        take_profits.append(tp_price)
    
    return take_profits


def risk_reward_take_profits(entry_price, stop_loss_price, direction, ratios=[1.0, 2.0, 3.0]):
    """
    Calculate take profit levels based on risk-reward ratios.
    
    Args:
        entry_price: Entry price of the position
        stop_loss_price: Stop loss price
        direction: Trade direction ('buy' or 'sell')
        ratios: Risk-reward ratios to use
        
    Returns:
        List of take profit prices
    """
    # Calculate the risk (distance from entry to stop)
    risk = abs(entry_price - stop_loss_price)
    
    # Calculate take profit prices based on risk-reward ratios
    take_profits = []
    
    for ratio in ratios:
        if direction.lower() == 'buy':
            tp_price = entry_price + (risk * ratio)
        else:
            tp_price = entry_price - (risk * ratio)
        
        take_profits.append(tp_price)
    
    return take_profits


def percentage_take_profits(entry_price, direction, percentages=[1.0, 2.0, 3.0]):
    """
    Calculate take profit levels based on percentage moves.
    
    Args:
        entry_price: Entry price of the position
        direction: Trade direction ('buy' or 'sell')
        percentages: List of percentage levels
        
    Returns:
        List of take profit prices
    """
    take_profits = []
    
    for pct in percentages:
        factor = 1.0 + (pct / 100.0)
        
        if direction.lower() == 'buy':
            tp_price = entry_price * factor
        else:
            tp_price = entry_price / factor
        
        take_profits.append(tp_price)
    
    return take_profits


def atr_based_take_profits(entry_price, atr_value, direction, multiples=[1.0, 2.0, 3.0, 5.0]):
    """
    Calculate take profit levels based on ATR multiples.
    
    Args:
        entry_price: Entry price of the position
        atr_value: Average True Range value
        direction: Trade direction ('buy' or 'sell')
        multiples: List of ATR multiples
        
    Returns:
        List of take profit prices
    """
    take_profits = []
    
    for multiple in multiples:
        distance = atr_value * multiple
        
        if direction.lower() == 'buy':
            tp_price = entry_price + distance
        else:
            tp_price = entry_price - distance
        
        take_profits.append(tp_price)
    
    return take_profits
