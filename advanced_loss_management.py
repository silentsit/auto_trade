"""
Advanced Loss Management System for FX Trading

This module provides sophisticated loss management capabilities including:
- Dynamic stop loss adjustments based on volatility
- Tiered stop loss systems with partial position closure
- Breakeven protection after profit thresholds
- Risk-adjusted position management
- Cost averaging mechanisms for underwater positions
- Drawdown protection and circuit breakers
"""

import logging
import threading
import time
from typing import Dict, List, Optional, Tuple, Any, Callable, Union
from datetime import datetime, timedelta
import statistics
import math


class AdvancedLossManagement:
    """
    Advanced loss management system that provides sophisticated stop loss 
    and risk management capabilities for trading positions.
    
    Features:
    - Dynamic stop loss adjustments based on market volatility
    - Tiered stop loss system with partial position closures
    - Breakeven protection after reaching profit thresholds
    - Time-based stop loss adjustments
    - Max drawdown protection
    - Correlation-based portfolio risk management
    """
    
    def __init__(self, position_tracker=None):
        """
        Initialize the Advanced Loss Management system.
        
        Args:
            position_tracker: Optional component that tracks positions
        """
        self.position_tracker = position_tracker
        self.positions = {}  # Stores loss management data for positions
        self.lock = threading.RLock()  # For thread safety
        self.logger = logging.getLogger("AdvancedLossManagement")
        self.running = False
        self.volatility_data = {}
        self.performance_metrics = {
            "stops_hit": 0,
            "positions_protected": 0,
            "average_loss_realized": 0.0,
            "max_drawdown": 0.0,
            "drawdown_events": 0
        }
    
    async def start(self):
        """Start the loss management system."""
        self.running = True
        self.logger.info("AdvancedLossManagement system started")
    
    async def stop(self):
        """Stop the loss management system."""
        self.running = False
        self.logger.info("AdvancedLossManagement system stopped")
    
    def register_position(self, position_id, entry_price, position_size, direction, 
                          initial_stop_loss=None, risk_amount=None):
        """
        Register a position with the loss management system.
        
        Args:
            position_id: Unique identifier for the position
            entry_price: Entry price of the position
            position_size: Size of the position
            direction: Direction of the position ('buy' or 'sell')
            initial_stop_loss: Initial stop loss price (optional)
            risk_amount: Amount of money risked on the position (optional)
        """
        with self.lock:
            self.positions[position_id] = {
                'entry_price': entry_price,
                'position_size': position_size,
                'current_size': position_size,  # For partial closures
                'direction': direction.lower(),
                'current_price': entry_price,
                'stop_loss_price': initial_stop_loss,
                'original_stop_loss': initial_stop_loss,
                'risk_amount': risk_amount,
                'max_price': entry_price,
                'min_price': entry_price,
                'stop_loss_levels': [],
                'breakeven_activated': False,
                'breakeven_threshold': None,
                'trailing_stop_activated': False,
                'trailing_stop_distance': None,
                'max_drawdown_percent': None,
                'time_based_adjustments': [],
                'last_update_time': datetime.now(),
                'partial_closes': []
            }
            
            self.logger.info(f"Position {position_id} registered with loss management system")
    
    def set_tiered_stop_loss(self, position_id, levels):
        """
        Set multiple tiered stop loss levels with partial closures.
        
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
            
            # Sort levels by price (descending for buys, ascending for sells)
            if position['direction'] == 'buy':
                sorted_levels = sorted(levels, key=lambda x: x[0], reverse=True)
            else:
                sorted_levels = sorted(levels, key=lambda x: x[0])
            
            position['stop_loss_levels'] = [
                {
                    'price': price,
                    'percentage': percentage,
                    'triggered': False,
                    'time_set': datetime.now()
                }
                for price, percentage in sorted_levels
            ]
            
            # Set the main stop loss to the final (most conservative) level
            if sorted_levels:
                position['stop_loss_price'] = sorted_levels[-1][0]
            
            self.logger.info(f"Set {len(levels)} tiered stop loss levels for position {position_id}")
            return True
    
    def set_breakeven_stop(self, position_id, profit_threshold, offset=0):
        """
        Set a breakeven stop loss that activates after a profit threshold.
        
        Args:
            position_id: Position identifier
            profit_threshold: Price level that triggers breakeven protection
            offset: Optional offset from entry price for the breakeven level
        """
        with self.lock:
            if position_id not in self.positions:
                self.logger.warning(f"Position {position_id} not registered")
                return False
            
            position = self.positions[position_id]
            
            if position['direction'] == 'buy':
                # For buys, threshold must be above entry
                if profit_threshold <= position['entry_price']:
                    self.logger.warning(f"Invalid profit threshold for buy position")
                    return False
            else:
                # For sells, threshold must be below entry
                if profit_threshold >= position['entry_price']:
                    self.logger.warning(f"Invalid profit threshold for sell position")
                    return False
            
            position['breakeven_threshold'] = profit_threshold
            position['breakeven_offset'] = offset
            position['breakeven_activated'] = False
            
            self.logger.info(f"Set breakeven protection for position {position_id} at threshold {profit_threshold}")
            return True
    
    def set_trailing_stop(self, position_id, activation_price, trailing_distance):
        """
        Set a trailing stop loss for a position.
        
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
            position['trailing_stop_distance'] = trailing_distance
            
            # For buy positions, activation price should be above entry
            if position['direction'] == 'buy':
                if activation_price <= position['entry_price']:
                    activation_price = position['entry_price'] + trailing_distance
                    
            # For sell positions, activation price should be below entry
            else:
                if activation_price >= position['entry_price']:
                    activation_price = position['entry_price'] - trailing_distance
            
            position['trailing_activation_price'] = activation_price
            position['trailing_stop_activated'] = False
            
            self.logger.info(f"Set trailing stop for position {position_id}, activation at {activation_price}")
            return True
    
    def set_max_drawdown_protection(self, position_id, max_drawdown_percent):
        """
        Set maximum drawdown protection for a position.
        
        Args:
            position_id: Position identifier
            max_drawdown_percent: Maximum drawdown allowed as percentage
        """
        with self.lock:
            if position_id not in self.positions:
                return False
            
            if max_drawdown_percent <= 0 or max_drawdown_percent > 100:
                self.logger.warning(f"Invalid max drawdown percentage: {max_drawdown_percent}")
                return False
            
            position = self.positions[position_id]
            position['max_drawdown_percent'] = max_drawdown_percent
            
            self.logger.info(f"Set max drawdown protection for position {position_id} at {max_drawdown_percent}%")
            return True
    
    def set_time_based_adjustment(self, position_id, adjustment_time, new_stop_loss_price=None, 
                                  adjustment_function=None):
        """
        Schedule a time-based stop loss adjustment.
        
        Args:
            position_id: Position identifier
            adjustment_time: Time when the adjustment should occur
            new_stop_loss_price: New stop loss price to set at the specified time
            adjustment_function: Function to calculate new stop loss price at the time
        """
        with self.lock:
            if position_id not in self.positions:
                return False
            
            position = self.positions[position_id]
            
            adjustment = {
                'time': adjustment_time,
                'executed': False,
                'new_price': new_stop_loss_price,
                'function': adjustment_function
            }
            
            position['time_based_adjustments'].append(adjustment)
            
            # Sort adjustments by time
            position['time_based_adjustments'].sort(key=lambda x: x['time'])
            
            self.logger.info(f"Scheduled time-based stop loss adjustment for position {position_id}")
            return True
    
    def update_price(self, position_id, current_price, current_time=None):
        """
        Update price for a position and check for stop loss conditions.
        
        Args:
            position_id: Position identifier
            current_price: Current market price
            current_time: Current time (optional, defaults to now)
            
        Returns:
            Dict with stop loss status or None if not triggered
        """
        if current_time is None:
            current_time = datetime.now()
        
        with self.lock:
            if position_id not in self.positions:
                return None
            
            position = self.positions[position_id]
            position['current_price'] = current_price
            position['last_update_time'] = current_time
            
            # Update max/min prices seen
            if current_price > position['max_price']:
                position['max_price'] = current_price
            
            if current_price < position['min_price']:
                position['min_price'] = current_price
            
            # Check for time-based adjustments
            self._process_time_based_adjustments(position_id, current_time)
            
            # Check breakeven activation
            self._check_breakeven_activation(position_id, current_price)
            
            # Check trailing stop activation
            self._update_trailing_stop(position_id, current_price)
            
            # Check for drawdown protection
            drawdown_triggered = self._check_drawdown_protection(position_id, current_price)
            if drawdown_triggered:
                return {'triggered': True, 'reason': 'Max drawdown protection', 'price': current_price}
            
            # Check tiered stop loss levels
            tiered_result = self._check_tiered_stop_loss(position_id, current_price)
            if tiered_result:
                return tiered_result
            
            # Check main stop loss
            if position['stop_loss_price'] is not None:
                if (position['direction'] == 'buy' and current_price <= position['stop_loss_price']) or \
                   (position['direction'] == 'sell' and current_price >= position['stop_loss_price']):
                    
                    # Update metrics
                    self.performance_metrics["stops_hit"] += 1
                    loss = abs(current_price - position['entry_price'])
                    self._update_average_loss(loss)
                    
                    self.logger.info(f"Stop loss triggered for position {position_id} at price {current_price}")
                    
                    return {
                        'triggered': True,
                        'reason': 'Main stop loss',
                        'price': current_price,
                        'stop_level': position['stop_loss_price']
                    }
            
            return None
    
    def _process_time_based_adjustments(self, position_id, current_time):
        """
        Process any time-based stop loss adjustments that are due.
        
        Args:
            position_id: Position identifier
            current_time: Current time
        """
        position = self.positions[position_id]
        
        for adjustment in position['time_based_adjustments']:
            if not adjustment['executed'] and current_time >= adjustment['time']:
                # Calculate new stop loss price
                if adjustment['function'] is not None:
                    new_price = adjustment['function'](
                        position['entry_price'],
                        position['current_price'],
                        position['direction']
                    )
                else:
                    new_price = adjustment['new_price']
                
                # Apply the new stop loss if it's valid
                if new_price is not None:
                    if (position['direction'] == 'buy' and 
                        (position['stop_loss_price'] is None or new_price > position['stop_loss_price'])) or \
                       (position['direction'] == 'sell' and 
                        (position['stop_loss_price'] is None or new_price < position['stop_loss_price'])):
                        
                        position['stop_loss_price'] = new_price
                        self.logger.info(f"Time-based stop loss adjustment applied for position {position_id}")
                
                adjustment['executed'] = True
    
    def _check_breakeven_activation(self, position_id, current_price):
        """
        Check if breakeven stop protection should be activated.
        
        Args:
            position_id: Position identifier
            current_price: Current market price
        """
        position = self.positions[position_id]
        
        if not position['breakeven_activated'] and position['breakeven_threshold'] is not None:
            # Check if price has reached breakeven threshold
            threshold_reached = (
                (position['direction'] == 'buy' and current_price >= position['breakeven_threshold']) or
                (position['direction'] == 'sell' and current_price <= position['breakeven_threshold'])
            )
            
            if threshold_reached:
                # Calculate breakeven stop level with offset
                breakeven_level = position['entry_price']
                if position['breakeven_offset'] is not None:
                    if position['direction'] == 'buy':
                        breakeven_level += position['breakeven_offset']
                    else:
                        breakeven_level -= position['breakeven_offset']
                
                # Only update if the new stop loss is better than current
                if position['stop_loss_price'] is None or (
                    (position['direction'] == 'buy' and breakeven_level > position['stop_loss_price']) or
                    (position['direction'] == 'sell' and breakeven_level < position['stop_loss_price'])
                ):
                    position['stop_loss_price'] = breakeven_level
                    position['breakeven_activated'] = True
                    
                    # Update metrics
                    self.performance_metrics["positions_protected"] += 1
                    
                    self.logger.info(f"Breakeven stop activated for position {position_id} at {breakeven_level}")
    
    def _update_trailing_stop(self, position_id, current_price):
        """
        Update trailing stop loss based on price movement.
        
        Args:
            position_id: Position identifier
            current_price: Current market price
        """
        position = self.positions[position_id]
        
        # Check if trailing stop conditions are configured
        if position['trailing_stop_distance'] is None:
            return
        
        # Check if trailing stop should be activated
        if not position['trailing_stop_activated']:
            activation_reached = (
                (position['direction'] == 'buy' and current_price >= position['trailing_activation_price']) or
                (position['direction'] == 'sell' and current_price <= position['trailing_activation_price'])
            )
            
            if activation_reached:
                position['trailing_stop_activated'] = True
                self.logger.info(f"Trailing stop activated for position {position_id}")
        
        # Update trailing stop level if activated
        if position['trailing_stop_activated']:
            new_stop_level = None
            
            if position['direction'] == 'buy':
                # For buy positions, trail below the highest price
                new_stop_level = position['max_price'] - position['trailing_stop_distance']
            else:
                # For sell positions, trail above the lowest price
                new_stop_level = position['min_price'] + position['trailing_stop_distance']
            
            # Only update if the new stop level is better than current
            if new_stop_level is not None and (
                position['stop_loss_price'] is None or
                (position['direction'] == 'buy' and new_stop_level > position['stop_loss_price']) or
                (position['direction'] == 'sell' and new_stop_level < position['stop_loss_price'])
            ):
                position['stop_loss_price'] = new_stop_level
                self.logger.debug(f"Updated trailing stop for position {position_id} to {new_stop_level}")
    
    def _check_drawdown_protection(self, position_id, current_price):
        """
        Check if maximum drawdown protection is triggered.
        
        Args:
            position_id: Position identifier
            current_price: Current market price
            
        Returns:
            True if drawdown protection triggered, False otherwise
        """
        position = self.positions[position_id]
        
        if position['max_drawdown_percent'] is None:
            return False
        
        # Calculate current drawdown
        if position['direction'] == 'buy':
            max_profit = position['max_price'] - position['entry_price']
            current_profit = current_price - position['entry_price']
        else:
            max_profit = position['entry_price'] - position['min_price']
            current_profit = position['entry_price'] - current_price
        
        # Avoid division by zero
        if max_profit <= 0:
            return False
        
        drawdown_percent = (max_profit - current_profit) / max_profit * 100
        
        if drawdown_percent >= position['max_drawdown_percent']:
            # Update metrics
            self.performance_metrics["drawdown_events"] += 1
            self.performance_metrics["stops_hit"] += 1
            
            # Track maximum drawdown seen
            if drawdown_percent > self.performance_metrics["max_drawdown"]:
                self.performance_metrics["max_drawdown"] = drawdown_percent
            
            self.logger.info(f"Max drawdown protection triggered for position {position_id} at {drawdown_percent}%")
            return True
        
        return False
    
    def _check_tiered_stop_loss(self, position_id, current_price):
        """
        Check if any tiered stop loss levels are triggered.
        
        Args:
            position_id: Position identifier
            current_price: Current market price
            
        Returns:
            Dict with stop loss info if triggered, None otherwise
        """
        position = self.positions[position_id]
        
        for i, level in enumerate(position['stop_loss_levels']):
            if not level['triggered']:
                # Check if price has reached this stop loss level
                if (position['direction'] == 'buy' and current_price <= level['price']) or \
                   (position['direction'] == 'sell' and current_price >= level['price']):
                    
                    level['triggered'] = True
                    level['trigger_time'] = datetime.now()
                    level['trigger_price'] = current_price
                    
                    # Update metrics
                    self.performance_metrics["stops_hit"] += 1
                    loss = abs(current_price - position['entry_price'])
                    self._update_average_loss(loss)
                    
                    # Record partial close
                    position['partial_closes'].append({
                        'level': i,
                        'price': current_price,
                        'percentage': level['percentage'],
                        'time': datetime.now()
                    })
                    
                    self.logger.info(f"Tiered stop loss level {i} triggered for position {position_id}")
                    
                    return {
                        'triggered': True,
                        'reason': f'Tiered stop loss level {i}',
                        'price': current_price,
                        'level': i,
                        'percentage': level['percentage'],
                        'stop_level': level['price']
                    }
        
        return None
    
    def _update_average_loss(self, loss):
        """
        Update the average loss realized metric.
        
        Args:
            loss: Loss amount to include in average
        """
        current_avg = self.performance_metrics["average_loss_realized"]
        total_stops = self.performance_metrics["stops_hit"]
        
        # Calculate new average
        if total_stops == 1:  # First stop
            self.performance_metrics["average_loss_realized"] = loss
        else:
            # Weighted average calculation
            self.performance_metrics["average_loss_realized"] = (
                (current_avg * (total_stops - 1) + loss) / total_stops
            )
    
    def adjust_stop_loss_for_volatility(self, position_id, volatility_factor, min_distance=None):
        """
        Adjust stop loss based on current market volatility.
        
        Args:
            position_id: Position identifier
            volatility_factor: Value representing current volatility (e.g. ATR)
            min_distance: Minimum distance from current price (optional)
            
        Returns:
            New stop loss price or None if no change
        """
        with self.lock:
            if position_id not in self.positions:
                return None
            
            position = self.positions[position_id]
            
            # Calculate dynamic stop loss distance based on volatility
            stop_distance = volatility_factor * 2  # Example: 2x ATR
            
            # Apply minimum distance if specified
            if min_distance is not None and stop_distance < min_distance:
                stop_distance = min_distance
            
            # Calculate new stop loss price
            if position['direction'] == 'buy':
                new_stop = position['current_price'] - stop_distance
                
                # Only update if better than current
                if position['stop_loss_price'] is None or new_stop > position['stop_loss_price']:
                    position['stop_loss_price'] = new_stop
                    self.logger.info(f"Adjusted stop loss for position {position_id} to {new_stop} based on volatility")
                    return new_stop
                    
            else:  # Sell position
                new_stop = position['current_price'] + stop_distance
                
                # Only update if better than current
                if position['stop_loss_price'] is None or new_stop < position['stop_loss_price']:
                    position['stop_loss_price'] = new_stop
                    self.logger.info(f"Adjusted stop loss for position {position_id} to {new_stop} based on volatility")
                    return new_stop
            
            return None
    
    def execute_partial_close(self, position_id, stop_level_index):
        """
        Execute a partial close for a triggered tiered stop loss level.
        
        Args:
            position_id: Position identifier
            stop_level_index: Index of the triggered stop level
            
        Returns:
            Dict with execution details
        """
        with self.lock:
            if position_id not in self.positions:
                return None
            
            position = self.positions[position_id]
            
            if stop_level_index >= len(position['stop_loss_levels']):
                return None
            
            level = position['stop_loss_levels'][stop_level_index]
            
            if not level['triggered']:
                return None
            
            # Calculate position size to close
            close_size = position['current_size'] * level['percentage']
            
            # Execute partial close (if position tracker is available)
            if self.position_tracker and hasattr(self.position_tracker, 'partial_close'):
                try:
                    result = self.position_tracker.partial_close(
                        position_id,
                        close_size,
                        level['trigger_price'],
                        f"Tiered stop loss level {stop_level_index}"
                    )
                    
                    # Update remaining position size
                    if result and result.get('success', False):
                        position['current_size'] -= close_size
                    
                    return result
                except Exception as e:
                    self.logger.error(f"Error executing partial close: {str(e)}")
                    return {'success': False, 'error': str(e)}
            
            # If no position tracker, just return success
            position['current_size'] -= close_size
            
            return {
                'success': True,
                'position_id': position_id,
                'level': stop_level_index,
                'size_closed': close_size,
                'remaining_size': position['current_size'],
                'price': level['trigger_price'],
                'time': level.get('trigger_time', datetime.now())
            }
    
    def remove_position(self, position_id):
        """
        Remove a position from the loss management system.
        
        Args:
            position_id: Position identifier to remove
        """
        with self.lock:
            if position_id in self.positions:
                del self.positions[position_id]
                self.logger.info(f"Position {position_id} removed from loss management system")
                return True
            return False
    
    def get_position_status(self, position_id):
        """
        Get the current loss management status for a position.
        
        Args:
            position_id: Position identifier
            
        Returns:
            Dict with loss management status
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
                'stop_loss_price': position['stop_loss_price'],
                'direction': position['direction'],
                'current_size': position['current_size'],
                'original_size': position['position_size'],
                'breakeven_activated': position['breakeven_activated'],
                'trailing_activated': position['trailing_stop_activated'],
                'partial_closes': len(position['partial_closes']),
                'max_price': position['max_price'],
                'min_price': position['min_price'],
                'last_update': position['last_update_time']
            }
            
            return status
    
    def get_performance_metrics(self):
        """
        Get performance metrics for the loss management system.
        
        Returns:
            Dict with performance metrics
        """
        with self.lock:
            return self.performance_metrics.copy()


# Utility functions for stop loss calculations

def atr_based_stop_loss(entry_price, current_price, atr_value, multiplier=2.0, direction='buy'):
    """
    Calculate a stop loss based on Average True Range.
    
    Args:
        entry_price: Position entry price
        current_price: Current price
        atr_value: Current ATR value
        multiplier: Multiplier for ATR distance
        direction: Trade direction ('buy' or 'sell')
        
    Returns:
        Calculated stop loss price
    """
    distance = atr_value * multiplier
    
    if direction.lower() == 'buy':
        return current_price - distance
    else:
        return current_price + distance


def percent_risk_stop_loss(entry_price, risk_percent, position_value):
    """
    Calculate a stop loss based on percentage risk.
    
    Args:
        entry_price: Position entry price
        risk_percent: Percentage of position value to risk
        position_value: Total value of the position
        
    Returns:
        Maximum stop loss distance in price units
    """
    risk_amount = position_value * (risk_percent / 100.0)
    # Requires position size to convert risk amount to price distance
    # This is a simplified calculation
    return risk_amount
