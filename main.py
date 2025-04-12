"""
Main configuration handlers for the FX Trading Bridge.

Provides functions for loading, accessing, updating and saving configuration.
"""
import os
import json
import logging
from typing import Any, Dict, Optional, Union, List, Tuple
from pathlib import Path
import threading
import time
from datetime import datetime

# Default configuration
DEFAULT_CONFIG = {
    "api": {
        "host": "0.0.0.0",
        "port": 8000,
        "debug": False
    },
    "logging": {
        "level": "INFO",
        "file": "fx_bridge.log",
        "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    },
    "broker": {
        "name": "default",
        "api_key": "",
        "api_secret": "",
        "demo": True,
        "timeout": 30
    },
    "trading": {
        "default_lot_size": 0.01,
        "max_lot_size": 1.0,
        "max_open_positions": 5,
        "default_stop_loss_pips": 50,
        "default_take_profit_pips": 100,
        "risk_percent": 1.0
    },
    "timeouts": {
        "order": 5000,
        "position": 5000,
        "account": 5000
    }
}

# Global configuration object
_config: Dict[str, Any] = {}
_config_file: Optional[str] = None


class ConfigurationError(Exception):
    """Exception raised for configuration errors."""
    pass


def load_config(config_file: str = "config.json") -> Dict[str, Any]:
    """
    Load configuration from a JSON file.
    
    Args:
        config_file: Path to the configuration file
        
    Returns:
        Dict containing the configuration
        
    Raises:
        ConfigurationError: If config file cannot be loaded
    """
    global _config, _config_file
    
    try:
        config_path = Path(config_file)
        
        # If file doesn't exist, create with defaults
        if not config_path.exists():
            logging.warning(f"Config file {config_file} not found, creating with defaults")
            _config = DEFAULT_CONFIG.copy()
            with open(config_path, 'w') as f:
                json.dump(_config, f, indent=2)
        else:
            # Load existing config
            with open(config_path, 'r') as f:
                loaded_config = json.load(f)
            
            # Merge with defaults to ensure all keys exist
            _config = DEFAULT_CONFIG.copy()
            for section, values in loaded_config.items():
                if section in _config and isinstance(_config[section], dict):
                    _config[section].update(values)
                else:
                    _config[section] = values
        
        _config_file = config_file
        return _config
        
    except Exception as e:
        raise ConfigurationError(f"Failed to load configuration: {str(e)}")


def get_config() -> Dict[str, Any]:
    """
    Get the current configuration.
    
    Returns:
        Dict containing the configuration
    """
    global _config
    
    if not _config:
        return load_config()
    
    return _config


def update_config(updates: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update the current configuration.
    
    Args:
        updates: Dict with configuration updates
        
    Returns:
        Updated configuration
    """
    global _config
    
    if not _config:
        _config = load_config()
    
    # Deep update of nested dictionaries
    for section, values in updates.items():
        if section in _config and isinstance(_config[section], dict) and isinstance(values, dict):
            _config[section].update(values)
        else:
            _config[section] = values
    
    return _config


def save_config(config_file: Optional[str] = None) -> bool:
    """
    Save the current configuration to a file.
    
    Args:
        config_file: Path to save to (defaults to previously loaded file)
        
    Returns:
        True if successful, False otherwise
    """
    global _config, _config_file
    
    if not config_file and not _config_file:
        config_file = "config.json"
    elif not config_file:
        config_file = _config_file
    
    try:
        with open(config_file, 'w') as f:
            json.dump(_config, f, indent=2)
        return True
    except Exception as e:
        logging.error(f"Failed to save configuration: {str(e)}")
        return False


def import_legacy_settings(legacy_file: str) -> Dict[str, Any]:
    """
    Import settings from a legacy format.
    
    Args:
        legacy_file: Path to the legacy settings file
        
    Returns:
        Dict with imported settings
    """
    # Placeholder for legacy import functionality
    return {}


class TimeBasedExitManager:
    """
    Manages time-based exit rules for trading positions.
    
    This component allows for the closure of trades based on time criteria:
    - Maximum holding time for positions
    - Time-of-day exits
    - Session-based exits
    - Scheduled exits based on economic events
    """
    
    def __init__(self, max_hold_time_seconds=None):
        """
        Initialize the TimeBasedExitManager.
        
        Args:
            max_hold_time_seconds: Default maximum position holding time
        """
        self.positions = {}
        self.max_hold_time = max_hold_time_seconds
        self.scheduled_exits = {}
        self.time_rules = {}
        self.logger = logging.getLogger("TimeBasedExitManager")
        self.running = False
    
    def start(self):
        """Start the time-based exit manager."""
        self.running = True
    
    def stop(self):
        """Stop the time-based exit manager."""
        self.running = False
    
    def register_position(self, position_id, entry_time, max_hold_time=None):
        """
        Register a position for time-based exit monitoring.
        
        Args:
            position_id: Unique identifier for the position
            entry_time: Time when the position was entered
            max_hold_time: Maximum time to hold this position (in seconds)
        """
        self.positions[position_id] = {
            'entry_time': entry_time,
            'max_hold_time': max_hold_time or self.max_hold_time,
            'scheduled_exit': None
        }
    
    def set_time_rule(self, rule_name, time_condition, exit_action):
        """
        Set a time-based rule for position exits.
        
        Args:
            rule_name: Name of the rule
            time_condition: Function that evaluates if the time condition is met
            exit_action: Function to execute when the condition is met
        """
        self.time_rules[rule_name] = {
            'condition': time_condition,
            'action': exit_action
        }
    
    def schedule_exit(self, position_id, exit_time, reason=None):
        """
        Schedule an exit for a specific position.
        
        Args:
            position_id: Position identifier
            exit_time: Time to exit the position
            reason: Reason for the scheduled exit
        """
        if position_id in self.positions:
            self.positions[position_id]['scheduled_exit'] = {
                'time': exit_time,
                'reason': reason or 'Scheduled exit'
            }
    
    def check_time_exits(self, current_time):
        """
        Check all positions for time-based exit conditions.
        
        Args:
            current_time: Current time to check against
            
        Returns:
            List of position IDs that should be exited
        """
        exits_needed = []
        
        for position_id, position_data in self.positions.items():
            # Check maximum hold time
            if position_data['max_hold_time']:
                elapsed = current_time - position_data['entry_time']
                if elapsed.total_seconds() >= position_data['max_hold_time']:
                    exits_needed.append((position_id, 'Maximum hold time reached'))
                    continue
            
            # Check scheduled exit
            if position_data['scheduled_exit']:
                if current_time >= position_data['scheduled_exit']['time']:
                    exits_needed.append((
                        position_id, 
                        position_data['scheduled_exit']['reason']
                    ))
                    continue
        
        return exits_needed
    
    def remove_position(self, position_id):
        """
        Remove a position from time-based exit monitoring.
        
        Args:
            position_id: Position identifier to remove
        """
        if position_id in self.positions:
            del self.positions[position_id]
    
    def get_next_exit(self):
        """
        Get the next scheduled exit time across all positions.
        
        Returns:
            Tuple of (position_id, exit_time) for the next exit or None
        """
        next_exit = None
        next_exit_time = None
        
        for position_id, position_data in self.positions.items():
            if position_data['scheduled_exit']:
                exit_time = position_data['scheduled_exit']['time']
                
                if next_exit_time is None or exit_time < next_exit_time:
                    next_exit = position_id
                    next_exit_time = exit_time
        
        if next_exit:
            return (next_exit, next_exit_time)
        return None


class MultiStageTakeProfitManager:
    """
    Manages multiple take profit levels for positions with partial closures.
    
    This component allows for setting multiple profit targets with different
    position size percentages at each level. Features include:
    - Multiple profit targets per position with partial closure at each level
    - Dynamic adjustment of take profit levels based on volatility and market conditions
    - Trailing take profits based on price movement
    - Integration with position tracker for partial closes
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
    
    def start(self):
        """Start the take profit manager."""
        self.running = True
        self.logger.info("MultiStageTakeProfitManager started")
    
    def stop(self):
        """Stop the take profit manager."""
        self.running = False
        self.logger.info("MultiStageTakeProfitManager stopped")
    
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
    
    def get_performance_metrics(self):
        """
        Get performance metrics for the take profit manager.
        
        Returns:
            Dict with performance metrics
        """
        with self.lock:
            return self.performance_metrics.copy() 