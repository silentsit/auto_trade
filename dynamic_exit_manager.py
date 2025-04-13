"""
Dynamic Exit Manager for FX Trading Bridge

This module provides advanced exit management with features including:
- Multi-level take profit targets
- Trailing stops with dynamic activation
- Partial position closing
- Time-based exits
- Volatility-adjusted exits
- Pattern-based exit adjustments
"""

import asyncio
import logging
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, timedelta, timezone
from collections import deque
import math
import json
import traceback
from functools import wraps
import uuid

# Setup logging
logger = logging.getLogger("fx-trading-bridge.exits")

# Error handling decorators
def handle_async_errors(func):
    """Decorator for handling errors in async functions"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    return wrapper

class DynamicExitManager:
    """
    Comprehensive exit management system that adapts to market conditions.
    
    Features:
    - Multi-level take profit targets
    - Dynamic trailing stops based on volatility
    - Time-based exits with adaptive timing
    - Volatility-based exit adjustment
    - Market regime-aware exit strategies
    - Integration with position tracker for execution
    """
    
    def __init__(self, 
                position_tracker=None, 
                multi_stage_tp_manager=None,
                time_exit_manager=None,
                volatility_monitor=None,
                market_analysis=None):
        """
        Initialize the DynamicExitManager.
        
        Args:
            position_tracker: Reference to the PositionTracker instance
            multi_stage_tp_manager: Reference to the MultiStageTakeProfitManager instance
            time_exit_manager: Reference to the TimeBasedExitManager instance
            volatility_monitor: Reference to the VolatilityMonitor instance
            market_analysis: Reference to the MarketAnalysis instance
        """
        self.position_tracker = position_tracker
        self.multi_stage_tp_manager = multi_stage_tp_manager
        self.time_exit_manager = time_exit_manager
        self.volatility_monitor = volatility_monitor
        self.market_analysis = market_analysis
        
        self._lock = asyncio.Lock()
        self.exit_strategies = {}  # {position_id: {"strategy_type": ..., "params": ...}}
        self.position_exits = {}   # {position_id: {"take_profits": [], "trailing_stop": {}, "time_exit": {}}}
        self.exit_history = []
        self.performance_metrics = {
            "strategies": {},
            "overall": {
                "total_exits": 0,
                "profitable_exits": 0,
                "avg_profit_pct": 0,
                "avg_loss_pct": 0,
                "best_exit": 0,
                "worst_exit": 0
            }
        }
        
        # Defaults for exit strategies
        self.exit_defaults = {
            "standard": {
                "take_profit_r": 2.0,
                "stop_loss_r": 1.0,
                "trailing_stop": False
            },
            "conservative": {
                "take_profit_r": 1.5,
                "stop_loss_r": 0.8,
                "trailing_stop": True,
                "trailing_activation_r": 1.0,
                "time_exit_hours": 24
            },
            "aggressive": {
                "take_profit_r": 3.0,
                "stop_loss_r": 1.2,
                "trailing_stop": True,
                "trailing_activation_r": 1.5
            },
            "swing": {
                "take_profit_r": 4.0,
                "stop_loss_r": 1.5,
                "trailing_stop": True,
                "trailing_activation_r": 2.0,
                "time_exit_days": 5
            },
            "scalp": {
                "take_profit_r": 1.0,
                "stop_loss_r": 0.5,
                "trailing_stop": True,
                "trailing_activation_r": 0.5,
                "time_exit_hours": 4
            }
        }
        
        logger.info("DynamicExitManager initialized")
    
    @handle_async_errors
    async def initialize_exits(self, 
                             position_id: str,
                             symbol: str,
                             entry_price: float,
                             stop_loss: float,
                             position_direction: str,
                             position_size: float,
                             timeframe: str = "H1",
                             strategy_type: str = "standard",
                             custom_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Initialize exit strategies for a position.
        
        Args:
            position_id: Unique identifier for the position
            symbol: Trading symbol
            entry_price: Entry price of the position
            stop_loss: Stop loss price
            position_direction: 'BUY' or 'SELL'
            position_size: Size of the position
            timeframe: Trading timeframe
            strategy_type: Type of exit strategy ('standard', 'conservative', 'aggressive', 'swing', 'scalp')
            custom_params: Optional custom parameters to override defaults
            
        Returns:
            Exit configuration details
        """
        async with self._lock:
            # Get base strategy parameters
            params = self.exit_defaults.get(strategy_type, self.exit_defaults["standard"]).copy()
            
            # Override with custom parameters if provided
            if custom_params:
                params.update(custom_params)
            
            # Calculate risk distance (1R)
            risk_distance = abs(entry_price - stop_loss)
            
            # Store strategy info
            self.exit_strategies[position_id] = {
                "strategy_type": strategy_type,
                "params": params,
                "symbol": symbol,
                "timeframe": timeframe,
                "entry_price": entry_price,
                "stop_loss": stop_loss,
                "position_direction": position_direction,
                "position_size": position_size,
                "risk_distance": risk_distance,
                "creation_time": datetime.now(timezone.utc)
            }
            
            # Initialize different exit types
            exits = {
                "take_profits": await self._setup_take_profits(position_id, symbol, entry_price, 
                                                            stop_loss, position_direction, 
                                                            position_size, timeframe, params),
                "trailing_stop": await self._setup_trailing_stop(position_id, entry_price, 
                                                              stop_loss, position_direction, params),
                "time_exit": await self._setup_time_exit(position_id, timeframe, params),
                "volatility_exits": await self._setup_volatility_exits(position_id, symbol, params)
            }
            
            self.position_exits[position_id] = exits
            
            logger.info(f"Initialized {strategy_type} exit strategy for position {position_id}")
            
            return {
                "position_id": position_id,
                "strategy_type": strategy_type,
                "exits": exits
            }
    
    @handle_async_errors
    async def _setup_take_profits(self, 
                                position_id: str, 
                                symbol: str,
                                entry_price: float,
                                stop_loss: float,
                                position_direction: str,
                                position_size: float,
                                timeframe: str,
                                params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Set up take profit levels using the multi-stage take profit manager.
        
        Returns:
            Take profit configuration
        """
        if not self.multi_stage_tp_manager:
            logger.warning("No multi-stage take profit manager available")
            return {"enabled": False}
        
        # Calculate take profit level based on R-multiple
        tp_r = params.get("take_profit_r", 2.0)
        risk_distance = abs(entry_price - stop_loss)
        
        if position_direction.upper() == "BUY":
            take_profit = entry_price + (risk_distance * tp_r)
        else:
            take_profit = entry_price - (risk_distance * tp_r)
        
        # Initialize take profit with the multi-stage manager
        tp_result = await self.multi_stage_tp_manager.set_take_profit_levels(
            position_id=position_id,
            entry_price=entry_price,
            stop_loss=stop_loss,
            position_direction=position_direction,
            position_size=position_size,
            symbol=symbol,
            timeframe=timeframe,
            initial_take_profit=take_profit
        )
        
        # Enable trailing take profit if specified
        if params.get("trailing_stop", False):
            await self.multi_stage_tp_manager.enable_trailing_take_profit(
                position_id=position_id,
                enabled=True
            )
        
        return {
            "enabled": True,
            "levels": tp_result.get("tp_levels", []),
            "trailing_enabled": params.get("trailing_stop", False)
        }
    
    @handle_async_errors
    async def _setup_trailing_stop(self, 
                                 position_id: str,
                                 entry_price: float,
                                 stop_loss: float,
                                 position_direction: str,
                                 params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Set up trailing stop parameters.
        
        Returns:
            Trailing stop configuration
        """
        trailing_enabled = params.get("trailing_stop", False)
        
        if not trailing_enabled:
            return {"enabled": False}
        
        # Calculate risk distance (1R)
        risk_distance = abs(entry_price - stop_loss)
        
        # Calculate trailing activation level
        trailing_activation_r = params.get("trailing_activation_r", 1.0)
        
        if position_direction.upper() == "BUY":
            activation_price = entry_price + (risk_distance * trailing_activation_r)
        else:
            activation_price = entry_price - (risk_distance * trailing_activation_r)
        
        # Calculate trailing offset (usually a portion of the risk distance)
        trailing_offset = risk_distance * 0.5  # Default to 0.5R
        
        return {
            "enabled": True,
            "activation_price": activation_price,
            "activation_r": trailing_activation_r,
            "trailing_offset": trailing_offset,
            "current_level": stop_loss
        }
    
    @handle_async_errors
    async def _setup_time_exit(self, 
                             position_id: str,
                             timeframe: str,
                             params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Set up time-based exit parameters.
        
        Returns:
            Time exit configuration
        """
        # Check if we have hours or days parameter
        time_exit_hours = params.get("time_exit_hours", 0)
        time_exit_days = params.get("time_exit_days", 0)
        
        # Convert days to hours if provided
        if time_exit_days > 0:
            time_exit_hours = time_exit_days * 24
        
        # If no time exit specified, disable
        if time_exit_hours <= 0:
            return {"enabled": False}
        
        # Register with time exit manager if available
        if self.time_exit_manager:
            creation_time = datetime.now(timezone.utc)
            
            # Convert hours to appropriate format for time exit manager
            await self.time_exit_manager.register_position(
                position_id=position_id,
                symbol="",  # Will be filled by position tracker
                direction="",  # Will be filled by position tracker
                entry_time=creation_time,
                timeframe=timeframe,
                max_duration_override=time_exit_hours
            )
        
        return {
            "enabled": True,
            "max_hours": time_exit_hours,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "expire_at": (datetime.now(timezone.utc) + timedelta(hours=time_exit_hours)).isoformat()
        }
    
    @handle_async_errors
    async def _setup_volatility_exits(self, 
                                    position_id: str,
                                    symbol: str,
                                    params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Set up volatility-based exit adjustments.
        
        Returns:
            Volatility exit configuration
        """
        if not self.volatility_monitor:
            return {"enabled": False}
        
        # Get current volatility state for this symbol
        volatility_state = self.volatility_monitor.get_volatility_state(symbol)
        
        # Determine if we need volatility-based adjustments
        adjust_for_volatility = params.get("adjust_for_volatility", True)
        
        if not adjust_for_volatility or not volatility_state:
            return {"enabled": False}
        
        # Calculate volatility multiplier
        volatility_percentile = volatility_state.get("volatility_percentile", 0.5)
        
        # Adjust take profit and stop loss based on volatility
        # High volatility = wider targets, low volatility = tighter targets
        if volatility_percentile > 0.7:  # High volatility
            tp_multiplier = 1.3
            sl_multiplier = 1.2
        elif volatility_percentile < 0.3:  # Low volatility
            tp_multiplier = 0.8
            sl_multiplier = 0.9
        else:  # Medium volatility
            tp_multiplier = 1.0
            sl_multiplier = 1.0
        
        return {
            "enabled": True,
            "volatility_percentile": volatility_percentile,
            "tp_multiplier": tp_multiplier,
            "sl_multiplier": sl_multiplier,
            "last_updated": datetime.now(timezone.utc).isoformat()
        }
    
    @handle_async_errors
    async def update_exit_levels(self, 
                               position_id: str, 
                               current_price: float,
                               current_volatility: Optional[float] = None) -> Dict[str, Any]:
        """
        Update exit levels based on current price and market conditions.
        
        Args:
            position_id: Position identifier
            current_price: Current market price
            current_volatility: Current volatility (optional)
            
        Returns:
            Updated exit status
        """
        async with self._lock:
            if position_id not in self.exit_strategies:
                return {"error": f"Position {position_id} not found in exit strategies"}
            
            if position_id not in self.position_exits:
                return {"error": f"Position {position_id} not found in position exits"}
            
            strategy = self.exit_strategies[position_id]
            exits = self.position_exits[position_id]
            
            position_direction = strategy["position_direction"]
            entry_price = strategy["entry_price"]
            risk_distance = strategy["risk_distance"]
            
            # Calculate current R-multiple
            if risk_distance > 0:
                price_movement = abs(current_price - entry_price)
                current_r = price_movement / risk_distance
                
                # Determine if we're in profit or loss
                in_profit = False
                if position_direction.upper() == "BUY":
                    in_profit = current_price > entry_price
                else:
                    in_profit = current_price < entry_price
            else:
                current_r = 0
                in_profit = False
            
            # Update trailing stop if enabled
            trailing_stop = exits.get("trailing_stop", {})
            trailing_stop_triggered = False
            
            if trailing_stop.get("enabled", False):
                # Check if price has reached activation level
                activation_price = trailing_stop.get("activation_price")
                trailing_activated = False
                
                if activation_price is not None:
                    if position_direction.upper() == "BUY":
                        trailing_activated = current_price >= activation_price
                    else:
                        trailing_activated = current_price <= activation_price
                
                # Update trailing stop level if activated
                if trailing_activated:
                    trailing_offset = trailing_stop.get("trailing_offset", risk_distance * 0.5)
                    current_level = trailing_stop.get("current_level")
                    
                    if position_direction.upper() == "BUY":
                        new_level = current_price - trailing_offset
                        if current_level is None or new_level > current_level:
                            trailing_stop["current_level"] = new_level
                    else:
                        new_level = current_price + trailing_offset
                        if current_level is None or new_level < current_level:
                            trailing_stop["current_level"] = new_level
                    
                    # Check if trailing stop is triggered
                    if position_direction.upper() == "BUY":
                        trailing_stop_triggered = current_price <= trailing_stop["current_level"]
                    else:
                        trailing_stop_triggered = current_price >= trailing_stop["current_level"]
            
            # Update volatility adjustments if needed
            volatility_exits = exits.get("volatility_exits", {})
            
            if volatility_exits.get("enabled", False) and current_volatility is not None:
                # Recalculate volatility percentile
                # This would require more data in a real implementation
                volatility_percentile = 0.5  # Placeholder
                
                if current_volatility > 0:
                    volatility_exits["volatility_percentile"] = volatility_percentile
                    
                    # Update multipliers based on new volatility
                    if volatility_percentile > 0.7:  # High volatility
                        volatility_exits["tp_multiplier"] = 1.3
                        volatility_exits["sl_multiplier"] = 1.2
                    elif volatility_percentile < 0.3:  # Low volatility
                        volatility_exits["tp_multiplier"] = 0.8
                        volatility_exits["sl_multiplier"] = 0.9
                    else:  # Medium volatility
                        volatility_exits["tp_multiplier"] = 1.0
                        volatility_exits["sl_multiplier"] = 1.0
                        
                    volatility_exits["last_updated"] = datetime.now(timezone.utc).isoformat()
            
            # Check take profit levels via multi-stage take profit manager
            tp_hit = False
            tp_levels_hit = []
            
            if self.multi_stage_tp_manager and "take_profits" in exits and exits["take_profits"].get("enabled", False):
                tp_result = await self.multi_stage_tp_manager.check_take_profit_levels(
                    position_id=position_id,
                    current_price=current_price
                )
                
                if tp_result.get("triggered_levels"):
                    tp_hit = True
                    tp_levels_hit = tp_result.get("triggered_levels", [])
            
            # Check time-based exit
            time_exit_triggered = False
            time_exit = exits.get("time_exit", {})
            
            if time_exit.get("enabled", False):
                expire_at = time_exit.get("expire_at")
                if expire_at:
                    expire_time = datetime.fromisoformat(expire_at)
                    time_exit_triggered = datetime.now(timezone.utc) >= expire_time
            
            # Store updated exit information
            exits["trailing_stop"] = trailing_stop
            exits["volatility_exits"] = volatility_exits
            
            # Determine overall exit status
            exit_triggered = trailing_stop_triggered or tp_hit or time_exit_triggered
            exit_reason = None
            exit_price = None
            
            if trailing_stop_triggered:
                exit_reason = "trailing_stop"
                exit_price = trailing_stop.get("current_level")
            elif tp_hit:
                exit_reason = "take_profit"
                # Use the last TP level hit as the exit price
                if tp_levels_hit:
                    exit_price = tp_levels_hit[-1].get("price")
            elif time_exit_triggered:
                exit_reason = "time_exit"
                exit_price = current_price
            
            return {
                "position_id": position_id,
                "current_price": current_price,
                "current_r": current_r,
                "in_profit": in_profit,
                "exit_triggered": exit_triggered,
                "exit_reason": exit_reason,
                "exit_price": exit_price,
                "trailing_stop": {
                    "triggered": trailing_stop_triggered,
                    "level": trailing_stop.get("current_level")
                },
                "take_profit": {
                    "hit": tp_hit,
                    "levels_hit": tp_levels_hit
                },
                "time_exit": {
                    "triggered": time_exit_triggered,
                    "expire_at": time_exit.get("expire_at")
                }
            }
    
    @handle_async_errors
    async def execute_exit_if_triggered(self, 
                                      position_id: str, 
                                      current_price: float) -> Dict[str, Any]:
        """
        Check if any exit conditions are met and execute the exit if needed.
        
        Args:
            position_id: Position identifier
            current_price: Current market price
            
        Returns:
            Exit execution result
        """
        # Update exit levels and check if any are triggered
        exit_status = await self.update_exit_levels(position_id, current_price)
        
        if not exit_status.get("exit_triggered", False):
            return {"executed": False, "status": exit_status}
        
        # Exit is triggered, execute it
        exit_reason = exit_status.get("exit_reason", "unknown")
        exit_price = exit_status.get("exit_price", current_price)
        
        result = await self._execute_exit(position_id, exit_price, exit_reason)
        
        if result.get("success", False):
            # Record exit in history
            await self._record_exit(position_id, exit_status, result)
            
            # Cleanup exit tracking
            await self._cleanup_exit_tracking(position_id)
            
            return {
                "executed": True,
                "reason": exit_reason,
                "price": exit_price,
                "status": exit_status,
                "execution": result
            }
        else:
            return {
                "executed": False,
                "reason": exit_reason,
                "price": exit_price,
                "status": exit_status,
                "execution": result,
                "error": result.get("error")
            }
    
    @handle_async_errors
    async def _execute_exit(self, 
                          position_id: str, 
                          exit_price: float, 
                          reason: str) -> Dict[str, Any]:
        """
        Execute position exit via position tracker.
        
        Args:
            position_id: Position identifier
            exit_price: Exit price
            reason: Exit reason
            
        Returns:
            Execution result
        """
        if not self.position_tracker:
            return {"success": False, "error": "No position tracker available"}
        
        try:
            # Calculate PnL and R-multiple
            strategy = self.exit_strategies.get(position_id)
            
            if not strategy:
                return {"success": False, "error": f"Strategy not found for position {position_id}"}
            
            entry_price = strategy.get("entry_price")
            position_direction = strategy.get("position_direction")
            position_size = strategy.get("position_size")
            risk_distance = strategy.get("risk_distance")
            
            # Calculate PnL
            if position_direction.upper() == "BUY":
                pnl = (exit_price - entry_price) * position_size
            else:
                pnl = (entry_price - exit_price) * position_size
            
            # Calculate R-multiple
            r_multiple = 0
            if risk_distance > 0:
                price_movement = abs(exit_price - entry_price)
                if position_direction.upper() == "BUY" and exit_price > entry_price:
                    r_multiple = price_movement / risk_distance
                elif position_direction.upper() == "SELL" and exit_price < entry_price:
                    r_multiple = price_movement / risk_distance
                else:
                    r_multiple = -price_movement / risk_distance
            
            # Execute exit via position tracker
            await self.position_tracker.clear_position(
                position_id=position_id,
                pnl=pnl,
                r_multiple=r_multiple,
                reason=reason
            )
            
            return {
                "success": True,
                "pnl": pnl,
                "r_multiple": r_multiple,
                "reason": reason
            }
        except Exception as e:
            logger.error(f"Error executing exit for position {position_id}: {str(e)}")
            return {"success": False, "error": str(e)}
    
    @handle_async_errors
    async def _record_exit(self, 
                         position_id: str, 
                         exit_status: Dict[str, Any], 
                         execution_result: Dict[str, Any]) -> None:
        """
        Record exit details for performance tracking.
        
        Args:
            position_id: Position identifier
            exit_status: Exit status information
            execution_result: Execution result information
        """
        strategy = self.exit_strategies.get(position_id)
        
        if not strategy:
            return
        
        strategy_type = strategy.get("strategy_type", "unknown")
        pnl = execution_result.get("pnl", 0)
        r_multiple = execution_result.get("r_multiple", 0)
        exit_reason = exit_status.get("exit_reason", "unknown")
        
        exit_record = {
            "position_id": position_id,
            "strategy_type": strategy_type,
            "exit_reason": exit_reason,
            "pnl": pnl,
            "r_multiple": r_multiple,
            "exit_time": datetime.now(timezone.utc).isoformat(),
            "exit_price": exit_status.get("exit_price"),
            "entry_price": strategy.get("entry_price"),
            "position_direction": strategy.get("position_direction"),
            "timeframe": strategy.get("timeframe"),
            "symbol": strategy.get("symbol")
        }
        
        # Add to exit history
        self.exit_history.append(exit_record)
        
        # Update performance metrics
        self.performance_metrics["overall"]["total_exits"] += 1
        
        if pnl > 0:
            self.performance_metrics["overall"]["profitable_exits"] += 1
        
        # Update strategy-specific metrics
        if strategy_type not in self.performance_metrics["strategies"]:
            self.performance_metrics["strategies"][strategy_type] = {
                "total_exits": 0,
                "profitable_exits": 0,
                "avg_r": 0,
                "total_r": 0,
                "exit_reasons": {}
            }
        
        strategy_metrics = self.performance_metrics["strategies"][strategy_type]
        strategy_metrics["total_exits"] += 1
        
        if pnl > 0:
            strategy_metrics["profitable_exits"] += 1
        
        # Update R multiple tracking
        strategy_metrics["total_r"] += r_multiple
        strategy_metrics["avg_r"] = strategy_metrics["total_r"] / strategy_metrics["total_exits"]
        
        # Update exit reason tracking
        if exit_reason not in strategy_metrics["exit_reasons"]:
            strategy_metrics["exit_reasons"][exit_reason] = 0
        
        strategy_metrics["exit_reasons"][exit_reason] += 1
    
    @handle_async_errors
    async def _cleanup_exit_tracking(self, position_id: str) -> None:
        """
        Clean up exit tracking data after position is closed.
        
        Args:
            position_id: Position identifier
        """
        # Remove from exit strategies and position exits
        if position_id in self.exit_strategies:
            del self.exit_strategies[position_id]
        
        if position_id in self.position_exits:
            del self.position_exits[position_id]
        
        # Remove from external managers
        if self.time_exit_manager:
            self.time_exit_manager.remove_position(position_id)
    
    @handle_async_errors
    async def get_exit_status(self, position_id: str) -> Dict[str, Any]:
        """
        Get current exit status for a position.
        
        Args:
            position_id: Position identifier
            
        Returns:
            Current exit configuration and status
        """
        async with self._lock:
            if position_id not in self.exit_strategies:
                return {"error": f"Position {position_id} not found in exit strategies"}
            
            if position_id not in self.position_exits:
                return {"error": f"Position {position_id} not found in position exits"}
            
            strategy = self.exit_strategies[position_id]
            exits = self.position_exits[position_id]
            
            return {
                "position_id": position_id,
                "strategy_type": strategy.get("strategy_type"),
                "symbol": strategy.get("symbol"),
                "timeframe": strategy.get("timeframe"),
                "entry_price": strategy.get("entry_price"),
                "stop_loss": strategy.get("stop_loss"),
                "risk_distance": strategy.get("risk_distance"),
                "exits": exits
            }
    
    @handle_async_errors
    async def get_performance_metrics(self, strategy_type: Optional[str] = None) -> Dict[str, Any]:
        """
        Get performance metrics for exit strategies.
        
        Args:
            strategy_type: Optional filter by strategy type
            
        Returns:
            Performance metrics
        """
        if strategy_type:
            if strategy_type in self.performance_metrics["strategies"]:
                return {
                    "strategy_type": strategy_type,
                    "metrics": self.performance_metrics["strategies"][strategy_type]
                }
            else:
                return {"error": f"No metrics available for strategy type: {strategy_type}"}
        else:
            # Overall metrics
            overall = self.performance_metrics["overall"]
            
            # Calculate win rate
            win_rate = 0
            if overall["total_exits"] > 0:
                win_rate = (overall["profitable_exits"] / overall["total_exits"]) * 100
            
            # Add win rate to metrics
            metrics = overall.copy()
            metrics["win_rate"] = win_rate
            
            # Add strategy breakdown
            metrics["strategies"] = self.performance_metrics["strategies"]
            
            return {
                "overall": metrics,
                "total_strategies": len(self.performance_metrics["strategies"])
            }
    
    @handle_async_errors
    async def get_exit_history(self, 
                             limit: int = 20, 
                             symbol: Optional[str] = None,
                             strategy_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get exit history with optional filtering.
        
        Args:
            limit: Maximum number of records to return
            symbol: Optional filter by symbol
            strategy_type: Optional filter by strategy type
            
        Returns:
            List of exit history records
        """
        # Make a copy of the history to avoid race conditions
        history = self.exit_history.copy()
        
        # Apply filters
        if symbol:
            history = [h for h in history if h.get("symbol") == symbol]
        
        if strategy_type:
            history = [h for h in history if h.get("strategy_type") == strategy_type]
        
        # Sort by exit time (newest first)
        history.sort(key=lambda x: x.get("exit_time", ""), reverse=True)
        
        # Limit results
        return history[:limit] 