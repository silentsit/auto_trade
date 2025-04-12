"""
Advanced Risk Analytics for FX Trading Bridge

This module provides comprehensive risk and performance metrics including:
- Sharpe ratio calculation
- Sortino ratio calculation 
- Maximum drawdown analysis
- Win/loss statistics
- Risk-adjusted returns
- Performance attribution
"""

import logging
import asyncio
import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone
from functools import wraps
import traceback
from collections import deque
import math
import json

# Setup logging
logger = logging.getLogger("fx-trading-bridge.risk_analytics")

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

def handle_sync_errors(func):
    """Decorator for handling errors in synchronous functions"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    return wrapper

class AdvancedRiskAnalytics:
    """
    Comprehensive risk analytics and performance measurement system.
    
    Features:
    - Trade performance metrics (Sharpe, Sortino, etc.)
    - Drawdown analysis
    - Risk-adjusted returns
    - Win/loss statistics
    - Performance attribution by timeframe, strategy, and symbol
    - Historical performance comparisons
    - Risk exposure analysis
    """
    
    def __init__(self, 
                position_tracker=None,
                trading_config=None,
                risk_free_rate: float = 0.02):  # 2% annual risk-free rate by default
        """
        Initialize the risk analytics system.
        
        Args:
            position_tracker: Reference to the position tracker
            trading_config: Reference to the trading configuration
            risk_free_rate: Annual risk-free rate for Sharpe ratio calculations
        """
        self.position_tracker = position_tracker
        self.trading_config = trading_config
        self.risk_free_rate = risk_free_rate / 365.0  # Convert to daily rate
        
        self._lock = asyncio.Lock()
        
        # Performance data
        self.daily_returns = []  # List of daily return percentages
        self.trade_returns = []  # List of all trade returns
        self.drawdown_history = []  # History of drawdowns
        self.peak_equity = 0.0  # Highest equity achieved
        self.metrics_history = {}  # Historical metrics
        
        # Performance by category
        self.performance_by_symbol = {}
        self.performance_by_timeframe = {}
        self.performance_by_strategy = {}
        
        # Current metrics
        self.current_metrics = {
            "sharpe_ratio": 0.0,
            "sortino_ratio": 0.0,
            "max_drawdown": 0.0,
            "current_drawdown": 0.0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "average_win": 0.0,
            "average_loss": 0.0,
            "largest_win": 0.0,
            "largest_loss": 0.0,
            "expectancy": 0.0,
            "total_trades": 0,
            "winning_trades": 0,
            "losing_trades": 0,
            "break_even_trades": 0,
            "r_multiple_avg": 0.0,
            "r_multiple_std": 0.0,
            "last_updated": datetime.now(timezone.utc).isoformat()
        }
        
        # Configuration
        self.metrics_window_days = 60  # Default metrics calculation window
        self.update_frequency_minutes = 30  # How often to update metrics
        self.min_trades_for_metrics = 10  # Minimum trades needed for meaningful metrics
        
        # Current state
        self.is_running = False
        self.update_task = None
        
        logger.info("Advanced Risk Analytics initialized")
    
    @handle_async_errors
    async def start(self):
        """Start the risk analytics system"""
        if self.is_running:
            return
        
        self.is_running = True
        
        # Load historical data
        await self._load_data()
        
        # Start periodic updates
        self.update_task = asyncio.create_task(self._periodic_updates())
        
        logger.info("Advanced Risk Analytics started")
    
    @handle_async_errors
    async def stop(self):
        """Stop the risk analytics system"""
        if not self.is_running:
            return
        
        self.is_running = False
        
        if self.update_task:
            self.update_task.cancel()
            try:
                await self.update_task
            except asyncio.CancelledError:
                pass
            self.update_task = None
        
        # Save data before stopping
        await self._save_data()
        
        logger.info("Advanced Risk Analytics stopped")
    
    @handle_async_errors
    async def _load_data(self):
        """Load historical performance data"""
        try:
            # Implementation would load data from files or database
            # For now, using empty initialization
            logger.info("Loading historical performance data")
            self.daily_returns = []
            self.trade_returns = []
            self.drawdown_history = []
            self.peak_equity = 0.0
            
            # Reset metrics
            await self._calculate_all_metrics()
        except Exception as e:
            logger.error(f"Error loading risk analytics data: {str(e)}")
    
    @handle_async_errors
    async def _save_data(self):
        """Save performance data"""
        try:
            # Implementation would save data to files or database
            logger.info("Saving performance data")
            # Save data implementation would go here
        except Exception as e:
            logger.error(f"Error saving risk analytics data: {str(e)}")
    
    @handle_async_errors
    async def _periodic_updates(self):
        """Periodically update metrics"""
        while self.is_running:
            try:
                await self._calculate_all_metrics()
                await asyncio.sleep(self.update_frequency_minutes * 60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic metrics update: {str(e)}")
                await asyncio.sleep(60)  # Reduced sleep on error
    
    @handle_async_errors
    async def _calculate_all_metrics(self):
        """Calculate all risk and performance metrics"""
        async with self._lock:
            if len(self.trade_returns) < self.min_trades_for_metrics:
                logger.info(f"Not enough trades ({len(self.trade_returns)}) for meaningful metrics calculation")
                return
            
            # Convert to numpy array for calculations
            returns_array = np.array(self.trade_returns)
            daily_returns_array = np.array(self.daily_returns) if self.daily_returns else np.array([0.0])
            
            # Calculate basic metrics
            total_trades = len(returns_array)
            winning_trades = np.sum(returns_array > 0)
            losing_trades = np.sum(returns_array < 0)
            break_even_trades = np.sum(returns_array == 0)
            
            win_rate = winning_trades / total_trades if total_trades > 0 else 0
            
            # Average returns
            avg_return = np.mean(returns_array) if total_trades > 0 else 0
            avg_win = np.mean(returns_array[returns_array > 0]) if winning_trades > 0 else 0
            avg_loss = np.mean(returns_array[returns_array < 0]) if losing_trades > 0 else 0
            
            # Largest win/loss
            largest_win = np.max(returns_array) if total_trades > 0 else 0
            largest_loss = np.min(returns_array) if total_trades > 0 else 0
            
            # Profit factor
            gross_profit = np.sum(returns_array[returns_array > 0]) if winning_trades > 0 else 0
            gross_loss = np.abs(np.sum(returns_array[returns_array < 0])) if losing_trades > 0 else 0
            profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
            
            # Risk-adjusted metrics
            # For Sharpe & Sortino, we need annualized returns and volatility
            if len(daily_returns_array) > 0:
                excess_returns = daily_returns_array - self.risk_free_rate
                annualized_return = np.mean(daily_returns_array) * 252  # Trading days in a year
                annualized_std = np.std(daily_returns_array) * np.sqrt(252)
                
                # Sharpe Ratio
                sharpe_ratio = (annualized_return - (self.risk_free_rate * 252)) / annualized_std if annualized_std > 0 else 0
                
                # Sortino Ratio (only considers downside deviation)
                downside_returns = daily_returns_array[daily_returns_array < 0]
                downside_deviation = np.std(downside_returns) * np.sqrt(252) if len(downside_returns) > 0 else 0.0001
                sortino_ratio = (annualized_return - (self.risk_free_rate * 252)) / downside_deviation if downside_deviation > 0 else 0
            else:
                sharpe_ratio = 0
                sortino_ratio = 0
            
            # Maximum drawdown calculation
            max_drawdown = self._calculate_max_drawdown()
            current_drawdown = self._calculate_current_drawdown()
            
            # R-multiple statistics if available
            r_multiple_avg = 0.0
            r_multiple_std = 0.0
            
            # Expectancy calculation: (Win% * Avg Win) - (Loss% * Avg Loss)
            expectancy = (win_rate * avg_win) - ((1 - win_rate) * abs(avg_loss)) if total_trades > 0 else 0
            
            # Update current metrics
            self.current_metrics = {
                "sharpe_ratio": sharpe_ratio,
                "sortino_ratio": sortino_ratio,
                "max_drawdown": max_drawdown,
                "current_drawdown": current_drawdown,
                "win_rate": win_rate,
                "profit_factor": profit_factor,
                "average_win": avg_win,
                "average_loss": avg_loss,
                "largest_win": largest_win,
                "largest_loss": largest_loss,
                "expectancy": expectancy,
                "total_trades": total_trades,
                "winning_trades": int(winning_trades),
                "losing_trades": int(losing_trades),
                "break_even_trades": int(break_even_trades),
                "r_multiple_avg": r_multiple_avg,
                "r_multiple_std": r_multiple_std,
                "last_updated": datetime.now(timezone.utc).isoformat()
            }
            
            # Save current metrics to history
            current_date = datetime.now(timezone.utc).date().isoformat()
            self.metrics_history[current_date] = self.current_metrics.copy()
            
            logger.info(f"Updated risk metrics: Sharpe={sharpe_ratio:.2f}, Win Rate={win_rate:.2%}, Expectancy={expectancy:.4f}")
    
    @handle_sync_errors
    def _calculate_max_drawdown(self) -> float:
        """Calculate maximum historical drawdown"""
        if not self.drawdown_history:
            return 0.0
        
        return max(self.drawdown_history)
    
    @handle_sync_errors
    def _calculate_current_drawdown(self) -> float:
        """Calculate current drawdown from peak equity"""
        if self.peak_equity <= 0:
            return 0.0
        
        # Would need current equity from position tracker
        current_equity = 0.0
        
        if self.position_tracker:
            # Implementation would get actual equity
            pass
        
        if current_equity <= 0 or current_equity >= self.peak_equity:
            return 0.0
        
        return (self.peak_equity - current_equity) / self.peak_equity
    
    @handle_async_errors
    async def record_trade_result(self, 
                                trade_data: Dict[str, Any]) -> None:
        """
        Record a completed trade for analytics.
        
        Args:
            trade_data: Dictionary containing trade details including:
                - pnl: Profit/loss amount
                - pnl_percentage: Profit/loss as percentage
                - symbol: Trading symbol
                - timeframe: Trading timeframe
                - strategy: Strategy used
                - entry_time: Entry timestamp
                - exit_time: Exit timestamp
                - direction: Trade direction (buy/sell)
                - r_multiple: R-multiple result if available
                - exit_reason: Reason for exit
        """
        async with self._lock:
            # Extract trade return
            pnl_percentage = trade_data.get("pnl_percentage", 0.0)
            self.trade_returns.append(pnl_percentage)
            
            # Update peak equity if needed
            current_equity = trade_data.get("equity_after", 0.0)
            if current_equity > self.peak_equity:
                self.peak_equity = current_equity
            
            # Calculate and record drawdown if applicable
            if self.peak_equity > 0 and current_equity < self.peak_equity:
                drawdown = (self.peak_equity - current_equity) / self.peak_equity
                self.drawdown_history.append(drawdown)
            
            # Update category performance
            symbol = trade_data.get("symbol", "unknown")
            timeframe = trade_data.get("timeframe", "unknown")
            strategy = trade_data.get("strategy", "unknown")
            
            # Update symbol performance
            if symbol not in self.performance_by_symbol:
                self.performance_by_symbol[symbol] = {
                    "trades": 0,
                    "winning_trades": 0,
                    "total_return": 0.0,
                    "average_return": 0.0,
                    "win_rate": 0.0
                }
            
            self.performance_by_symbol[symbol]["trades"] += 1
            self.performance_by_symbol[symbol]["total_return"] += pnl_percentage
            
            if pnl_percentage > 0:
                self.performance_by_symbol[symbol]["winning_trades"] += 1
            
            trades_for_symbol = self.performance_by_symbol[symbol]["trades"]
            winning_trades_for_symbol = self.performance_by_symbol[symbol]["winning_trades"]
            
            self.performance_by_symbol[symbol]["average_return"] = (
                self.performance_by_symbol[symbol]["total_return"] / trades_for_symbol
            )
            self.performance_by_symbol[symbol]["win_rate"] = (
                winning_trades_for_symbol / trades_for_symbol
            )
            
            # Similarly update timeframe and strategy performance
            # (Similar implementation as above for both categories)
            
            # Recalculate metrics after new trade
            await self._calculate_all_metrics()
    
    @handle_async_errors
    async def record_daily_return(self, date: datetime, return_pct: float) -> None:
        """
        Record daily return for Sharpe and Sortino calculations.
        
        Args:
            date: Date of the return
            return_pct: Percentage return for the day
        """
        async with self._lock:
            self.daily_returns.append(return_pct)
            
            # Limit history length if needed
            max_days = self.metrics_window_days
            if len(self.daily_returns) > max_days:
                self.daily_returns = self.daily_returns[-max_days:]
    
    @handle_async_errors
    async def get_current_metrics(self) -> Dict[str, Any]:
        """
        Get current risk and performance metrics.
        
        Returns:
            Dictionary of current metrics
        """
        async with self._lock:
            return self.current_metrics.copy()
    
    @handle_async_errors
    async def get_historical_metrics(self, 
                                   days: int = 30) -> Dict[str, Dict[str, Any]]:
        """
        Get historical metrics over a period.
        
        Args:
            days: Number of days of history to return
            
        Returns:
            Dictionary of metrics by date
        """
        async with self._lock:
            # Sort dates and take the most recent ones up to the requested number
            dates = sorted(self.metrics_history.keys(), reverse=True)
            recent_dates = dates[:days]
            
            return {date: self.metrics_history[date] for date in recent_dates}
    
    @handle_async_errors
    async def get_symbol_performance(self, 
                                   symbol: Optional[str] = None) -> Dict[str, Any]:
        """
        Get performance metrics by symbol.
        
        Args:
            symbol: Optional specific symbol to get, or all if None
            
        Returns:
            Performance metrics by symbol
        """
        async with self._lock:
            if symbol:
                return self.performance_by_symbol.get(symbol, {})
            else:
                return self.performance_by_symbol.copy()
    
    @handle_async_errors
    async def get_timeframe_performance(self, 
                                      timeframe: Optional[str] = None) -> Dict[str, Any]:
        """
        Get performance metrics by timeframe.
        
        Args:
            timeframe: Optional specific timeframe to get, or all if None
            
        Returns:
            Performance metrics by timeframe
        """
        async with self._lock:
            if timeframe:
                return self.performance_by_timeframe.get(timeframe, {})
            else:
                return self.performance_by_timeframe.copy()
    
    @handle_async_errors
    async def get_strategy_performance(self, 
                                     strategy: Optional[str] = None) -> Dict[str, Any]:
        """
        Get performance metrics by strategy.
        
        Args:
            strategy: Optional specific strategy to get, or all if None
            
        Returns:
            Performance metrics by strategy
        """
        async with self._lock:
            if strategy:
                return self.performance_by_strategy.get(strategy, {})
            else:
                return self.performance_by_strategy.copy()
    
    @handle_async_errors
    async def get_drawdown_analysis(self) -> Dict[str, Any]:
        """
        Get detailed drawdown analysis.
        
        Returns:
            Drawdown statistics and historical data
        """
        async with self._lock:
            if not self.drawdown_history:
                return {
                    "max_drawdown": 0.0,
                    "current_drawdown": 0.0,
                    "average_drawdown": 0.0,
                    "drawdown_count": 0,
                    "recovery_time_avg": 0,
                    "longest_drawdown_days": 0
                }
            
            max_dd = max(self.drawdown_history) if self.drawdown_history else 0.0
            current_dd = self._calculate_current_drawdown()
            avg_dd = np.mean(self.drawdown_history) if self.drawdown_history else 0.0
            
            return {
                "max_drawdown": max_dd,
                "current_drawdown": current_dd,
                "average_drawdown": avg_dd,
                "drawdown_count": len(self.drawdown_history),
                "recovery_time_avg": 0,  # Would require additional tracking
                "longest_drawdown_days": 0  # Would require additional tracking
            }
    
    @handle_async_errors
    async def get_risk_exposure_analysis(self) -> Dict[str, Any]:
        """
        Get current risk exposure analysis.
        
        Returns:
            Risk exposure metrics
        """
        # This would require integration with position tracker to get current positions
        # and calculate risk metrics based on those positions
        
        return {
            "total_exposure": 0.0,
            "exposure_by_symbol": {},
            "exposure_by_direction": {
                "long": 0.0,
                "short": 0.0
            },
            "concentration_risk": 0.0,
            "correlation_risk": 0.0,
            "position_count": 0
        } 