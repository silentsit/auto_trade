"""
INSTITUTIONAL PERFORMANCE ANALYTICS
Enhanced metrics collection with institutional-grade performance analysis
"""

import asyncio
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class MetricType(Enum):
    PNL = "pnl"
    DRAWDOWN = "drawdown"
    SHARPE_RATIO = "sharpe_ratio"
    SORTINO_RATIO = "sortino_ratio"
    CALMAR_RATIO = "calmar_ratio"
    WIN_RATE = "win_rate"
    PROFIT_FACTOR = "profit_factor"
    MAX_DRAWDOWN = "max_drawdown"
    VOLATILITY = "volatility"
    BETA = "beta"

@dataclass
class PerformanceMetrics:
    """Institutional performance metrics"""
    total_pnl: float
    total_return: float
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    max_drawdown: float
    current_drawdown: float
    win_rate: float
    profit_factor: float
    volatility: float
    beta: float
    total_trades: int
    winning_trades: int
    losing_trades: int
    avg_win: float
    avg_loss: float
    largest_win: float
    largest_loss: float
    consecutive_wins: int
    consecutive_losses: int
    avg_trade_duration: float
    risk_adjusted_return: float

class InstitutionalMetricsCollector:
    """
    Institutional-grade performance analytics with comprehensive metrics
    """
    
    def __init__(self):
        self.trade_history = []
        self.daily_returns = []
        self.metrics_cache = {}
        self.cache_duration = timedelta(minutes=5)
        self.last_cache_update = None
        
    async def add_trade(self, trade_data: Dict[str, Any]):
        """Add trade to history for analysis"""
        trade_record = {
            'timestamp': datetime.now(timezone.utc),
            'symbol': trade_data.get('symbol'),
            'side': trade_data.get('side'),
            'units': trade_data.get('units', 0),
            'entry_price': trade_data.get('entry_price', 0),
            'exit_price': trade_data.get('exit_price', 0),
            'pnl': trade_data.get('pnl', 0),
            'duration': trade_data.get('duration', 0),
            'stop_loss': trade_data.get('stop_loss'),
            'take_profit': trade_data.get('take_profit'),
            'risk_amount': trade_data.get('risk_amount', 0),
            'account_balance': trade_data.get('account_balance', 0)
        }
        
        self.trade_history.append(trade_record)
        
        # Update daily returns
        await self._update_daily_returns(trade_record)
        
        # Clear cache to force recalculation
        self.metrics_cache.clear()
        
        logger.debug(f"Added trade: {trade_record['symbol']} PnL: {trade_record['pnl']:.2f}")
    
    async def calculate_performance_metrics(self, 
                                         lookback_days: int = 30) -> PerformanceMetrics:
        """
        Calculate comprehensive performance metrics
        
        Args:
            lookback_days: Number of days to analyze
            
        Returns:
            PerformanceMetrics object
        """
        # Check cache first
        cache_key = f"metrics_{lookback_days}"
        if (self.metrics_cache.get(cache_key) and 
            self.last_cache_update and 
            datetime.now() - self.last_cache_update < self.cache_duration):
            return self.metrics_cache[cache_key]
        
        # Filter trades by lookback period
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        recent_trades = [
            trade for trade in self.trade_history
            if trade['timestamp'] >= cutoff_date
        ]
        
        if not recent_trades:
            return self._create_empty_metrics()
        
        # Calculate basic metrics
        total_pnl = sum(trade['pnl'] for trade in recent_trades)
        total_trades = len(recent_trades)
        winning_trades = [t for t in recent_trades if t['pnl'] > 0]
        losing_trades = [t for t in recent_trades if t['pnl'] < 0]
        
        # Win rate and profit metrics
        win_rate = len(winning_trades) / total_trades if total_trades > 0 else 0
        avg_win = np.mean([t['pnl'] for t in winning_trades]) if winning_trades else 0
        avg_loss = np.mean([t['pnl'] for t in losing_trades]) if losing_trades else 0
        
        # Profit factor
        total_wins = sum(t['pnl'] for t in winning_trades)
        total_losses = abs(sum(t['pnl'] for t in losing_trades))
        profit_factor = total_wins / total_losses if total_losses > 0 else float('inf')
        
        # Largest wins/losses
        largest_win = max([t['pnl'] for t in winning_trades]) if winning_trades else 0
        largest_loss = min([t['pnl'] for t in losing_trades]) if losing_trades else 0
        
        # Consecutive wins/losses
        consecutive_wins, consecutive_losses = self._calculate_consecutive_streaks(recent_trades)
        
        # Average trade duration
        avg_duration = np.mean([t['duration'] for t in recent_trades if t['duration'] > 0])
        
        # Calculate returns and risk metrics
        returns = await self._calculate_returns(lookback_days)
        volatility = np.std(returns) if len(returns) > 1 else 0
        
        # Risk-adjusted metrics
        sharpe_ratio = self._calculate_sharpe_ratio(returns)
        sortino_ratio = self._calculate_sortino_ratio(returns)
        
        # Drawdown analysis
        max_drawdown, current_drawdown = self._calculate_drawdown(recent_trades)
        
        # Calmar ratio
        calmar_ratio = total_pnl / max_drawdown if max_drawdown > 0 else 0
        
        # Beta calculation (simplified - would need market data)
        beta = 1.0  # Placeholder
        
        # Risk-adjusted return
        risk_adjusted_return = total_pnl / (volatility * np.sqrt(252)) if volatility > 0 else 0
        
        # Total return percentage
        initial_balance = recent_trades[0]['account_balance'] if recent_trades else 1
        total_return = (total_pnl / initial_balance) * 100 if initial_balance > 0 else 0
        
        metrics = PerformanceMetrics(
            total_pnl=total_pnl,
            total_return=total_return,
            sharpe_ratio=sharpe_ratio,
            sortino_ratio=sortino_ratio,
            calmar_ratio=calmar_ratio,
            max_drawdown=max_drawdown,
            current_drawdown=current_drawdown,
            win_rate=win_rate,
            profit_factor=profit_factor,
            volatility=volatility,
            beta=beta,
            total_trades=total_trades,
            winning_trades=len(winning_trades),
            losing_trades=len(losing_trades),
            avg_win=avg_win,
            avg_loss=avg_loss,
            largest_win=largest_win,
            largest_loss=largest_loss,
            consecutive_wins=consecutive_wins,
            consecutive_losses=consecutive_losses,
            avg_trade_duration=avg_duration,
            risk_adjusted_return=risk_adjusted_return
        )
        
        # Cache the results
        self.metrics_cache[cache_key] = metrics
        self.last_cache_update = datetime.now()
        
        return metrics
    
    async def _calculate_returns(self, lookback_days: int) -> List[float]:
        """Calculate daily returns"""
        if not self.daily_returns:
            return []
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        recent_returns = [
            ret for ret in self.daily_returns
            if ret['date'] >= cutoff_date
        ]
        
        return [ret['return'] for ret in recent_returns]
    
    def _calculate_sharpe_ratio(self, returns: List[float]) -> float:
        """Calculate Sharpe ratio"""
        if not returns or len(returns) < 2:
            return 0.0
        
        returns_array = np.array(returns)
        mean_return = np.mean(returns_array)
        std_return = np.std(returns_array)
        
        # Annualized Sharpe ratio (assuming daily returns)
        sharpe = (mean_return / std_return) * np.sqrt(252) if std_return > 0 else 0
        
        return sharpe
    
    def _calculate_sortino_ratio(self, returns: List[float]) -> float:
        """Calculate Sortino ratio"""
        if not returns or len(returns) < 2:
            return 0.0
        
        returns_array = np.array(returns)
        mean_return = np.mean(returns_array)
        
        # Calculate downside deviation
        downside_returns = returns_array[returns_array < 0]
        downside_deviation = np.std(downside_returns) if len(downside_returns) > 0 else 0
        
        # Annualized Sortino ratio
        sortino = (mean_return / downside_deviation) * np.sqrt(252) if downside_deviation > 0 else 0
        
        return sortino
    
    def _calculate_drawdown(self, trades: List[Dict[str, Any]]) -> Tuple[float, float]:
        """Calculate maximum and current drawdown"""
        if not trades:
            return 0.0, 0.0
        
        # Calculate cumulative PnL
        cumulative_pnl = []
        running_pnl = 0
        
        for trade in trades:
            running_pnl += trade['pnl']
            cumulative_pnl.append(running_pnl)
        
        if not cumulative_pnl:
            return 0.0, 0.0
        
        # Calculate drawdown
        peak = cumulative_pnl[0]
        max_drawdown = 0
        current_drawdown = 0
        
        for pnl in cumulative_pnl:
            if pnl > peak:
                peak = pnl
            else:
                drawdown = (peak - pnl) / peak if peak > 0 else 0
                max_drawdown = max(max_drawdown, drawdown)
                current_drawdown = drawdown
        
        return max_drawdown * 100, current_drawdown * 100  # Convert to percentage
    
    def _calculate_consecutive_streaks(self, trades: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Calculate consecutive winning and losing streaks"""
        if not trades:
            return 0, 0
        
        max_consecutive_wins = 0
        max_consecutive_losses = 0
        current_wins = 0
        current_losses = 0
        
        for trade in trades:
            if trade['pnl'] > 0:
                current_wins += 1
                current_losses = 0
                max_consecutive_wins = max(max_consecutive_wins, current_wins)
            elif trade['pnl'] < 0:
                current_losses += 1
                current_wins = 0
                max_consecutive_losses = max(max_consecutive_losses, current_losses)
        
        return max_consecutive_wins, max_consecutive_losses
    
    async def _update_daily_returns(self, trade: Dict[str, Any]):
        """Update daily returns calculation"""
        trade_date = trade['timestamp'].date()
        
        # Find existing daily return entry
        existing_entry = None
        for entry in self.daily_returns:
            if entry['date'].date() == trade_date:
                existing_entry = entry
                break
        
        if existing_entry:
            existing_entry['pnl'] += trade['pnl']
            existing_entry['trades'] += 1
        else:
            self.daily_returns.append({
                'date': trade['timestamp'],
                'pnl': trade['pnl'],
                'trades': 1,
                'return': 0  # Will be calculated later
            })
        
        # Calculate daily returns
        self._calculate_daily_return_percentages()
    
    def _calculate_daily_return_percentages(self):
        """Calculate daily return percentages"""
        if not self.daily_returns:
            return
        
        # Sort by date
        self.daily_returns.sort(key=lambda x: x['date'])
        
        # Calculate return percentages
        for i, entry in enumerate(self.daily_returns):
            if i == 0:
                entry['return'] = entry['pnl'] / 10000  # Assume $10k starting balance
            else:
                # Calculate return relative to previous day's cumulative PnL
                prev_cumulative = sum(self.daily_returns[j]['pnl'] for j in range(i))
                if prev_cumulative != 0:
                    entry['return'] = entry['pnl'] / (10000 + prev_cumulative)
                else:
                    entry['return'] = entry['pnl'] / 10000
    
    def _create_empty_metrics(self) -> PerformanceMetrics:
        """Create empty metrics when no data is available"""
        return PerformanceMetrics(
            total_pnl=0.0,
            total_return=0.0,
            sharpe_ratio=0.0,
            sortino_ratio=0.0,
            calmar_ratio=0.0,
            max_drawdown=0.0,
            current_drawdown=0.0,
            win_rate=0.0,
            profit_factor=0.0,
            volatility=0.0,
            beta=1.0,
            total_trades=0,
            winning_trades=0,
            losing_trades=0,
            avg_win=0.0,
            avg_loss=0.0,
            largest_win=0.0,
            largest_loss=0.0,
            consecutive_wins=0,
            consecutive_losses=0,
            avg_trade_duration=0.0,
            risk_adjusted_return=0.0
        )
    
    async def get_performance_summary(self, lookback_days: int = 30) -> Dict[str, Any]:
        """Get performance summary for API endpoints"""
        metrics = await self.calculate_performance_metrics(lookback_days)
        
        return {
            'total_pnl': round(metrics.total_pnl, 2),
            'total_return_percent': round(metrics.total_return, 2),
            'sharpe_ratio': round(metrics.sharpe_ratio, 3),
            'sortino_ratio': round(metrics.sortino_ratio, 3),
            'calmar_ratio': round(metrics.calmar_ratio, 3),
            'max_drawdown_percent': round(metrics.max_drawdown, 2),
            'current_drawdown_percent': round(metrics.current_drawdown, 2),
            'win_rate_percent': round(metrics.win_rate * 100, 1),
            'profit_factor': round(metrics.profit_factor, 2),
            'volatility_percent': round(metrics.volatility * 100, 2),
            'total_trades': metrics.total_trades,
            'winning_trades': metrics.winning_trades,
            'losing_trades': metrics.losing_trades,
            'avg_win': round(metrics.avg_win, 2),
            'avg_loss': round(metrics.avg_loss, 2),
            'largest_win': round(metrics.largest_win, 2),
            'largest_loss': round(metrics.largest_loss, 2),
            'consecutive_wins': metrics.consecutive_wins,
            'consecutive_losses': metrics.consecutive_losses,
            'avg_trade_duration_hours': round(metrics.avg_trade_duration / 3600, 1),
            'risk_adjusted_return': round(metrics.risk_adjusted_return, 3),
            'lookback_days': lookback_days,
            'last_updated': datetime.now(timezone.utc).isoformat()
        }
    
    async def get_trade_analytics(self, symbol: str = None, 
                                 timeframe: str = "all") -> Dict[str, Any]:
        """Get detailed trade analytics"""
        if not self.trade_history:
            return {'error': 'No trade history available'}
        
        # Filter trades
        filtered_trades = self.trade_history
        if symbol:
            filtered_trades = [t for t in filtered_trades if t['symbol'] == symbol]
        
        if not filtered_trades:
            return {'error': f'No trades found for symbol: {symbol}'}
        
        # Calculate analytics
        total_trades = len(filtered_trades)
        winning_trades = [t for t in filtered_trades if t['pnl'] > 0]
        losing_trades = [t for t in filtered_trades if t['pnl'] < 0]
        
        # Calculate profit factor
        total_wins = sum(t['pnl'] for t in winning_trades)
        total_losses = abs(sum(t['pnl'] for t in losing_trades))
        profit_factor = total_wins / total_losses if total_losses > 0 else float('inf')
        
        analytics = {
            'symbol': symbol or 'all',
            'total_trades': total_trades,
            'winning_trades': len(winning_trades),
            'losing_trades': len(losing_trades),
            'win_rate': len(winning_trades) / total_trades if total_trades > 0 else 0,
            'total_pnl': sum(t['pnl'] for t in filtered_trades),
            'avg_pnl': np.mean([t['pnl'] for t in filtered_trades]),
            'best_trade': max([t['pnl'] for t in filtered_trades]),
            'worst_trade': min([t['pnl'] for t in filtered_trades]),
            'avg_win': np.mean([t['pnl'] for t in winning_trades]) if winning_trades else 0,
            'avg_loss': np.mean([t['pnl'] for t in losing_trades]) if losing_trades else 0,
            'profit_factor': profit_factor
        }
        
        return analytics 