"""
ML-Enhanced Performance Tracker
Advanced backtesting and performance tracking with institutional-grade metrics
Based on jdehorty's MLExtensions backtesting framework
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
import asyncio
import json

from ml_extensions import MLExtensions
from kernel_functions import KernelFunctions

logger = logging.getLogger(__name__)

class MLPerformanceTracker:
    """
    Advanced performance tracking system with ML-enhanced backtesting.
    Provides institutional-grade metrics and early signal flip detection.
    """
    
    def __init__(self):
        self.ml_ext = MLExtensions()
        self.kernel_func = KernelFunctions()
        
        # Performance tracking data
        self.trades = []
        self.equity_curve = []
        self.drawdown_periods = []
        self.signal_quality_history = []
        
        # Backtesting state
        self.backtest_active = False
        self.backtest_start_date = None
        self.backtest_end_date = None
        
        # Performance metrics
        self.total_trades = 0
        self.total_wins = 0
        self.total_losses = 0
        self.gross_profit = 0.0
        self.gross_loss = 0.0
        self.max_drawdown = 0.0
        self.current_equity = 10000.0  # Starting equity
        self.peak_equity = 10000.0
        
        # ML-Enhanced metrics
        self.signal_flip_count = 0
        self.ml_quality_scores = []
        self.regime_accuracy = {}
        self.adaptive_metrics = {}
        
        # Risk metrics
        self.consecutive_losses = 0
        self.max_consecutive_losses = 0
        self.consecutive_wins = 0
        self.max_consecutive_wins = 0
        
        logger.info("✅ ML Performance Tracker initialized")

    async def start_backtest(self, start_date: datetime, end_date: datetime, 
                           initial_capital: float = 10000.0):
        """Start a new backtesting session"""
        self.backtest_active = True
        self.backtest_start_date = start_date
        self.backtest_end_date = end_date
        self.current_equity = initial_capital
        self.peak_equity = initial_capital
        
        # Reset all metrics
        self.trades.clear()
        self.equity_curve.clear()
        self.total_trades = 0
        self.total_wins = 0
        self.total_losses = 0
        self.gross_profit = 0.0
        self.gross_loss = 0.0
        self.max_drawdown = 0.0
        self.signal_flip_count = 0
        self.ml_quality_scores.clear()
        
        logger.info(f"📊 Backtest started: {start_date} to {end_date}, Capital: ${initial_capital:,.2f}")

    async def record_trade(self, trade_data: Dict[str, Any], ml_analysis: Optional[Dict[str, Any]] = None):
        """
        Record a trade with ML analysis.
        
        Args:
            trade_data: Trade information (entry, exit, pnl, etc.)
            ml_analysis: ML analysis data for the trade
        """
        try:
            # Calculate trade metrics
            entry_price = trade_data.get('entry_price', 0.0)
            exit_price = trade_data.get('exit_price', 0.0)
            position_size = trade_data.get('position_size', 0.0)
            direction = trade_data.get('direction', 'LONG')  # LONG or SHORT
            
            # Calculate PnL
            if direction == 'LONG':
                pnl = (exit_price - entry_price) * position_size
            else:
                pnl = (entry_price - exit_price) * position_size
            
            # Update equity
            self.current_equity += pnl
            self.peak_equity = max(self.peak_equity, self.current_equity)
            
            # Calculate drawdown
            current_drawdown = (self.peak_equity - self.current_equity) / self.peak_equity
            self.max_drawdown = max(self.max_drawdown, current_drawdown)
            
            # Record trade
            trade_record = {
                'timestamp': trade_data.get('timestamp', datetime.now(timezone.utc)),
                'symbol': trade_data.get('symbol', ''),
                'direction': direction,
                'entry_price': entry_price,
                'exit_price': exit_price,
                'position_size': position_size,
                'pnl': pnl,
                'equity_after': self.current_equity,
                'drawdown': current_drawdown,
                'trade_number': self.total_trades + 1
            }
            
            # Add ML analysis if provided
            if ml_analysis:
                trade_record['ml_analysis'] = ml_analysis
                trade_record['signal_quality'] = ml_analysis.get('signal_quality', 0.0)
                trade_record['regime_confidence'] = ml_analysis.get('regime_confidence', 0.0)
                trade_record['market_regime'] = ml_analysis.get('market_regime', 'unknown')
                
                # Track signal quality
                self.ml_quality_scores.append(ml_analysis.get('signal_quality', 0.0))
                
                # Check for early signal flip
                if ml_analysis.get('early_signal_flip', False):
                    self.signal_flip_count += 1
                    trade_record['early_signal_flip'] = True
            
            self.trades.append(trade_record)
            
            # Update statistics
            self.total_trades += 1
            
            if pnl > 0:
                self.total_wins += 1
                self.gross_profit += pnl
                self.consecutive_wins += 1
                self.consecutive_losses = 0
                self.max_consecutive_wins = max(self.max_consecutive_wins, self.consecutive_wins)
            else:
                self.total_losses += 1
                self.gross_loss += abs(pnl)
                self.consecutive_losses += 1
                self.consecutive_wins = 0
                self.max_consecutive_losses = max(self.max_consecutive_losses, self.consecutive_losses)
            
            # Record equity point
            self.equity_curve.append({
                'timestamp': trade_record['timestamp'],
                'equity': self.current_equity,
                'drawdown': current_drawdown,
                'trade_number': self.total_trades
            })
            
            logger.info(f"📈 Trade recorded: {direction} {trade_data.get('symbol', '')} P&L: ${pnl:,.2f}")
            
        except Exception as e:
            logger.error(f"Error recording trade: {e}")

    def calculate_performance_metrics(self) -> Dict[str, Any]:
        """Calculate comprehensive performance metrics"""
        try:
            if self.total_trades == 0:
                return {"error": "No trades to analyze"}
            
            # Basic metrics
            win_rate = self.total_wins / self.total_trades if self.total_trades > 0 else 0
            net_profit = self.gross_profit - self.gross_loss
            profit_factor = self.gross_profit / self.gross_loss if self.gross_loss > 0 else float('inf')
            
            # Average trade metrics
            avg_win = self.gross_profit / self.total_wins if self.total_wins > 0 else 0
            avg_loss = self.gross_loss / self.total_losses if self.total_losses > 0 else 0
            avg_trade = net_profit / self.total_trades
            
            # Risk metrics
            returns = self._calculate_returns()
            sharpe_ratio = self._calculate_sharpe_ratio(returns)
            sortino_ratio = self._calculate_sortino_ratio(returns)
            calmar_ratio = self._calculate_calmar_ratio(net_profit)
            
            # ML-Enhanced metrics
            avg_signal_quality = np.mean(self.ml_quality_scores) if self.ml_quality_scores else 0.0
            signal_quality_consistency = 1 - np.std(self.ml_quality_scores) if len(self.ml_quality_scores) > 1 else 0.0
            
            # Recovery metrics
            recovery_factor = net_profit / (self.max_drawdown * self.peak_equity) if self.max_drawdown > 0 else float('inf')
            
            # Expectancy
            expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)
            
            metrics = {
                # Basic Performance
                "total_trades": self.total_trades,
                "total_wins": self.total_wins,
                "total_losses": self.total_losses,
                "win_rate": win_rate,
                "net_profit": net_profit,
                "gross_profit": self.gross_profit,
                "gross_loss": self.gross_loss,
                "profit_factor": profit_factor,
                
                # Trade Analysis
                "avg_win": avg_win,
                "avg_loss": avg_loss,
                "avg_trade": avg_trade,
                "expectancy": expectancy,
                "largest_win": max([t['pnl'] for t in self.trades if t['pnl'] > 0], default=0),
                "largest_loss": min([t['pnl'] for t in self.trades if t['pnl'] < 0], default=0),
                
                # Risk Metrics
                "max_drawdown": self.max_drawdown,
                "current_equity": self.current_equity,
                "peak_equity": self.peak_equity,
                "sharpe_ratio": sharpe_ratio,
                "sortino_ratio": sortino_ratio,
                "calmar_ratio": calmar_ratio,
                "recovery_factor": recovery_factor,
                
                # Consistency Metrics
                "max_consecutive_wins": self.max_consecutive_wins,
                "max_consecutive_losses": self.max_consecutive_losses,
                "current_consecutive_wins": self.consecutive_wins,
                "current_consecutive_losses": self.consecutive_losses,
                
                # ML-Enhanced Metrics
                "signal_flip_count": self.signal_flip_count,
                "signal_flip_rate": self.signal_flip_count / self.total_trades if self.total_trades > 0 else 0,
                "avg_signal_quality": avg_signal_quality,
                "signal_quality_consistency": signal_quality_consistency,
                
                # Status
                "backtest_active": self.backtest_active,
                "start_date": self.backtest_start_date.isoformat() if self.backtest_start_date else None,
                "end_date": self.backtest_end_date.isoformat() if self.backtest_end_date else None,
                "last_updated": datetime.now(timezone.utc).isoformat()
            }
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error calculating performance metrics: {e}")
            return {"error": str(e)}

    def _calculate_returns(self) -> List[float]:
        """Calculate trade returns for risk metrics"""
        try:
            if len(self.equity_curve) < 2:
                return []
            
            returns = []
            for i in range(1, len(self.equity_curve)):
                prev_equity = self.equity_curve[i-1]['equity']
                curr_equity = self.equity_curve[i]['equity']
                
                if prev_equity > 0:
                    ret = (curr_equity - prev_equity) / prev_equity
                    returns.append(ret)
            
            return returns
            
        except Exception as e:
            logger.error(f"Error calculating returns: {e}")
            return []

    def _calculate_sharpe_ratio(self, returns: List[float], risk_free_rate: float = 0.02) -> float:
        """Calculate Sharpe ratio"""
        try:
            if not returns or len(returns) < 2:
                return 0.0
            
            # Annualize returns (assuming daily)
            annual_return = np.mean(returns) * 252
            annual_volatility = np.std(returns) * np.sqrt(252)
            
            if annual_volatility == 0:
                return 0.0
            
            sharpe = (annual_return - risk_free_rate) / annual_volatility
            return float(sharpe)
            
        except Exception as e:
            logger.error(f"Error calculating Sharpe ratio: {e}")
            return 0.0

    def _calculate_sortino_ratio(self, returns: List[float], risk_free_rate: float = 0.02) -> float:
        """Calculate Sortino ratio (focuses on downside deviation)"""
        try:
            if not returns or len(returns) < 2:
                return 0.0
            
            # Calculate downside returns
            downside_returns = [r for r in returns if r < 0]
            
            if not downside_returns:
                return float('inf')  # No downside
            
            annual_return = np.mean(returns) * 252
            downside_deviation = np.std(downside_returns) * np.sqrt(252)
            
            if downside_deviation == 0:
                return float('inf')
            
            sortino = (annual_return - risk_free_rate) / downside_deviation
            return float(sortino)
            
        except Exception as e:
            logger.error(f"Error calculating Sortino ratio: {e}")
            return 0.0

    def _calculate_calmar_ratio(self, net_profit: float) -> float:
        """Calculate Calmar ratio (annual return / max drawdown)"""
        try:
            if self.max_drawdown == 0:
                return float('inf')
            
            # Estimate annual return
            if self.backtest_start_date and self.backtest_end_date:
                days = (self.backtest_end_date - self.backtest_start_date).days
                annual_multiplier = 365 / days if days > 0 else 1
            else:
                annual_multiplier = 1
            
            annual_return = (net_profit / self.peak_equity) * annual_multiplier
            calmar = annual_return / self.max_drawdown
            
            return float(calmar)
            
        except Exception as e:
            logger.error(f"Error calculating Calmar ratio: {e}")
            return 0.0

    def get_regime_performance(self) -> Dict[str, Any]:
        """Analyze performance by market regime"""
        try:
            regime_stats = {}
            
            for trade in self.trades:
                regime = trade.get('market_regime', 'unknown')
                pnl = trade.get('pnl', 0)
                
                if regime not in regime_stats:
                    regime_stats[regime] = {
                        'trades': 0,
                        'wins': 0,
                        'losses': 0,
                        'total_pnl': 0.0,
                        'avg_quality': 0.0,
                        'quality_scores': []
                    }
                
                stats = regime_stats[regime]
                stats['trades'] += 1
                stats['total_pnl'] += pnl
                
                if pnl > 0:
                    stats['wins'] += 1
                else:
                    stats['losses'] += 1
                
                # Track signal quality
                quality = trade.get('signal_quality', 0.0)
                stats['quality_scores'].append(quality)
            
            # Calculate regime metrics
            for regime, stats in regime_stats.items():
                stats['win_rate'] = stats['wins'] / stats['trades'] if stats['trades'] > 0 else 0
                stats['avg_pnl'] = stats['total_pnl'] / stats['trades'] if stats['trades'] > 0 else 0
                stats['avg_quality'] = np.mean(stats['quality_scores']) if stats['quality_scores'] else 0.0
                del stats['quality_scores']  # Remove raw data
            
            return regime_stats
            
        except Exception as e:
            logger.error(f"Error analyzing regime performance: {e}")
            return {}

    def get_equity_curve_data(self) -> List[Dict[str, Any]]:
        """Get equity curve data for charting"""
        return self.equity_curve.copy()

    def get_drawdown_periods(self) -> List[Dict[str, Any]]:
        """Identify significant drawdown periods"""
        try:
            drawdown_periods = []
            current_dd_start = None
            current_dd_peak = None
            
            for point in self.equity_curve:
                equity = point['equity']
                timestamp = point['timestamp']
                
                # Check if we're in a new peak
                if current_dd_peak is None or equity > current_dd_peak:
                    # End previous drawdown if it exists
                    if current_dd_start is not None:
                        dd_magnitude = (current_dd_peak - equity) / current_dd_peak
                        if dd_magnitude > 0.05:  # Only record significant drawdowns (>5%)
                            drawdown_periods.append({
                                'start_date': current_dd_start,
                                'end_date': timestamp,
                                'peak_equity': current_dd_peak,
                                'trough_equity': equity,
                                'magnitude': dd_magnitude,
                                'duration_days': (timestamp - current_dd_start).days
                            })
                    
                    # Start tracking new potential drawdown
                    current_dd_peak = equity
                    current_dd_start = timestamp
                
            return drawdown_periods
            
        except Exception as e:
            logger.error(f"Error calculating drawdown periods: {e}")
            return []

    async def export_results(self, filename: Optional[str] = None) -> str:
        """Export performance results to JSON file"""
        try:
            if filename is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"ml_performance_results_{timestamp}.json"
            
            results = {
                "summary": self.calculate_performance_metrics(),
                "regime_performance": self.get_regime_performance(),
                "equity_curve": self.get_equity_curve_data(),
                "drawdown_periods": self.get_drawdown_periods(),
                "trade_details": self.trades,
                "export_timestamp": datetime.now(timezone.utc).isoformat()
            }
            
            with open(filename, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            logger.info(f"📊 Performance results exported to {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error exporting results: {e}")
            return ""

    def create_performance_table(self) -> Dict[str, Any]:
        """Create performance table similar to jdehorty's MLExtensions"""
        try:
            metrics = self.calculate_performance_metrics()
            
            # Create simplified table with key metrics
            table_data = {
                "STATUS": "PASS" if metrics.get("profit_factor", 0) > 1.5 and metrics.get("sharpe_ratio", 0) > 1.0 else "FAIL",
                "Total Trades": metrics.get("total_trades", 0),
                "Win Rate": f"{metrics.get('win_rate', 0) * 100:.1f}%",
                "Profit Factor": f"{metrics.get('profit_factor', 0):.2f}",
                "Sharpe Ratio": f"{metrics.get('sharpe_ratio', 0):.2f}",
                "Max Drawdown": f"{metrics.get('max_drawdown', 0) * 100:.2f}%",
                "Net P&L": f"${metrics.get('net_profit', 0):.2f}",
                "Signal Flips": metrics.get("signal_flip_count", 0),
                "Avg Quality": f"{metrics.get('avg_signal_quality', 0):.2f}"
            }
            
            return table_data
            
        except Exception as e:
            logger.error(f"Error creating performance table: {e}")
            return {"error": str(e)} 