"""
Backtest Engine for FX Trading Bridge

This module provides a complete backtesting framework for evaluating trading 
strategies using historical price data. It includes classes for configuring 
backtest parameters, simulating exchange operations, and analyzing results.
"""

import os
import csv
import json
import logging
import datetime
import requests
import pandas as pd
import numpy as np
from typing import Dict, List, Callable, Optional, Tuple, Union, Any
from dataclasses import dataclass, field, asdict
from enum import Enum
import matplotlib.pyplot as plt
from pathlib import Path
import uuid
import time

# Import the historical data manager
from historical_data import get_historical_data, list_available_symbols

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TradeDirection(Enum):
    LONG = "LONG"
    SHORT = "SHORT"

@dataclass
class BacktestConfig:
    """Configuration for a backtest run."""
    start_date: Union[str, datetime]
    end_date: Union[str, datetime]
    symbols: List[str]
    timeframe: str = "1h"
    initial_capital: float = 100000.0
    position_size_pct: float = 2.0  # Position size as percentage of capital
    data_provider: str = "mock"     # 'mock', 'alpha_vantage', 'oanda', etc.
    commission_pct: float = 0.1     # Commission as percentage (10 basis points)
    slippage_pct: float = 0.05      # Slippage as percentage (5 basis points)
    
    def __post_init__(self):
        """Convert string dates to datetime objects."""
        if isinstance(self.start_date, str):
            self.start_date = datetime.fromisoformat(self.start_date.replace('Z', '+00:00'))
        if isinstance(self.end_date, str):
            self.end_date = datetime.fromisoformat(self.end_date.replace('Z', '+00:00'))

@dataclass
class BacktestTrade:
    """Represents a trade in the backtesting system."""
    id: str
    symbol: str
    direction: TradeDirection
    entry_price: float
    entry_time: datetime
    size: float
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    profit_loss: Optional[float] = None
    profit_loss_pct: Optional[float] = None
    status: str = "OPEN"
    tags: List[str] = field(default_factory=list)
    
    def close(self, exit_price: float, exit_time: datetime) -> None:
        """Close the trade and calculate profit/loss."""
        self.exit_price = exit_price
        self.exit_time = exit_time
        self.status = "CLOSED"
        
        # Calculate profit/loss
        if self.direction == TradeDirection.LONG:
            self.profit_loss = (exit_price - self.entry_price) * self.size
            self.profit_loss_pct = (exit_price / self.entry_price - 1) * 100
        else:  # SHORT
            self.profit_loss = (self.entry_price - exit_price) * self.size
            self.profit_loss_pct = (self.entry_price / exit_price - 1) * 100
            
    def to_dict(self) -> Dict[str, Any]:
        """Convert trade to dictionary for serialization."""
        return {
            "id": self.id,
            "symbol": self.symbol,
            "direction": self.direction.value,
            "entry_price": self.entry_price,
            "entry_time": self.entry_time.isoformat(),
            "size": self.size,
            "exit_price": self.exit_price,
            "exit_time": self.exit_time.isoformat() if self.exit_time else None,
            "profit_loss": self.profit_loss,
            "profit_loss_pct": self.profit_loss_pct,
            "status": self.status,
            "tags": self.tags
        }

class BacktestResults:
    """Stores and analyzes backtest results."""
    
    def __init__(self, 
                 config: BacktestConfig, 
                 trades: List[BacktestTrade], 
                 equity_curve: pd.DataFrame,
                 strategy_name: str):
        """Initialize backtest results with trades and equity curve."""
        self.config = config
        self.trades = trades
        self.equity_curve = equity_curve
        self.strategy_name = strategy_name
        self.metrics = self._calculate_metrics()
        
    def _calculate_metrics(self) -> Dict[str, Any]:
        """Calculate performance metrics from trades."""
        if not self.trades:
            return {
                "total_trades": 0,
                "win_rate": 0,
                "profit_factor": 0,
                "total_return": 0,
                "max_drawdown": 0,
                "sharpe_ratio": 0,
                "avg_trade_duration": 0
            }
            
        # Filter for closed trades
        closed_trades = [t for t in self.trades if t.status == "CLOSED"]
        if not closed_trades:
            return {
                "total_trades": 0,
                "win_rate": 0,
                "profit_factor": 0,
                "total_return": 0,
                "max_drawdown": 0,
                "sharpe_ratio": 0,
                "avg_trade_duration": 0
            }
            
        # Calculate basic metrics
        total_trades = len(closed_trades)
        winning_trades = [t for t in closed_trades if t.profit_loss and t.profit_loss > 0]
        losing_trades = [t for t in closed_trades if t.profit_loss and t.profit_loss <= 0]
        
        win_rate = len(winning_trades) / total_trades if total_trades > 0 else 0
        
        # Calculate profit factor (sum of profits / sum of losses)
        total_profit = sum(t.profit_loss for t in winning_trades) if winning_trades else 0
        total_loss = abs(sum(t.profit_loss for t in losing_trades)) if losing_trades else 0
        profit_factor = total_profit / total_loss if total_loss > 0 else float('inf')
        
        # Calculate total return from equity curve
        if not self.equity_curve.empty:
            initial_equity = self.equity_curve['equity'].iloc[0]
            final_equity = self.equity_curve['equity'].iloc[-1]
            total_return_pct = (final_equity / initial_equity - 1) * 100
            
            # Calculate maximum drawdown
            equity = self.equity_curve['equity'].values
            peak = np.maximum.accumulate(equity)
            drawdown = (peak - equity) / peak * 100
            max_drawdown = drawdown.max()
            
            # Calculate Sharpe ratio (annualized)
            daily_returns = self.equity_curve['equity'].pct_change().dropna()
            sharpe_ratio = np.sqrt(252) * daily_returns.mean() / daily_returns.std() if len(daily_returns) > 1 else 0
            
            # Calculate average trade duration
            durations = [(t.exit_time - t.entry_time).total_seconds() / 3600 for t in closed_trades if t.exit_time]
            avg_duration = sum(durations) / len(durations) if durations else 0
            
            return {
                "total_trades": total_trades,
                "winning_trades": len(winning_trades),
                "losing_trades": len(losing_trades),
                "win_rate": win_rate,
                "profit_factor": profit_factor,
                "total_return": total_return_pct,
                "max_drawdown": max_drawdown,
                "sharpe_ratio": sharpe_ratio,
                "avg_trade_duration": avg_duration,
                "total_profit": total_profit,
                "total_loss": total_loss,
                "initial_capital": initial_equity,
                "final_capital": final_equity
            }
        else:
            return {
                "total_trades": total_trades,
                "win_rate": win_rate,
                "profit_factor": profit_factor,
                "total_return": 0,
                "max_drawdown": 0,
                "sharpe_ratio": 0,
                "avg_trade_duration": 0
            }
    
    def save_results(self, filename: Optional[str] = None) -> str:
        """Save backtest results to a JSON file."""
        if filename is None:
            # Generate a filename based on strategy and date
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"backtest_{self.strategy_name}_{timestamp}.json"
        
        # Create reports directory if it doesn't exist
        reports_dir = os.path.join(os.path.dirname(__file__), 'reports')
        os.makedirs(reports_dir, exist_ok=True)
        
        filepath = os.path.join(reports_dir, filename)
        
        # Prepare data for serialization
        result_data = {
            "strategy_name": self.strategy_name,
            "run_date": datetime.now().isoformat(),
            "config": {
                "start_date": self.config.start_date.isoformat(),
                "end_date": self.config.end_date.isoformat(),
                "symbols": self.config.symbols,
                "timeframe": self.config.timeframe,
                "initial_capital": self.config.initial_capital,
                "position_size_pct": self.config.position_size_pct,
                "data_provider": self.config.data_provider,
            },
            "metrics": self.metrics,
            "trades": [t.to_dict() for t in self.trades if t.status == "CLOSED"],
            "equity_curve": self.equity_curve.to_dict(orient='records')
        }
        
        # Save to file
        with open(filepath, 'w') as f:
            json.dump(result_data, f, indent=2)
            
        logger.info(f"Saved backtest results to {filepath}")
        return filepath
    
    def plot_equity_curve(self, save_path: Optional[str] = None) -> None:
        """Plot equity curve and drawdown."""
        if self.equity_curve.empty:
            logger.warning("No equity curve data to plot")
            return
            
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True, gridspec_kw={'height_ratios': [3, 1]})
        
        # Plot equity curve
        equity = self.equity_curve['equity'].values
        dates = self.equity_curve['timestamp'].values
        
        ax1.plot(dates, equity, label='Equity Curve')
        ax1.set_title(f'Backtest Results - {self.strategy_name}')
        ax1.set_ylabel('Equity')
        ax1.grid(True)
        ax1.legend()
        
        # Calculate and plot drawdown
        peak = np.maximum.accumulate(equity)
        drawdown = (peak - equity) / peak * 100
        
        ax2.fill_between(dates, 0, drawdown, color='red', alpha=0.3)
        ax2.set_ylabel('Drawdown %')
        ax2.set_xlabel('Date')
        ax2.grid(True)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path)
        else:
            plt.show()
            
class BacktestExchangeAdapter:
    """Simulates exchange API for backtest strategies."""
    
    def __init__(self, backtest_engine: 'BacktestEngine'):
        """Initialize with a reference to the backtest engine."""
        self.engine = backtest_engine
        
    def get_current_price(self, symbol: str) -> Dict[str, float]:
        """Get current price for a symbol."""
        return self.engine.get_current_price(symbol)
    
    def get_historical_data(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        """Get historical data for a symbol."""
        return self.engine.get_historical_data(symbol, timeframe, limit)
    
    def create_market_order(self, symbol: str, direction: str, size: float) -> BacktestTrade:
        """Create a market order in the backtest."""
        return self.engine.create_market_order(symbol, 
                                              TradeDirection(direction), 
                                              size)
    
    def close_trade(self, trade_id: str) -> BacktestTrade:
        """Close a trade in the backtest."""
        return self.engine.close_trade(trade_id)
    
    def get_open_trades(self) -> List[BacktestTrade]:
        """Get all open trades."""
        return self.engine.get_open_trades()

class BacktestEngine:
    """Engine for running trading strategy backtests."""
    
    def __init__(self, config: BacktestConfig):
        """Initialize the backtest engine with configuration."""
        self.config = config
        self.current_time = config.start_date
        self.end_time = config.end_date
        self.capital = config.initial_capital
        self.equity_history = []
        
        # Load historical data for all symbols
        self.historical_data = {}
        self.current_idx = {}
        
        for symbol in config.symbols:
            data = get_historical_data(
                symbol=symbol,
                timeframe=config.timeframe,
                start_date=config.start_date,
                end_date=config.end_date,
                provider=config.data_provider
            )
            
            if data.empty:
                raise ValueError(f"No historical data available for {symbol} in timeframe {config.timeframe}")
                
            self.historical_data[symbol] = data
            self.current_idx[symbol] = 0
        
        # Trading state
        self.trades = []
        self.next_trade_id = 1
        
        # Create exchange adapter
        self.exchange = BacktestExchangeAdapter(self)
        
    def run(self, strategy_func: Callable, strategy_name: str) -> BacktestResults:
        """Run a backtest with the provided strategy function."""
        logger.info(f"Starting backtest for {strategy_name} from {self.current_time} to {self.end_time}")
        
        # Initialize strategy
        strategy = strategy_func(self.exchange)
        
        # Align all data to the latest start
        self._align_start_dates()
        
        # Main backtest loop
        while self.current_time <= self.end_time:
            # Update current time from the first symbol's data
            symbol = self.config.symbols[0]
            if self.current_idx[symbol] >= len(self.historical_data[symbol]):
                break
                
            self.current_time = self.historical_data[symbol].iloc[self.current_idx[symbol]]['timestamp']
            
            # Execute strategy step
            try:
                strategy.on_data(self.current_time)
            except Exception as e:
                logger.error(f"Error in strategy execution: {e}")
            
            # Record equity
            self._record_equity()
            
            # Move to next candle for all symbols
            for symbol in self.config.symbols:
                if self.current_idx[symbol] < len(self.historical_data[symbol]) - 1:
                    self.current_idx[symbol] += 1
                    
        # Close any remaining open trades
        for trade in self.get_open_trades():
            self.close_trade(trade.id)
            
        # Create equity curve DataFrame
        equity_df = pd.DataFrame(self.equity_history)
        
        # Create and return results
        results = BacktestResults(
            config=self.config,
            trades=self.trades,
            equity_curve=equity_df,
            strategy_name=strategy_name
        )
        
        logger.info(f"Finished backtest for {strategy_name}")
        return results
    
    def _align_start_dates(self) -> None:
        """Align all data series to start at the same time."""
        # Find the latest start timestamp across all symbols
        latest_start = None
        
        for symbol in self.config.symbols:
            if len(self.historical_data[symbol]) == 0:
                continue
                
            first_timestamp = self.historical_data[symbol].iloc[0]['timestamp']
            if latest_start is None or first_timestamp > latest_start:
                latest_start = first_timestamp
        
        if latest_start is None:
            logger.warning("No data found for any symbols")
            return
            
        # Set current indices to align with latest start
        for symbol in self.config.symbols:
            if len(self.historical_data[symbol]) == 0:
                continue
                
            idx = 0
            while (idx < len(self.historical_data[symbol]) and 
                  self.historical_data[symbol].iloc[idx]['timestamp'] < latest_start):
                idx += 1
                
            self.current_idx[symbol] = idx
    
    def _record_equity(self) -> None:
        """Record current equity value."""
        # Calculate total value of open positions
        open_value = 0
        for trade in self.get_open_trades():
            current_price = self.get_current_price(trade.symbol)
            if trade.direction == TradeDirection.LONG:
                trade_value = trade.size * current_price['close']
            else:  # SHORT
                trade_value = trade.size * (2 * trade.entry_price - current_price['close'])
            open_value += trade_value
        
        # Total equity = cash + open position value
        total_equity = self.capital + open_value
        
        # Record to history
        self.equity_history.append({
            'timestamp': self.current_time,
            'equity': total_equity
        })
    
    def get_current_price(self, symbol: str) -> Dict[str, float]:
        """Get current price data for a symbol."""
        if symbol not in self.historical_data:
            raise ValueError(f"No data for symbol {symbol}")
            
        idx = self.current_idx[symbol]
        if idx >= len(self.historical_data[symbol]):
            raise ValueError(f"No more data for symbol {symbol}")
            
        row = self.historical_data[symbol].iloc[idx]
        return {
            'open': row['open'],
            'high': row['high'],
            'low': row['low'],
            'close': row['close'],
            'volume': row['volume']
        }
    
    def get_historical_data(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        """Get historical data up to the current moment."""
        if symbol not in self.historical_data:
            raise ValueError(f"No data for symbol {symbol}")
            
        idx = self.current_idx[symbol]
        if idx >= len(self.historical_data[symbol]):
            raise ValueError(f"No more data for symbol {symbol}")
            
        # Only return data up to the current index
        start_idx = max(0, idx - limit + 1)
        return self.historical_data[symbol].iloc[start_idx:idx+1].copy()
    
    def create_market_order(self, symbol: str, direction: TradeDirection, size: float) -> BacktestTrade:
        """Create a market order in the backtest."""
        # Get current price
        current_price = self.get_current_price(symbol)
        
        # Apply slippage
        if direction == TradeDirection.LONG:
            entry_price = current_price['close'] * (1 + self.config.slippage_pct / 100)
        else:  # SHORT
            entry_price = current_price['close'] * (1 - self.config.slippage_pct / 100)
        
        # Apply commission
        commission = entry_price * size * self.config.commission_pct / 100
        self.capital -= commission
        
        # Create trade
        trade = BacktestTrade(
            id=str(self.next_trade_id),
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            entry_time=self.current_time,
            size=size,
            status="OPEN"
        )
        
        self.next_trade_id += 1
        
        # Deduct the position value from capital
        self.capital -= size * entry_price
        
        # Add to trades list
        self.trades.append(trade)
        
        return trade
    
    def close_trade(self, trade_id: str) -> BacktestTrade:
        """Close a trade in the backtest."""
        # Find the trade
        trade = None
        for t in self.trades:
            if t.id == trade_id and t.status == "OPEN":
                trade = t
                break
                
        if trade is None:
            raise ValueError(f"Trade with ID {trade_id} not found or already closed")
            
        # Get current price
        current_price = self.get_current_price(trade.symbol)
        
        # Apply slippage
        if trade.direction == TradeDirection.LONG:
            exit_price = current_price['close'] * (1 - self.config.slippage_pct / 100)
        else:  # SHORT
            exit_price = current_price['close'] * (1 + self.config.slippage_pct / 100)
        
        # Close the trade
        trade.close(exit_price, self.current_time)
        
        # Apply commission
        commission = exit_price * trade.size * self.config.commission_pct / 100
        
        # Update capital
        if trade.direction == TradeDirection.LONG:
            self.capital += (exit_price * trade.size) - commission
        else:  # SHORT
            self.capital += (2 * trade.entry_price - exit_price) * trade.size - commission
        
        return trade
    
    def get_open_trades(self) -> List[BacktestTrade]:
        """Get all open trades."""
        return [t for t in self.trades if t.status == "OPEN"]

# Example strategy function
def sma_crossover_strategy(exchange: BacktestExchangeAdapter) -> Any:
    """Simple SMA crossover strategy for demonstration."""
    class Strategy:
        def __init__(self, exchange: BacktestExchangeAdapter):
            self.exchange = exchange
            self.positions = {}  # Tracks open positions by symbol
            
        def on_data(self, current_time: datetime) -> None:
            """Process new data and make trading decisions."""
            # Process each symbol
            for symbol in ['EUR_USD', 'GBP_USD', 'USD_JPY']:
                try:
                    # Get historical data - 50 bars
                    hist_data = self.exchange.get_historical_data(symbol, '1h', 50)
                    if len(hist_data) < 50:
                        continue  # Not enough data
                        
                    # Calculate indicators
                    hist_data['sma_fast'] = hist_data['close'].rolling(window=20).mean()
                    hist_data['sma_slow'] = hist_data['close'].rolling(window=50).mean()
                    
                    # Check for crossover signals
                    if len(hist_data) < 2:
                        continue
                        
                    current = hist_data.iloc[-1]
                    previous = hist_data.iloc[-2]
                    
                    # Check if we have necessary indicator values
                    if np.isnan(current['sma_fast']) or np.isnan(current['sma_slow']):
                        continue
                        
                    # Buy signal: fast SMA crosses above slow SMA
                    buy_signal = (previous['sma_fast'] <= previous['sma_slow'] and 
                                 current['sma_fast'] > current['sma_slow'])
                    
                    # Sell signal: fast SMA crosses below slow SMA
                    sell_signal = (previous['sma_fast'] >= previous['sma_slow'] and 
                                  current['sma_fast'] < current['sma_slow'])
                    
                    # Check if we already have a position for this symbol
                    has_position = symbol in self.positions
                    
                    # Execute trading logic
                    if buy_signal and not has_position:
                        # Calculate position size (fixed % of capital)
                        price = self.exchange.get_current_price(symbol)['close']
                        size = 100  # Standard lot size for simplicity
                        
                        # Open long position
                        trade = self.exchange.create_market_order(symbol, "LONG", size)
                        self.positions[symbol] = trade.id
                        
                    elif sell_signal and has_position:
                        # Close existing position
                        trade_id = self.positions[symbol]
                        self.exchange.close_trade(trade_id)
                        del self.positions[symbol]
                
                except Exception as e:
                    logger.error(f"Error processing {symbol}: {e}")
    
    return Strategy(exchange)

def bollinger_bands_strategy(exchange: BacktestExchangeAdapter) -> Any:
    """Bollinger Bands strategy for demonstration."""
    class Strategy:
        def __init__(self, exchange: BacktestExchangeAdapter):
            self.exchange = exchange
            self.positions = {}  # Tracks open positions by symbol
            
        def on_data(self, current_time: datetime) -> None:
            """Process new data and make trading decisions."""
            # Process each symbol
            for symbol in ['EUR_USD', 'GBP_USD', 'USD_JPY']:
                try:
                    # Get historical data - 30 bars for calculating Bollinger Bands
                    hist_data = self.exchange.get_historical_data(symbol, '1h', 30)
                    if len(hist_data) < 30:
                        continue  # Not enough data
                        
                    # Calculate Bollinger Bands
                    window = 20
                    std_dev = 2
                    
                    hist_data['sma'] = hist_data['close'].rolling(window=window).mean()
                    hist_data['std'] = hist_data['close'].rolling(window=window).std()
                    hist_data['upper_band'] = hist_data['sma'] + (hist_data['std'] * std_dev)
                    hist_data['lower_band'] = hist_data['sma'] - (hist_data['std'] * std_dev)
                    
                    # Check for signals
                    if len(hist_data) < 2:
                        continue
                        
                    current = hist_data.iloc[-1]
                    previous = hist_data.iloc[-2]
                    
                    # Check if we have necessary indicator values
                    if (np.isnan(current['upper_band']) or np.isnan(current['lower_band']) or 
                        np.isnan(current['sma'])):
                        continue
                        
                    current_price = current['close']
                    
                    # Buy signal: price is below lower band (oversold condition)
                    buy_signal = current_price < current['lower_band']
                    
                    # Sell signal: price is above upper band (overbought condition)
                    sell_signal = current_price > current['upper_band']
                    
                    # Exit signals
                    exit_long = current_price > current['sma']  # Exit long when price crosses above the mean
                    exit_short = current_price < current['sma']  # Exit short when price crosses below the mean
                    
                    # Check if we already have a position for this symbol
                    has_position = symbol in self.positions
                    position_type = None
                    
                    if has_position:
                        trade_id = self.positions[symbol]['id']
                        position_type = self.positions[symbol]['type']
                    
                    # Execute trading logic
                    if buy_signal and not has_position:
                        # Calculate position size (fixed)
                        size = 100  # Standard lot size
                        
                        # Open long position
                        trade = self.exchange.create_market_order(symbol, "LONG", size)
                        self.positions[symbol] = {'id': trade.id, 'type': 'LONG'}
                        
                    elif sell_signal and not has_position:
                        # Calculate position size (fixed)
                        size = 100  # Standard lot size
                        
                        # Open short position
                        trade = self.exchange.create_market_order(symbol, "SHORT", size)
                        self.positions[symbol] = {'id': trade.id, 'type': 'SHORT'}
                        
                    # Exit conditions
                    elif has_position:
                        if (position_type == 'LONG' and exit_long) or (position_type == 'SHORT' and exit_short):
                            # Close the position
                            trade_id = self.positions[symbol]['id']
                            self.exchange.close_trade(trade_id)
                            del self.positions[symbol]
                
                except Exception as e:
                    logger.error(f"Error processing {symbol}: {e}")
    
    return Strategy(exchange)

# Example usage
if __name__ == "__main__":
    # Create backtest configuration
    config = BacktestConfig(
        start_date=datetime.now() - timedelta(days=30),
        end_date=datetime.now(),
        symbols=["EUR_USD", "GBP_USD", "USD_JPY"],
        timeframe="1h",
        initial_capital=100000.0,
        position_size_pct=2.0,
        data_provider="mock"
    )
    
    # Create backtest engine
    engine = BacktestEngine(config)
    
    # Run backtest with SMA crossover strategy
    results = engine.run(sma_crossover_strategy, "SMA Crossover")
    
    # Print results
    print(f"Total Return: {results.metrics['total_return']:.2f}%")
    print(f"Win Rate: {results.metrics['win_rate']:.2f}")
    print(f"Profit Factor: {results.metrics['profit_factor']:.2f}")
    print(f"Total Trades: {results.metrics['total_trades']}")
    
    # Save results
    results.save_results()
    
    # Plot equity curve
    results.plot_equity_curve()
