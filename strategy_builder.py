import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Callable, Any, Union
from datetime import datetime, timedelta
import logging
from abc import ABC, abstractmethod

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('strategy_builder')

class Strategy(ABC):
    """Abstract base class for all trading strategies."""
    
    def __init__(self, exchange, params=None):
        """Initialize the strategy."""
        self.exchange = exchange
        self.params = params or {}
        self.positions = {}  # Track open positions
        self.initialized = False
        
    def initialize(self):
        """Initialize strategy-specific resources."""
        self.initialized = True
        return self
        
    @abstractmethod
    def on_data(self, current_time: datetime) -> None:
        """Process new data and make trading decisions."""
        pass
    
    def get_parameters(self) -> Dict[str, Any]:
        """Get strategy parameters."""
        return self.params
    
    def set_parameters(self, params: Dict[str, Any]) -> None:
        """Set strategy parameters."""
        self.params = params
        
    def get_position(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get position for a symbol if exists."""
        return self.positions.get(symbol)
    
    def has_position(self, symbol: str) -> bool:
        """Check if there's an open position for a symbol."""
        return symbol in self.positions
    
    def open_position(self, symbol: str, direction: str, size: float, 
                     tags: List[str] = None) -> None:
        """Open a new position."""
        if self.has_position(symbol):
            logger.warning(f"Already have position for {symbol}, not opening another")
            return
            
        trade = self.exchange.create_market_order(symbol, direction, size)
        self.positions[symbol] = {
            'id': trade.id,
            'type': direction,
            'size': size,
            'entry_time': trade.entry_time,
            'entry_price': trade.entry_price,
            'tags': tags or []
        }
        
        logger.info(f"Opened {direction} position for {symbol} at {trade.entry_price}")
        
    def close_position(self, symbol: str) -> None:
        """Close an existing position."""
        if not self.has_position(symbol):
            logger.warning(f"No position for {symbol} to close")
            return
            
        trade_id = self.positions[symbol]['id']
        trade = self.exchange.close_trade(trade_id)
        
        logger.info(f"Closed position for {symbol} at {trade.exit_price}, P/L: {trade.profit_loss}")
        
        del self.positions[symbol]
    
    def calculate_position_size(self, symbol: str, risk_pct: float = 1.0) -> float:
        """Calculate position size based on account balance and risk percentage."""
        # This is a simple implementation - can be extended
        price = self.exchange.get_current_price(symbol)['close']
        return 100  # Fixed size for simplicity, should be based on balance and risk

class IndicatorMixin:
    """Mixin class with common technical indicators."""
    
    @staticmethod
    def sma(data: pd.Series, period: int) -> pd.Series:
        """Calculate Simple Moving Average."""
        return data.rolling(window=period).mean()
    
    @staticmethod
    def ema(data: pd.Series, period: int) -> pd.Series:
        """Calculate Exponential Moving Average."""
        return data.ewm(span=period, adjust=False).mean()
    
    @staticmethod
    def bollinger_bands(data: pd.Series, period: int = 20, std_dev: float = 2.0) -> Dict[str, pd.Series]:
        """Calculate Bollinger Bands."""
        middle_band = data.rolling(window=period).mean()
        std = data.rolling(window=period).std()
        upper_band = middle_band + (std * std_dev)
        lower_band = middle_band - (std * std_dev)
        
        return {
            'middle': middle_band,
            'upper': upper_band,
            'lower': lower_band
        }
    
    @staticmethod
    def rsi(data: pd.Series, period: int = 14) -> pd.Series:
        """Calculate Relative Strength Index."""
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    @staticmethod
    def macd(data: pd.Series, fast_period: int = 12, slow_period: int = 26, 
             signal_period: int = 9) -> Dict[str, pd.Series]:
        """Calculate Moving Average Convergence Divergence."""
        fast_ema = data.ewm(span=fast_period, adjust=False).mean()
        slow_ema = data.ewm(span=slow_period, adjust=False).mean()
        macd_line = fast_ema - slow_ema
        signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
        histogram = macd_line - signal_line
        
        return {
            'macd': macd_line,
            'signal': signal_line,
            'histogram': histogram
        }
    
    @staticmethod
    def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
        """Calculate Average True Range."""
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        
        tr = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)
        atr = tr.rolling(window=period).mean()
        
        return atr

class SupportResistanceMixin:
    """Mixin for identifying support and resistance levels."""
    
    @staticmethod
    def find_swing_points(high: pd.Series, low: pd.Series, lookback: int = 5) -> Dict[str, List[float]]:
        """Find swing highs and lows."""
        swing_highs = []
        swing_lows = []
        
        for i in range(lookback, len(high) - lookback):
            # Check if this is a swing high
            if all(high[i] > high[i-j] for j in range(1, lookback+1)) and \
               all(high[i] > high[i+j] for j in range(1, lookback+1)):
                swing_highs.append(high[i])
                
            # Check if this is a swing low
            if all(low[i] < low[i-j] for j in range(1, lookback+1)) and \
               all(low[i] < low[i+j] for j in range(1, lookback+1)):
                swing_lows.append(low[i])
                
        return {
            'swing_highs': swing_highs,
            'swing_lows': swing_lows
        }
    
    @staticmethod
    def cluster_levels(levels: List[float], tolerance: float = 0.0005) -> List[float]:
        """Group nearby levels into clusters."""
        if not levels:
            return []
            
        sorted_levels = sorted(levels)
        clusters = [[sorted_levels[0]]]
        
        for level in sorted_levels[1:]:
            if level - clusters[-1][-1] <= tolerance:
                # Add to the last cluster
                clusters[-1].append(level)
            else:
                # Start a new cluster
                clusters.append([level])
                
        # Average the levels in each cluster
        return [sum(cluster) / len(cluster) for cluster in clusters]
    
    @staticmethod
    def identify_support_resistance(data: pd.DataFrame, lookback: int = 5, 
                                  tolerance: float = 0.0005) -> Dict[str, List[float]]:
        """Identify support and resistance levels."""
        swing_points = SupportResistanceMixin.find_swing_points(data['high'], data['low'], lookback)
        
        support_levels = SupportResistanceMixin.cluster_levels(swing_points['swing_lows'], tolerance)
        resistance_levels = SupportResistanceMixin.cluster_levels(swing_points['swing_highs'], tolerance)
        
        return {
            'support': support_levels,
            'resistance': resistance_levels
        }

class StrategyBuilder(Strategy, IndicatorMixin, SupportResistanceMixin):
    """Base class for building custom strategies with indicators and support/resistance."""
    
    def __init__(self, exchange, params=None):
        """Initialize the strategy builder."""
        super().__init__(exchange, params)
        self.indicators = {}  # Store calculated indicators
        
    def calculate_indicators(self, symbol: str, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate and store indicators for a symbol."""
        # This is an example - extend as needed
        indicators = {}
        
        # Add SMA indicators if configured
        if 'sma_periods' in self.params:
            for period in self.params['sma_periods']:
                indicators[f'sma_{period}'] = self.sma(data['close'], period)
                
        # Add EMA indicators if configured
        if 'ema_periods' in self.params:
            for period in self.params['ema_periods']:
                indicators[f'ema_{period}'] = self.ema(data['close'], period)
                
        # Add Bollinger Bands if configured
        if 'bollinger' in self.params:
            bb_params = self.params['bollinger']
            period = bb_params.get('period', 20)
            std_dev = bb_params.get('std_dev', 2.0)
            
            bb = self.bollinger_bands(data['close'], period, std_dev)
            indicators['bb_middle'] = bb['middle']
            indicators['bb_upper'] = bb['upper']
            indicators['bb_lower'] = bb['lower']
            
        # Add RSI if configured
        if 'rsi' in self.params:
            period = self.params['rsi'].get('period', 14)
            indicators['rsi'] = self.rsi(data['close'], period)
            
        # Add MACD if configured
        if 'macd' in self.params:
            macd_params = self.params['macd']
            fast = macd_params.get('fast_period', 12)
            slow = macd_params.get('slow_period', 26)
            signal = macd_params.get('signal_period', 9)
            
            macd = self.macd(data['close'], fast, slow, signal)
            indicators['macd'] = macd['macd']
            indicators['macd_signal'] = macd['signal']
            indicators['macd_histogram'] = macd['histogram']
            
        # Add ATR if configured
        if 'atr' in self.params:
            period = self.params['atr'].get('period', 14)
            indicators['atr'] = self.atr(data['high'], data['low'], data['close'], period)
            
        # Add support/resistance if configured
        if 'support_resistance' in self.params:
            sr_params = self.params['support_resistance']
            lookback = sr_params.get('lookback', 5)
            tolerance = sr_params.get('tolerance', 0.0005)
            
            sr_levels = self.identify_support_resistance(data, lookback, tolerance)
            indicators['support_levels'] = sr_levels['support']
            indicators['resistance_levels'] = sr_levels['resistance']
            
        # Store the indicators for this symbol
        self.indicators[symbol] = indicators
        
        return indicators
    
    def on_data(self, current_time: datetime) -> None:
        """Default implementation - override in subclasses."""
        # Process each configured symbol
        symbols = self.params.get('symbols', ['EUR_USD', 'GBP_USD', 'USD_JPY'])
        for symbol in symbols:
            try:
                # Get historical data
                lookback = self.params.get('lookback', 100)
                hist_data = self.exchange.get_historical_data(symbol, '1h', lookback)
                
                if len(hist_data) < lookback:
                    logger.warning(f"Not enough data for {symbol}, got {len(hist_data)} bars")
                    continue
                    
                # Calculate indicators
                indicators = self.calculate_indicators(symbol, hist_data)
                
                # Call signal generation method
                self.generate_signals(symbol, hist_data, indicators)
                
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
    
    def generate_signals(self, symbol: str, data: pd.DataFrame, 
                        indicators: Dict[str, Any]) -> None:
        """Generate trading signals - override in subclasses."""
        # This is a placeholder method to be overridden by subclasses
        logger.warning("Using default generate_signals method, no signals will be generated")
        pass


# Example strategy implementations

class SmaCrossoverStrategy(StrategyBuilder):
    """SMA crossover strategy implementation."""
    
    def initialize(self):
        """Initialize with default parameters if not provided."""
        defaults = {
            'symbols': ['EUR_USD', 'GBP_USD', 'USD_JPY'],
            'sma_periods': [20, 50],  # fast and slow periods
            'lookback': 100,
            'position_size': 100
        }
        
        # Apply defaults for any missing parameters
        for key, value in defaults.items():
            if key not in self.params:
                self.params[key] = value
                
        return super().initialize()
    
    def generate_signals(self, symbol: str, data: pd.DataFrame, 
                        indicators: Dict[str, Any]) -> None:
        """Generate signals based on SMA crossover."""
        if len(data) < 2:
            return
            
        # Get the SMAs
        fast_sma = indicators.get(f'sma_{self.params["sma_periods"][0]}')
        slow_sma = indicators.get(f'sma_{self.params["sma_periods"][1]}')
        
        if fast_sma is None or slow_sma is None:
            logger.error(f"SMAs not calculated for {symbol}")
            return
            
        # Check for crossover
        current_fast = fast_sma.iloc[-1]
        current_slow = slow_sma.iloc[-1]
        prev_fast = fast_sma.iloc[-2]
        prev_slow = slow_sma.iloc[-2]
        
        # Skip if we have NaN values
        if np.isnan(current_fast) or np.isnan(current_slow) or np.isnan(prev_fast) or np.isnan(prev_slow):
            return
        
        # Buy signal: fast SMA crosses above slow SMA
        buy_signal = prev_fast <= prev_slow and current_fast > current_slow
        
        # Sell signal: fast SMA crosses below slow SMA
        sell_signal = prev_fast >= prev_slow and current_fast < current_slow
        
        # Trading logic
        if buy_signal and not self.has_position(symbol):
            # Open long position
            self.open_position(
                symbol=symbol,
                direction="LONG",
                size=self.params['position_size'],
                tags=['sma_crossover', 'buy']
            )
            
        elif sell_signal and self.has_position(symbol):
            position = self.get_position(symbol)
            if position['type'] == 'LONG':
                # Close long position
                self.close_position(symbol)


class BollingerBandsStrategy(StrategyBuilder):
    """Bollinger Bands mean reversion strategy."""
    
    def initialize(self):
        """Initialize with default parameters if not provided."""
        defaults = {
            'symbols': ['EUR_USD', 'GBP_USD', 'USD_JPY'],
            'bollinger': {
                'period': 20,
                'std_dev': 2.0
            },
            'lookback': 50,
            'position_size': 100,
            'exit_on_middle_band': True
        }
        
        # Apply defaults for any missing parameters
        for key, value in defaults.items():
            if key not in self.params:
                self.params[key] = value
                
        return super().initialize()
    
    def generate_signals(self, symbol: str, data: pd.DataFrame, 
                        indicators: Dict[str, Any]) -> None:
        """Generate signals based on Bollinger Bands."""
        if len(data) < 2:
            return
            
        # Get Bollinger Bands
        bb_lower = indicators.get('bb_lower')
        bb_middle = indicators.get('bb_middle')
        bb_upper = indicators.get('bb_upper')
        
        if bb_lower is None or bb_middle is None or bb_upper is None:
            logger.error(f"Bollinger Bands not calculated for {symbol}")
            return
            
        # Current price and band values
        current_price = data['close'].iloc[-1]
        current_lower = bb_lower.iloc[-1]
        current_middle = bb_middle.iloc[-1]
        current_upper = bb_upper.iloc[-1]
        
        # Skip if we have NaN values
        if np.isnan(current_lower) or np.isnan(current_middle) or np.isnan(current_upper):
            return
        
        # Buy signal: price is below lower band (oversold)
        buy_signal = current_price < current_lower
        
        # Sell signal: price is above upper band (overbought)
        sell_signal = current_price > current_upper
        
        # Exit signals based on middle band
        exit_long = self.params['exit_on_middle_band'] and current_price > current_middle
        exit_short = self.params['exit_on_middle_band'] and current_price < current_middle
        
        # Check current position
        position = self.get_position(symbol)
        
        # Trading logic
        if buy_signal and not self.has_position(symbol):
            # Open long position
            self.open_position(
                symbol=symbol,
                direction="LONG",
                size=self.params['position_size'],
                tags=['bollinger_bands', 'oversold']
            )
            
        elif sell_signal and not self.has_position(symbol):
            # Open short position
            self.open_position(
                symbol=symbol,
                direction="SHORT",
                size=self.params['position_size'],
                tags=['bollinger_bands', 'overbought']
            )
            
        # Exit logic
        elif self.has_position(symbol):
            if position['type'] == 'LONG' and exit_long:
                self.close_position(symbol)
            elif position['type'] == 'SHORT' and exit_short:
                self.close_position(symbol)


# Factory function to create strategies
def create_strategy(strategy_type: str, exchange, params: Dict[str, Any] = None) -> Strategy:
    """Create a strategy instance by type."""
    strategies = {
        'sma_crossover': SmaCrossoverStrategy,
        'bollinger_bands': BollingerBandsStrategy,
        # Add more strategies here
    }
    
    if strategy_type not in strategies:
        raise ValueError(f"Unknown strategy type: {strategy_type}")
        
    strategy_class = strategies[strategy_type]
    return strategy_class(exchange, params).initialize()

# Strategy registry for dynamic loading
STRATEGY_REGISTRY = {
    'SMA Crossover': {
        'class': SmaCrossoverStrategy,
        'description': 'Simple moving average crossover strategy',
        'default_params': {
            'sma_periods': [20, 50],
            'position_size': 100
        }
    },
    'Bollinger Bands': {
        'class': BollingerBandsStrategy,
        'description': 'Mean reversion strategy using Bollinger Bands',
        'default_params': {
            'bollinger': {
                'period': 20,
                'std_dev': 2.0
            },
            'exit_on_middle_band': True,
            'position_size': 100
        }
    }
}

def get_available_strategies() -> Dict[str, Dict[str, Any]]:
    """Get information about all available strategies."""
    return {name: {
        'description': info['description'],
        'default_params': info['default_params']
    } for name, info in STRATEGY_REGISTRY.items()}

def get_strategy_by_name(name: str, exchange, params: Dict[str, Any] = None) -> Optional[Strategy]:
    """Get a strategy instance by name."""
    if name not in STRATEGY_REGISTRY:
        return None
        
    strategy_info = STRATEGY_REGISTRY[name]
    strategy_class = strategy_info['class']
    
    # Merge default parameters with provided parameters
    merged_params = strategy_info['default_params'].copy()
    if params:
        merged_params.update(params)
        
    return strategy_class(exchange, merged_params).initialize()

# Example usage
if __name__ == "__main__":
    # This would typically be used with a backtest engine
    print("Available strategies:")
    for name, info in get_available_strategies().items():
        print(f"- {name}: {info['description']}") 