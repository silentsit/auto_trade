import os
import time
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import requests
from typing import Dict, List, Union, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('historical_data')

# Constants
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data', 'historical')
CACHE_EXPIRY_DAYS = 7  # How long to consider cached data valid

class HistoricalDataManager:
    """Manages downloading, storing, and retrieving historical price data for backtesting.
    
    Features:
    - Cache management for historical data
    - Support for multiple data sources (local CSV, API providers)
    - Data validation and cleaning
    - Resampling to different timeframes
    """
    
    def __init__(self, data_dir: Optional[str] = None):
        """Initialize the historical data manager.
        
        Args:
            data_dir: Optional directory to store historical data files. Defaults to DATA_DIR.
        """
        self.data_dir = data_dir or DATA_DIR
        self._ensure_data_dir()
        self.cache = {}  # In-memory cache
        self.api_keys = {}  # Store API keys for different providers
        
        # Load API keys if available
        self._load_api_keys()
    
    def _ensure_data_dir(self) -> None:
        """Create the data directory if it doesn't exist."""
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Create subdirectories for different timeframes
        for timeframe in ['1m', '5m', '15m', '1h', '4h', '1d']:
            os.makedirs(os.path.join(self.data_dir, timeframe), exist_ok=True)
    
    def _load_api_keys(self) -> None:
        """Load API keys from config file."""
        try:
            config_path = os.path.join(os.path.dirname(__file__), 'config', 'api_keys.json')
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    self.api_keys = json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load API keys: {e}")
    
    def get_data(self, 
                 symbol: str, 
                 timeframe: str, 
                 start_date: Union[str, datetime], 
                 end_date: Union[str, datetime],
                 provider: str = 'mock',
                 force_download: bool = False) -> pd.DataFrame:
        """Get historical price data for a symbol.
        
        Args:
            symbol: Trading symbol (e.g., 'EUR_USD')
            timeframe: Data timeframe (e.g., '1m', '5m', '1h', '1d')
            start_date: Start date for historical data
            end_date: End date for historical data
            provider: Data provider ('mock', 'alpha_vantage', 'oanda', etc.)
            force_download: Force download even if cached data exists
            
        Returns:
            DataFrame with historical OHLCV data
        """
        # Convert dates to datetime objects if they're strings
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        
        # Check if we already have this data cached
        cache_key = f"{symbol}_{timeframe}_{start_date.date()}_{end_date.date()}"
        if cache_key in self.cache and not force_download:
            logger.info(f"Using in-memory cached data for {cache_key}")
            return self.cache[cache_key]
        
        # Check if we have this data on disk
        file_path = self._get_data_path(symbol, timeframe, start_date, end_date)
        if os.path.exists(file_path) and not force_download:
            # Check if the file is recent enough
            file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
            if (datetime.now() - file_mtime).days < CACHE_EXPIRY_DAYS:
                logger.info(f"Loading cached data from {file_path}")
                df = pd.read_csv(file_path, parse_dates=['timestamp'])
                self.cache[cache_key] = df
                return df
        
        # Download the data based on the specified provider
        if provider == 'mock':
            df = self._generate_mock_data(symbol, timeframe, start_date, end_date)
        elif provider == 'alpha_vantage':
            df = self._download_alpha_vantage(symbol, timeframe, start_date, end_date)
        elif provider == 'oanda':
            df = self._download_oanda(symbol, timeframe, start_date, end_date)
        else:
            raise ValueError(f"Unsupported data provider: {provider}")
        
        # Save to disk cache
        self._save_to_cache(df, file_path)
        
        # Save to memory cache
        self.cache[cache_key] = df
        
        return df
    
    def _get_data_path(self, symbol: str, timeframe: str, 
                       start_date: datetime, end_date: datetime) -> str:
        """Generate a file path for the cached data."""
        filename = f"{symbol}_{start_date.date()}_{end_date.date()}.csv"
        return os.path.join(self.data_dir, timeframe, filename)
    
    def _save_to_cache(self, df: pd.DataFrame, file_path: str) -> None:
        """Save data to disk cache."""
        try:
            df.to_csv(file_path, index=False)
            logger.info(f"Saved data to {file_path}")
        except Exception as e:
            logger.error(f"Failed to save data to {file_path}: {e}")
    
    def _generate_mock_data(self, symbol: str, timeframe: str, 
                           start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Generate mock price data for backtesting."""
        logger.info(f"Generating mock data for {symbol} from {start_date} to {end_date}")
        
        # Determine time delta based on timeframe
        delta_map = {
            '1m': timedelta(minutes=1),
            '5m': timedelta(minutes=5),
            '15m': timedelta(minutes=15),
            '1h': timedelta(hours=1),
            '4h': timedelta(hours=4),
            '1d': timedelta(days=1)
        }
        delta = delta_map.get(timeframe, timedelta(days=1))
        
        # Generate timestamps
        timestamps = []
        current = start_date
        while current <= end_date:
            # Skip weekends for forex symbols
            if symbol.endswith('_USD') or '_' in symbol:
                if current.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
                    current += delta
                    continue
            timestamps.append(current)
            current += delta
        
        # Base price and volatility for common symbols
        base_prices = {
            'EUR_USD': 1.1,
            'GBP_USD': 1.3,
            'USD_JPY': 110.0,
            'AUD_USD': 0.75,
            'USD_CAD': 1.25,
            'NZD_USD': 0.7
        }
        
        volatilities = {
            'EUR_USD': 0.0015,
            'GBP_USD': 0.0025,
            'USD_JPY': 0.25,
            'AUD_USD': 0.0018,
            'USD_CAD': 0.0018,
            'NZD_USD': 0.0018
        }
        
        base_price = base_prices.get(symbol, 100.0)
        volatility = volatilities.get(symbol, 0.02 * base_price)
        
        # Generate random walk with drift
        np.random.seed(hash(symbol) % 100000)  # Set seed based on symbol for consistency
        
        drift = 0.00001 * base_price  # Slight upward drift
        random_walk = np.random.normal(drift, volatility, size=len(timestamps))
        
        # Calculate prices
        price_changes = np.cumsum(random_walk)
        closes = base_price + price_changes
        
        # Ensure no negative prices
        if np.any(closes <= 0):
            closes = closes - np.min(closes) + base_price * 0.1
        
        # Create additional price data based on close price
        highs = closes * (1 + np.random.uniform(0, 0.003, size=len(timestamps)))
        lows = closes * (1 - np.random.uniform(0, 0.003, size=len(timestamps)))
        opens = lows + np.random.uniform(0, 1, size=len(timestamps)) * (highs - lows)
        
        # Generate volume data - higher for market hours
        volumes = []
        for ts in timestamps:
            hour = ts.hour
            if 8 <= hour <= 16:  # Market hours
                vol_mult = 1.5
            elif 0 <= hour <= 5:  # Quiet hours
                vol_mult = 0.5
            else:  # Regular hours
                vol_mult = 1.0
                
            # Add some daily patterns - Monday tends to be higher volume
            if ts.weekday() == 0:  # Monday
                vol_mult *= 1.2
            
            base_vol = np.random.normal(1000, 300)
            volumes.append(max(10, base_vol * vol_mult))
        
        # Create DataFrame
        df = pd.DataFrame({
            'timestamp': timestamps,
            'open': opens,
            'high': highs,
            'low': lows,
            'close': closes,
            'volume': volumes
        })
        
        logger.info(f"Generated {len(df)} mock data points for {symbol}")
        return df
    
    def _download_alpha_vantage(self, symbol: str, timeframe: str, 
                               start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Download historical data from Alpha Vantage API."""
        logger.info(f"Downloading Alpha Vantage data for {symbol}")
        
        if 'alpha_vantage' not in self.api_keys:
            raise ValueError("Alpha Vantage API key not found in configuration")
        
        api_key = self.api_keys['alpha_vantage']
        
        # Convert forex symbols to Alpha Vantage format
        if '_' in symbol:
            from_currency, to_currency = symbol.split('_')
            av_symbol = f"{from_currency}{to_currency}"
        else:
            av_symbol = symbol
        
        # Map timeframe to Alpha Vantage interval
        interval_map = {
            '1m': '1min',
            '5m': '5min',
            '15m': '15min',
            '30m': '30min',
            '1h': '60min',
            '1d': 'daily'
        }
        
        interval = interval_map.get(timeframe, 'daily')
        
        # For forex data
        if '_' in symbol:
            url = f"https://www.alphavantage.co/query?function=FX_{interval.upper()}&from_symbol={from_currency}&to_symbol={to_currency}&apikey={api_key}&outputsize=full"
        else:
            # For stock data
            url = f"https://www.alphavantage.co/query?function=TIME_SERIES_{interval.upper()}&symbol={av_symbol}&apikey={api_key}&outputsize=full"
        
        try:
            response = requests.get(url)
            data = response.json()
            
            # Parse the response
            if interval == 'daily':
                time_series = data.get('Time Series (Daily)', {})
                df = pd.DataFrame.from_dict(time_series, orient='index')
                df.index = pd.to_datetime(df.index)
                df.columns = ['open', 'high', 'low', 'close', 'volume']
            else:
                # Handle intraday data
                time_key = f"Time Series FX ({interval})" if '_' in symbol else f"Time Series ({interval})"
                time_series = data.get(time_key, {})
                df = pd.DataFrame.from_dict(time_series, orient='index')
                df.index = pd.to_datetime(df.index)
                df.columns = ['open', 'high', 'low', 'close', 'volume']
            
            # Reset index to make timestamp a column
            df = df.reset_index().rename(columns={'index': 'timestamp'})
            
            # Filter by date range
            df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
            
            # Convert columns to numeric
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col])
                
            return df
            
        except Exception as e:
            logger.error(f"Failed to download data from Alpha Vantage: {e}")
            # Return mock data as fallback
            return self._generate_mock_data(symbol, timeframe, start_date, end_date)
    
    def _download_oanda(self, symbol: str, timeframe: str, 
                       start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Download historical data from Oanda API."""
        logger.info(f"Downloading Oanda data for {symbol}")
        
        if 'oanda' not in self.api_keys:
            raise ValueError("Oanda API key not found in configuration")
        
        # Just use mock data for now as implementation placeholder
        logger.warning("Oanda implementation is a placeholder - using mock data")
        return self._generate_mock_data(symbol, timeframe, start_date, end_date)
    
    def resample_data(self, df: pd.DataFrame, target_timeframe: str) -> pd.DataFrame:
        """Resample OHLCV data to a different timeframe.
        
        Args:
            df: DataFrame with 'timestamp', 'open', 'high', 'low', 'close', 'volume' columns
            target_timeframe: Target timeframe (e.g., '1h', '4h', '1d')
            
        Returns:
            Resampled DataFrame
        """
        # Map timeframe to pandas frequency
        freq_map = {
            '1m': '1min',
            '5m': '5min',
            '15m': '15min',
            '30m': '30min',
            '1h': '1H',
            '4h': '4H',
            '1d': '1D',
            '1w': '1W'
        }
        
        if target_timeframe not in freq_map:
            raise ValueError(f"Unsupported target timeframe: {target_timeframe}")
        
        freq = freq_map[target_timeframe]
        
        # Set timestamp as index for resampling
        df_resampled = df.set_index('timestamp').copy()
        
        # Resample the data
        resampled = df_resampled.resample(freq).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        })
        
        # Reset index to make timestamp a column again
        resampled = resampled.reset_index()
        
        # Drop NA values that might be created during resampling
        resampled = resampled.dropna()
        
        return resampled
    
    def list_available_symbols(self) -> List[str]:
        """List all symbols that have cached data available."""
        symbols = set()
        
        # Scan all timeframe directories
        for timeframe in os.listdir(self.data_dir):
            tf_dir = os.path.join(self.data_dir, timeframe)
            if not os.path.isdir(tf_dir):
                continue
                
            # Check all files in the timeframe directory
            for filename in os.listdir(tf_dir):
                if filename.endswith('.csv'):
                    # Extract symbol from filename (symbol_startdate_enddate.csv)
                    parts = filename.split('_')
                    if len(parts) >= 3:
                        symbol = '_'.join(parts[:-2])  # Account for symbols with underscores
                        symbols.add(symbol)
        
        return sorted(list(symbols))

    def get_data_date_range(self, symbol: str, timeframe: str) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Get the earliest and latest dates for which we have data for a symbol and timeframe."""
        earliest_date = None
        latest_date = None
        
        tf_dir = os.path.join(self.data_dir, timeframe)
        if not os.path.isdir(tf_dir):
            return None, None
            
        for filename in os.listdir(tf_dir):
            if filename.startswith(f"{symbol}_") and filename.endswith('.csv'):
                parts = filename.split('_')
                if len(parts) >= 3:
                    try:
                        # Extract dates from filename (symbol_startdate_enddate.csv)
                        start_str = parts[-2].split('.')[0]  # Remove potential file extension
                        end_str = parts[-1].split('.')[0]    # Remove file extension
                        
                        start_date = datetime.strptime(start_str, '%Y-%m-%d')
                        end_date = datetime.strptime(end_str, '%Y-%m-%d')
                        
                        if earliest_date is None or start_date < earliest_date:
                            earliest_date = start_date
                            
                        if latest_date is None or end_date > latest_date:
                            latest_date = end_date
                    except Exception as e:
                        logger.warning(f"Failed to parse dates from filename {filename}: {e}")
        
        return earliest_date, latest_date

# Instantiate a global historical data manager
historical_data_manager = HistoricalDataManager()

def get_historical_data(symbol: str, 
                       timeframe: str, 
                       start_date: Union[str, datetime], 
                       end_date: Union[str, datetime],
                       provider: str = 'mock',
                       force_download: bool = False) -> pd.DataFrame:
    """Convenience function to get historical data using the global manager."""
    return historical_data_manager.get_data(
        symbol=symbol,
        timeframe=timeframe,
        start_date=start_date,
        end_date=end_date,
        provider=provider,
        force_download=force_download
    )

def list_available_symbols() -> List[str]:
    """List all symbols that have cached data."""
    return historical_data_manager.list_available_symbols()

def get_data_date_range(symbol: str, timeframe: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Get the date range for cached data."""
    return historical_data_manager.get_data_date_range(symbol, timeframe)

if __name__ == "__main__":
    # Example usage
    start = datetime.now() - timedelta(days=30)
    end = datetime.now()
    
    data = get_historical_data('EUR_USD', '1h', start, end)
    print(f"Downloaded {len(data)} rows of data")
    print(data.head())
    
    # Resample to daily
    daily = historical_data_manager.resample_data(data, '1d')
    print(f"Resampled to {len(daily)} daily candles")
    print(daily.head()) 