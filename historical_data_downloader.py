"""
Historical Data Downloader for FX Trading Bridge

This module provides functionality to download and process historical price data
from various sources for use in backtesting trading strategies.
"""
import os
import logging
import asyncio
import pandas as pd
import aiohttp
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("HistoricalDataDownloader")

class HistoricalDataSource:
    """Enum-like class defining available data sources"""
    ALPHA_VANTAGE = "alpha_vantage"
    OANDA = "oanda"
    MOCK = "mock"
    CSV = "csv"

class HistoricalDataDownloader:
    """
    Downloads and manages historical price data for backtesting.
    
    This class handles fetching data from various sources, processing it into
    a standardized format, and saving/loading from disk cache.
    """
    
    def __init__(self, data_dir: str = "historical_data"):
        """
        Initialize the downloader with the directory for storing data.
        
        Args:
            data_dir: Directory path for storing downloaded data
        """
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.api_keys = {}
        self.rate_limiters = {
            HistoricalDataSource.ALPHA_VANTAGE: asyncio.Semaphore(5),  # 5 requests per minute
            HistoricalDataSource.OANDA: asyncio.Semaphore(60),  # 60 requests per minute
        }
        logger.info(f"Historical data downloader initialized with data dir: {data_dir}")
        
    def set_api_key(self, source: str, api_key: str) -> None:
        """Set API key for a specific data source"""
        self.api_keys[source] = api_key
        logger.info(f"API key set for source: {source}")
        
    async def get_historical_data(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        timeframe: str = "1h",
        source: str = HistoricalDataSource.MOCK,
        force_download: bool = False
    ) -> pd.DataFrame:
        """
        Get historical price data for the specified symbol and date range.
        
        Args:
            symbol: Trading pair symbol (e.g., "EUR_USD")
            start_date: Start date for historical data
            end_date: End date for historical data
            timeframe: Data timeframe (e.g., "1m", "5m", "1h", "1d")
            source: Data source to use
            force_download: Whether to force download even if cached data exists
            
        Returns:
            DataFrame with historical price data (columns: timestamp, open, high, low, close, volume)
        """
        # Check if we have cached data first
        cached_file = self._get_cache_filename(symbol, start_date, end_date, timeframe)
        
        if os.path.exists(cached_file) and not force_download:
            logger.info(f"Loading cached data for {symbol} from {cached_file}")
            return pd.read_csv(cached_file, parse_dates=["timestamp"])
        
        # Download based on source
        if source == HistoricalDataSource.MOCK:
            data = await self._generate_mock_data(symbol, start_date, end_date, timeframe)
        elif source == HistoricalDataSource.ALPHA_VANTAGE:
            data = await self._download_alphavantage(symbol, start_date, end_date, timeframe)
        elif source == HistoricalDataSource.OANDA:
            data = await self._download_oanda(symbol, start_date, end_date, timeframe)
        elif source == HistoricalDataSource.CSV:
            data = await self._load_from_csv(symbol, start_date, end_date, timeframe)
        else:
            raise ValueError(f"Unsupported data source: {source}")
        
        # Save to cache
        self._save_to_cache(data, cached_file)
        
        return data
    
    def _get_cache_filename(
        self, symbol: str, start_date: datetime, end_date: datetime, timeframe: str
    ) -> str:
        """Generate a filename for cached data based on parameters"""
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")
        return os.path.join(
            self.data_dir, f"{symbol}_{timeframe}_{start_str}_{end_str}.csv"
        )
    
    def _save_to_cache(self, data: pd.DataFrame, filename: str) -> None:
        """Save data to cache file"""
        data.to_csv(filename, index=False)
        logger.info(f"Saved data to cache: {filename}")
    
    async def _generate_mock_data(
        self, symbol: str, start_date: datetime, end_date: datetime, timeframe: str
    ) -> pd.DataFrame:
        """
        Generate mock price data for testing and development.
        
        Creates realistic-looking price data with trends, volatility, and gaps.
        """
        logger.info(f"Generating mock data for {symbol} from {start_date} to {end_date}")
        
        # Calculate number of periods based on timeframe
        timeframe_minutes = self._timeframe_to_minutes(timeframe)
        total_minutes = int((end_date - start_date).total_seconds() / 60)
        periods = total_minutes // timeframe_minutes
        
        # Generate timestamp series
        timestamps = [start_date + timedelta(minutes=i * timeframe_minutes) 
                     for i in range(periods)]
        
        # Generate price data with random walk + trend + volatility
        close_prices = self._generate_price_series(symbol, periods)
        
        # Generate OHLCV data
        data = []
        for i in range(periods):
            # Add some randomness to OHLC relationship
            volatility = close_prices[i] * np.random.uniform(0.001, 0.003)
            high_low_range = volatility * 2
            
            open_price = close_prices[i-1] if i > 0 else close_prices[i] * (1 + np.random.uniform(-0.001, 0.001))
            high_price = max(open_price, close_prices[i]) + np.random.uniform(0, high_low_range)
            low_price = min(open_price, close_prices[i]) - np.random.uniform(0, high_low_range)
            volume = np.random.randint(100, 1000)
            
            data.append({
                "timestamp": timestamps[i],
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_prices[i],
                "volume": volume
            })
        
        df = pd.DataFrame(data)
        return df
    
    def _generate_price_series(self, symbol: str, periods: int) -> List[float]:
        """Generate a synthetic price series with realistic properties"""
        # Set base price and volatility based on symbol
        base_price = 1.0
        if symbol == "EUR_USD":
            base_price = 1.1 + np.random.uniform(-0.05, 0.05)
            daily_volatility = 0.006
        elif symbol == "GBP_USD":
            base_price = 1.3 + np.random.uniform(-0.07, 0.07)
            daily_volatility = 0.008
        elif symbol == "USD_JPY":
            base_price = 110.0 + np.random.uniform(-5.0, 5.0)
            daily_volatility = 0.007
        else:
            daily_volatility = 0.01
            
        # Generate series with random walk + trends
        prices = [base_price]
        
        # Add market regime and trends
        trend_duration = np.random.randint(periods // 10, periods // 5)
        trend_direction = np.random.choice([-1, 1])
        trend_strength = np.random.uniform(0.0001, 0.0005)
        
        for i in range(1, periods):
            # Change trend occasionally
            if i % trend_duration == 0:
                trend_direction = np.random.choice([-1, 1])
                trend_strength = np.random.uniform(0.0001, 0.0005)
                
            # Calculate price change with random walk, trend, and volatility
            daily_return = np.random.normal(trend_direction * trend_strength, daily_volatility)
            price = prices[-1] * (1 + daily_return)
            prices.append(price)
        
        return prices
    
    def _timeframe_to_minutes(self, timeframe: str) -> int:
        """Convert timeframe string to minutes"""
        unit = timeframe[-1].lower()
        value = int(timeframe[:-1])
        
        if unit == 'm':
            return value
        elif unit == 'h':
            return value * 60
        elif unit == 'd':
            return value * 60 * 24
        else:
            raise ValueError(f"Invalid timeframe: {timeframe}")
    
    async def _download_alphavantage(
        self, symbol: str, start_date: datetime, end_date: datetime, timeframe: str
    ) -> pd.DataFrame:
        """Download historical data from Alpha Vantage API"""
        logger.info(f"Downloading Alpha Vantage data for {symbol}")
        
        # Check for API key
        if HistoricalDataSource.ALPHA_VANTAGE not in self.api_keys:
            raise ValueError("Alpha Vantage API key not set")
        
        # Format symbol for Alpha Vantage
        base_currency, quote_currency = symbol.split('_')
        av_symbol = f"{base_currency}{quote_currency}"
        
        # Map timeframe to Alpha Vantage interval
        interval_map = {
            '1m': '1min', '5m': '5min', '15m': '15min', '30m': '30min',
            '1h': '60min', '1d': 'daily'
        }
        if timeframe not in interval_map:
            raise ValueError(f"Unsupported timeframe for Alpha Vantage: {timeframe}")
        
        av_interval = interval_map[timeframe]
        
        # Set up request parameters
        params = {
            'function': 'FX_INTRADAY' if 'min' in av_interval else 'FX_DAILY',
            'from_symbol': base_currency,
            'to_symbol': quote_currency,
            'interval': av_interval if 'min' in av_interval else None,
            'outputsize': 'full',
            'apikey': self.api_keys[HistoricalDataSource.ALPHA_VANTAGE]
        }
        params = {k: v for k, v in params.items() if v is not None}
        
        # Handle rate limiting
        async with self.rate_limiters[HistoricalDataSource.ALPHA_VANTAGE]:
            async with aiohttp.ClientSession() as session:
                async with session.get('https://www.alphavantage.co/query', params=params) as response:
                    if response.status != 200:
                        logger.error(f"Alpha Vantage API error: {response.status}")
                        raise RuntimeError(f"Alpha Vantage API error: {response.status}")
                    
                    data = await response.json()
                    
                    if 'Error Message' in data:
                        logger.error(f"Alpha Vantage error: {data['Error Message']}")
                        raise RuntimeError(f"Alpha Vantage error: {data['Error Message']}")
                    
                    # Parse response
                    time_series_key = [k for k in data.keys() if 'Time Series' in k][0]
                    time_series = data[time_series_key]
                    
                    result = []
                    for date_str, values in time_series.items():
                        timestamp = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S' if 'min' in av_interval else '%Y-%m-%d')
                        
                        if start_date <= timestamp <= end_date:
                            result.append({
                                'timestamp': timestamp,
                                'open': float(values['1. open']),
                                'high': float(values['2. high']),
                                'low': float(values['3. low']),
                                'close': float(values['4. close']),
                                'volume': int(float(values.get('5. volume', 0)))
                            })
                    
                    df = pd.DataFrame(result)
                    return df.sort_values('timestamp').reset_index(drop=True)
    
    async def _download_oanda(
        self, symbol: str, start_date: datetime, end_date: datetime, timeframe: str
    ) -> pd.DataFrame:
        """Download historical data from Oanda API"""
        logger.info(f"Downloading Oanda data for {symbol}")
        
        # Check for API key
        if HistoricalDataSource.OANDA not in self.api_keys:
            raise ValueError("Oanda API key not set")
        
        # For now, return mock data since implementation requires account details
        # In a real implementation, you would use the Oanda REST API client
        logger.warning("Oanda implementation is a placeholder. Using mock data instead.")
        return await self._generate_mock_data(symbol, start_date, end_date, timeframe)
    
    async def _load_from_csv(
        self, symbol: str, start_date: datetime, end_date: datetime, timeframe: str
    ) -> pd.DataFrame:
        """Load historical data from a custom CSV file"""
        logger.info(f"Loading CSV data for {symbol}")
        
        # Look for matching CSV files in the data directory
        csv_pattern = f"{symbol.lower().replace('_', '')}_*.csv"
        matching_files = []
        
        for filename in os.listdir(self.data_dir):
            if filename.lower().startswith(symbol.lower().replace('_', '')):
                matching_files.append(os.path.join(self.data_dir, filename))
        
        if not matching_files:
            logger.warning(f"No CSV files found for {symbol}. Using mock data instead.")
            return await self._generate_mock_data(symbol, start_date, end_date, timeframe)
        
        # Use the most recent file
        csv_file = sorted(matching_files)[-1]
        logger.info(f"Using CSV file: {csv_file}")
        
        # Load and process the CSV data
        try:
            df = pd.read_csv(csv_file)
            
            # Standardize column names (assuming a common format)
            column_mapping = {
                'date': 'timestamp', 'time': 'timestamp', 'datetime': 'timestamp',
                'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'
            }
            
            df = df.rename(columns={col: std_col for col, std_col in column_mapping.items() 
                                  if col in df.columns and std_col not in df.columns})
            
            # Ensure required columns exist
            required_cols = ['timestamp', 'open', 'high', 'low', 'close']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                logger.error(f"CSV file missing required columns: {missing_cols}")
                return await self._generate_mock_data(symbol, start_date, end_date, timeframe)
            
            # Add volume if missing
            if 'volume' not in df.columns:
                df['volume'] = 0
            
            # Convert timestamp to datetime if needed
            if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                try:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                except:
                    logger.error("Failed to parse timestamp column")
                    return await self._generate_mock_data(symbol, start_date, end_date, timeframe)
            
            # Filter by date range
            mask = (df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)
            df = df.loc[mask].reset_index(drop=True)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading CSV data: {str(e)}")
            return await self._generate_mock_data(symbol, start_date, end_date, timeframe) 