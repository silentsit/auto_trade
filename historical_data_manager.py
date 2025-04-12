import os
import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Tuple
import logging
import time
from pathlib import Path
import sqlite3
import aiohttp
import asyncio

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('historical_data')

class HistoricalDataManager:
    """
    Manages downloading, storing, and retrieving historical market data for backtesting.
    Supports multiple data sources and handles caching to minimize repeated downloads.
    """
    
    def __init__(self, data_dir: str = "data/historical", cache_days: int = 7):
        """
        Initialize the historical data manager.
        
        Args:
            data_dir: Directory to store historical data
            cache_days: Number of days to keep data in cache before re-downloading
        """
        self.data_dir = data_dir
        self.cache_days = cache_days
        self.timeframes = {
            "1m": 60,
            "5m": 300,
            "15m": 900,
            "30m": 1800,
            "1h": 3600,
            "4h": 14400,
            "1d": 86400
        }
        
        # Ensure data directory exists
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Initialize the database
        self.db_path = os.path.join(self.data_dir, "historical_data.db")
        self._initialize_database()
        
        # API keys for various data providers
        self.api_keys = {}
        self._load_api_keys()
    
    def _initialize_database(self):
        """Initialize the SQLite database for storing historical data."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables if they don't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS data_sources (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE,
            base_url TEXT,
            requires_key BOOLEAN,
            active BOOLEAN
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ohlcv_data (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            timeframe TEXT,
            timestamp INTEGER,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            source_id INTEGER,
            UNIQUE(symbol, timeframe, timestamp, source_id),
            FOREIGN KEY(source_id) REFERENCES data_sources(id)
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS download_history (
            id INTEGER PRIMARY KEY,
            symbol TEXT,
            timeframe TEXT,
            start_time INTEGER,
            end_time INTEGER,
            source_id INTEGER,
            download_time INTEGER,
            FOREIGN KEY(source_id) REFERENCES data_sources(id)
        )
        ''')
        
        # Insert default data sources
        cursor.execute('''
        INSERT OR IGNORE INTO data_sources (name, base_url, requires_key, active)
        VALUES 
            ('alphavantage', 'https://www.alphavantage.co/query', TRUE, TRUE),
            ('oanda', 'https://api-fxpractice.oanda.com/v3', TRUE, TRUE),
            ('mock', '', FALSE, TRUE)
        ''')
        
        conn.commit()
        conn.close()
    
    def _load_api_keys(self):
        """Load API keys from configuration file."""
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "api_keys.json")
        
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    self.api_keys = json.load(f)
                    logger.info(f"Loaded API keys for: {', '.join(self.api_keys.keys())}")
            except Exception as e:
                logger.error(f"Error loading API keys: {e}")
        else:
            logger.warning(f"API keys configuration file not found at {config_path}")
            self.api_keys = {}
    
    def _get_cache_file_path(self, symbol: str, timeframe: str, source: str = "default") -> str:
        """Get the path to the cache file for a symbol and timeframe."""
        return os.path.join(self.data_dir, f"{source}_{symbol}_{timeframe}.csv")
    
    def _is_cache_valid(self, filepath: str) -> bool:
        """Check if the cache file is still valid based on cache_days."""
        if not os.path.exists(filepath):
            return False
            
        file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
        now = datetime.now()
        
        return (now - file_time).days < self.cache_days
    
    async def download_data(self, 
                           symbol: str, 
                           timeframe: str, 
                           start_date: datetime,
                           end_date: datetime,
                           source: str = "oanda") -> pd.DataFrame:
        """
        Download historical data for a symbol and timeframe.
        
        Args:
            symbol: Trading symbol (e.g., "EUR_USD")
            timeframe: Time interval (e.g., "1h", "1d")
            start_date: Start date for historical data
            end_date: End date for historical data
            source: Data source to use
            
        Returns:
            DataFrame with historical OHLCV data
        """
        if source not in self.api_keys and source != "mock":
            logger.error(f"No API key found for source: {source}")
            return self._generate_mock_data(symbol, timeframe, start_date, end_date)
        
        # Check if we already have this data in database
        data = self.get_data_from_db(symbol, timeframe, start_date, end_date, source)
        if not data.empty:
            logger.info(f"Retrieved {len(data)} {timeframe} bars for {symbol} from database")
            return data
            
        try:
            if source == "mock":
                return self._generate_mock_data(symbol, timeframe, start_date, end_date)
            elif source == "oanda":
                return await self._download_from_oanda(symbol, timeframe, start_date, end_date)
            elif source == "alphavantage":
                return await self._download_from_alphavantage(symbol, timeframe, start_date, end_date)
            else:
                logger.error(f"Unsupported data source: {source}")
                return self._generate_mock_data(symbol, timeframe, start_date, end_date)
        except Exception as e:
            logger.error(f"Error downloading data for {symbol} {timeframe}: {e}")
            logger.info("Falling back to mock data")
            return self._generate_mock_data(symbol, timeframe, start_date, end_date)
    
    async def _download_from_oanda(self, 
                                  symbol: str, 
                                  timeframe: str, 
                                  start_date: datetime,
                                  end_date: datetime) -> pd.DataFrame:
        """Download historical data from Oanda."""
        if "oanda" not in self.api_keys:
            raise ValueError("Oanda API key not found")
            
        api_key = self.api_keys["oanda"]
        account_id = self.api_keys.get("oanda_account_id", "")
        
        if not account_id:
            raise ValueError("Oanda account ID not found")
        
        # Map timeframe to Oanda granularity
        granularity_map = {
            "1m": "M1", "5m": "M5", "15m": "M15", "30m": "M30",
            "1h": "H1", "4h": "H4", "1d": "D"
        }
        
        if timeframe not in granularity_map:
            raise ValueError(f"Unsupported timeframe for Oanda: {timeframe}")
            
        granularity = granularity_map[timeframe]
        
        # Format dates for Oanda API
        start_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Oanda has limits on the number of candles per request
        # For smaller timeframes, we need to make multiple requests
        max_count = 5000  # Oanda limit
        
        # Calculate number of candles between start and end
        td = end_date - start_date
        seconds = td.total_seconds()
        candle_seconds = self.timeframes[timeframe]
        total_candles = int(seconds / candle_seconds)
        
        # If we need more than max_count, we'll need multiple requests
        all_data = []
        
        current_start = start_date
        current_end = min(end_date, current_start + timedelta(seconds=max_count * candle_seconds))
        
        async with aiohttp.ClientSession() as session:
            while current_start < end_date:
                current_start_str = current_start.strftime("%Y-%m-%dT%H:%M:%SZ")
                current_end_str = current_end.strftime("%Y-%m-%dT%H:%M:%SZ")
                
                url = f"https://api-fxpractice.oanda.com/v3/instruments/{symbol}/candles"
                params = {
                    "price": "M",  # Midpoint candles
                    "granularity": granularity,
                    "from": current_start_str,
                    "to": current_end_str
                }
                
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }
                
                try:
                    async with session.get(url, params=params, headers=headers) as response:
                        if response.status != 200:
                            error_text = await response.text()
                            logger.error(f"Error from Oanda: {response.status} - {error_text}")
                            break
                            
                        data = await response.json()
                        
                        if "candles" not in data or not data["candles"]:
                            logger.warning(f"No candles returned for {symbol} {timeframe} from {current_start_str} to {current_end_str}")
                            break
                            
                        # Process candles
                        for candle in data["candles"]:
                            if candle["complete"]:  # Only use complete candles
                                timestamp = datetime.strptime(candle["time"], "%Y-%m-%dT%H:%M:%S.%fZ")
                                mid = candle["mid"]
                                
                                all_data.append({
                                    "timestamp": timestamp,
                                    "open": float(mid["o"]),
                                    "high": float(mid["h"]),
                                    "low": float(mid["l"]),
                                    "close": float(mid["c"]),
                                    "volume": float(candle["volume"])
                                })
                        
                        # Update for next request
                        current_start = current_end
                        current_end = min(end_date, current_start + timedelta(seconds=max_count * candle_seconds))
                        
                        # Rate limiting
                        await asyncio.sleep(0.5)
                        
                except Exception as e:
                    logger.error(f"Error in Oanda request: {e}")
                    break
        
        if not all_data:
            logger.warning(f"No data retrieved from Oanda for {symbol} {timeframe}")
            return pd.DataFrame()
            
        # Convert to DataFrame
        df = pd.DataFrame(all_data)
        df.set_index("timestamp", inplace=True)
        df.sort_index(inplace=True)
        
        # Store in database
        self._store_in_database(df, symbol, timeframe, "oanda")
        
        return df
    
    async def _download_from_alphavantage(self, 
                                        symbol: str, 
                                        timeframe: str, 
                                        start_date: datetime,
                                        end_date: datetime) -> pd.DataFrame:
        """Download historical data from Alpha Vantage."""
        if "alphavantage" not in self.api_keys:
            raise ValueError("Alpha Vantage API key not found")
            
        api_key = self.api_keys["alphavantage"]
        
        # Map timeframe to Alpha Vantage interval
        interval_map = {
            "1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min",
            "1h": "60min", "1d": "daily"
        }
        
        if timeframe not in interval_map:
            raise ValueError(f"Unsupported timeframe for Alpha Vantage: {timeframe}")
            
        interval = interval_map[timeframe]
        
        # For FX, we need to format the symbol correctly
        # Alpha Vantage uses format like "EURUSD" instead of "EUR_USD"
        av_symbol = symbol.replace("_", "")
        
        base_url = "https://www.alphavantage.co/query"
        
        if interval == "daily":
            function = "FX_DAILY"
            params = {
                "function": function,
                "from_symbol": av_symbol[:3],
                "to_symbol": av_symbol[3:],
                "outputsize": "full",
                "apikey": api_key
            }
        else:
            function = "FX_INTRADAY"
            params = {
                "function": function,
                "from_symbol": av_symbol[:3],
                "to_symbol": av_symbol[3:],
                "interval": interval,
                "outputsize": "full",
                "apikey": api_key
            }
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(base_url, params=params) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Error from Alpha Vantage: {response.status} - {error_text}")
                        return pd.DataFrame()
                        
                    data = await response.json()
                    
                    # Check for error messages
                    if "Error Message" in data:
                        logger.error(f"Alpha Vantage error: {data['Error Message']}")
                        return pd.DataFrame()
                        
                    # Parse the data based on the function
                    time_series_key = None
                    if function == "FX_DAILY":
                        time_series_key = "Time Series FX (Daily)"
                    elif function == "FX_INTRADAY":
                        time_series_key = f"Time Series FX ({interval})"
                        
                    if time_series_key not in data:
                        logger.error(f"Expected key {time_series_key} not found in Alpha Vantage response")
                        return pd.DataFrame()
                        
                    # Process the time series data
                    time_series = data[time_series_key]
                    all_data = []
                    
                    for date_str, values in time_series.items():
                        # Parse the timestamp
                        if function == "FX_DAILY":
                            timestamp = datetime.strptime(date_str, "%Y-%m-%d")
                        else:
                            timestamp = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                            
                        # Skip if outside our date range
                        if timestamp < start_date or timestamp > end_date:
                            continue
                            
                        all_data.append({
                            "timestamp": timestamp,
                            "open": float(values["1. open"]),
                            "high": float(values["2. high"]),
                            "low": float(values["3. low"]),
                            "close": float(values["4. close"]),
                            # Alpha Vantage doesn't provide volume for FX, use 0
                            "volume": 0.0
                        })
                    
                    if not all_data:
                        logger.warning(f"No data within date range from Alpha Vantage for {symbol} {timeframe}")
                        return pd.DataFrame()
                        
                    # Convert to DataFrame
                    df = pd.DataFrame(all_data)
                    df.set_index("timestamp", inplace=True)
                    df.sort_index(inplace=True)
                    
                    # Store in database
                    self._store_in_database(df, symbol, timeframe, "alphavantage")
                    
                    return df
                    
            except Exception as e:
                logger.error(f"Error in Alpha Vantage request: {e}")
                return pd.DataFrame()
    
    def _generate_mock_data(self, 
                           symbol: str, 
                           timeframe: str, 
                           start_date: datetime,
                           end_date: datetime) -> pd.DataFrame:
        """
        Generate mock data for testing when real data is unavailable.
        Uses a random walk with drift based on the symbol.
        """
        logger.info(f"Generating mock data for {symbol} {timeframe}")
        
        # Get seconds for the timeframe
        if timeframe not in self.timeframes:
            raise ValueError(f"Unsupported timeframe: {timeframe}")
            
        seconds = self.timeframes[timeframe]
        
        # Generate timestamps
        current = start_date
        timestamps = []
        
        while current <= end_date:
            # Skip weekends for daily timeframe
            if timeframe == "1d" and current.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
                current += timedelta(days=1)
                continue
                
            timestamps.append(current)
            
            if timeframe == "1d":
                current += timedelta(days=1)
            else:
                current += timedelta(seconds=seconds)
        
        if not timestamps:
            return pd.DataFrame()
        
        # Set seed based on symbol for reproducibility
        seed = sum(ord(c) for c in symbol)
        np.random.seed(seed)
        
        # Generate price data with random walk
        # Use different parameters for different symbols
        n = len(timestamps)
        
        # Base values and drift depend on the symbol
        if "JPY" in symbol:
            base_price = 100 + np.random.rand() * 20
            drift = 0.00002
            volatility = 0.0008
        elif "GBP" in symbol:
            base_price = 1.2 + np.random.rand() * 0.2
            drift = 0.00003
            volatility = 0.0006
        else:  # EUR, USD pairs
            base_price = 1.0 + np.random.rand() * 0.2
            drift = 0.00001
            volatility = 0.0005
        
        # Random walk with drift
        returns = np.random.normal(drift, volatility, n)
        returns[0] = 0  # Start with no return
        
        # Cumulative returns
        cum_returns = np.cumsum(returns)
        
        # Generate prices
        closes = base_price * (1 + cum_returns)
        
        # Generate OHLC based on close prices
        opens = np.roll(closes, 1)
        opens[0] = base_price
        
        # Calculate random highs and lows
        high_diffs = np.random.uniform(0, volatility * 2, n)
        low_diffs = np.random.uniform(0, volatility * 2, n)
        
        highs = np.maximum(opens, closes) + high_diffs
        lows = np.minimum(opens, closes) - low_diffs
        
        # Generate random volume
        volume_base = 1000
        volume_var = 500
        volumes = np.random.uniform(volume_base - volume_var, volume_base + volume_var, n)
        
        # Create DataFrame
        df = pd.DataFrame({
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes
        }, index=timestamps)
        
        # Store in database
        self._store_in_database(df, symbol, timeframe, "mock")
        
        return df
    
    def _store_in_database(self, 
                          df: pd.DataFrame, 
                          symbol: str, 
                          timeframe: str, 
                          source: str) -> None:
        """Store the downloaded data in the database."""
        if df.empty:
            return
            
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Get source_id
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM data_sources WHERE name = ?", (source,))
            result = cursor.fetchone()
            
            if not result:
                logger.error(f"Source {source} not found in database")
                conn.close()
                return
                
            source_id = result[0]
            
            # Prepare data for insertion
            data_to_insert = []
            for timestamp, row in df.iterrows():
                unix_time = int(timestamp.timestamp())
                data_to_insert.append((
                    symbol,
                    timeframe,
                    unix_time,
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"],
                    source_id
                ))
            
            # Insert data with REPLACE to handle duplicates
            cursor.executemany(
                '''
                INSERT OR REPLACE INTO ohlcv_data 
                (symbol, timeframe, timestamp, open, high, low, close, volume, source_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                data_to_insert
            )
            
            # Record the download in history
            start_time = int(df.index.min().timestamp())
            end_time = int(df.index.max().timestamp())
            download_time = int(datetime.now().timestamp())
            
            cursor.execute(
                '''
                INSERT INTO download_history 
                (symbol, timeframe, start_time, end_time, source_id, download_time)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                (symbol, timeframe, start_time, end_time, source_id, download_time)
            )
            
            conn.commit()
            logger.info(f"Stored {len(df)} {timeframe} bars for {symbol} in database")
            
        except Exception as e:
            logger.error(f"Error storing data in database: {e}")
            conn.rollback()
            
        finally:
            conn.close()
    
    def get_data_from_db(self, 
                        symbol: str, 
                        timeframe: str, 
                        start_date: datetime,
                        end_date: datetime,
                        source: str = "oanda") -> pd.DataFrame:
        """Retrieve data from the database."""
        start_time = int(start_date.timestamp())
        end_time = int(end_date.timestamp())
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Get source_id
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM data_sources WHERE name = ?", (source,))
            result = cursor.fetchone()
            
            if not result:
                logger.error(f"Source {source} not found in database")
                return pd.DataFrame()
                
            source_id = result[0]
            
            # Query for the data
            query = '''
            SELECT timestamp, open, high, low, close, volume
            FROM ohlcv_data
            WHERE symbol = ? AND timeframe = ? AND source_id = ?
            AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp
            '''
            
            df = pd.read_sql_query(
                query,
                conn,
                params=(symbol, timeframe, source_id, start_time, end_time),
                parse_dates={"timestamp": {"unit": "s"}}
            )
            
            if df.empty:
                return df
                
            # Set timestamp as index
            df.set_index("timestamp", inplace=True)
            
            return df
            
        except Exception as e:
            logger.error(f"Error retrieving data from database: {e}")
            return pd.DataFrame()
            
        finally:
            conn.close()
    
    async def get_historical_data(self, 
                                symbol: str, 
                                timeframe: str, 
                                start_date: Union[datetime, str],
                                end_date: Union[datetime, str] = None,
                                source: str = "oanda") -> pd.DataFrame:
        """
        Get historical data, either from cache or by downloading.
        
        Args:
            symbol: Trading symbol (e.g., "EUR_USD")
            timeframe: Time interval (e.g., "1h", "1d")
            start_date: Start date for historical data (datetime or string)
            end_date: End date for historical data (datetime or string, defaults to now)
            source: Data source to use
            
        Returns:
            DataFrame with historical OHLCV data
        """
        # Convert string dates to datetime if needed
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            
        if end_date is None:
            end_date = datetime.now()
        elif isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        
        # Download the data
        df = await self.download_data(symbol, timeframe, start_date, end_date, source)
        
        return df
    
    def clear_cache(self, older_than_days: int = None) -> None:
        """
        Clear cached data files.
        
        Args:
            older_than_days: Only clear files older than this many days
        """
        if older_than_days is None:
            older_than_days = self.cache_days
            
        now = datetime.now()
        count = 0
        
        for filename in os.listdir(self.data_dir):
            if filename.endswith('.csv'):
                filepath = os.path.join(self.data_dir, filename)
                file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                
                if (now - file_time).days > older_than_days:
                    try:
                        os.remove(filepath)
                        count += 1
                    except Exception as e:
                        logger.error(f"Error removing cache file {filepath}: {e}")
        
        logger.info(f"Cleared {count} cache files older than {older_than_days} days")
    
    def export_to_csv(self, 
                     symbol: str, 
                     timeframe: str,
                     start_date: datetime,
                     end_date: datetime,
                     source: str = "oanda",
                     filepath: str = None) -> str:
        """
        Export data to a CSV file.
        
        Args:
            symbol: Trading symbol
            timeframe: Time interval
            start_date: Start date
            end_date: End date
            source: Data source
            filepath: Output file path (optional)
            
        Returns:
            Path to the CSV file
        """
        # Get the data
        df = self.get_data_from_db(symbol, timeframe, start_date, end_date, source)
        
        if df.empty:
            logger.warning(f"No data to export for {symbol} {timeframe}")
            return None
            
        # Default filepath if not provided
        if filepath is None:
            filepath = os.path.join(
                self.data_dir, 
                f"export_{symbol}_{timeframe}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
            )
            
        # Export to CSV
        df.to_csv(filepath)
        logger.info(f"Exported {len(df)} rows to {filepath}")
        
        return filepath
    
    async def get_data_for_symbols(self, 
                                 symbols: List[str],
                                 timeframe: str,
                                 start_date: datetime,
                                 end_date: datetime,
                                 source: str = "oanda") -> Dict[str, pd.DataFrame]:
        """
        Get historical data for multiple symbols.
        
        Args:
            symbols: List of trading symbols
            timeframe: Time interval
            start_date: Start date
            end_date: End date
            source: Data source
            
        Returns:
            Dictionary mapping symbols to their respective DataFrames
        """
        results = {}
        
        # Process symbols concurrently
        tasks = [
            self.get_historical_data(symbol, timeframe, start_date, end_date, source)
            for symbol in symbols
        ]
        
        data_list = await asyncio.gather(*tasks, return_exceptions=True)
        
        for symbol, data in zip(symbols, data_list):
            if isinstance(data, Exception):
                logger.error(f"Error getting data for {symbol}: {data}")
                results[symbol] = pd.DataFrame()
            else:
                results[symbol] = data
                
        return results
    
    async def update_database(self, 
                            symbols: List[str], 
                            timeframes: List[str],
                            days_back: int = 30,
                            source: str = "oanda") -> Dict[str, Dict[str, int]]:
        """
        Update the database with the latest data for multiple symbols and timeframes.
        
        Args:
            symbols: List of trading symbols
            timeframes: List of time intervals
            days_back: Number of days to look back
            source: Data source
            
        Returns:
            Dictionary with statistics about updated data
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        results = {}
        
        for symbol in symbols:
            results[symbol] = {}
            
            for timeframe in timeframes:
                try:
                    data = await self.get_historical_data(
                        symbol, timeframe, start_date, end_date, source
                    )
                    
                    results[symbol][timeframe] = len(data)
                    logger.info(f"Updated {symbol} {timeframe}: {len(data)} bars")
                    
                except Exception as e:
                    logger.error(f"Error updating {symbol} {timeframe}: {e}")
                    results[symbol][timeframe] = 0
                    
        return results


# Example usage
async def main():
    # Create a data manager
    data_manager = HistoricalDataManager()
    
    # Download some data
    start_date = datetime.now() - timedelta(days=30)
    end_date = datetime.now()
    
    # Get historical data for EUR/USD
    data = await data_manager.get_historical_data(
        "EUR_USD", "1h", start_date, end_date, "mock"
    )
    
    print(f"Downloaded {len(data)} 1-hour bars for EUR_USD")
    if not data.empty:
        print(data.head())
    
    # Export to CSV
    filepath = data_manager.export_to_csv(
        "EUR_USD", "1h", start_date, end_date, "mock"
    )
    
    print(f"Exported data to {filepath}")

if __name__ == "__main__":
    asyncio.run(main()) 