"""
Historical Data Downloader for FX Trading Backtesting

This module provides functionality to download historical price data for backtesting.
It supports multiple data sources and formats the data to work with the backtesting engine.

Available data sources:
- Alpha Vantage (free API with registration)
- FXCM Demo (free historical data for forex)
- CSV import (for local or custom data sources)
"""

import os
import csv
import json
import aiohttp
import asyncio
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("data_downloader")

class DataDownloader:
    """
    Downloads and prepares historical market data for backtesting.
    
    This class provides methods to fetch data from various sources and
    save it in a standardized format for use with the backtesting engine.
    """
    
    def __init__(self, data_dir: str = "historical_data"):
        """
        Initialize the data downloader
        
        Args:
            data_dir: Directory to store downloaded data
        """
        self.data_dir = data_dir
        self.session = None
        
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
    
    async def initialize(self):
        """Initialize the downloader with an HTTP session"""
        if not self.session:
            self.session = aiohttp.ClientSession()
    
    async def shutdown(self):
        """Close the HTTP session"""
        if self.session:
            await self.session.close()
            self.session = None
    
    async def download_alpha_vantage_forex(
        self,
        symbol: str,
        api_key: str,
        interval: str = "60min",
        output_size: str = "full"
    ) -> bool:
        """
        Download forex data from Alpha Vantage API
        
        Args:
            symbol: Currency pair in format 'EURUSD'
            api_key: Alpha Vantage API key
            interval: Time interval ('1min', '5min', '15min', '30min', '60min', 'daily')
            output_size: 'compact' for last 100 points, 'full' for complete history
            
        Returns:
            True if successful, False otherwise
        """
        await self.initialize()
        
        # Format symbol for Alpha Vantage (e.g., 'EUR_USD' -> 'EURUSD')
        symbol_formatted = symbol.replace('_', '')
        from_currency = symbol_formatted[:3]
        to_currency = symbol_formatted[3:]
        
        # Map interval to Alpha Vantage format
        interval_map = {
            "1min": "1min",
            "5min": "5min", 
            "15min": "15min",
            "30min": "30min",
            "60min": "60min",
            "1h": "60min",
            "daily": "daily",
            "1d": "daily",
            "weekly": "weekly",
            "1w": "weekly",
            "monthly": "monthly"
        }
        
        av_interval = interval_map.get(interval, "60min")
        
        try:
            # Construct API URL
            if av_interval in ["daily", "weekly", "monthly"]:
                function = "FX_DAILY" if av_interval == "daily" else "FX_WEEKLY" if av_interval == "weekly" else "FX_MONTHLY"
                url = f"https://www.alphavantage.co/query?function={function}&from_symbol={from_currency}&to_symbol={to_currency}&outputsize={output_size}&apikey={api_key}"
            else:
                url = f"https://www.alphavantage.co/query?function=FX_INTRADAY&from_symbol={from_currency}&to_symbol={to_currency}&interval={av_interval}&outputsize={output_size}&apikey={api_key}"
            
            # Make the request
            async with self.session.get(url) as response:
                if response.status != 200:
                    logger.error(f"Failed to download data: HTTP {response.status}")
                    return False
                
                data = await response.json()
                
                # Check for error message
                if "Error Message" in data or "Information" in data or "Note" in data:
                    error_msg = data.get("Error Message") or data.get("Information") or data.get("Note")
                    logger.error(f"API returned error: {error_msg}")
                    return False
                
                # Extract time series data
                time_series_key = f"Time Series FX ({av_interval})" if av_interval not in ["daily", "weekly", "monthly"] else f"Time Series FX ({av_interval.replace('ly', '')})"
                
                if time_series_key not in data:
                    logger.error(f"Expected key {time_series_key} not found in response")
                    logger.error(f"Response keys: {data.keys()}")
                    return False
                
                time_series = data[time_series_key]
                
                # Convert to dataframe
                records = []
                for timestamp, values in time_series.items():
                    records.append({
                        "time": timestamp,
                        "open": float(values["1. open"]),
                        "high": float(values["2. high"]),
                        "low": float(values["3. low"]),
                        "close": float(values["4. close"]),
                        "volume": float(values.get("5. volume", 0))
                    })
                
                df = pd.DataFrame(records)
                df["time"] = pd.to_datetime(df["time"])
                df = df.sort_values("time")
                
                # Save to CSV
                output_file = os.path.join(self.data_dir, f"{symbol}_{interval}.csv")
                df.to_csv(output_file, index=False)
                
                logger.info(f"Downloaded {len(df)} records for {symbol} ({interval}) to {output_file}")
                return True
                
        except Exception as e:
            logger.error(f"Error downloading Alpha Vantage data: {str(e)}")
            return False
    
    async def download_fxcm_historical(
        self,
        symbol: str,
        timeframe: str = "H1",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> bool:
        """
        Download historical forex data from FXCM API
        
        Args:
            symbol: Currency pair in FXCM format (e.g., 'EUR/USD')
            timeframe: Time interval ('m1', 'm5', 'm15', 'm30', 'H1', 'H4', 'D1', 'W1', 'M1')
            start_date: Start date for data download (None for earliest available)
            end_date: End date for data download (None for latest available)
            
        Returns:
            True if successful, False otherwise
        """
        await self.initialize()
        
        # Format symbol for FXCM API
        symbol_formatted = symbol.replace('_', '/')
        
        # Map timeframe to FXCM format
        timeframe_map = {
            "1min": "m1",
            "5min": "m5", 
            "15min": "m15",
            "30min": "m30",
            "60min": "H1",
            "1h": "H1",
            "4h": "H4",
            "daily": "D1",
            "1d": "D1",
            "weekly": "W1",
            "1w": "W1",
            "monthly": "M1"
        }
        
        fxcm_timeframe = timeframe_map.get(timeframe, timeframe)
        
        # Format dates if provided
        date_from = start_date.strftime("%Y-%m-%d") if start_date else "2000-01-01"
        date_to = end_date.strftime("%Y-%m-%d") if end_date else datetime.now().strftime("%Y-%m-%d")
        
        try:
            # Construct API URL
            url = f"https://fxcm-data.herokuapp.com/candles/{symbol_formatted}/{fxcm_timeframe}/{date_from}/{date_to}"
            
            # Make the request
            async with self.session.get(url) as response:
                if response.status != 200:
                    logger.error(f"Failed to download FXCM data: HTTP {response.status}")
                    return False
                
                data = await response.json()
                
                if "data" not in data or not data["data"]:
                    logger.error(f"No data returned from FXCM API for {symbol_formatted}")
                    return False
                
                # Convert to dataframe
                df = pd.DataFrame(data["data"])
                df.columns = ["time", "open", "close", "high", "low", "ticks"]
                df["volume"] = df["ticks"]  # Use ticks as volume
                df["time"] = pd.to_datetime(df["time"], unit='ms')
                df = df.sort_values("time")
                
                # Keep only required columns
                df = df[["time", "open", "high", "low", "close", "volume"]]
                
                # Save to CSV
                output_file = os.path.join(self.data_dir, f"{symbol}_{timeframe}.csv")
                df.to_csv(output_file, index=False)
                
                logger.info(f"Downloaded {len(df)} records for {symbol} ({timeframe}) to {output_file}")
                return True
                
        except Exception as e:
            logger.error(f"Error downloading FXCM data: {str(e)}")
            return False
    
    def import_csv_data(
        self,
        file_path: str,
        symbol: str,
        timeframe: str,
        time_format: str = "%Y-%m-%d %H:%M:%S",
        has_header: bool = True,
        column_map: Dict[str, int] = None
    ) -> bool:
        """
        Import historical data from a CSV file
        
        Args:
            file_path: Path to the CSV file
            symbol: Symbol name to assign to this data
            timeframe: Timeframe to assign to this data
            time_format: Format of the timestamp in the CSV
            has_header: Whether the CSV has a header row
            column_map: Map of column names to indices (default assumes columns are
                        time, open, high, low, close, volume)
                        
        Returns:
            True if successful, False otherwise
        """
        try:
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                return False
            
            # Default column mapping
            if column_map is None:
                column_map = {
                    "time": 0,
                    "open": 1,
                    "high": 2,
                    "low": 3,
                    "close": 4,
                    "volume": 5
                }
            
            # Read CSV
            data = []
            with open(file_path, 'r') as f:
                csv_reader = csv.reader(f)
                if has_header:
                    next(csv_reader)  # Skip header
                
                for row in csv_reader:
                    if len(row) < max(column_map.values()) + 1:
                        logger.warning(f"Skipping row with insufficient columns: {row}")
                        continue
                    
                    try:
                        timestamp = datetime.strptime(row[column_map["time"]], time_format)
                        record = {
                            "time": timestamp,
                            "open": float(row[column_map["open"]]),
                            "high": float(row[column_map["high"]]),
                            "low": float(row[column_map["low"]]),
                            "close": float(row[column_map["close"]]),
                            "volume": float(row[column_map["volume"]])
                        }
                        data.append(record)
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Error parsing row: {row}. Error: {str(e)}")
                        continue
            
            # Convert to dataframe and sort
            df = pd.DataFrame(data)
            df = df.sort_values("time")
            
            # Save to standardized CSV
            output_file = os.path.join(self.data_dir, f"{symbol}_{timeframe}.csv")
            df.to_csv(output_file, index=False)
            
            logger.info(f"Imported {len(df)} records for {symbol} ({timeframe}) to {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error importing CSV data: {str(e)}")
            return False
    
    def list_available_data(self) -> List[Dict[str, str]]:
        """
        List all available historical data files
        
        Returns:
            List of dictionaries with symbol and timeframe information
        """
        try:
            result = []
            for filename in os.listdir(self.data_dir):
                if filename.endswith(".csv"):
                    parts = filename.replace(".csv", "").split("_")
                    if len(parts) >= 2:
                        symbol = parts[0]
                        timeframe = parts[1]
                        
                        # Get file info
                        file_path = os.path.join(self.data_dir, filename)
                        file_size = os.path.getsize(file_path)
                        modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                        
                        # Get record count and date range
                        try:
                            df = pd.read_csv(file_path)
                            count = len(df)
                            if "time" in df.columns:
                                df["time"] = pd.to_datetime(df["time"])
                                date_range = f"{df['time'].min().strftime('%Y-%m-%d')} to {df['time'].max().strftime('%Y-%m-%d')}"
                            else:
                                date_range = "Unknown"
                        except Exception:
                            count = 0
                            date_range = "Error reading file"
                        
                        result.append({
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "filename": filename,
                            "size_kb": round(file_size / 1024, 2),
                            "records": count,
                            "date_range": date_range,
                            "modified": modified_time.strftime("%Y-%m-%d %H:%M:%S")
                        })
            
            return result
            
        except Exception as e:
            logger.error(f"Error listing available data: {str(e)}")
            return []
    
    def get_symbols_with_data(self) -> List[str]:
        """Get a list of symbols that have data available"""
        try:
            symbols = set()
            for filename in os.listdir(self.data_dir):
                if filename.endswith(".csv"):
                    parts = filename.replace(".csv", "").split("_")
                    if len(parts) >= 2:
                        symbols.add(parts[0])
            
            return sorted(list(symbols))
            
        except Exception as e:
            logger.error(f"Error getting symbols: {str(e)}")
            return []
    
    def get_available_timeframes(self, symbol: str) -> List[str]:
        """Get available timeframes for a given symbol"""
        try:
            timeframes = []
            for filename in os.listdir(self.data_dir):
                if filename.endswith(".csv") and filename.startswith(f"{symbol}_"):
                    parts = filename.replace(".csv", "").split("_")
                    if len(parts) >= 2:
                        timeframes.append(parts[1])
            
            return timeframes
            
        except Exception as e:
            logger.error(f"Error getting timeframes: {str(e)}")
            return []

# Example usage
async def download_example_data():
    downloader = DataDownloader()
    await downloader.initialize()
    
    # You need your own Alpha Vantage API key (sign up at https://www.alphavantage.co/)
    api_key = "YOUR_ALPHA_VANTAGE_API_KEY"
    
    # Download some example data
    await downloader.download_alpha_vantage_forex("EUR_USD", api_key, interval="60min")
    await downloader.download_alpha_vantage_forex("GBP_USD", api_key, interval="60min")
    
    # Download from FXCM (no API key required)
    await downloader.download_fxcm_historical("EUR_USD", timeframe="H1")
    
    # List available data
    print("Available historical data:")
    for data_info in downloader.list_available_data():
        print(f"{data_info['symbol']} ({data_info['timeframe']}): {data_info['records']} records, {data_info['date_range']}")
    
    await downloader.shutdown()

if __name__ == "__main__":
    asyncio.run(download_example_data()) 