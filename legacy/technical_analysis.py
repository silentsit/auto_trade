"""
Technical Analysis Module
Provides essential technical indicators for trading strategies
"""

import pandas as pd
import numpy as np
from typing import Optional
import logging

logger = logging.getLogger(__name__)

def get_atr(df: pd.DataFrame, period: int = 14) -> Optional[float]:
    """
    Calculate Average True Range (ATR) from OHLC data
    
    Args:
        df: DataFrame with columns ['high', 'low', 'close'] or ['h', 'l', 'c']
        period: ATR calculation period (default: 14)
    
    Returns:
        float: Current ATR value or None if calculation fails
    """
    try:
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for ATR calculation")
            return None
            
        # Normalize column names (handle both 'high'/'h' formats)
        df_cols = df.columns.str.lower()
        
        # Map column names to standard format
        col_mapping = {}
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ['high', 'h']:
                col_mapping['high'] = col
            elif col_lower in ['low', 'l']:
                col_mapping['low'] = col
            elif col_lower in ['close', 'c']:
                col_mapping['close'] = col
                
        # Check if we have required columns
        required_cols = ['high', 'low', 'close']
        if not all(req_col in col_mapping for req_col in required_cols):
            logger.error(f"Missing required columns for ATR. Available: {list(df.columns)}")
            return None
            
        # Extract OHLC data
        high = df[col_mapping['high']]
        low = df[col_mapping['low']]
        close = df[col_mapping['close']]
        
        if len(df) < period + 1:
            logger.warning(f"Insufficient data for ATR calculation. Need {period + 1}, got {len(df)}")
            return None
            
        # Calculate True Range components
        prev_close = close.shift(1)
        
        tr1 = high - low
        tr2 = np.abs(high - prev_close)
        tr3 = np.abs(low - prev_close)
        
        # True Range is the maximum of the three components
        true_range = np.maximum(tr1, np.maximum(tr2, tr3))
        
        # Calculate ATR as simple moving average of True Range
        atr = true_range.rolling(window=period, min_periods=period).mean()
        
        # Return the most recent ATR value
        current_atr = atr.iloc[-1]
        
        if pd.isna(current_atr) or current_atr <= 0:
            logger.warning(f"Invalid ATR calculated: {current_atr}")
            return None
            
        logger.debug(f"ATR calculated successfully: {current_atr:.5f}")
        return float(current_atr)
        
    except Exception as e:
        logger.error(f"Error calculating ATR: {e}")
        return None

def calculate_sma(data: pd.Series, period: int) -> pd.Series:
    """Calculate Simple Moving Average"""
    return data.rolling(window=period).mean()

def calculate_ema(data: pd.Series, period: int) -> pd.Series:
    """Calculate Exponential Moving Average"""
    return data.ewm(span=period).mean()

def calculate_rsi(data: pd.Series, period: int = 14) -> pd.Series:
    """Calculate Relative Strength Index"""
    delta = data.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_bollinger_bands(data: pd.Series, period: int = 20, std_dev: float = 2.0) -> tuple:
    """Calculate Bollinger Bands"""
    sma = calculate_sma(data, period)
    std = data.rolling(window=period).std()
    upper_band = sma + (std * std_dev)
    lower_band = sma - (std * std_dev)
    return upper_band, sma, lower_band

# Additional utility functions for compatibility
def get_support_resistance(df: pd.DataFrame, window: int = 20) -> tuple:
    """Basic support and resistance calculation"""
    try:
        close_col = None
        for col in df.columns:
            if col.lower() in ['close', 'c']:
                close_col = col
                break
                
        if close_col is None:
            return None, None
            
        close = df[close_col]
        resistance = close.rolling(window=window).max()
        support = close.rolling(window=window).min()
        
        return float(resistance.iloc[-1]), float(support.iloc[-1])
    except Exception as e:
        logger.error(f"Error calculating support/resistance: {e}")
        return None, None
