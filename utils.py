"""
UTILITIES MODULE
Common utility functions for the trading bot
"""

import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Tuple, Union
import math

# Set up logger
logger = logging.getLogger(__name__)

# Constants
RETRY_DELAY = 5  # seconds

def is_market_hours(dt: Optional[datetime] = None) -> bool:
    """
    Check if markets are open (Monday 00:00 to Friday 17:00 NY time)
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
    
    # Convert to NY time (UTC-5, or UTC-4 during daylight saving)
    ny_tz = timezone(timedelta(hours=-5))  # Standard time
    ny_time = dt.astimezone(ny_tz)
    
    weekday = ny_time.weekday()  # 0=Monday, 6=Sunday
    hour = ny_time.hour
    
    # Markets are closed on weekends
    if weekday >= 5:  # Saturday or Sunday
        return False
    
    # Markets close at 17:00 NY time on Friday
    if weekday == 4 and hour >= 17:  # Friday 17:00+
        return False
    
    # Markets are open Monday 00:00 to Friday 17:00
    return True

def get_atr(symbol: str, period: int = 14) -> float:
    """
    Get Average True Range for a symbol
    Placeholder implementation - would integrate with market data
    """
    # Default ATR values for different instruments
    default_atrs = {
        'EUR_USD': 0.0008,
        'GBP_USD': 0.0012,
        'USD_JPY': 0.12,
        'AUD_USD': 0.0010,
        'USD_CAD': 0.0009,
        'NZD_USD': 0.0015,
        'USD_CHF': 0.0011,
        'EUR_GBP': 0.0006,
        'EUR_JPY': 0.15,
        'GBP_JPY': 0.25,
        'AUD_JPY': 0.20,
        'CAD_JPY': 0.18,
        'CHF_JPY': 0.19,
        'NZD_JPY': 0.25,
        'EUR_AUD': 0.0015,
        'EUR_CAD': 0.0010,
        'EUR_NZD': 0.0020,
        'GBP_AUD': 0.0020,
        'GBP_CAD': 0.0015,
        'GBP_NZD': 0.0025,
        'AUD_CAD': 0.0012,
        'AUD_NZD': 0.0020,
        'CAD_NZD': 0.0018,
        'EUR_CHF': 0.0008,
        'GBP_CHF': 0.0015,
        'AUD_CHF': 0.0018,
        'NZD_CHF': 0.0020,
        'CAD_CHF': 0.0012,
        'XAU_USD': 15.0,  # Gold
        'XAG_USD': 0.5,   # Silver
        'BTC_USD': 500.0, # Bitcoin
        'ETH_USD': 50.0,  # Ethereum
    }
    
    return default_atrs.get(symbol, 0.001)  # Default ATR

def get_instrument_type(symbol: str) -> str:
    """
    Get instrument type (forex, crypto, metal, etc.)
    """
    if symbol.endswith('_USD') and symbol.startswith(('XAU', 'XAG')):
        return 'metal'
    elif symbol.endswith('_USD') and symbol.startswith(('BTC', 'ETH', 'LTC', 'XRP')):
        return 'crypto'
    elif '_' in symbol and len(symbol) == 7:
        return 'forex'
    else:
        return 'unknown'

def get_atr_multiplier(symbol: str) -> float:
    """
    Get ATR multiplier for stop loss and take profit calculations
    """
    instrument_type = get_instrument_type(symbol)
    
    if instrument_type == 'crypto':
        return 2.0  # Higher multiplier for crypto due to volatility
    elif instrument_type == 'metal':
        return 1.5  # Medium multiplier for metals
    else:  # forex
        return 1.0  # Standard multiplier for forex

def round_price(price: float, symbol: str) -> float:
    """
    Round price to appropriate decimal places for the instrument
    """
    instrument_type = get_instrument_type(symbol)
    
    if instrument_type == 'crypto':
        return round(price, 2)  # 2 decimal places for crypto
    elif instrument_type == 'metal':
        return round(price, 2)  # 2 decimal places for metals
    else:  # forex
        if 'JPY' in symbol:
            return round(price, 3)  # 3 decimal places for JPY pairs
        else:
            return round(price, 5)  # 5 decimal places for other forex pairs

def enforce_min_distance(price1: float, price2: float, symbol: str, min_distance: float = 0.0001) -> Tuple[float, float]:
    """
    Ensure minimum distance between two prices
    """
    if abs(price1 - price2) < min_distance:
        if price1 > price2:
            price1 += min_distance
        else:
            price2 += min_distance
    
    return price1, price2

def get_instrument_leverage(symbol: str) -> float:
    """
    Get maximum leverage for an instrument
    """
    instrument_type = get_instrument_type(symbol)
    
    if instrument_type == 'crypto':
        return 2.0  # Lower leverage for crypto
    elif instrument_type == 'metal':
        return 10.0  # Medium leverage for metals
    else:  # forex
        return 30.0  # Higher leverage for forex

def round_position_size(size: float, symbol: str) -> float:
    """
    Round position size to appropriate precision
    """
    instrument_type = get_instrument_type(symbol)
    
    if instrument_type == 'crypto':
        return round(size, 4)  # 4 decimal places for crypto
    elif instrument_type == 'metal':
        return round(size, 2)  # 2 decimal places for metals
    else:  # forex
        return round(size, 2)  # 2 decimal places for forex

def get_position_size_limits(symbol: str) -> Tuple[float, float]:
    """
    Get minimum and maximum position size limits
    """
    instrument_type = get_instrument_type(symbol)
    
    if instrument_type == 'crypto':
        return (0.001, 10.0)  # Crypto limits
    elif instrument_type == 'metal':
        return (0.01, 100.0)  # Metal limits
    else:  # forex
        return (0.01, 1000.0)  # Forex limits

def calculate_position_size(account_balance: float, risk_percent: float, 
                          stop_loss_pips: float, symbol: str) -> float:
    """
    Calculate position size based on risk management
    """
    try:
        # Get pip value for the symbol
        pip_value = get_pip_value(symbol)
        
        # Calculate risk amount
        risk_amount = account_balance * (risk_percent / 100)
        
        # Calculate position size
        position_size = risk_amount / (stop_loss_pips * pip_value)
        
        # Apply limits
        min_size, max_size = get_position_size_limits(symbol)
        position_size = max(min_size, min(max_size, position_size))
        
        # Round to appropriate precision
        return round_position_size(position_size, symbol)
        
    except Exception as e:
        logger.error(f"Error calculating position size: {e}")
        return 0.01  # Return minimum size on error

def get_pip_value(symbol: str) -> float:
    """
    Get pip value for a symbol
    """
    if 'JPY' in symbol:
        return 0.01  # JPY pairs have 0.01 pip value
    else:
        return 0.0001  # Other pairs have 0.0001 pip value

def format_symbol_for_oanda(symbol: str) -> str:
    """
    Format symbol for OANDA API
    """
    # OANDA uses underscores for forex pairs
    if '_' not in symbol and len(symbol) == 6:
        # Convert from XXXYYY to XXX_YYY format
        return f"{symbol[:3]}_{symbol[3:]}"
    return symbol

def standardize_symbol(symbol: str) -> str:
    """
    Standardize symbol format
    """
    # Remove any spaces and convert to uppercase
    symbol = symbol.replace(' ', '').upper()
    
    # Ensure proper format
    if '_' not in symbol and len(symbol) == 6:
        return f"{symbol[:3]}_{symbol[3:]}"
    
    return symbol

def is_instrument_tradeable(symbol: str) -> bool:
    """
    Check if instrument is tradeable
    """
    # Basic validation
    if not symbol or len(symbol) < 3:
        return False
    
    # Check if it's a known instrument type
    instrument_type = get_instrument_type(symbol)
    return instrument_type != 'unknown'

def get_current_market_session() -> str:
    """
    Get current market session
    """
    dt = datetime.now(timezone.utc)
    hour = dt.hour
    
    if 0 <= hour < 8:
        return 'asian'
    elif 8 <= hour < 16:
        return 'european'
    else:
        return 'american'

def get_current_price(symbol: str) -> float:
    """
    Get current price for symbol
    Placeholder implementation - would integrate with market data
    """
    # Default prices for testing
    default_prices = {
        'EUR_USD': 1.0850,
        'GBP_USD': 1.2650,
        'USD_JPY': 150.00,
        'AUD_USD': 0.6550,
        'USD_CAD': 1.3650,
        'NZD_USD': 0.6050,
        'USD_CHF': 0.8750,
        'XAU_USD': 2000.0,
        'XAG_USD': 25.0,
        'BTC_USD': 45000.0,
        'ETH_USD': 3000.0,
    }
    
    return default_prices.get(symbol, 1.0000)

def _get_simulated_price(symbol: str) -> float:
    """
    Get simulated price for testing
    """
    return get_current_price(symbol)

def round_price_for_instrument(price: float, symbol: str) -> float:
    """
    Round price for specific instrument
    """
    return round_price(price, symbol)

def validate_oanda_prices(prices: Dict[str, float]) -> bool:
    """
    Validate OANDA prices
    """
    try:
        for symbol, price in prices.items():
            if price <= 0:
                return False
            if not isinstance(price, (int, float)):
                return False
        return True
    except Exception:
        return False

def format_crypto_symbol_for_oanda(symbol: str) -> str:
    """
    Format crypto symbol for OANDA
    """
    # OANDA crypto symbols
    crypto_mapping = {
        'BTC_USD': 'BTC_USD',
        'ETH_USD': 'ETH_USD',
        'LTC_USD': 'LTC_USD',
        'XRP_USD': 'XRP_USD',
    }
    
    return crypto_mapping.get(symbol, symbol)

def parse_iso_datetime(dt_str: str) -> datetime:
    """
    Parse ISO datetime string
    """
    try:
        return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
    except Exception:
        return datetime.now(timezone.utc)

def get_module_logger(module_name: str) -> logging.Logger:
    """
    Get logger for a module
    """
    return logging.getLogger(module_name)

# Market data unavailable error
class MarketDataUnavailableError(Exception):
    """Raised when market data is unavailable"""
    pass