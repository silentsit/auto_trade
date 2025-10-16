"""
UTILITIES MODULE
Common utility functions for the trading bot
"""

import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Tuple, Union, List
import math
import numpy as np

# Set up logger
logger = logging.getLogger(__name__)

# Constants
RETRY_DELAY = 5  # seconds

def is_market_hours(dt: Optional[datetime] = None) -> bool:
    """
    Check if FX markets are open (Sunday 17:00 NY to Friday 17:00 NY)
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
    
    # Convert to NY time (UTC-5, or UTC-4 during daylight saving)
    ny_tz = timezone(timedelta(hours=-5))  # Standard time
    ny_time = dt.astimezone(ny_tz)
    
    weekday = ny_time.weekday()  # 0=Monday, 6=Sunday
    hour = ny_time.hour
    
    # Saturday is always closed
    if weekday == 5:  # Saturday
        return False
    
    # Sunday: markets open at 17:00 NY time
    if weekday == 6:  # Sunday
        return hour >= 17
    
    # Monday-Thursday: markets are open all day
    if 0 <= weekday <= 3:  # Monday-Thursday
        return True
    
    # Friday: markets close at 17:00 NY time
    if weekday == 4:  # Friday
        return hour < 17
    
    return False

def get_atr(symbol: str, period: int = 14) -> float:
    """
    Get Average True Range for a symbol
    Placeholder implementation - would integrate with market data
    """
    # Default ATR values for different instruments - INCREASED for wider stops
    default_atrs = {
        'EUR_USD': 0.0020,    # Increased from 0.0008 to 0.0020 (20 pips)
        'GBP_USD': 0.0030,    # Increased from 0.0012 to 0.0030 (30 pips)
        'USD_JPY': 0.30,      # Increased from 0.12 to 0.30 (30 pips)
        'AUD_USD': 0.0025,    # Increased from 0.0010 to 0.0025 (25 pips)
        'USD_CAD': 0.0022,    # Increased from 0.0009 to 0.0022 (22 pips)
        'NZD_USD': 0.0035,    # Increased from 0.0015 to 0.0035 (35 pips)
        'USD_CHF': 0.0025,    # Increased from 0.0011 to 0.0025 (25 pips)
        'EUR_GBP': 0.0015,    # Increased from 0.0006 to 0.0015 (15 pips)
        'EUR_JPY': 0.35,      # Increased from 0.15 to 0.35 (35 pips)
        'GBP_JPY': 0.50,      # Increased from 0.25 to 0.50 (50 pips)
        'AUD_JPY': 0.45,      # Increased from 0.20 to 0.45 (45 pips)
        'CAD_JPY': 0.40,      # Increased from 0.18 to 0.40 (40 pips)
        'CHF_JPY': 0.42,      # Increased from 0.19 to 0.42 (42 pips)
        'NZD_JPY': 0.50,      # Increased from 0.25 to 0.50 (50 pips)
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
    INCREASED for wider stops to prevent premature exits
    """
    instrument_type = get_instrument_type(symbol)
    
    if instrument_type == 'crypto':
        return 4.0  # Much higher multiplier for crypto due to high volatility
    elif instrument_type == 'metal':
        return 3.0  # Higher multiplier for metals
    else:  # forex
        return 3.0  # Higher multiplier for forex - wider stops

def round_price(price: float, symbol: str) -> float:
    """
    Round price to appropriate decimal places for the instrument
    """
    instrument_type = get_instrument_type(symbol)
    
    if instrument_type == 'crypto':
        return round(price, 0)  # 0 decimal places for crypto (OANDA requirement)
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
        return round(size, 0)  # Whole numbers for crypto (OANDA requirement)
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
        return (1.0, 100000.0)  # Forex limits (min 1 unit, max 100k units)

def calculate_position_size(account_balance: float, risk_percent: float, 
                          stop_loss_pips: float, symbol: str, current_price: float = None) -> float:
    """
    Calculate position size based on risk management
    
    Args:
        account_balance: Account balance in USD
        risk_percent: Risk percentage (e.g., 1.0 for 1%)
        stop_loss_pips: Stop loss distance in pips
        symbol: Trading pair (e.g., 'EUR_USD')
        current_price: Current market price (needed for accurate pip value calculation)
    
    Returns:
        Position size in units
    """
    try:
        # Get pip value for the symbol (pass price for USD base pairs)
        pip_value = get_pip_value(symbol, current_price)
        
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
        return 1.0  # Return minimum forex size on error

def get_pip_value(symbol: str, current_price: float = None) -> float:
    """
    Get pip value for a symbol in USD per unit.
    
    For most pairs, pip value depends on which currency is the quote currency:
    - XXX_USD pairs: 1 pip = 0.0001 USD per unit
    - USD_XXX pairs: 1 pip = 0.0001 / current_price USD per unit
    - JPY pairs: Use 0.01 instead of 0.0001
    
    Args:
        symbol: Trading pair (e.g., 'EUR_USD', 'USD_CHF')
        current_price: Current exchange rate (needed for USD base currency pairs)
    
    Returns:
        Pip value in USD per unit
    """
    symbol_clean = symbol.replace('_', '')
    
    # JPY pairs use 0.01 as pip size (2 decimal places)
    if 'JPY' in symbol:
        pip_size = 0.01
        if symbol_clean.startswith('USD'):
            # USD_JPY: pip value = 0.01 / price
            if current_price:
                return pip_size / current_price
            return 0.0001  # Fallback approximation
        else:
            # XXX_JPY: pip value in JPY, need to convert to USD
            # For simplicity, return approximate value
            return 0.01
    else:
        pip_size = 0.0001
        # Check if USD is the base currency (first 3 letters)
        if symbol_clean.startswith('USD'):
            # USD_XXX: pip value = pip_size / current_price
            if current_price:
                return pip_size / current_price
            return 0.0001  # Fallback if price not provided
        else:
            # XXX_USD or cross pairs: 1 pip = 0.0001 USD per unit
            return pip_size

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

# TradingView field mapping
TV_FIELD_MAP = {
    'symbol': 'symbol',
    'action': 'action',
    'price': 'price',
    'size': 'size',
    'stop_loss': 'stop_loss',
    'take_profit': 'take_profit',
    'timeframe': 'timeframe',
    'strategy': 'strategy',
    'timestamp': 'timestamp'
}

# Asset class mapping
def get_asset_class(symbol: str) -> str:
    """Get asset class for symbol"""
    instrument_type = get_instrument_type(symbol)
    
    if instrument_type == 'crypto':
        return 'crypto'
    elif instrument_type == 'metal':
        return 'commodity'
    elif instrument_type == 'forex':
        return 'forex'
    else:
        return 'unknown'

# Metrics utilities class
class MetricsUtils:
    """Utility class for metrics calculations"""
    
    @staticmethod
    def calculate_sharpe_ratio(returns: List[float], risk_free_rate: float = 0.0) -> float:
        """Calculate Sharpe ratio"""
        if not returns or len(returns) < 2:
            return 0.0
        
        mean_return = np.mean(returns)
        std_return = np.std(returns)
        
        if std_return == 0:
            return 0.0
            
        return (mean_return - risk_free_rate) / std_return
    
    @staticmethod
    def calculate_max_drawdown(equity_curve: List[float]) -> float:
        """Calculate maximum drawdown"""
        if not equity_curve:
            return 0.0
            
        peak = equity_curve[0]
        max_dd = 0.0
        
        for value in equity_curve:
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak
            max_dd = max(max_dd, drawdown)
            
        return max_dd
    
    @staticmethod
    def calculate_win_rate(trades: List[Dict[str, Any]]) -> float:
        """Calculate win rate from trades"""
        if not trades:
            return 0.0
            
        winning_trades = sum(1 for trade in trades if trade.get('pnl', 0) > 0)
        return winning_trades / len(trades)
    
    @staticmethod
    def calculate_profit_factor(trades: List[Dict[str, Any]]) -> float:
        """Calculate profit factor"""
        if not trades:
            return 0.0
            
        gross_profit = sum(trade.get('pnl', 0) for trade in trades if trade.get('pnl', 0) > 0)
        gross_loss = abs(sum(trade.get('pnl', 0) for trade in trades if trade.get('pnl', 0) < 0))
        
        if gross_loss == 0:
            return float('inf') if gross_profit > 0 else 0.0
            
        return gross_profit / gross_loss