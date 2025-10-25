"""
UTILITIES MODULE
Common utility functions for the trading bot
"""

import logging
import requests
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

async def calculate_position_size_async(account_balance: float, risk_percent: float, 
                          stop_loss_pips: float, symbol: str, current_price: float = None, oanda_service=None) -> float:
    """
    ASYNC version: Calculate position size based on risk management using batched price fetching.
    
    Args:
        account_balance: Account balance in USD
        risk_percent: Risk percentage (e.g., 1.0 for 1%)
        stop_loss_pips: Stop loss distance in pips
        symbol: Trading pair (e.g., 'EUR_USD')
        current_price: Current market price (needed for accurate pip value calculation)
        oanda_service: Optional OandaService instance for batched async price fetching
    
    Returns:
        Position size in units
    """
    try:
        # Get pip value for the symbol (async, batched price fetch)
        pip_value = await get_pip_value_async(symbol, current_price, oanda_service)
        
        # Calculate risk amount
        risk_amount = account_balance * (risk_percent / 100)
        
        # Calculate position size
        position_size = risk_amount / (stop_loss_pips * pip_value)
        
        # Apply limits
        min_size, max_size = get_position_size_limits(symbol)
        position_size = max(min_size, min(max_size, position_size))
        
        # Enhanced logging for debugging
        logger.info(f"🎯 POSITION SIZING DEBUG (async) for {symbol}:")
        logger.info(f"   Account Balance: ${account_balance:,.2f}")
        logger.info(f"   Risk Percent: {risk_percent:.2f}%")
        logger.info(f"   Risk Amount: ${risk_amount:,.2f}")
        logger.info(f"   Stop Loss Pips: {stop_loss_pips:.2f}")
        logger.info(f"   Pip Value: ${pip_value:.6f}")
        logger.info(f"   Calculated Size: {position_size:.2f} units")
        logger.info(f"   Min/Max Limits: {min_size}/{max_size}")
        
        # Round to appropriate precision
        final_size = round_position_size(position_size, symbol)
        logger.info(f"   Final Position Size: {final_size} units")
        
        return final_size
        
    except Exception as e:
        logger.error(f"Error calculating position size (async): {e}")
        return 1.0  # Return minimum forex size on error

def calculate_position_size(account_balance: float, risk_percent: float, 
                          stop_loss_pips: float, symbol: str, current_price: float = None) -> float:
    """
    LEGACY SYNC version: Calculate position size based on risk management.
    
    NOTE: This is now a fallback for non-async contexts. Prefer calculate_position_size_async() for better performance.
    
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
        
        # Enhanced logging for debugging
        logger.info(f"🎯 POSITION SIZING DEBUG for {symbol}:")
        logger.info(f"   Account Balance: ${account_balance:,.2f}")
        logger.info(f"   Risk Percent: {risk_percent:.2f}%")
        logger.info(f"   Risk Amount: ${risk_amount:,.2f}")
        logger.info(f"   Stop Loss Pips: {stop_loss_pips:.2f}")
        logger.info(f"   Pip Value: ${pip_value:.6f}")
        logger.info(f"   Calculated Size: {position_size:.2f} units")
        logger.info(f"   Min/Max Limits: {min_size}/{max_size}")
        
        # Round to appropriate precision
        final_size = round_position_size(position_size, symbol)
        logger.info(f"   Final Position Size: {final_size} units")
        
        return final_size
        
    except Exception as e:
        logger.error(f"Error calculating position size: {e}")
        return 1.0  # Return minimum forex size on error

async def get_pip_value_async(symbol: str, current_price: float = None, oanda_service=None) -> float:
    """
    ASYNC version: Get pip value for a symbol in USD per unit using batched price fetching.
    
    For most pairs, pip value depends on which currency is the quote currency:
    - XXX_USD pairs: 1 pip = 0.0001 USD per unit
    - USD_XXX pairs: 1 pip = 0.0001 / current_price USD per unit
    - JPY pairs: Use 0.01 instead of 0.0001
    - Cross pairs: Calculate based on live USD legs via async batch fetch
    
    Args:
        symbol: Trading pair (e.g., 'EUR_USD', 'USD_CHF', 'GBP_NZD')
        current_price: Current exchange rate (needed for USD base currency pairs and cross pairs)
        oanda_service: Optional OandaService instance for batched async price fetching
    
    Returns:
        Pip value in USD per unit
    """
    symbol_clean = symbol.replace('_', '')

    # --- Helper: async fetch mid price via oanda_service batched path ---
    async def _fetch_mid_price_async(instrument: str) -> Optional[float]:
        if not oanda_service:
            return None
        try:
            prices = await oanda_service.get_current_prices([instrument])
            if instrument not in prices:
                return None
            bid = prices[instrument].get("bid")
            ask = prices[instrument].get("ask")
            if bid is None or ask is None:
                return None
            return (float(bid) + float(ask)) / 2.0
        except Exception:
            return None

    # --- Helper: USD per 1 unit of a given currency code (async) ---
    async def _usd_per_currency_async(ccy: str, base_ccy_for_cross: Optional[str] = None, cross_px: Optional[float] = None) -> Optional[float]:
        # Try CCY_USD directly (USD per CCY)
        direct = await _fetch_mid_price_async(f"{ccy}_USD")
        if direct and direct > 0:
            return direct
        # Try USD_CCY (CCY per USD) and invert
        inverse = await _fetch_mid_price_async(f"USD_{ccy}")
        if inverse and inverse > 0:
            return 1.0 / inverse
        # Use the cross relationship with the base currency if available
        if base_ccy_for_cross and cross_px and cross_px > 0:
            base = base_ccy_for_cross
            base_usd = await _fetch_mid_price_async(f"{base}_USD")
            if base_usd and base_usd > 0:
                return base_usd / cross_px
            usd_base = await _fetch_mid_price_async(f"USD_{base}")
            if usd_base and usd_base > 0:
                return (1.0 / usd_base) / cross_px
        return None
    
    # JPY pairs use 0.01 as pip size (2 decimal places)
    if 'JPY' in symbol:
        pip_size = 0.01
        if symbol_clean.startswith('USD'):
            # USD_JPY: pip value = 0.01 / price (USD per pip per unit)
            if current_price:
                return pip_size / current_price
            return 0.0001  # Fallback approximation
        elif symbol_clean.endswith('USD'):
            # JPY_USD is not standard; treat as XXX_USD general case if ever seen
            return pip_size
        else:
            # A/JPY cross: pip USD value = pip_size * (USD per JPY)
            base = symbol_clean[:3]
            usd_per_jpy = await _usd_per_currency_async('JPY', base_ccy_for_cross=base, cross_px=current_price)
            if usd_per_jpy and usd_per_jpy > 0:
                return pip_size * usd_per_jpy
            # Fallback conservative
            return 0.0001
    else:
        pip_size = 0.0001
        # USD as base
        if symbol_clean.startswith('USD'):
            if current_price:
                return pip_size / current_price
            return 0.0001
        # USD as quote
        if symbol_clean.endswith('USD'):
            return pip_size
        # Cross pair A/B: pip USD value = pip_size * (USD per B)
        base = symbol_clean[:3]
        quote = symbol_clean[3:]
        usd_per_quote = await _usd_per_currency_async(quote, base_ccy_for_cross=base, cross_px=current_price)
        if usd_per_quote and usd_per_quote > 0:
            return pip_size * usd_per_quote
        # Final fallback: conservative estimate
        return pip_size * 0.8

def get_pip_value(symbol: str, current_price: float = None) -> float:
    """
    LEGACY SYNC version: Get pip value for a symbol in USD per unit.
    
    NOTE: This is now a fallback for non-async contexts. Prefer get_pip_value_async() for better performance.
    
    For most pairs, pip value depends on which currency is the quote currency:
    - XXX_USD pairs: 1 pip = 0.0001 USD per unit
    - USD_XXX pairs: 1 pip = 0.0001 / current_price USD per unit
    - JPY pairs: Use 0.01 instead of 0.0001
    - Cross pairs: Calculate based on live USD legs for perfect accuracy
    
    Args:
        symbol: Trading pair (e.g., 'EUR_USD', 'USD_CHF', 'GBP_NZD')
        current_price: Current exchange rate (needed for USD base currency pairs and cross pairs)
    
    Returns:
        Pip value in USD per unit
    """
    symbol_clean = symbol.replace('_', '')

    # --- Helper: fetch mid price for an instrument from OANDA pricing API (SYNC) ---
    def _fetch_mid_price(instrument: str) -> Optional[float]:
        try:
            from config import settings
            base_url = settings.get_oanda_base_url()
            account_id = settings.oanda.account_id
            token = settings.oanda.access_token
            if not base_url or not account_id or not token:
                return None
            url = f"{base_url}/v3/accounts/{account_id}/pricing"
            headers = {"Authorization": f"Bearer {token}"}
            params = {"instruments": instrument}
            resp = requests.get(url, headers=headers, params=params, timeout=10)
            if resp.status_code != 200:
                return None
            data = resp.json() if resp.content else {}
            prices = data.get("prices", []) if isinstance(data, dict) else []
            if not prices:
                return None
            p = prices[0]
            # Prefer closeoutBid/Ask; fallback to bids/asks arrays
            bid = p.get("closeoutBid") or (p.get("bids")[0].get("price") if p.get("bids") else None)
            ask = p.get("closeoutAsk") or (p.get("asks")[0].get("price") if p.get("asks") else None)
            if bid is None or ask is None:
                return None
            return (float(bid) + float(ask)) / 2.0
        except Exception:
            return None

    # --- Helper: USD per 1 unit of a given currency code ---
    def _usd_per_currency(ccy: str, base_ccy_for_cross: Optional[str] = None, cross_px: Optional[float] = None) -> Optional[float]:
        # Try CCY_USD directly (USD per CCY)
        direct = _fetch_mid_price(f"{ccy}_USD")
        if direct and direct > 0:
            return direct
        # Try USD_CCY (CCY per USD) and invert
        inverse = _fetch_mid_price(f"USD_{ccy}")
        if inverse and inverse > 0:
            return 1.0 / inverse
        # Use the cross relationship with the base currency if available:
        # USD/CCY = (USD/Base) / (CCY/Base) = (Base_USD) / (Base_CCY)
        if base_ccy_for_cross and cross_px and cross_px > 0:
            base = base_ccy_for_cross
            base_usd = _fetch_mid_price(f"{base}_USD")
            if base_usd and base_usd > 0:
                return base_usd / cross_px
            usd_base = _fetch_mid_price(f"USD_{base}")
            if usd_base and usd_base > 0:
                return (1.0 / usd_base) / cross_px
        return None
    
    # JPY pairs use 0.01 as pip size (2 decimal places)
    if 'JPY' in symbol:
        pip_size = 0.01
        if symbol_clean.startswith('USD'):
            # USD_JPY: pip value = 0.01 / price (USD per pip per unit)
            if current_price:
                return pip_size / current_price
            return 0.0001  # Fallback approximation
        elif symbol_clean.endswith('USD'):
            # JPY_USD is not standard; treat as XXX_USD general case if ever seen
            return pip_size
        else:
            # A/JPY cross: pip USD value = pip_size * (USD per JPY)
            base = symbol_clean[:3]
            usd_per_jpy = _usd_per_currency('JPY', base_ccy_for_cross=base, cross_px=current_price)
            if usd_per_jpy and usd_per_jpy > 0:
                return pip_size * usd_per_jpy
            # Fallback conservative
            return 0.0001
    else:
        pip_size = 0.0001
        # USD as base
        if symbol_clean.startswith('USD'):
            if current_price:
                return pip_size / current_price
            return 0.0001
        # USD as quote
        if symbol_clean.endswith('USD'):
            return pip_size
        # Cross pair A/B: pip USD value = pip_size * (USD per B)
        base = symbol_clean[:3]
        quote = symbol_clean[3:]
        usd_per_quote = _usd_per_currency(quote, base_ccy_for_cross=base, cross_px=current_price)
        if usd_per_quote and usd_per_quote > 0:
            return pip_size * usd_per_quote
        # Final fallback: conservative estimate
        return pip_size * 0.8

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