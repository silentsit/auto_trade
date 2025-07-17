"""
INSTITUTIONAL TRADING UTILITIES
Enhanced position sizing, price handling, and risk management utilities
"""

import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple, List
import json
import math

logger = logging.getLogger(__name__)

# ===== PRICE AND MARKET DATA UTILITIES =====

# ===== TIMEFRAME UTILITIES =====
def normalize_timeframe(timeframe: str) -> str:
    """
    Normalize timeframe to OANDA format
    
    Args:
        timeframe: Input timeframe (e.g., '15', '1H', '4H', 'D')
        
    Returns:
        Normalized OANDA timeframe (e.g., 'M15', 'H1', 'H4', 'D')
    """
    timeframe = str(timeframe).strip().upper()
    
    # Handle numeric minutes
    if timeframe.isdigit():
        minutes = int(timeframe)
        if minutes < 60:
            return f"M{minutes}"
        elif minutes == 60:
            return "H1"
        elif minutes % 60 == 0:
            hours = minutes // 60
            return f"H{hours}"
    
    # Handle common formats
    timeframe_map = {
        '1M': 'M1', '5M': 'M5', '15M': 'M15', '30M': 'M30',
        '1H': 'H1', '2H': 'H2', '4H': 'H4', '8H': 'H8', '12H': 'H12',
        '1D': 'D', 'D': 'D', 'DAILY': 'D',
        'W': 'W', 'WEEKLY': 'W',
        'M': 'M', 'MONTHLY': 'M'
    }
    
    normalized = timeframe_map.get(timeframe, timeframe)
    logger.debug(f"Normalized timeframe {timeframe} -> {normalized}")
    return normalized


# ===== SIMULATED PRICE FUNCTION =====
def _get_simulated_price(symbol: str, side: str) -> float:
    """
    Generates a simulated market price for a given symbol.
    This is used as a fallback when live price fetching fails.
    """
    import random
    
    base_prices = {
        "EUR_USD": 1.0850, "GBP_USD": 1.2650, "USD_JPY": 157.20,
        "AUD_USD": 0.6650, "USD_CAD": 1.3710, "USD_CHF": 0.9150,
        "NZD_USD": 0.6150, "USD_THB": 36.75,
    }
    
    base = base_prices.get(symbol, 1.0)
    
    # Simulate a small random fluctuation and a bid-ask spread
    fluctuation = base * random.uniform(-0.0005, 0.0005)
    spread = base * 0.0002
    mid_price = base + fluctuation
    
    price = mid_price + spread / 2 if side.upper() == 'BUY' else mid_price - spread / 2
        
    logger.warning(f"[SIMULATED PRICE] Using fallback price for {symbol} ({side}): {price:.5f}")
    
    # Return price with appropriate precision for the currency pair
    return round(price, 3) if 'JPY' in symbol else round(price, 5)


# ===== INSTRUMENT SETTINGS AND LEVERAGE =====
def get_instrument_leverage(symbol: str) -> float:
    """Gets the max leverage for a symbol from its settings."""
    settings = get_instrument_settings(symbol)
    return settings.get("max_leverage", 50.0)


def get_instrument_settings(symbol: str) -> Dict[str, Any]:
    """
    Get instrument-specific trading settings
    
    Returns settings for minimum trade size, pip values, etc.
    """
    # Default settings
    default_settings = {
        "min_trade_size": 1,
        "max_trade_size": 100000000,
        "pip_value": 0.0001,
        "default_stop_pips": 50,
        "max_leverage": 50.0
    }
    
    # Instrument-specific overrides
    instrument_configs = {
        # Major EUR pairs
        "EUR_USD": {"pip_value": 0.0001, "default_stop_pips": 30, "min_trade_size": 1},
        "EUR_GBP": {"pip_value": 0.0001, "default_stop_pips": 40, "min_trade_size": 1},
        "EUR_JPY": {"pip_value": 0.01, "default_stop_pips": 50, "min_trade_size": 1},
        
        # Major GBP pairs  
        "GBP_USD": {"pip_value": 0.0001, "default_stop_pips": 40, "min_trade_size": 1},
        "GBP_JPY": {"pip_value": 0.01, "default_stop_pips": 60, "min_trade_size": 1},
        
        # JPY pairs (different pip structure)
        "USD_JPY": {"pip_value": 0.01, "default_stop_pips": 40, "min_trade_size": 1},
        "AUD_JPY": {"pip_value": 0.01, "default_stop_pips": 50, "min_trade_size": 1},
        "CAD_JPY": {"pip_value": 0.01, "default_stop_pips": 50, "min_trade_size": 1},
        "CHF_JPY": {"pip_value": 0.01, "default_stop_pips": 60, "min_trade_size": 1},
        "NZD_JPY": {"pip_value": 0.01, "default_stop_pips": 55, "min_trade_size": 1},
        
        # Other major pairs
        "USD_CHF": {"pip_value": 0.0001, "default_stop_pips": 35, "min_trade_size": 1},
        "AUD_USD": {"pip_value": 0.0001, "default_stop_pips": 45, "min_trade_size": 1},
        "USD_CAD": {"pip_value": 0.0001, "default_stop_pips": 40, "min_trade_size": 1},
        "NZD_USD": {"pip_value": 0.0001, "default_stop_pips": 50, "min_trade_size": 1},
        
        # Cross pairs
        "EUR_AUD": {"pip_value": 0.0001, "default_stop_pips": 60, "min_trade_size": 1},
        "EUR_CHF": {"pip_value": 0.0001, "default_stop_pips": 35, "min_trade_size": 1},
        "GBP_CHF": {"pip_value": 0.0001, "default_stop_pips": 50, "min_trade_size": 1},
        "GBP_AUD": {"pip_value": 0.0001, "default_stop_pips": 70, "min_trade_size": 1},
        "AUD_CHF": {"pip_value": 0.0001, "default_stop_pips": 55, "min_trade_size": 1},
        "CAD_CHF": {"pip_value": 0.0001, "default_stop_pips": 45, "min_trade_size": 1},
        "NZD_CHF": {"pip_value": 0.0001, "default_stop_pips": 60, "min_trade_size": 1},
        
        # Exotic pairs (higher spreads, wider stops)
        "USD_THB": {"pip_value": 0.0001, "default_stop_pips": 100, "min_trade_size": 1},
        "USD_ZAR": {"pip_value": 0.0001, "default_stop_pips": 150, "min_trade_size": 1},
        "USD_MXN": {"pip_value": 0.0001, "default_stop_pips": 200, "min_trade_size": 1},
        
        # Crypto (if supported by broker)
        "BTC_USD": {"pip_value": 1.0, "default_stop_pips": 500, "min_trade_size": 1, "max_leverage": 2.0},
        "ETH_USD": {"pip_value": 0.01, "default_stop_pips": 200, "min_trade_size": 1, "max_leverage": 2.0},
    }
    
    # Merge default with instrument-specific settings
    settings = default_settings.copy()
    if symbol in instrument_configs:
        settings.update(instrument_configs[symbol])
        
    return settings

# ===== TIME AND DATE UTILITIES =====

def parse_iso_datetime(iso_string: str) -> datetime:
    """
    Parse ISO datetime string to datetime object
    
    Handles various ISO format variations
    """
    try:
        # Remove timezone suffix variations and parse
        iso_clean = iso_string.replace('Z', '+00:00')
        
        # Try different parsing approaches
        try:
            return datetime.fromisoformat(iso_clean)
        except ValueError:
            # Fallback: manual parsing
            if '.' in iso_clean:
                # Has microseconds
                dt_part, tz_part = iso_clean.rsplit('+', 1) if '+' in iso_clean else (iso_clean.rsplit('-', 1)[0], None)
                dt = datetime.strptime(dt_part, "%Y-%m-%dT%H:%M:%S.%f")
            else:
                # No microseconds
                dt_part, tz_part = iso_clean.rsplit('+', 1) if '+' in iso_clean else (iso_clean.rsplit('-', 1)[0], None)
                dt = datetime.strptime(dt_part, "%Y-%m-%dT%H:%M:%S")
                
            # Add timezone info
            if tz_part:
                # Parse timezone offset
                if ':' in tz_part:
                    hours, minutes = map(int, tz_part.split(':'))
                else:
                    hours, minutes = int(tz_part[:2]), int(tz_part[2:]) if len(tz_part) > 2 else 0
                offset = timedelta(hours=hours, minutes=minutes)
                dt = dt.replace(tzinfo=timezone(offset))
            else:
                dt = dt.replace(tzinfo=timezone.utc)
                
            return dt
            
    except Exception as e:
        logger.error(f"Error parsing datetime '{iso_string}': {e}")
        # Return current time as fallback
        return datetime.now(timezone.utc)

def format_datetime_iso(dt: datetime) -> str:
    """Format datetime as ISO string"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace('+00:00', 'Z')

def is_market_hours(dt: Optional[datetime] = None) -> bool:
    """
    Check if current time is within market hours
    
    Forex market is open 24/5 (Sunday 5 PM ET to Friday 5 PM ET)
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
        
    # Convert to New York time for market hours calculation
    ny_tz = timezone(timedelta(hours=-5))  # EST (adjust for DST in production)
    ny_time = dt.astimezone(ny_tz)
    
    weekday = ny_time.weekday()
    hour = ny_time.hour
    
    # Market closed on weekends (Saturday and early Sunday)
    if weekday == 5:  # Saturday
        return False
    if weekday == 6 and hour < 17:  # Sunday before 5 PM
        return False
    if weekday == 4 and hour >= 17:  # Friday after 5 PM
        return False
        
    return True

# ===== VALIDATION AND ERROR HANDLING =====

def validate_trading_request(data: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate incoming trading request data
    
    Returns (is_valid, error_list)
    """
    errors = []
    
    # Required fields
    required_fields = ['symbol', 'action']
    for field in required_fields:
        if field not in data or not data[field]:
            errors.append(f"Missing required field: {field}")
    
    # Validate symbol format
    if 'symbol' in data:
        symbol = data['symbol']
        if not isinstance(symbol, str) or len(symbol) < 3:
            errors.append(f"Invalid symbol format: {symbol}")
    
    # Validate action
    if 'action' in data:
        action = data['action'].upper() if isinstance(data['action'], str) else ''
        valid_actions = ['BUY', 'SELL', 'CLOSE', 'EXIT']
        if action not in valid_actions:
            errors.append(f"Invalid action: {action}. Valid actions: {valid_actions}")
    
    # Validate risk percentage
    if 'risk_percent' in data:
        try:
            risk_pct = float(data['risk_percent'])
            if risk_pct <= 0 or risk_pct > 20:
                errors.append(f"Risk percentage must be between 0 and 20, got: {risk_pct}")
        except (ValueError, TypeError):
            errors.append(f"Invalid risk percentage: {data['risk_percent']}")
    
    # Validate accounts (if provided)
    if 'accounts' in data:
        accounts = data['accounts']
        if not isinstance(accounts, list) or len(accounts) == 0:
            errors.append("Accounts must be a non-empty list")
    
    return len(errors) == 0, errors

def safe_json_parse(json_string: str) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Safely parse JSON string with error handling
    
    Returns (parsed_data, error_message)
    """
    try:
        if not json_string or not json_string.strip():
            return None, "Empty JSON string"
            
        data = json.loads(json_string)
        return data, None
        
    except json.JSONDecodeError as e:
        return None, f"JSON decode error: {str(e)}"
    except Exception as e:
        return None, f"Unexpected error parsing JSON: {str(e)}"

# ===== PERFORMANCE AND MONITORING =====

class PerformanceTimer:
    """Context manager for timing operations"""
    
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.start_time = None
        
    def __enter__(self):
        self.start_time = datetime.now()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = datetime.now() - self.start_time
        duration_ms = duration.total_seconds() * 1000
        
        if duration_ms > 1000:  # Log slow operations
            logger.warning(f"⏱️ SLOW OPERATION: {self.operation_name} took {duration_ms:.1f}ms")
        else:
            logger.debug(f"⏱️ {self.operation_name} completed in {duration_ms:.1f}ms")

def log_trade_metrics(position_data: Dict[str, Any]):
    """Log important trade metrics for monitoring"""
    try:
        symbol = position_data.get('symbol', 'UNKNOWN')
        action = position_data.get('action', 'UNKNOWN')
        size = position_data.get('size', 0)
        entry_price = position_data.get('entry_price', 0)
        
        logger.info(f"📊 TRADE METRICS: {symbol} {action} Size={size} Entry={entry_price}")
        
        # Log to metrics file for analysis
        metrics_data = {
            "timestamp": format_datetime_iso(datetime.now(timezone.utc)),
            "symbol": symbol,
            "action": action,
            "size": size,
            "entry_price": entry_price,
            "position_id": position_data.get('position_id'),
        }
        
        # This could be enhanced to write to metrics database
        logger.debug(f"Trade metrics: {json.dumps(metrics_data)}")
        
    except Exception as e:
        logger.error(f"Error logging trade metrics: {e}")

# ===== EXPORT UTILITIES =====
__all__ = [
    'get_current_price',
    'format_symbol_for_oanda', 
    'calculate_position_size',
    'get_instrument_settings',
    'get_instrument_leverage',
    'round_position_size',
    'get_position_size_limits',
    'validate_trade_inputs',
    'parse_iso_datetime',
    'format_datetime_iso',
    'is_market_hours',
    'validate_trading_request',
    'safe_json_parse',
    'PerformanceTimer',
    'log_trade_metrics',
    'normalize_timeframe',
    '_get_simulated_price',
    'get_atr',
    'get_instrument_type',
    'get_atr_multiplier',
    'MarketDataUnavailableError'
]

# ===== MISSING FUNCTIONS FROM ROOT UTILS - ADDING TO FIX IMPORT ERRORS =====

# Error Classes
class MarketDataUnavailableError(Exception):
    """Custom exception for when market data cannot be fetched."""
    pass

# ===== POSITION SIZING AND RISK MANAGEMENT =====
def round_position_size(symbol: str, size: float) -> int:
    """Rounds the position size to the correct precision for the instrument (usually whole numbers for OANDA)."""
    return int(round(size, 0))

def get_position_size_limits(symbol: str) -> Tuple[int, int]:
    """Gets the min and max tradeable units for a symbol."""
    settings = get_instrument_settings(symbol)
    min_size = settings.get("min_trade_size", 1)
    max_size = settings.get("max_trade_size", 100_000_000)
    return min_size, max_size

def validate_trade_inputs(units: int, risk_percent: float, atr: float, stop_loss_distance: float, min_units: int, max_units: int) -> Tuple[bool, str]:
    """Performs final validation of all trade parameters before execution."""
    if units < min_units:
        return False, f"Calculated units ({units}) is below the minimum allowed ({min_units})."
    if units > max_units:
        return False, f"Calculated units ({units}) is above the maximum allowed ({max_units})."
    if risk_percent <= 0 or risk_percent > 20:
        return False, f"Invalid risk percentage: {risk_percent}"
    if atr is not None and atr <= 0:
        return False, "ATR value is zero or negative."
    if stop_loss_distance <= 0:
        return False, "Stop loss distance is zero or negative."
    return True, "Trade inputs are valid."

def calculate_position_size(
    symbol: str,
    entry_price: float,
    risk_percent: float,
    account_balance: float,
    leverage: float = 50.0,
    max_position_value: float = 100000.0
) -> Tuple[int, Dict[str, Any]]:
    """
    INSTITUTIONAL-GRADE POSITION SIZING with enhanced margin management
    """
    try:
        logger.info(f"[POSITION SIZING] {symbol}: risk={risk_percent}%, balance=${account_balance:.2f}, entry=${entry_price}")
        if risk_percent <= 0 or risk_percent > 20:
            return 0, {"error": "Invalid risk percentage"}
        if account_balance <= 0:
            return 0, {"error": "Invalid account balance"}
        if entry_price <= 0:
            return 0, {"error": "Invalid entry price"}
        risk_amount = account_balance * (risk_percent / 100.0)
        instrument_settings = get_instrument_settings(symbol)
        min_units, max_units = get_position_size_limits(symbol)
        pip_value = instrument_settings.get("pip_value", 0.0001)
        stop_loss_pips = instrument_settings.get("default_stop_pips", 50)
        stop_loss_distance = stop_loss_pips * pip_value
        raw_size = risk_amount / stop_loss_distance if stop_loss_distance > 0 else (risk_amount * leverage) / entry_price
        required_margin = (raw_size * entry_price) / leverage
        available_margin = account_balance * 0.60
        if required_margin > available_margin:
            safety_buffer = 0.90
            safe_margin = available_margin * safety_buffer
            raw_size = (safe_margin * leverage) / entry_price
            logger.warning(f"[MARGIN LIMIT] {symbol}: Position reduced to {raw_size:.2f} units")
        position_value = raw_size * entry_price
        if position_value > max_position_value:
            raw_size = max_position_value / entry_price
            logger.warning(f"[VALUE LIMIT] {symbol}: Position reduced to {raw_size:.2f} units")
        
        position_size = max(min_units, min(max_units, raw_size))
        final_position_size = round_position_size(symbol, position_size)

        if final_position_size < min_units:
            final_position_size = min_units

        final_margin = (final_position_size * entry_price) / leverage
        final_value = final_position_size * entry_price
        final_margin_pct = (final_margin / account_balance) * 100

        sizing_info = {
            "calculated_size": final_position_size, "entry_price": entry_price, "position_value": final_value,
            "required_margin": final_margin, "margin_utilization_pct": final_margin_pct, "risk_amount": risk_amount,
            "stop_loss_pips": stop_loss_pips, "pip_value": pip_value, "leverage": leverage
        }
        logger.info(f"[FINAL SIZING] {symbol}: Size={final_position_size}, Value=${final_value:.2f}, Margin=${final_margin:.2f} ({final_margin_pct:.1f}%)")
        return final_position_size, sizing_info
    except Exception as e:
        logger.error(f"Error calculating position size for {symbol}: {e}")
        return 0, {"error": str(e)}

def format_symbol_for_oanda(symbol: str) -> str:
    """
    Convert symbol to OANDA format
    """
    if "_" in symbol:
        return symbol
    symbol_mappings = {
        "EURUSD": "EUR_USD", "GBPUSD": "GBP_USD", "USDJPY": "USD_JPY", "USDCHF": "USD_CHF",
        "AUDUSD": "AUD_USD", "USDCAD": "USD_CAD", "NZDUSD": "NZD_USD", "GBPJPY": "GBP_JPY",
        "EURJPY": "EUR_JPY", "AUDJPY": "AUD_JPY", "EURGBP": "EUR_GBP", "EURAUD": "EUR_AUD",
        "EURCHF": "EUR_CHF", "GBPCHF": "GBP_CHF", "CADCHF": "CAD_CHF", "GBPAUD": "GBP_AUD",
        "AUDCHF": "AUD_CHF", "NZDCHF": "NZD_CHF", "NZDJPY": "NZD_JPY", "CADJPY": "CAD_JPY",
        "CHFJPY": "CHF_JPY", "BTCUSD": "BTC_USD", "ETHUSD": "ETH_USD", "USDTHB": "USD_THB",
        "USDZAR": "USD_ZAR", "USDMXN": "USD_MXN"
    }
    if symbol.upper() in symbol_mappings:
        return symbol_mappings[symbol.upper()]
    if len(symbol) == 6:
        base = symbol[:3].upper()
        quote = symbol[3:].upper()
        return f"{base}_{quote}"
    logger.warning(f"Could not format symbol {symbol} - using as-is")
    return symbol.upper()

async def get_current_price(symbol: str, side: str, oanda_service=None, fallback_enabled: bool = False) -> Optional[float]:
    """
    Enhanced price fetching with robust error handling
    """
    try:
        if not oanda_service:
            logger.error("OANDA service not available for price fetching")
            return None
        if not fallback_enabled:
            logger.debug(f"Fetching live price for {symbol} {side}")
            price = await oanda_service.get_current_price(symbol, side)
            if price is None:
                logger.error(f"Failed to get live price for {symbol} - no fallback enabled")
                return None
            logger.info(f"✅ Live price for {symbol} {side}: {price}")
            return price
        else:
            logger.warning("Simulated price fallback is disabled for live trading")
            return None
    except Exception as e:
        logger.error(f"Error getting current price for {symbol}: {e}")
        return None

# ===== ATR AND TECHNICAL ANALYSIS =====
async def get_atr(symbol: str, timeframe: str, period: int = 14) -> float:
    """
    Simplified ATR calculation with fallback values
    """
    try:
        # Try to import required libraries
        try:
            import pandas as pd
            import ta
        except ImportError as e:
            logger.warning(f"Required libraries not available for ATR calculation: {e}")
            # Fall back to default values
            return _get_default_atr(symbol, timeframe)
        
        # Normalize symbol
        symbol = format_symbol_for_oanda(symbol)
        
        # For now, return default ATR values until we can implement full calculation
        logger.info(f"[ATR] Using default ATR value for {symbol} {timeframe}")
        return _get_default_atr(symbol, timeframe)
        
    except Exception as e:
        logger.error(f"Error calculating ATR for {symbol}: {e}")
        return _get_default_atr(symbol, timeframe)

def _get_default_atr(symbol: str, timeframe: str) -> float:
    """Get default ATR values based on instrument type and timeframe"""
    instrument_type = get_instrument_type(symbol)
    
    default_atr_values = {
        "FOREX": {"M1": 0.0005, "M5": 0.0007, "M15": 0.0010, "M30": 0.0015, "H1": 0.0025, "H4": 0.0050, "D": 0.0100},
        "CRYPTO": {"M1": 0.0010, "M5": 0.0015, "M15": 0.0020, "M30": 0.0030, "H1": 0.0050, "H4": 0.0100, "D": 0.0200},
        "COMMODITY": {"M1": 0.05, "M5": 0.07, "M15": 0.10, "M30": 0.15, "H1": 0.25, "H4": 0.50, "D": 1.00},
        "INDICES": {"M1": 0.50, "M5": 0.70, "M15": 1.00, "M30": 1.50, "H1": 2.50, "H4": 5.00, "D": 10.00}
    }
    
    # Normalize timeframe
    normalized_tf = normalize_timeframe(timeframe)
    
    # Get default for instrument type
    type_defaults = default_atr_values.get(instrument_type, default_atr_values["FOREX"])
    return type_defaults.get(normalized_tf, type_defaults.get("H1", 0.0025))

def get_instrument_type(symbol: str) -> str:
    """
    Determine instrument type from symbol.
    Returns one of: 'FOREX', 'CRYPTO', 'COMMODITY', 'INDICES'.
    """
    try:
        if not symbol:
            logger.warning("Empty symbol provided, defaulting to FOREX")
            return "FOREX"
            
        inst = symbol.upper()
        
        # Define lists for identification
        crypto_list = ['BTC', 'ETH', 'XRP', 'LTC', 'BCH', 'DOT', 'ADA', 'SOL']
        commodity_list = ['XAU', 'XAG', 'XPT', 'XPD', 'WTI', 'BCO', 'NATGAS', 'OIL']
        index_list = ['SPX', 'NAS', 'US30', 'UK100', 'DE30', 'JP225', 'AUS200', 'DAX']

        # Check for underscore format (e.g., EUR_USD, BTC_USD)
        if '_' in inst:
            parts = inst.split('_')
            if len(parts) == 2:
                base, quote = parts
                
                # Check Crypto (Base only, e.g., BTC_USD)
                if base in crypto_list:
                    return "CRYPTO"
                    
                # Check Commodity (Base only, e.g., XAU_USD)
                if base in commodity_list:
                    return "COMMODITY"
                    
                # Check Index (Base only, e.g., SPX_USD)
                if base in index_list:
                    return "INDICES"
                    
                # Default to FOREX for currency pairs
                return "FOREX"
        
        # Check without underscore format
        for crypto in crypto_list:
            if crypto in inst:
                return "CRYPTO"
                
        for commodity in commodity_list:
            if commodity in inst:
                return "COMMODITY"
                
        for index in index_list:
            if index in inst:
                return "INDICES"
        
        # Default to FOREX
        return "FOREX"
        
    except Exception as e:
        logger.error(f"Error determining instrument type for {symbol}: {e}")
        return "FOREX"

def get_atr_multiplier(instrument_type: str, timeframe: str) -> float:
    """
    Get ATR multiplier based on instrument type and timeframe
    """
    try:
        # Default multipliers by instrument type and timeframe
        multipliers = {
            "FOREX": {"M1": 1.0, "M5": 1.2, "M15": 1.5, "M30": 1.8, "H1": 2.0, "H4": 2.5, "D": 3.0},
            "CRYPTO": {"M1": 1.5, "M5": 1.8, "M15": 2.0, "M30": 2.2, "H1": 2.5, "H4": 3.0, "D": 3.5},
            "COMMODITY": {"M1": 1.2, "M5": 1.5, "M15": 1.8, "M30": 2.0, "H1": 2.2, "H4": 2.8, "D": 3.2},
            "INDICES": {"M1": 1.0, "M5": 1.3, "M15": 1.6, "M30": 1.9, "H1": 2.1, "H4": 2.6, "D": 3.1}
        }
        
        # Normalize timeframe
        normalized_tf = normalize_timeframe(timeframe)
        
        # Get multiplier for instrument type and timeframe
        type_multipliers = multipliers.get(instrument_type, multipliers["FOREX"])
        return type_multipliers.get(normalized_tf, type_multipliers.get("H1", 2.0))
        
    except Exception as e:
        logger.error(f"Error getting ATR multiplier for {instrument_type} {timeframe}: {e}")
        return 2.0  # Default multiplier
