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
    'parse_iso_datetime',
    'format_datetime_iso',
    'is_market_hours',
    'validate_trading_request',
    'safe_json_parse',
    'PerformanceTimer',
    'log_trade_metrics',
    'normalize_timeframe',
    '_get_simulated_price'
]
