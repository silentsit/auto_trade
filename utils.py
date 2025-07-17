"""
INSTITUTIONAL TRADING UTILITIES
Merged, refactored, and optimized for multi-asset, multi-strategy trading
"""

import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Tuple, List
import json
import math
import random

logger = logging.getLogger(__name__)

# ===== LOGGER HELPER =====
def get_module_logger(module_name: str) -> logging.Logger:
    """Gets a logger instance for a specific module."""
    return logging.getLogger(module_name)

# ===== CONSTANTS & EXCEPTIONS =====
RETRY_DELAY = 2  # Default retry delay (override with config if needed)

class MarketDataUnavailableError(Exception):
    """Custom exception for when market data cannot be fetched."""
    pass

# ===== TIMEFRAME UTILITIES =====
def normalize_timeframe(timeframe: str) -> str:
    """
    Normalize timeframe to OANDA format (e.g., '15' -> 'M15', '1H' -> 'H1')
    """
    timeframe = str(timeframe).strip().upper()
    if timeframe.isdigit():
        minutes = int(timeframe)
        if minutes < 60:
            return f"M{minutes}"
        elif minutes == 60:
            return "H1"
        elif minutes % 60 == 0:
            hours = minutes // 60
            return f"H{hours}"
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

# ===== SYMBOL UTILITIES =====
def format_symbol_for_oanda(symbol: str) -> str:
    """
    Convert symbol to OANDA format (e.g., 'EURUSD' -> 'EUR_USD')
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

def standardize_symbol(symbol: str) -> str:
    """
    Standardize symbol for internal use (uppercase, OANDA format)
    """
    return format_symbol_for_oanda(symbol.strip().upper())

# ===== INSTRUMENT SETTINGS AND LEVERAGE =====
def get_instrument_settings(symbol: str) -> Dict[str, Any]:
    """
    Get instrument-specific trading settings
    """
    default_settings = {
        "min_trade_size": 1, "max_trade_size": 100_000_000, "pip_value": 0.0001,
        "default_stop_pips": 50, "max_leverage": 50.0
    }
    instrument_configs = {
        "EUR_USD": {"pip_value": 0.0001, "default_stop_pips": 30},
        "GBP_USD": {"pip_value": 0.0001, "default_stop_pips": 40},
        "USD_JPY": {"pip_value": 0.01, "default_stop_pips": 40},
        "BTC_USD": {"pip_value": 1.0, "default_stop_pips": 500, "max_leverage": 2.0},
        "ETH_USD": {"pip_value": 0.01, "default_stop_pips": 200, "max_leverage": 2.0},
    }
    settings = default_settings.copy()
    symbol = format_symbol_for_oanda(symbol)
    if symbol in instrument_configs:
        settings.update(instrument_configs[symbol])
    return settings

def get_instrument_leverage(symbol: str) -> float:
    settings = get_instrument_settings(symbol)
    return settings.get("max_leverage", 50.0)

def get_position_size_limits(symbol: str) -> Tuple[int, int]:
    settings = get_instrument_settings(symbol)
    min_size = settings.get("min_trade_size", 1)
    max_size = settings.get("max_trade_size", 100_000_000)
    return min_size, max_size

def round_position_size(symbol: str, size: float) -> int:
    return int(round(size, 0))

def validate_trade_inputs(units: int, risk_percent: float, atr: float, stop_loss_distance: float, min_units: int, max_units: int) -> Tuple[bool, str]:
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

# ===== PRICE AND MARKET DATA UTILITIES =====
async def get_current_price(symbol: str, side: str, oanda_service=None, fallback_enabled: bool = False) -> Optional[float]:
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

def _get_simulated_price(symbol: str, side: str) -> float:
    base_prices = {
        "EUR_USD": 1.0850, "GBP_USD": 1.2650, "USD_JPY": 157.20,
        "AUD_USD": 0.6650, "USD_CAD": 1.3710, "USD_CHF": 0.9150,
        "NZD_USD": 0.6150, "USD_THB": 36.75,
    }
    base = base_prices.get(symbol, 1.0)
    fluctuation = base * random.uniform(-0.0005, 0.0005)
    spread = base * 0.0002
    mid_price = base + fluctuation
    price = mid_price + spread / 2 if side.upper() == 'BUY' else mid_price - spread / 2
    logger.warning(f"[SIMULATED PRICE] Using fallback price for {symbol} ({side}): {price:.5f}")
    return round(price, 3) if 'JPY' in symbol else round(price, 5)

# ===== ATR AND TECHNICAL ANALYSIS =====
def get_instrument_type(symbol: str) -> str:
    try:
        if not symbol:
            logger.warning("Empty symbol provided, defaulting to FOREX")
            return "FOREX"
        inst = symbol.upper()
        crypto_list = ['BTC', 'ETH', 'XRP', 'LTC', 'BCH', 'DOT', 'ADA', 'SOL']
        commodity_list = ['XAU', 'XAG', 'XPT', 'XPD', 'WTI', 'BCO', 'NATGAS', 'OIL']
        index_list = ['SPX', 'NAS', 'US30', 'UK100', 'DE30', 'JP225', 'AUS200', 'DAX']
        if '_' in inst:
            parts = inst.split('_')
            if len(parts) == 2:
                base, quote = parts
                if base in crypto_list:
                    return "CRYPTO"
                if base in commodity_list:
                    return "COMMODITY"
                if base in index_list:
                    return "INDICES"
                return "FOREX"
        for crypto in crypto_list:
            if crypto in inst:
                return "CRYPTO"
        for commodity in commodity_list:
            if commodity in inst:
                return "COMMODITY"
        for index in index_list:
            if index in inst:
                return "INDICES"
        return "FOREX"
    except Exception as e:
        logger.error(f"Error determining instrument type for {symbol}: {e}")
        return "FOREX"

def get_atr_multiplier(instrument_type: str, timeframe: str) -> float:
    try:
        multipliers = {
            "FOREX": {"M1": 1.0, "M5": 1.2, "M15": 1.5, "M30": 1.8, "H1": 2.0, "H4": 2.5, "D": 3.0},
            "CRYPTO": {"M1": 1.5, "M5": 1.8, "M15": 2.0, "M30": 2.2, "H1": 2.5, "H4": 3.0, "D": 3.5},
            "COMMODITY": {"M1": 1.2, "M5": 1.5, "M15": 1.8, "M30": 2.0, "H1": 2.2, "H4": 2.8, "D": 3.2},
            "INDICES": {"M1": 1.0, "M5": 1.3, "M15": 1.6, "M30": 1.9, "H1": 2.1, "H4": 2.6, "D": 3.1}
        }
        normalized_tf = normalize_timeframe(timeframe)
        type_multipliers = multipliers.get(instrument_type, multipliers["FOREX"])
        return type_multipliers.get(normalized_tf, type_multipliers.get("H1", 2.0))
    except Exception as e:
        logger.error(f"Error getting ATR multiplier for {instrument_type} {timeframe}: {e}")
        return 2.0

async def get_atr(symbol: str, timeframe: str, period: int = 14) -> float:
    try:
        try:
            import pandas as pd
            import ta
        except ImportError as e:
            logger.warning(f"Required libraries not available for ATR calculation: {e}")
            return _get_default_atr(symbol, timeframe)
        symbol = format_symbol_for_oanda(symbol)
        logger.info(f"[ATR] Using default ATR value for {symbol} {timeframe}")
        return _get_default_atr(symbol, timeframe)
    except Exception as e:
        logger.error(f"Error calculating ATR for {symbol}: {e}")
        return _get_default_atr(symbol, timeframe)

def _get_default_atr(symbol: str, timeframe: str) -> float:
    instrument_type = get_instrument_type(symbol)
    default_atr_values = {
        "FOREX": {"M1": 0.0005, "M5": 0.0007, "M15": 0.0010, "M30": 0.0015, "H1": 0.0025, "H4": 0.0050, "D": 0.0100},
        "CRYPTO": {"M1": 0.0010, "M5": 0.0015, "M15": 0.0020, "M30": 0.0030, "H1": 0.0050, "H4": 0.0100, "D": 0.0200},
        "COMMODITY": {"M1": 0.05, "M5": 0.07, "M15": 0.10, "M30": 0.15, "H1": 0.25, "H4": 0.50, "D": 1.00},
        "INDICES": {"M1": 0.50, "M5": 0.70, "M15": 1.00, "M30": 1.50, "H1": 2.50, "H4": 5.00, "D": 10.00}
    }
    normalized_tf = normalize_timeframe(timeframe)
    type_defaults = default_atr_values.get(instrument_type, default_atr_values["FOREX"])
    return type_defaults.get(normalized_tf, type_defaults.get("H1", 0.0025))

# ===== TIME AND DATE UTILITIES =====
def parse_iso_datetime(iso_string: str) -> datetime:
    try:
        return datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        logger.error(f"Error parsing datetime '{iso_string}'")
        return datetime.now(timezone.utc)

def format_datetime_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat().replace('+00:00', 'Z')

def is_market_hours(dt: Optional[datetime] = None) -> bool:
    if dt is None:
        dt = datetime.now(timezone.utc)
    ny_tz = timezone(timedelta(hours=-5))
    ny_time = dt.astimezone(ny_tz)
    weekday = ny_time.weekday()
    hour = ny_time.hour
    if weekday == 5 or (weekday == 6 and hour < 17) or (weekday == 4 and hour >= 17):
        return False
    return True

# ===== VALIDATION AND ERROR HANDLING =====
def validate_trading_request(data: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errors = []
    required_fields = ['symbol', 'action']
    for field in required_fields:
        if field not in data or not data[field]:
            errors.append(f"Missing required field: {field}")
    if 'symbol' in data and (not isinstance(data['symbol'], str) or len(data['symbol']) < 3):
        errors.append(f"Invalid symbol format: {data['symbol']}")
    if 'action' in data and (data.get('action', '').upper() not in ['BUY', 'SELL', 'CLOSE', 'EXIT']):
        errors.append(f"Invalid action: {data['action']}")
    return len(errors) == 0, errors

def safe_json_parse(json_string: str) -> Tuple[Optional[Dict], Optional[str]]:
    try:
        if not json_string or not json_string.strip():
            return None, "Empty JSON string"
        return json.loads(json_string), None
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
        duration_ms = (datetime.now() - self.start_time).total_seconds() * 1000
        level = "warning" if duration_ms > 1000 else "debug"
        getattr(logger, level)(f"⏱️ {'SLOW ' if level == 'warning' else ''}OPERATION: {self.operation_name} took {duration_ms:.1f}ms")

def log_trade_metrics(position_data: Dict[str, Any]):
    try:
        metrics = {
            "timestamp": format_datetime_iso(datetime.now(timezone.utc)),
            "symbol": position_data.get('symbol', 'UNKNOWN'),
            "action": position_data.get('action', 'UNKNOWN'),
            "size": position_data.get('size', 0),
            "entry_price": position_data.get('entry_price', 0),
            "position_id": position_data.get('position_id'),
        }
        logger.info(f"📊 TRADE METRICS: {json.dumps(metrics)}")
    except Exception as e:
        logger.error(f"Error logging trade metrics: {e}")

# ===== EXPORT UTILITIES =====
__all__ = [
    'get_current_price',
    'format_symbol_for_oanda',
    'standardize_symbol',
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
    'MarketDataUnavailableError',
    'RETRY_DELAY',
    'get_module_logger'
]
