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
import random

from config import settings

logger = logging.getLogger(__name__)


# ===== LOGGER HELPER =====
def get_module_logger(module_name: str) -> logging.Logger:
    """Gets a logger instance for a specific module."""
    return logging.getLogger(module_name)

# ===== CONSTANTS & EXCEPTIONS =====
RETRY_DELAY = settings.oanda.retry_delay

class MarketDataUnavailableError(Exception):
    """Custom exception for when market data cannot be fetched."""
    pass

# FIX: Define all other missing utility functions with placeholder logic
def normalize_timeframe(timeframe: str) -> str:
    logger.debug(f"Normalizing timeframe: {timeframe}")
    return timeframe.upper()

def standardize_symbol(symbol: str) -> str:
    logger.debug(f"Standardizing symbol: {symbol}")
    return format_symbol_for_oanda(symbol)

def is_instrument_tradeable(symbol: str) -> bool:
    # Placeholder: assume all are tradeable for now
    return True

def get_instrument_type(symbol: str) -> str:
    # Placeholder: simple forex detection
    return "FOREX" if "_" in symbol else "UNKNOWN"
    
def get_atr_multiplier(symbol: str, default: float = 2.0) -> float:
    # Placeholder: could be dynamic based on volatility later
    return default

def get_trading_logger() -> logging.Logger:
    return logging.getLogger("trading_ops")

def calculate_notional_position_size(account_balance: float, risk_percent: float, entry_price: float, stop_loss_price: float) -> float:
    # Placeholder: simplified calculation
    risk_amount = account_balance * (risk_percent / 100)
    risk_per_unit = abs(entry_price - stop_loss_price)
    return risk_amount / risk_per_unit if risk_per_unit > 0 else 0

TV_FIELD_MAP = {
    "ticker": "symbol",
    "strategy.order.action": "action",
    "strategy.order.contracts": "size",
    "strategy.order.price": "entry_price",
    "strategy.order.comment": "comment",
    "time": "timestamp"
}

# ===== SIMULATED PRICE FUNCTION =====
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


# ===== PRICE AND MARKET DATA UTILITIES =====
async def get_current_price(symbol: str, side: str, oanda_service=None, fallback_enabled: bool = False) -> Optional[float]:
    try:
        if not oanda_service:
            logger.error("OANDA service not available for price fetching")
            return None
        if not fallback_enabled:
            price = await oanda_service.get_current_price(symbol, side)
            if price is None:
                return None
            return price
        else:
            return None
    except Exception as e:
        logger.error(f"Error getting current price for {symbol}: {e}")
        return None


def format_symbol_for_oanda(symbol: str) -> str:
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
        return f"{symbol[:3].upper()}_{symbol[3:].upper()}"
    return symbol.upper()


# ===== POSITION SIZING AND RISK MANAGEMENT =====
def get_instrument_leverage(symbol: str) -> float:
    settings = get_instrument_settings(symbol)
    return settings.get("max_leverage", 50.0)

def round_position_size(symbol: str, size: float) -> int:
    return int(round(size, 0))

def get_position_size_limits(symbol: str) -> Tuple[int, int]:
    settings = get_instrument_settings(symbol)
    min_size = settings.get("min_trade_size", 1)
    max_size = settings.get("max_trade_size", 100_000_000)
    return min_size, max_size

def validate_trade_inputs(units: int, risk_percent: float, atr: float, stop_loss_distance: float, min_units: int, max_units: int) -> Tuple[bool, str]:
    if units < min_units: return False, f"Units ({units}) below minimum ({min_units})."
    if units > max_units: return False, f"Units ({units}) above maximum ({max_units})."
    if not (0 < risk_percent <= 20): return False, f"Invalid risk percentage: {risk_percent}"
    if atr is not None and atr <= 0: return False, "ATR is zero or negative."
    if stop_loss_distance <= 0: return False, "Stop loss distance is zero or negative."
    return True, "Trade inputs are valid."


def calculate_position_size(symbol: str, entry_price: float, risk_percent: float, account_balance: float, leverage: float = 50.0, max_position_value: float = 100000.0) -> Tuple[int, Dict[str, Any]]:
    try:
        if not (0 < risk_percent <= 20): return 0, {"error": "Invalid risk percentage"}
        risk_amount = account_balance * (risk_percent / 100.0)
        instrument_settings = get_instrument_settings(symbol)
        stop_loss_pips = instrument_settings.get("default_stop_pips", 50)
        pip_value = instrument_settings.get("pip_value", 0.0001)
        stop_loss_distance = stop_loss_pips * pip_value
        raw_size = risk_amount / stop_loss_distance if stop_loss_distance > 0 else 0
        return int(raw_size), {}
    except Exception as e:
        logger.error(f"Error in calculate_position_size: {e}")
        return 0, {"error": str(e)}

def get_instrument_settings(symbol: str) -> Dict[str, Any]:
    default_settings = {
        "min_trade_size": 1, "max_trade_size": 100_000_000, "pip_value": 0.0001,
        "default_stop_pips": 50, "max_leverage": 50.0
    }
    instrument_configs = {
        "EUR_USD": {"default_stop_pips": 30}, "GBP_USD": {"default_stop_pips": 40},
        "USD_JPY": {"pip_value": 0.01, "default_stop_pips": 40},
    }
    settings = default_settings.copy()
    settings.update(instrument_configs.get(symbol, {}))
    return settings


# ===== TIME AND DATE UTILITIES =====
def parse_iso_datetime(iso_string: str) -> datetime:
    try:
        return datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)

# ... The rest of the functions in utils.py ...

# ===== EXPORT UTILITIES =====
__all__ = [
    'get_module_logger', 'RETRY_DELAY', 'MarketDataUnavailableError',
    'normalize_timeframe', 'standardize_symbol', 'is_instrument_tradeable',
    'get_instrument_type', 'get_atr_multiplier', 'get_trading_logger',
    'calculate_notional_position_size', 'TV_FIELD_MAP',
    'get_current_price', 'format_symbol_for_oanda', 'calculate_position_size',
    'get_instrument_settings', 'parse_iso_datetime', 'format_datetime_iso',
    'is_market_hours', 'validate_trading_request', 'safe_json_parse',
    'PerformanceTimer', 'log_trade_metrics', '_get_simulated_price',
    'get_instrument_leverage', 'round_position_size', 'get_position_size_limits',
    'validate_trade_inputs'
]
