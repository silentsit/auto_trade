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
import numpy as np

def format_crypto_symbol_for_oanda(symbol: str) -> str:
    """Format crypto symbols for OANDA API compatibility"""
    # Common crypto symbol mappings
    crypto_mappings = {
        'BTCUSD': 'BTC_USD',
        'BTC/USD': 'BTC_USD', 
        'ETHUSD': 'ETH_USD',
        'ETH/USD': 'ETH_USD',
        'LTCUSD': 'LTC_USD',
        'LTC/USD': 'LTC_USD',
        'XRPUSD': 'XRP_USD',
        'XRP/USD': 'XRP_USD',
        'BCHUSD': 'BCH_USD',
        'BCH/USD': 'BCH_USD'
    }
    
    symbol_upper = symbol.upper()
    
    # Direct mapping
    if symbol_upper in crypto_mappings:
        return crypto_mappings[symbol_upper]
    
    # Pattern-based formatting
    if '/' in symbol:
        base, quote = symbol.split('/')
        return f"{base.upper()}_{quote.upper()}"
    elif '_' not in symbol and 'USD' in symbol:
        # Handle BTCUSD format
        base = symbol.replace('USD', '').upper()
        return f"{base}_USD"
    
    return symbol

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

def format_crypto_symbol_for_oanda(symbol: str) -> str:
    """Format crypto symbols for OANDA API compatibility"""
    # Common crypto symbol mappings
    crypto_mappings = {
        'BTCUSD': 'BTC_USD',
        'BTC/USD': 'BTC_USD', 
        'ETHUSD': 'ETH_USD',
        'ETH/USD': 'ETH_USD',
        'LTCUSD': 'LTC_USD',
        'LTC/USD': 'LTC_USD',
        'XRPUSD': 'XRP_USD',
        'XRP/USD': 'XRP_USD',
        'BCHUSD': 'BCH_USD',
        'BCH/USD': 'BCH_USD'
    }
    
    symbol_upper = symbol.upper()
    
    # Direct mapping
    if symbol_upper in crypto_mappings:
        return crypto_mappings[symbol_upper]
    
    # Pattern-based formatting
    if '/' in symbol:
        base, quote = symbol.split('/')
        return f"{base.upper()}_{quote.upper()}"
    elif '_' not in symbol and 'USD' in symbol:
        # Handle BTCUSD format
        base = symbol.replace('USD', '').upper()
        return f"{base}_USD"
    
    return symbol

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
        "max_leverage": 20.0  # MAS Singapore retail FX leverage
    }
    instrument_configs = {
        # FX pairs (20:1)
        "EUR_USD": {"pip_value": 0.0001, "max_leverage": 20.0},
        "GBP_USD": {"pip_value": 0.0001, "max_leverage": 20.0},
        "USD_JPY": {"pip_value": 0.01,  "max_leverage": 20.0},
        "AUD_USD": {"pip_value": 0.0001, "max_leverage": 20.0},
        "USD_CAD": {"pip_value": 0.0001, "max_leverage": 20.0},
        "USD_CHF": {"pip_value": 0.0001, "max_leverage": 20.0},
        "NZD_USD": {"pip_value": 0.0001, "max_leverage": 20.0},
        # Add more FX pairs as needed
        # Crypto pairs (2:1)
        "BTC_USD": {"pip_value": 1.0,   "max_leverage": 2.0},
        "ETH_USD": {"pip_value": 0.01,  "max_leverage": 2.0},
        "LTC_USD": {"pip_value": 0.01,  "max_leverage": 2.0},
        "BCH_USD": {"pip_value": 0.01,  "max_leverage": 2.0},
        # Add more crypto pairs as needed
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

async def calculate_position_size(
    symbol: str,
    entry_price: float,
    risk_percent: float,
    account_balance: float,
    leverage: float = 50.0,
    max_position_value: float = 100000.0,
    stop_loss_price: Optional[float] = None,
    timeframe: str = "H1"
) -> Tuple[int, Dict[str, Any]]:
    try:
        logger.info(f"[POSITION SIZING] {symbol}: risk={risk_percent}%, balance=${account_balance:.2f}, entry=${entry_price}")
        if risk_percent <= 0 or risk_percent > 20:
            return 0, {"error": "Invalid risk percentage"}
        if account_balance <= 0:
            return 0, {"error": "Invalid account balance"}
        if entry_price <= 0:
            return 0, {"error": "Invalid entry price"}
        
        risk_percent = float(risk_percent)
        # INSTITUTIONAL FIX: Use proper risk-based position sizing
        risk_amount = account_balance * (risk_percent / 100.0)
        min_units, max_units = get_position_size_limits(symbol)
        
        # Calculate stop loss distance for risk-based sizing
        stop_loss_distance = None
        if stop_loss_price is not None:
            stop_loss_distance = abs(entry_price - stop_loss_price)
            logger.info(f"[RISK-BASED SIZING] {symbol}: Using provided stop_loss={stop_loss_price}, distance={stop_loss_distance}")
        
        # INSTITUTIONAL FIX: Use hybrid sizing approach for better margin utilization
        if stop_loss_distance and stop_loss_distance > 0:
            # Calculate both risk-based and percentage-based sizes
            risk_based_size = risk_amount / stop_loss_distance
            
            # Calculate percentage-based size for better margin utilization
            target_position_value = account_balance * (risk_percent / 100.0) * leverage
            percentage_based_size = target_position_value / entry_price
            
            # Use the larger of the two (better margin utilization)
            raw_size = max(risk_based_size, percentage_based_size)
            
            logger.info(f"[HYBRID SIZING] {symbol}: Risk-based={risk_based_size:,.0f}, Percentage-based={percentage_based_size:,.0f}, Using={raw_size:,.0f}")
        else:
            # Fallback: Use percentage-based sizing with leverage
            target_position_value = account_balance * (risk_percent / 100.0) * leverage
            raw_size = target_position_value / entry_price
            logger.info(f"[PERCENTAGE-BASED] {symbol}: Target value=${target_position_value:.2f}, Size={raw_size:.2f}")
        
        # INSTITUTIONAL FIX: Improve margin utilization (use configurable percentage)
        required_margin = (raw_size * entry_price) / leverage
        
        # Get margin utilization percentage from config
        try:
            from config import settings
            margin_utilization_pct = float(getattr(settings.trading, 'margin_utilization_percentage', 85.0))
        except:
            margin_utilization_pct = 85.0  # Fallback default

        # Ensure leverage is float
        leverage = float(leverage)

        available_margin = account_balance * (margin_utilization_pct / 100.0)

        # Ensure min_units and max_units are int
        min_units = int(min_units)
        max_units = int(max_units)
        
        if required_margin > available_margin:
            # Scale down position to fit within available margin
            safety_buffer = 0.95  # Use 95% of available margin
            safe_margin = available_margin * safety_buffer
            raw_size = (safe_margin * leverage) / entry_price
            logger.info(f"[MARGIN LIMIT] {symbol}: Position scaled to {raw_size:.2f} units (${safe_margin:.2f} margin)")
        
        # Apply position value limits
        position_value = raw_size * entry_price
        if position_value > max_position_value:
            raw_size = max_position_value / entry_price
            logger.info(f"[VALUE LIMIT] {symbol}: Position reduced to {raw_size:.2f} units")
        
        # INSTITUTIONAL FIX: Ensure reasonable minimum position size
        min_reasonable_size = max(min_units, 1000)  # At least 1000 units for forex
        
        if raw_size < min_reasonable_size:
            raw_size = min_reasonable_size
            logger.info(f"[MIN SIZE] {symbol}: Adjusted to minimum size of {min_reasonable_size:.0f} units")
        
        position_size = max(min_units, min(max_units, raw_size))
        final_position_size = round_position_size(symbol, position_size)
        
        # Final safety check
        if final_position_size < min_units:
            final_position_size = min_units
            logger.warning(f"[FINAL MIN] {symbol}: Using broker minimum of {min_units} units")
        
        final_margin = (final_position_size * entry_price) / leverage
        final_value = final_position_size * entry_price
        final_margin_pct = (final_margin / account_balance) * 100
        
        sizing_info = {
            "calculated_size": final_position_size, 
            "entry_price": entry_price, 
            "position_value": final_value,
            "required_margin": final_margin, 
            "margin_utilization_pct": final_margin_pct, 
            "risk_amount": risk_amount,
            "stop_loss_distance": stop_loss_distance, 
            "actual_risk": final_position_size * (stop_loss_distance if stop_loss_distance else 0), 
            "leverage": leverage
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
    logger.info(f"[SIMULATED PRICE] Using fallback price for {symbol} ({side}): {price:.5f}")
    return round(price, 3) if 'JPY' in symbol else round(price, 5)

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

# ===== MISSING FUNCTION IMPLEMENTATIONS =====

# TradingView field mapping for alert processing
TV_FIELD_MAP = {
    "ticker": "symbol",
    "action": "action", 
    "contracts": "size",
    "price": "price",
    "time": "timestamp",
    "timeframe": "timeframe",
    "strategy": "strategy",
    "percentage": "risk_percent"  # Map JSON percentage to internal risk_percent field
}

async def get_atr(symbol: str, timeframe: str, period: int = 14, oanda_service=None) -> Optional[float]:
    """
    Get Average True Range for a symbol by fetching historical data.
    This is the async version that takes symbol/timeframe parameters.
    """
    try:
        from technical_analysis import get_atr as sync_get_atr
        
        # Try to get oanda_service from parameter first, then try global scope
        if oanda_service is None:
            try:
                # Import main module to access global oanda_service
                import main
                oanda_service = main.oanda_service
            except (ImportError, AttributeError):
                # Fallback: try to import from current context
                try:
                    from __main__ import oanda_service as global_oanda_service
                    oanda_service = global_oanda_service
                except ImportError:
                    logger.warning(f"No OANDA service available for ATR calculation of {symbol}")
                    return None
        
        if not oanda_service:
            logger.warning(f"No OANDA service provided for ATR calculation of {symbol}")
            return None
            
        # Fetch historical data
        df = await oanda_service.get_historical_data(
            symbol=symbol, 
            granularity=normalize_timeframe(timeframe),
            count=max(50, period + 10)  # Ensure enough data for calculation
        )
        
        if df is None or df.empty:
            logger.error(f"No historical data available for ATR calculation: {symbol}")
            return None
            
        # Use the sync version from technical_analysis
        atr_value = sync_get_atr(df, period)
        
        if atr_value is None or atr_value <= 0:
            logger.warning(f"Invalid ATR calculated for {symbol}: {atr_value}")
            return None
            
        logger.debug(f"ATR for {symbol} ({timeframe}): {atr_value:.5f}")
        return atr_value
        
    except Exception as e:
        logger.error(f"Error calculating ATR for {symbol}: {e}")
        return None

def get_instrument_type(symbol: str) -> str:
    """
    Determine the instrument type (forex, crypto, etc.) from symbol.
    """
    symbol = symbol.upper().replace("_", "")
    
    # Crypto patterns
    crypto_patterns = ["BTC", "ETH", "LTC", "XRP", "ADA", "DOT", "DOGE", "SOL", "MATIC", "AVAX"]
    if any(crypto in symbol for crypto in crypto_patterns):
        return "crypto"
    
    # Forex major pairs
    forex_majors = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD"]
    if symbol in forex_majors:
        return "forex_major"
    
    # Forex crosses and minors
    forex_currencies = ["EUR", "GBP", "USD", "JPY", "CHF", "AUD", "CAD", "NZD", "THB", "ZAR", "MXN"]
    if len(symbol) == 6 and symbol[:3] in forex_currencies and symbol[3:] in forex_currencies:
        return "forex_minor"
    
    # Default to forex
    return "forex"

def get_atr_multiplier(instrument_type: str, timeframe: str) -> float:
    """
    Get ATR multiplier based on instrument type and timeframe.
    Used for stop loss and take profit calculations.
    """
    timeframe = normalize_timeframe(timeframe)
    
    # Base multipliers by instrument type
    base_multipliers = {
        "forex_major": 2.0,
        "forex_minor": 2.5,
        "crypto": 1.5,
        "index": 2.0,
        "commodity": 2.5
    }
    
    # Timeframe adjustments
    timeframe_adjustments = {
        "M1": 0.5, "M5": 0.7, "M15": 0.8, "M30": 0.9,
        "H1": 1.0, "H2": 1.1, "H4": 1.2, "H8": 1.3,
        "D": 1.5, "W": 2.0, "M": 2.5
    }
    
    base = base_multipliers.get(instrument_type, 2.0)
    adjustment = timeframe_adjustments.get(timeframe, 1.0)
    
    multiplier = base * adjustment
    logger.debug(f"ATR multiplier for {instrument_type} on {timeframe}: {multiplier}")
    return multiplier

def is_instrument_tradeable(symbol: str) -> bool:
    """
    Check if an instrument is tradeable (basic validation).
    """
    if not symbol or len(symbol.strip()) < 3:
        return False
    
    # Remove common separators and check length
    clean_symbol = symbol.replace("_", "").replace("/", "").replace("-", "").strip()
    
    if len(clean_symbol) < 3 or len(clean_symbol) > 12:
        return False
    
    # Check for valid characters (alphanumeric)
    if not clean_symbol.isalnum():
        return False
    
    return True

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
    'MarketDataUnavailableError',
    'RETRY_DELAY',
    'get_module_logger',
    'TV_FIELD_MAP',
    'get_atr',
    'get_instrument_type',
    'get_atr_multiplier',
    'is_instrument_tradeable'
]

# ===== INSTITUTIONAL POSITION SIZING ALGORITHMS =====

def calculate_kelly_criterion(win_rate: float, avg_win: float, avg_loss: float) -> float:
    """
    Calculate Kelly Criterion for optimal position sizing.
    
    Args:
        win_rate: Probability of winning (0.0 to 1.0)
        avg_win: Average winning trade size
        avg_loss: Average losing trade size (positive value)
    
    Returns:
        Kelly percentage (0.0 to 1.0) - represents optimal fraction of capital to risk
    """
    if avg_loss <= 0 or win_rate <= 0 or win_rate >= 1:
        return 0.0
    
    kelly = (win_rate * avg_win - (1 - win_rate) * avg_loss) / avg_win
    return max(0.0, min(1.0, kelly))  # Clamp between 0 and 1

def calculate_optimal_f(win_rate: float, avg_win: float, avg_loss: float, 
                       num_trades: int = 100) -> float:
    """
    Calculate Optimal F (Fixed Fraction) for maximum growth rate.
    
    Args:
        win_rate: Probability of winning (0.0 to 1.0)
        avg_win: Average winning trade size
        avg_loss: Average losing trade size (positive value)
        num_trades: Number of trades for simulation
    
    Returns:
        Optimal F percentage (0.0 to 1.0)
    """
    if avg_loss <= 0 or win_rate <= 0 or win_rate >= 1:
        return 0.0
    
    # Simulate different F values to find optimal
    best_f = 0.0
    best_growth = 0.0
    
    for f in range(1, 101):  # Test F from 1% to 100%
        f_pct = f / 100.0
        growth = 1.0
        
        for _ in range(num_trades):
            if random.random() < win_rate:
                growth *= (1 + f_pct * avg_win / avg_loss)
            else:
                growth *= (1 - f_pct)
        
        if growth > best_growth:
            best_growth = growth
            best_f = f_pct
    
    return best_f

def calculate_volatility_adjusted_size(base_size: float, current_vol: float, 
                                     historical_vol: float, vol_lookback: int = 20) -> float:
    """
    Adjust position size based on current vs historical volatility.
    
    Args:
        base_size: Base position size
        current_vol: Current volatility (ATR or standard deviation)
        historical_vol: Historical average volatility
        vol_lookback: Lookback period for historical volatility
    
    Returns:
        Volatility-adjusted position size
    """
    if historical_vol <= 0:
        return base_size
    
    vol_ratio = current_vol / historical_vol
    
    # Reduce size when volatility is high, increase when low
    if vol_ratio > 1.5:  # High volatility
        adjustment = 0.7
    elif vol_ratio > 1.2:  # Above average volatility
        adjustment = 0.85
    elif vol_ratio < 0.8:  # Low volatility
        adjustment = 1.2
    elif vol_ratio < 0.6:  # Very low volatility
        adjustment = 1.4
    else:  # Normal volatility
        adjustment = 1.0
    
    return base_size * adjustment

def calculate_drawdown_adjusted_size(base_size: float, current_drawdown: float, 
                                   max_drawdown: float = 0.20) -> float:
    """
    Adjust position size based on current drawdown.
    
    Args:
        base_size: Base position size
        current_drawdown: Current drawdown percentage (0.0 to 1.0)
        max_drawdown: Maximum allowed drawdown before size reduction
    
    Returns:
        Drawdown-adjusted position size
    """
    if current_drawdown >= max_drawdown:
        return base_size * 0.5  # 50% size reduction at max drawdown
    elif current_drawdown > 0:
        # Linear reduction from 100% to 50% as drawdown increases
        reduction = 0.5 + (0.5 * (1 - current_drawdown / max_drawdown))
        return base_size * reduction
    
    return base_size

def calculate_correlation_adjusted_size(base_size: float, symbol: str, 
                                      open_positions: Dict[str, Any], 
                                      correlation_threshold: float = 0.7) -> float:
    """
    Adjust position size based on correlation with existing positions.
    
    Args:
        base_size: Base position size
        symbol: Current symbol
        open_positions: Dictionary of open positions
        correlation_threshold: Correlation threshold for size reduction
    
    Returns:
        Correlation-adjusted position size
    """
    if not open_positions:
        return base_size
    
    # Count correlated positions (simplified correlation check)
    correlated_count = 0
    for pos_symbol in open_positions.keys():
        if pos_symbol != symbol:
            # Simple correlation check - in production, use actual correlation matrix
            if any(asset in pos_symbol for asset in ['EUR', 'GBP', 'AUD', 'NZD']):  # Major pairs
                correlated_count += 1
    
    if correlated_count >= 2:
        return base_size * 0.8  # 20% reduction for high correlation
    elif correlated_count == 1:
        return base_size * 0.9  # 10% reduction for moderate correlation
    
    return base_size

# ===== ENHANCED POSITION SIZING WITH MULTIPLE ALGORITHMS =====

async def calculate_advanced_position_size(
    symbol: str,
    entry_price: float,
    risk_percent: float,
    account_balance: float,
    leverage: float = 50.0,
    stop_loss_price: Optional[float] = None,
    timeframe: str = "H1",
    strategy_params: Optional[Dict[str, Any]] = None
) -> Tuple[int, Dict[str, Any]]:
    """
    Advanced position sizing using multiple institutional algorithms.
    
    Args:
        symbol: Trading symbol
        entry_price: Entry price
        risk_percent: Risk percentage (0.0 to 1.0)
        account_balance: Account balance
        leverage: Leverage
        stop_loss_price: Stop loss price
        timeframe: Timeframe
        strategy_params: Strategy parameters including win rate, avg win/loss
    
    Returns:
        Tuple of (position_size, sizing_info)
    """
    # Default strategy parameters
    if strategy_params is None:
        strategy_params = {
            'win_rate': 0.55,  # 55% win rate
            'avg_win': 1.5,    # 1.5R average win
            'avg_loss': 1.0,   # 1.0R average loss
            'use_kelly': True,
            'use_optimal_f': False,
            'use_volatility_adjustment': True,
            'use_drawdown_adjustment': True,
            'use_correlation_adjustment': True
        }
    
    # Calculate base position size using existing method
    base_size, base_info = await calculate_position_size(
        symbol, entry_price, risk_percent, account_balance, leverage,
        stop_loss_price=stop_loss_price, timeframe=timeframe
    )
    
    sizing_info = {
        'base_size': base_size,
        'base_info': base_info,
        'adjustments': {}
    }
    
    # Kelly Criterion adjustment
    if strategy_params.get('use_kelly', True):
        kelly_pct = calculate_kelly_criterion(
            strategy_params['win_rate'],
            strategy_params['avg_win'],
            strategy_params['avg_loss']
        )
        kelly_size = int(base_size * kelly_pct)
        sizing_info['adjustments']['kelly'] = {
            'percentage': kelly_pct,
            'adjusted_size': kelly_size
        }
        base_size = kelly_size
    
    # Optimal F adjustment (alternative to Kelly)
    if strategy_params.get('use_optimal_f', False):
        optimal_f = calculate_optimal_f(
            strategy_params['win_rate'],
            strategy_params['avg_win'],
            strategy_params['avg_loss']
        )
        optimal_f_size = int(base_size * optimal_f)
        sizing_info['adjustments']['optimal_f'] = {
            'percentage': optimal_f,
            'adjusted_size': optimal_f_size
        }
        base_size = optimal_f_size
    
    # Volatility adjustment
    if strategy_params.get('use_volatility_adjustment', True) and stop_loss_price:
        # Calculate current ATR (simplified - in production, get from market data)
        current_atr = abs(entry_price - stop_loss_price) / 2  # Approximate
        historical_atr = current_atr * 1.1  # Approximate historical average
        
        vol_adjusted_size = calculate_volatility_adjusted_size(
            base_size, current_atr, historical_atr
        )
        sizing_info['adjustments']['volatility'] = {
            'current_atr': current_atr,
            'historical_atr': historical_atr,
            'adjusted_size': int(vol_adjusted_size)
        }
        base_size = int(vol_adjusted_size)
    
    # Drawdown adjustment (would need current drawdown from risk manager)
    if strategy_params.get('use_drawdown_adjustment', True):
        # In production, get current drawdown from risk manager
        current_drawdown = 0.05  # Placeholder - 5% drawdown
        
        dd_adjusted_size = calculate_drawdown_adjusted_size(base_size, current_drawdown)
        sizing_info['adjustments']['drawdown'] = {
            'current_drawdown': current_drawdown,
            'adjusted_size': int(dd_adjusted_size)
        }
        base_size = int(dd_adjusted_size)
    
    # Correlation adjustment (would need open positions from position tracker)
    if strategy_params.get('use_correlation_adjustment', True):
        # In production, get open positions from position tracker
        open_positions = {}  # Placeholder
        
        corr_adjusted_size = calculate_correlation_adjusted_size(
            base_size, symbol, open_positions
        )
        sizing_info['adjustments']['correlation'] = {
            'adjusted_size': int(corr_adjusted_size)
        }
        base_size = int(corr_adjusted_size)
    
    # Final validation
    min_units, max_units = get_position_size_limits(symbol)
    final_size = max(min_units, min(max_units, base_size))
    
    sizing_info['final_size'] = final_size
    sizing_info['algorithm'] = 'advanced_multi_factor'
    
    return final_size, sizing_info

# ===== INSTITUTIONAL EXIT STRATEGIES =====

class ExitStrategyEngine:
    """Institutional-grade exit strategy engine with multi-timeframe analysis."""
    
    def __init__(self):
        self.exit_strategies = {
            'trailing_stop': self._calculate_trailing_stop,
            'time_based': self._calculate_time_based_exit,
            'momentum_based': self._calculate_momentum_exit,
            'volatility_based': self._calculate_volatility_exit,
            'support_resistance': self._calculate_support_resistance_exit
        }
    
    async def calculate_dynamic_exit_levels(
        self,
        symbol: str,
        entry_price: float,
        current_price: float,
        position_size: float,
        timeframe: str,
        market_data: Dict[str, Any],
        strategy_params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Calculate dynamic exit levels using multiple strategies.
        
        Args:
            symbol: Trading symbol
            entry_price: Position entry price
            current_price: Current market price
            position_size: Position size
            timeframe: Trading timeframe
            market_data: Market data including OHLCV
            strategy_params: Strategy parameters
        
        Returns:
            Dictionary with exit levels and recommendations
        """
        if strategy_params is None:
            strategy_params = {
                'use_trailing_stop': True,
                'use_time_based': True,
                'use_momentum': True,
                'use_volatility': True,
                'use_support_resistance': True,
                'trailing_stop_multiplier': 2.0,
                'time_exit_hours': 24,
                'momentum_threshold': 0.02,
                'volatility_multiplier': 1.5
            }
        
        exit_levels = {}
        
        # Calculate exit levels for each strategy
        for strategy_name, strategy_func in self.exit_strategies.items():
            if strategy_params.get(f'use_{strategy_name}', True):
                try:
                    level = await strategy_func(
                        symbol, entry_price, current_price, position_size,
                        timeframe, market_data, strategy_params
                    )
                    exit_levels[strategy_name] = level
                except Exception as e:
                    logger.warning(f"Failed to calculate {strategy_name} exit level: {e}")
        
        # Determine optimal exit level based on consensus
        optimal_exit = self._determine_optimal_exit(exit_levels, current_price)
        
        return {
            'exit_levels': exit_levels,
            'optimal_exit': optimal_exit,
            'recommendation': self._generate_exit_recommendation(exit_levels, optimal_exit)
        }
    
    async def _calculate_trailing_stop(
        self, symbol: str, entry_price: float, current_price: float,
        position_size: float, timeframe: str, market_data: Dict[str, Any],
        strategy_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate ATR-based trailing stop."""
        try:
            # Get ATR from market data
            atr = market_data.get('atr', 0.001)  # Default if not available
            multiplier = strategy_params.get('trailing_stop_multiplier', 2.0)
            
            # Calculate trailing stop distance
            stop_distance = atr * multiplier
            
            # Determine stop direction based on position
            if current_price > entry_price:  # Long position in profit
                trailing_stop = current_price - stop_distance
            else:  # Long position at loss or short position
                trailing_stop = current_price + stop_distance
            
            return {
                'type': 'trailing_stop',
                'price': trailing_stop,
                'distance': stop_distance,
                'confidence': 0.8
            }
        except Exception as e:
            logger.error(f"Error calculating trailing stop: {e}")
            return {'type': 'trailing_stop', 'error': str(e)}
    
    async def _calculate_time_based_exit(
        self, symbol: str, entry_price: float, current_price: float,
        position_size: float, timeframe: str, market_data: Dict[str, Any],
        strategy_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate time-based exit levels."""
        try:
            hours_held = strategy_params.get('time_exit_hours', 24)
            
            # Simple time-based exit - in production, use actual position duration
            if hours_held > 48:  # Long-term position
                time_exit = entry_price * 1.02  # 2% profit target
            elif hours_held > 24:  # Medium-term position
                time_exit = entry_price * 1.01  # 1% profit target
            else:  # Short-term position
                time_exit = entry_price * 1.005  # 0.5% profit target
            
            return {
                'type': 'time_based',
                'price': time_exit,
                'hours_held': hours_held,
                'confidence': 0.6
            }
        except Exception as e:
            logger.error(f"Error calculating time-based exit: {e}")
            return {'type': 'time_based', 'error': str(e)}
    
    async def _calculate_momentum_exit(
        self, symbol: str, entry_price: float, current_price: float,
        position_size: float, timeframe: str, market_data: Dict[str, Any],
        strategy_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate momentum-based exit levels."""
        try:
            # Calculate momentum indicators
            rsi = market_data.get('rsi', 50)  # Default if not available
            momentum_threshold = strategy_params.get('momentum_threshold', 0.02)
            
            # Exit on momentum reversal
            if rsi > 70:  # Overbought
                momentum_exit = current_price * (1 - momentum_threshold)
            elif rsi < 30:  # Oversold
                momentum_exit = current_price * (1 + momentum_threshold)
            else:
                momentum_exit = entry_price  # No momentum signal
            
            return {
                'type': 'momentum_based',
                'price': momentum_exit,
                'rsi': rsi,
                'confidence': 0.7
            }
        except Exception as e:
            logger.error(f"Error calculating momentum exit: {e}")
            return {'type': 'momentum_based', 'error': str(e)}
    
    async def _calculate_volatility_exit(
        self, symbol: str, entry_price: float, current_price: float,
        position_size: float, timeframe: str, market_data: Dict[str, Any],
        strategy_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate volatility-based exit levels."""
        try:
            current_vol = market_data.get('current_volatility', 0.001)
            historical_vol = market_data.get('historical_volatility', 0.001)
            multiplier = strategy_params.get('volatility_multiplier', 1.5)
            
            # Adjust exit based on volatility regime
            if current_vol > historical_vol * multiplier:
                # High volatility - tighten exits
                vol_exit = entry_price * 1.005  # 0.5% profit target
            else:
                # Normal volatility - standard exits
                vol_exit = entry_price * 1.01  # 1% profit target
            
            return {
                'type': 'volatility_based',
                'price': vol_exit,
                'current_vol': current_vol,
                'historical_vol': historical_vol,
                'confidence': 0.75
            }
        except Exception as e:
            logger.error(f"Error calculating volatility exit: {e}")
            return {'type': 'volatility_based', 'error': str(e)}
    
    async def _calculate_support_resistance_exit(
        self, symbol: str, entry_price: float, current_price: float,
        position_size: float, timeframe: str, market_data: Dict[str, Any],
        strategy_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate support/resistance-based exit levels."""
        try:
            # Get support/resistance levels from market data
            resistance = market_data.get('resistance', entry_price * 1.02)
            support = market_data.get('support', entry_price * 0.98)
            
            # Exit at key levels
            if current_price > entry_price:  # Long position in profit
                sr_exit = resistance
            else:  # Long position at loss
                sr_exit = support
            
            return {
                'type': 'support_resistance',
                'price': sr_exit,
                'resistance': resistance,
                'support': support,
                'confidence': 0.8
            }
        except Exception as e:
            logger.error(f"Error calculating support/resistance exit: {e}")
            return {'type': 'support_resistance', 'error': str(e)}
    
    def _determine_optimal_exit(self, exit_levels: Dict[str, Any], current_price: float) -> Dict[str, Any]:
        """Determine optimal exit level based on consensus of strategies."""
        valid_levels = []
        
        for strategy_name, level_data in exit_levels.items():
            if 'error' not in level_data and 'price' in level_data:
                valid_levels.append({
                    'strategy': strategy_name,
                    'price': level_data['price'],
                    'confidence': level_data.get('confidence', 0.5)
                })
        
        if not valid_levels:
            return {'price': current_price, 'confidence': 0.0, 'reason': 'No valid exit levels'}
        
        # Calculate weighted average based on confidence
        total_weight = sum(level['confidence'] for level in valid_levels)
        if total_weight == 0:
            return {'price': current_price, 'confidence': 0.0, 'reason': 'No confidence in exit levels'}
        
        weighted_price = sum(
            level['price'] * level['confidence'] for level in valid_levels
        ) / total_weight
        
        avg_confidence = total_weight / len(valid_levels)
        
        return {
            'price': weighted_price,
            'confidence': avg_confidence,
            'strategies_used': len(valid_levels),
            'reason': f'Consensus of {len(valid_levels)} strategies'
        }
    
    def _generate_exit_recommendation(self, exit_levels: Dict[str, Any], optimal_exit: Dict[str, Any]) -> str:
        """Generate human-readable exit recommendation."""
        if optimal_exit['confidence'] < 0.5:
            return "Hold position - insufficient exit signals"
        
        strategies_used = optimal_exit.get('strategies_used', 0)
        if strategies_used >= 3:
            return f"Strong exit signal - {strategies_used} strategies agree"
        elif strategies_used >= 2:
            return f"Moderate exit signal - {strategies_used} strategies agree"
        else:
            return "Weak exit signal - single strategy recommendation"

# ===== MULTI-TIMEFRAME ANALYSIS =====

class MultiTimeframeAnalyzer:
    """Multi-timeframe analysis for enhanced exit decisions."""
    
    def __init__(self):
        self.timeframes = ['M15', 'H1', 'H4', 'D1']
        self.weights = {
            'M15': 0.2,  # 20% weight
            'H1': 0.3,   # 30% weight
            'H4': 0.3,   # 30% weight
            'D1': 0.2    # 20% weight
        }
    
    async def analyze_multi_timeframe_exit(
        self,
        symbol: str,
        entry_price: float,
        current_price: float,
        market_data_by_timeframe: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Analyze exit signals across multiple timeframes.
        
        Args:
            symbol: Trading symbol
            entry_price: Position entry price
            current_price: Current market price
            market_data_by_timeframe: Market data organized by timeframe
        
        Returns:
            Multi-timeframe analysis results
        """
        timeframe_signals = {}
        total_weight = 0
        weighted_exit_price = 0
        
        for timeframe in self.timeframes:
            if timeframe in market_data_by_timeframe:
                signal = await self._analyze_timeframe_signal(
                    symbol, entry_price, current_price,
                    market_data_by_timeframe[timeframe], timeframe
                )
                timeframe_signals[timeframe] = signal
                
                if signal['should_exit']:
                    weight = self.weights[timeframe]
                    total_weight += weight
                    weighted_exit_price += signal['exit_price'] * weight
        
        # Calculate consensus exit price
        consensus_exit_price = (
            weighted_exit_price / total_weight if total_weight > 0 else current_price
        )
        
        # Determine overall exit recommendation
        exit_strength = self._calculate_exit_strength(timeframe_signals)
        
        return {
            'timeframe_signals': timeframe_signals,
            'consensus_exit_price': consensus_exit_price,
            'exit_strength': exit_strength,
            'recommendation': self._generate_mtf_recommendation(exit_strength, total_weight)
        }
    
    async def _analyze_timeframe_signal(
        self, symbol: str, entry_price: float, current_price: float,
        market_data: Dict[str, Any], timeframe: str
    ) -> Dict[str, Any]:
        """Analyze exit signal for a specific timeframe."""
        try:
            # Calculate basic metrics
            price_change = (current_price - entry_price) / entry_price
            atr = market_data.get('atr', 0.001)
            
            # Determine exit signal based on timeframe
            if timeframe == 'M15':
                # Short-term: Exit on quick moves
                should_exit = abs(price_change) > 0.005  # 0.5% move
                exit_price = entry_price * (1 + price_change * 0.8)  # Partial profit
            elif timeframe == 'H1':
                # Medium-term: Exit on trend continuation
                should_exit = abs(price_change) > 0.01  # 1% move
                exit_price = entry_price * (1 + price_change * 0.9)
            elif timeframe == 'H4':
                # Longer-term: Exit on major moves
                should_exit = abs(price_change) > 0.015  # 1.5% move
                exit_price = entry_price * (1 + price_change)
            else:  # D1
                # Daily: Exit on significant moves
                should_exit = abs(price_change) > 0.02  # 2% move
                exit_price = entry_price * (1 + price_change)
            
            return {
                'timeframe': timeframe,
                'should_exit': should_exit,
                'exit_price': exit_price,
                'price_change': price_change,
                'atr': atr,
                'confidence': 0.7 if should_exit else 0.3
            }
        except Exception as e:
            logger.error(f"Error analyzing {timeframe} signal: {e}")
            return {
                'timeframe': timeframe,
                'should_exit': False,
                'exit_price': current_price,
                'error': str(e),
                'confidence': 0.0
            }
    
    def _calculate_exit_strength(self, timeframe_signals: Dict[str, Any]) -> float:
        """Calculate overall exit signal strength."""
        total_strength = 0
        total_weight = 0
        
        for timeframe, signal in timeframe_signals.items():
            if 'error' not in signal:
                weight = self.weights[timeframe]
                strength = signal['confidence'] if signal['should_exit'] else 0
                total_strength += strength * weight
                total_weight += weight
        
        return total_strength / total_weight if total_weight > 0 else 0
    
    def _generate_mtf_recommendation(self, exit_strength: float, total_weight: float) -> str:
        """Generate recommendation based on multi-timeframe analysis."""
        if exit_strength > 0.7:
            return "Strong multi-timeframe exit signal"
        elif exit_strength > 0.5:
            return "Moderate multi-timeframe exit signal"
        elif exit_strength > 0.3:
            return "Weak multi-timeframe exit signal"
        else:
            return "Hold position - no clear exit signals across timeframes"

# ===== MARKET DATA QUALITY FRAMEWORK =====

class MarketDataValidator:
    """Institutional-grade market data validation and quality control."""
    
    def __init__(self):
        self.price_history = {}  # symbol -> recent prices
        self.volume_history = {}  # symbol -> recent volumes
        self.spread_history = {}  # symbol -> recent spreads
        self.max_history_length = 100
        
        # Validation thresholds
        self.max_price_change = 0.10  # 10% max price change
        self.max_volume_spike = 5.0   # 5x normal volume
        self.max_spread_widening = 3.0  # 3x normal spread
        self.min_volume_threshold = 100  # Minimum volume for validation
    
    async def validate_price_data(
        self, symbol: str, price: float, timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Validate price data for anomalies and quality issues.
        
        Args:
            symbol: Trading symbol
            price: Current price
            timestamp: Price timestamp
        
        Returns:
            Validation results
        """
        validation_result = {
            'is_valid': True,
            'anomalies': [],
            'quality_score': 1.0,
            'warnings': []
        }
        
        if symbol not in self.price_history:
            self.price_history[symbol] = []
        
        price_history = self.price_history[symbol]
        
        # Check for price anomalies
        if price_history:
            last_price = price_history[-1]['price']
            price_change = abs(price - last_price) / last_price
            
            # Detect extreme price movements
            if price_change > self.max_price_change:
                validation_result['is_valid'] = False
                validation_result['anomalies'].append({
                    'type': 'extreme_price_change',
                    'severity': 'high',
                    'details': f"Price change: {price_change:.2%} > {self.max_price_change:.2%}"
                })
                validation_result['quality_score'] *= 0.5
            
            # Detect price gaps
            if price_change > 0.02:  # 2% gap
                validation_result['warnings'].append({
                    'type': 'price_gap',
                    'details': f"Price gap detected: {price_change:.2%}"
                })
                validation_result['quality_score'] *= 0.9
        
        # Store price data
        price_data = {
            'price': price,
            'timestamp': timestamp or datetime.now(timezone.utc)
        }
        price_history.append(price_data)
        
        # Maintain history length
        if len(price_history) > self.max_history_length:
            price_history.pop(0)
        
        return validation_result
    
    async def validate_volume_data(
        self, symbol: str, volume: float, timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Validate volume data for anomalies.
        
        Args:
            symbol: Trading symbol
            volume: Current volume
            timestamp: Volume timestamp
        
        Returns:
            Validation results
        """
        validation_result = {
            'is_valid': True,
            'anomalies': [],
            'quality_score': 1.0,
            'warnings': []
        }
        
        if symbol not in self.volume_history:
            self.volume_history[symbol] = []
        
        volume_history = self.volume_history[symbol]
        
        # Check for volume anomalies
        if volume_history and volume > self.min_volume_threshold:
            recent_volumes = [v['volume'] for v in volume_history[-10:]]  # Last 10 periods
            avg_volume = sum(recent_volumes) / len(recent_volumes)
            
            volume_ratio = volume / avg_volume if avg_volume > 0 else 0
            
            # Detect volume spikes
            if volume_ratio > self.max_volume_spike:
                validation_result['warnings'].append({
                    'type': 'volume_spike',
                    'details': f"Volume spike: {volume_ratio:.1f}x normal"
                })
                validation_result['quality_score'] *= 0.8
            
            # Detect low volume
            if volume_ratio < 0.1:  # Less than 10% of normal
                validation_result['warnings'].append({
                    'type': 'low_volume',
                    'details': f"Low volume: {volume_ratio:.1f}x normal"
                })
                validation_result['quality_score'] *= 0.9
        
        # Store volume data
        volume_data = {
            'volume': volume,
            'timestamp': timestamp or datetime.now(timezone.utc)
        }
        volume_history.append(volume_data)
        
        # Maintain history length
        if len(volume_history) > self.max_history_length:
            volume_history.pop(0)
        
        return validation_result
    
    async def validate_spread_data(
        self, symbol: str, bid: float, ask: float, timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Validate spread data for quality issues.
        
        Args:
            symbol: Trading symbol
            bid: Bid price
            ask: Ask price
            timestamp: Spread timestamp
        
        Returns:
            Validation results
        """
        validation_result = {
            'is_valid': True,
            'anomalies': [],
            'quality_score': 1.0,
            'warnings': []
        }
        
        if symbol not in self.spread_history:
            self.spread_history[symbol] = []
        
        spread_history = self.spread_history[symbol]
        
        # Calculate spread
        spread = ask - bid
        spread_pips = spread * 10000  # Convert to pips for forex
        
        # Check for spread anomalies
        if spread_history:
            recent_spreads = [s['spread_pips'] for s in spread_history[-10:]]
            avg_spread = sum(recent_spreads) / len(recent_spreads)
            
            spread_ratio = spread_pips / avg_spread if avg_spread > 0 else 0
            
            # Detect spread widening
            if spread_ratio > self.max_spread_widening:
                validation_result['warnings'].append({
                    'type': 'spread_widening',
                    'details': f"Spread widened: {spread_ratio:.1f}x normal"
                })
                validation_result['quality_score'] *= 0.7
            
            # Detect negative spread (data error)
            if spread <= 0:
                validation_result['is_valid'] = False
                validation_result['anomalies'].append({
                    'type': 'negative_spread',
                    'severity': 'high',
                    'details': f"Negative spread: {spread}"
                })
                validation_result['quality_score'] = 0.0
        
        # Store spread data
        spread_data = {
            'spread_pips': spread_pips,
            'bid': bid,
            'ask': ask,
            'timestamp': timestamp or datetime.now(timezone.utc)
        }
        spread_history.append(spread_data)
        
        # Maintain history length
        if len(spread_history) > self.max_history_length:
            spread_history.pop(0)
        
        return validation_result
    
    async def validate_market_data_comprehensive(
        self, symbol: str, market_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Comprehensive market data validation.
        
        Args:
            symbol: Trading symbol
            market_data: Complete market data dictionary
        
        Returns:
            Comprehensive validation results
        """
        validation_results = {
            'overall_valid': True,
            'quality_score': 1.0,
            'anomalies': [],
            'warnings': [],
            'component_results': {}
        }
        
        # Validate price
        if 'price' in market_data:
            price_result = await self.validate_price_data(
                symbol, market_data['price'], market_data.get('timestamp')
            )
            validation_results['component_results']['price'] = price_result
            validation_results['overall_valid'] &= price_result['is_valid']
            validation_results['quality_score'] *= price_result['quality_score']
            validation_results['anomalies'].extend(price_result['anomalies'])
            validation_results['warnings'].extend(price_result['warnings'])
        
        # Validate volume
        if 'volume' in market_data:
            volume_result = await self.validate_volume_data(
                symbol, market_data['volume'], market_data.get('timestamp')
            )
            validation_results['component_results']['volume'] = volume_result
            validation_results['overall_valid'] &= volume_result['is_valid']
            validation_results['quality_score'] *= volume_result['quality_score']
            validation_results['anomalies'].extend(volume_result['anomalies'])
            validation_results['warnings'].extend(volume_result['warnings'])
        
        # Validate spread
        if 'bid' in market_data and 'ask' in market_data:
            spread_result = await self.validate_spread_data(
                symbol, market_data['bid'], market_data['ask'], market_data.get('timestamp')
            )
            validation_results['component_results']['spread'] = spread_result
            validation_results['overall_valid'] &= spread_result['is_valid']
            validation_results['quality_score'] *= spread_result['quality_score']
            validation_results['anomalies'].extend(spread_result['anomalies'])
            validation_results['warnings'].extend(spread_result['warnings'])
        
        return validation_results

class LatencyMonitor:
    """Monitor and track data feed latency for institutional trading."""
    
    def __init__(self):
        self.latency_history = {}  # symbol -> latency measurements
        self.max_latency_ms = 1000  # 1 second max latency
        self.alert_threshold_ms = 500  # 500ms alert threshold
    
    async def measure_latency(
        self, symbol: str, data_timestamp: datetime, 
        receive_timestamp: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Measure data feed latency.
        
        Args:
            symbol: Trading symbol
            data_timestamp: Timestamp from data source
            receive_timestamp: Timestamp when data was received
        
        Returns:
            Latency measurement results
        """
        if receive_timestamp is None:
            receive_timestamp = datetime.now(timezone.utc)
        
        latency_ms = (receive_timestamp - data_timestamp).total_seconds() * 1000
        
        if symbol not in self.latency_history:
            self.latency_history[symbol] = []
        
        latency_data = {
            'latency_ms': latency_ms,
            'data_timestamp': data_timestamp,
            'receive_timestamp': receive_timestamp
        }
        
        self.latency_history[symbol].append(latency_data)
        
        # Keep only recent measurements
        if len(self.latency_history[symbol]) > 100:
            self.latency_history[symbol] = self.latency_history[symbol][-100:]
        
        # Calculate statistics
        recent_latencies = [l['latency_ms'] for l in self.latency_history[symbol][-20:]]
        avg_latency = sum(recent_latencies) / len(recent_latencies) if recent_latencies else 0
        max_latency = max(recent_latencies) if recent_latencies else 0
        
        return {
            'current_latency_ms': latency_ms,
            'avg_latency_ms': avg_latency,
            'max_latency_ms': max_latency,
            'is_acceptable': latency_ms <= self.max_latency_ms,
            'alert_triggered': latency_ms > self.alert_threshold_ms
        }

# ===== INSTITUTIONAL PERFORMANCE MONITORING =====

class PerformanceMonitor:
    """Institutional-grade performance monitoring and metrics collection."""
    
    def __init__(self):
        self.trade_history = []
        self.daily_returns = []
        self.performance_metrics = {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'total_pnl': 0.0,
            'max_drawdown': 0.0,
            'sharpe_ratio': 0.0,
            'calmar_ratio': 0.0,
            'sortino_ratio': 0.0,
            'profit_factor': 0.0,
            'win_rate': 0.0,
            'avg_win': 0.0,
            'avg_loss': 0.0,
            'largest_win': 0.0,
            'largest_loss': 0.0,
            'consecutive_wins': 0,
            'consecutive_losses': 0
        }
        self.peak_equity = 0.0
        self.current_equity = 0.0
    
    async def record_trade(
        self, trade_id: str, symbol: str, entry_price: float, exit_price: float,
        position_size: float, entry_time: datetime, exit_time: datetime,
        pnl: float, strategy: str = "unknown"
    ) -> None:
        """
        Record a completed trade for performance analysis.
        
        Args:
            trade_id: Unique trade identifier
            symbol: Trading symbol
            entry_price: Entry price
            exit_price: Exit price
            position_size: Position size
            entry_time: Entry timestamp
            exit_time: Exit timestamp
            pnl: Profit/loss
            strategy: Strategy name
        """
        trade_data = {
            'trade_id': trade_id,
            'symbol': symbol,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'position_size': position_size,
            'entry_time': entry_time,
            'exit_time': exit_time,
            'duration': (exit_time - entry_time).total_seconds() / 3600,  # Hours
            'pnl': pnl,
            'strategy': strategy,
            'return_pct': (pnl / (entry_price * position_size)) * 100 if entry_price * position_size > 0 else 0
        }
        
        self.trade_history.append(trade_data)
        await self._update_performance_metrics()
    
    async def update_equity(self, current_equity: float) -> None:
        """
        Update current equity for drawdown calculations.
        
        Args:
            current_equity: Current account equity
        """
        self.current_equity = current_equity
        
        if current_equity > self.peak_equity:
            self.peak_equity = current_equity
        
        # Calculate drawdown
        if self.peak_equity > 0:
            drawdown = (self.peak_equity - current_equity) / self.peak_equity
            self.performance_metrics['max_drawdown'] = max(
                self.performance_metrics['max_drawdown'], drawdown
            )
    
    async def _update_performance_metrics(self) -> None:
        """Update all performance metrics based on trade history."""
        if not self.trade_history:
            return
        
        trades = self.trade_history
        
        # Basic metrics
        self.performance_metrics['total_trades'] = len(trades)
        self.performance_metrics['total_pnl'] = sum(t['pnl'] for t in trades)
        
        # Win/loss analysis
        winning_trades = [t for t in trades if t['pnl'] > 0]
        losing_trades = [t for t in trades if t['pnl'] < 0]
        
        self.performance_metrics['winning_trades'] = len(winning_trades)
        self.performance_metrics['losing_trades'] = len(losing_trades)
        self.performance_metrics['win_rate'] = len(winning_trades) / len(trades) if trades else 0
        
        # Average win/loss
        if winning_trades:
            self.performance_metrics['avg_win'] = sum(t['pnl'] for t in winning_trades) / len(winning_trades)
            self.performance_metrics['largest_win'] = max(t['pnl'] for t in winning_trades)
        
        if losing_trades:
            self.performance_metrics['avg_loss'] = sum(t['pnl'] for t in losing_trades) / len(losing_trades)
            self.performance_metrics['largest_loss'] = min(t['pnl'] for t in losing_trades)
        
        # Profit factor
        total_wins = sum(t['pnl'] for t in winning_trades)
        total_losses = abs(sum(t['pnl'] for t in losing_trades))
        self.performance_metrics['profit_factor'] = total_wins / total_losses if total_losses > 0 else float('inf')
        
        # Calculate ratios
        await self._calculate_risk_adjusted_ratios()
        
        # Calculate consecutive wins/losses
        await self._calculate_consecutive_streaks()
    
    async def _calculate_risk_adjusted_ratios(self) -> None:
        """Calculate risk-adjusted performance ratios."""
        if not self.trade_history:
            return
        
        # Calculate daily returns
        daily_returns = self._calculate_daily_returns()
        
        if not daily_returns:
            return
        
        returns_array = np.array(daily_returns)
        
        # Sharpe ratio (assuming 0% risk-free rate)
        mean_return = np.mean(returns_array)
        std_return = np.std(returns_array, ddof=1)
        self.performance_metrics['sharpe_ratio'] = mean_return / std_return if std_return > 0 else 0
        
        # Sortino ratio (downside deviation)
        downside_returns = returns_array[returns_array < 0]
        downside_std = np.std(downside_returns, ddof=1) if len(downside_returns) > 0 else 0
        self.performance_metrics['sortino_ratio'] = mean_return / downside_std if downside_std > 0 else 0
        
        # Calmar ratio (annualized return / max drawdown)
        if self.performance_metrics['max_drawdown'] > 0:
            # Annualize returns (assuming daily data)
            annualized_return = mean_return * 252
            self.performance_metrics['calmar_ratio'] = annualized_return / self.performance_metrics['max_drawdown']
    
    def _calculate_daily_returns(self) -> List[float]:
        """Calculate daily returns from trade history."""
        if not self.trade_history:
            return []
        
        # Group trades by day
        daily_pnl = {}
        for trade in self.trade_history:
            date_key = trade['exit_time'].date()
            if date_key not in daily_pnl:
                daily_pnl[date_key] = 0.0
            daily_pnl[date_key] += trade['pnl']
        
        # Convert to returns (assuming constant equity for simplicity)
        # In production, use actual daily equity values
        daily_equity = 10000  # Placeholder
        daily_returns = []
        
        for date, pnl in sorted(daily_pnl.items()):
            daily_return = pnl / daily_equity
            daily_returns.append(daily_return)
        
        return daily_returns
    
    async def _calculate_consecutive_streaks(self) -> None:
        """Calculate consecutive win/loss streaks."""
        if not self.trade_history:
            return
        
        current_streak = 0
        max_consecutive_wins = 0
        max_consecutive_losses = 0
        
        for trade in self.trade_history:
            if trade['pnl'] > 0:  # Win
                if current_streak >= 0:
                    current_streak += 1
                else:
                    current_streak = 1
                max_consecutive_wins = max(max_consecutive_wins, current_streak)
            else:  # Loss
                if current_streak <= 0:
                    current_streak -= 1
                else:
                    current_streak = -1
                max_consecutive_losses = max(max_consecutive_losses, abs(current_streak))
        
        self.performance_metrics['consecutive_wins'] = max_consecutive_wins
        self.performance_metrics['consecutive_losses'] = max_consecutive_losses
    
    async def get_performance_report(self) -> Dict[str, Any]:
        """
        Generate comprehensive performance report.
        
        Returns:
            Complete performance report
        """
        await self._update_performance_metrics()
        
        return {
            'summary': {
                'total_trades': self.performance_metrics['total_trades'],
                'win_rate': f"{self.performance_metrics['win_rate']:.2%}",
                'total_pnl': f"${self.performance_metrics['total_pnl']:,.2f}",
                'max_drawdown': f"{self.performance_metrics['max_drawdown']:.2%}",
                'sharpe_ratio': f"{self.performance_metrics['sharpe_ratio']:.2f}",
                'profit_factor': f"{self.performance_metrics['profit_factor']:.2f}"
            },
            'detailed_metrics': self.performance_metrics,
            'recent_trades': self.trade_history[-10:] if self.trade_history else [],
            'equity_curve': {
                'current_equity': self.current_equity,
                'peak_equity': self.peak_equity,
                'current_drawdown': (self.peak_equity - self.current_equity) / self.peak_equity if self.peak_equity > 0 else 0
            }
        }
    
    async def get_strategy_performance(self, strategy: str) -> Dict[str, Any]:
        """
        Get performance metrics for a specific strategy.
        
        Args:
            strategy: Strategy name
        
        Returns:
            Strategy-specific performance metrics
        """
        strategy_trades = [t for t in self.trade_history if t['strategy'] == strategy]
        
        if not strategy_trades:
            return {'error': f'No trades found for strategy: {strategy}'}
        
        # Calculate strategy-specific metrics
        total_pnl = sum(t['pnl'] for t in strategy_trades)
        winning_trades = [t for t in strategy_trades if t['pnl'] > 0]
        losing_trades = [t for t in strategy_trades if t['pnl'] < 0]
        
        win_rate = len(winning_trades) / len(strategy_trades)
        avg_win = sum(t['pnl'] for t in winning_trades) / len(winning_trades) if winning_trades else 0
        avg_loss = sum(t['pnl'] for t in losing_trades) / len(losing_trades) if losing_trades else 0
        
        return {
            'strategy': strategy,
            'total_trades': len(strategy_trades),
            'win_rate': f"{win_rate:.2%}",
            'total_pnl': f"${total_pnl:,.2f}",
            'avg_win': f"${avg_win:,.2f}",
            'avg_loss': f"${avg_loss:,.2f}",
            'profit_factor': avg_win / abs(avg_loss) if avg_loss != 0 else float('inf')
        }

# ===== OANDA PRICE VALIDATION =====

def validate_oanda_prices(
    symbol: str, 
    entry_price: float, 
    stop_loss: float, 
    take_profit: float, 
    action: str
) -> Dict[str, Any]:
    """
    Validate and adjust prices to meet OANDA's minimum distance requirements.
    
    Args:
        symbol: Trading symbol
        entry_price: Entry price
        stop_loss: Stop loss price
        take_profit: Take profit price
        action: Trade action (BUY/SELL)
    
    Returns:
        Dictionary with validated prices and validation info
    """
    # OANDA minimum distance requirements
    min_distance_pips = {
        'forex': 10,      # 10 pips for forex
        'crypto': 50,     # 50 pips for crypto
        'indices': 5,     # 5 pips for indices
        'commodities': 10  # 10 pips for commodities
    }
    
    # Determine instrument type
    instrument_type = get_instrument_type(symbol)
    min_pips = min_distance_pips.get(instrument_type, 10)
    min_distance = min_pips / 10000  # Convert pips to price
    
    validation_result = {
        'original_entry': entry_price,
        'original_stop_loss': stop_loss,
        'original_take_profit': take_profit,
        'adjusted_entry': entry_price,
        'adjusted_stop_loss': stop_loss,
        'adjusted_take_profit': take_profit,
        'min_distance_pips': min_pips,
        'min_distance_price': min_distance,
        'adjustments_made': [],
        'is_valid': True
    }
    
    # Calculate distances
    if action == "BUY":
        sl_distance = entry_price - stop_loss
        tp_distance = take_profit - entry_price
    else:  # SELL
        sl_distance = stop_loss - entry_price
        tp_distance = entry_price - take_profit
    
    # Validate stop loss distance
    if sl_distance < min_distance:
        validation_result['adjustments_made'].append({
            'type': 'stop_loss_distance',
            'original_distance': sl_distance,
            'required_distance': min_distance,
            'adjustment': f"Stop loss distance too small ({sl_distance:.5f} < {min_distance:.5f})"
        })
        
        # Adjust stop loss to meet minimum distance
        if action == "BUY":
            validation_result['adjusted_stop_loss'] = entry_price - min_distance
        else:  # SELL
            validation_result['adjusted_stop_loss'] = entry_price + min_distance
        
        validation_result['is_valid'] = False
    
    # Validate take profit distance
    if tp_distance < min_distance:
        validation_result['adjustments_made'].append({
            'type': 'take_profit_distance',
            'original_distance': tp_distance,
            'required_distance': min_distance,
            'adjustment': f"Take profit distance too small ({tp_distance:.5f} < {min_distance:.5f})"
        })
        
        # Adjust take profit to meet minimum distance
        if action == "BUY":
            validation_result['adjusted_take_profit'] = entry_price + min_distance
        else:  # SELL
            validation_result['adjusted_take_profit'] = entry_price - min_distance
        
        validation_result['is_valid'] = False
    
    # Additional validation: ensure stop loss and take profit are on opposite sides
    if action == "BUY":
        if validation_result['adjusted_stop_loss'] >= entry_price:
            validation_result['adjustments_made'].append({
                'type': 'stop_loss_side',
                'adjustment': "Stop loss must be below entry price for BUY orders"
            })
            validation_result['adjusted_stop_loss'] = entry_price - min_distance
            validation_result['is_valid'] = False
        
        if validation_result['adjusted_take_profit'] <= entry_price:
            validation_result['adjustments_made'].append({
                'type': 'take_profit_side',
                'adjustment': "Take profit must be above entry price for BUY orders"
            })
            validation_result['adjusted_take_profit'] = entry_price + min_distance
            validation_result['is_valid'] = False
    else:  # SELL
        if validation_result['adjusted_stop_loss'] <= entry_price:
            validation_result['adjustments_made'].append({
                'type': 'stop_loss_side',
                'adjustment': "Stop loss must be above entry price for SELL orders"
            })
            validation_result['adjusted_stop_loss'] = entry_price + min_distance
            validation_result['is_valid'] = False
        
        if validation_result['adjusted_take_profit'] >= entry_price:
            validation_result['adjustments_made'].append({
                'type': 'take_profit_side',
                'adjustment': "Take profit must be below entry price for SELL orders"
            })
            validation_result['adjusted_take_profit'] = entry_price - min_distance
            validation_result['is_valid'] = False
    
    # Round prices to appropriate decimal places and ensure they're valid numbers
    validation_result['adjusted_stop_loss'] = round(validation_result['adjusted_stop_loss'], 5)
    validation_result['adjusted_take_profit'] = round(validation_result['adjusted_take_profit'], 5)
    
    # Final validation: ensure prices are finite numbers
    import math
    if not (isinstance(validation_result['adjusted_stop_loss'], (int, float)) and 
            isinstance(validation_result['adjusted_take_profit'], (int, float)) and
            math.isfinite(validation_result['adjusted_stop_loss']) and
            math.isfinite(validation_result['adjusted_take_profit'])):
        validation_result['adjustments_made'].append({
            'type': 'invalid_price',
            'adjustment': "Prices must be valid finite numbers"
        })
        validation_result['is_valid'] = False
    
    # Ensure prices are positive
    if validation_result['adjusted_stop_loss'] <= 0 or validation_result['adjusted_take_profit'] <= 0:
        validation_result['adjustments_made'].append({
            'type': 'negative_price',
            'adjustment': "Prices must be positive"
        })
        validation_result['is_valid'] = False
    
    return validation_result

def format_price_for_oanda(price: float, symbol: str) -> str:
    """
    Format price for OANDA API with proper decimal places.
    
    Args:
        price: Price to format
        symbol: Trading symbol
    
    Returns:
        Formatted price string
    """
    # Determine decimal places based on instrument type
    instrument_type = get_instrument_type(symbol)
    
    if instrument_type == 'crypto':
        # Crypto typically uses 2 decimal places
        return f"{price:.2f}"
    elif instrument_type in ['forex_major', 'forex_minor']:
        # Forex uses 5 decimal places
        return f"{price:.5f}"
    else:
        # Default to 5 decimal places
        return f"{price:.5f}"

def get_oanda_minimum_distances(symbol: str) -> Dict[str, float]:
    """
    Get OANDA minimum distance requirements for a symbol.
    
    Args:
        symbol: Trading symbol
    
    Returns:
        Dictionary with minimum distances in pips and price
    """
    instrument_type = get_instrument_type(symbol)
    
    min_distance_pips = {
        'forex': 10,
        'crypto': 50,
        'indices': 5,
        'commodities': 10
    }
    
    min_pips = min_distance_pips.get(instrument_type, 10)
    min_distance = min_pips / 10000
    
    return {
        'instrument_type': instrument_type,
        'min_pips': min_pips,
        'min_distance': min_distance
    }

def validate_oanda_order(
    symbol: str,
    action: str,
    units: int,
    entry_price: float,
    stop_loss: Optional[float] = None,
    take_profit: Optional[float] = None
) -> Dict[str, Any]:
    """
    Comprehensive validation for OANDA order parameters.
    
    Args:
        symbol: Trading symbol
        action: Trade action (BUY/SELL)
        units: Position size in units
        entry_price: Entry price
        stop_loss: Stop loss price (optional)
        take_profit: Take profit price (optional)
    
    Returns:
        Dictionary with validation results and any issues found
    """
    validation_result = {
        'is_valid': True,
        'issues': [],
        'warnings': [],
        'order_data': {
            'symbol': symbol,
            'action': action,
            'units': units,
            'entry_price': entry_price,
            'stop_loss': stop_loss,
            'take_profit': take_profit
        }
    }
    
    # Basic parameter validation
    if not symbol or not isinstance(symbol, str):
        validation_result['issues'].append("Symbol is required and must be a string")
        validation_result['is_valid'] = False
    
    if action not in ['BUY', 'SELL']:
        validation_result['issues'].append("Action must be 'BUY' or 'SELL'")
        validation_result['is_valid'] = False
    
    if not isinstance(units, int) or units <= 0:
        validation_result['issues'].append("Units must be a positive integer")
        validation_result['is_valid'] = False
    
    if not isinstance(entry_price, (int, float)) or entry_price <= 0:
        validation_result['issues'].append("Entry price must be a positive number")
        validation_result['is_valid'] = False
    
    # Price validation
    if stop_loss is not None:
        if not isinstance(stop_loss, (int, float)) or stop_loss <= 0:
            validation_result['issues'].append("Stop loss must be a positive number")
            validation_result['is_valid'] = False
    
    if take_profit is not None:
        if not isinstance(take_profit, (int, float)) or take_profit <= 0:
            validation_result['issues'].append("Take profit must be a positive number")
            validation_result['is_valid'] = False
    
    # Validate price relationships
    if action == "BUY":
        if stop_loss is not None and stop_loss >= entry_price:
            validation_result['issues'].append("For BUY orders, stop loss must be below entry price")
            validation_result['is_valid'] = False
        
        if take_profit is not None and take_profit <= entry_price:
            validation_result['issues'].append("For BUY orders, take profit must be above entry price")
            validation_result['is_valid'] = False
    else:  # SELL
        if stop_loss is not None and stop_loss <= entry_price:
            validation_result['issues'].append("For SELL orders, stop loss must be above entry price")
            validation_result['is_valid'] = False
        
        if take_profit is not None and take_profit >= entry_price:
            validation_result['issues'].append("For SELL orders, take profit must be below entry price")
            validation_result['is_valid'] = False
    
    # Validate minimum distances if both SL and TP are provided
    if stop_loss is not None and take_profit is not None:
        price_validation = validate_oanda_prices(
            symbol=symbol,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            action=action
        )
        
        if not price_validation['is_valid']:
            for adjustment in price_validation['adjustments_made']:
                validation_result['warnings'].append(f"Price adjustment: {adjustment['adjustment']}")
        
        # Update with validated prices
        validation_result['order_data']['stop_loss'] = price_validation['adjusted_stop_loss']
        validation_result['order_data']['take_profit'] = price_validation['adjusted_take_profit']
    
    # Format prices for OANDA
    if validation_result['order_data']['stop_loss'] is not None:
        validation_result['order_data']['formatted_stop_loss'] = format_price_for_oanda(
            validation_result['order_data']['stop_loss'], symbol
        )
    
    if validation_result['order_data']['take_profit'] is not None:
        validation_result['order_data']['formatted_take_profit'] = format_price_for_oanda(
            validation_result['order_data']['take_profit'], symbol
        )
    
    return validation_result
