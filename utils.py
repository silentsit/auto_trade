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
        # Crypto pairs (2:1) - OANDA requires larger minimum sizes
        "BTC_USD": {"pip_value": 1.0, "max_leverage": 2.0, "min_trade_size": 0.05},
        "ETH_USD": {"pip_value": 0.01, "max_leverage": 2.0, "min_trade_size": 1},
        "LTC_USD": {"pip_value": 0.01, "max_leverage": 2.0, "min_trade_size": 1},
        "BCH_USD": {"pip_value": 0.01, "max_leverage": 2.0, "min_trade_size": 1},
        "XRP_USD": {"pip_value": 0.01, "max_leverage": 2.0, "min_trade_size": 1},
        "ADA_USD": {"pip_value": 0.01, "max_leverage": 2.0, "min_trade_size": 1},
        "DOT_USD": {"pip_value": 0.01, "max_leverage": 2.0, "min_trade_size": 1},
        "SOL_USD": {"pip_value": 0.01, "max_leverage": 2.0, "min_trade_size": 1},
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

def get_position_size_limits(symbol: str) -> Tuple[float, float]:
    settings = get_instrument_settings(symbol)
    min_size = settings.get("min_trade_size", 1)
    max_size = settings.get("max_trade_size", 100_000_000)
    
    # Convert to float for crypto, int for forex
    asset_class = get_asset_class(symbol.upper())
    if asset_class == "crypto":
        return float(min_size), float(max_size)
    else:
        return int(min_size), int(max_size)

def round_position_size(symbol: str, size: float) -> float:
    """Round position size appropriately for the instrument type"""
    asset_class = get_asset_class(symbol.upper())
    
    if asset_class == "crypto":
        # For crypto, allow fractional units (2 decimal places)
        return round(size, 2)
    else:
        # For forex, use whole units
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

# --- Asset-Type Detection Utility ---
def get_asset_class(symbol: str) -> str:
    symbol = symbol.upper()
    # Add more as needed for other asset classes
    if symbol in {"BTC_USD", "ETH_USD", "LTC_USD", "XRP_USD", "BCH_USD", "ADA_USD", "DOT_USD", "SOL_USD"}:
        return "crypto"
    elif symbol in {"XAU_USD", "XAG_USD"}:
        return "metal"
    elif "_" in symbol and symbol.endswith("USD"):
        return "fx"
    return "unknown"

async def calculate_position_size(
    symbol: str,
    entry_price: float,
    risk_percent: float,
    account_balance: float,
    leverage: float = 50.0,
    max_position_value: float = 100000.0,
    stop_loss_price: Optional[float] = None,
    timeframe: str = "H1"
) -> Tuple[float, Dict[str, Any]]:
    """
    Calculate position size using CONSERVATIVE risk management.
    
    This method:
    1. Uses ATR for dynamic stop loss calculation BUT with minimum stop distances
    2. Implements maximum position size caps to prevent oversized trades
    3. Ensures consistent risk per trade regardless of market volatility
    4. ✅ Now properly validates and uses actual account leverage
    5. 🚨 FIXED: Prevents massive position sizes from tiny ATR stops
    """
    try:
        logger.info(f"[CONSERVATIVE SIZING] {symbol}: risk={risk_percent}%, balance=${account_balance:.2f}, entry=${entry_price}, leverage={leverage:.1f}:1")
        
        # ✅ Validate leverage parameter
        if leverage <= 0 or leverage > 100:
            logger.warning(f"[LEVERAGE VALIDATION] {symbol}: Invalid leverage {leverage:.1f}:1, using fallback 50:1")
            leverage = 50.0
        
        if risk_percent <= 0 or risk_percent > 20:
            return 0, {"error": "Invalid risk percentage"}
        if account_balance <= 0:
            return 0, {"error": "Invalid account balance"}
        if entry_price <= 0:
            return 0, {"error": "Invalid entry price"}
        
        # Normalize symbol
        symbol_upper = symbol.upper()
        asset_class = get_asset_class(symbol_upper)
        
        # 🚨 CRITICAL FIX: Define minimum stop loss distances to prevent oversized positions
        MIN_STOP_DISTANCES = {
            "M1": 0.0020,    # 20 pips minimum for 1-minute charts
            "M5": 0.0015,    # 15 pips minimum for 5-minute charts  
            "M15": 0.0010,   # 10 pips minimum for 15-minute charts
            "M30": 0.0008,   # 8 pips minimum for 30-minute charts
            "H1": 0.0006,    # 6 pips minimum for 1-hour charts
            "H4": 0.0005,    # 5 pips minimum for 4-hour charts
            "D1": 0.0004,    # 4 pips minimum for daily charts
        }
        
        # Get ATR value for dynamic stop loss calculation
        atr_value = None
        try:
            from technical_analysis import get_atr
            atr_value = await get_atr(symbol_upper, timeframe, period=14)
            if atr_value is None or atr_value <= 0:
                logger.warning(f"[ATR UNAVAILABLE] {symbol}: ATR not available, using fallback method")
                atr_value = None
        except Exception as e:
            logger.warning(f"[ATR ERROR] {symbol}: Failed to get ATR: {e}")
            atr_value = None
        
        # ATR multiplier based on timeframe (matching Backtest-Adapter-3.0)
        if timeframe in ["M1", "M5", "M15"]:
            atr_multiplier = 1.5  # Intraday ≤ 15 minutes
        else:
            atr_multiplier = 2.0  # Default for 1H, 4H, 1D
        
        # 🚨 CRITICAL FIX: Calculate stop loss distance with MINIMUM limits
        stop_loss_distance = None
        if atr_value and atr_value > 0:
            # Use ATR-based stop loss BUT enforce minimum distance
            raw_atr_distance = atr_value * atr_multiplier
            min_stop_distance = MIN_STOP_DISTANCES.get(timeframe, 0.0005)  # Default 5 pips
            
            # Use the LARGER of ATR-based or minimum distance
            stop_loss_distance = max(raw_atr_distance, min_stop_distance)
            logger.info(f"[CONSERVATIVE STOP LOSS] {symbol}: ATR={atr_value:.5f}, multiplier={atr_multiplier}, raw_distance={raw_atr_distance:.5f}, min_distance={min_stop_distance:.5f}, final_distance={stop_loss_distance:.5f}")
        elif stop_loss_price is not None:
            # Fallback to provided stop loss
            stop_loss_distance = abs(entry_price - stop_loss_price)
            min_stop_distance = MIN_STOP_DISTANCES.get(timeframe, 0.0005)
            stop_loss_distance = max(stop_loss_distance, min_stop_distance)
            logger.info(f"[PROVIDED STOP LOSS] {symbol}: Using provided stop_loss={stop_loss_price}, distance={stop_loss_distance:.5f}")
        else:
            # Fallback: Use percentage-based stop loss with minimum
            fallback_stop_pct = 0.01  # 1% default
            fallback_distance = entry_price * fallback_stop_pct
            min_stop_distance = MIN_STOP_DISTANCES.get(timeframe, 0.0005)
            stop_loss_distance = max(fallback_distance, min_stop_distance)
            logger.info(f"[FALLBACK STOP LOSS] {symbol}: Using {fallback_stop_pct*100}% fallback, distance={stop_loss_distance:.5f}")
        
        # 🚨 CRITICAL FIX: Calculate position size with MAXIMUM caps
        risk_amount = account_balance * (risk_percent / 100.0)
        
        if stop_loss_distance and stop_loss_distance > 0:
            # Calculate position size using risk per share
            risk_per_share = stop_loss_distance
            raw_size = risk_amount / risk_per_share
            
            logger.info(f"[CONSERVATIVE SIZING] {symbol}: Risk=${risk_amount:.2f}, Risk/Share=${risk_per_share:.5f}, Raw Size={raw_size:,.0f}")
            
            # 🚨 CRITICAL FIX: Apply maximum position size caps
            max_position_size_caps = {
                "M1": 10000,    # Max 10k units for 1-minute charts
                "M5": 15000,    # Max 15k units for 5-minute charts
                "M15": 25000,   # Max 25k units for 15-minute charts
                "M30": 50000,   # Max 50k units for 30-minute charts
                "H1": 100000,   # Max 100k units for 1-hour charts
                "H4": 200000,   # Max 200k units for 4-hour charts
                "D1": 500000,   # Max 500k units for daily charts
            }
            
            max_cap = max_position_size_caps.get(timeframe, 100000)
            if raw_size > max_cap:
                logger.warning(f"[POSITION CAP] {symbol}: Raw size {raw_size:,.0f} exceeds {timeframe} cap of {max_cap:,}. Capping position size.")
                raw_size = max_cap
                
        else:
            # Fallback: Use percentage-based sizing
            target_position_value = account_balance * (risk_percent / 100.0) * leverage
            raw_size = target_position_value / entry_price
            logger.info(f"[FALLBACK SIZING] {symbol}: Target value=${target_position_value:.2f}, Size={raw_size:.2f}")
        
        # Get position size limits
        min_units, max_units = get_position_size_limits(symbol_upper)
        
        # INSTITUTIONAL FIX: Improve margin utilization
        required_margin = (raw_size * entry_price) / leverage
        
        # Get margin utilization percentage from config
        try:
            from config import settings
            margin_utilization_pct = float(getattr(settings.trading, 'margin_utilization_percentage', 85.0))
        except:
            margin_utilization_pct = 85.0  # Fallback default

        available_margin = account_balance * (margin_utilization_pct / 100.0)

        # Handle min_units and max_units based on asset class
        if asset_class == "crypto":
            # For crypto, keep as float to support fractional units
            min_units = float(min_units)
            max_units = float(max_units)
        else:
            # For forex, use integers
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
        min_reasonable_size = min_units  # Use instrument-specific minimum
        
        if raw_size < min_reasonable_size:
            raw_size = min_reasonable_size
            logger.info(f"[MIN SIZE] {symbol}: Adjusted to minimum size of {min_reasonable_size:.0f} units")
        
        # --- CRYPTO MAX UNITS FIX ---
        CRYPTO_MAX_UNITS = {
            "BTC_USD": 1000,  # Based on transaction history showing 1000.00 BTC trades
            "ETH_USD": 100,   # Based on transaction history showing up to 27.00 ETH trades
            "LTC_USD": 100,
            "XRP_USD": 1000,
            "BCH_USD": 100,
            "ADA_USD": 1000,
            "DOT_USD": 1000,
            "SOL_USD": 1000,
            # Add more as needed
        }
        if asset_class == "crypto" and symbol_upper in CRYPTO_MAX_UNITS:
            if raw_size > CRYPTO_MAX_UNITS[symbol_upper]:
                logger.warning(f"[CRYPTO LIMIT] {symbol}: Requested size {raw_size} exceeds crypto max {CRYPTO_MAX_UNITS[symbol_upper]}. Capping to max.")
                raw_size = CRYPTO_MAX_UNITS[symbol_upper]
        
        position_size = max(min_units, min(max_units, raw_size))
        final_position_size = round_position_size(symbol_upper, position_size)
        
        # Final safety check
        if final_position_size < min_units:
            final_position_size = min_units
            logger.warning(f"[FINAL MIN] {symbol}: Using broker minimum of {min_units} units")
        
        final_margin = (final_position_size * entry_price) / leverage
        final_value = final_position_size * entry_price
        final_margin_pct = (final_margin / account_balance) * 100
        
        # Calculate actual risk amount (matching Backtest-Adapter-3.0)
        actual_risk = final_position_size * stop_loss_distance if stop_loss_distance else 0
        
        sizing_info = {
            "calculated_size": final_position_size, 
            "entry_price": entry_price, 
            "position_value": final_value,
            "required_margin": final_margin, 
            "margin_utilization_pct": final_margin_pct, 
            "risk_amount": risk_amount,
            "stop_loss_distance": stop_loss_distance,
            "actual_risk": actual_risk,
            "leverage": leverage,
            "atr_value": atr_value,
            "atr_multiplier": atr_multiplier,
            "method": "Conservative ATR-based" if atr_value else "Conservative Fallback",
            "position_cap_applied": raw_size != (risk_amount / stop_loss_distance) if stop_loss_distance else False
        }
        
        logger.info(f"[FINAL CONSERVATIVE SIZING] {symbol}: Size={final_position_size:,.0f}, Value=${final_value:.2f}, Risk=${actual_risk:.2f}, Method={sizing_info['method']}")
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

def round_price(symbol: str, price: float) -> float:
    """
    Round price to correct precision for OANDA instruments.
    JPY pairs: 3 decimals, others: 5 decimals.
    """
    if 'JPY' in symbol.replace('_', ''):
        return round(price, 3)
    return round(price, 5)


def enforce_min_distance(symbol: str, entry: float, target: float, is_tp: bool) -> float:
    """
    Enforce OANDA's minimum distance for SL/TP prices.
    For most FX: 5 pips (0.0005), for JPY pairs: 0.05.
    """
    min_dist = 0.0005 if 'JPY' not in symbol.replace('_', '') else 0.05
    if is_tp:
        if entry < target:
            return max(target, entry + min_dist)
        else:
            return min(target, entry - min_dist)
    else:
        if entry < target:
            return min(target, entry + min_dist)
        else:
            return max(target, entry - min_dist)

def enforce_min_tp_distance(symbol: str, entry: float, tp: float, action: str) -> float:
    """Ensure TP is at least 50 pips from entry (0.005 for non-JPY, 0.5 for JPY pairs)."""
    if tp is None:
        return tp
    # 50 pips: 0.005 for most FX, 0.5 for JPY pairs
    min_dist = 0.005 if 'JPY' not in symbol.replace('_', '') else 0.5
    if action.upper() == "SELL":
        # TP must be below entry
        if tp >= entry - min_dist:
            tp = entry - min_dist
    elif action.upper() == "BUY":
        # TP must be above entry
        if tp <= entry + min_dist:
            tp = entry + min_dist
    return tp

def validate_tp_with_slippage_buffer(entry_price, tp, action, symbol):
    """Validate TP with slippage buffer to prevent TAKE_PROFIT_ON_FILL_LOSS errors."""
    if tp is None:
        return tp
    
    logger.info(f"🔍 Validating TP with slippage buffer: {action} {symbol} entry={entry_price} TP={tp}")
    
    # Add slippage buffer: 20 pips for most pairs, 2.0 for JPY pairs (more conservative)
    slippage_buffer = 0.002 if 'JPY' not in symbol.replace('_', '') else 0.2
    
    original_tp = tp
    
    if action.upper() == "SELL":
        # For SELL, TP must be below entry by at least slippage buffer
        min_tp_distance = slippage_buffer
        if tp >= entry_price - min_tp_distance:
            tp = entry_price - min_tp_distance
            logger.warning(f"TP adjusted for slippage buffer (SELL): original={original_tp}, adjusted={tp}, distance={entry_price - tp}")
    elif action.upper() == "BUY":
        # For BUY, TP must be above entry by at least slippage buffer
        min_tp_distance = slippage_buffer
        if tp <= entry_price + min_tp_distance:
            tp = entry_price + min_tp_distance
            logger.warning(f"TP adjusted for slippage buffer (BUY): original={original_tp}, adjusted={tp}, distance={tp - entry_price}")
    
    logger.info(f"✅ TP validation complete: {action} {symbol} final_TP={tp}")
    return tp

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

# ===== INSTRUMENT PRECISION MAP & PRICE ROUNDING =====
INSTRUMENT_PRECISION = {
    "EUR_USD": 5,
    "USD_JPY": 3,
    "GBP_JPY": 3,
    "GBP_USD": 5,
    "USD_CHF": 5,
    "EUR_JPY": 3,
    "XAU_USD": 2,
    # Crypto symbols
    "BTC_USD": 1,
    "ETH_USD": 2,
    "LTC_USD": 2,
    "XRP_USD": 4,
    "BCH_USD": 2,
    "ADA_USD": 4,
    "DOT_USD": 3,
    "SOL_USD": 3,
    # ... add more as needed ...
}

def get_instrument_precision(symbol: str) -> int:
    symbol = symbol.replace("/", "_").upper()
    return INSTRUMENT_PRECISION.get(symbol, 5)  # Default to 5 if unknown

def round_price_for_instrument(price: float, symbol: str) -> float:
    precision = get_instrument_precision(symbol)
    return round(price, precision)

def validate_tp_sl(entry_price, tp, sl, action):
    """Ensure TP/SL are on the correct side of entry for the given action."""
    if action.upper() == "SELL":
        if tp is not None and tp >= entry_price:
            raise ValueError(f"SELL TP {tp} must be below entry {entry_price}")
        if sl is not None and sl <= entry_price:
            raise ValueError(f"SELL SL {sl} must be above entry {entry_price}")
    elif action.upper() == "BUY":
        if tp is not None and tp <= entry_price:
            raise ValueError(f"BUY TP {tp} must be above entry {entry_price}")
        if sl is not None and sl >= entry_price:
            raise ValueError(f"BUY SL {sl} must be below entry {entry_price}")

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
    'is_instrument_tradeable',
    'get_instrument_precision',
    'round_price_for_instrument',
    'validate_tp_sl',
    'enforce_min_tp_distance',
    'validate_tp_with_slippage_buffer'
]

class MetricsUtils:
    @staticmethod
    def calculate_pnl_dd_ratio(starting_equity: float, final_equity: float, max_drawdown_absolute: float) -> float:
        """
        Institutional PnL/DD (Calmar) Ratio.
        Args:
            starting_equity: Equity at the start of the period.
            final_equity: Equity at the end of the period.
            max_drawdown_absolute: Maximum drawdown (absolute, not percent).
        Returns:
            Ratio of net profit to max drawdown (dimensionless).
        """
        if max_drawdown_absolute == 0:
            return float('inf') if final_equity > starting_equity else 0.0
        return (final_equity - starting_equity) / abs(max_drawdown_absolute)

    @staticmethod
    def calculate_max_drawdown_absolute(equity_curve: list) -> float:
        """
        Calculate max drawdown (absolute) from an equity curve.
        """
        if not equity_curve:
            return 0.0
        peak = equity_curve[0]
        max_dd = 0.0
        for equity in equity_curve:
            if equity > peak:
                peak = equity
            dd = peak - equity
            if dd > max_dd:
                max_dd = dd
        return max_dd