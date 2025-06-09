# utils.py
import os
import logging
import json
from datetime import datetime
import random
from time import timezone
from typing import Tuple

from distutils.command import config

# --- Static Mappings and Constants ---

MAX_DAILY_LOSS = config.max_daily_loss / 100 
MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds
MAX_POSITIONS_PER_SYMBOL = 5

TV_FIELD_MAP = {
    "ticker": "instrument",
    "side": "direction",
    "risk": "risk_percent",
    "entry": "entry_price",
    "sl": "stop_loss",
    "tp": "take_profit",
    "tf": "timeframe",
}

INSTRUMENT_LEVERAGES = {
    "XAU_USD": 20,
    "XAG_USD": 20,
    "EUR_USD": 30,
    "GBP_USD": 30,
    "USD_JPY": 30,
    "USD_CHF": 30,
    "AUD_USD": 30,
    "NZD_USD": 30,
    "USD_CAD": 30,
    "BTC_USD": 2,
    "ETH_USD": 5,
    "default": 20,
}

CRYPTO_MAPPING = {
    "BTCUSD": "BTC_USD",
    "ETHUSD": "ETH_USD",
    "LTCUSD": "LTC_USD",
    "XRPUSD": "XRP_USD",
    "BCHUSD": "BCH_USD",
    "DOTUSD": "DOT_USD",
    "ADAUSD": "ADA_USD",
    "SOLUSD": "SOL_USD",
    "BTCUSD:OANDA": "BTC_USD",
    "ETHUSD:OANDA": "ETH_USD",
    "BTC/USD": "BTC_USD",
    "ETH/USD": "ETH_USD",
}

CRYPTO_MIN_SIZES = {
    "BTC": 0.0001,
    "ETH": 0.002,
    "LTC": 0.05,
    "XRP": 0.01,
    "XAU": 0.2,  # Gold minimum
}

CRYPTO_MAX_SIZES = {
    "BTC": 10,
    "ETH": 135,
    "LTC": 3759,
    "XRP": 50000,
    "XAU": 500,  # Gold maximum
}

CRYPTO_TICK_SIZES = {
    "BTC": 0.001,
    "ETH": 0.05,
    "LTC": 0.01,
    "XRP": 0.001,
    "XAU": 0.01,  # Gold tick size
}

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "timestamp": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "thread_id": record.thread,
            "process_id": record.process,
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        if hasattr(record, "extra_data"):
            log_data.update(record.extra_data)
        if hasattr(record, "position_id"):
            log_data["position_id"] = record.position_id
        if hasattr(record, "symbol"):
            log_data["symbol"] = record.symbol
        if hasattr(record, "request_id"):
            log_data["request_id"] = record.request_id
        return json.dumps(log_data)

def setup_logging():
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    json_formatter = JSONFormatter()
    main_handler = logging.FileHandler(os.path.join(log_dir, "trading_system.log"))
    main_handler.setFormatter(json_formatter)
    main_handler.setLevel(logging.INFO)
    logger.addHandler(main_handler)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    return logger

logger = setup_logging()

class TradingLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        context = self.extra.copy()
        if "extra" in kwargs:
            context.update(kwargs["extra"])
        kwargs["extra"] = context
        return msg, kwargs

def get_trading_logger(name, **context):
    return TradingLogger(logging.getLogger(name), context)

def standardize_symbol(symbol: str) -> str:
    """Standardize symbol format with robust error handling and support for various formats"""
    if not symbol:
        return ""
    try:
        symbol_upper = symbol.upper().replace('-', '_').replace('/', '_')

        # Crypto mapping override (do this first)
        if symbol_upper in CRYPTO_MAPPING:
            return CRYPTO_MAPPING[symbol_upper]

        # If already in "XXX_YYY" form and both parts look valid, return it
        if "_" in symbol_upper and len(symbol_upper.split("_")) == 2:
            base, quote = symbol_upper.split("_")
            if len(base) >= 3 and len(quote) >= 3:
                return symbol_upper

        # Special handling for JPY pairs (6-char, no underscore)
        if "JPY" in symbol_upper and "_" not in symbol_upper and len(symbol_upper) == 6:
            return symbol_upper[:3] + "_" + symbol_upper[3:]

        # 6-char Forex pairs (not crypto)
        if (
            len(symbol_upper) == 6
            and not any(crypto in symbol_upper for crypto in ["BTC", "ETH", "XRP", "LTC", "BCH", "DOT", "ADA", "SOL"])
        ):
            return f"{symbol_upper[:3]}_{symbol_upper[3:]}"

        # Fallback for crypto pairs without mapping: e.g. "BTCUSD"
        for crypto in ["BTC", "ETH", "LTC", "XRP", "BCH", "DOT", "ADA", "SOL"]:
            if crypto in symbol_upper and "USD" in symbol_upper:
                return f"{crypto}_USD"

        # Broker-specific default
        active_exchange = (
            getattr(config, "active_exchange", "").lower()
            if "config" in globals()
            else "oanda"
        )
        if active_exchange == "oanda":
            return symbol_upper
        elif active_exchange == "binance":
            return symbol_upper.replace("_", "")

        # Final fallback
        return symbol_upper

    except Exception as e:
        logger.error(f"Error standardizing symbol {symbol}: {e}")
        return symbol.upper() if symbol else ""

def normalize_timeframe(tf: str, *, target: str = "OANDA") -> str:
    # ...use the most robust version from your codebase...
    return tf.upper()

def format_jpy_pair(symbol: str) -> str:
    """Properly format JPY pairs for OANDA"""
    if "JPY" in symbol and "_" not in symbol:
        if len(symbol) == 6:
            return symbol[:3] + "_" + symbol[3:]
        elif "/" in symbol:
            return symbol.replace("/", "_")
    return symbol

def format_for_oanda(symbol: str) -> str:
    if "_" in symbol:
        return symbol
    if len(symbol) == 6:
        return symbol[:3] + "_" + symbol[3:]
    return symbol

def get_current_market_session() -> str:
    """Return 'asian', 'london', 'new_york', or 'weekend' by UTC now."""
    now = datetime.utcnow()
    # Check for weekend first (Saturday=5, Sunday=6)
    if now.weekday() >= 5:
        return 'weekend'
    h = now.hour
    if 22 <= h or h < 7:  # Asia session (approx. 22:00 UTC to 07:00 UTC)
        return 'asian'
    if 7 <= h < 16:  # London session (approx. 07:00 UTC to 16:00 UTC)
        return 'london'
    # New York session (approx. 16:00 UTC to 22:00 UTC)
    # Note: NY often considered 13:00-22:00 UTC, but overlap starts earlier
    return 'new_york'

def _multiplier(instrument_type: str, timeframe: str) -> float:
    base_multipliers = {
        "forex": 2.0,
        "jpy_pair": 2.5,
        "metal": 1.5,
        "index": 2.0,
        "other": 2.0
    }
    timeframe_factors = {
        "M1": 1.5,
        "M5": 1.3,
        "M15": 1.2,
        "M30": 1.1,
        "H1": 1.0,
        "H4": 0.9,
        "D1": 0.8,
        "W1": 0.7
    }
    normalized_timeframe = normalize_timeframe(timeframe)
    base = base_multipliers.get(instrument_type.lower())
    factor = timeframe_factors.get(normalized_timeframe)
    if base is None:
        logger.warning(f"[ATR MULTIPLIER] Unknown instrument type '{instrument_type}', using default base of 2.0")
        base = 2.0
    if factor is None:
        logger.warning(f"[ATR MULTIPLIER] Unknown timeframe '{normalized_timeframe}', using default factor of 1.0")
        factor = 1.0
    result = base * factor
    logger.debug(f"[ATR MULTIPLIER] {instrument_type}:{normalized_timeframe} → base={base}, factor={factor}, multiplier={result}")
    return result

def get_atr_multiplier(instrument_type: str, timeframe: str) -> float:
    """
    Public method to retrieve ATR multiplier based on instrument type and timeframe.
    Falls back to a default multiplier if not found.
    """
    return _multiplier(instrument_type, timeframe)

def _get_simulated_price(symbol: str, direction: str) -> float:
    """Generate a simulated price when real price data is unavailable"""
    base_prices = {
        "EUR_USD": 1.10,
        "GBP_USD": 1.25,
        "USD_JPY": 110.0,
        "GBP_JPY": 175.0,
        "XAU_USD": 1900.0,
        "BTC_USD": 75000.0,
        "ETH_USD": 3500.0
    }
    base_price = base_prices.get(symbol, 100.0)
    variation = random.uniform(-0.001, 0.001)
    price = base_price * (1 + variation)
    spread_factor = 1.0005 if direction.upper() == "BUY" else 0.9995
    return price * spread_factor

def is_position_open(pos: dict) -> bool:
    """Return True if the position is open and not closed."""
    return pos.get("status") == "OPEN" and not pos.get("closed")

def get_commodity_pip_value(instrument: str) -> float:
    """Return the pip value for commodities based on instrument name."""
    inst = instrument.upper()
    if 'XAU' in inst:   return 0.01
    if 'XAG' in inst:   return 0.001
    if 'OIL' in inst or 'WTICO' in inst: return 0.01
    if 'NATGAS' in inst: return 0.001
    return 0.

def get_higher_timeframe(timeframe: str) -> str:
    """Get the next higher timeframe based on current timeframe."""
    timeframe_hierarchy = {
        "M1": "M15",
        "M5": "M30",
        "M15": "H1",
        "M30": "H4",
        "H1": "H4",
        "H4": "D1",
        "D1": "W1",
        "W1": "MN"
    }
    normalized_tf = normalize_timeframe(timeframe)
    return timeframe_hierarchy.get(normalized_tf, normalized_tf)

    def get_instrument_type(instrument: str) -> str:
        """
        Determine instrument type from symbol.
        Returns one of: 'FOREX', 'CRYPTO', 'COMMODITY', 'INDICES'.
        """
    try:
        # Handle None or empty input
        if not instrument:
            logger.warning("Empty instrument provided, defaulting to FOREX")
            return "FOREX"
            
        inst = instrument.upper()
        
        # Define comprehensive lists for identification
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
                    
                # Check Index (Base only, e.g., US30_USD)
                if base in index_list:
                    return "INDICES"
                    
                # Check Forex (standard 3-letter codes)
                if len(base) == 3 and len(quote) == 3 and base.isalpha() and quote.isalpha():
                    # Exclude if base is a commodity (e.g., XAU_CAD) - should be COMMODITY
                    if base not in commodity_list:
                        return "FOREX"
                    else:
                        return "COMMODITY"  # e.g., XAU_EUR is a commodity trade
        
        # Handle no-underscore format
        else:
            # Check Crypto (e.g., BTCUSD, ETHUSD)
            for crypto in crypto_list:
                if inst.startswith(crypto):
                    # Basic check: Starts with crypto and has common quote
                    if any(inst.endswith(q) for q in ["USD", "EUR", "USDT", "GBP", "JPY"]):
                        return "CRYPTO"
            
            # Check Commodity (e.g., XAUUSD, WTICOUSD)
            for comm in commodity_list:
                if inst.startswith(comm):
                    if any(inst.endswith(q) for q in ["USD", "EUR", "GBP", "JPY"]):
                        return "COMMODITY"
            
            # Check Index (e.g., US30USD, NAS100USD)
            for index in index_list:
                if inst.startswith(index):
                    if any(inst.endswith(q) for q in ["USD", "EUR", "GBP", "JPY"]):
                        return "INDICES"
            
            # Check standard 6-char Forex (e.g., EURUSD)
            if len(inst) == 6 and inst.isalpha():
                # Additional check for commodity pairs without underscore
                for comm in commodity_list:
                    if inst.startswith(comm):
                        return "COMMODITY"
                return "FOREX"

        # Default if no specific type matched
        logger.warning(f"Could not determine specific instrument type for '{instrument}', defaulting to FOREX.")
        return "FOREX"
        
    except Exception as e:
        logger.error(f"Error determining instrument type for '{instrument}': {str(e)}")
        return "FOREX"  # Default fallback

def is_instrument_tradeable(symbol: str) -> Tuple[bool, str]:
    """Check if an instrument is currently tradeable based on market hours"""
    now = datetime.now(timezone.utc)
    instrument_type = get_instrument_type(symbol)
    
    # Special handling for JPY pairs
    if "JPY" in symbol:
        instrument_type = "jpy_pair"
    
    if instrument_type in ["forex", "jpy_pair", "metal"]:
        if now.weekday() >= 5:
            return False, "Weekend - Market closed"
        if now.weekday() == 4 and now.hour >= 21:
            return False, "Weekend - Market closed"
        if now.weekday() == 0 and now.hour < 21:
            return False, "Market not yet open"
        return True, "Market open"
    
    if instrument_type == "index":
        if "SPX" in symbol or "NAS" in symbol:
            if now.weekday() >= 5:
                return False, "Weekend - Market closed"
            if not (13 <= now.hour < 20):
                return False, "Outside market hours"
        return True, "Market open"
    
    return True, "Market assumed open"