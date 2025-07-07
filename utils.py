# utils.py - Complete version with all ATR and market analysis functions
import os
import logging
import json
import random
import asyncio
from typing import Tuple, Dict, Any, Optional
from config import config
from datetime import datetime, time, timezone, timedelta
import pytz

# --- Static Mappings and Constants ---

MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds
MAX_POSITIONS_PER_SYMBOL = 5

# Replace TV_FIELD_MAP in utils.py with this:

TV_FIELD_MAP = {
    "action": "direction",        # Maps TradingView "action" to expected "direction"
    "percentage": "risk_percent", # Maps TradingView "percentage" to expected "risk_percent"  
    "risk_percent": "risk_percent", # Pass through user's risk_percent field
    "ticker": "symbol",           # Maps TradingView "ticker" to "symbol"
    "symbol": "symbol",           # Pass through direct symbol
    "timeframe": "timeframe",     # Pass through
    "interval": "timeframe",      # Alternative timeframe field
    "alert_id": "alert_id",       # Pass through user's alert_id field
    "position_id": "position_id", # Pass through user's position_id field
    "timestamp": "timestamp",     # Pass through user's timestamp field
    "orderType": "orderType",     # Pass through
    "timeInForce": "timeInForce", # Pass through
    "account": "account",         # Pass through
    "comment": "comment",         # Pass through
    "exchange": "exchange",       # Pass through
    "strategy": "strategy",       # Pass through
    "side": "direction",          # Alternative direction field
    "risk": "risk_percent"        # Alternative risk field
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
    "USD_THB": 30,  # FIX: Add USD_THB with 30x leverage like other major pairs
    "EUR_GBP": 30,
    "EUR_JPY": 30,
    "GBP_JPY": 30,
    "AUD_CAD": 30,
    "AUD_CHF": 30,
    "CAD_CHF": 30,
    "CHF_JPY": 30,
    "EUR_AUD": 30,
    "EUR_CAD": 30,
    "EUR_CHF": 30,
    "GBP_AUD": 30,
    "GBP_CAD": 30,
    "GBP_CHF": 30,
    "NZD_CAD": 30,
    "NZD_CHF": 30,
    "NZD_JPY": 30,
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

def get_module_logger(name, **context):
    """Get a logger for a specific module with context"""
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
            if config
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
    """Normalize timeframe format"""
    if not tf:
        return "H1"
    
    tf_upper = tf.upper()
    
    # Common mappings
    timeframe_map = {
        "1M": "M1", "5M": "M5", "15M": "M15", "30M": "M30",
        "1H": "H1", "4H": "H4", "1D": "D1", "1W": "W1"
    }
    
    return timeframe_map.get(tf_upper, tf_upper)

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
    now = datetime.now(timezone.utc)
    # Check for weekend first (Saturday=5, Sunday=6)
    if now.weekday() >= 5:
        return 'weekend'
    h = now.hour
    if 22 <= h or h < 7:  # Asia session (approx. 22:00 UTC to 07:00 UTC)
        return 'asian'
    if 7 <= h < 16:  # London session (approx. 07:00 UTC to 16:00 UTC)
        return 'london'
    # New York session (approx. 16:00 UTC to 22:00 UTC)
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
    base = base_multipliers.get(instrument_type.lower(), 2.0)
    factor = timeframe_factors.get(normalized_timeframe, 1.0)
    result = base * factor
    logger.debug(f"[ATR MULTIPLIER] {instrument_type}:{normalized_timeframe} → base={base}, factor={factor}, multiplier={result}")
    return result

def get_atr_multiplier(instrument_type: str, timeframe: str) -> float:
    """
    Public method to retrieve ATR multiplier based on instrument type and timeframe.
    Falls back to a default multiplier if not found.
    """
    return _multiplier(instrument_type, timeframe)

class MarketDataUnavailableError(Exception):
    """Raised when market data cannot be fetched from the broker/API."""
    pass

def _get_simulated_price(symbol: str, direction: str) -> float:
    raise MarketDataUnavailableError(f"Simulated price fallback is disabled for live trading. Cannot fetch price for {symbol}.")

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
    return 0.01

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

EST = pytz.timezone('US/Eastern')

def is_instrument_tradeable(symbol: str) -> Tuple[bool, str]:
    """
    Returns (True, "") if the instrument is tradeable now according to trading hours.
    For crypto, validates symbol and returns trading status.
    For forex/other instruments, checks market hours.
    
    Args:
        symbol: Trading symbol to validate
        
    Returns:
        Tuple[bool, str]: (is_tradeable, reason_if_not_tradeable)
    """
    # Input validation
    if not symbol or not isinstance(symbol, str):
        return False, "Invalid symbol: symbol must be a non-empty string"
    
    symbol = symbol.strip().upper()
    if not symbol:
        return False, "Invalid symbol: empty symbol after cleanup"
    
    # Determine instrument type with validation
    try:
        instrument_type = get_instrument_type(symbol)
    except Exception as e:
        return False, f"Unable to determine instrument type: {str(e)}"
    
    # Crypto validation and trading status
    if instrument_type == "CRYPTO":
        # Validate crypto symbol format
        valid_crypto_symbols = {
            "BTC_USD", "ETH_USD", "LTC_USD", "XRP_USD", "BCH_USD", 
            "DOT_USD", "ADA_USD", "SOL_USD", "BTCUSD", "ETHUSD",
            "LTCUSD", "XRPUSD", "BCHUSD", "DOTUSD", "ADAUSD", "SOLUSD"
        }
        
        # Check if symbol is in our supported crypto list
        if symbol not in valid_crypto_symbols:
            # Try to extract base currency for validation
            if "_" in symbol:
                base_currency = symbol.split("_")[0]
            elif len(symbol) >= 6:
                # Extract from formats like BTCUSD
                base_currency = symbol[:3]
            else:
                return False, f"Invalid crypto symbol format: {symbol}"
            
            # Check if base currency is supported
            supported_cryptos = {"BTC", "ETH", "LTC", "XRP", "BCH", "DOT", "ADA", "SOL"}
            if base_currency not in supported_cryptos:
                return False, f"Unsupported crypto currency: {base_currency}"
        
        # Crypto trades 24/7 but check for exchange maintenance windows
        # Most crypto exchanges have brief maintenance windows
        now_utc = datetime.now(timezone.utc)
        hour = now_utc.hour
        
        # Example: Some exchanges have maintenance around 00:00-00:30 UTC
        if hour == 0 and now_utc.minute < 30:
            return False, "Crypto exchange may be in maintenance window (00:00-00:30 UTC)"
        
        return True, ""  # Crypto is tradeable 24/7 (outside maintenance)
    
    # Forex and other instruments - check market hours
    # Convert current UTC time to Eastern Time for forex market hours
    EST = pytz.timezone('US/Eastern')
    now_utc = datetime.now(timezone.utc)
    now_est = now_utc.astimezone(EST)

    weekday = now_est.weekday()  # Mon=0 … Sun=6
    t = now_est.time()

    # Forex market hours validation
    # Sunday before open: Market opens Sunday at 17:05 EST
    if weekday == 6 and t < time(17, 5):
        next_open = now_est.replace(hour=17, minute=5, second=0, microsecond=0)
        if t > time(17, 5):  # If it's Sunday evening after 17:05
            next_open += timedelta(days=1)  # Monday
        hours_until_open = (next_open - now_est).total_seconds() / 3600
        return False, f"Forex market closed. Opens Sunday 17:05 EST (in {hours_until_open:.1f} hours)"

    # Friday after close: Market closes Friday at 17:00 EST
    if weekday == 4 and t >= time(17, 0):
        return False, "Forex market closed for weekend. Reopens Sunday 17:05 EST"

    # Saturday all day
    if weekday == 5:
        # Calculate hours until Sunday opening
        sunday_open = now_est.replace(hour=17, minute=5, second=0, microsecond=0)
        sunday_open += timedelta(days=(6 - weekday))  # Days until Sunday
        hours_until_open = (sunday_open - now_est).total_seconds() / 3600
        return False, f"Forex market closed Saturday. Opens Sunday 17:05 EST (in {hours_until_open:.1f} hours)"

    # Additional instrument-specific validations
    if instrument_type == "COMMODITY":
        # Some commodities have specific trading hours
        if "XAU" in symbol or "XAG" in symbol:  # Gold/Silver
            # Metals trade nearly 24/5 like forex
            pass
        elif "OIL" in symbol or "WTI" in symbol:
            # Oil has specific hours - simplified check
            if weekday >= 5:  # Weekend
                return False, "Oil markets closed during weekend"
    
    elif instrument_type == "INDICES":
        # Stock indices have specific market hours
        if weekday >= 5:  # Weekend
            return False, "Stock indices closed during weekend"
        
        # Simplified hours check (varies by index)
        if t < time(9, 30) or t > time(16, 0):  # US market hours approximation
            return False, "Stock indices outside trading hours (approx 09:30-16:00 EST)"

    # If we reach here, instrument should be tradeable
    return True, ""

def validate_symbol_format(symbol: str) -> Tuple[bool, str]:
    """
    Validate symbol format and structure
    
    Args:
        symbol: Trading symbol to validate
        
    Returns:
        Tuple[bool, str]: (is_valid, error_message_if_invalid)
    """
    if not symbol or not isinstance(symbol, str):
        return False, "Symbol must be a non-empty string"
    
    symbol = symbol.strip().upper()
    if len(symbol) < 3:
        return False, "Symbol too short (minimum 3 characters)"
    
    if len(symbol) > 12:
        return False, "Symbol too long (maximum 12 characters)"
    
    # Check for valid characters (letters, numbers, underscore, slash)
    import re
    if not re.match(r'^[A-Z0-9_/]+$', symbol):
        return False, "Symbol contains invalid characters (only A-Z, 0-9, _, / allowed)"
    
    return True, ""

def parse_iso_datetime(datetime_str: str) -> datetime:
    """Parse ISO datetime string to datetime object"""
    try:
        # Handle various ISO formats
        if datetime_str.endswith('Z'):
            datetime_str = datetime_str[:-1] + '+00:00'
        return datetime.fromisoformat(datetime_str)
    except ValueError:
        # Fallback parsing
        try:
            import dateutil.parser
            return dateutil.parser.parse(datetime_str)
        except ImportError:
            # If dateutil not available, try basic parsing
            return datetime.now(timezone.utc)

async def get_current_price(symbol: str, action: str) -> float:
    try:
        from oandapyV20.endpoints.pricing import PricingInfo
        pricing_request = PricingInfo(
            accountID=config.oanda_account_id,
            params={"instruments": symbol}
        )
        try:
            from main import robust_oanda_request
            response = await robust_oanda_request(pricing_request)
        except ImportError:
            import oandapyV20
            access_token = config.oanda_access_token
            if hasattr(access_token, 'get_secret_value'):
                access_token = access_token.get_secret_value()
            api = oandapyV20.API(
                access_token=access_token,
                environment=config.oanda_environment
            )
            response = api.request(pricing_request)
        if 'prices' in response and response['prices']:
            price_data = response['prices'][0]
            if action.upper() == "BUY":
                return float(price_data.get('ask', price_data.get('closeoutAsk', 0)))
            else:
                return float(price_data.get('bid', price_data.get('closeoutBid', 0)))
        raise MarketDataUnavailableError(f"No price data returned for {symbol}.")
    except Exception as e:
        logger.error(f"Error getting current price for {symbol}: {e}")
        raise MarketDataUnavailableError(f"Failed to fetch current price for {symbol}: {e}")

async def get_account_balance(use_fallback: bool = False) -> float:
    """Get account balance from OANDA or return fallback"""
    if use_fallback:
        return 10000.0  # Fallback balance for startup
    
    try:
        # Import here to avoid circular import
        try:
            from main import robust_oanda_request
            from oandapyV20.endpoints.accounts import AccountDetails
            
            account_request = AccountDetails(accountID=config.oanda_account_id)
            response = await robust_oanda_request(account_request)
            return float(response['account']['balance'])
        except ImportError:
            # Fallback if main is not available
            import oandapyV20
            from oandapyV20.endpoints.accounts import AccountDetails
            
            access_token = config.oanda_access_token
            if hasattr(access_token, 'get_secret_value'):
                access_token = access_token.get_secret_value()
            
            api = oandapyV20.API(
                access_token=access_token,
                environment=config.oanda_environment
            )
            account_request = AccountDetails(accountID=config.oanda_account_id)
            response = api.request(account_request)
            return float(response['account']['balance'])
    except Exception as e:
        logger.error(f"Error getting account balance: {e}")
        return 10000.0  # Fallback

# ===== ATR CACHE (in-memory, 24h expiry) =====
_ATR_CACHE = {}  # (symbol, timeframe) -> {"atr": float, "timestamp": datetime}
_ATR_CACHE_MAX_AGE = timedelta(hours=24)

def is_fx_symbol(symbol: str) -> bool:
    # Simple heuristic: FX pairs are usually 6 chars with _ (e.g., EUR_USD)
    return isinstance(symbol, str) and len(symbol) == 7 and symbol[3] == "_"

async def fetch_historical_data(symbol: str, timeframe: str, count: int = 20) -> list:
    """
    Fetch candles with redundancy: OANDA → Alpha Vantage (FX) or yfinance (stocks) → error
    """
    # 1. Try OANDA
    try:
        from oandapyV20.endpoints.instruments import InstrumentsCandles
        oanda_timeframe = convert_timeframe_to_oanda(timeframe)
        params = {
            "granularity": oanda_timeframe,
            "count": count,
            "price": "M"
        }
        candles_request = InstrumentsCandles(
            instrument=symbol,
            params=params
        )
        try:
            from main import robust_oanda_request
            response = await robust_oanda_request(candles_request)
        except ImportError:
            import oandapyV20
            access_token = config.oanda_access_token
            if hasattr(access_token, 'get_secret_value'):
                access_token = access_token.get_secret_value()
            api = oandapyV20.API(
                access_token=access_token,
                environment=config.oanda_environment
            )
            response = api.request(candles_request)
        candles = []
        if 'candles' in response:
            for candle_data in response['candles']:
                if candle_data.get('complete', True):
                    mid_data = candle_data.get('mid', {})
                    candle = {
                        'time': candle_data.get('time'),
                        'open': float(mid_data.get('o', 0)),
                        'high': float(mid_data.get('h', 0)),
                        'low': float(mid_data.get('l', 0)),
                        'close': float(mid_data.get('c', 0)),
                        'volume': int(candle_data.get('volume', 0))
                    }
                    candles.append(candle)
        if not candles:
            raise Exception("No candle data returned from OANDA.")
        # Data validation
        for c in candles:
            if c['high'] <= 0 or c['low'] <= 0 or c['close'] <= 0:
                raise Exception("Invalid candle data from OANDA.")
        return candles
    except Exception as e:
        logger.warning(f"OANDA candle fetch failed for {symbol} {timeframe}: {e}")
    # 2. Try secondary source
    if is_fx_symbol(symbol):
        # Alpha Vantage FX
        try:
            from alpha_vantage.foreignexchange import ForeignExchange
            av = ForeignExchange(key=config.alpha_vantage_api_key, output_format='json')
            # Only daily supported for free Alpha Vantage FX
            if timeframe.upper() == "D1":
                data, _ = av.get_currency_exchange_daily(
                    from_symbol=symbol[:3],
                    to_symbol=symbol[4:],
                    outputsize='compact'
                )
                candles = []
                for t, v in list(data['Time Series FX (Daily)'].items())[:count][::-1]:
                    candles.append({
                        'time': t,
                        'open': float(v['1. open']),
                        'high': float(v['2. high']),
                        'low': float(v['3. low']),
                        'close': float(v['4. close']),
                        'volume': 0
                    })
                if not candles:
                    raise Exception("No candle data from Alpha Vantage FX.")
                return candles
            else:
                raise Exception("Alpha Vantage FX only supports daily candles for free API.")
        except Exception as e:
            logger.warning(f"Alpha Vantage candle fetch failed for {symbol} {timeframe}: {e}")
    else:
        # yfinance for stocks/commodities
        try:
            import yfinance as yf
            yf_symbol = symbol.replace('_', '-')
            tf_map = {"D1": "1d", "H1": "1h", "M15": "15m"}
            interval = tf_map.get(timeframe.upper(), "1d")
            ticker = yf.Ticker(yf_symbol)
            hist = ticker.history(period=f"{count}d", interval=interval)
            candles = []
            for idx, row in hist.iterrows():
                candles.append({
                    'time': idx.isoformat(),
                    'open': float(row['Open']),
                    'high': float(row['High']),
                    'low': float(row['Low']),
                    'close': float(row['Close']),
                    'volume': int(row['Volume']) if 'Volume' in row else 0
                })
            if not candles:
                raise Exception("No candle data from yfinance.")
            # Data validation
            for c in candles:
                if c['high'] <= 0 or c['low'] <= 0 or c['close'] <= 0:
                    raise Exception("Invalid candle data from yfinance.")
            return candles
        except Exception as e:
            logger.warning(f"yfinance candle fetch failed for {symbol} {timeframe}: {e}")
    # 3. If all fail, raise error
    raise MarketDataUnavailableError(f"Failed to fetch historical candles for {symbol} {timeframe} from all sources.")

async def get_atr(symbol: str, timeframe: str, period: int = 14) -> float:
    cache_key = (symbol, timeframe)
    now = datetime.now(timezone.utc)
    # 1. Try fresh ATR calculation
    try:
        candles = await fetch_historical_data(symbol, timeframe, count=period + 5)
        if len(candles) < period + 1:
            raise Exception(f"Insufficient data for ATR calculation: {len(candles)} candles, need {period + 1}")
        true_ranges = []
        for i in range(1, len(candles)):
            current = candles[i]
            previous = candles[i-1]
            high = float(current.get('high', current.get('h', 0)))
            low = float(current.get('low', current.get('l', 0)))
            prev_close = float(previous.get('close', previous.get('c', 0)))
            tr1 = high - low
            tr2 = abs(high - prev_close)
            tr3 = abs(low - prev_close)
            true_range = max(tr1, tr2, tr3)
            true_ranges.append(true_range)
        if len(true_ranges) < period:
            raise Exception(f"Insufficient true ranges calculated: {len(true_ranges)}, need {period}")
        atr_values = []
        first_atr = sum(true_ranges[:period]) / period
        atr_values.append(first_atr)
        for i in range(period, len(true_ranges)):
            current_tr = true_ranges[i]
            prev_atr = atr_values[-1]
            atr = ((prev_atr * (period - 1)) + current_tr) / period
            atr_values.append(atr)
        final_atr = atr_values[-1]
        # Data sanity check
        if final_atr <= 0 or final_atr > 1000:
            raise Exception(f"ATR value out of range: {final_atr}")
        # Cache result
        _ATR_CACHE[cache_key] = {"atr": final_atr, "timestamp": now}
        logger.debug(f"Calculated ATR for {symbol} {timeframe}: {final_atr:.6f} (fresh)")
        return final_atr
    except Exception as e:
        logger.warning(f"ATR calculation failed for {symbol} {timeframe} (fresh): {e}")
    # 2. Try cache if <24h old
    cache = _ATR_CACHE.get(cache_key)
    if cache:
        age = now - cache["timestamp"]
        if age < _ATR_CACHE_MAX_AGE:
            logger.warning(f"Using cached ATR for {symbol} {timeframe} (age: {age})")
            return cache["atr"]
        else:
            logger.warning(f"Cached ATR for {symbol} {timeframe} is too old (age: {age})")
    # 3. All failed
    raise MarketDataUnavailableError(f"Failed to calculate ATR for {symbol} {timeframe} from all sources and cache.")

def convert_timeframe_to_oanda(timeframe: str) -> str:
    """Convert timeframe to OANDA granularity format"""
    timeframe_map = {
        "M1": "M1",
        "M5": "M5", 
        "M15": "M15",
        "M30": "M30",
        "H1": "H1",
        "H4": "H4",
        "D1": "D",
        "W1": "W",
        "MN": "M"
    }
    
    normalized = normalize_timeframe(timeframe)
    return timeframe_map.get(normalized, "H1")

def generate_simulated_candles(symbol: str, count: int) -> list:
    raise MarketDataUnavailableError(f"Simulated candle fallback is disabled for live trading. Cannot fetch candles for {symbol}.")

def get_fallback_atr(symbol: str, timeframe: str) -> float:
    raise MarketDataUnavailableError(f"Fallback ATR is disabled for live trading. Cannot fetch ATR for {symbol} {timeframe}.")

def calculate_atr_from_data(candles: list, period: int = 14) -> float:
    """
    Calculate ATR from candle data using Wilder's method
    
    Args:
        candles: List of OHLC candle dictionaries
        period: ATR period (default 14)
        
    Returns:
        float: ATR value
    """
    if len(candles) < period + 1:
        return 0.001
    
    true_ranges = []
    
    for i in range(1, len(candles)):
        current = candles[i]
        previous = candles[i-1]
        
        high = float(current['high'])
        low = float(current['low'])
        prev_close = float(previous['close'])
        
        # Calculate True Range
        tr1 = high - low
        tr2 = abs(high - prev_close)
        tr3 = abs(low - prev_close)
        
        true_range = max(tr1, tr2, tr3)
        true_ranges.append(true_range)
    
    if len(true_ranges) < period:
        return 0.001
    
    # Calculate initial ATR (simple average)
    atr = sum(true_ranges[:period]) / period
    
    # Apply Wilder's smoothing for remaining periods
    for i in range(period, len(true_ranges)):
        atr = ((atr * (period - 1)) + true_ranges[i]) / period
    
    return atr

# ===== MULTI-TIMEFRAME AND ADVANCED ATR FUNCTIONS =====

async def get_multi_timeframe_atr(symbol: str, base_timeframe: str) -> Dict[str, float]:
    """
    Get ATR values for multiple timeframes to assess market volatility structure
    
    Args:
        symbol: Trading symbol
        base_timeframe: Base timeframe to analyze
        
    Returns:
        dict: ATR values for different timeframes
    """
    timeframes = ["M15", "H1", "H4", "D1"]
    atr_values = {}
    
    try:
        # Add the base timeframe if not in list
        if base_timeframe not in timeframes:
            timeframes.append(base_timeframe)
        
        # Fetch ATR for each timeframe
        for tf in timeframes:
            try:
                atr = await get_atr(symbol, tf)
                atr_values[tf] = atr
                await asyncio.sleep(0.1)  # Small delay to avoid rate limiting
            except Exception as e:
                logger.warning(f"Failed to get ATR for {symbol} {tf}: {e}")
                atr_values[tf] = get_fallback_atr(symbol, tf)
        
        logger.debug(f"Multi-timeframe ATR for {symbol}: {atr_values}")
        return atr_values
        
    except Exception as e:
        logger.error(f"Error getting multi-timeframe ATR for {symbol}: {e}")
        return {tf: get_fallback_atr(symbol, tf) for tf in timeframes}

def calculate_volatility_percentile(current_atr: float, atr_history: list) -> float:
    """
    Calculate what percentile the current ATR represents in historical context
    
    Args:
        current_atr: Current ATR value
        atr_history: List of historical ATR values
        
    Returns:
        float: Percentile (0-100) where current ATR sits
    """
    if not atr_history or len(atr_history) < 5:
        return 50.0  # Neutral if insufficient data
    
    try:
        sorted_history = sorted(atr_history)
        position = 0
        
        for atr_val in sorted_history:
            if current_atr > atr_val:
                position += 1
            else:
                break
        
        percentile = (position / len(sorted_history)) * 100
        return min(100.0, max(0.0, percentile))
        
    except Exception as e:
        logger.error(f"Error calculating volatility percentile: {e}")
        return 50.0

# async def get_dynamic_stop_distance(symbol: str, timeframe: str, volatility_factor: float = 1.0) -> float:
#     """
#     Calculate dynamic stop loss distance based on current market volatility
#     ...
#     """
#     pass

def get_instrument_leverage(symbol: str) -> float:
    # Use INSTRUMENT_LEVERAGES for all instruments, including crypto
    return INSTRUMENT_LEVERAGES.get(symbol, INSTRUMENT_LEVERAGES.get("default", 20))

def calculate_simple_position_size(account_balance: float, risk_percent: float, entry_price: float, stop_loss: float, symbol: str) -> float:
    """
    Universal position sizing for FX, crypto, stocks, etc.
    - Supports fractional units.
    - Checks margin requirements.
    - Clamps to instrument min/max.
    """
    leverage = get_instrument_leverage(symbol)
    min_units, max_units = get_position_size_limits(symbol)
    risk_amount = account_balance * (risk_percent / 100.0)
    stop_distance = abs(entry_price - stop_loss)
    
    # Minimum stop distance check (prevents extremely tight stops)
    min_stop_distance = entry_price * 0.001  # 0.1% minimum stop distance
    if stop_distance < min_stop_distance:
        logger.warning(f"Stop distance {stop_distance:.6f} too small for {symbol}, using minimum {min_stop_distance:.6f}")
        stop_distance = min_stop_distance
    
    if stop_distance <= 0 or entry_price <= 0:
        return 0.0
    
    raw_size = risk_amount / stop_distance
    
    # Margin check (for margin assets)
    required_margin = (raw_size * entry_price) / leverage
    available_margin = account_balance * 0.80  # Use 80% of balance for safety (more conservative)
    
    logger.info(f"[MARGIN CHECK] {symbol}: Raw size={raw_size:.2f}, Required margin=${required_margin:.2f}, Available margin=${available_margin:.2f}")
    
    if required_margin > available_margin:
        # Reduce position size to fit available margin
        raw_size = (available_margin * leverage) / entry_price
        logger.warning(f"[MARGIN LIMIT] {symbol}: Position reduced to {raw_size:.2f} units due to margin constraints")
    
    # Clamp to min/max
    position_size = max(min_units, min(max_units, raw_size))
    
    # Final margin validation
    final_required_margin = (position_size * entry_price) / leverage
    if final_required_margin > available_margin:
        # Emergency fallback: use fixed percentage of account
        position_size = (available_margin * leverage * 0.5) / entry_price  # Use only 50% of available margin
        logger.error(f"[EMERGENCY SIZING] {symbol}: Using emergency position size {position_size:.2f}")
    
    # Additional safety: Cap position size to prevent extremely large trades
    max_position_value = account_balance * 0.5  # Never risk more than 50% of account in one trade
    max_position_size = (max_position_value * leverage) / entry_price
    
    if position_size > max_position_size:
        position_size = max_position_size
        logger.warning(f"[POSITION CAP] {symbol}: Position capped to {position_size:.2f} units (50% account limit)")
    
    return position_size

def get_pip_value(symbol: str) -> float:
    """Get pip value for a symbol"""
    if "JPY" in symbol:
        return 0.01  # For JPY pairs, pip is 0.01
    elif any(commodity in symbol for commodity in ["XAU", "XAG", "OIL"]):
        return get_commodity_pip_value(symbol)
    else:
        return 0.0001  # Standard forex pip

def get_position_size_limits(symbol: str) -> Tuple[float, float]:
    """Get minimum and maximum position sizes for a symbol"""
    instrument_type = get_instrument_type(symbol)
    
    if instrument_type == "CRYPTO":
        # Extract base currency for crypto limits
        base_currency = symbol.split("_")[0] if "_" in symbol else symbol[:3]
        min_size = CRYPTO_MIN_SIZES.get(base_currency, 0.001)
        max_size = CRYPTO_MAX_SIZES.get(base_currency, 1.0)
    elif instrument_type == "COMMODITY":
        if "XAU" in symbol:
            min_size, max_size = 0.01, 100.0
        elif "XAG" in symbol:
            min_size, max_size = 0.1, 1000.0
        else:
            min_size, max_size = 0.1, 1000.0
    else:  # FOREX and others
        min_size, max_size = 1.0, 10000000.0  # Standard forex limits
    
    return min_size, max_size

async def validate_trade_setup(symbol: str, entry_price: float, stop_loss: float, take_profit: float, timeframe: str, broker: str = "OANDA") -> Dict[str, Any]:
    """
    Validate a complete trade setup using ATR and volatility analysis
    Args:
        symbol: Trading symbol
        entry_price: Proposed entry price
        stop_loss: Proposed stop loss
        take_profit: Proposed take profit
        timeframe: Analysis timeframe
        broker: Broker name (default 'OANDA')
    Returns:
        dict: Validation results with recommendations
    """
    validation = {
        "valid": True,
        "warnings": [],
        "recommendations": [],
        "risk_reward_ratio": 0.0,
        "atr_analysis": {}
    }
    try:
        # Get current ATR
        current_atr = await get_atr(symbol, timeframe)
        validation["atr_analysis"]["current_atr"] = current_atr
        # Calculate distances
        stop_distance = abs(entry_price - stop_loss)
        profit_distance = abs(take_profit - entry_price)
        # Calculate risk-reward ratio
        if stop_distance > 0:
            validation["risk_reward_ratio"] = profit_distance / stop_distance
        # ATR-based validations
        instrument_type = get_instrument_type(symbol)
        recommended_stop_mult = get_atr_multiplier(instrument_type, timeframe)
        recommended_stop_distance = current_atr * recommended_stop_mult
        validation["atr_analysis"]["recommended_stop_distance"] = recommended_stop_distance
        validation["atr_analysis"]["actual_stop_distance"] = stop_distance
        validation["atr_analysis"]["stop_atr_ratio"] = stop_distance / current_atr if current_atr > 0 else 0
        # Validation checks
        if stop_distance < current_atr * 0.5:
            validation["warnings"].append("Stop loss too tight - less than 0.5 ATR")
            validation["recommendations"].append(f"Consider widening stop to at least {current_atr * 0.8:.6f}")
        if stop_distance > current_atr * 4.0:
            validation["warnings"].append("Stop loss very wide - more than 4 ATR")
            validation["recommendations"].append("Consider tightening stop or reducing position size")
        if validation["risk_reward_ratio"] < 1.5:
            validation["warnings"].append(f"Low risk-reward ratio: {validation['risk_reward_ratio']:.2f}")
            validation["recommendations"].append("Consider better entry or adjust targets for RR > 1.5")
        # Market session considerations (OANDA only)
        if broker.upper() == "OANDA":
            session = get_current_market_session()
            if session == "weekend":
                validation["warnings"].append("Weekend trading - low liquidity expected")
            elif session == "asian" and "USD" in symbol:
                validation["warnings"].append("Asian session for USD pairs - potentially lower volatility")
        # Final validation
        if len(validation["warnings"]) > 2:
            validation["valid"] = False
            validation["recommendations"].append("Consider reviewing trade setup before execution")
        logger.debug(f"Trade validation for {symbol}: {validation}")
        return validation
    except Exception as e:
        logger.error(f"Error validating trade setup for {symbol}: {e}")
        return {
            "valid": False,
            "error": str(e),
            "warnings": ["Validation failed due to technical error"],
            "recommendations": ["Manual review required"]
        }

def validate_trade_inputs(
    units: float, 
    risk_percent: float, 
    atr: float, 
    stop_loss_distance: float,
    min_units: int,
    max_units: int
) -> tuple[bool, str]:
    """
    Checks if trade inputs are valid, clamped, and safe to submit.
    Returns (is_valid, message)
    """
    if units is None or units <= 0:
        return False, f"Calculated units ({units}) is zero or negative."
    
    # Convert to int for comparison since OANDA expects whole numbers for most instruments
    units_int = int(units)
    
    if units_int < min_units:
        return False, f"Units ({units_int}) below minimum trade size ({min_units})."
    
    if units_int > max_units:
        return False, f"Units ({units_int}) above maximum allowed ({max_units})."
    
    if risk_percent is None or risk_percent <= 0:
        return False, f"Risk percent ({risk_percent}) is zero or negative."
    
    if atr is None or atr <= 0:
        return False, f"ATR ({atr}) is zero or negative."
    
    if stop_loss_distance is None or stop_loss_distance <= 0:
        return False, f"Stop loss distance ({stop_loss_distance}) is zero or negative."
    
    return True, "Inputs valid"

def get_volatility_stop_loss_modifier(symbol: str) -> float:
    """Get stop loss modifier based on volatility (placeholder)"""
    # This is a placeholder - you can implement your volatility logic here
    return 1.0

class VolatilityMonitor:
    """Simple volatility monitor for utils fallback"""
    def get_stop_loss_modifier(self, symbol: str) -> float:
        return get_volatility_stop_loss_modifier(symbol)


def resolve_template_symbol(alert_data: Dict[str, Any]) -> Optional[str]:
    """
    Resolve {{ticker}} template variables and handle TradingView symbol formats
    Returns the resolved symbol or None if unable to resolve
    """
    
    # Method 1: Check if TradingView passes symbol in other fields
    for field in ["ticker", "symbol", "instrument", "pair", "asset"]:
        if field in alert_data and alert_data[field] not in ["{{ticker}}", "{{symbol}}", "{{instrument}}"]:
            symbol = alert_data[field]
            
            # Clean up TradingView symbol format
            if ":" in symbol:
                # Remove exchange prefix: "OANDA:EURUSD" → "EURUSD"
                symbol = symbol.split(":")[1]
            
            # Handle common TradingView symbol formats
            symbol = symbol.upper().strip()
            
            # Convert common formats
            if len(symbol) == 6 and symbol.isalpha():
                # Convert EURUSD to EUR_USD
                if symbol not in ["BTCUSD", "ETHUSD", "XAUUSD", "XAGUSD"]:  # Keep crypto/metals as-is for now
                    symbol = f"{symbol[:3]}_{symbol[3:]}"
            
            logger.info(f"[TEMPLATE] Found and cleaned symbol in '{field}': {symbol}")
            return symbol
    
    # Method 2: Extract from comment if it contains symbol info
    if "comment" in alert_data:
        comment = alert_data["comment"]
        # Look for common patterns like "EUR/USD Signal" or "EURUSD Long"
        import re
        symbol_patterns = [
            r'([A-Z]{3}[/_]?[A-Z]{3})',  # EURUSD or EUR/USD or EUR_USD
            r'([A-Z]{6})',                # EURUSD
            r'(XAU[/_]?USD)',            # Gold
            r'(BTC[/_]?USD)',            # Bitcoin
        ]
        for pattern in symbol_patterns:
            match = re.search(pattern, comment.upper())
            if match:
                symbol = match.group(1)
                logger.info(f"[TEMPLATE] Extracted symbol from comment: {symbol}")
                return symbol
    
    # Method 3: Default fallback for testing
    logger.warning("[TEMPLATE] Using default fallback: EUR_USD")
    return "EUR_USD"

# Also add this function to better handle TradingView symbols:

def clean_tradingview_symbol(symbol: str) -> str:
    """
    Clean and standardize symbols from TradingView
    """
    if not symbol:
        return ""
    
    # Remove exchange prefix
    if ":" in symbol:
        symbol = symbol.split(":")[1]
    
    # Clean and uppercase
    symbol = symbol.upper().strip()
    
    # Handle special cases
    special_mappings = {
        "BTCUSD": "BTC_USD",
        "ETHUSD": "ETH_USD", 
        "XAUUSD": "XAU_USD",
        "XAGUSD": "XAG_USD",
    }
    
    if symbol in special_mappings:
        return special_mappings[symbol]
    
    # Convert 6-character forex pairs
    if len(symbol) == 6 and symbol.isalpha():
        return f"{symbol[:3]}_{symbol[3:]}"
    
    return symbol

def setup_production_logging():
    """Production-grade logging with structured output"""
    import logging.handlers
    # Create rotating file handler
    rotating_handler = logging.handlers.RotatingFileHandler(
        'logs/trading_system.log',
        maxBytes=50_000_000,  # 50MB
        backupCount=10
    )
    # Add critical alerts handler
    critical_handler = logging.handlers.SMTPHandler(
        mailhost='smtp.gmail.com',
        fromaddr='bot@yourcompany.com', 
        toaddrs=['alerts@yourcompany.com'],
        subject='CRITICAL: Trading Bot Alert'
    )
    critical_handler.setLevel(logging.CRITICAL)
    return [rotating_handler, critical_handler]

async def check_market_impact(symbol: str, position_size: float, timeframe: str = "D1", warn_threshold: float = None, cap_threshold: float = None) -> tuple:
    """
    Estimate market impact for a trade and enforce warning/cap thresholds.
    Returns (is_allowed: bool, warning: Optional[str], percent: float, volume: float, threshold: float)
    """
    from config import config
    import math
    logger = logging.getLogger("trading_system")
    if warn_threshold is None:
        warn_threshold = getattr(config, "market_impact_warn_percent", None) or getattr(config, "max_position_volume_percent", 5.0) or 5.0
    if cap_threshold is None:
        cap_threshold = getattr(config, "market_impact_cap_percent", None) or 5.0
    # Always ensure warn < cap
    warn_threshold = min(warn_threshold, cap_threshold)
    try:
        # Fetch recent daily volume
        instrument_type = get_instrument_type(symbol)
        volume = None
        if instrument_type in ("STOCK", "COMMODITY", "ETF"):
            try:
                import yfinance as yf
                yf_symbol = symbol.replace('_', '-')
                ticker = yf.Ticker(yf_symbol)
                hist = ticker.history(period="2d", interval="1d")
                if not hist.empty:
                    volume = float(hist["Volume"].iloc[-1])
            except Exception as e:
                logger.warning(f"Market impact: Failed to fetch volume for {symbol} via yfinance: {e}")
        elif instrument_type == "FOREX":
            # OANDA volume is tick volume, not real volume; warn and skip
            logger.info(f"Market impact: Skipping volume check for FX symbol {symbol} (no reliable volume data)")
            return True, None, 0.0, 0.0, cap_threshold
        else:
            logger.info(f"Market impact: No volume check for instrument type {instrument_type}")
            return True, None, 0.0, 0.0, cap_threshold
        if not volume or volume <= 0:
            logger.warning(f"Market impact: No valid volume data for {symbol}. Skipping impact check.")
            return True, None, 0.0, 0.0, cap_threshold
        percent = 100.0 * position_size / volume
        if percent >= cap_threshold:
            logger.error(f"Market impact: Trade size {position_size} is {percent:.2f}% of daily volume ({volume}) for {symbol}. Hard cap at {cap_threshold}% exceeded.")
            return False, f"Trade size ({percent:.2f}%) exceeds hard cap ({cap_threshold}%) of daily volume.", percent, volume, cap_threshold
        elif percent >= warn_threshold:
            logger.warning(f"Market impact: Trade size {position_size} is {percent:.2f}% of daily volume ({volume}) for {symbol}. Warning threshold ({warn_threshold}%) exceeded.")
            return True, f"Trade size ({percent:.2f}%) exceeds warning threshold ({warn_threshold}%) of daily volume.", percent, volume, warn_threshold
        else:
            logger.info(f"Market impact: Trade size {position_size} is {percent:.2f}% of daily volume ({volume}) for {symbol}. No warning.")
            return True, None, percent, volume, warn_threshold
    except Exception as e:
        logger.error(f"Market impact: Error estimating market impact for {symbol}: {e}")
        return True, None, 0.0, 0.0, cap_threshold

async def analyze_transaction_costs(symbol: str, position_size: float, action: str = "BUY") -> dict:
    """
    Analyze transaction costs (spread + commission) for a trade.
    Returns a dict with spread_pips, spread_percent, commission_per_unit, commission_total, estimated_total_cost, and a summary string.
    """
    from config import config
    import math
    logger = logging.getLogger("trading_system")
    result = {
        "spread_pips": 0.0,
        "spread_percent": 0.0,
        "commission_per_unit": 0.0,
        "commission_total": 0.0,
        "estimated_total_cost": 0.0,
        "summary": ""
    }
    try:
        instrument_type = get_instrument_type(symbol)
        if instrument_type == "FOREX":
            # Fetch both bid and ask
            try:
                from oandapyV20.endpoints.pricing import PricingInfo
                pricing_request = PricingInfo(
                    accountID=config.oanda_account_id,
                    params={"instruments": symbol}
                )
                from main import robust_oanda_request
                response = await robust_oanda_request(pricing_request)
                if 'prices' in response and response['prices']:
                    price_data = response['prices'][0]
                    bid = float(price_data.get('bid', price_data.get('closeoutBid', 0)))
                    ask = float(price_data.get('ask', price_data.get('closeoutAsk', 0)))
                    spread = ask - bid
                    pip_value = get_pip_value(symbol)
                    spread_pips = spread / pip_value if pip_value else 0.0
                    spread_percent = 100.0 * spread / ((ask + bid) / 2) if (ask + bid) > 0 else 0.0
                    # OANDA retail FX: commission is usually 0, but allow config override
                    commission_per_unit = getattr(config, "commission_per_unit", 0.0)
                    commission_total = commission_per_unit * position_size
                    estimated_total_cost = (spread * position_size) + commission_total
                    result.update({
                        "spread_pips": spread_pips,
                        "spread_percent": spread_percent,
                        "commission_per_unit": commission_per_unit,
                        "commission_total": commission_total,
                        "estimated_total_cost": estimated_total_cost,
                        "summary": f"Spread: {spread_pips:.2f} pips ({spread_percent:.4f}%), Commission: {commission_total:.2f}, Total cost: {estimated_total_cost:.2f}"
                    })
                else:
                    logger.warning(f"Transaction cost: No price data for {symbol}")
            except Exception as e:
                logger.error(f"Transaction cost: Error fetching FX prices for {symbol}: {e}")
        else:
            # Stocks/commodities: use yfinance for last close, estimate commission
            try:
                import yfinance as yf
                yf_symbol = symbol.replace('_', '-')
                ticker = yf.Ticker(yf_symbol)
                hist = ticker.history(period="2d", interval="1d")
                if not hist.empty:
                    last_close = float(hist["Close"].iloc[-1])
                    # Spread is not always available; set to 0 or estimate if possible
                    spread = 0.0
                    spread_pips = 0.0
                    spread_percent = 0.0
                    commission_rate = getattr(config, "stock_commission_rate", 0.001)  # 0.1% per side default
                    commission_total = commission_rate * position_size * last_close
                    estimated_total_cost = commission_total
                    result.update({
                        "spread_pips": spread_pips,
                        "spread_percent": spread_percent,
                        "commission_per_unit": commission_rate * last_close,
                        "commission_total": commission_total,
                        "estimated_total_cost": estimated_total_cost,
                        "summary": f"Commission: {commission_total:.2f} (at {commission_rate*100:.3f}%), Total cost: {estimated_total_cost:.2f}"
                    })
                else:
                    logger.warning(f"Transaction cost: No price data for {symbol}")
            except Exception as e:
                logger.error(f"Transaction cost: Error fetching stock/commodity prices for {symbol}: {e}")
        logger.info(f"Transaction cost analysis for {symbol}: {result['summary']}")
        return result
    except Exception as e:
        logger.error(f"Transaction cost: Error analyzing costs for {symbol}: {e}")
        return result

def round_position_size(symbol: str, position_size: float) -> float:
    """
    Round position size to appropriate precision based on instrument type and broker requirements
    
    Args:
        symbol: Trading symbol
        position_size: Raw calculated position size
        
    Returns:
        float: Properly rounded position size
    """
    try:
        instrument_type = get_instrument_type(symbol)
        
        # OANDA precision requirements
        if instrument_type == "FOREX":
            # Forex: ALWAYS round to whole units for OANDA (no decimals allowed)
            return round(position_size)
                
        elif instrument_type == "CRYPTO":
            # Crypto: Different precision based on currency
            if "BTC" in symbol:
                return round(position_size, 4)  # Bitcoin: 4 decimals
            elif "ETH" in symbol:
                return round(position_size, 3)  # Ethereum: 3 decimals
            else:
                return round(position_size, 2)  # Other crypto: 2 decimals
                
        elif instrument_type == "COMMODITY":
            # Commodities: Usually whole units or 1 decimal
            if "XAU" in symbol or "XAG" in symbol:  # Gold/Silver
                return round(position_size, 1)
            else:
                return round(position_size)
                
        else:
            # Default: Round to whole units
            return round(position_size)
            
    except Exception as e:
        logger.error(f"Error rounding position size for {symbol}: {e}")
        return round(position_size)  # Fallback to whole units

def calculate_notional_position_size(account_balance: float, allocation_percent: float, current_price: float, symbol: str) -> float:
    """
    Position size based on a fixed percent of account equity (notional allocation), ignoring stop loss.
    Args:
        account_balance: Current account equity
        allocation_percent: Percent of equity to allocate (e.g., 15 for 15%)
        current_price: Current market price
        symbol: Trading symbol
    Returns:
        float: Position size in units
    """
    leverage = get_instrument_leverage(symbol)
    notional_value = account_balance * (allocation_percent / 100.0) * leverage
    position_size = notional_value / current_price
    # Optionally: round to OANDA requirements
    position_size = round_position_size(symbol, position_size)
    return position_size
