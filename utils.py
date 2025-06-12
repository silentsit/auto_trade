# utils.py - Complete version with all ATR and market analysis functions
import os
import logging
import json
import random
import asyncio
from datetime import datetime, timezone
from typing import Tuple, Dict, Any, Optional
from config import config

# --- Static Mappings and Constants ---

MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds
MAX_POSITIONS_PER_SYMBOL = 5

# Replace TV_FIELD_MAP in utils.py with this:

TV_FIELD_MAP = {
    "action": "direction",        # Maps TradingView "action" to expected "direction"
    "percentage": "risk_percent", # Maps TradingView "percentage" to expected "risk_percent"  
    "ticker": "symbol",           # Maps TradingView "ticker" to "symbol"
    "symbol": "symbol",           # Pass through direct symbol
    "timeframe": "timeframe",     # Pass through
    "interval": "timeframe",      # Alternative timeframe field
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

def is_instrument_tradeable(symbol: str) -> Tuple[bool, str]:
    """Check if an instrument is currently tradeable based on market hours"""
    now = datetime.now(timezone.utc)
    instrument_type = get_instrument_type(symbol)
    
    # Special handling for JPY pairs
    if "JPY" in symbol:
        instrument_type = "jpy_pair"
    
    if instrument_type in ["FOREX", "jpy_pair", "COMMODITY"]:
        if now.weekday() >= 5:
            return False, "Weekend - Market closed"
        if now.weekday() == 4 and now.hour >= 21:
            return False, "Weekend - Market closed"
        if now.weekday() == 0 and now.hour < 21:
            return False, "Market not yet open"
        return True, "Market open"
    
    if instrument_type == "INDICES":
        if "SPX" in symbol or "NAS" in symbol:
            if now.weekday() >= 5:
                return False, "Weekend - Market closed"
            if not (13 <= now.hour < 20):
                return False, "Outside market hours"
        return True, "Market open"
    
    return True, "Market assumed open"

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
    """Get current price for symbol"""
    try:
        # Import here to avoid circular import
        from oandapyV20.endpoints.pricing import PricingInfo
        
        pricing_request = PricingInfo(
            accountID=config.oanda_account_id,
            params={"instruments": symbol}
        )
        
        # Import here to avoid circular import
        from main import robust_oanda_request
        response = await robust_oanda_request(pricing_request)
        
        if 'prices' in response and response['prices']:
            price_data = response['prices'][0]
            if action.upper() == "BUY":
                return float(price_data.get('ask', price_data.get('closeoutAsk', 0)))
            else:
                return float(price_data.get('bid', price_data.get('closeoutBid', 0)))
    except Exception as e:
        logger.error(f"Error getting current price for {symbol}: {e}")
        
    # Fallback to simulated price
    return _get_simulated_price(symbol, action)

async def get_account_balance(use_fallback: bool = False) -> float:
    """Get account balance from OANDA or return fallback"""
    if use_fallback:
        return 10000.0  # Fallback balance for startup
    
    try:
        # Import here to avoid circular import
        from main import robust_oanda_request
        from oandapyV20.endpoints.accounts import AccountDetails
        
        account_request = AccountDetails(accountID=config.oanda_account_id)
        response = await robust_oanda_request(account_request)
        return float(response['account']['balance'])
    except Exception as e:
        logger.error(f"Error getting account balance: {e}")
        return 10000.0  # Fallback

# ===== ATR CALCULATION FUNCTIONS =====

async def get_atr(symbol: str, timeframe: str, period: int = 14) -> float:
    """
    Get ATR value for symbol/timeframe by fetching historical data and calculating real ATR
    
    Args:
        symbol: Trading symbol (e.g., "EUR_USD")
        timeframe: Timeframe (e.g., "H1", "M15")
        period: ATR calculation period (default 14)
        
    Returns:
        float: ATR value
    """
    try:
        # Get historical candle data
        candles = await fetch_historical_data(symbol, timeframe, count=period + 5)
        
        if len(candles) < period + 1:
            logger.warning(f"Insufficient data for ATR calculation: {len(candles)} candles, need {period + 1}")
            return get_fallback_atr(symbol, timeframe)
        
        # Calculate True Range for each period
        true_ranges = []
        
        for i in range(1, len(candles)):
            current = candles[i]
            previous = candles[i-1]
            
            # True Range = max(high-low, |high-prev_close|, |low-prev_close|)
            high = float(current.get('high', current.get('h', 0)))
            low = float(current.get('low', current.get('l', 0)))
            prev_close = float(previous.get('close', previous.get('c', 0)))
            
            tr1 = high - low
            tr2 = abs(high - prev_close)
            tr3 = abs(low - prev_close)
            
            true_range = max(tr1, tr2, tr3)
            true_ranges.append(true_range)
        
        if len(true_ranges) < period:
            logger.warning(f"Insufficient true ranges calculated: {len(true_ranges)}, need {period}")
            return get_fallback_atr(symbol, timeframe)
        
        # Calculate ATR using Simple Moving Average of True Ranges
        atr_values = []
        
        # First ATR value is simple average of first 'period' true ranges
        first_atr = sum(true_ranges[:period]) / period
        atr_values.append(first_atr)
        
        # Subsequent ATR values use Wilder's smoothing: ATR = ((ATR_prev * (period-1)) + TR) / period
        for i in range(period, len(true_ranges)):
            current_tr = true_ranges[i]
            prev_atr = atr_values[-1]
            
            # Wilder's smoothing method
            atr = ((prev_atr * (period - 1)) + current_tr) / period
            atr_values.append(atr)
        
        # Return the most recent ATR value
        final_atr = atr_values[-1]
        
        logger.debug(f"Calculated ATR for {symbol} {timeframe}: {final_atr:.6f}")
        return final_atr
        
    except Exception as e:
        logger.error(f"Error calculating ATR for {symbol} {timeframe}: {e}")
        return get_fallback_atr(symbol, timeframe)

async def fetch_historical_data(symbol: str, timeframe: str, count: int = 20) -> list:
    """
    Fetch historical candlestick data from OANDA
    
    Args:
        symbol: Trading symbol
        timeframe: Timeframe 
        count: Number of candles to fetch
        
    Returns:
        list: List of candle dictionaries with OHLC data
    """
    try:
        from oandapyV20.endpoints.instruments import InstrumentsCandles
        
        # Convert timeframe to OANDA format
        oanda_timeframe = convert_timeframe_to_oanda(timeframe)
        
        # Prepare request parameters
        params = {
            "granularity": oanda_timeframe,
            "count": count,
            "price": "M"  # Mid prices
        }
        
        # Create request
        candles_request = InstrumentsCandles(
            instrument=symbol,
            params=params
        )
        
        # Execute request
        from main import robust_oanda_request
        response = await robust_oanda_request(candles_request)
        
        # Parse candles
        candles = []
        if 'candles' in response:
            for candle_data in response['candles']:
                if candle_data.get('complete', True):  # Only use complete candles
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
        
        logger.debug(f"Fetched {len(candles)} candles for {symbol} {timeframe}")
        return candles
        
    except Exception as e:
        logger.error(f"Error fetching historical data for {symbol} {timeframe}: {e}")
        
        # Return simulated data as fallback
        return generate_simulated_candles(symbol, count)

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
    """Generate simulated candle data for fallback"""
    candles = []
    base_price = _get_simulated_price(symbol, "BUY")
    
    for i in range(count):
        # Create realistic OHLC with small variations
        open_price = base_price * (1 + random.uniform(-0.002, 0.002))
        close_price = open_price * (1 + random.uniform(-0.003, 0.003))
        
        high_price = max(open_price, close_price) * (1 + random.uniform(0, 0.002))
        low_price = min(open_price, close_price) * (1 - random.uniform(0, 0.002))
        
        candle = {
            'time': datetime.now(timezone.utc).isoformat(),
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': random.randint(100, 1000)
        }
        candles.append(candle)
        
        # Use close as next open for continuity
        base_price = close_price
    
    return candles

def get_fallback_atr(symbol: str, timeframe: str) -> float:
    """Get fallback ATR values when calculation fails"""
    # Base ATR values for different instruments
    base_atr_values = {
        "EUR_USD": 0.0008,
        "GBP_USD": 0.0012,
        "USD_JPY": 0.08,
        "USD_CHF": 0.0007,
        "AUD_USD": 0.0010,
        "NZD_USD": 0.0009,
        "USD_CAD": 0.0008,
        "EUR_GBP": 0.0006,
        "EUR_JPY": 0.10,
        "GBP_JPY": 0.12,
        "XAU_USD": 1.5,
        "XAG_USD": 0.15,
        "BTC_USD": 500.0,
        "ETH_USD": 50.0,
    }
    
    # Get base ATR
    base_atr = base_atr_values.get(symbol, 0.001)
    
    # Adjust based on timeframe
    timeframe_multipliers = {
        "M1": 0.2,
        "M5": 0.4,
        "M15": 0.6,
        "M30": 0.8,
        "H1": 1.0,
        "H4": 1.8,
        "D1": 3.0,
        "W1": 5.0
    }
    
    multiplier = timeframe_multipliers.get(normalize_timeframe(timeframe), 1.0)
    fallback_atr = base_atr * multiplier
    
    logger.info(f"Using fallback ATR for {symbol} {timeframe}: {fallback_atr:.6f}")
    return fallback_atr

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

async def get_dynamic_stop_distance(symbol: str, timeframe: str, volatility_factor: float = 1.0) -> float:
    """
    Calculate dynamic stop loss distance based on current market volatility
    
    Args:
        symbol: Trading symbol
        timeframe: Analysis timeframe
        volatility_factor: Multiplier for volatility adjustment (default 1.0)
        
    Returns:
        float: Recommended stop distance in price units
    """
    try:
        # Get current ATR
        current_atr = await get_atr(symbol, timeframe)
        
        # Get multi-timeframe context
        atr_values = await get_multi_timeframe_atr(symbol, timeframe)
        
        # Determine instrument type for base multiplier
        instrument_type = get_instrument_type(symbol)
        base_multiplier = get_atr_multiplier(instrument_type, timeframe)
        
        # Calculate volatility adjustment
        if len(atr_values) >= 2:
            # Compare current timeframe to higher timeframe
            higher_tf = get_higher_timeframe(timeframe)
            if higher_tf in atr_values:
                # Adjust based on multi-timeframe volatility alignment
                tf_ratio = current_atr / atr_values[higher_tf] if atr_values[higher_tf] > 0 else 1.0
                volatility_adjustment = min(2.0, max(0.5, tf_ratio))
            else:
                volatility_adjustment = 1.0
        else:
            volatility_adjustment = 1.0
        
        # Apply market session adjustment
        session = get_current_market_session()
        session_multipliers = {
            'asian': 0.8,      # Lower volatility typically
            'london': 1.2,     # Higher volatility
            'new_york': 1.1,   # Moderate-high volatility
            'weekend': 0.5     # Minimal movement
        }
        session_mult = session_multipliers.get(session, 1.0)
        
        # Final calculation
        stop_distance = (current_atr * base_multiplier * volatility_factor * 
                        volatility_adjustment * session_mult)
        
        logger.debug(f"Dynamic stop for {symbol} {timeframe}: ATR={current_atr:.6f}, "
                    f"base_mult={base_multiplier}, vol_adj={volatility_adjustment:.2f}, "
                    f"session_mult={session_mult}, final={stop_distance:.6f}")
        
        return stop_distance
        
    except Exception as e:
        logger.error(f"Error calculating dynamic stop distance for {symbol}: {e}")
        # Fallback to static calculation
        fallback_atr = get_fallback_atr(symbol, timeframe)
        instrument_type = get_instrument_type(symbol)
        base_multiplier = get_atr_multiplier(instrument_type, timeframe)
        return fallback_atr * base_multiplier * volatility_factor

def calculate_position_risk_amount(account_balance: float, risk_percentage: float, 
                                 entry_price: float, stop_loss: float, 
                                 symbol: str) -> Tuple[float, float]:
    """
    Calculate position size based on risk amount and stop loss distance
    
    Args:
        account_balance: Account balance
        risk_percentage: Risk as percentage (e.g., 2.0 for 2%)
        entry_price: Entry price
        stop_loss: Stop loss price
        symbol: Trading symbol
        
    Returns:
        tuple: (position_size, risk_amount)
    """
    try:
        # Calculate risk amount in account currency
        risk_amount = account_balance * (risk_percentage / 100.0)
        
        # Calculate stop distance
        stop_distance = abs(entry_price - stop_loss)
        
        if stop_distance <= 0:
            logger.error(f"Invalid stop distance: {stop_distance}")
            return 0.0, 0.0
        
        # Get pip value for position sizing
        pip_value = get_pip_value(symbol)
        
        # Calculate position size
        # Position Size = Risk Amount / (Stop Distance in pips * Pip Value)
        stop_distance_pips = stop_distance / pip_value
        
        if stop_distance_pips <= 0:
            logger.error(f"Invalid stop distance in pips: {stop_distance_pips}")
            return 0.0, 0.0
        
        # For forex, position size is typically in units of base currency
        position_size = risk_amount / stop_distance
        
        # Apply position size limits based on instrument
        min_size, max_size = get_position_size_limits(symbol)
        position_size = max(min_size, min(max_size, position_size))
        
        logger.debug(f"Position sizing for {symbol}: Risk=${risk_amount:.2f}, "
                    f"Stop distance={stop_distance:.6f}, Size={position_size:.4f}")
        
        return position_size, risk_amount
        
    except Exception as e:
        logger.error(f"Error calculating position risk for {symbol}: {e}")
        return 0.0, 0.0

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

async def validate_trade_setup(symbol: str, entry_price: float, stop_loss: float, 
                              take_profit: float, timeframe: str) -> Dict[str, Any]:
    """
    Validate a complete trade setup using ATR and volatility analysis
    
    Args:
        symbol: Trading symbol
        entry_price: Proposed entry price
        stop_loss: Proposed stop loss
        take_profit: Proposed take profit
        timeframe: Analysis timeframe
        
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
        
        # Market session considerations
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


# In utils.py or a dedicated validation.py module

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
    if units < min_units:
        return False, f"Units ({units}) below minimum trade size ({min_units})."
    if units > max_units:
        return False, f"Units ({units}) above maximum allowed ({max_units})."
    if risk_percent is None or risk_percent <= 0:
        return False, f"Risk percent ({risk_percent}) is zero or negative."
    if atr is None or atr <= 0:
        return False, f"ATR ({atr}) is zero or negative."
    if stop_loss_distance is None or stop_loss_distance <= 0:
        return False, f"Stop loss distance ({stop_loss_distance}) is zero or negative."
    return True, "Inputs valid"



async def get_current_price(symbol: str, action: str) -> float:
    """Get current price for symbol"""
    try:
        # Import here to avoid circular import
        from oandapyV20.endpoints.pricing import PricingInfo
        
        pricing_request = PricingInfo(
            accountID=config.oanda_account_id,
            params={"instruments": symbol}
        )
        
        # Import here to avoid circular import
        try:
            from main import robust_oanda_request
            response = await robust_oanda_request(pricing_request)
        except ImportError:
            # Fallback if main is not available
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
    except Exception as e:
        logger.error(f"Error getting current price for {symbol}: {e}")
        
    # Fallback to simulated price
    return _get_simulated_price(symbol, action)

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

def get_volatility_stop_loss_modifier(symbol: str) -> float:
    """Get stop loss modifier based on volatility (placeholder)"""
    # This is a placeholder - you can implement your volatility logic here
    return 1.0

class VolatilityMonitor:
    """Simple volatility monitor for utils fallback"""
    def get_stop_loss_modifier(self, symbol: str) -> float:
        return get_volatility_stop_loss_modifier(symbol)

# Add this function at the end of utils.py:

def resolve_template_symbol(alert_data: Dict[str, Any]) -> Optional[str]:
    """
    Resolve {{ticker}} template variables using multiple fallback methods
    Returns the resolved symbol or None if unable to resolve
    """
    
    # Method 1: Check if TradingView passes symbol in other fields
    for field in ["instrument", "ticker", "pair", "asset"]:
        if field in alert_data and alert_data[field] != "{{ticker}}":
            symbol = alert_data[field]
            logger.info(f"[TEMPLATE] Found symbol in '{field}': {symbol}")
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
            r'(ETH[/_]?USD)',            # Ethereum
            r'(XRP[/_]?USD)',            # Ripple
            r'(LTC[/_]?USD)',            # Litecoin
        ]
        for pattern in symbol_patterns:
            match = re.search(pattern, comment.upper())
            if match:
                symbol = match.group(1)
                logger.info(f"[TEMPLATE] Extracted symbol from comment: {symbol}")
                return symbol
    
    # Method 3: Check for strategy-specific defaults
    if "strategy" in alert_data:
        strategy = alert_data["strategy"]
        strategy_defaults = {
            "Lorentzian_Classification": "EURUSD",
            "RSI_Strategy": "GBPUSD", 
            "MA_Cross": "USDJPY",
            "Breakout_Strategy": "GBPUSD",
            "Trend_Following": "EURUSD",
            # Add your strategy defaults here
        }
        symbol = strategy_defaults.get(strategy)
        if symbol:
            logger.info(f"[TEMPLATE] Using strategy default for '{strategy}': {symbol}")
            return symbol
    
    # Method 4: Use timeframe-based defaults (last resort)
    timeframe = alert_data.get("timeframe", "60")
    timeframe_defaults = {
        "1": "EURUSD",    # 1-minute scalping
        "5": "GBPUSD",    # 5-minute
        "15": "EURUSD",   # 15-minute  
        "60": "EURUSD",   # 1-hour
        "240": "USDJPY",  # 4-hour
        "1440": "GBPUSD", # Daily
    }
    symbol = timeframe_defaults.get(str(timeframe), "EURUSD")
    logger.warning(f"[TEMPLATE] Using timeframe default for {timeframe}min: {symbol}")
    return symbol
